//! Byte-weighted queues with local feedback. No transport parameters cross adapters.
use crossbeam_queue::SegQueue;
use futures_util::task::AtomicWaker;
use std::{
    fmt, io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll, ready},
    time::Duration,
};
use tokio::{sync::Notify, task::coop, time::Instant};

/// Initial burst allowance, not a maximum allocation or connection quota.
pub const INITIAL_BYTES: usize = 128 * 1024;
pub const FEEDBACK_INTERVAL: Duration = Duration::from_millis(16);
pub const TARGET_DELAY: Duration = Duration::from_micros(512);

/// A queue retains TARGET_DELAY of its measured consumption.
/// A stalled consumer cannot increase this target by filling the queue.
#[derive(Debug)]
pub struct Capacity {
    initial: usize,
    target: usize,
    epoch: Instant,
    completed: usize,
}

impl Capacity {
    pub fn new(initial: usize) -> Self {
        Self {
            initial,
            target: initial,
            epoch: Instant::now(),
            completed: 0,
        }
    }

    pub fn target(&mut self, now: Instant) -> usize {
        let elapsed = now.saturating_duration_since(self.epoch);
        if elapsed >= FEEDBACK_INTERVAL {
            let sample = (self.completed as u128).saturating_mul(TARGET_DELAY.as_nanos())
                / elapsed.as_nanos();
            let sample = usize::try_from(sample).unwrap_or(usize::MAX);
            let periods = (elapsed.as_nanos() / FEEDBACK_INTERVAL.as_nanos())
                .min(usize::BITS as u128 - 1) as u32;
            self.target = sample.max(self.target >> periods).max(self.initial);
            self.completed = 0;
            self.epoch = now;
        }
        self.target
    }

    pub fn complete(&mut self, bytes: usize, now: Instant) {
        self.target(now);
        self.completed = self.completed.saturating_add(bytes);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Full,
    Closed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Full => "queue full",
            Self::Closed => "queue closed",
        })
    }
}

impl std::error::Error for Error {}

impl From<Error> for io::Error {
    fn from(error: Error) -> Self {
        io::Error::new(
            match error {
                Error::Full => io::ErrorKind::WouldBlock,
                Error::Closed => io::ErrorKind::BrokenPipe,
            },
            error,
        )
    }
}

struct Shared<T> {
    items: SegQueue<(T, usize)>,
    initial: usize,
    epoch: Instant,
    updated: AtomicU64,
    completed: AtomicUsize,
    refreshing: AtomicBool,
    bytes: AtomicUsize,
    senders: AtomicUsize,
    closed: AtomicBool,
    target: AtomicUsize,
    readable: AtomicWaker,
    writable: Notify,
    size: fn(&T) -> usize,
}

impl<T> Shared<T> {
    fn refresh(&self) {
        let now = self.epoch.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let updated = self.updated.load(Ordering::Acquire);
        let elapsed = now.saturating_sub(updated);
        if elapsed < FEEDBACK_INTERVAL.as_nanos() as u64
            || self.refreshing.swap(true, Ordering::Acquire)
        {
            return;
        }
        // Another updater may have finished between the timestamp load and acquisition.
        let elapsed = now.saturating_sub(self.updated.load(Ordering::Relaxed));
        if elapsed >= FEEDBACK_INTERVAL.as_nanos() as u64 {
            let completed = self.completed.swap(0, Ordering::AcqRel);
            let sample = (completed as u128 * TARGET_DELAY.as_nanos() / elapsed as u128)
                .min(usize::MAX as u128) as usize;
            let periods =
                (elapsed / FEEDBACK_INTERVAL.as_nanos() as u64).min(usize::BITS as u64 - 1) as u32;
            let target = sample
                .max(self.target.load(Ordering::Relaxed) >> periods)
                .max(self.initial);
            self.target.store(target, Ordering::Release);
            self.updated.store(now, Ordering::Release);
        }
        self.refreshing.store(false, Ordering::Release);
    }

    fn release(&self, cost: usize) {
        let old = self.bytes.fetch_sub(cost, Ordering::AcqRel);
        debug_assert!(old >= cost);
    }

    fn discard(&self) {
        while let Some((_, cost)) = self.items.pop() {
            self.release(cost);
        }
    }
}

pub struct Sender<T>(Arc<Shared<T>>);

pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

/// Byte credits include outstanding reservations. Completion counters and target
/// refreshes are shared atomically; producers reserve against the current target.
pub fn channel<T>(initial: usize, size: fn(&T) -> usize) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        items: SegQueue::new(),
        initial,
        epoch: Instant::now(),
        updated: AtomicU64::new(0),
        completed: AtomicUsize::new(0),
        refreshing: AtomicBool::new(false),
        bytes: AtomicUsize::new(0),
        senders: AtomicUsize::new(1),
        closed: AtomicBool::new(false),
        target: AtomicUsize::new(initial),
        readable: AtomicWaker::new(),
        writable: Notify::new(),
        size,
    });
    (Sender(shared.clone()), Receiver { shared })
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.0.senders.fetch_add(1, Ordering::Relaxed);
        Self(self.0.clone())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.0.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.readable.wake();
        }
    }
}

pub struct Permit<'a, T> {
    sender: &'a Sender<T>,
    cost: usize,
}

impl<T> Sender<T> {
    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::Acquire)
    }

    pub fn try_reserve(&self, bytes: usize) -> Result<Permit<'_, T>, Error> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let cost = bytes
            .checked_add(std::mem::size_of::<(T, usize)>().max(1))
            .ok_or(Error::Full)?;
        self.0.refresh();
        let target = self.0.target.load(Ordering::Acquire);
        self.0
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                if used != 0 && cost > target.saturating_sub(used) {
                    return None;
                }
                used.checked_add(cost)
            })
            .map_err(|_| Error::Full)?;
        let permit = Permit { sender: self, cost };
        if self.is_closed() {
            return Err(Error::Closed);
        }
        Ok(permit)
    }

    pub async fn reserve(&self, bytes: usize) -> Result<Permit<'_, T>, Error> {
        coop::cooperative(self.wait_for_capacity(bytes)).await
    }

    async fn wait_for_capacity(&self, bytes: usize) -> Result<Permit<'_, T>, Error> {
        loop {
            let notified = self.0.writable.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_reserve(bytes) {
                Err(Error::Full) => notified.await,
                result => return result,
            }
        }
    }

    pub fn try_send(&self, item: T) -> Result<(), Error> {
        self.try_reserve((self.0.size)(&item))?.send(item);
        Ok(())
    }

    pub async fn send(&self, item: T) -> Result<(), Error> {
        self.reserve((self.0.size)(&item)).await?.send(item);
        Ok(())
    }
}

impl<T> Permit<'_, T> {
    pub fn send(mut self, item: T) {
        debug_assert!(
            (self.sender.0.size)(&item) <= self.cost - std::mem::size_of::<(T, usize)>().max(1)
        );
        if self.sender.is_closed() {
            return;
        }
        self.sender.0.items.push((item, self.cost));
        self.cost = 0;
        // Closure may race publication. Whichever observes the published item
        // releases its credit; SegQueue gives each item to exactly one consumer.
        if self.sender.is_closed() {
            self.sender.0.discard();
        }
        self.sender.0.readable.wake();
    }
}

impl<T> Drop for Permit<'_, T> {
    fn drop(&mut self) {
        if self.cost != 0 {
            self.sender.0.release(self.cost);
            self.sender.0.writable.notify_waiters();
        }
    }
}

impl<T> Receiver<T> {
    fn complete(&mut self, cost: usize) {
        self.shared.release(cost);
        self.shared.refresh();
        self.shared.completed.fetch_add(cost, Ordering::Relaxed);
        self.shared.writable.notify_waiters();
    }

    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let budget = ready!(coop::poll_proceed(cx));
        self.shared.readable.register(cx.waker());
        if let Some((item, cost)) = self.shared.items.pop() {
            self.complete(cost);
            budget.made_progress();
            return Poll::Ready(Some(item));
        }
        if self.shared.senders.load(Ordering::Acquire) == 0 {
            // Last-sender publication precedes its Release decrement.
            if let Some((item, cost)) = self.shared.items.pop() {
                self.complete(cost);
                budget.made_progress();
                return Poll::Ready(Some(item));
            }
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    pub async fn recv(&mut self) -> Option<T> {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub fn try_recv(&mut self) -> Result<T, Error> {
        if let Some((item, cost)) = self.shared.items.pop() {
            self.complete(cost);
            Ok(item)
        } else {
            Err(if self.shared.senders.load(Ordering::Acquire) == 0 {
                Error::Closed
            } else {
                Error::Full
            })
        }
    }

    pub async fn recv_many(&mut self, out: &mut Vec<T>, limit: usize) -> usize {
        if limit == 0 {
            return 0;
        }
        let Some(first) = self.recv().await else {
            return 0;
        };
        out.push(first);
        let mut count = 1;
        let mut cost = 0;
        while count < limit {
            let Some((item, bytes)) = self.shared.items.pop() else {
                break;
            };
            out.push(item);
            cost += bytes;
            count += 1;
        }
        if cost != 0 {
            self.complete(cost);
        }
        count
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
        self.shared.discard();
        self.shared.writable.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn consumption_grows_capacity_but_a_stalled_consumer_does_not() {
        let (tx, mut rx) = channel(128, Vec::<u8>::len);
        tx.try_send(vec![0; 80]).unwrap();
        assert_eq!(tx.try_send(vec![0; 80]), Err(Error::Full));
        tokio::time::advance(FEEDBACK_INTERVAL).await;
        assert_eq!(tx.try_send(vec![0; 80]), Err(Error::Full));
        for _ in 0..1024 {
            rx.recv().await.unwrap();
            tx.try_send(vec![0; 80]).unwrap();
        }
        tokio::time::advance(FEEDBACK_INTERVAL).await;
        for _ in 0..8 {
            tx.try_send(vec![0; 80]).unwrap();
        }
    }

    #[tokio::test]
    async fn cancellation_returns_reservations_and_receiver_close_wakes_producers() {
        let (tx, mut rx) = channel(1, Vec::<u8>::len);
        let permit = tx.reserve(1024).await.unwrap();
        assert_eq!(tx.try_send(vec![1]), Err(Error::Full));
        drop(permit);
        tx.try_send(vec![2]).unwrap();
        assert_eq!(rx.recv().await.unwrap(), [2]);
        tx.try_send(vec![3]).unwrap();
        let waiting = tx.send(vec![4]);
        tokio::pin!(waiting);
        assert!(futures_util::poll!(&mut waiting).is_pending());
        drop(rx);
        assert_eq!(waiting.await, Err(Error::Closed));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn competing_producers_deliver_in_order_and_release_the_receiver() {
        let (tx, mut rx) = channel(256, |_: &(usize, usize)| 64);
        let mut producers = tokio::task::JoinSet::new();
        for producer in 0..8 {
            let tx = tx.clone();
            producers.spawn(async move {
                for sequence in 0..256 {
                    if sequence % 17 == 0 {
                        // A cancelled reservation must not consume capacity or
                        // a descriptor needed by the other producers.
                        drop(tx.reserve(64).await.unwrap());
                    }
                    tx.send((producer, sequence)).await.unwrap();
                }
            });
        }
        drop(tx);
        let mut counts = [0; 8];
        while let Some((producer, sequence)) = rx.recv().await {
            assert_eq!(sequence, counts[producer]);
            counts[producer] += 1;
        }
        assert_eq!(counts, [256; 8]);
        while let Some(result) = producers.join_next().await {
            result.unwrap();
        }
    }
}
