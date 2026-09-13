//! Byte-weighted queues with local feedback. No transport parameters cross adapters.
use futures_util::task::AtomicWaker;
use std::{
    collections::VecDeque,
    fmt, io,
    sync::{Arc, Mutex, MutexGuard},
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

struct State<T> {
    items: VecDeque<(T, usize)>,
    bytes: usize,
    senders: usize,
    closed: bool,
    capacity: Capacity,
}

struct Shared<T> {
    state: Mutex<State<T>>,
    readable: AtomicWaker,
    writable: Notify,
    size: fn(&T) -> usize,
}

impl<T> Shared<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        // SAFETY: No user callbacks run under this lock. A panic here invalidates
        // queue accounting; continuing after poisoning could lose accepted data.
        self.state.lock().expect("queue state poisoned")
    }
}

pub struct Sender<T>(Arc<Shared<T>>);

pub struct Receiver<T>(Arc<Shared<T>>);

/// Each queue owns its feedback and backlog accounting. Shared packet paths
/// use try_send; reliable producers wait with send. Metadata counts even for
/// empty datagrams.
pub fn channel<T>(initial: usize, size: fn(&T) -> usize) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            items: VecDeque::new(),
            bytes: 0,
            senders: 1,
            closed: false,
            capacity: Capacity::new(initial),
        }),
        readable: AtomicWaker::new(),
        writable: Notify::new(),
        size,
    });
    (Sender(shared.clone()), Receiver(shared))
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.0.lock().senders += 1;
        Self(self.0.clone())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        debug_assert!(state.senders > 0);
        state.senders -= 1;
        let last = state.senders == 0;
        drop(state);
        if last {
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
        self.0.lock().closed
    }

    pub fn try_reserve(&self, bytes: usize) -> Result<Permit<'_, T>, Error> {
        let cost = bytes
            .checked_add(std::mem::size_of::<(T, usize)>().max(1))
            .ok_or(Error::Full)?;
        let mut state = self.0.lock();
        if state.closed {
            return Err(Error::Closed);
        }
        let target = state.capacity.target(Instant::now());
        // One complete item can exceed the target, e.g. a maximum UDP datagram.
        if state.bytes != 0 && cost > target.saturating_sub(state.bytes) {
            return Err(Error::Full);
        }
        let Some(total) = state.bytes.checked_add(cost) else {
            return Err(Error::Full);
        };
        state.bytes = total;
        Ok(Permit { sender: self, cost })
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
        let mut state = self.sender.0.lock();
        if state.closed {
            return;
        }
        state.items.push_back((item, self.cost));
        self.cost = 0;
        drop(state);
        self.sender.0.readable.wake();
    }
}

impl<T> Drop for Permit<'_, T> {
    fn drop(&mut self) {
        if self.cost != 0 {
            let mut state = self.sender.0.lock();
            debug_assert!(state.bytes >= self.cost);
            state.bytes -= self.cost;
            drop(state);
            self.sender.0.writable.notify_waiters();
        }
    }
}

impl<T> Receiver<T> {
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let coop = ready!(coop::poll_proceed(cx));
        let result = self.poll_recv_inner(cx);
        if result.is_ready() {
            coop.made_progress();
        }
        result
    }

    fn poll_recv_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.0.readable.register(cx.waker());
        let mut state = self.0.lock();
        if let Some((item, cost)) = state.items.pop_front() {
            debug_assert!(state.bytes >= cost);
            state.bytes -= cost;
            state.capacity.complete(cost, Instant::now());
            drop(state);
            self.0.writable.notify_waiters();
            return Poll::Ready(Some(item));
        }
        if state.senders == 0 {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    pub async fn recv(&mut self) -> Option<T> {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    pub fn try_recv(&mut self) -> Result<T, Error> {
        let mut state = self.0.lock();
        if let Some((item, cost)) = state.items.pop_front() {
            state.bytes -= cost;
            state.capacity.complete(cost, Instant::now());
            drop(state);
            self.0.writable.notify_waiters();
            Ok(item)
        } else {
            Err(if state.senders == 0 {
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
        while count < limit {
            let Ok(item) = self.try_recv() else {
                break;
            };
            out.push(item);
            count += 1;
        }
        count
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.closed = true;
        let items = std::mem::take(&mut state.items);
        let removed: usize = items.iter().map(|(_, cost)| cost).sum();
        state.bytes -= removed;
        drop(state);
        drop(items);
        self.0.writable.notify_waiters();
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
