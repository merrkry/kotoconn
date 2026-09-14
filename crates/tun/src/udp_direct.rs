use crate::{packet::Flow, tcp, transmit::Transmit, udp};
use kotoconn_protocol::{self as p, queue};
use std::{
    collections::VecDeque,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

pub(crate) struct Transfer {
    pub id: tcp::Ready,
    io: p::BoxPacketIo,
    incoming: queue::Receiver<p::Packet>,
    activity: p::Activity,
    done: oneshot::Sender<()>,
}

pub(crate) struct Handoff {
    pub id: tcp::Ready,
    pub sender: mpsc::UnboundedSender<Transfer>,
    pub scope: p::Scope,
}

struct Cancel(p::Scope);

impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.close();
    }
}

impl p::DatagramWorker for Handoff {
    fn transfer(
        &self,
        io: p::BoxPacketIo,
        incoming: queue::Receiver<p::Packet>,
        activity: p::Activity,
    ) -> futures_util::future::BoxFuture<'static, io::Result<()>> {
        let (done, completion) = oneshot::channel();
        let result = self.sender.send(Transfer {
            id: self.id,
            io,
            incoming,
            activity,
            done,
        });
        let cancel = Cancel(self.scope.clone());
        Box::pin(async move {
            let _cancel = cancel;
            result.map_err(|_| io::Error::other("TUN worker stopped"))?;
            completion
                .await
                .map_err(|_| io::Error::other("TUN transfer stopped"))
        })
    }
}

struct Ready {
    id: tcp::Ready,
    sender: mpsc::UnboundedSender<tcp::Ready>,
    queued: AtomicBool,
}

impl Wake for Ready {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if !self.queued.swap(true, Ordering::AcqRel) {
            let _ = self.sender.send(self.id);
        }
    }
}

pub(crate) struct Direct {
    flow: Flow,
    io: p::BoxPacketIo,
    pending: VecDeque<p::Packet>,
    received: Vec<p::Packet>,
    received_offset: usize,
    bytes: usize,
    capacity: queue::Capacity,
    activity: p::Activity,
    done: Option<oneshot::Sender<()>>,
    ready: Arc<Ready>,
}

impl Direct {
    pub fn new(mut transfer: Transfer, sender: mpsc::UnboundedSender<tcp::Ready>) -> Self {
        let mut pending = VecDeque::new();
        let mut bytes = 0;
        // The worker processes this transfer between input frames. Its old queue
        // has no other producer, so this drain precedes every future arrival.
        while let Ok(packet) = transfer.incoming.try_recv() {
            bytes += cost(&packet);
            pending.push_back(packet);
        }
        let direct = Self {
            flow: transfer.id.0,
            io: transfer.io,
            pending,
            received: Vec::with_capacity(32),
            received_offset: 0,
            bytes,
            capacity: queue::Capacity::new(queue::INITIAL_BYTES),
            activity: transfer.activity,
            done: Some(transfer.done),
            ready: Arc::new(Ready {
                id: transfer.id,
                sender,
                queued: AtomicBool::new(false),
            }),
        };
        direct.wake();
        direct
    }

    pub fn wake(&self) {
        self.ready.wake_by_ref();
    }

    pub fn enqueue(&mut self, packet: p::Packet) -> bool {
        let cost = cost(&packet);
        if self.bytes != 0 && self.bytes.saturating_add(cost) > self.capacity.target(Instant::now())
        {
            return false;
        }
        self.bytes += cost;
        self.pending.push_back(packet);
        self.wake();
        true
    }

    fn record(&self, activity: &p::Activity) {
        self.activity.record();
        activity.record();
    }

    fn flush_replies(
        &mut self,
        output: &queue::Sender<Transmit>,
        activity: &p::Activity,
    ) -> Progress {
        let mut completed = false;
        let mut progress = Progress::Idle;
        while let Some(packet) = self.received.get_mut(self.received_offset) {
            let flow = p::socket_addr(&packet.target)
                .ok()
                .and_then(|source| udp::reply_flow(self.flow, source));
            let Some((source, destination)) = flow else {
                self.received_offset += 1;
                continue;
            };
            let cost = packet.payload.len() + 48;
            let permit = match output.try_reserve(cost) {
                Ok(permit) => permit,
                Err(queue::Error::Full) => {
                    progress = Progress::Blocked(cost);
                    break;
                }
                Err(queue::Error::Closed) => {
                    progress = Progress::Closed;
                    break;
                }
            };
            permit.send(Transmit::Datagram {
                source,
                destination,
                payload: std::mem::take(&mut packet.payload),
            });
            self.received_offset += 1;
            completed = true;
        }
        if completed {
            self.record(activity);
        }
        if self.received_offset == self.received.len() {
            self.received.clear();
            self.received_offset = 0;
        }
        progress
    }

    /// Retain at most one received batch under output pressure. The worker retries
    /// on output readiness; this flow's send half and other flows remain runnable.
    pub fn poll(&mut self, output: &queue::Sender<Transmit>, activity: &p::Activity) -> Progress {
        self.ready.queued.store(false, Ordering::Release);
        let waker = Waker::from(self.ready.clone());
        let mut cx = Context::from_waker(&waker);
        if !self.pending.is_empty() {
            let packets = self.pending.make_contiguous();
            let count = match self
                .io
                .poll_send(&mut cx, &packets[..packets.len().min(32)])
            {
                Poll::Ready(Ok(count)) => {
                    debug_assert!(count > 0 && count <= packets.len());
                    self.record(activity);
                    count
                }
                Poll::Ready(Err(error)) => {
                    tracing::warn!(%error, "direct UDP send");
                    1
                }
                Poll::Pending => 0,
            };
            if count != 0 {
                let bytes = self
                    .pending
                    .drain(..count)
                    .map(|packet| cost(&packet))
                    .sum::<usize>();
                debug_assert!(self.bytes >= bytes);
                self.bytes -= bytes;
                self.capacity.complete(bytes, Instant::now());
                if !self.pending.is_empty() {
                    self.wake();
                }
            }
        }
        match self.flush_replies(output, activity) {
            Progress::Idle => {}
            blocked => return blocked,
        }
        match self.io.poll_recv(&mut cx, &mut self.received) {
            Poll::Ready(Ok(0)) => Progress::Closed,
            Poll::Ready(Ok(_)) => {
                let progress = self.flush_replies(output, activity);
                if matches!(progress, Progress::Idle) {
                    self.wake();
                }
                progress
            }
            Poll::Ready(Err(error)) => {
                tracing::warn!(%error, "direct UDP receive");
                self.wake();
                Progress::Idle
            }
            Poll::Pending => Progress::Idle,
        }
    }
}

pub(crate) enum Progress {
    Idle,
    Blocked(usize),
    Closed,
}

fn cost(packet: &p::Packet) -> usize {
    packet.payload.len() + std::mem::size_of::<p::Packet>()
}

impl Drop for Direct {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Partial(mpsc::UnboundedSender<u8>);
    impl p::PacketIo for Partial {
        fn poll_recv(
            &mut self,
            _: &mut Context<'_>,
            _: &mut Vec<p::Packet>,
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
        fn poll_send(
            &mut self,
            _: &mut Context<'_>,
            packets: &[p::Packet],
        ) -> Poll<io::Result<usize>> {
            let count = packets.len().min(2);
            for packet in &packets[..count] {
                self.0.send(packet.payload[0]).unwrap();
            }
            Poll::Ready(Ok(count))
        }
    }

    struct Replies {
        sent: mpsc::UnboundedSender<u8>,
        replies: Vec<p::Packet>,
    }

    impl p::PacketIo for Replies {
        fn poll_recv(
            &mut self,
            _: &mut Context<'_>,
            out: &mut Vec<p::Packet>,
        ) -> Poll<io::Result<usize>> {
            if self.replies.is_empty() {
                return Poll::Pending;
            }
            let count = self.replies.len();
            out.append(&mut self.replies);
            Poll::Ready(Ok(count))
        }
        fn poll_send(
            &mut self,
            _: &mut Context<'_>,
            packets: &[p::Packet],
        ) -> Poll<io::Result<usize>> {
            for packet in packets {
                self.sent.send(packet.payload[0]).unwrap();
            }
            Poll::Ready(Ok(packets.len()))
        }
    }

    #[tokio::test]
    async fn output_pressure_retains_empty_replies_and_allows_reverse_traffic() {
        let flow = Flow {
            source: "192.0.2.2:1000".parse().unwrap(),
            destination: "198.51.100.1:2000".parse().unwrap(),
        };
        let packet = |payload: Vec<u8>| p::Packet {
            target: p::target(flow.destination),
            payload: payload.into(),
        };
        let (input, incoming) =
            queue::channel(queue::INITIAL_BYTES, |p: &p::Packet| p.payload.len());
        drop(input);
        let (sent, mut sends) = mpsc::unbounded_channel();
        let (done, completion) = oneshot::channel();
        let (ready, _events) = mpsc::unbounded_channel();
        let mut direct = Direct::new(
            Transfer {
                id: (flow, 1),
                io: Box::new(Replies {
                    sent,
                    replies: vec![packet(vec![]), packet(vec![3])],
                }),
                incoming,
                activity: p::Activity::default(),
                done,
            },
            ready,
        );
        let (output, mut packets) = queue::channel(1, Transmit::size);
        output
            .try_send(Transmit::Packet(vec![0; 40].into()))
            .unwrap();
        let activity = p::Activity::default();
        assert!(matches!(
            direct.poll(&output, &activity),
            Progress::Blocked(_)
        ));
        assert!(direct.enqueue(packet(vec![9])));
        assert!(matches!(
            direct.poll(&output, &activity),
            Progress::Blocked(_)
        ));
        assert_eq!(sends.recv().await.unwrap(), 9);
        packets.try_recv().unwrap();
        assert!(matches!(
            direct.poll(&output, &activity),
            Progress::Blocked(_)
        ));
        let Transmit::Datagram { payload, .. } = packets.try_recv().unwrap() else {
            panic!()
        };
        assert!(payload.is_empty());
        assert!(matches!(direct.poll(&output, &activity), Progress::Idle));
        let Transmit::Datagram { payload, .. } = packets.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(payload, [3][..]);
        assert!(packets.try_recv().is_err());
        drop(direct);
        completion.await.unwrap();
    }

    #[tokio::test]
    async fn new_ingress_follows_prequeued_packets_across_partial_native_sends() {
        let flow = Flow {
            source: "192.0.2.2:1000".parse().unwrap(),
            destination: "198.51.100.1:2000".parse().unwrap(),
        };
        let packet = |byte| p::Packet {
            target: p::target(flow.destination),
            payload: vec![byte].into(),
        };
        let (tx, rx) = queue::channel(queue::INITIAL_BYTES, |p: &p::Packet| p.payload.len());
        tx.send(packet(0)).await.unwrap();
        tx.send(packet(1)).await.unwrap();
        let (sent, mut received) = mpsc::unbounded_channel();
        let (done, completion) = oneshot::channel();
        let (ready, _events) = mpsc::unbounded_channel();
        let mut direct = Direct::new(
            Transfer {
                id: (flow, 1),
                io: Box::new(Partial(sent)),
                incoming: rx,
                activity: p::Activity::default(),
                done,
            },
            ready,
        );
        assert!(direct.enqueue(packet(2)));
        assert!(direct.enqueue(packet(3)));
        let (output, _packets) = queue::channel(queue::INITIAL_BYTES, Transmit::size);
        assert!(matches!(
            direct.poll(&output, &p::Activity::default()),
            Progress::Idle
        ));
        assert!(matches!(
            direct.poll(&output, &p::Activity::default()),
            Progress::Idle
        ));
        for byte in 0..4 {
            assert_eq!(received.recv().await.unwrap(), byte);
        }
        drop(direct);
        completion.await.unwrap();
    }
}
