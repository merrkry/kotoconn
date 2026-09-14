use crate::{Activity, BoxPacketIo, PacketIo, Scope, Target, queue};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use std::pin::Pin;
use std::{
    io,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::oneshot;

pub use crate::chunk::{ChunkBuffer, Stream, copy_bidirectional, prefix};
pub use crate::scoped::stream_task;

pub type BoxStream = Pin<Box<dyn Stream>>;

#[derive(Debug, Clone)]
pub struct Packet {
    pub target: Target,
    /// Shared ownership lets ingress retain resource accounting through routing queues.
    pub payload: Bytes,
}

/// Each message is one complete datagram. No framing, mux or retransmission here.
/// Ingress on a shared socket uses try_send: a full session queue drops that packet.
pub struct Datagram {
    pub tx: queue::Sender<Packet>,
    pub rx: queue::Receiver<Packet>,
    pub scope: Scope,
    /// A TUN flow has one destination, so routing can own its ingress directly.
    pub single_target: Option<Target>,
    pub worker: Option<Arc<dyn DatagramWorker>>,
    handoff: Option<oneshot::Sender<oneshot::Sender<Option<BoxPacketIo>>>>,
    close_on_drop: bool,
}

impl Drop for Datagram {
    fn drop(&mut self) {
        if self.close_on_drop {
            self.scope.close();
        }
    }
}

/// A flow owner receives the selected transport only after policy routing.
pub trait DatagramWorker: Send + Sync {
    fn transfer(
        &self,
        io: BoxPacketIo,
        incoming: queue::Receiver<Packet>,
        activity: Activity,
    ) -> BoxFuture<'static, io::Result<()>>;
}

impl Datagram {
    /// The driver must stop and drop its queue endpoints before delivering the
    /// native transport. No later packet may be published to the old reply queue.
    pub fn offer_handoff(&mut self) -> oneshot::Receiver<oneshot::Sender<Option<BoxPacketIo>>> {
        let (sender, receiver) = oneshot::channel();
        self.handoff = Some(sender);
        receiver
    }

    /// The driver relinquishes its close guard only after transferring ownership.
    pub fn disarm(&mut self) {
        self.close_on_drop = false;
    }

    pub fn take_receiver(&mut self) -> queue::Receiver<Packet> {
        let (sender, receiver) =
            queue::channel(queue::INITIAL_BYTES, |packet: &Packet| packet.payload.len());
        drop(sender);
        std::mem::replace(&mut self.rx, receiver)
    }

    /// Must precede application sends. A driver that already consumed an outgoing
    /// datagram declines. Buffered replies remain ahead of future native receives.
    pub async fn take_native(&mut self) -> io::Result<Option<BoxPacketIo>> {
        let Some(request) = self.handoff.take() else {
            return Ok(None);
        };
        let (reply, receive) = oneshot::channel();
        request
            .send(reply)
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
        let Some(io) = receive
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?
        else {
            return Ok(None);
        };
        Ok(Some(Box::new(BufferedPackets {
            io,
            pending: Some(self.take_receiver()),
        })))
    }
}

struct BufferedPackets {
    io: BoxPacketIo,
    pending: Option<queue::Receiver<Packet>>,
}

impl PacketIo for BufferedPackets {
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        out: &mut Vec<Packet>,
    ) -> Poll<io::Result<usize>> {
        if let Some(pending) = &mut self.pending {
            let before = out.len();
            while out.len() - before < 32 {
                match pending.try_recv() {
                    Ok(packet) => out.push(packet),
                    Err(queue::Error::Closed) => {
                        self.pending = None;
                        break;
                    }
                    Err(queue::Error::Full) => break,
                }
            }
            if out.len() != before {
                return Poll::Ready(Ok(out.len() - before));
            }
        }
        self.io.poll_recv(cx, out)
    }
    fn poll_send(&mut self, cx: &mut Context<'_>, packets: &[Packet]) -> Poll<io::Result<usize>> {
        self.io.poll_send(cx, packets)
    }
}

pub fn packet_pair(scope: Scope) -> (Datagram, Datagram) {
    let (a_tx, b_rx) = queue::channel(queue::INITIAL_BYTES, |packet: &Packet| packet.payload.len());
    let (b_tx, a_rx) = queue::channel(queue::INITIAL_BYTES, |packet: &Packet| packet.payload.len());

    (
        Datagram {
            tx: a_tx,
            rx: a_rx,
            scope: scope.clone(),
            single_target: None,
            worker: None,
            handoff: None,
            close_on_drop: true,
        },
        Datagram {
            tx: b_tx,
            rx: b_rx,
            scope,
            single_target: None,
            worker: None,
            handoff: None,
            close_on_drop: true,
        },
    )
}
