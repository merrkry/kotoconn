use crate::{Scope, Target, queue};
use bytes::Bytes;
use std::pin::Pin;

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
}

impl Drop for Datagram {
    fn drop(&mut self) {
        self.scope.close();
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
        },
        Datagram {
            tx: b_tx,
            rx: b_rx,
            scope,
        },
    )
}
