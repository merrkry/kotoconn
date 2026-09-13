use crate::{
    device::Transmit,
    udp,
    worker::{self, Shared},
};
use anyhow::{Context as _, Result, anyhow, bail, ensure};
use kotoconn_protocol::ServerContext;
use std::{
    collections::hash_map::RandomState,
    future::poll_fn,
    io,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

/// A queue has one receive owner. Implementations preserve packet boundaries,
/// register the supplied waker on Pending, and return at most bytes.len() bytes.
pub trait PacketReceive: Send {
    fn poll_recv(&mut self, cx: &mut Context<'_>, bytes: &mut [u8]) -> Poll<io::Result<usize>>;
}

/// A queue has one transmit owner, independent of its receive owner.
pub trait PacketSend: Send {
    fn poll_send(&mut self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>>;

    /// Send only packets already available. Cancellation may transmit a prefix;
    /// the caller must not retry a cancelled batch.
    fn send_batch(
        &mut self,
        packets: &mut [Vec<u8>],
    ) -> impl Future<Output = io::Result<()>> + Send {
        async move {
            for (index, packet) in packets.iter().enumerate() {
                let len = poll_fn(|cx| self.poll_send(cx, packet)).await?;
                if len != packet.len() {
                    return Err(io::Error::other("partial TUN packet write"));
                }
                if (index + 1).is_multiple_of(64) {
                    tokio::task::yield_now().await;
                }
            }
            Ok(())
        }
    }
}

struct ConnectionGuard(kotoconn_protocol::Scope);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        // Dropping run must also cancel connections when its future is aborted.
        self.0.close();
    }
}

/// Own queue workers until TCP drains or cancellation stops device I/O.
/// Queue membership stays fixed so flow and fragment ownership cannot migrate.
pub async fn run<R: PacketReceive + 'static, W: PacketSend + 'static>(
    queues: Vec<(R, W)>,
    mtu: usize,
    mut context: ServerContext,
) -> Result<()> {
    ensure!(
        (1280..=65535).contains(&mtu),
        "TUN MTU must be between 1280 and 65535"
    );
    ensure!(!queues.is_empty(), "TUN needs at least one queue");
    // Local failure cancels this endpoint's connections, without closing other inbounds.
    context.scope = context.scope.child();
    let _connections = ConnectionGuard(context.scope.clone());
    let (inboxes, receivers): (Vec<_>, Vec<_>) =
        (0..queues.len()).map(|_| mpsc::channel(128)).unzip();
    let shared = Arc::new(Shared {
        hash: RandomState::new(),
        inboxes,
        tcp_slots: Arc::new(Semaphore::new(worker::MAX_TCP_CONNECTIONS)),
        udp_slots: Arc::new(Semaphore::new(worker::MAX_UDP_ASSOCIATIONS)),
        tcp_bytes: Arc::new(Semaphore::new(worker::INGRESS_BYTES)),
        udp_bytes: Arc::new(Semaphore::new(worker::INGRESS_BYTES)),
        transit_bytes: Arc::new(Semaphore::new(worker::INGRESS_BYTES)),
        reassembly: Default::default(),
        stop: CancellationToken::new(),
    });
    // Only UDP packetization shares mutable state: IP fragment IDs span flows.
    // Encoding never awaits and writers release this lock before touching a FD.
    let encoder = Arc::new(Mutex::new(udp::Encoder::new(mtu)));
    let mut workers: JoinSet<Result<()>> = JoinSet::new();
    for (id, ((read, write), inbox)) in queues.into_iter().zip(receivers).enumerate() {
        let (output, packets) = mpsc::channel(128);
        let state = shared.clone();
        let context = context.clone();
        let encoder = encoder.clone();
        workers.spawn(
            async move {
                // RX and TX progress independently, but share one queue's task and wakeup.
                // Other queues run in parallel; a pending writer does not stop this receiver.
                tokio::try_join!(
                    async {
                        worker::dispatch(id, read, inbox, state, mtu, context, output)
                            .await
                            .with_context(|| format!("TUN receive queue {id}"))
                    },
                    async {
                        transmit(id, write, packets, encoder)
                            .await
                            .with_context(|| format!("TUN transmit queue {id}"))
                    },
                )?;
                Ok(())
            }
            .in_current_span(),
        );
    }

    let result = async {
        // Holding all admission permits prevents a late SYN racing the drain.
        let _drained = tokio::select! {
            biased;
            _ = context.scope.cancelled() => return Ok(()),
            permits = async {
                context.stopping.cancelled().await;
                shared.tcp_slots.clone().acquire_many_owned(worker::MAX_TCP_CONNECTIONS as u32).await
            } => permits?,
            result = workers.join_next() => {
                result.ok_or_else(|| anyhow!("TUN has no workers"))??.context("TUN worker failed")?;
                bail!("TUN worker stopped before shutdown");
            },
        };
        shared.stop.cancel();
        loop {
            // Forced shutdown also interrupts blocked transmitters during graceful drain.
            let result = tokio::select! {
                biased;
                _ = context.scope.cancelled() => return Ok(()),
                result = workers.join_next() => result,
            };
            match result {
                Some(result) => result??,
                None => break,
            }
        }
        Ok(())
    }.await;

    shared.stop.cancel();
    context.scope.close();
    workers.abort_all();
    while workers.join_next().await.is_some() {}
    result
}

async fn transmit<W: PacketSend>(
    id: usize,
    mut device: W,
    mut input: mpsc::Receiver<Transmit>,
    encoder: Arc<Mutex<udp::Encoder>>,
) -> Result<()> {
    let mut items = Vec::with_capacity(64);
    let mut packets = Vec::with_capacity(64);
    let mut sent_packets = 0u64;
    let mut sent_bytes = 0u64;

    while input.recv_many(&mut items, 64).await != 0 {
        for item in items.drain(..) {
            match item {
                Transmit::Packet(packet) => packets.push(packet),
                Transmit::Datagram {
                    source,
                    destination,
                    payload,
                } => {
                    let datagram = encoder
                        .lock()
                        .map_err(|_| anyhow!("TUN UDP encoder lock poisoned"))?
                        .encode(source, destination, &payload);
                    if let Some(datagram) = datagram {
                        packets.extend(datagram);
                    }
                }
            }
        }
        device.send_batch(&mut packets).await?;
        sent_packets += packets.len() as u64;
        sent_bytes += packets.iter().map(|p| p.len() as u64).sum::<u64>();
        packets.clear();
        tokio::task::yield_now().await;
    }
    tracing::debug!(
        queue = id,
        sent_packets,
        sent_bytes,
        "TUN transmit worker stopped"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::Decoder;
    use tokio::time::Instant;

    struct Sink(mpsc::Sender<Vec<u8>>);
    impl PacketSend for Sink {
        fn poll_send(&mut self, _: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
            self.0.try_send(bytes.to_vec()).unwrap();
            Poll::Ready(Ok(bytes.len()))
        }
    }

    #[tokio::test]
    async fn udp_replies_share_packetization_and_preserve_complete_datagrams() {
        let source: std::net::SocketAddr = "198.51.100.1:443".parse().unwrap();
        let destination = "192.0.2.2:12345".parse().unwrap();
        let (output0, packets0) = mpsc::channel(1);
        let (output1, packets1) = mpsc::channel(1);
        let (sink, mut received) = mpsc::channel(16);
        let encoder = Arc::new(Mutex::new(udp::Encoder::new(1280)));
        let writer0 = transmit(0, Sink(sink.clone()), packets0, encoder.clone());
        let writer1 = transmit(1, Sink(sink), packets1, encoder);
        let producer = async {
            for (port, output) in [(443, output0), (8443, output1)] {
                let source = std::net::SocketAddr::new(source.ip(), port);
                output
                    .send(Transmit::Datagram {
                        source,
                        destination,
                        payload: vec![7; 2500].into(),
                    })
                    .await
                    .unwrap();
            }
        };
        let (result0, result1, _) = tokio::join!(writer0, writer1, producer);
        result0.unwrap();
        result1.unwrap();
        let mut ids = std::collections::HashSet::new();
        let mut decoder = Decoder::default();
        let mut complete = Vec::new();
        while let Ok(packet) = received.try_recv() {
            let header = smoltcp::wire::Ipv4Packet::new_checked(&packet).unwrap();
            if header.frag_offset() == 0 {
                assert!(ids.insert(header.ident()));
            }
            if let Some(packet) = decoder.decode(&packet, Instant::now()) {
                complete.push(packet.into_owned());
            }
        }
        assert_eq!(complete.len(), 2);
        for packet in complete {
            assert_eq!(packet.udp().unwrap().1, vec![7; 2500]);
        }
    }
}
