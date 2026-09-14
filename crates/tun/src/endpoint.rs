use crate::{
    transmit::Transmit,
    udp,
    worker::{self, Shared},
};
use anyhow::{Context as _, Result, anyhow, bail, ensure};
use bytes::Bytes;
use kotoconn_protocol::{ServerContext, queue};
use std::{
    collections::hash_map::RandomState,
    future::poll_fn,
    io,
    sync::Arc,
    task::{Context, Poll, ready},
};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

/// A queue has one receive owner. Implementations preserve packet boundaries,
/// register the supplied waker on Pending, and return at most bytes.len() bytes.
pub trait PacketReceive: Send {
    fn poll_recv(&mut self, cx: &mut Context<'_>, bytes: &mut [u8]) -> Poll<io::Result<usize>>;

    /// Transfer a frame's backing storage. The default supports ordinary packet
    /// readers; device implementations can preserve offload metadata as well.
    fn poll_frame(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut ReceiveBuffer,
    ) -> Poll<io::Result<Received>> {
        let lease = buffer
            .lease
            .get_or_insert_with(|| buffer.pool.acquire(65575));
        let len = ready!(self.poll_recv(cx, lease.as_mut()))?;
        Poll::Ready(Ok(Received {
            bytes: buffer.publish(len),
            checksum_verified: false,
            udp_segment_size: None,
        }))
    }
}

#[derive(Default)]
pub struct ReceiveBuffer {
    pub(crate) pool: crate::pool::Pool,
    pub(crate) lease: Option<kotoconn_protocol::pool::Lease>,
}

impl ReceiveBuffer {
    pub(crate) fn publish(&mut self, len: usize) -> Bytes {
        // SAFETY: The completed receive initialized this prefix of the installed
        // lease. Keep private scratch for short frames; only large frames take it.
        let lease = self.lease.as_ref().expect("receive lease");
        debug_assert!(len <= lease.as_ref().len());
        if len < lease.as_ref().len() / 4 {
            self.pool.copy(&lease.as_ref()[..len])
        } else {
            self.lease.take().expect("receive lease").freeze(len)
        }
    }
}

/// Offload assertions are accepted only from the device implementation. Ordinary
/// packet readers leave both metadata fields unset and receive full validation.
pub struct Received {
    pub bytes: Bytes,
    pub checksum_verified: bool,
    /// UDP L4 segmentation, never IP fragmentation. Each slice is one datagram.
    pub udp_segment_size: Option<u16>,
}

/// A queue has one transmit owner, independent of its receive owner.
pub trait PacketSend: Send {
    fn poll_send(&mut self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>>;

    /// Whether this writer accepts TCP aggregates with partial checksums.
    fn tcp_gso(&self) -> bool {
        false
    }

    fn udp_gso(&self) -> bool {
        false
    }

    /// Write complete equal-size UDP datagrams as one frame. A single datagram
    /// uses checksum offload only. Oversized datagrams still require fragmentation.
    fn send_udp_segments(
        &mut self,
        _header: &[u8],
        _payload: &[Bytes],
        _segment_size: u16,
    ) -> impl Future<Output = io::Result<()>> + Send {
        async { Err(io::ErrorKind::Unsupported.into()) }
    }

    /// Write one TCP aggregate. Only called when tcp_gso returned true.
    fn send_tcp_gso(
        &mut self,
        _packet: &[u8],
        _segment_size: u16,
    ) -> impl Future<Output = io::Result<()>> + Send {
        async { Err(io::ErrorKind::Unsupported.into()) }
    }

    /// Header and payload vectors belong to a single GSO frame.
    fn send_tcp_segments(
        &mut self,
        header: &[u8],
        payload: &[Bytes],
        segment_size: u16,
    ) -> impl Future<Output = io::Result<()>> + Send {
        async move {
            let mut packet = header.to_vec();
            for part in payload {
                packet.extend_from_slice(part);
            }
            self.send_tcp_gso(&packet, segment_size).await
        }
    }

    /// Send only packets already available. Cancellation may transmit a prefix;
    /// the caller must not retry a cancelled batch.
    fn send_batch(&mut self, packets: &[Bytes]) -> impl Future<Output = io::Result<()>> + Send {
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
    let (inboxes, receivers): (Vec<_>, Vec<_>) = (0..queues.len())
        .map(|_| queue::channel(queue::INITIAL_BYTES, worker::Forwarded::size))
        .unzip();
    let (drained, mut drains) = mpsc::unbounded_channel();
    let shared = Arc::new(Shared {
        drained,
        hash: RandomState::new(),
        inboxes,
        reassembly: Default::default(),
        stop: CancellationToken::new(),
    });
    // Each writer owns its packet storage; only atomic fragment IDs are shared.
    let encoder = udp::Encoder::new(mtu);
    let mut workers: JoinSet<Result<()>> = JoinSet::new();
    let count = queues.len();
    for (id, ((read, write), inbox)) in queues.into_iter().zip(receivers).enumerate() {
        let (output, packets) = queue::channel(queue::INITIAL_BYTES, Transmit::size);
        let gso = write.tcp_gso();
        let state = shared.clone();
        let context = context.clone();
        let encoder = encoder.clone();
        workers.spawn(
            async move {
                // RX and TX progress independently, but share one queue's task and wakeup.
                // Other queues run in parallel; a pending writer does not stop this receiver.
                tokio::try_join!(
                    async {
                        worker::dispatch(
                            id,
                            read,
                            inbox,
                            state,
                            crate::tcp::Link { mtu, gso },
                            context,
                            output,
                        )
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
        // A worker reports only after observing stop and removing every TCP
        // connection. No finite connection permit set is needed for graceful drain.
        let mut remaining = count;
        while remaining > 0 {
            tokio::select! {
                biased;
                _ = context.scope.cancelled() => return Ok(()),
                Some(_) = drains.recv() => remaining -= 1,
                result = workers.join_next() => {
                    result.ok_or_else(|| anyhow!("TUN has no workers"))??.context("TUN worker failed")?;
                    bail!("TUN worker stopped before shutdown");
                },
            }
        }
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
    mut input: queue::Receiver<Transmit>,
    mut encoder: udp::Encoder,
) -> Result<()> {
    let mut items = Vec::with_capacity(64);
    let mut packets = Vec::with_capacity(64);
    let mut parts = Vec::with_capacity(64);
    let mut sent_packets = 0u64;
    let mut sent_bytes = 0u64;

    while input.recv_many(&mut items, 64).await != 0 {
        let mut pending = items.drain(..).peekable();
        while let Some(item) = pending.next() {
            match item {
                Transmit::Packet(packet) => packets.push(packet),
                Transmit::TcpGso {
                    packet,
                    payload,
                    segment_size,
                } => {
                    if !packets.is_empty() {
                        device.send_batch(&packets).await?;
                        sent_packets += packets.len() as u64;
                        sent_bytes += packets.iter().map(|p| p.len() as u64).sum::<u64>();
                        packets.clear();
                    }
                    device
                        .send_tcp_segments(&packet, &payload, segment_size)
                        .await?;
                    sent_packets += 1;
                    sent_bytes +=
                        packet.len() as u64 + payload.iter().map(|p| p.len() as u64).sum::<u64>();
                }
                Transmit::Datagram {
                    source,
                    destination,
                    payload,
                } => {
                    let size = payload.len();
                    if device.udp_gso()
                        && size != 0
                        && encoder.can_offload(source, destination, size)
                    {
                        debug_assert!(parts.is_empty());
                        parts.push(payload);
                        while parts.len() < 64 && (parts.len() + 1) * size <= 65507 {
                            if !matches!(pending.peek(), Some(Transmit::Datagram { source: s, destination: d, payload: p })
                                if *s == source && *d == destination && p.len() == size)
                            {
                                break;
                            }
                            // SAFETY: peek established this item's variant; no other
                            // consumer can modify the local batch iterator.
                            let Some(Transmit::Datagram { payload, .. }) = pending.next() else {
                                unreachable!()
                            };
                            parts.push(payload);
                        }
                        // SAFETY: The first size was validated and aggregation stops
                        // before exceeding the IPv4 limit, also valid for IPv6.
                        let header = encoder
                            .header(source, destination, size, size * parts.len())
                            .expect("validated UDP aggregate");
                        if !packets.is_empty() {
                            device.send_batch(&packets).await?;
                            sent_packets += packets.len() as u64;
                            sent_bytes += packets.iter().map(|p| p.len() as u64).sum::<u64>();
                            packets.clear();
                        }
                        device
                            .send_udp_segments(&header, &parts, size as u16)
                            .await?;
                        sent_packets += parts.len() as u64;
                        sent_bytes += (header.len() * parts.len() + size * parts.len()) as u64;
                        parts.clear();
                        continue;
                    }
                    let datagram = encoder.encode(source, destination, &payload);
                    if let Some(datagram) = datagram {
                        packets.extend(datagram);
                    }
                }
            }
        }
        device.send_batch(&packets).await?;
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
        let (output0, packets0) = queue::channel(1, Transmit::size);
        let (output1, packets1) = queue::channel(1, Transmit::size);
        let (sink, mut received) = mpsc::channel(16);
        let encoder = udp::Encoder::new(1280);
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
