use crate::{
    device::Transmit,
    packet::{Decoder, Flow},
    tcp, udp,
};
use anyhow::{Result, ensure};
use bytes::Bytes;
use kotoconn_protocol::{self as p, Scope, ServerContext};
use smoltcp::wire::{IpProtocol, TcpControl};
use std::{
    collections::HashMap,
    future::poll_fn,
    io,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch},
    time::Instant,
};

const MAX_TCP_CONNECTIONS: usize = 256;

const MAX_UDP_ASSOCIATIONS: usize = 128;

const INGRESS_BYTES: usize = 8 * 1024 * 1024;

/// Packet boundaries are preserved. A successful send consumes exactly one IP
/// packet. Implementations must register readiness with the supplied context.
/// A successful receive returns the number of bytes written into `bytes`,
/// which must not exceed its length.
pub trait PacketIo: Send + Sync {
    fn poll_recv(&self, cx: &mut Context<'_>, bytes: &mut [u8]) -> Poll<io::Result<usize>>;
    fn poll_send(&self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>>;

    /// Send an available batch without waiting to collect more packets. A
    /// cancelled batch may have transmitted a prefix and must not be retried.
    fn send_batch(&self, packets: &mut [Vec<u8>]) -> impl Future<Output = io::Result<()>> + Send {
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

#[cfg(target_os = "linux")]
impl PacketIo for tun_rs::AsyncDevice {
    fn poll_recv(&self, cx: &mut Context<'_>, bytes: &mut [u8]) -> Poll<io::Result<usize>> {
        self.poll_recv(cx, bytes)
    }

    fn poll_send(&self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        self.poll_send(cx, bytes)
    }
}

struct TcpEntry {
    packets: mpsc::Sender<tcp::QueuedPacket>,
    generation: u64,
}

struct UdpEntry {
    packets: mpsc::Sender<p::Packet>,
    activity: watch::Sender<Instant>,
    scope: Scope,
    generation: u64,
}

impl Drop for UdpEntry {
    fn drop(&mut self) {
        self.scope.close();
    }
}

#[derive(Clone, Copy)]
enum Protocol {
    Tcp,
    Udp,
}

struct Completion {
    tx: mpsc::UnboundedSender<(Protocol, Flow, u64)>,
    protocol: Protocol,
    flow: Flow,
    generation: u64,
}

impl Drop for Completion {
    fn drop(&mut self) {
        let _ = self.tx.send((self.protocol, self.flow, self.generation));
    }
}

/// Run until admission stops and accepted TCP connections drain. The caller's
/// scope owns every connection task and interrupts device I/O on forced shutdown.
pub async fn run<D: PacketIo>(device: D, mtu: usize, context: ServerContext) -> Result<()> {
    ensure!(
        (1280..=65535).contains(&mtu),
        "TUN MTU must be between 1280 and 65535"
    );

    let (output, packets) = mpsc::channel::<Transmit>(128);
    let receive = dispatch(&device, mtu, context.clone(), output);
    let send = transmit(&device, mtu, packets);

    tokio::pin!(receive, send);
    tokio::select! {
        biased;
        _ = context.scope.cancelled() => Ok(()),
        result = &mut receive => {
            result?;
            send.await
        },
        result = &mut send => result,
    }
}

async fn transmit<D: PacketIo>(
    device: &D,
    mtu: usize,
    mut input: mpsc::Receiver<Transmit>,
) -> Result<()> {
    let mut encoder = udp::Encoder::new(mtu);
    let mut items = Vec::with_capacity(64);
    let mut packets = Vec::with_capacity(64);

    while input.recv_many(&mut items, 64).await != 0 {
        for item in items.drain(..) {
            match item {
                Transmit::Packet(packet) => packets.push(packet),
                Transmit::Datagram {
                    source,
                    destination,
                    payload,
                } => {
                    if let Some(datagram) = encoder.encode(source, destination, &payload) {
                        packets.extend(datagram);
                    }
                }
            }
        }
        device.send_batch(&mut packets).await?;
        packets.clear();
        tokio::task::yield_now().await;
    }
    Ok(())
}

async fn dispatch<D: PacketIo>(
    device: &D,
    mtu: usize,
    context: ServerContext,
    output: mpsc::Sender<Transmit>,
) -> Result<()> {
    let mut tcp = HashMap::<Flow, TcpEntry>::new();
    let mut udp = HashMap::<Flow, UdpEntry>::new();

    let mut decoder = Decoder::default();
    let mut rejector = tcp::Rejector::new(mtu);
    let mut buffer = vec![0; 65575];

    let (done, mut completed) = mpsc::unbounded_channel();
    let mut generation = 0u64;
    let mut stopping = false;

    let tcp_budget = Arc::new(Semaphore::new(INGRESS_BYTES));
    let udp_budget = Arc::new(Semaphore::new(INGRESS_BYTES));
    let mut turns = 0;

    loop {
        turns += 1;
        if turns == 64 {
            tokio::task::yield_now().await;
            turns = 0;
        }
        if stopping && tcp.is_empty() {
            return Ok(());
        }
        let expiry = decoder.deadline();

        tokio::select! {
            biased;
            _ = context.stopping.cancelled(), if !stopping => {
                stopping = true;
                udp.clear();
            }
            Some((protocol, flow, id)) = completed.recv() => {
                match protocol {
                    Protocol::Tcp if tcp.get(&flow).is_some_and(|e| e.generation == id) => {
                        tcp.remove(&flow);
                    }
                    Protocol::Udp if udp.get(&flow).is_some_and(|e| e.generation == id) => {
                        udp.remove(&flow);
                    }
                    _ => {}
                }
            }
            _ = async { match expiry { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => {
                decoder.expire(Instant::now());
            }
            len = poll_fn(|cx| device.poll_recv(cx, &mut buffer)) => {
                let len = len?;
                // SAFETY: PacketIo reports bytes written into the supplied
                // buffer. Invalid packet contents still go through Decoder.
                debug_assert!(len <= buffer.len(), "PacketIo returned an invalid receive length");
                let Some(packet) = decoder.decode(&buffer[..len], Instant::now()) else { continue; };

                match packet.ip.next_header() {
                    IpProtocol::Tcp => {
                        let Some((flow, repr)) = packet.tcp() else { continue; };

                        if tcp.get(&flow).is_some_and(|e| e.packets.is_closed()) {
                            tcp.remove(&flow);
                        }

                        if !tcp.contains_key(&flow) {
                            if repr.control != TcpControl::Syn || repr.ack_number.is_some() || stopping {
                                for response in rejector.reject(&packet) {
                                    let _ = output.try_send(Transmit::Packet(response));
                                }
                                continue;
                            }

                            if tcp.len() >= MAX_TCP_CONNECTIONS {
                                continue;
                            }

                            // SAFETY: IDs must never repeat while old completions may
                            // still be queued. Exhaustion must fail even in release.
                            debug_assert_ne!(generation, u64::MAX, "TUN connection generation exhausted");
                            generation = generation.checked_add(1).expect("TUN connection generation exhausted");
                            let conn = tcp::connection(flow, mtu, output.clone(), context.stopping.clone());
                            let session = context.scope.child();
                            let handler = context.handler.clone();
                            let admission_scope = session.clone();

                            session.spawn(async move {
                                let stream = conn.accepted.await?;
                                handler.tcp(p::target(flow.destination), Box::pin(stream), admission_scope).await
                            })?;
                            // Track FIN/RST cleanup in the session, but only forced listener
                            // cancellation may interrupt the driver while it sends that cleanup.
                            let driver_scope = context.scope.child().tracked_by(&session);
                            let completion = Completion {
                                tx: done.clone(),
                                protocol: Protocol::Tcp,
                                flow,
                                generation,
                            };

                            driver_scope.spawn(async move {
                                let _completion = completion;
                                if conn.driver.await.is_err() {
                                    session.close();
                                }
                                Ok(())
                            })?;

                            tcp.insert(
                                flow,
                                TcpEntry {
                                    packets: conn.packets,
                                    generation,
                                },
                            );
                        }

                        if let Ok(permit) = tcp_budget
                            .clone()
                            .try_acquire_many_owned(packet.ip.buffer_len() as u32)
                        {
                            // SAFETY: The flow was found or inserted above. Only
                            // dispatch mutates this map; tasks only send completions.
                            debug_assert!(tcp.contains_key(&flow));
                            let _ = tcp[&flow].packets.try_send(tcp::QueuedPacket {
                                bytes: packet.encode(),
                                _permit: Some(permit),
                            });
                        }
                    }
                    IpProtocol::Udp if !stopping => {
                        let Some((flow, payload)) = packet.udp() else { continue; };
                        let Some(payload) = budgeted_payload(payload, &udp_budget) else { continue; };

                        if udp.get(&flow).is_some_and(|e| e.scope.is_closed()) {
                            udp.remove(&flow);
                        }

                        if !udp.contains_key(&flow) {
                            if udp.len() >= MAX_UDP_ASSOCIATIONS {
                                continue;
                            }

                            // SAFETY: IDs must never repeat while old completions may
                            // still be queued. Exhaustion must fail even in release.
                            debug_assert_ne!(generation, u64::MAX, "TUN connection generation exhausted");
                            generation = generation.checked_add(1).expect("TUN connection generation exhausted");
                            let scope = context.scope.child();
                            let (association, driver) = p::packet_pair(scope.clone());
                            let packets = driver.tx.clone();
                            let (activity, clock) = watch::channel(Instant::now());
                            let handler = context.handler.clone();
                            let output = output.clone();
                            let completion = Completion {
                                tx: done.clone(),
                                protocol: Protocol::Udp,
                                flow,
                                generation,
                            };
                            let stopping = context.stopping.clone();
                            let idle = context.udp_idle_timeout;
                            let reply_activity = activity.clone();
                            let control = scope.clone();

                            scope.spawn(async move {
                                let _completion = completion;
                                let replies = udp_replies(flow, driver, output, reply_activity);
                                tokio::select! {
                                    _ = stopping.cancelled() => {},
                                    _ = until_idle(clock, idle) => {},
                                    result = handler.udp(association) => result?,
                                    result = replies => result?,
                                }
                                control.close();
                                Ok(())
                            })?;

                            udp.insert(
                                flow,
                                UdpEntry {
                                    packets,
                                    activity,
                                    scope,
                                    generation,
                                },
                            );
                        }

                        // SAFETY: The flow was found or inserted above. Only
                        // dispatch mutates this map; tasks only send completions.
                        debug_assert!(udp.contains_key(&flow));
                        let entry = &udp[&flow];
                        if entry
                            .packets
                            .try_send(p::Packet {
                                target: p::target(flow.destination),
                                payload,
                            })
                            .is_ok()
                        {
                            entry.activity.send_replace(Instant::now());
                        }
                    }
                    _ => {},
                }
            }
        }
    }
}

// The permit follows the payload through policy and outbound queues, including
// clones. Returning queue capacity alone would release ingress credit too soon.
struct BudgetedPayload {
    bytes: Vec<u8>,
    _permit: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for BudgetedPayload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn budgeted_payload(payload: &[u8], budget: &Arc<Semaphore>) -> Option<Bytes> {
    let permit = budget
        .clone()
        .try_acquire_many_owned(payload.len().max(1) as u32)
        .ok()?;
    Some(Bytes::from_owner(BudgetedPayload {
        bytes: payload.to_vec(),
        _permit: permit,
    }))
}

async fn udp_replies(
    flow: Flow,
    mut driver: p::Datagram,
    output: mpsc::Sender<Transmit>,
    activity: watch::Sender<Instant>,
) -> Result<()> {
    while let Some(packet) = driver.rx.recv().await {
        let Ok(source) = p::socket_addr(&packet.target) else {
            continue;
        };
        let Some((source, destination)) = udp::reply_flow(flow, source) else {
            continue;
        };
        // One queued item owns the whole datagram. Only this association waits
        // for capacity; shared ingress keeps receiving other connections.
        output
            .send(Transmit::Datagram {
                source,
                destination,
                payload: packet.payload,
            })
            .await?;
        activity.send_replace(Instant::now());
    }
    Ok(())
}

async fn until_idle(mut activity: watch::Receiver<Instant>, idle: std::time::Duration) {
    loop {
        let deadline = *activity.borrow_and_update() + idle;
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => return,
            result = activity.changed() => { if result.is_err() { return; } },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Sink(mpsc::Sender<Vec<u8>>);

    impl PacketIo for Sink {
        fn poll_recv(&self, _: &mut Context<'_>, _: &mut [u8]) -> Poll<io::Result<usize>> {
            unreachable!()
        }
        fn poll_send(&self, _: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
            self.0.try_send(bytes.to_vec()).unwrap();
            Poll::Ready(Ok(bytes.len()))
        }
    }

    #[tokio::test]
    async fn udp_ingress_credit_follows_packets_across_queues_and_clones() {
        let budget = Arc::new(Semaphore::new(8192));
        let payload = budgeted_payload(&[7; 8192], &budget).unwrap();
        let (sender, mut receiver) = mpsc::channel(1);
        sender
            .send(p::Packet {
                target: p::target("192.0.2.1:53".parse().unwrap()),
                payload,
            })
            .await
            .unwrap();
        assert!(budgeted_payload(&[], &budget).is_none());
        let packet = receiver.recv().await.unwrap();
        let retained = packet.clone();
        drop(packet);
        assert!(budgeted_payload(&[1], &budget).is_none());
        drop(retained);
        assert_eq!(budget.available_permits(), 8192);
        let empty = budgeted_payload(&[], &budget).unwrap();
        assert_eq!(budget.available_permits(), 8191);
        drop(empty);
        assert_eq!(budget.available_permits(), 8192);
    }

    #[tokio::test]
    async fn udp_replies_share_packetization_and_preserve_complete_datagrams() {
        let source: std::net::SocketAddr = "198.51.100.1:443".parse().unwrap();
        let destination = "192.0.2.2:12345".parse().unwrap();
        let (output, packets) = mpsc::channel(1);
        let (sink, mut received) = mpsc::channel(16);
        let sink = Sink(sink);
        let writer = transmit(&sink, 1280, packets);
        let producer = async {
            for port in [443, 8443] {
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
            drop(output);
        };
        let (result, _) = tokio::join!(writer, producer);
        result.unwrap();
        let mut ids = std::collections::HashSet::new();
        let mut decoder = Decoder::default();
        let mut complete = Vec::new();
        while let Ok(packet) = received.try_recv() {
            let header = smoltcp::wire::Ipv4Packet::new_checked(&packet).unwrap();
            if header.frag_offset() == 0 {
                assert!(ids.insert(header.ident()));
            }
            if let Some(packet) = decoder.decode(&packet, Instant::now()) {
                complete.push(packet);
            }
        }
        assert_eq!(complete.len(), 2);
        for packet in complete {
            assert_eq!(packet.udp().unwrap().1, vec![7; 2500]);
        }
    }
}
