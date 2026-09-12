use crate::{
    packet::{Decoder, Flow},
    tcp, udp,
};
use anyhow::{Result, ensure};
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
    sync::{Semaphore, mpsc, watch},
    time::Instant,
};

const MAX_TCP_CONNECTIONS: usize = 256;
const MAX_UDP_ASSOCIATIONS: usize = 128;

/// Packet boundaries are preserved. A successful send consumes exactly one IP
/// packet. Implementations must register readiness with the supplied context.
pub trait PacketIo: Send + Sync {
    fn poll_recv(&self, cx: &mut Context<'_>, bytes: &mut [u8]) -> Poll<io::Result<usize>>;
    fn poll_send(&self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>>;
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
    let (output, mut packets) = mpsc::channel::<Vec<u8>>(128);
    let receive = dispatch(&device, mtu, context.clone(), output);
    let send = async {
        while let Some(packet) = packets.recv().await {
            let len = poll_fn(|cx| device.poll_send(cx, &packet)).await?;
            ensure!(len == packet.len(), "partial TUN packet write");
        }
        Ok(())
    };
    tokio::pin!(receive, send);
    tokio::select! {
        biased;
        _ = context.scope.cancelled() => Ok(()),
        result = &mut receive => { result?; send.await },
        result = &mut send => result,
    }
}

async fn dispatch<D: PacketIo>(
    device: &D,
    mtu: usize,
    context: ServerContext,
    output: mpsc::Sender<Vec<u8>>,
) -> Result<()> {
    let mut tcp = HashMap::<Flow, TcpEntry>::new();
    let mut udp = HashMap::<Flow, UdpEntry>::new();
    let mut decoder = Decoder::default();
    let mut rejector = tcp::Rejector::new(mtu);
    let mut buffer = vec![0; 65575];
    let (done, mut completed) = mpsc::unbounded_channel();
    let mut generation = 0u64;
    let mut stopping = false;
    let packet_budget = Arc::new(Semaphore::new(8 * 1024 * 1024));
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
            _ = context.stopping.cancelled(), if !stopping => { stopping = true; udp.clear(); }
            Some((protocol, flow, id)) = completed.recv() => {
                match protocol {
                    Protocol::Tcp if tcp.get(&flow).is_some_and(|e| e.generation == id) => { tcp.remove(&flow); }
                    Protocol::Udp if udp.get(&flow).is_some_and(|e| e.generation == id) => { udp.remove(&flow); }
                    _ => {},
                }
            }
            _ = async { match expiry { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => {
                decoder.expire(Instant::now());
            }
            len = poll_fn(|cx| device.poll_recv(cx, &mut buffer)) => {
                let len = len?;
                let Some(packet) = decoder.decode(&buffer[..len], Instant::now()) else { continue; };
                match packet.ip.next_header() {
                    IpProtocol::Tcp => {
                        let Some((flow, repr)) = packet.tcp() else { continue; };
                        if tcp.get(&flow).is_some_and(|e| e.packets.is_closed()) { tcp.remove(&flow); }
                        if !tcp.contains_key(&flow) {
                            if repr.control != TcpControl::Syn || repr.ack_number.is_some() || stopping {
                                for response in rejector.reject(&packet) { let _ = output.try_send(response); }
                                continue;
                            }
                            if tcp.len() >= MAX_TCP_CONNECTIONS { continue; }
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
                            let completion = Completion { tx: done.clone(), protocol: Protocol::Tcp, flow, generation };
                            driver_scope.spawn(async move {
                                let _completion = completion;
                                if conn.driver.await.is_err() { session.close(); }
                                Ok(())
                            })?;
                            tcp.insert(flow, TcpEntry { packets: conn.packets, generation });
                        }
                        if let Ok(permit) = packet_budget.clone().try_acquire_many_owned(packet.ip.buffer_len() as u32) {
                            let _ = tcp[&flow].packets.try_send(tcp::QueuedPacket { bytes: packet.encode(), _permit: Some(permit) });
                        }
                    }
                    IpProtocol::Udp if !stopping => {
                        let Some((flow, payload)) = packet.udp() else { continue; };
                        if udp.get(&flow).is_some_and(|e| e.scope.is_closed()) { udp.remove(&flow); }
                        if !udp.contains_key(&flow) {
                            if udp.len() >= MAX_UDP_ASSOCIATIONS { continue; }
                            generation = generation.checked_add(1).expect("TUN connection generation exhausted");
                            let scope = context.scope.child();
                            let (association, driver) = p::packet_pair(scope.clone());
                            let packets = driver.tx.clone();
                            let (activity, clock) = watch::channel(Instant::now());
                            let handler = context.handler.clone();
                            let output = output.clone();
                            let completion = Completion { tx: done.clone(), protocol: Protocol::Udp, flow, generation };
                            let stopping = context.stopping.clone();
                            let idle = context.udp_idle_timeout;
                            let reply_activity = activity.clone();
                            let control = scope.clone();
                            scope.spawn(async move {
                                let _completion = completion;
                                let replies = udp_replies(flow, driver, output, mtu, reply_activity);
                                tokio::select! {
                                    _ = stopping.cancelled() => {},
                                    _ = until_idle(clock, idle) => {},
                                    result = handler.udp(association) => result?,
                                    result = replies => result?,
                                }
                                control.close();
                                Ok(())
                            })?;
                            udp.insert(flow, UdpEntry { packets, activity, scope, generation });
                        }
                        let entry = &udp[&flow];
                        if entry.packets.try_send(p::Packet { target: p::target(flow.destination), payload: payload.to_vec() }).is_ok() {
                            entry.activity.send_replace(Instant::now());
                        }
                    }
                    _ => {},
                }
            }
        }
    }
}

async fn udp_replies(
    flow: Flow,
    mut driver: p::Datagram,
    output: mpsc::Sender<Vec<u8>>,
    mtu: usize,
    activity: watch::Sender<Instant>,
) -> Result<()> {
    let mut encoder = udp::Encoder::new(mtu);
    while let Some(packet) = driver.rx.recv().await {
        let Ok(source) = p::socket_addr(&packet.target) else {
            continue;
        };
        let Some((source, destination)) = udp::reply_flow(flow, source) else {
            continue;
        };
        let Some(packets) = encoder.encode(source, destination, &packet.payload) else {
            continue;
        };
        // This wait belongs to one association, not shared ingress. Reserve a
        // whole datagram so a busy TCP flow cannot drop only its later fragments.
        let permits = output.reserve_many(packets.len()).await?;
        for (permit, packet) in permits.zip(packets) {
            permit.send(packet);
        }
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

    #[tokio::test(start_paused = true)]
    async fn fragmented_udp_reply_waits_for_capacity_without_losing_fragments() {
        let flow = Flow {
            source: "[fd00::2]:12345".parse().unwrap(),
            destination: "[2001:db8::1]:443".parse().unwrap(),
        };
        let (output, mut packets) = mpsc::channel(3);
        output.send(vec![99]).await.unwrap();
        let scope = Scope::new();
        let (association, driver) = p::packet_pair(scope);
        let (activity, clock) = watch::channel(Instant::now());
        association
            .tx
            .send(p::Packet {
                target: p::target(flow.destination),
                payload: vec![7; 2500],
            })
            .await
            .unwrap();
        let replies = udp_replies(flow, driver, output, 1280, activity);
        tokio::pin!(replies);
        tokio::select! {
            biased;
            result = &mut replies => panic!("reply worker ended: {result:?}"),
            _ = std::future::ready(()) => {},
        }
        assert!(!clock.has_changed().unwrap());
        assert_eq!(packets.try_recv().unwrap(), vec![99]);
        tokio::select! {
            biased;
            result = &mut replies => panic!("reply worker ended: {result:?}"),
            _ = std::future::ready(()) => {},
        }
        let mut decoder = Decoder::default();
        let mut completed = None;
        for _ in 0..3 {
            completed = decoder
                .decode(&packets.try_recv().unwrap(), Instant::now())
                .or(completed);
        }
        assert_eq!(completed.unwrap().udp().unwrap().1, vec![7; 2500]);
        assert!(clock.has_changed().unwrap());
    }
}
