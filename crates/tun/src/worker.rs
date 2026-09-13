use crate::{
    PacketReceive,
    device::Transmit,
    packet::{self, Decoder, Flow, Packet, ReassemblyLimits, RouteKey},
    tcp, udp,
};
use anyhow::Result;
use bytes::Bytes;
use kotoconn_protocol::{self as p, Scope, ServerContext, queue};
use smoltcp::wire::{IpProtocol, TcpControl};
use std::{
    collections::{HashMap, hash_map::RandomState},
    future::poll_fn,
    hash::BuildHasher,
    sync::Arc,
};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

pub(crate) struct Shared {
    pub drained: mpsc::UnboundedSender<usize>,
    pub hash: RandomState,
    pub inboxes: Vec<queue::Sender<Forwarded>>,
    pub reassembly: ReassemblyLimits,
    pub stop: CancellationToken,
}

impl Shared {
    pub fn owner(&self, key: RouteKey) -> usize {
        // SAFETY: run rejects an empty queue set and never changes worker membership.
        debug_assert!(!self.inboxes.is_empty());
        (self.hash.hash_one(key) as usize) % self.inboxes.len()
    }
}

pub(crate) struct Forwarded {
    packet: ForwardedPacket,
}

impl Forwarded {
    pub(crate) fn size(&self) -> usize {
        match &self.packet {
            ForwardedPacket::Frame(bytes) => bytes.len(),
            ForwardedPacket::Reassembled(packet) => packet.storage_size(),
        }
    }
}

enum ForwardedPacket {
    Frame(Vec<u8>),
    Reassembled(Packet<'static>),
}

struct Statistics {
    queue: usize,
    received_packets: u64,
    received_bytes: u64,
    forwarded_packets: u64,
    processed_packets: u64,
    capacity_drops: u64,
    forwarding_drops: u64,
}

impl Drop for Statistics {
    fn drop(&mut self) {
        tracing::debug!(
            queue = self.queue,
            received_packets = self.received_packets,
            received_bytes = self.received_bytes,
            forwarded_packets = self.forwarded_packets,
            processed_packets = self.processed_packets,
            capacity_drops = self.capacity_drops,
            forwarding_drops = self.forwarding_drops,
            "TUN receive worker stopped"
        );
    }
}

struct TcpEntry {
    packets: queue::Sender<tcp::QueuedPacket>,
    generation: u64,
}

struct UdpEntry {
    packets: queue::Sender<p::Packet>,
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

struct Worker {
    id: usize,
    mtu: usize,
    context: ServerContext,
    shared: Arc<Shared>,
    tcp: HashMap<Flow, TcpEntry>,
    udp: HashMap<Flow, UdpEntry>,
    done: mpsc::UnboundedSender<(Protocol, Flow, u64)>,
    output: queue::Sender<Transmit>,
    rejector: tcp::Rejector,
    generation: u64,
    stopping: bool,
    stats: Statistics,
}

impl Worker {
    fn forward_raw(&mut self, bytes: &[u8], owner: usize) {
        // SAFETY: owner comes from Shared::owner, and receive buffers hold at most 65575 bytes.
        debug_assert!(owner < self.shared.inboxes.len());
        debug_assert!(bytes.len() <= 65575);
        let Ok(entry) = self.shared.inboxes[owner].try_reserve(bytes.len()) else {
            self.stats.forwarding_drops += 1;
            return;
        };
        let bytes = bytes.to_vec();
        entry.send(Forwarded {
            packet: ForwardedPacket::Frame(bytes),
        });
        self.stats.forwarded_packets += 1;
    }

    fn forward_packet(&mut self, packet: Packet<'_>, owner: usize) {
        // SAFETY: owner comes from Shared::owner; Decoder bounds normalized IP lengths.
        debug_assert!(owner < self.shared.inboxes.len());
        debug_assert!(packet.ip.buffer_len() <= 65575);
        let Ok(entry) = self.shared.inboxes[owner].try_reserve(packet.storage_size()) else {
            self.stats.forwarding_drops += 1;
            return;
        };
        let packet = packet.into_owned();
        entry.send(Forwarded {
            packet: ForwardedPacket::Reassembled(packet),
        });
        self.stats.forwarded_packets += 1;
    }

    fn packet(&mut self, packet: Packet<'_>) -> Result<()> {
        match packet.ip.next_header() {
            IpProtocol::Tcp => {
                let Some((flow, repr)) = packet.tcp() else {
                    return Ok(());
                };
                let owner = self
                    .shared
                    .owner(RouteKey::Flow(u8::from(IpProtocol::Tcp), flow));
                if owner != self.id {
                    self.forward_packet(packet, owner);
                    return Ok(());
                }

                if self.tcp.get(&flow).is_some_and(|e| e.packets.is_closed()) {
                    self.tcp.remove(&flow);
                }

                if !self.tcp.contains_key(&flow) {
                    if repr.control != TcpControl::Syn || repr.ack_number.is_some() || self.stopping
                    {
                        let _ = self.rejector.reject(&packet, &self.output);
                        return Ok(());
                    }

                    // SAFETY: IDs must never repeat while old completions may
                    // still be queued. Exhaustion must fail even in release.
                    debug_assert_ne!(
                        self.generation,
                        u64::MAX,
                        "TUN connection generation exhausted"
                    );
                    self.generation = self
                        .generation
                        .checked_add(1)
                        .expect("TUN connection generation exhausted");
                    let conn = tcp::connection(
                        flow,
                        self.mtu,
                        self.output.clone(),
                        self.context.stopping.clone(),
                    );
                    let session = self.context.scope.child();
                    let handler = self.context.handler.clone();
                    let admission_scope = session.clone();

                    session.spawn(async move {
                        let stream = conn.accepted.await?;
                        handler
                            .tcp(
                                p::target(flow.destination),
                                Box::pin(stream),
                                admission_scope,
                            )
                            .await
                    })?;
                    // Track FIN/RST cleanup in the session, but only forced listener
                    // cancellation may interrupt the driver while it sends that cleanup.
                    let driver_scope = self.context.scope.child().tracked_by(&session);
                    let completion = Completion {
                        tx: self.done.clone(),
                        protocol: Protocol::Tcp,
                        flow,
                        generation: self.generation,
                    };

                    driver_scope.spawn(async move {
                        let _completion = completion;
                        if conn.driver.await.is_err() {
                            session.close();
                        }
                        Ok(())
                    })?;

                    self.tcp.insert(
                        flow,
                        TcpEntry {
                            packets: conn.packets,
                            generation: self.generation,
                        },
                    );
                }

                // SAFETY: Only dispatch mutates this map; the flow was admitted above.
                debug_assert!(self.tcp.contains_key(&flow));
                let entry = &self.tcp[&flow];
                match entry.packets.try_reserve(packet.ip.buffer_len()) {
                    Ok(permit) => permit.send(tcp::QueuedPacket {
                        bytes: packet.encode(),
                    }),
                    Err(_) => self.stats.capacity_drops += 1,
                }
            }
            IpProtocol::Udp if !self.stopping => {
                let Some((flow, payload)) = packet.udp() else {
                    return Ok(());
                };
                let owner = self
                    .shared
                    .owner(RouteKey::Flow(u8::from(IpProtocol::Udp), flow));
                if owner != self.id {
                    self.forward_packet(packet, owner);
                    return Ok(());
                }

                if self.udp.get(&flow).is_some_and(|e| e.scope.is_closed()) {
                    self.udp.remove(&flow);
                }

                if !self.udp.contains_key(&flow) {
                    // SAFETY: IDs must never repeat while old completions may
                    // still be queued. Exhaustion must fail even in release.
                    debug_assert_ne!(
                        self.generation,
                        u64::MAX,
                        "TUN connection generation exhausted"
                    );
                    self.generation = self
                        .generation
                        .checked_add(1)
                        .expect("TUN connection generation exhausted");
                    let scope = self.context.scope.child();
                    let (association, driver) = p::packet_pair(scope.clone());
                    let packets = driver.tx.clone();
                    let (activity, clock) = watch::channel(Instant::now());
                    let handler = self.context.handler.clone();
                    let output = self.output.clone();
                    let completion = Completion {
                        tx: self.done.clone(),
                        protocol: Protocol::Udp,
                        flow,
                        generation: self.generation,
                    };
                    let stopping = self.context.stopping.clone();
                    let idle = self.context.udp_idle_timeout;
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

                    self.udp.insert(
                        flow,
                        UdpEntry {
                            packets,
                            activity,
                            scope,
                            generation: self.generation,
                        },
                    );
                }

                // SAFETY: The flow was found or inserted above. Only
                // dispatch mutates this map; tasks only send completions.
                debug_assert!(self.udp.contains_key(&flow));
                let entry = &self.udp[&flow];
                let Ok(permit) = entry.packets.try_reserve(payload.len()) else {
                    self.stats.capacity_drops += 1;
                    return Ok(());
                };
                let payload = Bytes::copy_from_slice(payload);
                permit.send(p::Packet {
                    target: p::target(flow.destination),
                    payload,
                });
                entry.activity.send_replace(Instant::now());
            }
            _ => {}
        }
        self.stats.processed_packets += 1;
        Ok(())
    }
}

enum Input {
    Forwarded(Forwarded),
    Read(usize),
}

pub(crate) async fn dispatch<R: PacketReceive>(
    id: usize,
    mut device: R,
    mut inbox: queue::Receiver<Forwarded>,
    shared: Arc<Shared>,
    mtu: usize,
    context: ServerContext,
    output: queue::Sender<Transmit>,
) -> Result<()> {
    let (done, mut completed) = mpsc::unbounded_channel();
    let mut worker = Worker {
        id,
        mtu,
        context,
        shared: shared.clone(),
        tcp: HashMap::new(),
        udp: HashMap::new(),
        done,
        output,
        rejector: tcp::Rejector::new(mtu),
        generation: 0,
        stopping: false,
        stats: Statistics {
            queue: id,
            received_packets: 0,
            received_bytes: 0,
            forwarded_packets: 0,
            processed_packets: 0,
            capacity_drops: 0,
            forwarding_drops: 0,
        },
    };
    let mut decoder = Decoder::new(shared.reassembly.clone());
    let mut buffer = vec![0; 65575];
    let mut turns = 0;
    let mut reported_drained = false;

    loop {
        if worker.stopping && worker.tcp.is_empty() && !reported_drained {
            let _ = shared.drained.send(id);
            reported_drained = true;
        }
        turns += 1;
        if turns == 64 {
            tokio::task::yield_now().await;
            turns = 0;
        }
        let expiry = decoder.deadline();
        let event = tokio::select! {
            biased;
            _ = shared.stop.cancelled() => return Ok(()),
            _ = worker.context.stopping.cancelled(), if !worker.stopping => {
                worker.stopping = true;
                worker.udp.clear();
                continue;
            },
            Some((protocol, flow, generation)) = completed.recv() => {
                match protocol {
                    Protocol::Tcp if worker.tcp.get(&flow).is_some_and(|e| e.generation == generation) => {
                        worker.tcp.remove(&flow);
                    },
                    Protocol::Udp if worker.udp.get(&flow).is_some_and(|e| e.generation == generation) => {
                        worker.udp.remove(&flow);
                    },
                    _ => {},
                }
                continue;
            },
            _ = async {
                match expiry {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => {
                decoder.expire(Instant::now());
                continue;
            },
            event = async {
                // Neither forwarded traffic nor a busy kernel queue gets priority.
                tokio::select! {
                    Some(forwarded) = inbox.recv() => Ok(Input::Forwarded(forwarded)),
                    len = poll_fn(|cx| device.poll_recv(cx, &mut buffer)) => len.map(Input::Read),
                }
            } => event?,
        };

        match event {
            Input::Forwarded(forwarded) => match forwarded.packet {
                ForwardedPacket::Frame(bytes) => {
                    if let Some(packet) = decoder.decode(&bytes, Instant::now()) {
                        worker.packet(packet)?;
                    }
                }
                ForwardedPacket::Reassembled(packet) => worker.packet(packet)?,
            },
            Input::Read(len) => {
                // SAFETY: PacketReceive reports bytes written into buffer.
                debug_assert!(len <= buffer.len());
                let bytes = &buffer[..len];
                worker.stats.received_packets += 1;
                worker.stats.received_bytes += len as u64;

                let Some(parsed) = packet::parse(bytes) else {
                    continue;
                };
                let Some(key) = parsed.route() else {
                    continue;
                };
                let owner = shared.owner(key);
                if owner != id {
                    worker.forward_raw(bytes, owner);
                } else if let Some(packet) = parsed.decode(&mut decoder, Instant::now()) {
                    worker.packet(packet)?;
                }
            }
        }
    }
}

async fn udp_replies(
    flow: Flow,
    mut driver: p::Datagram,
    output: queue::Sender<Transmit>,
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
