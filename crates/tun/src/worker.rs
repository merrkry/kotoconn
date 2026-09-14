use crate::{
    PacketReceive,
    packet::{self, Decoder, Flow, Packet, ReassemblyLimits, RouteKey},
    tcp,
    transmit::Transmit,
    udp,
};
use anyhow::Result;
#[cfg(test)]
use bytes::Bytes;
use kotoconn_protocol::{self as p, Scope, ServerContext, queue};
use smoltcp::wire::{IpProtocol, TcpControl};
use std::{
    collections::{BTreeMap, HashMap, VecDeque, hash_map::RandomState},
    future::poll_fn,
    hash::BuildHasher,
    sync::Arc,
};
use tokio::{sync::mpsc, time::Instant};
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
            ForwardedPacket::Frame(frame) => frame.bytes.len(),
            ForwardedPacket::Reassembled(packet) => packet.storage_size(),
        }
    }
}

enum ForwardedPacket {
    Frame(crate::Received),
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
    connection: tcp::Connection,
    generation: u64,
    deadline: Option<Instant>,
    blocked: bool,
    _tracking: p::WorkGuard,
}

struct UdpEntry {
    packets: queue::Sender<p::Packet>,
    activity: p::Activity,
    scope: Scope,
    generation: u64,
    direct: Option<crate::udp_direct::Direct>,
    blocked: bool,
}

impl Drop for UdpEntry {
    fn drop(&mut self) {
        self.scope.close();
    }
}

struct Completion {
    tx: mpsc::UnboundedSender<(Flow, u64)>,
    flow: Flow,
    generation: u64,
}

impl Drop for Completion {
    fn drop(&mut self) {
        let _ = self.tx.send((self.flow, self.generation));
    }
}

struct Worker {
    id: usize,
    link: tcp::Link,
    context: ServerContext,
    shared: Arc<Shared>,
    tcp: HashMap<Flow, TcpEntry>,
    udp: HashMap<Flow, UdpEntry>,
    done: mpsc::UnboundedSender<(Flow, u64)>,
    output: queue::Sender<Transmit>,
    ready: mpsc::UnboundedSender<tcp::Ready>,
    transfers: mpsc::UnboundedSender<crate::udp_direct::Transfer>,
    timers: BTreeMap<(Instant, u64), Flow>,
    blocked: VecDeque<(tcp::Ready, usize)>,
    pool: crate::pool::Pool,
    arena: crate::storage::PacketArena,
    rejector: tcp::Rejector,
    generation: u64,
    stopping: bool,
    stats: Statistics,
}

impl Worker {
    fn drive(&mut self, id: tcp::Ready) {
        if let Some(entry) = self
            .udp
            .get_mut(&id.0)
            .filter(|entry| entry.generation == id.1)
            && let Some(direct) = &mut entry.direct
        {
            if entry.blocked {
                self.blocked.retain(|(waiting, _)| *waiting != id);
                entry.blocked = false;
            }
            let progress = if entry.scope.is_closed() {
                crate::udp_direct::Progress::Closed
            } else {
                direct.poll(&self.output, &entry.activity)
            };
            match progress {
                crate::udp_direct::Progress::Idle => {}
                crate::udp_direct::Progress::Blocked(bytes) => {
                    entry.blocked = true;
                    self.blocked.push_back((id, bytes));
                }
                crate::udp_direct::Progress::Closed => {
                    self.udp.remove(&id.0);
                }
            }
            return;
        }
        let (flow, generation) = id;
        let Some(entry) = self
            .tcp
            .get_mut(&flow)
            .filter(|e| e.generation == generation)
        else {
            return;
        };
        if let Some(at) = entry.deadline.take() {
            self.timers.remove(&(at, generation));
        }
        // Ingress or application readiness can supersede an output wait.
        // Remove its old cost even when this turn becomes idle or closes TCP.
        if entry.blocked {
            self.blocked.retain(|(waiting, _)| *waiting != id);
            entry.blocked = false;
        }

        entry.connection.begin_turn();
        match entry
            .connection
            .poll(Instant::now(), self.stopping, &self.output, &mut self.arena)
        {
            tcp::Progress::Idle(deadline) => {
                entry.deadline = deadline;
                if let Some(at) = deadline {
                    self.timers.insert((at, generation), flow);
                }
            }
            tcp::Progress::Again => entry.connection.wake(),
            tcp::Progress::Blocked(bytes) => {
                entry.blocked = true;
                self.blocked.push_back((id, bytes));
            }
            tcp::Progress::Closed => {
                self.tcp.remove(&flow);
            }
        }
    }

    fn frame(&mut self, frame: crate::Received, decoder: &mut Decoder) -> Result<()> {
        let bytes = &frame.bytes;
        self.stats.received_packets += 1;
        self.stats.received_bytes += bytes.len() as u64;
        let Some(parsed) = packet::parse(bytes) else {
            return Ok(());
        };
        let Some(key) = parsed.route() else {
            return Ok(());
        };
        let owner = self.shared.owner(key);
        if owner != self.id {
            self.forward_raw(frame, owner);
        } else if let Some(packet) = parsed.decode(decoder, Instant::now()) {
            self.packet(packet, Some(&frame))?;
        }
        Ok(())
    }

    fn forward_raw(&mut self, frame: crate::Received, owner: usize) {
        let bytes = &frame.bytes;
        // SAFETY: owner comes from Shared::owner, and receive buffers hold at most 65575 bytes.
        debug_assert!(owner < self.shared.inboxes.len());
        debug_assert!(bytes.len() <= 65575);
        let Ok(entry) = self.shared.inboxes[owner].try_reserve(bytes.len()) else {
            self.stats.forwarding_drops += 1;
            return;
        };
        entry.send(Forwarded {
            packet: ForwardedPacket::Frame(frame),
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

    fn packet(&mut self, packet: Packet<'_>, frame: Option<&crate::Received>) -> Result<()> {
        let verified = frame.is_some_and(|frame| frame.checksum_verified);
        let backing = frame.map(|frame| &frame.bytes);
        match packet.ip.next_header() {
            IpProtocol::Tcp => {
                let Some((flow, repr)) = packet.tcp_with_checksum(verified) else {
                    return Ok(());
                };
                let owner = self
                    .shared
                    .owner(RouteKey::Flow(u8::from(IpProtocol::Tcp), flow));
                if owner != self.id {
                    self.forward_packet(packet, owner);
                    return Ok(());
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
                    let (connection, accepted) = tcp::Connection::new(
                        flow,
                        self.link,
                        self.ready.clone(),
                        self.generation,
                        self.pool.clone(),
                    )?;
                    let session = self.context.scope.child();
                    let tracking = session.track()?;
                    let handler = self.context.handler.clone();
                    let admission_scope = session.clone();
                    session.spawn(async move {
                        let stream = accepted.await?;
                        handler
                            .tcp(
                                p::target(flow.destination),
                                Box::pin(stream),
                                admission_scope,
                            )
                            .await
                    })?;
                    self.tcp.insert(
                        flow,
                        TcpEntry {
                            connection,
                            generation: self.generation,
                            deadline: None,
                            blocked: false,
                            _tracking: tracking,
                        },
                    );
                }

                // SAFETY: The flow was admitted above and only this worker mutates the map.
                self.tcp
                    .get_mut(&flow)
                    .expect("admitted TCP flow missing")
                    .connection
                    .input_owned(
                        &packet,
                        &repr,
                        backing.cloned(),
                        &self.output,
                        &mut self.arena,
                    );
            }
            IpProtocol::Udp if !self.stopping => {
                let Some((flow, payload)) = packet.udp_with_checksum(verified) else {
                    return Ok(());
                };
                let owner = self
                    .shared
                    .owner(RouteKey::Flow(u8::from(IpProtocol::Udp), flow));
                if owner != self.id {
                    self.forward_packet(packet, owner);
                    return Ok(());
                }

                if self.udp.get(&flow).is_some_and(|e| e.scope.is_closed())
                    && let Some(entry) = self.udp.remove(&flow)
                    && entry.blocked
                {
                    self.blocked
                        .retain(|(id, _)| *id != (flow, entry.generation));
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
                    let (mut association, driver) = p::packet_pair(scope.clone());
                    association.single_target = Some(p::target(flow.destination));
                    association.worker = Some(Arc::new(crate::udp_direct::Handoff {
                        id: (flow, self.generation),
                        sender: self.transfers.clone(),
                        scope: scope.clone(),
                    }));
                    let packets = driver.tx.clone();
                    let activity = p::Activity::default();
                    let clock = activity.clone();
                    let handler = self.context.handler.clone();
                    let output = self.output.clone();
                    let completion = Completion {
                        tx: self.done.clone(),
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
                            _ = clock.until_idle(idle) => {},
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
                            direct: None,
                            blocked: false,
                            activity,
                            scope,
                            generation: self.generation,
                        },
                    );
                }

                // SAFETY: The flow was found or inserted above. Only
                // dispatch mutates this map; tasks only send completions.
                debug_assert!(self.udp.contains_key(&flow));
                let entry = self.udp.get_mut(&flow).expect("admitted UDP flow");
                let segment = frame
                    .and_then(|frame| frame.udp_segment_size)
                    .map(usize::from)
                    .unwrap_or(payload.len().max(1));
                // Empty UDP payloads are still one complete datagram.
                let mut parts = payload.chunks(segment).peekable();
                let mut first = true;
                while first || parts.peek().is_some() {
                    first = false;
                    let payload = parts.next().unwrap_or(&[]);
                    if let Some(direct) = &mut entry.direct {
                        let payload = backing
                            .and_then(|source| crate::storage::view(source, payload))
                            .unwrap_or_else(|| self.pool.copy(payload));
                        if !direct.enqueue(p::Packet {
                            target: p::target(flow.destination),
                            payload,
                        }) {
                            self.stats.capacity_drops += 1;
                        }
                        continue;
                    }
                    let Ok(permit) = entry.packets.try_reserve(payload.len()) else {
                        self.stats.capacity_drops += 1;
                        return Ok(());
                    };
                    let payload = backing
                        .and_then(|source| crate::storage::view(source, payload))
                        .unwrap_or_else(|| self.pool.copy(payload));
                    permit.send(p::Packet {
                        target: p::target(flow.destination),
                        payload,
                    });
                    entry.activity.record();
                }
            }
            _ => {}
        }
        self.stats.processed_packets += 1;
        Ok(())
    }
}

enum Input {
    Forwarded(Forwarded),
    Read(crate::Received),
}

pub(crate) async fn dispatch<R: PacketReceive>(
    id: usize,
    mut device: R,
    mut inbox: queue::Receiver<Forwarded>,
    shared: Arc<Shared>,
    link: tcp::Link,
    context: ServerContext,
    output: queue::Sender<Transmit>,
) -> Result<()> {
    let (done, mut completed) = mpsc::unbounded_channel();
    let (ready, mut runnable) = mpsc::unbounded_channel();
    let (transfers, mut transfer_events) = mpsc::unbounded_channel();
    let pool = crate::pool::Pool::default();
    let mut worker = Worker {
        id,
        link,
        context,
        shared: shared.clone(),
        tcp: HashMap::new(),
        udp: HashMap::new(),
        done,
        output,
        ready,
        transfers,
        timers: BTreeMap::new(),
        blocked: VecDeque::new(),
        pool: pool.clone(),
        arena: crate::storage::PacketArena::with_pool(pool),
        rejector: tcp::Rejector::new(link.mtu),
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
    let mut buffer = crate::ReceiveBuffer::default();
    let mut turns = 0;
    let mut reported_drained = false;

    let writable = worker.output.clone();
    loop {
        if worker.stopping && worker.tcp.is_empty() && !reported_drained {
            let _ = shared.drained.send(id);
            reported_drained = true;
        }
        turns += 1;
        if turns >= 64 {
            tokio::task::yield_now().await;
            turns = 0;
        }
        let expiry = decoder.deadline();
        let tcp_deadline = worker.timers.first_key_value().map(|(&(at, _), _)| at);
        let blocked = worker.blocked.front().copied();
        let event = tokio::select! {
            _ = shared.stop.cancelled() => return Ok(()),
            _ = worker.context.stopping.cancelled(), if !worker.stopping => {
                worker.stopping = true;
                worker.udp.clear();
                worker.blocked.retain(|(id, _)| worker.tcp.get(&id.0).is_some_and(|entry| entry.generation == id.1));
                for entry in worker.tcp.values() {
                    entry.connection.wake();
                }
                worker.pool.trim();
                continue;
            },
            Some((flow, generation)) = completed.recv() => {
                if worker.udp.get(&flow).is_some_and(|e| e.generation == generation) {
                    worker.udp.remove(&flow);
                    worker.blocked.retain(|(id, _)| *id != (flow, generation));
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
            Some(transfer) = transfer_events.recv() => {
                if let Some(entry) = worker.udp.get_mut(&transfer.id.0).filter(|entry| entry.generation == transfer.id.1 && !entry.scope.is_closed()) {
                    entry.direct = Some(crate::udp_direct::Direct::new(transfer, worker.ready.clone()));
                }
                continue;
            },
            Some(id) = runnable.recv() => {
                let udp_batch = worker.udp.contains_key(&id.0);
                worker.drive(id);
                // One UDP readiness event can consume a full batch. Let the
                // independent writer drain it before another batch monopolizes
                // this executor; ordinary packet ingress keeps its small budget.
                if udp_batch { tokio::task::yield_now().await; }
                continue;
            },
            _ = async {
                match tcp_deadline {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(((at, generation), flow)) = worker.timers.pop_first() {
                    debug_assert!(at <= Instant::now());
                    worker.drive((flow, generation));
                }
                continue;
            },
            permit = writable.reserve(blocked.map_or(0, |(_, bytes)| bytes)), if blocked.is_some() => {
                drop(permit?);
                if let Some((id, _)) = worker.blocked.pop_front() {
                    if let Some(entry) = worker.tcp.get_mut(&id.0).filter(|e| e.generation == id.1) {
                        entry.blocked = false;
                    }
                    if let Some(entry) = worker.udp.get_mut(&id.0).filter(|e| e.generation == id.1) {
                        entry.blocked = false;
                    }
                    worker.drive(id);
                }
                continue;
            },
            event = async {
                // Neither forwarded traffic nor a busy kernel queue gets priority.
                tokio::select! {
                    Some(forwarded) = inbox.recv() => Ok(Input::Forwarded(forwarded)),
                    len = poll_fn(|cx| device.poll_frame(cx, &mut buffer)) => len.map(Input::Read),
                }
            } => event?,
        };

        match event {
            Input::Forwarded(forwarded) => match forwarded.packet {
                ForwardedPacket::Frame(frame) => {
                    if let Some(packet) = decoder.decode(&frame.bytes, Instant::now()) {
                        worker.packet(packet, Some(&frame))?;
                    }
                }
                ForwardedPacket::Reassembled(packet) => worker.packet(packet, None)?,
            },
            Input::Read(frame) => {
                worker.frame(frame, &mut decoder)?;
                // Drain only frames already readable. Native UDP can then submit
                // a batch instead of scheduling one socket send per input frame.
                for _ in 0..31 {
                    let next =
                        poll_fn(|cx| std::task::Poll::Ready(device.poll_frame(cx, &mut buffer)))
                            .await;
                    match next {
                        std::task::Poll::Ready(frame) => {
                            worker.frame(frame?, &mut decoder)?;
                            turns += 1;
                        }
                        std::task::Poll::Pending => break,
                    }
                }
            }
        }
    }
}

async fn udp_replies(
    flow: Flow,
    mut driver: p::Datagram,
    output: queue::Sender<Transmit>,
    activity: p::Activity,
) -> Result<()> {
    let mut batch = Vec::with_capacity(32);
    while driver.rx.recv_many(&mut batch, 32).await != 0 {
        for packet in batch.drain(..) {
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
        }
        activity.record();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use smoltcp::wire::*;

    #[tokio::test(start_paused = true)]
    async fn cancelling_a_blocked_handshake_replaces_its_wait_and_releases_it_on_close() {
        let context = crate::parallel_tests::context();
        let (output, mut packets) = queue::channel(512, Transmit::size);
        output
            .try_reserve(512)
            .unwrap()
            .send(Transmit::Packet(Bytes::from(vec![0; 256])));
        let (ready, _runnable) = mpsc::unbounded_channel();
        let (done, _completed) = mpsc::unbounded_channel();
        let (drained, _draining) = mpsc::unbounded_channel();
        let pool = crate::pool::Pool::default();
        let link = tcp::Link {
            mtu: 1280,
            gso: false,
        };
        let flow = Flow {
            source: "192.0.2.2:12345".parse().unwrap(),
            destination: "198.51.100.1:443".parse().unwrap(),
        };
        let (mut connection, accepted) =
            tcp::Connection::new(flow, link, ready.clone(), 1, pool.clone()).unwrap();
        let mut arena = crate::storage::PacketArena::with_pool(pool.clone());
        let syn = TcpRepr {
            src_port: flow.source.port(),
            dst_port: flow.destination.port(),
            control: TcpControl::Syn,
            seq_number: TcpSeqNumber(100),
            ack_number: None,
            window_len: 65535,
            window_scale: Some(7),
            max_seg_size: Some(1220),
            sack_permitted: false,
            sack_ranges: [None; 3],
            timestamp: None,
            payload: &[],
        };
        let ip = IpRepr::new(
            flow.source.ip().into(),
            flow.destination.ip().into(),
            IpProtocol::Tcp,
            syn.buffer_len(),
            64,
        );
        connection.input(&ip, &syn, Instant::now(), &output, &mut arena);
        let entry = TcpEntry {
            connection,
            generation: 1,
            deadline: None,
            blocked: false,
            _tracking: context.scope.track().unwrap(),
        };
        let mut worker = Worker {
            id: 0,
            link,
            context,
            shared: Arc::new(Shared {
                drained,
                hash: RandomState::new(),
                inboxes: vec![],
                reassembly: ReassemblyLimits::default(),
                stop: CancellationToken::new(),
            }),
            tcp: HashMap::from([(flow, entry)]),
            udp: HashMap::new(),
            done,
            output,
            ready,
            transfers: mpsc::unbounded_channel().0,
            timers: BTreeMap::new(),
            blocked: VecDeque::new(),
            pool,
            arena,
            rejector: tcp::Rejector::new(link.mtu),
            generation: 1,
            stopping: false,
            stats: Statistics {
                queue: 0,
                received_packets: 0,
                received_bytes: 0,
                forwarded_packets: 0,
                processed_packets: 0,
                capacity_drops: 0,
                forwarding_drops: 0,
            },
        };

        worker.drive((flow, 1));
        let syn_cost = worker.blocked.front().unwrap().1;
        assert_eq!(worker.blocked.len(), 1);

        // Cancellation supersedes the pending SYN-ACK with a smaller reset.
        // The output queue remains full while another readiness event runs TCP.
        drop(accepted);
        worker.drive((flow, 1));
        assert_eq!(worker.blocked.len(), 1);
        assert!(worker.blocked.front().unwrap().1 < syn_cost);

        packets.try_recv().unwrap();
        worker.drive((flow, 1));
        assert!(worker.tcp.is_empty());
        assert!(worker.blocked.is_empty());
        let Transmit::Packet(bytes) = packets.try_recv().unwrap() else {
            panic!()
        };
        let packet = Decoder::default()
            .decode(&bytes, Instant::now())
            .unwrap()
            .into_owned();
        assert_eq!(packet.tcp().unwrap().1.control, TcpControl::Rst);
    }
}
