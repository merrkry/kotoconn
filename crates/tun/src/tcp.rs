//! TCP state belongs to the receive worker. Stream handles exchange immutable
//! payload blocks and consumption notifications without locking the socket.
use crate::{
    packet::{Flow, Packet},
    pool::Pool,
    storage::PacketArena,
    tcp_storage::Storage,
    transmit::Transmit,
};
use bytes::{Buf, Bytes};
use crossbeam_queue::SegQueue;
use futures_util::task::AtomicWaker;
use kotoconn_protocol::queue::{self, Capacity};
use smoltcp::{
    phy::ChecksumCapabilities,
    socket::{
        PollAt,
        tcp::{self, State},
    },
    wire::*,
};
use std::result::Result;
use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, ready},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{mpsc, oneshot},
    task::coop,
    time::Instant,
};

#[path = "tcp_direct.rs"]
mod direct;

const INITIAL: usize = 128 * 1024;
const MAX_WINDOW: usize = (u16::MAX as usize) << 7;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
pub(crate) struct Link {
    pub mtu: usize,
    pub gso: bool,
}

pub(crate) type Ready = (Flow, u64);

struct Shared {
    attachment: SegQueue<direct::Attachment>,
    incoming: SegQueue<Bytes>,
    outgoing: SegQueue<Bytes>,
    rx_used: Arc<AtomicUsize>,
    rx_consumed: Arc<AtomicUsize>,
    tx_used: Arc<AtomicUsize>,
    tx_acked: Arc<AtomicUsize>,
    rx_target: Arc<AtomicUsize>,
    tx_target: Arc<AtomicUsize>,
    reader: AtomicWaker,
    writer: AtomicWaker,
    eof: AtomicBool,
    finish: AtomicBool,
    abort: AtomicBool,
    reset: AtomicBool,
    queued: AtomicBool,
    ready: mpsc::UnboundedSender<Ready>,
    id: Ready,
    pool: Pool,
}

impl std::task::Wake for Shared {
    fn wake(self: Arc<Self>) {
        Shared::wake(&self);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        Shared::wake(self);
    }
}

impl Shared {
    fn wake(&self) {
        if !self.queued.swap(true, Ordering::AcqRel) {
            let _ = self.ready.send(self.id);
        }
    }
}

pub(crate) struct Stream {
    shared: Arc<Shared>,
    head: Bytes,
    read_eof: bool,
    write_eof: bool,
}

impl Drop for Stream {
    fn drop(&mut self) {
        if !(self.read_eof && self.write_eof) {
            self.shared.abort.store(true, Ordering::Release);
        }
        self.shared.wake();
    }
}

fn reset_error() -> io::Error {
    io::ErrorKind::ConnectionReset.into()
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if out.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let budget = ready!(coop::poll_proceed(cx));
        self.shared.reader.register(cx.waker());
        if self.shared.reset.load(Ordering::Acquire) {
            return Poll::Ready(Err(reset_error()));
        }
        let before = out.filled().len();
        while out.remaining() != 0 {
            if self.head.is_empty() {
                let Some(bytes) = self.shared.incoming.pop() else {
                    break;
                };
                self.head = bytes;
            }
            let n = out.remaining().min(self.head.len());
            out.put_slice(&self.head[..n]);
            self.head.advance(n);
        }
        let n = out.filled().len() - before;
        if n != 0 {
            let old = self.shared.rx_used.fetch_sub(n, Ordering::AcqRel);
            debug_assert!(old >= n);
            self.shared.rx_consumed.fetch_add(n, Ordering::Release);
            self.shared.wake();
            budget.made_progress();
            return Poll::Ready(Ok(()));
        }
        if self.shared.eof.load(Ordering::Acquire) {
            // EOF publication follows the final block. Recheck after acquiring
            // EOF in case it raced with the earlier empty-queue observation.
            if let Some(bytes) = self.shared.incoming.pop() {
                self.head = bytes;
                return self.poll_read(cx, out);
            }
            self.read_eof = true;
            budget.made_progress();
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let budget = ready!(coop::poll_proceed(cx));
        self.shared.writer.register(cx.waker());
        if self.shared.reset.load(Ordering::Acquire) {
            return Poll::Ready(Err(reset_error()));
        }
        if self.write_eof {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if data.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let available = self
            .shared
            .tx_target
            .load(Ordering::Acquire)
            .saturating_sub(self.shared.tx_used.load(Ordering::Acquire));
        let n = data.len().min(available).min(65536);
        if n == 0 {
            return Poll::Pending;
        }
        let bytes = self.shared.pool.copy(&data[..n]);
        self.shared.tx_used.fetch_add(n, Ordering::Release);
        self.shared.outgoing.push(bytes);
        self.shared.wake();
        budget.made_progress();
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.shared.reset.load(Ordering::Acquire) {
            Poll::Ready(Err(reset_error()))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.shared.reset.load(Ordering::Acquire) {
            return Poll::Ready(Err(reset_error()));
        }
        self.write_eof = true;
        self.shared.finish.store(true, Ordering::Release);
        self.shared.wake();
        Poll::Ready(Ok(()))
    }
}

impl kotoconn_protocol::Stream for Stream {
    fn take_over<'a>(
        mut self: Pin<&'a mut Self>,
        peer: &mut Option<kotoconn_protocol::BoxStream>,
    ) -> Option<futures_util::future::BoxFuture<'a, io::Result<(u64, u64)>>> {
        if !self.head.is_empty() || self.read_eof || self.write_eof {
            return None;
        }
        let peer = peer.take()?;
        let (done, completion) = oneshot::channel();
        self.shared
            .attachment
            .push(direct::Attachment { peer, done });
        self.shared.wake();
        let cancellation = direct::Cancellation {
            shared: self.shared.clone(),
            armed: true,
        };
        Some(Box::pin(async move {
            let result = completion.await.map_err(|_| reset_error())??;
            cancellation.complete();
            self.read_eof = true;
            self.write_eof = true;
            Ok(result)
        }))
    }

    fn poll_read_chunk(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _: &mut kotoconn_protocol::ChunkBuffer,
    ) -> Poll<io::Result<Bytes>> {
        let budget = ready!(coop::poll_proceed(cx));
        self.shared.reader.register(cx.waker());
        if self.shared.reset.load(Ordering::Acquire) {
            return Poll::Ready(Err(reset_error()));
        }
        if self.head.is_empty()
            && let Some(bytes) = self.shared.incoming.pop()
        {
            self.head = bytes;
        }
        if !self.head.is_empty() {
            budget.made_progress();
            return Poll::Ready(Ok(std::mem::take(&mut self.head)));
        }
        if self.shared.eof.load(Ordering::Acquire) {
            if let Some(bytes) = self.shared.incoming.pop() {
                budget.made_progress();
                return Poll::Ready(Ok(bytes));
            }
            self.read_eof = true;
            Poll::Ready(Ok(Bytes::new()))
        } else {
            Poll::Pending
        }
    }

    fn consume_chunk(self: Pin<&mut Self>, n: usize) {
        let old = self.shared.rx_used.fetch_sub(n, Ordering::AcqRel);
        debug_assert!(old >= n);
        self.shared.rx_consumed.fetch_add(n, Ordering::Release);
        self.shared.wake();
    }

    fn poll_write_chunk(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut Bytes,
    ) -> Poll<io::Result<usize>> {
        let budget = ready!(coop::poll_proceed(cx));
        self.shared.writer.register(cx.waker());
        if self.shared.reset.load(Ordering::Acquire) {
            return Poll::Ready(Err(reset_error()));
        }
        if self.write_eof {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        let available = self
            .shared
            .tx_target
            .load(Ordering::Acquire)
            .saturating_sub(self.shared.tx_used.load(Ordering::Acquire));
        let n = bytes.len().min(available);
        if n == 0 {
            return Poll::Pending;
        }
        self.shared.tx_used.fetch_add(n, Ordering::Release);
        self.shared.outgoing.push(bytes.split_to(n));
        self.shared.wake();
        budget.made_progress();
        Poll::Ready(Ok(n))
    }
}

pub(crate) struct Accept {
    receiver: oneshot::Receiver<Stream>,
    shared: Arc<Shared>,
    completed: bool,
}

impl Future for Accept {
    type Output = Result<Stream, oneshot::error::RecvError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = Pin::new(&mut self.receiver).poll(cx);
        if result.is_ready() {
            self.completed = true;
        }
        result
    }
}

impl Drop for Accept {
    fn drop(&mut self) {
        if !self.completed {
            self.shared.abort.store(true, Ordering::Release);
            self.shared.wake();
        }
    }
}

struct Host {
    now: smoltcp::time::Instant,
    mtu: usize,
    local: IpAddress,
    gso: bool,
}

impl tcp::TcpContext for Host {
    fn now(&self) -> smoltcp::time::Instant {
        self.now
    }

    fn random_u32(&mut self) -> u32 {
        rand::random()
    }

    fn ip_mtu(&self) -> usize {
        self.mtu
    }

    fn segmentation_caps(&self) -> smoltcp::phy::SegmentationCapabilities {
        let capacity = if self.gso {
            std::num::NonZeroUsize::new(65535)
        } else {
            None
        };
        let mut caps = smoltcp::phy::SegmentationCapabilities::default();
        caps.tcpv4 = capacity;
        caps.tcpv6 = capacity;
        caps
    }

    fn has_ip_addr(&self, address: IpAddress) -> bool {
        address == self.local
    }

    fn get_source_address(&self, _: &IpAddress) -> Option<IpAddress> {
        Some(self.local)
    }
}

pub(crate) struct Connection {
    socket: tcp::Socket<'static, Storage>,
    shared: Arc<Shared>,
    admission: Option<(oneshot::Sender<Stream>, Stream)>,
    host: Host,
    epoch: Instant,
    started: bool,
    rx_capacity: Capacity,
    tx_capacity: Capacity,
    closed_normally: bool,
    direct: Option<direct::Direct>,
    handed_over: bool,
}

pub(crate) enum Progress {
    Idle(Option<Instant>),
    Again,
    Blocked(usize),
    Closed,
}

impl Connection {
    pub fn new(
        flow: Flow,
        link: Link,
        ready: mpsc::UnboundedSender<Ready>,
        generation: u64,
        pool: Pool,
    ) -> io::Result<(Self, Accept)> {
        let shared = Arc::new(Shared {
            attachment: SegQueue::new(),
            incoming: SegQueue::new(),
            outgoing: SegQueue::new(),
            rx_used: Arc::new(AtomicUsize::new(0)),
            rx_consumed: Arc::new(AtomicUsize::new(0)),
            tx_used: Arc::new(AtomicUsize::new(0)),
            tx_acked: Arc::new(AtomicUsize::new(0)),
            rx_target: Arc::new(AtomicUsize::new(INITIAL)),
            tx_target: Arc::new(AtomicUsize::new(INITIAL)),
            reader: AtomicWaker::new(),
            writer: AtomicWaker::new(),
            eof: AtomicBool::new(false),
            finish: AtomicBool::new(false),
            abort: AtomicBool::new(false),
            reset: AtomicBool::new(false),
            queued: AtomicBool::new(false),
            ready,
            id: (flow, generation),
            pool: pool.clone(),
        });
        let rx = Storage::new(
            pool.clone(),
            shared.rx_target.clone(),
            false,
            shared.rx_used.clone(),
            shared.rx_consumed.clone(),
        );
        let tx = Storage::new(
            pool,
            shared.tx_target.clone(),
            true,
            shared.tx_used.clone(),
            shared.tx_acked.clone(),
        );
        let mut socket = tcp::Socket::with_buffers(rx, tx);
        socket.set_congestion_control(tcp::CongestionControl::Cubic);
        socket.set_timeout(Some(smoltcp::time::Duration::from_secs(120)));
        socket
            .listen(IpEndpoint::new(
                flow.destination.ip().into(),
                flow.destination.port(),
            ))
            .map_err(io::Error::other)?;
        let stream = Stream {
            shared: shared.clone(),
            head: Bytes::new(),
            read_eof: false,
            write_eof: false,
        };
        let (sender, receiver) = oneshot::channel();
        let accept = Accept {
            receiver,
            shared: shared.clone(),
            completed: false,
        };
        Ok((
            Self {
                socket,
                shared,
                admission: Some((sender, stream)),
                host: Host {
                    now: smoltcp::time::Instant::ZERO,
                    mtu: link.mtu,
                    gso: link.gso,
                    local: flow.destination.ip().into(),
                },
                epoch: Instant::now(),
                started: false,
                rx_capacity: Capacity::new(INITIAL),
                tx_capacity: Capacity::new(INITIAL),
                closed_normally: false,
                direct: None,
                handed_over: false,
            },
            accept,
        ))
    }

    pub fn wake(&self) {
        self.shared.wake();
    }

    pub fn begin_turn(&self) {
        self.shared.queued.store(false, Ordering::Release);
    }

    fn refresh(&mut self, now: Instant) {
        self.host.now = smoltcp::time::Instant::from_micros(
            now.saturating_duration_since(self.epoch).as_micros() as i64,
        );
        self.rx_capacity
            .complete(self.shared.rx_consumed.swap(0, Ordering::AcqRel), now);
        self.tx_capacity
            .complete(self.shared.tx_acked.swap(0, Ordering::AcqRel), now);
        self.shared.rx_target.store(
            self.rx_capacity.target(now).min(MAX_WINDOW),
            Ordering::Release,
        );
        self.shared.tx_target.store(
            self.tx_capacity.target(now).min(MAX_WINDOW),
            Ordering::Release,
        );
    }

    pub fn input(
        &mut self,
        ip: &IpRepr,
        repr: &TcpRepr<'_>,
        now: Instant,
        output: &queue::Sender<Transmit>,
        arena: &mut PacketArena,
    ) {
        self.refresh(now);
        if !self.socket.accepts(&mut self.host, ip, repr) {
            return;
        }
        let previous = self.socket.state();
        if let Some((ip, repr)) = self.socket.process(&mut self.host, ip, repr) {
            // Immediate ACKs may be lost like network packets. Data ownership is
            // unaffected; the peer retries and TCP still schedules later ACKs.
            let _ = emit(arena, output, ip, repr, None);
        }
        if self.socket.state() == State::TimeWait
            || (previous == State::LastAck
                && self.socket.state() == State::Closed
                && repr.control != TcpControl::Rst)
        {
            self.closed_normally = true;
        }
        if self.socket.state() == State::SynReceived {
            self.started = true;
        }
        if self.started && self.socket.state() == State::Listen {
            self.socket.abort();
        }
        self.wake();
    }

    pub fn input_owned(
        &mut self,
        packet: &crate::packet::Packet<'_>,
        repr: &TcpRepr<'_>,
        source: Option<Bytes>,
        output: &queue::Sender<Transmit>,
        arena: &mut PacketArena,
    ) {
        self.socket
            .receive_context(|storage| storage.source = source);
        self.input(&packet.ip, repr, Instant::now(), output, arena);
        self.socket.receive_context(|storage| storage.source = None);
    }

    pub fn poll(
        &mut self,
        now: Instant,
        stopping: bool,
        output: &queue::Sender<Transmit>,
        arena: &mut PacketArena,
    ) -> Progress {
        self.refresh(now);
        if self.shared.abort.load(Ordering::Acquire)
            || (self.admission.is_some() && now >= self.epoch + HANDSHAKE_TIMEOUT)
        {
            self.socket.abort();
        }
        if !self.handed_over
            && let Some(attachment) = self.shared.attachment.pop()
        {
            self.handed_over = true;
            self.direct = Some(direct::Direct::new(attachment));
        }
        if let Some(direct) = &mut self.direct {
            let waker = std::task::Waker::from(self.shared.clone());
            let mut cx = Context::from_waker(&waker);
            let result = if self.socket.state() == State::Closed && !self.closed_normally {
                Err(reset_error())
            } else {
                direct.poll(&mut self.socket, &self.shared, &mut cx)
            };
            if !matches!(result, Ok(false)) {
                // SAFETY: The state remains installed throughout its synchronous poll.
                let direct = self.direct.take().expect("active direct transfer");
                if result.is_err() {
                    self.socket.abort();
                }
                direct.finish(result.map(|_| ()));
            }
        }
        if !self.handed_over {
            if matches!(self.socket.state(), State::Established | State::CloseWait) {
                if let Some((sender, stream)) = self.admission.take()
                    && sender.send(stream).is_err()
                {
                    self.socket.abort();
                }
                for _ in 0..32 {
                    let Some(bytes) = self.shared.outgoing.pop() else {
                        break;
                    };
                    if self
                        .socket
                        .send_buffer(|buffer| {
                            let n = buffer.push(bytes);
                            (n, ())
                        })
                        .is_err()
                    {
                        self.socket.abort();
                        break;
                    }
                }
                if self.shared.finish.load(Ordering::Acquire) && self.shared.outgoing.is_empty() {
                    self.socket.close();
                }
            }
            if self.socket.can_recv() {
                let shared = &self.shared;
                let _ = self.socket.recv_buffer(|buffer| {
                    let mut count = 0;
                    for _ in 0..32 {
                        let Some(bytes) = buffer.take() else {
                            break;
                        };
                        count += bytes.len();
                        shared.rx_used.fetch_add(bytes.len(), Ordering::Release);
                        shared.incoming.push(bytes);
                    }
                    (count, ())
                });
                self.shared.reader.wake();
            }
            if self.admission.is_none()
                && !self.socket.may_recv()
                && (self.socket.state() != State::Closed || self.closed_normally)
            {
                self.shared.eof.store(true, Ordering::Release);
                self.shared.reader.wake();
            }
            self.shared.writer.wake();
            if self.socket.state() == State::Closed && !self.closed_normally {
                self.shared.reset.store(true, Ordering::Release);
                self.shared.reader.wake();
            }
        }
        if self.socket.state() == State::TimeWait && stopping {
            self.closed_normally = true;
            return Progress::Closed;
        }
        for _ in 0..32 {
            let mut sent = false;
            let result = self.socket.dispatch_scattered(
                &mut self.host,
                |_, meta, (ip, repr), storage, range| {
                    let result = emit_scattered(
                        arena,
                        output,
                        ip,
                        repr,
                        meta.segmentation_offload_size,
                        storage,
                        range,
                    );
                    sent = result.is_ok();
                    result
                },
            );
            match result {
                Err((queue::Error::Full, bytes)) => return Progress::Blocked(bytes),
                Err((queue::Error::Closed, _)) => return Progress::Closed,
                Ok(()) => {}
            }
            if self.socket.state() == State::Closed {
                return Progress::Closed;
            }
            if !sent {
                break;
            }
        }
        if !self.handed_over && (self.socket.can_recv() || !self.shared.outgoing.is_empty()) {
            return Progress::Again;
        }
        let deadline = match self.socket.poll_at(&mut self.host) {
            PollAt::Now => return Progress::Again,
            PollAt::Time(at) => {
                Some(self.epoch + Duration::from_micros(at.total_micros().max(0) as u64))
            }
            PollAt::Ingress => None,
        };
        let deadline = if self.admission.is_some() {
            Some(deadline.map_or(self.epoch + HANDSHAKE_TIMEOUT, |at| {
                at.min(self.epoch + HANDSHAKE_TIMEOUT)
            }))
        } else {
            deadline
        };
        Progress::Idle(deadline)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if !self.closed_normally {
            self.shared.reset.store(true, Ordering::Release);
        }
        self.shared.reader.wake();
        self.shared.writer.wake();
    }
}

fn emit_scattered(
    arena: &mut PacketArena,
    output: &queue::Sender<Transmit>,
    ip: IpRepr,
    repr: TcpRepr<'_>,
    segment_size: Option<std::num::NonZeroU16>,
    storage: &Storage,
    range: std::ops::Range<usize>,
) -> Result<(), (queue::Error, usize)> {
    if range.is_empty() {
        return emit(arena, output, ip, repr, segment_size);
    }
    let count = storage.segment_count(range.clone());
    let cost = crate::storage::charge(ip.buffer_len()) + count * std::mem::size_of::<Bytes>();
    let permit = output.try_reserve(cost).map_err(|e| (e, cost))?;
    let count = storage.segments(range);
    let header_len = ip.header_len() + repr.header_len();
    let len = if segment_size.is_some() {
        header_len
    } else {
        ip.buffer_len()
    };
    let (_, packet) = arena.encode(len, |bytes| {
        ip.emit(
            &mut bytes[..ip.header_len()],
            &ChecksumCapabilities::default(),
        );
        let tcp = &mut bytes[ip.header_len()..];
        let mut checksums = ChecksumCapabilities::default();
        checksums.tcp = smoltcp::phy::Checksum::Rx;
        // SAFETY: The allocation includes the complete IP and TCP headers.
        repr.emit(
            &mut TcpPacket::new_unchecked(&mut *tcp),
            &ip.src_addr(),
            &ip.dst_addr(),
            &checksums,
        );
        if segment_size.is_some() {
            let seed = checksum::pseudo_header(
                &ip.src_addr(),
                &ip.dst_addr(),
                IpProtocol::Tcp,
                ip.payload_len() as u32,
            );
            TcpPacket::new_unchecked(tcp).set_checksum(seed);
        } else {
            let mut at = repr.header_len();
            for part in &count {
                tcp[at..at + part.len()].copy_from_slice(part);
                at += part.len();
            }
            TcpPacket::new_unchecked(tcp).fill_checksum(&ip.src_addr(), &ip.dst_addr());
        }
    });
    permit.send(match segment_size {
        Some(size) => Transmit::TcpGso {
            packet,
            payload: count,
            segment_size: size.get(),
        },
        None => Transmit::Packet(packet),
    });
    Ok(())
}

fn emit(
    arena: &mut PacketArena,
    output: &queue::Sender<Transmit>,
    ip: IpRepr,
    repr: TcpRepr<'_>,
    segment_size: Option<std::num::NonZeroU16>,
) -> Result<(), (queue::Error, usize)> {
    let cost = crate::storage::charge(ip.buffer_len());
    let permit = output.try_reserve(cost).map_err(|error| (error, cost))?;
    let (_, packet) = arena.encode(ip.buffer_len(), |bytes| {
        let (header, payload) = bytes.split_at_mut(ip.header_len());
        ip.emit(header, &ChecksumCapabilities::default());
        let mut checksums = ChecksumCapabilities::default();
        if segment_size.is_some() {
            checksums.tcp = smoltcp::phy::Checksum::Rx;
        }
        // SAFETY: The arena allocated the complete IP/TCP packet length.
        repr.emit(
            &mut TcpPacket::new_unchecked(&mut *payload),
            &ip.src_addr(),
            &ip.dst_addr(),
            &checksums,
        );
        if segment_size.is_some() {
            // CHECKSUM_PARTIAL carries the folded pseudo-header sum, without
            // complement. Linux completes the TCP checksum and segmentation.
            let seed = checksum::pseudo_header(
                &ip.src_addr(),
                &ip.dst_addr(),
                IpProtocol::Tcp,
                repr.buffer_len() as u32,
            );
            TcpPacket::new_unchecked(payload).set_checksum(seed);
        }
    });
    permit.send(match segment_size {
        Some(size) => Transmit::TcpGso {
            packet,
            payload: Vec::new(),
            segment_size: size.get(),
        },
        None => Transmit::Packet(packet),
    });
    Ok(())
}

pub(crate) struct Rejector {
    arena: PacketArena,
}

impl Rejector {
    pub fn new(_: usize) -> Self {
        Self {
            arena: PacketArena::default(),
        }
    }

    pub fn reject(
        &mut self,
        packet: &Packet<'_>,
        output: &queue::Sender<Transmit>,
    ) -> Result<(), queue::Error> {
        let Some((_, repr)) = packet.tcp() else {
            return Ok(());
        };
        if repr.control == TcpControl::Rst {
            return Ok(());
        }
        let (ip, repr) = tcp::Socket::<tcp::SocketBuffer>::rst_reply(&packet.ip, &repr);
        emit(&mut self.arena, output, ip, repr, None).map_err(|(error, _)| error)
    }
}

#[cfg(test)]
#[path = "tcp_test_driver.rs"]
mod test_driver;
#[cfg(test)]
pub(crate) use test_driver::{QueuedPacket, connection};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::Decoder;

    #[cfg(target_os = "linux")]
    #[test]
    fn native_gso_preserves_dual_stack_payload_sequence_and_checksums() {
        for ipv6 in [false, true] {
            let source: IpAddress = if ipv6 { "fd00::1" } else { "192.0.2.1" }.parse().unwrap();
            let destination: IpAddress =
                if ipv6 { "fd00::2" } else { "192.0.2.2" }.parse().unwrap();
            let data: Vec<u8> = (0..60001).map(|n| (n % 251) as u8).collect();
            let repr = TcpRepr {
                src_port: 443,
                dst_port: 12345,
                control: TcpControl::Psh,
                seq_number: TcpSeqNumber(100),
                ack_number: Some(TcpSeqNumber(77)),
                window_len: 1234,
                window_scale: None,
                max_seg_size: None,
                sack_permitted: false,
                sack_ranges: [None; 3],
                timestamp: None,
                payload: &data,
            };
            let ip = IpRepr::new(source, destination, IpProtocol::Tcp, repr.buffer_len(), 64);
            let (output, mut input) = queue::channel(INITIAL, Transmit::size);
            let mut storage = Storage::new(
                Pool::default(),
                Arc::new(AtomicUsize::new(INITIAL)),
                true,
                Arc::new(AtomicUsize::new(data.len())),
                Arc::new(AtomicUsize::new(0)),
            );
            for part in data.chunks(8192) {
                storage.push(Bytes::copy_from_slice(part));
            }
            let repr = TcpRepr {
                payload: &[],
                ..repr
            };
            emit_scattered(
                &mut PacketArena::default(),
                &output,
                ip,
                repr,
                std::num::NonZeroU16::new(1200),
                &storage,
                0..data.len(),
            )
            .unwrap();
            let Transmit::TcpGso {
                packet,
                segment_size,
                payload,
            } = input.try_recv().unwrap()
            else {
                panic!()
            };
            assert!(payload.len() > 1);
            smoltcp::socket::tcp::Buffer::dequeue_allocated(&mut storage, data.len());
            let mut full_packet = packet.to_vec();
            for part in payload {
                full_packet.extend_from_slice(&part);
            }
            let packet = Bytes::from(full_packet);
            let header = crate::offload::tcp_gso_header(&packet, segment_size).unwrap();
            let header = tun_rs::VirtioNetHdr::decode(&header).unwrap();
            assert_eq!(header.flags, 1);
            assert_eq!(header.gso_size, 1200);
            let mut frames = vec![vec![0; 1280]; 51];
            let mut sizes = vec![0; frames.len()];
            let count = tun_rs::gso_split(
                &mut packet.to_vec(),
                header,
                &mut frames,
                &mut sizes,
                0,
                ipv6,
            )
            .unwrap();
            let mut reconstructed = Vec::new();
            for (index, (frame, len)) in frames.iter().zip(sizes).take(count).enumerate() {
                let packet = Decoder::default()
                    .decode(&frame[..len], Instant::now())
                    .unwrap()
                    .into_owned();
                let (_, tcp) = packet.tcp().unwrap();
                assert_eq!(tcp.seq_number, TcpSeqNumber(100) + reconstructed.len());
                assert_eq!(tcp.ack_number, Some(TcpSeqNumber(77)));
                assert!(tcp.payload.len() <= 1200);
                assert_eq!(tcp.control == TcpControl::Psh, index + 1 == count);
                reconstructed.extend_from_slice(tcp.payload);
            }
            assert_eq!(reconstructed, data);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn dynamic_receive_window_preserves_advertised_space_when_target_falls() {
        let flow = Flow {
            source: "192.0.2.2:12345".parse().unwrap(),
            destination: "198.51.100.1:443".parse().unwrap(),
        };
        let (ready, _runnable) = mpsc::unbounded_channel();
        let (mut conn, accepted) = Connection::new(
            flow,
            Link {
                mtu: 1280,
                gso: false,
            },
            ready,
            1,
            Pool::default(),
        )
        .unwrap();
        let (output, mut replies) = queue::channel(INITIAL, Transmit::size);
        let mut arena = PacketArena::default();
        let mut repr = TcpRepr {
            src_port: flow.source.port(),
            dst_port: flow.destination.port(),
            control: TcpControl::Syn,
            seq_number: TcpSeqNumber(100),
            ack_number: None,
            window_len: 65535,
            window_scale: Some(7),
            max_seg_size: Some(1220),
            sack_permitted: true,
            sack_ranges: [None; 3],
            timestamp: None,
            payload: &[],
        };
        let ip = IpRepr::new(
            flow.source.ip().into(),
            flow.destination.ip().into(),
            IpProtocol::Tcp,
            repr.buffer_len(),
            64,
        );
        let now = Instant::now();
        conn.input(&ip, &repr, now, &output, &mut arena);
        conn.poll(now, false, &output, &mut arena);
        let Transmit::Packet(synack) = replies.try_recv().unwrap() else {
            panic!()
        };
        let packet = Decoder::default()
            .decode(&synack, now)
            .unwrap()
            .into_owned();
        let (_, synack) = packet.tcp().unwrap();
        assert_eq!(synack.window_scale, Some(7));
        assert_eq!(synack.window_len, 65535);
        assert!(
            replies.try_recv().is_err(),
            "a larger receive window must not repeatedly transmit SYN-ACK"
        );
        assert!(matches!(
            conn.poll(now, false, &output, &mut arena),
            Progress::Idle(Some(at)) if at > now
        ));
        assert!(replies.try_recv().is_err());
        repr.seq_number = TcpSeqNumber(101);
        repr.ack_number = Some(synack.seq_number + 1);
        repr.control = TcpControl::None;
        repr.window_scale = None;
        conn.input(&ip, &repr, now, &output, &mut arena);
        conn.poll(now, false, &output, &mut arena);
        let stream = accepted.await.unwrap();
        let mut right = 101usize + 65535;
        while let Ok(Transmit::Packet(bytes)) = replies.try_recv() {
            let packet = Decoder::default().decode(&bytes, now).unwrap().into_owned();
            let (_, ack) = packet.tcp().unwrap();
            right = ack.ack_number.unwrap().0 as usize + ((ack.window_len as usize) << 7);
        }
        assert!(right <= 101 + INITIAL);
        assert!(right > 101 + 65535);

        // Lower the desired window, then receive a segment inside the previously
        // advertised range. Scaling may round the wire edge down by 127 bytes,
        // but all previously promised bytes must remain acceptable.
        conn.rx_capacity = Capacity::new(16384);
        repr.payload = b"payload";
        conn.input(&ip, &repr, now, &output, &mut arena);
        conn.poll(now + Duration::from_millis(20), false, &output, &mut arena);
        let mut observed = false;
        while let Ok(Transmit::Packet(bytes)) = replies.try_recv() {
            let packet = Decoder::default().decode(&bytes, now).unwrap().into_owned();
            let (_, ack) = packet.tcp().unwrap();
            let new_right = ack.ack_number.unwrap().0 as usize + ((ack.window_len as usize) << 7);
            assert!((right - 127..=right).contains(&new_right));
            observed = true;
        }
        assert!(observed);

        // Leave the application stalled. Fill the old window, including its
        // quantized tail, then probe past it with one-byte segments. Repeated
        // ACK rounding must not create fresh credit or discard the old tail.
        let fill = vec![0; 60000];
        let mut sent = repr.payload.len();
        while sent < INITIAL - 128 {
            let n = fill.len().min(INITIAL - 128 - sent);
            repr.seq_number = TcpSeqNumber(101) + sent;
            repr.payload = &fill[..n];
            conn.input(&ip, &repr, now, &output, &mut arena);
            conn.poll(now + Duration::from_millis(20), false, &output, &mut arena);
            while replies.try_recv().is_ok() {}
            sent += n;
        }
        let mut final_ack = None;
        for offset in sent..INITIAL + 256 {
            repr.seq_number = TcpSeqNumber(101) + offset;
            repr.payload = b"x";
            conn.input(&ip, &repr, now, &output, &mut arena);
            conn.poll(now + Duration::from_millis(20), false, &output, &mut arena);
            while let Ok(Transmit::Packet(bytes)) = replies.try_recv() {
                let packet = Decoder::default().decode(&bytes, now).unwrap().into_owned();
                let (_, ack) = packet.tcp().unwrap();
                final_ack = Some((ack.ack_number.unwrap(), ack.window_len));
            }
        }
        assert_eq!(conn.shared.rx_used.load(Ordering::Acquire), INITIAL);
        assert_eq!(final_ack, Some((TcpSeqNumber(101) + INITIAL, 0)));
        drop(stream);
    }
}
