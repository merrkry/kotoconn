//! Each connection owns its smoltcp state and timer. Applications exchange bytes
//! through bounded Tokio buffers; no lock protects a protocol state machine.
use crate::{
    device::{Device, Transmit},
    packet::Flow,
};
use smoltcp::{
    iface::{Config, Interface, SocketSet},
    socket::tcp::{self, State},
    wire::*,
};
use std::{
    future::poll_fn,
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf},
    sync::{mpsc, oneshot},
    time::Instant,
};

const BUFFER_SIZE: usize = 64 * 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct Stream {
    inner: DuplexStream,
    reset: Arc<AtomicBool>,
    dropped: Option<oneshot::Sender<bool>>,
    read_eof: bool,
    write_eof: bool,
}

impl Drop for Stream {
    fn drop(&mut self) {
        if let Some(tx) = self.dropped.take() {
            let _ = tx.send(self.read_eof && self.write_eof);
        }
    }
}

fn reset_error() -> io::Error {
    io::Error::from(io::ErrorKind::ConnectionReset)
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.reset.load(Ordering::Acquire) {
            return Poll::Ready(Err(reset_error()));
        }
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) && buf.filled().len() == before {
            self.read_eof = true;
        }
        result
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.reset.load(Ordering::Acquire) {
            return Poll::Ready(Err(reset_error()));
        }
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.reset.load(Ordering::Acquire) {
            return Poll::Ready(Err(reset_error()));
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.reset.load(Ordering::Acquire) {
            return Poll::Ready(Err(reset_error()));
        }
        let result = Pin::new(&mut self.inner).poll_shutdown(cx);
        if matches!(result, Poll::Ready(Ok(()))) {
            self.write_eof = true;
        }
        result
    }
}

struct ResetOnDrop {
    reset: Arc<AtomicBool>,
    armed: bool,
}

impl Drop for ResetOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.reset.store(true, Ordering::Release);
        }
    }
}

pub(crate) struct QueuedPacket {
    pub bytes: Vec<u8>,
    pub _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

pub(crate) struct Connection {
    pub packets: mpsc::Sender<QueuedPacket>,
    pub accepted: oneshot::Receiver<Stream>,
    pub driver: Pin<Box<dyn Future<Output = io::Result<()>> + Send>>,
}

pub(crate) fn connection(
    flow: Flow,
    mtu: usize,
    output: mpsc::Sender<Transmit>,
    stopping: tokio_util::sync::CancellationToken,
) -> Connection {
    let (packets, mut incoming) = mpsc::channel::<QueuedPacket>(32);
    let (accepted_tx, accepted) = oneshot::channel();
    let (local, mut app) = tokio::io::duplex(BUFFER_SIZE);
    let (dropped, mut drop_rx) = oneshot::channel();
    let reset = Arc::new(AtomicBool::new(false));
    let stream = Stream {
        inner: local,
        reset: reset.clone(),
        dropped: Some(dropped),
        read_eof: false,
        write_eof: false,
    };
    let driver = Box::pin(async move {
        let mut guard = ResetOnDrop { reset, armed: true };
        let epoch = Instant::now();
        let mut device = Device::new(mtu);
        let mut config = Config::new(HardwareAddress::Ip);
        config.random_seed = rand::random();
        let mut iface = Interface::new(config, &mut device, smoltcp::time::Instant::ZERO);
        let destination: IpAddress = flow.destination.ip().into();
        let source: IpAddress = flow.source.ip().into();
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(
                    destination,
                    if flow.destination.is_ipv4() { 32 } else { 128 },
                ))
                .unwrap();
        });
        match source {
            IpAddress::Ipv4(ip) => {
                iface.routes_mut().add_default_ipv4_route(ip).unwrap();
            }
            IpAddress::Ipv6(ip) => {
                iface.routes_mut().add_default_ipv6_route(ip).unwrap();
            }
        }
        let mut socket = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0; BUFFER_SIZE]),
            tcp::SocketBuffer::new(vec![0; BUFFER_SIZE]),
        );
        socket.set_congestion_control(tcp::CongestionControl::Cubic);
        socket.set_timeout(Some(smoltcp::time::Duration::from_secs(120)));
        socket
            .listen(IpEndpoint::new(destination, flow.destination.port()))
            .map_err(io::Error::other)?;
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(socket);
        let mut admission = Some((accepted_tx, stream));
        let mut read_eof = false;
        let mut write_eof = false;
        let mut drop_seen = false;
        let mut turns = 0;
        let mut stop_seen = false;
        let mut handshake_started = false;
        loop {
            let now = smoltcp::time::Instant::from_micros(epoch.elapsed().as_micros() as i64);
            iface.poll(now, &mut device, &mut sockets);
            let socket = sockets.get_mut::<tcp::Socket>(handle);
            if socket.state() == State::SynReceived {
                handshake_started = true;
            }
            // A smoltcp listener returns to Listen on an aborted handshake. This
            // driver represents one connection, so that transition ends it.
            if handshake_started && socket.state() == State::Listen {
                socket.abort();
            }
            if matches!(socket.state(), State::Established | State::CloseWait)
                && let Some((tx, stream)) = admission.take()
                && tx.send(stream).is_err()
            {
                socket.abort();
            }
            if socket.state() == State::Closed
                || (socket.state() == State::TimeWait && stopping.is_cancelled())
            {
                while let Some(packet) = device.outgoing.pop_front() {
                    output
                        .send(Transmit::Packet(packet))
                        .await
                        .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
                }
                if read_eof && write_eof {
                    // Dropping the driver after an orderly close must leave EOF readable.
                    guard.armed = false;
                    return Ok(());
                }
                return Err(reset_error());
            }
            let deadline = iface
                .poll_at(now, &sockets)
                .map(|at| epoch + Duration::from_micros(at.total_micros().max(0) as u64));
            // Bound work per scheduler turn even when all channels stay ready.
            turns += 1;
            if turns == 64 {
                tokio::task::yield_now().await;
                turns = 0;
            }
            let admitting = admission.is_some();
            tokio::select! {
                _ = async { if let Some((tx, _)) = &mut admission { tx.closed().await; } else { std::future::pending().await } }, if admitting => { sockets.get_mut::<tcp::Socket>(handle).abort(); }
                _ = stopping.cancelled(), if !stop_seen => { stop_seen = true; }
                result = &mut drop_rx, if !drop_seen => {
                    drop_seen = true;
                    if result != Ok(true) { sockets.get_mut::<tcp::Socket>(handle).abort(); }
                }
                _ = tokio::time::sleep_until(epoch + HANDSHAKE_TIMEOUT), if admitting => {
                    sockets.get_mut::<tcp::Socket>(handle).abort();
                }
                permit = output.reserve(), if !device.outgoing.is_empty() => {
                    permit.map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?.send(Transmit::Packet(device.outgoing.pop_front().unwrap()));
                }
                packet = incoming.recv(), if device.incoming.is_none() && device.outgoing.len() < 32 => {
                    match packet { Some(packet) => device.incoming = Some(packet.bytes), None => return Err(reset_error()) }
                }
                result = poll_fn(|cx| bridge(cx, sockets.get_mut::<tcp::Socket>(handle), &mut app, &mut read_eof, &mut write_eof)), if !admitting => {
                    if result.is_err() { sockets.get_mut::<tcp::Socket>(handle).abort(); }
                }
                _ = async { match deadline { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } }, if device.outgoing.len() < 32 => {}
            }
        }
    });
    Connection {
        packets,
        accepted,
        driver,
    }
}

fn bridge(
    cx: &mut Context<'_>,
    socket: &mut tcp::Socket<'_>,
    app: &mut DuplexStream,
    read_eof: &mut bool,
    write_eof: &mut bool,
) -> Poll<io::Result<()>> {
    let mut progressed = false;
    if socket.can_recv() {
        let result = socket
            .recv(|bytes| match Pin::new(&mut *app).poll_write(cx, bytes) {
                Poll::Ready(Ok(n)) => (n, Poll::Ready(Ok(()))),
                result => (0, result.map_ok(|_| ())),
            })
            .map_err(io::Error::other)?;
        if let Poll::Ready(result) = result {
            result?;
            progressed = true;
        }
    }
    if !socket.may_recv()
        && !socket.can_recv()
        && !*read_eof
        && let Poll::Ready(result) = Pin::new(&mut *app).poll_shutdown(cx)
    {
        result?;
        *read_eof = true;
        progressed = true;
    }
    if socket.can_send() && !*write_eof {
        let result = socket
            .send(|bytes| {
                let mut buf = ReadBuf::new(bytes);
                match Pin::new(&mut *app).poll_read(cx, &mut buf) {
                    Poll::Ready(Ok(())) => {
                        (buf.filled().len(), Poll::Ready(Ok(buf.filled().len())))
                    }
                    result => (0, result.map_ok(|_| 0)),
                }
            })
            .map_err(io::Error::other)?;
        if let Poll::Ready(result) = result {
            if result? == 0 {
                socket.close();
                *write_eof = true;
            }
            progressed = true;
        }
    }
    if progressed {
        Poll::Ready(Ok(()))
    } else {
        Poll::Pending
    }
}

/// Closed-port replies use smoltcp's TCP reset semantics, including ACK numbers
/// and the rule that an RST never elicits another RST.
pub(crate) struct Rejector {
    iface: Interface,
    device: Device,
    sockets: SocketSet<'static>,
}

impl Rejector {
    pub fn new(mtu: usize) -> Self {
        let mut device = Device::new(mtu);
        let mut config = Config::new(HardwareAddress::Ip);
        config.random_seed = rand::random();
        let iface = Interface::new(config, &mut device, smoltcp::time::Instant::ZERO);
        Self {
            iface,
            device,
            sockets: SocketSet::new(vec![]),
        }
    }

    pub fn reject(&mut self, packet: &crate::packet::Packet) -> Vec<Vec<u8>> {
        self.iface.update_ip_addrs(|addrs| {
            addrs.clear();
            addrs
                .push(IpCidr::new(
                    packet.ip.dst_addr(),
                    if matches!(packet.ip, IpRepr::Ipv4(_)) {
                        32
                    } else {
                        128
                    },
                ))
                .unwrap();
        });
        self.device.incoming = Some(packet.encode());
        self.iface.poll_ingress_single(
            smoltcp::time::Instant::ZERO,
            &mut self.device,
            &mut self.sockets,
        );
        self.device.outgoing.drain(..).collect()
    }
}
