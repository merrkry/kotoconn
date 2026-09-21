use blake2::{Blake2b, Digest, digest::consts::U32};
use bytes::Bytes;
use kotoconn_protocol::{Datagram, Packet, queue, target};
use quinn::{
    AsyncUdpSocket, UdpPoller,
    udp::{RecvMeta, Transmit},
};
use std::{
    fmt,
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, ready},
};

/// A connected virtual UDP socket. The carrier owns address resolution and I/O.
/// A full carrier queue drops a datagram, just like a full kernel UDP send queue;
/// Quinn remains responsible for congestion feedback and reliable retransmission.
pub struct CarrierSocket {
    transport: Mutex<Datagram>,
    peer: SocketAddr,
}

impl CarrierSocket {
    pub fn new(transport: Datagram, peer: SocketAddr) -> Self {
        Self {
            transport: Mutex::new(transport),
            peer,
        }
    }
}

impl fmt::Debug for CarrierSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CarrierSocket")
            .field("peer", &self.peer)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct Writable;

impl UdpPoller for Writable {
    fn poll_writable(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncUdpSocket for CarrierSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(Writable)
    }

    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        if transmit.destination != self.peer || transmit.segment_size.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unsupported virtual UDP destination or segmentation",
            ));
        }
        let transport = self
            .transport
            .lock()
            .map_err(|_| io::Error::other("carrier socket poisoned"))?;
        if transport.scope.is_closed() {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        match transport.tx.try_send(Packet {
            target: target(self.peer),
            payload: Bytes::copy_from_slice(transmit.contents),
        }) {
            Ok(()) | Err(queue::Error::Full) => Ok(()),
            Err(queue::Error::Closed) => Err(io::ErrorKind::BrokenPipe.into()),
        }
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let mut transport = self
            .transport
            .lock()
            .map_err(|_| io::Error::other("carrier socket poisoned"))?;
        if bufs.is_empty() || meta.is_empty() {
            return Poll::Ready(Ok(0));
        }
        for _ in 0..32 {
            let Some(packet) = ready!(transport.rx.poll_recv(cx)) else {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            };
            if packet.target != target(self.peer) || packet.payload.len() > bufs[0].len() {
                continue;
            }
            let length = packet.payload.len();
            bufs[0][..length].copy_from_slice(&packet.payload);
            meta[0] = RecvMeta {
                addr: self.peer,
                len: length,
                stride: length,
                ecn: None,
                dst_ip: None,
            };
            return Poll::Ready(Ok(1));
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        // Carrier does not expose its native bind address. Quinn uses this virtual
        // address only for address-family selection; packets retain the real peer.
        Ok(SocketAddr::new(
            if self.peer.is_ipv4() {
                std::net::Ipv4Addr::UNSPECIFIED.into()
            } else {
                std::net::Ipv6Addr::UNSPECIFIED.into()
            },
            0,
        ))
    }
}

/// Salamander wraps QUIC datagrams; BLAKE2b is supplied by RustCrypto.
#[derive(Debug)]
pub struct Salamander {
    inner: Arc<dyn AsyncUdpSocket>,
    password: Vec<u8>,
}

impl Salamander {
    pub fn wrap(inner: Arc<dyn AsyncUdpSocket>, password: Option<&str>) -> Arc<dyn AsyncUdpSocket> {
        match password {
            Some(password) => Arc::new(Self {
                inner,
                password: password.as_bytes().to_vec(),
            }),
            None => inner,
        }
    }

    fn mask(&self, salt: &[u8], payload: &mut [u8]) {
        let mut hash = Blake2b::<U32>::new();
        hash.update(&self.password);
        hash.update(salt);
        let digest = hash.finalize();
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= digest[index % digest.len()];
        }
    }
}

impl AsyncUdpSocket for Salamander {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        self.inner.clone().create_io_poller()
    }

    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        debug_assert!(transmit.segment_size.is_none());
        let salt: [u8; 8] = rand::random();
        let mut bytes = Vec::with_capacity(transmit.contents.len() + 8);
        bytes.extend_from_slice(&salt);
        bytes.extend_from_slice(transmit.contents);
        self.mask(&salt, &mut bytes[8..]);
        self.inner.try_send(&Transmit {
            contents: &bytes,
            segment_size: None,
            ..*transmit
        })
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        if bufs.is_empty() || meta.is_empty() {
            return Poll::Ready(Ok(0));
        }
        // GRO is disabled at the wrapper boundary. A larger scratch buffer keeps
        // the salt from truncating a maximum-sized QUIC datagram.
        let mut buffer = vec![0; bufs[0].len() + 8];
        let mut metadata = [RecvMeta::default()];
        for _ in 0..32 {
            let count = ready!(self.inner.poll_recv(
                cx,
                &mut [IoSliceMut::new(&mut buffer)],
                &mut metadata
            ))?;
            if count == 0 {
                continue;
            }
            let info = metadata[0];
            // A GRO aggregate cannot be represented by one deobfuscated packet.
            // Disable GRO on the native socket when constructing this wrapper.
            if info.len < 8 || info.len > buffer.len() || info.stride < info.len {
                continue;
            }
            let (salt, payload) = buffer[..info.len].split_at_mut(8);
            self.mask(salt, payload);
            bufs[0][..payload.len()].copy_from_slice(payload);
            meta[0] = RecvMeta {
                len: payload.len(),
                stride: payload.len(),
                ..info
            };
            return Poll::Ready(Ok(1));
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn salamander_matches_blake2b_256_vector() {
        let (transport, _peer) = kotoconn_protocol::packet_pair(kotoconn_protocol::Scope::new());
        let socket = Salamander {
            inner: Arc::new(CarrierSocket::new(
                transport,
                "127.0.0.1:443".parse().unwrap(),
            )),
            password: b"password".to_vec(),
        };
        let mut payload = [0; 32];
        socket.mask(b"12345678", &mut payload);
        // Python hashlib.blake2b(b"password12345678", digest_size=32).
        assert_eq!(
            payload,
            [
                197, 90, 225, 129, 19, 195, 33, 34, 35, 16, 153, 70, 35, 109, 78, 144, 55, 146, 88,
                207, 10, 97, 201, 97, 20, 120, 18, 38, 105, 109, 106, 208
            ]
        );
    }
}

/// Tokio's portable single-datagram API does not enable GRO or GSO, so Salamander
/// sees exactly one salt per receive. QUIC still supplies all transport behavior.
#[derive(Debug)]
pub struct NativeSocket(pub Arc<tokio::net::UdpSocket>);

#[derive(Debug)]
struct NativePoller(Arc<tokio::net::UdpSocket>);

impl UdpPoller for NativePoller {
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.poll_send_ready(cx)
    }
}

impl AsyncUdpSocket for NativeSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(NativePoller(self.0.clone()))
    }

    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        debug_assert!(transmit.segment_size.is_none());
        self.0
            .try_send_to(transmit.contents, transmit.destination)?;
        Ok(())
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        if bufs.is_empty() || meta.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let mut out = tokio::io::ReadBuf::new(&mut bufs[0]);
        let addr = ready!(self.0.poll_recv_from(cx, &mut out))?;
        let len = out.filled().len();
        meta[0] = RecvMeta {
            addr,
            len,
            stride: len,
            ecn: None,
            dst_ip: None,
        };
        Poll::Ready(Ok(1))
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }
}
