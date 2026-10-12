use anyhow::Result;
use bytes::Bytes;
use futures_util::io::{AsyncRead, AsyncWrite};
use hickory_net::runtime::iocompat::AsyncIoTokioAsStd;
use hickory_net::runtime::{DnsTcpStream, DnsUdpSocket, RuntimeProvider, Spawn, TokioTime};
use kotoconn_protocol::{BoxStream, Carrier, Datagram, Packet, Scope, Target};
use parking_lot::Mutex;
use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
    time::Duration,
};
use tokio::io::{AsyncRead as TokioRead, AsyncWrite as TokioWrite, ReadBuf};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) struct Runtime {
    pub carrier: Arc<dyn Carrier>,
    pub scope: Scope,
    pub finished: CancellationToken,
}

#[derive(Clone)]
pub(crate) struct Executor {
    scope: Scope,
    finished: CancellationToken,
}

struct Finished(CancellationToken);

impl Drop for Finished {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl Spawn for Executor {
    fn spawn_bg(&mut self, future: impl Future<Output = ()> + Send + 'static) {
        // Closed scopes reject new work. The future is dropped in that case.
        let guard = Finished(self.finished.clone());
        let _ = self.scope.spawn(async move {
            let _guard = guard;
            future.await;
            Ok(())
        });
    }
}

impl RuntimeProvider for Runtime {
    type Handle = Executor;
    type Timer = TokioTime;
    type Udp = Udp;
    type Tcp = Tcp;

    fn create_handle(&self) -> Self::Handle {
        Executor {
            scope: self.scope.clone(),
            finished: self.finished.clone(),
        }
    }

    fn connect_tcp(
        &self,
        server: SocketAddr,
        bind: Option<SocketAddr>,
        timeout: Option<Duration>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Tcp>> + Send>> {
        let runtime = self.clone();
        Box::pin(async move {
            if bind.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "DNS dialers do not support local bind addresses",
                ));
            }

            let connect = runtime
                .carrier
                .tcp_scoped(target(server), runtime.scope.clone());
            let stream = match timeout {
                Some(timeout) => tokio::time::timeout(timeout, connect)
                    .await
                    .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?,
                None => connect.await,
            }
            .map_err(io::Error::other)?;

            Ok(Tcp(AsyncIoTokioAsStd(SyncStream(Mutex::new(stream)))))
        })
    }

    fn bind_udp(
        &self,
        _local: SocketAddr,
        server: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = io::Result<Udp>> + Send>> {
        let runtime = self.clone();
        Box::pin(async move {
            // Source-port selection belongs to the selected dialer. Hickory's
            // proposed local address has no meaning for a remote proxy association.
            let datagram = runtime
                .carrier
                .udp_scoped(target(server), runtime.scope.clone())
                .await
                .map_err(io::Error::other)?;
            Ok(Udp(Mutex::new(UdpState {
                datagram,
                sending: None,
            })))
        })
    }
}

fn target(address: SocketAddr) -> Target {
    Target::Ip {
        address: address.ip(),
        port: address.port(),
    }
}

struct SyncStream(Mutex<BoxStream>);

impl TokioRead for SyncStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.0.get_mut().as_mut().poll_read(cx, buffer)
    }
}

impl TokioWrite for SyncStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.get_mut().as_mut().poll_write(cx, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.get_mut().as_mut().poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.get_mut().as_mut().poll_shutdown(cx)
    }
}

pub(crate) struct Tcp(AsyncIoTokioAsStd<SyncStream>);

impl DnsTcpStream for Tcp {
    type Time = TokioTime;
}

impl AsyncRead for Tcp {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_read(cx, buffer)
    }
}

impl AsyncWrite for Tcp {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_close(cx)
    }
}

type SendPacket = Pin<Box<dyn Future<Output = Result<usize>> + Send>>;

struct UdpState {
    datagram: Datagram,
    sending: Option<SendPacket>,
}

pub(crate) struct Udp(Mutex<UdpState>);

impl DnsUdpSocket for Udp {
    type Time = TokioTime;

    fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buffer: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddr)>> {
        let mut state = self.0.lock();
        let packet = ready!(state.datagram.rx.poll_recv(cx))
            .ok_or_else(|| io::Error::from(io::ErrorKind::BrokenPipe))?;
        let Target::Ip { address, port } = packet.target else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS reply source must be an IP address",
            )));
        };

        let length = packet.payload.len().min(buffer.len());
        buffer[..length].copy_from_slice(&packet.payload[..length]);
        // This is the logical upstream peer, never the proxy's outer address.
        Poll::Ready(Ok((length, SocketAddr::new(address, port))))
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buffer: &[u8],
        address: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        let mut state = self.0.lock();
        if state.sending.is_none() {
            let sender = state.datagram.tx.clone();
            let scope = state.datagram.scope.clone();
            let packet = Packet {
                target: target(address),
                payload: Bytes::copy_from_slice(buffer),
            };
            let length = buffer.len();
            state.sending = Some(Box::pin(async move {
                scope
                    .run(async move {
                        sender.send(packet).await?;
                        Ok(length)
                    })
                    .await
            }));
        }
        // SAFETY: the branch above installs the pending send before it is polled.
        let pending = state.sending.as_mut().expect("UDP send initialized");
        let result = ready!(pending.as_mut().poll(cx));
        state.sending = None;
        Poll::Ready(result.map_err(io::Error::other))
    }
}
