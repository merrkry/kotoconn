use anyhow::Result;
use futures_util::future::BoxFuture;
use kotoconn_protocol::*;
use tokio::net::{TcpStream, UdpSocket};

pub struct System {
    scope: Scope,
}

impl System {
    pub fn new(scope: Scope) -> Self {
        Self { scope }
    }
}

impl Carrier for System {
    fn capabilities(&self) -> Capabilities {
        Capabilities::BOTH
    }

    fn scope(&self) -> &Scope {
        &self.scope
    }

    fn tcp_scoped(&self, target: Target, caller: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        Box::pin(async move {
            let address = socket_addr(&target)?;
            tracing::debug!(%address, "connecting TCP socket");
            stream_task(self.scope.child().tracked_by(&caller), async move {
                Ok(Box::pin(TcpStream::connect(address).await?) as BoxStream)
            })
        })
    }

    fn udp_scoped(&self, target: Target, caller: Scope) -> BoxFuture<'_, Result<Datagram>> {
        Box::pin(async move {
            let peer = socket_addr(&target)?;

            let socket = UdpSocket::bind(if peer.is_ipv4() {
                "0.0.0.0:0"
            } else {
                "[::]:0"
            })
            .await?;
            socket.connect(peer).await?;
            #[cfg(target_os = "linux")]
            if let Err(error) = crate::udp_batch::enable_gro(&socket) {
                tracing::debug!(%error, "UDP GRO unavailable");
            }
            tracing::debug!(%peer, "connected UDP socket");

            let scope = self.scope.child().tracked_by(&caller);
            let (user, mut driver) = packet_pair(scope.clone());
            scope.spawn(async move {
                let pool = kotoconn_protocol::pool::Pool::shared();
                let send = async {
                    let mut packets = Vec::with_capacity(32);
                    #[cfg(target_os = "linux")]
                    let mut sender = crate::udp_batch::Sender::default();
                    while driver.rx.recv_many(&mut packets, 32).await != 0 {
                        if packets.iter().any(|p| p.target != target) {
                            anyhow::bail!("datagram target differs from connected target");
                        }
                        let mut sent = 0;
                        while sent < packets.len() {
                            #[cfg(target_os = "linux")]
                            let result = socket
                                .async_io(tokio::io::Interest::WRITABLE, || {
                                    sender.send(&socket, &packets[sent..])
                                })
                                .await;
                            #[cfg(not(target_os = "linux"))]
                            let result = socket.send(&packets[sent].payload).await.map(|_| 1);
                            match result {
                                Ok(n) => {
                                    debug_assert!(n > 0);
                                    sent += n;
                                }
                                Err(error) => {
                                    tracing::warn!(%error, "UDP send");
                                    sent += 1;
                                }
                            }
                        }
                        packets.clear();
                    }
                    Ok::<(), anyhow::Error>(())
                };
                let receive = async {
                    let mut buffers = Vec::new();
                    let mut batch = 1;
                    let mut lengths = [0usize; 32];
                    let mut segments = [0u16; 32];
                    loop {
                        socket.readable().await?;
                        buffers.resize_with(batch, || pool.acquire(65536));
                        #[cfg(target_os = "linux")]
                        let result = socket.try_io(tokio::io::Interest::READABLE, || {
                            crate::udp_batch::receive(
                                &socket,
                                &mut buffers,
                                &mut lengths,
                                &mut segments,
                            )
                        });
                        #[cfg(not(target_os = "linux"))]
                        let result = socket.recv(buffers[0].as_mut()).await.map(|n| {
                            lengths[0] = n;
                            1
                        });
                        let count = match result {
                            Ok(n) => n,
                            Err(error) => {
                                buffers.clear();
                                if error.kind() != std::io::ErrorKind::WouldBlock {
                                    tracing::warn!(%error, "UDP receive");
                                }
                                continue;
                            }
                        };
                        for (i, buffer) in buffers.drain(..).enumerate().take(count) {
                            let length = lengths[i];
                            if length == usize::MAX {
                                continue;
                            }
                            // Compact small receives. GRO aggregates retain one shared
                            // allocation instead of copying each datagram separately.
                            let payload = if length >= 8192 {
                                buffer.freeze(length)
                            } else {
                                pool.copy(&buffer.as_ref()[..length])
                            };
                            let size = if segments[i] == 0 {
                                length.max(1)
                            } else {
                                usize::from(segments[i])
                            };
                            let mut offset = 0;
                            loop {
                                let end = (offset + size).min(length);
                                if let Ok(permit) = driver.tx.try_reserve(end - offset) {
                                    permit.send(Packet {
                                        target: target.clone(),
                                        payload: payload.slice(offset..end),
                                    });
                                }
                                if end == length {
                                    break;
                                }
                                offset = end;
                            }
                        }
                        batch = if count == batch {
                            (batch * 2).min(32)
                        } else {
                            count.max(1)
                        };
                        buffers.clear();
                    }
                };
                tokio::select! { result = send => result, result = receive => result }
            })?;
            Ok(user)
        })
    }
}

/// Temporary DNS implementation for policies that use the system resolver.
pub struct SystemResolver;

impl Resolver for SystemResolver {
    fn resolve(&self, name: String) -> BoxFuture<'_, Result<Vec<std::net::IpAddr>>> {
        Box::pin(async move {
            let mut addresses = Vec::new();
            for address in tokio::net::lookup_host((name.as_str(), 0)).await? {
                if !addresses.contains(&address.ip()) {
                    addresses.push(address.ip());
                }
            }
            Ok(addresses)
        })
    }
}
