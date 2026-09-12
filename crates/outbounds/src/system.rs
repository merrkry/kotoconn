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
            let scope = self.scope.child().tracked_by(&caller);
            let (user, mut driver) = packet_pair(scope.clone());
            scope.spawn(async move {
                let mut buffer = vec![0; 65536];
                loop {
                    tokio::select! {
                        received = socket.recv(&mut buffer) => {
                            match received {
                                Ok(n) => { let _ = driver.tx.try_send(Packet { target: target.clone(), payload: buffer[..n].to_vec() }); }
                                Err(error) => eprintln!("UDP receive: {error}"),
                            }
                        }
                        packet = driver.rx.recv() => {
                            let Some(packet) = packet else { return Ok(()); };
                            if packet.target != target { anyhow::bail!("datagram target differs from connected target"); }
                            if let Err(error) = socket.send(&packet.payload).await { eprintln!("UDP send: {error}"); }
                        }
                    }
                }
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
