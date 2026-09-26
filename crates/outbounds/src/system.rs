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
                let stream = TcpStream::connect(address).await?;
                // Relayed request fragments must not wait for the peer's delayed ACK.
                stream.set_nodelay(true)?;
                Ok(Box::pin(stream) as BoxStream)
            })
        })
    }

    fn udp_scoped(&self, target: Target, caller: Scope) -> BoxFuture<'_, Result<Datagram>> {
        Box::pin(async move {
            let socket = udp_socket(&target).await?;
            crate::native_udp::start(socket, target, self.scope.child().tracked_by(&caller))
        })
    }

    fn udp_native_scoped(
        &self,
        target: Target,
        caller: Scope,
    ) -> BoxFuture<'_, Result<Option<NativeDatagram>>> {
        Box::pin(async move {
            let socket = udp_socket(&target).await?;
            crate::native_udp::open(socket, target, self.scope.child().tracked_by(&caller))
                .map(Some)
        })
    }
}

async fn udp_socket(target: &Target) -> Result<UdpSocket> {
    let peer = socket_addr(target)?;
    let socket = UdpSocket::bind(if peer.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    })
    .await?;
    socket.connect(peer).await?;
    tracing::debug!(%peer, "connected UDP socket");
    Ok(socket)
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
