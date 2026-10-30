use anyhow::{Result, ensure};
use futures_util::future::BoxFuture;
use kotoconn_config::HostNetworkConfig;
use kotoconn_protocol::*;
use std::io;
use tokio::net::{TcpSocket, UdpSocket};

/// The host network as a carrier. It requires IP targets.
pub struct System {
    scope: Scope,
    options: HostNetworkConfig,
}

impl System {
    pub fn new(scope: Scope, options: HostNetworkConfig) -> Result<Self> {
        #[cfg(target_os = "linux")]
        if let Some(interface) = &options.interface {
            // The kernel silently truncates longer names to IFNAMSIZ - 1 bytes.
            ensure!(
                !interface.is_empty()
                    && interface.len() < libc::IFNAMSIZ
                    && !interface.contains('\0'),
                "invalid network interface name {interface:?}"
            );
        }

        #[cfg(not(target_os = "linux"))]
        ensure!(
            options == HostNetworkConfig::default(),
            "host network socket marks and interface bindings require Linux"
        );

        Ok(Self { scope, options })
    }

    #[cfg(target_os = "linux")]
    fn prepare<S: std::os::fd::AsFd>(&self, socket: &S) -> io::Result<()> {
        let socket = socket2::SockRef::from(socket);

        if let Some(mark) = self.options.mark {
            socket.set_mark(mark)?;
        }
        if let Some(interface) = &self.options.interface {
            socket.bind_device(Some(interface.as_bytes()))?;
        }
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn prepare<S>(&self, _: &S) -> io::Result<()> {
        // System::new rejects options on other platforms.
        debug_assert_eq!(self.options, HostNetworkConfig::default());
        Ok(())
    }

    async fn udp_socket(&self, target: &Target) -> Result<UdpSocket> {
        let peer = socket_addr(target)?;
        let socket = UdpSocket::bind(if peer.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        })
        .await?;
        // Both options take effect when connect selects the route.
        self.prepare(&socket)?;

        socket.connect(peer).await?;
        tracing::debug!(%peer, "connected UDP socket");
        Ok(socket)
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
            let socket = if address.is_ipv4() {
                TcpSocket::new_v4()?
            } else {
                TcpSocket::new_v6()?
            };
            self.prepare(&socket)?;
            tracing::debug!(%address, "connecting TCP socket");

            stream_task(self.scope.child().tracked_by(&caller), async move {
                let stream = socket.connect(address).await?;
                // Relayed request fragments must not wait for the peer's delayed ACK.
                stream.set_nodelay(true)?;
                Ok(Box::pin(stream) as BoxStream)
            })
        })
    }

    fn udp_scoped(&self, target: Target, caller: Scope) -> BoxFuture<'_, Result<Datagram>> {
        Box::pin(async move {
            let socket = self.udp_socket(&target).await?;
            crate::native_udp::start(socket, target, self.scope.child().tracked_by(&caller))
        })
    }

    fn udp_native_scoped(
        &self,
        target: Target,
        caller: Scope,
    ) -> BoxFuture<'_, Result<Option<NativeDatagram>>> {
        Box::pin(async move {
            let socket = self.udp_socket(&target).await?;
            crate::native_udp::open(socket, target, self.scope.child().tracked_by(&caller))
                .map(Some)
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn prepare_sets_the_configured_mark_and_interface() {
        let system = System::new(
            Scope::new(),
            HostNetworkConfig {
                mark: Some(0x2a),
                interface: Some("lo".into()),
            },
        )
        .unwrap();
        let socket =
            socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::DGRAM, None).unwrap();

        match system.prepare(&socket) {
            Ok(()) => {
                assert_eq!(socket.mark().unwrap(), 0x2a);
                assert_eq!(socket.device().unwrap().as_deref(), Some(&b"lo"[..]));
            }
            // SO_MARK requires CAP_NET_ADMIN; the mark is applied first.
            Err(error) => assert_eq!(error.kind(), io::ErrorKind::PermissionDenied),
        }
    }
}
