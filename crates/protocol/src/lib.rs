//! Protocol-independent I/O, capability and lifetime contracts.
mod activity;
mod chunk;
mod control;
mod io;
mod packet_io;
pub mod pem;
pub mod pool;
pub mod queue;
mod scoped;
mod server;
mod session;
pub mod stream_buffer;

pub use activity::Activity;
use anyhow::{Result, bail};
pub use chunk::relay;
pub use control::{CloseScope, Scope, WorkGuard};
use futures_util::future::BoxFuture;
pub use io::{
    BoxStream, ChunkBuffer, Datagram, DatagramWorker, Packet, PeerStoppedReading, Stream,
    copy_bidirectional, packet_pair, prefix, stream_task,
};
pub use kotoconn_config::{Endpoint, TransportProtocol};
pub use packet_io::{BoxPacketIo, NativeDatagram, PacketIo, packet_io_task};
pub use server::{BoundServer, InboundHandler, Server, ServerContext, accept, accept_loop};
pub use session::UdpSession;
use std::{net::SocketAddr, sync::Arc};

/// What a carrier offers to the adapters above it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Capabilities {
    pub tcp: bool,
    pub udp: bool,
    /// The carrier passes domain targets on unchanged, for example inside its
    /// wire protocol. Without it, the carrier requires IP targets and rejects
    /// domains instead of resolving them.
    pub domain_targets: bool,
}

impl Capabilities {
    pub const TCP: Self = Self {
        tcp: true,
        udp: false,
        domain_targets: false,
    };

    pub const UDP: Self = Self {
        tcp: false,
        udp: true,
        domain_targets: false,
    };

    pub const BOTH: Self = Self {
        tcp: true,
        udp: true,
        domain_targets: false,
    };

    pub fn require(self, required: Self) -> Result<()> {
        if (required.tcp && !self.tcp)
            || (required.udp && !self.udp)
            || (required.domain_targets && !self.domain_targets)
        {
            bail!("carrier lacks required capabilities: {required:?}; available: {self:?}");
        }
        Ok(())
    }

    /// Check a connection request before it starts any I/O.
    pub fn require_for(self, protocol: TransportProtocol, target: &Endpoint) -> Result<()> {
        let transport = match protocol {
            TransportProtocol::Tcp => Self::TCP,
            TransportProtocol::Udp => Self::UDP,
        };

        self.require(Self {
            domain_targets: matches!(target, Endpoint::Domain { .. }),
            ..transport
        })
    }
}

/// One carrier reference provides all lower-layer capabilities a protocol needs.
pub trait Connector: Send + Sync {
    fn capabilities(&self) -> Capabilities;
    fn scope(&self) -> &Scope;
    fn tcp(&self, target: Endpoint) -> BoxFuture<'_, Result<BoxStream>> {
        self.tcp_scoped(target, self.scope().child())
    }

    fn udp(&self, target: Endpoint) -> BoxFuture<'_, Result<Datagram>> {
        self.udp_scoped(target, self.scope().child())
    }
    /// Propagate completion tracking through every layer of a connection attempt.
    fn tcp_scoped(&self, target: Endpoint, caller: Scope) -> BoxFuture<'_, Result<BoxStream>>;
    fn udp_scoped(&self, target: Endpoint, caller: Scope) -> BoxFuture<'_, Result<Datagram>>;

    /// Open packet I/O for a caller that supplies its own driver. Declining must
    /// perform no network I/O; the caller can then use udp_scoped normally.
    fn udp_native_scoped(
        &self,
        _target: Endpoint,
        _caller: Scope,
    ) -> BoxFuture<'_, Result<Option<NativeDatagram>>> {
        Box::pin(async { Ok(None) })
    }
}

/// Adapters own protocol state. The runtime supplies independent TCP/UDP lifetimes.
pub trait OutboundAdapter: Send + Sync {
    /// Release idle protocol resources during shutdown. Accepted work may still
    /// open connections; active streams must continue to drain.
    fn drain(&self) {}

    fn capabilities(&self) -> Capabilities;
    fn tcp(&self, target: Endpoint, scope: Scope) -> BoxFuture<'_, Result<BoxStream>>;
    fn udp(&self, target: Endpoint, scope: Scope) -> BoxFuture<'_, Result<Datagram>>;

    /// Same opt-in contract as Connector::udp_native_scoped. Encoded transports
    /// keep their ordinary datagram driver unless they explicitly implement it.
    fn udp_native(
        &self,
        _target: Endpoint,
        _scope: Scope,
    ) -> BoxFuture<'_, Result<Option<NativeDatagram>>> {
        Box::pin(async { Ok(None) })
    }
}

/// Resolves only an outbound's own server address. User targets bypass this trait.
pub trait Resolver: Send + Sync {
    fn resolve(&self, name: String) -> BoxFuture<'_, Result<Vec<std::net::IpAddr>>>;
}

#[derive(Clone)]
pub struct ServerEndpoint {
    pub address: Endpoint,
    pub resolver: Arc<dyn Resolver>,
}

impl ServerEndpoint {
    /// The target to request from a carrier with `capabilities`. The server
    /// name is resolved only when that carrier requires IP targets.
    pub async fn target(&self, capabilities: Capabilities) -> Result<Endpoint> {
        match &self.address {
            Endpoint::Domain { name, port } if !capabilities.domain_targets => {
                let addresses = self.resolver.resolve(name.clone()).await?;
                let address = addresses
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("resolver returned no addresses"))?;
                Ok(Endpoint::Ip {
                    address,
                    port: *port,
                })
            }
            _ => Ok(self.address.clone()),
        }
    }
}

/// Whether a datagram from `source` comes from the server at `server`. A
/// carrier that accepted a domain target reports whichever address it reached,
/// so only the port identifies that server.
pub fn from_server(server: &Endpoint, source: &Endpoint) -> bool {
    match server {
        Endpoint::Domain { port, .. } => source.port() == *port,
        Endpoint::Ip { .. } => server == source,
    }
}

pub fn socket_addr(target: &Endpoint) -> Result<SocketAddr> {
    match target {
        Endpoint::Ip { address, port } => Ok(SocketAddr::new(*address, *port)),
        Endpoint::Domain { .. } => {
            bail!("carrier requires IP targets; resolve domains in user policy")
        }
    }
}

pub fn target(address: SocketAddr) -> Endpoint {
    Endpoint::Ip {
        address: address.ip(),
        port: address.port(),
    }
}
