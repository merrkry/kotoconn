//! Protocol-independent I/O, capability and lifetime contracts.
mod control;
mod io;
pub mod queue;
mod server;
pub mod stream_buffer;

use anyhow::{Result, bail};
pub use control::{Scope, WorkGuard};
use futures_util::future::BoxFuture;
pub use io::{BoxStream, Datagram, Packet, Stream, packet_pair, stream_task};
pub use kotoconn_config::{Target, TransportProtocol};
pub use server::{BoundServer, Handler, Server, ServerContext, accept_loop};
use std::{net::SocketAddr, sync::Arc};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Capabilities {
    pub tcp: bool,
    pub udp: bool,
}

impl Capabilities {
    pub const BOTH: Self = Self {
        tcp: true,
        udp: true,
    };
    pub const TCP: Self = Self {
        tcp: true,
        udp: false,
    };
    pub fn require(self, required: Self) -> Result<()> {
        if (required.tcp && !self.tcp) || (required.udp && !self.udp) {
            bail!("carrier lacks required capabilities: {required:?}; available: {self:?}");
        }
        Ok(())
    }
}

/// One carrier reference provides all lower-layer capabilities a protocol needs.
pub trait Carrier: Send + Sync {
    fn capabilities(&self) -> Capabilities;
    fn scope(&self) -> &Scope;
    fn tcp(&self, target: Target) -> BoxFuture<'_, Result<BoxStream>> {
        self.tcp_scoped(target, self.scope().child())
    }

    fn udp(&self, target: Target) -> BoxFuture<'_, Result<Datagram>> {
        self.udp_scoped(target, self.scope().child())
    }
    /// Propagate completion tracking through every layer of a connection attempt.
    fn tcp_scoped(&self, target: Target, caller: Scope) -> BoxFuture<'_, Result<BoxStream>>;
    fn udp_scoped(&self, target: Target, caller: Scope) -> BoxFuture<'_, Result<Datagram>>;
}

/// Adapters own protocol state. The runtime supplies independent TCP/UDP lifetimes.
pub trait Client: Send + Sync {
    fn capabilities(&self) -> Capabilities;
    fn tcp(&self, target: Target, scope: Scope) -> BoxFuture<'_, Result<BoxStream>>;
    fn udp(&self, target: Target, scope: Scope) -> BoxFuture<'_, Result<Datagram>>;
}

/// Resolves only an outbound's own server address. User targets bypass this trait.
pub trait Resolver: Send + Sync {
    fn resolve(&self, name: String) -> BoxFuture<'_, Result<Vec<std::net::IpAddr>>>;
}

#[derive(Clone)]
pub struct Endpoint {
    pub address: Target,
    pub resolver: Arc<dyn Resolver>,
}

impl Endpoint {
    pub async fn resolve(&self) -> Result<Target> {
        match &self.address {
            Target::Ip { .. } => Ok(self.address.clone()),
            Target::Domain { name, port } => {
                let addresses = self.resolver.resolve(name.clone()).await?;
                let address = addresses
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("resolver returned no addresses"))?;
                Ok(Target::Ip {
                    address,
                    port: *port,
                })
            }
        }
    }
}

pub fn socket_addr(target: &Target) -> Result<SocketAddr> {
    match target {
        Target::Ip { address, port } => Ok(SocketAddr::new(*address, *port)),
        Target::Domain { .. } => {
            bail!("I/O carrier requires an IP target; resolve it in user policy")
        }
    }
}

pub fn target(address: SocketAddr) -> Target {
    Target::Ip {
        address: address.ip(),
        port: address.port(),
    }
}
