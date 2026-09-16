//! Public inbound construction. Wire protocols live in their own adapter crates.
mod direct;

use anyhow::Result;
pub use kotoconn_anytls as anytls;
use kotoconn_config::InboundImpl;
pub use kotoconn_http as http;
use kotoconn_protocol::{Server, ServerContext};
pub use kotoconn_shadowsocks2022 as shadowsocks2022;
pub use kotoconn_socks5 as socks5;
use std::{net::SocketAddr, sync::Arc};

/// A bound inbound need not be an IP listener.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InboundAddress {
    Socket(SocketAddr),
    Tun(String),
}

pub struct BoundInbound {
    pub address: InboundAddress,
    pub run: futures_util::future::BoxFuture<'static, Result<()>>,
}

pub async fn bind(config: InboundImpl, context: ServerContext) -> Result<BoundInbound> {
    let (address, server): (SocketAddr, Arc<dyn Server>) = match config {
        InboundImpl::AnyTls(options) => (options.listen, Arc::new(anytls::Server::new(&options)?)),
        InboundImpl::Http(options) => (options.listen, Arc::new(http::Server)),
        InboundImpl::Socks5(options) => (options.listen, Arc::new(socks5::Server)),
        InboundImpl::Shadowsocks2022(options) => (
            options.listen,
            Arc::new(shadowsocks2022::Server::new(&options.password)?),
        ),
        InboundImpl::Direct(options) => (options.listen, Arc::new(direct::Server(options.target))),
        InboundImpl::Tun(options) => {
            let bound = kotoconn_tun::bind(options, context)?;
            return Ok(BoundInbound {
                address: InboundAddress::Tun(bound.name),
                run: bound.run,
            });
        }
    };
    let bound = server.bind(address, context).await?;
    Ok(BoundInbound {
        address: InboundAddress::Socket(bound.local_addr),
        run: bound.run,
    })
}
