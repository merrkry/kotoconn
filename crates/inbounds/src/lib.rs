//! Public inbound construction. Wire protocols live in their own adapter crates.
mod direct;
use anyhow::Result;
use kotoconn_config::InboundImpl;
pub use kotoconn_http as http;
use kotoconn_protocol::Server;
pub use kotoconn_shadowsocks2022 as shadowsocks2022;
pub use kotoconn_socks5 as socks5;
use std::{net::SocketAddr, sync::Arc};

pub fn build(config: InboundImpl) -> Result<(SocketAddr, Arc<dyn Server>)> {
    Ok(match config {
        InboundImpl::Http(options) => (options.listen, Arc::new(http::Server)),
        InboundImpl::Socks5(options) => (options.listen, Arc::new(socks5::Server)),
        InboundImpl::Shadowsocks2022(options) => (
            options.listen,
            Arc::new(shadowsocks2022::Server::new(&options.password)?),
        ),
        InboundImpl::Direct(options) => (options.listen, Arc::new(direct::Server(options.target))),
    })
}
