mod direct;
mod http;
mod hysteria2;
mod shadowsocks2022;
mod socks5;

use crate::ResolveHandlerId;

pub use direct::DirectOutboundConfig;
pub use http::HttpOutboundConfig;
pub use hysteria2::Hysteria2OutboundConfig;
pub use shadowsocks2022::Shadowsocks2022OutboundConfig;
pub use socks5::Socks5OutboundConfig;

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct OutboundConfig {
    pub resolve_handler: ResolveHandlerId,
    pub implementation: OutboundImpl,
}

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
#[ts(type = "{ readonly __brand: unique symbol }")]
pub enum OutboundImpl {
    Hysteria2(Hysteria2OutboundConfig),
    AnyTls(crate::AnyTlsOutboundConfig),
    Shadowsocks2022(Shadowsocks2022OutboundConfig),
    Http(HttpOutboundConfig),
    Direct(DirectOutboundConfig),
    Socks5(Socks5OutboundConfig),
}
