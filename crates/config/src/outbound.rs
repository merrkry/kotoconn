mod direct;
mod http;
mod naive;
mod shadowsocks2022;
mod socks5;

use crate::ResolveHandlerId;

pub use direct::DirectOutboundConfig;
pub use http::HttpOutboundConfig;
pub use naive::NaiveOutboundConfig;
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
    Shadowsocks2022(Shadowsocks2022OutboundConfig),
    Http(HttpOutboundConfig),
    Naive(NaiveOutboundConfig),
    Direct(DirectOutboundConfig),
    Socks5(Socks5OutboundConfig),
}
