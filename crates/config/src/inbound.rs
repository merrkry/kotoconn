mod shadowsocks2022;
pub use shadowsocks2022::Shadowsocks2022InboundConfig;
mod socks5;
pub use socks5::Socks5InboundConfig;
mod http;
pub use http::HttpInboundConfig;
mod direct;
mod tun;
pub use tun::{TunAddress, TunInboundConfig};

use std::{num::NonZeroU64, time::Duration};

use crate::RoutingHandlerId;

pub use direct::DirectInboundConfig;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ts_rs::TS)]
#[ts(rename = "Inbound")]
#[ts(type = "{ readonly __brand: unique symbol }")]
pub struct InboundId(pub NonZeroU64);

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct InboundConfig {
    pub routing_handler: RoutingHandlerId,
    #[ts(type = "Timeout")]
    pub udp_idle_timeout: Duration,
    pub implementation: InboundImpl,
}

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
#[ts(type = "{ readonly __brand: unique symbol }")]
pub enum InboundImpl {
    Shadowsocks2022(Shadowsocks2022InboundConfig),
    Socks5(Socks5InboundConfig),
    Http(HttpInboundConfig),
    Direct(DirectInboundConfig),
    Tun(TunInboundConfig),
}
