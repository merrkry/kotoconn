mod direct;
mod http;
mod hysteria2;
mod naive;
mod shadowsocks2022;
mod socks5;

use crate::ResolvePolicy;
use std::num::NonZeroU64;

pub use direct::DirectOutboundConfig;
pub use http::HttpOutboundConfig;
pub use hysteria2::Hysteria2OutboundConfig;
pub use naive::NaiveOutboundConfig;
pub use shadowsocks2022::Shadowsocks2022OutboundConfig;
pub use socks5::Socks5OutboundConfig;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ts_rs::TS)]
#[ts(rename = "Outbound")]
#[ts(type = "Resource & { equals(other: Outbound): boolean; readonly __brand: unique symbol }")]
pub struct OutboundId(pub NonZeroU64);

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct OutboundConfig {
    pub via: OutboundVia,
    #[ts(type = "ResolvePolicy")]
    pub resolve_server: ResolvePolicy,
    pub protocol: OutboundProtocol,
}

/// How an outbound reaches the network or its proxy server.
#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
#[ts(type = "Outbound | HostNetwork")]
pub enum OutboundVia {
    Outbound(OutboundId),
    Host(HostNetworkConfig),
}

/// Options applied to every socket the host network opens for one outbound.
/// Both are Linux socket options; other platforms reject them.
#[derive(Debug, Clone, Default, PartialEq, Eq, ts_rs::TS)]
pub struct HostNetworkConfig {
    /// Socket mark (`SO_MARK`) for policy routing and firewall rules.
    #[ts(optional)]
    pub mark: Option<u32>,
    /// Network interface the sockets are bound to (`SO_BINDTODEVICE`).
    #[ts(optional)]
    pub interface: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
#[ts(type = "{ readonly __brand: unique symbol }")]
pub enum OutboundProtocol {
    Hysteria2(Hysteria2OutboundConfig),
    AnyTls(crate::AnyTlsOutboundConfig),
    Shadowsocks2022(Shadowsocks2022OutboundConfig),
    Http(HttpOutboundConfig),
    Naive(NaiveOutboundConfig),
    Direct(DirectOutboundConfig),
    Socks5(Socks5OutboundConfig),
}
