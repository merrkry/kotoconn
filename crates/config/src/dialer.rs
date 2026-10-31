use std::num::NonZeroU64;

use crate::OutboundConfig;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ts_rs::TS)]
#[ts(rename = "Dialer")]
#[ts(type = "{ readonly __brand: unique symbol }")]
pub struct DialerId(pub NonZeroU64);

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct DialerConfig {
    pub carrier: CarrierConfig,
    pub outbound: OutboundConfig,
}

/// The lower layer that carries a dialer's connections.
#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
#[ts(type = "Dialer | HostNetwork")]
pub enum CarrierConfig {
    Dialer(DialerId),
    Host(HostNetworkConfig),
}

/// Options applied to every socket the host network opens for one dialer.
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
