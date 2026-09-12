use std::num::NonZeroU64;

use crate::OutboundConfig;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ts_rs::TS)]
#[ts(rename = "Dialer")]
#[ts(type = "{ readonly __brand: unique symbol }")]
pub struct DialerId(pub NonZeroU64);

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct DialerConfig {
    // None ends protocol wrapping and hands the request to the I/O layer.
    pub dialer: Option<DialerId>,
    pub outbound: OutboundConfig,
}
