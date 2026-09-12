use std::num::NonZeroU64;

use crate::OutboundConfig;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DialerId(pub NonZeroU64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DialerConfig {
    // None ends protocol wrapping and hands the request to the I/O layer.
    pub dialer: Option<DialerId>,
    pub outbound: OutboundConfig,
}
