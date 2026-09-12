mod direct;

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
    Direct(DirectInboundConfig),
}
