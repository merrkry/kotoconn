mod direct;

use std::{num::NonZeroU64, time::Duration};

use crate::RoutingHandlerId;

pub use direct::DirectInboundConfig;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InboundId(pub NonZeroU64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundConfig {
    pub routing_handler: RoutingHandlerId,
    pub udp_idle_timeout: Duration,
    pub implementation: InboundImpl,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundImpl {
    Direct(DirectInboundConfig),
}
