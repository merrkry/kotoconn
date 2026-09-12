use std::num::NonZeroU64;

use crate::{DialerId, Target};

// IDs refer to handlers registered by the host during configuration loading.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ts_rs::TS)]
#[ts(rename = "Routing")]
#[ts(type = "{ readonly __brand: unique symbol }")]
pub struct RoutingHandlerId(pub NonZeroU64);

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
#[ts(rename = "Decision")]
#[ts(type = "{ readonly __brand: unique symbol }")]
pub enum RouteDecision {
    Route { dialer: DialerId, target: Target },
    Reject,
}
