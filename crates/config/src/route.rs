use std::num::NonZeroU64;

use crate::{DialerId, Target};

// IDs refer to handlers registered by the host during configuration loading.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RoutingHandlerId(pub NonZeroU64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    Route { dialer: DialerId, target: Target },
    Reject,
}
