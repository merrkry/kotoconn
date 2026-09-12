use crate::{OutboundId, Target};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    Route {
        outbound: OutboundId,
        target: Target,
    },
    Reject,
}
