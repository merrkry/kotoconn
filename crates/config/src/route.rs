use crate::{DialerId, Target};

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
#[ts(rename = "Decision")]
#[ts(type = "{ readonly __brand: unique symbol }")]
pub enum RouteDecision {
    Route { dialer: DialerId, target: Target },
    Udp { dialer: DialerId },
    Reject,
}
