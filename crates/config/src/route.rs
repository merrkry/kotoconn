use crate::{DnsPolicy, Endpoint, OutboundId};

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
#[ts(rename = "Decision")]
#[ts(type = "{ readonly __brand: unique symbol }")]
pub enum RouteDecision {
    ForwardTcp {
        outbound: OutboundId,
        to: Endpoint,
    },
    ForwardUdp {
        outbound: OutboundId,
    },
    /// Answer DNS messages with policy.
    Dns {
        answer: DnsPolicy,
    },
    Reject,
}
