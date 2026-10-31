use std::{net::SocketAddr, num::NonZeroU64, time::Duration};

use crate::{DialerId, DnsHandler, InboundId, TransportProtocol};

pub use hickory_proto::op::{DnsRequest, DnsResponse};

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ts_rs::TS)]
#[ts(rename = "DnsServiceRef")]
#[ts(
    type = "Resource & { equals(other: DnsServiceRef): boolean; readonly __brand: unique symbol }"
)]
pub struct DnsServiceId(pub NonZeroU64);

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct DnsUpstreamConfig {
    pub protocol: TransportProtocol,
    #[ts(type = "SocketAddr")]
    pub server: SocketAddr,
}

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct DnsServiceConfig {
    pub upstream: DnsUpstreamConfig,
    pub dialer: DialerId,
    /// Zero disables the service's TTL cache.
    pub cache_capacity: u32,
    #[ts(type = "Timeout")]
    pub timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct DnsListenerConfig {
    #[ts(type = "SocketAddr")]
    pub listen: SocketAddr,
    pub protocols: Vec<TransportProtocol>,
    #[ts(type = "DnsHandler")]
    pub dns_handler: DnsHandler,
}

#[derive(Debug, Clone, Copy)]
pub struct DnsRequestInfo {
    pub inbound: InboundId,
    pub source: SocketAddr,
    pub protocol: TransportProtocol,
}

// A handler exception is an execution error, not Drop.
#[derive(Debug, Clone, ts_rs::TS)]
#[ts(rename = "DnsResult")]
#[ts(type = "{ readonly __brand: unique symbol }")]
pub enum DnsHandlerResult {
    Response(DnsResponse),
    Drop,
}
