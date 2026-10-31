use std::num::NonZeroU64;

use crate::{
    DialerConfig, DialerId, DnsListenerConfig, DnsServiceConfig, DnsServiceId, InboundConfig,
    InboundId,
};

/// Identifies one resource of any kind. IDs are never reused, so a reference to a
/// released resource cannot reach one created later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResourceId(pub NonZeroU64);

impl From<DialerId> for ResourceId {
    fn from(id: DialerId) -> Self {
        Self(id.0)
    }
}

impl From<InboundId> for ResourceId {
    fn from(id: InboundId) -> Self {
        Self(id.0)
    }
}

impl From<DnsServiceId> for ResourceId {
    fn from(id: DnsServiceId) -> Self {
        Self(id.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceConfig {
    Inbound(InboundConfig),
    Dialer(DialerConfig),
    DnsService(DnsServiceConfig),
    DnsListener(DnsListenerConfig),
}
