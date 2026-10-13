use std::collections::{HashMap, HashSet};

use crate::{
    DialerConfig, DialerId, DnsHandlerId, DnsListenerConfig, DnsServiceConfig, DnsServiceId,
    InboundConfig, InboundId, ResolveHandlerId, RoutingHandlerId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub inbounds: HashMap<InboundId, InboundConfig>,
    pub dialers: HashMap<DialerId, DialerConfig>,
    pub dns_services: HashMap<DnsServiceId, DnsServiceConfig>,
    pub dns_listeners: HashMap<InboundId, DnsListenerConfig>,
    // The host owns the JS functions; configuration records their registered IDs.
    pub routing_handlers: HashSet<RoutingHandlerId>,
    pub resolve_handlers: HashSet<ResolveHandlerId>,
    pub dns_handlers: HashSet<DnsHandlerId>,
}
