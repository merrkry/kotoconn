use std::collections::{HashMap, HashSet};

use crate::{
    DialerConfig, DialerId, DnsHandlerId, InboundConfig, InboundId, ResolveHandlerId,
    RoutingHandlerId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub inbounds: HashMap<InboundId, InboundConfig>,
    pub dialers: HashMap<DialerId, DialerConfig>,
    // The host owns the JS functions; configuration records their registered IDs.
    pub routing_handlers: HashSet<RoutingHandlerId>,
    pub resolve_handlers: HashSet<ResolveHandlerId>,
    pub dns_handlers: HashSet<DnsHandlerId>,
}
