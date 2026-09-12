use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf};

use crate::Target;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OutboundId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResolverId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuleSetId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundConfig {
    Direct,
    Socks5 { server: Target },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolverTransport {
    Udp { server: SocketAddr },
    Tcp { server: SocketAddr },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolverConfig {
    pub transport: ResolverTransport,
    pub via: OutboundId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleSetConfig {
    Domain { path: PathBuf },
    Ip { path: PathBuf },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Config {
    pub outbounds: BTreeMap<OutboundId, OutboundConfig>,
    pub resolvers: BTreeMap<ResolverId, ResolverConfig>,
    pub rule_sets: BTreeMap<RuleSetId, RuleSetConfig>,
}
