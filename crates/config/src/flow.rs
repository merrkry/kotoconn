use std::net::SocketAddr;

use crate::Target;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DomainSource {
    Socks,
    Http,
    TlsSni,
    FakeIp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainFact {
    pub name: String,
    pub source: DomainSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Flow {
    pub protocol: TransportProtocol,
    pub source: SocketAddr,
    pub original_target: Target,
    pub domains: Vec<DomainFact>,
}
