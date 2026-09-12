use std::{net::IpAddr, time::Duration};

use crate::ResolverId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedAddress {
    pub address: IpAddr,
    pub ttl: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DnsAnswer {
    pub resolver: ResolverId,
    pub name: String,
    pub addresses: Vec<ResolvedAddress>,
}
