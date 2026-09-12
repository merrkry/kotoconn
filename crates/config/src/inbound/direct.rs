use std::net::SocketAddr;

use crate::Target;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectInboundConfig {
    pub listen: SocketAddr,
    pub target: Target,
}
