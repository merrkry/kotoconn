use std::net::SocketAddr;

use crate::Target;

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct DirectInboundConfig {
    #[ts(type = "SocketAddr")]
    pub listen: SocketAddr,
    pub target: Target,
}
