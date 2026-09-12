mod direct;
mod socks5;

use crate::ResolveHandlerId;

pub use direct::DirectOutboundConfig;
pub use socks5::Socks5OutboundConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundConfig {
    pub resolve_handler: ResolveHandlerId,
    pub implementation: OutboundImpl,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundImpl {
    Direct(DirectOutboundConfig),
    Socks5(Socks5OutboundConfig),
}
