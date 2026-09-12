use crate::Target;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Socks5OutboundConfig {
    pub server: Target,
}
