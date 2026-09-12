use crate::Target;

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct Socks5OutboundConfig {
    pub server: Target,
}
