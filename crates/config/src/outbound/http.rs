#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct HttpOutboundConfig {
    pub server: crate::Target,
}
