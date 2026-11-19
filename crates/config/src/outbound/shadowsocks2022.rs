#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct Shadowsocks2022OutboundConfig {
    pub server: crate::Endpoint,
    pub password: String,
}
