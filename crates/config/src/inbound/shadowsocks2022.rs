#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct Shadowsocks2022InboundConfig {
    #[ts(type = "SocketAddr")]
    pub listen: std::net::SocketAddr,
    pub password: String,
}
