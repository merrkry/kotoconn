#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct Socks5InboundConfig {
    #[ts(type = "SocketAddr")]
    pub listen: std::net::SocketAddr,
}
