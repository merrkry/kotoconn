#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct HttpInboundConfig {
    #[ts(type = "SocketAddr")]
    pub listen: std::net::SocketAddr,
}
