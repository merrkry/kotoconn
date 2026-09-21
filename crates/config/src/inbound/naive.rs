/// TLS HTTP/2 Naive listener with a PEM certificate chain and private key.
#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct NaiveInboundConfig {
    #[ts(type = "SocketAddr")]
    pub listen: std::net::SocketAddr,
    pub username: String,
    pub password: String,
    pub certificate: String,
    pub private_key: String,
}
