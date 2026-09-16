#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct Hysteria2InboundConfig {
    #[ts(type = "SocketAddr")]
    pub listen: std::net::SocketAddr,
    pub password: String,
    /// PEM certificate chain, leaf first.
    pub certificate: String,
    /// PEM private key matching the leaf certificate.
    pub private_key: String,
    pub obfs_password: Option<String>,
}
