#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct Hysteria2OutboundConfig {
    pub server: crate::Target,
    pub password: String,
    /// TLS server name; defaults to the configured server's domain or IP.
    pub server_name: Option<String>,
    /// Additional trusted PEM CA certificates. Public roots remain enabled.
    pub ca_certificate: Option<String>,
    pub obfs_password: Option<String>,
}
