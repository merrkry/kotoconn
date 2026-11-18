#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct Hysteria2OutboundConfig {
    pub server: crate::Target,
    pub password: String,
    /// TLS server name; defaults to the configured server's domain or IP.
    #[ts(optional)]
    pub server_name: Option<String>,
    /// Additional trusted PEM CA certificates. Public roots remain enabled.
    #[ts(optional)]
    pub ca_certificate: Option<String>,
    #[ts(optional)]
    pub obfs_password: Option<String>,
}
