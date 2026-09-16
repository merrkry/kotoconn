/// PEM contents, not file paths. TLS verifies both the certificate and server name.
#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct TlsClientConfig {
    pub server_name: Option<String>,
    /// Additional trust anchors alongside Mozilla's public roots.
    pub certificate: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct TlsServerConfig {
    /// PEM certificate chain, leaf first.
    pub certificate: String,
    /// PEM private key matching the leaf certificate.
    pub private_key: String,
}
