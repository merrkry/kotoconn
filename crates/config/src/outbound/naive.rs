/// Naive CONNECT tunnels. QUIC changes the carrier requirement to UDP; the
/// exposed transport is still TCP. Certificates are PEM trust anchors.
#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct NaiveOutboundConfig {
    pub server: crate::Target,
    pub username: String,
    pub password: String,
    #[ts(optional)]
    pub server_name: Option<String>,
    #[ts(optional)]
    pub certificate: Option<String>,
    #[ts(optional)]
    pub quic: Option<bool>,
}
