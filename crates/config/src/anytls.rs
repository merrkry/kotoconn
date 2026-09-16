use crate::{Target, TlsClientConfig, TlsServerConfig};
use std::{net::SocketAddr, time::Duration};

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct AnyTlsOutboundConfig {
    pub server: Target,
    pub password: String,
    pub tls: TlsClientConfig,
    #[ts(optional, type = "Timeout")]
    pub idle_session_timeout: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct AnyTlsInboundConfig {
    #[ts(type = "SocketAddr")]
    pub listen: SocketAddr,
    pub password: String,
    pub tls: TlsServerConfig,
    #[ts(optional)]
    pub padding_scheme: Option<String>,
}
