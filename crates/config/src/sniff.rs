use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct SniffConfig {
    #[ts(type = "Timeout")]
    pub timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ts_rs::TS)]
#[ts(rename_all = "lowercase")]
pub enum SniffProtocol {
    Http,
    Tls,
    Quic,
}

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct SniffResult {
    pub protocol: SniffProtocol,
    #[ts(type = "string | undefined")]
    pub domain: Option<String>,
}
