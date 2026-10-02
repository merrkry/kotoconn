use crate::{InboundId, Target};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ts_rs::TS)]
#[ts(rename_all = "lowercase")]
pub enum TransportProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct Flow {
    pub inbound: InboundId,
    #[ts(type = "SocketAddr")]
    pub source: std::net::SocketAddr,
    pub protocol: TransportProtocol,
    // The client's requested destination, unchanged by routing or resolution.
    pub dest: Target,
    #[ts(type = "SniffResult | undefined")]
    pub sniff: Option<crate::SniffResult>,
}
