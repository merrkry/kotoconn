use crate::Target;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ts_rs::TS)]
#[ts(rename_all = "lowercase")]
pub enum TransportProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, PartialEq, Eq, ts_rs::TS)]
pub struct Flow {
    pub protocol: TransportProtocol,
    // The client's requested destination, unchanged by routing or resolution.
    pub dest: Target,
}
