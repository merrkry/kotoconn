use crate::Target;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportProtocol {
    Tcp,
    Udp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Flow {
    pub protocol: TransportProtocol,
    // The client's requested destination, unchanged by routing or resolution.
    pub dest: Target,
}
