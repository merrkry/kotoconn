use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Target {
    Domain { name: String, port: u16 },
    Ip(SocketAddr),
}

impl Target {
    pub fn port(&self) -> u16 {
        match self {
            Self::Domain { port, .. } => *port,
            Self::Ip(address) => address.port(),
        }
    }
}
