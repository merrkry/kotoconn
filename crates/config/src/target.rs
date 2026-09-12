use crate::IpAddr;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Target {
    Domain { name: String, port: u16 },
    Ip { address: IpAddr, port: u16 },
}

impl Target {
    pub fn port(&self) -> u16 {
        match self {
            Self::Domain { port, .. } | Self::Ip { port, .. } => *port,
        }
    }
}
