use crate::storage;
use bytes::Bytes;
use std::net::SocketAddr;

/// TCP already produced an IP packet. UDP is packetized by the endpoint so
/// all associations share the same fragment identification state.
#[derive(Debug)]
pub(crate) enum Transmit {
    Packet(Bytes),
    Datagram {
        source: SocketAddr,
        destination: SocketAddr,
        payload: bytes::Bytes,
    },
}

impl Transmit {
    pub(crate) fn size(&self) -> usize {
        match self {
            Self::Packet(packet) => storage::charge(packet.len()),
            Self::Datagram { payload, .. } => payload.len() + 48,
        }
    }
}
