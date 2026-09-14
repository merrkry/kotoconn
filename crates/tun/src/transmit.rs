use crate::storage;
use bytes::Bytes;
use std::net::SocketAddr;

/// TCP already produced an IP packet. UDP is packetized by the endpoint so
/// all associations share the same fragment identification state.
#[derive(Debug)]
pub(crate) enum Transmit {
    Packet(Bytes),
    TcpGso {
        packet: Bytes,
        payload: Vec<Bytes>,
        segment_size: u16,
    },
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
            Self::TcpGso {
                packet, payload, ..
            } => {
                storage::charge(packet.len() + payload.iter().map(Bytes::len).sum::<usize>())
                    + payload.len() * std::mem::size_of::<Bytes>()
            }
            Self::Datagram { payload, .. } => payload.len() + 48,
        }
    }
}
