use crate::storage::{self, PacketArena};
use bytes::Bytes;
use smoltcp::{
    phy::{self, DeviceCapabilities, Medium},
    time::Instant,
};
use std::{collections::VecDeque, net::SocketAddr};

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

/// One ingress packet and bounded egress storage. The TCP driver waits for
/// egress capacity before polling again when the device cannot transmit.
pub(crate) struct Device {
    pub incoming: Option<Vec<u8>>,
    pub outgoing: VecDeque<Bytes>,
    arena: PacketArena,
    mtu: usize,
}

impl Device {
    pub fn new(mtu: usize) -> Self {
        Self {
            incoming: None,
            outgoing: VecDeque::new(),
            arena: PacketArena::default(),
            mtu,
        }
    }
}

impl phy::Device for Device {
    type RxToken<'a> = Rx;
    type TxToken<'a> = Tx<'a>;

    fn receive(&mut self, _: Instant) -> Option<(Rx, Tx<'_>)> {
        if self.outgoing.len() >= 32 {
            return None;
        }
        let incoming = self.incoming.take()?;
        let tx = self.transmit(Instant::ZERO)?;
        Some((Rx(incoming), tx))
    }

    fn transmit(&mut self, _: Instant) -> Option<Tx<'_>> {
        if self.outgoing.len() >= 32 {
            return None;
        }
        Some(Tx {
            outgoing: &mut self.outgoing,
            arena: &mut self.arena,
            mtu: self.mtu,
        })
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ip;
        caps.max_transmission_unit = self.mtu;
        // The egress queue is drained between polls. It does not limit the
        // TCP receive window advertised to the peer.
        caps.max_burst_size = None;
        caps
    }
}

pub(crate) struct Rx(Vec<u8>);

impl phy::RxToken for Rx {
    fn consume<R, F: FnOnce(&[u8]) -> R>(self, f: F) -> R {
        f(&self.0)
    }
}

pub(crate) struct Tx<'a> {
    outgoing: &'a mut VecDeque<Bytes>,
    arena: &'a mut PacketArena,
    mtu: usize,
}

impl phy::TxToken for Tx<'_> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        // SAFETY: The token exclusively borrows the device's bounded work queue;
        // smoltcp derives packet lengths from the advertised device MTU.
        debug_assert!(self.outgoing.len() < 32);
        assert!(len <= self.mtu, "smoltcp exceeded device MTU");
        let (result, bytes) = self.arena.encode(len, f);
        self.outgoing.push_back(bytes);
        result
    }
}
