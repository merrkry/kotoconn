use smoltcp::{
    phy::{self, DeviceCapabilities, Medium},
    time::Instant,
};
use std::{collections::VecDeque, net::SocketAddr};

/// TCP already produced an IP packet. UDP is packetized by the endpoint so
/// all associations share the same fragment identification state.
#[derive(Debug)]
pub(crate) enum Transmit {
    Packet(Vec<u8>),
    Datagram {
        source: SocketAddr,
        destination: SocketAddr,
        payload: bytes::Bytes,
    },
}

/// One ingress packet and bounded egress storage. The TCP driver waits for
/// egress capacity before polling again when the device cannot transmit.
pub(crate) struct Device {
    pub incoming: Option<Vec<u8>>,
    pub outgoing: VecDeque<Vec<u8>>,
    mtu: usize,
}

impl Device {
    pub fn new(mtu: usize) -> Self {
        Self {
            incoming: None,
            outgoing: VecDeque::new(),
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
        Some((Rx(self.incoming.take()?), Tx(&mut self.outgoing)))
    }

    fn transmit(&mut self, _: Instant) -> Option<Tx<'_>> {
        (self.outgoing.len() < 32).then_some(Tx(&mut self.outgoing))
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

pub(crate) struct Tx<'a>(&'a mut VecDeque<Vec<u8>>);

impl phy::TxToken for Tx<'_> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        // SAFETY: receive/transmit issue a token only below the queue limit.
        // Its exclusive borrow prevents another token from filling this slot.
        debug_assert!(self.0.len() < 32);
        let mut packet = vec![0; len];
        let result = f(&mut packet);
        self.0.push_back(packet);
        result
    }
}
