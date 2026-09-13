//! smoltcp reassembles IPv4 before raw socket delivery. Use that path only for
//! fragments so ordinary TCP/UDP packets avoid an extra socket-buffer copy.
use crate::device::Device;
use smoltcp::{
    iface::{Config, Interface, SocketHandle, SocketSet},
    socket::raw,
    wire::{HardwareAddress, IpVersion, Ipv4FragKey, Ipv4Packet},
};
use std::{collections::BTreeMap, time::Duration};
use tokio::time::Instant;

struct HeaderLimit {
    length: usize,
    expires: Instant,
}

pub(crate) struct Ipv4Reassembly {
    epoch: Instant,
    iface: Interface,
    device: Device,
    sockets: SocketSet<'static>,
    raw: SocketHandle,
    headers: BTreeMap<Ipv4FragKey, HeaderLimit>,
}

impl Default for Ipv4Reassembly {
    fn default() -> Self {
        let mut device = Device::new(65535);
        let mut config = Config::new(HardwareAddress::Ip);
        config.random_seed = rand::random();
        let mut iface = Interface::new(config, &mut device, smoltcp::time::Instant::ZERO);
        iface.set_reassembly_timeout(smoltcp::time::Duration::from_secs(60));
        let mut sockets = SocketSet::new(vec![]);
        let raw = sockets.add(raw::Socket::new(
            Some(IpVersion::Ipv4),
            None,
            raw::PacketBuffer::new(vec![raw::PacketMetadata::EMPTY; 1], vec![0; 65535]),
            raw::PacketBuffer::new(vec![], vec![]),
        ));
        Self {
            epoch: Instant::now(),
            iface,
            device,
            sockets,
            raw,
            headers: BTreeMap::new(),
        }
    }
}

impl Ipv4Reassembly {
    pub fn process(&mut self, data: &[u8], now: Instant) -> Option<Vec<u8>> {
        let packet = Ipv4Packet::new_checked(data).ok()?;
        let key = packet.get_key();
        self.headers.retain(|_, header| header.expires > now);
        if !self.headers.contains_key(&key)
            && self.headers.len() >= smoltcp::config::REASSEMBLY_BUFFER_COUNT
        {
            return None;
        }
        let header = self.headers.entry(key).or_insert(HeaderLimit {
            length: 20,
            expires: now + Duration::from_secs(60),
        });
        header.length = header.length.max(usize::from(packet.header_len()));
        let now =
            smoltcp::time::Instant::from_micros(now.duration_since(self.epoch).as_micros() as i64);
        self.iface.poll_maintenance(now);
        self.device.incoming = Some(data.to_vec());
        self.iface
            .poll_ingress_single(now, &mut self.device, &mut self.sockets);
        self.device.outgoing.clear();
        // SAFETY: Default inserted this IPv4 raw socket; process never removes
        // it. A received buffer includes the IPv4 header emitted by smoltcp.
        debug_assert!(self.sockets.iter().any(|(id, socket)| {
            id == self.raw && matches!(socket, smoltcp::socket::Socket::Raw(_))
        }));
        let assembled = self.sockets.get_mut::<raw::Socket>(self.raw).recv().ok()?;
        debug_assert!(assembled.len() >= 20);
        // Raw sockets re-emit a 20-byte header, dropping IPv4 options. Retain
        // only header metadata to enforce the original 65535-byte IP limit;
        // smoltcp still owns all fragment storage and reassembly decisions.
        let header = self.headers.remove(&key)?;
        (assembled.len() - 20 + header.length <= 65535).then(|| assembled.to_vec())
    }
}
