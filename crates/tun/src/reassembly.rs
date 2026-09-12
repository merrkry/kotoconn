//! smoltcp reassembles IPv4 before raw socket delivery. Use that path only for
//! fragments so ordinary TCP/UDP packets avoid an extra socket-buffer copy.
use crate::device::Device;
use smoltcp::{
    iface::{Config, Interface, SocketHandle, SocketSet},
    socket::raw,
    wire::{HardwareAddress, IpVersion},
};
use tokio::time::Instant;

pub(crate) struct Ipv4Reassembly {
    epoch: Instant,
    iface: Interface,
    device: Device,
    sockets: SocketSet<'static>,
    raw: SocketHandle,
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
        }
    }
}

impl Ipv4Reassembly {
    pub fn process(&mut self, data: &[u8], now: Instant) -> Option<Vec<u8>> {
        let now =
            smoltcp::time::Instant::from_micros(now.duration_since(self.epoch).as_micros() as i64);
        self.iface.poll_maintenance(now);
        self.device.incoming = Some(data.to_vec());
        self.iface
            .poll_ingress_single(now, &mut self.device, &mut self.sockets);
        self.device.outgoing.clear();
        self.sockets
            .get_mut::<raw::Socket>(self.raw)
            .recv()
            .ok()
            .map(<[u8]>::to_vec)
    }
}
