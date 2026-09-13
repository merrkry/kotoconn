use crate::{
    device::Device,
    packet::{Flow, Packet},
};
use smoltcp::{
    iface::{Config, Interface, SocketHandle, SocketSet},
    phy::ChecksumCapabilities,
    socket::raw,
    wire::*,
};
use std::net::SocketAddr;

/// Uses smoltcp's IPv4 fragmenter. IPv6 fragmentation is missing on Medium::Ip
/// in smoltcp 0.14; its wire representations still own the header encoding.
pub(crate) struct Encoder {
    iface: Interface,
    device: Device,
    sockets: SocketSet<'static>,
    raw: SocketHandle,
    mtu: usize,
    ipv6_id: u32,
}

impl Encoder {
    pub fn new(mtu: usize) -> Self {
        // SAFETY: run validates the MTU. IPv6 fragmentation later subtracts
        // 48 header bytes and requires a nonzero, eight-byte-aligned chunk.
        debug_assert!((1280..=65535).contains(&mtu));
        let mut device = Device::new(mtu);
        let mut config = Config::new(HardwareAddress::Ip);
        config.random_seed = rand::random();
        let iface = Interface::new(config, &mut device, smoltcp::time::Instant::ZERO);
        let mut sockets = SocketSet::new(vec![]);
        let raw = sockets.add(raw::Socket::new(
            Some(IpVersion::Ipv4),
            Some(IpProtocol::Udp),
            raw::PacketBuffer::new(vec![], vec![]),
            raw::PacketBuffer::new(vec![raw::PacketMetadata::EMPTY], vec![0; 65535]),
        ));
        Self {
            iface,
            device,
            sockets,
            raw,
            mtu,
            ipv6_id: rand::random(),
        }
    }

    pub fn encode(
        &mut self,
        source: SocketAddr,
        destination: SocketAddr,
        payload: &[u8],
    ) -> Option<Vec<Vec<u8>>> {
        if source.is_ipv4() != destination.is_ipv4()
            || source.port() == 0
            || payload.len() > if source.is_ipv4() { 65507 } else { 65527 }
        {
            return None;
        }

        let udp = UdpRepr {
            src_port: source.port(),
            dst_port: destination.port(),
        };
        let source_ip = source.ip().into();
        let destination_ip = destination.ip().into();
        let mut transport = vec![0; 8 + payload.len()];
        // SAFETY: The buffer includes the UDP header and complete payload;
        // the size and matching address families were validated above.
        debug_assert!(u16::try_from(transport.len()).is_ok());
        udp.emit(
            &mut UdpPacket::new_unchecked(&mut transport),
            &source_ip,
            &destination_ip,
            payload.len(),
            |bytes| bytes.copy_from_slice(payload),
            &ChecksumCapabilities::default(),
        );

        let ip = IpRepr::new(
            source_ip,
            destination_ip,
            IpProtocol::Udp,
            transport.len(),
            64,
        );
        let packet = Packet {
            ip,
            payload: transport,
        };

        if packet.ip.buffer_len() <= self.mtu {
            return Some(vec![packet.encode()]);
        }
        if source.is_ipv4() {
            // SAFETY: new inserted this raw socket and no method removes it.
            debug_assert!(self.sockets.iter().any(|(id, socket)| {
                id == self.raw && matches!(socket, smoltcp::socket::Socket::Raw(_))
            }));
            self.sockets
                .get_mut::<raw::Socket>(self.raw)
                .send_slice(&packet.encode())
                .ok()?;
            let mut frames = Vec::new();
            loop {
                self.iface.poll_egress(
                    smoltcp::time::Instant::ZERO,
                    &mut self.device,
                    &mut self.sockets,
                );
                if self.device.outgoing.is_empty() {
                    break;
                }
                frames.extend(self.device.outgoing.drain(..));
            }
            return Some(frames);
        }

        // SAFETY: IpRepr was built from matching address families, and the
        // IPv4 branch returned above. Only the IPv6 variant can reach here.
        debug_assert!(source.is_ipv6() && destination.is_ipv6());
        debug_assert!(matches!(packet.ip, IpRepr::Ipv6(_)));
        let IpRepr::Ipv6(mut repr) = packet.ip else {
            unreachable!()
        };
        let size = (self.mtu - 48) / 8 * 8;
        debug_assert!(size > 0 && size.is_multiple_of(8));
        let id = self.ipv6_id;
        self.ipv6_id = self.ipv6_id.wrapping_add(1);
        let chunks = packet.payload.chunks(size);
        let count = chunks.len();
        Some(
            chunks
                .enumerate()
                .map(|(i, data)| {
                    repr.next_header = IpProtocol::Ipv6Frag;
                    repr.payload_len = data.len() + 8;
                    let mut bytes = vec![0; 48 + data.len()];
                    // SAFETY: Each buffer holds the 40-byte IPv6 header,
                    // 8-byte fragment header and this chunk. Validated UDP
                    // size also bounds every fragment offset to 13 bits.
                    debug_assert!(bytes.len() <= self.mtu);
                    debug_assert_eq!(bytes.len(), 40 + repr.payload_len);
                    debug_assert!(i * size / 8 <= 0x1fff);
                    repr.emit(&mut Ipv6Packet::new_unchecked(&mut bytes));
                    bytes[40] = u8::from(IpProtocol::Udp);
                    Ipv6FragmentRepr {
                        frag_offset: (i * size / 8) as u16,
                        more_frags: i + 1 < count,
                        ident: id,
                    }
                    .emit(&mut Ipv6FragmentHeader::new_unchecked(&mut bytes[42..48]));
                    bytes[48..].copy_from_slice(data);
                    bytes
                })
                .collect(),
        )
    }
}

pub(crate) fn reply_flow(flow: Flow, source: SocketAddr) -> Option<(SocketAddr, SocketAddr)> {
    // Outbound reply metadata selects the wire source. It must be usable in the
    // client's address family; a domain cannot be put into an IP header.
    (source.is_ipv4() == flow.source.is_ipv4() && crate::packet::unicast(source.ip().into()))
        .then_some((source, flow.source))
}
