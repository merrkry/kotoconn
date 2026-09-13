use crate::{
    packet::{Flow, Packet},
    storage::PacketArena,
};
use bytes::Bytes;
use smoltcp::{phy::ChecksumCapabilities, wire::*};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU16, AtomicU32, Ordering},
    },
};

struct Identifiers {
    ipv4: AtomicU16,
    ipv6: AtomicU32,
}

/// Each writer packs its own packets. Only fragment identifiers cross writers.
pub(crate) struct Encoder {
    mtu: usize,
    identifiers: Arc<Identifiers>,
    arena: PacketArena,
}

impl Clone for Encoder {
    fn clone(&self) -> Self {
        Self {
            mtu: self.mtu,
            identifiers: self.identifiers.clone(),
            arena: PacketArena::default(),
        }
    }
}

impl Encoder {
    pub fn new(mtu: usize) -> Self {
        // SAFETY: The endpoint validates MTU before fragment header subtraction.
        debug_assert!((1280..=65535).contains(&mtu));
        Self {
            mtu,
            identifiers: Arc::new(Identifiers {
                ipv4: AtomicU16::new(rand::random()),
                ipv6: AtomicU32::new(rand::random()),
            }),
            arena: PacketArena::default(),
        }
    }

    pub fn encode(
        &mut self,
        source: SocketAddr,
        destination: SocketAddr,
        payload: &[u8],
    ) -> Option<Vec<Bytes>> {
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
        // size and matching address families were validated above.
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
            payload: transport.into(),
        };
        if packet.ip.buffer_len() <= self.mtu {
            return Some(vec![
                self.arena
                    .encode(packet.ip.buffer_len(), |bytes| packet.emit(bytes))
                    .1,
            ]);
        }

        if let IpRepr::Ipv4(mut repr) = packet.ip {
            let size = (self.mtu - 20) / 8 * 8;
            let id = self.identifiers.ipv4.fetch_add(1, Ordering::Relaxed);
            let chunks = packet.payload.chunks(size);
            let count = chunks.len();
            let mut frames = Vec::with_capacity(count);
            for (index, data) in chunks.enumerate() {
                repr.payload_len = data.len();
                let (_, bytes) = self.arena.encode(20 + data.len(), |bytes| {
                    // SAFETY: The frame contains the header and complete chunk;
                    // validated UDP length bounds the byte offset.
                    debug_assert!(index * size <= 0xfff8);
                    let mut header = Ipv4Packet::new_unchecked(&mut *bytes);
                    repr.emit(&mut header, &ChecksumCapabilities::default());
                    header.set_ident(id);
                    header.set_dont_frag(false);
                    header.set_more_frags(index + 1 < count);
                    header.set_frag_offset((index * size) as u16);
                    header.fill_checksum();
                    bytes[20..].copy_from_slice(data);
                });
                frames.push(bytes);
            }
            return Some(frames);
        }

        // SAFETY: Matching address families constructed IpRepr; IPv4 returned
        // above, so only the IPv6 variant can reach this branch.
        debug_assert!(source.is_ipv6() && destination.is_ipv6());
        let IpRepr::Ipv6(mut repr) = packet.ip else {
            unreachable!()
        };
        let size = (self.mtu - 48) / 8 * 8;
        debug_assert!(size > 0 && size.is_multiple_of(8));
        let id = self.identifiers.ipv6.fetch_add(1, Ordering::Relaxed);
        let chunks = packet.payload.chunks(size);
        let count = chunks.len();
        let mut frames = Vec::with_capacity(count);
        for (index, data) in chunks.enumerate() {
            repr.next_header = IpProtocol::Ipv6Frag;
            repr.payload_len = data.len() + 8;
            let (_, bytes) = self.arena.encode(48 + data.len(), |bytes| {
                // SAFETY: Each frame includes both headers and this chunk.
                debug_assert!(index * size / 8 <= 0x1fff);
                repr.emit(&mut Ipv6Packet::new_unchecked(&mut *bytes));
                bytes[40] = u8::from(IpProtocol::Udp);
                Ipv6FragmentRepr {
                    frag_offset: (index * size / 8) as u16,
                    more_frags: index + 1 < count,
                    ident: id,
                }
                .emit(&mut Ipv6FragmentHeader::new_unchecked(&mut bytes[42..48]));
                bytes[48..].copy_from_slice(data);
            });
            frames.push(bytes);
        }
        Some(frames)
    }
}

pub(crate) fn reply_flow(flow: Flow, source: SocketAddr) -> Option<(SocketAddr, SocketAddr)> {
    // Outbound reply metadata selects the wire source. It must be usable in the
    // client's address family; a domain cannot be put into an IP header.
    (source.is_ipv4() == flow.source.is_ipv4() && crate::packet::unicast(source.ip().into()))
        .then_some((source, flow.source))
}
