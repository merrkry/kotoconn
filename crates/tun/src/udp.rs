use crate::{packet::Flow, storage::PacketArena};
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

    pub fn can_offload(&self, source: SocketAddr, destination: SocketAddr, size: usize) -> bool {
        source.is_ipv4() == destination.is_ipv4()
            && source.port() != 0
            && size + if source.is_ipv4() { 28 } else { 48 } <= self.mtu
    }

    /// Header for a checksum-offloaded UDP aggregate. Individual datagrams must
    /// fit the link MTU; IP fragmentation remains the ordinary encoder's job.
    pub fn header(
        &mut self,
        source: SocketAddr,
        destination: SocketAddr,
        size: usize,
        total: usize,
    ) -> Option<Bytes> {
        let ip_len = if source.is_ipv4() { 20 } else { 40 };
        if !self.can_offload(source, destination, size)
            || total > if source.is_ipv4() { 65507 } else { 65527 }
        {
            return None;
        }
        let ip = IpRepr::new(
            source.ip().into(),
            destination.ip().into(),
            IpProtocol::Udp,
            8 + total,
            64,
        );
        Some(
            self.arena
                .encode(ip_len + 8, |bytes| {
                    ip.emit(&mut bytes[..ip_len], &ChecksumCapabilities::default());
                    // SAFETY: The arena allocated both fixed headers; the checks above
                    // bound the aggregate's UDP length and preserve address families.
                    let mut udp = UdpPacket::new_unchecked(&mut bytes[ip_len..]);
                    udp.set_src_port(source.port());
                    udp.set_dst_port(destination.port());
                    udp.set_len((8 + total) as u16);
                    udp.set_checksum(checksum::pseudo_header(
                        &ip.src_addr(),
                        &ip.dst_addr(),
                        IpProtocol::Udp,
                        (8 + total) as u32,
                    ));
                })
                .1,
        )
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
        let ip = IpRepr::new(
            source_ip,
            destination_ip,
            IpProtocol::Udp,
            8 + payload.len(),
            64,
        );
        if ip.buffer_len() <= self.mtu {
            let (_, bytes) = self.arena.encode(ip.buffer_len(), |bytes| {
                let (header, body) = bytes.split_at_mut(ip.header_len());
                ip.emit(header, &ChecksumCapabilities::default());
                // SAFETY: The arena allocated the IP header, UDP header and complete payload.
                udp.emit(
                    &mut UdpPacket::new_unchecked(body),
                    &source_ip,
                    &destination_ip,
                    payload.len(),
                    |bytes| bytes.copy_from_slice(payload),
                    &ChecksumCapabilities::default(),
                );
            });
            return Some(vec![bytes]);
        }

        // Checksum the header and borrowed payload separately. All fragment
        // offsets are eight-byte aligned, so only the first includes this header.
        let length = 8 + payload.len();
        let mut header = [0; 8];
        // SAFETY: The fixed storage holds a UDP header; validated payload length
        // fits its wire field. No header method below accesses the payload.
        let mut transport = UdpPacket::new_unchecked(&mut header);
        transport.set_src_port(source.port());
        transport.set_dst_port(destination.port());
        transport.set_len(length as u16);
        let sum = !checksum::combine(&[
            checksum::pseudo_header(&source_ip, &destination_ip, IpProtocol::Udp, length as u32),
            checksum::data(transport.as_ref()),
            checksum::data(payload),
        ]);
        transport.set_checksum(if sum == 0 { 0xffff } else { sum });
        if let IpRepr::Ipv4(mut repr) = ip {
            let size = (self.mtu - 20) / 8 * 8;
            let id = self.identifiers.ipv4.fetch_add(1, Ordering::Relaxed);
            let count = length.div_ceil(size);
            let mut frames = Vec::with_capacity(count);
            for index in 0..count {
                let len = (length - index * size).min(size);
                repr.payload_len = len;
                let (_, bytes) = self.arena.encode(20 + len, |bytes| {
                    // SAFETY: The frame contains the header and complete chunk;
                    // validated UDP length bounds the byte offset.
                    debug_assert!(index * size <= 0xfff8);
                    let mut ipv4 = Ipv4Packet::new_unchecked(&mut *bytes);
                    repr.emit(&mut ipv4, &ChecksumCapabilities::default());
                    ipv4.set_ident(id);
                    ipv4.set_dont_frag(false);
                    ipv4.set_more_frags(index + 1 < count);
                    ipv4.set_frag_offset((index * size) as u16);
                    ipv4.fill_checksum();
                    copy_fragment(&header, payload, index * size, &mut bytes[20..]);
                });
                frames.push(bytes);
            }
            return Some(frames);
        }

        // SAFETY: Matching address families constructed IpRepr; IPv4 returned
        // above, so only the IPv6 variant can reach this branch.
        debug_assert!(source.is_ipv6() && destination.is_ipv6());
        let IpRepr::Ipv6(mut repr) = ip else {
            unreachable!()
        };
        let size = (self.mtu - 48) / 8 * 8;
        debug_assert!(size > 0 && size.is_multiple_of(8));
        let id = self.identifiers.ipv6.fetch_add(1, Ordering::Relaxed);
        let count = length.div_ceil(size);
        let mut frames = Vec::with_capacity(count);
        for index in 0..count {
            let len = (length - index * size).min(size);
            repr.next_header = IpProtocol::Ipv6Frag;
            repr.payload_len = len + 8;
            let (_, bytes) = self.arena.encode(48 + len, |bytes| {
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
                copy_fragment(&header, payload, index * size, &mut bytes[48..]);
            });
            frames.push(bytes);
        }
        Some(frames)
    }
}

fn copy_fragment(header: &[u8; 8], payload: &[u8], offset: usize, out: &mut [u8]) {
    // SAFETY: Fragment lengths cover the validated UDP datagram exactly. MTU
    // guarantees the first fragment holds its complete eight-byte UDP header.
    debug_assert!(offset.is_multiple_of(8));
    debug_assert!(offset + out.len() <= header.len() + payload.len());
    if offset == 0 {
        let (first, rest) = out.split_at_mut(header.len());
        first.copy_from_slice(header);
        rest.copy_from_slice(&payload[..rest.len()]);
    } else {
        let start = offset - header.len();
        out.copy_from_slice(&payload[start..start + out.len()]);
    }
}

pub(crate) fn reply_flow(flow: Flow, source: SocketAddr) -> Option<(SocketAddr, SocketAddr)> {
    // Outbound reply metadata selects the wire source. It must be usable in the
    // client's address family; a domain cannot be put into an IP header.
    (source.is_ipv4() == flow.source.is_ipv4() && crate::packet::unicast(source.ip().into()))
        .then_some((source, flow.source))
}
