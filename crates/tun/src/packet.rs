//! Validate and normalize IP before allocating transport state. Reassembly is
//! shared by TCP and UDP, including fragmented initial SYNs.
use smoltcp::{phy::ChecksumCapabilities, wire::*};
use std::{borrow::Cow, collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

const MAX_DATAGRAMS: usize = 64;

const REASSEMBLY_LIFETIME: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Flow {
    pub source: SocketAddr,
    pub destination: SocketAddr,
}

pub(crate) struct Packet<'a> {
    pub ip: IpRepr,
    pub payload: Cow<'a, [u8]>,
}

impl Packet<'_> {
    pub fn into_owned(self) -> Packet<'static> {
        Packet {
            ip: self.ip,
            payload: Cow::Owned(self.payload.into_owned()),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        // SAFETY: Decoder normalizes the IP payload length after reassembly;
        // outbound encoders construct it from their transport buffer length.
        // The allocation must fit both the emitted header and payload copy.
        debug_assert_eq!(
            self.ip.buffer_len(),
            self.ip.header_len() + self.payload.len()
        );
        let mut data = vec![0; self.ip.buffer_len()];
        self.ip.emit(&mut data, &ChecksumCapabilities::default());
        data[self.ip.header_len()..].copy_from_slice(&self.payload);
        data
    }

    pub fn tcp(&self) -> Option<(Flow, TcpRepr<'_>)> {
        let packet = TcpPacket::new_checked(&self.payload[..]).ok()?;
        let repr = TcpRepr::parse(
            &packet,
            &self.ip.src_addr(),
            &self.ip.dst_addr(),
            &ChecksumCapabilities::default(),
        )
        .ok()?;
        Some((self.flow(repr.src_port, repr.dst_port)?, repr))
    }

    pub fn udp(&self) -> Option<(Flow, &[u8])> {
        let packet = UdpPacket::new_checked(&self.payload[..]).ok()?;
        // smoltcp 0.14's verify_checksum accepts zero for both address families.
        // RFC 8200 section 8.1 forbids it for ordinary UDP over IPv6.
        if matches!(self.ip, IpRepr::Ipv6(_)) && packet.checksum() == 0 {
            return None;
        }
        UdpRepr::parse(
            &packet,
            &self.ip.src_addr(),
            &self.ip.dst_addr(),
            &ChecksumCapabilities::default(),
        )
        .ok()?;
        // The IP payload may contain padding, but the UDP length defines the datagram.
        Some((
            self.flow(packet.src_port(), packet.dst_port())?,
            packet.payload(),
        ))
    }

    fn flow(&self, source: u16, destination: u16) -> Option<Flow> {
        (destination != 0).then(|| Flow {
            source: SocketAddr::new(self.ip.src_addr().into(), source),
            destination: SocketAddr::new(self.ip.dst_addr().into(), destination),
        })
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct FragmentKey {
    source: IpAddress,
    destination: IpAddress,
    id: u32,
}

#[derive(Clone, Copy)]
struct Fragment {
    id: u32,
    offset: usize,
    more: bool,
    prefix_len: usize,
}

struct Assembly {
    _permit: OwnedSemaphorePermit,
    expires: Instant,
    ranges: smoltcp::storage::Assembler,
    data: Vec<u8>,
    protocol: IpProtocol,
    prefix_len: usize,
    total: Option<usize>,
    poisoned: bool,
}

impl Assembly {
    fn poison(&mut self) {
        self.ranges.clear();
        self.data.clear();
        self.poisoned = true;
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum RouteKey {
    Flow(u8, Flow),
    Ipv4Fragment {
        source: Ipv4Address,
        destination: Ipv4Address,
        protocol: u8,
        id: u32,
    },
    Ipv6Fragment {
        source: Ipv6Address,
        destination: Ipv6Address,
        id: u32,
    },
}

pub(crate) struct Parsed<'a> {
    bytes: &'a [u8],
    ip: IpRepr,
    payload: &'a [u8],
    fragment: Option<Fragment>,
}

impl<'a> Parsed<'a> {
    pub fn route(&self) -> Option<RouteKey> {
        if let Some(fragment) = self.fragment {
            return Some(match self.ip {
                IpRepr::Ipv4(ip) => RouteKey::Ipv4Fragment {
                    source: ip.src_addr,
                    destination: ip.dst_addr,
                    protocol: u8::from(ip.next_header),
                    id: fragment.id,
                },
                IpRepr::Ipv6(ip) => RouteKey::Ipv6Fragment {
                    source: ip.src_addr,
                    destination: ip.dst_addr,
                    id: fragment.id,
                },
            });
        }
        if !matches!(self.ip.next_header(), IpProtocol::Tcp | IpProtocol::Udp) {
            return None;
        }
        let ports = self.payload.get(..4)?;
        // SAFETY: get above established all four bytes of the transport ports.
        debug_assert!(ports.len() == 4);
        let source = u16::from_be_bytes([ports[0], ports[1]]);
        let destination = u16::from_be_bytes([ports[2], ports[3]]);
        (destination != 0).then(|| {
            RouteKey::Flow(
                u8::from(self.ip.next_header()),
                Flow {
                    source: SocketAddr::new(self.ip.src_addr().into(), source),
                    destination: SocketAddr::new(self.ip.dst_addr().into(), destination),
                },
            )
        })
    }

    pub fn decode(self, decoder: &mut Decoder, now: Instant) -> Option<Packet<'a>> {
        if self.fragment.is_some() && matches!(self.ip, IpRepr::Ipv4(_)) {
            let assembled = decoder.ipv4.process(self.bytes, now)?;
            return decoder.decode(&assembled, now).map(Packet::into_owned);
        }
        let mut ip = self.ip;
        let payload = if let Some(fragment) = self.fragment {
            let key = FragmentKey {
                source: ip.src_addr(),
                destination: ip.dst_addr(),
                id: fragment.id,
            };
            Cow::Owned(decoder.reassemble(key, ip.next_header(), fragment, self.payload, now)?)
        } else {
            Cow::Borrowed(self.payload)
        };
        ip.set_payload_len(payload.len());
        Some(Packet { ip, payload })
    }
}

pub(crate) fn parse(data: &[u8]) -> Option<Parsed<'_>> {
    // SAFETY: IpVersion::of_packet reads the first byte without checking length.
    data.first()?;
    let (ip, payload, fragment) = match IpVersion::of_packet(data).ok()? {
        IpVersion::Ipv4 => {
            let packet = Ipv4Packet::new_checked(data).ok()?;
            let repr = Ipv4Repr::parse(&packet, &ChecksumCapabilities::default()).ok()?;
            // SAFETY: Successful checked parsing establishes the fixed
            // header and the complete options slice within this buffer.
            debug_assert!((20..=data.len()).contains(&usize::from(packet.header_len())));
            // Source routing is deliberately outside this unicast proxy's policy.
            if !ipv4_options(&data[20..packet.header_len() as usize]) {
                return None;
            }
            let fragment = (packet.more_frags() || packet.frag_offset() != 0).then_some((
                u32::from(packet.ident()),
                usize::from(packet.frag_offset()),
                packet.more_frags(),
            ));
            if let Some((_, offset, more)) = fragment
                && (packet.payload().is_empty()
                    || offset + packet.payload().len() > 65515
                    || (more && !packet.payload().len().is_multiple_of(8)))
            {
                return None;
            }
            (
                IpRepr::Ipv4(repr),
                packet.payload(),
                fragment.map(|(id, offset, more)| Fragment {
                    id,
                    offset,
                    more,
                    prefix_len: 0,
                }),
            )
        }
        IpVersion::Ipv6 => {
            let packet = Ipv6Packet::new_checked(data).ok()?;
            let mut repr = Ipv6Repr::parse(&packet).ok()?;
            let (protocol, payload, fragment) =
                ipv6_extensions(repr.next_header, packet.payload())?;
            repr.next_header = protocol;
            repr.payload_len = payload.len();
            (IpRepr::Ipv6(repr), payload, fragment)
        }
    };

    if !unicast(ip.src_addr()) || !unicast(ip.dst_addr()) || ip.hop_limit() == 0 {
        return None;
    }
    Some(Parsed {
        bytes: data,
        ip,
        payload,
        fragment,
    })
}

#[derive(Clone)]
pub(crate) struct ReassemblyLimits {
    ipv4: Arc<Semaphore>,
    ipv6: Arc<Semaphore>,
}

impl Default for ReassemblyLimits {
    fn default() -> Self {
        Self {
            ipv4: Arc::new(Semaphore::new(smoltcp::config::REASSEMBLY_BUFFER_COUNT)),
            ipv6: Arc::new(Semaphore::new(MAX_DATAGRAMS)),
        }
    }
}

pub(crate) struct Decoder {
    fragments: HashMap<FragmentKey, Assembly>,
    slots: Arc<Semaphore>,
    ipv4: crate::reassembly::Ipv4Reassembly,
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new(ReassemblyLimits::default())
    }
}

impl Decoder {
    pub fn new(limits: ReassemblyLimits) -> Self {
        Self {
            fragments: HashMap::new(),
            slots: limits.ipv6,
            ipv4: crate::reassembly::Ipv4Reassembly::new(limits.ipv4),
        }
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.fragments
            .values()
            .map(|a| a.expires)
            .chain(self.ipv4.deadline())
            .min()
    }

    pub fn expire(&mut self, now: Instant) {
        self.fragments.retain(|_, a| a.expires > now);
        self.ipv4.expire(now);
    }

    pub fn decode<'a>(&mut self, data: &'a [u8], now: Instant) -> Option<Packet<'a>> {
        parse(data)?.decode(self, now)
    }

    fn reassemble(
        &mut self,
        key: FragmentKey,
        protocol: IpProtocol,
        fragment: Fragment,
        data: &[u8],
        now: Instant,
    ) -> Option<Vec<u8>> {
        let Fragment {
            offset,
            more,
            prefix_len,
            ..
        } = fragment;
        // SAFETY: ipv6_extensions derives these values from a checked IPv6
        // payload length and the fragment header's 13-bit offset field.
        debug_assert!(prefix_len <= 65535);
        debug_assert!(offset <= 0xfff8 && offset.is_multiple_of(8));
        self.expire(now);

        if let std::collections::hash_map::Entry::Vacant(entry) = self.fragments.entry(key) {
            let permit = self.slots.clone().try_acquire_owned().ok()?;
            entry.insert(Assembly {
                _permit: permit,
                expires: now + REASSEMBLY_LIFETIME,
                ranges: smoltcp::storage::Assembler::new(),
                data: Vec::new(),
                protocol,
                prefix_len,
                total: None,
                poisoned: false,
            });
        }
        let assembly = self.fragments.get_mut(&key)?;
        if assembly.poisoned {
            return None;
        }

        let end = offset + data.len();
        if data.is_empty()
            || end > 65535 - prefix_len
            || prefix_len != assembly.prefix_len
            || (offset == 0 && !complete_transport_header(protocol, data))
            || protocol != assembly.protocol
            || (more && !data.len().is_multiple_of(8))
            || assembly
                .total
                .is_some_and(|total| end > total || (!more && end != total))
            || (!more && assembly.data.len() > end)
            || assembly
                .ranges
                .iter_data()
                .any(|(start, stop)| start < end && stop > offset)
        {
            // RFC 5722: poison the whole IPv6 datagram until its original expiry.
            assembly.poison();
            return None;
        }
        if assembly.ranges.add(offset, data.len()).is_err() {
            assembly.poison();
            return None;
        }
        if !more {
            assembly.total = Some(end);
        }

        assembly.data.resize(assembly.data.len().max(end), 0);
        // SAFETY: The validated fragment fits the resized buffer, and end was
        // computed from this offset and this exact fragment's length.
        debug_assert!(offset <= end && end <= assembly.data.len());
        debug_assert_eq!(end - offset, data.len());
        assembly.data[offset..end].copy_from_slice(data);
        if assembly.total != Some(assembly.ranges.peek_front()) {
            return None;
        }
        Some(self.fragments.remove(&key)?.data)
    }
}

pub(crate) fn unicast(address: IpAddress) -> bool {
    match address {
        IpAddress::Ipv4(ip) => {
            !ip.is_unspecified()
                && !ip.is_multicast()
                && !ip.is_broadcast()
                && ip.octets()[0] != 0
                && ip.octets()[0] < 240
        }
        IpAddress::Ipv6(ip) => {
            !ip.is_unspecified() && !ip.is_multicast() && ip.to_ipv4_mapped().is_none()
        }
    }
}

fn ipv4_options(mut options: &[u8]) -> bool {
    while let Some(&kind) = options.first() {
        match kind {
            0 => return true,
            1 => options = &options[1..],
            // Loose and strict source route options.
            131 | 137 => return false,
            _ => {
                let Some(&len) = options.get(1) else {
                    return false;
                };
                if len < 2 || usize::from(len) > options.len() {
                    return false;
                }
                options = &options[usize::from(len)..];
            }
        }
    }
    true
}

fn ipv6_extensions(
    mut protocol: IpProtocol,
    mut payload: &[u8],
) -> Option<(IpProtocol, &[u8], Option<Fragment>)> {
    let initial_len = payload.len();

    // A finite chain bounds CPU even for tiny extension headers. Unsupported
    // routing/security extensions are filtered before transport admission.
    for index in 0..8 {
        match protocol {
            IpProtocol::HopByHop | IpProtocol::Ipv6Opts => {
                if protocol == IpProtocol::HopByHop && index != 0 {
                    return None;
                }
                let header = Ipv6ExtHeader::new_checked(payload).ok()?;
                let repr = Ipv6ExtHeaderRepr::parse(&header).ok()?;
                for option in Ipv6OptionsIterator::new(repr.data) {
                    if let Ipv6OptionRepr::Unknown { type_, .. } = option.ok()?
                        && Ipv6OptionFailureType::from(type_) != Ipv6OptionFailureType::Skip
                    {
                        return None;
                    }
                }
                protocol = repr.next_header;
                // SAFETY: The checked extension header and parsed option data
                // both borrow this payload and include the complete header.
                debug_assert!(repr.header_len() + repr.data.len() <= payload.len());
                payload = &payload[repr.header_len() + repr.data.len()..];
            }
            IpProtocol::Ipv6Frag => {
                let header = payload.get(..8)?;
                // SAFETY: get above established all eight fragment-header
                // bytes, including the fields after the first two bytes.
                debug_assert!(payload.len() >= 8);
                let fragment = Ipv6FragmentHeader::new_checked(&header[2..]).ok()?;
                let fragment = Ipv6FragmentRepr::parse(&fragment).ok()?;
                protocol = IpProtocol::from(header[0]);
                // Require an upper-layer header directly after the fragment header.
                if !matches!(
                    protocol,
                    IpProtocol::Tcp | IpProtocol::Udp | IpProtocol::Icmpv6
                ) {
                    return None;
                }
                let offset = usize::from(fragment.frag_offset) * 8;
                let more = fragment.more_frags;
                let id = fragment.ident;
                // RFC 6946 atomic fragments never interact with queued fragments.
                return Some((
                    protocol,
                    &payload[8..],
                    (offset != 0 || more).then_some(Fragment {
                        id,
                        offset,
                        more,
                        prefix_len: initial_len - payload.len(),
                    }),
                ));
            }
            IpProtocol::Tcp | IpProtocol::Udp | IpProtocol::Icmpv6 => {
                return Some((protocol, payload, None));
            }
            _ => return None,
        }
    }
    None
}

// RFC 7112: the first IPv6 fragment must contain the complete transport header.
// TCP options belong to that header; UDP/ICMP need their fixed eight bytes.
fn complete_transport_header(protocol: IpProtocol, data: &[u8]) -> bool {
    match protocol {
        IpProtocol::Tcp => TcpPacket::new_checked(data).is_ok(),
        IpProtocol::Udp | IpProtocol::Icmpv6 => data.len() >= 8,
        _ => false,
    }
}
