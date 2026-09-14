//! Normalize Linux virtio frames before the common IP validation path.
use crate::{PacketReceive, PacketSend, ReceiveBuffer, Received};
use bytes::Bytes;
use smoltcp::wire::{IpProtocol, Ipv4Packet, Ipv6Packet, TcpPacket, UdpPacket};
use std::{
    io::{self, IoSlice},
    sync::Arc,
    task::{Context, Poll, ready},
};
use tun_rs::{
    AsyncDevice, VIRTIO_NET_HDR_GSO_TCPV4, VIRTIO_NET_HDR_GSO_TCPV6, VIRTIO_NET_HDR_GSO_UDP_L4,
    VIRTIO_NET_HDR_LEN, VirtioNetHdr,
};

const MAX_IP_PACKET: usize = 65575;

pub(crate) struct Receiver {
    device: Arc<AsyncDevice>,
}

pub(crate) struct Sender {
    device: Arc<AsyncDevice>,
}

pub(super) fn tcp_gso_header(
    packet: &[u8],
    segment_size: u16,
) -> io::Result<[u8; VIRTIO_NET_HDR_LEN]> {
    let (ip_len, gso_type) = match packet.first().map(|b| b >> 4) {
        Some(4) => (usize::from(packet[0] & 15) * 4, VIRTIO_NET_HDR_GSO_TCPV4),
        Some(6) => (40, VIRTIO_NET_HDR_GSO_TCPV6),
        _ => return Err(invalid("invalid GSO IP version")),
    };
    let payload = packet
        .get(ip_len..)
        .ok_or_else(|| invalid("short GSO IP header"))?;
    let tcp = TcpPacket::new_checked(payload).map_err(|_| invalid("invalid GSO TCP header"))?;
    if segment_size == 0 {
        return Err(invalid("zero GSO segment size"));
    }
    let mut bytes = [0; VIRTIO_NET_HDR_LEN];
    VirtioNetHdr {
        flags: 1, // VIRTIO_NET_HDR_F_NEEDS_CSUM
        gso_type,
        hdr_len: (ip_len + usize::from(tcp.header_len())) as u16,
        gso_size: segment_size,
        csum_start: ip_len as u16,
        csum_offset: 16,
    }
    .encode(&mut bytes)?;
    Ok(bytes)
}

fn split(device: AsyncDevice) -> (Receiver, Sender) {
    let device = Arc::new(device);
    (
        Receiver {
            device: device.clone(),
        },
        Sender { device },
    )
}

pub(crate) fn queues(device: AsyncDevice) -> io::Result<Vec<(Receiver, Sender)>> {
    let count = match std::thread::available_parallelism() {
        Ok(count) => count.get(),
        Err(error) => {
            tracing::warn!(%error, "failed to determine available CPUs; using one TUN queue");
            1
        }
    };
    let mut queues = Vec::with_capacity(count);
    // Keep the original descriptor at index zero, matching Linux queue indices.
    for _ in 1..count {
        queues.push(split(device.try_clone()?));
    }
    queues.insert(0, split(device));
    Ok(queues)
}

impl PacketReceive for Receiver {
    fn poll_recv(&mut self, cx: &mut Context<'_>, bytes: &mut [u8]) -> Poll<io::Result<usize>> {
        let frame = ready!(self.poll_frame(cx, &mut ReceiveBuffer::default()))?;
        if frame.udp_segment_size.is_some() || frame.checksum_verified {
            return Poll::Ready(Err(invalid("offloaded frames require poll_frame")));
        }
        let Some(out) = bytes.get_mut(..frame.bytes.len()) else {
            return Poll::Ready(Err(invalid("short packet buffer")));
        };
        out.copy_from_slice(&frame.bytes);
        Poll::Ready(Ok(out.len()))
    }

    fn poll_frame(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut ReceiveBuffer,
    ) -> Poll<io::Result<Received>> {
        ready!(self.device.poll_readable(cx))?;
        let offload = self.device.tcp_gso();
        let length = MAX_IP_PACKET + if offload { VIRTIO_NET_HDR_LEN } else { 0 };
        let lease = buffer
            .lease
            .get_or_insert_with(|| buffer.pool.acquire(length));
        let len = ready!(self.device.poll_recv(cx, lease.as_mut()))?;
        let metadata = if offload {
            normalize(&mut lease.as_mut()[..len])?
        } else {
            (false, None)
        };
        // SAFETY: poll_recv initialized this prefix and the lease is installed.
        let lease = buffer.lease.take().expect("receive lease");
        let bytes = lease.publish(len);
        Poll::Ready(Ok(Received {
            bytes: if offload {
                bytes.slice(VIRTIO_NET_HDR_LEN..)
            } else {
                bytes
            },
            checksum_verified: metadata.0,
            udp_segment_size: metadata.1,
        }))
    }
}

impl PacketSend for Sender {
    fn tcp_gso(&self) -> bool {
        self.device.tcp_gso()
    }

    fn udp_gso(&self) -> bool {
        self.device.udp_gso()
    }

    async fn send_udp_segments(
        &mut self,
        header: &[u8],
        payload: &[Bytes],
        size: u16,
    ) -> io::Result<()> {
        let ip_len = match header.first().map(|b| b >> 4) {
            Some(4) => 20,
            Some(6) => 40,
            _ => return Err(invalid("invalid UDP IP version")),
        };
        let mut virtio = [0; VIRTIO_NET_HDR_LEN];
        VirtioNetHdr {
            flags: 1,
            gso_type: if payload.len() > 1 {
                VIRTIO_NET_HDR_GSO_UDP_L4
            } else {
                0
            },
            hdr_len: ip_len + 8,
            gso_size: if payload.len() > 1 { size } else { 0 },
            csum_start: ip_len,
            csum_offset: 6,
        }
        .encode(&mut virtio)?;
        send_vectors(&self.device, &virtio, header, payload).await
    }

    async fn send_tcp_gso(&mut self, packet: &[u8], segment_size: u16) -> io::Result<()> {
        let header = tcp_gso_header(packet, segment_size)?;
        let len = loop {
            std::future::poll_fn(|cx| self.device.poll_writable(cx)).await?;
            match self
                .device
                .try_send_vectored(&[IoSlice::new(&header), IoSlice::new(packet)])
            {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                result => break result?,
            }
        };
        if len != header.len() + packet.len() {
            return Err(invalid("partial TUN GSO write"));
        }
        Ok(())
    }

    async fn send_tcp_segments(
        &mut self,
        header: &[u8],
        payload: &[Bytes],
        segment_size: u16,
    ) -> io::Result<()> {
        let virtio = tcp_gso_header(header, segment_size)?;
        send_vectors(&self.device, &virtio, header, payload).await
    }

    fn poll_send(&mut self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        if !self.device.tcp_gso() {
            return self.device.poll_send(cx, bytes);
        }
        let header = [0; VIRTIO_NET_HDR_LEN];
        loop {
            ready!(self.device.poll_writable(cx))?;
            match self
                .device
                .try_send_vectored(&[IoSlice::new(&header), IoSlice::new(bytes)])
            {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                result => {
                    return Poll::Ready(result.and_then(|len| {
                        len.checked_sub(VIRTIO_NET_HDR_LEN)
                            .ok_or_else(|| invalid("short virtio write"))
                    }));
                }
            }
        }
    }
}

async fn send_vectors(
    device: &AsyncDevice,
    virtio: &[u8],
    header: &[u8],
    payload: &[Bytes],
) -> io::Result<()> {
    // TCP bounds its descriptor count at 64; the endpoint does the same for UDP.
    debug_assert!(payload.len() <= 64);
    let mut vectors = [IoSlice::new(&[]); 66];
    vectors[0] = IoSlice::new(virtio);
    vectors[1] = IoSlice::new(header);
    for (out, part) in vectors[2..].iter_mut().zip(payload) {
        *out = IoSlice::new(part);
    }
    let vectors = &vectors[..payload.len() + 2];
    loop {
        std::future::poll_fn(|cx| device.poll_writable(cx)).await?;
        match device.try_send_vectored(vectors) {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            result => {
                if result? != vectors.iter().map(|v| v.len()).sum::<usize>() {
                    return Err(invalid("partial TUN offload write"));
                }
                return Ok(());
            }
        }
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Normalize lengths in place. Linux has already validated or scheduled the
/// transport checksum; retain that fact instead of computing and verifying it.
/// Structural validation still runs here and in the common decoder.
pub(super) fn normalize(frame: &mut [u8]) -> io::Result<(bool, Option<u16>)> {
    let header = VirtioNetHdr::decode(frame)?;
    let packet = &mut frame[VIRTIO_NET_HDR_LEN..];
    let gso = header.gso_type != 0;
    let verified = header.flags & 3 != 0;
    if !gso && !verified {
        return Ok((false, None));
    }
    if gso && (header.gso_size == 0 || !verified) {
        return Err(invalid("invalid GSO checksum or segment size"));
    }
    let is_v6 = match packet.first().map(|b| b >> 4) {
        Some(4) => false,
        Some(6) => true,
        _ => return Err(invalid("invalid offload IP version")),
    };
    if gso {
        if is_v6 {
            let length = packet
                .len()
                .checked_sub(40)
                .filter(|n| *n <= 65535)
                .ok_or_else(|| invalid("invalid GSO IPv6 size"))?;
            // SAFETY: The length check establishes the fixed IPv6 header.
            Ipv6Packet::new_unchecked(&mut *packet).set_payload_len(length as u16);
        } else {
            let length =
                u16::try_from(packet.len()).map_err(|_| invalid("invalid GSO IPv4 size"))?;
            let mut ip = Ipv4Packet::new_checked(&mut *packet)
                .map_err(|_| invalid("short GSO IPv4 header"))?;
            ip.set_total_len(length);
            ip.fill_checksum();
        }
    }
    let (protocol, start) = crate::packet::offload_transport(packet)
        .ok_or_else(|| invalid("invalid offload IP header or fragmented frame"))?;
    let expected_offset = match protocol {
        IpProtocol::Tcp => 16,
        IpProtocol::Udp => 6,
        _ => return Err(invalid("unsupported offload transport")),
    };
    if (gso || header.flags & 1 != 0)
        && (usize::from(header.csum_start) != start || header.csum_offset != expected_offset)
    {
        return Err(invalid(
            "offload checksum metadata does not match transport",
        ));
    }
    if protocol == IpProtocol::Tcp {
        TcpPacket::new_checked(&packet[start..])
            .map_err(|_| invalid("short offload TCP header"))?;
        if gso
            && header.gso_type
                != if is_v6 {
                    VIRTIO_NET_HDR_GSO_TCPV6
                } else {
                    VIRTIO_NET_HDR_GSO_TCPV4
                }
        {
            return Err(invalid("GSO type does not match TCP"));
        }
        return Ok((true, None));
    }
    let length =
        u16::try_from(packet.len() - start).map_err(|_| invalid("invalid offload UDP size"))?;
    let mut udp = UdpPacket::new_checked(&mut packet[start..])
        .map_err(|_| invalid("short offload UDP header"))?;
    if gso {
        if header.gso_type != VIRTIO_NET_HDR_GSO_UDP_L4 {
            return Err(invalid("GSO type does not match UDP"));
        }
        udp.set_len(length);
    }
    Ok((true, gso.then_some(header.gso_size)))
}
