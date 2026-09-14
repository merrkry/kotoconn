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
    gro: tun_rs::GROTable,
    buffers: Vec<Vec<u8>>,
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
        Sender {
            device,
            gro: tun_rs::GROTable::default(),
            buffers: Vec::new(),
        },
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
        let bytes = lease.freeze(len);
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
        let mut vectors = Vec::with_capacity(payload.len() + 2);
        vectors.push(IoSlice::new(&virtio));
        vectors.push(IoSlice::new(header));
        vectors.extend(payload.iter().map(|part| IoSlice::new(part)));
        loop {
            std::future::poll_fn(|cx| self.device.poll_writable(cx)).await?;
            match self.device.try_send_vectored(&vectors) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                result => {
                    let n = result?;
                    if n != vectors.iter().map(|v| v.len()).sum::<usize>() {
                        return Err(invalid("partial TUN GSO write"));
                    }
                    return Ok(());
                }
            }
        }
    }

    async fn send_batch(&mut self, packets: &[Bytes]) -> io::Result<()> {
        let offset = if self.device.tcp_gso() {
            VIRTIO_NET_HDR_LEN
        } else {
            0
        };
        // GRO needs room to combine complete IP packets. This reusable scratch
        // pool belongs to this writer, independently of queued packet storage.
        while self.buffers.len() < packets.len().min(64) {
            self.buffers
                .push(Vec::with_capacity(MAX_IP_PACKET + 2 * VIRTIO_NET_HDR_LEN));
        }

        // SAFETY: The pool was grown to hold each bounded batch.
        debug_assert!(self.buffers.len() >= packets.len().min(64));
        for chunk in packets.chunks(64) {
            for (buffer, packet) in self.buffers.iter_mut().zip(chunk) {
                buffer.clear();
                buffer.resize(offset, 0);
                buffer.extend_from_slice(packet);
            }
            self.device
                .send_multiple(&mut self.gro, &mut self.buffers[..chunk.len()], offset)
                .await?;
        }
        Ok(())
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
