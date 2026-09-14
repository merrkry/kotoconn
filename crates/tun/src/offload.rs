//! Normalize Linux virtio frames before the common IP validation path.
use crate::{PacketReceive, PacketSend};
use bytes::Bytes;
use kotoconn_protocol::queue::{Capacity, INITIAL_BYTES};
use smoltcp::wire::TcpPacket;
use std::{
    collections::VecDeque,
    io::{self, IoSlice},
    sync::Arc,
    task::{Context, Poll, ready},
};
use tokio::time::Instant;
use tun_rs::{
    AsyncDevice, VIRTIO_NET_HDR_GSO_TCPV4, VIRTIO_NET_HDR_GSO_TCPV6, VIRTIO_NET_HDR_GSO_UDP_L4,
    VIRTIO_NET_HDR_LEN, VirtioNetHdr,
};

const MAX_IP_PACKET: usize = 65575;

pub(crate) struct Receiver {
    device: Arc<AsyncDevice>,
    frame: Vec<u8>,
    datagrams: VecDeque<Vec<u8>>,
    capacity: Capacity,
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
            frame: vec![0; MAX_IP_PACKET + VIRTIO_NET_HDR_LEN],
            datagrams: VecDeque::new(),
            capacity: Capacity::new(INITIAL_BYTES),
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
        if !self.device.tcp_gso() {
            return self.device.poll_recv(cx, bytes);
        }
        if let Some(packet) = self.datagrams.pop_front() {
            self.capacity.complete(packet.len(), Instant::now());
            return Poll::Ready(copy_packet(&packet, bytes));
        }

        let len = ready!(self.device.poll_recv(cx, &mut self.frame))?;
        // SAFETY: AsyncDevice reports the bytes written into this frame.
        debug_assert!(len <= self.frame.len());
        Poll::Ready(normalize(
            &mut self.frame[..len],
            bytes,
            &mut self.datagrams,
            &mut self.capacity,
        ))
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

fn copy_packet(packet: &[u8], output: &mut [u8]) -> io::Result<usize> {
    let destination = output
        .get_mut(..packet.len())
        .ok_or_else(|| invalid("TUN receive buffer too small"))?;
    destination.copy_from_slice(packet);
    Ok(packet.len())
}

pub(super) fn normalize(
    frame: &mut [u8],
    output: &mut [u8],
    datagrams: &mut VecDeque<Vec<u8>>,
    capacity: &mut Capacity,
) -> io::Result<usize> {
    let mut header = VirtioNetHdr::decode(frame)?;
    // SAFETY: decode checked the complete virtio header. The remaining bytes
    // are still untrusted and go through the common IP decoder after this step.
    debug_assert!(frame.len() >= VIRTIO_NET_HDR_LEN);
    let packet = &mut frame[VIRTIO_NET_HDR_LEN..];
    if header.gso_type == 0 {
        if header.flags & 1 != 0 {
            let start = usize::from(header.csum_start);
            let at = start + usize::from(header.csum_offset);
            let checksum = packet
                .get_mut(at..at + 2)
                .ok_or_else(|| invalid("partial checksum lies outside packet"))?;
            let initial = u16::from_be_bytes([checksum[0], checksum[1]]);
            checksum.fill(0);
            // SAFETY: at >= start and the checksum field was checked above.
            debug_assert!(start <= at && at + 2 <= packet.len());
            let checksum = !tun_rs::checksum(&packet[start..], u64::from(initial));
            // Linux uses checksum offset 6 for UDP. A computed zero must be
            // encoded as all ones; a literal zero means "checksum omitted".
            let checksum = if header.csum_offset == 6 && checksum == 0 {
                0xffff
            } else {
                checksum
            };
            packet[at..at + 2].copy_from_slice(&checksum.to_be_bytes());
        }
        return copy_packet(packet, output);
    }

    let version = packet.first().ok_or_else(|| invalid("empty GSO packet"))? >> 4;
    let is_v6 = version == 6;
    if !matches!(version, 4 | 6)
        || packet.len() > if is_v6 { MAX_IP_PACKET } else { 65535 }
        || header.gso_size == 0
    {
        return Err(invalid("invalid GSO IP version, size or segment size"));
    }
    let start = usize::from(header.csum_start);
    let transport = packet
        .get(start..)
        .ok_or_else(|| invalid("invalid GSO checksum start"))?;
    match header.gso_type {
        VIRTIO_NET_HDR_GSO_TCPV4 | VIRTIO_NET_HDR_GSO_TCPV6 => {
            if is_v6 != (header.gso_type == VIRTIO_NET_HDR_GSO_TCPV6) || header.csum_offset != 16 {
                return Err(invalid("GSO TCP metadata does not match IP"));
            }
            let tcp =
                TcpPacket::new_checked(transport).map_err(|_| invalid("short GSO TCP header"))?;
            header.hdr_len = header
                .csum_start
                .checked_add(u16::from(tcp.header_len()))
                .ok_or_else(|| invalid("GSO TCP header too large"))?;
            // A TCP byte stream can consume the aggregate as one large segment.
            // tun-rs rebuilds its lengths and checksum; smoltcp still validates
            // the resulting packet and owns sequence, ACK and window handling.
            header.gso_size = u16::MAX;
            let mut sizes = [0];
            tun_rs::gso_split(packet, header, &mut [output], &mut sizes, 0, is_v6)?;
            Ok(sizes[0])
        }
        VIRTIO_NET_HDR_GSO_UDP_L4 => {
            if header.csum_offset != 6 || transport.len() < 8 {
                return Err(invalid("short GSO UDP header"));
            }
            header.hdr_len = header
                .csum_start
                .checked_add(8)
                .ok_or_else(|| invalid("GSO UDP header too large"))?;
            let count = (transport.len() - 8).div_ceil(usize::from(header.gso_size));
            if count == 0 {
                return Ok(0);
            }
            let size = usize::from(header.hdr_len) + usize::from(header.gso_size);
            // The previous aggregate is drained before reading another frame.
            // Charge duplicated headers as well as payload; GSO segment count
            // is not an independent admission quota.
            debug_assert!(datagrams.is_empty());
            let cost = count.saturating_mul(size + std::mem::size_of::<Vec<u8>>());
            if count > 1 && cost > capacity.target(Instant::now()) {
                return Ok(0);
            }
            let mut packets = vec![vec![0; size]; count];
            let mut sizes = vec![0; count];
            let count = tun_rs::gso_split(packet, header, &mut packets, &mut sizes, 0, is_v6)?;
            for (mut packet, size) in packets.into_iter().zip(sizes).take(count) {
                packet.truncate(size);
                // tun-rs 2.8.9 emits the computed value directly. Preserve UDP's
                // zero-checksum encoding, including for each IPv6 datagram.
                let checksum = packet
                    .get_mut(start + 6..start + 8)
                    .ok_or_else(|| invalid("short segmented UDP packet"))?;
                if checksum == [0, 0] {
                    checksum.fill(0xff);
                }
                datagrams.push_back(packet);
            }
            match datagrams.pop_front() {
                Some(packet) => {
                    capacity.complete(packet.len(), Instant::now());
                    copy_packet(&packet, output)
                }
                None => Ok(0),
            }
        }
        _ => Err(invalid("unsupported TUN GSO type")),
    }
}
