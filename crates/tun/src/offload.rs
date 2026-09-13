//! Normalize Linux virtio frames before the common IP validation path.
use crate::PacketIo;
use smoltcp::wire::TcpPacket;
use std::{
    collections::VecDeque,
    io::{self, IoSlice},
    sync::Mutex,
    task::{Context, Poll, ready},
};
use tun_rs::{
    AsyncDevice, VIRTIO_NET_HDR_GSO_TCPV4, VIRTIO_NET_HDR_GSO_TCPV6, VIRTIO_NET_HDR_GSO_UDP_L4,
    VIRTIO_NET_HDR_LEN, VirtioNetHdr,
};

const MAX_IP_PACKET: usize = 65575;

pub(crate) struct OffloadDevice {
    device: AsyncDevice,
    receive: Mutex<Receive>,
    send: tokio::sync::Mutex<Send>,
}

struct Send {
    gro: tun_rs::GROTable,
    buffers: Vec<Vec<u8>>,
}

struct Receive {
    frame: Vec<u8>,
    datagrams: VecDeque<Vec<u8>>,
}

impl OffloadDevice {
    pub fn new(device: AsyncDevice) -> Self {
        Self {
            device,
            send: tokio::sync::Mutex::new(Send {
                gro: tun_rs::GROTable::default(),
                buffers: Vec::new(),
            }),
            receive: Mutex::new(Receive {
                frame: vec![0; MAX_IP_PACKET + VIRTIO_NET_HDR_LEN],
                datagrams: VecDeque::new(),
            }),
        }
    }
}

impl PacketIo for OffloadDevice {
    async fn send_batch(&self, packets: &mut [Vec<u8>]) -> io::Result<()> {
        let offset = if self.device.tcp_gso() {
            VIRTIO_NET_HDR_LEN
        } else {
            0
        };
        let mut send = self.send.lock().await;
        // tun-rs only coalesces into existing capacity. Retain a bounded pool
        // across batches instead of allocating a large Vec for every IP packet.
        while send.buffers.len() < packets.len().min(64) {
            send.buffers
                .push(Vec::with_capacity(MAX_IP_PACKET + 2 * VIRTIO_NET_HDR_LEN));
        }
        // SAFETY: The pool was grown to the largest chunk this batch can use.
        debug_assert!(send.buffers.len() >= packets.len().min(64));
        let Send { gro, buffers } = &mut *send;
        for chunk in packets.chunks(64) {
            for (buffer, packet) in buffers.iter_mut().zip(chunk) {
                buffer.clear();
                buffer.resize(offset, 0);
                buffer.extend_from_slice(packet);
            }
            self.device
                .send_multiple(gro, &mut buffers[..chunk.len()], offset)
                .await?;
        }
        Ok(())
    }

    fn poll_recv(&self, cx: &mut Context<'_>, bytes: &mut [u8]) -> Poll<io::Result<usize>> {
        if !self.device.tcp_gso() {
            return self.device.poll_recv(cx, bytes);
        }
        let mut receive = self
            .receive
            .lock()
            .map_err(|_| io::Error::other("TUN receive lock poisoned"))?;
        if let Some(packet) = receive.datagrams.pop_front() {
            return Poll::Ready(copy_packet(&packet, bytes));
        }

        let len = ready!(self.device.poll_recv(cx, &mut receive.frame))?;
        // SAFETY: AsyncDevice returns the number of bytes read into frame.
        debug_assert!(len <= receive.frame.len());
        let Receive { frame, datagrams } = &mut *receive;
        Poll::Ready(normalize(&mut frame[..len], bytes, datagrams))
    }

    fn poll_send(&self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        if !self.device.tcp_gso() {
            return self.device.poll_send(cx, bytes);
        }
        // A zero virtio header describes a complete, checksummed IP packet.
        // writev adds it without allocating or copying the packet payload.
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
            if count == 0 || count > tun_rs::IDEAL_BATCH_SIZE {
                // Bound the extra UDP queue independently of the sender's GSO size.
                return Ok(0);
            }
            let size = usize::from(header.hdr_len) + usize::from(header.gso_size);
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
                Some(packet) => copy_packet(&packet, output),
                None => Ok(0),
            }
        }
        _ => Err(invalid("unsupported TUN GSO type")),
    }
}

pub(crate) struct Queues {
    devices: Vec<OffloadDevice>,
    next: std::sync::atomic::AtomicUsize,
}

impl Queues {
    pub fn new(device: AsyncDevice) -> io::Result<Self> {
        let count = std::thread::available_parallelism()?.get().min(4);
        let mut devices = Vec::with_capacity(count);
        for _ in 1..count {
            devices.push(OffloadDevice::new(device.try_clone()?));
        }
        devices.push(OffloadDevice::new(device));
        Ok(Self {
            devices,
            next: std::sync::atomic::AtomicUsize::new(0),
        })
    }
}

impl PacketIo for Queues {
    fn poll_recv(&self, cx: &mut Context<'_>, bytes: &mut [u8]) -> Poll<io::Result<usize>> {
        // SAFETY: new uses a nonzero CPU count, capped at four queues.
        debug_assert!((1..=4).contains(&self.devices.len()));
        let next = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        for index in 0..self.devices.len() {
            let device = &self.devices[next.wrapping_add(index) % self.devices.len()];
            if let Poll::Ready(result) = device.poll_recv(cx, bytes) {
                return Poll::Ready(result);
            }
        }
        Poll::Pending
    }

    fn poll_send(&self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        // SAFETY: new creates at least one queue from a nonzero CPU count.
        debug_assert!(!self.devices.is_empty());
        self.devices[0].poll_send(cx, bytes)
    }

    async fn send_batch(&self, packets: &mut [Vec<u8>]) -> io::Result<()> {
        // Keep one writer so endpoint UDP fragment identifiers stay shared.
        // SAFETY: new creates at least one queue from a nonzero CPU count.
        debug_assert!(!self.devices.is_empty());
        self.devices[0].send_batch(packets).await
    }
}
