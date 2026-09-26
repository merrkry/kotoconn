use super::*;
use packet::{Decoder, Flow, Packet};
use smoltcp::{phy::ChecksumCapabilities, wire::*};
use std::{io, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

pub(super) fn flow(ipv6: bool) -> Flow {
    Flow {
        source: if ipv6 {
            "[fd00::2]:12345"
        } else {
            "192.0.2.2:12345"
        }
        .parse()
        .unwrap(),
        destination: if ipv6 {
            "[2001:db8::1]:443"
        } else {
            "198.51.100.1:443"
        }
        .parse()
        .unwrap(),
    }
}

pub(super) fn segment(
    flow: Flow,
    seq: i32,
    ack: Option<i32>,
    control: TcpControl,
    payload: &[u8],
) -> tcp::QueuedPacket {
    let repr = TcpRepr {
        src_port: flow.source.port(),
        dst_port: flow.destination.port(),
        control,
        seq_number: TcpSeqNumber(seq),
        ack_number: ack.map(TcpSeqNumber),
        window_len: 65535,
        window_scale: None,
        max_seg_size: Some(1220),
        sack_permitted: false,
        sack_ranges: [None; 3],
        timestamp: None,
        payload,
    };
    let ip = IpRepr::new(
        flow.source.ip().into(),
        flow.destination.ip().into(),
        IpProtocol::Tcp,
        repr.buffer_len(),
        64,
    );
    let mut payload = vec![0; repr.buffer_len()];
    repr.emit(
        &mut TcpPacket::new_unchecked(&mut payload),
        &ip.src_addr(),
        &ip.dst_addr(),
        &ChecksumCapabilities::default(),
    );
    tcp::QueuedPacket {
        bytes: Packet {
            ip,
            payload: payload.into(),
        }
        .encode(),
    }
}

pub(super) fn decoded(bytes: &[u8]) -> Packet<'static> {
    Decoder::default()
        .decode(bytes, Instant::now())
        .unwrap()
        .into_owned()
}

#[cfg(target_os = "linux")]
#[test]
fn gso_tcp_preserves_aggregate_bytes_sequence_and_flags_in_both_families() {
    for ipv6 in [false, true] {
        let payload: Vec<_> = (0..60000).map(|i| (i % 251) as u8).collect();
        let packet = segment(flow(ipv6), 100, Some(200), TcpControl::Psh, &payload).bytes;
        let header = tun_rs::VirtioNetHdr {
            flags: 1,
            gso_type: if ipv6 {
                tun_rs::VIRTIO_NET_HDR_GSO_TCPV6
            } else {
                tun_rs::VIRTIO_NET_HDR_GSO_TCPV4
            },
            hdr_len: 0, // Linux forwarding may report an unusable hdr_len.
            gso_size: 1220,
            csum_start: if ipv6 { 40 } else { 20 },
            csum_offset: 16,
        };
        let mut frame = vec![0; tun_rs::VIRTIO_NET_HDR_LEN];
        header.encode(&mut frame).unwrap();
        frame.extend_from_slice(&packet);
        let (verified, segments) = offload::normalize(&mut frame).unwrap();
        let packet = decoded(&frame[tun_rs::VIRTIO_NET_HDR_LEN..]);
        let (actual_flow, tcp) = packet.tcp_with_checksum(verified).unwrap();
        assert_eq!(actual_flow, flow(ipv6));
        assert_eq!(tcp.seq_number, TcpSeqNumber(100));
        assert_eq!(tcp.ack_number, Some(TcpSeqNumber(200)));
        assert_eq!(tcp.control, TcpControl::Psh);
        assert_eq!(tcp.payload, payload);
        assert!(segments.is_none());
    }
}

#[cfg(target_os = "linux")]
#[test]
fn gso_udp_preserves_payload_and_segment_size_in_both_families() {
    for (ipv6, segment_size) in [(false, 1000), (true, 1000), (false, 8), (true, 8)] {
        let flow = flow(ipv6);
        let payload: Vec<_> = (0..2501).map(|i| (i % 251) as u8).collect();
        let packet = udp::Encoder::new(65535)
            .encode(flow.source, flow.destination, &payload)
            .unwrap()
            .remove(0)
            .to_vec();
        let header = tun_rs::VirtioNetHdr {
            flags: 1,
            gso_type: tun_rs::VIRTIO_NET_HDR_GSO_UDP_L4,
            hdr_len: 0,
            gso_size: segment_size as u16,
            csum_start: if ipv6 { 40 } else { 20 },
            csum_offset: 6,
        };
        let mut frame = vec![0; tun_rs::VIRTIO_NET_HDR_LEN];
        header.encode(&mut frame).unwrap();
        frame.extend_from_slice(&packet);
        let (verified, size) = offload::normalize(&mut frame).unwrap();
        let packet = decoded(&frame[tun_rs::VIRTIO_NET_HDR_LEN..]);
        let (actual_flow, actual) = packet.udp_with_checksum(verified).unwrap();
        assert_eq!(actual_flow, flow);
        assert_eq!(size, Some(segment_size as u16));
        assert_eq!(actual, payload);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn udp_offload_encodes_a_computed_zero_checksum_as_all_ones() {
    for ipv6 in [false, true] {
        let flow = flow(ipv6);
        let start = if ipv6 { 40 } else { 20 };
        let mut encoder = udp::Encoder::new(65535);
        let seed = encoder
            .encode(flow.source, flow.destination, &[0; 2])
            .unwrap()
            .remove(0)
            .to_vec();
        // Adding this word to the payload makes the computed checksum zero.
        let payload = UdpPacket::new_checked(&seed[start..])
            .unwrap()
            .checksum()
            .to_be_bytes();
        for gso in [false, true] {
            let data = if gso {
                payload.repeat(2)
            } else {
                payload.to_vec()
            };
            let mut packet = encoder
                .encode(flow.source, flow.destination, &data)
                .unwrap()
                .remove(0)
                .to_vec();
            if !gso {
                let mut udp = UdpPacket::new_checked(&mut packet[start..]).unwrap();
                assert_eq!(udp.checksum(), 0xffff);
                udp.set_checksum(smoltcp::wire::checksum::pseudo_header(
                    &flow.source.ip().into(),
                    &flow.destination.ip().into(),
                    IpProtocol::Udp,
                    10,
                ));
            }
            let header = tun_rs::VirtioNetHdr {
                flags: 1,
                gso_type: if gso {
                    tun_rs::VIRTIO_NET_HDR_GSO_UDP_L4
                } else {
                    0
                },
                hdr_len: 0,
                gso_size: if gso { 2 } else { 0 },
                csum_start: start as u16,
                csum_offset: 6,
            };
            let mut frame = vec![0; tun_rs::VIRTIO_NET_HDR_LEN];
            header.encode(&mut frame).unwrap();
            frame.extend_from_slice(&packet);
            let (verified, size) = offload::normalize(&mut frame).unwrap();
            let packet = decoded(&frame[tun_rs::VIRTIO_NET_HDR_LEN..]);
            let (_, actual) = packet.udp_with_checksum(verified).unwrap();
            let parts: Vec<_> = actual
                .chunks(size.map(usize::from).unwrap_or(actual.len()))
                .collect();
            assert_eq!(parts.len(), if gso { 2 } else { 1 });
            for part in parts {
                assert_eq!(part, payload);
                let encoded = encoder.encode(flow.source, flow.destination, part).unwrap();
                assert_eq!(
                    UdpPacket::new_checked(&encoded[0][start..])
                        .unwrap()
                        .checksum(),
                    0xffff
                );
                assert_eq!(decoded(&encoded[0]).udp().unwrap().1, payload);
            }
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn udp_output_views_segment_into_valid_wire_datagrams() {
    for ipv6 in [false, true] {
        let flow = flow(ipv6);
        let mut encoder = udp::Encoder::new(1500);
        let size = 1232;
        let payload: Vec<_> = (0..3).flat_map(|i| vec![i; size]).collect();
        let header = encoder
            .header(flow.source, flow.destination, size, payload.len())
            .unwrap();
        let mut aggregate = header.to_vec();
        aggregate.extend_from_slice(&payload);
        let metadata = tun_rs::VirtioNetHdr {
            flags: 1,
            gso_type: tun_rs::VIRTIO_NET_HDR_GSO_UDP_L4,
            hdr_len: header.len() as u16,
            gso_size: size as u16,
            csum_start: if ipv6 { 40 } else { 20 },
            csum_offset: 6,
        };
        let mut packets = vec![vec![0; 1500]; 3];
        let mut lengths = [0; 3];
        assert_eq!(
            tun_rs::gso_split(
                &mut aggregate,
                metadata,
                &mut packets,
                &mut lengths,
                0,
                ipv6
            )
            .unwrap(),
            3
        );
        for (i, packet) in packets.iter().enumerate() {
            let packet = decoded(&packet[..lengths[i]]);
            let (actual, data) = packet.udp().unwrap();
            assert_eq!(actual, flow);
            assert_eq!(data, vec![i as u8; size]);
        }
        assert!(!encoder.can_offload(flow.source, flow.destination, 4096));
        assert!(
            encoder
                .encode(flow.source, flow.destination, &[0; 4096])
                .unwrap()
                .len()
                > 1
        );
    }
}

#[test]
fn udp_roundtrips_empty_and_fragmented_datagrams_in_both_families() {
    for mtu in [1280, 1500, 9000, 65535] {
        for ipv6 in [false, true] {
            let flow = flow(ipv6);
            let boundary = mtu - if ipv6 { 48 } else { 28 };
            let maximum = if ipv6 { 65527 } else { 65507 };
            for size in [
                0,
                1,
                boundary - 1,
                boundary,
                (boundary + 1).min(maximum),
                4096,
                maximum,
            ] {
                let payload: Vec<_> = (0..size).map(|i| (i % 251) as u8).collect();
                let mut encoder = udp::Encoder::new(mtu);
                let mut packets = encoder
                    .encode(flow.source, flow.destination, &payload)
                    .unwrap();
                assert!(!packets.is_empty());
                assert!(packets.iter().all(|p| p.len() <= mtu));
                // The last fragment may arrive first.
                packets.reverse();
                let mut decoder = Decoder::default();
                let now = Instant::now();
                let mut completed = Vec::new();
                for packet in packets {
                    if let Some(packet) = decoder.decode(&packet, now) {
                        completed.push(packet.into_owned());
                    }
                }
                assert_eq!(completed.len(), 1, "IPv6={ipv6}, payload={size}");
                let (received_flow, received) = completed[0].udp().unwrap();
                assert_eq!(received_flow, flow);
                assert_eq!(received, payload);
            }
        }
    }
}

#[test]
fn checksums_lengths_flags_and_unspecified_addresses_are_validated() {
    for ipv6 in [false, true] {
        let flow = flow(ipv6);
        let original = segment(flow, 100, None, TcpControl::Syn, &[]).bytes;
        for cut in 0..original.len() {
            let packet = Decoder::default().decode(&original[..cut], Instant::now());
            assert!(packet.is_none_or(|p| p.tcp().is_none()));
        }
        let mut corrupt = original.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        assert!(decoded(&corrupt).tcp().is_none());
        let header = if ipv6 { 40 } else { 20 };
        let mut corrupt = original.clone();
        corrupt[header + 13] |= 1; // SYN + FIN is invalid, even with a valid checksum.
        let mut tcp = TcpPacket::new_unchecked(&mut corrupt[header..]);
        tcp.fill_checksum(&flow.source.ip().into(), &flow.destination.ip().into());
        assert!(decoded(&corrupt).tcp().is_none());
        let mut bad_flow = flow;
        bad_flow
            .source
            .set_ip(if ipv6 { "::" } else { "0.0.0.0" }.parse().unwrap());
        assert!(
            Decoder::default()
                .decode(
                    &segment(bad_flow, 1, None, TcpControl::Syn, &[]),
                    Instant::now()
                )
                .is_none()
        );
        if !ipv6 {
            let mut corrupt = original;
            corrupt[8] ^= 1;
            assert!(
                Decoder::default()
                    .decode(&corrupt, Instant::now())
                    .is_none()
            );
        }
    }
}

#[test]
fn ipv6_overlaps_poison_the_datagram_but_atomic_fragments_are_independent() {
    let flow = flow(true);
    let mut packets = udp::Encoder::new(1280)
        .encode(flow.source, flow.destination, &vec![7; 2500])
        .unwrap();
    let mut decoder = Decoder::default();
    let now = Instant::now();
    assert!(decoder.decode(&packets[0], now).is_none());
    assert!(decoder.decode(&packets[0], now).is_none());
    for packet in &packets[1..] {
        assert!(decoder.decode(packet, now).is_none());
    }
    // Reuse the ID with an atomic fragment. RFC 6946 forbids sharing reassembly state.
    let small = udp::Encoder::new(1280)
        .encode(flow.source, flow.destination, b"atomic")
        .unwrap()
        .remove(0)
        .to_vec();
    let mut atomic = vec![0; small.len() + 8];
    atomic[..40].copy_from_slice(&small[..40]);
    let mut ip = Ipv6Packet::new_unchecked(&mut atomic);
    ip.set_next_header(IpProtocol::Ipv6Frag);
    ip.set_payload_len((small.len() - 40 + 8) as u16);
    atomic[40] = u8::from(IpProtocol::Udp);
    atomic[44..48].copy_from_slice(&packets[0][44..48]);
    atomic[48..].copy_from_slice(&small[40..]);
    assert_eq!(
        decoder.decode(&atomic, now).unwrap().udp().unwrap().1,
        b"atomic"
    );
    // Expiry is measured from the first fragment, not refreshed by invalid traffic.
    decoder.expire(now + Duration::from_secs(60));
    let mut completed = None;
    for packet in packets.drain(..) {
        completed = decoder
            .decode(&packet, now + Duration::from_secs(60))
            .map(Packet::into_owned)
            .or(completed);
    }
    assert_eq!(completed.unwrap().udp().unwrap().1, vec![7; 2500]);
}

#[test]
fn closed_tcp_ports_use_rfc_reset_sequence_numbers() {
    for ipv6 in [false, true] {
        let mut rejector = tcp::Rejector::new(1280);
        let (output, mut replies) =
            kotoconn_protocol::queue::channel(128 * 1024, transmit::Transmit::size);
        let syn = decoded(&segment(flow(ipv6), 100, None, TcpControl::Syn, &[]));
        rejector.reject(&syn, &output).unwrap();
        let transmit::Transmit::Packet(bytes) = replies.try_recv().unwrap() else {
            panic!("packet expected")
        };
        let reply = decoded(&bytes);
        let (_, repr) = reply.tcp().unwrap();
        assert_eq!(repr.control, TcpControl::Rst);
        assert_eq!(repr.ack_number, Some(TcpSeqNumber(101)));
        let ack = decoded(&segment(flow(ipv6), 100, Some(999), TcpControl::None, &[]));
        rejector.reject(&ack, &output).unwrap();
        let transmit::Transmit::Packet(bytes) = replies.try_recv().unwrap() else {
            panic!("packet expected")
        };
        let reply = decoded(&bytes);
        let (_, repr) = reply.tcp().unwrap();
        assert_eq!(repr.seq_number, TcpSeqNumber(999));
        assert_eq!(repr.ack_number, None);
        let rst = decoded(&segment(flow(ipv6), 100, None, TcpControl::Rst, &[]));
        rejector.reject(&rst, &output).unwrap();
        assert!(replies.try_recv().is_err());
    }
}

#[tokio::test(start_paused = true)]
async fn dropped_resets_do_not_scatter_queued_packets_across_blocks() {
    use kotoconn_protocol::queue;

    for mtu in [1500, 65535] {
        let mut rejector = tcp::Rejector::new(mtu);
        let (output, mut replies) = queue::channel(128 * 1024, transmit::Transmit::size);
        let syn = decoded(&segment(flow(false), 100, None, TcpControl::Syn, &[]));
        let mut queued = 0;
        while rejector.reject(&syn, &output).is_ok() {
            queued += 1;
        }
        assert!(queued * 40 > 16 * 1024);

        // Keep the queue full while repeatedly dropping enough responses to
        // consume an entire arena block if admission happens after encoding.
        for _ in 0..queued {
            drop(replies.try_recv().unwrap());
            rejector.reject(&syn, &output).unwrap();
            for _ in 0..512 {
                assert_eq!(rejector.reject(&syn, &output), Err(queue::Error::Full));
            }
        }
        let mut packets = Vec::new();
        while let Ok(transmit::Transmit::Packet(packet)) = replies.try_recv() {
            assert_eq!(packet.len(), 40);
            assert_eq!(decoded(&packet).tcp().unwrap().1.control, TcpControl::Rst);
            packets.push(packet);
        }
        assert_eq!(packets.len(), queued);
        // Retain every view while checking contiguous packing, so allocator
        // reuse cannot make unrelated blocks appear to be shared.
        let blocks = 1 + packets
            .windows(2)
            .filter(|pair| pair[1].as_ptr() as usize != pair[0].as_ptr() as usize + pair[0].len())
            .count();
        assert!(blocks * 16 * 1024 <= queued * 40 * 2 + 2 * 16 * 1024);
    }
}

async fn accepted(
    ipv6: bool,
) -> (
    tcp::Stream,
    kotoconn_protocol::queue::Sender<tcp::QueuedPacket>,
    kotoconn_protocol::queue::Receiver<transmit::Transmit>,
    tokio::task::JoinHandle<io::Result<()>>,
    i32,
) {
    let (output, mut replies) =
        kotoconn_protocol::queue::channel(128 * 1024, transmit::Transmit::size);
    let conn = tcp::connection(flow(ipv6), 1280, output, CancellationToken::new());
    let driver = tokio::spawn(conn.driver);
    conn.packets
        .send(segment(flow(ipv6), 100, None, TcpControl::Syn, &[]))
        .await
        .unwrap();
    let synack = decoded(&replies.recv().await.unwrap().packet());
    let (_, repr) = synack.tcp().unwrap();
    assert_eq!(repr.control, TcpControl::Syn);
    assert_eq!(repr.ack_number, Some(TcpSeqNumber(101)));
    let server_seq = repr.seq_number.0.wrapping_add(1);
    conn.packets
        .send(segment(
            flow(ipv6),
            101,
            Some(server_seq),
            TcpControl::None,
            &[],
        ))
        .await
        .unwrap();
    (
        conn.accepted.await.unwrap(),
        conn.packets,
        replies,
        driver,
        server_seq,
    )
}

#[tokio::test(start_paused = true)]
async fn tcp_admits_without_application_io_and_preserves_half_close() {
    for ipv6 in [false, true] {
        let (mut stream, packets, mut replies, driver, server_seq) = accepted(ipv6).await;
        packets
            .send(segment(
                flow(ipv6),
                101,
                Some(server_seq),
                TcpControl::Fin,
                b"hello",
            ))
            .await
            .unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, b"hello");
        stream.write_all(b"world").await.unwrap();
        stream.shutdown().await.unwrap();
        let mut response = Vec::new();
        loop {
            let packet = decoded(&replies.recv().await.unwrap().packet());
            let (_, repr) = packet.tcp().unwrap();
            response.extend_from_slice(repr.payload);
            let fin = repr.control == TcpControl::Fin;
            let end = repr.seq_number + repr.payload.len() + usize::from(fin);
            packets
                .send(segment(flow(ipv6), 107, Some(end.0), TcpControl::None, &[]))
                .await
                .unwrap();
            if fin {
                break;
            }
        }
        assert_eq!(response, b"world");
        drop(stream);
        driver.await.unwrap().unwrap();
    }
}

struct ShortWrites {
    socket: tokio::net::TcpStream,
    pause: bool,
}

impl tokio::io::AsyncRead for ShortWrites {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        out: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.socket).poll_read(cx, out)
    }
}

impl tokio::io::AsyncWrite for ShortWrites {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        std::pin::Pin::new(&mut self.socket).poll_write(cx, &bytes[..bytes.len().min(3)])
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[io::IoSlice<'_>],
    ) -> std::task::Poll<io::Result<usize>> {
        if std::mem::take(&mut self.pause) {
            cx.waker().wake_by_ref();
            return std::task::Poll::Pending;
        }
        let mut prefix = [0; 3];
        let mut count = 0;
        for part in bytes {
            let n = part.len().min(prefix.len() - count);
            prefix[count..count + n].copy_from_slice(&part[..n]);
            count += n;
        }
        let result = std::pin::Pin::new(&mut self.socket).poll_write(cx, &prefix[..count]);
        self.pause = result.is_ready();
        result
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.socket).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::pin::Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

impl kotoconn_protocol::Stream for ShortWrites {
    fn poll_direct(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<bool>> {
        std::task::Poll::Ready(Ok(true))
    }
}

#[tokio::test]
async fn direct_handoff_preserves_prequeued_bytes_partial_writes_half_close_and_counts() {
    for ipv6 in [false, true] {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (stream, packets, mut replies, driver, server_seq) = accepted(ipv6).await;
            let listener =
                tokio::net::TcpListener::bind(if ipv6 { "[::1]:0" } else { "127.0.0.1:0" })
                    .await
                    .unwrap();
            let socket = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (mut peer, _) = listener.accept().await.unwrap();
            let mut sequence = 101;
            for bytes in [b"he".as_slice(), b"llo", b" wo", b"rld"] {
                packets
                    .send(segment(
                        flow(ipv6),
                        sequence,
                        Some(server_seq),
                        TcpControl::None,
                        bytes,
                    ))
                    .await
                    .unwrap();
                sequence += bytes.len() as i32;
            }
            packets
                .send(segment(
                    flow(ipv6),
                    sequence,
                    Some(server_seq),
                    TcpControl::Fin,
                    &[],
                ))
                .await
                .unwrap();
            let relay = tokio::spawn(async move {
                let mut stream: kotoconn_protocol::BoxStream = Box::pin(stream);
                // Force accepted prefixes to end both inside a block and across
                // block boundaries before FIN can shut down the socket.
                kotoconn_protocol::relay(
                    &mut stream,
                    Box::pin(ShortWrites {
                        socket,
                        pause: false,
                    }),
                )
                .await
            });
            let remote = tokio::spawn(async move {
                let mut data = Vec::new();
                peer.read_to_end(&mut data).await.unwrap();
                assert_eq!(data, b"hello world");
                peer.write_all(b"world").await.unwrap();
                peer.shutdown().await.unwrap();
            });
            let mut received = Vec::new();
            loop {
                let packet = decoded(&replies.recv().await.unwrap().packet());
                let (_, repr) = packet.tcp().unwrap();
                received.extend_from_slice(repr.payload);
                let fin = repr.control == TcpControl::Fin;
                let end = repr.seq_number + repr.payload.len() + usize::from(fin);
                packets
                    .send(segment(
                        flow(ipv6),
                        sequence + 1,
                        Some(end.0),
                        TcpControl::None,
                        &[],
                    ))
                    .await
                    .unwrap();
                if fin {
                    break;
                }
            }
            assert_eq!(received, b"world");
            assert_eq!(relay.await.unwrap().unwrap(), (11, 5));
            remote.await.unwrap();
            // The data contract completed; stop this fixture's TIME_WAIT owner.
            driver.abort();
            let _ = driver.await;
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn cancelling_handoff_before_its_first_poll_revokes_the_socket() {
    use kotoconn_protocol::Stream as _;
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut stream, _packets, _replies, driver, _) = accepted(false).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        let mut socket: Option<kotoconn_protocol::BoxStream> = Some(Box::pin(socket));
        let completion = std::pin::Pin::new(&mut stream)
            .take_over(&mut socket)
            .unwrap();
        drop(completion);
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        assert_eq!(
            driver.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::ConnectionReset
        );
    })
    .await
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn tcp_drop_aborts_and_remote_reset_is_an_io_error() {
    let (stream, _packets, mut replies, driver, _) = accepted(false).await;
    drop(stream);
    let mut reset = false;
    while let Some(packet) = replies.recv().await {
        reset |= decoded(&packet.packet()).tcp().unwrap().1.control == TcpControl::Rst;
    }
    assert!(reset);
    assert_eq!(
        driver.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::ConnectionReset
    );
    let (mut stream, packets, _replies, driver, server_seq) = accepted(true).await;
    packets
        .send(segment(
            flow(true),
            101,
            Some(server_seq),
            TcpControl::Rst,
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(
        driver.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::ConnectionReset
    );
    assert_eq!(
        stream.read(&mut [0]).await.unwrap_err().kind(),
        io::ErrorKind::ConnectionReset
    );
}

#[tokio::test(start_paused = true)]
async fn tcp_retransmits_without_caller_polling_and_cancelled_admission_releases_state() {
    let (output, mut replies) =
        kotoconn_protocol::queue::channel(128 * 1024, transmit::Transmit::size);
    let conn = tcp::connection(flow(false), 1280, output, CancellationToken::new());
    let driver = tokio::spawn(conn.driver);
    conn.packets
        .send(segment(flow(false), 100, None, TcpControl::Syn, &[]))
        .await
        .unwrap();
    let first = decoded(&replies.recv().await.unwrap().packet());
    let second = decoded(&replies.recv().await.unwrap().packet());
    assert_eq!(first.payload, second.payload);
    drop(conn.accepted);
    assert_eq!(
        driver.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::ConnectionReset
    );
}

impl std::ops::Deref for tcp::QueuedPacket {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}

#[test]
fn udp_zero_checksum_is_only_valid_for_ipv4_and_source_port_may_be_omitted() {
    for ipv6 in [false, true] {
        let flow = flow(ipv6);
        let mut bytes = udp::Encoder::new(1280)
            .encode(flow.source, flow.destination, b"test")
            .unwrap()
            .remove(0)
            .to_vec();
        let offset = if ipv6 { 40 } else { 20 };
        let mut udp = UdpPacket::new_unchecked(&mut bytes[offset..]);
        udp.set_src_port(0);
        udp.fill_checksum(&flow.source.ip().into(), &flow.destination.ip().into());
        assert_eq!(decoded(&bytes).udp().unwrap().0.source.port(), 0);
        UdpPacket::new_unchecked(&mut bytes[offset..]).set_checksum(0);
        assert_eq!(decoded(&bytes).udp().is_some(), !ipv6);
    }
}

impl transmit::Transmit {
    fn packet(self) -> Vec<u8> {
        match self {
            Self::Packet(packet) => packet.to_vec(),
            Self::Datagram { .. } | Self::TcpGso { .. } => panic!("expected ordinary TCP packet"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn reset_during_handshake_releases_state_without_waiting_for_a_timeout() {
    let (output, mut replies) =
        kotoconn_protocol::queue::channel(128 * 1024, transmit::Transmit::size);
    let conn = tcp::connection(flow(false), 1280, output, CancellationToken::new());
    let driver = tokio::spawn(conn.driver);
    conn.packets
        .send(segment(flow(false), 100, None, TcpControl::Syn, &[]))
        .await
        .unwrap();
    let synack = decoded(&replies.recv().await.unwrap().packet());
    let seq = (synack.tcp().unwrap().1.seq_number + 1).0;
    let before = Instant::now();
    conn.packets
        .send(segment(flow(false), 101, Some(seq), TcpControl::Rst, &[]))
        .await
        .unwrap();
    assert!(conn.accepted.await.is_err());
    assert!(driver.await.unwrap().is_err());
    assert_eq!(Instant::now(), before);
    assert!(
        replies.try_recv().is_err(),
        "RST must not elicit another RST"
    );
}

fn ipv6_fragment(
    bytes: &[u8],
    protocol: IpProtocol,
    offset: usize,
    more: bool,
    id: u32,
) -> Vec<u8> {
    let flow = flow(true);
    let repr = Ipv6Repr {
        src_addr: match flow.source.ip() {
            std::net::IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        dst_addr: match flow.destination.ip() {
            std::net::IpAddr::V6(ip) => ip,
            _ => unreachable!(),
        },
        next_header: IpProtocol::Ipv6Frag,
        payload_len: bytes.len() + 8,
        hop_limit: 64,
    };
    let mut packet = vec![0; 48 + bytes.len()];
    repr.emit(&mut Ipv6Packet::new_unchecked(&mut packet));
    packet[40] = u8::from(protocol);
    Ipv6FragmentRepr {
        frag_offset: (offset / 8) as u16,
        more_frags: more,
        ident: id,
    }
    .emit(&mut Ipv6FragmentHeader::new_unchecked(&mut packet[42..48]));
    packet[48..].copy_from_slice(bytes);
    packet
}

#[test]
fn ipv6_first_fragment_requires_the_complete_tcp_header_and_reserved_bits_are_ignored() {
    let syn = segment(flow(true), 100, None, TcpControl::Syn, &[]);
    let tcp = &syn.bytes[40..];
    let mut decoder = Decoder::default();
    let now = Instant::now();
    assert!(
        decoder
            .decode(&ipv6_fragment(&tcp[..8], IpProtocol::Tcp, 0, true, 1), now)
            .is_none()
    );
    assert!(
        decoder
            .decode(&ipv6_fragment(&tcp[8..], IpProtocol::Tcp, 8, false, 1), now)
            .is_none()
    );
    let mut atomic = ipv6_fragment(tcp, IpProtocol::Tcp, 0, false, 2);
    atomic[41] = 0xff;
    atomic[43] |= 6;
    assert!(decoder.decode(&atomic, now).unwrap().tcp().is_some());
}

#[test]
fn ipv6_reassembly_capacity_expires_and_malformed_input_does_not_panic() {
    let mut decoder = Decoder::default();
    let now = Instant::now();
    let first = ipv6_fragment(&[0; 8], IpProtocol::Udp, 0, true, 64);
    let last = ipv6_fragment(&[0; 8], IpProtocol::Udp, 8, false, 64);
    assert!(decoder.decode(&first, now).is_none());
    decoder.expire(now + Duration::from_secs(60));
    assert!(
        decoder
            .decode(&last, now + Duration::from_secs(60))
            .is_none()
    );
    assert!(
        decoder
            .decode(&first, now + Duration::from_secs(60))
            .is_some()
    );
    // Exercise all lengths and version nibbles with a reproducible corpus.
    for length in 0..512 {
        for version in 0..16 {
            let mut bytes: Vec<u8> = (0..length).map(|i| (i * 31 + length) as u8).collect();
            if let Some(first) = bytes.first_mut() {
                *first = version << 4 | (*first & 15);
            }
            if let Some(packet) = decoder.decode(&bytes, now + Duration::from_secs(60)) {
                match packet.ip.next_header() {
                    IpProtocol::Tcp => {
                        let _ = packet.tcp();
                    }
                    IpProtocol::Udp => {
                        let _ = packet.udp();
                    }
                    _ => {}
                }
            }
        }
    }
}

#[test]
fn ipv4_reassembly_accounts_for_options_in_the_original_size_limit() {
    let flow = flow(false);
    for size in [4096, 65507] {
        let mut packets = udp::Encoder::new(1280)
            .encode(flow.source, flow.destination, &vec![7; size])
            .unwrap()
            .into_iter()
            .map(|packet| packet.to_vec())
            .collect::<Vec<_>>();
        packets[0].splice(20..20, [1, 1, 1, 1]);
        let len = packets[0].len();
        let mut first = Ipv4Packet::new_unchecked(&mut packets[0]);
        first.set_header_len(24);
        first.set_total_len(len as u16);
        first.fill_checksum();
        for reverse in [false, true] {
            let mut decoder = Decoder::default();
            let now = Instant::now();
            let mut complete = None;
            if reverse {
                packets.reverse();
            }
            for packet in &packets {
                complete = decoder.decode(packet, now).or(complete);
            }
            assert_eq!(complete.is_some(), size == 4096);
            if let Some(packet) = complete {
                assert_eq!(packet.udp().unwrap().1, vec![7; size]);
            }
        }
    }
}

#[test]
fn concurrent_reassembly_keeps_datagrams_separate_across_workers() {
    for ipv6 in [false, true] {
        let limits = packet::ReassemblyLimits::new(1024 * 1024);
        let mut decoders = [Decoder::new(limits.clone()), Decoder::new(limits)];
        let f = flow(ipv6);
        let now = Instant::now();
        let mut encoder = udp::Encoder::new(1280);
        let mut datagrams = Vec::new();
        for index in 0..8 {
            let frames = encoder
                .encode(f.source, f.destination, &vec![index as u8; 2500])
                .unwrap();
            assert!(decoders[index % 2].decode(&frames[0], now).is_none());
            datagrams.push(frames);
        }
        for (index, frames) in datagrams.iter().enumerate() {
            let mut completed = None;
            for frame in &frames[1..] {
                completed = decoders[index % 2]
                    .decode(frame, now)
                    .map(Packet::into_owned)
                    .or(completed);
            }
            assert_eq!(completed.unwrap().udp().unwrap().1, vec![index as u8; 2500]);
        }
    }
}

#[test]
fn reassembly_fits_its_allowance_without_geometric_buffer_growth() {
    for ipv6 in [false, true] {
        let limits = packet::ReassemblyLimits::new(5000);
        let mut decoders = [Decoder::new(limits.clone()), Decoder::new(limits)];
        let f = flow(ipv6);
        let now = Instant::now();
        let mut encoder = udp::Encoder::new(1280);

        // Payload and metadata fit, but Vec's geometric growth would not.
        // Completing on one worker must release the shared allowance for another.
        for decoder in &mut decoders {
            let frames = encoder.encode(f.source, f.destination, &[7; 4000]).unwrap();
            let mut completed = None;
            for frame in &frames {
                completed = decoder
                    .decode(frame, now)
                    .map(Packet::into_owned)
                    .or(completed);
            }
            assert_eq!(completed.unwrap().udp().unwrap().1, [7; 4000]);
        }
    }
}

#[test]
fn idle_reassembly_releases_shared_storage_at_its_deadline() {
    for ipv6 in [false, true] {
        let limits = packet::ReassemblyLimits::new(8192);
        let mut idle = Decoder::new(limits.clone());
        let mut active = Decoder::new(limits);
        let f = flow(ipv6);
        let now = Instant::now();
        let mut encoder = udp::Encoder::new(1280);
        for _ in 0..64 {
            let frames = encoder.encode(f.source, f.destination, &[7; 2500]).unwrap();
            assert!(idle.decode(&frames[0], now).is_none());
        }
        let frames = encoder.encode(f.source, f.destination, &[9; 2500]).unwrap();
        for frame in &frames {
            assert!(active.decode(frame, now).is_none());
        }
        let deadline = idle.deadline().unwrap();
        idle.expire(deadline);
        active.expire(deadline);
        assert_eq!(idle.deadline(), None);
        let mut completed = None;
        for frame in &frames {
            completed = active
                .decode(frame, deadline)
                .map(Packet::into_owned)
                .or(completed);
        }
        assert_eq!(completed.unwrap().udp().unwrap().1, [9; 2500]);
    }
}
