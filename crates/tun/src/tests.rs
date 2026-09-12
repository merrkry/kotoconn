use super::*;
use packet::{Decoder, Flow, Packet};
use smoltcp::{phy::ChecksumCapabilities, wire::*};
use std::{io, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

fn flow(ipv6: bool) -> Flow {
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

fn segment(
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
        bytes: Packet { ip, payload }.encode(),
        _permit: None,
    }
}

fn decoded(bytes: &[u8]) -> Packet {
    Decoder::default().decode(bytes, Instant::now()).unwrap()
}

#[test]
fn udp_roundtrips_empty_and_fragmented_datagrams_in_both_families() {
    for ipv6 in [false, true] {
        let flow = flow(ipv6);
        for size in [0, 1, 1232, 4096, 65507] {
            let payload: Vec<_> = (0..size).map(|i| (i % 251) as u8).collect();
            let mut encoder = udp::Encoder::new(1280);
            let mut packets = encoder
                .encode(flow.source, flow.destination, &payload)
                .unwrap();
            assert!(!packets.is_empty());
            assert!(packets.iter().all(|p| p.len() <= 1280));
            // The last fragment may arrive first.
            packets.reverse();
            let mut decoder = Decoder::default();
            let now = Instant::now();
            let mut completed = Vec::new();
            for packet in packets {
                if let Some(packet) = decoder.decode(&packet, now) {
                    completed.push(packet);
                }
            }
            assert_eq!(completed.len(), 1, "IPv6={ipv6}, payload={size}");
            let (received_flow, received) = completed[0].udp().unwrap();
            assert_eq!(received_flow, flow);
            assert_eq!(received, payload);
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
        .remove(0);
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
            .or(completed);
    }
    assert_eq!(completed.unwrap().udp().unwrap().1, vec![7; 2500]);
}

#[test]
fn closed_tcp_ports_use_rfc_reset_sequence_numbers() {
    for ipv6 in [false, true] {
        let mut rejector = tcp::Rejector::new(1280);
        let syn = decoded(&segment(flow(ipv6), 100, None, TcpControl::Syn, &[]));
        let replies = rejector.reject(&syn);
        let reply = decoded(&replies[0]);
        let (_, repr) = reply.tcp().unwrap();
        assert_eq!(repr.control, TcpControl::Rst);
        assert_eq!(repr.ack_number, Some(TcpSeqNumber(101)));
        let ack = decoded(&segment(flow(ipv6), 100, Some(999), TcpControl::None, &[]));
        let replies = rejector.reject(&ack);
        let reply = decoded(&replies[0]);
        let (_, repr) = reply.tcp().unwrap();
        assert_eq!(repr.seq_number, TcpSeqNumber(999));
        assert_eq!(repr.ack_number, None);
        let rst = decoded(&segment(flow(ipv6), 100, None, TcpControl::Rst, &[]));
        assert!(rejector.reject(&rst).is_empty());
    }
}

async fn accepted(
    ipv6: bool,
) -> (
    tcp::Stream,
    mpsc::Sender<tcp::QueuedPacket>,
    mpsc::Receiver<Vec<u8>>,
    tokio::task::JoinHandle<io::Result<()>>,
    i32,
) {
    let (output, mut replies) = mpsc::channel(128);
    let conn = tcp::connection(flow(ipv6), 1280, output, CancellationToken::new());
    let driver = tokio::spawn(conn.driver);
    conn.packets
        .send(segment(flow(ipv6), 100, None, TcpControl::Syn, &[]))
        .await
        .unwrap();
    let synack = decoded(&replies.recv().await.unwrap());
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
            let packet = decoded(&replies.recv().await.unwrap());
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

#[tokio::test(start_paused = true)]
async fn tcp_drop_aborts_and_remote_reset_is_an_io_error() {
    let (stream, _packets, mut replies, driver, _) = accepted(false).await;
    drop(stream);
    let mut reset = false;
    while let Some(packet) = replies.recv().await {
        reset |= decoded(&packet).tcp().unwrap().1.control == TcpControl::Rst;
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
    let (output, mut replies) = mpsc::channel(128);
    let conn = tcp::connection(flow(false), 1280, output, CancellationToken::new());
    let driver = tokio::spawn(conn.driver);
    conn.packets
        .send(segment(flow(false), 100, None, TcpControl::Syn, &[]))
        .await
        .unwrap();
    let first = decoded(&replies.recv().await.unwrap());
    let second = decoded(&replies.recv().await.unwrap());
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
            .remove(0);
        let offset = if ipv6 { 40 } else { 20 };
        let mut udp = UdpPacket::new_unchecked(&mut bytes[offset..]);
        udp.set_src_port(0);
        udp.fill_checksum(&flow.source.ip().into(), &flow.destination.ip().into());
        assert_eq!(decoded(&bytes).udp().unwrap().0.source.port(), 0);
        UdpPacket::new_unchecked(&mut bytes[offset..]).set_checksum(0);
        assert_eq!(decoded(&bytes).udp().is_some(), !ipv6);
    }
}
