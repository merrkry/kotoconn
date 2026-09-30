use kotoconn_config::{SniffConfig, SniffProtocol, SniffResult};
use kotoconn_inbounds::sniff;
use kotoconn_protocol::{Packet, queue, target};
use rustls::{ClientConfig, RootCertStore, Side, quic::Version};
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn config() -> SniffConfig {
    SniffConfig {
        timeout: Duration::from_millis(300),
    }
}

fn client_config() -> Arc<ClientConfig> {
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let mut config = ClientConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    // Make the ClientHello span several TLS records.
    config.max_fragment_size = Some(64);
    Arc::new(config)
}

fn tls_hello(sni: bool) -> Vec<u8> {
    let mut config = (*client_config()).clone();
    config.enable_sni = sni;
    let mut client =
        rustls::ClientConnection::new(Arc::new(config), "sniff.example".try_into().unwrap())
            .unwrap();
    let mut bytes = Vec::new();
    while client.wants_write() {
        client.write_tls(&mut bytes).unwrap();
    }
    bytes
}

#[tokio::test]
async fn tcp_inspects_fragmented_headers_and_replays_every_byte() {
    let cases = [
        (
            b"GET / HTTP/1.1\r\nhOsT: Sniff.Example:8080\r\n\r\nbody".to_vec(),
            Some(SniffResult {
                protocol: SniffProtocol::Http,
                domain: Some("sniff.example".into()),
            }),
        ),
        (
            b"GET / HTTP/1.1\r\nHost: [::1]:8080\r\n\r\n".to_vec(),
            Some(SniffResult {
                protocol: SniffProtocol::Http,
                domain: None,
            }),
        ),
        (
            tls_hello(true),
            Some(SniffResult {
                protocol: SniffProtocol::Tls,
                domain: Some("sniff.example".into()),
            }),
        ),
        (
            tls_hello(false),
            Some(SniffResult {
                protocol: SniffProtocol::Tls,
                domain: None,
            }),
        ),
        (vec![0, 255, 22, 3, 3], None),
        (b"GET / HTTP/1.1\r\nHost: incomplete".to_vec(), None),
        (vec![22, 3, 3, 0, 2, 255, 255], None),
        (
            [b"GET / HTTP/1.1\r\nHost: ".as_slice(), &vec![b'a'; 70000]].concat(),
            None,
        ),
    ];

    for (payload, expected) in cases {
        let (mut peer, stream) = tokio::io::duplex(17);
        let sent = payload.clone();
        let sending = tokio::spawn(async move {
            peer.write_all(&sent).await.unwrap();
            peer.shutdown().await.unwrap();
        });
        let (mut stream, result) = sniff::tcp(Box::pin(stream), &config()).await.unwrap();
        assert_eq!(result, expected);

        let mut received = Vec::new();
        stream.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, payload);
        sending.await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn tcp_timeout_preserves_partial_payload_and_allows_server_first_io() {
    let (mut peer, stream) = tokio::io::duplex(1024);
    peer.write_all(b"GET / HTTP/1.1\r\nHost: ").await.unwrap();
    let (mut stream, result) = sniff::tcp(Box::pin(stream), &config()).await.unwrap();
    assert_eq!(result, None);

    let mut bytes = [0; 22];
    stream.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"GET / HTTP/1.1\r\nHost: ");
    stream.write_all(b"hello").await.unwrap();
    let mut greeting = [0; 5];
    peer.read_exact(&mut greeting).await.unwrap();
    assert_eq!(&greeting, b"hello");

    let (_peer, stream) = tokio::io::duplex(1024);
    let (_, result) = sniff::tcp(Box::pin(stream), &config()).await.unwrap();
    assert_eq!(result, None);
}

fn varint(value: usize, out: &mut Vec<u8>) {
    if value < 64 {
        out.push(value as u8);
    } else {
        assert!(value < 16384);
        out.extend_from_slice(&((value as u16) | 0x4000).to_be_bytes());
    }
}

fn quic_hello(version: Version) -> Vec<u8> {
    let mut client = rustls::quic::ClientConnection::new(
        client_config(),
        version,
        "sniff.example".try_into().unwrap(),
        Vec::new(),
    )
    .unwrap();
    let mut handshake = Vec::new();
    client.write_hs(&mut handshake);
    handshake
}

fn initial(version: Version, number: u8, offset: usize, crypto: &[u8]) -> Vec<u8> {
    let dcid = b"sniffcid";
    let suite = rustls::crypto::aws_lc_rs::cipher_suite::TLS13_AES_128_GCM_SHA256
        .tls13()
        .unwrap()
        .quic_suite()
        .unwrap();
    let keys = suite.keys(dcid, Side::Client, version).local;
    let (first, wire_version) = match version {
        Version::V1 => (0xc0, 1u32),
        Version::V2 => (0xd0, 0x6b3343cf),
        _ => unreachable!(),
    };
    let mut payload = vec![6];
    varint(offset, &mut payload);
    varint(crypto.len(), &mut payload);
    payload.extend_from_slice(crypto);
    payload.resize(payload.len().max(1200), 0);

    let mut packet = vec![first];
    packet.extend_from_slice(&wire_version.to_be_bytes());
    packet.push(dcid.len() as u8);
    packet.extend_from_slice(dcid);
    packet.extend_from_slice(&[0, 0]); // Empty source CID and token.
    varint(1 + payload.len() + keys.packet.tag_len(), &mut packet);
    let pn_offset = packet.len();
    packet.push(number);
    let tag = keys
        .packet
        .encrypt_in_place(number as u64, &packet, &mut payload)
        .unwrap();
    packet.extend_from_slice(&payload);
    packet.extend_from_slice(tag.as_ref());

    let sample = packet[pn_offset + 4..pn_offset + 4 + keys.header.sample_len()].to_vec();
    let (first, rest) = packet.split_first_mut().unwrap();
    keys.header
        .encrypt_in_place(&sample, first, &mut rest[pn_offset - 1..pn_offset])
        .unwrap();
    packet
}

#[tokio::test]
async fn udp_reassembles_authenticated_quic_crypto_and_replays_packets_in_order() {
    for version in [Version::V1, Version::V2] {
        let hello = quic_hello(version);
        let half = hello.len() / 2;
        let tail = initial(version, 1, half, &hello[half..]);
        let head = initial(version, 0, 0, &hello[..half]);
        let packets = [tail.clone(), tail, head, Vec::new()];
        let destination = target("127.0.0.1:443".parse().unwrap());
        let (sender, mut receiver) =
            queue::channel(queue::INITIAL_BYTES, |packet: &Packet| packet.payload.len());
        for payload in &packets {
            sender
                .send(Packet {
                    target: destination.clone(),
                    payload: payload.clone().into(),
                })
                .await
                .unwrap();
        }
        drop(sender);

        assert_eq!(
            sniff::udp(&mut receiver, &config()).await,
            Some(SniffResult {
                protocol: SniffProtocol::Quic,
                domain: Some("sniff.example".into()),
            })
        );
        let mut replayed = Vec::new();
        assert_eq!(receiver.recv_many(&mut replayed, 32).await, packets.len());
        for (packet, expected) in replayed.iter().zip(&packets) {
            assert_eq!(packet.target, destination);
            assert_eq!(packet.payload.as_ref(), expected);
        }
        assert!(receiver.recv().await.is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn udp_unknown_corrupt_and_incomplete_payloads_remain_unchanged() {
    let hello = quic_hello(Version::V1);
    let mut corrupt = initial(Version::V1, 0, 0, &hello);
    *corrupt.last_mut().unwrap() ^= 1;
    let partial = initial(Version::V1, 0, 0, &hello[..20]);
    for payload in [
        Vec::new(),
        vec![0, 1, 2],
        corrupt,
        partial,
        vec![0xc0; 65536],
    ] {
        let destination = target("127.0.0.1:443".parse().unwrap());
        let (sender, mut receiver) =
            queue::channel(queue::INITIAL_BYTES, |packet: &Packet| packet.payload.len());
        sender
            .send(Packet {
                target: destination.clone(),
                payload: payload.clone().into(),
            })
            .await
            .unwrap();

        assert_eq!(sniff::udp(&mut receiver, &config()).await, None);
        let replayed = receiver.try_recv().unwrap();
        assert_eq!(replayed.target, destination);
        assert_eq!(replayed.payload.as_ref(), payload);
    }
}
