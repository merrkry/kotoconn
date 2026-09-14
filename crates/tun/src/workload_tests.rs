//! Stateful input sequences complement the single-packet protocol tests.
use super::*;
use bytes::Bytes;
use smoltcp::wire::{Ipv4Packet, UdpPacket};
use tokio::time::Instant;

#[test]
fn mixed_corrupt_and_valid_datagrams_preserve_decoder_state_across_reuse() {
    for mtu in [1280, 1500, 9000, 65535] {
        for ipv6 in [false, true] {
            let source = if ipv6 {
                "[fd00::2]:1234"
            } else {
                "192.0.2.2:1234"
            }
            .parse()
            .unwrap();
            let destination = if ipv6 {
                "[2001:db8::1]:9001"
            } else {
                "198.18.0.1:9001"
            }
            .parse()
            .unwrap();
            let mut encoder = udp::Encoder::new(mtu);
            let mut decoder = packet::Decoder::default();
            let now = Instant::now();
            for sequence in 0..128u64 {
                let payload: Vec<_> = (0..8193)
                    .map(|i| (i as u64 ^ sequence.rotate_left(3)) as u8)
                    .collect();
                let valid = encoder.encode(source, destination, &payload).unwrap();
                let mut corrupt = encoder
                    .encode(source, destination, b"INVALID-checksum")
                    .unwrap()[0]
                    .to_vec();
                let start = if ipv6 { 40 } else { 20 };
                let mut udp = UdpPacket::new_checked(&mut corrupt[start..]).unwrap();
                let original = udp.checksum();
                udp.set_checksum(if original == 1 { 2 } else { 1 });
                assert!(decoder.decode(&corrupt, now).unwrap().udp().is_none());

                // Alternate partial headers with a valid fragmented datagram in reverse order.
                // A persistent decoder must retain the good assembly across rejected input.
                let mut completed = Vec::new();
                for fragment in valid.iter().rev() {
                    assert!(decoder.decode(&fragment[..1], now).is_none());
                    if let Some(packet) = decoder.decode(fragment, now) {
                        let (flow, received) = packet.udp().unwrap();
                        assert_eq!(flow.source, source);
                        assert_eq!(flow.destination, destination);
                        completed.push(received.to_vec());
                    }
                }
                assert_eq!(completed, [payload]);
                if !ipv6 {
                    Ipv4Packet::new_unchecked(&mut corrupt).set_checksum(0);
                    assert!(decoder.decode(&corrupt, now).is_none());
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn offload_metadata_must_match_the_packet_before_checksums_can_be_trusted() {
    use tun_rs::{VIRTIO_NET_HDR_GSO_UDP_L4, VIRTIO_NET_HDR_LEN, VirtioNetHdr};
    for ipv6 in [false, true] {
        let source = if ipv6 {
            "[fd00::2]:1234"
        } else {
            "192.0.2.2:1234"
        }
        .parse()
        .unwrap();
        let destination = if ipv6 {
            "[2001:db8::1]:9001"
        } else {
            "198.18.0.1:9001"
        }
        .parse()
        .unwrap();
        let packet = udp::Encoder::new(65535)
            .encode(source, destination, &[7; 3200])
            .unwrap()
            .remove(0);
        let valid = VirtioNetHdr {
            flags: 1,
            gso_type: VIRTIO_NET_HDR_GSO_UDP_L4,
            hdr_len: 0,
            gso_size: 1000,
            csum_start: if ipv6 { 40 } else { 20 },
            csum_offset: 6,
        };
        let encode = |header: VirtioNetHdr| {
            let mut bytes = vec![0; VIRTIO_NET_HDR_LEN];
            header.encode(&mut bytes).unwrap();
            bytes.extend_from_slice(&packet);
            bytes
        };
        assert_eq!(
            offload::normalize(&mut encode(valid)).unwrap(),
            (true, Some(1000))
        );
        for header in [
            VirtioNetHdr { flags: 0, ..valid },
            VirtioNetHdr {
                gso_size: 0,
                ..valid
            },
            VirtioNetHdr {
                csum_start: valid.csum_start + 1,
                ..valid
            },
            VirtioNetHdr {
                csum_offset: 16,
                ..valid
            },
            VirtioNetHdr {
                gso_type: tun_rs::VIRTIO_NET_HDR_GSO_TCPV4,
                ..valid
            },
        ] {
            assert!(offload::normalize(&mut encode(header)).is_err());
        }
        let original = Bytes::from(encode(valid));
        for length in 0..VIRTIO_NET_HDR_LEN + usize::from(valid.csum_start) + 8 {
            assert!(offload::normalize(&mut original[..length].to_vec()).is_err());
        }
    }
}
