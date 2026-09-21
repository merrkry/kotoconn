use anyhow::{Result, bail, ensure};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use kotoconn_protocol::{Packet, Target};
use quinn::VarInt;
use quinn_proto::coding::Codec;
use rand::{Rng, distr::Alphanumeric};
use std::{collections::HashMap, net::SocketAddr};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const TCP_REQUEST: u64 = 0x401;
pub const MAX_ADDRESS: usize = 2048;
const MAX_PADDING: usize = 4096;
const MAX_PAYLOAD: usize = 65507;
const MAX_PENDING: usize = 64;
const MAX_BUFFERED: usize = 256 * 1024;

pub fn padding() -> String {
    let mut rng = rand::rng();
    let length = rng.random_range(64..=512);
    (&mut rng)
        .sample_iter(Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

pub fn address(target: &Target) -> Result<String> {
    let value = match target {
        Target::Ip { address, port } => SocketAddr::new(*address, *port).to_string(),
        Target::Domain { name, port } => {
            ensure!(
                !name.is_empty() && !name.contains([':', '[', ']', '\0']),
                "invalid Hysteria domain"
            );
            format!("{name}:{port}")
        }
    };
    ensure!(value.len() <= MAX_ADDRESS, "Hysteria address too long");
    Ok(value)
}

pub fn parse_address(value: &str) -> Result<Target> {
    ensure!(value.len() <= MAX_ADDRESS, "Hysteria address too long");
    if let Ok(address) = value.parse::<SocketAddr>() {
        return Ok(kotoconn_protocol::target(address));
    }

    let (name, port) = value
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("missing Hysteria port"))?;
    ensure!(
        !name.is_empty() && !name.contains([':', '[', ']', '\0']),
        "invalid Hysteria domain"
    );
    Ok(Target::Domain {
        name: name.into(),
        port: port.parse()?,
    })
}

pub fn put_varint(out: &mut Vec<u8>, value: u64) -> Result<()> {
    VarInt::from_u64(value)?.encode(out);
    Ok(())
}

pub async fn read_varint(input: &mut (impl AsyncRead + Unpin)) -> Result<u64> {
    let mut buffer = [0; 8];
    input.read_exact(&mut buffer[..1]).await?;
    let length = 1 << (buffer[0] >> 6);
    input.read_exact(&mut buffer[1..length]).await?;
    Ok(VarInt::decode(&mut &buffer[..length])?.into_inner())
}

async fn read_field(input: &mut (impl AsyncRead + Unpin), max: usize) -> Result<Vec<u8>> {
    let length = read_varint(input).await?;
    ensure!(length <= max as u64, "oversized Hysteria field");
    let mut bytes = vec![0; length as usize];
    input.read_exact(&mut bytes).await?;
    Ok(bytes)
}

fn put_field(out: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    put_varint(out, value.len() as u64)?;
    out.extend_from_slice(value);
    Ok(())
}

pub async fn request(output: &mut (impl AsyncWrite + Unpin), target: &Target) -> Result<()> {
    let mut bytes = Vec::new();
    put_varint(&mut bytes, TCP_REQUEST)?;
    put_field(&mut bytes, address(target)?.as_bytes())?;
    put_field(&mut bytes, padding().as_bytes())?;
    output.write_all(&bytes).await?;
    Ok(())
}

/// The stream classifier has already consumed the TCPRequest ID.
pub async fn read_request(input: &mut (impl AsyncRead + Unpin)) -> Result<Target> {
    let bytes = read_field(input, MAX_ADDRESS).await?;
    let target = parse_address(std::str::from_utf8(&bytes)?)?;
    read_field(input, MAX_PADDING).await?;
    Ok(target)
}

pub async fn response(output: &mut (impl AsyncWrite + Unpin)) -> Result<()> {
    let mut bytes = vec![0];
    put_field(&mut bytes, b"")?;
    put_field(&mut bytes, padding().as_bytes())?;
    output.write_all(&bytes).await?;
    Ok(())
}

pub async fn read_response(input: &mut (impl AsyncRead + Unpin)) -> Result<()> {
    let status = input.read_u8().await?;
    let message = read_field(input, MAX_ADDRESS).await?;
    read_field(input, MAX_PADDING).await?;
    ensure!(
        status == 0,
        "Hysteria TCP rejected: {}",
        String::from_utf8_lossy(&message)
    );
    Ok(())
}

#[derive(Debug)]
pub struct Fragment {
    pub session: u32,
    packet: u16,
    index: u8,
    count: u8,
    target: Target,
    payload: Bytes,
}

impl Fragment {
    pub fn decode(mut bytes: Bytes) -> Result<Self> {
        ensure!(bytes.remaining() >= 9, "short Hysteria datagram");
        let session = bytes.get_u32();
        let packet = bytes.get_u16();
        let index = bytes.get_u8();
        let count = bytes.get_u8();
        ensure!(
            count > 0 && (count == 1 || index < count),
            "invalid Hysteria fragment index"
        );
        let length = VarInt::decode(&mut bytes)?.into_inner();
        ensure!(
            length <= MAX_ADDRESS as u64 && length <= bytes.remaining() as u64,
            "invalid Hysteria address length"
        );
        let target = parse_address(std::str::from_utf8(&bytes.split_to(length as usize))?)?;
        ensure!(bytes.len() <= MAX_PAYLOAD, "oversized Hysteria datagram");
        Ok(Self {
            session,
            packet,
            index,
            count,
            target,
            payload: bytes,
        })
    }
}

pub fn fragments(session: u32, packet: u16, value: &Packet, mtu: usize) -> Result<Vec<Bytes>> {
    ensure!(value.payload.len() <= MAX_PAYLOAD, "oversized UDP payload");
    let mut header = Vec::new();
    header.put_u32(session);
    header.put_u16(packet);
    header.extend_from_slice(&[0, 1]);
    put_field(&mut header, address(&value.target)?.as_bytes())?;
    ensure!(
        mtu > header.len(),
        "QUIC datagram cannot fit Hysteria address"
    );
    let capacity = mtu - header.len();
    let count = value.payload.len().div_ceil(capacity).max(1);
    ensure!(count <= u8::MAX as usize, "too many Hysteria fragments");
    header[7] = count as u8;

    let mut result = Vec::with_capacity(count);
    for index in 0..count {
        header[6] = index as u8;
        let start = index * capacity;
        let end = (start + capacity).min(value.payload.len());
        let mut bytes = BytesMut::with_capacity(header.len() + end - start);
        bytes.extend_from_slice(&header);
        bytes.extend_from_slice(&value.payload[start..end]);
        debug_assert!(bytes.len() <= mtu);
        result.push(bytes.freeze());
    }
    Ok(result)
}

struct Pending {
    target: Target,
    fragments: Vec<Option<Bytes>>,
    bytes: usize,
    received: usize,
    created: tokio::time::Instant,
}

/// Bounded per connection. Incomplete packets are evicted under pressure, with no
/// retransmission or scheduling-dependent expiry needed for correctness.
#[derive(Default)]
pub struct Reassembly {
    pending: HashMap<(u32, u16), Pending>,
    order: std::collections::VecDeque<(u32, u16)>,
    bytes: usize,
}

impl Reassembly {
    fn remove(&mut self, key: &(u32, u16)) -> Option<Pending> {
        let pending = self.pending.remove(key)?;
        debug_assert!(self.bytes >= pending.bytes);
        self.bytes -= pending.bytes;
        self.order.retain(|item| item != key);
        Some(pending)
    }

    pub fn receive(&mut self, part: Fragment) -> Result<Option<(u32, Packet)>> {
        // Fragment retention is an external-network deadline. Packet IDs can wrap,
        // so an old incomplete packet must not survive indefinitely across reuse.
        let now = tokio::time::Instant::now();
        while let Some(key) = self.order.front().copied() {
            if self.pending.get(&key).is_some_and(|value| {
                now.duration_since(value.created) < std::time::Duration::from_secs(10)
            }) {
                break;
            }
            self.remove(&key);
        }
        if part.count == 1 {
            return Ok(Some((
                part.session,
                Packet {
                    target: part.target,
                    payload: part.payload,
                },
            )));
        }
        let key = (part.session, part.packet);
        if let Some(previous) = self.pending.get(&key)
            && (previous.target != part.target || previous.fragments.len() != part.count as usize)
        {
            self.remove(&key);
            bail!("inconsistent Hysteria fragments");
        }

        while self.bytes + part.payload.len() > MAX_BUFFERED
            || (!self.pending.contains_key(&key) && self.pending.len() >= MAX_PENDING)
        {
            let Some(oldest) = self.order.front().copied() else {
                break;
            };
            self.remove(&oldest);
        }

        let pending = self.pending.entry(key).or_insert_with(|| {
            self.order.push_back(key);
            Pending {
                target: part.target,
                fragments: vec![None; part.count as usize],
                bytes: 0,
                received: 0,
                created: now,
            }
        });
        // SAFETY: decode checked index < count, and an existing entry's count was checked above.
        let slot = pending
            .fragments
            .get_mut(part.index as usize)
            .expect("validated fragment index");
        if slot.is_some() {
            return Ok(None);
        }
        pending.bytes += part.payload.len();
        pending.received += 1;
        self.bytes += part.payload.len();
        *slot = Some(part.payload);
        if pending.bytes > MAX_PAYLOAD {
            self.remove(&key);
            bail!("oversized reassembled Hysteria datagram");
        }
        if pending.received != pending.fragments.len() {
            return Ok(None);
        }

        // SAFETY: This task inserted or found the entry above and has exclusive access.
        let complete = self.remove(&key).expect("complete packet");
        let mut payload = BytesMut::with_capacity(complete.bytes);
        for fragment in complete.fragments {
            // SAFETY: received counts only previously empty slots; all slots are present.
            payload.extend_from_slice(&fragment.expect("complete fragment"));
        }
        Ok(Some((
            key.0,
            Packet {
                target: complete.target,
                payload: payload.freeze(),
            },
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn framing_accepts_nonminimal_varints_and_preserves_payload() {
        let destination = parse_address("[::1]:443").unwrap();
        let mut encoded = Vec::new();
        request(&mut encoded, &destination).await.unwrap();
        encoded.extend_from_slice(b"payload");
        let mut input = encoded.as_slice();
        assert_eq!(read_varint(&mut input).await.unwrap(), TCP_REQUEST);
        assert_eq!(read_request(&mut input).await.unwrap(), destination);
        assert_eq!(input, b"payload");
        assert_eq!(
            read_varint(&mut &b"\xc0\0\0\0\0\0\x04\x01"[..])
                .await
                .unwrap(),
            TCP_REQUEST
        );
        assert!(
            read_request(&mut &b"\xff\xff\xff\xff\xff\xff\xff\xff"[..])
                .await
                .is_err()
        );
    }

    #[test]
    fn datagrams_reassemble_out_of_order_with_duplicates_and_empty_payloads() {
        for payload in [Bytes::new(), Bytes::from(vec![7; 60000])] {
            let packet = Packet {
                target: parse_address("example.org:53").unwrap(),
                payload,
            };
            let encoded = fragments(42, 9, &packet, 1200).unwrap();
            let mut receiver = Reassembly::default();
            let mut result = None;
            for bytes in encoded.iter().rev() {
                result = receiver
                    .receive(Fragment::decode(bytes.clone()).unwrap())
                    .unwrap()
                    .or(result);
                if encoded.len() > 1 && result.is_none() {
                    assert!(
                        receiver
                            .receive(Fragment::decode(bytes.clone()).unwrap())
                            .unwrap()
                            .is_none()
                    );
                }
            }
            let (session, decoded) = result.unwrap();
            assert_eq!(session, 42);
            assert_eq!(decoded.target, packet.target);
            assert_eq!(decoded.payload, packet.payload);
            assert_eq!(receiver.bytes, 0);
        }
    }

    #[test]
    fn malformed_and_incomplete_datagrams_are_bounded() {
        for length in 0..12 {
            assert!(Fragment::decode(Bytes::from(vec![0; length])).is_err());
        }
        let packet = Packet {
            target: parse_address("127.0.0.1:53").unwrap(),
            payload: Bytes::from(vec![1; 4000]),
        };
        let mut receiver = Reassembly::default();
        for id in 0..1024 {
            let encoded = fragments(1, id, &packet, 1200).unwrap();
            assert!(
                receiver
                    .receive(Fragment::decode(encoded[0].clone()).unwrap())
                    .unwrap()
                    .is_none()
            );
        }
        assert!(receiver.pending.len() <= MAX_PENDING);
        assert!(receiver.order.len() <= MAX_PENDING);
        assert!(receiver.bytes <= MAX_BUFFERED);
        let mut invalid = fragments(1, 1023, &packet, 1200).unwrap()[1].to_vec();
        invalid[7] += 1;
        assert!(
            receiver
                .receive(Fragment::decode(invalid.into()).unwrap())
                .is_err()
        );
    }
}
