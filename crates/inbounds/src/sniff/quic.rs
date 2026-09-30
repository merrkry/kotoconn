use super::{MAX_BYTES, Outcome, tls};
use rustls::{Side, crypto::aws_lc_rs::cipher_suite::TLS13_AES_128_GCM_SHA256, quic::Version};

#[derive(Default)]
pub(super) struct Inspector {
    connection: Option<(u32, Vec<u8>)>,
    largest_packet: Option<u64>,
    crypto: Vec<u8>,
    received: Vec<bool>,
    contiguous: usize,
}

impl Inspector {
    pub(super) fn inspect(&mut self, mut datagram: &[u8]) -> Outcome {
        while !datagram.is_empty() {
            let Some(rest) = self.initial(datagram) else {
                return Outcome::Unknown;
            };
            datagram = rest;

            let outcome = tls::handshake(&self.crypto[..self.contiguous]);
            if !matches!(outcome, Outcome::NeedMore) {
                return outcome;
            }
            // A short-header packet cannot carry Initial CRYPTO data.
            if datagram.first().is_some_and(|first| first & 0x80 == 0) {
                break;
            }
        }
        Outcome::NeedMore
    }

    fn initial<'a>(&mut self, datagram: &'a [u8]) -> Option<&'a [u8]> {
        let mut cursor = Cursor(datagram);
        let first = cursor.byte()?;
        if first & 0xc0 != 0xc0 {
            return None;
        }
        let version_bytes = cursor.take(4)?;
        let version = u32::from_be_bytes(version_bytes.try_into().ok()?);
        let (tls_version, initial_type) = match version {
            1 => (Version::V1, 0),
            0x6b3343cf => (Version::V2, 1),
            0xff00001d..=0xff000020 => (Version::V1Draft, 0),
            _ => return None,
        };
        if (first >> 4) & 3 != initial_type {
            return None;
        }

        let dcid_len = cursor.byte()? as usize;
        if dcid_len > 20 {
            return None;
        }
        let dcid = cursor.take(dcid_len)?;
        let scid_len = cursor.byte()? as usize;
        if scid_len > 20 {
            return None;
        }
        cursor.take(scid_len)?;
        let token_len = cursor.size()?;
        cursor.take(token_len)?;
        let payload_len = cursor.size()?;
        let pn_offset = datagram.len() - cursor.0.len();
        cursor.take(payload_len)?;
        let packet_len = pn_offset + payload_len;

        if let Some((previous_version, previous_dcid)) = &self.connection
            && (*previous_version != version || previous_dcid != dcid)
        {
            return None;
        }

        let suite = TLS13_AES_128_GCM_SHA256.tls13()?.quic_suite()?;
        let keys = suite.keys(dcid, Side::Server, tls_version).remote;
        let mut packet = datagram[..packet_len].to_vec();
        let sample_start = pn_offset.checked_add(4)?;
        let sample = packet
            .get(sample_start..sample_start + keys.header.sample_len())?
            .to_vec();
        let (first_byte, rest) = packet.split_first_mut()?;
        let pn = rest.get_mut(pn_offset - 1..pn_offset + 3)?;
        keys.header.decrypt_in_place(&sample, first_byte, pn).ok()?;
        if *first_byte & 0x0c != 0 {
            return None;
        }
        let pn_len = (*first_byte as usize & 3) + 1;
        let truncated = packet
            .get(pn_offset..pn_offset + pn_len)?
            .iter()
            .fold(0u64, |number, byte| (number << 8) | *byte as u64);
        let number = packet_number(truncated, pn_len, self.largest_packet);
        let (header, payload) = packet.split_at_mut(pn_offset + pn_len);
        let plaintext = keys.packet.decrypt_in_place(number, header, payload).ok()?;

        self.connection = Some((version, dcid.to_vec()));
        self.largest_packet = Some(
            self.largest_packet
                .map_or(number, |previous| previous.max(number)),
        );
        self.frames(plaintext)?;
        Some(cursor.0)
    }

    fn frames(&mut self, bytes: &[u8]) -> Option<()> {
        let mut cursor = Cursor(bytes);
        while !cursor.0.is_empty() {
            match cursor.varint()? {
                0 | 1 => {}
                frame_type @ (2 | 3) => {
                    // ACK ranges are variable length; each range consumes at
                    // least two bytes, so malicious counts cannot cause long loops.
                    cursor.varint()?;
                    cursor.varint()?;
                    let ranges = cursor.size()?;
                    cursor.varint()?;
                    if ranges > cursor.0.len() / 2 {
                        return None;
                    }
                    for _ in 0..ranges {
                        cursor.varint()?;
                        cursor.varint()?;
                    }
                    if frame_type == 3 {
                        for _ in 0..3 {
                            cursor.varint()?;
                        }
                    }
                }
                6 => {
                    let offset = cursor.size()?;
                    let length = cursor.size()?;
                    let fragment = cursor.take(length)?;
                    let end = offset.checked_add(length)?;
                    if end > MAX_BYTES {
                        return None;
                    }
                    if end > self.crypto.len() {
                        self.crypto.resize(end, 0);
                        self.received.resize(end, false);
                    }
                    debug_assert_eq!(self.crypto.len(), self.received.len());
                    // SAFETY: Both buffers cover offset..end, and each position
                    // belongs to the checked fragment range below MAX_BYTES.
                    for (index, byte) in fragment.iter().enumerate() {
                        let position = offset + index;
                        if self.received[position] && self.crypto[position] != *byte {
                            return None;
                        }
                        self.crypto[position] = *byte;
                        self.received[position] = true;
                    }
                    while self.received.get(self.contiguous) == Some(&true) {
                        self.contiguous += 1;
                    }
                    debug_assert!(self.contiguous <= self.crypto.len());
                }
                _ => return None,
            }
        }
        Some(())
    }
}

fn packet_number(truncated: u64, length: usize, largest: Option<u64>) -> u64 {
    let expected = largest.map_or(0, |number| number + 1);
    let window = 1u64 << (length * 8);
    let half = window / 2;
    let candidate = (expected & !(window - 1)) | truncated;
    if candidate + half <= expected && candidate + window < (1 << 62) {
        candidate + window
    } else if candidate > expected + half && candidate >= window {
        candidate - window
    } else {
        candidate
    }
}

struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, count: usize) -> Option<&'a [u8]> {
        let bytes = self.0.get(..count)?;
        self.0 = &self.0[count..];
        Some(bytes)
    }

    fn byte(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn varint(&mut self) -> Option<u64> {
        let first = *self.0.first()?;
        let length = 1 << (first >> 6);
        let bytes = self.take(length)?;
        let mut value = (first & 0x3f) as u64;
        for byte in &bytes[1..] {
            value = (value << 8) | *byte as u64;
        }
        Some(value)
    }

    fn size(&mut self) -> Option<usize> {
        self.varint()?.try_into().ok()
    }
}
