//! Padding schemes shape the TLS record sizes of a client's first writes.
use crate::frame::{self, Command};
use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use md5::{Digest, Md5};
use rand::Rng;
use std::{
    collections::HashMap,
    fmt::Write as _,
    sync::{Arc, LazyLock},
};
use tokio::io::{AsyncWrite, AsyncWriteExt};

const DEFAULT: &str = "stop=8
0=30-30
1=100-400
2=400-500,c,500-1000,c,500-1000,c,500-1000,c,500-1000
3=9-9,500-1000
4=500-1000
5=500-1000
6=500-1000
7=500-1000";

static DEFAULT_SCHEME: LazyLock<PaddingScheme> = LazyLock::new(|| {
    // SAFETY: The reference default scheme is a valid constant.
    PaddingScheme::parse(DEFAULT.as_bytes()).expect("default padding scheme")
});

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Segment {
    /// A TLS plaintext record size, chosen uniformly from the inclusive range.
    Record { min: u16, max: u16 },
    /// Stop padding this write once the payload is exhausted.
    Check,
}

struct Inner {
    raw: Bytes,
    md5: String,
    stop: u32,
    packets: HashMap<u32, Box<[Segment]>>,
}

/// Packet 0 is the authentication request, which only uses the first record size.
/// Session writes count from packet 1, and writes from `stop` on are unpadded.
#[derive(Clone)]
pub(crate) struct PaddingScheme(Arc<Inner>);

impl Default for PaddingScheme {
    fn default() -> Self {
        DEFAULT_SCHEME.clone()
    }
}

impl PaddingScheme {
    /// Strict where the reference parser silently skips malformed ranges, so a
    /// typo cannot quietly disable shaping. Lines without `=` carry no setting.
    pub fn parse(raw: &[u8]) -> Result<Self> {
        ensure!(raw.len() <= u16::MAX as usize, "padding scheme too large");
        let text = std::str::from_utf8(raw).context("padding scheme is not UTF-8")?;

        let mut stop = None;
        let mut packets = HashMap::new();
        for (key, value) in text.split('\n').filter_map(|line| line.split_once('=')) {
            if key == "stop" {
                stop = Some(value.parse::<u32>().context("invalid padding stop")?);
                continue;
            }
            let packet = key
                .parse::<u32>()
                .with_context(|| format!("invalid padding packet number {key:?}"))?;
            let segments = value
                .split(',')
                .map(|segment| self::segment(packet, segment))
                .collect::<Result<_>>()?;
            packets.insert(packet, segments);
        }
        let Some(stop) = stop else {
            bail!("padding scheme has no stop");
        };

        let mut md5 = String::with_capacity(32);
        for byte in Md5::digest(raw) {
            // SAFETY: Formatting into a String cannot fail.
            write!(md5, "{byte:02x}").expect("hex digest");
        }
        Ok(Self(Arc::new(Inner {
            raw: Bytes::copy_from_slice(raw),
            md5,
            stop,
            packets,
        })))
    }

    pub fn raw(&self) -> &Bytes {
        &self.0.raw
    }

    /// Lowercase hex, as exchanged in the `padding-md5` setting.
    pub fn md5(&self) -> &str {
        &self.0.md5
    }

    pub fn applies(&self, packet: u32) -> bool {
        packet < self.0.stop
    }

    pub fn authentication_padding(&self) -> u16 {
        match self.segments(0).first() {
            Some(Segment::Record { min, max }) => size(*min, *max),
            _ => 0,
        }
    }

    fn segments(&self, packet: u32) -> &[Segment] {
        self.0.packets.get(&packet).map_or(&[], |segments| segments)
    }
}

fn segment(packet: u32, segment: &str) -> Result<Segment> {
    if segment == "c" {
        ensure!(
            packet != 0,
            "authentication padding cannot contain a check mark"
        );
        return Ok(Segment::Check);
    }
    let Some((min, max)) = segment.split_once('-') else {
        bail!("invalid padding range {segment:?}");
    };
    let min = min.parse::<u16>().context("invalid padding size")?;
    let max = max.parse::<u16>().context("invalid padding size")?;
    ensure!(min > 0 && max > 0, "padding size must be positive");
    Ok(Segment::Record {
        min: min.min(max),
        max: min.max(max),
    })
}

fn size(min: u16, max: u16) -> u16 {
    rand::rng().random_range(min..=max)
}

fn waste(length: usize) -> Vec<u8> {
    let mut record = Vec::with_capacity(frame::HEADER + length);
    record.extend_from_slice(&frame::header(Command::Waste, 0, length));
    record.resize(frame::HEADER + length, 0);
    record
}

/// Write one counted packet as the scheme's records, matching the reference
/// `writeConn`: a record larger than the remaining payload carries the rest
/// plus a Waste frame, and a record after the payload is all Waste.
pub(crate) async fn write_packet(
    writer: &mut (impl AsyncWrite + Unpin),
    mut payload: &[u8],
    scheme: &PaddingScheme,
    packet: u32,
) -> Result<()> {
    debug_assert!(scheme.applies(packet));
    for segment in scheme.segments(packet) {
        let size = match *segment {
            Segment::Check if payload.is_empty() => break,
            Segment::Check => continue,
            Segment::Record { min, max } => usize::from(size(min, max)),
        };

        if payload.len() > size {
            writer.write_all(&payload[..size]).await?;
            payload = &payload[size..];
        } else if !payload.is_empty() {
            let mut record = payload.to_vec();
            if let Some(padding) = size.checked_sub(payload.len() + frame::HEADER)
                && padding > 0
            {
                record.extend_from_slice(&waste(padding));
            }
            writer.write_all(&record).await?;
            payload = &[];
        } else {
            writer.write_all(&waste(size)).await?;
        }
    }
    writer.write_all(payload).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Record sizes as seen by a peer reading one write per record.
    async fn records(scheme: &str, packet: u32, payload: usize) -> Vec<usize> {
        struct Records(Vec<usize>);
        impl AsyncWrite for Records {
            fn poll_write(
                mut self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                bytes: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                self.0.push(bytes.len());
                std::task::Poll::Ready(Ok(bytes.len()))
            }
            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
            fn poll_shutdown(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
        }

        let scheme = PaddingScheme::parse(scheme.as_bytes()).unwrap();
        let mut writer = Records(Vec::new());
        write_packet(&mut writer, &vec![1; payload], &scheme, packet)
            .await
            .unwrap();
        writer.0.into_iter().filter(|&size| size > 0).collect()
    }

    #[tokio::test]
    async fn records_follow_the_reference_split_pad_and_check_rules() {
        let scheme = "stop=3\n1=100-100,c,50-50\n2=10-10,20-20";
        // Split, then the remaining 50 bytes fill a 50-byte record exactly.
        assert_eq!(records(scheme, 1, 150).await, [100, 50]);
        // The check mark ends the write once the payload is exhausted.
        assert_eq!(records(scheme, 1, 60).await, [100]);
        // Waste fills a record only when its header fits after the payload;
        // a record after the payload is pure Waste of the record's size.
        assert_eq!(records(scheme, 2, 2).await, [10, 20 + frame::HEADER]);
        assert_eq!(records(scheme, 2, 4).await, [4, 20 + frame::HEADER]);
        // A payload longer than every record is sent unpadded afterwards.
        assert_eq!(records(scheme, 2, 100).await, [10, 20, 70]);
    }

    #[test]
    fn default_scheme_matches_the_reference_digest() {
        let scheme = PaddingScheme::default();
        // Computed by the official Go implementation for its default scheme.
        assert_eq!(scheme.md5(), "75cff2ad89aadf5e257059ee571ebe11");
        assert_eq!(scheme.authentication_padding(), 30);
        assert!(scheme.applies(7) && !scheme.applies(8));
    }

    #[test]
    fn rejects_unbounded_or_malformed_schemes() {
        for scheme in [
            "",
            "0=30-30",
            "stop=2\n0=-1-5",
            "stop=2\n1=999999999-999999999",
            "stop=2\n0=c",
            "stop=2\n1=0-5",
            "stop=2\nnext=1-5",
            "stop=2\n1=5",
        ] {
            assert!(
                PaddingScheme::parse(scheme.as_bytes()).is_err(),
                "{scheme:?}"
            );
        }
        let swapped = PaddingScheme::parse(b"stop=2\n0=40-30\n").unwrap();
        assert!((30..=40).contains(&swapped.authentication_padding()));
    }
}
