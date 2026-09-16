use anyhow::{Result, bail, ensure};
use anytls::core::{CHECK_MARK, Command, Frame, HEADER_OVERHEAD_SIZE, PaddingFactory};
use bytes::{Buf, Bytes, BytesMut};
use fast_socks5::util::target_addr::{TargetAddr, read_address};
use kotoconn_protocol::Target;
use sha2::{Digest, Sha256};
use std::{io, net::SocketAddr};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::Decoder;

pub(crate) fn password_hash(password: &str) -> [u8; 32] {
    Sha256::digest(password.as_bytes()).into()
}

pub(crate) async fn authenticate(
    io: &mut (impl AsyncRead + Unpin),
    password: &[u8; 32],
) -> Result<()> {
    let mut supplied = [0; 32];
    io.read_exact(&mut supplied).await?;
    ensure!(
        bool::from(password.ct_eq(&supplied)),
        "AnyTLS authentication failed"
    );

    let length = io.read_u16().await?;
    let mut padding = vec![0; usize::from(length)];
    io.read_exact(&mut padding).await?;
    Ok(())
}

pub(crate) async fn send_authentication(
    io: &mut (impl AsyncWrite + Unpin),
    password: &[u8; 32],
    padding: &PaddingFactory,
) -> Result<()> {
    let size = padding
        .generate_record_payload_sizes(0)
        .first()
        .copied()
        .unwrap_or(0);
    let length =
        u16::try_from(size).map_err(|_| anyhow::anyhow!("invalid authentication padding"))?;
    let mut bytes = Vec::with_capacity(34 + usize::from(length));
    bytes.extend_from_slice(password);
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.resize(34 + usize::from(length), 0);
    io.write_all(&bytes).await?;
    io.flush().await?;
    Ok(())
}

pub(crate) fn address(target: &Target) -> Result<Vec<u8>> {
    let address = match target {
        Target::Ip { address, port } => TargetAddr::Ip(SocketAddr::new(*address, *port)),
        Target::Domain { name, port } => {
            ensure!(!name.is_empty(), "empty destination domain");
            TargetAddr::Domain(name.clone(), *port)
        }
    };
    Ok(address.to_be_bytes()?)
}

pub(crate) async fn read_target(io: &mut (impl AsyncRead + Unpin)) -> Result<Target> {
    let family = io.read_u8().await?;
    read_target_family(io, family).await
}

pub(crate) async fn read_target_family(
    io: &mut (impl AsyncRead + Unpin),
    family: u8,
) -> Result<Target> {
    Ok(match read_address(io, family).await? {
        TargetAddr::Ip(socket) => kotoconn_protocol::target(socket),
        TargetAddr::Domain(name, port) => {
            ensure!(!name.is_empty(), "empty destination domain");
            Target::Domain { name, port }
        }
    })
}

/// Apply library-generated record sizes to TLS writes. Each call is one counted
/// application write, even when the padding schedule splits it into records.
pub(crate) async fn write_records(
    writer: &mut (impl AsyncWrite + Unpin),
    mut bytes: Bytes,
    padding: Option<&PaddingFactory>,
    packet: u32,
) -> Result<()> {
    if let Some(padding) = padding.filter(|padding| packet < padding.stop()) {
        for size in padding.generate_record_payload_sizes(packet) {
            if size == CHECK_MARK {
                if bytes.is_empty() {
                    break;
                }
                continue;
            }
            let size = usize::try_from(size)?;
            ensure!(size <= u16::MAX as usize, "AnyTLS padding record too large");
            if bytes.len() >= size {
                writer.write_all(&bytes.split_to(size)).await?;
            } else {
                let mut record = bytes.to_vec();
                bytes = Bytes::new();
                if size >= record.len() + HEADER_OVERHEAD_SIZE {
                    let waste = Frame::with_data(
                        Command::Waste,
                        0,
                        Bytes::from(vec![0; size - record.len() - HEADER_OVERHEAD_SIZE]),
                    );
                    record.extend_from_slice(&waste.to_bytes()?);
                }
                writer.write_all(&record).await?;
            }
        }
    }
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

pub(crate) fn padding_scheme(raw: Option<&str>) -> Result<PaddingFactory> {
    let Some(raw) = raw else {
        return Ok(PaddingFactory::default());
    };
    // Bound record allocation before calling the library's size generator. It
    // accepts i64 ranges and casts them to i32, so reject oversized input here.
    ensure!(raw.len() <= u16::MAX as usize, "padding scheme too large");
    for line in raw.lines() {
        let Some((key, value)) = line.split_once('=') else {
            bail!("invalid padding scheme line");
        };
        if key == "stop" {
            ensure!(value.parse::<u32>().is_ok(), "invalid padding stop");
            continue;
        }
        ensure!(key.parse::<u32>().is_ok(), "invalid padding packet number");
        for range in value.split(',') {
            if range == "c" && key != "0" {
                continue;
            }
            let Some((min, max)) = range.split_once('-') else {
                bail!("invalid padding range");
            };
            let min = min.parse::<u16>()?;
            let max = max.parse::<u16>()?;
            ensure!(min > 0 && max > 0, "padding size must be positive");
        }
    }
    PaddingFactory::new(raw.as_bytes()).ok_or_else(|| anyhow::anyhow!("invalid padding scheme"))
}

pub(crate) struct Frames;

impl Decoder for Frames {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, bytes: &mut BytesMut) -> io::Result<Option<Frame>> {
        let Some(frame) = Frame::from_bytes(bytes) else {
            return Ok(None);
        };
        let consumed = HEADER_OVERHEAD_SIZE + frame.data.len();
        debug_assert!(consumed <= bytes.len());
        bytes.advance(consumed);
        Ok(Some(frame))
    }
}
