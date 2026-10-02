//! Inspect application payload before routing, then replay it unchanged.
mod quic;
mod tls;

use bytes::Bytes;
use kotoconn_config::{SniffConfig, SniffProtocol, SniffResult};
use kotoconn_protocol::{BoxStream, Packet, Scope, prefix, queue};
use std::io;
use tokio::io::AsyncReadExt;

const MAX_BYTES: usize = 64 * 1024;
const MAX_PACKETS: usize = 32;

enum Outcome {
    NeedMore,
    Unknown,
    Found(SniffResult),
}

/// I/O errors remain session errors. Unrecognized, incomplete or oversized
/// payloads, timeout and interruption leave the flow without sniff metadata.
/// Dropping this future terminates inspection without returning or replaying data.
pub async fn tcp(
    mut stream: BoxStream,
    config: &SniffConfig,
    interruption: &Scope,
) -> io::Result<(BoxStream, Option<SniffResult>)> {
    let mut bytes = Vec::new();
    let mut acceptor = rustls::server::Acceptor::default();
    let mut fed = 0;

    let inspecting = async {
        let mut buffer = [0; 4096];
        loop {
            let outcome = if bytes.first() == Some(&22) {
                tls::records(&mut acceptor, &bytes[fed..])
            } else {
                http(&bytes)
            };
            fed = bytes.len();

            match outcome {
                Outcome::Found(result) => return Ok::<_, io::Error>(Some(result)),
                Outcome::Unknown => return Ok(None),
                Outcome::NeedMore => {}
            }
            if bytes.len() == MAX_BYTES {
                return Ok(None);
            }

            let remaining = (MAX_BYTES - bytes.len()).min(buffer.len());
            let count = stream.read(&mut buffer[..remaining]).await?;
            if count == 0 {
                return Ok(None);
            }
            bytes.extend_from_slice(&buffer[..count]);
        }
    };

    let result = tokio::select! {
        biased;
        _ = interruption.cancelled() => Ok(None),
        _ = tokio::time::sleep(config.timeout) => Ok(None),
        result = inspecting => result,
    };

    Ok((prefix(Bytes::from(bytes), stream), result?))
}

/// Each destination-specific UDP session is inspected once. Buffered datagrams
/// retain their addresses and order, including when a TUN worker takes over.
pub async fn udp(
    incoming: &mut queue::Receiver<Packet>,
    config: &SniffConfig,
    interruption: &Scope,
) -> Option<SniffResult> {
    let mut packets = Vec::new();
    let mut bytes = 0;
    let mut quic = quic::Inspector::default();

    let inspecting = async {
        while packets.len() < MAX_PACKETS && bytes < MAX_BYTES {
            let packet = incoming.recv().await?;
            bytes += packet.payload.len();
            let outcome = if bytes <= MAX_BYTES {
                quic.inspect(&packet.payload)
            } else {
                Outcome::Unknown
            };
            packets.push(packet);

            match outcome {
                Outcome::Found(result) => return Some(result),
                Outcome::Unknown => return None,
                Outcome::NeedMore => {}
            }
        }
        None
    };

    let result = tokio::select! {
        biased;
        _ = interruption.cancelled() => None,
        _ = tokio::time::sleep(config.timeout) => None,
        result = inspecting => result,
    };
    incoming.prepend(packets);
    result
}

fn http(bytes: &[u8]) -> Outcome {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut request = httparse::Request::new(&mut headers);
    match request.parse(bytes) {
        Ok(httparse::Status::Partial) => Outcome::NeedMore,
        Err(_) => Outcome::Unknown,
        Ok(httparse::Status::Complete(_)) => {
            let domain = request
                .headers
                .iter()
                .find(|header| header.name.eq_ignore_ascii_case("host"))
                .and_then(|header| std::str::from_utf8(header.value).ok())
                .and_then(domain_from_authority);

            Outcome::Found(SniffResult {
                protocol: SniffProtocol::Http,
                domain,
            })
        }
    }
}

fn domain_from_authority(authority: &str) -> Option<String> {
    let authority = authority.parse::<::http::uri::Authority>().ok()?;
    let host = authority
        .host()
        .trim_start_matches('[')
        .trim_end_matches(']');
    if host.parse::<std::net::IpAddr>().is_ok() || host.is_empty() {
        return None;
    }
    Some(host.to_ascii_lowercase())
}
