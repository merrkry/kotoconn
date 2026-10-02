use super::Outcome;
use kotoconn_config::{SniffProtocol, SniffResult};

pub(super) fn records(acceptor: &mut rustls::server::Acceptor, mut bytes: &[u8]) -> Outcome {
    while !bytes.is_empty() {
        match acceptor.read_tls(&mut bytes) {
            Ok(0) | Err(_) => return Outcome::Unknown,
            Ok(_) => {}
        }
        match acceptor.accept() {
            Ok(Some(accepted)) => {
                return Outcome::Found(SniffResult {
                    protocol: SniffProtocol::Tls,
                    domain: accepted.client_hello().server_name().map(str::to_owned),
                });
            }
            Ok(None) => {}
            Err(_) => return Outcome::Unknown,
        }
    }
    Outcome::NeedMore
}

/// QUIC CRYPTO carries handshake messages without the TLS record header.
pub(super) fn handshake(bytes: &[u8]) -> Outcome {
    if bytes.len() < 4 {
        return Outcome::NeedMore;
    }
    if bytes[0] != 1 {
        return Outcome::Unknown;
    }
    let size = ((bytes[1] as usize) << 16) | ((bytes[2] as usize) << 8) | bytes[3] as usize;
    let size = size + 4;
    if size > super::MAX_BYTES {
        return Outcome::Unknown;
    }
    if bytes.len() < size {
        return Outcome::NeedMore;
    }

    let mut acceptor = rustls::server::Acceptor::default();
    let mut outcome = Outcome::NeedMore;
    for chunk in bytes[..size].chunks(16384) {
        let mut record = vec![22, 3, 3];
        debug_assert!(chunk.len() <= 16384);
        // SAFETY: chunks(16384) bounds each record length below u16::MAX.
        record.extend_from_slice(&(chunk.len() as u16).to_be_bytes());
        record.extend_from_slice(chunk);
        outcome = records(&mut acceptor, &record);
        if !matches!(outcome, Outcome::NeedMore) {
            break;
        }
    }
    if let Outcome::Found(result) = &mut outcome {
        result.protocol = SniffProtocol::Quic;
    }
    outcome
}
