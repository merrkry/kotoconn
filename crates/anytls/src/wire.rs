use crate::padding::PaddingScheme;
use anyhow::{Result, ensure};
use fast_socks5::util::target_addr::{TargetAddr, read_address};
use kotoconn_protocol::Endpoint;
use sha2::{Digest, Sha256};
use std::net::SocketAddr;
use subtle::ConstantTimeEq;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

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
    padding: &PaddingScheme,
) -> Result<()> {
    let length = padding.authentication_padding();
    let mut bytes = Vec::with_capacity(34 + usize::from(length));
    bytes.extend_from_slice(password);
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.resize(34 + usize::from(length), 0);
    io.write_all(&bytes).await?;
    io.flush().await?;
    Ok(())
}

pub(crate) fn address(target: &Endpoint) -> Result<Vec<u8>> {
    let address = match target {
        Endpoint::Ip { address, port } => TargetAddr::Ip(SocketAddr::new(*address, *port)),
        Endpoint::Domain { name, port } => {
            ensure!(!name.is_empty(), "empty destination domain");
            TargetAddr::Domain(name.clone(), *port)
        }
    };
    Ok(address.to_be_bytes()?)
}

pub(crate) async fn read_target(io: &mut (impl AsyncRead + Unpin)) -> Result<Endpoint> {
    let family = io.read_u8().await?;
    read_target_family(io, family).await
}

pub(crate) async fn read_target_family(
    io: &mut (impl AsyncRead + Unpin),
    family: u8,
) -> Result<Endpoint> {
    Ok(match read_address(io, family).await? {
        TargetAddr::Ip(socket) => kotoconn_protocol::target(socket),
        TargetAddr::Domain(name, port) => {
            ensure!(!name.is_empty(), "empty destination domain");
            Endpoint::Domain { name, port }
        }
    })
}
