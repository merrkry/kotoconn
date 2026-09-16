use crate::wire;
use anyhow::{Result, bail, ensure};
use kotoconn_protocol::{BoxStream, Datagram, Packet, Scope, Target, packet_pair};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const SENTINEL: &str = "sp.v2.udp-over-tcp.arpa";

pub(crate) fn sentinel() -> Target {
    Target::Domain {
        name: SENTINEL.into(),
        port: 0,
    }
}

pub(crate) fn is_sentinel(target: &Target) -> bool {
    matches!(target, Target::Domain { name, .. } if name == SENTINEL)
}

pub(crate) fn request(destination: &Target) -> Result<Vec<u8>> {
    let mut request = vec![1];
    request.extend(wire::address(destination)?);
    Ok(request)
}

pub(crate) async fn read_request(stream: &mut BoxStream) -> Result<Option<Target>> {
    let mode = stream.read_u8().await?;
    ensure!(mode <= 1, "invalid UoT mode");
    let destination = wire::read_target(stream).await?;
    Ok((mode == 1).then_some(destination))
}

pub(crate) fn start(
    stream: BoxStream,
    destination: Option<Target>,
    scope: Scope,
) -> Result<Datagram> {
    let (mut user, driver) = packet_pair(scope.clone());
    user.single_target = destination.clone();
    scope.spawn(async move {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let mut driver = driver;
        let read = async {
            loop {
                let packet = read_packet(&mut reader, destination.as_ref()).await?;
                // An association has no per-packet acknowledgement; preserve the
                // runtime's drop-on-overflow policy without blocking other flows.
                let _ = driver.tx.try_send(packet);
            }
        };
        let write = async {
            while let Some(packet) = driver.rx.recv().await {
                if destination
                    .as_ref()
                    .is_some_and(|target| *target != packet.target)
                {
                    tracing::warn!("UoT packet target differs from connected destination");
                    continue;
                }
                write_packet(&mut writer, &packet, destination.is_some()).await?;
            }
            Ok(())
        };
        tokio::select! {
            result = read => result,
            result = write => result,
        }
    })?;
    Ok(user)
}

async fn read_packet(
    reader: &mut (impl AsyncRead + Unpin),
    connected: Option<&Target>,
) -> Result<Packet> {
    let target = if let Some(destination) = connected {
        destination.clone()
    } else {
        // UoT datagrams use 0/1/2 for IPv4/IPv6/domain. Reuse the SOCKS codec
        // for the rest of the address; the initial request uses ordinary SOCKS.
        let family = match reader.read_u8().await? {
            0 => 1,
            1 => 4,
            2 => 3,
            _ => bail!("invalid UoT address family"),
        };
        wire::read_target_family(reader, family).await?
    };
    let length = reader.read_u16().await?;
    let mut payload = vec![0; usize::from(length)];
    reader.read_exact(&mut payload).await?;
    Ok(Packet {
        target,
        payload: payload.into(),
    })
}

async fn write_packet(
    writer: &mut (impl AsyncWrite + Unpin),
    packet: &Packet,
    connected: bool,
) -> Result<()> {
    let length = u16::try_from(packet.payload.len())?;
    let mut bytes = if connected {
        Vec::new()
    } else {
        wire::address(&packet.target)?
    };
    if !connected {
        // SAFETY: fast-socks5 serializes every address with an initial family byte.
        debug_assert!(!bytes.is_empty());
        bytes[0] = match bytes[0] {
            1 => 0,
            4 => 1,
            3 => 2,
            _ => bail!("invalid SOCKS address family"),
        };
    }
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(&packet.payload);
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}
