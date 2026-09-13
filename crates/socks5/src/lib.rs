//! SOCKS5 CONNECT and UDP ASSOCIATE. Association framing stays in this crate.
mod client;
mod server;
use anyhow::{Result, ensure};
pub use client::Client;
use fast_socks5::util::target_addr::TargetAddr;
use kotoconn_protocol::{Packet, Target, target};
pub use server::Server;
use std::net::SocketAddr;

fn address(value: Target) -> TargetAddr {
    match value {
        Target::Ip { address, port } => TargetAddr::Ip(SocketAddr::new(address, port)),
        Target::Domain { name, port } => TargetAddr::Domain(name, port),
    }
}

fn from_address(value: TargetAddr) -> Target {
    match value {
        TargetAddr::Ip(value) => target(value),
        TargetAddr::Domain(name, port) => Target::Domain { name, port },
    }
}

fn encode(packet: Packet) -> Result<Vec<u8>> {
    let mut wire = fast_socks5::new_udp_header(address(packet.target))?;
    wire.extend(packet.payload);
    ensure!(wire.len() <= 65507, "SOCKS datagram too large");
    Ok(wire)
}

async fn decode(wire: &[u8]) -> Result<Packet> {
    ensure!(
        wire.len() >= 4 && wire[..2] == [0, 0],
        "invalid SOCKS UDP reserved field"
    );
    let (fragment, destination, payload) = fast_socks5::parse_udp_request(wire).await?;
    // RFC 1928 allows implementations without fragmentation to discard FRAG != 0.
    ensure!(fragment == 0, "SOCKS UDP fragmentation is not supported");
    Ok(Packet {
        target: from_address(destination),
        payload: payload.to_vec().into(),
    })
}
