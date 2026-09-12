//! Single-user Shadowsocks 2022 AES-128-GCM adapters.
mod client;
mod server;
mod tcp;
use anyhow::Result;
pub use client::Client;
use kotoconn_protocol::{Target, target};
pub use server::Server;
use shadowsocks::{
    ServerConfig,
    config::ServerType,
    context::{Context, SharedContext},
    crypto::CipherKind,
    relay::{socks5::Address, udprelay::options::UdpSocketControlData},
};
use std::net::SocketAddr;

const METHOD: CipherKind = CipherKind::AEAD2022_BLAKE3_AES_128_GCM;
const PACKET_LIMIT: u64 = u64::MAX - (1 << 13);

struct Crypto {
    context: SharedContext,
    config: ServerConfig,
}

impl Crypto {
    fn new(password: &str, side: ServerType) -> Result<Self> {
        // The library requires a server address to derive protocol configuration;
        // this value is never used for I/O, which always goes through the carrier.
        Ok(Self {
            context: Context::new_shared(side),
            config: ServerConfig::new("127.0.0.1:0".parse::<SocketAddr>()?, password, METHOD)?,
        })
    }
}

fn address(value: Target) -> Address {
    match value {
        Target::Ip { address, port } => Address::SocketAddress(SocketAddr::new(address, port)),
        Target::Domain { name, port } => Address::DomainNameAddress(name, port),
    }
}

fn from_address(value: Address) -> Target {
    match value {
        Address::SocketAddress(value) => target(value),
        Address::DomainNameAddress(name, port) => Target::Domain { name, port },
    }
}

fn control(client: u64, server: u64, packet: u64) -> UdpSocketControlData {
    let mut result = UdpSocketControlData::default();
    result.client_session_id = client;
    result.server_session_id = server;
    result.packet_id = packet;
    result
}
