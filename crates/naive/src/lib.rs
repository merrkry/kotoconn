//! NaiveProxy CONNECT tunnels. Cronet owns outbound TLS/H2/H3; h2 and rustls
//! own inbound TLS/H2. Only the Naive padding codec lives in this adapter.
#[cfg(target_os = "linux")]
mod client;
#[cfg(target_os = "linux")]
mod native;
mod padding;
mod server;
#[cfg(target_os = "linux")]
mod transport;

#[cfg(target_os = "linux")]
pub use client::Client;
pub use server::Server;

use anyhow::{Result, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use kotoconn_protocol::{Target, target};
use std::net::SocketAddr;

fn authorization(username: &str, password: &str) -> Result<String> {
    ensure!(!username.is_empty(), "Naive username must not be empty");
    ensure!(
        !username.contains(':'),
        "Naive username must not contain ':'"
    );
    Ok(format!(
        "Basic {}",
        STANDARD.encode(format!("{username}:{password}"))
    ))
}

#[cfg(target_os = "linux")]
fn authority(destination: &Target) -> Result<String> {
    let value = match destination {
        Target::Ip { address, port } => SocketAddr::new(*address, *port).to_string(),
        Target::Domain { name, port } => {
            ensure!(valid_hostname(name), "invalid Naive destination hostname");
            format!("{name}:{port}")
        }
    };
    parse_authority(&value)?;
    Ok(value)
}

fn valid_hostname(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-._".contains(&c))
}

fn parse_authority(value: &str) -> Result<Target> {
    if let Ok(address) = value.parse::<SocketAddr>() {
        ensure!(
            address.port() != 0,
            "Naive destination requires a nonzero port"
        );
        return Ok(target(address));
    }

    let value: http::uri::Authority = value.parse()?;
    ensure!(
        valid_hostname(value.host()),
        "invalid Naive destination hostname"
    );
    let port = value
        .port_u16()
        .filter(|port| *port != 0)
        .ok_or_else(|| anyhow::anyhow!("Naive destination requires a nonzero port"))?;
    Ok(Target::Domain {
        name: value.host().into(),
        port,
    })
}
