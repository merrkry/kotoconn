//! AnyTLS v2 over verified TLS. The anytls crate owns frame and settings semantics;
//! this adapter supplies bounded I/O, carrier integration and session lifetimes.
mod client;
mod server;
mod session;
mod stream;
mod tls;
mod udp;
mod wire;

pub use client::Client;
pub use server::Server;

use std::time::Duration;

// These deadlines bound network handshakes and stalled remote writes.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(test)]
mod tests;
