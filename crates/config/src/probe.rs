use std::time::Duration;

use crate::Target;

/// A health check that opens a TCP connection through a dialer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Probe {
    pub target: Target,
    /// Sent once connected; empty for protocols where the server speaks first.
    pub request: Vec<u8>,
    /// Response bytes to read before the probe completes, unless the server
    /// closes the connection first.
    pub length: usize,
    pub timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeResult {
    /// From the start of the probe to the first response bytes.
    pub latency: Duration,
    /// The response bytes read, at most the requested length.
    pub response: Vec<u8>,
}
