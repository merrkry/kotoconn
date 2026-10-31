use std::time::Duration;

use crate::Target;

/// A health check that opens a TCP connection through a dialer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Probe {
    pub target: Target,
    /// Sent once connected; empty for protocols where the server speaks first.
    pub request: Vec<u8>,
    pub timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeResult {
    /// From the start of the probe to the first response bytes.
    pub latency: Duration,
    /// The first response bytes the connection delivered.
    pub response: Vec<u8>,
}
