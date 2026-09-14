//! Linux TUN packet processing and worker-owned TCP connections.
mod endpoint;
mod linux;
#[cfg(target_os = "linux")]
mod offload;
mod packet;
mod pool;
mod reassembly;
mod storage;
mod tcp;
mod tcp_storage;
mod transmit;
mod udp;
mod worker;

pub use endpoint::{PacketReceive, PacketSend, ReceiveBuffer, Received, run};
pub use linux::{BoundTun, bind};

#[cfg(test)]
mod parallel_tests;
#[cfg(test)]
mod tests;
