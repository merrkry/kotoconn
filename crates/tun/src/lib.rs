//! Linux TUN packet processing and independently driven smoltcp connections.
mod device;
mod endpoint;
mod linux;
#[cfg(target_os = "linux")]
mod offload;
mod packet;
mod reassembly;
mod tcp;
mod udp;

pub use endpoint::{PacketIo, run};
pub use linux::{BoundTun, bind};

#[cfg(test)]
mod tests;
