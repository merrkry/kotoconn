//! Linux TUN packet processing and independently driven smoltcp connections.
mod packet;

mod device;
mod tcp;

mod reassembly;

mod udp;

mod endpoint;
pub use endpoint::{PacketIo, run};

mod linux;
pub use linux::{BoundTun, bind};

#[cfg(test)]
mod tests;
