//! DNS forwarding through Kotoconn carriers and UDP/TCP serving.

mod cache;
mod runtime;
mod server;
mod service;

pub use server::{BoundListener, Handler, bind};
pub use service::Service;
