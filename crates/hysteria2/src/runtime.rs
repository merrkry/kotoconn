use kotoconn_protocol::Scope;
use quinn::{AsyncTimer, AsyncUdpSocket, Runtime};
use std::{future::Future, io, pin::Pin, sync::Arc, time::Instant};

/// Register Quinn's own endpoint and connection drivers in carrier completion.
#[derive(Debug)]
pub struct ScopedRuntime(pub Scope);

impl Runtime for ScopedRuntime {
    fn new_timer(&self, at: Instant) -> Pin<Box<dyn AsyncTimer>> {
        quinn::TokioRuntime.new_timer(at)
    }

    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        // Closing the owner cancels library drivers and drops their I/O resources.
        // A closed scope rejects new work and drops the supplied future immediately.
        let _ = self.0.spawn(async move {
            future.await;
            Ok(())
        });
    }

    fn wrap_udp_socket(&self, socket: std::net::UdpSocket) -> io::Result<Arc<dyn AsyncUdpSocket>> {
        quinn::TokioRuntime.wrap_udp_socket(socket)
    }
}

pub struct CloseScope(pub Scope);

impl Drop for CloseScope {
    fn drop(&mut self) {
        self.0.close();
    }
}

pub struct CloseConnection(pub quinn::Connection);

impl Drop for CloseConnection {
    fn drop(&mut self) {
        self.0.close(0u32.into(), b"");
    }
}
