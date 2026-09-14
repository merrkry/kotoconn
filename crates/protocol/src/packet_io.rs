use crate::{Packet, Scope, scoped::Shared};
use std::{
    io,
    sync::Arc,
    task::{Context, Poll},
};

pub type BoxPacketIo = Box<dyn PacketIo>;

/// Prefix progress counts complete datagrams. Empty payloads remain valid.
/// Pending leaves output/input unchanged; an error accepts no new datagrams.
pub trait PacketIo: Send {
    fn poll_recv(&mut self, cx: &mut Context<'_>, out: &mut Vec<Packet>)
    -> Poll<io::Result<usize>>;
    fn poll_send(&mut self, cx: &mut Context<'_>, packets: &[Packet]) -> Poll<io::Result<usize>>;
}

pub fn packet_io_task(scope: Scope, io: BoxPacketIo) -> anyhow::Result<BoxPacketIo> {
    let shared = crate::scoped::installed(&scope, io)?;
    Ok(Box::new(Scoped { shared, scope }))
}

struct Scoped {
    shared: Arc<Shared<BoxPacketIo>>,
    scope: Scope,
}

impl Drop for Scoped {
    fn drop(&mut self) {
        self.shared.close();
        self.scope.close();
    }
}

impl PacketIo for Scoped {
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        out: &mut Vec<Packet>,
    ) -> Poll<io::Result<usize>> {
        self.shared.reader.register(cx.waker());
        self.shared
            .with(|io| io.poll_recv(cx, out))
            .unwrap_or(Poll::Ready(Ok(0)))
    }
    fn poll_send(&mut self, cx: &mut Context<'_>, packets: &[Packet]) -> Poll<io::Result<usize>> {
        self.shared.writer.register(cx.waker());
        self.shared
            .with(|io| io.poll_send(cx, packets))
            .unwrap_or(Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Idle(Arc<AtomicBool>);
    impl Drop for Idle {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    impl PacketIo for Idle {
        fn poll_recv(
            &mut self,
            _: &mut Context<'_>,
            _: &mut Vec<Packet>,
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
        fn poll_send(&mut self, _: &mut Context<'_>, _: &[Packet]) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn scope_close_revokes_an_unpolled_transferred_transport() {
        let scope = Scope::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let mut io = packet_io_task(scope.clone(), Box::new(Idle(dropped.clone()))).unwrap();
        scope.close();
        scope.wait().await;
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(
            std::future::poll_fn(|cx| io.poll_recv(cx, &mut Vec::new()))
                .await
                .unwrap(),
            0
        );
    }
}
