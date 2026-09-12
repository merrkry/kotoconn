use crate::{Scope, Target};
use anyhow::Result;
use bytes::Bytes;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::mpsc,
};

pub trait Stream: AsyncRead + AsyncWrite + Send {}

impl<T: AsyncRead + AsyncWrite + Send> Stream for T {}

pub type BoxStream = Pin<Box<dyn Stream>>;

#[derive(Debug, Clone)]
pub struct Packet {
    pub target: Target,
    /// Shared ownership lets ingress retain resource accounting through routing queues.
    pub payload: Bytes,
}

/// Each message is one complete datagram. No framing, mux or retransmission here.
/// Ingress on a shared socket uses try_send: a full session queue drops that packet.
pub struct Datagram {
    pub tx: mpsc::Sender<Packet>,
    pub rx: mpsc::Receiver<Packet>,
    pub scope: Scope,
}

impl Drop for Datagram {
    fn drop(&mut self) {
        self.scope.close();
    }
}

pub fn packet_pair(scope: Scope) -> (Datagram, Datagram) {
    let (a_tx, b_rx) = mpsc::channel(64);
    let (b_tx, a_rx) = mpsc::channel(64);
    (
        Datagram {
            tx: a_tx,
            rx: a_rx,
            scope: scope.clone(),
        },
        Datagram {
            tx: b_tx,
            rx: b_rx,
            scope,
        },
    )
}

/// Decouples protocol progress from the caller's read/write scheduling. Reading
/// alone advances setup, including server-first protocols. Drop cancels setup/I/O.
pub fn stream_task(
    scope: Scope,
    connect: impl Future<Output = Result<BoxStream>> + Send + 'static,
) -> Result<BoxStream> {
    let (local, mut remote) = tokio::io::duplex(64 * 1024);
    let handle = scope.clone();
    scope.spawn(async move {
        let mut stream = connect.await?;
        tokio::io::copy_bidirectional(&mut remote, &mut stream).await?;
        Ok(())
    })?;
    Ok(Box::pin(OwnedStream {
        inner: local,
        scope: handle,
    }))
}

struct OwnedStream {
    inner: tokio::io::DuplexStream,
    scope: Scope,
}

impl Drop for OwnedStream {
    fn drop(&mut self) {
        self.scope.close();
    }
}

impl AsyncRead for OwnedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for OwnedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
