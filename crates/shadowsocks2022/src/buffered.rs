use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Keep the crypto writer's input unchanged across pending writes.
///
/// shadowsocks 1.25.0 buffers an encrypted frame but reports the current input
/// length when that frame finishes. Tokio's copy can grow that input meanwhile.
/// Retain the original input until the write completes, without delaying writes
/// until flush. A retry may append bytes, but must preserve the pending prefix.
///
/// Upstream issue: <https://github.com/shadowsocks/shadowsocks-rust/issues/2175>.
pub(super) struct Buffered<S> {
    stream: S,
    pending: Option<Vec<u8>>,
}

impl<S> Buffered<S> {
    pub(super) fn new(stream: S) -> Self {
        Self {
            stream,
            pending: None,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Buffered<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, output)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Buffered<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let input = match &this.pending {
            Some(pending) => {
                if !data.starts_with(pending) {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Shadowsocks write retry changed its pending prefix",
                    )));
                }
                pending.as_slice()
            }
            // Stay below the crypto library's maximum plaintext frame size,
            // and bound the allocation needed only when a write is pending.
            None => &data[..data.len().min(8192)],
        };

        match Pin::new(&mut this.stream).poll_write(cx, input) {
            Poll::Pending => {
                if this.pending.is_none() {
                    this.pending = Some(input.to_vec());
                }
                Poll::Pending
            }
            result => {
                this.pending = None;
                result
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
