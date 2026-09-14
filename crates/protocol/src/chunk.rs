use crate::{
    BoxStream,
    pool::{Lease, Pool},
};
use bytes::{Buf, Bytes};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Reusable receive storage for I/O implementations that cannot return owned data.
pub struct ChunkBuffer {
    pool: Pool,
    lease: Option<Lease>,
}

impl Default for ChunkBuffer {
    fn default() -> Self {
        Self {
            pool: Pool::shared(),
            lease: None,
        }
    }
}

/// Chunk writes consume exactly the accepted prefix. Pending never changes the
/// caller's chunk. Receive credit is returned explicitly after destination acceptance.
pub trait Stream: AsyncRead + AsyncWrite + Send {
    fn poll_read_chunk(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ChunkBuffer,
    ) -> Poll<io::Result<Bytes>> {
        let lease = buffer
            .lease
            .get_or_insert_with(|| buffer.pool.acquire(65536));
        let mut out = ReadBuf::new(lease.as_mut());
        ready!(self.as_mut().poll_read(cx, &mut out))?;
        let n = out.filled().len();
        if n == 0 {
            return Poll::Ready(Ok(Bytes::new()));
        }
        // SAFETY: The lease was installed above and remained owned during the read.
        Poll::Ready(Ok(buffer.lease.take().expect("read lease").freeze(n)))
    }

    fn poll_write_chunk(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut Bytes,
    ) -> Poll<io::Result<usize>> {
        let n = ready!(self.as_mut().poll_write(cx, bytes))?;
        bytes.advance(n);
        Poll::Ready(Ok(n))
    }

    fn consume_chunk(self: Pin<&mut Self>, _bytes: usize) {}
}

impl Stream for tokio::net::TcpStream {
    fn poll_read_chunk(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ChunkBuffer,
    ) -> Poll<io::Result<Bytes>> {
        ready!(self.poll_read_ready(cx))?;
        let lease = buffer
            .lease
            .get_or_insert_with(|| buffer.pool.acquire(65536));
        let mut out = ReadBuf::new(lease.as_mut());
        ready!(self.as_mut().poll_read(cx, &mut out))?;
        let n = out.filled().len();
        if n == 0 {
            return Poll::Ready(Ok(Bytes::new()));
        }
        // SAFETY: The read used the lease still held by this buffer.
        Poll::Ready(Ok(buffer
            .lease
            .take()
            .expect("socket read lease")
            .freeze(n)))
    }
}

impl Stream for tokio::io::DuplexStream {}

struct Prefix {
    bytes: Bytes,
    inner: BoxStream,
    prefix_read: bool,
}

/// Preserve a protocol handshake's read-ahead without hiding native chunk I/O.
pub fn prefix(bytes: Bytes, inner: BoxStream) -> BoxStream {
    if bytes.is_empty() {
        return inner;
    }
    Box::pin(Prefix {
        bytes,
        inner,
        prefix_read: false,
    })
}

impl AsyncRead for Prefix {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if !self.bytes.is_empty() {
            let n = out.remaining().min(self.bytes.len());
            out.put_slice(&self.bytes.split_to(n));
            return Poll::Ready(Ok(()));
        }
        self.inner.as_mut().poll_read(cx, out)
    }
}

impl AsyncWrite for Prefix {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.inner.as_mut().poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.as_mut().poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.as_mut().poll_shutdown(cx)
    }
}

impl Stream for Prefix {
    fn poll_read_chunk(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ChunkBuffer,
    ) -> Poll<io::Result<Bytes>> {
        self.prefix_read = !self.bytes.is_empty();
        if self.prefix_read {
            return Poll::Ready(Ok(std::mem::take(&mut self.bytes)));
        }
        self.inner.as_mut().poll_read_chunk(cx, buffer)
    }
    fn poll_write_chunk(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut Bytes,
    ) -> Poll<io::Result<usize>> {
        self.inner.as_mut().poll_write_chunk(cx, bytes)
    }
    fn consume_chunk(mut self: Pin<&mut Self>, bytes: usize) {
        if !self.prefix_read {
            self.inner.as_mut().consume_chunk(bytes);
        }
    }
}

#[derive(Default)]
struct Direction {
    buffer: ChunkBuffer,
    bytes: Bytes,
    transferred: u64,
    eof: bool,
    done: bool,
}

impl Direction {
    fn poll(
        &mut self,
        cx: &mut Context<'_>,
        mut reader: Pin<&mut dyn Stream>,
        mut writer: Pin<&mut dyn Stream>,
    ) -> Poll<io::Result<()>> {
        if self.done {
            return Poll::Ready(Ok(()));
        }
        for _ in 0..32 {
            if self.bytes.is_empty() && !self.eof {
                match reader.as_mut().poll_read_chunk(cx, &mut self.buffer) {
                    Poll::Ready(result) => {
                        self.bytes = result?;
                        self.eof = self.bytes.is_empty();
                    }
                    Poll::Pending => {
                        ready!(writer.as_mut().poll_flush(cx))?;
                        return Poll::Pending;
                    }
                }
            }
            if self.eof {
                ready!(writer.as_mut().poll_shutdown(cx))?;
                self.done = true;
                return Poll::Ready(Ok(()));
            }
            let before = self.bytes.len();
            let n = ready!(writer.as_mut().poll_write_chunk(cx, &mut self.bytes))?;
            debug_assert_eq!(before - self.bytes.len(), n);
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            reader.as_mut().consume_chunk(n);
            self.transferred += n as u64;
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// One executor advances both halves without a duplex buffer or a stream lock.
pub async fn copy_bidirectional(a: &mut BoxStream, b: &mut BoxStream) -> io::Result<(u64, u64)> {
    let mut upload = Direction::default();
    let mut download = Direction::default();
    std::future::poll_fn(|cx| {
        let first = upload.poll(cx, a.as_mut(), b.as_mut())?;
        let second = download.poll(cx, b.as_mut(), a.as_mut())?;
        ready!(first);
        ready!(second);
        Poll::Ready(Ok((upload.transferred, download.transferred)))
    })
    .await
}
