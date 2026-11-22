use crate::{
    BoxStream,
    pool::{Lease, Pool},
};
use bytes::{Buf, Bytes};
use std::{
    cell::RefCell,
    io,
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    task::coop,
};

thread_local! {
    // A speculative read must not leave 64 KiB attached to every idle socket.
    // Native reads finish synchronously, so sockets can share this scratch.
    static TCP_RECEIVE: RefCell<Option<Lease>> = const { RefCell::new(None) };
}

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

/// A write error meaning the peer stopped reading while bytes it sent earlier
/// may still be readable, as after an AnyTLS FIN. A relay keeps forwarding the
/// other direction and discards this direction's input until it ends: closing
/// a source with unread input resets the connection, which can discard the
/// reply just forwarded to it. As with a half-closed TCP peer, the relay ends
/// when the source does.
#[derive(Debug)]
pub struct PeerStoppedReading;

impl PeerStoppedReading {
    pub fn error() -> io::Error {
        io::Error::new(io::ErrorKind::BrokenPipe, Self)
    }

    pub fn is(error: &io::Error) -> bool {
        error.get_ref().is_some_and(|inner| inner.is::<Self>())
    }
}

impl std::fmt::Display for PeerStoppedReading {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("peer stopped reading")
    }
}

impl std::error::Error for PeerStoppedReading {}

/// Chunk writes consume exactly the accepted prefix. Pending never changes the
/// caller's chunk. Receive credit is returned explicitly after destination acceptance.
/// Streams that can tell the peer will not read their writes fail them with
/// [`PeerStoppedReading`]; other write errors fail the whole relay.
pub trait Stream: AsyncRead + AsyncWrite + Send {
    /// True only after setup when a worker may poll the established byte stream
    /// directly. Codecs can decline and continue using the ordinary chunk relay.
    fn poll_direct(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<bool>> {
        Poll::Ready(Ok(false))
    }

    /// Called before relay I/O. Acceptance takes the peer and returns completion;
    /// dropping that future must cancel the handoff. Declining leaves peer intact.
    fn take_over<'a>(
        self: Pin<&'a mut Self>,
        _peer: &mut Option<BoxStream>,
    ) -> Option<futures_util::future::BoxFuture<'a, io::Result<(u64, u64)>>> {
        None
    }

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
            buffer.lease = None;
            return Poll::Ready(Ok(Bytes::new()));
        }
        // SAFETY: The lease was installed above and remained owned during the read.
        Poll::Ready(Ok(buffer.lease.take().expect("read lease").publish(n)))
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
    fn poll_direct(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<bool>> {
        Poll::Ready(Ok(true))
    }
    fn poll_read_chunk(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ChunkBuffer,
    ) -> Poll<io::Result<Bytes>> {
        let budget = ready!(coop::poll_proceed(cx));
        loop {
            ready!(self.poll_read_ready(cx))?;
            let result: io::Result<Bytes> = TCP_RECEIVE.with_borrow_mut(|scratch| {
                let lease = scratch.get_or_insert_with(|| buffer.pool.acquire(65536));
                let n = self.try_read(lease.as_mut())?;
                if n == 0 {
                    return Ok(Bytes::new());
                }
                if n < 16384 {
                    return Ok(buffer.pool.copy(&lease.as_ref()[..n]));
                }

                // SAFETY: The scratch lease stayed installed during try_read,
                // which initialized n bytes. No borrow crosses a suspension.
                Ok(scratch.take().expect("socket read scratch").freeze(n))
            });
            match result {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                result => {
                    budget.made_progress();
                    return Poll::Ready(result);
                }
            }
        }
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

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.inner.as_mut().poll_write_vectored(cx, bytes)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.as_mut().poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.as_mut().poll_shutdown(cx)
    }
}

impl Stream for Prefix {
    fn poll_direct(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<bool>> {
        if self.bytes.is_empty() {
            self.inner.as_mut().poll_direct(cx)
        } else {
            Poll::Ready(Ok(false))
        }
    }
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

#[derive(Default, PartialEq, Eq)]
enum Phase {
    #[default]
    Copying,
    /// The destination stopped reading; the source is read to its end.
    Draining,
    Done,
}

#[derive(Default)]
struct Direction {
    buffer: ChunkBuffer,
    bytes: Bytes,
    transferred: u64,
    eof: bool,
    phase: Phase,
}

impl Direction {
    fn poll(
        &mut self,
        cx: &mut Context<'_>,
        mut reader: Pin<&mut dyn Stream>,
        writer: Pin<&mut dyn Stream>,
    ) -> Poll<io::Result<()>> {
        if self.phase == Phase::Copying {
            match self.transfer(cx, reader.as_mut(), writer) {
                Poll::Ready(Err(error)) if PeerStoppedReading::is(&error) => {
                    reader.as_mut().consume_chunk(self.bytes.len());
                    self.bytes = Bytes::new();
                    self.phase = if self.eof {
                        Phase::Done
                    } else {
                        Phase::Draining
                    };
                }
                result => return result,
            }
        }
        if self.phase == Phase::Draining {
            return self.drain(cx, reader);
        }
        Poll::Ready(Ok(()))
    }

    fn drain(
        &mut self,
        cx: &mut Context<'_>,
        mut reader: Pin<&mut dyn Stream>,
    ) -> Poll<io::Result<()>> {
        for _ in 0..32 {
            let bytes = ready!(reader.as_mut().poll_read_chunk(cx, &mut self.buffer))?;
            if bytes.is_empty() {
                self.phase = Phase::Done;
                return Poll::Ready(Ok(()));
            }
            reader.as_mut().consume_chunk(bytes.len());
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn transfer(
        &mut self,
        cx: &mut Context<'_>,
        mut reader: Pin<&mut dyn Stream>,
        mut writer: Pin<&mut dyn Stream>,
    ) -> Poll<io::Result<()>> {
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
                self.phase = Phase::Done;
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

/// Routing and outbound construction precede this optional transfer of execution.
pub async fn relay(a: &mut BoxStream, mut b: BoxStream) -> io::Result<(u64, u64)> {
    if std::future::poll_fn(|cx| b.as_mut().poll_direct(cx)).await? {
        let mut peer = Some(b);
        if let Some(completion) = a.as_mut().take_over(&mut peer) {
            return completion.await;
        }
        // SAFETY: The takeover contract leaves a declined peer in its slot.
        b = peer.expect("declined stream takeover");
    }
    copy_bidirectional(a, &mut b).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A peer that already closed: its reply stays readable, writes are refused.
    struct Closed(Bytes);

    impl AsyncRead for Closed {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            out: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let length = out.remaining().min(self.0.len());
            out.put_slice(&self.0.split_to(length));
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for Closed {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(PeerStoppedReading::error()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl Stream for Closed {}

    #[tokio::test]
    async fn a_peer_that_stopped_reading_still_delivers_its_reply() {
        let (local, mut app) = tokio::io::duplex(64);
        let mut local: BoxStream = Box::pin(local);
        let peer: BoxStream = Box::pin(Closed(Bytes::from_static(b"rejected")));
        let client = async {
            // The client is still uploading when the reply arrives.
            app.write_all(&[1; 4096]).await.unwrap();
            let mut reply = Vec::new();
            app.read_to_end(&mut reply).await.unwrap();
            app.shutdown().await.unwrap();
            reply
        };
        let relaying = async { tokio::join!(relay(&mut local, peer), client) };
        let limit = std::time::Duration::from_secs(10);
        let (relayed, reply) = tokio::time::timeout(limit, relaying).await.unwrap();
        assert_eq!(relayed.unwrap(), (0, 8));
        assert_eq!(reply, b"rejected");
    }
}
