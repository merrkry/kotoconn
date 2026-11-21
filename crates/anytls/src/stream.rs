use crate::{frame::Sid, session::Write};
use bytes::{Buf, Bytes};
use kotoconn_protocol::{ChunkBuffer, Stream, WorkGuard, pool::Pool};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll, ready},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{OwnedSemaphorePermit, mpsc, oneshot},
};
use tokio_util::sync::{CancellationToken, PollSender, WaitForCancellationFutureOwned};

pub(crate) const CHUNK: usize = 16 * 1024;

pub(crate) struct Received {
    pub bytes: Bytes,
    pub credit: OwnedSemaphorePermit,
}

/// Asks the session to remove a stream and send FIN unless the peer closed it.
pub(crate) struct Closed {
    pub sid: Sid,
    pub _work: Option<WorkGuard>,
    /// Completes once the FIN reached the transport; dropped if the session
    /// ends first.
    pub complete: Option<oneshot::Sender<()>>,
}

#[derive(Default)]
pub(crate) struct Status {
    pub closed: CancellationToken,
    pub accepted: CancellationToken,
    pub error: OnceLock<String>,
}

impl Status {
    pub fn close(&self, error: Option<String>) {
        if let Some(error) = error {
            let _ = self.error.set(error);
        }
        self.closed.cancel();
    }

    fn error(&self) -> io::Error {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            self.error
                .get()
                .cloned()
                .unwrap_or_else(|| "AnyTLS stream closed".into()),
        )
    }
}

/// Received bytes not yet accepted by the reader, holding their receive credit.
struct Held {
    bytes: Bytes,
    _credit: OwnedSemaphorePermit,
    /// Bytes handed out by `poll_read_chunk` and awaiting `consume_chunk`.
    lent: usize,
}

enum Slot {
    Reserved,
    Discard,
}

pub(crate) struct LogicalStream {
    sid: Sid,
    incoming: mpsc::UnboundedReceiver<Received>,
    held: Option<Held>,
    outgoing: PollSender<Write>,
    closing: mpsc::UnboundedSender<Closed>,
    work: Option<WorkGuard>,
    status: Arc<Status>,
    closed: Pin<Box<WaitForCancellationFutureOwned>>,
    pool: Pool,
    shutdown: bool,
    closing_complete: Option<oneshot::Receiver<()>>,
}

impl LogicalStream {
    pub async fn wait_handshake(&mut self) -> io::Result<()> {
        tokio::select! {
            biased;
            _ = self.status.accepted.cancelled() => Ok(()),
            _ = self.status.closed.cancelled() => Err(self.status.error()),
        }
    }

    pub fn new(
        sid: Sid,
        incoming: mpsc::UnboundedReceiver<Received>,
        outgoing: mpsc::Sender<Write>,
        closing: mpsc::UnboundedSender<Closed>,
        status: Arc<Status>,
        work: Option<WorkGuard>,
    ) -> Self {
        Self {
            sid,
            incoming,
            held: None,
            outgoing: PollSender::new(outgoing),
            closing,
            work,
            closed: Box::pin(status.closed.clone().cancelled_owned()),
            status,
            pool: Pool::shared(),
            shutdown: false,
            closing_complete: None,
        }
    }

    /// Received bytes are buffered until the peer's FIN; then end of stream follows.
    fn poll_held(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<&mut Held>>> {
        if self.held.is_none() {
            match ready!(self.incoming.poll_recv(cx)) {
                Some(Received { bytes, credit }) => {
                    self.held = Some(Held {
                        bytes,
                        _credit: credit,
                        lent: 0,
                    });
                }
                None if self.status.error.get().is_some() => {
                    return Poll::Ready(Err(self.status.error()));
                }
                None => return Poll::Ready(Ok(None)),
            }
        }
        Poll::Ready(Ok(self.held.as_mut()))
    }

    fn poll_reserve(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Slot>> {
        loop {
            if self.shutdown {
                return Poll::Ready(Err(self.status.error()));
            }
            if self.status.closed.is_cancelled() {
                let peer_closed = self.status.error.get().is_none();
                let unread = self.held.is_some() || !self.incoming.is_empty();
                if peer_closed && unread {
                    return Poll::Ready(Ok(Slot::Discard));
                }
                return Poll::Ready(Err(self.status.error()));
            }
            if let Poll::Ready(reserved) = self.outgoing.poll_reserve(cx) {
                reserved.map_err(|_| self.status.error())?;
                return Poll::Ready(Ok(Slot::Reserved));
            }
            // A FIN must reach a writer waiting for queue space as well.
            ready!(self.closed.as_mut().poll(cx));
        }
    }

    /// Requires a slot from `poll_reserve`.
    fn send(&mut self, data: Bytes) -> io::Result<()> {
        let write = Write::Push {
            sid: self.sid,
            data,
        };
        self.outgoing
            .send_item(write)
            .map_err(|_| self.status.error())
    }
}

impl Drop for LogicalStream {
    fn drop(&mut self) {
        // At most a shutdown and a drop notice exist per admitted stream, so
        // this channel stays bounded independently of data backpressure.
        let _ = self.closing.send(Closed {
            sid: self.sid,
            _work: self.work.take(),
            complete: None,
        });
    }
}

impl AsyncRead for LogicalStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if out.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let Some(held) = ready!(self.poll_held(cx))? else {
            return Poll::Ready(Ok(()));
        };
        debug_assert_eq!(held.lent, 0);
        let length = out.remaining().min(held.bytes.len());
        out.put_slice(&held.bytes.split_to(length));
        if held.bytes.is_empty() {
            self.held = None;
        }
        Poll::Ready(Ok(()))
    }
}

/// Writes queue PSH frames behind this stream's earlier frames. The peer's FIN
/// closes both directions, so nothing written afterwards is ever read, yet
/// bytes that arrived before it stay readable. Until the reader drains those,
/// writes are accepted and dropped: failing them aborts a relay copying both
/// ways before it forwards the bytes, and blocking them deadlocks a caller
/// that reads only after its write completes. Later writes fail.
impl AsyncWrite for LogicalStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if let Slot::Discard = ready!(self.poll_reserve(cx))? {
            return Poll::Ready(Ok(bytes.len()));
        }
        let length = bytes.len().min(CHUNK);
        let data = self.pool.copy(&bytes[..length]);
        self.send(data)?;
        Poll::Ready(Ok(length))
    }

    /// Like a TCP send buffer: accepted writes already belong to the session
    /// writer, which flushes whenever it drains its queue. Failing to send them
    /// surfaces on later writes and on shutdown.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// Completes once FIN and every earlier write reached the transport, or
    /// the peer closed the stream first.
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.shutdown {
            self.shutdown = true;
            if !self.status.closed.is_cancelled() {
                let (complete, completed) = oneshot::channel();
                let closed = Closed {
                    sid: self.sid,
                    _work: None,
                    complete: Some(complete),
                };
                self.closing.send(closed).map_err(|_| self.status.error())?;
                self.closing_complete = Some(completed);
            }
        }
        if let Some(completed) = &mut self.closing_complete {
            let sent = ready!(Pin::new(completed).poll(cx));
            self.closing_complete = None;
            sent.map_err(|_| self.status.error())?;
        }
        // A session failure or a rejected handshake leaves an error behind.
        match self.status.error.get() {
            Some(_) => Poll::Ready(Err(self.status.error())),
            None => Poll::Ready(Ok(())),
        }
    }
}

impl Stream for LogicalStream {
    fn poll_read_chunk(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _buffer: &mut ChunkBuffer,
    ) -> Poll<io::Result<Bytes>> {
        let Some(held) = ready!(self.poll_held(cx))? else {
            return Poll::Ready(Ok(Bytes::new()));
        };
        debug_assert_eq!(held.lent, 0);
        held.lent = held.bytes.len();
        Poll::Ready(Ok(held.bytes.clone()))
    }

    fn consume_chunk(mut self: Pin<&mut Self>, bytes: usize) {
        let Some(held) = &mut self.held else {
            debug_assert_eq!(bytes, 0, "consumed bytes that were never read");
            return;
        };
        debug_assert!(bytes <= held.lent);
        held.lent -= bytes;
        held.bytes.advance(bytes);
        if held.bytes.is_empty() {
            // Dropping the chunk returns its credit to the session reader.
            debug_assert_eq!(held.lent, 0);
            self.held = None;
        }
    }

    fn poll_write_chunk(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut Bytes,
    ) -> Poll<io::Result<usize>> {
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if let Slot::Discard = ready!(self.poll_reserve(cx))? {
            let length = bytes.len();
            bytes.clear();
            return Poll::Ready(Ok(length));
        }
        let data = bytes.split_to(bytes.len().min(CHUNK));
        let length = data.len();
        self.send(data)?;
        Poll::Ready(Ok(length))
    }
}
