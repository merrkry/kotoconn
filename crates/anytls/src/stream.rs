use crate::session::{Dropped, Message};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use kotoconn_protocol::{Stream, WorkGuard};
use std::{
    io,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll, ready},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{OwnedSemaphorePermit, mpsc, oneshot},
};
use tokio_util::{
    sync::WaitForCancellationFutureOwned,
    sync::{CancellationToken, PollSender},
};

pub(crate) const CHUNK: usize = 16 * 1024;

pub(crate) struct Received {
    pub bytes: Bytes,
    pub _credit: OwnedSemaphorePermit,
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

pub(crate) struct LogicalStream {
    sid: u32,
    incoming: mpsc::UnboundedReceiver<Received>,
    pending: Option<Received>,
    outgoing: PollSender<Message>,
    dropped: mpsc::UnboundedSender<Dropped>,
    work: Option<WorkGuard>,
    status: Arc<Status>,
    closed: Pin<Box<WaitForCancellationFutureOwned>>,
    flushing: Option<BoxFuture<'static, io::Result<()>>>,
    shutdown: bool,
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
        sid: u32,
        incoming: mpsc::UnboundedReceiver<Received>,
        outgoing: mpsc::Sender<Message>,
        dropped: mpsc::UnboundedSender<Dropped>,
        status: Arc<Status>,
        work: Option<WorkGuard>,
    ) -> Self {
        Self {
            sid,
            incoming,
            pending: None,
            outgoing: PollSender::new(outgoing),
            dropped,
            work,
            closed: Box::pin(status.closed.clone().cancelled_owned()),
            status,
            flushing: None,
            shutdown: false,
        }
    }

    fn poll_ack(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(flushing) = &mut self.flushing {
            let result = ready!(flushing.as_mut().poll(cx));
            self.flushing = None;
            result?;
        }
        Poll::Ready(Ok(()))
    }

    fn ack(&mut self, receiver: oneshot::Receiver<io::Result<()>>) {
        self.flushing = Some(Box::pin(async move {
            receiver
                .await
                .map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?
        }));
    }
}

impl Drop for LogicalStream {
    fn drop(&mut self) {
        // Only one handle exists per SID; the drop channel contains at most one
        // entry per admitted stream, independently of data-channel backpressure.
        let _ = self.dropped.send(Dropped {
            sid: self.sid,
            _work: self.work.take(),
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

        if self.pending.is_none() {
            match ready!(self.incoming.poll_recv(cx)) {
                Some(bytes) => self.pending = Some(bytes),
                None => {
                    return Poll::Ready(match self.status.error.get() {
                        Some(_) => Err(self.status.error()),
                        None => Ok(()),
                    });
                }
            }
        }
        // SAFETY: The branch above either installs a received chunk or returns.
        let pending = self.pending.as_mut().expect("received chunk");
        let length = out.remaining().min(pending.bytes.len());
        out.put_slice(&pending.bytes.split_to(length));
        if pending.bytes.is_empty() {
            self.pending = None;
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for LogicalStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.shutdown || self.closed.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(self.status.error()));
        }
        ready!(self.poll_ack(cx))?;
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }

        ready!(self.outgoing.poll_reserve(cx)).map_err(|_| self.status.error())?;
        let length = bytes.len().min(CHUNK);
        let message = Message::Data {
            sid: self.sid,
            bytes: Bytes::copy_from_slice(&bytes[..length]),
        };
        self.outgoing
            .send_item(message)
            .map_err(|_| self.status.error())?;
        Poll::Ready(Ok(length))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.flushing.is_none() {
            if self.status.closed.is_cancelled() {
                return Poll::Ready(Err(self.status.error()));
            }
            ready!(self.outgoing.poll_reserve(cx)).map_err(|_| self.status.error())?;
            let (send, receive) = oneshot::channel();
            let message = Message::Flush {
                sid: self.sid,
                complete: send,
            };
            self.outgoing
                .send_item(message)
                .map_err(|_| self.status.error())?;
            self.ack(receive);
        }
        self.poll_ack(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.poll_ack(cx))?;
        if self.shutdown || self.status.closed.is_cancelled() {
            return Poll::Ready(Ok(()));
        }

        ready!(self.outgoing.poll_reserve(cx)).map_err(|_| self.status.error())?;
        let (send, receive) = oneshot::channel();
        let message = Message::Close {
            sid: self.sid,
            complete: send,
        };
        self.outgoing
            .send_item(message)
            .map_err(|_| self.status.error())?;
        self.shutdown = true;
        self.ack(receive);
        self.poll_ack(cx)
    }
}

impl Stream for LogicalStream {}
