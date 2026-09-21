//! Route Hysteria streams around h3 through its public transport traits.
use crate::stream::{self, Stream};
use bytes::Bytes;
use futures_util::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use h3::quic::{self, OpenStreams, RecvStream, SendStream};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use tokio::sync::mpsc;

type ConnectionError = quic::ConnectionErrorIncoming;
type StreamError = quic::StreamErrorIncoming;
type Classification = BoxFuture<'static, anyhow::Result<(bool, Stream)>>;

pub struct Connection {
    inner: h3_quinn::Connection,
    pending: FuturesUnordered<Classification>,
    tcp: mpsc::Sender<Stream>,
    authenticated: Arc<AtomicBool>,
}

impl Connection {
    pub fn new(
        connection: quinn::Connection,
        tcp: mpsc::Sender<Stream>,
        authenticated: Arc<AtomicBool>,
    ) -> Self {
        Self {
            inner: h3_quinn::Connection::new(connection),
            pending: FuturesUnordered::new(),
            tcp,
            authenticated,
        }
    }
}

pub struct Opener(h3_quinn::OpenStreams);

impl Clone for Opener {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl quic::Connection<Bytes> for Connection {
    type RecvStream = h3_quinn::RecvStream;
    type OpenStreams = Opener;

    fn poll_accept_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::RecvStream, ConnectionError>> {
        quic::Connection::<Bytes>::poll_accept_recv(&mut self.inner, cx)
    }

    fn poll_accept_bidi(&mut self, cx: &mut Context<'_>) -> Poll<Result<Stream, ConnectionError>> {
        for _ in 0..32 {
            while self.pending.len() < 256 {
                match quic::Connection::<Bytes>::poll_accept_bidi(&mut self.inner, cx) {
                    Poll::Ready(Ok(stream)) => self.pending.push(Box::pin(async move {
                        tokio::time::timeout(
                            crate::tls::IO_TIMEOUT,
                            stream::classify(Stream::new(stream)),
                        )
                        .await?
                    })),
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => break,
                }
            }

            match self.pending.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok((false, stream)))) => return Poll::Ready(Ok(stream)),
                Poll::Ready(Some(Ok((true, mut stream)))) => {
                    if !self.authenticated.load(Ordering::Acquire) {
                        stream.reset(0x107);
                        stream.stop_sending(0x107);
                    } else if let Err(error) = self.tcp.try_send(stream) {
                        let mut stream = error.into_inner();
                        stream.reset(0x107);
                        stream.stop_sending(0x107);
                    }
                }
                Poll::Ready(Some(Err(_))) => {}
                Poll::Pending | Poll::Ready(None) => return Poll::Pending,
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn opener(&self) -> Opener {
        Opener(quic::Connection::<Bytes>::opener(&self.inner))
    }
}

impl OpenStreams<Bytes> for Connection {
    type SendStream = h3_quinn::SendStream<Bytes>;
    type BidiStream = Stream;

    fn poll_open_bidi(&mut self, cx: &mut Context<'_>) -> Poll<Result<Stream, StreamError>> {
        quic::OpenStreams::<Bytes>::poll_open_bidi(&mut self.inner, cx).map_ok(Stream::new)
    }

    fn poll_open_send(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::SendStream, StreamError>> {
        quic::OpenStreams::<Bytes>::poll_open_send(&mut self.inner, cx)
    }

    fn close(&mut self, code: h3::error::Code, reason: &[u8]) {
        quic::OpenStreams::<Bytes>::close(&mut self.inner, code, reason);
    }
}

impl OpenStreams<Bytes> for Opener {
    type SendStream = h3_quinn::SendStream<Bytes>;
    type BidiStream = Stream;

    fn poll_open_bidi(&mut self, cx: &mut Context<'_>) -> Poll<Result<Stream, StreamError>> {
        quic::OpenStreams::<Bytes>::poll_open_bidi(&mut self.0, cx).map_ok(Stream::new)
    }

    fn poll_open_send(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::SendStream, StreamError>> {
        quic::OpenStreams::<Bytes>::poll_open_send(&mut self.0, cx)
    }

    fn close(&mut self, code: h3::error::Code, reason: &[u8]) {
        quic::OpenStreams::<Bytes>::close(&mut self.0, code, reason);
    }
}
