//! I/O adaptation only. h3-quinn retains HTTP/3's Quinn stream implementation.
use bytes::{Buf, Bytes};
use h3::quic::{self, BidiStream, RecvStream, SendStream, SendStreamUnframed};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

type Error = quic::StreamErrorIncoming;

pub struct Receive {
    pub prefix: Bytes,
    inner: h3_quinn::RecvStream,
}

impl RecvStream for Receive {
    type Buf = Bytes;

    fn poll_data(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Bytes>, Error>> {
        if !self.prefix.is_empty() {
            return Poll::Ready(Ok(Some(std::mem::take(&mut self.prefix))));
        }
        self.inner.poll_data(cx)
    }

    fn stop_sending(&mut self, code: u64) {
        self.inner.stop_sending(code);
    }

    fn recv_id(&self) -> quic::StreamId {
        self.inner.recv_id()
    }
}

pub struct Stream {
    send: h3_quinn::SendStream<Bytes>,
    pub recv: Receive,
    owner: Option<Box<dyn Send + Sync>>,
}

impl Stream {
    pub fn retain(&mut self, owner: Box<dyn Send + Sync>) {
        debug_assert!(self.owner.is_none());
        self.owner = Some(owner);
    }

    pub fn new(stream: h3_quinn::BidiStream<Bytes>) -> Self {
        let (send, inner) = stream.split();
        Self {
            send,
            recv: Receive {
                prefix: Bytes::new(),
                inner,
            },
            owner: None,
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if out.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.recv.prefix.is_empty() {
            let Some(bytes) = ready!(self.recv.inner.poll_data(cx)).map_err(io::Error::other)?
            else {
                return Poll::Ready(Ok(()));
            };
            self.recv.prefix = bytes;
        }
        let length = out.remaining().min(self.recv.prefix.len());
        out.put_slice(&self.recv.prefix.split_to(length));
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.send
            .poll_send(cx, &mut bytes)
            .map_err(io::Error::other)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.send.poll_ready(cx).map_err(io::Error::other)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.send.poll_finish(cx).map_err(io::Error::other)
    }
}

impl kotoconn_protocol::Stream for Stream {}

impl RecvStream for Stream {
    type Buf = Bytes;

    fn poll_data(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Bytes>, Error>> {
        self.recv.poll_data(cx)
    }

    fn stop_sending(&mut self, code: u64) {
        self.recv.stop_sending(code);
    }

    fn recv_id(&self) -> quic::StreamId {
        self.recv.recv_id()
    }
}

impl SendStream<Bytes> for Stream {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.send.poll_ready(cx)
    }

    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.send.poll_finish(cx)
    }

    fn reset(&mut self, code: u64) {
        self.send.reset(code);
    }

    fn send_data<T: Into<quic::WriteBuf<Bytes>>>(&mut self, data: T) -> Result<(), Error> {
        self.send.send_data(data)
    }

    fn send_id(&self) -> quic::StreamId {
        self.send.send_id()
    }
}

impl BidiStream<Bytes> for Stream {
    type SendStream = h3_quinn::SendStream<Bytes>;
    type RecvStream = Receive;

    fn split(self) -> (Self::SendStream, Self::RecvStream) {
        debug_assert!(self.owner.is_none(), "only HTTP/3 streams are split");
        (self.send, self.recv)
    }
}

pub async fn open(connection: &quinn::Connection) -> anyhow::Result<Stream> {
    let mut transport = h3_quinn::Connection::new(connection.clone());
    let stream =
        std::future::poll_fn(|cx| quic::OpenStreams::<Bytes>::poll_open_bidi(&mut transport, cx))
            .await?;
    Ok(Stream::new(stream))
}

/// Read only the first frame type. Preserve every byte for ordinary HTTP/3.
pub async fn classify(mut stream: Stream) -> anyhow::Result<(bool, Stream)> {
    use quinn_proto::coding::Codec;
    use tokio::io::AsyncReadExt;
    let mut prefix = [0; 8];
    stream.read_exact(&mut prefix[..1]).await?;
    let length = 1 << (prefix[0] >> 6);
    stream.read_exact(&mut prefix[1..length]).await?;
    let frame = quinn::VarInt::decode(&mut &prefix[..length])?.into_inner();
    let tcp = frame == crate::wire::TCP_REQUEST;
    if !tcp {
        let mut restored = Vec::with_capacity(length + stream.recv.prefix.remaining());
        restored.extend_from_slice(&prefix[..length]);
        restored.extend_from_slice(&stream.recv.prefix);
        stream.recv.prefix = restored.into();
    }
    Ok((tcp, stream))
}
