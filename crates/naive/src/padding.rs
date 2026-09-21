use bytes::{Buf, Bytes, BytesMut};
use rand::Rng;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, Join, ReadBuf, ReadHalf, WriteHalf};
use tokio_util::{
    codec::{Decoder, Encoder, FramedRead, FramedWrite},
    io::{SinkWriter, StreamReader},
};

const FIRST_PADDINGS: u8 = 8;
const MAX_PAYLOAD: usize = u16::MAX as usize;

/// The header's presence negotiates padding independently for every tunnel.
pub(crate) fn header(response: bool) -> String {
    const ALPHABET: &[u8] = b"!#$()+<>?@[]^`{}";
    let mut rng = rand::rng();
    let length = if response {
        rng.random_range(30..62)
    } else {
        rng.random_range(16..32)
    };
    (0..length)
        .map(|index| {
            if index < 16 {
                ALPHABET[rng.random_range(0..ALPHABET.len())] as char
            } else {
                '~'
            }
        })
        .collect()
}

struct Codec {
    remaining: u8,
}

impl Codec {
    fn new(enabled: bool) -> Self {
        Self {
            remaining: if enabled { FIRST_PADDINGS } else { 0 },
        }
    }
}

impl Decoder for Codec {
    type Item = Bytes;
    type Error = io::Error;

    fn decode(&mut self, source: &mut BytesMut) -> io::Result<Option<Bytes>> {
        while !source.is_empty() {
            if self.remaining == 0 {
                return Ok(Some(source.split().freeze()));
            }
            if source.len() < 3 {
                return Ok(None);
            }

            let payload = u16::from_be_bytes([source[0], source[1]]) as usize;
            let padding = source[2] as usize;
            let total = 3 + payload + padding;
            if source.len() < total {
                source.reserve(total - source.len());
                return Ok(None);
            }

            source.advance(3);
            let bytes = source.split_to(payload).freeze();
            source.advance(padding);
            self.remaining -= 1;
            if !bytes.is_empty() {
                return Ok(Some(bytes));
            }
        }
        Ok(None)
    }
}

impl Encoder<&[u8]> for Codec {
    type Error = io::Error;

    fn encode(&mut self, bytes: &[u8], destination: &mut BytesMut) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        if bytes.len() > MAX_PAYLOAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Naive payload exceeds 65535 bytes",
            ));
        }
        debug_assert!(bytes.len() <= MAX_PAYLOAD);
        if self.remaining == 0 {
            destination.extend_from_slice(bytes);
            return Ok(());
        }

        let padding = rand::rng().random_range(0..=255_u8);
        destination.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        destination.extend_from_slice(&[padding]);
        destination.extend_from_slice(bytes);
        destination.resize(destination.len() + usize::from(padding), 0);
        self.remaining -= 1;
        Ok(())
    }
}

type Framed<T> = Join<
    StreamReader<FramedRead<ReadHalf<T>, Codec>, Bytes>,
    SinkWriter<FramedWrite<WriteHalf<T>, Codec>>,
>;

pub(crate) struct Padding<T> {
    inner: Framed<T>,
}

impl<T: AsyncRead + AsyncWrite + Unpin> Padding<T> {
    pub(crate) fn new(io: T, enabled: bool) -> Self {
        let (read, write) = tokio::io::split(io);
        Self {
            inner: tokio::io::join(
                StreamReader::new(FramedRead::new(read, Codec::new(enabled))),
                SinkWriter::new(FramedWrite::new(write, Codec::new(enabled))),
            ),
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> AsyncRead for Padding<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, out)
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> AsyncWrite for Padding<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, &bytes[..bytes.len().min(MAX_PAYLOAD)])
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> kotoconn_protocol::Stream for Padding<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn fragmented_padding_empty_frames_and_raw_transition() {
        let mut codec = Codec::new(true);
        let mut buffer = BytesMut::new();
        for frame in 0..FIRST_PADDINGS {
            let payload: &[u8] = if frame % 2 == 0 { b"" } else { b"abc" };
            let wire = [
                vec![0, payload.len() as u8, 2],
                payload.to_vec(),
                vec![0, 0],
            ]
            .concat();
            for (index, byte) in wire.iter().enumerate() {
                buffer.extend_from_slice(&[*byte]);
                let decoded = codec.decode(&mut buffer).unwrap();
                if index + 1 == wire.len() && !payload.is_empty() {
                    assert_eq!(decoded.unwrap(), payload);
                } else {
                    assert!(decoded.is_none());
                }
            }
        }
        buffer.extend_from_slice(b"unframed ninth chunk");
        assert_eq!(
            codec.decode(&mut buffer).unwrap().unwrap(),
            b"unframed ninth chunk"[..]
        );
        assert!(codec.decode_eof(&mut buffer).unwrap().is_none());
    }

    #[test]
    fn rejects_truncated_frames_but_accepts_eof_between_frames() {
        for bytes in [&[0][..], &[0, 1], &[0, 1, 0], &[0, 1, 2, 42, 0]] {
            assert!(
                Codec::new(true)
                    .decode_eof(&mut BytesMut::from(bytes))
                    .is_err()
            );
        }
        assert!(
            Codec::new(true)
                .decode_eof(&mut BytesMut::new())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn encoder_limits_and_negotiation() {
        let mut codec = Codec::new(true);
        for _ in 0..FIRST_PADDINGS {
            let mut buffer = BytesMut::new();
            codec.encode(&vec![42; MAX_PAYLOAD], &mut buffer).unwrap();
            assert_eq!(&buffer[..2], &[255, 255]);
            assert_eq!(buffer.len(), MAX_PAYLOAD + 3 + buffer[2] as usize);
        }
        let mut buffer = BytesMut::new();
        codec.encode(b"raw", &mut buffer).unwrap();
        assert_eq!(buffer, b"raw"[..]);
        assert!(
            codec
                .encode(&vec![0; MAX_PAYLOAD + 1], &mut buffer)
                .is_err()
        );

        let mut plain = Codec::new(false);
        assert_eq!(plain.decode(&mut buffer).unwrap().unwrap(), b"raw"[..]);
        plain.encode(b"plain", &mut buffer).unwrap();
        assert_eq!(buffer, b"plain"[..]);
    }

    #[tokio::test]
    async fn tiny_buffers_preserve_large_writes_and_half_close() {
        let (a, b) = tokio::io::duplex(7);
        let mut a = Padding::new(a, true);
        let mut b = Padding::new(b, true);
        let payload = vec![123; 200_000];
        let sending = async {
            a.write_all(&payload).await.unwrap();
            a.shutdown().await.unwrap();
            let mut response = Vec::new();
            a.read_to_end(&mut response).await.unwrap();
            assert_eq!(response, b"after FIN");
        };
        let receiving = async {
            let mut received = Vec::new();
            b.read_to_end(&mut received).await.unwrap();
            assert_eq!(received, payload);
            b.write_all(b"after FIN").await.unwrap();
            b.shutdown().await.unwrap();
        };
        tokio::join!(sending, receiving);
    }
}
