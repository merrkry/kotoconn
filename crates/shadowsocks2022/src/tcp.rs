//! Use the library's crypto stream with an explicit, nonempty request padding.
//! ProxyClientStream's empty-write helper can randomly choose zero padding.
use super::{Crypto, METHOD, address};
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use kotoconn_protocol::{BoxStream, Target};
use rand::RngCore;
use shadowsocks::{
    context::SharedContext,
    relay::tcprelay::crypto_io::{CryptoRead, CryptoStream, CryptoWrite, StreamType},
};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};

pub(super) async fn connect(
    stream: BoxStream,
    target: Target,
    crypto: &Crypto,
) -> Result<BoxStream> {
    let mut stream = Encrypted {
        stream: CryptoStream::from_stream(
            &crypto.context,
            stream,
            StreamType::Client,
            METHOD,
            crypto.config.key(),
        ),
        context: crypto.context.clone(),
        failed: false,
        verified: false,
    };

    let mut header = BytesMut::new();
    address(target).write_to_buf(&mut header);

    let padding = rand::random_range(1..=900);

    header.put_u16(padding);
    let start = header.len();
    header.resize(start + usize::from(padding), 0);
    rand::rng().fill_bytes(&mut header[start..]);
    stream.write_all(&header).await?;
    stream.flush().await?;

    Ok(Box::pin(stream))
}

struct Encrypted {
    stream: CryptoStream<BoxStream>,
    context: SharedContext,
    failed: bool,
    verified: bool,
}

impl AsyncRead for Encrypted {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.failed {
            return Poll::Ready(Err(io::Error::other("invalid Shadowsocks response")));
        }
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.verified {
            return Pin::new(&mut this.stream)
                .poll_read_decrypted(cx, &this.context, output)
                .map_err(Into::into);
        }

        // Keep first-response plaintext private until the echoed request salt
        // has been checked. An error must not modify the caller's output buffer.
        let mut bytes = [0; 8192];
        let capacity = output.remaining().min(bytes.len());
        let mut pending = ReadBuf::new(&mut bytes[..capacity]);
        ready!(Pin::new(&mut this.stream).poll_read_decrypted(cx, &this.context, &mut pending))
            .map_err(io::Error::from)?;
        if pending.filled().is_empty() {
            return Poll::Ready(Ok(()));
        }
        if this.stream.received_request_nonce() != Some(this.stream.sent_nonce()) {
            this.failed = true;
            return Poll::Ready(Err(io::Error::other(
                "Shadowsocks response salt does not match request",
            )));
        }
        this.verified = true;
        output.put_slice(pending.filled());
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Encrypted {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream)
            .poll_write_encrypted(cx, data)
            .map_err(Into::into)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.stream.poll_flush(cx).map_err(Into::into)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.stream.poll_shutdown(cx).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::future::poll_fn;
    use shadowsocks::config::ServerType;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn response_with_another_requests_salt_never_exposes_plaintext() -> Result<()> {
        let crypto = Crypto::new("AAECAwQFBgcICQoLDA0ODw==", ServerType::Local)?;
        let server_crypto = Crypto::new("AAECAwQFBgcICQoLDA0ODw==", ServerType::Server)?;
        let (client, server) = tokio::io::duplex(4096);
        let client = async {
            let mut client = connect(
                Box::pin(client),
                kotoconn_protocol::target("127.0.0.1:80".parse()?),
                &crypto,
            )
            .await?;
            let mut output = [0; 3];
            assert!(client.read(&mut output).await.is_err());
            assert_eq!(output, [0; 3]);
            assert!(client.read(&mut output).await.is_err());
            Ok::<_, anyhow::Error>(())
        };
        let server = async {
            let mut encrypted = CryptoStream::from_stream(
                &server_crypto.context,
                Box::pin(server) as BoxStream,
                StreamType::Server,
                METHOD,
                server_crypto.config.key(),
            );
            let mut bytes = [0; 2048];
            let mut buffer = ReadBuf::new(&mut bytes);
            poll_fn(|cx| {
                Pin::new(&mut encrypted).poll_read_decrypted(
                    cx,
                    &server_crypto.context,
                    &mut buffer,
                )
            })
            .await?;
            encrypted.set_request_nonce(&[0; 16]);
            let written =
                poll_fn(|cx| Pin::new(&mut encrypted).poll_write_encrypted(cx, b"bad")).await?;
            assert_eq!(written, 3);
            Ok::<_, anyhow::Error>(())
        };
        tokio::try_join!(client, server)?;
        Ok(())
    }
}
