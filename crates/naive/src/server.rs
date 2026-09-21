use crate::{
    authorization,
    padding::{self, Padding},
    parse_authority,
};
use anyhow::{Result, ensure};
use bytes::{Buf, Bytes};
use futures_util::future::BoxFuture;
use h2::{RecvStream, SendStream};
use http::{Method, Response, StatusCode};
use kotoconn_config::NaiveInboundConfig;
use kotoconn_protocol::{self as p, BoundServer, Scope, ServerContext};
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpListener,
};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    },
};

pub struct Server {
    tls: TlsAcceptor,
    authorization: String,
}

impl Server {
    pub fn new(options: &NaiveInboundConfig) -> Result<Self> {
        let certificates = CertificateDer::pem_slice_iter(options.certificate.as_bytes())
            .collect::<std::result::Result<Vec<_>, _>>()?;
        ensure!(
            !certificates.is_empty(),
            "Naive requires a PEM certificate chain"
        );
        let key = PrivateKeyDer::from_pem_slice(options.private_key.as_bytes())?;
        let mut tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certificates, key)?;
        tls.alpn_protocols = vec![b"h2".to_vec()];

        Ok(Self {
            tls: TlsAcceptor::from(Arc::new(tls)),
            authorization: authorization(&options.username, &options.password)?,
        })
    }
}

struct Close(Scope);

impl Drop for Close {
    fn drop(&mut self) {
        self.0.close();
    }
}

impl p::Server for Server {
    fn bind(
        &self,
        address: SocketAddr,
        context: ServerContext,
    ) -> BoxFuture<'_, Result<BoundServer>> {
        Box::pin(async move {
            let listener = TcpListener::bind(address).await?;
            let local_addr = listener.local_addr()?;
            let tls = self.tls.clone();
            let authorization = self.authorization.clone();

            Ok(BoundServer {
                local_addr,
                run: Box::pin(async move {
                    let handler = context.handler.clone();
                    let stopping = context.stopping.clone();
                    p::accept_loop(listener, context, move |io, _, _, scope| {
                        let tls = tls.clone();
                        let authorization = authorization.clone();
                        let handler = handler.clone();
                        let stopping = stopping.clone();

                        async move {
                            let _close = Close(scope.clone());
                            let tls = tls.accept(io).await?;
                            ensure!(
                                tls.get_ref().1.alpn_protocol() == Some(b"h2"),
                                "Naive requires HTTP/2 ALPN"
                            );
                            let mut connection = h2::server::Builder::new()
                                .max_concurrent_streams(1024)
                                .max_header_list_size(16 * 1024)
                                .handshake(tls)
                                .await?;

                            let mut draining = false;
                            loop {
                                let request = tokio::select! {
                                    biased;
                                    _ = stopping.cancelled(), if !draining => {
                                        connection.graceful_shutdown();
                                        draining = true;
                                        continue;
                                    }
                                    request = connection.accept() => request,
                                };
                                let Some(request) = request else {
                                    break;
                                };
                                let (request, mut response) = request?;
                                let authenticated = request
                                    .headers()
                                    .get(http::header::PROXY_AUTHORIZATION)
                                    .is_some_and(|value| {
                                        bool::from(value.as_bytes().ct_eq(authorization.as_bytes()))
                                    });
                                if !authenticated {
                                    response.send_response(
                                        Response::builder()
                                            .status(StatusCode::NOT_FOUND)
                                            .body(())?,
                                        true,
                                    )?;
                                    continue;
                                }

                                let destination = request
                                    .uri()
                                    .authority()
                                    .filter(|_| request.method() == Method::CONNECT)
                                    .and_then(|value| parse_authority(value.as_str()).ok());
                                let Some(destination) = destination else {
                                    response.send_response(
                                        Response::builder()
                                            .status(StatusCode::BAD_REQUEST)
                                            .body(())?,
                                        true,
                                    )?;
                                    continue;
                                };
                                let padded = request.headers().contains_key("padding");
                                let mut reply = Response::builder().status(StatusCode::OK);
                                if padded {
                                    reply = reply.header("padding", padding::header(true));
                                }
                                // Admission precedes routing, as with the other inbound adapters.
                                let send = response.send_response(reply.body(())?, false)?;
                                let io = Padding::new(H2Io::new(request.into_body(), send), padded);
                                let session = scope.child();
                                let handler = handler.clone();
                                let work_scope = session.clone();
                                session.spawn(async move {
                                    handler.tcp(destination, Box::pin(io), work_scope).await
                                })?;
                            }
                            Ok(())
                        }
                    })
                    .await
                }),
            })
        })
    }
}

struct H2Io {
    recv: RecvStream,
    send: SendStream<Bytes>,
    pending: Bytes,
    eof: bool,
    shutdown: bool,
}

impl H2Io {
    fn new(recv: RecvStream, send: SendStream<Bytes>) -> Self {
        Self {
            recv,
            send,
            pending: Bytes::new(),
            eof: false,
            shutdown: false,
        }
    }
}

impl AsyncRead for H2Io {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if out.remaining() == 0 || self.eof {
            return Poll::Ready(Ok(()));
        }
        for _ in 0..32 {
            if !self.pending.is_empty() {
                let n = out.remaining().min(self.pending.len());
                out.put_slice(&self.pending[..n]);
                self.pending.advance(n);
                self.recv
                    .flow_control()
                    .release_capacity(n)
                    .map_err(io::Error::other)?;
                return Poll::Ready(Ok(()));
            }

            match ready!(self.recv.poll_data(cx)) {
                Some(Ok(bytes)) => self.pending = bytes,
                Some(Err(error)) => return Poll::Ready(Err(io::Error::other(error))),
                None => {
                    self.eof = true;
                    return Poll::Ready(Ok(()));
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl AsyncWrite for H2Io {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.shutdown {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let wanted = bytes.len().min(16 * 1024);
        self.send.reserve_capacity(wanted);
        while self.send.capacity() == 0 {
            match ready!(self.send.poll_capacity(cx)) {
                Some(Ok(_)) => {}
                Some(Err(error)) => return Poll::Ready(Err(io::Error::other(error))),
                None => return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
            }
        }

        let n = wanted.min(self.send.capacity());
        self.send
            .send_data(Bytes::copy_from_slice(&bytes[..n]), false)
            .map_err(io::Error::other)?;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.shutdown {
            self.send
                .send_data(Bytes::new(), true)
                .map_err(io::Error::other)?;
            self.shutdown = true;
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for H2Io {
    fn drop(&mut self) {
        if !self.eof || !self.shutdown {
            self.send.send_reset(h2::Reason::CANCEL);
        }
    }
}
