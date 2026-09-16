use crate::{session::Session, tls, udp, wire};
use anyhow::Result;
use anytls::core::PaddingFactory;
use futures_util::future::BoxFuture;
use kotoconn_config::AnyTlsInboundConfig;
use kotoconn_protocol::{self as p, *};
use std::net::SocketAddr;
use tokio::{net::TcpListener, sync::watch};
use tokio_rustls::TlsAcceptor;

pub struct Server {
    acceptor: TlsAcceptor,
    password: [u8; 32],
    padding: PaddingFactory,
}

impl Server {
    pub fn new(options: &AnyTlsInboundConfig) -> Result<Self> {
        Ok(Self {
            acceptor: tls::server(&options.tls)?,
            password: wire::password_hash(&options.password),
            padding: wire::padding_scheme(options.padding_scheme.as_deref())?,
        })
    }
}

impl p::Server for Server {
    fn bind(
        &self,
        address: SocketAddr,
        context: ServerContext,
    ) -> BoxFuture<'_, Result<BoundServer>> {
        let acceptor = self.acceptor.clone();
        let password = self.password;
        let padding = self.padding.clone();
        Box::pin(async move {
            let listener = TcpListener::bind(address).await?;
            let local_addr = listener.local_addr()?;
            let connection_context = context.clone();
            Ok(BoundServer {
                local_addr,
                run: Box::pin(accept_loop(
                    listener,
                    context,
                    move |stream, _, _, scope| {
                        let acceptor = acceptor.clone();
                        let padding = padding.clone();
                        let context = connection_context.clone();
                        async move {
                            let handshake = tokio::time::timeout(crate::HANDSHAKE_TIMEOUT, async {
                                let mut tls = acceptor.accept(stream).await?;
                                wire::authenticate(&mut tls, &password).await?;
                                Ok::<_, anyhow::Error>(tls)
                            });
                            let tls = tokio::select! {
                                _ = context.stopping.cancelled() => return Ok(()),
                                result = handshake => result??,
                            };
                            let streams = scope.clone();
                            let stopping = context.stopping.clone();
                            let (padding, _) = watch::channel(padding);
                            let session = Session::start(
                                tls,
                                &scope,
                                padding,
                                Some(Box::new(move |mut stream| {
                                    if context.stopping.is_cancelled() {
                                        return Ok(());
                                    }
                                    let scope = streams.child();
                                    let handler = context.handler.clone();
                                    let stopping = context.stopping.clone();
                                    let work_scope = scope.clone();
                                    scope.spawn(async move {
                                        let request = tokio::time::timeout(
                                            crate::HANDSHAKE_TIMEOUT,
                                            wire::read_target(&mut stream),
                                        );
                                        let destination = tokio::select! {
                                            _ = stopping.cancelled() => return Ok(()),
                                            result = request => result??,
                                        };
                                        if udp::is_sentinel(&destination) {
                                            let destination = tokio::time::timeout(
                                                crate::HANDSHAKE_TIMEOUT,
                                                udp::read_request(&mut stream),
                                            )
                                            .await??;
                                            let association =
                                                udp::start(stream, destination, work_scope)?;
                                            tokio::select! {
                                                _ = stopping.cancelled() => Ok(()),
                                                result = handler.udp(association) => result,
                                            }
                                        } else {
                                            handler.tcp(destination, stream, work_scope).await
                                        }
                                    })
                                })),
                                None,
                                Some(stopping),
                            )?;
                            session.closed.cancelled().await;
                            scope.close();
                            Ok(())
                        }
                    },
                )),
            })
        })
    }
}
