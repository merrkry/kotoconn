use crate::{
    http3,
    runtime::{CloseConnection, CloseScope, ScopedRuntime},
    socket::{NativeSocket, Salamander},
    tls, udp, wire,
};
use anyhow::{Result, ensure};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use kotoconn_config::Hysteria2InboundConfig;
use kotoconn_protocol::{self as p, BoundServer, ServerContext};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::mpsc;

pub struct Server {
    tls: quinn::ServerConfig,
    password: String,
    obfs: Option<String>,
}

impl Server {
    pub fn new(options: &Hysteria2InboundConfig) -> Result<Self> {
        Ok(Self {
            tls: tls::server(options)?,
            password: options.password.clone(),
            obfs: options.obfs_password.clone(),
        })
    }
}

impl p::Server for Server {
    fn bind(
        &self,
        address: SocketAddr,
        context: ServerContext,
    ) -> BoxFuture<'_, Result<BoundServer>> {
        Box::pin(async move {
            let socket = Arc::new(tokio::net::UdpSocket::bind(address).await?);
            let local_addr = socket.local_addr()?;
            let socket = Salamander::wrap(Arc::new(NativeSocket(socket)), self.obfs.as_deref());
            let endpoint = quinn::Endpoint::new_with_abstract_socket(
                Default::default(),
                Some(self.tls.clone()),
                socket,
                Arc::new(ScopedRuntime(context.scope.clone())),
            )?;
            let password = self.password.clone();
            Ok(BoundServer {
                local_addr,
                run: Box::pin(async move {
                    loop {
                        tokio::select! {
                            biased;
                            _ = context.stopping.cancelled() => {
                                endpoint.set_server_config(None);
                                return Ok(());
                            }
                            _ = context.scope.cancelled() => return Ok(()),
                            incoming = endpoint.accept() => {
                                let Some(incoming) = incoming else { return Ok(()); };
                                let mut context = context.clone();
                                context.scope = context.scope.child();
                                let scope = context.scope.clone();
                                let password = password.clone();
                                let endpoint = endpoint.clone();
                                scope.spawn(async move {
                                    let _scope = CloseScope(context.scope.clone());
                                    let _endpoint = endpoint;
                                    let connection = tokio::time::timeout(tls::IO_TIMEOUT, incoming).await??;
                                    let _close = CloseConnection(connection.clone());
                                    serve(connection, context, password).await
                                })?;
                            }
                        }
                    }
                }),
            })
        })
    }
}

async fn serve(
    connection: quinn::Connection,
    context: ServerContext,
    password: String,
) -> Result<()> {
    let authenticated = Arc::new(AtomicBool::new(false));
    let (tcp, mut incoming) = mpsc::channel(64);
    let adapter = http3::Connection::new(connection.clone(), tcp, authenticated.clone());
    let mut http = h3::server::builder()
        .max_field_section_size(16384)
        .build::<_, Bytes>(adapter)
        .await?;
    let tcp_scope = context.scope.child();
    let udp_context = context.clone();
    let udp_connection = connection.clone();
    let udp_authenticated = authenticated.clone();
    context
        .scope
        .spawn(async move { udp::server(udp_connection, udp_context, udp_authenticated).await })?;
    let deadline = tokio::time::sleep(tls::IO_TIMEOUT);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            biased;
            _ = context.stopping.cancelled() => {
                // Keep Quinn alive for already admitted TCP sessions while the
                // daemon drains them. Its shutdown deadline cancels this scope.
                tcp_scope.wait().await;
                return Ok(());
            }
            _ = &mut deadline, if !authenticated.load(Ordering::Acquire) => {
                ensure!(authenticated.load(Ordering::Acquire), "Hysteria authentication timed out");
            }
            request = http.accept() => {
                let Some(request) = request? else { return Ok(()); };
                let authenticated = authenticated.clone();
                let password = password.clone();
                context.scope.spawn(async move {
                    tokio::time::timeout(tls::IO_TIMEOUT, async move {
                        let (request, mut response) = request.resolve_request().await?;
                        let valid = request.method() == http::Method::POST
                            && request.uri().host() == Some("hysteria")
                            && request.uri().path() == "/auth"
                            && request.headers().get("Hysteria-Auth").is_some_and(|value| credentials_match(value.as_bytes(), password.as_bytes()));
                        let reply = if valid {
                            authenticated.store(true, Ordering::Release);
                            http::Response::builder().status(233)
                                .header("Hysteria-UDP", "true")
                                .header("Hysteria-CC-RX", "auto")
                                .header("Hysteria-Padding", wire::padding())
                                .body(())?
                        } else {
                            // Ordinary web-server behavior for probes and bad credentials.
                            http::Response::builder().status(404).header("content-length", "0").body(())?
                        };
                        response.send_response(reply).await?;
                        response.finish().await?;
                        Ok::<_, anyhow::Error>(())
                    }).await?
                })?;
            }
            stream = incoming.recv() => {
                let Some(mut stream) = stream else { return Ok(()); };
                let handler = context.handler.clone();
                let scope = tcp_scope.child();
                let connection_scope = scope.clone();
                scope.spawn(async move {
                    let _close = CloseScope(connection_scope.clone());
                    let target = tokio::time::timeout(tls::IO_TIMEOUT, async {
                        let target = wire::read_request(&mut stream).await?;
                        wire::response(&mut stream).await?;
                        Ok::<_, anyhow::Error>(target)
                    }).await??;
                    handler.tcp(target, Box::pin(stream), connection_scope).await
                })?;
            }
        }
    }
}

fn credentials_match(actual: &[u8], expected: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    bool::from(actual.ct_eq(expected))
}
