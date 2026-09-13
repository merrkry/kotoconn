//! HTTP/1 CONNECT adapter. HTTP has no datagram carrier capability.
use anyhow::{Result, bail, ensure};
use futures_util::future::BoxFuture;
use kotoconn_protocol::{self as p, *};
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};

pub struct Client {
    pub endpoint: Endpoint,
    pub carrier: Arc<dyn Carrier>,
}

impl p::Client for Client {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            tcp: self.carrier.capabilities().tcp,
            udp: false,
        }
    }

    fn tcp(&self, target: Target, scope: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        Box::pin(async move {
            self.carrier.capabilities().require(Capabilities::TCP)?;
            let authority = authority(&target)?;

            let mut stream = BufReader::new(
                self.carrier
                    .tcp_scoped(self.endpoint.resolve().await?, scope.clone())
                    .await?,
            );
            stream
                .write_all(
                    format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n\r\n").as_bytes(),
                )
                .await?;
            stream.flush().await?;

            let header = header(&mut stream).await?;
            let mut headers = [httparse::EMPTY_HEADER; 64];
            let mut response = httparse::Response::new(&mut headers);
            ensure!(
                response.parse(&header)?.is_complete(),
                "incomplete CONNECT response"
            );
            ensure!(
                response.code.is_some_and(|code| (200..300).contains(&code)),
                "CONNECT rejected: {:?}",
                response.code
            );
            Ok(Box::pin(stream) as BoxStream)
        })
    }

    fn udp(&self, _: Target, _: Scope) -> BoxFuture<'_, Result<Datagram>> {
        Box::pin(async { bail!("HTTP CONNECT does not support UDP") })
    }
}

pub struct Server;

impl p::Server for Server {
    fn bind(
        &self,
        address: SocketAddr,
        context: ServerContext,
    ) -> BoxFuture<'_, Result<BoundServer>> {
        Box::pin(async move {
            let listener = TcpListener::bind(address).await?;
            let local_addr = listener.local_addr()?;

            Ok(BoundServer {
                local_addr,
                run: Box::pin(async move {
                    let handler = context.handler.clone();

                    accept_loop(listener, context, move |stream, _, _, scope| {
                        let handler = handler.clone();

                        async move {
                            let mut stream = BufReader::new(stream);
                            let request = header(&mut stream).await?;

                            let destination = match destination(&request) {
                                Ok(destination) => destination,
                                Err(error) => {
                                    stream
                                        .write_all(
                                            b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                                        )
                                        .await?;
                                    return Err(error);
                                }
                            };

                            // This acknowledges admission to Kotoconn, independently of routing.
                            stream
                                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                                .await?;
                            stream.flush().await?;
                            handler.tcp(destination, Box::pin(stream), scope).await
                        }
                    })
                    .await
                }),
            })
        })
    }
}

fn authority(target: &Target) -> Result<String> {
    let text = match target {
        Target::Ip { address, port } => SocketAddr::new(*address, *port).to_string(),
        Target::Domain { name, port } => format!("{name}:{port}"),
    };
    parse_authority(&text)?;
    Ok(text)
}

fn parse_authority(value: &str) -> Result<Target> {
    if let Ok(address) = value.parse::<SocketAddr>() {
        return Ok(p::target(address));
    }
    let value = value.parse::<http::uri::Authority>()?;
    ensure!(
        !value.host().is_empty() && !value.host().contains('@'),
        "invalid authority"
    );
    Ok(Target::Domain {
        name: value.host().into(),
        port: value
            .port_u16()
            .ok_or_else(|| anyhow::anyhow!("CONNECT requires a port"))?,
    })
}

fn destination(header: &[u8]) -> Result<Target> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut request = httparse::Request::new(&mut headers);
    ensure!(
        request.parse(header)?.is_complete(),
        "incomplete HTTP request"
    );

    ensure!(
        request.method == Some("CONNECT"),
        "only HTTP CONNECT is supported"
    );
    parse_authority(
        request
            .path
            .ok_or_else(|| anyhow::anyhow!("missing authority"))?,
    )
}

async fn header(stream: &mut (impl tokio::io::AsyncRead + Unpin)) -> Result<Vec<u8>> {
    let mut result = Vec::new();
    while result.len() < 16 * 1024 {
        result.push(stream.read_u8().await?);
        if result.ends_with(b"\r\n\r\n") {
            return Ok(result);
        }
    }
    bail!("HTTP header exceeds 16 KiB")
}
