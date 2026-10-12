use crate::{cache::ResponseCache, runtime::Runtime};
use anyhow::{Context, Result, ensure};
use futures_util::StreamExt;
use hickory_net::{
    NetError,
    proto::op::{DnsRequest, DnsResponse, MessageType, OpCode},
    proto::rr::RecordType,
    tcp::TcpClientStream,
    udp::UdpClientStream,
    xfer::{DnsExchange, DnsHandle},
};
use kotoconn_config::{DnsServiceConfig, TransportProtocol};
use kotoconn_protocol::{Capabilities, Carrier, Scope};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct Connection {
    generation: u64,
    handle: DnsExchange<Runtime>,
    finished: CancellationToken,
}

pub struct Service {
    config: DnsServiceConfig,
    runtime: Runtime,
    connections: [Mutex<Option<Connection>>; 2],
    generation: std::sync::atomic::AtomicU64,
    cache: Option<ResponseCache>,
}

impl Service {
    pub fn new(config: DnsServiceConfig, carrier: Arc<dyn Carrier>, scope: Scope) -> Result<Self> {
        ensure!(!config.timeout.is_zero(), "DNS timeout must be positive");
        ensure!(
            config.upstream.server.port() != 0,
            "DNS upstream port must be positive"
        );
        carrier
            .capabilities()
            .require(match config.upstream.protocol {
                TransportProtocol::Tcp => Capabilities::TCP,
                TransportProtocol::Udp => Capabilities {
                    tcp: false,
                    udp: true,
                },
            })?;

        let cache = (config.cache_capacity > 0).then(|| ResponseCache::new(config.cache_capacity));
        Ok(Self {
            config,
            runtime: Runtime {
                carrier,
                scope,
                finished: CancellationToken::new(),
            },
            connections: [Mutex::new(None), Mutex::new(None)],
            generation: std::sync::atomic::AtomicU64::new(0),
            cache,
        })
    }

    pub async fn query(&self, request: DnsRequest) -> Result<DnsResponse> {
        ensure!(
            request.metadata.message_type == MessageType::Query
                && request.metadata.op_code == OpCode::Query
                && request.queries.len() == 1,
            "DNS service requires one ordinary question"
        );
        ensure!(
            request.signature.is_none(),
            "authenticated DNS forwarding is unsupported"
        );
        ensure!(
            !matches!(
                request.queries[0].query_type,
                RecordType::AXFR | RecordType::IXFR
            ),
            "DNS zone transfers are unsupported"
        );

        let key = self
            .cache
            .as_ref()
            .and_then(|_| ResponseCache::key(&request));
        if let (Some(cache), Some(key)) = (&self.cache, &key)
            && let Some(response) = cache.get(key, &request)
        {
            return Ok(response);
        }

        let mut response = tokio::time::timeout(self.config.timeout, async {
            let response = self
                .exchange(request.clone(), self.config.upstream.protocol)
                .await?;
            if response.metadata.truncation
                && self.config.upstream.protocol == TransportProtocol::Udp
            {
                return self.exchange(request.clone(), TransportProtocol::Tcp).await;
            }
            Ok::<_, anyhow::Error>(response)
        })
        .await
        .context("DNS upstream deadline exceeded")??;
        ensure!(
            response.metadata.message_type == MessageType::Response
                && response.queries == request.queries,
            "DNS upstream response does not match the question"
        );
        response.metadata.id = request.metadata.id;
        response.queries = request.queries.clone();
        // Rebuild the encoded buffer after restoring the caller's ID and question.
        let response = DnsResponse::from_message(response.into_message())?;

        if let (Some(cache), Some(key)) = (&self.cache, key) {
            cache.insert(key, &response);
        }
        Ok(response)
    }

    async fn exchange(
        &self,
        request: DnsRequest,
        protocol: TransportProtocol,
    ) -> Result<DnsResponse> {
        let slot = match protocol {
            TransportProtocol::Tcp => 0,
            TransportProtocol::Udp => 1,
        };
        let mut reconnected = false;
        loop {
            let (connection, reused) = {
                let mut cached = self.connections[slot].lock().await;
                if cached
                    .as_ref()
                    .is_some_and(|connection| connection.finished.is_cancelled())
                {
                    *cached = None;
                }
                let reused = cached.is_some();
                if cached.is_none() {
                    let runtime = Runtime {
                        finished: CancellationToken::new(),
                        ..self.runtime.clone()
                    };
                    let connection = match protocol {
                        TransportProtocol::Tcp => {
                            TcpClientStream::exchange(
                                self.config.upstream.server,
                                None,
                                self.config.timeout,
                                // Multiplexing is limited by the DNS transaction-ID space.
                                Some(usize::from(u16::MAX) + 1),
                                runtime.clone(),
                            )
                            .await?
                        }
                        TransportProtocol::Udp => {
                            UdpClientStream::builder(self.config.upstream.server, runtime.clone())
                                .with_timeout(Some(self.config.timeout))
                                .exchange()
                        }
                    };
                    let generation = self
                        .generation
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    *cached = Some(Connection {
                        generation,
                        handle: connection,
                        finished: runtime.finished,
                    });
                }
                // SAFETY: the branch above installs a connection before this access.
                (
                    cached.as_ref().expect("DNS connection initialized").clone(),
                    reused,
                )
            };

            let result = match connection.handle.send(request.clone()).next().await {
                Some(result) => result,
                None => Err(NetError::from(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "DNS upstream closed without a response",
                ))),
            };
            if let Err(error) = &result {
                let mut cached = self.connections[slot].lock().await;
                if cached
                    .as_ref()
                    .is_some_and(|current| current.generation == connection.generation)
                {
                    *cached = None;
                }
                // A server may close an idle pooled TCP connection. Reconnect once
                // for that failure; deadlines and DNS error responses are not retried.
                if reused
                    && !reconnected
                    && protocol == TransportProtocol::Tcp
                    && (error.is_connection_closed() || connection.finished.is_cancelled())
                {
                    reconnected = true;
                    continue;
                }
            }
            return result.map_err(anyhow::Error::from);
        }
    }
}
