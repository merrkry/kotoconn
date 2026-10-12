use anyhow::{Context, Result, ensure};
use futures_util::future::BoxFuture;
use hickory_server::{
    Server,
    net::xfer::Protocol,
    proto::op::{DnsRequest, Header, HeaderCounts, Message, MessageType, OpCode, ResponseCode},
    proto::rr::RecordType,
    server::{Request, RequestHandler, ResponseHandler, ResponseInfo},
    zone_handler::MessageResponseBuilder,
};
use kotoconn_config::{
    DnsHandlerResult, DnsListenerConfig, DnsRequestInfo, InboundId, TransportProtocol,
};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::{TcpListener, UdpSocket};
use tokio_util::sync::CancellationToken;

pub trait Handler: Send + Sync + 'static {
    fn handle(
        &self,
        request: DnsRequest,
        info: DnsRequestInfo,
    ) -> BoxFuture<'_, Result<DnsHandlerResult>>;
}

pub struct BoundListener {
    pub address: SocketAddr,
    pub run: BoxFuture<'static, Result<()>>,
}

pub async fn bind(
    id: InboundId,
    config: &DnsListenerConfig,
    handler: Arc<dyn Handler>,
    stopping: CancellationToken,
) -> Result<BoundListener> {
    ensure!(
        !config.protocols.is_empty(),
        "DNS listener requires at least one protocol"
    );
    ensure!(
        config.protocols.len() <= 2
            && (config.protocols.len() != 2 || config.protocols[0] != config.protocols[1]),
        "duplicate DNS listener protocol"
    );

    let (address, tcp, udp) = bind_sockets(config).await?;
    let mut server = Server::new(Adapter { id, handler });
    if let Some(listener) = tcp {
        // This bounds buffered replies for one TCP client, not executing policy calls.
        server.register_listener(listener, Duration::from_secs(30), 32);
    }
    if let Some(socket) = udp {
        server.register_socket(socket);
    }

    let shutdown = server.shutdown_token().clone();
    let run = Box::pin(async move {
        let done = server.block_until_done();
        tokio::pin!(done);
        tokio::select! {
            result = &mut done => result?,
            _ = stopping.cancelled() => {
                shutdown.cancel();
                done.await?;
            }
        }
        Ok(())
    });
    Ok(BoundListener { address, run })
}

async fn bind_sockets(
    config: &DnsListenerConfig,
) -> Result<(SocketAddr, Option<TcpListener>, Option<UdpSocket>)> {
    for attempt in 0..8 {
        let mut address = config.listen;
        let tcp = if config.protocols.contains(&TransportProtocol::Tcp) {
            let listener = TcpListener::bind(address)
                .await
                .with_context(|| format!("DNS TCP listener {address}"))?;
            address = listener.local_addr()?;
            Some(listener)
        } else {
            None
        };
        let udp = if config.protocols.contains(&TransportProtocol::Udp) {
            match UdpSocket::bind(address).await {
                Ok(socket) => {
                    address = socket.local_addr()?;
                    Some(socket)
                }
                // TCP port-zero allocation cannot see UDP reservations. A new
                // allocation may resolve that collision; explicit ports fail.
                Err(error)
                    if error.kind() == std::io::ErrorKind::AddrInUse
                        && config.listen.port() == 0
                        && tcp.is_some()
                        && attempt < 7 =>
                {
                    continue;
                }
                Err(error) => {
                    return Err(error).with_context(|| format!("DNS UDP listener {address}"));
                }
            }
        } else {
            None
        };
        return Ok((address, tcp, udp));
    }
    // SAFETY: every iteration returns except retryable collisions, and the last
    // iteration does not allow a retry.
    unreachable!("final bind attempt returns its result")
}

struct Adapter {
    id: InboundId,
    handler: Arc<dyn Handler>,
}

#[async_trait::async_trait]
impl RequestHandler for Adapter {
    async fn handle_request<R: ResponseHandler, T: hickory_server::net::runtime::Time>(
        &self,
        request: &Request,
        mut response_handle: R,
    ) -> ResponseInfo {
        let mut metadata = request.metadata;
        metadata.message_type = MessageType::Response;
        let fallback = ResponseInfo::from(Header {
            metadata,
            counts: HeaderCounts::default(),
        });

        let error_code = if metadata.op_code != OpCode::Query || request.signature.is_some() {
            Some(ResponseCode::NotImp)
        } else {
            match request.request_info() {
                Err(_) => Some(ResponseCode::FormErr),
                Ok(info)
                    if matches!(info.query.query_type(), RecordType::AXFR | RecordType::IXFR) =>
                {
                    Some(ResponseCode::NotImp)
                }
                Ok(_) => None,
            }
        };
        let result = if let Some(code) = error_code {
            Err(code)
        } else {
            let result = async {
                let message = Message::from_vec(request.as_slice())?;
                let info = DnsRequestInfo {
                    inbound: self.id,
                    source: request.src(),
                    protocol: match request.protocol() {
                        Protocol::Udp => TransportProtocol::Udp,
                        _ => TransportProtocol::Tcp,
                    },
                };
                self.handler.handle(DnsRequest::from(message), info).await
            }
            .await;
            match result {
                Ok(DnsHandlerResult::Drop) => return fallback,
                Ok(DnsHandlerResult::Response(response)) => Ok(response),
                Err(error) => {
                    tracing::warn!(inbound_id = self.id.0.get(), source = %request.src(), error = %format_args!("{error:#}"), "DNS policy failed");
                    Err(ResponseCode::ServFail)
                }
            }
        };

        let sent = match result {
            Ok(response) => {
                let mut message = response.into_message();
                message.metadata.id = request.metadata.id;
                message.metadata.message_type = MessageType::Response;
                // Honor the client's UDP limit even when the upstream used TCP.
                if let Some(edns) = &mut message.edns {
                    let maximum = request.edns.as_ref().map_or(512, |edns| edns.max_payload());
                    edns.set_max_payload(edns.max_payload().min(maximum));
                }
                let response = MessageResponseBuilder::new(&request.queries, message.edns.as_ref())
                    .build(
                        message.metadata,
                        message.answers.iter(),
                        message.authorities.iter(),
                        std::iter::empty(),
                        message.additionals.iter(),
                    );
                response_handle.send_response(response).await
            }
            Err(code) => {
                response_handle
                    .send_response(
                        MessageResponseBuilder::from_message_request(request)
                            .error_msg(&request.metadata, code),
                    )
                    .await
            }
        };
        match sent {
            Ok(info) => info,
            Err(error) => {
                tracing::debug!(%error, "DNS client response failed");
                fallback
            }
        }
    }
}
