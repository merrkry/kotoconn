use anyhow::{Context, Result, ensure};
use futures_util::future::BoxFuture;
use hickory_server::{
    net::xfer::Protocol,
    proto::op::{DnsRequest, Header, Message, MessageType, OpCode, ResponseCode},
    proto::rr::{Record, RecordType},
    proto::serialize::binary::{BinDecodable, BinDecoder, BinEncoder},
    server::Request,
    zone_handler::{MessageResponse, MessageResponseBuilder},
};
use kotoconn_config::{
    DnsHandlerResult, DnsListenerConfig, DnsRequestInfo, InboundId, TransportProtocol,
};
use kotoconn_protocol::Scope;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
};
use tokio_util::sync::CancellationToken;

/// A TCP client that sends no complete message for this long is disconnected.
const TCP_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

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

/// Requests run in `scope`. Once `stopping` is cancelled, the listener reads no
/// new request; requests already read are still answered.
pub async fn bind(
    id: InboundId,
    config: &DnsListenerConfig,
    handler: Arc<dyn Handler>,
    stopping: CancellationToken,
    scope: Scope,
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
    let responder = Arc::new(Responder::new(id, handler));
    let run = Box::pin(async move {
        let udp = async {
            match udp {
                Some(socket) => serve_udp(Arc::new(socket), &responder, &stopping, &scope).await,
                None => Ok(()),
            }
        };
        let tcp = async {
            match tcp {
                Some(listener) => serve_tcp(listener, &responder, &stopping, &scope).await,
                None => Ok(()),
            }
        };

        tokio::try_join!(udp, tcp)?;
        Ok(())
    });
    Ok(BoundListener { address, run })
}

async fn serve_udp(
    socket: Arc<UdpSocket>,
    responder: &Arc<Responder>,
    stopping: &CancellationToken,
    scope: &Scope,
) -> Result<()> {
    let mut buffer = vec![0; usize::from(u16::MAX)];

    loop {
        let received = tokio::select! {
            biased;
            _ = stopping.cancelled() => return Ok(()),
            received = socket.recv_from(&mut buffer) => received,
        };
        let (length, source) = match received {
            Ok(received) => received,
            // Some platforms report ICMP errors of earlier replies on the next receive.
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                tracing::debug!(%error, "DNS UDP receive failed");
                continue;
            }
            Err(error) => return Err(error).context("DNS UDP listener"),
        };
        // A reply to port zero cannot be delivered.
        if source.port() == 0 {
            continue;
        }

        let request = buffer[..length].to_vec();
        let (responder, socket) = (responder.clone(), socket.clone());
        scope.spawn(async move {
            let Some(response) = responder
                .respond(request, source, TransportProtocol::Udp)
                .await
            else {
                return Ok(());
            };
            if let Err(error) = socket.send_to(&response, source).await {
                tracing::debug!(%source, %error, "DNS client response failed");
            }
            Ok(())
        })?;
    }
}

async fn serve_tcp(
    listener: TcpListener,
    responder: &Arc<Responder>,
    stopping: &CancellationToken,
    scope: &Scope,
) -> Result<()> {
    while let Some((stream, source)) =
        kotoconn_protocol::accept(&listener, stopping.cancelled()).await
    {
        let (responder, stopping) = (responder.clone(), stopping.clone());
        scope.spawn(async move {
            if let Err(error) = serve_stream(stream, source, &responder, &stopping).await {
                tracing::debug!(%source, error = %format_args!("{error:#}"), "DNS TCP connection failed");
            }
            Ok(())
        })?;
    }
    Ok(())
}

/// Answers the DNS messages of one TCP connection, one at a time, so one client
/// cannot run unbounded policy calls. Once `stopping` is cancelled, it reads no
/// new message.
pub async fn serve_stream(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    source: SocketAddr,
    responder: &Responder,
    stopping: &CancellationToken,
) -> Result<()> {
    loop {
        let read = async {
            let length = stream.read_u16().await?;
            let mut message = vec![0; usize::from(length)];
            stream.read_exact(&mut message).await?;
            Ok::<_, std::io::Error>(message)
        };
        let message = tokio::select! {
            biased;
            _ = stopping.cancelled() => return Ok(()),
            message = tokio::time::timeout(TCP_IDLE_TIMEOUT, read) => match message {
                Ok(Ok(message)) => message,
                Ok(Err(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => return Ok(()),
            },
        };

        if let Some(response) = responder
            .respond(message, source, TransportProtocol::Tcp)
            .await
        {
            let length = u16::try_from(response.len()).context("DNS response too long")?;
            stream.write_all(&length.to_be_bytes()).await?;
            stream.write_all(&response).await?;
        }
    }
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

/// Answers DNS messages that `inbound` received with a policy handler.
pub struct Responder {
    inbound: InboundId,
    handler: Arc<dyn Handler>,
}

impl Responder {
    pub fn new(inbound: InboundId, handler: Arc<dyn Handler>) -> Self {
        Self { inbound, handler }
    }

    /// The encoded response to one received message, if it gets any.
    pub async fn respond(
        &self,
        bytes: Vec<u8>,
        source: SocketAddr,
        protocol: TransportProtocol,
    ) -> Option<Vec<u8>> {
        let protocol = match protocol {
            TransportProtocol::Udp => Protocol::Udp,
            TransportProtocol::Tcp => Protocol::Tcp,
        };
        // A message without a complete header is not a DNS query and gets no response.
        let header = Header::read(&mut BinDecoder::new(&bytes)).ok()?;
        // Answering a response could start a loop between two listeners.
        if header.metadata.message_type != MessageType::Query {
            return None;
        }
        let request = match Request::from_bytes(bytes, source, protocol) {
            Ok(request) => request,
            Err(_) => {
                let metadata = header.metadata;
                return Message::error_msg(metadata.id, metadata.op_code, ResponseCode::FormErr)
                    .to_vec()
                    .ok();
            }
        };
        // RFC 1035 limits UDP responses to 512 bytes unless EDNS raises the limit.
        let limit = match protocol {
            Protocol::Udp => request.edns.as_ref().map_or(512, |edns| edns.max_payload()),
            _ => u16::MAX,
        };

        let error_code = if request.metadata.op_code != OpCode::Query || request.signature.is_some()
        {
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
                    inbound: self.inbound,
                    source,
                    protocol: match protocol {
                        Protocol::Udp => TransportProtocol::Udp,
                        _ => TransportProtocol::Tcp,
                    },
                };
                self.handler.handle(DnsRequest::from(message), info).await
            }
            .await;
            match result {
                Ok(DnsHandlerResult::Drop) => return None,
                Ok(DnsHandlerResult::Response(response)) => Ok(response),
                Err(error) => {
                    tracing::warn!(inbound_id = self.inbound.0.get(), %source, error = %format_args!("{error:#}"), "DNS policy failed");
                    Err(ResponseCode::ServFail)
                }
            }
        };

        let id = request.metadata.id;
        match result {
            Ok(response) => {
                let mut message = response.into_message();
                message.metadata.id = id;
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
                encode(response, id, limit)
            }
            Err(code) => encode(
                MessageResponseBuilder::from_message_request(&request)
                    .error_msg(&request.metadata, code),
                id,
                limit,
            ),
        }
    }
}

/// Emitting truncates a response that exceeds `limit`; a response that still
/// cannot be encoded becomes SERVFAIL.
fn encode<'a>(
    response: MessageResponse<
        '_,
        'a,
        impl Iterator<Item = &'a Record> + Send + 'a,
        impl Iterator<Item = &'a Record> + Send + 'a,
        impl Iterator<Item = &'a Record> + Send + 'a,
        impl Iterator<Item = &'a Record> + Send + 'a,
    >,
    id: u16,
    limit: u16,
) -> Option<Vec<u8>> {
    let mut bytes = Vec::with_capacity(512);
    let mut encoder = BinEncoder::new(&mut bytes);
    encoder.set_max_size(limit);

    match response.destructive_emit(&mut encoder) {
        Ok(_) => Some(bytes),
        Err(error) => {
            tracing::error!(%error, "encoding a DNS response failed");
            Message::error_msg(id, OpCode::Query, ResponseCode::ServFail)
                .to_vec()
                .ok()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Unused;

    impl Handler for Unused {
        fn handle(
            &self,
            _: DnsRequest,
            _: DnsRequestInfo,
        ) -> BoxFuture<'_, Result<DnsHandlerResult>> {
            panic!("response packets must never enter policy");
        }
    }

    #[tokio::test]
    async fn responses_get_no_reply() {
        let responder = Responder::new(InboundId(std::num::NonZeroU64::MIN), Arc::new(Unused));
        let response = vec![0x12, 0x34, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let reply = responder
            .respond(
                response,
                "127.0.0.1:53".parse().unwrap(),
                TransportProtocol::Udp,
            )
            .await;
        assert!(reply.is_none(), "listener replied to a response: {reply:?}");
    }
}
