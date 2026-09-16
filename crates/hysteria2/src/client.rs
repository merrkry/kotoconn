use crate::{
    runtime::{CloseConnection, CloseScope, ScopedRuntime},
    socket::{CarrierSocket, Salamander},
    stream, tls, udp, wire,
};
use anyhow::{Result, ensure};
use futures_util::future::BoxFuture;
use kotoconn_config::Hysteria2OutboundConfig;
use kotoconn_protocol::{
    self as p, BoxStream, Capabilities, Carrier, Datagram, Endpoint, Scope, Target,
};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

type Request = oneshot::Sender<Result<Lease>>;

pub struct Client {
    scope: Scope,
    settings: Arc<Settings>,
    requests: Mutex<Option<mpsc::Sender<Request>>>,
}

struct Lease {
    session: Arc<Session>,
    // Fields drop in declaration order: release the Arc before notifying its owner.
    _released: Released,
}

struct Released(mpsc::Sender<()>);

impl Drop for Released {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

struct Settings {
    endpoint: Endpoint,
    carrier: Arc<dyn Carrier>,
    tls: quinn::ClientConfig,
    server_name: String,
    password: String,
    obfs: Option<String>,
}

struct Session {
    connection: quinn::Connection,
    udp: Option<mpsc::Sender<udp::Registration>>,
    _endpoint: quinn::Endpoint,
    _close_connection: CloseConnection,
    _close_scope: CloseScope,
}

impl Client {
    pub fn new(
        endpoint: Endpoint,
        carrier: Arc<dyn Carrier>,
        options: &Hysteria2OutboundConfig,
    ) -> Result<Self> {
        carrier.capabilities().require(Capabilities {
            tcp: false,
            udp: true,
        })?;
        let tls = tls::client(options)?;
        let server_name = options
            .server_name
            .clone()
            .unwrap_or_else(|| match &endpoint.address {
                Target::Domain { name, .. } => name.clone(),
                Target::Ip { address, .. } => address.to_string(),
            });
        rustls::pki_types::ServerName::try_from(server_name.as_str())?;
        let scope = carrier.scope().child();
        Ok(Self {
            scope,
            requests: Mutex::new(None),
            settings: Arc::new(Settings {
                endpoint,
                carrier,
                tls,
                server_name,
                password: options.password.clone(),
                obfs: options.obfs_password.clone(),
            }),
        })
    }

    async fn session(&self, caller: &Scope) -> Result<Lease> {
        caller
            .run(async {
                loop {
                    let requests = {
                        let mut slot = self
                            .requests
                            .lock()
                            .map_err(|_| anyhow::anyhow!("Hysteria client poisoned"))?;
                        if slot.as_ref().is_none_or(|sender| sender.is_closed()) {
                            let (sender, receive) = mpsc::channel(64);
                            let settings = self.settings.clone();
                            let scope = self.scope.clone();
                            self.scope
                                .spawn(async move { manage(settings, receive, scope).await })?;
                            *slot = Some(sender);
                        }
                        // SAFETY: The slot was initialized above under this same lock.
                        slot.as_ref().expect("Hysteria manager").clone()
                    };
                    let (reply, receive) = oneshot::channel();
                    if requests.send(reply).await.is_err() {
                        continue;
                    }
                    match receive.await {
                        Ok(result) => return result,
                        // An idle manager can retire while this request is enqueued.
                        // Retry against a new owner; no proxy stream has been opened.
                        Err(_) if !self.scope.is_closed() => continue,
                        Err(error) => return Err(error.into()),
                    }
                }
            })
            .await
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.scope.close();
    }
}

impl p::Client for Client {
    fn capabilities(&self) -> Capabilities {
        Capabilities::BOTH
    }

    fn tcp(&self, target: Target, scope: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        Box::pin(async move {
            let lease = self.session(&scope).await?;
            let session = &lease.session;
            let mut stream = tokio::time::timeout(tls::IO_TIMEOUT, async {
                let mut stream = stream::open(&session.connection).await?;
                wire::request(&mut stream, &target).await?;
                wire::read_response(&mut stream).await?;
                Ok::<_, anyhow::Error>(stream)
            })
            .await??;
            // Keep the session alive until the stream is handed to its caller.
            tokio::io::AsyncWriteExt::flush(&mut stream).await?;
            stream.retain(Box::new(lease));
            Ok(Box::pin(stream) as BoxStream)
        })
    }

    fn udp(&self, _: Target, scope: Scope) -> BoxFuture<'_, Result<Datagram>> {
        Box::pin(async move {
            let lease = self.session(&scope).await?;
            let session = &lease.session;
            let registrations = session
                .udp
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Hysteria server disabled UDP"))?;
            let datagram = scope
                .run(udp::register(registrations, scope.clone()))
                .await?;
            // Keep the shared connection while this independently closable UDP
            // entry is alive. Cancellation drops the lease before completion.
            let lifetime = scope.clone();
            scope.spawn(async move {
                let _lease = lease;
                lifetime.cancelled().await;
                Ok(())
            })?;
            Ok(datagram)
        })
    }
}

async fn manage(
    settings: Arc<Settings>,
    mut requests: mpsc::Receiver<Request>,
    scope: Scope,
) -> Result<()> {
    let mut session: Option<Arc<Session>> = None;
    let (released, mut releases) = mpsc::channel(1);
    loop {
        let reply = tokio::select! {
            biased;
            reply = requests.recv() => {
                let Some(reply) = reply else { return Ok(()); };
                reply
            }
            _ = releases.recv() => {
                if session.as_ref().is_none_or(|value| Arc::strong_count(value) == 1) {
                    // No active stream or UDP entry needs the shared connection.
                    // Retiring immediately lets daemon shutdown drain without a
                    // timer or a permanently registered pool worker.
                    return Ok(());
                }
                continue;
            }
        };
        if reply.is_closed() {
            if session
                .as_ref()
                .is_none_or(|value| Arc::strong_count(value) == 1)
            {
                return Ok(());
            }
            continue;
        }
        if session
            .as_ref()
            .is_some_and(|value| value.connection.close_reason().is_some())
        {
            session = None;
        }
        if session.is_none() {
            match tokio::time::timeout(tls::IO_TIMEOUT, connect(&settings, scope.child()))
                .await
                .map_err(anyhow::Error::from)
                .and_then(|result| result)
            {
                Ok(value) => session = Some(Arc::new(value)),
                Err(error) => {
                    let _ = reply.send(Err(error));
                    return Ok(());
                }
            }
        }
        // SAFETY: A connection error returns above; otherwise session is populated.
        let lease = Lease {
            session: session.as_ref().expect("connected session").clone(),
            _released: Released(released.clone()),
        };
        let _ = reply.send(Ok(lease));
    }
}

async fn connect(settings: &Settings, scope: Scope) -> Result<Session> {
    let close_scope = CloseScope(scope.clone());
    let peer = p::socket_addr(&settings.endpoint.resolve().await?)?;
    let transport = settings
        .carrier
        .udp_scoped(p::target(peer), scope.clone())
        .await?;
    let socket = Salamander::wrap(
        Arc::new(CarrierSocket::new(transport, peer)),
        settings.obfs.as_deref(),
    );
    let mut endpoint = quinn::Endpoint::new_with_abstract_socket(
        Default::default(),
        None,
        socket,
        Arc::new(ScopedRuntime(scope.clone())),
    )?;
    endpoint.set_default_client_config(settings.tls.clone());
    let connection = endpoint.connect(peer, &settings.server_name)?.await?;
    let close_connection = CloseConnection(connection.clone());
    let (mut driver, mut sender) =
        h3::client::new(h3_quinn::Connection::new(connection.clone())).await?;
    let close = connection.clone();
    let keep_sender = sender.clone();
    scope.spawn(async move {
        let _sender = keep_sender;
        let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
        close.close(0u32.into(), b"HTTP/3 closed");
        Ok(())
    })?;
    let request = http::Request::post("https://hysteria/auth")
        .header("Hysteria-Auth", &settings.password)
        .header("Hysteria-CC-RX", "0")
        .header("Hysteria-Padding", wire::padding())
        .body(())?;
    let mut auth = sender.send_request(request).await?;
    auth.finish().await?;
    let response = auth.recv_response().await?;
    ensure!(
        response.status().as_u16() == 233,
        "Hysteria authentication failed: {}",
        response.status()
    );
    let udp_enabled = response
        .headers()
        .get("Hysteria-UDP")
        .is_some_and(|value| value == "true");
    let udp = if udp_enabled {
        ensure!(
            connection.max_datagram_size().is_some(),
            "Hysteria server advertised UDP without QUIC datagrams"
        );
        let (registrations, receive) = mpsc::channel(64);
        let connection = connection.clone();
        let owner = scope.clone();
        scope.spawn(async move {
            let _close = CloseScope(owner);
            udp::client(connection, receive).await
        })?;
        Some(registrations)
    } else {
        None
    };
    // Keep h3's request sender alive in its driver so dropping authentication does
    // not send GOAWAY on the QUIC connection shared with proxy streams.
    Ok(Session {
        connection,
        udp,
        _endpoint: endpoint,
        _close_connection: close_connection,
        _close_scope: close_scope,
    })
}
