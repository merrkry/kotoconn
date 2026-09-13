//! Daemon lifecycle and thread-safe access to one policy state machine.

use futures_util::{StreamExt, stream::FuturesUnordered};
use kotoconn_config::*;
use kotoconn_script::{ModuleSource, Script};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::{
    runtime::{Handle, RuntimeFlavor},
    sync::{mpsc, oneshot, watch},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::Instrument;

mod network;
mod source;
pub use kotoconn_inbounds::InboundAddress;
pub use network::{SessionHandle, SessionId};

// Bound both queued and executing calls. Awaiting admission provides backpressure.
const CAPACITY: usize = 64;

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("network: {0}")]
    Network(String),
    #[error("configuration: {0}")]
    Config(String),
    #[error("policy service is closed")]
    Closed,
    #[error("policy: {0}")]
    Script(String),
    #[error("policy worker: {0}")]
    Worker(String),
}

impl From<kotoconn_script::Error> for Error {
    fn from(error: kotoconn_script::Error) -> Self {
        Self::Script(error.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shutdown {
    Drained,
    TimedOut,
}

type Reply<T> = oneshot::Sender<Result<T, Error>>;

enum Command {
    Route(RoutingHandlerId, Flow, Reply<RouteDecision>),
    Resolve(ResolveHandlerId, String, Reply<Vec<IpAddr>>),
    Dns(DnsHandlerId, Box<DnsRequest>, Reply<DnsHandlerResult>),
}

/// Cloneable shared reference. Only owned Rust values cross the JS thread boundary.
#[derive(Clone)]
pub struct Policy {
    commands: mpsc::Sender<(tracing::Span, Command)>,
    stopping: CancellationToken,
    config: Arc<Config>,
}

impl Policy {
    pub fn config(&self) -> &Config {
        &self.config
    }

    #[tracing::instrument(skip_all, fields(handler_id = id.0.get()), err)]
    pub async fn route(&self, id: RoutingHandlerId, flow: Flow) -> Result<RouteDecision, Error> {
        let (reply, result) = oneshot::channel();
        self.send(Command::Route(id, flow, reply)).await?;
        result.await.map_err(|_| Error::Closed)?
    }

    #[tracing::instrument(skip_all, fields(handler_id = id.0.get(), %name), err)]
    pub async fn resolve(&self, id: ResolveHandlerId, name: String) -> Result<Vec<IpAddr>, Error> {
        let (reply, result) = oneshot::channel();
        self.send(Command::Resolve(id, name, reply)).await?;
        result.await.map_err(|_| Error::Closed)?
    }

    #[tracing::instrument(skip_all, fields(handler_id = id.0.get()), err)]
    pub async fn dns(
        &self,
        id: DnsHandlerId,
        request: DnsRequest,
    ) -> Result<DnsHandlerResult, Error> {
        let (reply, result) = oneshot::channel();
        self.send(Command::Dns(id, Box::new(request), reply))
            .await?;
        result.await.map_err(|_| Error::Closed)?
    }

    async fn send(&self, command: Command) -> Result<(), Error> {
        tokio::select! {
            biased;
            _ = self.stopping.cancelled() => Err(Error::Closed),
            result = self.commands.send((tracing::Span::current(), command)) => result.map_err(|_| Error::Closed),
        }
    }
}

/// Owns the policy worker. Keep this alive while native components use its Policy.
/// Explicit shutdown drains calls; dropping the owner requests immediate cancellation.
pub struct Daemon {
    policy: Policy,
    worker: Worker,
    network: network::Network,
}

struct Worker {
    stopping: CancellationToken,
    force: CancellationToken,
    finished: watch::Receiver<Option<Result<Shutdown, Error>>>,
    _deadline: AbortOnDropHandle<()>,
}

impl Daemon {
    /// Read a TypeScript policy file and its imports, then start its worker.
    /// Must run inside a Tokio multi-thread runtime.
    pub async fn start(path: impl AsRef<Path>, shutdown_timeout: Duration) -> Result<Self, Error> {
        let path = path.as_ref();
        let (entry, source) = source::Files::open(path)
            .map_err(|error| Error::Config(format!("{}: {error}", path.display())))?;

        Self::start_with_sources(entry, source, shutdown_timeout).await
    }

    /// Embed a source provider without filesystem access. Startup may use top-level await.
    pub async fn start_with_sources(
        entry: String,
        sources: impl ModuleSource + Send + 'static,
        shutdown_timeout: Duration,
    ) -> Result<Self, Error> {
        let runtime = Handle::try_current().map_err(|error| Error::Worker(error.to_string()))?;

        if runtime.runtime_flavor() != RuntimeFlavor::MultiThread {
            return Err(Error::Worker(
                "a Tokio multi-thread runtime is required".into(),
            ));
        }

        let stopping = CancellationToken::new();
        let force = CancellationToken::new();
        let (commands, receiver) = mpsc::channel(CAPACITY);
        let (ready, started) = oneshot::channel();
        let (finished, completion) = watch::channel(None);

        let deadline = tokio::spawn({
            let stopping = stopping.clone();
            let force = force.clone();
            async move {
                stopping.cancelled().await;
                tokio::time::sleep(shutdown_timeout).await;
                force.cancel();
            }
        });

        // The guard also stops startup if its caller drops the start future.
        let worker = Worker {
            stopping: stopping.clone(),
            force: force.clone(),
            finished: completion,
            _deadline: AbortOnDropHandle::new(deadline),
        };

        // The dedicated worker needs the caller's subscriber as well as its span.
        let span = tracing::Span::current();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        std::thread::Builder::new()
            .name("kotoconn-policy".into())
            .spawn(move || {
                let _dispatch = tracing::dispatcher::set_default(&dispatch);
                let _span = span.enter();
                let result = runtime.block_on(async {
                    let interrupt = force.clone();

                    let script = tokio::select! {
                        biased;
                        _ = force.cancelled() => return Ok(Shutdown::TimedOut),
                        result = Script::load_with_interrupt(&entry, sources, move || interrupt.is_cancelled()) => {
                            result?
                        }
                    };

                    let config = Arc::new(script.config().await?);
                    if ready.send(config).is_err() {
                        return Ok(Shutdown::Drained);
                    }
                    serve(&script, receiver, stopping, force).await
                });
                // The worker drops all JS state before reporting completion.
                let _ = finished.send(Some(result));
            })
            .map_err(|error| Error::Worker(error.to_string()))?;

        let config = match started.await {
            Ok(config) => config,
            Err(_) => return Err(worker.wait().await.err().unwrap_or(Error::Closed)),
        };

        let policy = Policy {
            commands,
            stopping: worker.stopping.clone(),
            config,
        };
        let network = network::Network::start(
            policy.clone(),
            worker.stopping.clone(),
            worker.force.clone(),
        )
        .await?;

        Ok(Self {
            policy,
            worker,
            network,
        })
    }

    /// Actual bound addresses, including OS-assigned ports.
    pub fn listen_addresses(&self) -> &std::collections::HashMap<InboundId, std::net::SocketAddr> {
        &self.network.addresses
    }

    /// Bound socket addresses and TUN interface names, indexed by inbound.
    pub fn inbound_addresses(&self) -> &std::collections::HashMap<InboundId, InboundAddress> {
        &self.network.inbound_addresses
    }

    pub async fn sessions(&self) -> anyhow::Result<Vec<SessionHandle>> {
        self.network.sessions.list().await
    }

    pub fn client_control(
        &self,
        id: DialerId,
        protocol: TransportProtocol,
    ) -> Option<kotoconn_protocol::Scope> {
        self.network
            .clients
            .get(&id)
            .map(|client| client.control(protocol))
    }

    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    /// Request admission shutdown without waiting for accepted sessions to drain.
    pub fn stop(&self) {
        self.policy.stopping.cancel();
    }

    /// Idempotent; cancellation of this future does not cancel the shutdown request.
    pub async fn shutdown(&self) -> Result<Shutdown, Error> {
        self.stop();
        self.wait().await
    }

    /// Also reports worker failures before a shutdown request.
    pub async fn wait(&self) -> Result<Shutdown, Error> {
        // Wait for both sides so network sessions can drain while the policy
        // worker finishes its outstanding handler calls.
        match tokio::try_join!(self.worker.wait(), self.network.wait()) {
            Ok((policy, network)) => Ok(
                if policy == Shutdown::TimedOut || network == Shutdown::TimedOut {
                    Shutdown::TimedOut
                } else {
                    Shutdown::Drained
                },
            ),
            Err(error) => {
                self.policy.stopping.cancel();
                self.worker.force.cancel();
                Err(error)
            }
        }
    }
}

impl Worker {
    async fn wait(&self) -> Result<Shutdown, Error> {
        let mut finished = self.finished.clone();
        let result = finished
            .wait_for(|result| result.is_some())
            .await
            .map_err(|_| Error::Worker("thread exited without a result".into()))?;
        result.as_ref().expect("completion was checked").clone()
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.stopping.cancel();
        self.force.cancel();
    }
}

async fn serve(
    script: &Script,
    mut commands: mpsc::Receiver<(tracing::Span, Command)>,
    stopping: CancellationToken,
    force: CancellationToken,
) -> Result<Shutdown, Error> {
    let mut calls = FuturesUnordered::new();
    let mut exhausted = false;
    let driver = script.drive();
    tokio::pin!(driver);

    loop {
        if exhausted && calls.is_empty() {
            // Include native promises started without awaiting them in the handler.
            return tokio::select! {
                biased;
                _ = force.cancelled() => Ok(Shutdown::TimedOut),
                _ = script.idle() => Ok(Shutdown::Drained),
            };
        }
        tokio::select! {
            biased;
            _ = force.cancelled() => {
                return Ok(Shutdown::TimedOut);
            }
            _ = stopping.cancelled(), if !commands.is_closed() => {
                commands.close();
            }
            _ = calls.next(), if !calls.is_empty() => {}
            command = commands.recv(), if !exhausted && calls.len() < CAPACITY => {
                match command {
                    Some((span, command)) => {
                        calls.push(dispatch(script, command).instrument(span));
                    }
                    None => exhausted = true,
                }
            }
            _ = &mut driver => unreachable!("QuickJS driver lives while its runtime exists"),
        }
    }
}

async fn dispatch(script: &Script, command: Command) {
    tracing::debug!("dispatching policy call");

    match command {
        Command::Route(id, flow, reply) => {
            let _ = reply.send(script.route(id, flow).await.map_err(Into::into));
        }
        Command::Resolve(id, name, reply) => {
            let _ = reply.send(script.resolve(id, &name).await.map_err(Into::into));
        }
        Command::Dns(id, request, reply) => {
            let _ = reply.send(script.dns(id, *request).await.map_err(Into::into));
        }
    }
}
