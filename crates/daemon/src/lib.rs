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

// Bound queued calls. Native I/O may call resolve policy again, so pending calls
// must not consume a global execution budget and block their own dependencies.
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
    Dns(
        DnsHandlerId,
        Box<DnsRequest>,
        Option<DnsRequestInfo>,
        Reply<DnsHandlerResult>,
    ),
    InstallDns(Arc<dyn kotoconn_script::DnsServices>, Reply<()>),
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
        self.dns_with_info(id, request, None).await
    }

    async fn dns_with_info(
        &self,
        id: DnsHandlerId,
        request: DnsRequest,
        info: Option<DnsRequestInfo>,
    ) -> Result<DnsHandlerResult, Error> {
        let (reply, result) = oneshot::channel();
        self.send(Command::Dns(id, Box::new(request), info, reply))
            .await?;
        result.await.map_err(|_| Error::Closed)?
    }

    async fn install_dns_services(
        &self,
        services: Arc<dyn kotoconn_script::DnsServices>,
    ) -> Result<(), Error> {
        let (reply, result) = oneshot::channel();
        self.send(Command::InstallDns(services, reply)).await?;
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

// Shutdown phases: `shutdown` starts the deadline and runs policy shutdown hooks
// while admission stays open; the worker then cancels `stopping`, which closes
// admission and drains the remaining work; `force` cancels whatever is left.
struct Worker {
    shutdown: CancellationToken,
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

        let shutdown = CancellationToken::new();
        let stopping = CancellationToken::new();
        let force = CancellationToken::new();
        let (commands, receiver) = mpsc::channel(CAPACITY);
        let (ready, started) = oneshot::channel();
        let (finished, completion) = watch::channel(None);

        let deadline = tokio::spawn({
            let shutdown = shutdown.clone();
            let force = force.clone();
            async move {
                shutdown.cancelled().await;
                tokio::time::sleep(shutdown_timeout).await;
                force.cancel();
            }
        });

        // The guard also stops startup if its caller drops the start future.
        let worker = Worker {
            shutdown: shutdown.clone(),
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
                    serve(&script, receiver, shutdown, stopping, force).await
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
        let network = match network::Network::start(
            policy.clone(),
            worker.shutdown.clone(),
            worker.stopping.clone(),
            worker.force.clone(),
        )
        .await
        {
            Ok(network) => network,
            Err(error) => {
                // Evaluation may already have changed host configuration that its hooks undo.
                worker.shutdown.cancel();
                if let Err(hooks) = worker.wait().await {
                    tracing::warn!(error = %hooks, "policy shutdown after a startup failure");
                }
                return Err(error);
            }
        };

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

    /// Request shutdown without waiting for it. Policy shutdown hooks run first;
    /// admission then stops and accepted work drains.
    pub fn stop(&self) {
        self.worker.shutdown.cancel();
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
        // SAFETY: wait_for returns the value its predicate accepted, so it is Some.
        result.as_ref().expect("completion was checked").clone()
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.stopping.cancel();
        self.force.cancel();
    }
}

async fn serve(
    script: &Script,
    mut commands: mpsc::Receiver<(tracing::Span, Command)>,
    shutdown: CancellationToken,
    stopping: CancellationToken,
    force: CancellationToken,
) -> Result<Shutdown, Error> {
    let mut calls = FuturesUnordered::new();
    let mut exhausted = false;
    let driver = script.drive();
    tokio::pin!(driver);

    // Hooks run while calls are still served, so they can rely on policy handlers
    // and decide what to stop before the daemon drains the rest.
    let hooks = async {
        shutdown.cancelled().await;
        tracing::debug!("running policy shutdown hooks");
        let result = script.shutdown().await;
        stopping.cancel();
        result
    };
    tokio::pin!(hooks);
    let mut hooks_finished = false;

    loop {
        // Admission normally stops after the hooks; a daemon failure or drop can stop it while they run.
        let hooks_running = shutdown.is_cancelled() && !hooks_finished;

        if exhausted && calls.is_empty() && !hooks_running {
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
            result = &mut hooks, if !hooks_finished => {
                hooks_finished = true;
                result?;
            }
            _ = stopping.cancelled(), if !commands.is_closed() => {
                commands.close();
            }
            _ = calls.next(), if !calls.is_empty() => {}
            command = commands.recv(), if !exhausted => {
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
        Command::Dns(id, request, info, reply) => {
            let _ = reply.send(
                script
                    .dns_with_info(id, *request, info)
                    .await
                    .map_err(Into::into),
            );
        }
        Command::InstallDns(services, reply) => {
            let _ = reply.send(
                script
                    .install_dns_services(services)
                    .await
                    .map_err(Into::into),
            );
        }
    }
}
