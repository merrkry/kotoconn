//! Daemon lifecycle and thread-safe access to one policy state machine.

use futures_util::{StreamExt, stream::FuturesUnordered};
use kotoconn_config::*;
pub use kotoconn_inbounds::InboundAddress;
use kotoconn_script::{ModuleSource, Script};
use resources::{Manager, Tier};
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};
use tokio::{
    runtime::{Handle, RuntimeFlavor},
    sync::{mpsc, oneshot, watch},
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::Instrument;

mod network;
mod resources;
mod source;
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
    Route(RoutingHandler, Flow, Reply<RouteDecision>),
    Resolve(ResolveHandler, String, Reply<Vec<IpAddr>>),
    Dns(
        DnsHandler,
        Box<DnsRequest>,
        Option<DnsRequestInfo>,
        Reply<DnsHandlerResult>,
    ),
}

/// Cloneable shared reference. Only owned Rust values cross the JS thread boundary.
/// A call holds its handler reference, so the handler stays registered meanwhile.
#[derive(Clone)]
pub struct Policy {
    commands: mpsc::Sender<(tracing::Span, Command)>,
    stopping: CancellationToken,
}

impl Policy {
    #[tracing::instrument(skip_all, fields(handler_id = handler.id().get()), err)]
    pub async fn route(
        &self,
        handler: &RoutingHandler,
        flow: Flow,
    ) -> Result<RouteDecision, Error> {
        let (reply, result) = oneshot::channel();
        self.send(Command::Route(handler.clone(), flow, reply))
            .await?;
        result.await.map_err(|_| Error::Closed)?
    }

    #[tracing::instrument(skip_all, fields(handler_id = handler.id().get(), %name), err)]
    pub async fn resolve(
        &self,
        handler: &ResolveHandler,
        name: String,
    ) -> Result<Vec<IpAddr>, Error> {
        let (reply, result) = oneshot::channel();
        self.send(Command::Resolve(handler.clone(), name, reply))
            .await?;
        result.await.map_err(|_| Error::Closed)?
    }

    #[tracing::instrument(skip_all, fields(handler_id = handler.id().get()), err)]
    pub async fn dns(
        &self,
        handler: &DnsHandler,
        request: DnsRequest,
        info: Option<DnsRequestInfo>,
    ) -> Result<DnsHandlerResult, Error> {
        let (reply, result) = oneshot::channel();
        self.send(Command::Dns(
            handler.clone(),
            Box::new(request),
            info,
            reply,
        ))
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

/// Owns the policy worker and the resources policy creates. Keep this alive while
/// native components use its Policy. Explicit shutdown drains calls and resources;
/// dropping the owner requests immediate cancellation.
pub struct Daemon {
    policy: Policy,
    worker: Worker,
    resources: Arc<Manager>,
}

// Shutdown phases: `shutdown` starts the deadline and runs policy shutdown hooks
// while admission stays open; the worker then cancels `stopping`, which closes
// admission and drains entry points and policy calls, after which `calls` lets the
// resources that serve them drain; `force` aborts whatever is left.
struct Worker {
    shutdown: CancellationToken,
    stopping: CancellationToken,
    force: CancellationToken,
    finished: watch::Receiver<Option<Result<Shutdown, Error>>>,
    /// Cancelled once every resource has been released after shutdown.
    released: CancellationToken,
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
        let calls = CancellationToken::new();
        let released = CancellationToken::new();
        let (commands, receiver) = mpsc::channel(CAPACITY);
        let (ready, started) = oneshot::channel();
        let (finished, completion) = watch::channel(None);

        let policy = Policy {
            commands,
            stopping: stopping.clone(),
        };
        let resources = Manager::new(policy.clone(), shutdown.clone());

        let deadline = tokio::spawn({
            let shutdown = shutdown.clone();
            let force = force.clone();
            async move {
                shutdown.cancelled().await;
                tokio::time::sleep(shutdown_timeout).await;
                force.cancel();
            }
        });

        // Detached, so that dropping the owner still aborts every resource.
        tokio::spawn(
            drain(
                resources.clone(),
                stopping.clone(),
                calls.clone(),
                force.clone(),
                released.clone(),
            )
            .in_current_span(),
        );

        // The guard also stops startup if its caller drops the start future.
        let worker = Worker {
            shutdown: shutdown.clone(),
            stopping: stopping.clone(),
            force: force.clone(),
            finished: completion,
            released,
            _deadline: AbortOnDropHandle::new(deadline),
        };

        // The dedicated worker needs the caller's subscriber as well as its span.
        let span = tracing::Span::current();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let manager = resources.clone();
        std::thread::Builder::new()
            .name("kotoconn-policy".into())
            .spawn(move || {
                let _dispatch = tracing::dispatcher::set_default(&dispatch);
                let _span = span.enter();
                // Resources that serve policy calls drain once no call can start.
                let _calls = calls.clone().drop_guard();
                let tokens = Tokens {
                    shutdown,
                    stopping,
                    force,
                    calls,
                };

                let result = runtime.block_on(async {
                    let interrupt = tokens.force.clone();
                    let script =
                        Script::new(sources, manager, move || interrupt.is_cancelled()).await?;
                    serve(&script, &entry, ready, receiver, &tokens).await
                });
                // The worker drops all JS state before reporting completion.
                let _ = finished.send(Some(result));
            })
            .map_err(|error| Error::Worker(error.to_string()))?;

        let daemon = Self {
            policy,
            worker,
            resources,
        };
        match started.await {
            Ok(Ok(())) => Ok(daemon),
            // A failed startup still runs the shutdown hooks and releases the
            // resources that evaluation created.
            Ok(Err(error)) => {
                if let Err(shutdown) = daemon.wait().await {
                    tracing::warn!(error = %shutdown, "shutdown after a startup failure");
                }
                Err(error)
            }
            Err(_) => Err(daemon.worker.wait().await.err().unwrap_or(Error::Closed)),
        }
    }

    /// Actual bound socket addresses of open inbounds and DNS listeners, including
    /// OS-assigned ports.
    pub fn listen_addresses(&self) -> HashMap<InboundId, std::net::SocketAddr> {
        self.resources
            .inbound_addresses()
            .into_iter()
            .filter_map(|(id, address)| match address {
                InboundAddress::Socket(address) => Some((id, address)),
                InboundAddress::Tun(_) => None,
            })
            .collect()
    }

    /// Bound socket addresses and TUN interface names, indexed by inbound.
    pub fn inbound_addresses(&self) -> HashMap<InboundId, InboundAddress> {
        self.resources.inbound_addresses()
    }

    /// Configurations of the resources that have not been released, by creation order.
    pub fn resources(&self) -> Vec<(ResourceId, ResourceConfig)> {
        self.resources.resources()
    }

    pub async fn sessions(&self) -> anyhow::Result<Vec<SessionHandle>> {
        self.resources.sessions.list().await
    }

    pub fn client_control(
        &self,
        id: DialerId,
        protocol: TransportProtocol,
    ) -> Option<kotoconn_protocol::Scope> {
        self.resources.client_control(id, protocol)
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

    /// Also reports worker and inbound failures before a shutdown request.
    pub async fn wait(&self) -> Result<Shutdown, Error> {
        // Resources drain while the policy worker finishes its outstanding handler
        // calls. An inbound failure has already started the shutdown, so only a
        // failed worker aborts the resources.
        let policy = async {
            let result = self.worker.wait().await;
            if result.is_err() {
                self.worker.stopping.cancel();
                self.worker.force.cancel();
            }
            result
        };
        let (policy, ()) = tokio::join!(policy, self.worker.released.cancelled());

        let network = match self.resources.failure() {
            Some(error) => Err(Error::Network(error)),
            None if self.worker.force.is_cancelled() => Ok(Shutdown::TimedOut),
            None => Ok(Shutdown::Drained),
        };

        let (policy, network) = (policy?, network?);
        Ok(
            if policy == Shutdown::TimedOut || network == Shutdown::TimedOut {
                Shutdown::TimedOut
            } else {
                Shutdown::Drained
            },
        )
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

/// Drains resources in dependency order once admission stops: entry points first,
/// then, once they are released and no policy call remains, the resources that
/// served them. The shutdown deadline aborts the rest.
async fn drain(
    resources: Arc<Manager>,
    stopping: CancellationToken,
    calls: CancellationToken,
    force: CancellationToken,
    released: CancellationToken,
) {
    let ordered = async {
        stopping.cancelled().await;
        tracing::debug!("draining resources");
        resources.drain_tier(Tier::EntryPoint);
        tokio::join!(resources.released_tier(Tier::EntryPoint), calls.cancelled());
        resources.drain_tier(Tier::Service);
        resources.released_tier(Tier::Service).await;
    };

    tokio::select! {
        biased;
        _ = force.cancelled() => {
            resources.abort();
            tokio::join!(
                resources.released_tier(Tier::EntryPoint),
                resources.released_tier(Tier::Service),
            );
        }
        _ = ordered => {}
    }
    released.cancel();
}

struct Tokens {
    shutdown: CancellationToken,
    stopping: CancellationToken,
    force: CancellationToken,
    /// Cancelled once no policy call can start or remains.
    calls: CancellationToken,
}

async fn serve(
    script: &Script,
    entry: &str,
    ready: oneshot::Sender<Result<(), Error>>,
    mut commands: mpsc::Receiver<(tracing::Span, Command)>,
    tokens: &Tokens,
) -> Result<Shutdown, Error> {
    let Tokens {
        shutdown,
        stopping,
        force,
        calls: drained,
    } = tokens;
    let mut calls = FuturesUnordered::new();
    let mut exhausted = false;
    let driver = script.drive();
    tokio::pin!(driver);

    // Handlers can serve calls while the entry module evaluates, for example to
    // resolve a server name for a connection that evaluation awaits.
    let evaluation = script.evaluate(entry);
    tokio::pin!(evaluation);
    let mut ready = Some(ready);

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

        if exhausted && calls.is_empty() && !hooks_running && ready.is_none() {
            drained.cancel();
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
            result = &mut evaluation, if ready.is_some() => {
                let result = result.map_err(Error::from);
                if let Err(error) = &result {
                    tracing::debug!(%error, "policy evaluation failed");
                    shutdown.cancel();
                }
                // SAFETY: this branch runs only while the sender remains.
                let _ = ready.take().expect("evaluation reports once").send(result);
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
        Command::Route(handler, flow, reply) => {
            let _ = reply.send(script.route(&handler, flow).await.map_err(Into::into));
        }
        Command::Resolve(handler, name, reply) => {
            let _ = reply.send(script.resolve(&handler, &name).await.map_err(Into::into));
        }
        Command::Dns(handler, request, info, reply) => {
            let _ = reply.send(
                script
                    .dns(&handler, *request, info)
                    .await
                    .map_err(Into::into),
            );
        }
    }
}
