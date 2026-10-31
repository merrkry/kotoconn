//! The native resource manager: creation, references between resources and lifetimes.

use crate::{
    Policy,
    network::{self, PolicyResolver, SessionHandler, sessions::Sessions},
};
use anyhow::{Context, Result, bail, ensure};
use futures_util::{FutureExt, future::BoxFuture};
use kotoconn_config::*;
use kotoconn_inbounds::InboundAddress;
use kotoconn_outbounds::{Clients, System};
use kotoconn_protocol::{Carrier, Scope, ServerContext};
use std::{
    any::Any,
    collections::HashMap,
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex, MutexGuard, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::watch,
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

/// Resources that daemon shutdown drains first. The others serve them, and drain
/// once these have been released and policy calls have finished.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tier {
    EntryPoint,
    Service,
}

pub(crate) struct Manager {
    policy: Policy,
    /// Parent of every resource scope; closing it aborts all resources.
    root: Scope,
    pub sessions: Sessions,
    next: AtomicU64,
    registry: Mutex<Registry>,
    /// The first inbound failure, which also starts daemon shutdown.
    failure: watch::Sender<Option<String>>,
    shutdown: CancellationToken,
    this: Weak<Manager>,
}

struct Registry {
    /// Cleared once daemon shutdown starts draining; later creations fail.
    accepting: bool,
    entries: HashMap<ResourceId, Entry>,
}

struct Entry {
    tier: Tier,
    /// `None` once released, while the native state is destroyed; waiters still
    /// find the entry until that finishes.
    live: Option<Live>,
    lifetime: Arc<Lifetime>,
    /// The resource this one was created on, such as a dialer's carrier.
    uses: Option<ResourceId>,
    /// Open resources created on this one; it cannot close while any remain.
    users: usize,
}

struct Live {
    config: ResourceConfig,
    native: Native,
}

enum Native {
    Inbound(InboundAddress),
    Dialer(Arc<Clients>),
    DnsService(Arc<DnsService>),
}

pub(crate) struct DnsService {
    service: kotoconn_dns::Service,
    /// Tracks queries in flight, which a drain lets finish.
    queries: Scope,
}

/// Abort or drain state shared by a resource's supervisor and its controls.
struct Lifetime {
    scope: Scope,
    /// Cancelled when close or drain starts; the resource then admits no new work.
    admission: CancellationToken,
    abort: CancellationToken,
    /// Fixed by the drain that starts termination.
    deadline: watch::Sender<Option<Instant>>,
    released: CancellationToken,
}

/// A resource that is ready to start once registered.
struct Built {
    native: Native,
    lifetime: Arc<Lifetime>,
    uses: Option<ResourceId>,
    /// Completes when the resource has no accepted work left to drain.
    idle: BoxFuture<'static, ()>,
    /// Releases resources that outlive accepted work, such as idle connection pools.
    on_drain: Option<Box<dyn FnOnce() + Send>>,
    /// Work that runs in the resource scope, such as an inbound's accept loop.
    run: Option<BoxFuture<'static, Result<()>>>,
}

/// A reference held while a resource is created on it, so it cannot close meanwhile.
struct Reservation<'a> {
    manager: &'a Manager,
    id: Option<ResourceId>,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            self.manager.release_user(&mut self.manager.registry(), id);
        }
    }
}

impl Lifetime {
    fn new(scope: Scope) -> Arc<Self> {
        Arc::new(Self {
            scope,
            admission: CancellationToken::new(),
            abort: CancellationToken::new(),
            deadline: watch::Sender::new(None),
            released: CancellationToken::new(),
        })
    }

    fn is_open(&self) -> bool {
        !self.admission.is_cancelled()
    }
}

impl Manager {
    pub fn new(policy: Policy, shutdown: CancellationToken) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            policy,
            root: Scope::new(),
            sessions: Sessions::new(),
            next: AtomicU64::new(0),
            registry: Mutex::new(Registry {
                accepting: true,
                entries: HashMap::new(),
            }),
            failure: watch::Sender::new(None),
            shutdown,
            this: this.clone(),
        })
    }

    fn registry(&self) -> MutexGuard<'_, Registry> {
        // Registry updates keep it consistent at every step, so a panic elsewhere
        // leaves a usable registry.
        self.registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn failure(&self) -> Option<String> {
        self.failure.borrow().clone()
    }

    /// Abort every resource, including ones still being created.
    pub fn abort(&self) {
        let mut registry = self.registry();
        registry.accepting = false;
        self.root.close();
    }

    /// An open dialer for new work.
    pub fn dialer(&self, id: DialerId) -> Result<Arc<Clients>> {
        self.open_dialer(id).map(|(clients, _)| clients)
    }

    fn open_dialer(&self, id: DialerId) -> Result<(Arc<Clients>, Arc<Lifetime>)> {
        let registry = self.registry();
        let entry = registry.entries.get(&id.into());
        match entry.and_then(|entry| Some((&entry.live.as_ref()?.native, &entry.lifetime))) {
            Some((Native::Dialer(clients), lifetime)) if lifetime.is_open() => {
                Ok((clients.clone(), lifetime.clone()))
            }
            Some((Native::Dialer(_), _)) | None => bail!("dialer is closed"),
            Some(_) => bail!("resource is not a dialer"),
        }
    }

    pub fn client_control(&self, id: DialerId, protocol: TransportProtocol) -> Option<Scope> {
        match self.registry().native(id.into())? {
            (Native::Dialer(clients), _) => Some(clients.control(protocol)),
            _ => None,
        }
    }

    /// Configurations of resources that have not been released.
    pub fn resources(&self) -> Vec<(ResourceId, ResourceConfig)> {
        let mut resources: Vec<_> = self
            .registry()
            .entries
            .iter()
            .filter_map(|(id, entry)| Some((*id, entry.live.as_ref()?.config.clone())))
            .collect();
        resources.sort_by_key(|(id, _)| id.0);
        resources
    }

    pub fn inbound_addresses(&self) -> HashMap<InboundId, InboundAddress> {
        self.registry()
            .entries
            .iter()
            .filter_map(|(id, entry)| match &entry.live.as_ref()?.native {
                Native::Inbound(address) => Some((InboundId(id.0), address.clone())),
                _ => None,
            })
            .collect()
    }

    async fn create(self: &Arc<Self>, config: ResourceConfig) -> Result<ResourceId> {
        ensure!(self.registry().accepting, "the daemon is shutting down");

        let id = self
            .next
            .fetch_add(1, Ordering::Relaxed)
            .checked_add(1)
            .and_then(std::num::NonZeroU64::new)
            .map(ResourceId)
            .context("resource IDs exhausted")?;

        let mut reservation = Reservation {
            manager: self,
            id: None,
        };
        let built = match &config {
            ResourceConfig::Inbound(config) => self.inbound(InboundId(id.0), config).await?,
            ResourceConfig::Dialer(config) => self.build_dialer(config, &mut reservation)?,
            ResourceConfig::DnsService(config) => self.dns_service(config, &mut reservation)?,
            ResourceConfig::DnsListener(config) => {
                self.dns_listener(InboundId(id.0), config).await?
            }
        };
        debug_assert_eq!(built.uses, reservation.id);

        let lifetime = built.lifetime.clone();
        let mut registry = self.registry();
        if !registry.accepting {
            lifetime.scope.close();
            bail!("the daemon is shutting down");
        }
        // The reservation becomes the entry's use of the resource it was created on.
        reservation.id = None;
        registry.entries.insert(
            id,
            Entry {
                tier: built.native.tier(),
                live: Some(Live {
                    config,
                    native: built.native,
                }),
                lifetime: lifetime.clone(),
                uses: built.uses,
                users: 0,
            },
        );
        drop(registry);

        // The supervisor releases every registered entry, including one whose
        // scope closed before its work could start.
        tokio::spawn(
            self.clone()
                .supervise(id, lifetime.clone(), built.idle, built.on_drain)
                .in_current_span(),
        );
        if let Some(run) = built.run {
            self.run(id, &lifetime.scope, run)?;
        }
        tracing::debug!(resource_id = id.0.get(), "resource created");
        Ok(id)
    }

    /// Reserves an open resource of the expected kind to create another on it.
    fn reserve<T>(
        &self,
        reservation: &mut Reservation<'_>,
        id: ResourceId,
        select: impl FnOnce(&Native) -> Option<T>,
    ) -> Result<T> {
        let mut registry = self.registry();
        let entry = registry
            .entries
            .get_mut(&id)
            .filter(|entry| entry.lifetime.is_open())
            .context("referenced resource is closed")?;
        let live = entry
            .live
            .as_ref()
            .context("referenced resource is closed")?;
        let selected = select(&live.native).context("referenced resource has the wrong kind")?;

        entry.users += 1;
        reservation.id = Some(id);
        Ok(selected)
    }

    fn release_user(&self, registry: &mut Registry, id: ResourceId) {
        if let Some(entry) = registry.entries.get_mut(&id) {
            debug_assert!(entry.users > 0, "each use is released once");
            entry.users = entry.users.saturating_sub(1);
        }
    }

    async fn inbound(&self, id: InboundId, config: &InboundConfig) -> Result<Built> {
        ensure!(
            !config.udp_idle_timeout.is_zero(),
            "UDP idle timeout must be positive"
        );
        if let Some(sniff) = &config.sniff {
            ensure!(!sniff.timeout.is_zero(), "sniff timeout must be positive");
        }

        let lifetime = Lifetime::new(self.root.child());
        let handler = Arc::new(SessionHandler {
            inbound: id,
            policy: self.policy.clone(),
            manager: self.this.clone(),
            routing: config.routing_handler.clone(),
            sessions: self.sessions.clone(),
            idle: config.udp_idle_timeout,
            sniff: config.sniff.clone(),
            stopping: lifetime.admission.clone(),
        });

        let bound = kotoconn_inbounds::bind(
            config.implementation.clone(),
            ServerContext {
                handler,
                scope: lifetime.scope.clone(),
                stopping: lifetime.admission.clone(),
                udp_idle_timeout: config.udp_idle_timeout,
            },
        )
        .await?;
        tracing::info!(inbound_id = id.0.get(), address = ?bound.address, "inbound bound");

        let scope = lifetime.scope.clone();
        Ok(Built {
            native: Native::Inbound(bound.address),
            lifetime,
            uses: None,
            idle: Box::pin(async move { scope.wait().await }),
            on_drain: None,
            run: Some(bound.run),
        })
    }

    async fn dns_listener(&self, id: InboundId, config: &DnsListenerConfig) -> Result<Built> {
        let lifetime = Lifetime::new(self.root.child());
        let handler = Arc::new(network::dns::Handler {
            policy: self.policy.clone(),
            handler: config.dns_handler.clone(),
        });

        let bound = kotoconn_dns::bind(
            id,
            config,
            handler,
            lifetime.admission.clone(),
            lifetime.scope.clone(),
        )
        .await?;
        tracing::info!(inbound_id = id.0.get(), address = %bound.address, "DNS listener bound");

        let scope = lifetime.scope.clone();
        Ok(Built {
            native: Native::Inbound(InboundAddress::Socket(bound.address)),
            lifetime,
            uses: None,
            idle: Box::pin(async move { scope.wait().await }),
            on_drain: None,
            run: Some(bound.run),
        })
    }

    fn build_dialer(
        &self,
        config: &DialerConfig,
        reservation: &mut Reservation<'_>,
    ) -> Result<Built> {
        // Each host network dialer has its own carrier for its socket options.
        let (carrier, scope): (Arc<dyn Carrier>, _) = match &config.carrier {
            CarrierConfig::Dialer(carrier) => {
                let clients =
                    self.reserve(reservation, (*carrier).into(), |native| match native {
                        Native::Dialer(clients) => Some(clients.clone()),
                        _ => None,
                    })?;
                (clients, None)
            }
            CarrierConfig::Host(options) => {
                let scope = self.root.child();
                (
                    Arc::new(System::new(scope.clone(), options.clone())?),
                    Some(scope),
                )
            }
        };

        let resolver = Arc::new(PolicyResolver {
            policy: self.policy.clone(),
            handler: config.outbound.resolve_handler.clone(),
        });
        let clients = Clients::new(config.outbound.implementation.clone(), carrier, resolver);
        let clients = match clients {
            Ok(clients) => Arc::new(clients),
            Err(error) => {
                if let Some(scope) = scope {
                    scope.close();
                }
                return Err(error);
            }
        };

        // A host network dialer also owns its carrier's sockets.
        let lifetime = Lifetime::new(scope.unwrap_or_else(|| clients.scope().clone()));
        let (idle, drain) = (clients.clone(), clients.clone());
        Ok(Built {
            native: Native::Dialer(clients),
            lifetime,
            uses: reservation.id,
            idle: Box::pin(async move { idle.connections_closed().await }),
            on_drain: Some(Box::new(move || drain.drain())),
            run: None,
        })
    }

    fn dns_service(
        &self,
        config: &DnsServiceConfig,
        reservation: &mut Reservation<'_>,
    ) -> Result<Built> {
        let clients = self.reserve(reservation, config.dialer.into(), |native| match native {
            Native::Dialer(clients) => Some(clients.clone()),
            _ => None,
        })?;

        let lifetime = Lifetime::new(self.root.child());
        let service = kotoconn_dns::Service::new(config.clone(), clients, lifetime.scope.clone());
        let service = match service {
            Ok(service) => Arc::new(DnsService {
                service,
                queries: lifetime.scope.child(),
            }),
            Err(error) => {
                lifetime.scope.close();
                return Err(error);
            }
        };

        let queries = service.queries.clone();
        Ok(Built {
            native: Native::DnsService(service),
            lifetime,
            uses: reservation.id,
            idle: Box::pin(async move { queries.wait().await }),
            on_drain: None,
            run: None,
        })
    }

    async fn dns_query(&self, id: DnsServiceId, request: DnsRequest) -> Result<DnsResponse> {
        let service = match self.registry().native(id.into()) {
            Some((Native::DnsService(service), true)) => service.clone(),
            Some((Native::DnsService(_), false)) | None => bail!("DNS service is closed"),
            Some(_) => bail!("resource is not a DNS service"),
        };

        let _query = service.queries.track()?;
        service.queries.run(service.service.query(request)).await
    }

    /// Connects through an open dialer. Closing or draining the dialer ends the probe.
    async fn probe(&self, id: DialerId, probe: Probe) -> Result<ProbeResult> {
        let (clients, lifetime) = self.open_dialer(id)?;

        let started = Instant::now();
        let exchange = async {
            let mut stream = clients.tcp_scoped(probe.target, Scope::new()).await?;
            if !probe.request.is_empty() {
                stream.write_all(&probe.request).await?;
            }

            let mut response = vec![0; 4096];
            let length = stream.read(&mut response).await?;
            ensure!(length > 0, "connection closed before a response");
            response.truncate(length);
            Ok(response)
        };

        let response = tokio::select! {
            biased;
            _ = lifetime.admission.cancelled() => bail!("dialer is closed"),
            result = tokio::time::timeout(probe.timeout, exchange) => {
                result.context("probe timed out")??
            }
        };
        Ok(ProbeResult {
            latency: started.elapsed(),
            response,
        })
    }

    /// Runs `work` in the resource scope. A failure starts daemon shutdown.
    fn run(
        &self,
        id: ResourceId,
        scope: &Scope,
        work: BoxFuture<'static, Result<()>>,
    ) -> Result<()> {
        let failure = self.failure.clone();
        let shutdown = self.shutdown.clone();
        let span = tracing::info_span!("inbound", inbound_id = id.0.get());

        span.in_scope(|| {
            scope.spawn(async move {
                // A panic ends the inbound like an error instead of silently stopping it.
                let result = match AssertUnwindSafe(work).catch_unwind().await {
                    Ok(result) => result,
                    Err(panic) => Err(anyhow::anyhow!(
                        "inbound panicked: {}",
                        panic_message(&*panic)
                    )),
                };

                if let Err(error) = &result {
                    tracing::error!(error = %format_args!("{error:#}"), "inbound failed; shutting down");
                    failure.send_if_modified(|failure| {
                        failure.get_or_insert_with(|| format!("{error:#}"));
                        true
                    });
                    // A full shutdown runs the policy hooks, which may need to
                    // undo host configuration that depended on this inbound.
                    shutdown.cancel();
                }
                result
            })
        })
    }

    async fn supervise(
        self: Arc<Self>,
        id: ResourceId,
        lifetime: Arc<Lifetime>,
        idle: BoxFuture<'static, ()>,
        on_drain: Option<Box<dyn FnOnce() + Send>>,
    ) {
        // Closing the root scope aborts every resource without a drain.
        tokio::select! {
            _ = lifetime.admission.cancelled() => {}
            _ = lifetime.scope.cancelled() => {}
        }
        if let Some(on_drain) = on_drain {
            on_drain();
        }

        let mut deadline = lifetime.deadline.subscribe();
        let expired = async {
            let deadline = *deadline
                .wait_for(Option::is_some)
                .await
                .unwrap_or_else(|_| unreachable!("the lifetime owns the deadline"));
            // SAFETY: wait_for returned a value that its predicate accepted.
            tokio::time::sleep_until(deadline.expect("deadline is set")).await;
        };
        tokio::select! {
            _ = lifetime.abort.cancelled() => {}
            _ = lifetime.scope.cancelled() => {}
            _ = expired => tracing::debug!(resource_id = id.0.get(), "drain deadline expired"),
            _ = idle => {}
        }

        lifetime.scope.close();
        lifetime.scope.wait().await;

        // Native state, including handler references, is destroyed before waiters resume.
        let live = self
            .registry()
            .entries
            .get_mut(&id)
            .and_then(|entry| entry.live.take());
        debug_assert!(live.is_some(), "only the supervisor releases its entry");
        drop(live);

        self.registry().entries.remove(&id);
        lifetime.released.cancel();
        tracing::debug!(resource_id = id.0.get(), "resource released");
    }

    /// Starts termination. Policy cannot end a resource that open resources use.
    fn terminate(&self, id: ResourceId, ending: Ending) -> Result<()> {
        let mut registry = self.registry();
        let Some(entry) = registry.entries.get(&id) else {
            return Ok(());
        };
        ensure!(
            entry.users == 0,
            "resource is used by {} open resources; close them first",
            entry.users
        );
        Self::end(&mut registry, id, ending);
        Ok(())
    }

    /// A later abort can cut a drain short; a later drain changes nothing.
    fn end(registry: &mut Registry, id: ResourceId, ending: Ending) {
        let Some(entry) = registry.entries.get(&id) else {
            return;
        };
        let lifetime = entry.lifetime.clone();
        let uses = entry.uses;

        if lifetime.is_open() {
            if let Ending::Drain(Some(deadline)) = ending {
                lifetime.deadline.send_replace(Some(deadline));
            }
            lifetime.admission.cancel();

            if let Some(uses) = uses
                && let Some(used) = registry.entries.get_mut(&uses)
            {
                debug_assert!(used.users > 0, "an open resource counts as a user");
                used.users = used.users.saturating_sub(1);
            }
        }
        if let Ending::Abort = ending {
            lifetime.abort.cancel();
        }
    }

    /// Stops creation and drains the open resources of `tier`. The daemon's
    /// shutdown deadline aborts what remains.
    pub fn drain_tier(&self, tier: Tier) {
        let mut registry = self.registry();
        registry.accepting = false;

        let ids: Vec<_> = registry
            .entries
            .iter()
            .filter(|(_, entry)| entry.tier == tier && entry.lifetime.is_open())
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            Self::end(&mut registry, id, Ending::Drain(None));
        }
    }

    /// Completes once every resource of `tier` has been released.
    pub async fn released_tier(&self, tier: Tier) {
        let released: Vec<_> = self
            .registry()
            .entries
            .values()
            .filter(|entry| entry.tier == tier)
            .map(|entry| entry.lifetime.released.clone())
            .collect();

        for released in released {
            released.cancelled().await;
        }
    }

    fn released(&self, id: ResourceId) -> BoxFuture<'static, ()> {
        let released = self
            .registry()
            .entries
            .get(&id)
            .map(|entry| entry.lifetime.released.clone());

        Box::pin(async move {
            if let Some(released) = released {
                released.cancelled().await;
            }
        })
    }
}

enum Ending {
    Abort,
    /// Without a deadline, only an abort ends the drain early.
    Drain(Option<Instant>),
}

impl Native {
    fn tier(&self) -> Tier {
        match self {
            Native::Inbound(_) => Tier::EntryPoint,
            Native::Dialer(_) | Native::DnsService(_) => Tier::Service,
        }
    }
}

impl Registry {
    /// The native state of a resource that has not been released, and whether it is open.
    fn native(&self, id: ResourceId) -> Option<(&Native, bool)> {
        let entry = self.entries.get(&id)?;
        Some((&entry.live.as_ref()?.native, entry.lifetime.is_open()))
    }
}

impl kotoconn_script::Resources for Manager {
    fn create(
        &self,
        config: ResourceConfig,
    ) -> kotoconn_script::NativeFuture<std::result::Result<ResourceId, String>> {
        // SAFETY: the policy runtime holds this manager through an Arc.
        let this = self.this.upgrade().expect("manager is shared");
        Box::pin(
            async move {
                this.create(config)
                    .await
                    .map_err(|error| format!("{error:#}"))
            }
            .in_current_span(),
        )
    }

    fn close(&self, id: ResourceId) -> std::result::Result<(), String> {
        self.terminate(id, Ending::Abort)
            .map_err(|error| format!("{error:#}"))
    }

    fn drain(&self, id: ResourceId, timeout: Duration) -> std::result::Result<(), String> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| "drain timeout is out of range".to_owned())?;
        self.terminate(id, Ending::Drain(Some(deadline)))
            .map_err(|error| format!("{error:#}"))
    }

    fn released(&self, id: ResourceId) -> kotoconn_script::NativeFuture<()> {
        Manager::released(self, id)
    }

    fn probe(
        &self,
        dialer: DialerId,
        probe: Probe,
    ) -> kotoconn_script::NativeFuture<std::result::Result<ProbeResult, String>> {
        // SAFETY: the policy runtime holds this manager through an Arc.
        let this = self.this.upgrade().expect("manager is shared");
        Box::pin(async move {
            this.probe(dialer, probe)
                .await
                .map_err(|error| format!("{error:#}"))
        })
    }

    fn dns_query(
        &self,
        service: DnsServiceId,
        request: DnsRequest,
    ) -> kotoconn_script::NativeFuture<std::result::Result<DnsResponse, String>> {
        // SAFETY: the policy runtime holds this manager through an Arc.
        let this = self.this.upgrade().expect("manager is shared");
        Box::pin(async move {
            this.dns_query(service, request)
                .await
                .map_err(|error| format!("{error:#}"))
        })
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> &str {
    if let Some(message) = panic.downcast_ref::<&str>() {
        message
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message
    } else {
        "unknown payload"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager() -> Arc<Manager> {
        let (commands, _) = tokio::sync::mpsc::channel(1);
        let policy = Policy {
            commands,
            stopping: CancellationToken::new(),
        };
        Manager::new(policy, CancellationToken::new())
    }

    /// Blocks its destructor until the test lets it finish.
    struct Dropping {
        entered: Option<tokio::sync::oneshot::Sender<()>>,
        release: std::sync::mpsc::Receiver<()>,
    }

    impl Drop for Dropping {
        fn drop(&mut self) {
            self.entered.take().unwrap().send(()).unwrap();
            self.release.recv().unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_includes_the_destruction_of_native_state() {
        let manager = manager();
        let (entered, dropping) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let registration = Arc::new(Mutex::new(Dropping {
            entered: Some(entered),
            release: released,
        }));
        let id = manager
            .create(ResourceConfig::Dialer(DialerConfig {
                carrier: CarrierConfig::Host(Default::default()),
                outbound: OutboundConfig {
                    implementation: OutboundImpl::Direct(DirectOutboundConfig {}),
                    resolve_handler: ResolveHandler::new(std::num::NonZeroU64::MIN, registration),
                },
            }))
            .await
            .unwrap();

        manager.terminate(id, Ending::Abort).unwrap();
        dropping.await.unwrap();
        let returned_before_destruction = tokio::select! {
            biased;
            _ = manager.released(id) => true,
            _ = std::future::ready(()) => false,
        };
        release.send(()).unwrap();
        assert!(
            !returned_before_destruction,
            "wait completed while the resource still owned its handler registration"
        );
    }

    #[tokio::test]
    async fn creation_after_an_abort_registers_nothing() {
        let manager = manager();
        manager.abort();

        let config = ResourceConfig::Inbound(InboundConfig {
            implementation: InboundImpl::Socks5(Socks5InboundConfig {
                listen: "127.0.0.1:0".parse().unwrap(),
            }),
            routing_handler: RoutingHandler::new(std::num::NonZeroU64::MIN, Arc::new(())),
            udp_idle_timeout: Duration::from_secs(1),
            sniff: None,
        });
        assert!(manager.create(config).await.is_err());
        assert!(manager.resources().is_empty());
    }
}
