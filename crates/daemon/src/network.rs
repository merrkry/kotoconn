//! Native protocol execution. Each session progresses independently of supervision.
mod sessions;
mod udp;
use crate::{Error, Policy, Shutdown};
use anyhow::{Context, Result, bail, ensure};
use futures_util::future::BoxFuture;
use kotoconn_config::*;
use kotoconn_outbounds::{Clients, System};
use kotoconn_protocol::{self as p, *};
pub use sessions::{SessionHandle, SessionId};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

pub(crate) struct Network {
    pub addresses: HashMap<InboundId, SocketAddr>,
    pub inbound_addresses: HashMap<InboundId, kotoconn_inbounds::InboundAddress>,
    pub clients: Arc<HashMap<DialerId, Arc<Clients>>>,
    pub sessions: sessions::Sessions,
    scope: Scope,
    force: CancellationToken,
    failure: tokio::sync::watch::Receiver<Option<String>>,
    _registry: AbortOnDropHandle<()>,
}

impl Network {
    pub async fn start(
        policy: Policy,
        stopping: CancellationToken,
        force: CancellationToken,
    ) -> Result<Self, Error> {
        Self::build(policy, stopping, force)
            .await
            .map_err(|e| Error::Config(e.to_string()))
    }
    async fn build(
        policy: Policy,
        stopping: CancellationToken,
        force: CancellationToken,
    ) -> Result<Self> {
        let scope = Scope::new();
        let clients = Arc::new(build_clients(&policy, scope.clone())?);
        let (sessions, registry) = sessions::Sessions::new();
        let mut bound = Vec::new();
        let mut addresses = HashMap::new();
        let mut inbound_addresses = HashMap::new();
        for (id, config) in &policy.config().inbounds {
            ensure!(
                !config.udp_idle_timeout.is_zero(),
                "UDP idle timeout must be positive"
            );
            ensure!(
                policy
                    .config()
                    .routing_handlers
                    .contains(&config.routing_handler),
                "unknown routing handler"
            );
            let handler = Arc::new(SessionHandler {
                policy: policy.clone(),
                clients: clients.clone(),
                routing: config.routing_handler,
                sessions: sessions.clone(),
                idle: config.udp_idle_timeout,
            });
            let server = kotoconn_inbounds::bind(
                config.implementation.clone(),
                ServerContext {
                    handler,
                    scope: scope.clone(),
                    stopping: stopping.clone(),
                    udp_idle_timeout: config.udp_idle_timeout,
                },
            )
            .await?;
            if let kotoconn_inbounds::InboundAddress::Socket(address) = &server.address {
                addresses.insert(*id, *address);
            }
            inbound_addresses.insert(*id, server.address.clone());
            bound.push(server);
        }
        let (failed, failure) = tokio::sync::watch::channel(None);
        for server in bound {
            let failed = failed.clone();
            let stop = stopping.clone();
            let control = scope.clone();
            scope.spawn(async move {
                let result = server.run.await;
                if let Err(error) = &result {
                    failed.send_replace(Some(format!("{error:#}")));
                    stop.cancel();
                    control.close();
                }
                result
            })?;
        }
        let close = scope.clone();
        let force_signal = force.clone();
        // This supervisor is not part of the sessions it waits for.
        let registry = AbortOnDropHandle::new(tokio::spawn(async move {
            tokio::select! {
                _ = force_signal.cancelled() => close.close(),
                _ = registry => {},
            }
        }));
        Ok(Self {
            addresses,
            inbound_addresses,
            clients,
            sessions,
            scope,
            force,
            failure,
            _registry: registry,
        })
    }
    pub async fn wait(&self) -> Result<Shutdown, Error> {
        self.scope.wait().await;
        match self.failure.borrow().as_ref() {
            Some(error) => Err(Error::Network(error.clone())),
            None if self.force.is_cancelled() => Ok(Shutdown::TimedOut),
            None => Ok(Shutdown::Drained),
        }
    }
}

impl Drop for Network {
    fn drop(&mut self) {
        self.scope.close();
    }
}

struct PolicyResolver {
    policy: Policy,
    id: ResolveHandlerId,
}

impl Resolver for PolicyResolver {
    fn resolve(&self, name: String) -> BoxFuture<'_, Result<Vec<IpAddr>>> {
        Box::pin(async move { Ok(self.policy.resolve(self.id, name).await?) })
    }
}

fn build_clients(policy: &Policy, scope: Scope) -> Result<HashMap<DialerId, Arc<Clients>>> {
    let definitions = &policy.config().dialers;
    for (id, config) in definitions {
        ensure!(
            policy
                .config()
                .resolve_handlers
                .contains(&config.outbound.resolve_handler),
            "unknown resolve handler"
        );
        let mut seen = HashSet::from([*id]);
        let mut next = config.dialer;
        while let Some(id) = next {
            ensure!(seen.insert(id), "carrier cycle");
            next = definitions.get(&id).context("unknown carrier")?.dialer;
        }
    }
    let system: Arc<dyn Carrier> = Arc::new(System::new(scope));
    let mut clients = HashMap::<DialerId, Arc<Clients>>::new();
    while clients.len() < definitions.len() {
        for (id, config) in definitions {
            if clients.contains_key(id) {
                continue;
            }
            let carrier: Arc<dyn Carrier> = match config.dialer {
                Some(id) => match clients.get(&id) {
                    Some(client) => client.clone(),
                    None => continue,
                },
                None => system.clone(),
            };
            let resolver = Arc::new(PolicyResolver {
                policy: policy.clone(),
                id: config.outbound.resolve_handler,
            });
            clients.insert(
                *id,
                Arc::new(Clients::new(
                    config.outbound.implementation.clone(),
                    carrier,
                    resolver,
                )?),
            );
        }
    }
    Ok(clients)
}

#[derive(Clone)]
struct SessionHandler {
    policy: Policy,
    clients: Arc<HashMap<DialerId, Arc<Clients>>>,
    routing: RoutingHandlerId,
    sessions: sessions::Sessions,
    idle: std::time::Duration,
}

impl p::Handler for SessionHandler {
    fn tcp(
        &self,
        destination: Target,
        mut stream: BoxStream,
        scope: Scope,
    ) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let _registration = self
                .sessions
                .register(destination.clone(), TransportProtocol::Tcp, scope.clone())
                .await?;
            scope
                .run(async {
                    let decision = self
                        .policy
                        .route(
                            self.routing,
                            Flow {
                                protocol: TransportProtocol::Tcp,
                                dest: destination,
                            },
                        )
                        .await?;
                    let RouteDecision::Route { dialer, target } = decision else {
                        bail!("TCP session rejected");
                    };
                    let client = self.clients.get(&dialer).context("unknown dialer")?;
                    let control = client.control(TransportProtocol::Tcp);
                    control
                        .run(async {
                            let mut outbound = client.tcp_scoped(target, scope.clone()).await?;
                            tokio::io::copy_bidirectional(&mut stream, &mut outbound).await?;
                            Ok(())
                        })
                        .await
                })
                .await
        })
    }
    fn udp(&self, packets: Datagram) -> BoxFuture<'_, Result<()>> {
        Box::pin(udp::association(self.clone(), packets))
    }
}
