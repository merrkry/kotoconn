use crate::{
    session::{Handle, Session},
    tls, udp, wire,
};
use anyhow::{Result, ensure};
use anytls::core::PaddingFactory;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use kotoconn_config::AnyTlsOutboundConfig;
use kotoconn_protocol::{self as p, *};
use parking_lot::Mutex;
use rustls::pki_types::ServerName;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{sync::watch, time::Instant};
use tokio_rustls::TlsConnector;

struct Slot {
    handle: Arc<Handle>,
    scope: Scope,
    idle_since: Option<Instant>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.scope.close();
    }
}

#[derive(Default)]
struct Pool {
    sessions: BTreeMap<u64, Slot>,
    sequence: u64,
}

pub struct Client {
    endpoint: Endpoint,
    carrier: Arc<dyn Carrier>,
    connector: TlsConnector,
    name: ServerName<'static>,
    password: [u8; 32],
    padding: watch::Sender<PaddingFactory>,
    pool: Arc<Mutex<Pool>>,
    scope: Scope,
    idle_timeout: Duration,
}

impl Client {
    pub fn new(
        options: &AnyTlsOutboundConfig,
        carrier: Arc<dyn Carrier>,
        resolver: Arc<dyn Resolver>,
    ) -> Result<Self> {
        let (connector, name) = tls::client(&options.tls, &options.server)?;
        let idle_timeout = options
            .idle_session_timeout
            .unwrap_or(Duration::from_secs(60));
        ensure!(
            !idle_timeout.is_zero(),
            "AnyTLS idle session timeout must be positive"
        );
        let (padding, _) = watch::channel(PaddingFactory::default());
        let pool = Arc::new(Mutex::new(Pool::default()));
        let scope = carrier.scope().child();
        let cleaning = Arc::downgrade(&pool);
        scope.spawn(async move {
            let mut interval = tokio::time::interval(idle_timeout.min(Duration::from_secs(30)));
            loop {
                interval.tick().await;
                let Some(pool) = cleaning.upgrade() else {
                    return Ok(());
                };
                pool.lock().sessions.retain(|_, slot| {
                    !slot.handle.closed.is_cancelled()
                        && slot
                            .idle_since
                            .is_none_or(|since| since.elapsed() < idle_timeout)
                });
            }
        })?;
        Ok(Self {
            endpoint: Endpoint {
                address: options.server.clone(),
                resolver,
            },
            carrier,
            connector,
            name,
            password: wire::password_hash(&options.password),
            padding,
            pool,
            scope,
            idle_timeout,
        })
    }

    async fn connect(&self, address: Bytes) -> Result<BoxStream> {
        self.carrier.capabilities().require(Capabilities::TCP)?;
        let reusable = {
            let mut pool = self.pool.lock();
            pool.sessions.retain(|_, slot| {
                !slot.handle.closed.is_cancelled()
                    && slot
                        .idle_since
                        .is_none_or(|since| since.elapsed() < self.idle_timeout)
            });
            pool.sessions
                .values_mut()
                .rev()
                .find(|slot| slot.idle_since.is_some())
                .map(|slot| {
                    slot.idle_since = None;
                    slot.handle.clone()
                })
        };
        let session = match reusable {
            Some(session) => session,
            None => self.new_session().await?,
        };
        session.open(address).await
    }

    async fn open(&self, address: Bytes, scope: Scope) -> Result<BoxStream> {
        let stream = scope
            .run(async {
                tokio::time::timeout(crate::HANDSHAKE_TIMEOUT, self.connect(address)).await?
            })
            .await?;
        stream_task(scope, async move { Ok(stream) })
    }

    async fn new_session(&self) -> Result<Arc<Handle>> {
        let session_scope = self.scope.child();
        // Cancel a partially constructed transport if DNS, TLS, auth or the
        // caller fails before the session enters the pool.
        struct Closing(Option<Scope>);
        impl Drop for Closing {
            fn drop(&mut self) {
                if let Some(scope) = &self.0 {
                    scope.close();
                }
            }
        }
        let mut closing = Closing(Some(session_scope.clone()));
        let transport = self
            .carrier
            .tcp_scoped(self.endpoint.resolve().await?, session_scope.clone())
            .await?;
        let mut tls = self.connector.connect(self.name.clone(), transport).await?;
        let padding = self.padding.borrow().clone();
        wire::send_authentication(&mut tls, &self.password, &padding).await?;

        let sequence = {
            let mut pool = self.pool.lock();
            pool.sequence = pool
                .sequence
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("AnyTLS session IDs exhausted"))?;
            pool.sequence
        };
        let idle_pool = Arc::downgrade(&self.pool);
        let idle = Box::new(move || {
            if let Some(pool) = idle_pool.upgrade()
                && let Some(slot) = pool.lock().sessions.get_mut(&sequence)
            {
                slot.idle_since = Some(Instant::now());
            }
        });
        let handle = Session::start(tls, &session_scope, self.padding.clone(), None, Some(idle))?;
        self.pool.lock().sessions.insert(
            sequence,
            Slot {
                handle: handle.clone(),
                scope: session_scope,
                idle_since: None,
            },
        );
        closing.0 = None;
        Ok(handle)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.scope.close();
    }
}

impl p::Client for Client {
    fn capabilities(&self) -> Capabilities {
        let tcp = self.carrier.capabilities().tcp;
        Capabilities { tcp, udp: tcp }
    }

    fn tcp(&self, destination: Target, scope: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        Box::pin(async move { self.open(wire::address(&destination)?.into(), scope).await })
    }

    fn udp(&self, destination: Target, scope: Scope) -> BoxFuture<'_, Result<Datagram>> {
        Box::pin(async move {
            let mut address = wire::address(&udp::sentinel())?;
            address.extend(udp::request(&destination)?);
            // Send the UoT request before waiting for SYNACK: reference servers
            // acknowledge only after reading the connected UDP destination.
            let stream = self.open(address.into(), scope.clone()).await?;
            udp::start(stream, Some(destination), scope)
        })
    }
}
