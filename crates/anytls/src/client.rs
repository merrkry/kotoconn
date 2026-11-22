use crate::{
    padding::PaddingScheme,
    session::{Handle, Session},
    tls, udp, wire,
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use kotoconn_config::AnyTlsOutboundConfig;
use kotoconn_protocol::{self as p, *};
use parking_lot::Mutex;
use rustls::pki_types::ServerName;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{sync::watch, time::Instant};
use tokio_rustls::TlsConnector;
use tokio_util::sync::CancellationToken;

/// Dropping the last handle lets an idle session end gracefully, after it
/// sent everything queued, such as the last stream's FIN.
struct Slot {
    handle: Arc<Handle>,
    idle_since: Option<Instant>,
}

#[derive(Default)]
struct Pool {
    sessions: BTreeMap<u64, Slot>,
    sequence: u64,
    draining: bool,
}

impl Pool {
    fn prune(&mut self, timeout: Duration) {
        self.sessions.retain(|_, slot| {
            !slot.handle.closed.is_cancelled()
                && slot
                    .idle_since
                    .is_none_or(|since| since.elapsed() < timeout)
        });
    }

    /// The protocol reuses the newest idle session, so the oldest ones expire.
    fn reuse(&mut self, timeout: Duration) -> Option<Arc<Handle>> {
        self.prune(timeout);
        let slot = self
            .sessions
            .values_mut()
            .rev()
            .find(|slot| slot.idle_since.is_some())?;
        slot.idle_since = None;
        Some(slot.handle.clone())
    }

    fn mark_idle(&mut self, id: u64) {
        if self.draining {
            self.sessions.remove(&id);
        } else if let Some(slot) = self.sessions.get_mut(&id) {
            debug_assert!(slot.idle_since.is_none());
            slot.idle_since = Some(Instant::now());
        }
    }
}

pub struct Client {
    endpoint: ServerEndpoint,
    carrier: Arc<dyn Connector>,
    connector: TlsConnector,
    name: ServerName<'static>,
    password: [u8; 32],
    padding: watch::Sender<PaddingScheme>,
    pool: Arc<Mutex<Pool>>,
    scope: Scope,
    idle_timeout: Duration,
    cleanup_stop: CancellationToken,
}

impl Client {
    pub fn new(
        options: &AnyTlsOutboundConfig,
        carrier: Arc<dyn Connector>,
        resolver: Arc<dyn Resolver>,
    ) -> Result<Self> {
        tokio::runtime::Handle::try_current().context("AnyTLS client requires a Tokio runtime")?;
        let (connector, name) = tls::client(&options.tls, &options.server)?;
        let idle_timeout = options
            .idle_session_timeout
            .unwrap_or(Duration::from_secs(60));
        ensure!(
            !idle_timeout.is_zero(),
            "AnyTLS idle session timeout must be positive"
        );
        let (padding, _) = watch::channel(PaddingScheme::default());
        let pool = Arc::new(Mutex::new(Pool::default()));
        let scope = carrier.scope().child();
        let cleaning = Arc::downgrade(&pool);
        let cleanup_stop = CancellationToken::new();
        let stopped = cleanup_stop.clone();
        scope.spawn(async move {
            let mut interval = tokio::time::interval(idle_timeout.min(Duration::from_secs(30)));
            loop {
                tokio::select! {
                    _ = stopped.cancelled() => return Ok(()),
                    _ = interval.tick() => {}
                }
                let Some(pool) = cleaning.upgrade() else {
                    return Ok(());
                };
                pool.lock().prune(idle_timeout);
            }
        })?;
        Ok(Self {
            endpoint: ServerEndpoint {
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
            cleanup_stop,
        })
    }

    async fn connect(&self, address: Bytes, work: WorkGuard) -> Result<BoxStream> {
        self.carrier.capabilities().require(Capabilities::TCP)?;
        let reusable = self.pool.lock().reuse(self.idle_timeout);
        let session = match reusable {
            Some(session) => session,
            None => self.new_session().await?,
        };
        session.open(address, work).await
    }

    async fn open(&self, address: Bytes, scope: Scope) -> Result<BoxStream> {
        let work = scope.track()?;
        let stream = scope
            .run(async {
                tokio::time::timeout(crate::HANDSHAKE_TIMEOUT, self.connect(address, work)).await?
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
            .tcp_scoped(
                self.endpoint.target(self.carrier.capabilities()).await?,
                session_scope.clone(),
            )
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
            if let Some(pool) = idle_pool.upgrade() {
                pool.lock().mark_idle(sequence);
            }
        });
        let handle = Session::client(tls, &session_scope, self.padding.clone(), idle)?;
        self.pool.lock().sessions.insert(
            sequence,
            Slot {
                handle: handle.clone(),
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

impl p::OutboundAdapter for Client {
    fn drain(&self) {
        let mut pool = self.pool.lock();
        pool.draining = true;
        pool.sessions.retain(|_, slot| slot.idle_since.is_none());
        self.cleanup_stop.cancel();
    }

    fn capabilities(&self) -> Capabilities {
        let tcp = self.carrier.capabilities().tcp;
        Capabilities {
            tcp,
            udp: tcp,
            domain_targets: true,
        }
    }

    fn tcp(&self, destination: Endpoint, scope: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        Box::pin(async move { self.open(wire::address(&destination)?.into(), scope).await })
    }

    fn udp(&self, destination: Endpoint, scope: Scope) -> BoxFuture<'_, Result<Datagram>> {
        Box::pin(async move {
            let mut address = wire::address(&udp::sentinel())?;
            address.extend(udp::request(&destination)?);
            let stream = self.open(address.into(), scope.clone()).await?;
            udp::start(stream, Some(destination), scope)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn reuse_takes_the_newest_idle_session_and_expires_the_rest() {
        let scope = Scope::new();
        let mut pool = Pool::default();
        let mut peers = Vec::new();
        for id in 1..=2 {
            let (io, peer) = tokio::io::duplex(1024);
            peers.push(peer);
            let (padding, _) = watch::channel(PaddingScheme::default());
            let handle = Session::client(io, &scope.child(), padding, Box::new(|| {})).unwrap();
            let slot = Slot {
                handle,
                idle_since: None,
            };
            pool.sessions.insert(id, slot);
        }

        let older = pool.sessions[&1].handle.clone();
        let newer = pool.sessions[&2].handle.clone();
        pool.mark_idle(2);
        pool.mark_idle(1);
        let timeout = Duration::from_secs(60);
        assert!(Arc::ptr_eq(&pool.reuse(timeout).unwrap(), &newer));
        assert!(Arc::ptr_eq(&pool.reuse(timeout).unwrap(), &older));
        assert!(pool.reuse(timeout).is_none());

        pool.mark_idle(1);
        pool.mark_idle(2);
        newer.closed.cancel();
        assert!(Arc::ptr_eq(&pool.reuse(timeout).unwrap(), &older));
        assert!(!pool.sessions.contains_key(&2));

        pool.mark_idle(1);
        tokio::time::advance(timeout).await;
        assert!(pool.reuse(timeout).is_none());
        assert!(pool.sessions.is_empty());
        scope.close();
        scope.wait().await;
    }
}
