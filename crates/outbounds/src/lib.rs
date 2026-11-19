//! Outbound construction and independent TCP/UDP lifetimes.
mod native_udp;
mod system;
#[cfg(target_os = "linux")]
mod udp_batch;

use anyhow::Result;
use futures_util::future::BoxFuture;
pub use kotoconn_anytls as anytls;
use kotoconn_config::{OutboundProtocol, TransportProtocol};
pub use kotoconn_http as http;
pub use kotoconn_hysteria2 as hysteria2;
pub use kotoconn_naive as naive;
use kotoconn_protocol::*;
pub use kotoconn_shadowsocks2022 as shadowsocks2022;
pub use kotoconn_socks5 as socks5;
use std::sync::Arc;
pub use system::{System, SystemResolver};

pub struct Outbound {
    adapter: Arc<dyn OutboundAdapter>,
    scope: Scope,
    tcp: Scope,
    udp: Scope,
}

impl Outbound {
    pub fn drain(&self) {
        self.adapter.drain();
    }

    /// Closing the returned outbound's scope also ends the protocol's own tasks,
    /// such as connection pools, which the adapter starts in its carrier's scope.
    pub fn new(
        config: OutboundProtocol,
        carrier: Arc<dyn Connector>,
        resolver: Arc<dyn Resolver>,
    ) -> Result<Self> {
        let scope = carrier.scope().child();
        let carrier: Arc<dyn Connector> = Arc::new(Owned {
            carrier,
            scope: scope.clone(),
        });
        let tcp = scope.child();

        let adapter: Arc<dyn OutboundAdapter> = match config {
            OutboundProtocol::Hysteria2(options) => Arc::new(hysteria2::Client::new(
                ServerEndpoint {
                    address: options.server.clone(),
                    resolver,
                },
                carrier,
                &options,
            )?),
            OutboundProtocol::AnyTls(options) => {
                Arc::new(anytls::Client::new(&options, carrier, resolver)?)
            }
            #[cfg(target_os = "linux")]
            OutboundProtocol::Naive(options) => {
                // Naive carries only TCP, so closing its TCP scope also closes the
                // engine's pooled sessions. They are not connections, so draining
                // the outbound does not wait for them.
                let pool = scope.child();
                let client = naive::Client::new(
                    options.clone(),
                    ServerEndpoint {
                        address: options.server,
                        resolver,
                    },
                    carrier,
                    pool.clone(),
                )?;

                let closing = pool.clone();
                let closed = tcp.clone();
                pool.spawn(async move {
                    closed.cancelled().await;
                    closing.close();
                    Ok(())
                })?;
                Arc::new(client)
            }
            #[cfg(not(target_os = "linux"))]
            OutboundProtocol::Naive(_) => {
                anyhow::bail!("Naive outbound requires the Linux SagerNet Cronet runtime")
            }
            OutboundProtocol::Http(options) => Arc::new(http::Client {
                endpoint: ServerEndpoint {
                    address: options.server,
                    resolver,
                },
                carrier,
            }),
            OutboundProtocol::Socks5(options) => Arc::new(socks5::Client {
                endpoint: ServerEndpoint {
                    address: options.server,
                    resolver,
                },
                carrier,
            }),
            OutboundProtocol::Shadowsocks2022(options) => Arc::new(shadowsocks2022::Client::new(
                ServerEndpoint {
                    address: options.server,
                    resolver,
                },
                carrier,
                &options.password,
            )?),
            OutboundProtocol::Direct(_) => Arc::new(Direct(carrier)),
        };

        Ok(Self {
            tcp,
            udp: scope.child(),
            scope,
            adapter,
        })
    }

    /// Completes once no TCP or UDP connection opened through this outbound remains.
    /// Protocol tasks that outlive connections, such as idle pools, do not count.
    pub async fn connections_closed(&self) {
        tokio::join!(self.tcp.wait(), self.udp.wait());
    }

    pub fn control(&self, protocol: TransportProtocol) -> Scope {
        match protocol {
            TransportProtocol::Tcp => self.tcp.clone(),
            TransportProtocol::Udp => self.udp.clone(),
        }
    }
}

impl Connector for Outbound {
    fn capabilities(&self) -> Capabilities {
        self.adapter.capabilities()
    }

    fn scope(&self) -> &Scope {
        &self.scope
    }

    fn tcp_scoped(&self, target: Endpoint, caller: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        Box::pin(async move {
            self.capabilities()
                .require_for(TransportProtocol::Tcp, &target)?;
            let scope = self.tcp.child().tracked_by(&caller);
            let adapter = self.adapter.clone();
            let connection_scope = scope.clone();
            stream_task(
                scope,
                async move { adapter.tcp(target, connection_scope).await },
            )
        })
    }

    fn udp_scoped(&self, target: Endpoint, caller: Scope) -> BoxFuture<'_, Result<Datagram>> {
        Box::pin(async move {
            self.capabilities()
                .require_for(TransportProtocol::Udp, &target)?;
            let scope = self.udp.child().tracked_by(&caller);
            let transport = scope.run(self.adapter.udp(target, scope.clone())).await?;
            follow_datagram(scope, transport.scope.clone())?;
            Ok(transport)
        })
    }

    fn udp_native_scoped(
        &self,
        target: Endpoint,
        caller: Scope,
    ) -> BoxFuture<'_, Result<Option<NativeDatagram>>> {
        Box::pin(async move {
            self.capabilities()
                .require_for(TransportProtocol::Udp, &target)?;
            let scope = self.udp.child().tracked_by(&caller);
            let transport = scope
                .run(self.adapter.udp_native(target, scope.clone()))
                .await?;
            if let Some(transport) = &transport {
                follow_datagram(scope, transport.scope.clone())?;
            }
            Ok(transport)
        })
    }
}

fn follow_datagram(scope: Scope, lower: Scope) -> Result<()> {
    let close = lower.clone().close_on_drop();
    let control = scope.clone();
    scope.spawn(async move {
        let close = close;
        lower.cancelled().await;
        control.close();
        drop(close);
        Ok(())
    })
}

/// The lower connector with the enclosing outbound's scope.
struct Owned {
    carrier: Arc<dyn Connector>,
    scope: Scope,
}

impl Connector for Owned {
    fn capabilities(&self) -> Capabilities {
        self.carrier.capabilities()
    }

    fn scope(&self) -> &Scope {
        &self.scope
    }

    fn tcp_scoped(&self, target: Endpoint, caller: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        self.carrier.tcp_scoped(target, caller)
    }

    fn udp_scoped(&self, target: Endpoint, caller: Scope) -> BoxFuture<'_, Result<Datagram>> {
        self.carrier.udp_scoped(target, caller)
    }

    fn udp_native_scoped(
        &self,
        target: Endpoint,
        caller: Scope,
    ) -> BoxFuture<'_, Result<Option<NativeDatagram>>> {
        self.carrier.udp_native_scoped(target, caller)
    }
}

struct Direct(Arc<dyn Connector>);

impl OutboundAdapter for Direct {
    fn capabilities(&self) -> Capabilities {
        self.0.capabilities()
    }

    fn tcp(&self, target: Endpoint, scope: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        self.0.tcp_scoped(target, scope)
    }

    fn udp(&self, target: Endpoint, scope: Scope) -> BoxFuture<'_, Result<Datagram>> {
        self.0.udp_scoped(target, scope)
    }

    fn udp_native(
        &self,
        target: Endpoint,
        scope: Scope,
    ) -> BoxFuture<'_, Result<Option<NativeDatagram>>> {
        self.0.udp_native_scoped(target, scope)
    }
}
