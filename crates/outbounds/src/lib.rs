//! Public client construction and independent TCP/UDP close handles.
mod system;

use anyhow::Result;
use futures_util::future::BoxFuture;
use kotoconn_config::{OutboundImpl, TransportProtocol};
pub use kotoconn_http as http;
use kotoconn_protocol::*;
pub use kotoconn_shadowsocks2022 as shadowsocks2022;
pub use kotoconn_socks5 as socks5;
use std::sync::Arc;
pub use system::{System, SystemResolver};

pub struct Clients {
    protocol: Arc<dyn Client>,
    scope: Scope,
    tcp: Scope,
    udp: Scope,
}

impl Clients {
    pub fn new(
        config: OutboundImpl,
        carrier: Arc<dyn Carrier>,
        resolver: Arc<dyn Resolver>,
    ) -> Result<Self> {
        let scope = carrier.scope().child();

        let protocol: Arc<dyn Client> = match config {
            OutboundImpl::Http(options) => Arc::new(http::Client {
                endpoint: Endpoint {
                    address: options.server,
                    resolver,
                },
                carrier,
            }),
            OutboundImpl::Socks5(options) => Arc::new(socks5::Client {
                endpoint: Endpoint {
                    address: options.server,
                    resolver,
                },
                carrier,
            }),
            OutboundImpl::Shadowsocks2022(options) => Arc::new(shadowsocks2022::Client::new(
                Endpoint {
                    address: options.server,
                    resolver,
                },
                carrier,
                &options.password,
            )?),
            OutboundImpl::Direct(_) => Arc::new(Direct(carrier)),
        };

        Ok(Self {
            tcp: scope.child(),
            udp: scope.child(),
            scope,
            protocol,
        })
    }

    pub fn control(&self, protocol: TransportProtocol) -> Scope {
        match protocol {
            TransportProtocol::Tcp => self.tcp.clone(),
            TransportProtocol::Udp => self.udp.clone(),
        }
    }
}

impl Carrier for Clients {
    fn capabilities(&self) -> Capabilities {
        self.protocol.capabilities()
    }

    fn scope(&self) -> &Scope {
        &self.scope
    }

    fn tcp_scoped(&self, target: Target, caller: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        Box::pin(async move {
            self.capabilities().require(Capabilities::TCP)?;
            let scope = self.tcp.child().tracked_by(&caller);
            let protocol = self.protocol.clone();
            let connection_scope = scope.clone();
            stream_task(scope, async move {
                protocol.tcp(target, connection_scope).await
            })
        })
    }

    fn udp_scoped(&self, target: Target, caller: Scope) -> BoxFuture<'_, Result<Datagram>> {
        Box::pin(async move {
            self.capabilities().require(Capabilities {
                tcp: false,
                udp: true,
            })?;
            let scope = self.udp.child().tracked_by(&caller);
            let (user, mut driver) = packet_pair(scope.clone());
            let protocol = self.protocol.clone();
            let connection_scope = scope.clone();

            scope.spawn(async move {
                let mut transport = protocol.udp(target, connection_scope.child()).await?;

                // Independent forwarding futures avoid blocking replies on a full
                // request queue, while retaining bounded backpressure in each direction.
                let outgoing = async {
                    while let Some(packet) = driver.rx.recv().await {
                        transport.tx.send(packet).await?;
                    }
                    Ok::<(), anyhow::Error>(())
                };
                let incoming = async {
                    while let Some(packet) = transport.rx.recv().await {
                        driver.tx.send(packet).await?;
                    }
                    Ok::<(), anyhow::Error>(())
                };

                tokio::select! { result = outgoing => result, result = incoming => result }
            })?;
            Ok(user)
        })
    }
}

struct Direct(Arc<dyn Carrier>);

impl Client for Direct {
    fn capabilities(&self) -> Capabilities {
        self.0.capabilities()
    }

    fn tcp(&self, target: Target, scope: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        self.0.tcp_scoped(target, scope)
    }

    fn udp(&self, target: Target, scope: Scope) -> BoxFuture<'_, Result<Datagram>> {
        self.0.udp_scoped(target, scope)
    }
}
