//! Public client construction and independent TCP/UDP close handles.
mod native_udp;
mod system;
#[cfg(target_os = "linux")]
mod udp_batch;

use anyhow::Result;
use futures_util::future::BoxFuture;
pub use kotoconn_anytls as anytls;
use kotoconn_config::{OutboundImpl, TransportProtocol};
pub use kotoconn_http as http;
pub use kotoconn_hysteria2 as hysteria2;
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
    pub fn drain(&self) {
        self.protocol.drain();
    }

    pub fn new(
        config: OutboundImpl,
        carrier: Arc<dyn Carrier>,
        resolver: Arc<dyn Resolver>,
    ) -> Result<Self> {
        let scope = carrier.scope().child();

        let protocol: Arc<dyn Client> = match config {
            OutboundImpl::Hysteria2(options) => Arc::new(hysteria2::Client::new(
                Endpoint {
                    address: options.server.clone(),
                    resolver,
                },
                carrier,
                &options,
            )?),
            OutboundImpl::AnyTls(options) => {
                Arc::new(anytls::Client::new(&options, carrier, resolver)?)
            }
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
            let transport = scope.run(self.protocol.udp(target, scope.clone())).await?;
            let lower = transport.scope.clone();
            let control = scope.clone();
            struct Close(Scope);
            impl Drop for Close {
                fn drop(&mut self) {
                    self.0.close();
                }
            }
            let close = Close(lower.clone());
            scope.spawn(async move {
                let close = close;
                lower.cancelled().await;
                control.close();
                drop(close);
                Ok(())
            })?;
            Ok(transport)
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
