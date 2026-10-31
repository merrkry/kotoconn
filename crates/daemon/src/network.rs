//! Native protocol execution. Each session progresses independently of supervision.
pub(crate) mod dns;
pub(crate) mod sessions;
mod udp;

use crate::{Policy, resources::Manager};
use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use kotoconn_config::*;
use kotoconn_protocol::{self as p, *};
pub use sessions::{SessionHandle, SessionId};
use std::{
    net::SocketAddr,
    sync::{Arc, Weak},
};
use tracing::Instrument;

pub(crate) struct PolicyResolver {
    pub policy: Policy,
    pub handler: ResolveHandler,
}

impl Resolver for PolicyResolver {
    fn resolve(&self, name: String) -> BoxFuture<'_, Result<Vec<IpAddr>>> {
        Box::pin(async move { Ok(self.policy.resolve(&self.handler, name).await?) })
    }
}

#[derive(Clone)]
pub(crate) struct SessionHandler {
    pub inbound: InboundId,
    pub policy: Policy,
    pub manager: Weak<Manager>,
    pub routing: RoutingHandler,
    pub sessions: sessions::Sessions,
    pub idle: std::time::Duration,
    pub sniff: Option<SniffConfig>,
}

impl SessionHandler {
    fn dialer(&self, id: DialerId) -> Result<Arc<kotoconn_outbounds::Clients>> {
        self.manager
            .upgrade()
            .context("the daemon has stopped")?
            .dialer(id)
    }
}

impl p::Handler for SessionHandler {
    fn tcp(
        &self,
        source: SocketAddr,
        destination: Target,
        mut stream: BoxStream,
        scope: Scope,
    ) -> BoxFuture<'_, Result<()>> {
        let span = tracing::info_span!(
            "session",
            protocol = "tcp",
            ?destination,
            session_id = tracing::field::Empty
        );
        Box::pin(
            async move {
                let registration = self
                    .sessions
                    .register(destination.clone(), TransportProtocol::Tcp, scope.clone())
                    .await?;
                tracing::Span::current().record("session_id", registration.id.0);
                tracing::debug!("session started");

                let result = scope
                    .run(async {
                        let sniff = if let Some(config) = &self.sniff {
                            let (inspected, result) =
                                kotoconn_inbounds::sniff::tcp(stream, config, &scope).await?;
                            stream = inspected;
                            result
                        } else {
                            None
                        };

                        let decision = self
                            .policy
                            .route(
                                &self.routing,
                                Flow {
                                    inbound: self.inbound,
                                    source,
                                    protocol: TransportProtocol::Tcp,
                                    dest: destination,
                                    sniff,
                                },
                            )
                            .await?;
                        let (dialer, target) = match decision {
                            RouteDecision::Route { dialer, target } => (dialer, target),
                            // Rejection is a policy outcome, not a session failure.
                            RouteDecision::Reject => {
                                tracing::debug!("TCP session rejected");
                                return Ok(());
                            }
                            // SAFETY: Policy::route rejects UDP decisions for TCP flows.
                            RouteDecision::Udp { .. } => {
                                unreachable!("UDP decision for a TCP flow")
                            }
                        };
                        tracing::debug!(dialer_id = dialer.0.get(), ?target, "TCP route selected");

                        let client = self.dialer(dialer)?;
                        let control = client.control(TransportProtocol::Tcp);

                        control
                            .run(async {
                                let outbound = client.tcp_scoped(target, scope.clone()).await?;
                                let (sent_bytes, received_bytes) =
                                    p::relay(&mut stream, outbound).await?;
                                tracing::debug!(sent_bytes, received_bytes, "TCP relay completed");
                                Ok(())
                            })
                            .await
                    })
                    .await;
                if let Err(error) = &result
                    && !scope.is_closed()
                {
                    tracing::warn!(error = %format_args!("{error:#}"), "TCP session failed");
                }
                tracing::debug!("session finished");
                result
            }
            .instrument(span),
        )
    }

    fn udp(&self, source: SocketAddr, packets: Datagram) -> BoxFuture<'_, Result<()>> {
        udp::association(self.clone(), source, packets)
    }
}
