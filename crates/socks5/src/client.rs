use super::*;
use anyhow::bail;
use fast_socks5::{Socks5Command, client::Socks5Stream};
use futures_util::future::BoxFuture;
use kotoconn_protocol::{self as p, *};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

pub struct Client {
    pub endpoint: Endpoint,
    pub carrier: Arc<dyn Carrier>,
}
impl p::Client for Client {
    fn capabilities(&self) -> Capabilities {
        let lower = self.carrier.capabilities();
        Capabilities {
            tcp: lower.tcp,
            udp: lower.tcp && lower.udp,
        }
    }
    fn tcp(&self, destination: Target, scope: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        Box::pin(async move {
            self.carrier.capabilities().require(Capabilities::TCP)?;
            let stream = self
                .carrier
                .tcp_scoped(self.endpoint.resolve().await?, scope.clone())
                .await?;
            let mut protocol = Socks5Stream::use_stream(stream, None, Default::default()).await?;
            protocol
                .request(Socks5Command::TCPConnect, address(destination))
                .await?;
            Ok(protocol.get_socket())
        })
    }
    fn udp(&self, _: Target, scope: Scope) -> BoxFuture<'_, Result<Datagram>> {
        Box::pin(async move {
            self.carrier.capabilities().require(Capabilities::BOTH)?;
            let endpoint = self.endpoint.resolve().await?;
            let stream = self
                .carrier
                .tcp_scoped(endpoint.clone(), scope.clone())
                .await?;
            let mut protocol = Socks5Stream::use_stream(stream, None, Default::default()).await?;
            let relay = protocol
                .request(
                    Socks5Command::UDPAssociate,
                    address(p::target("0.0.0.0:0".parse()?)),
                )
                .await?;
            let relay = match from_address(relay) {
                Target::Ip { address, port } if address.is_unspecified() => match endpoint {
                    Target::Ip { address, .. } => Target::Ip { address, port },
                    _ => unreachable!("endpoint resolved"),
                },
                other => other,
            };
            // Both sockets use exactly the same carrier. No direct-I/O fallback.
            let mut transport = self
                .carrier
                .udp_scoped(relay.clone(), scope.clone())
                .await?;
            let mut control = protocol.get_socket();
            let (user, mut driver) = packet_pair(scope.clone());
            scope.spawn(async move {
                let mut byte = [0];
                loop {
                    tokio::select! {
                        biased;
                        _ = control.read(&mut byte) => return Ok(()),
                        received = transport.rx.recv() => {
                            let Some(received) = received else { bail!("SOCKS UDP carrier closed"); };
                            if let Ok(packet) = decode(&received.payload).await { let _ = driver.tx.try_send(packet); }
                        }
                        packet = driver.rx.recv() => {
                            let Some(packet) = packet else { return Ok(()); };
                            match encode(packet) {
                                Ok(payload) => {
                                    if matches!(transport.tx.try_send(Packet { target: relay.clone(), payload }), Err(tokio::sync::mpsc::error::TrySendError::Closed(_))) {
                                        return Ok(());
                                    }
                                },
                                Err(error) => eprintln!("SOCKS UDP drop: {error}"),
                            }
                        }
                    }
                }
            })?;
            Ok(user)
        })
    }
}
