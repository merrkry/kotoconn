use super::*;
use anyhow::ensure;
use bytes::BytesMut;
use futures_util::future::BoxFuture;
use kotoconn_protocol::{self as p, *};
use shadowsocks::relay::udprelay::crypto_io::{decrypt_server_payload, encrypt_client_payload};
use shadowsocks_service::net::packet_window::PacketWindowFilter;
use std::{collections::HashMap, sync::Arc};

pub struct Client {
    endpoint: Endpoint,
    carrier: Arc<dyn Carrier>,
    crypto: Arc<Crypto>,
}

impl Client {
    pub fn new(endpoint: Endpoint, carrier: Arc<dyn Carrier>, password: &str) -> Result<Self> {
        Ok(Self {
            endpoint,
            carrier,
            crypto: Arc::new(Crypto::new(password, ServerType::Local)?),
        })
    }
}

impl p::Client for Client {
    fn capabilities(&self) -> Capabilities {
        self.carrier.capabilities()
    }
    fn tcp(&self, target: Target, scope: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        Box::pin(async move {
            let stream = self
                .carrier
                .tcp_scoped(self.endpoint.resolve().await?, scope.clone())
                .await?;
            super::tcp::connect(stream, target, &self.crypto).await
        })
    }
    fn udp(&self, _: Target, scope: Scope) -> BoxFuture<'_, Result<Datagram>> {
        Box::pin(async move {
            let endpoint = self.endpoint.resolve().await?;
            let mut transport = self
                .carrier
                .udp_scoped(endpoint.clone(), scope.clone())
                .await?;
            let crypto = self.crypto.clone();
            let (user, mut driver) = packet_pair(scope.clone());
            scope.spawn(async move {
                let client_id = rand::random();
                let mut packet_id = 0;
                let mut windows = HashMap::<u64, PacketWindowFilter>::new();

                loop {
                    tokio::select! {
                        received = transport.rx.recv() => {
                            let Some(received) = received else { return Ok(()); };
                            let mut payload = Vec::from(received.payload);
                            let Ok((n, destination, Some(ctrl))) =
                                decrypt_server_payload(
                                    &crypto.context,
                                    METHOD,
                                    crypto.config.key(),
                                    &mut payload,
                                )
                            else {
                                continue;
                            };

                            if ctrl.client_session_id != client_id { continue; }

                            // Bound authenticated server rotations per association.
                            if windows.len() >= 1024 && !windows.contains_key(&ctrl.server_session_id) {
                                continue;
                            }

                            if !windows
                                .entry(ctrl.server_session_id)
                                .or_default()
                                .validate_packet_id(ctrl.packet_id, PACKET_LIMIT)
                            {
                                continue;
                            }

                            payload.truncate(n);

                            let _ = driver
                                .tx
                                .try_send(Packet { target: from_address(destination), payload: payload.into() });
                        }
                        packet = driver.rx.recv() => {
                            let Some(packet) = packet else { return Ok(()); };
                            packet_id += 1;
                            ensure!(packet_id < PACKET_LIMIT, "Shadowsocks packet counter exhausted");

                            let mut wire = BytesMut::new();

                            encrypt_client_payload(
                                &crypto.context,
                                METHOD,
                                crypto.config.key(),
                                &address(packet.target),
                                &control(client_id, 0, packet_id),
                                &[],
                                &packet.payload,
                                &mut wire,
                            );

                            if wire.len() > 65507 {
                                continue;
                            }

                            if matches!(
                                transport.tx.try_send(Packet {
                                    target: endpoint.clone(),
                                    payload: wire.freeze(),
                                }),
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_))
                            ) {
                                return Ok(());
                            }
                        }
                    }
                }
            })?;
            Ok(user)
        })
    }
}
