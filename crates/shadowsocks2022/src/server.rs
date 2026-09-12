use super::*;
use bytes::BytesMut;
use futures_util::future::BoxFuture;
use kotoconn_protocol::{self as p, *};
use shadowsocks::relay::{
    tcprelay::proxy_stream::ProxyServerStream,
    udprelay::crypto_io::{decrypt_client_payload, encrypt_server_payload},
};
use shadowsocks_service::net::packet_window::PacketWindowFilter;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, UdpSocket},
    sync::mpsc,
    time::Instant,
};

pub struct Server {
    crypto: Arc<Crypto>,
}

impl Server {
    pub fn new(password: &str) -> Result<Self> {
        Ok(Self {
            crypto: Arc::new(Crypto::new(password, ServerType::Server)?),
        })
    }
}

impl p::Server for Server {
    fn bind(
        &self,
        address: SocketAddr,
        context: ServerContext,
    ) -> BoxFuture<'_, Result<BoundServer>> {
        Box::pin(async move {
            let listener = TcpListener::bind(address).await?;
            let local_addr = listener.local_addr()?;
            let socket = UdpSocket::bind(local_addr).await?;
            let crypto = self.crypto.clone();
            Ok(BoundServer {
                local_addr,
                run: Box::pin(async move {
                    let tcp_context = context.clone();
                    let tcp_crypto = crypto.clone();
                    let tcp = async move {
                        let handler = tcp_context.handler.clone();
                        accept_loop(listener, tcp_context, move |stream, _, _, scope| {
                            let crypto = tcp_crypto.clone();
                            let handler = handler.clone();
                            async move {
                                let mut stream = ProxyServerStream::from_stream(
                                    crypto.context.clone(),
                                    stream,
                                    METHOD,
                                    crypto.config.key(),
                                );
                                let destination = from_address(stream.handshake().await?);
                                handler.tcp(destination, Box::pin(stream), scope).await
                            }
                        })
                        .await
                    };
                    tokio::try_join!(tcp, udp(socket, context, crypto))?;
                    Ok(())
                }),
            })
        })
    }
}

struct Association {
    peer: SocketAddr,
    server: u64,
    packet: u64,
    window: PacketWindowFilter,
    active: Instant,
    tx: mpsc::Sender<Packet>,
    scope: Scope,
}

impl Drop for Association {
    fn drop(&mut self) {
        self.scope.close();
    }
}

async fn udp(socket: UdpSocket, context: ServerContext, crypto: Arc<Crypto>) -> Result<()> {
    let mut associations = HashMap::<u64, Association>::new();
    let (responses, mut replies) = mpsc::channel::<(u64, Packet)>(64);
    let mut buffer = vec![0; 65536];
    // Replay state must outlive short application sessions across the wire's
    // timestamp acceptance window. Application idle expiry remains in the daemon.
    let retention = context.udp_idle_timeout.max(Duration::from_secs(60));
    loop {
        let expiry = associations.values().map(|a| a.active + retention).min();
        tokio::select! {
            biased;
            _ = context.stopping.cancelled() => return Ok(()),
            _ = context.scope.cancelled() => return Ok(()),
            _ = async { match expiry { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => {
                associations.retain(|_, a| a.active + retention > Instant::now());
            }
            received = socket.recv_from(&mut buffer) => {
                let (n, peer) = received?;
                let Ok((n, destination, Some(ctrl))) =
                    decrypt_client_payload(
                        &crypto.context,
                        METHOD,
                        crypto.config.key(),
                        &mut buffer[..n],
                        None,
                    )
                else {
                    continue;
                };

                let id = ctrl.client_session_id;

                if !associations.contains_key(&id) {
                    if associations.len() >= 4096 {
                        continue;
                    }

                    let scope = context.scope.child();
                    let (connection, mut driver) = packet_pair(scope.clone());
                    let tx = driver.tx.clone();
                    let handler = context.handler.clone();
                    let responses = responses.clone();

                    scope.spawn(async move {
                        let work = handler.udp(connection);
                        tokio::pin!(work);

                        loop {
                            tokio::select! {
                                result = &mut work => return result,
                                response = driver.rx.recv() => {
                                    let Some(response) = response else { return Ok(()); };
                                    let _ = responses.try_send((id, response));
                                }
                            }
                        }
                    })?;

                    associations.insert(
                        id,
                        Association {
                            peer,
                            server: rand::random(),
                            packet: 0,
                            window: PacketWindowFilter::new(),
                            active: Instant::now(),
                            tx,
                            scope,
                        },
                    );
                }

                let association = associations.get_mut(&id).expect("inserted association");
                if !association
                    .window
                    .validate_packet_id(ctrl.packet_id, PACKET_LIMIT)
                {
                    continue;
                }

                // Authenticated packets permit source address migration.
                association.peer = peer;
                association.active = Instant::now();
                let _ = association.tx.try_send(Packet {
                    target: from_address(destination),
                    payload: buffer[..n].to_vec().into(),
                });
            }
            response = replies.recv() => {
                let Some((id, packet)) = response else { return Ok(()); };
                let Some(association) = associations.get_mut(&id) else { continue; };
                association.packet += 1;
                if association.packet >= PACKET_LIMIT {
                    associations.remove(&id);
                    continue;
                }

                let mut wire = BytesMut::new();
                encrypt_server_payload(
                    &crypto.context,
                    METHOD,
                    crypto.config.key(),
                    &address(packet.target),
                    &control(id, association.server, association.packet),
                    &packet.payload,
                    &mut wire,
                );

                if wire.len() <= 65507
                    && let Err(error) = socket.send_to(&wire, association.peer).await
                {
                    eprintln!("Shadowsocks UDP reply: {error}");
                }

                association.active = Instant::now();
            }
        }
    }
}
