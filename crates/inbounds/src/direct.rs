use anyhow::Result;
use futures_util::future::BoxFuture;
use kotoconn_protocol::{self as p, *};
use std::{collections::HashMap, net::SocketAddr};
use tokio::{
    net::{TcpListener, UdpSocket},
    sync::mpsc,
};

pub struct Server(pub Target);

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
            let target = self.0.clone();

            Ok(BoundServer {
                local_addr,
                run: Box::pin(async move {
                    let tcp_context = context.clone();
                    let tcp_target = target.clone();

                    let tcp = async move {
                        let handler = tcp_context.handler.clone();
                        accept_loop(listener, tcp_context, move |stream, peer, _, scope| {
                            let handler = handler.clone();
                            let target = tcp_target.clone();
                            async move { handler.tcp(peer, target, stream, scope).await }
                        })
                        .await
                    };
                    tokio::try_join!(tcp, udp(socket, target, context))?;
                    Ok(())
                }),
            })
        })
    }
}

struct SessionEntry {
    tx: kotoconn_protocol::queue::Sender<Packet>,
    scope: Scope,
    activity: Activity,
    generation: u64,
}

impl Drop for SessionEntry {
    fn drop(&mut self) {
        self.scope.close();
    }
}

struct Completion {
    peer: SocketAddr,
    generation: u64,
    sender: mpsc::UnboundedSender<(SocketAddr, u64)>,
}

impl Drop for Completion {
    fn drop(&mut self) {
        let _ = self.sender.send((self.peer, self.generation));
    }
}

async fn udp(socket: UdpSocket, target: Target, context: ServerContext) -> Result<()> {
    let mut peers = HashMap::<SocketAddr, SessionEntry>::new();
    let (responses, mut replies) = mpsc::channel::<(SocketAddr, u64, Packet)>(64);
    let (completed, mut completions) = mpsc::unbounded_channel();
    let mut generation = 0u64;
    let mut buffer = vec![0; 65536];

    loop {
        tokio::select! {
            biased;
            _ = context.stopping.cancelled() => return Ok(()),
            _ = context.scope.cancelled() => return Ok(()),
            Some((peer, generation)) = completions.recv() => {
                if peers.get(&peer).is_some_and(|entry| entry.generation == generation) {
                    peers.remove(&peer);
                }
            }
            received = socket.recv_from(&mut buffer) => {
                let (n, peer) = received?;
                if peers.get(&peer).is_some_and(|entry| entry.scope.is_closed()) {
                    peers.remove(&peer);
                }

                if !peers.contains_key(&peer) {
                    if peers.len() >= 4096 {
                        continue;
                    }

                    generation = generation.checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("direct UDP generation exhausted"))?;
                    let scope = context.scope.child();
                    let lifetime = UdpSession::new(scope.clone(), context.udp_idle_timeout);
                    let activity = lifetime.activity();
                    let (mut association, mut driver) = packet_pair(scope.clone());
                    association.single_target = Some(target.clone());
                    association.session_activity = Some(activity.clone());
                    let tx = driver.tx.clone();
                    let handler = context.handler.clone();
                    let responses = responses.clone();
                    let completion = Completion { peer, generation, sender: completed.clone() };

                    scope.spawn(async move {
                        let _completion = completion;
                        lifetime.run(async {
                            let work = handler.udp(peer, association);
                            tokio::pin!(work);

                            loop {
                                tokio::select! {
                                    result = &mut work => return result,
                                    reply = driver.rx.recv() => {
                                        let Some(reply) = reply else { return Ok(()); };
                                        let _ = responses.try_send((peer, generation, reply));
                                    }
                                }
                            }
                        }).await
                    })?;

                    peers.insert(peer, SessionEntry { tx, scope, activity, generation });
                }

                // SAFETY: this loop is the sole map owner and found or inserted this peer.
                debug_assert!(peers.contains_key(&peer));
                let association = peers.get(&peer).expect("admitted direct UDP session");
                let _ = association.tx.try_send(Packet {
                    target: target.clone(),
                    payload: buffer[..n].to_vec().into(),
                });
            }
            reply = replies.recv() => {
                let Some((peer, generation, packet)) = reply else { return Ok(()); };
                let Some(association) = peers.get(&peer)
                    .filter(|entry| entry.generation == generation && !entry.scope.is_closed())
                else { continue; };

                match socket.send_to(&packet.payload, peer).await {
                    Ok(_) => association.activity.record(),
                    Err(error) => {
                        tracing::warn!(error = %format_args!("{error:#}"), "direct UDP reply");
                    }
                }
            }
        }
    }
}
