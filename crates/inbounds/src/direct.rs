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
                        accept_loop(listener, tcp_context, move |stream, _, _, scope| {
                            let handler = handler.clone();
                            let target = tcp_target.clone();
                            async move { handler.tcp(target, stream, scope).await }
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
struct Association {
    tx: mpsc::Sender<Packet>,
    scope: Scope,
    active: tokio::time::Instant,
}
impl Drop for Association {
    fn drop(&mut self) {
        self.scope.close();
    }
}
async fn udp(socket: UdpSocket, target: Target, context: ServerContext) -> Result<()> {
    let mut peers = HashMap::<SocketAddr, Association>::new();
    let (responses, mut replies) = mpsc::channel::<(SocketAddr, Packet)>(64);
    let mut buffer = vec![0; 65536];
    loop {
        let expiry = peers
            .values()
            .map(|a| a.active + context.udp_idle_timeout)
            .min();
        tokio::select! {
            biased;
            _ = context.stopping.cancelled() => return Ok(()),
            _ = context.scope.cancelled() => return Ok(()),
            _ = async { match expiry { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => {
                peers.retain(|_, a| a.active + context.udp_idle_timeout > tokio::time::Instant::now());
            }
            received = socket.recv_from(&mut buffer) => {
                let (n, peer) = received?;
                if !peers.contains_key(&peer) {
                    if peers.len() >= 4096 { continue; }
                    let scope = context.scope.child();
                    let (association, mut driver) = packet_pair(scope.clone());
                    let tx = driver.tx.clone();
                    let handler = context.handler.clone();
                    let responses = responses.clone();
                    scope.spawn(async move {
                        let work = handler.udp(association);
                        tokio::pin!(work);
                        loop {
                            tokio::select! {
                                result = &mut work => return result,
                                reply = driver.rx.recv() => {
                                    let Some(reply) = reply else { return Ok(()); };
                                    let _ = responses.try_send((peer, reply));
                                }
                            }
                        }
                    })?;
                    peers.insert(peer, Association { tx, scope, active: tokio::time::Instant::now() });
                }
                let association = peers.get_mut(&peer).expect("inserted association");
                association.active = tokio::time::Instant::now();
                let _ = association.tx.try_send(Packet { target: target.clone(), payload: buffer[..n].to_vec() });
            }
            reply = replies.recv() => {
                let Some((peer, packet)) = reply else { return Ok(()); };
                if let Some(association) = peers.get_mut(&peer) {
                    association.active = tokio::time::Instant::now();
                    if let Err(error) = socket.send_to(&packet.payload, peer).await { eprintln!("direct UDP reply: {error}"); }
                }
            }
        }
    }
}
