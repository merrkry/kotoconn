use super::*;
use fast_socks5::{ReplyError, Socks5Command, server::Socks5ServerProtocol};
use futures_util::future::BoxFuture;
use kotoconn_protocol::{self as p, *};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, UdpSocket},
};

pub struct Server;

impl p::Server for Server {
    fn bind(
        &self,
        address: SocketAddr,
        context: ServerContext,
    ) -> BoxFuture<'_, Result<BoundServer>> {
        Box::pin(async move {
            let listener = TcpListener::bind(address).await?;
            let local_addr = listener.local_addr()?;
            Ok(BoundServer {
                local_addr,
                run: Box::pin(async move {
                    let handler = context.handler.clone();
                    let stopping = context.stopping.clone();

                    accept_loop(listener, context, move |stream, peer, local, scope| {
                        let handler = handler.clone();
                        let stopping = stopping.clone();

                        async move {
                            let (protocol, command, destination) =
                                Socks5ServerProtocol::accept_no_auth(stream)
                                    .await?
                                    .read_command()
                                    .await?;

                            match command {
                                Socks5Command::TCPConnect => {
                                    let stream = protocol.reply_success(local).await?;
                                    handler.tcp(from_address(destination), stream, scope).await
                                }
                                Socks5Command::UDPAssociate => {
                                    let socket = UdpSocket::bind(SocketAddr::new(local.ip(), 0)).await?;
                                    let mut control = protocol.reply_success(socket.local_addr()?).await?;
                                    let (association, mut driver) = packet_pair(scope.child());
                                    let work = handler.udp(association);
                                    tokio::pin!(work);

                                    let mut buffer = vec![0; 65536];
                                    let mut byte = [0];

                                    // The advertised client endpoint can be unknown. Pin the first
                                    // valid packet's port, while always requiring the TCP peer IP.
                                    let mut client = match destination {
                                        fast_socks5::util::target_addr::TargetAddr::Ip(addr)
                                            if addr.port() != 0 =>
                                        {
                                            Some(SocketAddr::new(peer.ip(), addr.port()))
                                        }
                                        _ => None,
                                    };

                                    loop {
                                        tokio::select! {
                                            biased;
                                            _ = stopping.cancelled() => return Ok(()),
                                            _ = control.read(&mut byte) => return Ok(()),
                                            result = &mut work => return result,
                                            received = socket.recv_from(&mut buffer) => {
                                                let (n, source) = received?;

                                                if source.ip() != peer.ip()
                                                    || client.is_some_and(|expected| expected != source)
                                                {
                                                    continue;
                                                }

                                                if let Ok(packet) = decode(&buffer[..n]).await {
                                                    client = Some(source);
                                                    let _ = driver.tx.try_send(packet);
                                                }
                                            }
                                            response = driver.rx.recv() => {
                                                let Some(response) = response else { return Ok(()); };

                                                if let (Some(client), Ok(wire)) =
                                                    (client, encode(response))
                                                    && let Err(error) =
                                                        socket.send_to(&wire, client).await
                                                {
                                                    eprintln!("SOCKS UDP reply: {error}");
                                                }
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    protocol
                                        .reply_error(&ReplyError::CommandNotSupported)
                                        .await?;
                                    Ok(())
                                }
                            }
                        }
                    })
                    .await
                }),
            })
        })
    }
}
