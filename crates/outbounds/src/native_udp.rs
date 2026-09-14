use kotoconn_protocol::{pool::Pool, *};
use std::{
    future::poll_fn,
    io,
    task::{Context, Poll, ready},
};
use tokio::{io::Interest, net::UdpSocket, sync::oneshot};

struct Native {
    socket: UdpSocket,
    target: Target,
    pool: Pool,
    batch: usize,
    #[cfg(target_os = "linux")]
    sender: crate::udp_batch::Sender,
}

pub(super) fn start(socket: UdpSocket, target: Target, scope: Scope) -> anyhow::Result<Datagram> {
    #[cfg(target_os = "linux")]
    if let Err(error) = crate::udp_batch::enable_gro(&socket) {
        tracing::debug!(%error, "UDP GRO unavailable");
    }
    let native = packet_io_task(
        scope.clone(),
        Box::new(Native {
            socket,
            target,
            pool: Pool::shared(),
            batch: 1,
            #[cfg(target_os = "linux")]
            sender: crate::udp_batch::Sender::default(),
        }),
    )?;
    let (mut user, driver) = packet_pair(scope.clone());
    let requests = user.offer_handoff();
    scope.spawn(drive(native, driver, requests))?;
    Ok(user)
}

impl PacketIo for Native {
    fn poll_send(&mut self, cx: &mut Context<'_>, packets: &[Packet]) -> Poll<io::Result<usize>> {
        let valid = packets
            .iter()
            .take_while(|packet| packet.target == self.target)
            .count();
        if valid == 0 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "datagram target differs from connected target",
            )));
        }
        let packets = &packets[..valid];
        loop {
            ready!(self.socket.poll_send_ready(cx))?;
            #[cfg(target_os = "linux")]
            let result = self.socket.try_io(Interest::WRITABLE, || {
                self.sender.send(&self.socket, packets)
            });
            #[cfg(not(target_os = "linux"))]
            let result = self.socket.try_send(&packets[0].payload).map(|_| 1);
            match result {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                result => return Poll::Ready(result),
            }
        }
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        out: &mut Vec<Packet>,
    ) -> Poll<io::Result<usize>> {
        loop {
            ready!(self.socket.poll_recv_ready(cx))?;
            let mut buffers: Vec<_> = (0..self.batch).map(|_| self.pool.acquire(65536)).collect();
            let mut lengths = [0; 32];
            let mut segments = [0; 32];
            #[cfg(target_os = "linux")]
            let result = self.socket.try_io(Interest::READABLE, || {
                crate::udp_batch::receive(&self.socket, &mut buffers, &mut lengths, &mut segments)
            });
            #[cfg(not(target_os = "linux"))]
            let result = self.socket.try_recv(buffers[0].as_mut()).map(|n| {
                lengths[0] = n;
                1
            });
            let count = match result {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                result => result?,
            };
            let before = out.len();
            for (i, buffer) in buffers.into_iter().enumerate().take(count) {
                let length = lengths[i];
                if length == usize::MAX {
                    continue;
                }
                // Compact small receives; retain shared GRO aggregate storage.
                let payload = if length >= 8192 {
                    buffer.freeze(length)
                } else {
                    self.pool.copy(&buffer.as_ref()[..length])
                };
                let size = if segments[i] == 0 {
                    length.max(1)
                } else {
                    usize::from(segments[i])
                };
                let mut offset = 0;
                loop {
                    let end = (offset + size).min(length);
                    out.push(Packet {
                        target: self.target.clone(),
                        payload: payload.slice(offset..end),
                    });
                    if end == length {
                        break;
                    }
                    offset = end;
                }
            }
            let datagrams = out.len() - before;
            // recvmmsg counts messages, while a GRO message contains many
            // datagrams. Bound the next turn by datagrams rather than aggregates.
            let messages = if count == self.batch {
                (count * 2).min(32)
            } else {
                count.max(1)
            };
            self.batch = messages.min((32 * count / datagrams.max(1)).max(1));
            if datagrams != 0 {
                return Poll::Ready(Ok(datagrams));
            }
        }
    }
}

async fn drive(
    mut native: BoxPacketIo,
    mut driver: Datagram,
    mut requests: oneshot::Receiver<oneshot::Sender<Option<BoxPacketIo>>>,
) -> anyhow::Result<()> {
    let mut pending = Vec::with_capacity(32);
    let mut received = Vec::with_capacity(32);
    let mut used = false;
    let mut requested = false;
    loop {
        let handoff = poll_fn(|cx| {
            if !requested && let Poll::Ready(request) = std::pin::Pin::new(&mut requests).poll(cx) {
                requested = true;
                if let Ok(reply) = request {
                    if !used {
                        return Poll::Ready(Ok(Some(reply)));
                    }
                    let _ = reply.send(None);
                }
            }
            if pending.is_empty() {
                match driver.rx.poll_recv_many(cx, &mut pending, 32) {
                    Poll::Ready(0) => {
                        return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
                    }
                    Poll::Ready(_) => used = true,
                    Poll::Pending => {}
                }
            }
            let mut progress = false;
            if !pending.is_empty() {
                match native.poll_send(cx, &pending) {
                    Poll::Ready(Ok(count)) => {
                        debug_assert!(count > 0 && count <= pending.len());
                        pending.drain(..count);
                        progress = true;
                    }
                    Poll::Ready(Err(error)) => {
                        tracing::warn!(%error, "UDP send");
                        pending.remove(0);
                        progress = true;
                    }
                    Poll::Pending => {}
                }
            }
            match native.poll_recv(cx, &mut received) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
                }
                Poll::Ready(Ok(_)) => {
                    for packet in received.drain(..) {
                        let _ = driver.tx.try_send(packet);
                    }
                    progress = true;
                }
                Poll::Ready(Err(error)) => {
                    tracing::warn!(%error, "UDP receive");
                    progress = true;
                }
                Poll::Pending => {}
            }
            if progress {
                Poll::Ready(Ok(None))
            } else {
                Poll::Pending
            }
        })
        .await?;
        if let Some(reply) = handoff {
            // No outgoing batch was consumed. Queued inbound replies stay owned
            // by the user's receiver and are drained before future native reads.
            driver.disarm();
            let _ = reply.send(Some(native));
            return Ok(());
        }
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn transfer_keeps_queued_replies_before_native_replies_and_closes_idle_io() {
        for address in ["127.0.0.1:0", "[::1]:0"] {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                let a = UdpSocket::bind(address).await.unwrap();
                let b = UdpSocket::bind(address).await.unwrap();
                a.connect(b.local_addr().unwrap()).await.unwrap();
                b.connect(a.local_addr().unwrap()).await.unwrap();
                let scope = Scope::new();
                let mut user = start(a, target(b.local_addr().unwrap()), scope.clone()).unwrap();
                b.send(b"first").await.unwrap();
                assert_eq!(user.rx.recv().await.unwrap().payload, b"first"[..]);
                b.send(b"second").await.unwrap();
                let mut native = user.take_native().await.unwrap().unwrap();
                b.send(b"third").await.unwrap();
                let mut received = Vec::new();
                while received.len() < 2 {
                    poll_fn(|cx| native.poll_recv(cx, &mut received))
                        .await
                        .unwrap();
                }
                assert_eq!(received[0].payload, b"second"[..]);
                assert_eq!(received[1].payload, b"third"[..]);
                let empty = Packet {
                    target: target(b.local_addr().unwrap()),
                    payload: Vec::new().into(),
                };
                assert_eq!(
                    poll_fn(|cx| native.poll_send(cx, std::slice::from_ref(&empty)))
                        .await
                        .unwrap(),
                    1
                );
                assert_eq!(b.recv(&mut [0; 16]).await.unwrap(), 0);
                scope.close();
                scope.wait().await;
                assert_eq!(
                    poll_fn(|cx| native.poll_recv(cx, &mut received))
                        .await
                        .unwrap(),
                    0
                );
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn transfer_declines_after_driver_consumes_application_data() {
        let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let scope = Scope::new();
        let mut user = start(a, target(b.local_addr().unwrap()), scope.clone()).unwrap();
        user.tx
            .send(Packet {
                target: target(b.local_addr().unwrap()),
                payload: vec![1].into(),
            })
            .await
            .unwrap();
        b.recv(&mut [0; 16]).await.unwrap();
        assert!(user.take_native().await.unwrap().is_none());
        b.send(b"reply").await.unwrap();
        assert_eq!(user.rx.recv().await.unwrap().payload, b"reply"[..]);
        drop(user);
        scope.wait().await;
    }
}
