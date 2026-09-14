use kotoconn_protocol::{
    pool::{Lease, Pool},
    *,
};
use std::{
    cell::RefCell,
    future::poll_fn,
    io,
    task::{Context, Poll, ready},
};
#[cfg(target_os = "linux")]
use tokio::io::Interest;
use tokio::{net::UdpSocket, sync::oneshot};

thread_local! {
    // Synchronous receive scratch is shared by sockets on this executor thread.
    // At most 32 maximum datagrams stay private, independent of idle flow count.
    static RECEIVE: RefCell<Vec<Lease>> = const { RefCell::new(Vec::new()) };
}

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
            let result = RECEIVE.with_borrow_mut(|buffers| self.receive(buffers, out));
            let (count, datagrams) = match result {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                result => result?,
            };
            // recvmmsg counts messages, while a GRO message contains many
            // datagrams. Bound the next turn by datagrams rather than aggregates.
            let messages = if count == self.batch {
                (count * 2).min(32)
            } else {
                count.max(1)
            };
            self.batch = if cfg!(target_os = "linux") {
                messages.min((32 * count / datagrams.max(1)).max(1))
            } else {
                1
            };
            if datagrams != 0 {
                return Poll::Ready(Ok(datagrams));
            }
        }
    }
}

impl Native {
    fn receive(
        &self,
        buffers: &mut Vec<Lease>,
        out: &mut Vec<Packet>,
    ) -> io::Result<(usize, usize)> {
        debug_assert!((1..=32).contains(&self.batch));
        while buffers.len() < self.batch {
            buffers.push(self.pool.acquire(65536));
        }
        let buffers = &mut buffers[..self.batch];
        let mut lengths = [0; 32];
        #[cfg(target_os = "linux")]
        let mut segments = [0; 32];
        #[cfg(not(target_os = "linux"))]
        let segments = [0; 32];
        #[cfg(target_os = "linux")]
        let count = self.socket.try_io(Interest::READABLE, || {
            crate::udp_batch::receive(&self.socket, buffers, &mut lengths, &mut segments)
        })?;
        #[cfg(not(target_os = "linux"))]
        let count = self.socket.try_recv(buffers[0].as_mut()).map(|n| {
            lengths[0] = n;
            1
        })?;

        let before = out.len();
        for (i, buffer) in buffers.iter_mut().enumerate().take(count) {
            let length = lengths[i];
            if length == usize::MAX {
                continue;
            }
            // Small datagrams copy once without returning receive scratch to the
            // pool. Large/GRO messages transfer ownership and replace that slot.
            let payload = if length >= 8192 {
                std::mem::replace(buffer, self.pool.acquire(65536)).freeze(length)
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
        Ok((count, out.len() - before))
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
            drop(driver);
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
    async fn shared_receive_scratch_preserves_retained_packets_across_sockets_and_sizes() {
        let mut sockets = Vec::new();
        for _ in 0..2 {
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            socket.connect(peer.local_addr().unwrap()).await.unwrap();
            peer.connect(socket.local_addr().unwrap()).await.unwrap();
            sockets.push((
                Native {
                    socket,
                    target: target(peer.local_addr().unwrap()),
                    pool: Pool::shared(),
                    batch: 32,
                    #[cfg(target_os = "linux")]
                    sender: crate::udp_batch::Sender::default(),
                },
                peer,
            ));
        }
        let mut retained = Vec::new();
        let mut expected = Vec::new();
        for size in [0, 1, 1200, 8191, 8192, 60000, 1200] {
            for (index, (native, peer)) in sockets.iter_mut().enumerate() {
                let bytes = vec![(size % 251 + index) as u8; size];
                peer.send(&bytes).await.unwrap();
                assert_eq!(
                    poll_fn(|cx| native.poll_recv(cx, &mut retained))
                        .await
                        .unwrap(),
                    1
                );
                expected.push((native.target.clone(), bytes));
            }
        }
        drop(sockets);
        assert_eq!(retained.len(), expected.len());
        for (packet, (target, bytes)) in retained.iter().zip(expected) {
            assert_eq!(packet.target, target);
            assert_eq!(packet.payload, bytes);
        }
    }

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
