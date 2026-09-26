use crate::{PacketReceive, PacketSend, packet, run, udp};
use anyhow::{Result, bail};
use futures_util::future::BoxFuture;
use kotoconn_protocol::{self as p, Scope, ServerContext};
use std::{
    io,
    sync::Arc,
    task::{Context, Poll, ready},
    time::Duration,
};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

struct Receiver {
    packets: mpsc::Receiver<Vec<u8>>,
    dropped: Option<oneshot::Sender<()>>,
}

impl PacketReceive for Receiver {
    fn poll_recv(&mut self, cx: &mut Context<'_>, bytes: &mut [u8]) -> Poll<io::Result<usize>> {
        let Some(packet) = ready!(self.packets.poll_recv(cx)) else {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        };
        bytes[..packet.len()].copy_from_slice(&packet);
        Poll::Ready(Ok(packet.len()))
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}

struct Sender {
    queue: usize,
    packets: mpsc::UnboundedSender<(usize, Vec<u8>)>,
}

impl PacketSend for Sender {
    fn poll_send(&mut self, _: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        self.packets.send((self.queue, bytes.to_vec())).unwrap();
        Poll::Ready(Ok(bytes.len()))
    }
}

struct Echo;

impl p::Handler for Echo {
    fn tcp(&self, _: p::Target, _: p::BoxStream, _: Scope) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { bail!("TCP is not used in this test") })
    }

    fn udp(&self, mut packets: p::Datagram) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            while let Some(packet) = packets.rx.recv().await {
                packets.tx.send(packet).await?;
            }
            Ok(())
        })
    }
}

pub(super) fn context() -> ServerContext {
    ServerContext {
        handler: Arc::new(Echo),
        scope: Scope::new(),
        stopping: CancellationToken::new(),
        udp_idle_timeout: Duration::from_secs(30),
    }
}

struct TcpAcceptor(mpsc::UnboundedSender<p::BoxStream>);

impl p::Handler for TcpAcceptor {
    fn tcp(&self, _: p::Target, stream: p::BoxStream, _: Scope) -> BoxFuture<'_, Result<()>> {
        self.0.send(stream).unwrap();
        Box::pin(async { Ok(()) })
    }

    fn udp(&self, _: p::Datagram) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { bail!("UDP is not used in this test") })
    }
}

#[tokio::test(start_paused = true)]
async fn tcp_reuses_time_wait_without_losing_old_eof_or_accepting_old_syn() {
    use crate::tests::{decoded, flow, segment};
    use smoltcp::wire::{TcpControl, TcpSeqNumber};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn control(
        received: &mut mpsc::UnboundedReceiver<(usize, Vec<u8>)>,
        control: TcpControl,
    ) -> (TcpSeqNumber, Option<TcpSeqNumber>) {
        loop {
            let (_, bytes) = received.recv().await.unwrap();
            let packet = decoded(&bytes);
            let (_, repr) = packet.tcp().unwrap();
            assert_ne!(repr.control, TcpControl::Rst);
            if repr.control == control {
                return (repr.seq_number, repr.ack_number);
            }
        }
    }

    for ipv6 in [false, true] {
        let flow = flow(ipv6);
        let mut context = context();
        let (accepted, mut streams) = mpsc::unbounded_channel();
        context.handler = Arc::new(TcpAcceptor(accepted));
        let (input, packets) = mpsc::channel(16);
        let (output, mut received) = mpsc::unbounded_channel();
        let endpoint = tokio::spawn(run(
            vec![(
                Receiver {
                    packets,
                    dropped: None,
                },
                Sender {
                    queue: 0,
                    packets: output,
                },
            )],
            1280,
            context.clone(),
        ));
        let started = tokio::time::Instant::now();
        input
            .send(segment(flow, 100, None, TcpControl::Syn, &[]).bytes)
            .await
            .unwrap();
        let (isn, ack) = control(&mut received, TcpControl::Syn).await;
        assert_eq!(ack, Some(TcpSeqNumber(101)));
        input
            .send(segment(flow, 101, Some((isn + 1).0), TcpControl::None, &[]).bytes)
            .await
            .unwrap();
        let mut old = streams.recv().await.unwrap();
        old.shutdown().await.unwrap();
        let (fin, _) = control(&mut received, TcpControl::Fin).await;

        // Queue FIN with unread data, a duplicate SYN, and a new incarnation
        // together. The worker must finish old I/O even within one ingress batch.
        input
            .send(segment(flow, 101, Some((fin + 1).0), TcpControl::Fin, b"tail").bytes)
            .await
            .unwrap();
        input
            .send(segment(flow, 100, None, TcpControl::Syn, &[]).bytes)
            .await
            .unwrap();
        input
            .send(segment(flow, 65536, None, TcpControl::Syn, &[]).bytes)
            .await
            .unwrap();
        let (next_isn, ack) = control(&mut received, TcpControl::Syn).await;
        assert_eq!(ack, Some(TcpSeqNumber(65537)));
        assert!(next_isn > fin);
        let mut tail = Vec::new();
        old.read_to_end(&mut tail).await.unwrap();
        assert_eq!(tail, b"tail");
        drop(old); // Its final readiness event must not affect the new generation.

        input
            .send(
                segment(
                    flow,
                    65537,
                    Some((next_isn + 1).0),
                    TcpControl::None,
                    b"next",
                )
                .bytes,
            )
            .await
            .unwrap();
        let mut new = streams.recv().await.unwrap();
        let mut data = [0; 4];
        new.read_exact(&mut data).await.unwrap();
        assert_eq!(&data, b"next");
        new.shutdown().await.unwrap();
        let (fin, _) = control(&mut received, TcpControl::Fin).await;
        input
            .send(segment(flow, 65541, Some((fin + 1).0), TcpControl::Fin, &[]).bytes)
            .await
            .unwrap();
        assert_eq!(new.read(&mut data).await.unwrap(), 0);
        assert_eq!(tokio::time::Instant::now(), started);

        context.stopping.cancel();
        endpoint.await.unwrap().unwrap();
        context.scope.wait().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragments_cross_receive_queues_and_replies_keep_the_flow_owner() {
    let context = context();
    let (output, mut received) = mpsc::unbounded_channel();
    let mut inputs = Vec::new();
    let mut queues = Vec::new();
    for queue in 0..2 {
        let (input, packets) = mpsc::channel(16);
        inputs.push(input);
        queues.push((
            Receiver {
                packets,
                dropped: None,
            },
            Sender {
                queue,
                packets: output.clone(),
            },
        ));
    }
    let endpoint = tokio::spawn(run(queues, 1280, context.clone()));
    let mut encoder = udp::Encoder::new(1280);

    for (source, destination) in [
        ("192.0.2.2:12345", "198.51.100.1:443"),
        ("[fd00::2]:12345", "[2001:db8::1]:443"),
    ] {
        let source = source.parse().unwrap();
        let destination = destination.parse().unwrap();
        let probe = encoder
            .encode(source, destination, b"probe")
            .unwrap()
            .remove(0);
        inputs[0].send(probe.to_vec()).await.unwrap();
        let (owner, bytes) = received.recv().await.unwrap();
        let mut decoder = packet::Decoder::default();
        assert_eq!(
            decoder
                .decode(&bytes, tokio::time::Instant::now())
                .unwrap()
                .udp()
                .unwrap()
                .1,
            b"probe"
        );

        let payload = vec![0x5a; 2500];
        let fragments = encoder.encode(source, destination, &payload).unwrap();
        assert!(fragments.len() > 1);
        let key = packet::parse(&fragments[0]).unwrap().route().unwrap();
        for (index, fragment) in fragments.into_iter().rev().enumerate() {
            assert_eq!(packet::parse(&fragment).unwrap().route().unwrap(), key);
            inputs[index % inputs.len()]
                .send(fragment.to_vec())
                .await
                .unwrap();
        }
        loop {
            let (queue, bytes) = received.recv().await.unwrap();
            assert_eq!(queue, owner);
            if let Some(packet) = decoder.decode(&bytes, tokio::time::Instant::now()) {
                assert_eq!(packet.udp().unwrap().1, payload);
                break;
            }
        }
    }

    context.stopping.cancel();
    endpoint.await.unwrap().unwrap();
    context.scope.wait().await;
}

#[tokio::test]
async fn receive_failure_releases_other_queues_without_cancelling_the_parent_scope() {
    for dedicated in [false, true] {
        let context = context();
        let (output, _received) = mpsc::unbounded_channel();
        let (input0, packets0) = mpsc::channel(1);
        let (_input1, packets1) = mpsc::channel(1);
        let (dropped, released) = oneshot::channel();
        let queues = vec![
            (
                Receiver {
                    packets: packets0,
                    dropped: None,
                },
                Sender {
                    queue: 0,
                    packets: output.clone(),
                },
            ),
            (
                Receiver {
                    packets: packets1,
                    dropped: Some(dropped),
                },
                Sender {
                    queue: 1,
                    packets: output,
                },
            ),
        ];
        let endpoint = tokio::spawn(crate::endpoint::run_workers(
            queues.into_iter().map(|queue| move || Ok(queue)).collect(),
            1280,
            context.clone(),
            dedicated,
        ));
        drop(input0);
        assert!(endpoint.await.unwrap().is_err());
        released.await.unwrap();
        assert!(!context.scope.is_closed());
        context.scope.wait().await;
    }
}

#[tokio::test]
async fn aborting_a_dedicated_endpoint_releases_its_queue_after_open() {
    let context = context();
    let (output, _received) = mpsc::unbounded_channel();
    let (_input, packets) = mpsc::channel(1);
    let (opened, ready) = oneshot::channel();
    let (dropped, released) = oneshot::channel();
    let owner = std::thread::current().id();
    let endpoint = tokio::spawn(crate::endpoint::run_workers(
        vec![move || {
            // Device readiness must be registered in the worker's runtime.
            opened.send(std::thread::current().id()).unwrap();
            Ok((
                Receiver {
                    packets,
                    dropped: Some(dropped),
                },
                Sender {
                    queue: 0,
                    packets: output,
                },
            ))
        }],
        1280,
        context.clone(),
        true,
    ));
    assert_ne!(ready.await.unwrap(), owner);

    endpoint.abort();
    assert!(endpoint.await.unwrap_err().is_cancelled());
    released.await.unwrap();
    assert!(!context.scope.is_closed());
    context.scope.wait().await;
}

struct SelectiveReader(Arc<Notify>);

impl p::Handler for SelectiveReader {
    fn tcp(&self, _: p::Target, _: p::BoxStream, _: Scope) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { bail!("TCP is not used in this test") })
    }

    fn udp(&self, mut packets: p::Datagram) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            while let Some(packet) = packets.rx.recv().await {
                if p::socket_addr(&packet.target)?.port() == 443 {
                    self.0.notify_one();
                    std::future::pending::<()>().await;
                }
                packets.tx.send(packet).await?;
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn a_stalled_udp_reader_does_not_block_other_flows_or_shutdown() {
    let mut context = context();
    let stalled = Arc::new(Notify::new());
    context.handler = Arc::new(SelectiveReader(stalled.clone()));
    let (input, packets) = mpsc::channel(16);
    let (output, mut received) = mpsc::unbounded_channel();
    let endpoint = tokio::spawn(run(
        vec![(
            Receiver {
                packets,
                dropped: None,
            },
            Sender {
                queue: 0,
                packets: output,
            },
        )],
        65535,
        context.clone(),
    ));
    let mut encoder = udp::Encoder::new(65535);
    let source = "192.0.2.2:12345".parse().unwrap();
    let packet = encoder
        .encode(source, "198.51.100.1:443".parse().unwrap(), &[7; 16384])
        .unwrap()
        .remove(0);
    input.send(packet.to_vec()).await.unwrap();
    stalled.notified().await;
    // The stopped reader cannot increase its allowance. This exceeds the
    // initial queue storage before the independent flow reaches dispatch.
    for _ in 0..64 {
        input.send(packet.to_vec()).await.unwrap();
    }
    let probe = encoder
        .encode(
            source,
            "198.51.100.1:8443".parse().unwrap(),
            b"still progressing",
        )
        .unwrap()
        .remove(0);
    input.send(probe.to_vec()).await.unwrap();
    let (_, reply) = received.recv().await.unwrap();
    assert_eq!(
        packet::Decoder::default()
            .decode(&reply, tokio::time::Instant::now())
            .unwrap()
            .udp()
            .unwrap()
            .1,
        b"still progressing"
    );

    context.stopping.cancel();
    endpoint.await.unwrap().unwrap();
    context.scope.wait().await;
}

struct BlockedSender(Arc<Notify>);

impl PacketSend for BlockedSender {
    fn poll_send(&mut self, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
        self.0.notify_one();
        Poll::Pending
    }
}

#[tokio::test]
async fn forced_shutdown_interrupts_a_writer_after_receive_workers_have_drained() {
    let context = context();
    let (input, packets) = mpsc::channel(1);
    let (dropped, released) = oneshot::channel();
    let writing = Arc::new(Notify::new());
    let endpoint = tokio::spawn(run(
        vec![(
            Receiver {
                packets,
                dropped: Some(dropped),
            },
            BlockedSender(writing.clone()),
        )],
        1280,
        context.clone(),
    ));
    let packet = udp::Encoder::new(1280)
        .encode(
            "192.0.2.2:12345".parse().unwrap(),
            "198.51.100.1:443".parse().unwrap(),
            b"blocked",
        )
        .unwrap()
        .remove(0);
    input.send(packet.to_vec()).await.unwrap();
    writing.notified().await;
    context.stopping.cancel();
    released.await.unwrap();
    context.scope.close();
    endpoint.await.unwrap().unwrap();
    context.scope.wait().await;
}

struct ObservedSessions(mpsc::UnboundedSender<Scope>);

impl p::Handler for ObservedSessions {
    fn tcp(&self, _: p::Target, _: p::BoxStream, _: Scope) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { bail!("TCP is not used in this test") })
    }

    fn udp(&self, mut packets: p::Datagram) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.0.send(packets.scope.clone()).unwrap();
            while let Some(packet) = packets.rx.recv().await {
                packets.tx.send(packet).await?;
            }
            Ok(())
        })
    }
}

#[tokio::test(start_paused = true)]
async fn sparse_udp_activity_preserves_only_its_session_and_expired_flows_can_be_reused() {
    let mut context = context();
    context.udp_idle_timeout = Duration::from_secs(10);
    let (admitted, mut sessions) = mpsc::unbounded_channel();
    context.handler = Arc::new(ObservedSessions(admitted));
    let (input, packets) = mpsc::channel(16);
    let (output, mut received) = mpsc::unbounded_channel();
    let endpoint = tokio::spawn(run(
        vec![(
            Receiver {
                packets,
                dropped: None,
            },
            Sender {
                queue: 0,
                packets: output,
            },
        )],
        1500,
        context.clone(),
    ));
    let mut encoder = udp::Encoder::new(1500);
    let source = "192.0.2.2:12345".parse().unwrap();
    for cycle in 0..16u8 {
        let a = encoder
            .encode(source, "198.18.0.1:1000".parse().unwrap(), &[cycle, 0])
            .unwrap()
            .remove(0);
        let b = encoder
            .encode(source, "198.18.0.1:1001".parse().unwrap(), &[cycle, 1])
            .unwrap()
            .remove(0);
        input.send(a.to_vec()).await.unwrap();
        let active = sessions.recv().await.unwrap();
        received.recv().await.unwrap();
        input.send(b.to_vec()).await.unwrap();
        let idle = sessions.recv().await.unwrap();
        received.recv().await.unwrap();

        tokio::time::advance(Duration::from_secs(9)).await;
        input.send(a.to_vec()).await.unwrap();
        received.recv().await.unwrap();
        assert!(sessions.try_recv().is_err(), "active flow was readmitted");
        tokio::time::advance(Duration::from_secs(1)).await;
        idle.cancelled().await;
        idle.wait().await;
        assert!(
            !active.is_closed(),
            "another flow's idle expiry closed the active session"
        );

        input.send(b.to_vec()).await.unwrap();
        let replacement = sessions.recv().await.unwrap();
        let (_, reply) = received.recv().await.unwrap();
        assert_eq!(
            packet::Decoder::default()
                .decode(&reply, tokio::time::Instant::now())
                .unwrap()
                .udp()
                .unwrap()
                .1,
            [cycle, 1]
        );
        assert!(!replacement.is_closed());
        active.close();
        replacement.close();
        active.wait().await;
        replacement.wait().await;
    }
    context.stopping.cancel();
    endpoint.await.unwrap().unwrap();
    context.scope.wait().await;
}
