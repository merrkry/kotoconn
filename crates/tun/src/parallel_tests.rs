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
    let endpoint = tokio::spawn(run(queues, 1280, context.clone()));
    drop(input0);
    assert!(endpoint.await.unwrap().is_err());
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
