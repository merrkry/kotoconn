//! Always-ready I/O must leave opportunities for reverse traffic and cancellation.
use kotoconn_protocol::{Scope, queue, stream_buffer};
use std::cell::Cell;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Notify,
};

const BACKLOG: usize = 8192;

#[tokio::test]
async fn busy_datagram_forwarding_allows_reverse_traffic_and_cancellation() {
    let (source, mut incoming) = queue::channel(1024 * 1024, |_: &usize| 0);
    let (outgoing, _sink) = queue::channel(1024 * 1024, |_: &usize| 0);
    let (replies, mut reverse) = queue::channel(1024, |_: &usize| 0);
    for index in 0..BACKLOG {
        source.try_send(index).unwrap();
    }
    replies.try_send(42).unwrap();
    let scope = Scope::new();
    let started = Notify::new();
    let forwarded = Cell::new(0);

    let forward = scope.run(async {
        while let Some(packet) = incoming.recv().await {
            outgoing.send(packet).await?;
            forwarded.set(forwarded.get() + 1);
            started.notify_one();
        }
        Ok(())
    });
    let backward = async {
        started.notified().await;
        assert_eq!(reverse.recv().await, Some(42));
        assert!(forwarded.get() > 0 && forwarded.get() < BACKLOG);
        scope.close();
    };
    let (result, ()) = tokio::join!(forward, backward);
    assert!(result.is_err());
}

#[tokio::test]
async fn ready_sends_yield_without_waiting_for_a_full_queue() {
    let (tx, _rx) = queue::channel(1024 * 1024, |_: &usize| 0);
    let scope = Scope::new();
    let sent = Cell::new(0);
    let sender = scope.run(async {
        for index in 0..BACKLOG {
            tx.send(index).await?;
            sent.set(sent.get() + 1);
        }
        Ok(())
    });
    let cancel = async {
        assert!(sent.get() > 0 && sent.get() < BACKLOG);
        scope.close();
    };
    let (result, ()) = tokio::join!(biased; sender, cancel);
    assert!(result.is_err());
}

#[tokio::test]
async fn ready_stream_reads_and_writes_allow_cancellation() {
    for reading in [true, false] {
        let (mut stream, mut peer) = stream_buffer::duplex_with_capacity(2 * BACKLOG);
        if reading {
            peer.write_all(&vec![7; BACKLOG]).await.unwrap();
        }
        let scope = Scope::new();
        let completed = Cell::new(0);
        let transfer = scope.run(async {
            for _ in 0..BACKLOG {
                if reading {
                    let mut byte = [0];
                    stream.read_exact(&mut byte).await?;
                    assert_eq!(byte, [7]);
                } else {
                    stream.write_all(&[7]).await?;
                }
                completed.set(completed.get() + 1);
            }
            Ok(())
        });
        let cancel = async {
            assert!(completed.get() > 0 && completed.get() < BACKLOG);
            scope.close();
        };
        let (result, ()) = tokio::join!(biased; transfer, cancel);
        assert!(result.is_err());
    }
}
