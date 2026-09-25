//! Always-ready I/O must yield so other tasks can run and cancel it.
use kotoconn_protocol::{Scope, queue, stream_buffer};
use std::cell::Cell;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const BACKLOG: usize = 8192;

#[tokio::test]
async fn ready_queue_receives_and_sends_allow_cancellation() {
    for receiving in [true, false] {
        let (tx, mut rx) = queue::channel(1024 * 1024, |_: &usize| 0);
        if receiving {
            for index in 0..BACKLOG {
                tx.try_send(index).unwrap();
            }
        }
        let scope = Scope::new();
        let completed = Cell::new(0);
        let transfer = scope.run(async {
            for index in 0..BACKLOG {
                // Exercise each direction alone: a send must not hide a receive
                // that never yields, or vice versa.
                if receiving {
                    assert_eq!(rx.recv().await, Some(index));
                } else {
                    tx.send(index).await?;
                }
                completed.set(completed.get() + 1);
            }
            Ok(())
        });
        let cancel = async {
            assert!(
                completed.get() > 0 && completed.get() < BACKLOG,
                "receiving={receiving}"
            );
            scope.close();
        };
        let (result, ()) = tokio::join!(biased; transfer, cancel);
        assert!(result.is_err());
    }
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
