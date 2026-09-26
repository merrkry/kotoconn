use kotoconn_protocol::{ChunkBuffer, Stream};
use std::{future::poll_fn, pin::Pin, task::Poll, time::Duration};
use tokio::{io::AsyncWriteExt, net::TcpListener};

#[tokio::test]
async fn native_chunks_survive_other_sockets_short_reads_and_eof() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut sockets = Vec::new();
        for address in ["127.0.0.1:0", "[::1]:0"] {
            let listener = TcpListener::bind(address).await.unwrap();
            let socket = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (peer, _) = listener.accept().await.unwrap();
            sockets.push((socket, peer, ChunkBuffer::default()));
        }

        let mut retained = Vec::new();
        let mut expected = Vec::new();
        for size in [1, 1200, 16383, 16384, 60001, 7] {
            for (index, (socket, peer, buffer)) in sockets.iter_mut().enumerate() {
                let bytes = vec![(size % 251 + index) as u8; size];
                peer.write_all(&bytes).await.unwrap();
                let mut received = 0;
                while received < size {
                    let chunk = poll_fn(|cx| Pin::new(&mut *socket).poll_read_chunk(cx, buffer))
                        .await
                        .unwrap();
                    assert!(!chunk.is_empty());
                    received += chunk.len();
                    retained.push(chunk);
                }
                assert_eq!(received, size);
                expected.extend_from_slice(&bytes);

                // Clear stale readiness before another socket uses the scratch.
                assert!(
                    poll_fn(|cx| Poll::Ready(Pin::new(&mut *socket).poll_read_chunk(cx, buffer)))
                        .await
                        .is_pending()
                );
            }
        }

        for (socket, peer, buffer) in &mut sockets {
            peer.shutdown().await.unwrap();
            assert!(
                poll_fn(|cx| Pin::new(&mut *socket).poll_read_chunk(cx, buffer))
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        drop(sockets);
        assert_eq!(retained.concat(), expected);
    })
    .await
    .unwrap();
}
