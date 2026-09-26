use kotoconn_config::{DirectOutboundConfig, OutboundImpl, TransportProtocol};
use kotoconn_outbounds::{Clients, System, SystemResolver};
use kotoconn_protocol::*;
use std::{future::poll_fn, io, sync::Arc, time::Duration};
use tokio::net::UdpSocket;

async fn exchange(io: &mut BoxPacketIo, target: Target, payload: Vec<u8>) {
    let packet = Packet {
        target: target.clone(),
        payload: payload.clone().into(),
    };
    assert_eq!(
        poll_fn(|cx| io.poll_send(cx, std::slice::from_ref(&packet)))
            .await
            .unwrap(),
        1
    );
    let mut replies = Vec::new();
    assert_eq!(
        poll_fn(|cx| io.poll_recv(cx, &mut replies)).await.unwrap(),
        1
    );
    assert_eq!(replies[0].target, target);
    assert_eq!(replies[0].payload, payload);
}

#[tokio::test]
async fn native_datagrams_preserve_boundaries_and_nested_carrier_cancellation() {
    for address in ["127.0.0.1:0", "[::1]:0"] {
        tokio::time::timeout(Duration::from_secs(5), async {
            let socket = UdpSocket::bind(address).await.unwrap();
            let destination = target(socket.local_addr().unwrap());
            let echo = tokio::spawn(async move {
                let mut buffer = vec![0; 65536];
                for _ in 0..4 {
                    let (len, peer) = socket.recv_from(&mut buffer).await.unwrap();
                    socket.send_to(&buffer[..len], peer).await.unwrap();
                }
            });
            let root = Scope::new();
            let system = Arc::new(System::new(root.clone()));
            let middle = Arc::new(
                Clients::new(
                    OutboundImpl::Direct(DirectOutboundConfig {}),
                    system,
                    Arc::new(SystemResolver),
                )
                .unwrap(),
            );
            let leaf = Clients::new(
                OutboundImpl::Direct(DirectOutboundConfig {}),
                middle.clone(),
                Arc::new(SystemResolver),
            )
            .unwrap();
            let caller = Scope::new();
            let mut native = leaf
                .udp_native_scoped(destination.clone(), caller.clone())
                .await
                .unwrap()
                .unwrap();
            let mut sibling = middle
                .udp_native_scoped(destination.clone(), caller.clone())
                .await
                .unwrap()
                .unwrap();

            for bytes in [vec![], vec![7; 17], vec![9; 60000]] {
                exchange(&mut native.io, destination.clone(), bytes).await;
            }
            let control = leaf.control(TransportProtocol::Udp);
            control.close();
            control.wait().await;
            assert!(native.scope.is_closed());
            assert_eq!(
                poll_fn(|cx| native.io.poll_recv(cx, &mut Vec::new()))
                    .await
                    .unwrap(),
                0
            );
            let packet = Packet {
                target: destination.clone(),
                payload: vec![1].into(),
            };
            assert_eq!(
                poll_fn(|cx| native.io.poll_send(cx, std::slice::from_ref(&packet)))
                    .await
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );

            exchange(&mut sibling.io, destination, vec![3; 1200]).await;
            drop(sibling);
            caller.wait().await;
            assert!(!middle.control(TransportProtocol::Udp).is_closed());
            root.close();
            root.wait().await;
            echo.await.unwrap();
        })
        .await
        .unwrap();
    }
}
