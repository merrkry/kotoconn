use super::{
    Client, Server,
    frame::{self, Command, Frame, FrameReader, Sid},
    padding::PaddingScheme,
    tls, udp, wire,
};
use anyhow::{Result, bail};
use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use kotoconn_config::{
    AnyTlsInboundConfig, AnyTlsOutboundConfig, TlsClientConfig, TlsServerConfig,
};
use kotoconn_protocol::{self as p, *};
use std::{
    net::IpAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;

const LIMIT: Duration = Duration::from_secs(10);

fn sid(sid: u32) -> Sid {
    Sid::new(sid).unwrap()
}

fn encode(frames: &[Frame]) -> Bytes {
    let mut bytes = BytesMut::new();
    for frame in frames {
        frame.encode(&mut bytes);
    }
    bytes.freeze()
}

#[cfg(feature = "interop-tests")]
#[path = "../../../e2e/anytls/interop.rs"]
mod interop;

struct Network {
    scope: Scope,
    connections: AtomicUsize,
}

impl Connector for Network {
    fn capabilities(&self) -> Capabilities {
        Capabilities::TCP
    }
    fn scope(&self) -> &Scope {
        &self.scope
    }
    fn tcp_scoped(&self, target: Endpoint, scope: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        self.connections.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            stream_task(scope, async move {
                let stream = TcpStream::connect(socket_addr(&target)?).await?;
                stream.set_nodelay(true)?;
                Ok(Box::pin(stream) as BoxStream)
            })
        })
    }
    fn udp_scoped(&self, _: Endpoint, _: Scope) -> BoxFuture<'_, Result<Datagram>> {
        Box::pin(async { bail!("test carrier has no UDP") })
    }
}

impl Resolver for Network {
    fn resolve(&self, name: String) -> BoxFuture<'_, Result<Vec<IpAddr>>> {
        Box::pin(async move {
            assert_eq!(name, "localhost");
            Ok(vec!["127.0.0.1".parse()?])
        })
    }
}

struct Echo(mpsc::UnboundedSender<Endpoint>);

impl InboundHandler for Echo {
    fn tcp(
        &self,
        _: std::net::SocketAddr,
        target: Endpoint,
        mut stream: BoxStream,
        _: Scope,
    ) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let finish = target.port() == 2;
            self.0.send(target)?;
            stream.write_all(b"ready").await?;
            stream.flush().await?;
            if finish {
                stream.shutdown().await?;
                return Ok(());
            }
            let (mut reader, mut writer) = tokio::io::split(stream);
            tokio::io::copy(&mut reader, &mut writer).await?;
            Ok(())
        })
    }
    fn udp(&self, _: std::net::SocketAddr, mut packets: Datagram) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            while let Some(packet) = packets.rx.recv().await {
                self.0.send(packet.target.clone())?;
                packets.tx.send(packet).await?;
            }
            Ok(())
        })
    }
}

struct Fixture {
    scope: Scope,
    network: Arc<Network>,
    options: AnyTlsOutboundConfig,
    targets: mpsc::UnboundedReceiver<Endpoint>,
    #[cfg(feature = "interop-tests")]
    private_key: String,
}

impl Fixture {
    async fn new(padding: Option<&str>) -> Result<Self> {
        Self::with_handler(padding, |targets| Arc::new(Echo(targets))).await
    }

    async fn with_handler(
        padding: Option<&str>,
        handler: impl FnOnce(mpsc::UnboundedSender<Endpoint>) -> Arc<dyn InboundHandler>,
    ) -> Result<Self> {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
        let certificate = cert.cert.pem();
        let private_key = cert.signing_key.serialize_pem();
        let scope = Scope::new();
        let (targets, receive) = mpsc::unbounded_channel();
        let server = Server::new(&AnyTlsInboundConfig {
            listen: "127.0.0.1:0".parse()?,
            password: "secret".into(),
            tls: TlsServerConfig {
                certificate: certificate.clone(),
                private_key: private_key.clone(),
            },
            padding_scheme: padding.map(str::to_owned),
        })?;
        let bound = p::Server::bind(
            &server,
            "127.0.0.1:0".parse()?,
            ServerContext {
                handler: handler(targets),
                scope: scope.clone(),
                stopping: CancellationToken::new(),
                udp_idle_timeout: Duration::from_secs(60),
            },
        )
        .await?;
        let options = AnyTlsOutboundConfig {
            server: Endpoint::Domain {
                name: "localhost".into(),
                port: bound.local_addr.port(),
            },
            password: "secret".into(),
            tls: TlsClientConfig {
                server_name: None,
                certificate: Some(certificate),
            },
            idle_session_timeout: None,
        };
        scope.spawn(bound.run)?;
        let network = Arc::new(Network {
            scope: scope.clone(),
            connections: AtomicUsize::new(0),
        });
        Ok(Self {
            scope,
            network,
            options,
            targets: receive,
            #[cfg(feature = "interop-tests")]
            private_key,
        })
    }

    fn client(&self) -> Result<Client> {
        Client::new(&self.options, self.network.clone(), self.network.clone())
    }

    async fn close(self) {
        self.scope.close();
        self.scope.wait().await;
    }

    async fn raw(&self) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
        let (tls, name) = tls::client(&self.options.tls, &self.options.server)?;
        let tcp = TcpStream::connect(("127.0.0.1", self.options.server.port())).await?;
        let mut stream = tls.connect(name, tcp).await?;
        wire::send_authentication(
            &mut stream,
            &wire::password_hash("secret"),
            &PaddingScheme::default(),
        )
        .await?;
        Ok(stream)
    }
}

#[tokio::test]
async fn server_first_tcp_reuses_sessions_and_preserves_domains() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let mut fixture =
            Fixture::new(Some("stop=3\n0=31-31\n1=200-200\n2=100-100,c,200-200")).await?;
        let client = fixture.client()?;
        for destination in [
            target("127.0.0.1:1234".parse()?),
            target("[::1]:4567".parse()?),
            Endpoint::Domain {
                name: "unresolved.invalid".into(),
                port: 80,
            },
        ] {
            let mut stream =
                p::OutboundAdapter::tcp(&client, destination.clone(), fixture.scope.child())
                    .await?;
            let mut ready = [0; 5];
            stream.read_exact(&mut ready).await?;
            assert_eq!(&ready, b"ready");
            assert_eq!(fixture.targets.recv().await, Some(destination));

            let data: Vec<_> = (0..1_048_576).map(|n| n as u8).collect();
            let mut response = vec![0; data.len()];
            let (mut read, mut write) = tokio::io::split(&mut stream);
            tokio::try_join!(
                async {
                    write.write_all(&data).await?;
                    write.flush().await
                },
                read.read_exact(&mut response)
            )?;
            assert_eq!(response, data);
            stream.shutdown().await?;
        }
        assert_eq!(fixture.network.connections.load(Ordering::Relaxed), 1);
        drop(client);
        fixture.close().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn udp_over_tcp_supports_empty_and_large_datagrams() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let mut fixture = Fixture::new(None).await?;
        let client = fixture.client()?;
        assert_eq!(
            p::OutboundAdapter::capabilities(&client),
            Capabilities {
                domain_targets: true,
                ..Capabilities::BOTH
            }
        );
        for destination in [
            target("127.0.0.1:53".parse()?),
            target("[::1]:53".parse()?),
            Endpoint::Domain {
                name: "dns.invalid".into(),
                port: 53,
            },
        ] {
            let mut connection =
                p::OutboundAdapter::udp(&client, destination.clone(), fixture.scope.child())
                    .await?;
            for payload in [vec![], vec![42], vec![7; 65507]] {
                connection
                    .tx
                    .send(Packet {
                        target: destination.clone(),
                        payload: payload.clone().into(),
                    })
                    .await?;
                let response = connection.rx.recv().await.unwrap();
                assert_eq!(response.payload, payload);
                assert_eq!(response.target, destination);
                assert_eq!(fixture.targets.recv().await, Some(destination.clone()));
            }
        }
        drop(client);
        fixture.close().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn rejects_wrong_password_untrusted_certificate_and_wrong_name() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(None).await?;
        for kind in ["password", "certificate", "name"] {
            let mut options = fixture.options.clone();
            match kind {
                "password" => options.password = "wrong".into(),
                "certificate" => options.tls.certificate = None,
                _ => options.tls.server_name = Some("wrong.invalid".into()),
            }
            let client = Client::new(&options, fixture.network.clone(), fixture.network.clone())?;
            let opened = p::OutboundAdapter::tcp(
                &client,
                target("127.0.0.1:80".parse()?),
                fixture.scope.child(),
            )
            .await;
            // The server drops a wrong password without replying, after the
            // client already returned the stream.
            let failed = match opened {
                Err(_) => true,
                Ok(mut stream) => stream.read(&mut [0]).await.is_err(),
            };
            assert!(failed, "{kind}");
        }
        fixture.close().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn rejects_syn_before_settings_and_control_payloads() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(None).await?;
        let mut stream = fixture.raw().await?;
        stream.write_all(&encode(&[Frame::Syn(sid(1))])).await?;
        stream.flush().await?;
        let mut frames = FrameReader::new(stream);
        assert!(matches!(frames.next().await?.unwrap(), Frame::Alert(_)));

        let mut stream = fixture.raw().await?;
        stream
            .write_all(&frame::header(Command::Fin, 1, b"invalid".len()))
            .await?;
        stream.write_all(b"invalid").await?;
        stream.flush().await?;
        assert!(stream.read_exact(&mut [0]).await.is_err());
        fixture.close().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn uot_datagram_mode_uses_distinct_address_families() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let mut fixture = Fixture::new(None).await?;
        let client = fixture.client()?;
        let mut stream =
            p::OutboundAdapter::tcp(&client, udp::sentinel(), fixture.scope.child()).await?;
        let mut request = vec![0];
        request.extend(wire::address(&target("0.0.0.0:0".parse()?))?);
        stream.write_all(&request).await?;
        // Literal vectors, independent of the adapter's serializer.
        for (header, destination) in [
            (
                vec![0, 127, 0, 0, 1, 0, 53],
                target("127.0.0.1:53".parse()?),
            ),
            (
                vec![1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 53],
                target("[::1]:53".parse()?),
            ),
            (
                vec![2, 3, b'd', b'n', b's', 0, 53],
                Endpoint::Domain {
                    name: "dns".into(),
                    port: 53,
                },
            ),
        ] {
            let mut bytes = header;
            bytes.extend_from_slice(&[0, 3, 1, 2, 3]);
            for byte in &bytes {
                stream.write_all(&[*byte]).await?;
            }
            stream.flush().await?;
            let mut response = vec![0; bytes.len()];
            stream.read_exact(&mut response).await?;
            assert_eq!(response, bytes);
            assert_eq!(fixture.targets.recv().await, Some(destination));
        }
        drop(client);
        fixture.close().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn cancellation_releases_a_stalled_tls_handshake() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(None).await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut options = fixture.options.clone();
        options.server = target(listener.local_addr()?);
        let client = Client::new(&options, fixture.network.clone(), fixture.network.clone())?;
        let scope = fixture.scope.child();
        {
        let opening = p::OutboundAdapter::tcp(&client, target("127.0.0.1:80".parse()?), scope.clone());
        tokio::pin!(opening);
        let (accepted, _) = tokio::select! {
            result = &mut opening => panic!("handshake completed before server replied: {}", result.is_ok()),
            result = listener.accept() => result?,
        };
        scope.close();
        assert!(opening.await.is_err());
        scope.wait().await;
        drop(accepted);
        }
        drop(client);
        fixture.close().await;
        Ok::<_, anyhow::Error>(())
    }).await?
}

#[test]
fn rejects_invalid_tls_configuration() {
    assert!(
        tls::server(&TlsServerConfig {
            certificate: String::new(),
            private_key: String::new()
        })
        .is_err()
    );
}

#[tokio::test]
async fn peer_fin_preserves_queued_bytes_and_closes_both_directions() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(None).await?;
        let client = fixture.client()?;
        for _ in 0..16 {
            let mut stream = p::OutboundAdapter::tcp(
                &client,
                target("127.0.0.1:2".parse()?),
                fixture.scope.child(),
            )
            .await?;
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await?;
            assert_eq!(response, b"ready");
            let error = stream.write_all(b"after FIN").await.unwrap_err();
            assert!(p::PeerStoppedReading::is(&error));
        }
        assert_eq!(fixture.network.connections.load(Ordering::Relaxed), 1);
        drop(client);
        fixture.close().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn cancellation_waits_for_release_and_leaves_other_streams_usable() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(None).await?;
        let client = fixture.client()?;
        let cancelled = fixture.scope.child();
        let mut first =
            p::OutboundAdapter::tcp(&client, target("127.0.0.1:80".parse()?), cancelled.clone())
                .await?;
        let mut second = p::OutboundAdapter::tcp(
            &client,
            target("127.0.0.1:80".parse()?),
            fixture.scope.child(),
        )
        .await?;
        first.read_exact(&mut [0; 5]).await?;
        second.read_exact(&mut [0; 5]).await?;
        cancelled.close();
        cancelled.wait().await;
        assert!(first.read_exact(&mut [0]).await.is_err());

        second.write_all(b"still alive").await?;
        let mut reply = [0; 11];
        second.read_exact(&mut reply).await?;
        assert_eq!(&reply, b"still alive");
        let mut reused = p::OutboundAdapter::tcp(
            &client,
            target("127.0.0.1:80".parse()?),
            fixture.scope.child(),
        )
        .await?;
        reused.read_exact(&mut [0; 5]).await?;
        assert_eq!(fixture.network.connections.load(Ordering::Relaxed), 2);
        drop(client);
        fixture.close().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn received_fin_preserves_siblings_and_heartbeats_without_being_echoed() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(None).await?;
        let tls = fixture.raw().await?;
        let (reader, mut writer) = tokio::io::split(tls);
        let mut frames = FrameReader::new(reader);
        let settings = format!(
            "v=2\nclient=kotoconn-test\npadding-md5={}",
            PaddingScheme::default().md5()
        );
        let mut opening = vec![Frame::Settings(settings.into())];
        for id in [1, 2] {
            opening.push(Frame::Syn(sid(id)));
            let address = wire::address(&target("127.0.0.1:80".parse()?))?;
            opening.push(Frame::Push(sid(id), address.into()));
        }
        writer.write_all(&encode(&opening)).await?;
        writer.flush().await?;
        let mut greeted = std::collections::HashSet::new();
        while greeted.len() < 2 {
            if let Frame::Push(id, data) = frames.next().await?.unwrap() {
                assert_eq!(&data[..], b"ready");
                greeted.insert(id.get());
            }
        }
        assert_eq!(greeted, std::collections::HashSet::from([1, 2]));

        // The heartbeat follows FIN on the wire, so an echoed FIN fails this read.
        writer
            .write_all(&encode(&[Frame::Fin(sid(1)), Frame::HeartRequest]))
            .await?;
        writer.flush().await?;
        assert_eq!(frames.next().await?.unwrap(), Frame::HeartResponse);

        writer
            .write_all(&encode(&[Frame::Push(
                sid(2),
                Bytes::from_static(b"alive"),
            )]))
            .await?;
        writer.flush().await?;
        assert_eq!(
            frames.next().await?.unwrap(),
            Frame::Push(sid(2), Bytes::from_static(b"alive"))
        );

        writer
            .write_all(&encode(&[Frame::Fin(sid(2)), Frame::HeartRequest]))
            .await?;
        writer.flush().await?;
        assert_eq!(frames.next().await?.unwrap(), Frame::HeartResponse);

        fixture.close().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn transport_close_after_fin_still_delivers_queued_bytes() -> Result<()> {
    struct GatedReader {
        entered: mpsc::UnboundedSender<Scope>,
        received: mpsc::UnboundedSender<Vec<u8>>,
        release: CancellationToken,
    }

    impl InboundHandler for GatedReader {
        fn tcp(
            &self,
            _: std::net::SocketAddr,
            _: Endpoint,
            mut stream: BoxStream,
            scope: Scope,
        ) -> BoxFuture<'_, Result<()>> {
            Box::pin(async move {
                self.entered.send(scope)?;
                self.release.cancelled().await;
                let mut bytes = Vec::new();
                stream.read_to_end(&mut bytes).await?;
                self.received.send(bytes)?;
                Ok(())
            })
        }

        fn udp(&self, _: std::net::SocketAddr, _: Datagram) -> BoxFuture<'_, Result<()>> {
            Box::pin(async { bail!("unexpected UDP association") })
        }
    }

    tokio::time::timeout(LIMIT, async {
        let (entered, mut active) = mpsc::unbounded_channel();
        let (received, mut payloads) = mpsc::unbounded_channel();
        let release = CancellationToken::new();
        let fixture = Fixture::with_handler(None, |_| {
            Arc::new(GatedReader {
                entered,
                received,
                release: release.clone(),
            })
        })
        .await?;
        let (reader, mut writer) = tokio::io::split(fixture.raw().await?);
        let mut frames = FrameReader::new(reader);
        let settings = format!("v=2\npadding-md5={}", PaddingScheme::default().md5());
        writer
            .write_all(&encode(&[
                Frame::Settings(settings.into()),
                Frame::Syn(sid(1)),
                Frame::Push(sid(1), Bytes::from_static(&[1, 127, 0, 0, 1, 0, 80])),
            ]))
            .await?;
        let stream_scope = active.recv().await.unwrap();
        assert!(matches!(
            frames.next().await?,
            Some(Frame::ServerSettings(_))
        ));
        assert_eq!(
            frames.next().await?,
            Some(Frame::SynAck(sid(1), Bytes::new()))
        );

        writer
            .write_all(&encode(&[
                Frame::Push(sid(1), Bytes::from_static(b"complete request")),
                Frame::Fin(sid(1)),
                Frame::HeartRequest,
            ]))
            .await?;
        // A client that disables reuse closes TLS right after FIN.
        assert_eq!(frames.next().await?, Some(Frame::HeartResponse));
        writer.shutdown().await?;
        assert!(!matches!(frames.next().await, Ok(Some(_))));

        // The handler reads only after the session ended.
        release.cancel();
        stream_scope.wait().await;
        let received = payloads.try_recv().ok();
        fixture.close().await;
        assert_eq!(received.as_deref(), Some(&b"complete request"[..]));
        Ok(())
    })
    .await?
}
