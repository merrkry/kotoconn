use super::{Client, Server, tls, udp, wire};
use anyhow::{Result, bail};
use anytls::core::{Command, Frame, PaddingFactory};
use bytes::Bytes;
use futures_util::{StreamExt, future::BoxFuture};
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
use tokio_util::{codec::FramedRead, sync::CancellationToken};

const LIMIT: Duration = Duration::from_secs(10);

#[path = "interop_tests.rs"]
mod interop;

struct Network {
    scope: Scope,
    connections: AtomicUsize,
}

impl Carrier for Network {
    fn capabilities(&self) -> Capabilities {
        Capabilities::TCP
    }
    fn scope(&self) -> &Scope {
        &self.scope
    }
    fn tcp_scoped(&self, target: Target, scope: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        self.connections.fetch_add(1, Ordering::Relaxed);
        Box::pin(async move {
            stream_task(scope, async move {
                let stream = TcpStream::connect(socket_addr(&target)?).await?;
                stream.set_nodelay(true)?;
                Ok(Box::pin(stream) as BoxStream)
            })
        })
    }
    fn udp_scoped(&self, _: Target, _: Scope) -> BoxFuture<'_, Result<Datagram>> {
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

struct Echo(mpsc::UnboundedSender<Target>);

impl Handler for Echo {
    fn tcp(&self, target: Target, mut stream: BoxStream, _: Scope) -> BoxFuture<'_, Result<()>> {
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
    fn udp(&self, mut packets: Datagram) -> BoxFuture<'_, Result<()>> {
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
    targets: mpsc::UnboundedReceiver<Target>,
    private_key: String,
}

impl Fixture {
    async fn new(padding: Option<&str>) -> Result<Self> {
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
                handler: Arc::new(Echo(targets)),
                scope: scope.clone(),
                stopping: CancellationToken::new(),
                udp_idle_timeout: Duration::from_secs(60),
            },
        )
        .await?;
        let options = AnyTlsOutboundConfig {
            server: Target::Domain {
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
            &PaddingFactory::default(),
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
            Target::Domain {
                name: "unresolved.invalid".into(),
                port: 80,
            },
        ] {
            let mut stream =
                p::Client::tcp(&client, destination.clone(), fixture.scope.child()).await?;
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
        assert_eq!(p::Client::capabilities(&client), Capabilities::BOTH);
        for destination in [
            target("127.0.0.1:53".parse()?),
            target("[::1]:53".parse()?),
            Target::Domain {
                name: "dns.invalid".into(),
                port: 53,
            },
        ] {
            let mut connection =
                p::Client::udp(&client, destination.clone(), fixture.scope.child()).await?;
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
            assert!(
                p::Client::tcp(
                    &client,
                    target("127.0.0.1:80".parse()?),
                    fixture.scope.child(),
                )
                .await
                .is_err()
            );
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
        stream
            .write_all(&Frame::new(Command::Syn, 1).to_bytes()?)
            .await?;
        stream.flush().await?;
        let mut frames = FramedRead::new(stream, wire::Frames);
        assert_eq!(frames.next().await.unwrap()?.cmd, Command::Alert);

        let mut stream = fixture.raw().await?;
        stream
            .write_all(
                &Frame::with_data(Command::Fin, 1, Bytes::from_static(b"invalid")).to_bytes()?,
            )
            .await?;
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
        let mut stream = p::Client::tcp(&client, udp::sentinel(), fixture.scope.child()).await?;
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
                Target::Domain {
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
        let opening = p::Client::tcp(&client, target("127.0.0.1:80".parse()?), scope.clone());
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
fn rejects_invalid_tls_and_unbounded_padding_configuration() {
    for scheme in [
        "",
        "stop=2\n0=-1-5",
        "stop=2\n1=999999999-999999999",
        "stop=2\n0=c",
    ] {
        assert!(wire::padding_scheme(Some(scheme)).is_err());
    }
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
            let mut stream = p::Client::tcp(
                &client,
                target("127.0.0.1:2".parse()?),
                fixture.scope.child(),
            )
            .await?;
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await?;
            assert_eq!(response, b"ready");
            assert!(stream.write_all(b"after FIN").await.is_err());
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
            p::Client::tcp(&client, target("127.0.0.1:80".parse()?), cancelled.clone()).await?;
        let mut second = p::Client::tcp(
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
        let mut reused = p::Client::tcp(
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
async fn received_fin_is_not_echoed_and_heartbeats_continue() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(None).await?;
        let tls = fixture.raw().await?;
        let (reader, mut writer) = tokio::io::split(tls);
        let mut frames = FramedRead::new(reader, wire::Frames);
        let settings = format!(
            "v=2\nclient=kotoconn-test\npadding-md5={}",
            PaddingFactory::default().md5()
        );
        writer
            .write_all(&Frame::with_data(Command::Settings, 0, settings.into()).to_bytes()?)
            .await?;
        for sid in [1, 2] {
            for frame in [
                Frame::new(Command::Syn, sid),
                Frame::with_data(
                    Command::Psh,
                    sid,
                    wire::address(&target("127.0.0.1:80".parse()?))?.into(),
                ),
            ] {
                writer.write_all(&frame.to_bytes()?).await?;
            }
            writer.flush().await?;
            loop {
                let frame = frames.next().await.unwrap()?;
                if frame.cmd == Command::Psh {
                    assert_eq!(frame.sid, sid);
                    assert_eq!(&frame.data[..], b"ready");
                    break;
                }
            }
            writer
                .write_all(&Frame::new(Command::Fin, sid).to_bytes()?)
                .await?;
            writer
                .write_all(&Frame::new(Command::HeartRequest, 0).to_bytes()?)
                .await?;
            writer.flush().await?;
            // The heartbeat is a protocol barrier after FIN, without a delay.
            assert_eq!(frames.next().await.unwrap()?.cmd, Command::HeartResponse);
        }
        fixture.close().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn closing_one_multiplexed_stream_preserves_the_other() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new(None).await?;
        let tls = fixture.raw().await?;
        let (reader, mut writer) = tokio::io::split(tls);
        let mut frames = FramedRead::new(reader, wire::Frames);
        let settings = format!(
            "v=2\nclient=kotoconn-test\npadding-md5={}",
            PaddingFactory::default().md5()
        );
        writer
            .write_all(&Frame::with_data(Command::Settings, 0, settings.into()).to_bytes()?)
            .await?;
        for sid in [1, 2] {
            for frame in [
                Frame::new(Command::Syn, sid),
                Frame::with_data(
                    Command::Psh,
                    sid,
                    wire::address(&target("127.0.0.1:80".parse()?))?.into(),
                ),
            ] {
                writer.write_all(&frame.to_bytes()?).await?;
            }
        }
        writer.flush().await?;
        let mut greeted = std::collections::HashSet::new();
        while greeted.len() < 2 {
            let frame = frames.next().await.unwrap()?;
            if frame.cmd == Command::Psh {
                assert_eq!(&frame.data[..], b"ready");
                greeted.insert(frame.sid);
            }
        }
        assert_eq!(greeted, std::collections::HashSet::from([1, 2]));

        writer
            .write_all(&Frame::new(Command::Fin, 1).to_bytes()?)
            .await?;
        writer
            .write_all(&Frame::with_data(Command::Psh, 2, Bytes::from_static(b"alive")).to_bytes()?)
            .await?;
        writer.flush().await?;
        let reply = frames.next().await.unwrap()?;
        assert_eq!(reply.sid, 2);
        assert_eq!(reply.cmd, Command::Psh);
        assert_eq!(&reply.data[..], b"alive");
        fixture.close().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
