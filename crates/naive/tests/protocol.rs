#![cfg(target_os = "linux")]

use anyhow::{Result, bail};
use futures_util::future::{BoxFuture, try_join_all};
use kotoconn_config::{NaiveInboundConfig, NaiveOutboundConfig};
use kotoconn_naive::{Client, Server};
use kotoconn_protocol::{self as p, Client as _, Server as _, *};
use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
};

const LIMIT: Duration = Duration::from_secs(20);

fn certificate() -> Result<rcgen::CertifiedKey<rcgen::KeyPair>> {
    let signing_key = rcgen::KeyPair::generate()?;
    let mut params = rcgen::CertificateParams::new(vec!["proxy.test".into()])?;
    let now = std::time::SystemTime::now();
    params.not_before = (now - Duration::from_secs(60)).into();
    params.not_after = (now + Duration::from_secs(86400)).into();
    let cert = params.self_signed(&signing_key)?;
    Ok(rcgen::CertifiedKey { cert, signing_key })
}

struct SocketCarrier {
    scope: Scope,
    destination: SocketAddr,
    connections: AtomicUsize,
}

impl Carrier for SocketCarrier {
    fn capabilities(&self) -> Capabilities {
        Capabilities::TCP
    }
    fn scope(&self) -> &Scope {
        &self.scope
    }

    fn tcp_scoped(&self, target: Target, caller: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        Box::pin(async move {
            assert_eq!(socket_addr(&target)?, self.destination);
            self.connections.fetch_add(1, Ordering::Relaxed);
            let scope = self.scope.child().tracked_by(&caller);
            p::stream_task(scope, async move {
                Ok(Box::pin(TcpStream::connect(socket_addr(&target)?).await?) as BoxStream)
            })
        })
    }

    fn udp_scoped(&self, _: Target, _: Scope) -> BoxFuture<'_, Result<Datagram>> {
        Box::pin(async { bail!("UDP must not be attempted") })
    }
}

struct Resolve(mpsc::UnboundedSender<String>);

impl Resolver for Resolve {
    fn resolve(&self, name: String) -> BoxFuture<'_, Result<Vec<IpAddr>>> {
        Box::pin(async move {
            self.0.send(name)?;
            Ok(vec!["127.0.0.1".parse()?])
        })
    }
}

struct Echo(mpsc::UnboundedSender<Target>);

impl Handler for Echo {
    fn tcp(&self, target: Target, mut io: BoxStream, _: Scope) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.0.send(target)?;
            io.write_all(b"hello").await?;
            io.flush().await?;
            let (mut read, mut write) = tokio::io::split(io);
            tokio::io::copy(&mut read, &mut write).await?;
            write.write_all(b"after FIN").await?;
            write.shutdown().await?;
            Ok(())
        })
    }

    fn udp(&self, _: Datagram) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { bail!("no UDP") })
    }
}

struct Fixture {
    scope: Scope,
    pool: Scope,
    carrier: Arc<SocketCarrier>,
    options: NaiveOutboundConfig,
    endpoint: Endpoint,
    names: mpsc::UnboundedReceiver<String>,
    targets: mpsc::UnboundedReceiver<Target>,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let scope = Scope::new();
        let pool = scope.child();
        let cert = certificate()?;
        let (send, targets) = mpsc::unbounded_channel();
        let server = Server::new(&NaiveInboundConfig {
            listen: "127.0.0.1:0".parse()?,
            username: "user".into(),
            password: "secret".into(),
            certificate: cert.cert.pem(),
            private_key: cert.signing_key.serialize_pem(),
        })?;
        let bound = server
            .bind(
                "127.0.0.1:0".parse()?,
                ServerContext {
                    handler: Arc::new(Echo(send)),
                    scope: scope.clone(),
                    stopping: Default::default(),
                    udp_idle_timeout: Duration::from_secs(30),
                },
            )
            .await?;
        let server = Target::Domain {
            name: "network.test".into(),
            port: bound.local_addr.port(),
        };
        let carrier = Arc::new(SocketCarrier {
            scope: scope.clone(),
            destination: bound.local_addr,
            connections: AtomicUsize::new(0),
        });
        scope.spawn(bound.run)?;
        let (send, names) = mpsc::unbounded_channel();
        let endpoint = Endpoint {
            address: server.clone(),
            resolver: Arc::new(Resolve(send)),
        };
        let options = NaiveOutboundConfig {
            server,
            username: "user".into(),
            password: "secret".into(),
            server_name: Some("proxy.test".into()),
            certificate: Some(cert.cert.pem()),
            quic: None,
        };
        Ok(Self {
            scope,
            pool,
            carrier,
            options,
            endpoint,
            names,
            targets,
        })
    }

    fn client(&self) -> Result<Client> {
        Client::new(
            self.options.clone(),
            self.endpoint.clone(),
            self.carrier.clone(),
            self.pool.clone(),
        )
    }

    async fn stop(&self) {
        self.scope.close();
        self.scope.wait().await;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.scope.close();
    }
}

fn destination() -> Target {
    Target::Domain {
        name: "target.invalid".into(),
        port: 443,
    }
}

async fn roundtrip(client: &Client, scope: Scope, size: usize) -> Result<()> {
    let mut io = client.tcp(destination(), scope).await?;
    let mut hello = [0; 5];
    io.read_exact(&mut hello).await?;
    assert_eq!(&hello, b"hello");
    let payload = (0..size).map(|i| i as u8).collect::<Vec<_>>();
    let (mut reader, mut writer) = tokio::io::split(io);
    let sending = async {
        writer.write_all(&payload).await?;
        writer.shutdown().await
    };
    let receiving = async {
        let mut output = Vec::new();
        reader.read_to_end(&mut output).await?;
        Ok::<_, std::io::Error>(output)
    };
    let ((), output) = tokio::try_join!(sending, receiving)?;
    assert_eq!(output, [payload, b"after FIN".to_vec()].concat());
    Ok(())
}

#[tokio::test]
async fn native_h2_multiplexes_and_preserves_half_close_on_one_runtime_thread() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let mut fixture = Fixture::new().await?;
        let client = fixture.client()?;
        roundtrip(&client, fixture.pool.child(), 0).await?;
        try_join_all((0..12).map(|i| roundtrip(&client, fixture.pool.child(), 200_000 + i)))
            .await?;
        assert_eq!(fixture.carrier.connections.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.names.recv().await.as_deref(), Some("network.test"));
        assert!(fixture.names.try_recv().is_err());
        for _ in 0..13 {
            assert_eq!(fixture.targets.recv().await, Some(destination()));
        }
        assert!(!client.capabilities().udp);
        assert!(
            client
                .udp(destination(), fixture.pool.child())
                .await
                .is_err()
        );
        fixture.stop().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn canceling_a_tunnel_keeps_its_siblings_and_pool_alive() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let fixture = Fixture::new().await?;
        let client = fixture.client()?;
        let first = fixture.pool.child();
        let mut io = client.tcp(destination(), first.clone()).await?;
        io.read_exact(&mut [0; 5]).await?;
        first.close();
        first.wait().await;
        drop(io);
        roundtrip(&client, fixture.pool.child(), 1000).await?;
        assert_eq!(fixture.carrier.connections.load(Ordering::Relaxed), 1);
        fixture.pool.close();
        fixture.pool.wait().await;
        assert!(
            client
                .tcp(destination(), fixture.pool.child())
                .await
                .is_err()
        );
        fixture.stop().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn authentication_trust_hostname_and_capabilities_fail_closed() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        for failure in ["password", "certificate", "hostname", "quic"] {
            let mut fixture = Fixture::new().await?;
            match failure {
                "password" => fixture.options.password = "wrong".into(),
                "certificate" => fixture.options.certificate = Some(certificate()?.cert.pem()),
                "hostname" => fixture.options.server_name = Some("other.test".into()),
                "quic" => fixture.options.quic = Some(true),
                _ => unreachable!(),
            }
            let client = fixture.client()?;
            assert!(
                client
                    .tcp(destination(), fixture.pool.child())
                    .await
                    .is_err(),
                "{failure}"
            );
            assert!(fixture.targets.try_recv().is_err());
            if failure == "quic" {
                assert_eq!(fixture.carrier.connections.load(Ordering::Relaxed), 0);
                assert!(!client.capabilities().tcp);
            }
            fixture.stop().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn malformed_authorities_fail_before_network_io() -> Result<()> {
    let fixture = Fixture::new().await?;
    let client = fixture.client()?;
    for name in [
        "",
        "x\r\nInjected: foo",
        "x\0foo",
        "user@host",
        "host:123",
        "[::1]",
    ] {
        let destination = Target::Domain {
            name: name.into(),
            port: 443,
        };
        assert!(client.tcp(destination, fixture.pool.child()).await.is_err());
    }
    assert_eq!(fixture.carrier.connections.load(Ordering::Relaxed), 0);
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn ordinary_h2_clients_negotiate_unpadded_streams_and_authenticate_each_request() -> Result<()>
{
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use bytes::Bytes;
    use tokio_rustls::{
        TlsConnector,
        rustls::{
            self,
            pki_types::{CertificateDer, pem::PemObject},
        },
    };

    tokio::time::timeout(LIMIT, async {
        let mut fixture = Fixture::new().await?;
        let mut roots = rustls::RootCertStore::empty();
        roots.add(CertificateDer::from_pem_slice(
            fixture.options.certificate.as_ref().unwrap().as_bytes(),
        )?)?;
        let mut tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        tls.alpn_protocols = vec![b"h2".to_vec()];
        let io = TlsConnector::from(Arc::new(tls))
            .connect(
                "proxy.test".try_into()?,
                TcpStream::connect(fixture.carrier.destination).await?,
            )
            .await?;
        let (mut sender, connection) = h2::client::handshake(io).await?;
        fixture.scope.spawn(async move {
            connection.await?;
            Ok(())
        })?;

        for auth in [None, Some("Basic wrong")] {
            let mut request = http::Request::builder()
                .method("CONNECT")
                .uri("target.invalid:443");
            if let Some(auth) = auth {
                request = request.header("proxy-authorization", auth);
            }
            let (response, _) = sender.send_request(request.body(())?, true)?;
            assert_eq!(response.await?.status(), 404);
        }
        assert!(fixture.targets.try_recv().is_err());

        let request = http::Request::builder()
            .method("CONNECT")
            .uri("target.invalid:443")
            .header(
                "proxy-authorization",
                format!("Basic {}", STANDARD.encode("user:secret")),
            )
            .body(())?;
        let (response, mut upload) = sender.send_request(request, false)?;
        upload.send_data(Bytes::from_static(b"raw body without padding"), true)?;
        let response = response.await?;
        assert_eq!(response.status(), 200);
        assert!(!response.headers().contains_key("padding"));
        let mut body = response.into_body();
        let mut received = Vec::new();
        while let Some(bytes) = body.data().await {
            let bytes = bytes?;
            body.flow_control().release_capacity(bytes.len())?;
            received.extend_from_slice(&bytes);
        }
        assert_eq!(received, b"helloraw body without paddingafter FIN");
        assert_eq!(fixture.targets.recv().await, Some(destination()));
        fixture.stop().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn native_client_obeys_a_proxy_response_without_padding() -> Result<()> {
    use bytes::Bytes;
    use tokio::net::TcpListener;
    use tokio_rustls::{
        TlsAcceptor,
        rustls::{self, pki_types::PrivatePkcs8KeyDer},
    };

    tokio::time::timeout(LIMIT, async {
        let mut fixture = Fixture::new().await?;
        let cert = certificate()?;
        let mut tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.cert.der().clone()],
                PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
            )?;
        tls.alpn_protocols = vec![b"h2".to_vec()];
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let acceptor = TlsAcceptor::from(Arc::new(tls));
        let sessions = fixture.scope.clone();
        fixture.scope.spawn(async move {
            let (io, _) = listener.accept().await?;
            let mut connection = h2::server::handshake(acceptor.accept(io).await?).await?;
            while let Some(request) = connection.accept().await {
                let (request, mut response) = request?;
                assert!(request.headers().contains_key("padding"));
                let mut send = response.send_response(http::Response::new(()), false)?;
                sessions.spawn(async move {
                    send.send_data(Bytes::from_static(b"hello"), false)?;
                    let mut receive = request.into_body();
                    let mut input = Vec::new();
                    while let Some(bytes) = receive.data().await {
                        let bytes = bytes?;
                        receive.flow_control().release_capacity(bytes.len())?;
                        input.extend_from_slice(&bytes);
                        send.send_data(bytes, false)?;
                    }
                    assert_eq!(input, (0..128).map(|i| i as u8).collect::<Vec<_>>());
                    send.send_data(Bytes::from_static(b"after FIN"), true)?;
                    Ok(())
                })?;
            }
            Ok(())
        })?;
        fixture.carrier = Arc::new(SocketCarrier {
            scope: fixture.scope.clone(),
            destination: address,
            connections: AtomicUsize::new(0),
        });
        fixture.options.server = target(address);
        fixture.endpoint.address = target(address);
        fixture.options.certificate = Some(cert.cert.pem());
        let client = fixture.client()?;
        roundtrip(&client, fixture.pool.child(), 128).await?;
        fixture.stop().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn closing_during_resolver_wait_finishes_native_setup_and_carrier_work() -> Result<()> {
    struct Blocked(mpsc::UnboundedSender<()>);
    impl Resolver for Blocked {
        fn resolve(&self, _: String) -> BoxFuture<'_, Result<Vec<IpAddr>>> {
            Box::pin(async move {
                self.0.send(())?;
                std::future::pending().await
            })
        }
    }

    tokio::time::timeout(LIMIT, async {
        let mut fixture = Fixture::new().await?;
        let (entered, mut entered_rx) = mpsc::unbounded_channel();
        fixture.endpoint.resolver = Arc::new(Blocked(entered));
        let client = fixture.client()?;
        let connecting = client.tcp(destination(), fixture.pool.child());
        tokio::pin!(connecting);
        tokio::select! {
            result = &mut connecting => panic!("connection finished before resolver: {}", result.is_ok()),
            _ = entered_rx.recv() => {},
        }
        fixture.pool.close();
        assert!(connecting.await.is_err());
        fixture.pool.wait().await;
        fixture.stop().await;
        Ok::<_, anyhow::Error>(())
    }).await?
}
