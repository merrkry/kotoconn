use anyhow::Result;
use kotoconn_config::*;
use kotoconn_daemon::Daemon;
use kotoconn_outbounds::{Clients, System, SystemResolver};
use kotoconn_protocol::*;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
};

const KEY: &str = "AAECAwQFBgcICQoLDA0ODw==";

const LIMIT: Duration = Duration::from_secs(20);

fn hysteria_certificate() -> &'static rcgen::CertifiedKey<rcgen::KeyPair> {
    static CERTIFICATE: std::sync::LazyLock<rcgen::CertifiedKey<rcgen::KeyPair>> =
        std::sync::LazyLock::new(|| {
            rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap()
        });
    &CERTIFICATE
}

fn direct() -> OutboundImpl {
    OutboundImpl::Direct(DirectOutboundConfig {})
}

fn outbound(kind: &str, server: Target) -> OutboundImpl {
    match kind {
        "hysteria2" => OutboundImpl::Hysteria2(Hysteria2OutboundConfig {
            server,
            password: KEY.into(),
            server_name: Some("localhost".into()),
            ca_certificate: Some(hysteria_certificate().cert.pem()),
            obfs_password: Some("salamander-test".into()),
        }),
        "http" => OutboundImpl::Http(HttpOutboundConfig { server }),
        "socks5" => OutboundImpl::Socks5(Socks5OutboundConfig { server }),
        "shadowsocks2022" => OutboundImpl::Shadowsocks2022(Shadowsocks2022OutboundConfig {
            server,
            password: KEY.into(),
        }),
        _ => unreachable!(),
    }
}

async fn daemon() -> Result<Daemon> {
    daemon_with_idle(10000).await
}

async fn daemon_with_idle(idle: u32) -> Result<Daemon> {
    let source = format!(
        r#"
        import {{ kotoconn as k }} from '@kotoconn/bindings';
        const resolver = k.resolve_handler(name => k.lookup(name));
        const direct = k.dialer({{ dialer: undefined, outbound: {{ resolve_handler: resolver, implementation: k.direct_outbound({{}}) }} }});
        const routing = k.routing_handler(flow => flow.protocol === 'udp' ? k.route_udp(direct) : k.route(direct, flow.dest));
        const listen = {{ address: k.ip('127.0.0.1'), port: 0 }};
        for (const implementation of [k.http_inbound({{listen}}), k.socks5_inbound({{listen}}), k.shadowsocks2022_inbound({{listen, password: '{KEY}'}}), k.hysteria2_inbound({{listen, password: '{KEY}', certificate: {certificate:?}, private_key: {private_key:?}, obfs_password: 'salamander-test'}})]) {{
            k.inbound({{ implementation, routing_handler: routing, udp_idle_timeout: k.timeout({idle}) }});
        }}
    "#,
        certificate = hysteria_certificate().cert.pem(),
        private_key = hysteria_certificate().signing_key.serialize_pem(),
    );
    Ok(Daemon::start_with_sources(
        "main.ts".into(),
        HashMap::from([("main.ts".into(), source)]),
        Duration::from_secs(1),
    )
    .await?)
}

fn address(daemon: &Daemon, kind: &str) -> Target {
    let id = daemon
        .policy()
        .config()
        .inbounds
        .iter()
        .find(|(_, c)| {
            matches!(
                (kind, &c.implementation),
                ("hysteria2", InboundImpl::Hysteria2(_))
                    | ("http", InboundImpl::Http(_))
                    | ("socks5", InboundImpl::Socks5(_))
                    | ("shadowsocks2022", InboundImpl::Shadowsocks2022(_))
            )
        })
        .unwrap()
        .0;
    target(daemon.listen_addresses()[id])
}

async fn echoes(scope: &Scope) -> Result<(Target, Target)> {
    let tcp = TcpListener::bind("127.0.0.1:0").await?;
    let udp = UdpSocket::bind("127.0.0.1:0").await?;
    let addresses = (target(tcp.local_addr()?), target(udp.local_addr()?));
    let children = scope.clone();
    scope.spawn(async move {
        loop {
            let (mut stream, _) = tcp.accept().await?;
            children.spawn(async move {
                stream.write_all(b"hello").await?;
                let (mut read, mut write) = stream.split();
                tokio::io::copy(&mut read, &mut write).await?;
                write.shutdown().await?;
                Ok(())
            })?;
        }
    })?;
    scope.spawn(async move {
        let mut buffer = vec![0; 65536];
        loop {
            let (n, peer) = udp.recv_from(&mut buffer).await?;
            udp.send_to(&buffer[..n], peer).await?;
        }
    })?;
    Ok(addresses)
}

async fn tcp_roundtrip(client: &dyn Carrier, target: Target) -> Result<()> {
    let mut stream = client.tcp(target).await?;
    let mut greeting = [0; 5];
    stream.read_exact(&mut greeting).await?;
    assert_eq!(&greeting, b"hello");
    let payload: Vec<_> = (0..131072).map(|n| n as u8).collect();
    let (mut reader, mut writer) = tokio::io::split(stream);
    let sending = async {
        writer.write_all(&payload).await?;
        writer.shutdown().await
    };
    let receiving = async {
        let mut response = Vec::new();
        reader.read_to_end(&mut response).await?;
        Ok::<_, std::io::Error>(response)
    };
    let ((), response) = tokio::try_join!(sending, receiving)?;
    assert_eq!(response, payload);
    Ok(())
}

async fn udp_roundtrip(client: &dyn Carrier, target: Target) -> Result<()> {
    let mut connection = client.udp(target.clone()).await?;
    for payload in [vec![], vec![42], (0..8192).map(|n| n as u8).collect()] {
        connection
            .tx
            .send(Packet {
                target: target.clone(),
                payload: payload.clone().into(),
            })
            .await?;
        let response = connection.rx.recv().await.unwrap();
        assert_eq!(response.target, target);
        assert_eq!(response.payload, payload);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn protocols_and_nested_carriers_preserve_tcp_and_udp() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let daemon = daemon().await?;
        let scope = Scope::new();
        let (tcp, udp) = echoes(&scope).await?;
        let system: Arc<dyn Carrier> = Arc::new(System::new(scope.clone()));

        for kind in ["http", "socks5", "shadowsocks2022", "hysteria2"] {
            let client = Clients::new(
                outbound(kind, address(&daemon, kind)),
                system.clone(),
                Arc::new(SystemResolver),
            )?;
            tcp_roundtrip(&client, tcp.clone()).await?;
            if kind != "http" {
                udp_roundtrip(&client, udp.clone()).await?;
            } else {
                assert!(client.udp(udp.clone()).await.is_err());
            }
        }

        // SOCKS over Shadowsocks uses one carrier for both control and data.
        let ss: Arc<dyn Carrier> = Arc::new(Clients::new(
            outbound("shadowsocks2022", address(&daemon, "shadowsocks2022")),
            system.clone(),
            Arc::new(SystemResolver),
        )?);
        let socks: Arc<dyn Carrier> = Arc::new(Clients::new(
            outbound("socks5", address(&daemon, "socks5")),
            ss,
            Arc::new(SystemResolver),
        )?);
        tcp_roundtrip(socks.as_ref(), tcp.clone()).await?;
        udp_roundtrip(socks.as_ref(), udp.clone()).await?;
        let http = Clients::new(
            outbound("http", address(&daemon, "http")),
            socks,
            Arc::new(SystemResolver),
        )?;
        tcp_roundtrip(&http, tcp.clone()).await?;

        // QUIC travels through SOCKS UDP carried by Shadowsocks.
        let lower = Arc::new(Clients::new(
            outbound("shadowsocks2022", address(&daemon, "shadowsocks2022")),
            system.clone(),
            Arc::new(SystemResolver),
        )?);
        let lower = Arc::new(Clients::new(
            outbound("socks5", address(&daemon, "socks5")),
            lower,
            Arc::new(SystemResolver),
        )?);
        let hysteria = Clients::new(
            outbound("hysteria2", address(&daemon, "hysteria2")),
            lower,
            Arc::new(SystemResolver),
        )?;
        tcp_roundtrip(&hysteria, tcp.clone()).await?;
        udp_roundtrip(&hysteria, udp.clone()).await?;

        // A domain user target cannot accidentally fall through to OS DNS.
        let direct = Clients::new(direct(), system, Arc::new(SystemResolver))?;
        let mut rejected = direct
            .tcp(Target::Domain {
                name: "localhost".into(),
                port: 80,
            })
            .await?;
        assert_eq!(rejected.read(&mut [0]).await?, 0);
        scope.close();
        scope.wait().await;
        daemon.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_admission_preserves_pipelined_bytes_and_rejects_bad_headers() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let daemon = daemon().await?;
        let scope = Scope::new();
        let (tcp, _) = echoes(&scope).await?;
        let endpoint = socket_addr(&address(&daemon, "http"))?;
        let destination = socket_addr(&tcp)?;
        let mut stream = TcpStream::connect(endpoint).await?;

        stream
            .write_all(
                format!("CONNECT {destination} HTTP/1.1\r\nHost: {destination}\r\n\r\npipelined")
                    .as_bytes(),
            )
            .await?;
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            header.push(stream.read_u8().await?);
        }
        assert!(header.starts_with(b"HTTP/1.1 200"));
        let mut payload = [0; 14];
        stream.read_exact(&mut payload).await?;
        assert_eq!(&payload, b"hellopipelined");
        drop(stream);

        let mut stream = TcpStream::connect(endpoint).await?;
        stream
            .write_all(b"CONNECT invalid HTTP/1.1\r\n\r\n")
            .await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        assert!(response.starts_with(b"HTTP/1.1 400"));

        scope.close();
        scope.wait().await;
        daemon.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sessions_split_by_destination_and_close_without_killing_association() -> Result<()> {
    for kind in ["socks5", "hysteria2"] {
        tokio::time::timeout(LIMIT, async {
            let daemon = daemon().await?;
            let scope = Scope::new();
            let (_, a) = echoes(&scope).await?;
            let (_, b) = echoes(&scope).await?;
            let client = Clients::new(
                outbound(kind, address(&daemon, kind)),
                Arc::new(System::new(scope.clone())),
                Arc::new(SystemResolver),
            )?;
            let mut association = client.udp(a.clone()).await?;

            for destination in [&a, &b, &a] {
                association
                    .tx
                    .send(Packet {
                        target: destination.clone(),
                        payload: vec![1].into(),
                    })
                    .await?;
                assert_eq!(association.rx.recv().await.unwrap().target, *destination);
            }

            let sessions = daemon.sessions().await?;
            let a_session = sessions.iter().find(|s| s.destination == a).unwrap();
            let b_session = sessions.iter().find(|s| s.destination == b).unwrap();
            assert_eq!(sessions.len(), 2);
            a_session.close();
            a_session.wait().await;

            association
                .tx
                .send(Packet {
                    target: b.clone(),
                    payload: vec![2].into(),
                })
                .await?;
            assert_eq!(association.rx.recv().await.unwrap().payload, vec![2]);
            assert!(
                daemon
                    .sessions()
                    .await?
                    .iter()
                    .any(|s| s.id == b_session.id)
            );
            association
                .tx
                .send(Packet {
                    target: a.clone(),
                    payload: vec![3].into(),
                })
                .await?;
            assert_eq!(association.rx.recv().await.unwrap().payload, vec![3]);
            assert!(
                daemon
                    .sessions()
                    .await?
                    .iter()
                    .any(|s| s.destination == a && s.id != a_session.id)
            );

            drop(association);
            scope.close();
            scope.wait().await;
            daemon.shutdown().await?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lower_tcp_close_ends_socks_udp_control_but_not_sibling_udp() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let daemon = daemon().await?;
        let scope = Scope::new();
        let (_, target) = echoes(&scope).await?;
        let lower = Arc::new(Clients::new(
            outbound("shadowsocks2022", address(&daemon, "shadowsocks2022")),
            Arc::new(System::new(scope.clone())),
            Arc::new(SystemResolver),
        )?);
        let socks = Clients::new(
            outbound("socks5", address(&daemon, "socks5")),
            lower.clone(),
            Arc::new(SystemResolver),
        )?;
        let mut association = socks.udp(target.clone()).await?;

        association
            .tx
            .send(Packet {
                target: target.clone(),
                payload: vec![1].into(),
            })
            .await?;
        association.rx.recv().await.unwrap();
        lower.control(TransportProtocol::Tcp).close();
        assert!(association.rx.recv().await.is_none());
        lower.control(TransportProtocol::Tcp).wait().await;
        // Closing TCP must not close the same protocol instance's UDP entry.
        udp_roundtrip(lower.as_ref(), target).await?;

        scope.close();
        scope.wait().await;
        daemon.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shadowsocks_udp_rejects_tampering_and_replay_before_routing() -> Result<()> {
    use bytes::BytesMut;
    use shadowsocks::{
        config::ServerType,
        context::Context,
        crypto::CipherKind,
        relay::{
            socks5::Address,
            udprelay::{
                crypto_io::{decrypt_server_payload, encrypt_client_payload},
                options::UdpSocketControlData,
            },
        },
    };
    tokio::time::timeout(LIMIT, async {
        let daemon = daemon().await?;
        let scope = Scope::new();
        let (_, echo) = echoes(&scope).await?;
        let remote = socket_addr(&address(&daemon, "shadowsocks2022"))?;
        let socket = UdpSocket::bind("127.0.0.1:0").await?;
        socket.connect(remote).await?;
        let method = CipherKind::AEAD2022_BLAKE3_AES_128_GCM;
        let config = shadowsocks::ServerConfig::new(remote, KEY, method)?;
        let context = Context::new(ServerType::Local);
        let mut ctrl = UdpSocketControlData::default();
        ctrl.client_session_id = 123456;
        let mut buffer = vec![0; 65536];

        for id in [1, 2] {
            ctrl.packet_id = id;
            let mut wire = BytesMut::new();
            encrypt_client_payload(
                &context,
                method,
                config.key(),
                &Address::SocketAddress(socket_addr(&echo)?),
                &ctrl,
                &[],
                &[id as u8],
                &mut wire,
            );
            if id == 1 {
                let mut corrupt = wire.to_vec();
                *corrupt.last_mut().unwrap() ^= 1;
                socket.send(&corrupt).await?;
            }
            socket.send(&wire).await?;
            let n = socket.recv(&mut buffer).await?;
            let (n, _, response) =
                decrypt_server_payload(&context, method, config.key(), &mut buffer[..n])?;
            assert_eq!(&buffer[..n], &[id as u8]);
            assert_eq!(response.unwrap().client_session_id, ctrl.client_session_id);
            if id == 1 {
                socket.send(&wire).await?;
            }
            // The next reply must be packet 2, not a second echo of replayed packet 1.
        }

        scope.close();
        scope.wait().await;
        daemon.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capability_checks_and_udp_policy_contract_reject_invalid_requests() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let scope = Scope::new();
        let http: Arc<dyn Carrier> = Arc::new(Clients::new(
            outbound("http", target("127.0.0.1:1".parse()?)),
            Arc::new(System::new(scope.clone())),
            Arc::new(SystemResolver),
        )?);
        let socks = Clients::new(
            outbound("socks5", target("127.0.0.1:1".parse()?)),
            http,
            Arc::new(SystemResolver),
        )?;

        assert!(socks.capabilities().tcp);
        assert!(!socks.capabilities().udp);
        // Fails before any connection attempt or fallback to system UDP.
        assert!(socks.udp(target("127.0.0.1:2".parse()?)).await.is_err());
        let source = r#"
            import { kotoconn as k } from '@kotoconn/bindings';
            const resolver = k.resolve_handler(name => k.lookup(name));
            const d = k.dialer({dialer: undefined, outbound: {resolve_handler: resolver, implementation: k.direct_outbound({})}});
            k.routing_handler(flow => k.route(d, flow.dest));
        "#;
        let daemon = Daemon::start_with_sources(
            "main.ts".into(),
            HashMap::from([("main.ts".into(), source.into())]),
            Duration::from_secs(1),
        )
        .await?;
        let handler = *daemon.policy().config().routing_handlers.iter().next().unwrap();
        let error = daemon
            .policy()
            .route(
                handler,
                Flow {
                    protocol: TransportProtocol::Udp,
                    dest: target("127.0.0.1:2".parse()?),
                },
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("route_udp"));
        daemon.shutdown().await?;
        scope.close();
        scope.wait().await;
        Ok::<_, anyhow::Error>(())
    }).await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_resolver_only_receives_outbound_server_names() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let daemon = daemon().await?;
        let scope = Scope::new();
        let (echo, _) = echoes(&scope).await?;

        struct Recording(tokio::sync::mpsc::UnboundedSender<String>);
        impl Resolver for Recording {
            fn resolve(
                &self,
                name: String,
            ) -> futures_util::future::BoxFuture<'_, Result<Vec<std::net::IpAddr>>> {
                Box::pin(async move {
                    self.0.send(name)?;
                    Ok(vec!["127.0.0.1".parse()?])
                })
            }
        }

        let (tx, mut names) = tokio::sync::mpsc::unbounded_channel();
        let endpoint = Target::Domain {
            name: "proxy.test".into(),
            port: address(&daemon, "socks5").port(),
        };
        let client = Clients::new(
            outbound("socks5", endpoint),
            Arc::new(System::new(scope.clone())),
            Arc::new(Recording(tx)),
        )?;
        tcp_roundtrip(&client, echo).await?;
        assert_eq!(names.recv().await.unwrap(), "proxy.test");
        assert!(names.try_recv().is_err());
        scope.close();
        scope.wait().await;
        daemon.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn socks_udp_discards_fragments_and_control_close_finishes_sessions() -> Result<()> {
    use fast_socks5::{Socks5Command, client::Socks5Stream, util::target_addr::TargetAddr};
    tokio::time::timeout(LIMIT, async {
        let daemon = daemon().await?;
        let scope = Scope::new();
        let (_, echo) = echoes(&scope).await?;
        let tcp = TcpStream::connect(socket_addr(&address(&daemon, "socks5"))?).await?;
        let mut control = Socks5Stream::use_stream(tcp, None, Default::default()).await?;
        let relay = control
            .request(
                Socks5Command::UDPAssociate,
                TargetAddr::Ip("0.0.0.0:0".parse()?),
            )
            .await?;
        let TargetAddr::Ip(relay) = relay else {
            anyhow::bail!("expected IP relay");
        };
        let socket = UdpSocket::bind("127.0.0.1:0").await?;
        socket.connect(relay).await?;

        let mut wire = fast_socks5::new_udp_header(socket_addr(&echo)?)?;
        wire.extend(b"discard");
        wire[2] = 1;
        socket.send(&wire).await?;
        wire[2] = 0;
        wire.truncate(wire.len() - b"discard".len());
        wire.extend(b"valid");
        socket.send(&wire).await?;
        let mut buffer = vec![0; 65536];
        let n = socket.recv(&mut buffer).await?;
        let (_, _, payload) = fast_socks5::parse_udp_request(&buffer[..n]).await?;
        assert_eq!(payload, b"valid");

        let session = daemon
            .sessions()
            .await?
            .into_iter()
            .find(|s| s.destination == echo)
            .unwrap();
        drop(control);
        session.wait().await;
        scope.close();
        scope.wait().await;
        daemon.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_deadline_reports_forced_network_cleanup() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let daemon = daemon().await?;
        let scope = Scope::new();
        let (echo, _) = echoes(&scope).await?;
        let client = Clients::new(
            outbound("http", address(&daemon, "http")),
            Arc::new(System::new(scope.clone())),
            Arc::new(SystemResolver),
        )?;
        let mut stream = client.tcp(echo).await?;

        let mut greeting = [0; 5];
        stream.read_exact(&mut greeting).await?;
        assert_eq!(
            daemon.shutdown().await?,
            kotoconn_daemon::Shutdown::TimedOut
        );
        assert_eq!(stream.read(&mut [0]).await?, 0);
        scope.close();
        scope.wait().await;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hysteria_shared_connection_keeps_siblings_alive_and_retires_when_unused() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct CountUdp {
        lower: System,
        calls: Arc<AtomicUsize>,
    }
    impl Carrier for CountUdp {
        fn capabilities(&self) -> Capabilities {
            Capabilities::BOTH
        }
        fn scope(&self) -> &Scope {
            self.lower.scope()
        }
        fn tcp_scoped(
            &self,
            target: Target,
            scope: Scope,
        ) -> futures_util::future::BoxFuture<'_, Result<BoxStream>> {
            self.lower.tcp_scoped(target, scope)
        }
        fn udp_scoped(
            &self,
            target: Target,
            scope: Scope,
        ) -> futures_util::future::BoxFuture<'_, Result<Datagram>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.lower.udp_scoped(target, scope)
        }
    }
    tokio::time::timeout(LIMIT, async {
        let daemon = daemon().await?;
        let scope = Scope::new();
        let (tcp, udp) = echoes(&scope).await?;
        let client_scope = Scope::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let client = Clients::new(
            outbound("hysteria2", address(&daemon, "hysteria2")),
            Arc::new(CountUdp {
                lower: System::new(client_scope.clone()),
                calls: calls.clone(),
            }),
            Arc::new(SystemResolver),
        )?;
        let mut stream = client.tcp(tcp.clone()).await?;
        let mut greeting = [0; 5];
        stream.read_exact(&mut greeting).await?;
        assert_eq!(&greeting, b"hello");
        let mut packets = client.udp(udp.clone()).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Closing TCP must leave a UDP lease on the same QUIC connection usable.
        client.control(TransportProtocol::Tcp).close();
        client.control(TransportProtocol::Tcp).wait().await;
        assert_eq!(stream.read(&mut [0]).await?, 0);
        drop(stream);
        packets
            .tx
            .send(Packet {
                target: udp.clone(),
                payload: vec![3; 8192].into(),
            })
            .await?;
        assert_eq!(packets.rx.recv().await.unwrap().payload, vec![3; 8192]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        drop(packets);
        // The configured client itself stays alive. No fixed idle timer is needed
        // to release its manager, QUIC drivers, and lower UDP carrier.
        client_scope.wait().await;
        udp_roundtrip(&client, udp).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        client_scope.wait().await;
        client_scope.close();
        scope.close();
        scope.wait().await;
        assert_eq!(daemon.shutdown().await?, kotoconn_daemon::Shutdown::Drained);
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hysteria_rejects_bad_credentials_certificates_and_tcp_only_carriers() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let daemon = daemon().await?;
        let scope = Scope::new();
        let server = address(&daemon, "hysteria2");
        let OutboundImpl::Hysteria2(options) = outbound("hysteria2", server.clone()) else {
            unreachable!()
        };
        let endpoint = Endpoint {
            address: server.clone(),
            resolver: Arc::new(SystemResolver),
        };
        let system: Arc<dyn Carrier> = Arc::new(System::new(scope.clone()));
        for failure in ["password", "name", "ca"] {
            let mut options = options.clone();
            match failure {
                "password" => options.password = "wrong".into(),
                "name" => options.server_name = Some("wrong.example".into()),
                "ca" => options.ca_certificate = None,
                _ => unreachable!(),
            }
            let client = kotoconn_outbounds::hysteria2::Client::new(
                endpoint.clone(),
                system.clone(),
                &options,
            )?;
            assert!(
                Client::tcp(&client, target("127.0.0.1:9".parse()?), scope.child())
                    .await
                    .is_err(),
                "{failure}"
            );
            drop(client);
        }
        let http: Arc<dyn Carrier> = Arc::new(Clients::new(
            outbound("http", server),
            system,
            Arc::new(SystemResolver),
        )?);
        assert!(kotoconn_outbounds::hysteria2::Client::new(endpoint, http, &options).is_err());
        scope.close();
        scope.wait().await;
        assert_eq!(daemon.shutdown().await?, kotoconn_daemon::Shutdown::Drained);
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hysteria_cancelled_setup_releases_the_carrier_without_an_idle_deadline() -> Result<()> {
    struct PendingCarrier {
        scope: Scope,
        started: tokio::sync::mpsc::Sender<()>,
    }
    impl Carrier for PendingCarrier {
        fn capabilities(&self) -> Capabilities {
            Capabilities::BOTH
        }
        fn scope(&self) -> &Scope {
            &self.scope
        }
        fn tcp_scoped(
            &self,
            _: Target,
            _: Scope,
        ) -> futures_util::future::BoxFuture<'_, Result<BoxStream>> {
            Box::pin(async { anyhow::bail!("unexpected TCP carrier request") })
        }
        fn udp_scoped(
            &self,
            _: Target,
            _: Scope,
        ) -> futures_util::future::BoxFuture<'_, Result<Datagram>> {
            Box::pin(async move {
                self.started.send(()).await?;
                std::future::pending().await
            })
        }
    }
    tokio::time::timeout(LIMIT, async {
        let root = Scope::new();
        let (started, mut ready) = tokio::sync::mpsc::channel(1);
        let server = target("127.0.0.1:1".parse()?);
        let OutboundImpl::Hysteria2(options) = outbound("hysteria2", server.clone()) else {
            unreachable!()
        };
        let client = Arc::new(kotoconn_outbounds::hysteria2::Client::new(
            Endpoint {
                address: server.clone(),
                resolver: Arc::new(SystemResolver),
            },
            Arc::new(PendingCarrier {
                scope: root.clone(),
                started,
            }),
            &options,
        )?);
        let caller = Scope::new();
        let task_client = client.clone();
        let task_scope = caller.clone();
        let task =
            tokio::spawn(
                async move { Client::tcp(task_client.as_ref(), server, task_scope).await },
            );
        ready.recv().await.unwrap();
        caller.close();
        assert!(task.await?.is_err());
        root.wait().await;
        drop(client);
        root.close();
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}
