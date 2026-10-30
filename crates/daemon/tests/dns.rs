use anyhow::Result;
use hickory_proto::{
    op::{Edns, Message, Query, ResponseCode},
    rr::{
        Name, RData, Record, RecordType,
        rdata::{A, SOA, TXT, opt::EdnsOption},
    },
};
use kotoconn_daemon::{Daemon, Shutdown};
use std::{collections::HashMap, net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::mpsc,
};
use tokio_util::task::AbortOnDropHandle;

const LIMIT: Duration = Duration::from_secs(10);

struct Upstream {
    address: SocketAddr,
    requests: mpsc::UnboundedReceiver<Message>,
    _task: AbortOnDropHandle<()>,
}

fn answer(request: &Message) -> Message {
    let mut response = Message::response(request.metadata.id, request.metadata.op_code);
    response.metadata.recursion_available = true;
    response.metadata.recursion_desired = request.metadata.recursion_desired;
    response.metadata.checking_disabled = request.metadata.checking_disabled;
    response.queries = request.queries.clone();
    response.edns = request.edns.clone();
    let question = &request.queries[0];
    let name = question.name.to_utf8();
    if name == "missing.example." {
        response.metadata.response_code = ResponseCode::NXDomain;
        response.authorities.push(Record::from_rdata(
            Name::from_ascii("example.").unwrap(),
            60,
            RData::SOA(SOA::new(
                Name::from_ascii("ns.example.").unwrap(),
                Name::from_ascii("hostmaster.example.").unwrap(),
                1,
                60,
                60,
                60,
                30,
            )),
        ));
    } else if name == "large.example." {
        for _ in 0..10 {
            response.answers.push(Record::from_rdata(
                question.name.clone(),
                60,
                RData::TXT(TXT::new(vec!["x".repeat(100)])),
            ));
        }
    } else {
        let address = if request.metadata.checking_disabled {
            [192, 0, 2, 2]
        } else {
            [192, 0, 2, 1]
        };
        response.answers.push(Record::from_rdata(
            question.name.clone(),
            if name == "zero.example." { 0 } else { 60 },
            RData::A(A(address.into())),
        ));
    }
    response
}

async fn write_message(stream: &mut TcpStream, message: &Message) -> Result<()> {
    let wire = message.to_vec()?;
    stream.write_u16(u16::try_from(wire.len())?).await?;
    stream.write_all(&wire).await?;
    Ok(())
}

async fn read_message(stream: &mut TcpStream) -> Result<Message> {
    let length = stream.read_u16().await?;
    let mut wire = vec![0; usize::from(length)];
    stream.read_exact(&mut wire).await?;
    Ok(Message::from_vec(&wire)?)
}

async fn upstream(truncate_udp: bool) -> Result<Upstream> {
    let tcp = TcpListener::bind("127.0.0.1:0").await?;
    let address = tcp.local_addr()?;
    let udp = UdpSocket::bind(address).await?;
    let (sender, requests) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        let mut buffer = vec![0; 65535];
        loop {
            tokio::select! {
                accepted = tcp.accept() => {
                    let (mut stream, _) = accepted.unwrap();
                    let sender = sender.clone();
                    connections.spawn(async move {
                        while let Ok(request) = read_message(&mut stream).await {
                            sender.send(request.clone()).unwrap();
                            if request.queries[0].name.to_utf8() == "stall.example." {
                                std::future::pending::<()>().await;
                            }
                            if write_message(&mut stream, &answer(&request)).await.is_err() {
                                break;
                            }
                            if request.queries[0].name.to_utf8() == "close.example." {
                                stream.shutdown().await.unwrap();
                                break;
                            }
                        }
                    });
                }
                received = udp.recv_from(&mut buffer) => {
                    let (length, peer) = received.unwrap();
                    let request = Message::from_vec(&buffer[..length]).unwrap();
                    sender.send(request.clone()).unwrap();
                    let mut response = answer(&request);
                    if truncate_udp {
                        response.metadata.truncation = true;
                        response.answers.clear();
                    }
                    udp.send_to(&response.to_vec().unwrap(), peer).await.unwrap();
                }
                _ = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });
    Ok(Upstream {
        address,
        requests,
        _task: AbortOnDropHandle::new(task),
    })
}

async fn start(source: String) -> Result<Daemon> {
    Ok(Daemon::start_with_sources(
        "main.ts".into(),
        HashMap::from([("main.ts".into(), source)]),
        Duration::from_secs(1),
    )
    .await?)
}

fn ip_endpoint(address: SocketAddr) -> String {
    format!(
        "{{address: k.ip('{}'), port: {}}}",
        address.ip(),
        address.port()
    )
}

async fn gateway(upstream: SocketAddr) -> Result<Daemon> {
    start(format!(r#"
        import {{kotoconn as k}} from '@kotoconn/bindings';
        let calls = 0;
        const resolve = k.resolve_handler(() => [k.ip(`192.0.2.${{calls}}`)]);
        const direct = k.dialer({{carrier: k.host_network({{}}), outbound: {{resolve_handler: resolve, implementation: k.direct_outbound({{}})}}}});
        k.inbound({{implementation: k.socks5_inbound({{listen: {{address: k.ip('127.0.0.1'), port: 0}}}}), udp_idle_timeout: k.timeout(10000), routing_handler: k.routing_handler(flow => {{
            calls++;
            return flow.protocol === 'udp' ? k.route_udp(direct) : k.route(direct, k.ip_target(k.ip('{}'), {}));
        }})}});
    "#, upstream.ip(), upstream.port())).await
}

async fn gateway_calls(gateway: &Daemon) -> usize {
    let resolver = *gateway
        .policy()
        .config()
        .resolve_handlers
        .iter()
        .next()
        .unwrap();
    let address = gateway
        .policy()
        .resolve(resolver, "count".into())
        .await
        .unwrap()[0];
    let std::net::IpAddr::V4(address) = address else {
        panic!("IPv4 counter")
    };
    usize::from(address.octets()[3])
}

fn source(
    proxy: SocketAddr,
    server: SocketAddr,
    protocol: &str,
    cache: &str,
    timeout: u32,
) -> String {
    format!(
        r#"
        import {{kotoconn as k}} from '@kotoconn/bindings';
        import {{dns_service}} from '@kotoconn/api';
        let calls = 0;
        const resolve = k.resolve_handler(name => {{
            if (name === 'stats') return [k.ip(`192.0.2.${{calls}}`)];
            return k.lookup(name);
        }});
        const proxy = k.dialer({{carrier: k.host_network({{}}), outbound: {{resolve_handler: resolve, implementation: k.socks5_outbound({{server: k.ip_target(k.ip('{}'), {})}})}}}});
        const service = dns_service({{dialer: proxy, upstream: {{protocol: '{protocol}', server: {server}}}, {cache} timeout: k.timeout({timeout})}});
        const policy = k.dns_handler(async request => {{
            calls++;
            if (!request.inbound.equals(listener) || !request.source.address.is_loopback() || request.source.port === 0) throw Error('missing DNS context');
            if (request.query.class !== 1 || !['udp', 'tcp'].includes(request.protocol)) throw Error('missing DNS question');
            if (request.query.name === 'drop.example') return k.drop();
            if (request.query.name === 'error.example') throw Error('policy error');
            if (request.query.name === 'wrapped.example') return k.respond(await service.query(request));
            return service.handler(request);
        }});
        const listener = k.dns_listener({{listen: {{address: k.ip('127.0.0.1'), port: 0}}, protocols: ['udp', 'tcp'], dns_handler: policy}});
    "#,
        proxy.ip(),
        proxy.port(),
        server = ip_endpoint(server)
    )
}

fn listen(daemon: &Daemon) -> SocketAddr {
    *daemon.listen_addresses().values().next().unwrap()
}

fn query(name: &str, id: u16) -> Message {
    let mut message = Message::query();
    message.metadata.id = id;
    message.metadata.recursion_desired = true;
    message
        .queries
        .push(Query::query(Name::from_ascii(name).unwrap(), RecordType::A));
    message
}

async fn udp_query(address: SocketAddr, request: &Message) -> Result<Message> {
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    socket.send_to(&request.to_vec()?, address).await?;
    let mut buffer = vec![0; 65535];
    let (length, peer) = tokio::time::timeout(LIMIT, socket.recv_from(&mut buffer)).await??;
    assert_eq!(peer, address);
    Ok(Message::from_vec(&buffer[..length])?)
}

async fn tcp_query(address: SocketAddr, request: &Message) -> Result<Message> {
    let mut stream = TcpStream::connect(address).await?;
    write_message(&mut stream, request).await?;
    tokio::time::timeout(LIMIT, read_message(&mut stream)).await?
}

async fn seen(upstream: &mut Upstream) -> Message {
    tokio::time::timeout(LIMIT, upstream.requests.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn policy_runs_on_cache_hits_and_tcp_upstream_uses_the_selected_proxy() -> Result<()> {
    let mut upstream = upstream(false).await?;
    let gateway = gateway(upstream.address).await?;
    // Only the gateway translates this documentation address to the test peer.
    let daemon = start(source(
        listen(&gateway),
        "192.0.2.53:53".parse()?,
        "tcp",
        "",
        5000,
    ))
    .await?;
    let address = listen(&daemon);
    let first = udp_query(address, &query("Example.", 100)).await?;
    assert_eq!(first.metadata.id, 100);
    assert_eq!(first.answers[0].data, RData::A(A([192, 0, 2, 1].into())));
    assert_eq!(
        seen(&mut upstream).await.queries[0].name.to_utf8(),
        "Example."
    );

    let second = tcp_query(address, &query("Example.", 200)).await?;
    assert_eq!(second.metadata.id, 200);
    assert_eq!(second.answers, first.answers);
    assert!(upstream.requests.try_recv().is_err());

    let mut malformed = query("example.", 9);
    malformed.queries.clear();
    assert_eq!(
        udp_query(address, &malformed).await?.metadata.response_code,
        ResponseCode::FormErr
    );
    let mut transfer = query("example.", 10);
    transfer.queries[0].query_type = RecordType::AXFR;
    assert_eq!(
        udp_query(address, &transfer).await?.metadata.response_code,
        ResponseCode::NotImp
    );

    let mut flags = query("Example.", 300);
    flags.metadata.checking_disabled = true;
    flags.edns = Some(Edns::new());
    flags.edns.as_mut().unwrap().set_dnssec_ok(true);
    let response = udp_query(address, &flags).await?;
    assert_eq!(response.answers[0].data, RData::A(A([192, 0, 2, 2].into())));
    let forwarded = seen(&mut upstream).await;
    assert!(forwarded.metadata.checking_disabled);
    assert_eq!(forwarded.edns, flags.edns);

    flags.metadata.id = 301;
    flags.metadata.checking_disabled = false;
    let response = udp_query(address, &flags).await?;
    assert_eq!(response.answers[0].data, RData::A(A([192, 0, 2, 1].into())));
    let forwarded = seen(&mut upstream).await;
    assert!(!forwarded.metadata.checking_disabled);
    assert_eq!(forwarded.edns, flags.edns);

    for id in [400, 401] {
        let response = udp_query(address, &query("missing.example.", id)).await?;
        assert_eq!(response.metadata.response_code, ResponseCode::NXDomain);
        assert_eq!(response.metadata.id, id);
    }
    seen(&mut upstream).await;
    assert!(upstream.requests.try_recv().is_err());
    assert_eq!(
        gateway_calls(&gateway).await,
        1,
        "TCP connection should be reused"
    );

    let resolver = *daemon
        .policy()
        .config()
        .resolve_handlers
        .iter()
        .next()
        .unwrap();
    assert_eq!(
        daemon.policy().resolve(resolver, "stats".into()).await?[0].to_string(),
        "192.0.2.6"
    );
    assert_eq!(daemon.shutdown().await?, Shutdown::Drained);
    assert_eq!(gateway.shutdown().await?, Shutdown::Drained);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn udp_upstream_and_tcp_fallback_both_use_the_proxy() -> Result<()> {
    let mut upstream = upstream(true).await?;
    let gateway = gateway(upstream.address).await?;
    let daemon = start(source(
        listen(&gateway),
        upstream.address,
        "udp",
        "cache: false,",
        5000,
    ))
    .await?;
    let response = udp_query(listen(&daemon), &query("wrapped.example.", 123)).await?;
    assert_eq!(response.metadata.id, 123);
    assert!(!response.metadata.truncation);
    assert_eq!(response.answers.len(), 1);
    seen(&mut upstream).await;
    seen(&mut upstream).await;
    assert_eq!(gateway_calls(&gateway).await, 2);
    assert_eq!(daemon.shutdown().await?, Shutdown::Drained);
    gateway.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disabled_cache_zero_ttl_and_client_specific_edns_reach_upstream() -> Result<()> {
    let mut upstream = upstream(false).await?;
    let gateway = gateway(upstream.address).await?;
    for cache in ["cache: false,", "cache: {capacity: 32},"] {
        let daemon = start(source(
            listen(&gateway),
            "192.0.2.53:53".parse()?,
            "tcp",
            cache,
            5000,
        ))
        .await?;
        let address = listen(&daemon);
        let mut request = query(
            if cache.contains("false") {
                "example."
            } else {
                "zero.example."
            },
            1,
        );
        for id in [1, 2] {
            request.metadata.id = id;
            assert_eq!(udp_query(address, &request).await?.metadata.id, id);
            seen(&mut upstream).await;
        }
        if !cache.contains("false") {
            request = query("ecs.example.", 3);
            let mut edns = Edns::new();
            edns.options_mut()
                .insert(EdnsOption::Subnet("192.0.2.0/24".parse()?));
            request.edns = Some(edns);
            for id in [3, 4] {
                request.metadata.id = id;
                udp_query(address, &request).await?;
                assert_eq!(seen(&mut upstream).await.edns, request.edns);
            }
        }
        daemon.shutdown().await?;
    }
    gateway.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn policy_errors_drop_and_udp_size_limits_have_distinct_results() -> Result<()> {
    let mut upstream = upstream(false).await?;
    let gateway = gateway(upstream.address).await?;
    let daemon = start(source(
        listen(&gateway),
        "192.0.2.53:53".parse()?,
        "tcp",
        "",
        1000,
    ))
    .await?;
    let address = listen(&daemon);
    let error = udp_query(address, &query("error.example.", 1)).await?;
    assert_eq!(error.metadata.response_code, ResponseCode::ServFail);
    assert!(upstream.requests.try_recv().is_err());

    let stalled = udp_query(address, &query("stall.example.", 2)).await?;
    assert_eq!(stalled.metadata.response_code, ResponseCode::ServFail);
    seen(&mut upstream).await;

    let mut large = query("large.example.", 3);
    large.queries[0].query_type = RecordType::TXT;
    let truncated = udp_query(address, &large).await?;
    assert!(truncated.metadata.truncation);
    assert!(truncated.to_vec()?.len() <= 512);
    let full = tcp_query(address, &large).await?;
    assert!(!full.metadata.truncation);
    assert_eq!(full.answers.len(), 10);

    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    socket
        .send_to(&query("drop.example.", 4).to_vec()?, address)
        .await?;
    let mut buffer = [0; 512];
    assert!(
        tokio::time::timeout(Duration::from_millis(100), socket.recv_from(&mut buffer))
            .await
            .is_err()
    );
    assert_eq!(daemon.shutdown().await?, Shutdown::Drained);
    gateway.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dns_configuration_rejects_invalid_listeners_deadlines_and_dialer_capabilities() {
    let base = source(
        "127.0.0.1:1080".parse().unwrap(),
        "192.0.2.53:53".parse().unwrap(),
        "tcp",
        "",
        5000,
    );
    for (source, expected) in [
        (
            base.replace("protocols: ['udp', 'tcp']", "protocols: []"),
            "at least one protocol",
        ),
        (
            base.replace("protocols: ['udp', 'tcp']", "protocols: ['udp', 'udp']"),
            "duplicate DNS listener protocol",
        ),
        (
            base.replace("k.timeout(5000)", "k.timeout(0)"),
            "DNS timeout must be positive",
        ),
        (
            base.replace("protocol: 'tcp'", "protocol: 'udp'")
                .replace("k.socks5_outbound", "k.http_outbound"),
            "carrier lacks required capabilities",
        ),
    ] {
        let result = tokio::time::timeout(LIMIT, start(source)).await.unwrap();
        let error = match result {
            Ok(_) => panic!("accepted invalid DNS configuration"),
            Err(error) => error,
        };
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cached_tcp_connection_reconnects_after_the_upstream_closes_it() -> Result<()> {
    let mut upstream = upstream(false).await?;
    let gateway = gateway(upstream.address).await?;
    let daemon = start(source(
        listen(&gateway),
        "192.0.2.53:53".parse()?,
        "tcp",
        "cache: false,",
        5000,
    ))
    .await?;
    for id in [1, 2] {
        let response = udp_query(listen(&daemon), &query("close.example.", id)).await?;
        assert_eq!(response.metadata.response_code, ResponseCode::NoError);
        assert_eq!(response.metadata.id, id);
        seen(&mut upstream).await;
    }
    assert_eq!(gateway_calls(&gateway).await, 2);
    daemon.shutdown().await?;
    gateway.shutdown().await?;
    Ok(())
}
