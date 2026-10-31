use anyhow::Result;
use kotoconn_config::*;
use kotoconn_daemon::{Daemon, Shutdown};
use kotoconn_outbounds::{Clients, System, SystemResolver};
use kotoconn_protocol::*;
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const LIMIT: Duration = Duration::from_secs(20);

async fn start(source: String) -> Result<Daemon> {
    Ok(Daemon::start_with_sources(
        "main.ts".into(),
        HashMap::from([("main.ts".into(), source)]),
        LIMIT,
    )
    .await?)
}

/// The resolve handler of the first dialer, which the policies use as a command hook.
fn hook(daemon: &Daemon) -> ResolveHandler {
    daemon
        .resources()
        .into_iter()
        .find_map(|(_, config)| match config {
            ResourceConfig::Dialer(dialer) => Some(dialer.outbound.resolve_handler),
            _ => None,
        })
        .unwrap()
}

async fn command(daemon: &Daemon, name: &str) -> Result<(), kotoconn_daemon::Error> {
    daemon
        .policy()
        .resolve(&hook(daemon), name.into())
        .await
        .map(drop)
}

/// A TCP server that greets each client and closes.
async fn greeter(scope: &Scope) -> Result<Target> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = target(listener.local_addr()?);
    scope.spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await?;
            stream.write_all(b"hello").await?;
            stream.shutdown().await?;
        }
    })?;
    Ok(address)
}

/// Whether a SOCKS5 inbound at `proxy` relays a connection to `destination`.
async fn relays(proxy: SocketAddr, destination: &Target) -> Result<bool> {
    let client = Clients::new(
        OutboundImpl::Socks5(Socks5OutboundConfig {
            server: target(proxy),
        }),
        Arc::new(System::new(Scope::new(), Default::default())?),
        Arc::new(SystemResolver),
    )?;

    // A failed route ends the stream before its first byte.
    let mut greeting = Vec::new();
    Ok(match client.tcp(destination.clone()).await {
        Ok(mut stream) => {
            let _ = stream.read_to_end(&mut greeting).await;
            greeting == b"hello"
        }
        Err(_) => false,
    })
}

fn listen_address(daemon: &Daemon) -> SocketAddr {
    *daemon.listen_addresses().values().next().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn policy_creates_and_closes_resources_while_running() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let scope = Scope::new();
        let destination = greeter(&scope).await?;
        let daemon = start(
            r#"
            import { kotoconn as k } from '@kotoconn/bindings';
            const direct = k.direct_outbound({});
            let carrier, upper, inbound, service;
            const hook = async name => {
                switch (name) {
                    case 'create':
                        carrier = await k.dialer({carrier: k.host_network({}), outbound: {resolve_handler: hook, implementation: direct}});
                        upper = await k.dialer({carrier, outbound: {resolve_handler: hook, implementation: direct}});
                        service = await k.dns_service({
                            dialer: carrier, cache_capacity: 0, timeout: k.timeout(1000),
                            upstream: {protocol: 'udp', server: {address: k.ip('127.0.0.1'), port: 53}},
                        });
                        inbound = await k.inbound({
                            implementation: k.socks5_inbound({listen: {address: k.ip('127.0.0.1'), port: 0}}),
                            routing_handler: flow => k.route(upper, flow.dest),
                            udp_idle_timeout: k.timeout(1000),
                        });
                        break;
                    case 'close carrier': carrier.close(); break;
                    case 'close upper': upper.close(); await upper.wait(); break;
                    case 'close service': service.drain({timeout: k.timeout(60000)}); await service.wait(); break;
                    // References to the closed dialer never reach a later one.
                    case 'replace upper':
                        await k.dialer({carrier: k.host_network({}), outbound: {resolve_handler: hook, implementation: direct}});
                        break;
                    case 'reuse carrier':
                        await k.dialer({carrier, outbound: {resolve_handler: hook, implementation: direct}});
                        break;
                    case 'drain inbound': inbound.drain({timeout: k.timeout(60000)}); await inbound.wait(); break;
                }
                return [];
            };
            await k.dialer({carrier: k.host_network({}), outbound: {resolve_handler: hook, implementation: direct}});
        "#
            .into(),
        )
        .await?;
        assert!(daemon.listen_addresses().is_empty());

        command(&daemon, "create").await?;
        assert_eq!(daemon.resources().len(), 5);
        let proxy = listen_address(&daemon);
        assert!(relays(proxy, &destination).await?);

        let error = command(&daemon, "close carrier").await.unwrap_err();
        assert!(error.to_string().contains("used by 2 open resources"), "{error}");
        command(&daemon, "close upper").await?;
        assert!(!relays(proxy, &destination).await?);
        command(&daemon, "close service").await?;
        command(&daemon, "close carrier").await?;
        let error = command(&daemon, "reuse carrier").await.unwrap_err();
        assert!(error.to_string().contains("referenced resource is closed"), "{error}");
        command(&daemon, "replace upper").await?;
        assert!(!relays(proxy, &destination).await?);

        command(&daemon, "drain inbound").await?;
        assert!(daemon.listen_addresses().is_empty());
        assert_eq!(daemon.resources().len(), 2);
        assert!(tokio::net::TcpStream::connect(proxy).await.is_err());

        assert_eq!(daemon.shutdown().await?, Shutdown::Drained);
        scope.close();
        scope.wait().await;
        Ok(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dialer_drain_keeps_accepted_sessions_until_they_finish() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let echo = TcpListener::bind("127.0.0.1:0").await?;
        let destination = target(echo.local_addr()?);
        let daemon = start(
            r#"
            import { kotoconn as k } from '@kotoconn/bindings';
            const direct = k.direct_outbound({});
            const hook = async name => {
                if (name === 'drain') dialer.drain({timeout: k.timeout(60000)});
                if (name === 'wait') await dialer.wait();
                return [];
            };
            const dialer = await k.dialer({carrier: k.host_network({}), outbound: {resolve_handler: hook, implementation: direct}});
            await k.inbound({
                implementation: k.socks5_inbound({listen: {address: k.ip('127.0.0.1'), port: 0}}),
                routing_handler: flow => k.route(dialer, flow.dest),
                udp_idle_timeout: k.timeout(1000),
            });
        "#
            .into(),
        )
        .await?;
        let proxy = listen_address(&daemon);

        let client = Clients::new(
            OutboundImpl::Socks5(Socks5OutboundConfig {
                server: target(proxy),
            }),
            Arc::new(System::new(Scope::new(), Default::default())?),
            Arc::new(SystemResolver),
        )?;
        let mut session = client.tcp(destination.clone()).await?;
        let (mut accepted, _) = echo.accept().await?;
        session.write_all(b"before").await?;
        let mut received = [0; 6];
        accepted.read_exact(&mut received).await?;

        command(&daemon, "drain").await?;
        assert!(!relays(proxy, &destination).await?);

        // The release waits for the accepted session, which still forwards traffic.
        let released = command(&daemon, "wait");
        tokio::pin!(released);
        let traffic = async {
            accepted.write_all(b"during").await?;
            session.read_exact(&mut received).await?;
            assert_eq!(&received, b"during");
            Ok::<_, anyhow::Error>(())
        };
        tokio::select! {
            result = &mut released => panic!("drain ended with an active session: {result:?}"),
            result = traffic => result?,
        }

        drop(session);
        drop(accepted);
        released.await?;
        assert_eq!(daemon.shutdown().await?, Shutdown::Drained);
        Ok(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_failure_releases_the_resources_that_evaluation_created() -> Result<()> {
    let port = TcpListener::bind("127.0.0.1:0").await?.local_addr()?.port();
    let source = format!(
        r#"
        import {{ kotoconn as k }} from '@kotoconn/bindings';
        const dialer = await k.dialer({{carrier: k.host_network({{}}), outbound: {{
            resolve_handler: () => [], implementation: k.direct_outbound({{}}),
        }}}});
        await k.inbound({{
            implementation: k.socks5_inbound({{listen: {{address: k.ip('127.0.0.1'), port: {port}}}}}),
            routing_handler: flow => k.route(dialer, flow.dest),
            udp_idle_timeout: k.timeout(1000),
        }});
        throw Error('later failure');
    "#
    );

    let error = tokio::time::timeout(LIMIT, start(source))
        .await?
        .err()
        .unwrap();
    assert!(error.to_string().contains("later failure"), "{error}");
    TcpListener::bind(("127.0.0.1", port)).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dns_listener_drain_answers_accepted_queries_and_admits_no_new_ones() -> Result<()> {
    tokio::time::timeout(LIMIT, async {
        let daemon = start(
            r#"
            import { kotoconn as k } from '@kotoconn/bindings';
            let calls = 0, markEntered, release;
            const entered = new Promise(resolve => markEntered = resolve);
            const gate = new Promise(resolve => release = resolve);
            let listener;
            const hook = async name => {
                if (name === 'entered') await entered;
                if (name === 'drain') listener.drain({timeout: k.timeout(60000)});
                if (name === 'release') release();
                if (name === 'wait') await listener.wait();
                if (name === 'calls' && calls !== 1) throw Error(`${calls} DNS handler calls`);
                return [];
            };
            await k.dialer({carrier: k.host_network({}), outbound: {
                resolve_handler: hook, implementation: k.direct_outbound({}),
            }});
            listener = await k.dns_listener({
                listen: {address: k.ip('127.0.0.1'), port: 0}, protocols: ['udp'],
                dns_handler: async request => {
                    calls++;
                    markEntered();
                    await gate;
                    const bytes = k.request_bytes(request);
                    bytes[2] |= 0x80;
                    return k.respond(k.dns_response(bytes));
                },
            });
        "#
            .into(),
        )
        .await?;
        let listener = listen_address(&daemon);
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
        let query = |id: u8| {
            [
                0x12, id, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 4, b't', b'e', b's', b't', 0, 0, 1, 0, 1,
            ]
        };

        socket.send_to(&query(1), listener).await?;
        command(&daemon, "entered").await?;
        command(&daemon, "drain").await?;
        socket.send_to(&query(2), listener).await?;

        // The accepted query is answered after the drain started.
        command(&daemon, "release").await?;
        let mut response = [0; 512];
        let received = socket.recv_from(&mut response).await?.0;
        let mut answered = query(1);
        answered[2] |= 0x80;
        assert_eq!(response[..received], answered);

        command(&daemon, "wait").await?;
        command(&daemon, "calls").await?;
        assert!(daemon.listen_addresses().is_empty());
        assert_eq!(daemon.shutdown().await?, Shutdown::Drained);
        Ok(())
    })
    .await?
}
