use anyhow::Result;
use kotoconn_config::{InboundImpl, OutboundImpl, Socks5OutboundConfig};
use kotoconn_daemon::Daemon;
use kotoconn_outbounds::{Clients, System, SystemResolver};
use kotoconn_protocol::*;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
};

async fn start(source: String) -> Result<Daemon> {
    Ok(Daemon::start_with_sources(
        "main.ts".into(),
        HashMap::from([("main.ts".into(), source)]),
        Duration::from_secs(1),
    )
    .await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typescript_anytls_routes_tcp_and_udp_over_an_http_carrier() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
        let pem = certificate.cert.pem();
        let key = certificate.signing_key.serialize_pem();
        let server = start(format!(r#"
            import {{ kotoconn as k }} from '@kotoconn/bindings';
            const resolver = k.resolve_handler(name => k.lookup(name));
            const direct = k.dialer({{ outbound: {{resolve_handler: resolver, implementation: k.direct_outbound({{}})}} }});
            const routing = k.routing_handler(flow => flow.protocol === 'udp' ? k.route_udp(direct) : k.route(direct, flow.dest));
            const listen = {{address: k.ip('127.0.0.1'), port: 0}};
            for (const implementation of [
                k.http_inbound({{listen}}),
                k.anytls_inbound({{listen, password: 'secret', tls: {{certificate: {pem:?}, private_key: {key:?}}}, padding_scheme: 'stop=2\n0=32-32\n1=256-256'}}),
            ]) k.inbound({{implementation, routing_handler: routing, udp_idle_timeout: k.timeout(30000)}});
        "#)).await?;
        let addresses = server.listen_addresses();
        let inbound = |anytls| {
            let id = server.policy().config().inbounds.iter().find(|(_, config)| matches!(config.implementation, InboundImpl::AnyTls(_)) == anytls).unwrap().0;
            addresses[id]
        };
        let client = start(format!(r#"
            import {{ kotoconn as k }} from '@kotoconn/bindings';
            const resolver = k.resolve_handler(name => k.lookup(name));
            const lower = k.dialer({{outbound: {{resolve_handler: resolver, implementation: k.http_outbound({{server: k.ip_target(k.ip('127.0.0.1'), {})}})}}}});
            const anytls = k.dialer({{dialer: lower, outbound: {{resolve_handler: resolver, implementation: k.anytls_outbound({{
                server: k.ip_target(k.ip('127.0.0.1'), {}), password: 'secret',
                tls: {{server_name: 'localhost', certificate: {pem:?}}}, idle_session_timeout: k.timeout(60000),
            }})}}}});
            const routing = k.routing_handler(flow => flow.protocol === 'udp' ? k.route_udp(anytls) : k.route(anytls, flow.dest));
            k.inbound({{implementation: k.socks5_inbound({{listen: {{address: k.ip('127.0.0.1'), port: 0}}}}), routing_handler: routing, udp_idle_timeout: k.timeout(30000)}});
        "#, inbound(false).port(), inbound(true).port())).await?;

        let scope = Scope::new();
        let tcp = TcpListener::bind("127.0.0.1:0").await?;
        let udp = UdpSocket::bind("127.0.0.1:0").await?;
        let tcp_target = target(tcp.local_addr()?);
        let udp_target = target(udp.local_addr()?);
        scope.spawn(async move {
            let (mut stream, _) = tcp.accept().await?;
            stream.write_all(b"hello").await?;
            let (mut read, mut write) = stream.split();
            tokio::io::copy(&mut read, &mut write).await?;
            Ok(())
        })?;
        scope.spawn(async move {
            let mut bytes = vec![0; 65536];
            loop {
                let (n, peer) = udp.recv_from(&mut bytes).await?;
                udp.send_to(&bytes[..n], peer).await?;
            }
        })?;
        let carrier = Clients::new(
            OutboundImpl::Socks5(Socks5OutboundConfig { server: target(*client.listen_addresses().values().next().unwrap()) }),
            Arc::new(System::new(scope.clone())), Arc::new(SystemResolver),
        )?;
        let mut stream = carrier.tcp(tcp_target).await?;
        let mut greeting = [0; 5];
        stream.read_exact(&mut greeting).await?;
        assert_eq!(&greeting, b"hello");
        stream.write_all(b"through-http-anytls").await?;
        let mut reply = [0; 19];
        stream.read_exact(&mut reply).await?;
        assert_eq!(&reply, b"through-http-anytls");

        let mut association = carrier.udp(udp_target.clone()).await?;
        for payload in [vec![], vec![42; 8192]] {
            association.tx.send(Packet { target: udp_target.clone(), payload: payload.clone().into() }).await?;
            let reply = association.rx.recv().await.unwrap();
            assert_eq!(reply.payload, payload);
            assert_eq!(reply.target, udp_target);
        }

        drop(stream);
        drop(association);
        scope.close();
        scope.wait().await;
        assert_eq!(client.shutdown().await?, kotoconn_daemon::Shutdown::Drained);
        assert_eq!(server.shutdown().await?, kotoconn_daemon::Shutdown::Drained);
        Ok::<_, anyhow::Error>(())
    }).await?
}
