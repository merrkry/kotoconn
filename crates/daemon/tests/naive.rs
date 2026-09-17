#![cfg(target_os = "linux")]

use anyhow::Result;
use kotoconn_config::{OutboundImpl, Socks5OutboundConfig};
use kotoconn_daemon::{Daemon, Shutdown};
use kotoconn_outbounds::{Clients, System, SystemResolver};
use kotoconn_protocol::{Carrier, Scope, target};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn daemon(implementation: String, outbound: String) -> Result<Daemon> {
    let source = format!(
        r#"
        import {{ kotoconn as k }} from '@kotoconn/bindings';
        const resolver = k.resolve_handler(name => k.lookup(name));
        const dialer = k.dialer({{ dialer: null, outbound: {{ resolve_handler: resolver, implementation: {outbound} }} }});
        const routing = k.routing_handler(flow => k.route(dialer, flow.dest));
        const listen = {{ address: k.ip('127.0.0.1'), port: 0 }};
        k.inbound({{ implementation: {implementation}, routing_handler: routing, udp_idle_timeout: k.timeout(1000) }});
    "#
    );
    Ok(Daemon::start_with_sources(
        "main.ts".into(),
        HashMap::from([("main.ts".into(), source)]),
        Duration::from_secs(5),
    )
    .await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_drains_active_h2_streams_then_closes_idle_pools() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        let scope = Scope::new();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let destination = target(listener.local_addr()?);
        scope.spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            stream.write_all(b"ready").await?;
            let (mut reader, mut writer) = stream.split();
            tokio::io::copy(&mut reader, &mut writer).await?;
            writer.shutdown().await?;
            Ok(())
        })?;

        let key = rcgen::KeyPair::generate()?;
        let mut cert = rcgen::CertificateParams::new(vec!["proxy.test".into()])?;
        cert.not_before = (SystemTime::now() - Duration::from_secs(60)).into();
        cert.not_after = (SystemTime::now() + Duration::from_secs(86400)).into();
        let certificate = serde_json::to_string(&cert.self_signed(&key)?.pem())?;
        let private_key = serde_json::to_string(&key.serialize_pem())?;
        let server = daemon(
            format!("k.naive_inbound({{listen, username: 'user', password: 'secret', certificate: {certificate}, private_key: {private_key}}})"),
            "k.direct_outbound({})".into(),
        ).await?;
        let server_port = server.listen_addresses().values().next().unwrap().port();
        let client = daemon(
            "k.socks5_inbound({listen})".into(),
            format!("k.naive_outbound({{server: k.ip_target(k.ip('127.0.0.1'), {server_port}), server_name: 'proxy.test', username: 'user', password: 'secret', certificate: {certificate}}})"),
        ).await?;

        let entry = Clients::new(
            OutboundImpl::Socks5(Socks5OutboundConfig { server: target(*client.listen_addresses().values().next().unwrap()) }),
            Arc::new(System::new(scope.clone())), Arc::new(SystemResolver),
        )?;
        let mut stream = entry.tcp(destination).await?;
        let mut greeting = [0; 5];
        stream.read_exact(&mut greeting).await?;
        assert_eq!(&greeting, b"ready");
        server.stop();

        let traffic = async {
            stream.write_all(b"accepted before GOAWAY").await?;
            stream.shutdown().await?;
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await?;
            assert_eq!(response, b"accepted before GOAWAY");
            drop(stream);
            Ok::<_, anyhow::Error>(())
        };
        let ((), shutdown) = tokio::try_join!(traffic, async { Ok::<_, anyhow::Error>(server.wait().await?) })?;
        assert_eq!(shutdown, Shutdown::Drained);
        assert_eq!(client.shutdown().await?, Shutdown::Drained);
        scope.close();
        scope.wait().await;
        Ok::<_, anyhow::Error>(())
    }).await?
}
