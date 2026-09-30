use anyhow::Result;
use futures_util::future::BoxFuture;
use kotoconn_config::HttpInboundConfig;
use kotoconn_outbounds::{Clients, System, SystemResolver};
use kotoconn_protocol::*;
use std::{sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

struct Inspect(tokio::sync::mpsc::UnboundedSender<Target>);

impl Handler for Inspect {
    fn tcp(
        &self,
        destination: Target,
        mut stream: BoxStream,
        _: Scope,
    ) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            // Reads client payload before any routing or outbound establishment.
            let mut prefix = [0; 5];
            stream.read_exact(&mut prefix).await?;
            assert_eq!(&prefix, b"sniff");
            self.0.send(destination)?;
            stream.write_all(&prefix).await?;
            Ok(())
        })
    }

    fn udp(&self, _: Datagram) -> BoxFuture<'_, Result<()>> {
        unreachable!()
    }
}

#[tokio::test]
async fn inbound_admission_allows_inspection_and_preserves_domain_target() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let scope = Scope::new();
        let stopping = CancellationToken::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let bound = kotoconn_inbounds::bind(
            kotoconn_config::InboundImpl::Http(HttpInboundConfig {
                listen: "127.0.0.1:0".parse()?,
            }),
            ServerContext {
                handler: Arc::new(Inspect(tx)),
                scope: scope.clone(),
                stopping,
                udp_idle_timeout: Duration::from_secs(30),
            },
        )
        .await?;
        let kotoconn_inbounds::InboundAddress::Socket(address) = bound.address else {
            panic!("HTTP must bind a socket");
        };
        let address = target(address);
        scope.spawn(bound.run)?;

        let client = Clients::new(
            kotoconn_config::OutboundImpl::Http(kotoconn_config::HttpOutboundConfig {
                server: address,
            }),
            Arc::new(System::new(scope.clone())),
            Arc::new(SystemResolver),
        )?;
        let destination = Target::Domain {
            name: "unresolved.invalid".into(),
            port: 443,
        };
        let mut stream = client.tcp(destination.clone()).await?;
        stream.write_all(b"sniff").await?;
        let mut echoed = [0; 5];
        stream.read_exact(&mut echoed).await?;
        assert_eq!(&echoed, b"sniff");

        assert_eq!(rx.recv().await.unwrap(), destination);
        scope.close();
        scope.wait().await;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sniff_metadata_reaches_policy_and_tcp_routing_preserves_payload() -> Result<()> {
    use kotoconn_daemon::Daemon;
    use std::collections::HashMap;
    use tokio::net::TcpListener;

    tokio::time::timeout(Duration::from_secs(10), async {
        let echo = TcpListener::bind("127.0.0.1:0").await?;
        let port = echo.local_addr()?.port();
        let payload = b"GET / HTTP/1.1\r\nHost: sniff.example:8080\r\n\r\nbody";
        let echoing = tokio::spawn(async move {
            let (mut stream, _) = echo.accept().await?;
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await?;
            assert_eq!(received, payload);
            stream.write_all(&received).await?;
            stream.shutdown().await?;
            Ok::<_, anyhow::Error>(())
        });

        let source = format!(r#"
            import {{ kotoconn as k }} from '@kotoconn/bindings';
            const resolve = k.resolve_handler(() => []);
            const direct = k.dialer({{ dialer: null, outbound: {{
                resolve_handler: resolve, implementation: k.direct_outbound({{}})
            }} }});
            const routing = k.routing_handler(flow => {{
                if (flow.dest.domain !== 'original.invalid' || flow.dest.port !== 443) {{
                    throw Error('sniff changed the original destination');
                }}
                if (flow.sniff?.protocol !== 'http' || flow.sniff.domain !== 'sniff.example') {{
                    throw Error('missing sniff metadata');
                }}
                return k.route(direct, k.ip_target(k.ip('127.0.0.1'), {port}));
            }});
            k.inbound({{
                routing_handler: routing, udp_idle_timeout: k.timeout(10000),
                sniff: {{ timeout: k.timeout(300) }},
                implementation: k.http_inbound({{ listen: {{ address: k.ip('127.0.0.1'), port: 0 }} }})
            }});
        "#);
        let daemon = Daemon::start_with_sources(
            "main.ts".into(), HashMap::from([("main.ts".into(), source)]), Duration::from_secs(1),
        ).await?;
        let scope = Scope::new();
        let address = *daemon.listen_addresses().values().next().unwrap();
        let client = Clients::new(
            kotoconn_config::OutboundImpl::Http(kotoconn_config::HttpOutboundConfig { server: target(address) }),
            Arc::new(System::new(scope.clone())), Arc::new(SystemResolver),
        )?;
        let mut stream = client.tcp(Target::Domain { name: "original.invalid".into(), port: 443 }).await?;
        stream.write_all(payload).await?;
        stream.shutdown().await?;
        let mut echoed = Vec::new();
        stream.read_to_end(&mut echoed).await?;
        assert_eq!(echoed, payload);
        echoing.await??;

        drop(stream);
        daemon.shutdown().await?;
        scope.close();
        scope.wait().await;
        Ok::<_, anyhow::Error>(())
    }).await??;
    Ok(())
}
