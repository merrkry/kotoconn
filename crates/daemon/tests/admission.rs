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
        let (address, server) =
            kotoconn_inbounds::build(kotoconn_config::InboundImpl::Http(HttpInboundConfig {
                listen: "127.0.0.1:0".parse()?,
            }))?;
        let bound = server
            .bind(
                address,
                ServerContext {
                    handler: Arc::new(Inspect(tx)),
                    scope: scope.clone(),
                    stopping,
                    udp_idle_timeout: Duration::from_secs(30),
                },
            )
            .await?;
        let address = target(bound.local_addr);
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
