use crate::{Server, stream, tls, wire};
use anyhow::Result;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use kotoconn_config::{Hysteria2InboundConfig, Hysteria2OutboundConfig};
use kotoconn_protocol::{self as p, BoundServer, Scope, Server as _, ServerContext, Target};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Handler(Arc<AtomicUsize>);

impl p::Handler for Handler {
    fn tcp(&self, _: Target, mut stream: p::BoxStream, _: Scope) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            stream.write_all(b"hello").await?;
            stream.shutdown().await?;
            Ok(())
        })
    }

    fn udp(&self, _: p::Datagram) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { anyhow::bail!("unexpected UDP session") })
    }
}

async fn auth(
    sender: &mut h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    uri: &str,
    password: &str,
) -> Result<http::Response<()>> {
    let mut request = sender
        .send_request(
            http::Request::post(uri)
                .header("Hysteria-Auth", password)
                .header("Hysteria-CC-RX", "0")
                .body(())?,
        )
        .await?;
    request.finish().await?;
    Ok(request.recv_response().await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http3_authentication_gates_proxying_and_stalled_streams_do_not_block_admission()
-> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
        let server = Server::new(&Hysteria2InboundConfig {
            listen: "127.0.0.1:0".parse()?,
            password: "secret".into(),
            certificate: certificate.cert.pem(),
            private_key: certificate.signing_key.serialize_pem(),
            obfs_password: None,
        })?;
        let scope = Scope::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let BoundServer { local_addr, run } = server
            .bind(
                "127.0.0.1:0".parse()?,
                ServerContext {
                    handler: Arc::new(Handler(calls.clone())),
                    scope: scope.clone(),
                    stopping: Default::default(),
                    udp_idle_timeout: Duration::from_secs(30),
                },
            )
            .await?;
        scope.spawn(run)?;
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse()?)?;
        endpoint.set_default_client_config(tls::client(&Hysteria2OutboundConfig {
            server: p::target(local_addr),
            password: "secret".into(),
            server_name: Some("localhost".into()),
            ca_certificate: Some(certificate.cert.pem()),
            obfs_password: None,
        })?);
        let connection = endpoint.connect(local_addr, "localhost")?.await?;
        let (mut driver, mut sender) =
            h3::client::new(h3_quinn::Connection::new(connection.clone())).await?;
        scope.spawn(async move {
            let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
            Ok(())
        })?;

        let mut unauthorized = stream::open(&connection).await?;
        wire::request(&mut unauthorized, &p::target(local_addr)).await?;
        assert!(wire::read_response(&mut unauthorized).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            auth(&mut sender, "https://hysteria/auth", "wrong")
                .await?
                .status(),
            404
        );
        assert_eq!(
            auth(&mut sender, "https://example.test/auth", "secret")
                .await?
                .status(),
            404
        );
        let response = auth(&mut sender, "https://hysteria/auth", "secret").await?;
        assert_eq!(response.status().as_u16(), 233);
        assert_eq!(response.headers()["Hysteria-CC-RX"], "auto");
        assert_eq!(response.headers()["Hysteria-UDP"], "true");

        // One partial frame type must not hold up another bidirectional stream.
        let mut stalled = stream::open(&connection).await?;
        stalled.write_all(&[0x40]).await?;
        let mut accepted = stream::open(&connection).await?;
        wire::request(&mut accepted, &p::target(local_addr)).await?;
        wire::read_response(&mut accepted).await?;
        let mut greeting = [0; 5];
        accepted.read_exact(&mut greeting).await?;
        assert_eq!(&greeting, b"hello");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // HTTP/3 continues handling requests after native TCP streams were used.
        assert_eq!(
            auth(&mut sender, "https://hysteria/unknown", "secret")
                .await?
                .status(),
            404
        );
        assert_eq!(
            auth(&mut sender, "https://hysteria/auth", "secret")
                .await?
                .status()
                .as_u16(),
            233
        );
        connection.close(0u32.into(), b"");
        scope.close();
        scope.wait().await;
        endpoint.wait_idle().await;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn early_auth_response_accepts_only_success_with_a_clean_request_stop() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
        for (status, code, accepted) in [
            (233, h3::error::Code::H3_NO_ERROR, true),
            (404, h3::error::Code::H3_NO_ERROR, false),
            (233, h3::error::Code::H3_REQUEST_REJECTED, false),
        ] {
            let scope = Scope::new();
            let server = quinn::Endpoint::server(
                tls::server(&Hysteria2InboundConfig {
                    listen: "127.0.0.1:0".parse()?,
                    password: "secret".into(),
                    certificate: certificate.cert.pem(),
                    private_key: certificate.signing_key.serialize_pem(),
                    obfs_password: None,
                })?,
                "127.0.0.1:0".parse()?,
            )?;
            let address = server.local_addr()?;
            let accepting = server.clone();
            scope.spawn(async move {
                let connection = accepting.accept().await.unwrap().await?;
                let mut http = h3::server::builder()
                    .build::<_, Bytes>(h3_quinn::Connection::new(connection.clone()))
                    .await?;
                let (_, mut response) = http.accept().await?.unwrap().resolve_request().await?;
                response.stop_sending(code);
                response
                    .send_response(http::Response::builder().status(status).body(())?)
                    .await?;
                response.finish().await?;
                connection.closed().await;
                Ok(())
            })?;

            let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse()?)?;
            endpoint.set_default_client_config(tls::client(&Hysteria2OutboundConfig {
                server: p::target(address),
                password: "secret".into(),
                server_name: Some("localhost".into()),
                ca_certificate: Some(certificate.cert.pem()),
                obfs_password: None,
            })?);
            let connection = endpoint.connect(address, "localhost")?.await?;
            let (mut driver, mut sender) =
                h3::client::new(h3_quinn::Connection::new(connection.clone())).await?;
            scope.spawn(async move {
                let _ = std::future::poll_fn(|cx| driver.poll_close(cx)).await;
                Ok(())
            })?;
            let auth = sender
                .send_request(http::Request::post("https://hysteria/auth").body(())?)
                .await?;

            // Observe receipt of STOP_SENDING before finishing the request.
            // This forces the race without relying on a delay or packet timing.
            while connection.stats().frame_rx.stop_sending == 0 {
                tokio::task::yield_now().await;
            }
            assert_eq!(
                crate::client::authentication_response(auth).await.is_ok(),
                accepted
            );

            connection.close(0u32.into(), b"");
            scope.close();
            scope.wait().await;
            server.close(0u32.into(), b"");
            endpoint.close(0u32.into(), b"");
            server.wait_idle().await;
            endpoint.wait_idle().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
