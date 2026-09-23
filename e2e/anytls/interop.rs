use super::*;
use std::process::Stdio;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
};

fn peer() -> Command {
    let mut command = Command::new("anytls-go-peer");
    command.kill_on_drop(true).stderr(Stdio::inherit());
    command
}

fn certificates(fixture: &Fixture) -> Result<tempfile::TempDir> {
    let directory = tempfile::tempdir()?;
    std::fs::write(
        directory.path().join("cert.pem"),
        fixture.options.tls.certificate.as_ref().unwrap(),
    )?;
    std::fs::write(directory.path().join("key.pem"), &fixture.private_key)?;
    Ok(directory)
}

#[tokio::test]
async fn rust_client_interoperates_with_official_go_server() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut fixture = Fixture::new(None).await?;
        let certificates = certificates(&fixture)?;
        let mut server = peer()
            .arg("server")
            .arg(certificates.path().join("cert.pem"))
            .arg(certificates.path().join("key.pem"))
            .stdout(Stdio::piped())
            .spawn()?;
        let mut output = BufReader::new(server.stdout.take().unwrap()).lines();
        let address = output
            .next_line()
            .await?
            .expect("Go server address")
            .parse()?;
        fixture.options.server = target(address);
        fixture.options.tls.server_name = Some("localhost".into());
        let client = fixture.client()?;

        for _ in 0..3 {
            let mut stream = p::Client::tcp(
                &client,
                Target::Domain {
                    name: "unresolved.invalid".into(),
                    port: 80,
                },
                fixture.scope.child(),
            )
            .await?;
            let mut greeting = [0; 5];
            stream.read_exact(&mut greeting).await?;
            assert_eq!(&greeting, b"ready");
            let payload = vec![123; 262144];
            let mut reply = vec![0; payload.len()];
            let (mut reader, mut writer) = tokio::io::split(&mut stream);
            tokio::try_join!(
                async {
                    writer.write_all(&payload).await?;
                    writer.flush().await
                },
                reader.read_exact(&mut reply)
            )?;
            assert_eq!(reply, payload);
            stream.shutdown().await?;
        }
        assert_eq!(fixture.network.connections.load(Ordering::Relaxed), 1);

        // Keep one stream active to force a fresh authenticated TLS session using
        // the padding scheme the Go server supplied on the previous session.
        let mut active = p::Client::tcp(
            &client,
            target("127.0.0.1:80".parse()?),
            fixture.scope.child(),
        )
        .await?;
        active.read_exact(&mut [0; 5]).await?;
        for destination in [
            target("127.0.0.1:53".parse()?),
            target("[::1]:53".parse()?),
            Target::Domain {
                name: "dns.invalid".into(),
                port: 53,
            },
        ] {
            let mut packets =
                p::Client::udp(&client, destination.clone(), fixture.scope.child()).await?;
            for payload in [vec![], vec![42], vec![7; 65507]] {
                packets
                    .tx
                    .send(Packet {
                        target: destination.clone(),
                        payload: payload.clone().into(),
                    })
                    .await?;
                let reply = packets.rx.recv().await.unwrap();
                assert_eq!(reply.target, destination);
                assert_eq!(reply.payload, payload);
            }
        }
        assert!(
            p::Client::tcp(
                &client,
                target("127.0.0.1:1".parse()?),
                fixture.scope.child()
            )
            .await
            .is_err()
        );
        assert!(fixture.network.connections.load(Ordering::Relaxed) >= 2);
        drop(active);
        drop(client);
        server.kill().await?;
        server.wait().await?;
        fixture.close().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test]
async fn official_go_client_interoperates_with_rust_server() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        let fixture = Fixture::new(Some("stop=2\n0=40-40\n1=256-256")).await?;
        let certificates = certificates(&fixture)?;
        let output = peer()
            .arg("client")
            .arg(format!("127.0.0.1:{}", fixture.options.server.port()))
            .arg(certificates.path().join("cert.pem"))
            .output()
            .await?;
        assert!(
            output.status.success(),
            "Go peer failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(String::from_utf8(output.stdout)?.trim(), "ok");
        fixture.close().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
