//! Adapt the configured carrier to Cronet-owned file descriptors. Hooks return
//! immediately; policy resolution and carrier I/O run on the Tokio executor.
use anyhow::{Result, ensure};
use cronet::{NetworkHooks, UdpDialResult};
use kotoconn_protocol::{Carrier, Endpoint, Packet, Scope};
use std::{
    io,
    net::{TcpListener, TcpStream, UdpSocket},
    os::fd::IntoRawFd,
    sync::Arc,
};
use tokio::runtime::Handle;

const ADDRESS: &str = "192.0.2.1";

pub(crate) fn resolver_rules(host: &str) -> String {
    format!("MAP {host} {ADDRESS}, MAP * ~NOTFOUND")
}

pub(crate) fn hooks(
    carrier: Arc<dyn Carrier>,
    endpoint: Endpoint,
    scope: Scope,
    quic: bool,
    host: &str,
) -> NetworkHooks {
    let runtime = Handle::current();
    let port = endpoint.address.port();
    let allowed_ip = host
        .parse::<std::net::IpAddr>()
        .ok()
        .map(|ip| ip.to_string());
    let allowed = move |address: &str, requested_port| {
        requested_port == port && (address == ADDRESS || allowed_ip.as_deref() == Some(address))
    };
    let tcp_carrier = carrier.clone();
    let tcp_endpoint = endpoint.clone();
    let tcp_scope = scope.clone();
    let tcp_runtime = runtime.clone();
    let tcp_allowed = allowed.clone();

    NetworkHooks::default().tcp_dialer(move |address, port| {
        let result = (|| -> Result<i32> {
            ensure!(!quic && tcp_allowed(address, port), "unexpected Cronet TCP destination");
            // Real TCP sockets support Chromium's socket options. Both ends
            // remain on loopback; only the carrier can reach the proxy server.
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let native = TcpStream::connect(listener.local_addr()?)?;
            let (local, peer) = listener.accept()?;
            ensure!(peer == native.local_addr()?, "unexpected Cronet bridge peer");
            native.set_nodelay(true)?;
            local.set_nodelay(true)?;
            local.set_nonblocking(true)?;
            let _enter = tcp_runtime.enter();
            let mut local = tokio::net::TcpStream::from_std(local)?;
            let carrier = tcp_carrier.clone();
            let endpoint = tcp_endpoint.clone();
            let connection = tcp_scope.child();
            let work_scope = connection.clone();

            connection.spawn(async move {
                let mut remote = carrier.tcp_scoped(endpoint.resolve().await?, work_scope).await?;
                tokio::io::copy_bidirectional_with_sizes(&mut local, &mut remote, 65536, 65536).await?;
                Ok(())
            })?;
            // Cronet now owns this descriptor; the relay owns its other end.
            Ok(native.into_raw_fd())
        })();
        match result {
            Ok(fd) => fd,
            Err(error) => {
                tracing::debug!(%error, "Cronet carrier TCP setup failed");
                -104 // Chromium ERR_CONNECTION_FAILED.
            }
        }
    }).udp_dialer(move |address, port| {
        let result = (|| -> Result<UdpDialResult> {
            ensure!(quic && allowed(address, port), "unexpected Cronet UDP destination");
            let native = UdpSocket::bind("127.0.0.1:0")?;
            let local = UdpSocket::bind("127.0.0.1:0")?;
            native.connect(local.local_addr()?)?;
            local.connect(native.local_addr()?)?;
            local.set_nonblocking(true)?;
            let local_address = native.local_addr()?;
            let _enter = runtime.enter();
            let local = tokio::net::UdpSocket::from_std(local)?;
            let carrier = carrier.clone();
            let endpoint = endpoint.clone();
            let connection = scope.child();
            let work_scope = connection.clone();

            connection.spawn(async move {
                let target = endpoint.resolve().await?;
                let mut remote = carrier.udp_scoped(target.clone(), work_scope).await?;
                let mut buffer = vec![0; 65536];
                loop {
                    tokio::select! {
                        result = local.recv(&mut buffer) => {
                            let n = result?;
                            remote.tx.send(Packet { target: target.clone(), payload: buffer[..n].to_vec().into() }).await?;
                        }
                        packet = remote.rx.recv() => {
                            let Some(packet) = packet else { return Ok(()); };
                            if packet.target == target {
                                let n = local.send(&packet.payload).await?;
                                if n != packet.payload.len() {
                                    return Err(io::Error::from(io::ErrorKind::WriteZero).into());
                                }
                            }
                        }
                    }
                }
            })?;
            Ok(UdpDialResult {
                fd: native.into_raw_fd(),
                local_address: local_address.ip().to_string(),
                local_port: local_address.port(),
            })
        })();
        result.unwrap_or_else(|error| {
            tracing::debug!(%error, "Cronet carrier UDP setup failed");
            UdpDialResult { fd: -104, local_address: String::new(), local_port: 0 }
        })
    })
}
