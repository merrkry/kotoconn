use anyhow::{Result, bail};
use futures_util::future::BoxFuture;
use kotoconn_config::*;
use kotoconn_outbounds::{Clients, System, SystemResolver};
use kotoconn_protocol::*;
use std::{
    net::IpAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::io::AsyncReadExt;

const KEY: &str = "AAECAwQFBgcICQoLDA0ODw==";

/// Records each connection request and fails it without I/O.
struct Recording {
    scope: Scope,
    capabilities: Capabilities,
    requests: Mutex<Vec<Target>>,
}

impl Recording {
    fn new(domain_targets: bool) -> Arc<Self> {
        Arc::new(Self {
            scope: Scope::new(),
            capabilities: Capabilities {
                domain_targets,
                ..Capabilities::BOTH
            },
            requests: Mutex::new(Vec::new()),
        })
    }

    fn record(&self, target: Target) {
        self.requests.lock().unwrap().push(target);
    }
}

impl Carrier for Recording {
    fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    fn scope(&self) -> &Scope {
        &self.scope
    }

    fn tcp_scoped(&self, target: Target, _: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        self.record(target);
        Box::pin(async { bail!("recorded") })
    }

    fn udp_scoped(&self, target: Target, _: Scope) -> BoxFuture<'_, Result<Datagram>> {
        self.record(target);
        Box::pin(async { bail!("recorded") })
    }
}

struct Counting(AtomicUsize);

impl Resolver for Counting {
    fn resolve(&self, _: String) -> BoxFuture<'_, Result<Vec<IpAddr>>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(vec!["192.0.2.7".parse()?]) })
    }
}

/// Connection setup runs in the stream's own task, so a setup failure can
/// instead end the stream before its first byte.
async fn failed_tcp(client: &Clients, target: Target) {
    if let Ok(mut stream) = client.tcp(target).await {
        assert!(!matches!(stream.read(&mut [0]).await, Ok(1)));
    }
}

fn server() -> Target {
    Target::Domain {
        name: "proxy.test".into(),
        port: 443,
    }
}

fn outbounds() -> [OutboundImpl; 5] {
    [
        OutboundImpl::Http(HttpOutboundConfig { server: server() }),
        OutboundImpl::Socks5(Socks5OutboundConfig { server: server() }),
        OutboundImpl::Shadowsocks2022(Shadowsocks2022OutboundConfig {
            server: server(),
            password: KEY.into(),
        }),
        OutboundImpl::AnyTls(AnyTlsOutboundConfig {
            server: server(),
            password: KEY.into(),
            tls: TlsClientConfig {
                server_name: None,
                certificate: None,
            },
            idle_session_timeout: None,
        }),
        OutboundImpl::Hysteria2(Hysteria2OutboundConfig {
            server: server(),
            password: KEY.into(),
            server_name: None,
            ca_certificate: None,
            obfs_password: None,
        }),
    ]
}

#[tokio::test]
async fn server_names_are_resolved_only_for_carriers_that_require_ip_targets() {
    for domain_targets in [true, false] {
        for outbound in outbounds() {
            let name = format!("{outbound:?}");
            let carrier = Recording::new(domain_targets);
            let resolver = Arc::new(Counting(AtomicUsize::new(0)));
            let client = Clients::new(outbound, carrier.clone(), resolver.clone()).unwrap();
            assert!(client.capabilities().domain_targets, "{name}");

            let destination = target("198.51.100.1:80".parse().unwrap());
            failed_tcp(&client, destination).await;

            let expected = if domain_targets {
                server()
            } else {
                target("192.0.2.7:443".parse().unwrap())
            };
            assert_eq!(carrier.requests.lock().unwrap()[0], expected, "{name}");
            assert_eq!(
                resolver.0.load(Ordering::SeqCst),
                usize::from(!domain_targets),
                "{name}"
            );
        }
    }
}

#[tokio::test]
async fn domain_targets_pass_through_direct_only_when_its_carrier_accepts_them() {
    let destination = Target::Domain {
        name: "destination.test".into(),
        port: 80,
    };
    let direct = || OutboundImpl::Direct(DirectOutboundConfig {});

    let host = Clients::new(
        direct(),
        Arc::new(System::new(Scope::new(), Default::default()).unwrap()),
        Arc::new(SystemResolver),
    )
    .unwrap();
    assert!(!host.capabilities().domain_targets);
    for result in [
        host.tcp(destination.clone()).await.err(),
        host.udp(destination.clone()).await.err(),
    ] {
        let error = result.unwrap().to_string();
        assert!(
            error.contains("carrier lacks required capabilities"),
            "{error}"
        );
    }

    let carrier = Recording::new(false);
    let socks = Clients::new(
        OutboundImpl::Socks5(Socks5OutboundConfig {
            server: target("192.0.2.1:1080".parse().unwrap()),
        }),
        carrier.clone(),
        Arc::new(SystemResolver),
    )
    .unwrap();
    let proxied = Clients::new(direct(), Arc::new(socks), Arc::new(SystemResolver)).unwrap();
    assert!(proxied.capabilities().domain_targets);
    failed_tcp(&proxied, destination).await;
    assert_eq!(
        *carrier.requests.lock().unwrap(),
        [target("192.0.2.1:1080".parse().unwrap())]
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn host_network_options_apply_to_every_socket_kind() {
    let destination = target("127.0.0.1:9".parse().unwrap());
    let missing = System::new(
        Scope::new(),
        HostNetworkConfig {
            mark: None,
            interface: Some("kotoconn-none".into()),
        },
    )
    .unwrap();

    // Binding to a missing interface fails, so each path must have applied it.
    let errors = [
        missing.tcp(destination.clone()).await.err(),
        missing.udp(destination.clone()).await.err(),
        missing
            .udp_native_scoped(destination, Scope::new())
            .await
            .err(),
    ];
    for error in errors {
        let error = error.unwrap();
        let error = error.downcast_ref::<std::io::Error>().unwrap();
        assert_eq!(error.raw_os_error(), Some(libc::ENODEV), "{error}");
    }

    for interface in ["", "sixteen-bytes-xx", "nul\0"] {
        let options = HostNetworkConfig {
            mark: None,
            interface: Some(interface.into()),
        };
        assert!(System::new(Scope::new(), options).is_err(), "{interface:?}");
    }
}

// A carrier can be another open dialer, so rejected options must leave no work on it.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn rejected_options_leave_no_work_on_the_carrier() {
    let scope = Scope::new();
    let result = Clients::new(
        OutboundImpl::Naive(NaiveOutboundConfig {
            server: target("127.0.0.1:443".parse().unwrap()),
            username: String::new(),
            password: "password".into(),
            server_name: None,
            certificate: None,
            quic: None,
        }),
        Arc::new(System::new(scope.clone(), Default::default()).unwrap()),
        Arc::new(SystemResolver),
    );
    assert!(result.is_err());

    let released = tokio::select! {
        biased;
        _ = scope.wait() => true,
        _ = std::future::ready(()) => false,
    };
    assert!(released, "rejected options left work on the carrier");
}
