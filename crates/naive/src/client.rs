use crate::{authority, authorization, native, padding, transport, valid_hostname};
use anyhow::{Result, bail, ensure};
use cronet::{Engine, EngineParams, Header};
use futures_util::future::BoxFuture;
use kotoconn_config::NaiveOutboundConfig;
use kotoconn_protocol::{
    self as p, BoxStream, Capabilities, Carrier, Datagram, Endpoint, Scope, Target,
};
use std::{
    net::IpAddr,
    sync::{Arc, Weak},
};
use tokio::sync::OnceCell;

pub struct Client {
    carrier: Arc<dyn Carrier>,
    endpoint: Endpoint,
    pool: Scope,
    engine: OnceCell<Weak<Engine>>,
    host: String,
    url: String,
    authorization: String,
    certificate: Option<String>,
    quic: bool,
}

impl Client {
    pub fn new(
        options: NaiveOutboundConfig,
        endpoint: Endpoint,
        carrier: Arc<dyn Carrier>,
        pool: Scope,
    ) -> Result<Self> {
        ensure!(
            options.server.port() != 0,
            "Naive proxy requires a nonzero port"
        );
        let host = options
            .server_name
            .unwrap_or_else(|| match &options.server {
                Target::Ip { address, .. } => address.to_string(),
                Target::Domain { name, .. } => name.clone(),
            });
        ensure!(
            valid_hostname(&host) || host.parse::<IpAddr>().is_ok(),
            "invalid Naive TLS server name"
        );
        if let Some(certificate) = &options.certificate {
            ensure!(
                !certificate.is_empty() && !certificate.contains('\0'),
                "invalid Naive PEM certificate"
            );
        }
        let url_host = match host.parse::<IpAddr>() {
            Ok(IpAddr::V6(_)) => format!("[{host}]"),
            _ => host.clone(),
        };
        let url = format!("https://{url_host}:{}", options.server.port());

        Ok(Self {
            carrier,
            endpoint,
            pool,
            engine: OnceCell::new(),
            host,
            url,
            authorization: authorization(&options.username, &options.password)?,
            certificate: options.certificate,
            quic: options.quic.unwrap_or(false),
        })
    }

    fn required(&self) -> Capabilities {
        Capabilities {
            tcp: !self.quic,
            udp: self.quic,
        }
    }

    async fn engine(&self, caller: &Scope) -> Result<Arc<Engine>> {
        let engine = self
            .engine
            .get_or_try_init(|| async {
                let mut hooks = transport::hooks(
                    self.carrier.clone(),
                    self.endpoint.clone(),
                    self.pool.clone(),
                    self.quic,
                    &self.host,
                );
                if let Some(certificate) = &self.certificate {
                    hooks = hooks.trusted_root_certificates(certificate.clone());
                }
                let host = self.host.clone();
                let quic = self.quic;
                let starting = self.pool.child().tracked_by(caller).track()?;
                let engine = Arc::new(
                    tokio::task::spawn_blocking(move || -> Result<Engine> {
                        let _starting = starting;
                        let mut params = EngineParams::new();
                        params
                            .enable_check_result(true)
                            .enable_http2(!quic)
                            .enable_quic(quic);
                        params.host_resolver_rules(&transport::resolver_rules(&host))?;
                        params.use_dns_https_svcb(false)?;
                        params.socket_pool_limits(2048, 2048, 2040)?;
                        if quic {
                            params.quic_options("", 6 * 1024 * 1024, 15 * 1024 * 1024)?;
                        } else {
                            params.http2_windows(128 * 1024 * 1024, 64 * 1024 * 1024)?;
                        }
                        Ok(Engine::start_with_network_hooks(&params, hooks)?)
                    })
                    .await??,
                );

                let pool = self.pool.clone();
                let guard = pool.track()?;
                let reference = Arc::downgrade(&engine);
                tokio::spawn(async move {
                    let _guard = guard;
                    pool.cancelled().await;
                    let _ =
                        tokio::task::spawn_blocking(move || engine.close_all_connections()).await;
                });
                Ok::<_, anyhow::Error>(reference)
            })
            .await?;
        engine
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("Naive client closed"))
    }
}

impl p::Client for Client {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            tcp: self.carrier.capabilities().require(self.required()).is_ok(),
            udp: false,
        }
    }

    fn tcp(&self, target: Target, scope: Scope) -> BoxFuture<'_, Result<BoxStream>> {
        Box::pin(async move {
            self.carrier.capabilities().require(self.required())?;
            ensure!(!self.pool.is_closed(), "Naive client closed");
            let destination = authority(&target)?;
            scope
                .run(async {
                    let engine = self.engine(&scope).await?;
                    let mut headers = vec![
                        Header {
                            name: "-connect-authority".into(),
                            value: destination,
                        },
                        Header {
                            name: "padding".into(),
                            value: padding::header(false),
                        },
                        Header {
                            name: "proxy-authorization".into(),
                            value: self.authorization.clone(),
                        },
                    ];
                    if self.quic {
                        headers.push(Header {
                            name: "-force-quic".into(),
                            value: "true".into(),
                        });
                    }
                    native::connect(engine, self.url.clone(), headers, scope.clone()).await
                })
                .await
        })
    }

    fn udp(&self, _: Target, _: Scope) -> BoxFuture<'_, Result<Datagram>> {
        Box::pin(async { bail!("Naive CONNECT does not support UDP payloads") })
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.pool.close();
    }
}
