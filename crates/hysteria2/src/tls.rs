use anyhow::{Result, ensure};
use kotoconn_config::{Hysteria2InboundConfig, Hysteria2OutboundConfig};
use quinn::{
    TransportConfig,
    congestion::BbrConfig,
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
};
use rustls::{
    RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use std::{sync::Arc, time::Duration};

pub const IO_TIMEOUT: Duration = Duration::from_secs(10);

pub fn transport() -> Arc<TransportConfig> {
    let mut config = TransportConfig::default();
    config.congestion_controller_factory(Arc::new(BbrConfig::default()));
    config.max_concurrent_bidi_streams(256u32.into());
    config.max_concurrent_uni_streams(16u32.into());
    config.stream_receive_window((8u32 * 1024 * 1024).into());
    config.receive_window((32u32 * 1024 * 1024).into());
    config.datagram_receive_buffer_size(Some(1024 * 1024));
    config.datagram_send_buffer_size(1024 * 1024);
    config.keep_alive_interval(Some(Duration::from_secs(10)));
    // SAFETY: 30,000 milliseconds is representable by QUIC's 62-bit idle timeout.
    config.max_idle_timeout(Some(
        Duration::from_secs(30)
            .try_into()
            .expect("QUIC idle timeout"),
    ));
    // Carrier chains need not expose an IP path MTU. Use QUIC's baseline size.
    config.mtu_discovery_config(None);
    Arc::new(config)
}

fn certificates(pem: &str) -> Result<Vec<CertificateDer<'static>>> {
    let chain = CertificateDer::pem_slice_iter(pem.as_bytes())
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(!chain.is_empty(), "empty PEM certificate chain");
    Ok(chain)
}

pub fn validate(password: &str, obfs: Option<&str>) -> Result<()> {
    ensure!(!password.is_empty(), "empty Hysteria password");
    http::HeaderValue::from_str(password)?;
    ensure!(
        obfs.is_none_or(|value| !value.is_empty()),
        "empty Salamander password"
    );
    Ok(())
}

pub fn client(options: &Hysteria2OutboundConfig) -> Result<quinn::ClientConfig> {
    validate(&options.password, options.obfs_password.as_deref())?;

    let mut roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(pem) = &options.ca_certificate {
        for certificate in certificates(pem)? {
            roots.add(certificate)?;
        }
    }

    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![b"h3".to_vec()];

    let mut config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls)?));
    config.transport_config(transport());
    Ok(config)
}

pub fn server(options: &Hysteria2InboundConfig) -> Result<quinn::ServerConfig> {
    validate(&options.password, options.obfs_password.as_deref())?;

    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .with_no_client_auth()
    .with_single_cert(
        certificates(&options.certificate)?,
        PrivateKeyDer::from_pem_slice(options.private_key.as_bytes())?,
    )?;
    tls.alpn_protocols = vec![b"h3".to_vec()];

    let mut config = quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls)?));
    config.transport_config(transport());
    Ok(config)
}
