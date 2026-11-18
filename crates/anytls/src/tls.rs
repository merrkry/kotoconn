use anyhow::{Context, Result};
use kotoconn_config::{Target, TlsClientConfig, TlsServerConfig};
use kotoconn_protocol::pem;
use rustls::{ClientConfig, RootCertStore, ServerConfig, pki_types::ServerName};
use std::sync::Arc;
use tokio_rustls::{TlsAcceptor, TlsConnector};

pub(crate) fn client(
    options: &TlsClientConfig,
    server: &Target,
) -> Result<(TlsConnector, ServerName<'static>)> {
    let name = options.server_name.clone().unwrap_or_else(|| match server {
        Target::Ip { address, .. } => address.to_string(),
        Target::Domain { name, .. } => name.clone(),
    });
    let name = ServerName::try_from(name).context("invalid TLS server name")?;
    let mut roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(certificate) = &options.certificate {
        let certificates = pem::certificates(certificate)?;
        for certificate in certificates {
            roots.add(certificate).context("invalid TLS trust anchor")?;
        }
    }

    let config = ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();

    Ok((TlsConnector::from(Arc::new(config)), name))
}

pub(crate) fn server(options: &TlsServerConfig) -> Result<TlsAcceptor> {
    let certificates = pem::certificates(&options.certificate)?;
    let key = pem::private_key(&options.private_key)?;

    let config = ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(certificates, key)
    .context("invalid TLS certificate or mismatched private key")?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}
