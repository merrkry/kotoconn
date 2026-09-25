use anyhow::{Context, Result, ensure};
use kotoconn_config::{Target, TlsClientConfig, TlsServerConfig};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
};
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
    if let Some(pem) = &options.certificate {
        let certificates = certificates(pem)?;
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
    let certificates = certificates(&options.certificate)?;
    let key = PrivateKeyDer::from_pem_slice(options.private_key.as_bytes())
        .context("invalid TLS private key PEM")?;

    let config = ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(certificates, key)
    .context("invalid TLS certificate or mismatched private key")?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn certificates(pem: &str) -> Result<Vec<CertificateDer<'static>>> {
    let certificates = CertificateDer::pem_slice_iter(pem.as_bytes())
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("invalid TLS certificate PEM")?;
    ensure!(!certificates.is_empty(), "TLS certificate PEM is empty");
    Ok(certificates)
}
