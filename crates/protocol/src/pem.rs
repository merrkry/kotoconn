use anyhow::{Context, Result, ensure};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

pub fn certificates(pem: &str) -> Result<Vec<CertificateDer<'static>>> {
    let certificates = CertificateDer::pem_slice_iter(pem.as_bytes())
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("invalid TLS certificate PEM")?;
    ensure!(!certificates.is_empty(), "empty PEM certificate chain");
    Ok(certificates)
}

pub fn private_key(pem: &str) -> Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_slice(pem.as_bytes()).context("invalid TLS private key PEM")
}
