//! TLS certificate management. Supports manual certs and auto-generated
//! self-signed certificates (matching etcd's `--auto-tls` behavior).

use std::path::Path;
use std::sync::Arc;

use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

/// TLS configuration for the server.
#[derive(Clone, Debug)]
pub struct TlsConfig {
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub trusted_ca_file: Option<String>,
    pub auto_tls: bool,
    pub client_cert_auth: bool,
    pub self_signed_cert_validity: u32,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            cert_file: None,
            key_file: None,
            trusted_ca_file: None,
            auto_tls: false,
            client_cert_auth: false,
            self_signed_cert_validity: 1,
        }
    }
}

impl TlsConfig {
    /// Returns `true` if TLS should be enabled for client connections.
    pub fn is_enabled(&self) -> bool {
        self.auto_tls || (self.cert_file.is_some() && self.key_file.is_some())
    }
}

/// Generate a self-signed certificate and save to `data_dir/auto-tls/`.
///
/// Returns paths to `(cert.pem, key.pem)`. Reuses existing certs if present.
pub fn generate_self_signed(
    data_dir: &str,
    validity_years: u32,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let tls_dir = format!("{}/auto-tls", data_dir);
    std::fs::create_dir_all(&tls_dir)?;

    let cert_path = format!("{}/cert.pem", tls_dir);
    let key_path = format!("{}/key.pem", tls_dir);

    // Reuse existing certs if they exist.
    if Path::new(&cert_path).exists() && Path::new(&key_path).exists() {
        return Ok((cert_path, key_path));
    }

    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()])?;
    params.not_after = time::OffsetDateTime::now_utc()
        + time::Duration::days(365 * validity_years as i64);

    let key_pair = rcgen::KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;

    std::fs::write(&cert_path, cert.pem())?;
    std::fs::write(&key_path, key_pair.serialize_pem())?;

    Ok((cert_path, key_path))
}

/// Build a `tokio-rustls` TLS acceptor from cert/key PEM files.
///
/// This is used for the HTTP/JSON gateway (axum).
pub fn build_tls_acceptor(
    cert_path: &str,
    key_path: &str,
) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    // Ensure a crypto provider is installed (ring). Ignore the error if
    // one is already set -- that just means another call got there first.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cert_pem = std::fs::read(cert_path)?;
    let key_pem = std::fs::read(key_path)?;

    let certs: Vec<_> = rustls_pemfile::certs(&mut &cert_pem[..])
        .collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut &key_pem[..])?
        .ok_or("no private key found in PEM file")?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Build a tonic `ServerTlsConfig` from cert/key files.
///
/// This is used for the gRPC server.
pub fn build_tonic_tls(
    cert_path: &str,
    key_path: &str,
) -> Result<tonic::transport::ServerTlsConfig, Box<dyn std::error::Error>> {
    let cert = std::fs::read_to_string(cert_path)?;
    let key = std::fs::read_to_string(key_path)?;
    let identity = tonic::transport::Identity::from_pem(cert, key);
    Ok(tonic::transport::ServerTlsConfig::new().identity(identity))
}

/// Build a tonic `ServerTlsConfig` with mutual TLS (client certificate validation).
///
/// Used when `--client-cert-auth` is enabled. kube-apiserver passes client certs
/// via `--etcd-certfile`/`--etcd-keyfile` and expects the server to validate them
/// against the trusted CA.
pub fn build_tonic_tls_with_client_auth(
    cert_path: &str,
    key_path: &str,
    ca_cert_pem: &str,
) -> Result<tonic::transport::ServerTlsConfig, Box<dyn std::error::Error>> {
    let cert = std::fs::read_to_string(cert_path)?;
    let key = std::fs::read_to_string(key_path)?;
    let identity = tonic::transport::Identity::from_pem(cert, key);
    let client_ca = tonic::transport::Certificate::from_pem(ca_cert_pem);
    Ok(tonic::transport::ServerTlsConfig::new()
        .identity(identity)
        .client_ca_root(client_ca))
}

/// Build a `tokio-rustls` TLS acceptor with mutual TLS (client certificate validation).
///
/// Used for the HTTP/JSON gateway when `--client-cert-auth` is enabled.
pub fn build_tls_acceptor_with_client_auth(
    cert_path: &str,
    key_path: &str,
    ca_cert_pem: &[u8],
) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cert_pem = std::fs::read(cert_path)?;
    let key_pem = std::fs::read(key_path)?;

    let certs: Vec<_> = rustls_pemfile::certs(&mut &cert_pem[..])
        .collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut &key_pem[..])?
        .ok_or("no private key found in PEM file")?;

    // Parse CA certificates for client verification.
    let mut root_store = rustls::RootCertStore::empty();
    for ca_cert in rustls_pemfile::certs(&mut &ca_cert_pem[..]) {
        root_store.add(ca_cert?)?;
    }

    let client_verifier = tokio_rustls::rustls::server::WebPkiClientVerifier::builder(
        Arc::new(root_store),
    )
    .build()?;

    let config = ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certs, key)?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_disabled_by_default() {
        let config = TlsConfig::default();
        assert!(!config.is_enabled());
    }

    #[test]
    fn test_tls_config_enabled_with_auto_tls() {
        let config = TlsConfig {
            auto_tls: true,
            ..Default::default()
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_tls_config_enabled_with_cert_and_key() {
        let config = TlsConfig {
            cert_file: Some("cert.pem".to_string()),
            key_file: Some("key.pem".to_string()),
            ..Default::default()
        };
        assert!(config.is_enabled());
    }

    #[test]
    fn test_tls_config_not_enabled_with_cert_only() {
        let config = TlsConfig {
            cert_file: Some("cert.pem".to_string()),
            ..Default::default()
        };
        assert!(!config.is_enabled());
    }

    #[test]
    fn test_auto_tls_generates_certs() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = generate_self_signed(dir.path().to_str().unwrap(), 1).unwrap();
        assert!(std::path::Path::new(&cert).exists());
        assert!(std::path::Path::new(&key).exists());

        // Second call should reuse existing certs.
        let (cert2, key2) = generate_self_signed(dir.path().to_str().unwrap(), 1).unwrap();
        assert_eq!(cert, cert2);
        assert_eq!(key, key2);
    }

    #[test]
    fn test_build_tls_acceptor_from_generated_certs() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = generate_self_signed(dir.path().to_str().unwrap(), 1).unwrap();
        let acceptor = build_tls_acceptor(&cert, &key);
        assert!(acceptor.is_ok());
    }

    #[test]
    fn test_build_tonic_tls_from_generated_certs() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = generate_self_signed(dir.path().to_str().unwrap(), 1).unwrap();
        let tls_config = build_tonic_tls(&cert, &key);
        assert!(tls_config.is_ok());
    }
}
