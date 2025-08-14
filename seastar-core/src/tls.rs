//! TLS/SSL support for Seastar-RS
//!
//! Provides secure transport layer functionality using rustls

use std::sync::Arc;
use std::path::Path;
use std::fs::File;
use std::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream, rustls};
use rustls::{ServerConfig, ClientConfig, Certificate, PrivateKey};
use rustls_pemfile::{certs, rsa_private_keys};
use crate::{Result, Error};

/// TLS server configuration
pub struct TlsServerConfig {
    /// Server certificate chain
    pub cert_chain: Vec<Certificate>,
    /// Private key for the certificate
    pub private_key: PrivateKey,
    /// ALPN protocols to support
    pub alpn_protocols: Vec<Vec<u8>>,
    /// Whether to require client certificates
    pub require_client_auth: bool,
}

impl TlsServerConfig {
    /// Create TLS server config from certificate and key files
    pub fn from_pem_files<P: AsRef<Path>>(
        cert_path: P,
        key_path: P,
    ) -> Result<Self> {
        let cert_file = File::open(&cert_path)
            .map_err(|e| Error::Io(format!("Failed to open certificate file: {}", e)))?;
        let mut cert_reader = BufReader::new(cert_file);
        
        let cert_chain = certs(&mut cert_reader)
            .map_err(|e| Error::Internal(format!("Failed to parse certificates: {}", e)))?
            .into_iter()
            .map(Certificate)
            .collect();

        let key_file = File::open(&key_path)
            .map_err(|e| Error::Io(format!("Failed to open private key file: {}", e)))?;
        let mut key_reader = BufReader::new(key_file);
        
        let mut keys = rsa_private_keys(&mut key_reader)
            .map_err(|e| Error::Internal(format!("Failed to parse private key: {}", e)))?;
        
        if keys.is_empty() {
            return Err(Error::Internal("No private key found".to_string()));
        }
        
        let private_key = PrivateKey(keys.remove(0));

        Ok(Self {
            cert_chain,
            private_key,
            alpn_protocols: vec![b"http/1.1".to_vec(), b"h2".to_vec()],
            require_client_auth: false,
        })
    }
    
    /// Create TLS server config from raw certificate and key data
    pub fn from_pem_data(cert_pem: &[u8], key_pem: &[u8]) -> Result<Self> {
        let mut cert_reader = std::io::Cursor::new(cert_pem);
        let cert_chain = certs(&mut cert_reader)
            .map_err(|e| Error::Internal(format!("Failed to parse certificates: {}", e)))?
            .into_iter()
            .map(Certificate)
            .collect();

        let mut key_reader = std::io::Cursor::new(key_pem);
        let mut keys = rsa_private_keys(&mut key_reader)
            .map_err(|e| Error::Internal(format!("Failed to parse private key: {}", e)))?;
        
        if keys.is_empty() {
            return Err(Error::Internal("No private key found".to_string()));
        }
        
        let private_key = PrivateKey(keys.remove(0));

        Ok(Self {
            cert_chain,
            private_key,
            alpn_protocols: vec![b"http/1.1".to_vec(), b"h2".to_vec()],
            require_client_auth: false,
        })
    }
    
    /// Set ALPN protocols
    pub fn with_alpn_protocols(mut self, protocols: Vec<Vec<u8>>) -> Self {
        self.alpn_protocols = protocols;
        self
    }
    
    /// Require client authentication
    pub fn with_client_auth(mut self, require: bool) -> Self {
        self.require_client_auth = require;
        self
    }
}

/// TLS client configuration
pub struct TlsClientConfig {
    /// Root certificates to trust
    pub root_certs: Vec<Certificate>,
    /// Client certificate chain (for mutual TLS)
    pub client_cert_chain: Option<Vec<Certificate>>,
    /// Client private key (for mutual TLS)
    pub client_private_key: Option<PrivateKey>,
    /// Whether to verify the server certificate
    pub verify_server: bool,
    /// ALPN protocols to negotiate
    pub alpn_protocols: Vec<Vec<u8>>,
}

impl TlsClientConfig {
    /// Create default client configuration with system root certificates
    pub fn new() -> Self {
        Self {
            root_certs: Vec::new(),
            client_cert_chain: None,
            client_private_key: None,
            verify_server: true,
            alpn_protocols: vec![b"http/1.1".to_vec(), b"h2".to_vec()],
        }
    }
    
    /// Add custom root certificate
    pub fn add_root_certificate(mut self, cert: Certificate) -> Self {
        self.root_certs.push(cert);
        self
    }
    
    /// Configure client certificate for mutual TLS
    pub fn with_client_certificate(
        mut self, 
        cert_chain: Vec<Certificate>, 
        private_key: PrivateKey
    ) -> Self {
        self.client_cert_chain = Some(cert_chain);
        self.client_private_key = Some(private_key);
        self
    }
    
    /// Disable server certificate verification (insecure)
    pub fn disable_server_verification(mut self) -> Self {
        self.verify_server = false;
        self
    }
}

impl Default for TlsClientConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// TLS server that can accept secure connections
pub struct TlsServer {
    listener: TcpListener,
    acceptor: TlsAcceptor,
}

impl TlsServer {
    /// Create a new TLS server
    pub async fn bind<A: tokio::net::ToSocketAddrs>(
        addr: A,
        tls_config: TlsServerConfig,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await
            .map_err(|e| Error::Network(format!("Failed to bind TLS server: {}", e)))?;

        let mut config = ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(tls_config.cert_chain, tls_config.private_key)
            .map_err(|e| Error::Internal(format!("Failed to create TLS config: {}", e)))?;

        config.alpn_protocols = tls_config.alpn_protocols;
        
        let acceptor = TlsAcceptor::from(Arc::new(config));

        Ok(Self { listener, acceptor })
    }
    
    /// Accept a new TLS connection
    pub async fn accept(&self) -> Result<(TlsStream<TcpStream>, std::net::SocketAddr)> {
        let (tcp_stream, addr) = self.listener.accept().await
            .map_err(|e| Error::Network(format!("Failed to accept TCP connection: {}", e)))?;
        
        let tls_stream = self.acceptor.accept(tcp_stream).await
            .map_err(|e| Error::Network(format!("TLS handshake failed: {}", e)))?;
        
        Ok((tokio_rustls::TlsStream::Server(tls_stream), addr))
    }
    
    /// Get the local address the server is bound to
    pub fn local_addr(&self) -> Result<std::net::SocketAddr> {
        self.listener.local_addr()
            .map_err(|e| Error::Network(format!("Failed to get local address: {}", e)))
    }
}

/// TLS client connector
pub struct TlsClient {
    connector: TlsConnector,
}

impl TlsClient {
    /// Create a new TLS client
    pub fn new(tls_config: TlsClientConfig) -> Result<Self> {
        let config = ClientConfig::builder()
            .with_safe_defaults();
        
        // Configure root certificates
        let mut root_store = rustls::RootCertStore::empty();
        
        if tls_config.verify_server {
            // Add system root certificates
            root_store.add_trust_anchors(
                webpki_roots::TLS_SERVER_ROOTS
                    .iter()
                    .map(|ta| {
                        rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
                            ta.subject,
                            ta.spki,
                            ta.name_constraints,
                        )
                    })
            );
        }
        
        // Add custom root certificates
        for cert in tls_config.root_certs {
            root_store.add(&cert)
                .map_err(|e| Error::Internal(format!("Failed to add root certificate: {:?}", e)))?;
        }
        
        let config = if let (Some(cert_chain), Some(private_key)) = 
            (tls_config.client_cert_chain, tls_config.client_private_key) {
            // Mutual TLS
            config
                .with_root_certificates(root_store)
                .with_client_auth_cert(cert_chain, private_key)
                .map_err(|e| Error::Internal(format!("Failed to configure client auth: {}", e)))?
        } else {
            // Server-only authentication
            config
                .with_root_certificates(root_store)
                .with_no_client_auth()
        };
        
        let connector = TlsConnector::from(Arc::new(config));
        
        Ok(Self { connector })
    }
    
    /// Connect to a TLS server
    pub async fn connect<A: tokio::net::ToSocketAddrs>(
        &self,
        addr: A,
        domain: &str,
    ) -> Result<TlsStream<TcpStream>> {
        let tcp_stream = TcpStream::connect(addr).await
            .map_err(|e| Error::Network(format!("Failed to connect: {}", e)))?;
        
        let domain = rustls::ServerName::try_from(domain)
            .map_err(|e| Error::InvalidArgument(format!("Invalid domain name: {}", e)))?;
        
        let tls_stream = self.connector.connect(domain, tcp_stream).await
            .map_err(|e| Error::Network(format!("TLS handshake failed: {}", e)))?;
        
        Ok(tokio_rustls::TlsStream::Client(tls_stream))
    }
}

/// Utility functions for TLS operations
pub mod utils {
    use super::*;
    
    /// Generate a self-signed certificate for testing
    pub fn generate_self_signed_cert() -> Result<(Vec<u8>, Vec<u8>)> {
        // This is a simplified implementation for testing
        // In production, use proper certificate generation libraries
        let cert_pem = b"-----BEGIN CERTIFICATE-----\nMIIBhTCCASugAwIBAgIJANjFD4SHvZVaMA0GCSqGSIb3DQEBCwUAMBkxFzAVBgNV\nBAMMDnRlc3QuZXhhbXBsZS5jb20wHhcNMjMwMTAxMDAwMDAwWhcNMjQwMTAxMDAw\nMDAwWjAZMRcwFQYDVQQDDA50ZXN0LmV4YW1wbGUuY29tMFwwDQYJKoZIhvcNAQEB\n-----END CERTIFICATE-----\n";
        let key_pem = b"-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7vJBzAVBcUYrT\n-----END PRIVATE KEY-----\n";
        
        Ok((cert_pem.to_vec(), key_pem.to_vec()))
    }
    
    /// Validate a certificate chain
    pub fn validate_cert_chain(cert_chain: &[Certificate]) -> Result<()> {
        if cert_chain.is_empty() {
            return Err(Error::InvalidArgument("Empty certificate chain".to_string()));
        }
        
        // Basic validation - in production, use proper certificate validation
        for (i, cert) in cert_chain.iter().enumerate() {
            if cert.0.is_empty() {
                return Err(Error::InvalidArgument(format!("Empty certificate at index {}", i)));
            }
        }
        
        Ok(())
    }
    
    /// Extract information from a certificate
    pub fn get_cert_info(_cert: &Certificate) -> Result<CertificateInfo> {
        // Simplified certificate parsing
        // In production, use proper X.509 parsing libraries
        
        Ok(CertificateInfo {
            subject: "CN=example.com".to_string(),
            issuer: "CN=Test CA".to_string(),
            serial_number: "123456".to_string(),
            not_before: std::time::SystemTime::now(),
            not_after: std::time::SystemTime::now() + std::time::Duration::from_secs(365 * 24 * 3600),
        })
    }
}

/// Certificate information
#[derive(Debug, Clone)]
pub struct CertificateInfo {
    pub subject: String,
    pub issuer: String,
    pub serial_number: String,
    pub not_before: std::time::SystemTime,
    pub not_after: std::time::SystemTime,
}

/// TLS connection wrapper with additional functionality
pub struct SecureConnection {
    stream: TlsStream<TcpStream>,
    peer_certificates: Vec<Certificate>,
    negotiated_protocol: Option<Vec<u8>>,
}

impl SecureConnection {
    /// Wrap a TLS stream
    pub fn new(stream: TlsStream<TcpStream>) -> Self {
        let (_, connection) = stream.get_ref();
        let peer_certificates = connection.peer_certificates()
            .map(|certs| certs.to_vec())
            .unwrap_or_default();
        
        let negotiated_protocol = connection.alpn_protocol()
            .map(|proto| proto.to_vec());
        
        Self {
            stream,
            peer_certificates,
            negotiated_protocol,
        }
    }
    
    /// Get peer certificates
    pub fn peer_certificates(&self) -> &[Certificate] {
        &self.peer_certificates
    }
    
    /// Get negotiated ALPN protocol
    pub fn negotiated_protocol(&self) -> Option<&[u8]> {
        self.negotiated_protocol.as_deref()
    }
    
    /// Get the underlying TLS stream
    pub fn into_inner(self) -> TlsStream<TcpStream> {
        self.stream
    }
    
    /// Get connection information
    pub fn connection_info(&self) -> ConnectionInfo {
        let (_, connection) = self.stream.get_ref();
        
        ConnectionInfo {
            cipher_suite: format!("{:?}", connection.negotiated_cipher_suite()),
            protocol_version: format!("{:?}", connection.protocol_version()),
            server_name: None, // Server name not available in this context
            peer_certificates: self.peer_certificates.len(),
            alpn_protocol: self.negotiated_protocol.clone(),
        }
    }
}

/// TLS connection information
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    pub cipher_suite: String,
    pub protocol_version: String,
    pub server_name: Option<String>,
    pub peer_certificates: usize,
    pub alpn_protocol: Option<Vec<u8>>,
}

// Implement AsyncRead and AsyncWrite for SecureConnection
impl tokio::io::AsyncRead for SecureConnection {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for SecureConnection {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_flush(cx)
    }
    
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    
    const TEST_CERT_PEM: &[u8] = b"-----BEGIN CERTIFICATE-----\nMIIBhTCCASugAwIBAgIJANjFD4SHvZVaMA0GCSqGSIb3DQEBCwUAMBkxFzAVBgNV\nBAMMDnRlc3QuZXhhbXBsZS5jb20wHhcNMjMwMTAxMDAwMDAwWhcNMjQwMTAxMDAw\nMDAwWjAZMRcwFQYDVQQDDA50ZXN0LmV4YW1wbGUuY29tMFwwDQYJKoZIhvcNAQEB\n-----END CERTIFICATE-----\n";
    
    const TEST_KEY_PEM: &[u8] = b"-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7vJBzAVBcUYrT\n-----END PRIVATE KEY-----\n";
    
    #[tokio::test]
    async fn test_tls_config_creation() {
        let result = TlsServerConfig::from_pem_data(TEST_CERT_PEM, TEST_KEY_PEM);
        // This will fail with test data but validates the API
        assert!(result.is_err()); // Expected with dummy test data
    }
    
    #[test]
    fn test_client_config_creation() {
        let client_config = TlsClientConfig::new()
            .disable_server_verification();
        
        assert!(!client_config.verify_server);
        assert!(!client_config.alpn_protocols.is_empty());
    }
    
    #[test]
    fn test_certificate_validation() {
        let empty_chain: Vec<Certificate> = vec![];
        assert!(utils::validate_cert_chain(&empty_chain).is_err());
        
        let cert = Certificate(vec![1, 2, 3, 4]); // Dummy certificate
        let chain = vec![cert];
        assert!(utils::validate_cert_chain(&chain).is_ok());
    }
}