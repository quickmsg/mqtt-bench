use rustls_pemfile::Item;
use tokio_rustls::rustls::{
    self,
    pki_types::{InvalidDnsNameError, ServerName},
    ClientConfig, RootCertStore,
};
use tokio_rustls::TlsConnector as RustlsConnector;

use std::convert::TryFrom;
use std::io::{BufReader, Cursor};
use std::sync::Arc;

use crate::framed::AsyncReadWrite;
use crate::TlsConfiguration;

use std::io;
use std::net::AddrParseError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error parsing IP address
    #[error("Addr")]
    Addr(#[from] AddrParseError),
    /// I/O related error
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    /// Certificate/Name validation error
    #[error("Web Pki: {0}")]
    WebPki(#[from] webpki::Error),
    /// Invalid DNS name
    #[error("DNS name")]
    DNSName(#[from] InvalidDnsNameError),
    /// Error from rustls module
    #[error("TLS error: {0}")]
    TLS(#[from] rustls::Error),
    /// No valid CA cert found
    #[error("No valid CA certificate provided")]
    NoValidCertInChain,
    /// No valid client cert found
    #[error("No valid certificate for client authentication in chain")]
    NoValidClientCertInChain,
    /// No valid key found
    #[error("No valid key in chain")]
    NoValidKeyInChain,
}

pub async fn rustls_connector(tls_config: &TlsConfiguration) -> Result<RustlsConnector, Error> {
    let config = match tls_config {
        TlsConfiguration::Simple {
            ca,
            alpn,
            client_auth,
        } => {
            // Add ca to root store if the connection is TLS
            let mut root_cert_store = RootCertStore::empty();
            let certs = rustls_pemfile::certs(&mut BufReader::new(Cursor::new(ca)))
                .collect::<Result<Vec<_>, _>>()?;

            root_cert_store.add_parsable_certificates(certs);

            if root_cert_store.is_empty() {
                return Err(Error::NoValidCertInChain);
            }

            let config = ClientConfig::builder().with_root_certificates(root_cert_store);

            // Add der encoded client cert and key
            let mut config = if let Some(client) = client_auth.as_ref() {
                let certs =
                    rustls_pemfile::certs(&mut BufReader::new(Cursor::new(client.0.clone())))
                        .collect::<Result<Vec<_>, _>>()?;
                if certs.is_empty() {
                    return Err(Error::NoValidClientCertInChain);
                }

                // Create buffer for key file
                let mut key_buffer = BufReader::new(Cursor::new(client.1.clone()));

                // Read PEM items until we find a valid key.
                let key = loop {
                    let item = rustls_pemfile::read_one(&mut key_buffer)?;
                    match item {
                        Some(Item::Sec1Key(key)) => {
                            break key.into();
                        }
                        Some(Item::Pkcs1Key(key)) => {
                            break key.into();
                        }
                        Some(Item::Pkcs8Key(key)) => {
                            break key.into();
                        }
                        None => return Err(Error::NoValidKeyInChain),
                        _ => {}
                    }
                };

                config.with_client_auth_cert(certs, key)?
            } else {
                config.with_no_client_auth()
            };

            // Set ALPN
            if let Some(alpn) = alpn.as_ref() {
                config.alpn_protocols.extend_from_slice(alpn);
            }

            Arc::new(config)
        }
        TlsConfiguration::Rustls(tls_client_config) => tls_client_config.clone(),
        #[allow(unreachable_patterns)]
        _ => unreachable!("This cannot be called for other TLS backends than Rustls"),
    };

    Ok(RustlsConnector::from(config))
}

pub async fn tls_connect(
    addr: &str,
    _port: u16,
    tls_config: &TlsConfiguration,
    tcp: Box<dyn AsyncReadWrite>,
) -> Result<Box<dyn AsyncReadWrite>, Error> {
    let tls: Box<dyn AsyncReadWrite> = match tls_config {
        TlsConfiguration::Simple { .. } | TlsConfiguration::Rustls(_) => {
            let connector = rustls_connector(tls_config).await?;
            let domain = ServerName::try_from(addr)?.to_owned();
            Box::new(connector.connect(domain, tcp).await?)
        }
        #[allow(unreachable_patterns)]
        _ => panic!("Unknown or not enabled TLS backend configuration"),
    };
    Ok(tls)
}
