use std::{
    io::{BufReader, Cursor},
    sync::Arc,
};

use rumqttc::tokio_rustls::rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    ClientConfig, RootCertStore, SignatureScheme,
};
use rustls_pemfile::Item;
use types::SslConf;

pub(crate) fn get_ssl_config(ssl_conf: &SslConf) -> ClientConfig {
    let builder = match &ssl_conf.verify {
        true => {
            let root_cert_store = match &ssl_conf.ca_cert {
                Some(ca_cert) => {
                    let mut root_cert_store = RootCertStore::empty();
                    let certs = rustls_pemfile::certs(&mut BufReader::new(Cursor::new(ca_cert)))
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    for cert in certs {
                        root_cert_store.add(cert).unwrap();
                    }
                    root_cert_store
                }
                None => RootCertStore {
                    roots: webpki_roots::TLS_SERVER_ROOTS.into(),
                },
            };
            ClientConfig::builder().with_root_certificates(root_cert_store)
        }
        false => ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(ServerCertVerifierNo {})),
    };

    match (&ssl_conf.client_cert, &ssl_conf.client_key) {
        (Some(client_cert), Some(client_key)) => {
            let client_cert = client_cert.clone().into_bytes();
            let client_key = client_key.clone().into_bytes();
            let client_certs = rustls_pemfile::certs(&mut BufReader::new(Cursor::new(client_cert)))
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            let mut key_buffer = BufReader::new(Cursor::new(client_key));
            let key = loop {
                let item = rustls_pemfile::read_one(&mut key_buffer).unwrap();
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
                    None => {
                        panic!("no valid key in chain");
                    }
                    _ => {}
                }
            };
            builder.with_client_auth_cert(client_certs, key).unwrap()
        }
        _ => builder.with_no_client_auth(),
    }
}

#[derive(Debug)]
struct ServerCertVerifierNo {}

impl ServerCertVerifier for ServerCertVerifierNo {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<
        rumqttc::tokio_rustls::rustls::client::danger::ServerCertVerified,
        rumqttc::tokio_rustls::rustls::Error,
    > {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rumqttc::tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        rumqttc::tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        rumqttc::tokio_rustls::rustls::Error,
    > {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rumqttc::tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        rumqttc::tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        rumqttc::tokio_rustls::rustls::Error,
    > {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rumqttc::tokio_rustls::rustls::SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}
