use mqtt_api::{Error, Options};
use rumqttc::{TlsConfiguration, Transport};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    CertificateError, ClientConfig, Error as RustlsError, RootCertStore,
};
use std::{
    error::Error as StdError,
    io::{self, Cursor},
};

pub(crate) fn transport(options: &Options) -> Result<Transport, Error> {
    let roots = match &options.ca_pem {
        Some(pem) => custom_roots(pem)?,
        None => system_roots()?,
    };
    let builder = ClientConfig::builder().with_root_certificates(roots);
    let config = match (&options.client_cert_pem, &options.client_key_pem) {
        (None, None) => builder.with_no_client_auth(),
        (Some(cert_pem), Some(key_pem)) => {
            let certs = parse_client_certs(cert_pem)?;
            let key = parse_client_key(key_pem)?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(map_client_identity)?
        }
        _ => return Err(Error::CertificateKeyMismatch),
    };
    let config = TlsConfiguration::from(config);
    Ok(if options.use_websocket {
        Transport::wss_with_config(config)
    } else {
        Transport::tls_with_config(config)
    })
}

fn parse_client_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, Error> {
    let parsed = rustls_pemfile::certs(&mut Cursor::new(pem)).collect::<Vec<_>>();
    if parsed.is_empty() {
        return Err(Error::InvalidClientCertificate);
    }
    let mut certs = Vec::with_capacity(parsed.len());
    for cert in parsed {
        certs.push(cert.map_err(|_| Error::InvalidClientCertificate)?);
    }
    Ok(certs)
}

fn parse_client_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, Error> {
    rustls_pemfile::private_key(&mut Cursor::new(pem))
        .map_err(|_| Error::InvalidClientCertificate)?
        .ok_or(Error::InvalidClientCertificate)
}

fn map_client_identity(err: RustlsError) -> Error {
    match err {
        RustlsError::InconsistentKeys(_) => Error::CertificateKeyMismatch,
        _ => Error::InvalidClientCertificate,
    }
}

fn custom_roots(pem: &[u8]) -> Result<RootCertStore, Error> {
    let certs = rustls_pemfile::certs(&mut Cursor::new(pem)).collect::<Vec<_>>();
    if certs.is_empty() {
        return Err(Error::InvalidArgument);
    }
    let mut ders = Vec::with_capacity(certs.len());
    for cert in certs {
        ders.push(cert.map_err(|_| Error::InvalidArgument)?);
    }
    let mut roots = RootCertStore::empty();
    if roots.add_parsable_certificates(ders).0 == 0 {
        return Err(Error::InvalidArgument);
    }
    Ok(roots)
}

fn system_roots() -> Result<RootCertStore, Error> {
    let loaded = rustls_native_certs::load_native_certs();
    if loaded.certs.is_empty() {
        return Err(Error::Tls);
    }
    let mut roots = RootCertStore::empty();
    if roots.add_parsable_certificates(loaded.certs).0 == 0 {
        return Err(Error::Tls);
    }
    Ok(roots)
}

pub(crate) fn rustls_from_io(io: &io::Error) -> Option<Error> {
    if let Some(error) = io
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<RustlsError>())
    {
        return Some(map_rustls(error));
    }
    let mut current = io.source();
    while let Some(inner) = current {
        if let Some(error) = inner.downcast_ref::<RustlsError>() {
            return Some(map_rustls(error));
        }
        current = inner.source();
    }
    // OpenSSL-based brokers may close the TCP stream on a client-certificate
    // failure without a TLS alert that rustls can surface as AlertReceived.
    if mentions_tls_close_notify(io) {
        return Some(Error::Tls);
    }
    None
}

fn mentions_tls_close_notify(err: &io::Error) -> bool {
    let mut current: Option<&dyn StdError> = Some(err);
    while let Some(inner) = current {
        if inner.to_string().contains("TLS close_notify") {
            return true;
        }
        current = inner.source();
    }
    false
}

fn map_rustls(err: &RustlsError) -> Error {
    match err {
        RustlsError::InvalidCertificate(cert) => map_certificate(cert),
        _ => Error::Tls,
    }
}

fn map_certificate(err: &CertificateError) -> Error {
    match err {
        CertificateError::Expired
        | CertificateError::ExpiredContext { .. }
        | CertificateError::NotValidYet
        | CertificateError::NotValidYetContext { .. } => Error::CertificateExpired,
        CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. } => {
            Error::HostnameMismatch
        }
        _ => Error::CertificateUntrusted,
    }
}

// WSS TLS failures are nested in tungstenite rather than rumqttc's TLS arm.
// Traverse typed error sources, never format potentially sensitive URL/headers.
pub(crate) fn map_websocket_error(err: &(dyn StdError + 'static)) -> Error {
    let mut current = Some(err);
    while let Some(e) = current {
        if let Some(tls) = e.downcast_ref::<RustlsError>() {
            return map_rustls(tls);
        }
        if let Some(io) = e.downcast_ref::<io::Error>() {
            if let Some(mapped) = rustls_from_io(io) {
                return mapped;
            }
        }
        current = e.source();
    }
    Error::Network
}
