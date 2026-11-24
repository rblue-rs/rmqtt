use std::time::Duration;

pub const MAX_CA_PEM: usize = 65536;
pub const MAX_CLIENT_CERT_PEM: usize = 65536;
pub const MAX_CLIENT_KEY_PEM: usize = 65536;
// MQTT Binary Data has a two-byte length in both supported protocols.
pub const MAX_WILL_PAYLOAD: usize = 65535;

/// MQTT 3.1.1 Last Will. QoS 2 is rejected as unsupported.
#[derive(Clone)]
pub struct Will {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
}
impl std::fmt::Debug for Will {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Will")
            .field("topic", &self.topic)
            .field("payload_len", &self.payload.len())
            .field("qos", &self.qos)
            .field("retain", &self.retain)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Error {
    InvalidArgument = 1,
    NotInitialized = 2,
    Closed = 3,
    Busy = 4,
    QueueFull = 5,
    Unsupported = 6,
    Network = 7,
    Refused = 8,
    BufferTooSmall = 9,
    Empty = 10,
    Internal = 11,
    Overflow = 12,
    Timeout = 13,
    Tls = 14,
    CertificateUntrusted = 15,
    HostnameMismatch = 16,
    CertificateExpired = 17,
    /// Password on plaintext TCP without an explicit allow switch.
    PlaintextCredentials = 18,
    /// CONNACK rejected the username/password (or not authorized).
    BadCredentials = 19,
    /// Client certificate or private key PEM could not be parsed.
    InvalidClientCertificate = 20,
    /// Client certificate and private key are not a usable pair.
    CertificateKeyMismatch = 21,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

fn zeroize(buf: &mut [u8]) {
    for b in buf.iter_mut() {
        *b = 0;
    }
}

fn take_zeroized_bytes(value: &mut Option<Vec<u8>>) {
    if let Some(mut bytes) = value.take() {
        zeroize(&mut bytes);
    }
}

/// Automatic reconnect policy. Disabled by default. When enabled, a later drop
/// may retry with equal jitter and a required attempt ceiling. First connect
/// failure never enters this loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconnectPolicy {
    pub enabled: bool,
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}
impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            max_attempts: 8,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
        }
    }
}
impl ReconnectPolicy {
    pub fn validate(&self) -> Result<(), Error> {
        if !self.enabled {
            return Ok(());
        }
        if self.max_attempts < 1
            || self.max_attempts > 10_000
            || self.base_delay < Duration::from_millis(1)
            || self.base_delay > self.max_delay
            || self.max_delay > Duration::from_secs(300)
            || !self.base_delay.subsec_nanos().is_multiple_of(1_000_000)
            || !self.max_delay.subsec_nanos().is_multiple_of(1_000_000)
        {
            return Err(Error::InvalidArgument);
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Options {
    pub server: String,
    pub client_id: String,
    pub port: u16,
    pub protocol: u8,
    pub keep_alive: Duration,
    /// When true, MQTT is carried over TLS with certificate and hostname checks.
    pub use_tls: bool,
    /// Carry MQTT in WebSocket binary frames. use_tls selects WSS vs WS.
    pub use_websocket: bool,
    /// Absolute URI path, without query, fragment, userinfo or custom headers.
    pub websocket_path: String,
    /// PEM-encoded CA certificates. `None` with `use_tls` trusts host system
    /// roots only. `Some` trusts only the provided CAs and does not mix in
    /// system roots. Empty or invalid PEM is rejected; it is not a fallback.
    pub ca_pem: Option<Vec<u8>>,
    /// PEM-encoded client certificate chain. Must be paired with
    /// `client_key_pem` and `use_tls`.
    pub client_cert_pem: Option<Vec<u8>>,
    /// PEM-encoded client private key. Must be paired with `client_cert_pem`
    /// and `use_tls`. Held only in memory.
    pub client_key_pem: Option<Vec<u8>>,
    pub username: Option<String>,
    pub password: Option<String>,
    /// When true, a password may be sent on plaintext TCP. Default false.
    /// TLS connections may send a password without this switch.
    pub allow_plaintext_credentials: bool,
    pub will: Option<Will>,
    /// MQTT 3.1.1 Clean Session. Default true, matching the previous implicit engine default.
    pub clean_session: bool,
    /// Default disabled. Enabling requires a finite max_attempts ceiling.
    pub reconnect: ReconnectPolicy,
}
impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field(
                "server",
                &if self.use_websocket {
                    "<redacted>"
                } else {
                    &self.server
                },
            )
            .field("client_id", &self.client_id)
            .field("port", &self.port)
            .field("protocol", &self.protocol)
            .field("keep_alive", &self.keep_alive)
            .field("use_tls", &self.use_tls)
            .field("use_websocket", &self.use_websocket)
            .field("websocket_path", &"<redacted>")
            .field("ca_pem", &self.ca_pem.as_ref().map(|_| "<redacted>"))
            .field(
                "client_cert_pem",
                &self.client_cert_pem.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "client_key_pem",
                &self.client_key_pem.as_ref().map(|_| "<redacted>"),
            )
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field(
                "allow_plaintext_credentials",
                &self.allow_plaintext_credentials,
            )
            .field("will", &self.will)
            .field("clean_session", &self.clean_session)
            .field("reconnect", &self.reconnect)
            .finish()
    }
}
impl Options {
    pub fn new(server: impl Into<String>, client_id: impl Into<String>, port: u16) -> Self {
        Self {
            server: server.into(),
            client_id: client_id.into(),
            port,
            protocol: 4,
            keep_alive: Duration::ZERO,
            use_tls: false,
            use_websocket: false,
            websocket_path: "/mqtt".into(),
            ca_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            username: None,
            password: None,
            allow_plaintext_credentials: false,
            will: None,
            clean_session: true,
            reconnect: ReconnectPolicy::default(),
        }
    }
    /// Validated WS/WSS URI. IPv6, URLs, credentials, queries and fragments are
    /// deliberately not accepted as host/path inputs in this baseline.
    pub fn websocket_uri(&self) -> Result<String, Error> {
        let host = &self.server;
        if !self.use_websocket
            || self.port == 0
            || host.is_empty()
            || host.len() > 253
            || host.split('.').any(|label| {
                label.is_empty()
                    || label.len() > 63
                    || label.starts_with('-')
                    || label.ends_with('-')
                    || !label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
            || (host.bytes().all(|b| b.is_ascii_digit() || b == b'.')
                && host.parse::<std::net::Ipv4Addr>().is_err())
        {
            return Err(Error::InvalidArgument);
        }
        let path = self.websocket_path.as_bytes();
        if path.is_empty() || path.len() > 2048 || path[0] != b'/' || path.starts_with(b"//") {
            return Err(Error::InvalidArgument);
        }
        let mut i = 0;
        while i < path.len() {
            let b = path[i];
            if b == b'%' {
                if i + 2 >= path.len()
                    || !path[i + 1].is_ascii_hexdigit()
                    || !path[i + 2].is_ascii_hexdigit()
                {
                    return Err(Error::InvalidArgument);
                }
                i += 3;
                continue;
            }
            if !b.is_ascii_alphanumeric() && !b"/-._~!$&'()*+,;=:@".contains(&b) {
                return Err(Error::InvalidArgument);
            }
            i += 1;
        }
        Ok(format!(
            "{}://{}:{}{}",
            if self.use_tls { "wss" } else { "ws" },
            host,
            self.port,
            self.websocket_path
        ))
    }
    /// Best-effort clearing of secret material. This is not a secure-erase guarantee.
    pub fn clear_secrets(&mut self) {
        take_zeroized_bytes(&mut self.ca_pem);
        take_zeroized_bytes(&mut self.client_cert_pem);
        take_zeroized_bytes(&mut self.client_key_pem);
        if let Some(password) = self.password.take() {
            let mut bytes = password.into_bytes();
            zeroize(&mut bytes);
        }
    }
    pub fn validate(&self) -> Result<(), Error> {
        if self.use_websocket {
            self.websocket_uri()?;
        } else if self.websocket_path != "/mqtt" {
            return Err(Error::InvalidArgument);
        }

        if self.server.is_empty()
            || self.server.len() > 65535
            || self.client_id.is_empty()
            || self.client_id.len() > 65535
            || self.server.contains('\0')
            || self.client_id.contains('\0')
            || self.port == 0
            || self.keep_alive.as_secs() > 65535
            || self.keep_alive.subsec_nanos() != 0
        {
            return Err(Error::InvalidArgument);
        }
        if self.use_tls {
            if let Some(pem) = &self.ca_pem {
                if pem.is_empty() || pem.len() > MAX_CA_PEM || pem.contains(&0) {
                    return Err(Error::InvalidArgument);
                }
            }
        } else if self.ca_pem.is_some() {
            return Err(Error::InvalidArgument);
        }
        if self.client_cert_pem.is_some() || self.client_key_pem.is_some() {
            if !self.use_tls {
                return Err(Error::InvalidArgument);
            }
            match (&self.client_cert_pem, &self.client_key_pem) {
                (Some(cert), Some(key)) => {
                    if cert.is_empty()
                        || cert.len() > MAX_CLIENT_CERT_PEM
                        || cert.contains(&0)
                        || key.is_empty()
                        || key.len() > MAX_CLIENT_KEY_PEM
                        || key.contains(&0)
                    {
                        return Err(Error::InvalidArgument);
                    }
                }
                _ => return Err(Error::CertificateKeyMismatch),
            }
        }
        if let Some(username) = &self.username {
            if username.is_empty() || username.len() > 65535 || username.contains('\0') {
                return Err(Error::InvalidArgument);
            }
        }
        if let Some(password) = &self.password {
            if self.username.is_none() || password.len() > 65535 || password.contains('\0') {
                return Err(Error::InvalidArgument);
            }
            if !self.use_tls && !self.allow_plaintext_credentials {
                return Err(Error::PlaintextCredentials);
            }
        }
        if let Some(will) = &self.will {
            if will.topic.is_empty()
                || will.topic.len() > 65535
                || will.topic.contains('\0')
                || will.topic.contains(['#', '+'])
                || will.payload.len() > MAX_WILL_PAYLOAD
            {
                return Err(Error::InvalidArgument);
            }
            match will.qos {
                0 | 1 => {}
                2 => return Err(Error::Unsupported),
                _ => return Err(Error::InvalidArgument),
            }
        }
        self.reconnect.validate()?;
        if self.protocol == 5 && (self.keep_alive.as_secs() < 5 || !self.clean_session) {
            return Err(Error::Unsupported);
        }
        if self.protocol != 4 && self.protocol != 5 {
            return Err(Error::Unsupported);
        }
        Ok(())
    }
}
impl Drop for Options {
    fn drop(&mut self) {
        self.clear_secrets();
    }
}
