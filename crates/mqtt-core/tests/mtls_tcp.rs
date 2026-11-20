mod common;
use common::next;
use mqtt_api::{Error, Options};
use mqtt_core::{Client, Kind};
use rustls::{
    server::WebPkiClientVerifier, RootCertStore, ServerConfig, ServerConnection, StreamOwned,
};
use std::{
    fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Certs(PathBuf);
impl Certs {
    fn generate() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("mqtt-mtls-{}-{stamp}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let script =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts/gen_test_certs.py");
        let status = Command::new("python3")
            .arg(&script)
            .arg(&dir)
            .stdout(std::process::Stdio::null())
            .status()
            .expect("run gen_test_certs.py");
        assert!(status.success(), "gen_test_certs.py failed");
        Self(dir)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
    fn pem(&self, name: &str) -> Vec<u8> {
        fs::read(self.path(name)).unwrap()
    }
}
impl Drop for Certs {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn load_certs(path: &Path) -> Vec<rustls::pki_types::CertificateDer<'static>> {
    rustls_pemfile::certs(&mut io::BufReader::new(fs::File::open(path).unwrap()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn load_key(path: &Path) -> rustls::pki_types::PrivateKeyDer<'static> {
    rustls_pemfile::private_key(&mut io::BufReader::new(fs::File::open(path).unwrap()))
        .unwrap()
        .expect("key")
}

fn mtls_server_config(cert: &Path, key: &Path, client_ca: &Path) -> Arc<ServerConfig> {
    let mut roots = RootCertStore::empty();
    assert!(
        roots.add_parsable_certificates(load_certs(client_ca)).0 > 0,
        "client CA"
    );
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .expect("client verifier");
    Arc::new(
        ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(load_certs(cert), load_key(key))
            .unwrap(),
    )
}

enum Mode {
    EchoQos1,
    HandshakeOnly,
}

struct Server {
    port: u16,
    accepts: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}
impl Server {
    fn start(mode: Mode, cert: &Path, key: &Path, client_ca: &Path) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepts = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let tls = mtls_server_config(cert, key, client_ca);
        let acc = accepts.clone();
        let halt = stop.clone();
        let handle = thread::spawn(move || {
            while !halt.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        acc.fetch_add(1, Ordering::SeqCst);
                        let _ = stream.set_nonblocking(false);
                        let _ = stream.set_nodelay(true);
                        let _ = stream.set_read_timeout(Some(Duration::from_secs(3)));
                        let _ = stream.set_write_timeout(Some(Duration::from_secs(3)));
                        match mode {
                            Mode::HandshakeOnly => {
                                let _ = tls_handshake(stream, tls.clone());
                            }
                            Mode::EchoQos1 => {
                                if let Ok(mut s) = tls_handshake(stream, tls.clone()) {
                                    let _ = mqtt_echo(&mut s);
                                }
                            }
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            port,
            accepts,
            stop,
            handle: Some(handle),
        }
    }
    fn assert_no_retry(&self) {
        let until = Instant::now() + Duration::from_millis(250);
        while Instant::now() < until {
            assert_eq!(
                self.accepts.load(Ordering::SeqCst),
                1,
                "first failure retried"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn tls_handshake(
    stream: TcpStream,
    config: Arc<ServerConfig>,
) -> io::Result<StreamOwned<ServerConnection, TcpStream>> {
    let conn = ServerConnection::new(config).map_err(io::Error::other)?;
    let mut s = StreamOwned::new(conn, stream);
    s.flush()?;
    Ok(s)
}

fn remaining_length(mut n: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut b = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            b |= 128;
        }
        out.push(b);
        if n == 0 {
            break;
        }
    }
    out
}

fn read_packet(s: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut b = [0];
    s.read_exact(&mut b)?;
    let h = b[0];
    let mut len = 0usize;
    let mut shift = 0;
    loop {
        s.read_exact(&mut b)?;
        len += (b[0] as usize & 127) << shift;
        if b[0] & 128 == 0 {
            break;
        }
        shift += 7;
    }
    let mut v = vec![0; len];
    s.read_exact(&mut v)?;
    let mut out = vec![h];
    out.extend(v);
    Ok(out)
}

fn packet_id(packet: &[u8]) -> u16 {
    u16::from_be_bytes([packet[1], packet[2]])
}

fn parse_publish(packet: &[u8]) -> (Vec<u8>, u16, Vec<u8>, u8) {
    let qos = (packet[0] >> 1) & 0x3;
    let mut i = 1;
    let tlen = u16::from_be_bytes([packet[i], packet[i + 1]]) as usize;
    i += 2;
    let topic = packet[i..i + tlen].to_vec();
    i += tlen;
    let id = if qos > 0 {
        let id = u16::from_be_bytes([packet[i], packet[i + 1]]);
        i += 2;
        id
    } else {
        0
    };
    (topic, id, packet[i..].to_vec(), qos)
}

fn write_publish(
    s: &mut impl Write,
    topic: &[u8],
    payload: &[u8],
    qos: u8,
    id: u16,
) -> io::Result<()> {
    let mut body = Vec::new();
    body.extend((topic.len() as u16).to_be_bytes());
    body.extend(topic);
    if qos > 0 {
        body.extend(id.to_be_bytes());
    }
    body.extend(payload);
    s.write_all(&[0x30 | (qos << 1)])?;
    s.write_all(&remaining_length(body.len()))?;
    s.write_all(&body)?;
    s.flush()
}

fn mqtt_echo(stream: &mut (impl Read + Write)) -> io::Result<()> {
    loop {
        let packet = match read_packet(stream) {
            Ok(packet) => packet,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(err) => return Err(err),
        };
        match packet[0] & 0xf0 {
            0x10 => stream.write_all(&[0x20, 2, 0, 0])?,
            0x80 => {
                let id = packet_id(&packet);
                stream.write_all(&[0x90, 3, (id >> 8) as u8, id as u8, 1])?;
                stream.flush()?;
            }
            0x30 => {
                let (topic, id, payload, qos) = parse_publish(&packet);
                if qos == 1 {
                    stream.write_all(&[0x40, 2, (id >> 8) as u8, id as u8])?;
                }
                write_publish(stream, &topic, &payload, 1, 7)?;
            }
            0x40 | 0xc0 => {}
            0xe0 => return Ok(()),
            _ => {}
        }
        stream.flush()?;
    }
}

fn with_runtime<T>(f: impl FnOnce() -> T) -> T {
    mqtt_runtime::init().unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    let _ = mqtt_runtime::shutdown();
    match result {
        Ok(v) => v,
        Err(p) => std::panic::resume_unwind(p),
    }
}

fn connect_mtls(
    client: &mut Client,
    id: &str,
    port: u16,
    ca: Vec<u8>,
    cert: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
) -> Result<(), Error> {
    let mut options = Options::new("127.0.0.1", id, port);
    options.use_tls = true;
    options.ca_pem = Some(ca);
    options.client_cert_pem = cert;
    options.client_key_pem = key;
    client.connect(1, options)
}

fn assert_no_secrets(text: &str, certs: &Certs) {
    for name in [
        "ca.pem",
        "ca.key",
        "other-ca.pem",
        "other-ca.key",
        "server-127.pem",
        "server-127.key",
        "client-ok.pem",
        "client-ok.key",
        "client-other.pem",
        "client-other.key",
    ] {
        let body = String::from_utf8(certs.pem(name)).unwrap();
        assert!(!text.contains(&body), "secret material from {name} leaked");
    }
    assert!(!text.contains("BEGIN CERTIFICATE"), "{text}");
    assert!(!text.contains("BEGIN PRIVATE KEY"), "{text}");
    assert!(!text.contains("BEGIN RSA PRIVATE KEY"), "{text}");
    assert!(!text.contains("-----BEGIN"), "{text}");
}

fn require_server(certs: &Certs, mode: Mode) -> Server {
    Server::start(
        mode,
        &certs.path("server-127.pem"),
        &certs.path("server-127.key"),
        &certs.path("ca.pem"),
    )
}

fn fail_once(id: &str, cert: Option<&str>, key: Option<&str>, expected: Error) {
    with_runtime(|| {
        let certs = Certs::generate();
        let server = require_server(&certs, Mode::HandshakeOnly);
        let mut client = Client::new().unwrap();
        assert_eq!(
            connect_mtls(
                &mut client,
                id,
                server.port,
                certs.pem("ca.pem"),
                cert.map(|n| certs.pem(n)),
                key.map(|n| certs.pem(n)),
            ),
            Ok(())
        );
        let failure = next(&client);
        assert_eq!(failure.kind, Kind::Failure);
        assert_eq!(
            failure.code, expected as i32,
            "unexpected failure code {failure:?}"
        );
        assert!(failure.topic.is_empty());
        assert!(failure.payload.is_empty());
        let text = format!("{failure:?} {expected} {expected:?}");
        assert_no_secrets(&text, &certs);
        server.assert_no_retry();
        assert_eq!(client.pop().unwrap_err(), Error::Empty);
        drop(client);
    });
}

#[test]
fn required_client_certificate_connects_and_qos1() {
    with_runtime(|| {
        let certs = Certs::generate();
        let server = require_server(&certs, Mode::EchoQos1);
        let mut client = Client::new().unwrap();
        assert_eq!(
            connect_mtls(
                &mut client,
                "mtls-ok",
                server.port,
                certs.pem("ca.pem"),
                Some(certs.pem("client-ok.pem")),
                Some(certs.pem("client-ok.key")),
            ),
            Ok(())
        );
        let connected = next(&client);
        assert_eq!(connected.kind, Kind::Connected);
        assert_eq!(connected.op, 1);
        assert!(!connected.session_present);
        assert_eq!(client.subscribe(2, "test/mtls".into(), 1), Ok(()));
        let sub = next(&client);
        assert_eq!(sub.kind, Kind::Subscribed);
        assert_eq!(sub.op, 2);
        assert_eq!(
            client.publish(3, "test/mtls".into(), b"hello-mtls".to_vec(), 1, false),
            Ok(())
        );
        let mut queued = false;
        let mut sent = false;
        let mut acked = false;
        let mut message = false;
        let end = Instant::now() + Duration::from_secs(8);
        while Instant::now() < end && !(queued && sent && acked && message) {
            match client.pop() {
                Ok(event) => match event.kind {
                    Kind::Queued => {
                        assert_eq!(event.op, 3);
                        queued = true;
                    }
                    Kind::Sent => {
                        assert!(queued);
                        sent = true;
                    }
                    Kind::Acknowledged => {
                        assert!(sent);
                        acked = true;
                    }
                    Kind::Message => {
                        assert_eq!(event.topic, b"test/mtls");
                        assert_eq!(event.payload, b"hello-mtls");
                        assert_eq!(event.qos, 1);
                        message = true;
                    }
                    other => panic!("unexpected {other:?}"),
                },
                Err(Error::Empty) => thread::sleep(Duration::from_millis(5)),
                Err(e) => panic!("{e:?}"),
            }
        }
        assert!(queued && sent && acked && message, "missing qos1 events");
        client.close().unwrap();
    });
}

#[test]
fn missing_client_certificate_fails_without_retry() {
    fail_once("mtls-missing", None, None, Error::Tls);
}

#[test]
fn untrusted_client_certificate_fails_without_retry() {
    fail_once(
        "mtls-untrusted",
        Some("client-other.pem"),
        Some("client-other.key"),
        Error::Tls,
    );
}

#[test]
fn mismatched_client_certificate_and_key_rejected_before_connect() {
    with_runtime(|| {
        let certs = Certs::generate();
        let mut client = Client::new().unwrap();
        let err = connect_mtls(
            &mut client,
            "mtls-mismatch",
            8883,
            certs.pem("ca.pem"),
            Some(certs.pem("client-ok.pem")),
            Some(certs.pem("client-other.key")),
        )
        .unwrap_err();
        assert_eq!(err, Error::CertificateKeyMismatch);
        assert_no_secrets(&format!("{err} {err:?}"), &certs);
        client.close().unwrap();
    });
}

#[test]
fn invalid_client_certificate_pem_rejected_before_connect() {
    with_runtime(|| {
        let certs = Certs::generate();
        let mut client = Client::new().unwrap();
        let err = connect_mtls(
            &mut client,
            "mtls-bad-cert",
            8883,
            certs.pem("ca.pem"),
            Some(b"this is not a certificate".to_vec()),
            Some(certs.pem("client-ok.key")),
        )
        .unwrap_err();
        assert_eq!(err, Error::InvalidClientCertificate);
        assert_no_secrets(&format!("{err} {err:?}"), &certs);
        client.close().unwrap();
    });
}

#[test]
fn invalid_client_key_pem_rejected_before_connect() {
    with_runtime(|| {
        let certs = Certs::generate();
        let mut client = Client::new().unwrap();
        let err = connect_mtls(
            &mut client,
            "mtls-bad-key",
            8883,
            certs.pem("ca.pem"),
            Some(certs.pem("client-ok.pem")),
            Some(b"this is not a private key".to_vec()),
        )
        .unwrap_err();
        assert_eq!(err, Error::InvalidClientCertificate);
        assert_no_secrets(&format!("{err} {err:?}"), &certs);
        client.close().unwrap();
    });
}

#[test]
fn client_identity_without_tls_is_rejected() {
    with_runtime(|| {
        let certs = Certs::generate();
        let mut client = Client::new().unwrap();
        let mut options = Options::new("127.0.0.1", "mtls-plain", 1883);
        options.client_cert_pem = Some(certs.pem("client-ok.pem"));
        options.client_key_pem = Some(certs.pem("client-ok.key"));
        assert_eq!(client.connect(1, options), Err(Error::InvalidArgument));
        client.close().unwrap();
    });
}

#[test]
fn mtls_secrets_stay_out_of_debug() {
    with_runtime(|| {
        let certs = Certs::generate();
        let mut options = Options::new("127.0.0.1", "redact", 1);
        options.use_tls = true;
        options.ca_pem = Some(certs.pem("ca.pem"));
        options.client_cert_pem = Some(certs.pem("client-ok.pem"));
        options.client_key_pem = Some(certs.pem("client-ok.key"));
        assert_no_secrets(&format!("{options:?}"), &certs);
    });
}
