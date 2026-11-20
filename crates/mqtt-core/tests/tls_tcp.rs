mod common;
use common::next;
use mqtt_api::{Error, Options};
use mqtt_core::{Client, Kind};
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use std::{
    fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc,
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
        let dir = std::env::temp_dir().join(format!("mqtt-tls-{}-{stamp}", std::process::id()));
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

fn server_config(cert: &Path, key: &Path) -> Arc<ServerConfig> {
    let certs = rustls_pemfile::certs(&mut io::BufReader::new(fs::File::open(cert).unwrap()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = rustls_pemfile::private_key(&mut io::BufReader::new(fs::File::open(key).unwrap()))
        .unwrap()
        .expect("server key");
    Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap(),
    )
}

enum Mode {
    EchoQos1,
    HandshakeOnly,
    Plaintext,
}

struct Server {
    port: u16,
    accepts: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}
impl Server {
    fn start(mode: Mode, cert: Option<(&Path, &Path)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepts = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let tls = cert.map(|(c, k)| server_config(c, k));
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
                            Mode::Plaintext => {
                                let _ = stream;
                            }
                            Mode::HandshakeOnly => {
                                if let Some(config) = &tls {
                                    let _ = tls_handshake(stream, config.clone());
                                }
                            }
                            Mode::EchoQos1 => {
                                if let Some(config) = &tls {
                                    if let Ok(mut s) = tls_handshake(stream, config.clone()) {
                                        let _ = mqtt_echo(&mut s);
                                    }
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

fn connect_tls(client: &mut Client, id: &str, port: u16, ca: Option<Vec<u8>>) -> Result<(), Error> {
    let mut options = Options::new("127.0.0.1", id, port);
    options.use_tls = true;
    options.ca_pem = ca;
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
        "server-expired.pem",
        "server-expired.key",
        "server-wrong-host.pem",
        "server-wrong-host.key",
    ] {
        let body = String::from_utf8(certs.pem(name)).unwrap();
        assert!(!text.contains(&body), "secret material from {name} leaked");
    }
    assert!(!text.contains("BEGIN CERTIFICATE"), "{text}");
    assert!(!text.contains("BEGIN PRIVATE KEY"), "{text}");
    assert!(!text.contains("BEGIN RSA PRIVATE KEY"), "{text}");
    assert!(!text.contains("-----BEGIN"), "{text}");
}

fn fail_once(id: &str, mode: Mode, server_name: &str, ca_name: Option<&str>, expected: Error) {
    with_runtime(|| {
        let certs = Certs::generate();
        let server = match mode {
            Mode::Plaintext => Server::start(Mode::Plaintext, None),
            _ => Server::start(
                mode,
                Some((
                    &certs.path(server_name),
                    &certs.path(&server_name.replace(".pem", ".key")),
                )),
            ),
        };
        let mut client = Client::new().unwrap();
        let ca = ca_name.map(|n| certs.pem(n));
        assert_eq!(
            connect_tls(&mut client, id, server.port, ca.clone()),
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
        if let Some(pem) = &ca {
            assert!(!text.contains(String::from_utf8_lossy(pem).as_ref()));
        }
        assert_no_secrets(&text, &certs);
        server.assert_no_retry();
        assert_eq!(client.pop().unwrap_err(), Error::Empty);
        drop(client);
    });
}

#[test]
fn trusted_ca_and_matching_hostname_connect_and_qos1() {
    with_runtime(|| {
        let certs = Certs::generate();
        let server = Server::start(
            Mode::EchoQos1,
            Some((&certs.path("server-127.pem"), &certs.path("server-127.key"))),
        );
        let mut client = Client::new().unwrap();
        assert_eq!(
            connect_tls(
                &mut client,
                "tls-ok",
                server.port,
                Some(certs.pem("ca.pem"))
            ),
            Ok(())
        );
        let connected = next(&client);
        assert_eq!(connected.kind, Kind::Connected);
        assert_eq!(connected.op, 1);
        assert!(!connected.session_present);
        assert_eq!(client.subscribe(2, "test/tls".into(), 1), Ok(()));
        let sub = next(&client);
        assert_eq!(sub.kind, Kind::Subscribed);
        assert_eq!(sub.op, 2);
        assert_eq!(
            client.publish(3, "test/tls".into(), b"hello-tls".to_vec(), 1, false),
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
                        assert_eq!(event.topic, b"test/tls");
                        assert_eq!(event.payload, b"hello-tls");
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
fn untrusted_ca_fails_without_retry() {
    fail_once(
        "tls-untrusted",
        Mode::HandshakeOnly,
        "server-127.pem",
        Some("other-ca.pem"),
        Error::CertificateUntrusted,
    );
}

#[test]
fn hostname_mismatch_fails_without_retry() {
    fail_once(
        "tls-hostname",
        Mode::HandshakeOnly,
        "server-wrong-host.pem",
        Some("ca.pem"),
        Error::HostnameMismatch,
    );
}

#[test]
fn expired_certificate_fails_without_retry() {
    fail_once(
        "tls-expired",
        Mode::HandshakeOnly,
        "server-expired.pem",
        Some("ca.pem"),
        Error::CertificateExpired,
    );
}

#[test]
fn tls_to_plaintext_port_fails_without_retry() {
    fail_once(
        "tls-plain",
        Mode::Plaintext,
        "server-127.pem",
        Some("ca.pem"),
        Error::Tls,
    );
}

#[test]
fn system_roots_do_not_trust_fixture_ca() {
    fail_once(
        "tls-system",
        Mode::HandshakeOnly,
        "server-127.pem",
        None,
        Error::CertificateUntrusted,
    );
}

#[test]
fn tls_failure_secrets_stay_out_of_debug() {
    with_runtime(|| {
        let certs = Certs::generate();
        let mut options = Options::new("127.0.0.1", "redact", 1);
        options.use_tls = true;
        options.ca_pem = Some(certs.pem("ca.pem"));
        assert_no_secrets(&format!("{options:?}"), &certs);
        let _ = mpsc::channel::<()>();
    });
}

#[test]
fn invalid_custom_ca_pem_is_rejected_before_connect() {
    with_runtime(|| {
        let mut client = Client::new().unwrap();
        let mut options = Options::new("127.0.0.1", "bad-pem", 1883);
        options.use_tls = true;
        options.ca_pem = Some(b"this is not a certificate".to_vec());
        assert_eq!(client.connect(1, options), Err(Error::InvalidArgument));
        client.close().unwrap();
    });
}
