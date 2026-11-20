mod common;
use common::{next, TestBroker};
use mqtt_api::{Error, Options};
use mqtt_core::{Client, Kind};
use std::{
    fs,
    io::Write,
    net::TcpListener,
    path::PathBuf,
    process::Command,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const SECRET: &str = "core-fixture-secret-7f3a";
const USER: &str = "core-test-user";

fn with_runtime<T>(f: impl FnOnce() -> T) -> T {
    mqtt_runtime::init().unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    let _ = mqtt_runtime::shutdown();
    match result {
        Ok(v) => v,
        Err(p) => std::panic::resume_unwind(p),
    }
}

fn assert_no_secret(text: &str) {
    assert!(!text.contains(SECRET), "secret material leaked");
    assert!(!text.to_ascii_lowercase().contains("core-fixture-secret"));
}

struct Certs(PathBuf);
impl Certs {
    fn generate() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("mqtt-auth-{}-{stamp}", std::process::id()));
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

fn credentials(options: &mut Options, allow_plain: bool) {
    options.username = Some(USER.into());
    options.password = Some(SECRET.into());
    options.allow_plaintext_credentials = allow_plain;
}

#[test]
fn correct_plaintext_credentials_connect_when_explicitly_allowed() {
    with_runtime(|| {
        let broker = TestBroker::start(Some((USER, SECRET)), None);
        let mut client = Client::new().unwrap();
        let mut options = Options::new("127.0.0.1", "auth-ok", broker.plain_port);
        credentials(&mut options, true);
        client.connect(1, options).unwrap();
        let event = next(&client);
        assert_eq!(event.kind, Kind::Connected);
        assert_eq!(event.op, 1);
        assert_eq!(event.code, 0);
        assert_no_secret(&format!("{event:?}"));
    });
}

#[test]
fn wrong_plaintext_credentials_are_rejected_once() {
    with_runtime(|| {
        let broker = TestBroker::start(Some((USER, SECRET)), None);
        let mut client = Client::new().unwrap();
        let mut options = Options::new("127.0.0.1", "auth-bad", broker.plain_port);
        options.username = Some(USER.into());
        options.password = Some("core-fixture-wrong".into());
        options.allow_plaintext_credentials = true;
        client.connect(1, options).unwrap();
        let failure = next(&client);
        assert_eq!(failure.kind, Kind::Failure);
        assert_eq!(failure.code, Error::BadCredentials as i32);
        assert!(failure.topic.is_empty());
        assert!(failure.payload.is_empty());
        assert_no_secret(&format!("{failure:?}"));
        assert_eq!(client.pop().unwrap_err(), Error::Empty);
    });
}

#[test]
fn refused_auth_connack_does_not_retry() {
    with_runtime(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            common::packet(&mut socket);
            socket.write_all(&[0x20, 2, 0, 4]).unwrap();
            drop(socket);
            listener.set_nonblocking(true).unwrap();
            let until = Instant::now() + Duration::from_millis(200);
            while Instant::now() < until {
                assert!(listener.accept().is_err(), "first failure retried");
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        let mut client = Client::new().unwrap();
        let mut options = Options::new("127.0.0.1", "auth-connack", port);
        credentials(&mut options, true);
        client.connect(1, options).unwrap();
        let failure = next(&client);
        assert_eq!(failure.kind, Kind::Failure);
        assert_eq!(failure.code, Error::BadCredentials as i32);
        assert_no_secret(&format!("{failure:?}"));
        server.join().unwrap();
        assert_eq!(client.pop().unwrap_err(), Error::Empty);
    });
}

#[test]
fn tls_password_connects_without_plaintext_allow() {
    with_runtime(|| {
        let certs = Certs::generate();
        let broker = TestBroker::start(
            Some((USER, SECRET)),
            Some((&certs.path("server-127.pem"), &certs.path("server-127.key"))),
        );
        let mut client = Client::new().unwrap();
        let mut options = Options::new("127.0.0.1", "auth-tls", broker.tls_port.unwrap());
        options.use_tls = true;
        options.ca_pem = Some(certs.pem("ca.pem"));
        credentials(&mut options, false);
        client.connect(1, options).unwrap();
        let event = next(&client);
        assert_eq!(event.kind, Kind::Connected);
        assert_no_secret(&format!("{event:?}"));
        let pem = String::from_utf8(certs.pem("ca.pem")).unwrap();
        assert!(!format!("{event:?}").contains(&pem));
    });
}

#[test]
fn plaintext_password_without_allow_is_rejected_before_connect() {
    with_runtime(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut client = Client::new().unwrap();
        let mut options = Options::new("127.0.0.1", "auth-denied", port);
        credentials(&mut options, false);
        assert_eq!(client.connect(1, options), Err(Error::PlaintextCredentials));
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            listener.accept().is_err(),
            "plaintext password connected without allow"
        );
        assert_eq!(client.pop().unwrap_err(), Error::Empty);
    });
}

#[test]
fn failure_and_options_text_do_not_include_password() {
    let mut options = Options::new("127.0.0.1", "auth-debug", 1883);
    credentials(&mut options, true);
    let text = format!(
        "{options:?} {} {:?}",
        Error::BadCredentials,
        Error::PlaintextCredentials
    );
    assert_no_secret(&text);
    assert!(text.contains("<redacted>"));
}
