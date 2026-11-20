use mqtt_api::Error;
use mqtt_core::Client;
use std::{
    io::Read,
    time::{Duration, Instant},
};
#[allow(dead_code)]
pub fn packet(s: &mut impl Read) -> Vec<u8> {
    let mut b = [0];
    s.read_exact(&mut b).unwrap();
    let h = b[0];
    let mut len = 0;
    let mut shift = 0;
    loop {
        s.read_exact(&mut b).unwrap();
        len += (b[0] as usize & 127) << shift;
        if b[0] & 128 == 0 {
            break;
        }
        shift += 7;
    }
    let mut v = vec![0; len];
    s.read_exact(&mut v).unwrap();
    let mut out = vec![h];
    out.extend(v);
    out
}
pub fn next(c: &Client) -> mqtt_core::Event {
    let end = Instant::now() + Duration::from_secs(8);
    loop {
        match c.pop() {
            Ok(e) => return e,
            Err(Error::Empty) => {
                assert!(Instant::now() < end, "event timeout");
                std::thread::sleep(Duration::from_millis(5));
            }
            e => panic!("{e:?}"),
        }
    }
}

#[allow(dead_code)]
pub struct TestBroker {
    pub plain_port: u16,
    pub tls_port: Option<u16>,
    child: Option<std::process::Child>,
    ready: std::path::PathBuf,
}

#[allow(dead_code)]
impl TestBroker {
    pub fn start(
        auth: Option<(&str, &str)>,
        tls: Option<(&std::path::Path, &std::path::Path)>,
    ) -> Self {
        Self::start_with(auth, tls, None)
    }

    pub fn start_with(
        auth: Option<(&str, &str)>,
        tls: Option<(&std::path::Path, &std::path::Path)>,
        client_ca: Option<&std::path::Path>,
    ) -> Self {
        use std::{
            fs,
            process::{Command, Stdio},
            thread,
            time::{Duration, Instant, SystemTime, UNIX_EPOCH},
        };
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let ready =
            std::env::temp_dir().join(format!("mqtt-auth-ready-{}-{stamp}", std::process::id()));
        let _ = fs::remove_file(&ready);
        let script = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../scripts/with_broker.py");
        let python = std::env::var("MQTT_TEST_PYTHON").unwrap_or_else(|_| "python3".into());
        let mut command = Command::new(&python);
        command
            .arg(&script)
            .arg("--serve")
            .arg(&ready)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some((user, password)) = auth {
            command.env("MQTT_TEST_AUTH_USERNAME", user);
            command.env("MQTT_TEST_AUTH_PASSWORD", password);
        }
        if let Some((cert, key)) = tls {
            command.env("MQTT_TEST_TLS_CERT", cert);
            command.env("MQTT_TEST_TLS_KEY", key);
        }
        if let Some(ca) = client_ca {
            command.env("MQTT_TEST_TLS_CA", ca);
            command.env("MQTT_TEST_REQUIRE_CLIENT_CERT", "1");
        }
        let child = command.spawn().expect("start with_broker.py");
        let deadline = Instant::now() + Duration::from_secs(15);
        while !ready.exists() {
            assert!(
                Instant::now() < deadline,
                "broker ready file was not created"
            );
            thread::sleep(Duration::from_millis(20));
        }
        let body = fs::read_to_string(&ready).expect("read ready file");
        assert!(
            !body.to_ascii_lowercase().contains("password"),
            "ready file must not mention credentials"
        );
        let mut plain_port = 0;
        let mut tls_port = None;
        for line in body.lines() {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            let port: u16 = value.parse().expect("port");
            match key {
                "plain_port" => plain_port = port,
                "tls_port" => tls_port = Some(port),
                "port" if tls.is_some() => tls_port = Some(port),
                "port" => plain_port = port,
                _ => {}
            }
        }
        assert!(plain_port != 0, "missing plain port");
        if tls.is_some() {
            assert!(tls_port.is_some(), "missing tls port");
        }
        Self {
            plain_port,
            tls_port,
            child: Some(child),
            ready,
        }
    }
}

impl Drop for TestBroker {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            drop(child.stdin.take());
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_file(&self.ready);
    }
}
