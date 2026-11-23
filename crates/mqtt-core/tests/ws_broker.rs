//! Actual Mosquitto native WS/WSS integration for both protocols (no relay).
mod common;
use common::next;
use mqtt_api::{Error, Options};
use mqtt_core::{Client, Kind};
use std::{
    fs,
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
struct Broker {
    child: Child,
    dir: PathBuf,
    port: u16,
}
impl Broker {
    fn start(tls: bool, stem: &str, require_client: bool) -> Self {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let dir = std::env::temp_dir().join(format!(
            "mqtt-ws-broker-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let mut config =
            format!("listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\nprotocol websockets\n");
        if tls {
            let python = std::env::var("MQTT_TEST_PYTHON").unwrap_or_else(|_| "python3".into());
            assert!(Command::new(python)
                .arg(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../../scripts/gen_test_certs.py"
                ))
                .arg(&dir)
                .stdout(Stdio::null())
                .status()
                .unwrap()
                .success());

            config.push_str(&format!(
                "certfile {}\nkeyfile {}\n",
                dir.join(format!("{stem}.pem")).display(),
                dir.join(format!("{stem}.key")).display()
            ));
        }
        if require_client {
            config.push_str(&format!(
                "cafile {}\nrequire_certificate true\n",
                dir.join("ca.pem").display()
            ));
        }
        let path = dir.join("mosquitto.conf");
        fs::write(&path, config).unwrap();
        let exe = std::env::var("MQTT5_BROKER").unwrap_or_else(|_| "/usr/sbin/mosquitto".into());
        let mut child = Command::new(exe)
            .arg("-c")
            .arg(path)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("install Mosquitto or set MQTT5_BROKER");
        let end = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(
                child.try_wait().unwrap().is_none(),
                "Mosquitto exited at startup"
            );
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            assert!(Instant::now() < end, "broker startup deadline");
            thread::sleep(Duration::from_millis(10));
        }
        Self { child, dir, port }
    }
    fn options(&self, tls: bool, protocol: u8) -> Options {
        let mut o = Options::new("127.0.0.1", "v5-real", self.port);
        o.protocol = protocol;
        o.use_websocket = true;
        o.websocket_path = "/mqtt".into();
        o.keep_alive = if protocol == 5 {
            Duration::from_secs(5)
        } else {
            Duration::ZERO
        };
        o.use_tls = tls;
        if tls {
            o.ca_pem = Some(fs::read(self.dir.join("ca.pem")).unwrap());
        }
        o
    }
}
impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.dir);
    }
}
#[test]
fn mosquitto_native_ws_wss_both_protocols_binary_qos0_qos1_and_unsubscribe() {
    let _guard = LOCK.lock().unwrap();
    mqtt_runtime::init().unwrap();
    for protocol in [4, 5] {
        for tls in [false, true] {
            let b = Broker::start(tls, "server-127", false);
            let mut c = Client::new().unwrap();
            c.connect(1, b.options(tls, protocol)).unwrap();
            assert_eq!(next(&c).kind, Kind::Connected);
            c.subscribe(2, "v5/binary".into(), 1).unwrap();
            assert_eq!(next(&c).kind, Kind::Subscribed);
            for q in [0, 1] {
                c.publish(3 + q as u64, "v5/binary".into(), vec![0, 255, 7], q, false)
                    .unwrap();
                let mut ack = q == 0;
                let mut msg = false;
                let mut sent = false;
                let mut queued = false;
                while !(ack && msg && sent && queued) {
                    let e = next(&c);
                    eprintln!("tls={tls} qos={q} {e:?}");
                    match e.kind {
                        Kind::Queued => queued = true,
                        Kind::Sent => sent = true,
                        Kind::Acknowledged => ack = true,
                        Kind::Message => {
                            assert_eq!(e.payload, vec![0, 255, 7]);
                            msg = true;
                        }
                        _ => panic!("unexpected {e:?}"),
                    }
                }
            }
            c.unsubscribe(5, "v5/binary".into()).unwrap();
            assert_eq!(next(&c).kind, Kind::Unsubscribed);
            c.begin_drain(Duration::from_secs(1)).unwrap();
            let end = Instant::now() + Duration::from_secs(2);
            while c.drain_status().unwrap() == mqtt_core::DrainStatus::Pending {
                assert!(Instant::now() < end);
                thread::sleep(Duration::from_millis(5));
            }
            assert_eq!(c.drain_status().unwrap(), mqtt_core::DrainStatus::Drained);
            c.close().unwrap();
        }
    }
    mqtt_runtime::shutdown().unwrap();
}
#[test]
fn mosquitto_wss_untrusted_and_hostname_fail_once() {
    let _guard = LOCK.lock().unwrap();
    mqtt_runtime::init().unwrap();
    for protocol in [4, 5] {
        for (stem, expected) in [
            ("server-127", Error::CertificateUntrusted),
            ("server-wrong-host", Error::HostnameMismatch),
            ("server-expired", Error::CertificateExpired),
        ] {
            let b = Broker::start(true, stem, false);
            let mut o = b.options(true, protocol);
            if expected == Error::CertificateUntrusted {
                o.ca_pem = None;
            }
            o.reconnect.enabled = true;
            let mut c = Client::new().unwrap();
            c.connect(1, o).unwrap();
            let e = next(&c);
            assert_eq!(e.kind, Kind::Failure);
            assert_eq!(e.code, expected as i32);
            assert!(common::try_next(&c, Duration::from_millis(100)).is_none());
            c.close().unwrap();
        }
    }
    mqtt_runtime::shutdown().unwrap();
}

#[test]
fn native_wss_mtls_identity_and_plaintext_password_guard() {
    let _guard = LOCK.lock().unwrap();
    mqtt_runtime::init().unwrap();
    for protocol in [4, 5] {
        let b = Broker::start(true, "server-127", true);
        let mut o = b.options(true, protocol);
        o.client_cert_pem = Some(fs::read(b.dir.join("client-ok.pem")).unwrap());
        o.client_key_pem = Some(fs::read(b.dir.join("client-ok.key")).unwrap());
        let mut c = Client::new().unwrap();
        c.connect(1, o).unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        c.disconnect(2).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        c.close().unwrap();
        let mut o = b.options(false, protocol);
        o.ca_pem = None;
        o.username = Some("fixture".into());
        o.password = Some("never-sent".into());
        let mut c = Client::new().unwrap();
        assert_eq!(c.connect(1, o), Err(Error::PlaintextCredentials));
        c.close().unwrap();
    }
    mqtt_runtime::shutdown().unwrap();
}
