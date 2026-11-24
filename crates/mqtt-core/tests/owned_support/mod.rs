use mqtt_api::Options;
use std::{
    fs,
    io::{self, Cursor, Read, Write},
    net::TcpListener,
    path::PathBuf,
    process::Command,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
trait Io: Read + Write + Send {}
impl<T: Read + Write + Send> Io for T {}
pub struct Certs {
    pub dir: PathBuf,
    pub config: Arc<rustls::ServerConfig>,
}
impl Certs {
    pub fn new() -> Self {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let dir = std::env::temp_dir().join(format!(
            "mqtt-own-fault-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let p = std::env::var("MQTT_TEST_PYTHON").unwrap_or("python3".into());
        assert!(Command::new(p)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../scripts/gen_test_certs.py"
            ))
            .arg(&dir)
            .status()
            .unwrap()
            .success());
        let c = rustls_pemfile::certs(&mut Cursor::new(
            fs::read(dir.join("server-127.pem")).unwrap(),
        ))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let k = rustls_pemfile::private_key(&mut Cursor::new(
            fs::read(dir.join("server-127.key")).unwrap(),
        ))
        .unwrap()
        .unwrap();
        let config = Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(c, k)
                .unwrap(),
        );
        Self { dir, config }
    }
    pub fn options(&self, port: u16, proto: u8, tls: bool, ws: bool) -> Options {
        let mut o = Options::new("127.0.0.1", "owned-wire", port);
        o.protocol = proto;
        o.use_tls = tls;
        o.use_websocket = ws;
        if proto == 5 {
            o.keep_alive = Duration::from_secs(5)
        }
        if tls {
            o.ca_pem = Some(fs::read(self.dir.join("ca.pem")).unwrap());
        }
        o
    }
}
impl Drop for Certs {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}
pub struct Peer {
    wire: Wire,
}
enum Wire {
    Raw(Box<dyn Io>),
    Ws(Box<async_tungstenite::tungstenite::WebSocket<Box<dyn Io>>>),
}
pub fn packet(s: &mut impl Read) -> Vec<u8> {
    let mut b = [0];
    s.read_exact(&mut b).unwrap();
    let mut out = vec![b[0]];
    let (mut n, mut mul) = (0, 1);
    loop {
        s.read_exact(&mut b).unwrap();
        n += (b[0] as usize & 127) * mul;
        if b[0] & 128 == 0 {
            break;
        }
        mul *= 128
    }
    let mut body = vec![0; n];
    s.read_exact(&mut body).unwrap();
    out.append(&mut body);
    out
}
impl Peer {
    pub fn accept(l: &TcpListener, tls: bool, ws: bool, c: Arc<rustls::ServerConfig>) -> Self {
        Self::accept_inner(l, tls, ws, c, false)
    }
    // Only Linux resource fixtures request a closed receive window.
    #[allow(dead_code)]
    pub fn accept_small_receive(
        l: &TcpListener,
        tls: bool,
        ws: bool,
        c: Arc<rustls::ServerConfig>,
    ) -> Self {
        Self::accept_inner(l, tls, ws, c, true)
    }
    fn accept_inner(
        l: &TcpListener,
        tls: bool,
        ws: bool,
        c: Arc<rustls::ServerConfig>,
        small: bool,
    ) -> Self {
        let (s, _) = l.accept().unwrap();
        #[cfg(target_os = "linux")]
        if small {
            use std::os::fd::AsRawFd;
            unsafe extern "C" {
                fn setsockopt(
                    fd: i32,
                    level: i32,
                    opt: i32,
                    value: *const std::ffi::c_void,
                    len: u32,
                ) -> i32;
            }
            let size = 1024i32;
            // SAFETY: live Linux TCP socket; initialized i32 and exact socklen.
            assert_eq!(
                unsafe { setsockopt(s.as_raw_fd(), 1, 8, (&size as *const i32).cast(), 4) },
                0
            );
        }
        #[cfg(not(target_os = "linux"))]
        assert!(!small, "Linux-only pressure fixture");
        s.set_nodelay(true).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        s.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
        let s: Box<dyn Io> = if tls {
            Box::new(rustls::StreamOwned::new(
                rustls::ServerConnection::new(c).unwrap(),
                s,
            ))
        } else {
            Box::new(s)
        };
        let wire = if ws {
            let w = async_tungstenite::tungstenite::accept_hdr(s, MqttTestProtocol).unwrap();
            Wire::Ws(Box::new(w))
        } else {
            Wire::Raw(s)
        };
        Self { wire }
    }
    pub fn packet(&mut self) -> Vec<u8> {
        match &mut self.wire {
            Wire::Raw(s) => packet(s),
            Wire::Ws(s) => loop {
                let b = s.read().unwrap();
                if b.is_binary() {
                    return packet(&mut Cursor::new(b.into_data()));
                }
            },
        }
    }
    pub fn send(&mut self, b: &[u8]) {
        match &mut self.wire {
            Wire::Raw(s) => {
                s.write_all(b).unwrap();
                s.flush().unwrap();
            }
            Wire::Ws(s) => s
                .send(async_tungstenite::tungstenite::Message::Binary(
                    b.to_vec().into(),
                ))
                .unwrap(),
        }
    }
    pub fn connack(&mut self, proto: u8, present: bool) {
        let c = self.packet();
        assert_eq!(c[0], 0x10);
        assert_eq!(c[7], proto);
        assert_eq!(&c[9..11], if proto == 5 { &[0, 5] } else { &[0, 0] });
        let a = if proto == 5 {
            vec![0x20, 3, present as u8, 0, 0]
        } else {
            vec![0x20, 2, present as u8, 0]
        };
        self.send(&a);
    }
    // Shared raw-wire helper: only the resource test binary needs this bypass.
    #[allow(dead_code)]
    pub fn raw_ws(&mut self, b: &[u8]) -> io::Result<()> {
        match &mut self.wire {
            Wire::Ws(s) => {
                s.get_mut().write_all(b)?;
                s.get_mut().flush()
            }
            _ => panic!("not WS"),
        }
    }
    #[allow(dead_code)]
    pub fn closed_after_pongs(&mut self) {
        use async_tungstenite::tungstenite::{error::ProtocolError, Error, Message};
        let Wire::Ws(s) = &mut self.wire else {
            panic!("not WS")
        };
        for _ in 0..100001 {
            match s.read() {
                Ok(Message::Pong(_)) => continue,
                Err(
                    Error::ConnectionClosed
                    | Error::AlreadyClosed
                    | Error::Protocol(ProtocolError::ResetWithoutClosingHandshake),
                ) => return,
                Err(Error::Io(e))
                    if matches!(
                        e.kind(),
                        io::ErrorKind::UnexpectedEof
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    return
                }
                r => panic!("unexpected post-flood packet or missing teardown {r:?}"),
            }
        }
        panic!("unbounded control replies");
    }
    pub fn expect_closed(&mut self) {
        use async_tungstenite::tungstenite::{error::ProtocolError, Error};
        match &mut self.wire {
            Wire::Raw(s) => match s.read(&mut [0]) {
                Ok(0) => {}
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::UnexpectedEof
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::ConnectionAborted
                    ) => {}
                r => panic!("unexpected packet or missing teardown {r:?}"),
            },
            Wire::Ws(s) => match s.read() {
                Err(
                    Error::ConnectionClosed
                    | Error::AlreadyClosed
                    | Error::Protocol(ProtocolError::ResetWithoutClosingHandshake),
                ) => {}
                Err(Error::Io(e))
                    if matches!(
                        e.kind(),
                        io::ErrorKind::UnexpectedEof
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::ConnectionAborted
                    ) => {}
                r => panic!("unexpected packet or missing teardown {r:?}"),
            },
        }
    }
}
pub fn wire(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut b = vec![kind];
    let mut n = body.len();
    loop {
        let mut v = (n % 128) as u8;
        n /= 128;
        if n != 0 {
            v |= 128
        }
        b.push(v);
        if n == 0 {
            break;
        }
    }
    b.extend(body);
    b
}
pub fn id(p: &[u8]) -> [u8; 2] {
    let n = u16::from_be_bytes([p[1], p[2]]) as usize;
    [p[3 + n], p[4 + n]]
}
pub fn next(c: &mqtt_core::Client) -> mqtt_core::Event {
    let end = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        match c.pop() {
            Ok(e) => return e,
            Err(mqtt_api::Error::Empty) => {
                assert!(std::time::Instant::now() < end, "public completion missing");
                std::thread::yield_now()
            }
            e => panic!("{e:?}"),
        }
    }
}
pub fn guard(f: impl FnOnce()) {
    mqtt_runtime::init().unwrap();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    let _ = mqtt_runtime::shutdown();
    if let Err(p) = r {
        std::panic::resume_unwind(p)
    }
}

struct MqttTestProtocol;
impl async_tungstenite::tungstenite::handshake::server::Callback for MqttTestProtocol {
    fn on_request(
        self,
        _: &async_tungstenite::tungstenite::handshake::server::Request,
        mut r: async_tungstenite::tungstenite::handshake::server::Response,
    ) -> Result<
        async_tungstenite::tungstenite::handshake::server::Response,
        async_tungstenite::tungstenite::handshake::server::ErrorResponse,
    > {
        r.headers_mut()
            .insert("Sec-WebSocket-Protocol", "mqtt".parse().unwrap());
        Ok(r)
    }
}
