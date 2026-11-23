//! Owned production transport with finite WS framing, cooperative reads and persistent writes.
use async_tungstenite::{
    tokio::TokioAdapter,
    tungstenite::{client::IntoClientRequest, protocol::WebSocketConfig, Message},
    WebSocketStream,
};
use futures::{Sink, StreamExt};
use mqtt_api::{Error, Options};
use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
pub(crate) const LIMIT: usize = 131080;
pub(crate) const SEND_LIMIT: usize = 393216;
const HANDSHAKE: usize = 16384;
const CONTROL_BATCH: usize = 32;
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
type Stream = Box<dyn Io>;
struct HandshakeBudget {
    reads: usize,
    inner: Stream,
    left: usize,
    enabled: Arc<AtomicBool>,
}
impl AsyncRead for HandshakeBudget {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        // Even zero-length continuation frames must eventually return Pending.
        // Bound each raw read and the number of ready reads between wakeups.
        if self.reads == 4 {
            self.reads = 0;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let limited = self.enabled.load(Ordering::SeqCst);
        if limited && self.left == 0 {
            return Poll::Ready(Err(io::Error::other("handshake input limit")));
        }
        let n = buf
            .remaining()
            .min(8192)
            .min(if limited { self.left } else { usize::MAX });
        let mut small = ReadBuf::new(buf.initialize_unfilled_to(n));
        let result = Pin::new(&mut self.inner).poll_read(cx, &mut small);
        if matches!(result, Poll::Ready(Ok(()))) {
            let read = small.filled().len();
            buf.advance(read);
            if read > 0 {
                self.reads += 1;
            }
            if limited {
                self.left -= read;
            }
        }

        result
    }
}
impl AsyncWrite for HandshakeBudget {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, b)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
enum Wire {
    Raw(Stream),
    Ws(Box<WebSocketStream<TokioAdapter<HandshakeBudget>>>),
}
struct PendingWrite {
    wire: Vec<u8>,
    offset: usize,
    started: bool,
}
pub(crate) struct Transport {
    #[cfg(feature = "ws-test-observer")]
    counted: bool,
    tls: bool,
    pending_write: Option<PendingWrite>,
    wire: Wire,
    data: Vec<u8>,
    pub(crate) control_batches: usize,
}
fn map_ws(e: async_tungstenite::tungstenite::Error) -> Error {
    match e {
        async_tungstenite::tungstenite::Error::Capacity(_)
        | async_tungstenite::tungstenite::Error::WriteBufferFull(_) => Error::Overflow,
        _ => crate::tls::map_websocket_error(&e),
    }
}
impl Transport {
    pub(crate) fn config() -> WebSocketConfig {
        WebSocketConfig::default()
            .read_buffer_size(8192)
            .write_buffer_size(8192)
            .max_write_buffer_size(2 * LIMIT)
            .max_frame_size(Some(LIMIT))
            .max_message_size(Some(LIMIT))
    }
    pub(crate) async fn connect(o: &Options) -> Result<Self, Error> {
        o.validate()?;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let tcp = tokio::net::TcpStream::connect((o.server.as_str(), o.port))
                .await
                .map_err(|e| crate::tls::rustls_from_io(&e).unwrap_or(Error::Network))?;
            tcp.set_nodelay(true)
                .map_err(|e| crate::tls::rustls_from_io(&e).unwrap_or(Error::Network))?;
            let io: Stream = if o.use_tls {
                let config = match crate::tls::transport(o)? {
                    rumqttc::Transport::Tls(rumqttc::TlsConfiguration::Rustls(c))
                    | rumqttc::Transport::Wss(rumqttc::TlsConfiguration::Rustls(c)) => c,
                    _ => return Err(Error::Tls),
                };
                let name = rustls::pki_types::ServerName::try_from(o.server.clone())
                    .map_err(|_| Error::HostnameMismatch)?;
                Box::new(
                    tokio_rustls::TlsConnector::from(config)
                        .connect(name, tcp)
                        .await
                        .map_err(|e| crate::tls::rustls_from_io(&e).unwrap_or(Error::Tls))?,
                )
            } else {
                Box::new(tcp)
            };
            let wire = if o.use_websocket {
                let uri = o.websocket_uri()?;
                let mut request = uri
                    .into_client_request()
                    .map_err(|_| Error::InvalidArgument)?;
                request.headers_mut().insert(
                    "Sec-WebSocket-Protocol",
                    "mqtt".parse().map_err(|_| Error::Internal)?,
                );
                let enabled = Arc::new(AtomicBool::new(true));
                let capped = HandshakeBudget {
                    reads: 0,
                    inner: io,
                    left: HANDSHAKE,
                    enabled: enabled.clone(),
                };
                let (socket, response) = async_tungstenite::tokio::client_async_with_config(
                    request,
                    capped,
                    Some(Self::config()),
                )
                .await
                .map_err(map_ws)?;
                if response
                    .headers()
                    .get("Sec-WebSocket-Protocol")
                    .and_then(|s| s.to_str().ok())
                    != Some("mqtt")
                {
                    return Err(Error::Network);
                }
                enabled.store(false, Ordering::SeqCst);
                Wire::Ws(Box::new(socket))
            } else {
                Wire::Raw(io)
            };
            #[cfg(feature = "ws-test-observer")]
            crate::ws_test_observer::created();
            Ok(Self {
                #[cfg(feature = "ws-test-observer")]
                counted: true,
                pending_write: None,
                tls: o.use_tls,
                wire,
                data: Vec::with_capacity(2 * LIMIT),
                control_batches: 0,
            })
        })
        .await
        .map_err(|_| Error::Timeout)?
    }
    pub(crate) fn queue_send(&mut self, wire: &[u8]) -> Result<(), Error> {
        if wire.len() > SEND_LIMIT {
            return Err(Error::Overflow);
        }
        if self.pending_write.is_some() {
            return Err(Error::Busy);
        }
        self.pending_write = Some(PendingWrite {
            wire: wire.to_vec(),
            offset: 0,
            started: false,
        });
        Ok(())
    }
    pub(crate) async fn flush_pending(&mut self) -> Result<(), Error> {
        let io_error = if self.tls { Error::Tls } else { Error::Network };
        let Some(p) = self.pending_write.as_mut() else {
            return Ok(());
        };
        match &mut self.wire {
            Wire::Raw(s) => {
                while p.offset < p.wire.len() {
                    let n = s
                        .write(&p.wire[p.offset..])
                        .await
                        .map_err(|e| crate::tls::rustls_from_io(&e).unwrap_or(io_error))?;
                    if n == 0 {
                        return Err(io_error);
                    }
                    p.offset += n;
                }
                s.flush()
                    .await
                    .map_err(|e| crate::tls::rustls_from_io(&e).unwrap_or(io_error))?;
            }
            Wire::Ws(s) => {
                while p.offset < p.wire.len() {
                    let end = (p.offset + LIMIT).min(p.wire.len());
                    if !p.started {
                        futures::future::poll_fn(|cx| Pin::new(&mut **s).poll_ready(cx))
                            .await
                            .map_err(map_ws)?;
                        Pin::new(&mut **s)
                            .start_send(Message::Binary(p.wire[p.offset..end].to_vec().into()))
                            .map_err(map_ws)?;
                        p.started = true;
                    }
                    futures::future::poll_fn(|cx| {
                        let result = Pin::new(&mut **s).poll_flush(cx);
                        #[cfg(feature = "ws-test-observer")]
                        crate::ws_test_observer::pending(result.is_pending(), p.wire.capacity());
                        result
                    })
                    .await
                    .map_err(map_ws)?;
                    p.offset = end;
                    p.started = false;
                }
            }
        }
        self.pending_write = None;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) async fn send(&mut self, wire: &[u8]) -> Result<(), Error> {
        self.queue_send(wire)?;
        self.flush_pending().await
    }
    fn length(data: &[u8]) -> Result<Option<usize>, Error> {
        if data.len() < 2 {
            return Ok(None);
        }
        let mut n = 0usize;
        let mut mul = 1;
        for i in 1..=4 {
            let Some(b) = data.get(i) else {
                return Ok(None);
            };
            n += (b & 127) as usize * mul;
            if b & 128 == 0 {
                let total = n + i + 1;
                if total > LIMIT {
                    return Err(Error::Overflow);
                }
                return Ok(Some(total));
            }
            mul *= 128;
        }
        Err(Error::Network)
    }
    pub(crate) async fn packet(&mut self) -> Result<Vec<u8>, Error> {
        let io_error = if self.tls { Error::Tls } else { Error::Network };
        loop {
            if let Some(n) = Self::length(&self.data)? {
                if self.data.len() >= n {
                    let out = self.data[..n].to_vec();
                    self.data.drain(..n);
                    return Ok(out);
                }
            }
            match &mut self.wire {
                Wire::Raw(s) => {
                    let mut chunk = [0; 8192];
                    let available = 2 * LIMIT - self.data.len();
                    if available == 0 {
                        return Err(Error::Overflow);
                    }
                    let n = s
                        .read(&mut chunk[..available.min(8192)])
                        .await
                        .map_err(|e| crate::tls::rustls_from_io(&e).unwrap_or(io_error))?;
                    if n == 0 {
                        return Err(io_error);
                    }
                    self.data.extend_from_slice(&chunk[..n]);
                }
                Wire::Ws(s) => {
                    let mut controls = 0;
                    loop {
                        match s.next().await.ok_or(Error::Network)?.map_err(map_ws)? {
                            Message::Binary(b) => {
                                if b.len() > 2 * LIMIT - self.data.len() {
                                    return Err(Error::Overflow);
                                }
                                self.data.extend_from_slice(&b);
                                break;
                            }
                            Message::Ping(_) | Message::Pong(_) => {
                                controls += 1;
                                if controls == CONTROL_BATCH {
                                    self.control_batches += 1;
                                    tokio::task::yield_now().await;
                                    controls = 0;
                                }
                            }
                            _ => return Err(Error::Network),
                        }
                    }
                }
            }
        }
    }
}
#[cfg(test)]
struct MqttTestProtocol;
#[cfg(test)]
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

#[cfg(feature = "ws-test-observer")]
impl Drop for Transport {
    fn drop(&mut self) {
        if self.counted {
            crate::ws_test_observer::dropped();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };
    fn http(s: &mut std::net::TcpStream) {
        let mut request = Vec::new();
        let mut b = [0];
        while !request.ends_with(b"\r\n\r\n") {
            s.read_exact(&mut b).unwrap();
            request.push(b[0]);
        }
        let text = String::from_utf8(request).unwrap();
        assert!(
            text.contains("Sec-WebSocket-Protocol: mqtt")
                || text.contains("sec-websocket-protocol: mqtt")
        );
        let mut accept = async_tungstenite::tungstenite::handshake::derive_accept_key(
            text.lines()
                .find_map(|l| {
                    l.split_once(":")
                        .filter(|(k, _)| k.eq_ignore_ascii_case("sec-websocket-key"))
                        .map(|(_, v)| v.trim())
                })
                .unwrap()
                .as_bytes(),
        );
        accept.insert_str(0,"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Protocol: mqtt\r\nSec-WebSocket-Accept: ");
        accept.push_str("\r\n\r\n");
        s.write_all(accept.as_bytes()).unwrap();
    }
    #[test]
    fn oversized_frame_header_is_terminal_before_body() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let peer = thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            s.set_read_timeout(Some(std::time::Duration::from_secs(3)))
                .unwrap();
            http(&mut s);
            s.write_all(&[0x82, 0x7f, 0, 0, 0, 0, 0, 2, 0, 9]).unwrap();
            let mut b = [0; 32];
            let _ = s.read(&mut b);
        });
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut o = Options::new("127.0.0.1", "owned", port);
            o.use_websocket = true;
            let mut t = Transport::connect(&o)
                .await
                .expect("direct controlled connection missing");
            assert_eq!(t.packet().await, Err(Error::Overflow));
        });
        peer.join().unwrap();
    }
    #[test]
    fn declared_mqtt_length_rejected_across_small_ws_messages() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let peer = thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            s.set_read_timeout(Some(std::time::Duration::from_secs(3)))
                .unwrap();
            http(&mut s);
            for byte in [0x30, 0xff, 0xff, 0x7f] {
                s.write_all(&[0x82, 1, byte]).unwrap();
            }
            let mut b = [0; 32];
            let _ = s.read(&mut b);
        });
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut o = Options::new("127.0.0.1", "owned", port);
            o.use_websocket = true;
            let mut t = Transport::connect(&o)
                .await
                .expect("direct controlled connection missing");
            assert_eq!(t.packet().await, Err(Error::Overflow));
        });
        peer.join().unwrap();
    }

    #[test]
    fn oversized_handshake_rejected_without_websocket_body() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let peer = thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            s.set_read_timeout(Some(std::time::Duration::from_secs(3)))
                .unwrap();
            let mut request = Vec::new();
            let mut b = [0];
            while !request.ends_with(b"\r\n\r\n") {
                s.read_exact(&mut b).unwrap();
                request.push(b[0]);
            }
            let _ = s.write_all(b"HTTP/1.1 101 Switching Protocols\r\nX-Fill: ");
            let _ = s.write_all(&[b'x'; 20000]);
            let mut b = [0; 32];
            let _ = s.read(&mut b);
        });
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut o = Options::new("127.0.0.1", "owned", port);
            o.use_websocket = true;
            assert!(matches!(Transport::connect(&o).await, Err(Error::Network)));
        });
        peer.join().unwrap();
    }

    #[derive(Clone, Copy, Debug)]
    enum GeneratedCase {
        Frame,
        Fragments,
        Pings,
        Tiny,
        EmptyFragments,
    }
    struct Generated {
        case: GeneratedCase,
        frame: usize,
        pos: usize,
    }
    impl Generated {
        fn spec(&self) -> Option<(u8, usize)> {
            match self.case {
                GeneratedCase::Frame => (self.frame == 0).then_some((0x82, 3 * 1024 * 1024)),
                GeneratedCase::Fragments => (self.frame < 96).then_some((
                    if self.frame == 0 {
                        2
                    } else if self.frame == 95 {
                        0x80
                    } else {
                        0
                    },
                    32768,
                )),
                GeneratedCase::Pings => {
                    if self.frame < 32768 {
                        Some((0x89, 125))
                    } else {
                        (self.frame == 32768).then_some((0x82, 2))
                    }
                }
                GeneratedCase::Tiny => (self.frame < 65536).then_some((0x82, 1)),
                GeneratedCase::EmptyFragments => {
                    if self.frame < 32768 {
                        Some((if self.frame == 0 { 2 } else { 0 }, 0))
                    } else {
                        (self.frame == 32768).then_some((0x80, 2))
                    }
                }
            }
        }
    }
    impl AsyncRead for Generated {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let mut n = 0;
            while buf.remaining() > 0 {
                let Some((op, len)) = self.spec() else { break };
                let header = if len < 126 {
                    2
                } else if len <= 65535 {
                    4
                } else {
                    10
                };
                if self.pos == header + len {
                    self.frame += 1;
                    self.pos = 0;
                    continue;
                }
                let b = match self.pos {
                    0 => op,
                    1 => {
                        if len < 126 {
                            len as u8
                        } else if len <= 65535 {
                            126
                        } else {
                            127
                        }
                    }
                    i if i < header => ((len as u64) >> ((header - i - 1) * 8)) as u8,
                    _ => match self.case {
                        GeneratedCase::Pings | GeneratedCase::EmptyFragments
                            if self.frame == 32768 =>
                        {
                            if self.pos == header {
                                0xd0
                            } else {
                                0
                            }
                        }
                        GeneratedCase::Tiny if self.frame.is_multiple_of(2) => 0xd0,
                        GeneratedCase::Tiny => 0,
                        _ => 0,
                    },
                };
                buf.put_slice(&[b]);
                self.pos += 1;
                n += 1;
            }
            if n == 0 {
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
    }
    impl AsyncWrite for Generated {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }
    async fn generated(case: GeneratedCase) -> Transport {
        let mut config = Transport::config();
        match std::env::var("OWNED_MUTATE_CONFIG").as_deref() {
            Ok("write") => config.max_write_buffer_size = usize::MAX,
            Ok("frame") => config.max_frame_size = Some(16 * 1024 * 1024),
            Ok("message") => config.max_message_size = Some(64 * 1024 * 1024),
            _ => {}
        }
        let capped = HandshakeBudget {
            reads: 0,
            inner: Box::new(Generated {
                case,
                frame: 0,
                pos: 0,
            }),
            left: 0,
            enabled: Arc::new(AtomicBool::new(false)),
        };
        let ws = WebSocketStream::from_raw_socket(
            TokioAdapter::new(capped),
            async_tungstenite::tungstenite::protocol::Role::Client,
            Some(config),
        )
        .await;
        Transport {
            #[cfg(feature = "ws-test-observer")]
            counted: false,
            pending_write: None,
            tls: false,
            wire: Wire::Ws(Box::new(ws)),
            data: Vec::with_capacity(2 * LIMIT),
            control_batches: 0,
        }
    }
    #[test]
    fn owned_transport_requested_allocations_and_cooperative_controls() {
        // Warm exact futures executor/masking RNG/control-yield path before lifetime
        // accounting; no connection-owned buffer is subtracted from captured peak.
        futures::executor::block_on(async {
            let mut t = generated(GeneratedCase::Pings).await;
            assert_eq!(t.packet().await.unwrap(), [0xd0, 0]);
        });
        for case in [
            GeneratedCase::Frame,
            GeneratedCase::Fragments,
            GeneratedCase::Pings,
            GeneratedCase::Tiny,
            GeneratedCase::EmptyFragments,
        ] {
            let (peak, max, released) = super::super::alloc_probe::measure(|| {
                futures::executor::block_on(async {
                    let mut t = generated(case).await;
                    match case {
                        GeneratedCase::Frame | GeneratedCase::Fragments => {
                            assert_eq!(t.packet().await, Err(Error::Overflow))
                        }
                        GeneratedCase::Pings => {
                            assert_eq!(t.packet().await.unwrap(), [0xd0, 0]);
                            assert!(
                                t.control_batches >= 1024,
                                "control budget did not yield cooperatively"
                            );
                        }
                        GeneratedCase::EmptyFragments => {
                            assert_eq!(t.packet().await.unwrap(), [0xd0, 0])
                        }
                        GeneratedCase::Tiny => {
                            for _ in 0..32768 {
                                assert_eq!(t.packet().await.unwrap(), [0xd0, 0]);
                            }
                        }
                    }
                })
            });
            println!("owned transport case={case:?} requested_peak={peak} max_request={max} released={released}; isolated Rust fixture, NOT public client aggregate cap");
            assert!(
                peak <= 2 * 1024 * 1024,
                "owned WS transport fixture budget exceeded"
            );
            assert!(
                max <= 4 * LIMIT,
                "single allocation exceeded conservative frame/write growth envelope"
            );
            assert!(
                released,
                "selected transport object allocations not released"
            );
        }
    }

    #[test]
    fn handshake_budget_supports_uninitialized_async_read_buffers() {
        let mut s = HandshakeBudget {
            reads: 0,
            inner: Box::new(Generated {
                case: GeneratedCase::Tiny,
                frame: 0,
                pos: 0,
            }),
            left: 16,
            enabled: Arc::new(AtomicBool::new(true)),
        };
        let mut storage = [std::mem::MaybeUninit::<u8>::uninit(); 64];
        let mut read = ReadBuf::uninit(&mut storage);
        let w = futures::task::noop_waker();
        let mut cx = Context::from_waker(&w);
        assert!(matches!(
            Pin::new(&mut s).poll_read(&mut cx, &mut read),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(read.filled().len(), 16);
        assert_eq!(s.left, 0);
    }

    #[test]
    fn zero_length_fragment_flood_yields_before_terminal_message() {
        use std::future::Future;
        let mut t = futures::executor::block_on(generated(GeneratedCase::EmptyFragments));
        let mut f = Box::pin(t.packet());
        let w = futures::task::noop_waker();
        let mut cx = Context::from_waker(&w);
        assert!(
            matches!(f.as_mut().poll(&mut cx), Poll::Pending),
            "unbounded zero-byte continuations consumed without cooperative Pending"
        );
        assert_eq!(futures::executor::block_on(f).unwrap(), [0xd0, 0]);
    }

    struct PartialWriter {
        allowed: Arc<std::sync::atomic::AtomicUsize>,
        output: Arc<std::sync::Mutex<Vec<u8>>>,
    }
    impl AsyncRead for PartialWriter {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }
    impl AsyncWrite for PartialWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            b: &[u8],
        ) -> Poll<io::Result<usize>> {
            let n = self.allowed.load(Ordering::SeqCst).min(b.len());
            if n == 0 {
                return Poll::Pending;
            }
            self.output.lock().unwrap().extend_from_slice(&b[..n]);
            self.allowed.fetch_sub(n, Ordering::SeqCst);
            Poll::Ready(Ok(n))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    #[test]
    fn cancelled_write_future_resumes_exact_offset_without_duplicate_prefix() {
        use std::future::Future;
        let allowed = Arc::new(std::sync::atomic::AtomicUsize::new(2));
        let output = Arc::new(std::sync::Mutex::new(vec![]));
        let mut t = Transport {
            #[cfg(feature = "ws-test-observer")]
            counted: false,
            pending_write: None,
            tls: false,
            wire: Wire::Raw(Box::new(PartialWriter {
                allowed: allowed.clone(),
                output: output.clone(),
            })),
            data: Vec::new(),
            control_batches: 0,
        };
        t.queue_send(&[1, 2, 3, 4]).unwrap();
        let mut f = Box::pin(t.flush_pending());
        let w = futures::task::noop_waker();
        let mut cx = Context::from_waker(&w);
        assert!(matches!(f.as_mut().poll(&mut cx), Poll::Pending));
        drop(f);
        assert_eq!(*output.lock().unwrap(), [1, 2]);
        allowed.store(2, Ordering::SeqCst);
        futures::executor::block_on(t.flush_pending()).unwrap();
        assert_eq!(*output.lock().unwrap(), [1, 2, 3, 4]);
        futures::executor::block_on(t.flush_pending()).unwrap();
        assert_eq!(output.lock().unwrap().len(), 4);
    }

    #[test]
    fn large_connect_sized_write_is_split_into_finitely_bounded_ws_messages() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let peer = thread::spawn(move || {
            let (s, _) = l.accept().unwrap();
            s.set_read_timeout(Some(std::time::Duration::from_secs(3)))
                .unwrap();
            let mut ws = async_tungstenite::tungstenite::accept_hdr(s, MqttTestProtocol).unwrap();
            let (mut n, mut messages) = (0, 0);
            while n < SEND_LIMIT {
                let b = ws.read().unwrap().into_data();
                assert!(
                    b.len() <= LIMIT,
                    "one outgoing message exceeded finite ordinary frame budget"
                );
                assert!(b.iter().all(|&v| v == 7));
                n += b.len();
                messages += 1;
            }
            assert_eq!(n, SEND_LIMIT);
            assert_eq!(messages, 3);
        });
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut o = Options::new("127.0.0.1", "large", port);
            o.use_websocket = true;
            let mut t = Transport::connect(&o).await.unwrap();
            t.send(&vec![7; SEND_LIMIT]).await.unwrap();
        });
        peer.join().unwrap();
    }

    #[tokio::test]
    async fn tls_io_eof_and_broken_pipe_keep_typed_tls_failure() {
        for tls in [false, true] {
            let (io, peer) = tokio::io::duplex(8);
            drop(peer);
            let mut t = Transport {
                #[cfg(feature = "ws-test-observer")]
                counted: false,
                tls,
                pending_write: None,
                wire: Wire::Raw(Box::new(io)),
                data: Vec::with_capacity(2 * LIMIT),
                control_batches: 0,
            };
            let expected = if tls { Error::Tls } else { Error::Network };
            assert_eq!(t.packet().await, Err(expected));
            t.queue_send(&[0xe0, 0]).unwrap();
            assert_eq!(t.flush_pending().await, Err(expected));
        }
    }
}
