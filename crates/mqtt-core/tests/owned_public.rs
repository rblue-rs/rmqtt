mod common;
use common::{next, packet, try_next};
use mqtt_api::{Error, Options};
use mqtt_core::{Client, Kind};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
    thread,
    time::Duration,
};
fn guard(f: impl FnOnce()) {
    mqtt_runtime::init().unwrap();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    let _ = mqtt_runtime::shutdown();
    if let Err(p) = r {
        std::panic::resume_unwind(p)
    }
}
fn until(c: &Client, k: Kind) -> mqtt_core::Event {
    loop {
        let e = try_next(c, Duration::from_secs(3)).expect("bounded public driver event");
        assert_ne!(e.kind, Kind::Failure, "{e:?}");
        if e.kind == k {
            return e;
        }
    }
}
#[test]
fn public_ws_declared_frame_limit_is_enforced_without_body() {
    guard(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let (go_tx, go_rx) = mpsc::channel();
        let peer = thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            s.set_read_timeout(Some(Duration::from_secs(4))).unwrap();
            let mut ws =
                async_tungstenite::tungstenite::accept_hdr(&mut s, MqttTestProtocol).unwrap();
            assert!(ws.read().unwrap().is_binary());
            ws.send(async_tungstenite::tungstenite::Message::Binary(
                vec![0x20, 2, 0, 0].into(),
            ))
            .unwrap();
            go_rx.recv().unwrap();
            let mut header = vec![0x82, 127];
            header.extend((3u64 * 1024 * 1024).to_be_bytes());
            ws.get_mut().write_all(&header).unwrap();
            let mut b = [0];
            assert_eq!(
                ws.get_mut().read(&mut b).unwrap(),
                0,
                "public limit failure must tear down socket"
            );
        });
        let mut c = Client::new().unwrap();
        let mut o = Options::new("127.0.0.1", "ws-limit", port);
        o.use_websocket = true;
        c.connect(1, o).unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        go_tx.send(()).unwrap();
        let e = try_next(&c, Duration::from_secs(2))
            .expect("declared oversized frame rejected before body");
        assert_eq!(e.kind, Kind::Failure);
        assert_eq!(e.code, Error::Overflow as i32);
        c.close().unwrap();
        peer.join().unwrap();
    });
}
#[test]
fn public_qos2_pubcomp_only_duplicate_controls_and_shared_ids() {
    guard(|| {
        for proto in [4, 5] {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = l.local_addr().unwrap().port();
            let (tx, rx) = mpsc::channel();
            let peer = thread::spawn(move || {
                let (mut s, _) = l.accept().unwrap();
                s.set_read_timeout(Some(Duration::from_secs(4))).unwrap();
                packet(&mut s);
                s.write_all(if proto == 5 {
                    &[0x20, 3, 0, 0, 0]
                } else {
                    &[0x20, 2, 0, 0]
                })
                .unwrap();
                let p = packet(&mut s);
                assert_eq!(p[0], 0x34);
                let n = u16::from_be_bytes([p[1], p[2]]) as usize;
                let id = u16::from_be_bytes([p[3 + n], p[4 + n]]);
                let [h, b] = id.to_be_bytes();
                for _ in 0..2 {
                    s.write_all(&[0x50, 2, h, b]).unwrap();
                    let rel = packet(&mut s);
                    assert_eq!(&rel[..3], &[0x62, h, b]);
                }
                s.write_all(&[0x70, 2, h, b.wrapping_add(1)]).unwrap();
                s.write_all(if proto == 5 {
                    &[0x30, 5, 0, 1, b'f', 0, 9]
                } else {
                    &[0x30, 4, 0, 1, b'f', 9]
                })
                .unwrap();
                rx.recv().unwrap();
                s.write_all(&[0x70, 2, h, b, 0x70, 2, h, b]).unwrap();
                let sub = packet(&mut s);
                assert_eq!(sub[0], 0x82);
                let sid = [sub[1], sub[2]];
                assert_ne!(sid, [h, b]);
                let sa = if proto == 5 {
                    vec![0x90, 4, sid[0], sid[1], 0, 2]
                } else {
                    vec![0x90, 3, sid[0], sid[1], 2]
                };
                s.write_all(&sa).unwrap();
                assert_eq!(packet(&mut s)[0], 0xe0);
            });
            let mut c = Client::new().unwrap();
            let mut o = Options::new("127.0.0.1", "qos2", port);
            o.protocol = proto;
            if proto == 5 {
                o.keep_alive = Duration::from_secs(5)
            }
            c.connect(1, o).unwrap();
            assert_eq!(next(&c).kind, Kind::Connected);
            assert_eq!(
                c.publish(2, "q".into(), vec![0, 255], 2, false),
                Ok(()),
                "public QoS2 must use real owned driver"
            );
            loop {
                let e = try_next(&c, Duration::from_secs(2)).unwrap();
                assert_ne!(e.kind, Kind::Failure, "{e:?}");
                assert_ne!(
                    e.kind,
                    Kind::Acknowledged,
                    "PUBREC or wrong-id COMP cannot acknowledge"
                );
                if e.kind == Kind::Message {
                    break;
                }
            }
            tx.send(()).unwrap();
            assert_eq!(until(&c, Kind::Acknowledged).op, 2);
            c.subscribe(3, "q".into(), 2).unwrap();
            assert_eq!(until(&c, Kind::Subscribed).op, 3);
            c.disconnect(4).unwrap();
            until(&c, Kind::Disconnected);
            c.close().unwrap();
            peer.join().unwrap();
        }
    });
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
