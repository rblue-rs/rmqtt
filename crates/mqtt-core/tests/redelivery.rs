mod common;
use common::{next, packet};
use mqtt_api::{Error, Options};
use mqtt_core::{Client, Kind};
use std::{io::Write, net::TcpListener, sync::mpsc, time::Duration};

#[test]
fn qos1_duplicate_is_delivered_again_and_unsuback_is_gated() {
    mqtt_runtime::init().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (ready, rx) = mpsc::channel();
    let (go, gate) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        assert_eq!(packet(&mut s)[0], 0x10);
        s.write_all(&[0x20, 2, 0, 0]).unwrap();
        // Simulate an ACK lost by the peer: retransmit the same packet id with DUP.
        // This tests local duplicate delivery, not cross-process/session recovery.
        for head in [0x32, 0x3a] {
            s.write_all(&[head, 6, 0, 1, b'x', 0, 9, 0xff]).unwrap();
            assert_eq!(packet(&mut s), vec![0x40, 0, 9]);
        }
        let unsub = packet(&mut s);
        assert_eq!(unsub[0], 0xa2);
        ready.send(()).unwrap();
        gate.recv_timeout(Duration::from_secs(5)).unwrap();
        s.write_all(&[0xb0, 2, unsub[1], unsub[2]]).unwrap();
        assert_eq!(packet(&mut s), vec![0xe0]);
    });
    let mut c = Client::new().unwrap();
    c.connect(1, Options::new("127.0.0.1", "redelivery", port))
        .unwrap();
    assert_eq!(next(&c).kind, Kind::Connected);
    for _ in 0..2 {
        let e = next(&c);
        assert_eq!(e.kind, Kind::Message);
        assert_eq!(e.packet_id, 9);
        assert_eq!(e.qos, 1);
        assert_eq!(e.topic, b"x");
        assert_eq!(e.payload, [0xff]);
    }
    c.unsubscribe(2, "x".into()).unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(c.pop().unwrap_err(), Error::Empty, "must await UNSUBACK");
    go.send(()).unwrap();
    let e = next(&c);
    assert_eq!(e.kind, Kind::Unsubscribed);
    assert_eq!(e.op, 2);
    c.disconnect(3).unwrap();
    assert_eq!(next(&c).kind, Kind::Disconnected);
    server.join().unwrap();
    drop(c);
    mqtt_runtime::shutdown().unwrap();
}
