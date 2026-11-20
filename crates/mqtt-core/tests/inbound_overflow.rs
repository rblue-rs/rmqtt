mod common;
use common::{next, packet};
use mqtt_api::{Error, Options};
use mqtt_core::{Client, Kind};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
    time::Duration,
};

#[test]
fn full_queue_preserves_control_and_never_acks_unaccepted_message() {
    mqtt_runtime::init().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (ready, rx) = mpsc::channel();
    let (go, gate) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        packet(&mut s);
        s.write_all(&[0x20, 2, 0, 0]).unwrap();
        let sub = packet(&mut s);
        s.write_all(&[0x90, 3, sub[1], sub[2], 1]).unwrap();
        for id in 1..=127u8 {
            s.write_all(&[0x32, 6, 0, 1, b'x', 0, id, b'!']).unwrap();
            assert_eq!(packet(&mut s), vec![0x40, 0, id]);
        }
        // One SUBACK + 127 messages occupy all slots. The next publish cannot enter.
        s.write_all(&[0x32, 6, 0, 1, b'x', 0, 128, b'!']).unwrap();
        let mut b = [0];
        assert_eq!(
            s.read(&mut b).unwrap(),
            0,
            "must close without PUBACK for packet 128"
        );
        ready.send(()).unwrap();
        gate.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    let mut c = Client::new().unwrap();
    c.connect(1, Options::new("127.0.0.1", "inbound-overflow", port))
        .unwrap();
    assert_eq!(next(&c).kind, Kind::Connected);
    c.subscribe(2, "x".into(), 1).unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let control = next(&c);
    assert_eq!(control.kind, Kind::Subscribed);
    assert_eq!(control.op, 2);
    for id in 1..=127 {
        let e = next(&c);
        assert_eq!(e.kind, Kind::Message);
        assert_eq!(e.packet_id, id);
    }
    let fatal = next(&c);
    assert_eq!(fatal.kind, Kind::Failure);
    assert_eq!(fatal.code, Error::Overflow as i32);
    assert_eq!(c.pop().unwrap_err(), Error::Empty);
    go.send(()).unwrap();
    server.join().unwrap();
    drop(c);
    mqtt_runtime::shutdown().unwrap();
}
