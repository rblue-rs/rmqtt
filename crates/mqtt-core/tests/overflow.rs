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
fn producer_side_overflow_disconnects_and_preserves_existing_events() {
    mqtt_runtime::init().unwrap();
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let (full, rx) = mpsc::channel();
    let (go, gate) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        packet(&mut s);
        s.write_all(&[0x20, 2, 0, 0]).unwrap();
        gate.recv_timeout(Duration::from_secs(3)).unwrap();
        for id in 1..=128u8 {
            s.write_all(&[0x32, 6, 0, 1, b'x', 0, id, b'!']).unwrap();
            assert_eq!(packet(&mut s), vec![0x40, 0, id]);
        }
        full.send(()).unwrap();
        let mut b = [0];
        assert_eq!(
            s.read(&mut b).unwrap(),
            0,
            "fatal overflow must close socket even without further traffic"
        );
    });
    let mut c = Client::new().unwrap();
    c.connect(1, Options::new("127.0.0.1", "overflow", port))
        .unwrap();
    next(&c);
    go.send(()).unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        c.publish(2, "x".into(), vec![], 0, false),
        Err(Error::Overflow)
    );
    for _ in 0..128 {
        assert_eq!(next(&c).kind, Kind::Message);
    }
    let fatal = next(&c);
    assert_eq!(fatal.kind, Kind::Failure);
    assert_eq!(fatal.code, Error::Overflow as i32);
    server.join().unwrap();
    c.close().unwrap();
    drop(c);
    mqtt_runtime::shutdown().unwrap();
}
