mod common;
use common::{next, packet};
use mqtt_api::{Error, Options};
use mqtt_core::{Client, Kind};
use std::{io::Write, net::TcpListener, sync::mpsc, time::Duration};
#[test]
fn suback_gate_and_inbound_acceptance_before_protocol_ack() {
    mqtt_runtime::init().unwrap();
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let (tx, rx) = mpsc::channel();
    let (go, gate) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        packet(&mut s);
        s.write_all(&[0x20, 2, 0, 0]).unwrap();
        let sub = packet(&mut s);
        assert_eq!(sub[0], 0x82);
        tx.send(()).unwrap();
        gate.recv_timeout(Duration::from_secs(3)).unwrap();
        s.write_all(&[0x90, 3, sub[1], sub[2], 1]).unwrap();
        s.write_all(&[0x32, 6, 0, 1, b'x', 0, 9, b'!']).unwrap();
        let ack = packet(&mut s);
        assert_eq!(ack, vec![0x40, 0, 9]);
        tx.send(()).unwrap();
        gate.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(packet(&mut s), vec![0xe0]);
    });
    let mut c = Client::new().unwrap();
    c.connect(1, Options::new("127.0.0.1", "acks", port))
        .unwrap();
    next(&c);
    c.subscribe(2, "x".into(), 1).unwrap();
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(c.pop().unwrap_err(), Error::Empty);
    go.send(()).unwrap();
    let ack = next(&c);
    assert_eq!(ack.kind, Kind::Subscribed);
    assert_eq!(ack.qos, 1);
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
    let incoming = c.peek().unwrap();
    assert_eq!(incoming.kind, Kind::Message);
    assert_eq!(incoming.payload, b"!");
    assert_eq!(c.pop().unwrap().packet_id, 9);
    go.send(()).unwrap();
    c.disconnect(3).unwrap();
    assert_eq!(next(&c).kind, Kind::Disconnected);
    server.join().unwrap();
    drop(c);
    mqtt_runtime::shutdown().unwrap();
}
