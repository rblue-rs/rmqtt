mod common;
use common::{next, packet};
use mqtt_api::{Error, Options};
use mqtt_core::{Client, Kind};
use std::{io::Write, net::TcpListener, sync::mpsc, time::Duration};

#[test]
fn duplicate_disconnects_complete_and_handle_reconnects() {
    mqtt_runtime::init().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (go, gate) = mpsc::channel();
    let (sent, received) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        assert_eq!(packet(&mut s)[0], 0x10);
        s.write_all(&[0x20, 2, 0, 0]).unwrap();
        let p = packet(&mut s);
        assert_eq!(p[0] & 0xf0, 0x30);
        sent.send(()).unwrap();
        gate.recv_timeout(Duration::from_secs(5)).unwrap();
        let n = ((p[1] as usize) << 8) | p[2] as usize;
        s.write_all(&[0x40, 2, p[n + 3], p[n + 4]]).unwrap();
        assert_eq!(packet(&mut s), vec![0xe0]);
        let (mut s, _) = listener.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        assert_eq!(packet(&mut s)[0], 0x10);
        s.write_all(&[0x20, 2, 0, 0]).unwrap();
        gate.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    let mut c = Client::new().unwrap();
    c.connect(1, Options::new("127.0.0.1", "duplicate", port))
        .unwrap();
    assert_eq!(next(&c).kind, Kind::Connected);
    c.publish(2, "x".into(), vec![1], 1, false).unwrap();
    received.recv_timeout(Duration::from_secs(5)).unwrap();
    let first = c.disconnect(3);
    let second = c.disconnect(4);
    go.send(()).unwrap();
    let mut disconnects = vec![];
    while disconnects.len() < if second.is_ok() { 2 } else { 1 } {
        let e = next(&c);
        if e.kind == Kind::Disconnected {
            disconnects.push(e.op);
        }
    }
    let reconnect = c.connect(5, Options::new("127.0.0.1", "duplicate", port));
    assert_eq!(next(&c).kind, Kind::Connected);
    c.close().unwrap();
    assert_eq!(c.pop().unwrap_err(), Error::Closed);
    drop(c);
    go.send(()).unwrap();
    server.join().unwrap();
    mqtt_runtime::shutdown().unwrap();
    assert_eq!(first, Ok(()));
    assert_eq!(
        second,
        Ok(()),
        "concurrent disconnect must coalesce, not Busy"
    );
    assert_eq!(disconnects, [3, 4]);
    assert_eq!(reconnect, Ok(()));
}

#[test]
fn disconnected_requests_are_idempotent_and_close_is_permanent() {
    mqtt_runtime::init().unwrap();
    let mut c = Client::new().unwrap();
    c.disconnect(1).unwrap();
    c.disconnect(2).unwrap();
    assert_eq!(next(&c).op, 1);
    assert_eq!(next(&c).op, 2);
    assert_eq!(c.pop().unwrap_err(), Error::Empty);
    c.close().unwrap();
    c.close().unwrap();
    assert_eq!(c.disconnect(3), Err(Error::Closed));
    assert_eq!(c.peek().unwrap_err(), Error::Closed);
    drop(c);
    mqtt_runtime::shutdown().unwrap();
}
