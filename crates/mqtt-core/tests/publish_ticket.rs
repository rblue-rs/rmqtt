mod common;
use common::{next, packet};
use mqtt_api::{Error, Options};
use mqtt_core::{Client, Kind};
use std::{io::Write, net::TcpListener, sync::mpsc, time::Duration};
#[test]
fn qos_ack_gates_and_bounded_requests() {
    mqtt_runtime::init().unwrap();
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let (ready, rx) = mpsc::channel();
    let (go, gate) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        packet(&mut s);
        s.write_all(&[0x20, 2, 0, 0]).unwrap();
        let p = packet(&mut s);
        assert_eq!(p[0] & 6, 2);
        let n = u16::from_be_bytes([p[1], p[2]]) as usize;
        let id = [p[3 + n], p[4 + n]];
        ready.send(()).unwrap();
        gate.recv_timeout(Duration::from_secs(5)).unwrap();
        s.write_all(&[0x40, 2, id[0], id[1]]).unwrap();
        for _ in 0..32 {
            let p = packet(&mut s);
            assert_eq!(p[0] & 6, 0);
        }
        assert_eq!(packet(&mut s), vec![0xe0]);
    });
    let mut c = Client::new().unwrap();
    c.connect(1, Options::new("127.0.0.1", "tickets", port))
        .unwrap();
    assert_eq!(next(&c).kind, Kind::Connected);
    c.publish(2, "x".into(), vec![1], 1, false).unwrap();
    assert_eq!(next(&c).kind, Kind::Queued);
    assert_eq!(next(&c).kind, Kind::Sent);
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(
        c.pop().unwrap_err(),
        Error::Empty,
        "not acknowledged before PUBACK"
    );
    for op in 3..35 {
        c.publish(op, "x".into(), vec![2], 0, false).unwrap();
    }
    assert_eq!(
        c.publish(35, "x".into(), vec![3], 0, false),
        Err(Error::QueueFull)
    );
    for _ in 0..32 {
        assert_eq!(next(&c).kind, Kind::Queued);
    }
    go.send(()).unwrap();
    let e = next(&c);
    assert_eq!(e.kind, Kind::Acknowledged);
    assert_eq!(e.op, 2);
    for _ in 0..32 {
        assert_eq!(next(&c).kind, Kind::Sent);
    }
    c.disconnect(36).unwrap();
    assert_eq!(next(&c).kind, Kind::Disconnected);
    server.join().unwrap();
    c.close().unwrap();
    drop(c);
    mqtt_runtime::shutdown().unwrap();
}
