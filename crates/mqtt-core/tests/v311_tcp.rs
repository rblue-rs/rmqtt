use mqtt_api::{Error, Options};
use mqtt_core::{Client, Kind};
use std::{
    io::{ErrorKind, Read, Write},
    net::TcpListener,
    sync::mpsc,
    time::Duration,
};
mod common;
use common::{next, packet};
#[test]
fn connack_and_wire_keepalive_contracts() {
    mqtt_runtime::init().unwrap();
    for keep in [0, 1] {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let (tx, rx) = mpsc::channel();
        let (go, wait) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            s.set_read_timeout(Some(Duration::from_millis(1400)))
                .unwrap();
            let p = packet(&mut s);
            assert_eq!(p[0], 0x10);
            assert_eq!(u16::from_be_bytes([p[9], p[10]]), keep);
            tx.send(()).unwrap();
            wait.recv_timeout(Duration::from_secs(3)).unwrap();
            s.write_all(&[0x20, 2, 0, 0]).unwrap();
            if keep == 0 {
                let mut b = [0];
                let error = s
                    .read(&mut b)
                    .expect_err("zero keepalive: no bytes or EOF expected");
                assert!(matches!(
                    error.kind(),
                    ErrorKind::WouldBlock | ErrorKind::TimedOut
                ));
            } else {
                assert_eq!(packet(&mut s), vec![0xc0]);
                s.write_all(&[0xd0, 0]).unwrap();
            }
        });
        let mut c = Client::new().unwrap();
        let mut options = Options::new("127.0.0.1", format!("wire-{keep}"), port);
        if keep != 0 {
            options.keep_alive = Duration::from_secs(keep as u64);
        }
        assert_eq!(c.connect(1, options), Ok(()));
        rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(
            c.pop().unwrap_err(),
            Error::Empty,
            "no completion before CONNACK"
        );
        go.send(()).unwrap();
        let event = next(&c);
        assert_eq!(event.kind, Kind::Connected);
        assert_eq!(event.op, 1);
        assert!(!event.session_present);
        server.join().unwrap();
        assert_eq!(next(&c).kind, Kind::Failure);
        c.close().unwrap();
        assert_eq!(
            c.publish(3, "a".into(), vec![], 0, false),
            Err(Error::Closed)
        );
    }
    mqtt_runtime::shutdown().unwrap();
}
