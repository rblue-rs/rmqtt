mod common;
use common::{next, packet, try_next};
use mqtt_api::{Error, Options};
use mqtt_core::{Client, Kind};
use std::{io::Write, net::TcpListener, sync::Mutex, thread, time::Duration};
static LOCK: Mutex<()> = Mutex::new(());
fn opts(port: u16) -> Options {
    let mut o = Options::new("127.0.0.1", "v5-test", port);
    o.protocol = 5;
    o.keep_alive = Duration::from_secs(5);
    o
}
fn run(f: impl FnOnce()) {
    let _guard = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    mqtt_runtime::init().unwrap();
    f();
    mqtt_runtime::shutdown().unwrap();
}
#[test]
fn supported_v5_options_and_unsupported_zero_send_no_packets() {
    run(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut o = opts(l.local_addr().unwrap().port());
        assert_eq!(o.validate(), Ok(()));
        for secs in 0..5 {
            o.keep_alive = Duration::from_secs(secs);
            let mut c = Client::new().unwrap();
            assert_eq!(c.connect(1, o.clone()), Err(Error::Unsupported));
            c.close().unwrap();
        }
        o.keep_alive = Duration::from_secs(5);
        o.clean_session = false;
        assert_eq!(o.validate(), Err(Error::Unsupported));
        l.set_nonblocking(true).unwrap();
        assert!(l.accept().is_err());
    });
}
#[test]
fn v5_tcp_baseline_exchange() {
    run(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            let p = packet(&mut s);
            assert_eq!(p[7], 5); // MQTT protocol level
            assert_eq!(&p[9..11], &[0, 5]); // explicit keepalive
            s.write_all(&[0x20, 3, 0, 0, 0]).unwrap();
            let sub = packet(&mut s);
            assert_eq!(sub[0], 0x82);
            s.write_all(&[0x90, 4, sub[1], sub[2], 0, 1]).unwrap();
            let p0 = packet(&mut s);
            assert_eq!(p0[0] & 0xf6, 0x30);
            let p1 = packet(&mut s);
            assert_eq!(p1[0] & 0xf6, 0x32);
            let n = u16::from_be_bytes([p1[1], p1[2]]) as usize;
            s.write_all(&[0x40, 2, p1[3 + n], p1[4 + n]]).unwrap();
            // QoS1 inbound, zero properties; expect manual PUBACK.
            s.write_all(&[0x32, 9, 0, 1, b't', 0, 77, 0, 1, 0, 255])
                .unwrap();
            assert_eq!(packet(&mut s), vec![0x40, 0, 77]);
            let unsub = packet(&mut s);
            assert_eq!(unsub[0], 0xa2);
            s.write_all(&[0xb0, 4, unsub[1], unsub[2], 0, 0]).unwrap();
            assert_eq!(packet(&mut s)[0], 0xe0);
        });
        let mut c = Client::new().unwrap();
        c.connect(1, opts(port)).unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        assert_eq!(
            c.publish(99, "t".into(), vec![], 3, false),
            Err(Error::Unsupported)
        );
        c.subscribe(2, "t".into(), 1).unwrap();
        assert_eq!(next(&c).kind, Kind::Subscribed);
        c.publish(3, "t".into(), vec![1, 0, 255], 0, false).unwrap();
        assert_eq!(next(&c).kind, Kind::Queued);
        assert_eq!(next(&c).kind, Kind::Sent);
        c.publish(4, "t".into(), vec![1, 0, 255], 1, false).unwrap();
        assert_eq!(next(&c).kind, Kind::Queued);
        assert_eq!(next(&c).kind, Kind::Sent);
        assert_eq!(next(&c).kind, Kind::Acknowledged);
        let msg = next(&c);
        assert_eq!(msg.kind, Kind::Message);
        assert_eq!(msg.payload, vec![1, 0, 255]);
        c.unsubscribe(5, "t".into()).unwrap();
        assert_eq!(next(&c).kind, Kind::Unsubscribed);
        c.disconnect(6).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        c.close().unwrap();
        server.join().unwrap();
    });
}
#[test]
fn v5_negative_ack_is_not_success_and_connack_refusal_does_not_retry() {
    run(|| {
        for (packet_type, reason, expected) in [
            (0x20, 0x86, Error::BadCredentials),
            (0x20, 0x88, Error::Refused),
            (0x90, 0x87, Error::Refused),
            (0x40, 0x87, Error::Refused),
            (0xb0, 0x87, Error::Refused),
        ] {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = l.local_addr().unwrap().port();
            let (tx, rx) = std::sync::mpsc::channel();
            let server = thread::spawn(move || {
                let (mut s, _) = l.accept().unwrap();
                s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                packet(&mut s);
                if packet_type == 0x20 {
                    s.write_all(&[0x20, 3, 0, reason, 0]).unwrap();
                } else {
                    s.write_all(&[0x20, 3, 0, 0, 0]).unwrap();
                    let p = packet(&mut s);
                    let id = if packet_type == 0x40 {
                        let n = u16::from_be_bytes([p[1], p[2]]) as usize;
                        [p[3 + n], p[4 + n]]
                    } else {
                        [p[1], p[2]]
                    };
                    if packet_type == 0x40 {
                        s.write_all(&[packet_type, 4, id[0], id[1], reason, 0])
                            .unwrap();
                    } else {
                        s.write_all(&[packet_type, 4, id[0], id[1], 0, reason])
                            .unwrap();
                    }
                }
                rx.recv_timeout(Duration::from_secs(3)).unwrap();
            });
            let mut c = Client::new().unwrap();
            let mut o = opts(port);
            o.reconnect.enabled = true;
            c.connect(1, o).unwrap();
            if packet_type != 0x20 {
                assert_eq!(next(&c).kind, Kind::Connected);
                match packet_type {
                    0x90 => c.subscribe(2, "t".into(), 1),
                    0xb0 => c.unsubscribe(2, "t".into()),
                    _ => c.publish(2, "t".into(), vec![1], 1, false),
                }
                .unwrap();
                if packet_type == 0x40 {
                    assert_eq!(next(&c).kind, Kind::Queued);
                    assert_eq!(next(&c).kind, Kind::Sent);
                }
            }
            let e = next(&c);
            assert_eq!(e.kind, Kind::Failure);
            assert_eq!(e.code, expected as i32);
            assert!(try_next(&c, Duration::from_millis(100)).is_none());
            c.close().unwrap();
            tx.send(()).unwrap();
            server.join().unwrap();
        }
    });
}

#[test]
fn v5_drain_waits_for_puback_and_force_close_is_immediate() {
    run(|| {
        for force in [false, true] {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = l.local_addr().unwrap().port();
            let (tx, rx) = std::sync::mpsc::channel();
            let server = thread::spawn(move || {
                let (mut s, _) = l.accept().unwrap();
                s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                packet(&mut s);
                s.write_all(&[0x20, 3, 0, 0, 0]).unwrap();
                let p = packet(&mut s);
                rx.recv_timeout(Duration::from_secs(3)).unwrap();
                if force {
                    let mut b = [0];
                    assert!(std::io::Read::read(&mut s, &mut b).unwrap_or(0) == 0);
                } else {
                    let n = u16::from_be_bytes([p[1], p[2]]) as usize;
                    s.write_all(&[0x40, 2, p[3 + n], p[4 + n]]).unwrap();
                    assert_eq!(packet(&mut s)[0], 0xe0);
                }
            });
            let mut c = Client::new().unwrap();
            c.connect(1, opts(port)).unwrap();
            assert_eq!(next(&c).kind, Kind::Connected);
            c.publish(2, "t".into(), vec![1], 1, false).unwrap();
            assert_eq!(next(&c).kind, Kind::Queued);
            assert_eq!(next(&c).kind, Kind::Sent);
            c.begin_drain(Duration::from_secs(1)).unwrap();
            assert_eq!(c.drain_status().unwrap(), mqtt_core::DrainStatus::Pending);
            if force {
                c.close().unwrap();
            }
            tx.send(()).unwrap();
            if !force {
                assert_eq!(next(&c).kind, Kind::Acknowledged);
                let end = std::time::Instant::now() + Duration::from_secs(2);
                while c.drain_status().unwrap() == mqtt_core::DrainStatus::Pending {
                    assert!(std::time::Instant::now() < end);
                    thread::sleep(Duration::from_millis(5));
                }
                assert_eq!(c.drain_status().unwrap(), mqtt_core::DrainStatus::Drained);
                c.close().unwrap();
            }
            server.join().unwrap();
        }
    });
}
#[test]
fn v5_idle_reconnect_works_but_drop_with_outstanding_work_fails_terminally() {
    run(|| {
        for busy in [false, true] {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = l.local_addr().unwrap().port();
            let (tx, rx) = std::sync::mpsc::channel();
            let server = thread::spawn(move || {
                let (mut s, _) = l.accept().unwrap();
                s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                packet(&mut s);
                s.write_all(&[0x20, 3, 0, 0, 0]).unwrap();
                if busy {
                    packet(&mut s);
                }
                rx.recv_timeout(Duration::from_secs(3)).unwrap();
                drop(s);
                if !busy {
                    let (mut s, _) = l.accept().unwrap();
                    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                    packet(&mut s);
                    s.write_all(&[0x20, 3, 0, 0, 0]).unwrap();
                    rx.recv_timeout(Duration::from_secs(3)).unwrap();
                }
            });
            let mut c = Client::new().unwrap();
            let mut o = opts(port);
            o.reconnect.enabled = true;
            o.reconnect.base_delay = Duration::from_millis(20);
            o.reconnect.max_delay = Duration::from_millis(20);
            c.connect(1, o).unwrap();
            assert_eq!(next(&c).kind, Kind::Connected);
            if busy {
                c.publish(2, "t".into(), vec![1], 1, false).unwrap();
                assert_eq!(next(&c).kind, Kind::Queued);
                assert_eq!(next(&c).kind, Kind::Sent);
            }
            tx.send(()).unwrap();
            if busy {
                assert_eq!(next(&c).kind, Kind::Failure);
                assert!(try_next(&c, Duration::from_millis(100)).is_none());
            } else {
                assert_eq!(next(&c).kind, Kind::Disconnected);
                assert_eq!(next(&c).kind, Kind::Reconnecting);
                assert_eq!(next(&c).kind, Kind::Reconnected);
                assert_eq!(next(&c).kind, Kind::SubscriptionsResubscribed);
                tx.send(()).unwrap();
            }
            c.close().unwrap();
            server.join().unwrap();
        }
    });
}

#[test]
fn v5_drain_timeout_prevents_reconnect_with_unacked_publish() {
    run(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let (tx, rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            packet(&mut s);
            s.write_all(&[0x20, 3, 0, 0, 0]).unwrap();
            packet(&mut s);
            rx.recv_timeout(Duration::from_secs(3)).unwrap();
        });
        let mut c = Client::new().unwrap();
        let mut o = opts(port);
        o.reconnect.enabled = true;
        c.connect(1, o).unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        c.publish(2, "t".into(), vec![1], 1, false).unwrap();
        assert_eq!(next(&c).kind, Kind::Queued);
        assert_eq!(next(&c).kind, Kind::Sent);
        c.begin_drain(Duration::from_millis(30)).unwrap();
        let e = next(&c);
        assert_eq!(e.kind, Kind::Failure);
        assert_eq!(e.code, Error::Timeout as i32);
        assert_eq!(c.drain_status().unwrap(), mqtt_core::DrainStatus::TimedOut);
        assert!(try_next(&c, Duration::from_millis(100)).is_none());
        c.close().unwrap();
        tx.send(()).unwrap();
        server.join().unwrap();
    });
}
#[test]
fn v5_inbound_qos2_delivers_only_after_release_and_duplicates_are_idempotent() {
    run(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let (tx, rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            packet(&mut s);
            s.write_all(&[0x20, 3, 0, 0, 0]).unwrap();
            s.write_all(&[0x34, 7, 0, 1, b't', 0, 77, 0, 1]).unwrap();
            assert_eq!(packet(&mut s), [0x50, 0, 77, 0, 0]);
            s.write_all(&[0x30, 5, 0, 1, b'f', 0, b'A']).unwrap();
            rx.recv_timeout(Duration::from_secs(3)).unwrap();
            for reason in [0, 0x92] {
                s.write_all(&[0x62, 2, 0, 77]).unwrap();
                assert_eq!(packet(&mut s), [0x70, 0, 77, reason, 0]);
            }
            s.write_all(&[0x30, 5, 0, 1, b'f', 0, b'B']).unwrap();
            assert_eq!(packet(&mut s), [0xe0]);
        });
        let mut c = Client::new().unwrap();
        c.connect(1, opts(port)).unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        let fence = next(&c);
        assert_eq!(
            (fence.kind, fence.qos, fence.payload),
            (Kind::Message, 0, vec![b'A'])
        );
        tx.send(()).unwrap();
        let m = next(&c);
        assert_eq!((m.kind, m.qos, m.payload), (Kind::Message, 2, vec![1]));
        let fence = next(&c);
        assert_eq!(fence.payload, [b'B']);
        c.disconnect(2).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        c.close().unwrap();
        server.join().unwrap();
    });
}
#[test]
fn v5_first_connack_timeout_does_not_reconnect() {
    run(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let (tx, rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            packet(&mut s);
            rx.recv_timeout(Duration::from_secs(8)).unwrap();
            l.set_nonblocking(true).unwrap();
            assert!(l.accept().is_err());
        });
        let mut c = Client::new().unwrap();
        let mut o = opts(port);
        o.reconnect.enabled = true;
        c.connect(1, o).unwrap();
        let e = next(&c);
        assert_eq!(e.kind, Kind::Failure);
        assert_eq!(e.code, Error::Timeout as i32);
        assert!(try_next(&c, Duration::from_millis(100)).is_none());
        c.close().unwrap();
        tx.send(()).unwrap();
        server.join().unwrap();
    });
}

#[test]
fn v5_non_error_reason_codes_complete_successfully() {
    run(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            packet(&mut s);
            s.write_all(&[0x20, 3, 0, 0, 0]).unwrap();
            let p = packet(&mut s);
            let n = u16::from_be_bytes([p[1], p[2]]) as usize;
            s.write_all(&[0x40, 4, p[3 + n], p[4 + n], 0x10, 0])
                .unwrap();
            let u = packet(&mut s);
            s.write_all(&[0xb0, 4, u[1], u[2], 0, 0x11]).unwrap();
            assert_eq!(packet(&mut s)[0], 0xe0);
        });
        let mut c = Client::new().unwrap();
        c.connect(1, opts(port)).unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        c.publish(2, "t".into(), vec![1], 1, false).unwrap();
        assert_eq!(next(&c).kind, Kind::Queued);
        assert_eq!(next(&c).kind, Kind::Sent);
        assert_eq!(next(&c).kind, Kind::Acknowledged);
        c.unsubscribe(3, "t".into()).unwrap();
        assert_eq!(next(&c).kind, Kind::Unsubscribed);
        c.disconnect(4).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        c.close().unwrap();
        server.join().unwrap();
    });
}
