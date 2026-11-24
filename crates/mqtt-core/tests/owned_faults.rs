mod owned_support;
use mqtt_api::{Error, ReconnectPolicy};
use mqtt_core::{Client, Kind};
use owned_support::*;
use std::{net::TcpListener, sync::mpsc, thread, time::Duration};
#[test]
fn public_persistent311_replays_both_phases_with_duplicates_over_all_transports() {
    guard(|| {
        let certs = Certs::new();
        for tls in [false, true] {
            for ws in [false, true] {
                let l = TcpListener::bind("127.0.0.1:0").unwrap();
                let mut o = certs.options(l.local_addr().unwrap().port(), 4, tls, ws);
                o.clean_session = false;
                o.reconnect = ReconnectPolicy {
                    enabled: true,
                    max_attempts: 2,
                    base_delay: Duration::from_millis(1),
                    max_delay: Duration::from_millis(1),
                };
                let config = certs.config.clone();
                let (tx, rx) = mpsc::channel();
                let peer = thread::spawn(move || {
                    let mut s = Peer::accept(&l, tls, ws, config.clone());
                    s.connack(4, false);
                    let p = s.packet();
                    assert_eq!(p[0], 0x34);
                    let [h, b] = id(&p);
                    drop(s);
                    let mut s = Peer::accept(&l, tls, ws, config.clone());
                    s.connack(4, true);
                    let p = s.packet();
                    assert_eq!(p[0], 0x3c);
                    assert_eq!(id(&p), [h, b]);
                    for _ in 0..2 {
                        s.send(&[0x50, 2, h, b]);
                        assert_eq!(&s.packet()[..3], &[0x62, h, b]);
                    }
                    drop(s);
                    let mut s = Peer::accept(&l, tls, ws, config);
                    s.connack(4, true);
                    assert_eq!(&s.packet()[..3], &[0x62, h, b]);
                    s.send(&[0x50, 2, h, b]);
                    assert_eq!(&s.packet()[..3], &[0x62, h, b]);
                    s.send(&[0x70, 2, h, b.wrapping_add(1)]);
                    s.send(&[0x30, 4, 0, 1, b'f', 9]);
                    rx.recv().unwrap();
                    s.send(&[0x70, 2, h, b, 0x70, 2, h, b]);
                    assert_eq!(s.packet()[0], 0xe0);
                });
                let mut c = Client::new().unwrap();
                c.connect(1, o).unwrap();
                assert_eq!(next(&c).kind, Kind::Connected);
                c.publish(2, "q".into(), vec![0, 255], 2, false).unwrap();
                let mut sent = 0;
                loop {
                    let e = next(&c);
                    assert_ne!(e.kind, Kind::Failure, "{e:?}");
                    assert_ne!(e.kind, Kind::Acknowledged, "no PUBCOMP yet");
                    if e.kind == Kind::Sent {
                        sent += 1;
                    }
                    if e.kind == Kind::Message {
                        break;
                    }
                }
                assert_eq!(sent, 1);
                tx.send(()).unwrap();
                loop {
                    let e = next(&c);
                    assert_ne!(e.kind, Kind::Failure, "{e:?}");
                    if e.kind == Kind::Acknowledged {
                        assert_eq!(e.op, 2);
                        break;
                    }
                }
                c.disconnect(3).unwrap();
                assert_eq!(next(&c).kind, Kind::Disconnected);
                c.close().unwrap();
                peer.join().unwrap();
                println!("PUBLIC replay protocol4 tls={tls} ws={ws} preREC/PUBREL phases duplicates wrongCOMP budget2 PASS");
            }
        }
    });
}
#[test]
fn public_v5_negative_rec_comp_and_drop_are_terminal_over_all_transports() {
    guard(|| {
        let certs = Certs::new();
        for tls in [false, true] {
            for ws in [false, true] {
                for phase in [0, 1, 2] {
                    let l = TcpListener::bind("127.0.0.1:0").unwrap();
                    let mut o = certs.options(l.local_addr().unwrap().port(), 5, tls, ws);
                    o.reconnect.enabled = true;
                    o.reconnect.base_delay = Duration::from_millis(1);
                    let config = certs.config.clone();
                    let peer = thread::spawn(move || {
                        let mut s = Peer::accept(&l, tls, ws, config);
                        s.connack(5, false);
                        let p = s.packet();
                        let [h, b] = id(&p);
                        if phase == 2 {
                            return;
                        }
                        if phase == 1 {
                            s.send(&[0x50, 2, h, b]);
                            assert_eq!(&s.packet()[..3], &[0x62, h, b]);
                            s.send(&[0x70, 4, h, b, 0x92, 0]);
                        } else {
                            s.send(&[0x50, 4, h, b, 0x97, 0]);
                        }
                        s.expect_closed();
                    });
                    let mut c = Client::new().unwrap();
                    c.connect(1, o).unwrap();
                    assert_eq!(next(&c).kind, Kind::Connected);
                    c.publish(2, "q".into(), vec![0, 255], 2, false).unwrap();
                    loop {
                        let e = next(&c);
                        assert_ne!(e.kind, Kind::Acknowledged);
                        assert_ne!(e.kind, Kind::Reconnecting);
                        if e.kind == Kind::Failure {
                            assert_eq!(
                                e.code,
                                if phase == 2 {
                                    if tls {
                                        Error::Tls as i32
                                    } else {
                                        Error::Network as i32
                                    }
                                } else {
                                    Error::Refused as i32
                                }
                            );
                            break;
                        }
                    }
                    c.close().unwrap();
                    peer.join().unwrap();
                    println!("PUBLIC v5 terminal tls={tls} ws={ws} phase={phase} PASS");
                }
            }
        }
    });
}
#[test]
fn public_inbound_qos2_duplicates_deliver_once_then_fence_all_eight_cells() {
    guard(|| {
        let certs = Certs::new();
        for proto in [4, 5] {
            for tls in [false, true] {
                for ws in [false, true] {
                    let l = TcpListener::bind("127.0.0.1:0").unwrap();
                    let o = certs.options(l.local_addr().unwrap().port(), proto, tls, ws);
                    let config = certs.config.clone();
                    let peer = thread::spawn(move || {
                        let mut s = Peer::accept(&l, tls, ws, config);
                        s.connack(proto, false);
                        let mut b = vec![0, 1, b'q', 0, 9];
                        if proto == 5 {
                            b.push(0)
                        }
                        b.extend([0, 255]);
                        let p = wire(0x34, &b);
                        for _ in 0..2 {
                            s.send(&p);
                            assert_eq!(&s.packet()[..3], &[0x50, 0, 9]);
                        }
                        for _ in 0..3 {
                            s.send(&[0x62, 2, 0, 9]);
                            assert_eq!(&s.packet()[..3], &[0x70, 0, 9]);
                        }
                        s.send(if proto == 5 {
                            &[0x30, 5, 0, 1, b'f', 0, 9]
                        } else {
                            &[0x30, 4, 0, 1, b'f', 9]
                        });
                        assert_eq!(s.packet()[0], 0xe0);
                    });
                    let mut c = Client::new().unwrap();
                    c.connect(1, o).unwrap();
                    assert_eq!(next(&c).kind, Kind::Connected);
                    let mut deliveries = 0;
                    loop {
                        let e = next(&c);
                        assert_eq!(e.kind, Kind::Message, "{e:?}");
                        if e.topic == b"f" {
                            break;
                        }
                        assert_eq!(e.payload, [0, 255]);
                        assert_eq!(e.qos, 2);
                        deliveries += 1;
                    }
                    assert_eq!(deliveries, 1);
                    c.disconnect(2).unwrap();
                    assert_eq!(next(&c).kind, Kind::Disconnected);
                    c.close().unwrap();
                    peer.join().unwrap();
                    println!("PUBLIC inbound protocol={proto} tls={tls} ws={ws} duplicate PUBLISH/REL once+fence PASS");
                }
            }
        }
    });
}

#[test]
fn public_qos2_drain_finish_deadline_force_all_eight_cells() {
    use mqtt_core::DrainStatus;
    guard(|| {
        let cert = Certs::new();
        for proto in [4, 5] {
            for tls in [false, true] {
                for ws in [false, true] {
                    for mode in 0..3 {
                        let l = TcpListener::bind("127.0.0.1:0").unwrap();
                        let o = cert.options(l.local_addr().unwrap().port(), proto, tls, ws);
                        let cfg = cert.config.clone();
                        let (ready, observed) = std::sync::mpsc::channel();
                        let (go, wait) = std::sync::mpsc::channel();
                        let peer = thread::spawn(move || {
                            let mut p = Peer::accept(&l, tls, ws, cfg);
                            p.connack(proto, false);
                            let q = p.packet();
                            let id = id(&q);
                            p.send(&[0x50, 2, id[0], id[1]]);
                            let rel = p.packet();
                            assert_eq!(&rel[..3], &[0x62, id[0], id[1]]);
                            ready.send(()).unwrap();
                            wait.recv_timeout(Duration::from_secs(3)).unwrap();
                            if mode == 0 {
                                p.send(&[0x70, 2, id[0], id[1]]);
                                assert_eq!(p.packet(), [0xe0]);
                            } else {
                                p.expect_closed();
                            }
                        });
                        let mut c = Client::new().unwrap();
                        c.connect(1, o).unwrap();
                        assert_eq!(next(&c).kind, Kind::Connected);
                        c.publish(2, "q".into(), vec![255], 2, false).unwrap();
                        observed.recv_timeout(Duration::from_secs(3)).unwrap();
                        c.begin_drain(if mode == 0 {
                            Duration::from_secs(1)
                        } else {
                            Duration::from_millis(25)
                        })
                        .unwrap();
                        assert_eq!(c.drain_status().unwrap(), DrainStatus::Pending);
                        assert_eq!(
                            c.publish(3, "q".into(), vec![], 0, false),
                            Err(Error::Closed)
                        );
                        if mode == 2 {
                            c.close().unwrap();
                        }
                        go.send(()).unwrap();
                        let end = std::time::Instant::now() + Duration::from_secs(3);
                        while c.drain_status().unwrap() == DrainStatus::Pending {
                            assert!(std::time::Instant::now() < end);
                            thread::yield_now();
                        }
                        assert_eq!(
                            c.drain_status().unwrap(),
                            match mode {
                                0 => DrainStatus::Drained,
                                1 => DrainStatus::TimedOut,
                                _ => DrainStatus::Forced,
                            }
                        );
                        if mode != 2 {
                            let mut ack = 0;
                            let mut failure = 0;
                            while let Ok(e) = c.pop() {
                                if e.kind == Kind::Acknowledged {
                                    assert_eq!(e.op, 2);
                                    ack += 1
                                }
                                if e.kind == Kind::Failure {
                                    assert_eq!(e.code, Error::Timeout as i32);
                                    failure += 1
                                }
                            }
                            assert_eq!((ack, failure), if mode == 0 { (1, 0) } else { (0, 1) });
                        }
                        c.close().unwrap();
                        peer.join().unwrap();
                        eprintln!(
                            "PUBLIC QoS2 drain proto={} tls={} ws={} mode={} PASS",
                            proto, tls, ws, mode
                        );
                    }
                }
            }
        }
    });
}
#[test]
fn public_qos2_clean_session_loss_and_exhaustion_are_terminal_all_transports() {
    guard(|| {
        let cert = Certs::new();
        for tls in [false, true] {
            for ws in [false, true] {
                for mode in 0..3 {
                    let l = TcpListener::bind("127.0.0.1:0").unwrap();
                    let mut o = cert.options(l.local_addr().unwrap().port(), 4, tls, ws);
                    o.clean_session = mode == 2;
                    o.reconnect.enabled = true;
                    o.reconnect.max_attempts = 1;
                    o.reconnect.base_delay = Duration::from_millis(1);
                    let cfg = cert.config.clone();
                    let peer = thread::spawn(move || {
                        let mut p = Peer::accept(&l, tls, ws, cfg.clone());
                        p.connack(4, false);
                        let first = p.packet();
                        let id = id(&first);
                        drop(p);
                        if mode != 2 {
                            let mut p = Peer::accept(&l, tls, ws, cfg);
                            p.connack(4, mode == 0);
                            if mode == 0 {
                                let repeat = p.packet();
                                assert_eq!(repeat[0] & 8, 8);
                                assert_eq!(&repeat[1..], &first[1..]);
                                assert_eq!(id, owned_support::id(&repeat));
                            } else {
                                p.expect_closed();
                            }
                        }
                    });
                    let mut c = Client::new().unwrap();
                    c.connect(1, o).unwrap();
                    assert_eq!(next(&c).kind, Kind::Connected);
                    c.publish(2, "q".into(), vec![0, 255], 2, false).unwrap();
                    let mut reconnects = 0;
                    let mut disconnects = 0;
                    loop {
                        let e = next(&c);
                        assert_ne!(e.kind, Kind::Acknowledged);
                        if e.kind == Kind::Reconnecting {
                            reconnects += 1;
                        }
                        if mode == 0 && e.kind == Kind::Disconnected {
                            disconnects += 1;
                            if disconnects == 3 {
                                break;
                            }
                        }
                        if e.kind == Kind::Failure {
                            assert_ne!(
                                mode, 0,
                                "retry exhaustion retains existing double-Disconnected contract"
                            );
                            assert_eq!(e.op, 2);
                            assert!(matches!(e.code, 7 | 14));
                            break;
                        }
                    }
                    assert_eq!(reconnects, if mode == 2 { 0 } else { 1 });
                    peer.join().unwrap();
                    c.close().unwrap();
                    eprintln!(
                        "PUBLIC QoS2 finite budget / session loss tls={} ws={} mode={} PASS",
                        tls, ws, mode
                    );
                }
            }
        }
    });
}
