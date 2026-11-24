//! PUBLIC client paths, separate from the private transport allocator oracle.
mod owned_support;
use mqtt_api::Error;
use mqtt_core::{Client, Kind};
use owned_support::*;
use std::{
    net::TcpListener,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
fn frame(h: u8, b: &[u8]) -> Vec<u8> {
    let mut w = vec![h];
    if b.len() < 126 {
        w.push(b.len() as u8)
    } else {
        w.push(126);
        w.extend((b.len() as u16).to_be_bytes())
    }
    w.extend(b);
    w
}
#[test]
fn public_fragment_totals_and_one_byte_fragment_floods_are_bounded_all_ws_cells() {
    guard(|| {
        let cert = Certs::new();
        for proto in [4, 5] {
            for tls in [false, true] {
                for attack_mode in 0..4 {
                    let l = TcpListener::bind("127.0.0.1:0").unwrap();
                    let o = cert.options(l.local_addr().unwrap().port(), proto, tls, true);
                    let config = cert.config.clone();
                    let server = thread::spawn(move || {
                        let mut p = Peer::accept(&l, tls, true, config);
                        p.connack(proto, false);
                        let mut attack = Vec::new();
                        if attack_mode == 1 {
                            for i in 0..131081 {
                                attack.extend([if i == 0 { 2 } else { 0 }, 1, 0]);
                            }
                        } else if attack_mode == 0 {
                            let payload = vec![0; 65535];
                            attack.extend(frame(2, &payload));
                            attack.extend(frame(0, &payload));
                            attack.extend(frame(0x80, &[0; 11]));
                        } else if attack_mode == 2 {
                            // A 3MiB declared frame header, deliberately NO body.
                            attack.extend([0x82, 127]);
                            attack.extend((3u64 * 1024 * 1024).to_be_bytes());
                        } else {
                            // Illegal >125-byte control payload remains bounded and typed.
                            attack.extend(frame(0x89, &[0; 126]));
                        }
                        let _ = p.raw_ws(&attack);
                        p.expect_closed();
                    });
                    let mut c = Client::new().unwrap();
                    c.connect(1, o).unwrap();
                    assert_eq!(next(&c).kind, Kind::Connected);
                    let e = next(&c);
                    assert_eq!(
                        (e.kind, e.code),
                        (
                            Kind::Failure,
                            if attack_mode == 3 {
                                Error::Network as i32
                            } else {
                                Error::Overflow as i32
                            }
                        )
                    );
                    server.join().unwrap();
                    c.close().unwrap();
                    eprintln!(
                        "PUBLIC fragment/header/control attack_mode={} proto={} tls={} typed failure+teardown PASS",
                        attack_mode, proto, tls
                    );
                }
            }
        }
    });
}
#[test]
fn public_slow_consumer_reaches_exact_event_cap_and_closes_all_ws_cells() {
    guard(|| {
        let cert = Certs::new();
        for proto in [4, 5] {
            for tls in [false, true] {
                let l = TcpListener::bind("127.0.0.1:0").unwrap();
                let o = cert.options(l.local_addr().unwrap().port(), proto, tls, true);
                let cfg = cert.config.clone();
                let (go, wait) = mpsc::channel();
                let (closed, done) = mpsc::channel();
                let server = thread::spawn(move || {
                    let mut p = Peer::accept(&l, tls, true, cfg);
                    p.connack(proto, false);
                    wait.recv().unwrap();
                    let mut b = vec![0, 1, b'q'];
                    if proto == 5 {
                        b.push(0)
                    }
                    b.push(255);
                    for _ in 0..129 {
                        p.send(&wire(0x30, &b));
                    }
                    p.expect_closed();
                    closed.send(()).unwrap();
                });
                let mut c = Client::new().unwrap();
                c.connect(1, o).unwrap();
                assert_eq!(next(&c).kind, Kind::Connected);
                go.send(()).unwrap();
                done.recv_timeout(Duration::from_secs(3))
                    .expect("must terminate without application polling");
                for _ in 0..128 {
                    let e = next(&c);
                    assert_eq!(e.kind, Kind::Message);
                    assert_eq!(e.payload, [255]);
                }
                let e = next(&c);
                assert_eq!((e.kind, e.code), (Kind::Failure, Error::Overflow as i32));
                assert_eq!(c.pop().unwrap_err(), Error::Empty);
                server.join().unwrap();
                c.close().unwrap();
                eprintln!(
                    "PUBLIC slow consumer proto={} tls={} exactly128+Overflow+teardown PASS",
                    proto, tls
                );
            }
        }
    });
}
#[test]
fn public_control_flood_has_fenced_progress_and_immediate_close_all_ws_cells() {
    guard(|| {
        let cert = Certs::new();
        for proto in [4, 5] {
            for tls in [false, true] {
                let l = TcpListener::bind("127.0.0.1:0").unwrap();
                let o = cert.options(l.local_addr().unwrap().port(), proto, tls, true);
                let cfg = cert.config.clone();
                let (go, wait) = mpsc::channel();
                let server = thread::spawn(move || {
                    let mut p = Peer::accept(&l, tls, true, cfg);
                    p.connack(proto, false);
                    let q = p.packet();
                    let id = id(&q);
                    p.send(&[0x50, 2, id[0], id[1]]);
                    let mut rel = vec![0x62, id[0], id[1]];
                    if proto == 5 {
                        rel.extend([0, 0]);
                    }
                    assert_eq!(p.packet(), rel);
                    p.send(&[0x70, 2, id[0], id[1]]);
                    let mut attack = Vec::new();
                    for _ in 0..20000 {
                        attack.extend([0x89, 0]);
                    }
                    let mut b = vec![0, 1, b'f'];
                    if proto == 5 {
                        b.push(0)
                    }
                    b.push(255);
                    attack.extend(frame(0x82, &wire(0x30, &b)));
                    p.raw_ws(&attack).unwrap();
                    wait.recv().unwrap();
                    p.closed_after_pongs();
                });
                let mut c = Client::new().unwrap();
                c.connect(1, o).unwrap();
                assert_eq!(next(&c).kind, Kind::Connected);
                c.publish(2, "q".into(), vec![0, 255], 2, false).unwrap();
                let mut ack = false;
                loop {
                    let e = next(&c);
                    assert_ne!(e.kind, Kind::Failure);
                    if e.kind == Kind::Acknowledged {
                        ack = true
                    }
                    if e.kind == Kind::Message {
                        assert_eq!(e.payload, [255]);
                        break;
                    }
                }
                assert!(ack);
                let start = Instant::now();
                c.close().unwrap();
                assert!(start.elapsed() < Duration::from_secs(1));
                go.send(()).unwrap();
                server.join().unwrap();
                eprintln!(
                    "PUBLIC 20000 controls proto={} tls={} fenced progress+close PASS",
                    proto, tls
                );
            }
        }
    });
}

#[test]
#[cfg(target_os = "linux")]
fn public_blocked_writer_has_finite_admission_deadline_and_closes_all_ws_cells() {
    guard(|| {
        let cert = Certs::new();
        for proto in [4, 5] {
            for tls in [false, true] {
                let l = TcpListener::bind("127.0.0.1:0").unwrap();
                let o = cert.options(l.local_addr().unwrap().port(), proto, tls, true);
                let cfg = cert.config.clone();
                let (release, hold) = mpsc::channel();
                let server = thread::spawn(move || {
                    let mut p = Peer::accept_small_receive(&l, tls, true, cfg);
                    p.connack(proto, false);
                    hold.recv_timeout(Duration::from_secs(10))
                        .expect("bounded stalled writer");
                    drop(p);
                });
                let mut c = Client::new().unwrap();
                c.connect(1, o).unwrap();
                assert_eq!(next(&c).kind, Kind::Connected);
                let start = Instant::now();
                let (mut admitted, mut sent, mut full) = (0u64, 0u64, 0u64);
                'events: loop {
                    while let Ok(e) = c.pop() {
                        match e.kind {
                            Kind::Failure => {
                                assert_eq!(e.code, Error::Timeout as i32);
                                break 'events;
                            }
                            Kind::Sent => sent += 1,
                            Kind::Queued => {}
                            _ => panic!("unexpected event {e:?}"),
                        }
                    }
                    assert!(
                        start.elapsed() < Duration::from_secs(8),
                        "writer deadline did not terminate"
                    );
                    assert!(admitted < 1024, "fixture failed to close receive window");
                    match c.publish(admitted + 2, "q".into(), vec![255; 65536], 0, false) {
                        Ok(()) => admitted += 1,
                        Err(Error::QueueFull) => full += 1,
                        Err(Error::Busy) => {}
                        e => panic!("unexpected admission {e:?}"),
                    };
                    thread::yield_now();
                }
                assert!(sent > 0 && admitted > sent && full > 0);
                assert!(admitted - sent <= 38, "bounded command/engine window");
                assert!(start.elapsed() >= Duration::from_millis(4500));
                let close = Instant::now();
                c.close().unwrap();
                assert!(close.elapsed() < Duration::from_secs(1));
                assert_eq!(
                    c.publish(admitted + 3, "q".into(), vec![1], 0, false),
                    Err(Error::Closed)
                );
                release.send(()).unwrap();
                server.join().unwrap();
                eprintln!("PUBLIC blocked writer proto={} tls={} admitted={} flushed={} full={} finite5s Timeout+close PASS",proto,tls,admitted,sent,full);
            }
        }
    });
}
