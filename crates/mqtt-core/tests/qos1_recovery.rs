mod common;
use common::{next, packet, try_next, write_connack};
use mqtt_api::{Options, ReconnectPolicy};
use mqtt_core::{Client, Kind};
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};
fn accept(l: &TcpListener) -> TcpStream {
    let (s, _) = l.accept().unwrap();
    s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    s.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
    s
}
fn options(port: u16) -> Options {
    let mut o = Options::new("127.0.0.1", "recover", port);
    o.clean_session = false;
    o.reconnect = ReconnectPolicy {
        enabled: true,
        max_attempts: 2,
        base_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(10),
    };
    o
}
fn pid(p: &[u8]) -> u16 {
    let n = u16::from_be_bytes([p[1], p[2]]) as usize;
    u16::from_be_bytes([p[3 + n], p[4 + n]])
}
fn ack(s: &mut TcpStream, id: u16) {
    s.write_all(&[0x40, 2, (id >> 8) as u8, id as u8]).unwrap();
}
fn until(c: &Client, k: Kind) -> mqtt_core::Event {
    loop {
        let e = try_next(c, Duration::from_secs(2)).expect("bounded completion");
        assert_ne!(e.kind, Kind::Failure, "unexpected failure {e:?}");
        if e.kind == k {
            return e;
        }
    }
}
fn guard(f: impl FnOnce()) {
    mqtt_runtime::init().unwrap();
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    let _ = mqtt_runtime::shutdown();
    if let Err(p) = r {
        std::panic::resume_unwind(p)
    }
}
#[test]
fn persistent_resume_keeps_id_sets_dup_and_completes_once_after_two_drops() {
    guard(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let o = options(l.local_addr().unwrap().port());
        let server = thread::spawn(move || {
            let mut s = accept(&l);
            packet(&mut s);
            write_connack(&mut s, false, 0);
            let p = packet(&mut s);
            assert_eq!(p[0], 0x32);
            let id = pid(&p);
            assert_ne!(id, 0);
            drop(s);
            for last in [false, true] {
                let mut s = accept(&l);
                packet(&mut s);
                write_connack(&mut s, true, 0);
                let replay = packet(&mut s);
                assert_eq!(replay[0], 0x3a, "DUP1 QoS1 on persistent resume");
                assert_eq!(pid(&replay), id);
                assert_eq!(&replay[1..], &p[1..]);
                if last {
                    ack(&mut s, id);
                    assert_eq!(packet(&mut s)[0], 0xe0);
                }
            }
        });
        let mut c = Client::new().unwrap();
        c.connect(1, o).unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        c.publish(2, "x".into(), vec![0, 255], 1, false).unwrap();
        let a = until(&c, Kind::Acknowledged);
        assert_eq!(a.op, 2);
        assert_ne!(a.packet_id, 0);
        c.disconnect(3).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        c.close().unwrap();
        server.join().unwrap();
    });
}
#[test]
fn session_loss_fails_ticket_without_replay() {
    guard(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let o = options(l.local_addr().unwrap().port());
        let server = thread::spawn(move || {
            let mut s = accept(&l);
            packet(&mut s);
            write_connack(&mut s, false, 0);
            assert_eq!(packet(&mut s)[0], 0x32);
            drop(s);
            let mut s = accept(&l);
            packet(&mut s);
            write_connack(&mut s, false, 0);
            let mut b = [0];
            use std::io::Read;
            assert_eq!(
                s.read(&mut b).unwrap(),
                0,
                "new session must close without replay"
            );
        });
        let mut c = Client::new().unwrap();
        c.connect(1, o).unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        c.publish(2, "x".into(), vec![1], 1, false).unwrap();
        let failure = loop {
            let e = try_next(&c, Duration::from_secs(2))
                .expect("interrupted ticket must fail, not strand");
            assert_ne!(e.kind, Kind::Acknowledged);
            if e.kind == Kind::Failure {
                break e;
            }
        };
        assert_eq!(failure.op, 2);
        c.close().unwrap();
        server.join().unwrap();
    });
}

#[test]
fn resumed_ack_releases_queued_work_without_packet_id_reuse_or_duplicate_stages() {
    guard(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let o = options(l.local_addr().unwrap().port());
        let (drop_tx, drop_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let mut s = accept(&l);
            packet(&mut s);
            write_connack(&mut s, false, 0);
            let first = packet(&mut s);
            let first_id = pid(&first);
            drop_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            drop(s);
            let mut s = accept(&l);
            packet(&mut s);
            write_connack(&mut s, true, 0);
            let replay = packet(&mut s);
            assert_eq!(replay[0], 0x3a);
            assert_eq!(pid(&replay), first_id);
            ack(&mut s, first_id);
            let second = packet(&mut s);
            assert_eq!(second[0], 0x32);
            assert_ne!(pid(&second), first_id, "old PUBACK must not match new work");
            ack(&mut s, pid(&second));
            assert_eq!(packet(&mut s)[0], 0xe0);
        });
        let mut c = Client::new().unwrap();
        c.connect(1, o).unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        c.publish(2, "a".into(), vec![0, 255], 1, false).unwrap();
        until(&c, Kind::Sent);
        c.publish(3, "b".into(), vec![128], 1, false).unwrap();
        drop_tx.send(()).unwrap();
        let mut stages = Vec::new();
        let mut acks = Vec::new();
        while acks.len() < 2 {
            let e = try_next(&c, Duration::from_secs(2)).expect("queued work remains owned");
            assert_ne!(e.kind, Kind::Failure);
            if e.kind == Kind::Sent {
                stages.push(e.op)
            }
            if e.kind == Kind::Acknowledged {
                acks.push(e.op)
            }
        }
        assert_eq!(stages, vec![3], "replay must not emit duplicate sent stage");
        assert_eq!(acks, vec![2, 3]);
        c.disconnect(4).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        c.close().unwrap();
        server.join().unwrap();
    });
}

#[test]
fn clean_session_drop_settles_outstanding_work_terminally() {
    guard(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut o = options(l.local_addr().unwrap().port());
        o.clean_session = true;
        let server = thread::spawn(move || {
            let mut s = accept(&l);
            packet(&mut s);
            write_connack(&mut s, false, 0);
            assert_eq!(packet(&mut s)[0], 0x32);
        });
        let mut c = Client::new().unwrap();
        c.connect(1, o).unwrap();
        next(&c);
        c.publish(2, "x".into(), vec![1], 1, false).unwrap();
        let failure = until_failure(&c);
        assert_eq!(failure.op, 2);
        assert_eq!(failure.code, mqtt_api::Error::Network as i32);
        c.close().unwrap();
        server.join().unwrap();
    });
}
fn until_failure(c: &Client) -> mqtt_core::Event {
    loop {
        let e = try_next(c, Duration::from_secs(2)).expect("must settle");
        assert_ne!(e.kind, Kind::Acknowledged);
        if e.kind == Kind::Failure {
            return e;
        }
    }
}

#[test]
fn backoff_drain_does_not_claim_interrupted_work_was_drained() {
    guard(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut o = options(l.local_addr().unwrap().port());
        o.reconnect.base_delay = Duration::from_secs(1);
        o.reconnect.max_delay = Duration::from_secs(1);
        let server = thread::spawn(move || {
            let mut s = accept(&l);
            packet(&mut s);
            write_connack(&mut s, false, 0);
            packet(&mut s);
        });
        let mut c = Client::new().unwrap();
        c.connect(1, o).unwrap();
        next(&c);
        c.publish(2, "x".into(), vec![1], 1, false).unwrap();
        until(&c, Kind::Reconnecting);
        c.begin_drain(Duration::from_millis(100)).unwrap();
        assert_eq!(c.drain_status().unwrap(), mqtt_core::DrainStatus::Failed);
        c.close().unwrap();
        server.join().unwrap();
    });
}

#[test]
fn ack_adjacent_eof_yields_either_real_ack_or_terminal_failure_never_hangs() {
    for enabled in [false, true] {
        guard(|| {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let mut o = options(l.local_addr().unwrap().port());
            o.reconnect.enabled = enabled;
            let server = thread::spawn(move || {
                let mut s = accept(&l);
                packet(&mut s);
                write_connack(&mut s, false, 0);
                let p = packet(&mut s);
                ack(&mut s, pid(&p));
                s.shutdown(std::net::Shutdown::Write).unwrap();
            });
            let mut c = Client::new().unwrap();
            c.connect(1, o).unwrap();
            next(&c);
            c.publish(2, "x".into(), vec![1], 1, false).unwrap();
            let mut acknowledgements = 0;
            let mut attempts = 0;
            loop {
                let e =
                    try_next(&c, Duration::from_secs(2)).expect("ACK+EOF must settle explicitly");
                if e.kind == Kind::Acknowledged {
                    assert_eq!(e.op, 2);
                    acknowledgements += 1;
                }
                if e.kind == Kind::Reconnecting {
                    attempts += 1;
                }
                if e.kind == Kind::Failure
                    || (enabled && e.kind == Kind::Disconnected && attempts == 2)
                {
                    break;
                }
            }
            assert!(acknowledgements <= 1);
            c.close().unwrap();
            server.join().unwrap();
        });
    }
}

#[test]
fn exhaustion_is_terminal_and_never_fabricates_acknowledgement() {
    guard(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let o = options(l.local_addr().unwrap().port());
        let server = thread::spawn(move || {
            let mut s = accept(&l);
            packet(&mut s);
            write_connack(&mut s, false, 0);
            packet(&mut s);
        });
        let mut c = Client::new().unwrap();
        c.connect(1, o).unwrap();
        next(&c);
        c.publish(2, "x".into(), vec![1], 1, false).unwrap();
        let mut drops = 0;
        let mut attempts = 0;
        while drops < 2 {
            let e = try_next(&c, Duration::from_secs(2)).expect("bounded exhaustion");
            assert_ne!(e.kind, Kind::Acknowledged);
            if e.kind == Kind::Disconnected {
                drops += 1
            }
            if e.kind == Kind::Reconnecting {
                attempts += 1
            }
        }
        assert_eq!(attempts, 2);
        c.close().unwrap();
        server.join().unwrap();
    });
}

#[test]
fn queue_limit_and_force_close_or_explicit_disconnect_during_backoff() {
    for force in [false, true] {
        guard(|| {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let mut o = options(l.local_addr().unwrap().port());
            o.reconnect.base_delay = Duration::from_secs(1);
            o.reconnect.max_delay = Duration::from_secs(1);
            let (drop_tx, drop_rx) = std::sync::mpsc::channel();
            let (finish_tx, finish_rx) = std::sync::mpsc::channel();
            let server = thread::spawn(move || {
                let mut s = accept(&l);
                packet(&mut s);
                write_connack(&mut s, false, 0);
                packet(&mut s);
                drop_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                drop(s);
                finish_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                l.set_nonblocking(true).unwrap();
                assert_eq!(
                    l.accept().unwrap_err().kind(),
                    std::io::ErrorKind::WouldBlock
                );
            });
            let mut c = Client::new().unwrap();
            c.connect(1, o).unwrap();
            next(&c);
            c.publish(2, "x".into(), vec![0], 1, false).unwrap();
            until(&c, Kind::Sent);
            for op in 3..35 {
                c.publish(op, "q".into(), vec![1], 1, false).unwrap();
            }
            assert_eq!(
                c.publish(35, "q".into(), vec![2], 1, false),
                Err(mqtt_api::Error::QueueFull)
            );
            drop_tx.send(()).unwrap();
            until(&c, Kind::Reconnecting);
            if !force {
                c.disconnect(36).unwrap();
                let e = next(&c);
                assert_eq!(e.kind, Kind::Disconnected);
                assert_eq!(e.op, 36);
            }
            c.close().unwrap();
            assert_eq!(
                c.publish(37, "x".into(), vec![], 1, false),
                Err(mqtt_api::Error::Closed)
            );
            finish_tx.send(()).unwrap();
            server.join().unwrap();
        });
    }
}

#[test]
fn late_old_puback_cannot_ack_a_different_queued_ticket() {
    guard(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let o = options(l.local_addr().unwrap().port());
        let (drop_tx, drop_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let mut s = accept(&l);
            packet(&mut s);
            write_connack(&mut s, false, 0);
            let p = packet(&mut s);
            let old = pid(&p);
            drop_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            drop(s);
            let mut s = accept(&l);
            packet(&mut s);
            write_connack(&mut s, true, 0);
            let replay = packet(&mut s);
            assert_eq!(pid(&replay), old);
            ack(&mut s, old);
            let new = packet(&mut s);
            assert_ne!(pid(&new), old);
            ack(&mut s, old);
        });
        let mut c = Client::new().unwrap();
        c.connect(1, o).unwrap();
        next(&c);
        c.publish(2, "a".into(), vec![1], 1, false).unwrap();
        until(&c, Kind::Sent);
        c.publish(3, "b".into(), vec![2], 1, false).unwrap();
        drop_tx.send(()).unwrap();
        let mut acknowledged = Vec::new();
        let mut reconnecting = 0;
        loop {
            let e = try_next(&c, Duration::from_secs(2)).expect("late ACK must not strand future");
            if e.kind == Kind::Acknowledged {
                acknowledged.push(e.op)
            }
            if e.kind == Kind::Reconnecting {
                reconnecting += 1
            }
            if (e.kind == Kind::Disconnected && reconnecting >= 3) || e.kind == Kind::Failure {
                break;
            }
        }
        assert_eq!(acknowledged, vec![2]);
        c.close().unwrap();
        server.join().unwrap();
    });
}

#[test]
fn drain_during_replay_waits_for_puback_or_reports_deadline_not_success() {
    for acknowledge in [false, true] {
        guard(|| {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let o = options(l.local_addr().unwrap().port());
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let (proceed_tx, proceed_rx) = std::sync::mpsc::channel();
            let server = thread::spawn(move || {
                let mut s = accept(&l);
                packet(&mut s);
                write_connack(&mut s, false, 0);
                packet(&mut s);
                drop(s);
                let mut s = accept(&l);
                packet(&mut s);
                write_connack(&mut s, true, 0);
                let p = packet(&mut s);
                ready_tx.send(()).unwrap();
                proceed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
                if acknowledge {
                    ack(&mut s, pid(&p));
                    assert_eq!(packet(&mut s)[0], 0xe0);
                } else {
                    use std::io::Read;
                    assert_eq!(s.read(&mut [0]).unwrap(), 0);
                }
            });
            let mut c = Client::new().unwrap();
            c.connect(1, o).unwrap();
            next(&c);
            c.publish(2, "x".into(), vec![0, 255], 1, false).unwrap();
            ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            c.begin_drain(Duration::from_millis(100)).unwrap();
            assert_eq!(
                c.publish(3, "x".into(), vec![], 1, false),
                Err(mqtt_api::Error::Closed)
            );
            proceed_tx.send(()).unwrap();
            let end = std::time::Instant::now() + Duration::from_secs(2);
            while c.drain_status().unwrap() == mqtt_core::DrainStatus::Pending {
                assert!(std::time::Instant::now() < end);
                thread::yield_now();
            }
            assert_eq!(
                c.drain_status().unwrap(),
                if acknowledge {
                    mqtt_core::DrainStatus::Drained
                } else {
                    mqtt_core::DrainStatus::TimedOut
                }
            );
            let mut ack_count = 0;
            while let Ok(e) = c.pop() {
                if e.kind == Kind::Acknowledged {
                    ack_count += 1
                }
            }
            assert_eq!(ack_count, usize::from(acknowledge));
            c.close().unwrap();
            server.join().unwrap();
        });
    }
}

struct NativeBroker {
    child: std::process::Child,
    dir: std::path::PathBuf,
    port: u16,
}
impl NativeBroker {
    fn start() -> Self {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let dir = std::env::temp_dir().join(format!("mqtt-qos1-{}-{}", std::process::id(), port));
        std::fs::create_dir_all(&dir).unwrap();
        let config = dir.join("broker.conf");
        std::fs::write(
            &config,
            format!("listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\n"),
        )
        .unwrap();
        let mut child = std::process::Command::new(
            std::env::var("MQTT5_BROKER").unwrap_or_else(|_| "/usr/sbin/mosquitto".into()),
        )
        .args(["-c", config.to_str().unwrap()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
        let end = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            assert!(child.try_wait().unwrap().is_none());
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            assert!(std::time::Instant::now() < end);
            thread::sleep(Duration::from_millis(10));
        }
        Self { child, dir, port }
    }
}
impl Drop for NativeBroker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
fn forward(s: &mut TcpStream, p: &[u8]) {
    let mut frame = vec![p[0]];
    let mut n = p.len() - 1;
    loop {
        let mut b = (n % 128) as u8;
        n /= 128;
        if n != 0 {
            b |= 128
        }
        frame.push(b);
        if n == 0 {
            break;
        }
    }
    frame.extend_from_slice(&p[1..]);
    s.write_all(&frame).unwrap();
}
#[test]
fn native_mosquitto_persistent_session_replays_after_puback_is_lost() {
    guard(|| {
        let broker = NativeBroker::start();
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let o = options(l.local_addr().unwrap().port());
        let port = broker.port;
        // MQTT-aware TCP fault relay only: Mosquitto is the real session/PUBACK engine.
        let relay = thread::spawn(move || {
            let mut downstream = accept(&l);
            let mut upstream = TcpStream::connect(("127.0.0.1", port)).unwrap();
            upstream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            upstream
                .set_write_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            forward(&mut upstream, &packet(&mut downstream));
            let connack = packet(&mut upstream);
            assert_eq!(connack, [0x20, 0, 0]);
            forward(&mut downstream, &connack);
            let first = packet(&mut downstream);
            let id = pid(&first);
            forward(&mut upstream, &first);
            assert_eq!(packet(&mut upstream), [0x40, (id >> 8) as u8, id as u8]);
            drop(upstream);
            drop(downstream);
            let mut downstream = accept(&l);
            let mut upstream = TcpStream::connect(("127.0.0.1", port)).unwrap();
            upstream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            upstream
                .set_write_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            forward(&mut upstream, &packet(&mut downstream));
            let connack = packet(&mut upstream);
            assert_eq!(connack, [0x20, 1, 0], "real broker preserved session");
            forward(&mut downstream, &connack);
            let replay = packet(&mut downstream);
            assert_eq!(replay[0], 0x3a);
            assert_eq!(pid(&replay), id);
            assert_eq!(&replay[1..], &first[1..]);
            forward(&mut upstream, &replay);
            let receipt = packet(&mut upstream);
            assert_eq!(receipt, [0x40, (id >> 8) as u8, id as u8]);
            forward(&mut downstream, &receipt);
            let disconnect = packet(&mut downstream);
            assert_eq!(disconnect[0], 0xe0);
            forward(&mut upstream, &disconnect);
        });
        let mut c = Client::new().unwrap();
        c.connect(1, o).unwrap();
        next(&c);
        c.publish(2, "native/qos1".into(), vec![0, 255, 128], 1, false)
            .unwrap();
        let ack = until(&c, Kind::Acknowledged);
        assert_eq!(ack.op, 2);
        c.disconnect(3).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        c.close().unwrap();
        relay.join().unwrap();
    });
}

#[test]
fn drain_while_reconnect_connack_is_pending_cancels_the_inflight_socket() {
    guard(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let o = options(l.local_addr().unwrap().port());
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let mut s = accept(&l);
            packet(&mut s);
            write_connack(&mut s, false, 0);
            packet(&mut s);
            drop(s);
            let mut s = accept(&l);
            packet(&mut s);
            ready_tx.send(()).unwrap();
            use std::io::Read;
            assert_eq!(
                s.read(&mut [0]).unwrap(),
                0,
                "drain cancels connect before CONNACK timeout"
            );
        });
        let mut c = Client::new().unwrap();
        c.connect(1, o).unwrap();
        next(&c);
        c.publish(2, "x".into(), vec![1], 1, false).unwrap();
        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        c.begin_drain(Duration::from_millis(100)).unwrap();
        assert_eq!(c.drain_status().unwrap(), mqtt_core::DrainStatus::Failed);
        server.join().unwrap();
        c.close().unwrap();
    });
}

#[test]
fn local_protocol_rejection_is_terminal_not_a_live_socket_reconnect() {
    guard(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let o = options(l.local_addr().unwrap().port());
        let server = thread::spawn(move || {
            let mut s = accept(&l);
            packet(&mut s);
            write_connack(&mut s, false, 0);
            s.write_all(&[0x36, 6, 0, 1, b'x', 0, 9, 1]).unwrap();
            use std::io::Read;
            assert_eq!(s.read(&mut [0]).unwrap(), 0);
        });
        let mut c = Client::new().unwrap();
        c.connect(1, o).unwrap();
        next(&c);
        let e = next(&c);
        assert_eq!(e.kind, Kind::Failure);
        assert_eq!(e.code, mqtt_api::Error::Network as i32);
        c.close().unwrap();
        server.join().unwrap();
    });
}

#[test]
fn successful_connacks_do_not_reset_budget_for_the_same_unacked_ticket() {
    guard(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let o = options(l.local_addr().unwrap().port());
        let server = thread::spawn(move || {
            let mut original = None;
            for attempt in 0..3 {
                let mut s = accept(&l);
                packet(&mut s);
                write_connack(&mut s, attempt != 0, 0);
                let p = packet(&mut s);
                if attempt == 0 {
                    original = Some(p.clone());
                } else {
                    assert_eq!(p[0], 0x3a);
                    assert_eq!(&p[1..], &original.as_ref().unwrap()[1..]);
                }
            }
        });
        let mut c = Client::new().unwrap();
        c.connect(1, o).unwrap();
        next(&c);
        c.publish(2, "x".into(), vec![1], 1, false).unwrap();
        let expected = [
            Kind::Disconnected,
            Kind::Reconnecting,
            Kind::Reconnected,
            Kind::SubscriptionsAligned,
            Kind::Disconnected,
            Kind::Reconnecting,
            Kind::Reconnected,
            Kind::SubscriptionsAligned,
            Kind::Disconnected,
            Kind::Disconnected,
        ];
        for kind in expected {
            loop {
                let e = next(&c);
                if matches!(e.kind, Kind::Queued | Kind::Sent) {
                    continue;
                }
                assert_eq!(e.kind, kind, "bounded packet-owner retry schedule");
                break;
            }
        }
        c.close().unwrap();
        server.join().unwrap();
    });
}
