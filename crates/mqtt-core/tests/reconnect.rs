mod common;
use common::{connect_clean_session, next, packet, try_next, write_connack};
use mqtt_api::{Error, Options, ReconnectPolicy};
use mqtt_core::{equal_jitter, reconnect_delay_cap, Client, Kind};
use std::{
    io::ErrorKind,
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

fn runtime_guard<T>(f: impl FnOnce() -> T) -> T {
    mqtt_runtime::init().unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    let _ = mqtt_runtime::shutdown();
    match result {
        Ok(v) => v,
        Err(p) => std::panic::resume_unwind(p),
    }
}

fn policy(enabled: bool, max_attempts: u32, base_ms: u64, max_ms: u64) -> ReconnectPolicy {
    ReconnectPolicy {
        enabled,
        max_attempts,
        base_delay: Duration::from_millis(base_ms),
        max_delay: Duration::from_millis(max_ms),
    }
}

fn options(id: &str, port: u16, reconnect: ReconnectPolicy) -> Options {
    let mut o = Options::new("127.0.0.1", id, port);
    o.reconnect = reconnect;
    o
}

fn accept(listener: &TcpListener) -> TcpStream {
    let (s, _) = listener.accept().unwrap();
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    s.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
    s
}

fn handshake(stream: &mut TcpStream, session_present: bool) -> Vec<u8> {
    let connect = packet(stream);
    assert_eq!(connect[0], 0x10);
    write_connack(stream, session_present, 0);
    connect
}

fn no_accept(listener: &TcpListener, window: Duration) {
    listener.set_nonblocking(true).unwrap();
    let until = Instant::now() + window;
    while Instant::now() < until {
        match listener.accept() {
            Ok(_) => panic!("unexpected extra connection"),
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {}
            Err(e) => panic!("{e}"),
        }
        thread::sleep(Duration::from_millis(5));
    }
    listener.set_nonblocking(false).unwrap();
}

#[test]
fn delay_cap_and_equal_jitter_are_bounded() {
    let base = Duration::from_millis(100);
    let max = Duration::from_millis(800);
    assert_eq!(reconnect_delay_cap(1, base, max), base);
    assert_eq!(
        reconnect_delay_cap(2, base, max),
        Duration::from_millis(200)
    );
    assert_eq!(
        reconnect_delay_cap(3, base, max),
        Duration::from_millis(400)
    );
    assert_eq!(
        reconnect_delay_cap(4, base, max),
        Duration::from_millis(800)
    );
    assert_eq!(
        reconnect_delay_cap(10, base, max),
        Duration::from_millis(800)
    );
    let cap = Duration::from_millis(200);
    for sample in [0.0, 0.25, 0.5, 0.75, 0.999] {
        let d = equal_jitter(cap, sample);
        assert!(d >= Duration::from_millis(100), "{d:?}");
        assert!(d <= cap, "{d:?}");
    }
    assert_eq!(equal_jitter(cap, 0.0), Duration::from_millis(100));
    assert_eq!(equal_jitter(Duration::ZERO, 0.5), Duration::ZERO);
}

#[test]
fn default_policy_is_off_and_existing_drop_is_failure() {
    runtime_guard(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let mut s = accept(&listener);
            handshake(&mut s, false);
            drop(s);
            no_accept(&listener, Duration::from_millis(250));
        });
        let mut c = Client::new().unwrap();
        let o = Options::new("127.0.0.1", "default-off", port);
        assert!(!o.reconnect.enabled);
        c.connect(1, o).unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        let failure = next(&c);
        assert_eq!(failure.kind, Kind::Failure);
        assert!(try_next(&c, Duration::from_millis(200))
            .is_none_or(|e| e.kind != Kind::Reconnecting && e.kind != Kind::Reconnected));
        server.join().unwrap();
        c.close().unwrap();
    });
}

#[test]
fn first_connect_refusal_does_not_enter_reconnect_loop() {
    runtime_guard(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let mut s = accept(&listener);
            packet(&mut s);
            write_connack(&mut s, false, 3);
            drop(s);
            no_accept(&listener, Duration::from_millis(300));
        });
        let mut c = Client::new().unwrap();
        c.connect(1, options("first-fail", port, policy(true, 5, 20, 20)))
            .unwrap();
        let failure = next(&c);
        assert_eq!(failure.kind, Kind::Failure);
        assert_eq!(failure.code, Error::Refused as i32);
        assert!(try_next(&c, Duration::from_millis(250)).is_none());
        server.join().unwrap();
        c.close().unwrap();
    });
}

#[test]
fn drop_after_success_emits_disconnected_reconnecting_then_reconnected() {
    runtime_guard(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (started, wait_start) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut first = accept(&listener);
            handshake(&mut first, false);
            started.send(()).unwrap();
            drop(first);
            let mut second = accept(&listener);
            handshake(&mut second, false);
            thread::sleep(Duration::from_millis(50));
        });
        let mut c = Client::new().unwrap();
        c.connect(1, options("resume", port, policy(true, 3, 20, 20)))
            .unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        wait_start.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        assert_eq!(next(&c).kind, Kind::Reconnecting);
        let reconnected = next(&c);
        assert_eq!(reconnected.kind, Kind::Reconnected);
        assert!(!reconnected.session_present);
        server.join().unwrap();
        c.close().unwrap();
    });
}

#[test]
fn backoff_uses_jittered_delay_and_stops_after_max_attempts() {
    runtime_guard(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let times = Arc::new(Mutex::new(Vec::new()));
        let times_t = times.clone();
        let server = thread::spawn(move || {
            let mut first = accept(&listener);
            handshake(&mut first, false);
            drop(first);
            for _ in 0..2 {
                let mut s = accept(&listener);
                times_t.lock().unwrap().push(Instant::now());
                packet(&mut s);
                write_connack(&mut s, false, 3);
                drop(s);
            }
            no_accept(&listener, Duration::from_millis(400));
        });
        let mut c = Client::new().unwrap();
        c.connect(1, options("backoff", port, policy(true, 2, 120, 120)))
            .unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        assert_eq!(next(&c).kind, Kind::Disconnected);
        assert_eq!(next(&c).kind, Kind::Reconnecting);
        assert_eq!(next(&c).kind, Kind::Reconnecting);
        let terminal = next(&c);
        assert_eq!(terminal.kind, Kind::Disconnected);
        assert!(try_next(&c, Duration::from_millis(200))
            .is_none_or(|e| e.kind != Kind::Reconnecting && e.kind != Kind::Connected));
        server.join().unwrap();
        let stamps = times.lock().unwrap().clone();
        assert_eq!(stamps.len(), 2);
        let gap = stamps[1].duration_since(stamps[0]);
        assert!(
            gap >= Duration::from_millis(60) && gap <= Duration::from_millis(250),
            "gap {gap:?} should include equal jitter around 120ms"
        );
        c.close().unwrap();
    });
}

#[test]
fn never_opens_two_connections_at_once() {
    runtime_guard(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let concurrent = Arc::new(AtomicUsize::new(0));
        let max = Arc::new(AtomicUsize::new(0));
        let concurrent_t = concurrent.clone();
        let max_t = max.clone();
        let (hold, release) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut first = accept(&listener);
            handshake(&mut first, false);
            drop(first);
            let mut second = accept(&listener);
            let now = concurrent_t.fetch_add(1, Ordering::SeqCst) + 1;
            max_t.fetch_max(now, Ordering::SeqCst);
            handshake(&mut second, false);
            // Hold the reconnect open long enough that a second attempt would overlap.
            thread::sleep(Duration::from_millis(250));
            // A racing second connection would show up here.
            listener.set_nonblocking(true).unwrap();
            if listener.accept().is_ok() {
                let now = concurrent_t.fetch_add(1, Ordering::SeqCst) + 1;
                max_t.fetch_max(now, Ordering::SeqCst);
            }
            concurrent_t.fetch_sub(1, Ordering::SeqCst);
            drop(second);
            hold.send(()).unwrap();
        });
        let mut c = Client::new().unwrap();
        c.connect(1, options("one-conn", port, policy(true, 3, 20, 20)))
            .unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        assert_eq!(next(&c).kind, Kind::Disconnected);
        assert_eq!(next(&c).kind, Kind::Reconnecting);
        assert_eq!(next(&c).kind, Kind::Reconnected);
        assert_eq!(
            c.connect(99, options("one-conn", port, policy(true, 3, 20, 20))),
            Err(Error::Busy)
        );
        release.recv_timeout(Duration::from_secs(3)).unwrap();
        server.join().unwrap();
        assert_eq!(max.load(Ordering::SeqCst), 1);
        c.close().unwrap();
    });
}

#[test]
fn disconnect_during_backoff_stops_reconnect() {
    runtime_guard(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (dropped, wait_drop) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut first = accept(&listener);
            handshake(&mut first, false);
            drop(first);
            dropped.send(()).unwrap();
            no_accept(&listener, Duration::from_millis(400));
        });
        let mut c = Client::new().unwrap();
        c.connect(1, options("stop-disc", port, policy(true, 5, 300, 300)))
            .unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        wait_drop.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        assert_eq!(next(&c).kind, Kind::Reconnecting);
        c.disconnect(2).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        assert!(try_next(&c, Duration::from_millis(200))
            .is_none_or(|e| e.kind != Kind::Reconnected && e.kind != Kind::Connected));
        server.join().unwrap();
        c.close().unwrap();
    });
}

#[test]
fn drain_during_backoff_stops_reconnect() {
    runtime_guard(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (dropped, wait_drop) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut first = accept(&listener);
            handshake(&mut first, false);
            drop(first);
            dropped.send(()).unwrap();
            no_accept(&listener, Duration::from_millis(400));
        });
        let mut c = Client::new().unwrap();
        c.connect(1, options("stop-drain", port, policy(true, 5, 300, 300)))
            .unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        wait_drop.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        assert_eq!(next(&c).kind, Kind::Reconnecting);
        c.begin_drain(Duration::from_millis(200)).unwrap();
        let end = Instant::now() + Duration::from_secs(2);
        loop {
            match c.drain_status().unwrap() {
                mqtt_core::DrainStatus::Drained | mqtt_core::DrainStatus::Failed => break,
                other => {
                    assert!(Instant::now() < end, "drain stuck at {other:?}");
                    thread::sleep(Duration::from_millis(5));
                }
            }
        }
        assert!(
            try_next(&c, Duration::from_millis(150)).is_none_or(|e| e.kind != Kind::Reconnected)
        );
        server.join().unwrap();
        c.close().unwrap();
    });
}

#[test]
fn force_close_during_backoff_stops_reconnect() {
    runtime_guard(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (dropped, wait_drop) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut first = accept(&listener);
            handshake(&mut first, false);
            drop(first);
            dropped.send(()).unwrap();
            no_accept(&listener, Duration::from_millis(300));
        });
        let mut c = Client::new().unwrap();
        c.connect(1, options("stop-close", port, policy(true, 5, 300, 300)))
            .unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        wait_drop.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        assert_eq!(next(&c).kind, Kind::Reconnecting);
        c.close().unwrap();
        assert_eq!(c.peek().unwrap_err(), Error::Closed);
        server.join().unwrap();
    });
}

#[test]
fn clean_session_flag_is_written_on_connect() {
    runtime_guard(|| {
        for clean in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            let (tx, rx) = mpsc::channel();
            let server = thread::spawn(move || {
                let mut s = accept(&listener);
                let connect = handshake(&mut s, false);
                tx.send(connect_clean_session(&connect)).unwrap();
            });
            let mut c = Client::new().unwrap();
            let mut o = Options::new("127.0.0.1", format!("clean-{clean}"), port);
            o.clean_session = clean;
            c.connect(1, o).unwrap();
            assert_eq!(next(&c).kind, Kind::Connected);
            assert_eq!(rx.recv_timeout(Duration::from_secs(3)).unwrap(), clean);
            server.join().unwrap();
            c.close().unwrap();
        }
    });
}

#[test]
fn default_clean_session_remains_true() {
    let o = Options::new("127.0.0.1", "defaults", 1883);
    assert!(o.clean_session);
    assert!(!o.reconnect.enabled);
}
