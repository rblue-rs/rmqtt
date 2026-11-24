mod common;
use common::{
    next, packet, subscribe_filters, try_next, write_connack, write_suback, write_unsuback,
};
use mqtt_api::{Options, ReconnectPolicy};
use mqtt_core::{Client, Kind};
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    sync::mpsc,
    thread,
    time::Duration,
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

fn policy() -> ReconnectPolicy {
    ReconnectPolicy {
        enabled: true,
        max_attempts: 3,
        base_delay: Duration::from_millis(20),
        max_delay: Duration::from_millis(20),
    }
}

fn opts(id: &str, port: u16, clean: bool) -> Options {
    let mut o = Options::new("127.0.0.1", id, port);
    o.clean_session = clean;
    o.reconnect = policy();
    o
}

fn accept(listener: &TcpListener) -> TcpStream {
    let (s, _) = listener.accept().unwrap();
    s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    s.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
    s
}

fn serve_until_subscribe(stream: &mut TcpStream) -> Vec<(String, u8)> {
    loop {
        let p = packet(stream);
        match p[0] & 0xf0 {
            0x80 => {
                let filters = subscribe_filters(&p);
                write_suback(
                    stream,
                    u16::from_be_bytes([p[1], p[2]]),
                    &filters.iter().map(|(_, q)| *q).collect::<Vec<_>>(),
                );
                return filters;
            }
            0xa0 => write_unsuback(stream, u16::from_be_bytes([p[1], p[2]])),
            _ => {}
        }
    }
}

// A SUBACK write is not proof that the client has observed it. Keep this
// connection alive until the application observes reconciliation and explicitly
// disconnects; EOF adjacent to SUBACK can prevent the upstream event being yielded.
fn finish_reconciliation(stream: &mut TcpStream, result: mpsc::Sender<Vec<(String, u8)>>) {
    let mut filters = serve_until_subscribe(stream);
    filters.extend(subscribes_until_disconnect(stream));
    result.send(filters).unwrap();
}

#[test]
fn reconciliation_fixture_stays_live_until_protocol_disconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (tx, rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let mut stream = accept(&listener);
        assert_eq!(packet(&mut stream)[0], 0x10);
        write_connack(&mut stream, false, 0);
        finish_reconciliation(&mut stream, tx);
    });
    // Independent raw MQTT peer: no Client/supervisor transition code is reused.
    let mut peer = TcpStream::connect(("127.0.0.1", port)).unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    peer.write_all(&[
        0x10, 16, 0, 4, b'M', b'Q', b'T', b'T', 4, 2, 0, 0, 0, 4, b't', b'e', b's', b't',
    ])
    .unwrap();
    assert_eq!(packet(&mut peer), [0x20, 0, 0]);
    peer.write_all(&[0x82, 6, 0, 1, 0, 1, b'a', 1]).unwrap();
    assert_eq!(packet(&mut peer), [0x90, 0, 1, 1]);
    // PINGRESP is a positive wire barrier, not a timeout-based absence guess.
    peer.write_all(&[0xc0, 0]).unwrap();
    let response = common::try_packet(&mut peer);
    assert!(
        response.is_ok(),
        "fixture must remain live after SUBACK: {response:?}"
    );
    assert_eq!(response.unwrap(), [0xd0]);
    peer.write_all(&[0xe0, 0]).unwrap();
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(3)).unwrap(),
        vec![("a".into(), 1)]
    );
    server.join().unwrap();
}

// DISCONNECT is an ordered wire fence: after the application observes the
// reconciliation event, capture ALL SUBSCRIBEs up to that explicit terminal packet.
// EOF/timeout is a failure, never evidence of an empty subscription set.
fn subscribes_until_disconnect(stream: &mut TcpStream) -> Vec<(String, u8)> {
    let mut collected = Vec::new();
    loop {
        let p = packet(stream);
        match p[0] & 0xf0 {
            0x80 => {
                let filters = subscribe_filters(&p);
                write_suback(
                    stream,
                    u16::from_be_bytes([p[1], p[2]]),
                    &filters.iter().map(|(_, q)| *q).collect::<Vec<_>>(),
                );
                collected.extend(filters);
            }
            0xc0 => stream.write_all(&[0xd0, 0]).unwrap(),
            0xe0 => return collected,
            other => panic!("unexpected packet before disconnect fence: {other:#x}"),
        }
    }
}

// Retain the original two-second observation bound, with strict event order and
// ACK correlation. No sleep or timeout extension substitutes for a completion.
fn observe_resubscribe(c: &Client, filter: &str) {
    let end = std::time::Instant::now() + Duration::from_secs(2);
    let subscribed = try_next(c, end.saturating_duration_since(std::time::Instant::now()))
        .expect("application should observe resubscribe");
    assert_eq!(subscribed.kind, Kind::Subscribed);
    assert_eq!(subscribed.op, 0);
    assert_eq!(subscribed.code, 0);
    assert_ne!(subscribed.packet_id, 0);
    assert_eq!(subscribed.topic, filter.as_bytes());
    assert_eq!(subscribed.qos, 1);
    let done = try_next(c, end.saturating_duration_since(std::time::Instant::now()))
        .expect("resubscribe completion should be observable");
    assert_eq!(done.kind, Kind::SubscriptionsResubscribed);
}

#[test]
fn duplicate_subscribe_of_same_filter_does_not_crash() {
    runtime_guard(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (done_tx, done_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut s = accept(&listener);
            packet(&mut s);
            write_connack(&mut s, false, 0);
            let a = serve_until_subscribe(&mut s);
            let b = serve_until_subscribe(&mut s);
            assert_eq!(a, vec![("dup/topic".into(), 1)]);
            assert_eq!(b, vec![("dup/topic".into(), 0)]);
            done_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        });
        let mut c = Client::new().unwrap();
        c.connect(1, Options::new("127.0.0.1", "dup-sub", port))
            .unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        c.subscribe(2, "dup/topic".into(), 1).unwrap();
        assert_eq!(next(&c).kind, Kind::Subscribed);
        c.subscribe(3, "dup/topic".into(), 0).unwrap();
        assert_eq!(next(&c).kind, Kind::Subscribed);
        done_tx.send(()).unwrap();
        server.join().unwrap();
        c.close().unwrap();
    });
}

#[test]
fn session_present_zero_resubscribes_expectation_set_after_unsubscribe() {
    runtime_guard(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::channel();
        let (unsub_done_tx, unsub_done_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut first = accept(&listener);
            packet(&mut first);
            write_connack(&mut first, false, 0);
            assert_eq!(
                serve_until_subscribe(&mut first),
                vec![("keep/a".into(), 1)]
            );
            assert_eq!(
                serve_until_subscribe(&mut first),
                vec![("drop/b".into(), 1)]
            );
            // unsubscribe drop/b
            let u = packet(&mut first);
            assert_eq!(u[0] & 0xf0, 0xa0);
            write_unsuback(&mut first, u16::from_be_bytes([u[1], u[2]]));
            unsub_done_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            drop(first);
            let mut second = accept(&listener);
            packet(&mut second);
            write_connack(&mut second, false, 0);
            finish_reconciliation(&mut second, tx);
        });
        let mut c = Client::new().unwrap();
        c.connect(1, opts("expect-0", port, false)).unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        c.subscribe(2, "keep/a".into(), 1).unwrap();
        assert_eq!(next(&c).kind, Kind::Subscribed);
        c.subscribe(3, "drop/b".into(), 1).unwrap();
        assert_eq!(next(&c).kind, Kind::Subscribed);
        c.unsubscribe(4, "drop/b".into()).unwrap();
        assert_eq!(next(&c).kind, Kind::Unsubscribed);
        unsub_done_tx.send(()).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        assert_eq!(next(&c).kind, Kind::Reconnecting);
        let reconnected = next(&c);
        assert_eq!(reconnected.kind, Kind::Reconnected);
        assert!(!reconnected.session_present);
        observe_resubscribe(&c, "keep/a");
        c.disconnect(5).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        let restored = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(restored, vec![("keep/a".into(), 1u8)]);
        server.join().unwrap();
        c.close().unwrap();
    });
}

#[test]
fn session_present_one_does_not_automatically_full_resubscribe() {
    runtime_guard(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::channel();
        let (sub_done_tx, sub_done_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut first = accept(&listener);
            packet(&mut first);
            write_connack(&mut first, false, 0);
            assert_eq!(
                serve_until_subscribe(&mut first),
                vec![("held/filter".into(), 1)]
            );
            sub_done_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            drop(first);
            let mut second = accept(&listener);
            packet(&mut second);
            write_connack(&mut second, true, 0);
            let extras = subscribes_until_disconnect(&mut second);
            tx.send(extras).unwrap();
        });
        let mut c = Client::new().unwrap();
        c.connect(1, opts("expect-1", port, false)).unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        c.subscribe(2, "held/filter".into(), 1).unwrap();
        assert_eq!(next(&c).kind, Kind::Subscribed);
        sub_done_tx.send(()).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        assert_eq!(next(&c).kind, Kind::Reconnecting);
        let reconnected = next(&c);
        assert_eq!(reconnected.kind, Kind::Reconnected);
        assert!(reconnected.session_present);
        let aligned = next(&c);
        assert_eq!(aligned.kind, Kind::SubscriptionsAligned);
        c.disconnect(3).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        let extras = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert!(
            extras.is_empty(),
            "session_present=1 must not automatically re-SUBSCRIBE held filters: {extras:?}"
        );
        server.join().unwrap();
        c.close().unwrap();
    });
}

#[test]
fn clean_session_resubscribes_even_if_connack_claims_session() {
    runtime_guard(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::channel();
        let (sub_done_tx, sub_done_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut first = accept(&listener);
            packet(&mut first);
            write_connack(&mut first, false, 0);
            assert_eq!(serve_until_subscribe(&mut first), vec![("c/s".into(), 1)]);
            sub_done_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            drop(first);
            let mut second = accept(&listener);
            packet(&mut second);
            write_connack(&mut second, true, 0);
            finish_reconciliation(&mut second, tx);
        });
        let mut c = Client::new().unwrap();
        c.connect(1, opts("clean-overrides", port, true)).unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        c.subscribe(2, "c/s".into(), 1).unwrap();
        assert_eq!(next(&c).kind, Kind::Subscribed);
        sub_done_tx.send(()).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        assert_eq!(next(&c).kind, Kind::Reconnecting);
        let connected = next(&c);
        assert_eq!(connected.kind, Kind::Reconnected);
        assert!(connected.session_present);
        observe_resubscribe(&c, "c/s");
        c.disconnect(3).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        let restored = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(restored, vec![("c/s".into(), 1u8)]);
        server.join().unwrap();
        c.close().unwrap();
    });
}

#[test]
fn cancelled_or_unacked_local_subscribe_still_updates_expectation_set() {
    runtime_guard(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::channel();
        let (hold_tx, hold_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut first = accept(&listener);
            packet(&mut first);
            write_connack(&mut first, false, 0);
            let pending = packet(&mut first);
            assert_eq!(pending[0] & 0xf0, 0x80);
            // No SUBACK: local waiter may time out/cancel, but the packet is not withdrawn.
            hold_tx.send(subscribe_filters(&pending)).unwrap();
            go_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            drop(first);
            let mut second = accept(&listener);
            packet(&mut second);
            write_connack(&mut second, false, 0);
            finish_reconciliation(&mut second, tx);
        });
        let mut c = Client::new().unwrap();
        c.connect(1, opts("local-decision", port, true)).unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        c.subscribe(2, "pending/filter".into(), 1).unwrap();
        let accepted = hold_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(accepted, vec![("pending/filter".into(), 1)]);
        // Do not wait for SUBACK; the expectation set already follows the local decision.
        go_tx.send(()).unwrap();
        let interrupted = next(&c);
        assert_eq!(interrupted.kind, Kind::Subscribed);
        assert_eq!(interrupted.op, 2);
        assert_eq!(interrupted.code, mqtt_api::Error::Network as i32);
        assert_eq!(next(&c).kind, Kind::Disconnected);
        assert_eq!(next(&c).kind, Kind::Reconnecting);
        assert_eq!(next(&c).kind, Kind::Reconnected);
        observe_resubscribe(&c, "pending/filter");
        c.disconnect(3).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        let restored = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(restored, vec![("pending/filter".into(), 1u8)]);
        server.join().unwrap();
        c.close().unwrap();
    });
}

#[test]
fn unacknowledged_unsubscribe_is_not_restored_when_session_is_lost() {
    runtime_guard(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (seen_tx, seen_rx) = mpsc::channel();
        let (drop_tx, drop_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let mut first = accept(&listener);
            packet(&mut first);
            write_connack(&mut first, false, 0);
            assert_eq!(
                serve_until_subscribe(&mut first),
                vec![("removed/filter".into(), 1)]
            );
            let unsub = packet(&mut first);
            assert_eq!(unsub[0] & 0xf0, 0xa0);
            // Deliberately withhold UNSUBACK. Admission, not the ACK or the
            // application's waiting, controls the in-process expectation set.
            seen_tx.send(()).unwrap();
            drop_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            drop(first);
            let mut second = accept(&listener);
            packet(&mut second);
            write_connack(&mut second, false, 0);
            let extras = subscribes_until_disconnect(&mut second);
            done_tx.send(extras).unwrap();
        });
        let mut c = Client::new().unwrap();
        c.connect(1, opts("unacked-unsubscribe", port, false))
            .unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        c.subscribe(2, "removed/filter".into(), 1).unwrap();
        assert_eq!(next(&c).kind, Kind::Subscribed);
        c.unsubscribe(3, "removed/filter".into()).unwrap();
        seen_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        drop_tx.send(()).unwrap();
        let interrupted = next(&c);
        assert_eq!(interrupted.kind, Kind::Unsubscribed);
        assert_eq!(interrupted.op, 3);
        assert_eq!(interrupted.code, mqtt_api::Error::Network as i32);
        assert_eq!(next(&c).kind, Kind::Disconnected);
        assert_eq!(next(&c).kind, Kind::Reconnecting);
        let reconnected = next(&c);
        assert_eq!(reconnected.kind, Kind::Reconnected);
        assert!(!reconnected.session_present);
        assert_eq!(next(&c).kind, Kind::SubscriptionsResubscribed);
        c.disconnect(4).unwrap();
        assert_eq!(next(&c).kind, Kind::Disconnected);
        assert!(done_rx
            .recv_timeout(Duration::from_secs(3))
            .unwrap()
            .is_empty());
        server.join().unwrap();
        c.close().unwrap();
    });
}
