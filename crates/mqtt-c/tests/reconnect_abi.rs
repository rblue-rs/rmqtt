use mqtt_c::*;
use std::{
    io::{Read, Write},
    net::TcpListener,
    ptr::{null, null_mut},
    thread,
    time::{Duration, Instant},
};

fn packet(s: &mut impl Read) -> Vec<u8> {
    let mut b = [0];
    s.read_exact(&mut b).unwrap();
    let h = b[0];
    let mut len = 0;
    let mut shift = 0;
    loop {
        s.read_exact(&mut b).unwrap();
        len += (b[0] as usize & 127) << shift;
        if b[0] & 128 == 0 {
            break;
        }
        shift += 7;
    }
    let mut v = vec![0; len];
    s.read_exact(&mut v).unwrap();
    let mut out = vec![h];
    out.extend(v);
    out
}

unsafe fn poll_kind(h: u64, want: u32) -> MqttEvent {
    let end = Instant::now() + Duration::from_secs(5);
    let mut event = MqttEvent::default();
    loop {
        let code = mqtt_client_poll(h, &mut event, null_mut(), 0, null_mut(), 0);
        if code == 0 && event.kind == want {
            return event;
        }
        if code != 0 && code != 10 {
            panic!("poll {code}");
        }
        assert!(Instant::now() < end, "timeout waiting for kind {want}");
        thread::sleep(Duration::from_millis(5));
    }
}

fn no_kind(h: u64, forbidden: u32, window: Duration) {
    let end = Instant::now() + window;
    let mut event = MqttEvent::default();
    while Instant::now() < end {
        unsafe {
            let code = mqtt_client_poll(h, &mut event, null_mut(), 0, null_mut(), 0);
            if code == 0 {
                assert_ne!(event.kind, forbidden, "unexpected kind {forbidden}");
            } else {
                assert_eq!(code, 10);
            }
        }
        thread::sleep(Duration::from_millis(5));
    }
}

fn empty_connect(h: u64, op: u64, clean: u8, policy: Option<MqttReconnectPolicy>) -> i32 {
    unsafe {
        mqtt_client_connect_ex(
            h,
            op,
            4,
            0,
            0,
            null(),
            0,
            null(),
            0,
            null(),
            0,
            0,
            null(),
            0,
            null(),
            0,
            0,
            0,
            null(),
            0,
            null(),
            0,
            clean,
            policy.as_ref().map(|p| p as *const _).unwrap_or(null()),
        )
    }
}

#[test]
fn connect_ex_validates_and_defaults_do_not_reconnect() {
    assert_eq!(mqtt_runtime_init(), 0);
    unsafe {
        let mut h = 0;
        assert_eq!(
            mqtt_client_new(
                b"127.0.0.1".as_ptr(),
                9,
                b"abi-def".as_ptr(),
                7,
                1883,
                &mut h
            ),
            0
        );
        let mut bad = MqttReconnectPolicy {
            enabled: 2,
            max_attempts: 1,
            base_delay_ms: 20,
            max_delay_ms: 20,
        };
        assert_eq!(empty_connect(h, 1, 2, None), 1);
        assert_eq!(empty_connect(h, 1, 1, Some(bad)), 1);
        bad.enabled = 1;
        bad.max_attempts = 0;
        assert_eq!(empty_connect(h, 1, 1, Some(bad)), 1);
        assert_eq!(mqtt_client_close(h), 0);
        assert_eq!(mqtt_client_free(h), 0);
    }

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        let p = packet(&mut s);
        assert_eq!(p[0], 0x10);
        assert_ne!(p[8] & 0x02, 0, "legacy connect stays clean_session=true");
        s.write_all(&[0x20, 2, 0, 0]).unwrap();
        drop(s);
        listener.set_nonblocking(true).unwrap();
        let until = Instant::now() + Duration::from_millis(200);
        while Instant::now() < until {
            assert!(listener.accept().is_err());
            thread::sleep(Duration::from_millis(5));
        }
    });
    unsafe {
        let mut h = 0;
        let id = b"abi-legacy";
        assert_eq!(
            mqtt_client_new(
                b"127.0.0.1".as_ptr(),
                9,
                id.as_ptr(),
                id.len(),
                port,
                &mut h
            ),
            0
        );
        assert_eq!(
            mqtt_client_connect(
                h,
                1,
                4,
                0,
                0,
                null(),
                0,
                null(),
                0,
                null(),
                0,
                0,
                null(),
                0,
                null(),
                0,
                0,
                0,
                null(),
                0,
                null(),
                0
            ),
            0
        );
        assert_eq!(poll_kind(h, 1).session_present, 0);
        assert_eq!(poll_kind(h, 8).kind, 8);
        no_kind(h, 10, Duration::from_millis(150));
        assert_eq!(mqtt_client_close(h), 0);
        assert_eq!(mqtt_client_free(h), 0);
    }
    server.join().unwrap();
    assert_eq!(mqtt_runtime_shutdown(), 0);
}

#[test]
fn connect_ex_clean_session_and_reconnect_events() {
    assert_eq!(mqtt_runtime_init(), 0);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        first
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let connect = packet(&mut first);
        assert_eq!(connect[8] & 0x02, 0, "clean_session=0 must be on the wire");
        first.write_all(&[0x20, 2, 0, 0]).unwrap();
        drop(first);
        let (mut second, _) = listener.accept().unwrap();
        second
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        packet(&mut second);
        second.write_all(&[0x20, 2, 1, 0]).unwrap();
        thread::sleep(Duration::from_millis(50));
    });
    unsafe {
        let mut h = 0;
        let id = b"abi-ex";
        assert_eq!(
            mqtt_client_new(
                b"127.0.0.1".as_ptr(),
                9,
                id.as_ptr(),
                id.len(),
                port,
                &mut h
            ),
            0
        );
        let policy = MqttReconnectPolicy {
            enabled: 1,
            max_attempts: 3,
            base_delay_ms: 20,
            max_delay_ms: 20,
        };
        assert_eq!(empty_connect(h, 1, 0, Some(policy)), 0);
        assert_eq!(poll_kind(h, 1).kind, 1);
        assert_eq!(poll_kind(h, 7).kind, 7);
        assert_eq!(poll_kind(h, 10).kind, 10);
        let reconnected = poll_kind(h, 11);
        assert_eq!(reconnected.kind, 11);
        assert_eq!(reconnected.session_present, 1);
        assert_eq!(poll_kind(h, 12).kind, 12);
        assert_eq!(mqtt_client_close(h), 0);
        assert_eq!(mqtt_client_free(h), 0);
    }
    server.join().unwrap();
    assert_eq!(mqtt_runtime_shutdown(), 0);
}
