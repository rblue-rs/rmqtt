//! close invalidates operations immediately, including poll, before free.
use mqtt_c::*;
use std::ptr::null_mut;

#[test]
fn closed_handle_poll_must_return_closed_before_free() {
    let actual = unsafe {
        assert_eq!(mqtt_runtime_init(), 0);
        let mut h = 0;
        assert_eq!(
            mqtt_client_new(
                b"localhost".as_ptr(),
                9,
                b"closed".as_ptr(),
                6,
                1883,
                &mut h
            ),
            0
        );
        assert_eq!(mqtt_client_close(h), 0);
        let mut event = MqttEvent::default();
        let code = mqtt_client_poll(h, &mut event, null_mut(), 0, null_mut(), 0);
        // Clean up before asserting so the red test cannot leak runtime state.
        assert_eq!(mqtt_client_free(h), 0);
        assert_eq!(mqtt_runtime_shutdown(), 0);
        code
    };
    assert_eq!(actual, 3, "close must return Closed, not Empty");
}

#[test]
fn closed_handle_must_not_deliver_a_retained_message() {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::mpsc,
        time::{Duration, Instant},
    };
    assert_eq!(mqtt_runtime_init(), 0);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (go, gate) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut b = [0];
        s.read_exact(&mut b).unwrap();
        assert_eq!(b[0], 0x10);
        let mut length = 0;
        for shift in [0, 7, 14, 21] {
            s.read_exact(&mut b).unwrap();
            length += ((b[0] & 127) as usize) << shift;
            if b[0] & 128 == 0 {
                break;
            }
        }
        s.read_exact(&mut vec![0; length]).unwrap();
        s.write_all(&[0x20, 2, 0, 0, 0x30, 4, 0, 1, b'x', b'!'])
            .unwrap();
        gate.recv_timeout(Duration::from_secs(5)).unwrap();
    });
    let actual = unsafe {
        let mut h = 0;
        assert_eq!(
            mqtt_client_new(
                b"127.0.0.1".as_ptr(),
                9,
                b"queued".as_ptr(),
                6,
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
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                0,
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                0,
                0,
                std::ptr::null(),
                0,
                std::ptr::null(),
                0
            ),
            0
        );
        let mut event = MqttEvent::default();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let code = mqtt_client_poll(h, &mut event, null_mut(), 0, null_mut(), 0);
            if code == 9 {
                break;
            }
            assert!(code == 0 || code == 10);
            assert!(Instant::now() < deadline, "message never queued");
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(event.kind, 6);
        assert_eq!(mqtt_client_close(h), 0);
        let mut topic = [0];
        let mut payload = [0];
        let code = mqtt_client_poll(
            h,
            &mut event,
            topic.as_mut_ptr(),
            1,
            payload.as_mut_ptr(),
            1,
        );
        let delivered = (event.kind, topic, payload);
        assert_eq!(mqtt_client_free(h), 0);
        (code, delivered)
    };
    go.send(()).unwrap();
    server.join().unwrap();
    assert_eq!(mqtt_runtime_shutdown(), 0);
    assert_eq!(actual.0, 3, "delivered after close: {actual:?}");
}
