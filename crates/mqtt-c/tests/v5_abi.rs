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

fn connect5(h: u64, keepalive: u16) -> i32 {
    unsafe {
        mqtt_client_connect_ex(
            h,
            1,
            5,
            keepalive,
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
            1,
            null(),
        )
    }
}

#[test]
fn v5_c_abi_zero_rejected_and_explicit_five_exchanges() {
    assert_eq!(mqtt_runtime_init(), 0);
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        let c = packet(&mut s);
        assert_eq!(c[7], 5);
        assert_eq!(&c[9..11], &[0, 5]);
        s.write_all(&[0x20, 3, 0, 0, 0]).unwrap();
        let sub = packet(&mut s);
        assert_eq!(sub[0], 0x82);
        s.write_all(&[0x90, 4, sub[1], sub[2], 0, 1]).unwrap();
        let p = packet(&mut s);
        let n = u16::from_be_bytes([p[1], p[2]]) as usize;
        s.write_all(&[0x40, 2, p[3 + n], p[4 + n]]).unwrap();
        let u = packet(&mut s);
        assert_eq!(u[0], 0xa2);
        s.write_all(&[0xb0, 4, u[1], u[2], 0, 0]).unwrap();
        assert_eq!(packet(&mut s)[0], 0xe0);
    });
    unsafe {
        let mut h = 0;
        assert_eq!(
            mqtt_client_new(b"127.0.0.1".as_ptr(), 9, b"v5-c".as_ptr(), 4, port, &mut h),
            0
        );
        assert_eq!(connect5(h, 0), 6);
        assert_eq!(connect5(h, 4), 6);
        assert_eq!(connect5(h, 5), 0);
        assert_eq!(poll_kind(h, 1).code, 0);
        assert_eq!(mqtt_client_subscribe(h, 2, b"t".as_ptr(), 1, 1), 0);
        assert_eq!(poll_kind(h, 2).code, 0);
        assert_eq!(
            mqtt_client_publish(h, 3, b"t".as_ptr(), 1, b"a".as_ptr(), 1, 1, 0),
            0
        );
        assert_eq!(poll_kind(h, 5).operation_id, 3);
        assert_eq!(mqtt_client_unsubscribe(h, 4, b"t".as_ptr(), 1), 0);
        assert_eq!(poll_kind(h, 9).code, 0);
        assert_eq!(mqtt_client_disconnect(h, 5), 0);
        assert_eq!(poll_kind(h, 7).code, 0);
        assert_eq!(mqtt_client_close(h), 0);
        assert_eq!(mqtt_client_free(h), 0);
    }
    server.join().unwrap();
    assert_eq!(mqtt_runtime_shutdown(), 0);
}
