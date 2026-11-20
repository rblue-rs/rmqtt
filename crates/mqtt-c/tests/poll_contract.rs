use mqtt_c::*;
use std::ptr::{null, null_mut};

#[test]
fn empty_invalid_inputs_and_concurrent_handle_release() {
    unsafe {
        assert_eq!(mqtt_runtime_init(), 0);
        let mut h = 0;
        assert_eq!(
            mqtt_client_new(b"localhost".as_ptr(), 9, b"audit".as_ptr(), 5, 1883, &mut h),
            0
        );
        let mut event = MqttEvent {
            kind: 0xaabb,
            operation_id: 77,
            ..Default::default()
        };
        let mut topic = [0xaa; 8];
        let mut payload = [0xbb; 8];
        assert_eq!(
            mqtt_client_poll(
                h,
                &mut event,
                topic.as_mut_ptr(),
                8,
                payload.as_mut_ptr(),
                8
            ),
            10
        );
        assert_eq!(event.kind, 0xaabb);
        assert_eq!(event.operation_id, 77);
        assert_eq!(topic, [0xaa; 8]);
        assert_eq!(payload, [0xbb; 8]);
        assert_eq!(
            mqtt_client_poll(h, null_mut(), null_mut(), 0, null_mut(), 0),
            1
        );
        assert_eq!(
            mqtt_client_poll(h, &mut event, null_mut(), 1, null_mut(), 0),
            1
        );
        assert_eq!(
            mqtt_client_poll(h, &mut event, null_mut(), 0, null_mut(), 1),
            1
        );
        assert_eq!(
            mqtt_client_new(null(), 1, b"x".as_ptr(), 1, 1883, &mut h),
            1
        );
        assert_eq!(
            mqtt_client_new(b"x".as_ptr(), 65536, b"x".as_ptr(), 1, 1883, &mut h),
            1
        );
        assert_eq!(
            mqtt_client_publish(h, 1, b"x".as_ptr(), 1, null(), 1, 0, 0),
            1
        );
        assert_eq!(
            mqtt_client_publish(h, 1, b"x".as_ptr(), 1, b"x".as_ptr(), 65537, 0, 0),
            1
        );
        assert_eq!(mqtt_client_subscribe(h, 1, [0xff].as_ptr(), 1, 1), 1);
        assert_eq!(
            mqtt_client_connect(
                h,
                1,
                5,
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
            6
        );
        assert_eq!(mqtt_client_free(h), 4);
        let workers: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(move || {
                    let code = mqtt_client_close(h);
                    assert!(code == 0 || code == 3);
                    assert_eq!(mqtt_client_free(h), 0);
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(
            mqtt_client_connect(
                h,
                2,
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
            3
        );
        assert_eq!(mqtt_client_free(h), 0);
        assert_eq!(mqtt_runtime_shutdown(), 0);
    }
}
