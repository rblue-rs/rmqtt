use mqtt_c::*;
#[test]
fn additive_drain_status_validates_outputs_and_survives_close() {
    assert_eq!(mqtt_runtime_init(), 0);
    unsafe {
        let mut h = 0;
        assert_eq!(
            mqtt_client_new(
                b"127.0.0.1".as_ptr(),
                9,
                b"drain-abi".as_ptr(),
                9,
                1883,
                &mut h
            ),
            0
        );
        let mut status = 99;
        assert_eq!(mqtt_client_drain_status(h, std::ptr::null_mut()), 1);
        assert_eq!(mqtt_client_drain_status(h, &mut status), 0);
        assert_eq!(status, 0);
        assert_eq!(mqtt_client_drain_begin(h, 0), 1);
        assert_eq!(mqtt_client_drain_begin(h, 300001), 1);
        assert_eq!(mqtt_client_drain_begin(h, 1000), 0);
        assert_eq!(mqtt_client_drain_status(h, &mut status), 0);
        assert_eq!(status, 2);
        assert_eq!(mqtt_client_disconnect(h, 1), 3);
        assert_eq!(mqtt_client_close(h), 0);
        assert_eq!(mqtt_client_drain_status(h, &mut status), 0);
        assert_eq!(status, 2); // force-close must not rewrite committed result
        assert_eq!(mqtt_client_drain_begin(h, 1000), 0);
        assert_eq!(mqtt_client_free(h), 0);
        assert_eq!(mqtt_client_drain_status(h, &mut status), 3);
        assert_eq!(status, 2); // errors do not write output
    }
    // Concurrent ABI calls share the existing handle-table linearization.
    // Each thread owns its output; never race caller buffers.
    for _ in 0..64 {
        let mut h = 0;
        unsafe {
            assert_eq!(
                mqtt_client_new(b"127.0.0.1".as_ptr(), 9, b"race".as_ptr(), 4, 1883, &mut h),
                0
            );
        }
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let b = barrier.clone();
        let start = std::thread::spawn(move || {
            b.wait();
            mqtt_client_drain_begin(h, 1000)
        });
        let b = barrier.clone();
        let force = std::thread::spawn(move || {
            b.wait();
            mqtt_client_close(h)
        });
        barrier.wait();
        let begin = start.join().unwrap();
        assert_eq!(force.join().unwrap(), 0);
        let mut status = 99;
        unsafe {
            assert_eq!(mqtt_client_drain_status(h, &mut status), 0);
            assert_eq!((begin, status), if begin == 0 { (0, 2) } else { (3, 0) });
            let mut event = MqttEvent::default();
            assert_eq!(
                mqtt_client_poll(
                    h,
                    &mut event,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null_mut(),
                    0
                ),
                3
            );
        }
        assert_eq!(mqtt_client_free(h), 0);
    }
    assert_eq!(mqtt_runtime_shutdown(), 0);
}
