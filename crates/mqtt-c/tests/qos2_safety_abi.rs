use mqtt_c::*;
use std::ptr::null_mut;
#[test]
fn invalid_qos_is_typed_unsupported_without_creating_an_operation() {
    assert_eq!(mqtt_runtime_init(), 0);
    unsafe {
        let mut h = 0;
        assert_eq!(
            mqtt_client_new(b"127.0.0.1".as_ptr(), 9, b"q2-c".as_ptr(), 4, 1883, &mut h),
            0
        );
        for op in 1..=128 {
            assert_eq!(mqtt_client_subscribe(h, op, b"q2".as_ptr(), 2, 3), 6);
            assert_eq!(
                mqtt_client_publish(h, op, b"q2".as_ptr(), 2, [0, 255, 128].as_ptr(), 3, 3, 0),
                6
            );
            let mut event = std::mem::MaybeUninit::uninit();
            assert_eq!(
                mqtt_client_poll(h, event.as_mut_ptr(), null_mut(), 0, null_mut(), 0),
                10
            );
        }
        assert_eq!(mqtt_client_close(h), 0);
        assert_eq!(mqtt_client_free(h), 0);
    }
    assert_eq!(mqtt_runtime_shutdown(), 0);
}
