use mqtt_c::*;
#[test]
fn safe_numeric_handles_and_lifecycle() {
    unsafe {
        let mut h = 0;
        assert_eq!(
            mqtt_client_new(b"localhost".as_ptr(), 9, b"ffi".as_ptr(), 3, 1883, &mut h),
            2
        );
        assert_eq!(mqtt_runtime_init(), 0);
        assert_eq!(
            mqtt_client_new(std::ptr::null(), 9, b"ffi".as_ptr(), 3, 1883, &mut h),
            1
        );
        assert_eq!(
            mqtt_client_new(b"localhost".as_ptr(), 9, b"ffi".as_ptr(), 3, 1883, &mut h),
            0
        );
        assert_ne!(h, 0);
        assert_eq!(mqtt_runtime_shutdown(), 4);
        assert_eq!(mqtt_client_free(h), 4);
        assert_eq!(mqtt_client_close(h), 0);
        assert_eq!(mqtt_client_close(h), 0);
        assert_eq!(mqtt_client_free(h), 0);
        assert_eq!(mqtt_client_free(h), 0);
        assert_eq!(mqtt_runtime_shutdown(), 0);
        assert_eq!(mqtt_client_close(h), 3);
    }
}
