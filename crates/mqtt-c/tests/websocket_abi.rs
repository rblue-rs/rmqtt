use mqtt_c::*;
use std::ptr::null;
#[test]
fn additive_websocket_configuration_validates_and_preserves_close_rules() {
    assert_eq!(mqtt_runtime_init(), 0);
    unsafe {
        let mut h = 0;
        assert_eq!(
            mqtt_client_new(b"127.0.0.1".as_ptr(), 9, b"ws-c".as_ptr(), 4, 80, &mut h),
            0
        );
        assert_eq!(mqtt_client_set_websocket(h, 2, null(), 0), 1);
        assert_eq!(
            mqtt_client_set_websocket(h, 1, b"not-a-path".as_ptr(), 10),
            1
        );
        assert_eq!(mqtt_client_set_websocket(h, 1, b"/mqtt".as_ptr(), 5), 0);
        assert_eq!(mqtt_client_set_websocket(h, 0, null(), 0), 0);
        assert_eq!(mqtt_client_close(h), 0);
        assert_eq!(mqtt_client_set_websocket(h, 1, null(), 0), 3);
        assert_eq!(mqtt_client_free(h), 0);
    }
    assert_eq!(mqtt_runtime_shutdown(), 0);
}
