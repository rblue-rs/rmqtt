use mqtt_c::*;
use std::ptr::null;
#[test]
fn will_u16_validation_precedes_handle_lookup_and_copy() {
    let payload = vec![0; 65536];
    for protocol in [4, 5] {
        for (len, expected) in [(65535, 3), (65536, 1)] {
            // Invalid handle deliberately proves input guard precedes ownership/network.
            let result = unsafe {
                mqtt_client_connect_ex(
                    0,
                    1,
                    protocol,
                    5,
                    0,
                    null(),
                    0,
                    null(),
                    0,
                    null(),
                    0,
                    0,
                    b"dummy".as_ptr(),
                    5,
                    payload.as_ptr(),
                    len,
                    0,
                    0,
                    null(),
                    0,
                    null(),
                    0,
                    1,
                    null(),
                )
            };
            assert_eq!(result, expected, "protocol={protocol} len={len}");
        }
    }
}
