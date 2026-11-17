use mqtt_api::{Error, Options};
use std::time::Duration;
#[test]
fn locked_upstream_v5_setter_rejects_zero_but_our_validation_does_not_panic() {
    let mut upstream = rumqttc::v5::MqttOptions::new("probe", "127.0.0.1", 1883);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        upstream.set_keep_alive(Duration::ZERO);
    }))
    .is_err());
    let mut options = Options::new("127.0.0.1", "probe", 1883);
    options.protocol = 5;
    assert_eq!(options.keep_alive, Duration::ZERO);
    assert_eq!(options.validate(), Err(Error::Unsupported));
}
