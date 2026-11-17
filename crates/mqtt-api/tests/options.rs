use mqtt_api::{Error, Options};
use std::time::Duration;
#[test]
fn default_keepalive_zero_and_supported_v311() {
    let o = Options::new("localhost", "test", 1883);
    assert_eq!(o.keep_alive, Duration::ZERO);
    assert_eq!(o.validate(), Ok(()));
}
#[test]
fn v5_zero_is_typed_unsupported_not_panic() {
    let mut o = Options::new("localhost", "test", 1883);
    o.protocol = 5;
    assert_eq!(o.validate(), Err(Error::Unsupported));
}
#[test]
fn reject_invalid_inputs_before_engine() {
    for (server, id, port) in [("", "x", 1883), ("h", "", 1883), ("h", "x", 0)] {
        assert_eq!(
            Options::new(server, id, port).validate(),
            Err(Error::InvalidArgument)
        );
    }
    let mut o = Options::new("h", "x", 1883);
    o.keep_alive = Duration::from_millis(500);
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
    o.keep_alive = Duration::from_secs(65536);
    assert_eq!(o.validate(), Err(Error::InvalidArgument));
}
