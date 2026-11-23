use mqtt_api::{Error, Options};
#[test]
fn websocket_defaults_uri_and_validation() {
    let mut o = Options::new("example.com", "ws", 443);
    assert!(!o.use_websocket);
    assert_eq!(o.websocket_path, "/mqtt");
    assert_eq!(o.validate(), Ok(()));
    o.use_websocket = true;
    assert_eq!(o.websocket_uri().unwrap(), "ws://example.com:443/mqtt");
    o.use_tls = true;
    o.websocket_path = "/a%20b/mqtt".into();
    assert_eq!(
        o.websocket_uri().unwrap(),
        "wss://example.com:443/a%20b/mqtt"
    );
    o.server = "user:secret@example.com".into();
    assert!(
        !format!("{o:?}").contains("secret"),
        "invalid WS authority must not leak through Debug"
    );
    for host in [
        "ws://example.com",
        "user:secret@example.com",
        "example.com:443",
        "a/b",
        "a?b",
        "::1",
        "[::1]",
        "bad host",
        "a..b",
    ] {
        o.server = host.into();
        assert_eq!(o.validate(), Err(Error::InvalidArgument), "host {host}");
    }
    o.server = "127.0.0.1".into();
    for path in [
        "mqtt",
        "//elsewhere/x",
        "/x?secret=1",
        "/x#f",
        "/a b",
        "/a\\b",
        "/x\r\nOops:1",
        "/bad%",
        "/bad%gg",
    ] {
        o.websocket_path = path.into();
        assert_eq!(o.validate(), Err(Error::InvalidArgument), "path {path:?}");
    }
    o.websocket_path = "/mqtt".into();
    o.protocol = 5;
    assert_eq!(o.validate(), Err(Error::Unsupported));
    o.keep_alive = std::time::Duration::from_secs(5);
    assert_eq!(o.validate(), Ok(()));
    o.websocket_path = "/secret-path".into();
    assert!(!format!("{o:?}").contains("secret-path"));
}
