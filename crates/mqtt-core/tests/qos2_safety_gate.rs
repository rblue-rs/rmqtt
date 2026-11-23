//! Invalid-QoS/unsupported-Will admission guard. Real QoS2 is tested by public wire peers.
use mqtt_api::{Error, Options, Will};
use mqtt_core::Client;
use std::{io::ErrorKind, net::TcpListener, sync::Mutex, time::Duration};

static LOCK: Mutex<()> = Mutex::new(());

#[test]
fn invalid_qos_never_enters_operation_queues() {
    let _guard = LOCK.lock().unwrap();
    mqtt_runtime::init().unwrap();
    let mut c = Client::new().unwrap();
    for op in 1..=128 {
        assert_eq!(c.subscribe(op, "q2/#".into(), 3), Err(Error::Unsupported));
        assert_eq!(
            c.publish(op, "q2/binary".into(), vec![0, 255, 128], 3, false),
            Err(Error::Unsupported)
        );
        assert!(
            matches!(c.pop(), Err(Error::Empty)),
            "rejected work must not enqueue completions or consume capacity"
        );
    }
    c.close().unwrap();
    drop(c); // The public Rust handle retains its runtime guard until Drop.
    mqtt_runtime::shutdown().unwrap();
}

#[test]
fn qos2_will_rejected_before_network_for_all_transport_protocol_retry_modes() {
    let _guard = LOCK.lock().unwrap();
    mqtt_runtime::init().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    for protocol in [4, 5] {
        for ws in [false, true] {
            for tls in [false, true] {
                for reconnect in [false, true] {
                    let mut o = Options::new("127.0.0.1", "q2-rejected", port);
                    o.protocol = protocol;
                    o.use_tls = tls;
                    o.use_websocket = ws;
                    o.reconnect.enabled = reconnect;
                    if protocol == 5 {
                        o.keep_alive = Duration::from_secs(5);
                    }
                    o.will = Some(Will {
                        topic: "q2/will".into(),
                        payload: vec![0, 255, 128],
                        qos: 2,
                        retain: false,
                    });
                    let mut c = Client::new().unwrap();
                    assert_eq!(c.connect(1, o), Err(Error::Unsupported));
                    assert!(matches!(c.pop(), Err(Error::Empty)));
                    c.close().unwrap();
                }
            }
        }
    }
    assert!(
        matches!(listener.accept(),Err(e) if e.kind()==ErrorKind::WouldBlock),
        "unsupported Will must never attempt TCP/TLS/WS/WSS"
    );
    mqtt_runtime::shutdown().unwrap();
}
