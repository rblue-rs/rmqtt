mod common;
use common::{next, TestBroker};
use mqtt_api::{Error, Options, Will};
use mqtt_core::{Client, Kind};

fn with_runtime<T>(f: impl FnOnce() -> T) -> T {
    mqtt_runtime::init().unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    let _ = mqtt_runtime::shutdown();
    match result {
        Ok(v) => v,
        Err(p) => std::panic::resume_unwind(p),
    }
}

fn connect(client: &mut Client, id: &str, port: u16, will: Option<Will>) {
    let mut options = Options::new("127.0.0.1", id, port);
    options.will = will;
    client.connect(1, options).unwrap();
    let event = next(client);
    assert_eq!(event.kind, Kind::Connected);
}

fn wait_message(client: &Client, topic: &str, payload: &[u8], qos: u8) {
    let event = next(client);
    assert_eq!(event.kind, Kind::Message);
    assert_eq!(event.topic, topic.as_bytes());
    assert_eq!(event.payload, payload);
    assert_eq!(event.qos, qos);
}

#[test]
fn will_qos0_is_delivered_after_abnormal_disconnect() {
    with_runtime(|| {
        let broker = TestBroker::start(None, None);
        let mut subscriber = Client::new().unwrap();
        connect(&mut subscriber, "will-sub-0", broker.plain_port, None);
        subscriber.subscribe(2, "test/will/qos0".into(), 0).unwrap();
        assert_eq!(next(&subscriber).kind, Kind::Subscribed);

        let mut owner = Client::new().unwrap();
        connect(
            &mut owner,
            "will-owner-0",
            broker.plain_port,
            Some(Will {
                topic: "test/will/qos0".into(),
                payload: b"gone-0".to_vec(),
                qos: 0,
                retain: false,
            }),
        );
        owner.close().unwrap();
        wait_message(&subscriber, "test/will/qos0", b"gone-0", 0);
    });
}

#[test]
fn will_qos1_is_delivered_after_abnormal_disconnect() {
    with_runtime(|| {
        let broker = TestBroker::start(None, None);
        let mut subscriber = Client::new().unwrap();
        connect(&mut subscriber, "will-sub-1", broker.plain_port, None);
        subscriber.subscribe(2, "test/will/qos1".into(), 1).unwrap();
        assert_eq!(next(&subscriber).kind, Kind::Subscribed);

        let mut owner = Client::new().unwrap();
        connect(
            &mut owner,
            "will-owner-1",
            broker.plain_port,
            Some(Will {
                topic: "test/will/qos1".into(),
                payload: b"gone-1".to_vec(),
                qos: 1,
                retain: false,
            }),
        );
        owner.close().unwrap();
        wait_message(&subscriber, "test/will/qos1", b"gone-1", 1);
    });
}

#[test]
fn retained_will_is_delivered_to_a_later_subscriber() {
    with_runtime(|| {
        let broker = TestBroker::start(None, None);
        let mut owner = Client::new().unwrap();
        connect(
            &mut owner,
            "will-owner-retain",
            broker.plain_port,
            Some(Will {
                topic: "test/will/retain".into(),
                payload: b"still-here".to_vec(),
                qos: 1,
                retain: true,
            }),
        );
        owner.close().unwrap();
        // Allow the broker to store the retained will before the next subscribe.
        std::thread::sleep(std::time::Duration::from_millis(100));

        let mut subscriber = Client::new().unwrap();
        connect(&mut subscriber, "will-sub-retain", broker.plain_port, None);
        subscriber
            .subscribe(2, "test/will/retain".into(), 1)
            .unwrap();
        assert_eq!(next(&subscriber).kind, Kind::Subscribed);
        wait_message(&subscriber, "test/will/retain", b"still-here", 1);
    });
}

#[test]
fn invalid_will_is_rejected_before_connect() {
    with_runtime(|| {
        let broker = TestBroker::start(None, None);
        let mut client = Client::new().unwrap();
        let mut empty = Options::new("127.0.0.1", "will-empty", broker.plain_port);
        empty.will = Some(Will {
            topic: String::new(),
            payload: b"x".to_vec(),
            qos: 0,
            retain: false,
        });
        assert_eq!(client.connect(1, empty), Err(Error::InvalidArgument));

        let mut qos2 = Options::new("127.0.0.1", "will-qos2", broker.plain_port);
        qos2.will = Some(Will {
            topic: "test/will/qos2".into(),
            payload: b"x".to_vec(),
            qos: 2,
            retain: false,
        });
        assert_eq!(client.connect(2, qos2), Err(Error::Unsupported));
        assert_eq!(client.pop().unwrap_err(), Error::Empty);
    });
}
