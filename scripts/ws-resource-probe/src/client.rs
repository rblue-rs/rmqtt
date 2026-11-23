//! Public-client adversarial fixture consumer. No production hooks/changes.
use mqtt_api::{Error, Options};
use mqtt_core::{Client, DrainStatus, Kind};
use std::{
    io::{self, Write},
    process::ExitCode,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};
fn next(c: &Client) -> mqtt_core::Event {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        match c.pop() {
            Ok(e) => return e,
            Err(Error::Empty) => {
                assert!(Instant::now() < end, "event timeout");
                std::thread::yield_now();
            }
            Err(e) => panic!("event error {e:?}"),
        }
    }
}
fn disconnect(c: &Client, op: u64) {
    c.disconnect(op).unwrap();
    let e = next(c);
    assert_eq!(e.kind, Kind::Disconnected);
    assert_eq!(e.op, op);
}
pub fn run() -> ExitCode {
    let a: Vec<_> = std::env::args().collect();
    let offset = a.iter().position(|s| s == "--client").unwrap();
    let a = &a[offset + 1..];
    let port: u16 = a[0].parse().unwrap();
    let protocol: u8 = a[1].parse().unwrap();
    let tls = a[2] == "1";
    let case = &a[4];
    println!("client protocol={protocol} tls={tls} case={case}");
    mqtt_runtime::init().unwrap();
    let mut c = Client::new().unwrap();
    let mut o = Options::new("127.0.0.1", "resource-probe", port);
    o.protocol = protocol;
    o.use_websocket = true;
    o.use_tls = tls;
    o.keep_alive = if protocol == 5 {
        Duration::from_secs(5)
    } else {
        Duration::ZERO
    };
    if tls {
        o.ca_pem = Some(std::fs::read(&a[3]).unwrap());
    }
    c.connect(1, o).unwrap();
    assert_eq!(next(&c).kind, Kind::Connected);
    let baseline = super::LIVE.load(Ordering::SeqCst);
    super::PEAK.store(baseline, Ordering::SeqCst);
    super::MAX_REQUEST.store(0, Ordering::SeqCst);
    super::TRACK.store(true, Ordering::SeqCst);
    println!("CLIENT_READY");
    io::stdout().flush().unwrap();
    let mut terminal = 0;
    if case == "quota-count" || case == "quota-bytes" {
        let count = if case == "quota-count" { 128 } else { 5 };
        let mut op = 2;
        let mut first = String::new();
        for n in 0..count {
            let t = if case == "quota-count" {
                format!("q/{n:03}")
            } else {
                if n < 4 {
                    format!("{n}{}", "é".repeat(32767))
                } else {
                    "edge".into()
                }
            };
            if n == 0 {
                first = t.clone();
            }
            c.subscribe(op, t, 0).unwrap();
            let e = next(&c);
            assert_eq!(e.kind, Kind::Subscribed);
            assert_eq!(e.op, op);
            op += 1;
        }
        c.subscribe(op, first.clone(), 1).unwrap();
        assert_eq!(next(&c).kind, Kind::Subscribed);
        op += 1;
        let forbidden = if case == "quota-count" {
            "forbidden/extra".into()
        } else {
            "z".to_string()
        };
        assert_eq!(
            c.subscribe(op, forbidden, 0),
            Err(Error::QueueFull),
            "retained expectation quota was not enforced"
        );
        // Rejection consumes neither op id nor a command slot. Unsubscribe changes the
        // admitted expectation before UNSUBACK (peer waits on this positive fence).
        c.unsubscribe(op, first).unwrap();
        op += 1;
        c.subscribe(op, "replacement".into(), 0).unwrap();
        println!("REPLACEMENT_ADMITTED");
        io::stdout().flush().unwrap();
        let e = next(&c);
        assert_eq!(e.kind, Kind::Unsubscribed);
        let e = next(&c);
        assert_eq!(e.kind, Kind::Subscribed);
        disconnect(&c, op + 1);
    } else if matches!(case.as_str(), "frame" | "fragment" | "header") {
        let e = next(&c);
        assert_eq!(e.kind, Kind::Failure);
        assert_eq!(e.code, Error::Network as i32);
        terminal = e.code;
        if case != "header" {
            assert!(
                super::MAX_REQUEST.load(Ordering::SeqCst) > 2 * 1024 * 1024,
                "counterexample allocation not observed"
            );
        }
    } else if case == "slow" {
        let mut signal = String::new();
        io::stdin().read_line(&mut signal).unwrap();
        assert_eq!(signal.trim(), "PEER_EOF");
        let mut messages = 0;
        loop {
            let e = next(&c);
            if e.kind == Kind::Failure {
                assert_eq!(e.code, Error::Overflow as i32);
                terminal = e.code;
                break;
            }
            assert_eq!(e.kind, Kind::Message);
            messages += 1;
            assert!(messages <= 128);
        }
        assert_eq!(messages, 128);
    } else if case == "backpressure" {
        let mut full = false;
        for op in 2..2050 {
            match c.publish(op, "q".into(), vec![0; 65536], 0, false) {
                Ok(()) => {}
                Err(Error::QueueFull) => {
                    full = true;
                    break;
                }
                Err(e) => panic!("unexpected admission {e:?}"),
            };
            loop {
                match c.pop() {
                    Ok(e) => assert!(matches!(e.kind, Kind::Queued | Kind::Sent)),
                    Err(Error::Empty) => break,
                    Err(e) => panic!("unexpected {e:?}"),
                }
            }
        }
        assert!(full);
        terminal = Error::QueueFull as i32;
        c.close().unwrap();
        assert_eq!(c.subscribe(99999, "late".into(), 0), Err(Error::Closed));
        println!("FORCE_CLOSED");
        io::stdout().flush().unwrap();
    } else if matches!(case.as_str(), "close" | "drop" | "drain") {
        let end = Instant::now() + Duration::from_secs(3);
        while super::MAX_REQUEST.load(Ordering::SeqCst) < 512 * 1024 {
            assert!(
                Instant::now() < end,
                "no partial reassembly allocation observed"
            );
            std::thread::yield_now();
        }
        if case == "drain" {
            c.begin_drain(Duration::from_secs(2)).unwrap();
            let end = Instant::now() + Duration::from_secs(3);
            while c.drain_status().unwrap() == DrainStatus::Pending {
                assert!(Instant::now() < end);
                std::thread::yield_now();
            }
            assert_eq!(c.drain_status().unwrap(), DrainStatus::Drained);
        } else if case == "close" {
            c.close().unwrap();
            assert_eq!(c.subscribe(99999, "late".into(), 0), Err(Error::Closed));
        }
    } else {
        let e = next(&c);
        assert_eq!(e.kind, Kind::Message);
        assert_eq!(e.payload, vec![0, 255, 1]);
        disconnect(&c, 2);
    }
    super::TRACK.store(false, Ordering::SeqCst);
    println!("public_case_pass protocol={protocol} tls={tls} case={case} terminal={terminal} peak_requested_live_delta={} max_allocation_request={} NOT_an_aggregate_upper_bound",super::PEAK.load(Ordering::SeqCst).saturating_sub(baseline),super::MAX_REQUEST.load(Ordering::SeqCst));
    if case != "drop" {
        c.close().unwrap();
    }
    drop(c);
    mqtt_runtime::shutdown().unwrap();
    ExitCode::SUCCESS
}
