mod common;
use common::{next, packet};
use mqtt_api::{Error, Options};
use mqtt_core::{Client, DrainStatus, Kind};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
    time::{Duration, Instant},
};

fn wait(c: &Client) -> DrainStatus {
    let end = Instant::now() + Duration::from_secs(3);
    loop {
        let status = c.drain_status().unwrap();
        if status != DrainStatus::Pending {
            return status;
        }
        assert!(Instant::now() < end);
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn drain_ack_cutoff_deadline_force_and_loss() {
    mqtt_runtime::init().unwrap();
    for outcome in [
        DrainStatus::Drained,
        DrainStatus::TimedOut,
        DrainStatus::Forced,
        DrainStatus::Failed,
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (sent, received) = mpsc::channel();
        let (go, gate) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            assert_eq!(packet(&mut s)[0], 0x10);
            s.write_all(&[0x20, 2, 0, 0]).unwrap();
            let p = packet(&mut s);
            sent.send(()).unwrap();
            gate.recv_timeout(Duration::from_secs(3)).unwrap();
            if outcome == DrainStatus::Drained {
                let n = ((p[1] as usize) << 8) | p[2] as usize;
                s.write_all(&[0x40, 2, p[n + 3], p[n + 4]]).unwrap();
                // Second accepted QoS0 must be sent before disconnect.
                assert_eq!(packet(&mut s)[0], 0x30);
                assert_eq!(packet(&mut s), vec![0xe0]);
            } else if outcome != DrainStatus::Failed {
                // Deadline and force-close must actually close the transport.
                assert_eq!(s.read(&mut [0; 1]).unwrap(), 0);
            }
        });
        let mut c = Client::new().unwrap();
        c.connect(1, Options::new("127.0.0.1", "drain", port))
            .unwrap();
        assert_eq!(next(&c).kind, Kind::Connected);
        c.publish(2, "x".into(), vec![1], 1, false).unwrap();
        received.recv_timeout(Duration::from_secs(3)).unwrap();
        c.publish(3, "x".into(), vec![2], 0, false).unwrap();
        if outcome == DrainStatus::TimedOut {
            // Out-of-band drain must be accepted even with all 32 slots full.
            for op in 4..35 {
                c.publish(op, "x".into(), vec![2], 0, false).unwrap();
            }
            assert_eq!(
                c.publish(35, "x".into(), vec![], 0, false),
                Err(Error::QueueFull)
            );
        }
        c.begin_drain(if outcome == DrainStatus::Drained {
            Duration::from_secs(3)
        } else {
            Duration::from_millis(100)
        })
        .unwrap();
        c.begin_drain(Duration::from_secs(300)).unwrap(); // first deadline wins
        assert_eq!(
            c.publish(4, "x".into(), vec![], 0, false),
            Err(Error::Closed)
        );
        assert_eq!(c.disconnect(5), Err(Error::Closed));
        assert_eq!(c.subscribe(6, "x".into(), 0), Err(Error::Closed));
        assert_eq!(c.unsubscribe(7, "x".into()), Err(Error::Closed));
        if outcome == DrainStatus::Forced {
            c.close().unwrap();
        }
        go.send(()).unwrap();
        assert_eq!(wait(&c), outcome);
        if outcome == DrainStatus::Drained {
            let mut kinds = vec![];
            while let Ok(e) = c.pop() {
                kinds.push((e.kind, e.op));
            }
            assert!(kinds.contains(&(Kind::Acknowledged, 2)));
            assert!(kinds.contains(&(Kind::Sent, 3)));
        }
        c.begin_drain(Duration::from_millis(1)).unwrap();
        assert_eq!(c.drain_status().unwrap(), outcome);
        c.close().unwrap();
        assert_eq!(c.peek().unwrap_err(), Error::Closed);
        drop(c);
        server.join().unwrap();
    }
    let mut idle = Client::new().unwrap();
    assert_eq!(
        idle.begin_drain(Duration::ZERO),
        Err(Error::InvalidArgument)
    );
    assert_eq!(
        idle.begin_drain(Duration::from_micros(1500)),
        Err(Error::InvalidArgument)
    );
    idle.begin_drain(Duration::from_secs(1)).unwrap();
    assert_eq!(idle.drain_status().unwrap(), DrainStatus::Drained);
    assert_eq!(
        idle.connect(1, Options::new("127.0.0.1", "x", 1883)),
        Err(Error::Closed)
    );
    idle.close().unwrap();
    drop(idle);
    mqtt_runtime::shutdown().unwrap();
}
