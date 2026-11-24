//! Independent TCP fake-broker gate for the unmodified upstream engine.
//! Not a public client QoS2 test; never use these probes as support claims.
use rumqttc::{v5, AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::ExitCode,
    thread,
    time::{Duration, Instant},
};
use v5::mqttbytes::{v5 as p5, QoS as Q5};

type TestResult<T> = Result<T, String>;
fn check(ok: bool, why: &str) -> TestResult<()> {
    if ok {
        Ok(())
    } else {
        Err(why.into())
    }
}
fn read(s: &mut TcpStream) -> TestResult<(u8, Vec<u8>)> {
    let mut b = [0];
    s.read_exact(&mut b).map_err(|e| e.to_string())?;
    let head = b[0];
    let mut len = 0usize;
    let mut scale = 1;
    for _ in 0..4 {
        s.read_exact(&mut b).map_err(|e| e.to_string())?;
        len += (b[0] & 127) as usize * scale;
        check(len <= 128, "fixture packet exceeds128 bytes")?;
        if b[0] & 128 == 0 {
            let mut body = vec![0; len];
            s.read_exact(&mut body).map_err(|e| e.to_string())?;
            return Ok((head, body));
        }
        scale *= 128;
    }
    Err("invalid remaining length".into())
}
fn send(s: &mut TcpStream, bytes: &[u8]) -> TestResult<()> {
    s.write_all(bytes).map_err(|e| e.to_string())
}
fn accept(l: &TcpListener) -> TestResult<TcpStream> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match l.accept() {
            Ok((s, _)) => {
                s.set_read_timeout(Some(Duration::from_millis(600)))
                    .map_err(|e| e.to_string())?;
                s.set_write_timeout(Some(Duration::from_millis(600)))
                    .map_err(|e| e.to_string())?;
                return Ok(s);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(2))
            }
            Err(e) => return Err(e.to_string()),
        }
    }
}
fn connect(s: &mut TcpStream, v5: bool, present: bool) -> TestResult<()> {
    let (h, b) = read(s)?;
    check(
        h == 0x10 && b.len() > 9 && b[6] == if v5 { 5 } else { 4 } && b[7] & 2 == 0,
        "expected persistent diagnostic CONNECT",
    )?;
    if v5 {
        // Raw diagnostic only: a nonzero expiry makes session_present1 legal.
        // The product deliberately exposes neither this setting nor CleanStart0.
        check(
            b.get(10..16) == Some(&[5, 0x11, 0, 0, 0, 60][..]),
            "persistent MQTT5 fixture needs explicit session expiry60",
        )?;
        send(s, &[0x20, 3, present as u8, 0, 0])
    } else {
        send(s, &[0x20, 2, present as u8, 0])
    }
}
fn id(packet: &(u8, Vec<u8>)) -> TestResult<u16> {
    let offset = if packet.0 >> 4 == 3 {
        check(packet.1.len() >= 2, "short topic")?;
        2 + u16::from_be_bytes([packet.1[0], packet.1[1]]) as usize
    } else {
        0
    };
    check(packet.1.len() >= offset + 2, "short packet id")?;
    Ok(u16::from_be_bytes([packet.1[offset], packet.1[offset + 1]]))
}
fn want(s: &mut TcpStream, head: u8, pkid: u16) -> TestResult<()> {
    let p = read(s)?;
    check(
        p.0 == head && id(&p)? == pkid,
        &format!("expected header{head:#x}/id{pkid}, got {p:?}"),
    )
}
fn ack(s: &mut TcpStream, head: u8, pkid: u16) -> TestResult<()> {
    let [hi, lo] = pkid.to_be_bytes();
    send(s, &[head, 2, hi, lo])
}
fn peer(l: TcpListener, v5: bool, case: &str) -> TestResult<()> {
    let mut s = accept(&l)?;
    connect(&mut s, v5, false)?;
    if case == "duplicate_pubrel" {
        if v5 {
            send(&mut s, &[0x34, 7, 0, 1, b'q', 0, 7, 0, 255])?;
        } else {
            send(&mut s, &[0x34, 6, 0, 1, b'q', 0, 7, 255])?;
        }
        want(&mut s, 0x50, 7)?;
        ack(&mut s, 0x62, 7)?;
        want(&mut s, 0x70, 7)?;
        ack(&mut s, 0x62, 7)?;
        return want(&mut s, 0x70, 7).map_err(|e| format!("duplicate PUBREL lost PUBCOMP: {e}"));
    }
    let publish = read(&mut s)?;
    check(
        publish.0 == 0x34,
        "expected initial QoS2 PUBLISH without DUP",
    )?;
    let pkid = id(&publish)?;
    check(pkid != 0, "zero publish id")?;
    if case == "duplicate_pubrec" {
        ack(&mut s, 0x50, pkid)?;
        want(&mut s, 0x62, pkid)?;
        ack(&mut s, 0x50, pkid)?;
        want(&mut s, 0x62, pkid).map_err(|e| format!("duplicate PUBREC lost PUBREL: {e}"))?;
        return ack(&mut s, 0x70, pkid);
    }
    let released = case.starts_with("after_rec");
    let present = case.ends_with("present1");
    if released {
        ack(&mut s, 0x50, pkid)?;
        want(&mut s, 0x62, pkid)?;
    }
    drop(s);
    let mut s = accept(&l)?;
    connect(&mut s, v5, present)?;
    let p = read(&mut s)?;
    if present {
        if released {
            check(
                p.0 == 0x62 && id(&p)? == pkid,
                "resumed session must replay same PUBREL id",
            )?;
            ack(&mut s, 0x70, pkid)
        } else {
            check(
                p.0 == 0x3c && id(&p)? == pkid,
                &format!("resumed PUBLISH needs original id{pkid} and DUP1; got {p:?}"),
            )?;
            ack(&mut s, 0x50, pkid)?;
            want(&mut s, 0x62, pkid)?;
            ack(&mut s, 0x70, pkid)
        }
    } else {
        // A *new diagnostic request* after CONNACK, not recovery of the old one.
        check(
            p.0 == 0x34 && p.1.last() == Some(&0xbb),
            "new session replayed lost old work instead of fresh request",
        )?;
        let fresh = id(&p)?;
        check(fresh != 0, "zero fresh id")?;
        ack(&mut s, 0x50, fresh)?;
        want(&mut s, 0x62, fresh)?;
        ack(&mut s, 0x70, fresh)
    }
}
enum Probe {
    V4(AsyncClient, Box<EventLoop>),
    V5(v5::AsyncClient, Box<v5::EventLoop>),
}
impl Probe {
    fn new(port: u16, v5: bool) -> Self {
        if v5 {
            let mut o = v5::MqttOptions::new("q2-diagnostic", "127.0.0.1", port);
            o.set_keep_alive(Duration::from_secs(5))
                .set_clean_start(false)
                .set_manual_acks(true)
                .set_outgoing_inflight_upper_limit(2)
                // Only this raw upstream diagnostic needs a retained v5 session.
                .set_session_expiry_interval(Some(60));
            let (c, e) = v5::AsyncClient::new(o, 4);
            Self::V5(c, Box::new(e))
        } else {
            let mut o = MqttOptions::new("q2-diagnostic", "127.0.0.1", port);
            o.set_keep_alive(Duration::ZERO)
                .set_clean_session(false)
                .set_manual_acks(true)
                .set_inflight(2);
            let (c, e) = AsyncClient::new(o, 4);
            Self::V4(c, Box::new(e))
        }
    }
    fn publish(&self, payload: u8) -> TestResult<()> {
        match self {
            Self::V4(c, _) => c
                .try_publish("q", QoS::ExactlyOnce, false, vec![payload])
                .map_err(|e| e.to_string()),
            Self::V5(c, _) => c
                .try_publish("q", Q5::ExactlyOnce, false, vec![payload])
                .map_err(|e| e.to_string()),
        }
    }
    async fn poll(&mut self) -> TestResult<bool> {
        match self {
            Self::V4(c, e) => match e.poll().await.map_err(|e| format!("{e:?}"))? {
                Event::Incoming(Packet::ConnAck(_)) => Ok(true),
                Event::Incoming(Packet::Publish(p)) => {
                    c.try_ack(&p).map_err(|e| e.to_string())?;
                    Ok(false)
                }
                _ => Ok(false),
            },
            Self::V5(c, e) => match e.poll().await.map_err(|e| format!("{e:?}"))? {
                v5::Event::Incoming(p5::Packet::ConnAck(_)) => Ok(true),
                v5::Event::Incoming(p5::Packet::Publish(p)) => {
                    c.try_ack(&p).map_err(|e| e.to_string())?;
                    Ok(false)
                }
                _ => Ok(false),
            },
        }
    }
}
async fn scenario(v5: bool, case: &'static str) -> TestResult<()> {
    let l = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
    let port = l.local_addr().map_err(|e| e.to_string())?.port();
    l.set_nonblocking(true).map_err(|e| e.to_string())?;
    let server = thread::spawn(move || peer(l, v5, case));
    let mut probe = Probe::new(port, v5);
    let mut driver_error = if case != "duplicate_pubrel" {
        probe.publish(0xaa).err()
    } else {
        None
    };
    let mut connections = 0;
    let mut errors = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(4);
    while driver_error.is_none() && !server.is_finished() && Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(30), probe.poll()).await {
            Ok(Ok(true)) => {
                connections += 1;
                if connections == 2 && case.ends_with("present0") {
                    driver_error = probe.publish(0xbb).err();
                }
            }
            Ok(Err(e)) if errors.len() < 4 => {
                errors.push(e);
            }
            _ => {}
        }
    }
    drop(probe);
    // All peer accept/read/write waits are bounded independently; no detached task.
    let result = server
        .join()
        .map_err(|_| "fixture thread panicked".to_string())?;
    if let Some(error) = driver_error {
        return Err(format!("driver: {error}; peer: {result:?}"));
    }
    result.map_err(|e| format!("{e}; engine_errors={errors:?}"))
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let mut passed = 0;
    let mut failed = 0;
    for v5 in [false, true] {
        for case in [
            "duplicate_pubrec",
            "duplicate_pubrel",
            "before_rec_present1",
            "before_rec_present0",
            "after_rec_present1",
            "after_rec_present0",
        ] {
            match scenario(v5, case).await {
                Ok(()) => {
                    passed += 1;
                    println!("PASS protocol={} {case}", if v5 { 5 } else { 4 });
                }
                Err(e) => {
                    failed += 1;
                    println!("FAIL protocol={} {case}: {e}", if v5 { 5 } else { 4 });
                }
            }
        }
    }
    println!("wire readiness: {passed} passed, {failed} failed; raw upstream diagnostics ONLY, no product recovery promise");
    if failed == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
