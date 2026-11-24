//! Explicit QoS2 adapter-boundary diagnostic, not a product implementation.
//! Four strict duplicate-handshake readiness cases MUST pass before enablement.
//! One v3 public-Network capability case only proves a manual-driver alternative.
use rumqttc::{v5, Event, Packet, QoS};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::ExitCode,
    sync::mpsc,
    thread,
    time::Duration,
};
use v5::mqttbytes::{v5 as p5, QoS as Q5};
type Result<T> = std::result::Result<T, String>;
fn read(s: &mut TcpStream) -> Result<(u8, Vec<u8>)> {
    let mut b = [0];
    s.read_exact(&mut b).map_err(|e| e.to_string())?;
    let head = b[0];
    s.read_exact(&mut b).map_err(|e| e.to_string())?;
    if b[0] > 127 {
        return Err("fixture only accepts <=127 byte packets".into());
    }
    let mut body = vec![0; b[0] as usize];
    s.read_exact(&mut body).map_err(|e| e.to_string())?;
    Ok((head, body))
}
fn want(s: &mut TcpStream, head: u8, id: u16) -> Result<()> {
    let p = read(s)?;
    if p.0 != head || p.1.get(..2) != Some(&id.to_be_bytes()) {
        return Err(format!("expected {head:#x}/id{id}, got {p:?}"));
    }
    Ok(())
}
fn send(s: &mut TcpStream, b: &[u8]) -> Result<()> {
    s.write_all(b).map_err(|e| e.to_string())
}
fn peer(listener: TcpListener, five: bool, case: &str, ready: mpsc::Sender<()>) -> Result<()> {
    // The driver always opens a loopback connection; all peer I/O waits bounded.
    listener.set_nonblocking(true).map_err(|e| e.to_string())?;
    let end = std::time::Instant::now() + Duration::from_secs(2);
    let mut s = loop {
        match listener.accept() {
            Ok((s, _)) => break s,
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    && std::time::Instant::now() < end =>
            {
                thread::yield_now()
            }
            Err(e) => return Err(e.to_string()),
        }
    };
    s.set_read_timeout(Some(Duration::from_secs(1)))
        .map_err(|e| e.to_string())?;
    s.set_write_timeout(Some(Duration::from_secs(1)))
        .map_err(|e| e.to_string())?;
    let connect = read(&mut s)?;
    if connect.0 != 0x10 || connect.1.get(6) != Some(&(if five { 5 } else { 4 })) {
        return Err("CONNECT mismatch".into());
    }
    if five {
        send(&mut s, &[0x20, 3, 0, 0, 0])?;
    } else {
        send(&mut s, &[0x20, 2, 0, 0])?;
    }
    if case == "batched_pubrec" {
        let p = read(&mut s)?;
        if p.0 != 0x34 {
            return Err(format!("expected QoS2 PUBLISH, got {p:?}"));
        }
        let id = u16::from_be_bytes([p.1[3], p.1[4]]);
        let [hi, lo] = id.to_be_bytes();
        // One write, then a cross-thread positive barrier before driver's poll.
        send(&mut s, &[0x50, 2, hi, lo, 0x50, 2, hi, lo])?;
        ready.send(()).map_err(|e| e.to_string())?;
        want(&mut s, 0x62, id)?;
        want(&mut s, 0x62, id)?;
        send(&mut s, &[0x70, 2, hi, lo])?;
    } else {
        if case == "batched_pubrel" {
            if five {
                send(&mut s, &[0x34, 7, 0, 1, b'q', 0, 7, 0, 255])?;
            } else {
                send(&mut s, &[0x34, 6, 0, 1, b'q', 0, 7, 255])?;
            }
            want(&mut s, 0x50, 7)?;
        }
        send(&mut s, &[0x62, 2, 0, 7, 0x62, 2, 0, 7])?;
        ready.send(()).map_err(|e| e.to_string())?;
        want(&mut s, 0x70, 7)?;
        want(&mut s, 0x70, 7)?;
    }
    let final_packet = read(&mut s)?;
    if final_packet.0 != 0xe0 {
        return Err(format!(
            "expected live DISCONNECT fence, got {final_packet:?}"
        ));
    }
    Ok(())
}
enum Probe {
    V4(rumqttc::AsyncClient, Box<rumqttc::EventLoop>),
    V5(v5::AsyncClient, Box<v5::EventLoop>),
}
#[derive(Debug, PartialEq)]
enum Notice {
    Connected,
    Publish,
    OutPublish,
    OutRec,
    Rec,
    Rel,
    OutRel,
    OutComp,
    Comp,
    Disconnected,
    Other,
}
impl Probe {
    fn new(port: u16, five: bool) -> Self {
        if five {
            let mut o = v5::MqttOptions::new("q2-boundary", "127.0.0.1", port);
            o.set_keep_alive(Duration::from_secs(5))
                .set_manual_acks(true);
            let (c, e) = v5::AsyncClient::new(o, 4);
            Self::V5(c, Box::new(e))
        } else {
            let mut o = rumqttc::MqttOptions::new("q2-boundary", "127.0.0.1", port);
            o.set_keep_alive(Duration::ZERO).set_manual_acks(true);
            let (c, e) = rumqttc::AsyncClient::new(o, 4);
            Self::V4(c, Box::new(e))
        }
    }
    fn publish(&self) -> Result<()> {
        match self {
            Self::V4(c, _) => c
                .try_publish("q", QoS::ExactlyOnce, false, vec![0, 255])
                .map_err(|e| e.to_string()),
            Self::V5(c, _) => c
                .try_publish("q", Q5::ExactlyOnce, false, vec![0, 255])
                .map_err(|e| e.to_string()),
        }
    }
    async fn poll(&mut self) -> Result<Notice> {
        Ok(match self {
            Self::V4(c, e) => match e.poll().await.map_err(|e| format!("{e:?}"))? {
                Event::Incoming(Packet::ConnAck(_)) => Notice::Connected,
                Event::Incoming(Packet::Publish(p)) => {
                    c.try_ack(&p).map_err(|e| e.to_string())?;
                    Notice::Publish
                }
                Event::Outgoing(rumqttc::Outgoing::Publish(_)) => Notice::OutPublish,
                Event::Outgoing(rumqttc::Outgoing::PubRec(_)) => Notice::OutRec,
                Event::Incoming(Packet::PubRec(_)) => Notice::Rec,
                Event::Incoming(Packet::PubRel(_)) => Notice::Rel,
                Event::Incoming(Packet::PubComp(_)) => Notice::Comp,
                Event::Outgoing(rumqttc::Outgoing::PubRel(_)) => Notice::OutRel,
                Event::Outgoing(rumqttc::Outgoing::PubComp(_)) => Notice::OutComp,
                Event::Outgoing(rumqttc::Outgoing::Disconnect) => Notice::Disconnected,
                _ => Notice::Other,
            },
            Self::V5(c, e) => match e.poll().await.map_err(|e| format!("{e:?}"))? {
                v5::Event::Incoming(p5::Packet::ConnAck(_)) => Notice::Connected,
                v5::Event::Incoming(p5::Packet::Publish(p)) => {
                    c.try_ack(&p).map_err(|e| e.to_string())?;
                    Notice::Publish
                }
                v5::Event::Outgoing(rumqttc::Outgoing::Publish(_)) => Notice::OutPublish,
                v5::Event::Outgoing(rumqttc::Outgoing::PubRec(_)) => Notice::OutRec,
                v5::Event::Incoming(p5::Packet::PubRec(_)) => Notice::Rec,
                v5::Event::Incoming(p5::Packet::PubRel(_)) => Notice::Rel,
                v5::Event::Incoming(p5::Packet::PubComp(_)) => Notice::Comp,
                v5::Event::Outgoing(rumqttc::Outgoing::PubRel(_)) => Notice::OutRel,
                v5::Event::Outgoing(rumqttc::Outgoing::PubComp(_)) => Notice::OutComp,
                v5::Event::Outgoing(rumqttc::Outgoing::Disconnect) => Notice::Disconnected,
                _ => Notice::Other,
            },
        })
    }
    fn disconnect(&self) -> Result<()> {
        match self {
            Self::V4(c, _) => c.try_disconnect().map_err(|e| e.to_string()),
            Self::V5(c, _) => c.try_disconnect().map_err(|e| e.to_string()),
        }
    }
    fn boundary(&self) -> String {
        match self {
            Self::V4(_, e) => format!(
                "network_present={} buffered_events={} pending={}",
                e.network.is_some(),
                e.state.events.len(),
                e.pending.len()
            ),
            Self::V5(_, e) => format!(
                "network_visibility=private buffered_events={} pending={}",
                e.state.events.len(),
                e.pending.len()
            ),
        }
    }
    async fn manual_pre_read(&mut self, mutate: bool) -> Result<()> {
        // Supported public packet I/O really is accessible. This TAKES OVER reads
        // before state processing; it does not repair EventLoop::poll callbacks.
        let id = if mutate { 8 } else { 7 };
        match self {
            Self::V4(_, e) => {
                let n = e.network.as_mut().ok_or("no network")?;
                for _ in 0..2 {
                    let p = n.read().await.map_err(|e| e.to_string())?;
                    if !matches!(p,Packet::PubRel(p) if p.pkid==7) {
                        return Err("expected raw PUBREL7".into());
                    }
                    n.write(Packet::PubComp(rumqttc::PubComp::new(id)))
                        .await
                        .map_err(|e| e.to_string())?;
                    n.flush().await.map_err(|e| e.to_string())?;
                }
                n.write(Packet::Disconnect)
                    .await
                    .map_err(|e| e.to_string())?;
                n.flush().await.map_err(|e| e.to_string())?;
            }
            Self::V5(_, _) => {
                return Err("v5 EventLoop.network is private; see compile probe".into())
            }
        }
        Ok(())
    }
}
async fn scenario(five: bool, case: &'static str, mutate: bool) -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    let (tx, rx) = mpsc::channel();
    let server = thread::spawn(move || peer(listener, five, case, tx));
    let mut probe = Probe::new(port, five);
    let outcome = async {
        if probe.poll().await? != Notice::Connected {
            return Err("no CONNACK".into());
        }
        if case == "public_network_pre_read" {
            rx.recv_timeout(Duration::from_secs(2))
                .map_err(|e| e.to_string())?;
            probe.manual_pre_read(mutate).await?;
            return Ok("public Network packet interception works; requires a different packet driver, NOT readiness".to_string());
        }
        if case == "batched_pubrec" {
            probe.publish()?;
            if probe.poll().await? != Notice::OutPublish {
                return Err("missing outgoing publish".into());
            }
        } else {
            if probe.poll().await? != Notice::Publish {
                return Err("missing incoming publish".into());
            }
            if probe.poll().await? != Notice::OutRec {
                return Err("missing PUBREC".into());
            }
        }
        rx.recv_timeout(Duration::from_secs(2))
            .map_err(|e| e.to_string())?;
        let mut replies = 0;
        let mut comp = false;
        // A future official fix can pass this strict gate; no hardcoded failure.
        loop {
            let observed = probe.poll().await.map_err(|e| {
                format!(
                    "poll_error={e}; no pre-state callback; {}",
                    probe.boundary()
                )
            })?;
            if observed
                == if case == "batched_pubrec" {
                    Notice::OutRel
                } else {
                    Notice::OutComp
                }
            {
                replies += 1;
            }
            if observed == Notice::Comp {
                comp = true;
            }
            if observed == Notice::Publish {
                return Err("duplicate app-facing publish notice".into());
            }
            if replies == 2 && (case != "batched_pubrec" || comp) {
                break;
            }
        }
        probe.disconnect()?;
        while probe.poll().await? != Notice::Disconnected {}
        Ok("same-connection duplicate handshake and disconnect fence observed".into())
    };
    let result = tokio::time::timeout(Duration::from_secs(3), outcome)
        .await
        .map_err(|e| e.to_string())
        .and_then(|v| v);
    drop(probe);
    let peer = server.join().map_err(|_| "peer panic")?;
    match (result, peer) {
        (Ok(v), Ok(())) => Ok(v),
        (a, b) => Err(format!("driver={a:?}; peer={b:?}")),
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let mutate = std::env::var_os("QOS2_PROBE_MUTATE_ACK_ID").is_some();
    let mut readiness_pass = 0;
    let mut capability_pass = 0;
    for five in [false, true] {
        for case in [
            "batched_pubrec",
            "batched_pubrel",
            "public_network_pre_read",
        ] {
            if five && case == "public_network_pre_read" {
                println!("UNAVAILABLE v5 public Network access: field private (compile evidence retained)");
                continue;
            }
            match scenario(five, case, mutate).await {
                Ok(v) => {
                    if case == "public_network_pre_read" {
                        capability_pass += 1;
                    } else {
                        readiness_pass += 1;
                    }
                    println!("PASS v5={five} {case}: {v}");
                }
                Err(e) => eprintln!("FAIL v5={five} {case}: {e}"),
            }
        }
    }
    println!("duplicate-handshake readiness {readiness_pass}/4; manual-public-Network capability {capability_pass}/1 (NOT QoS2 support)");
    if readiness_pass == 4 && capability_pass == 1 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
