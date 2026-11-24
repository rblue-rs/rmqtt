//! Native broker evidence for candidate only, NOT public plugin support.
use super::{
    state::{Action, Machine},
    transport::Transport,
};
use mqtt_api::Options;
use std::{
    fs,
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
struct Broker {
    child: Child,
    dir: PathBuf,
    port: u16,
}
impl Broker {
    fn new(tls: bool, ws: bool) -> Self {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        drop(l);
        let dir = std::env::temp_dir().join(format!(
            "mqtt-owned-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&dir).unwrap();
        let mut conf =
            format!("listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\n");
        if ws {
            conf.push_str("protocol websockets\n");
        }
        if tls {
            let python = std::env::var("MQTT_TEST_PYTHON").unwrap_or("python3".into());
            assert!(Command::new(python)
                .arg(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../../scripts/gen_test_certs.py"
                ))
                .arg(&dir)
                .stdout(Stdio::null())
                .status()
                .unwrap()
                .success());
            conf.push_str(&format!(
                "certfile {}\nkeyfile {}\n",
                dir.join("server-127.pem").display(),
                dir.join("server-127.key").display()
            ));
        }
        fs::write(dir.join("broker.conf"), conf).unwrap();
        let mut child =
            Command::new(std::env::var("MQTT5_BROKER").unwrap_or("/usr/sbin/mosquitto".into()))
                .arg("-c")
                .arg(dir.join("broker.conf"))
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
        let end = Instant::now() + Duration::from_secs(5);
        loop {
            assert!(child.try_wait().unwrap().is_none());
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            assert!(Instant::now() < end);
            std::thread::yield_now();
        }
        Self { child, dir, port }
    }
}
impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.dir);
    }
}
fn rl(mut n: usize) -> Vec<u8> {
    let mut b = vec![];
    loop {
        let x = (n % 128) as u8;
        n /= 128;
        b.push(x | if n > 0 { 128 } else { 0 });
        if n == 0 {
            return b;
        }
    }
}
fn packet(kind: u8, body: Vec<u8>) -> Vec<u8> {
    let mut b = vec![kind];
    b.extend(rl(body.len()));
    b.extend(body);
    b
}
#[test]
fn candidate_native_mosquitto_qos2_all_protocol_transport_cells() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for p in [4, 5] {
        for tls in [false, true] {
            for ws in [false, true] {
                let broker = Broker::new(tls, ws);
                rt.block_on(async{
  tokio::time::timeout(Duration::from_secs(8),async{
   let mut o=Options::new("127.0.0.1","owned-candidate",broker.port);o.protocol=p;o.use_tls=tls;o.use_websocket=ws;o.keep_alive=if p==5{Duration::from_secs(5)}else{Duration::ZERO};if tls{o.ca_pem=Some(fs::read(broker.dir.join("ca.pem")).unwrap());}
   let mut t=Transport::connect(&o).await.unwrap();let mut connect=vec![0,4,b'M',b'Q',b'T',b'T',p,2,0,5];if p==5{connect.push(0);}connect.extend([0,5,b'o',b'w',b'n',b'e',b'd']);t.send(&packet(0x10,connect)).await.unwrap();assert_eq!(t.packet().await.unwrap()[0],0x20);
   let mut sub=vec![0,100];if p==5{sub.push(0);}sub.extend([0,1,b'q',2]);t.send(&packet(0x82,sub)).await.unwrap();let ack=t.packet().await.unwrap();assert_eq!(ack[0],0x90);assert_eq!(ack.last(),Some(&2));
   let mut m=Machine::new(p);let id=m.allocate().unwrap();let mut body=vec![0,1,b'q'];body.extend(id.to_be_bytes());if p==5{body.push(0);}body.extend([0,255,1]);let wire=packet(0x34,body);m.begin(id,wire.clone()).unwrap();t.send(&wire).await.unwrap();
   let mut completed=false;let mut delivered=0;while !completed||delivered==0{let incoming=t.packet().await.unwrap();for a in m.receive(&incoming).unwrap(){match a{Action::Send(b)=>t.send(&b).await.unwrap(),Action::Complete(n)=>{assert_eq!(n,id);assert!(!completed);completed=true;},Action::Deliver(_,topic,b)=>{assert_eq!(topic,b"q");assert_eq!(b,[0,255,1]);delivered+=1;assert_eq!(delivered,1);},Action::Failed(_,e)=>panic!("candidate protocol failed: {e:?}"),}}}
   let mut unsub=vec![0,101];if p==5{unsub.push(0);}unsub.extend([0,1,b'q']);t.send(&packet(0xa2,unsub)).await.unwrap();assert_eq!(t.packet().await.unwrap()[0],0xb0);t.send(&[0xe0,0]).await.unwrap();
   println!("candidate native Mosquitto protocol={p} tls={tls} ws={ws}: SUB QoS2 binary outbound PUBCOMP inbound PUBREL unsubscribe PASS; NOT public support");
  }).await.unwrap();
 });
            }
        }
    }
}

#[test]
fn production_native_mosquitto_qos2_all_protocol_transport_cells() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    mqtt_runtime::init().unwrap();
    fn next(c: &crate::Client) -> crate::Event {
        let end = Instant::now() + Duration::from_secs(4);
        loop {
            match c.pop() {
                Ok(e) => {
                    assert_ne!(e.kind, crate::Kind::Failure, "{e:?}");
                    return e;
                }
                Err(mqtt_api::Error::Empty) => {
                    assert!(Instant::now() < end);
                    std::thread::yield_now();
                }
                e => panic!("{e:?}"),
            }
        }
    }
    for protocol in [4, 5] {
        for tls in [false, true] {
            for ws in [false, true] {
                let broker = Broker::new(tls, ws);
                let mut o = Options::new("127.0.0.1", "production-qos2", broker.port);
                o.protocol = protocol;
                o.keep_alive = if protocol == 5 {
                    Duration::from_secs(5)
                } else {
                    Duration::ZERO
                };
                o.use_tls = tls;
                o.use_websocket = ws;
                if tls {
                    o.ca_pem = Some(fs::read(broker.dir.join("ca.pem")).unwrap());
                }
                let mut c = crate::Client::new().unwrap();
                c.connect(1, o).unwrap();
                assert_eq!(next(&c).kind, crate::Kind::Connected);
                c.subscribe(2, "native/qos2".into(), 2).unwrap();
                let s = next(&c);
                assert_eq!(s.kind, crate::Kind::Subscribed);
                assert_eq!(s.qos, 2);
                c.publish(3, "native/qos2".into(), vec![0, 255, 128], 2, false)
                    .unwrap();
                let (mut ack, mut message) = (0, 0);
                while ack == 0 || message == 0 {
                    let e = next(&c);
                    match e.kind {
                        crate::Kind::Acknowledged => {
                            assert_eq!(e.op, 3);
                            assert_eq!(e.qos, 2);
                            ack += 1;
                            assert_eq!(ack, 1)
                        }
                        crate::Kind::Message => {
                            assert_eq!(e.qos, 2);
                            assert_eq!(e.payload, [0, 255, 128]);
                            message += 1;
                            assert_eq!(message, 1)
                        }
                        _ => {}
                    }
                }
                c.unsubscribe(4, "native/qos2".into()).unwrap();
                assert_eq!(next(&c).kind, crate::Kind::Unsubscribed);
                c.disconnect(5).unwrap();
                assert_eq!(next(&c).kind, crate::Kind::Disconnected);
                c.close().unwrap();
                drop(c);
                println!("PRODUCTION native broker protocol={protocol} tls={tls} ws={ws} binary QoS2 PUBCOMP+one message PASS");
            }
        }
    }
    mqtt_runtime::shutdown().unwrap();
}
