//! Owned production driver. Async futures borrow persistent socket/write/state;
//! the actor may cancel poll at any await without losing offsets or ownership.
use crate::{
    driver_codec as codec,
    owned_driver::{
        state::{Action, Machine},
        transport::Transport,
    },
};
use mqtt_api::{Error, Options};
use rumqttc::{Event, Outgoing, Packet, QoS};
use std::collections::VecDeque;
use tokio::{
    sync::mpsc,
    time::{Duration, Instant},
};
pub(crate) enum Request {
    Publish(String, QoS, bool, Vec<u8>),
    Subscribe(String, QoS),
    Unsubscribe(String),
    Ack(u16),
    Disconnect,
}
pub(crate) struct Client {
    tx: mpsc::Sender<Request>,
}
struct Pending {
    id: u16,
    kind: u8,
    qos: QoS,
    wire: Vec<u8>,
    sent: bool,
}
struct Write {
    wire: Vec<u8>,
    event: Option<Outgoing>,
}
pub(crate) struct Engine {
    options: Options,
    rx: mpsc::Receiver<Request>,
    transport: Option<Transport>,
    online: bool,
    reconnecting: bool,
    replay: bool,
    machine: Machine,
    pending: Option<Pending>,
    writes: VecDeque<Write>,
    writing: Option<Option<Outgoing>>,
    write_deadline: Option<Instant>,
    connect_deadline: Option<Instant>,
    last_write: Instant,
    ping: Option<Instant>,
    keep_alive: Duration,
    limits: codec::Limits,
    retryable: bool,
    network_fault: bool,
}
pub(crate) fn session(o: &Options) -> Result<(Client, Engine), Error> {
    o.validate()?;
    if o.use_tls {
        drop(crate::tls::transport(o)?);
    }
    let (tx, rx) = mpsc::channel(4);
    Ok((
        Client { tx },
        Engine {
            options: o.clone(),
            rx,
            transport: None,
            online: false,
            reconnecting: false,
            replay: false,
            machine: Machine::new(o.protocol),
            pending: None,
            writes: VecDeque::new(),
            writing: None,
            write_deadline: None,
            connect_deadline: None,
            last_write: Instant::now(),
            ping: None,
            keep_alive: o.keep_alive,
            limits: codec::Limits::default(),
            retryable: false,
            network_fault: false,
        },
    ))
}
impl Client {
    pub(crate) fn try_publish(&self, t: &str, q: QoS, r: bool, p: Vec<u8>) -> Result<(), ()> {
        self.tx
            .try_send(Request::Publish(t.into(), q, r, p))
            .map_err(|_| ())
    }
    pub(crate) fn try_subscribe(&self, t: &str, q: QoS) -> Result<(), ()> {
        self.tx
            .try_send(Request::Subscribe(t.into(), q))
            .map_err(|_| ())
    }
    pub(crate) fn try_unsubscribe(&self, t: &str) -> Result<(), ()> {
        self.tx
            .try_send(Request::Unsubscribe(t.into()))
            .map_err(|_| ())
    }
    pub(crate) fn try_disconnect(&self) -> Result<(), ()> {
        self.tx.try_send(Request::Disconnect).map_err(|_| ())
    }
    pub(crate) fn try_ack(&self, p: &rumqttc::Publish) -> Result<(), ()> {
        if p.qos != QoS::AtLeastOnce {
            return Ok(());
        }
        self.tx.try_send(Request::Ack(p.pkid)).map_err(|_| ())
    }
}
impl Engine {
    pub(crate) fn is_v5(&self) -> bool {
        self.options.protocol == 5
    }
    pub(crate) fn safe_to_retry(&self) -> bool {
        self.transport.is_none() && self.retryable
    }
    pub(crate) fn prepare_resume(
        &mut self,
        active: &Option<crate::Command>,
        clean: bool,
    ) -> Result<(), Error> {
        if !self.safe_to_retry() {
            return Err(Error::Network);
        }
        if active.is_some()
            && (clean || !matches!(active, Some(crate::Command::Publish(_, _, _, 1 | 2, _))))
        {
            return Err(Error::Network);
        }
        if active.is_some() && self.pending.is_none() && self.rx.is_empty() {
            return Err(Error::Network);
        }
        // Controls cannot survive a connection; their admitted expectations live in actor.
        if self.pending.as_ref().is_some_and(|p| p.kind != 3) {
            self.pending = None;
        }
        self.reconnecting = true;
        Ok(())
    }
    fn enqueue(&mut self, wire: Vec<u8>, event: Option<Outgoing>) -> Result<(), Error> {
        if self.writes.len() >= 4 {
            return Err(Error::Overflow);
        }
        self.writes.push_back(Write { wire, event });
        Ok(())
    }
    fn matching(&self, id: u16, kind: u8, qos: Option<QoS>) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|p| p.id == id && p.kind == kind && qos.is_none_or(|q| q == p.qos))
    }
    fn request(&mut self, r: Request) -> Result<(), Error> {
        if let Request::Ack(id) = r {
            return self.enqueue(codec::ack(self.options.protocol, 0x40, id, 0), None);
        }
        if let Request::Disconnect = r {
            return self.enqueue(vec![0xe0, 0], Some(Outgoing::Disconnect));
        }
        if self.pending.is_some() {
            return Err(Error::Internal);
        }
        let (kind, qos) = match &r {
            Request::Publish(_, q, retain, _) => {
                if *q as u8 > self.limits.qos || (*retain && !self.limits.retain) {
                    return Err(Error::Refused);
                }
                (3, *q)
            }
            Request::Subscribe(t, q) => {
                if (!self.limits.wildcard && t.contains(['#', '+']))
                    || (!self.limits.shared && t.starts_with("$share/"))
                {
                    return Err(Error::Refused);
                }
                (8, *q)
            }
            Request::Unsubscribe(_) => (10, QoS::AtMostOnce),
            _ => (0, QoS::AtMostOnce),
        };
        let id = if kind == 3 && qos == QoS::AtMostOnce {
            0
        } else {
            self.machine.allocate()?
        };
        let wire = codec::encode(self.options.protocol, &r, id)?;
        if wire.len() > self.limits.packet {
            return Err(Error::Overflow);
        }
        let event = match kind {
            3 => Outgoing::Publish(id),
            8 => Outgoing::Subscribe(id),
            _ => Outgoing::Unsubscribe(id),
        };
        if kind == 3 && qos == QoS::ExactlyOnce {
            self.machine.begin(id, wire.clone())?;
        }
        self.pending = Some(Pending {
            id,
            kind,
            qos,
            wire: wire.clone(),
            sent: false,
        });
        self.enqueue(wire, Some(event))
    }
    fn qos2(&mut self, wire: &[u8]) -> Result<Option<Event>, Error> {
        let mut event = None;
        for a in self.machine.receive(wire)? {
            match a {
                Action::Send(w) => self.enqueue(w, None)?,
                Action::Complete(id) => {
                    if self.matching(id, 3, Some(QoS::ExactlyOnce)) {
                        self.pending = None;
                        event = Some(Event::Incoming(Packet::PubComp(rumqttc::PubComp::new(id))));
                    }
                }
                Action::Failed(id, e) => {
                    if self.matching(id, 3, Some(QoS::ExactlyOnce)) {
                        self.pending = None;
                        return Err(e);
                    }
                }
                Action::Deliver(id, t, p) => {
                    let mut m = rumqttc::Publish::new(
                        String::from_utf8(t).map_err(|_| Error::Network)?,
                        QoS::ExactlyOnce,
                        p,
                    );
                    m.pkid = id;
                    event = Some(Event::Incoming(Packet::Publish(m)));
                }
            }
        }
        Ok(event)
    }
    pub(crate) async fn poll(&mut self) -> Result<Event, Error> {
        self.network_fault = false;
        let result = self.next().await;
        if let Err(e) = result {
            // Only established transport I/O may retry a generic TLS I/O failure.
            // A fresh handshake still verifies all certificates and remains terminal
            // on authentication/hostname failures; no TLS error is swallowed.
            let established_tls_io = self.online && e == Error::Tls;
            self.transport = None;
            self.online = false;
            self.writing = None;
            self.write_deadline = None;
            self.connect_deadline = None;
            self.writes.clear();
            self.ping = None;
            self.retryable = self.network_fault
                && (matches!(e, Error::Network | Error::Timeout | Error::Refused)
                    || established_tls_io);
        }
        result
    }
    async fn next(&mut self) -> Result<Event, Error> {
        loop {
            if self.transport.is_none() {
                self.network_fault = true;
                self.transport = Some(Transport::connect(&self.options).await?);
                self.network_fault = false;
                self.limits = codec::Limits::default();
                self.keep_alive = self.options.keep_alive;
                self.connect_deadline = Some(Instant::now() + Duration::from_secs(5));
                self.enqueue(codec::connect(&self.options)?, None)?;
            }
            if self.writing.is_none() {
                if let Some(w) = self.writes.pop_front() {
                    self.transport.as_mut().unwrap().queue_send(&w.wire)?;
                    self.writing = Some(w.event);
                    self.write_deadline = Some(Instant::now() + Duration::from_secs(5));
                }
            }
            if self.writing.is_some() {
                self.network_fault = true;
                tokio::time::timeout_at(
                    self.write_deadline.unwrap(),
                    self.transport.as_mut().unwrap().flush_pending(),
                )
                .await
                .map_err(|_| Error::Timeout)??;
                self.network_fault = false;
                self.last_write = Instant::now();
                self.write_deadline = None;
                if let Some(o) = self.writing.take().flatten() {
                    if let Some(p) = self.pending.as_mut() {
                        p.sent = true;
                    }
                    if matches!(o, Outgoing::Publish(0)) {
                        self.pending = None;
                    }
                    return Ok(Event::Outgoing(o));
                }
                continue;
            }
            if self.online && self.replay {
                self.replay = false;
                if let Some(p) = &self.pending {
                    if p.kind == 3 && p.qos == QoS::AtLeastOnce {
                        let mut w = p.wire.clone();
                        if p.sent {
                            w[0] |= 8;
                        }
                        self.enqueue(w, Some(Outgoing::Publish(p.id)))?;
                    } else if p.kind == 3 && p.qos == QoS::ExactlyOnce {
                        let id = p.id;
                        for a in self.machine.resume(true, false) {
                            if let Action::Send(w) = a {
                                let event = if w[0] >> 4 == 3 {
                                    Some(Outgoing::Publish(id))
                                } else {
                                    None
                                };
                                self.enqueue(w, event)?;
                            }
                        }
                    }
                }
                continue;
            }
            if self.online {
                match self.rx.try_recv() {
                    Ok(r) => {
                        self.request(r)?;
                        continue;
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => return Err(Error::Closed),
                    Err(mpsc::error::TryRecvError::Empty) => {}
                }
            }
            let idle = if self.online && !self.keep_alive.is_zero() {
                Some(self.ping.unwrap_or(self.last_write + self.keep_alive))
            } else {
                self.connect_deadline
            };
            self.network_fault = true;
            let wire = tokio::select! {
             biased;
             _=async{match idle{Some(t)=>tokio::time::sleep_until(t).await,None=>std::future::pending().await}}=>{if !self.online||self.ping.is_some(){return Err(Error::Timeout)}self.ping=Some(Instant::now()+self.keep_alive);self.enqueue(vec![0xc0,0],None)?;continue;},
             r=self.transport.as_mut().unwrap().packet()=>r?,
             r=self.rx.recv(),if self.online=>{self.network_fault=false;self.request(r.ok_or(Error::Closed)?)?;continue;}
            };
            self.network_fault = false;
            let kind = wire[0] >> 4;
            if (5..=7).contains(&kind) {
                if !self.online {
                    return Err(Error::Network);
                }
                if let Some(e) = self.qos2(&wire)? {
                    return Ok(e);
                }
                continue;
            }
            let (p, limits) = match codec::decode(self.options.protocol, &wire) {
                Ok(v) => v,
                Err(e) => {
                    // A broker CONNACK refusal can consume another bounded
                    // reconnect attempt. A negative publish ACK cannot.
                    if kind == 2 && !self.online && e == Error::Refused {
                        self.network_fault = true;
                    }
                    return Err(e);
                }
            };
            if !self.online {
                let Packet::ConnAck(a) = &p else {
                    return Err(Error::Network);
                };
                self.online = true;
                self.connect_deadline = None;
                if let Some(l) = limits {
                    self.limits = l;
                    if let Some(k) = l.keep_alive {
                        self.keep_alive = Duration::from_secs(k as u64);
                    }
                }
                if self.reconnecting {
                    self.replay = a.session_present && !self.options.clean_session;
                    if !self.replay {
                        self.machine.resume(false, true);
                        self.pending = None;
                    }
                }
                self.reconnecting = false;
                return Ok(Event::Incoming(p));
            }
            match &p {
                Packet::ConnAck(_) => return Err(Error::Network),
                Packet::PubAck(a) => {
                    if !self.matching(a.pkid, 3, Some(QoS::AtLeastOnce)) {
                        continue;
                    }
                    self.pending = None;
                }
                Packet::SubAck(a) => {
                    if !self.matching(a.pkid, 8, None) {
                        continue;
                    }
                    if a.return_codes.len() != 1 {
                        return Err(Error::Network);
                    }
                    self.pending = None;
                }
                Packet::UnsubAck(a) => {
                    if !self.matching(a.pkid, 10, None) {
                        continue;
                    }
                    self.pending = None;
                }
                Packet::Publish(p) if p.qos == QoS::ExactlyOnce => {
                    self.qos2(&wire)?;
                    continue;
                }
                Packet::PingResp => {
                    self.ping = None;
                    continue;
                }
                Packet::Disconnect => {
                    self.network_fault = true;
                    return Err(Error::Network);
                }
                _ => {}
            }
            return Ok(Event::Incoming(p));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };
    fn read(s: &mut impl Read) -> Vec<u8> {
        let mut b = [0];
        s.read_exact(&mut b).unwrap();
        let h = b[0];
        let mut n = 0;
        let mut mul = 1;
        loop {
            s.read_exact(&mut b).unwrap();
            n += (b[0] as usize & 127) * mul;
            if b[0] & 128 == 0 {
                break;
            }
            mul *= 128
        }
        let mut v = vec![0; n];
        s.read_exact(&mut v).unwrap();
        let mut p = vec![h];
        p.extend(v);
        p
    }
    #[test]
    fn replay_before_original_sent_event_still_yields_publish_id() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut o = Options::new("127.0.0.1", "partial", l.local_addr().unwrap().port());
        o.clean_session = false;
        let peer = thread::spawn(move || {
            let (mut s, _) = l.accept().unwrap();
            s.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            read(&mut s);
            s.write_all(&[0x20, 2, 1, 0]).unwrap();
            let p = read(&mut s);
            assert_eq!(p[0], 0x3c);
            s.write_all(&[0x50, 2, 0, 1]).unwrap();
            assert_eq!(&read(&mut s)[..3], &[0x62, 0, 1]);
            s.write_all(&[0x70, 2, 0, 1]).unwrap();
        });
        let (_c, mut e) = session(&o).unwrap();
        e.request(Request::Publish(
            "q".into(),
            QoS::ExactlyOnce,
            false,
            vec![1],
        ))
        .unwrap();
        e.writes.clear();
        e.reconnecting = true;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            assert!(matches!(
                e.poll().await.unwrap(),
                Event::Incoming(Packet::ConnAck(_))
            ));
            assert!(
                matches!(
                    e.poll().await.unwrap(),
                    Event::Outgoing(Outgoing::Publish(1))
                ),
                "replay must establish actor id even when the original write never yielded Sent"
            );
            assert!(matches!(
                e.poll().await.unwrap(),
                Event::Incoming(Packet::PubComp(_))
            ));
        });
        peer.join().unwrap();
    }
}
