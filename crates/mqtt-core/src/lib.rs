//! Bounded, no-auto-reconnect MQTT 3.1.1 actor. Experimental host slice.
use mqtt_api::{Error, Options};
use rumqttc::{AsyncClient, Event as EngineEvent, MqttOptions, Outgoing, Packet, QoS};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    runtime::Runtime,
    sync::{mpsc, Notify},
    task::JoinHandle,
};

pub const MAX_PAYLOAD: usize = 65536;
const EVENT_CAPACITY: usize = 128;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Kind {
    Connected = 1,
    Subscribed = 2,
    Queued = 3,
    Sent = 4,
    Acknowledged = 5,
    Message = 6,
    Disconnected = 7,
    Failure = 8,
    Unsubscribed = 9,
}
#[derive(Debug, Clone)]
pub struct Event {
    pub kind: Kind,
    pub op: u64,
    pub code: i32,
    pub packet_id: u16,
    pub qos: u8,
    pub session_present: bool,
    pub topic: Vec<u8>,
    pub payload: Vec<u8>,
}
impl Event {
    fn new(kind: Kind, op: u64) -> Self {
        Self {
            kind,
            op,
            code: 0,
            packet_id: 0,
            qos: 0,
            session_present: false,
            topic: vec![],
            payload: vec![],
        }
    }
}
#[derive(Default)]
struct Events {
    items: VecDeque<Event>,
    fatal: Option<Event>,
    terminated: bool,
}
impl Events {
    fn push(&mut self, e: Event) -> Result<(), Error> {
        if self.terminated {
            return Err(Error::Overflow);
        }
        if self.items.len() == EVENT_CAPACITY {
            self.fail(Error::Overflow, 0);
            return Err(Error::Overflow);
        }
        self.items.push_back(e);
        Ok(())
    }
    fn fail(&mut self, code: Error, op: u64) {
        if !self.terminated {
            self.terminated = true;
            let mut e = Event::new(Kind::Failure, op);
            e.code = code as i32;
            self.fatal = Some(e);
        }
    }
}
type Queue = Arc<Mutex<Events>>;
fn emit(q: &Queue, e: Event) -> Result<(), Error> {
    q.lock().map_err(|_| Error::Internal)?.push(e)
}
#[derive(Debug)]
enum Command {
    Publish(u64, String, Vec<u8>, u8, bool),
    Subscribe(u64, String, u8),
    Unsubscribe(u64, String),
    Disconnect(u64),
}
impl Command {
    fn op(&self) -> u64 {
        match self {
            Self::Publish(o, ..)
            | Self::Subscribe(o, ..)
            | Self::Unsubscribe(o, ..)
            | Self::Disconnect(o) => *o,
        }
    }
}
pub struct Client {
    stop: Arc<Notify>,
    runtime: Arc<Runtime>,
    events: Queue,
    state: Arc<AtomicU8>,
    last_op: AtomicU64,
    tx: Option<mpsc::Sender<Command>>,
    task: Option<JoinHandle<()>>,
}
impl Client {
    pub fn new() -> Result<Self, Error> {
        Ok(Self {
            stop: Arc::new(Notify::new()),
            runtime: mqtt_runtime::acquire()?,
            events: Arc::default(),
            state: Arc::new(AtomicU8::new(0)),
            last_op: AtomicU64::new(0),
            tx: None,
            task: None,
        })
    }
    fn op(&self, op: u64) -> Result<(), Error> {
        {
            let mut old = self.last_op.load(Ordering::SeqCst);
            loop {
                if op <= old {
                    return Err(Error::InvalidArgument);
                }
                match self.last_op.compare_exchange_weak(
                    old,
                    op,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => return Ok(()),
                    Err(current) => old = current,
                }
            }
        }
    }
    pub fn connect(&mut self, op: u64, options: Options) -> Result<(), Error> {
        options.validate()?;
        match self.state.load(Ordering::SeqCst) {
            4 => return Err(Error::Closed),
            0 => {}
            _ => return Err(Error::Busy),
        }
        {
            let mut q = self.events.lock().map_err(|_| Error::Internal)?;
            if !q.items.is_empty() || q.fatal.is_some() {
                return Err(Error::Busy);
            }
            q.terminated = false;
        }
        self.op(op)?;
        if let Some(task) = self.task.take() {
            task.abort();
        }
        let mut opts = MqttOptions::new(options.client_id, options.server, options.port);
        opts.set_keep_alive(options.keep_alive)
            .set_manual_acks(true)
            .set_max_packet_size(MAX_PAYLOAD + 65536, MAX_PAYLOAD);
        let (client, mut engine) = AsyncClient::new(opts, 4);
        engine.network_options.set_connection_timeout(5);
        let (tx, rx) = mpsc::channel(32);
        self.tx = Some(tx);
        self.state.store(1, Ordering::SeqCst);
        self.stop = Arc::new(Notify::new());
        let stop = self.stop.clone();
        let queue = self.events.clone();
        let state = self.state.clone();
        self.task = Some(self.runtime.spawn(async move {
            if let Err((err, operation)) = run(client, engine, rx, &queue, &state, &stop, op).await
            {
                for old in [1, 2, 3] {
                    let _ = state.compare_exchange(old, 0, Ordering::SeqCst, Ordering::SeqCst);
                }
                if let Ok(mut q) = queue.lock() {
                    q.fail(err, operation);
                }
            }
        }));
        Ok(())
    }
    fn submit(&self, cmd: Command) -> Result<(), Error> {
        match self.state.load(Ordering::SeqCst) {
            4 => return Err(Error::Closed),
            2 => {}
            _ => return Err(Error::Busy),
        }
        let tx = self.tx.as_ref().ok_or(Error::Closed)?;
        let permit = tx.try_reserve().map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::QueueFull,
            _ => Error::Closed,
        })?;
        self.op(cmd.op())?;
        if let Command::Publish(op, _, _, qos, _) = &cmd {
            let mut e = Event::new(Kind::Queued, *op);
            e.qos = *qos;
            if let Err(error) = emit(&self.events, e) {
                self.stop.notify_one();
                return Err(error);
            }
        }
        if matches!(cmd, Command::Disconnect(_)) {
            self.state.store(3, Ordering::SeqCst);
        }
        permit.send(cmd);
        Ok(())
    }
    pub fn subscribe(&self, op: u64, topic: String, qos: u8) -> Result<(), Error> {
        validate_topic(&topic, true)?;
        qos_value(qos)?;
        self.submit(Command::Subscribe(op, topic, qos))
    }
    pub fn publish(
        &self,
        op: u64,
        topic: String,
        payload: Vec<u8>,
        qos: u8,
        retain: bool,
    ) -> Result<(), Error> {
        validate_topic(&topic, false)?;
        qos_value(qos)?;
        if payload.len() > MAX_PAYLOAD {
            return Err(Error::InvalidArgument);
        }
        self.submit(Command::Publish(op, topic, payload, qos, retain))
    }
    pub fn unsubscribe(&self, op: u64, topic: String) -> Result<(), Error> {
        validate_topic(&topic, true)?;
        self.submit(Command::Unsubscribe(op, topic))
    }
    pub fn disconnect(&self, op: u64) -> Result<(), Error> {
        self.submit(Command::Disconnect(op))
    }
    /// Force-stop prototype close: no callbacks and no draining guarantee yet.
    pub fn close(&mut self) -> Result<(), Error> {
        self.state.store(4, Ordering::SeqCst);
        if let Some(t) = self.task.take() {
            t.abort();
        }
        self.tx = None;
        Ok(())
    }
    pub fn peek(&self) -> Result<Event, Error> {
        let q = self.events.lock().map_err(|_| Error::Internal)?;
        q.items
            .front()
            .or(q.fatal.as_ref())
            .cloned()
            .ok_or(Error::Empty)
    }
    pub fn pop(&self) -> Result<Event, Error> {
        let mut q = self.events.lock().map_err(|_| Error::Internal)?;
        q.items
            .pop_front()
            .or_else(|| q.fatal.take())
            .ok_or(Error::Empty)
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
fn qos_value(q: u8) -> Result<QoS, Error> {
    match q {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        _ => Err(Error::Unsupported),
    }
}
fn validate_topic(t: &str, filter: bool) -> Result<(), Error> {
    if t.is_empty() || t.len() > 65535 || t.contains('\0') {
        return Err(Error::InvalidArgument);
    }
    let valid = if filter {
        rumqttc::valid_filter(t)
    } else {
        !t.contains(['#', '+'])
    };
    if valid {
        Ok(())
    } else {
        Err(Error::InvalidArgument)
    }
}

async fn run(
    client: AsyncClient,
    mut engine: rumqttc::EventLoop,
    mut commands: mpsc::Receiver<Command>,
    queue: &Queue,
    state: &AtomicU8,
    stop: &Notify,
    connect_op: u64,
) -> Result<(), (Error, u64)> {
    let mut active: Option<Command> = None;
    let mut packet_id = 0;
    loop {
        tokio::select! {
            _ = stop.notified() => return Err((Error::Overflow, 0)),
            command = commands.recv(), if active.is_none() && state.load(Ordering::SeqCst) >= 2 => {
                let Some(cmd) = command else { return Ok(()); };
                let op = cmd.op();
                let result = match &cmd {
                    Command::Publish(_, t, p, q, r) => client.try_publish(t, qos_value(*q).map_err(|e| (e, op))?, *r, p.clone()),
                    Command::Subscribe(_, t, q) => client.try_subscribe(t, qos_value(*q).map_err(|e| (e, op))?),
                    Command::Unsubscribe(_, t) => client.try_unsubscribe(t),
                    Command::Disconnect(_) => client.try_disconnect(),
                };
                result.map_err(|_| (Error::QueueFull, op))?;
                active = Some(cmd);
                packet_id = 0;
            },
            event = engine.poll() => {
                let event = event.map_err(|e| (if matches!(e, rumqttc::ConnectionError::ConnectionRefused(_)) {Error::Refused} else {Error::Network}, connect_op))?;
                if process_event(event, &mut active, &mut packet_id, queue, state, connect_op, &client)? {
                    return Ok(());
                }
            }
        }
    }
}

fn process_event(
    event: EngineEvent,
    active: &mut Option<Command>,
    packet_id: &mut u16,
    queue: &Queue,
    state: &AtomicU8,
    connect_op: u64,
    client: &AsyncClient,
) -> Result<bool, (Error, u64)> {
    match event {
        EngineEvent::Incoming(Packet::ConnAck(ack)) => {
            state.store(2, Ordering::SeqCst);
            let mut e = Event::new(Kind::Connected, connect_op);
            e.session_present = ack.session_present;
            emit(queue, e).map_err(|e| (e, connect_op))?;
        }
        EngineEvent::Outgoing(Outgoing::Publish(id)) => {
            if let Some(Command::Publish(op, _, _, qos, _)) = active {
                *packet_id = id;
                let mut e = Event::new(Kind::Sent, *op);
                e.packet_id = id;
                e.qos = *qos;
                emit(queue, e).map_err(|e| (e, *op))?;
                if *qos == 0 {
                    *active = None;
                }
            }
        }
        EngineEvent::Outgoing(Outgoing::Subscribe(id) | Outgoing::Unsubscribe(id)) => {
            *packet_id = id;
        }
        EngineEvent::Incoming(Packet::PubAck(ack)) => {
            if let Some(Command::Publish(op, _, _, qos, _)) = active {
                if ack.pkid == *packet_id {
                    let mut e = Event::new(Kind::Acknowledged, *op);
                    e.packet_id = *packet_id;
                    e.qos = *qos;
                    emit(queue, e).map_err(|e| (e, *op))?;
                    *active = None;
                }
            }
        }
        EngineEvent::Incoming(Packet::SubAck(ack)) => {
            if let Some(Command::Subscribe(op, _, _)) = active {
                if ack.pkid == *packet_id {
                    let mut e = Event::new(Kind::Subscribed, *op);
                    e.packet_id = *packet_id;
                    if let Some(rumqttc::SubscribeReasonCode::Success(q)) = ack.return_codes.first()
                    {
                        e.qos = *q as u8;
                    }
                    if ack
                        .return_codes
                        .iter()
                        .any(|c| matches!(c, rumqttc::SubscribeReasonCode::Failure))
                    {
                        e.code = Error::Refused as i32;
                    }
                    emit(queue, e).map_err(|e| (e, *op))?;
                    *active = None;
                }
            }
        }
        EngineEvent::Incoming(Packet::UnsubAck(ack)) => {
            if let Some(Command::Unsubscribe(op, _)) = active {
                if ack.pkid == *packet_id {
                    emit(queue, Event::new(Kind::Unsubscribed, *op)).map_err(|e| (e, *op))?;
                    *active = None;
                }
            }
        }
        EngineEvent::Incoming(Packet::Publish(p)) => {
            if p.qos == QoS::ExactlyOnce {
                return Err((Error::Unsupported, 0));
            }
            if p.payload.len() > MAX_PAYLOAD {
                return Err((Error::Overflow, 0));
            }
            let mut e = Event::new(Kind::Message, 0);
            e.topic = p.topic.as_bytes().to_vec();
            e.payload = p.payload.to_vec();
            e.qos = p.qos as u8;
            e.packet_id = p.pkid;
            emit(queue, e).map_err(|e| (e, 0))?;
            client.try_ack(&p).map_err(|_| (Error::QueueFull, 0))?;
        }
        EngineEvent::Outgoing(Outgoing::Disconnect) => {
            let op = active.as_ref().map_or(0, Command::op);
            state.store(0, Ordering::SeqCst);
            emit(queue, Event::new(Kind::Disconnected, op)).map_err(|e| (e, op))?;
            return Ok(true);
        }
        _ => {}
    }
    Ok(false)
}

#[cfg(test)]
mod boundary_tests {
    use super::*;
    #[test]
    fn reject_unsupported_inbound_before_queue_or_ack() {
        let (client, _) = AsyncClient::new(MqttOptions::new("test", "localhost", 1883), 4);
        for (qos, size, expected) in [
            (QoS::ExactlyOnce, 1, Error::Unsupported),
            (QoS::AtMostOnce, MAX_PAYLOAD + 1, Error::Overflow),
        ] {
            let mut p = rumqttc::Publish::new("x", qos, vec![0; size]);
            p.pkid = 1;
            let q = Queue::default();
            let result = process_event(
                EngineEvent::Incoming(Packet::Publish(p)),
                &mut None,
                &mut 0,
                &q,
                &AtomicU8::new(2),
                1,
                &client,
            );
            assert_eq!(result, Err((expected, 0)));
            assert!(q.lock().unwrap().items.is_empty());
        }
    }
}
