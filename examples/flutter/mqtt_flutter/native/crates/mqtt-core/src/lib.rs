//! Bounded MQTT 3.1.1 / MQTT5 baseline actor. Automatic reconnect is off by default.
mod generation;
mod reconnect;
mod tls;
#[cfg(feature = "ws-test-observer")]
#[doc(hidden)]
pub mod ws_test_observer;
use engine::Client as AsyncClient;
use mqtt_api::{Error, Options, ReconnectPolicy};
use rumqttc::{Event as EngineEvent, Outgoing, Packet, QoS};
mod budget;
mod driver_codec;
mod engine;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    runtime::Runtime,
    sync::{mpsc, Notify},
    task::JoinHandle,
    time::{Duration, Instant},
};

pub const MAX_PAYLOAD: usize = 65536;
pub use reconnect::{delay_cap as reconnect_delay_cap, equal_jitter};
const EVENT_CAPACITY: usize = 128;
// WS retained expectation metadata only; NOT an aggregate transport memory cap.
const WS_EXPECTATION_TOPICS: usize = 128;
const WS_EXPECTATION_TOPIC_BYTES: usize = 256 * 1024;
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
    Reconnecting = 10,
    Reconnected = 11,
    SubscriptionsAligned = 12,
    SubscriptionsResubscribed = 13,
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
/// Out-of-band drain state; terminal status is retained until the client is freed.
/// Drained means protocol completion, never subscriber/application delivery.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum DrainStatus {
    #[default]
    NotStarted = 0,
    Pending = 1,
    Drained = 2,
    TimedOut = 3,
    Failed = 4,
    Forced = 5,
}
#[derive(Default)]
struct Events {
    items: VecDeque<Event>,
    fatal: Option<Event>,
    terminated: bool,
    sealed: bool,
    disconnects: Vec<u64>,
    drain: DrainStatus,
    deadline: Option<Instant>,
    expectations: BTreeMap<String, u8>,
    limit_ws_expectations: bool,
    halt_reconnect: bool,
    recovery_pending: bool,
}
impl Events {
    fn push(&mut self, e: Event) -> Result<(), Error> {
        if self.sealed {
            return Err(Error::Closed);
        }
        if self.terminated {
            return Err(Error::Overflow);
        }
        if e.kind == Kind::Message && self.drain != DrainStatus::NotStarted {
            return Ok(());
        }
        if self.items.len() == EVENT_CAPACITY {
            self.fail(Error::Overflow, 0);
            return Err(Error::Overflow);
        }
        self.items.push_back(e);
        Ok(())
    }
    fn fail(&mut self, code: Error, op: u64) {
        if self.sealed {
            return;
        }
        if !self.terminated {
            self.terminated = true;
            let mut e = Event::new(Kind::Failure, op);
            e.code = code as i32;
            self.fatal = Some(e);
        }
    }
    fn seal(&mut self) {
        self.sealed = true;
        self.items.clear();
        self.fatal = None;
        self.disconnects.clear();
        self.expectations.clear();
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
// Packet limits include topic/packet-id/remaining-length framing, not only payload.
const MAX_WIRE_PACKET: usize = MAX_PAYLOAD + u16::MAX as usize + 9;
pub struct Client {
    stop: Arc<Notify>,
    drain_wake: Arc<Notify>,
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
            drain_wake: Arc::new(Notify::new()),
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
    pub fn connect(&mut self, op: u64, mut options: Options) -> Result<(), Error> {
        options.validate()?;
        match self.state.load(Ordering::SeqCst) {
            4..=6 => return Err(Error::Closed),
            0 => {}
            _ => return Err(Error::Busy),
        }
        // Abort is only a retirement REQUEST. Reserve before retaining any new
        // normalized options/session/queue/future; old generation stays charged.
        let generation = generation::acquire()?;
        budget::normalize_options(&mut options);
        {
            let mut q = self.events.lock().map_err(|_| Error::Internal)?;
            if !q.items.is_empty() || q.fatal.is_some() {
                return Err(Error::Busy);
            }
            q.terminated = false;
            q.disconnects.clear();
            q.halt_reconnect = false;
            q.recovery_pending = false;
            q.expectations.clear();
            q.limit_ws_expectations = options.use_websocket;
        }
        self.op(op)?;
        if let Some(task) = self.task.take() {
            task.abort();
        }
        // Build (and drop) a session now so TLS/options errors stay synchronous
        // on connect, matching the previous admission behavior.
        drop(new_session(&options)?);
        let (tx, rx) = mpsc::channel(32);
        self.tx = Some(tx);
        self.state.store(1, Ordering::SeqCst);
        self.stop = Arc::new(Notify::new());
        let stop = self.stop.clone();
        let queue = self.events.clone();
        let state = self.state.clone();
        let drain_wake = self.drain_wake.clone();
        self.task = Some(self.runtime.spawn(generation.own(async move {
            #[cfg(feature = "ws-test-observer")]
            let _probe = ws_test_observer::Actor::new();
            if let Err((err, operation)) =
                supervise(options, rx, &queue, &state, (&stop, &drain_wake), op).await
            {
                if let Ok(mut q) = queue.lock() {
                    // Commit failure and state under the admission/drain lock;
                    // otherwise an idle drain could claim success in between.
                    for old in [1, 2, 3, 7] {
                        let _ = state.compare_exchange(old, 0, Ordering::SeqCst, Ordering::SeqCst);
                    }
                    if q.drain == DrainStatus::Pending {
                        q.drain = if err == Error::Timeout {
                            DrainStatus::TimedOut
                        } else {
                            DrainStatus::Failed
                        };
                        state.store(6, Ordering::SeqCst);
                    }
                    q.fail(err, operation);
                }
            }
        })));
        Ok(())
    }
    fn submit(&self, cmd: Command) -> Result<(), Error> {
        // Same lock as begin_drain: no accepted command can cross its cutoff.
        let mut q = self.events.lock().map_err(|_| Error::Internal)?;
        match self.state.load(Ordering::SeqCst) {
            4..=6 => return Err(Error::Closed),
            2 => {}
            _ => return Err(Error::Busy),
        }
        if let Command::Subscribe(_, topic, _) = &cmd {
            if q.limit_ws_expectations && !q.expectations.contains_key(topic) {
                let total = q
                    .expectations
                    .keys()
                    .try_fold(topic.len(), |sum, key| sum.checked_add(key.len()));
                if q.expectations.len() >= WS_EXPECTATION_TOPICS
                    || total.is_none_or(|bytes| bytes > WS_EXPECTATION_TOPIC_BYTES)
                {
                    return Err(Error::QueueFull);
                }
            }
        }
        let tx = self.tx.as_ref().ok_or(Error::Closed)?;
        let permit = tx.try_reserve().map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::QueueFull,
            _ => Error::Closed,
        })?;
        self.op(cmd.op())?;
        let cmd = match cmd {
            Command::Publish(o, t, p, q, r) => Command::Publish(
                o,
                t.into_boxed_str().into_string(),
                p.into_boxed_slice().into_vec(),
                q,
                r,
            ),
            Command::Subscribe(o, t, q) => {
                Command::Subscribe(o, t.into_boxed_str().into_string(), q)
            }
            Command::Unsubscribe(o, t) => Command::Unsubscribe(o, t.into_boxed_str().into_string()),
            c => c,
        };
        match &cmd {
            Command::Subscribe(_, topic, qos) => {
                q.expectations.insert(topic.clone(), *qos);
            }
            Command::Unsubscribe(_, topic) => {
                q.expectations.remove(topic);
            }
            _ => {}
        }
        if let Command::Publish(op, _, _, qos, _) = &cmd {
            let mut e = Event::new(Kind::Queued, *op);
            e.qos = *qos;
            if let Err(error) = q.push(e) {
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
    /// Coalesce concurrent disconnect requests without sending multiple packets.
    /// Each accepted (monotonic) operation gets a completion or terminal failure.
    pub fn disconnect(&self, op: u64) -> Result<(), Error> {
        let mut q = self.events.lock().map_err(|_| Error::Internal)?;
        match self.state.load(Ordering::SeqCst) {
            4..=6 => Err(Error::Closed),
            0 => {
                self.op(op)?;
                q.push(Event::new(Kind::Disconnected, op))
            }
            2 => {
                let tx = self.tx.as_ref().ok_or(Error::Closed)?;
                let permit = tx.try_reserve().map_err(|e| match e {
                    mpsc::error::TrySendError::Full(_) => Error::QueueFull,
                    _ => Error::Closed,
                })?;
                self.op(op)?;
                q.disconnects.push(op);
                self.state.store(3, Ordering::SeqCst);
                permit.send(Command::Disconnect(op));
                Ok(())
            }
            3 => {
                if q.disconnects.len() >= 32 {
                    return Err(Error::QueueFull);
                }
                self.op(op)?;
                q.disconnects.push(op);
                Ok(())
            }
            7 => {
                self.op(op)?;
                q.halt_reconnect = true;
                self.state.store(0, Ordering::SeqCst);
                let result = q.push(Event::new(Kind::Disconnected, op));
                self.drain_wake.notify_one();
                result
            }
            _ => Err(Error::Busy),
        }
    }
    /// Begin an irreversible, bounded drain. This never blocks a runtime worker.
    /// First accepted deadline wins. New work is Closed from this call's cutoff.
    /// Connecting/disconnecting clients return Busy without changing admission.
    pub fn begin_drain(&self, timeout: Duration) -> Result<(), Error> {
        if timeout < Duration::from_millis(1)
            || timeout > Duration::from_secs(300)
            || !timeout.subsec_nanos().is_multiple_of(1_000_000)
        {
            return Err(Error::InvalidArgument);
        }
        let mut q = self.events.lock().map_err(|_| Error::Internal)?;
        if q.drain != DrainStatus::NotStarted {
            return Ok(());
        }
        let status = match self.state.load(Ordering::SeqCst) {
            0 | 7 => {
                if q.terminated || q.recovery_pending {
                    DrainStatus::Failed
                } else {
                    DrainStatus::Drained
                }
            }
            2 => DrainStatus::Pending,
            4..=6 => return Err(Error::Closed),
            _ => return Err(Error::Busy),
        };
        if self.state.load(Ordering::SeqCst) == 7 {
            q.halt_reconnect = true;
        }
        q.drain = status;
        q.deadline = Some(Instant::now() + timeout);
        // Inbound delivery is outside the drain contract; preserve control events.
        q.items.retain(|e| e.kind != Kind::Message);
        self.state.store(
            if status == DrainStatus::Pending { 5 } else { 6 },
            Ordering::SeqCst,
        );
        self.drain_wake.notify_one();
        Ok(())
    }
    pub fn drain_status(&self) -> Result<DrainStatus, Error> {
        Ok(self.events.lock().map_err(|_| Error::Internal)?.drain)
    }
    /// Force-stop prototype close: no callbacks and no draining guarantee yet.
    pub fn close(&mut self) -> Result<(), Error> {
        #[cfg(feature = "ws-test-observer")]
        ws_test_observer::closing();
        let mut q = self.events.lock().map_err(|_| Error::Internal)?;
        if q.drain == DrainStatus::Pending {
            q.drain = DrainStatus::Forced;
        }
        q.seal();
        self.state.store(4, Ordering::SeqCst);
        drop(q);
        self.tx = None;
        self.stop.notify_one();
        if let Some(t) = self.task.take() {
            t.abort();
        }
        Ok(())
    }
    pub fn peek(&self) -> Result<Event, Error> {
        let q = self.events.lock().map_err(|_| Error::Internal)?;
        if q.sealed || self.state.load(Ordering::SeqCst) == 4 {
            return Err(Error::Closed);
        }
        q.items
            .front()
            .or(q.fatal.as_ref())
            .cloned()
            .ok_or(Error::Empty)
    }
    pub fn pop(&self) -> Result<Event, Error> {
        let mut q = self.events.lock().map_err(|_| Error::Internal)?;
        if q.sealed || self.state.load(Ordering::SeqCst) == 4 {
            return Err(Error::Closed);
        }
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
        2 => Ok(QoS::ExactlyOnce),
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

fn new_session(options: &Options) -> Result<(AsyncClient, engine::Engine), Error> {
    engine::session(options)
}

fn halt_requested(queue: &Queue, state: &AtomicU8) -> Result<bool, (Error, u64)> {
    let q = queue.lock().map_err(|_| (Error::Internal, 0))?;
    if q.sealed || q.halt_reconnect || q.drain != DrainStatus::NotStarted {
        return Ok(true);
    }
    Ok(matches!(state.load(Ordering::SeqCst), 0 | 3 | 4 | 5 | 6))
}

async fn backoff(
    policy: &ReconnectPolicy,
    attempt: u32,
    queue: &Queue,
    state: &AtomicU8,
    signals: (&Notify, &Notify),
) -> Result<bool, (Error, u64)> {
    let (stop, drain_wake) = signals;
    let delay = reconnect::jittered(policy, attempt);
    let end = Instant::now() + delay;
    loop {
        if halt_requested(queue, state)? {
            return Ok(true);
        }
        let now = Instant::now();
        if now >= end {
            return Ok(false);
        }
        tokio::select! {
            biased;
            _ = stop.notified() => return Err((Error::Overflow, 0)),
            _ = drain_wake.notified() => {}
            _ = tokio::time::sleep_until(end) => return halt_requested(queue, state),
        }
    }
}

fn snapshot_expectations(queue: &Queue) -> Result<Vec<(String, u8)>, (Error, u64)> {
    let q = queue.lock().map_err(|_| (Error::Internal, 0))?;
    Ok(q.expectations
        .iter()
        .map(|(t, qos)| (t.clone(), *qos))
        .collect())
}

async fn supervise(
    options: Options,
    mut commands: mpsc::Receiver<Command>,
    queue: &Queue,
    state: &AtomicU8,
    signals: (&Notify, &Notify),
    connect_op: u64,
) -> Result<(), (Error, u64)> {
    let (stop, drain_wake) = signals;
    let (mut client, mut engine) = new_session(&options).map_err(|e| (e, connect_op))?;
    let mut active = None;
    let mut packet_id = 0;
    match run(EngineSession {
        client: &client,
        engine: &mut engine,
        active: &mut active,
        packet_id: &mut packet_id,
        commands: &mut commands,
        queue,
        state,
        signals: (stop, drain_wake),
        connect_op,
        is_reconnect: false,
        clean_session: options.clean_session,
    })
    .await
    {
        Ok(()) => return Ok(()),
        Err((err, operation, false)) => return Err((err, operation)),
        Err((err, operation, true)) => {
            if !options.reconnect.enabled
                || !engine.safe_to_retry()
                || halt_requested(queue, state)?
                || !matches!(state.load(Ordering::SeqCst), 2 | 7)
            {
                return Err((err, operation));
            }
            emit(queue, Event::new(Kind::Disconnected, 0)).map_err(|e| (e, 0))?;
        }
    }
    let mut attempts = 0u32;
    loop {
        attempts = attempts.saturating_add(1);
        if attempts > options.reconnect.max_attempts {
            let _ = state.compare_exchange(2, 0, Ordering::SeqCst, Ordering::SeqCst);
            let _ = state.compare_exchange(7, 0, Ordering::SeqCst, Ordering::SeqCst);
            emit(queue, Event::new(Kind::Disconnected, 0)).map_err(|e| (e, 0))?;
            return Ok(());
        }
        if halt_requested(queue, state)? {
            return Ok(());
        }
        {
            // Same admission mutex as drain/close/disconnect: never resurrect a
            // closed handle between the halt check and reconnect announcement.
            let mut q = queue.lock().map_err(|_| (Error::Internal, 0))?;
            if q.sealed
                || q.halt_reconnect
                || q.drain != DrainStatus::NotStarted
                || !matches!(state.load(Ordering::SeqCst), 2 | 7)
            {
                return Ok(());
            }
            state.store(7, Ordering::SeqCst);
            q.push(Event::new(Kind::Reconnecting, 0))
                .map_err(|e| (e, 0))?;
        }
        if backoff(&options.reconnect, attempts, queue, state, signals).await? {
            return Ok(());
        }
        if engine.is_v5() {
            (client, engine) = new_session(&options).map_err(|e| (e, 0))?;
        }
        let recovering_operation = active.as_ref().map(Command::op);
        match run(EngineSession {
            client: &client,
            engine: &mut engine,
            active: &mut active,
            packet_id: &mut packet_id,
            commands: &mut commands,
            queue,
            state,
            signals: (stop, drain_wake),
            connect_op,
            is_reconnect: true,
            clean_session: options.clean_session,
        })
        .await
        {
            Ok(()) => return Ok(()),
            Err((err, operation, established)) => {
                if halt_requested(queue, state)?
                    || !engine.safe_to_retry()
                    || !options.reconnect.enabled
                    || matches!(state.load(Ordering::SeqCst), 1 | 3..=6)
                {
                    return Err((err, operation));
                }
                if established {
                    emit(queue, Event::new(Kind::Disconnected, 0)).map_err(|e| (e, 0))?;
                    // CONNACK alone cannot grant one interrupted publish an
                    // infinite replay budget. A different/completed operation
                    // starts a later outage with the normal fresh budget.
                    if active.is_none() || active.as_ref().map(Command::op) != recovering_operation
                    {
                        attempts = 0;
                    }
                    continue;
                }
            }
        }
    }
}

struct EngineSession<'a> {
    client: &'a AsyncClient,
    engine: &'a mut engine::Engine,
    active: &'a mut Option<Command>,
    packet_id: &'a mut u16,
    commands: &'a mut mpsc::Receiver<Command>,
    queue: &'a Queue,
    state: &'a AtomicU8,
    signals: (&'a Notify, &'a Notify),
    connect_op: u64,
    is_reconnect: bool,
    clean_session: bool,
}
struct EventCtx<'a> {
    active: &'a mut Option<Command>,
    packet_id: &'a mut u16,
    queue: &'a Queue,
    state: &'a AtomicU8,
    connect_op: u64,
    client: &'a AsyncClient,
    is_reconnect: bool,
    clean_session: bool,
    resub: &'a mut VecDeque<(String, u8)>,
    resub_active: &'a mut Option<(String, u8)>,
    resub_pending: &'a mut Option<usize>,
}

// Linearize the error cutoff with submit/drain/close. A command cannot be
// accepted between testing the channel and committing reconnecting state.
fn v5_recoverable(
    queue: &Queue,
    state: &AtomicU8,
    commands: &mpsc::Receiver<Command>,
    active: bool,
    error: Error,
) -> Result<bool, Error> {
    let _q = queue.lock().map_err(|_| Error::Internal)?;
    let recoverable =
        !active && commands.is_empty() && !matches!(error, Error::Refused | Error::Unsupported);
    // Pending terminal failure fences admission in state 1 until the supervisor
    // commits Failure under this same mutex. State 7 would let a racing drain
    // report Drained before accepted work's failure was committed.
    let _ = state.compare_exchange(
        2,
        if recoverable { 7 } else { 1 },
        Ordering::SeqCst,
        Ordering::SeqCst,
    );
    Ok(recoverable)
}

async fn run(session: EngineSession<'_>) -> Result<(), (Error, u64, bool)> {
    let EngineSession {
        client,
        engine,
        active,
        packet_id,
        commands,
        queue,
        state,
        signals,
        connect_op,
        is_reconnect,
        clean_session,
    } = session;
    let (stop, drain_wake) = signals;
    let mut resub: VecDeque<(String, u8)> = VecDeque::new();
    let mut resub_active: Option<(String, u8)> = None;
    let mut resub_pending: Option<usize> = None;
    let mut established = false;
    loop {
        if state.load(Ordering::SeqCst) == 2 {
            established = true;
        }
        if is_reconnect
            && !established
            && halt_requested(queue, state).map_err(|(e, op)| (e, op, false))?
        {
            return Ok(());
        }
        let deadline = {
            let q = queue
                .lock()
                .map_err(|_| (Error::Internal, 0, established))?;
            q.deadline
        };
        if deadline.is_some()
            && active.is_none()
            && resub_active.is_none()
            && resub.is_empty()
            && commands.is_empty()
        {
            client
                .try_disconnect()
                .map_err(|_| (Error::QueueFull, 0, established))?;
            *active = Some(Command::Disconnect(0));
        }
        if active.is_none()
            && resub_active.is_none()
            && state.load(Ordering::SeqCst) == 2
            && deadline.is_none()
        {
            if let Some((topic, qos)) = resub.pop_front() {
                client
                    .try_subscribe(&topic, qos_value(qos).map_err(|e| (e, 0, established))?)
                    .map_err(|_| (Error::QueueFull, 0, established))?;
                resub_active = Some((topic, qos));
                *packet_id = 0;
            } else if resub_pending == Some(0) {
                emit(queue, Event::new(Kind::SubscriptionsResubscribed, 0))
                    .map_err(|e| (e, 0, established))?;
                resub_pending = None;
            }
        }
        tokio::select! {
            biased;
            _ = async {
                match deadline {
                    Some(end) => tokio::time::sleep_until(end).await,
                    None => std::future::pending::<()>().await,
                }
            } => return Err((Error::Timeout, 0, established)),
            _ = drain_wake.notified() => continue,
            _ = stop.notified() => return Err((Error::Overflow, 0, established)),
            command = commands.recv(), if active.is_none() && resub_active.is_none() && resub.is_empty() && matches!(state.load(Ordering::SeqCst), 2 | 3 | 5) => {
                let Some(cmd) = command else { return Ok(()); };
                let op = cmd.op();
                let result = match &cmd {
                    Command::Publish(_, t, p, q, r) => client.try_publish(t, qos_value(*q).map_err(|e| (e, op, established))?, *r, p.clone()),
                    Command::Subscribe(_, t, q) => client.try_subscribe(t, qos_value(*q).map_err(|e| (e, op, established))?),
                    Command::Unsubscribe(_, t) => client.try_unsubscribe(t),
                    Command::Disconnect(_) => client.try_disconnect(),
                };
                result.map_err(|_| (Error::QueueFull, op, established))?;
                *active = Some(cmd);
                *packet_id = 0;
            },
            event = engine.poll() => {
                let event = event.map_err(|e| {
                    // v5 has no promised in-process QoS recovery. With accepted
                    // work or an explicit negative ACK, fail terminally rather
                    // than hiding the error behind reconnect and stranding waits.
                    let recoverable = if engine.is_v5() {
                        v5_recoverable(queue, state, commands, active.is_some() || resub_active.is_some(), e).unwrap_or(false)
                    } else {
                        (|| {
                            let mut q = queue.lock().map_err(|_| Error::Internal)?;
                            // Controls are not publish ownership. Settle the original
                            // waiter with an explicit operation error, retaining its
                            // admitted expectation for broker-session reconciliation.
                            let failed_control = match active.as_ref() {
                                Some(Command::Subscribe(op, _, _)) => Some((Kind::Subscribed, *op)),
                                Some(Command::Unsubscribe(op, _)) => Some((Kind::Unsubscribed, *op)),
                                _ => None,
                            };
                            if let Some((kind, op)) = failed_control {
                                let mut event = Event::new(kind, op);
                                event.code = Error::Network as i32;
                                q.push(event)?;
                                *active = None;
                                *packet_id = 0;
                            }
                            let recoverable = engine.prepare_resume(active, clean_session).is_ok();
                            q.recovery_pending = active.is_some() || !commands.is_empty();
                            for old in [2, 7] {
                                let _ = state.compare_exchange(old, if recoverable { 7 } else { 1 }, Ordering::SeqCst, Ordering::SeqCst);
                            }
                            Ok::<bool, Error>(recoverable)
                        })().unwrap_or(false)
                    };
                    (e, active.as_ref().map(Command::op).unwrap_or(if is_reconnect { 0 } else { connect_op }), established && recoverable)
                })?;
                let is_connack = matches!(&event, EngineEvent::Incoming(Packet::ConnAck(_)));
                // A new broker session cannot own the interrupted packet identifier.
                // Fail the accepted work explicitly; never silently discard/replay it.
                if matches!(&event, EngineEvent::Incoming(Packet::ConnAck(a)) if is_reconnect && (clean_session || !a.session_present)) && active.is_some() {
                    let _q = queue.lock().map_err(|_| (Error::Internal, 0, false))?;
                    let _ = state.compare_exchange(7, 1, Ordering::SeqCst, Ordering::SeqCst);
                    return Err((Error::Network, active.as_ref().map_or(0, Command::op), false));
                }
                if process_event(
                    event,
                    EventCtx {
                        active,
                        packet_id,
                        queue,
                        state,
                        connect_op,
                        client,
                        is_reconnect,
                        clean_session,
                        resub: &mut resub,
                        resub_active: &mut resub_active,
                        resub_pending: &mut resub_pending,
                    },
                )
                .map_err(|(e, op)| (e, op, established && !engine.is_v5()))?
                {
                    return Ok(());
                }
                if is_connack { established = true; }
            }
        }
    }
}

fn process_event(event: EngineEvent, ctx: EventCtx<'_>) -> Result<bool, (Error, u64)> {
    let EventCtx {
        active,
        packet_id,
        queue,
        state,
        connect_op,
        client,
        is_reconnect,
        clean_session,
        resub,
        resub_active,
        resub_pending,
    } = ctx;
    match event {
        EngineEvent::Incoming(Packet::ConnAck(ack)) => {
            let expected = if is_reconnect { 7 } else { 1 };
            {
                let mut q = queue.lock().map_err(|_| (Error::Internal, connect_op))?;
                state
                    .compare_exchange(expected, 2, Ordering::SeqCst, Ordering::SeqCst)
                    .map_err(|_| (Error::Closed, connect_op))?;
                q.recovery_pending = false;
            }
            if is_reconnect {
                let mut e = Event::new(Kind::Reconnected, 0);
                e.session_present = ack.session_present;
                emit(queue, e).map_err(|e| (e, 0))?;
                if clean_session || !ack.session_present {
                    let set = snapshot_expectations(queue)?;
                    *resub_pending = Some(set.len());
                    resub.extend(set);
                    if resub_pending == &Some(0) {
                        emit(queue, Event::new(Kind::SubscriptionsResubscribed, 0))
                            .map_err(|e| (e, 0))?;
                        *resub_pending = None;
                    }
                } else {
                    emit(queue, Event::new(Kind::SubscriptionsAligned, 0)).map_err(|e| (e, 0))?;
                }
            } else {
                let mut e = Event::new(Kind::Connected, connect_op);
                e.session_present = ack.session_present;
                emit(queue, e).map_err(|e| (e, connect_op))?;
            }
        }
        EngineEvent::Outgoing(Outgoing::Publish(id)) => {
            if let Some(Command::Publish(op, _, _, qos, _)) = active {
                if *packet_id != 0 {
                    if *packet_id != id {
                        return Err((Error::Internal, *op));
                    }
                    return Ok(false);
                }
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
        EngineEvent::Incoming(Packet::PubComp(ack)) => {
            if let Some(Command::Publish(op, _, _, 2, _)) = active {
                if ack.pkid == *packet_id {
                    let mut e = Event::new(Kind::Acknowledged, *op);
                    e.packet_id = *packet_id;
                    e.qos = 2;
                    emit(queue, e).map_err(|e| (e, *op))?;
                    *active = None;
                }
            }
        }
        EngineEvent::Incoming(Packet::PubAck(ack)) => {
            if let Some(Command::Publish(op, _, _, qos, _)) = active {
                if *qos == 1 && ack.pkid == *packet_id {
                    let mut e = Event::new(Kind::Acknowledged, *op);
                    e.packet_id = *packet_id;
                    e.qos = *qos;
                    emit(queue, e).map_err(|e| (e, *op))?;
                    *active = None;
                }
            }
        }
        EngineEvent::Incoming(Packet::SubAck(ack)) => {
            if let Some((topic, qos)) = resub_active.take() {
                if ack.pkid == *packet_id {
                    let mut e = Event::new(Kind::Subscribed, 0);
                    e.packet_id = *packet_id;
                    e.qos = qos;
                    e.topic = topic.into_bytes();
                    if ack
                        .return_codes
                        .iter()
                        .any(|c| matches!(c, rumqttc::SubscribeReasonCode::Failure))
                    {
                        e.code = Error::Refused as i32;
                    }
                    emit(queue, e).map_err(|e| (e, 0))?;
                    if let Some(left) = resub_pending.as_mut() {
                        *left = left.saturating_sub(1);
                    }
                } else {
                    *resub_active = Some((topic, qos));
                }
            } else if let Some(Command::Subscribe(op, _, _)) = active {
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
            // Drain does not accept inbound messages or ACK discarded QoS1.
            let mut q = queue.lock().map_err(|_| (Error::Internal, 0))?;
            if q.drain != DrainStatus::NotStarted {
                return Ok(false);
            }
            if p.payload.len() > MAX_PAYLOAD {
                return Err((Error::Overflow, 0));
            }
            let mut e = Event::new(Kind::Message, 0);
            e.topic = p.topic.as_bytes().to_vec();
            e.payload = p.payload.to_vec();
            e.qos = p.qos as u8;
            e.packet_id = p.pkid;
            q.push(e).map_err(|e| (e, 0))?;
            client.try_ack(&p).map_err(|_| (Error::QueueFull, 0))?;
        }
        EngineEvent::Outgoing(Outgoing::Disconnect) => {
            let op = active.as_ref().map_or(0, Command::op);
            let mut q = queue.lock().map_err(|_| (Error::Internal, op))?;
            if q.sealed {
                return Err((Error::Closed, op));
            }
            if q.drain == DrainStatus::Pending {
                q.drain = DrainStatus::Drained;
                state.store(6, Ordering::SeqCst);
                return Ok(true);
            }
            for id in std::mem::take(&mut q.disconnects) {
                q.push(Event::new(Kind::Disconnected, id))
                    .map_err(|e| (e, id))?;
            }
            let _ = state.compare_exchange(3, 0, Ordering::SeqCst, Ordering::SeqCst);
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
    fn v5_error_seals_admission_before_reconnect_decision() {
        let q = Queue::default();
        let state = AtomicU8::new(2);
        let (tx, rx) = mpsc::channel(4);
        tx.try_send(Command::Unsubscribe(2, "t".into())).unwrap();
        assert!(!v5_recoverable(&q, &state, &rx, false, Error::Network).unwrap());
        assert_eq!(state.load(Ordering::SeqCst), 1);
        drop(rx);
        let (_, rx) = mpsc::channel(4);
        state.store(2, Ordering::SeqCst);
        assert!(v5_recoverable(&q, &state, &rx, false, Error::Network).unwrap());
        assert_eq!(state.load(Ordering::SeqCst), 7);
    }
    #[test]
    fn reject_oversized_inbound_before_queue_or_ack() {
        let (client, _) = new_session(&Options::new("localhost", "test", 1883)).unwrap();
        for (qos, size, expected) in [
            (QoS::ExactlyOnce, MAX_PAYLOAD + 1, Error::Overflow),
            (QoS::AtMostOnce, MAX_PAYLOAD + 1, Error::Overflow),
        ] {
            let mut p = rumqttc::Publish::new("x", qos, vec![0; size]);
            p.pkid = 1;
            let q = Queue::default();
            let result = process_event(
                EngineEvent::Incoming(Packet::Publish(p)),
                EventCtx {
                    active: &mut None,
                    packet_id: &mut 0,
                    queue: &q,
                    state: &AtomicU8::new(2),
                    connect_op: 1,
                    client: &client,
                    is_reconnect: false,
                    clean_session: true,
                    resub: &mut VecDeque::new(),
                    resub_active: &mut None,
                    resub_pending: &mut None,
                },
            );
            assert_eq!(result, Err((expected, 0)));
            assert!(q.lock().unwrap().items.is_empty());
        }
    }
}

#[cfg(test)]
mod close_seal_tests {
    use super::*;
    use mqtt_api::Error;

    fn with_runtime<T>(f: impl FnOnce() -> T) -> T {
        mqtt_runtime::init().unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        let _ = mqtt_runtime::shutdown();
        match result {
            Ok(v) => v,
            Err(p) => std::panic::resume_unwind(p),
        }
    }

    #[test]
    fn peek_and_pop_return_closed_after_empty_close() {
        with_runtime(|| {
            let mut c = Client::new().unwrap();
            assert_eq!(c.peek().unwrap_err(), Error::Empty);
            c.close().unwrap();
            c.close().unwrap();
            assert_eq!(c.peek().unwrap_err(), Error::Closed);
            assert_eq!(c.pop().unwrap_err(), Error::Closed);
        });
    }

    #[test]
    fn close_discards_queued_events_and_rejects_late_producers() {
        with_runtime(|| {
            let mut c = Client::new().unwrap();
            let mut message = Event::new(Kind::Message, 1);
            message.topic = b"x".to_vec();
            message.payload = b"!".to_vec();
            emit(&c.events, message).unwrap();
            assert_eq!(c.peek().unwrap().kind, Kind::Message);
            c.close().unwrap();
            assert_eq!(c.peek().unwrap_err(), Error::Closed);
            assert_eq!(c.pop().unwrap_err(), Error::Closed);
            assert_eq!(
                emit(&c.events, Event::new(Kind::Message, 2)).unwrap_err(),
                Error::Closed
            );
            assert_eq!(c.peek().unwrap_err(), Error::Closed);
            assert_eq!(
                c.connect(1, Options::new("127.0.0.1", "closed", 1883))
                    .unwrap_err(),
                Error::Closed
            );
        });
    }
    #[test]
    fn drain_drops_inbound_but_retains_control_until_force_close() {
        with_runtime(|| {
            let mut c = Client::new().unwrap();
            emit(&c.events, Event::new(Kind::Message, 0)).unwrap();
            emit(&c.events, Event::new(Kind::Acknowledged, 2)).unwrap();
            c.begin_drain(Duration::from_secs(1)).unwrap();
            assert_eq!(c.pop().unwrap().kind, Kind::Acknowledged);
            emit(&c.events, Event::new(Kind::Message, 0)).unwrap();
            assert_eq!(c.peek().unwrap_err(), Error::Empty);
            c.close().unwrap();
            assert_eq!(c.peek().unwrap_err(), Error::Closed);
        });
    }
}

#[cfg(test)]
mod recovery_cutoff_tests {
    use super::*;
    #[tokio::test]
    async fn halted_reconnect_never_polls_engine_or_opens_socket() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let options = Options::new("127.0.0.1", "cutoff", listener.local_addr().unwrap().port());
        let (client, mut engine) = new_session(&options).unwrap();
        let (_tx, mut commands) = mpsc::channel(32);
        let queue: Queue = Arc::default();
        {
            let mut q = queue.lock().unwrap();
            q.halt_reconnect = true;
            q.drain = DrainStatus::Failed;
        }
        let state = AtomicU8::new(6);
        let stop = Notify::new();
        let wake = Notify::new();
        let mut active = Some(Command::Publish(2, "x".into(), vec![1], 1, false));
        let mut packet_id = 1;
        tokio::time::timeout(
            Duration::from_millis(100),
            run(EngineSession {
                client: &client,
                engine: &mut engine,
                active: &mut active,
                packet_id: &mut packet_id,
                commands: &mut commands,
                queue: &queue,
                state: &state,
                signals: (&stop, &wake),
                connect_op: 1,
                is_reconnect: true,
                clean_session: false,
            }),
        )
        .await
        .expect("halted reconnect must return without network activity")
        .unwrap();
        assert!(tokio::time::timeout(Duration::ZERO, listener.accept())
            .await
            .is_err());
    }
}

#[cfg(test)]
mod ws_resource_tests {
    use super::*;
    #[test]
    fn seal_releases_retained_subscription_metadata() {
        let mut q = Events::default();
        q.expectations.insert("retained".repeat(100), 1);
        q.seal();
        assert!(
            q.expectations.is_empty(),
            "close retained subscription metadata"
        );
    }
}

// Candidate is deliberately not reachable by public clients until lifecycle gates pass.
mod owned_driver;

#[cfg(test)]
mod retained_command_tests {
    use super::*;
    #[test]
    fn admitted_commands_discard_excess_input_capacity() {
        mqtt_runtime::init().unwrap();
        let mut c = Client::new().unwrap();
        let (tx, mut rx) = mpsc::channel(32);
        c.tx = Some(tx);
        c.state.store(2, Ordering::SeqCst);
        let mut t = String::with_capacity(4 * 1024 * 1024);
        t.push('t');
        let mut p = Vec::with_capacity(4 * 1024 * 1024);
        p.extend([0, 255]);
        c.publish(1, t, p, 1, false).unwrap();
        let Command::Publish(_, t, p, _, _) = rx.try_recv().unwrap() else {
            panic!("publish")
        };
        assert_eq!(t.capacity(), 1);
        assert_eq!(p.capacity(), 2);
        c.close().unwrap();
        drop(c);
        let _ = mqtt_runtime::shutdown();
    }
}
