> Current integration: the owned Rust production driver supersedes the previous
> EventLoop adapter discussed below. QoS2 publish/subscribe are implemented;
> Will QoS2 remains Unsupported. See [current matrix](qos2-readiness.md),
> [driver](owned-driver.md) and [capacity audit](websocket-memory.md). Historical
> statements about unsupported QoS2 or inaccessible WS constructors below describe
> the previous adapter, not the current production route.

# Optional bounded graceful close (Linux host)

`close()` / `mqtt_client_close` remain immediate **force-close**: stop polling,
abort transport, discard queued events, reject unfinished waits, release as before.
They never deliver queued old messages or secretly wait for graceful completion.
`disconnect()` remains the existing reusable disconnect, not this permanent close.

## Additive API

Dart (the `MqttDrainClose` extension is included by `mqtt_flutter.dart`):

```dart
final operation = client.beginDrainClose(timeout: const Duration(seconds: 5));
// Or: final result = await client.drainClose(timeout: const Duration(seconds: 5));
final MqttDrainResult result = await operation.future;
```

`MqttDrainResult` is `drained`, `timedOut`, `failed`, `forced`, or `cancelled`.
`operation.cancel()` (or `client.cancel(operation.operationId)` while pending)
returns **cancelled for only that waiter**. It does not retract a packet, cancel
native drain, remove the shared deadline, or reopen admission. The physical drain
continues even if every waiter cancels. At most 32 outstanding Dart drain waiters;
excess callers get `MqttException(5)`. Cancel removes the waiter without retaining
an unbounded join continuation. Repeats after completion get the cached result.
Rejected starts (invalid duration, closed-without-drain, connecting/disconnecting)
use existing ArgumentError / StateError / MqttException conventions, not a success
result. Existing operation Futures keep their own deadlines and cancellation rules.

Rust: `Client::begin_drain(Duration) -> Result<(), Error>` starts or joins;
`Client::drain_status() -> Result<DrainStatus, Error>` is nonblocking.
C: `mqtt_client_drain_begin(uint64_t handle, uint32_t timeout_ms)` and
`mqtt_client_drain_status(uint64_t handle, uint32_t *out_status)` return ordinary
ABI error codes; `out_status` is a **separate drain-status domain**:

| Value | Rust status | Meaning |
| --- | --- | --- |
| 0 | NotStarted | no drain was accepted |
| 1 | Pending | irreversible draining attempt is active |
| 2 | Drained | all accepted operations reached their boundary, then Outgoing Disconnect |
| 3 | TimedOut | native deadline; actor/transport stopped |
| 4 | Failed | network/TLS/protocol/queue failure; actor/transport stopped |
| 5 | Forced | force-close won while drain was Pending |

A C status error leaves output unchanged. Null output is InvalidArgument. Status
can be read after force-close until free; stale/freed handles return Closed.
Detailed native failures still use the existing Failure event/code (`Network`,
`Overflow`, `Timeout` etc.); `Failed` deliberately does not claim successful work.
The old event layout, function signatures, and error code values are unchanged.

## Cutoff and completion boundaries

- Timeout is required, **whole milliseconds 1..300000** (maximum five minutes).
  Invalid starts do not change admission. First accepted caller fixes the deadline;
  later valid starts join it, even when callers request a different timeout.
- Only connected or disconnected idle clients can start. Connecting/disconnecting
  returns Busy and leaves the existing attempt alone. A never-connected idle Dart
  client closes successfully without creating a native handle.
- Native drain acceptance and command admission serialize under the event mutex
  (and the existing C handle-table mutex for C). That acceptance is the cutoff:
  no new connect/publish/subscribe/unsubscribe/disconnect is accepted afterward.
  Native valid operations return Closed; Dart new work rejects with StateError.
  Input validation may still precede native admission validation.
- Drain covers the already accepted FIFO of native outgoing commands, including
  work whose Dart waiter has timed out or been cancelled: QoS0 reaches **Sent**,
  QoS1 reaches **PUBACK**, subscribe/unsubscribe reaches **SUBACK/UNSUBACK**.
  Broker refusal remains an operation error, but is a completed protocol outcome.
  Drained is not an assertion that every operation succeeded.
- After this FIFO is complete, send one DISCONNECT and wait for the engine's
  Outgoing Disconnect event. This is **not** proof that the broker received
  DISCONNECT. PUBACK is broker protocol acknowledgement, not subscriber receipt,
  application processing, persistence, or exactly-once delivery.
- Drain is out-of-band, so a full 32-slot command queue cannot prevent its start.
  It adds one deadline/status record, not an unbounded native waiter queue.

## Incoming messages, polling, and terminal results

Inbound application messages are **not** part of drain. At the cutoff, discard
queued native Message events, retain existing control/progress completions, and
suppress later inbound delivery. Incoming QoS1 discarded during drain is not newly
manually ACKed; an ACK already accepted before the cutoff can still be sent.
Already delivered application objects are unaffected.

Native terminal drain stops the actor/transport permanently and rejects work, but
keeps the numeric handle/runtime lease and retained control events alive for the
caller to poll. After those events, poll returns Empty until explicit close/free.
This differs deliberately from force-close, which immediately seals poll as Closed
and discards everything. Rust/C callers must still close/drop/free resources.

Dart continues bounded polling (64 events per turn), **even if streams pause**
during drain, suppresses incoming delivery, settles retained control completions,
and then performs the usual close/free/buffer release and closes streams. The
finite outstanding control/progress notifications may buffer for paused listeners;
no new commands are accepted. Existing 32 commands + one active, 128 events plus
fatal slot, and 16 listeners per stream bounds remain. Drain has its own native
deadline; it never waits on Dart application consumption, callbacks, or a worker
`block_on`. A Dart timer keeps the explicit draining client reachable until it
finishes; ordinary weak idle polling/finalizer behavior is unchanged. As with any
Dart API, a blocked Dart event loop can delay Future notification/release, not the
native transport deadline; this is not a hard realtime scheduling guarantee.

Deadline, failure, or explicit force-close settles at most once. Force-close can
change Pending to Forced, never rewrite a committed native terminal result. It
still discards undelivered completions: callers choosing force-close accept the
existing failure semantics for those pending Futures. Cancelling a waiter is local
and never overwrites another caller's result. An owner-isolate exit/finalizer uses
immediate force-close/free and never secretly drains; no Dart Future can be returned
to a dead isolate. Surviving raw C observers get Closed after that handle is freed.

No disk outbox, QoS2, MQTT5, new TLS bypass, or mobile verification is
introduced. Keepalive remains default zero. Automatic reconnect stays off unless
explicitly enabled; a drain, force-close, or explicit disconnect in progress
does not start or continue a reconnect loop. See [reconnect.md](reconnect.md).
