# Automatic reconnect and in-process subscription expectation set

This is a Linux-host contract. It is not mobile evidence, not disk recovery, and
not a claim that a broker subscriber has received a message.

## Defaults

Automatic reconnect is **off** by default. Callers that do not set a policy
behave as before: a first-connect failure or a later drop does not start a
reconnect loop and does not emit `Reconnecting` / `Reconnected`.

MQTT 3.1.1 `cleanSession` / `clean_session` is explicit and defaults to `true`,
matching the previous implicit rumqttc default. MQTT 5 baseline requires explicit keepalive >=5s and cleanSession=true;
see [MQTT5](mqtt5.md). There is no cleanStart/session-expiry API.

`keepAlivePeriod` remains default 0: it only disables MQTT protocol keepalive.
It is not a disconnect and does not keep the connection alive. iOS is not a
background-persistent connection.

## Policy

Rust `ReconnectPolicy`, Dart `MqttReconnectPolicy`, and C `MqttReconnectPolicy`
share the same fields:

- `enabled` (default false)
- `max_attempts` / `maxAttempts` (default 8; required ≥ 1 when enabled)
- `base_delay` / `baseDelay` (default 1s)
- `max_delay` / `maxDelay` (default 30s)

When enabled, delays must be whole milliseconds, at least 1 ms, not exceed
`maxDelay`, and `maxDelay` must be ≤ 300 seconds. The ceiling is required:
the client never retries indefinitely.

Backoff is **equal jitter**: for attempt *n*,
`cap = min(maxDelay, baseDelay * 2^(n-1))`, then
`delay = cap/2 + random(0, cap/2)`. After a successful reconnect, the attempt
counter resets for a later drop after completion/different work. A successful
CONNACK alone does not reset the budget of the same interrupted QoS1 publish; see
[QoS1 recovery](qos1-recovery.md). At most one reconnect attempt is in flight;
`connect` on an already connecting, connected, or reconnecting handle is
rejected (`Busy` / `StateError`). A second TCP session is not opened while
one is still up.

## First failure vs later drop

A first `connect` that fails (timeout, refusal, TLS failure) fails the
connect Future/result once. It does not enter the reconnect loop and does
not emit reconnect events. The application decides whether to retry.

Only after a successful `CONNACK` may a later drop reconnect. The client
emits `Disconnected` and then `Reconnecting`. It does not swallow the
disconnect event. After a successful reconnect it emits `Reconnected` with
`session_present` from `CONNACK`. If `max_attempts` is exhausted, it stops
and emits a terminal `Disconnected`.

Native event kinds (additive; 1–9 unchanged): `Reconnecting` 10,
`Reconnected` 11, `SubscriptionsAligned` 12, `SubscriptionsResubscribed` 13.
Error/status codes 1–21 are unchanged. Dart appends
`MqttConnectionState.reconnecting` and `MqttClientEventKind` values; existing
`MqttClientEvent(state, [error])` still describes a state transition.

## Expectation set and session_present

The supervisor keeps an in-process set of topic filters (last requested QoS
wins). It is **not** the broker’s actual subscriptions and is **not**
cross-process persistence. A process kill is a new session.

The set is updated when a subscribe or unsubscribe is **accepted locally**,
even if the local waiter later times out or is cancelled. Cancellation never
withdraws an already-sent MQTT packet. Duplicate subscribe of the same
filter does not crash the client; it updates the expected QoS and sends
another `SUBSCRIBE`.

After auto-reconnect:

- `session_present=0`, **or** a clean session (`cleanSession=true`):
  resubscribe the expectation set. The application can observe
  `Subscribed` events (operation id 0 for supervisor-generated requests)
  and `SubscriptionsResubscribed`. Do not pretend in-flight local QoS
  state survived the clean/new session.
- `session_present=1` and `cleanSession=false`: do **not** automatically
  full-resubscribe held filters. Emit `SubscriptionsAligned`. This does
  not mean local in-flight QoS 1 promises must be cleared just because of
  the flag.

The set lives only for the current supervisor. An explicit `disconnect`
ends that actor; a later user `connect` starts with an empty set.

## Close, drain, and disconnect

A drain, force-close, or explicit disconnect in progress does not start or
continue a reconnect loop. `begin_drain` during reconnect stops the loop
and, with no outgoing work, completes as drained. Force-close, finalizers,
and owner-exit remain immediate and non-draining. Close/shutdown still must
not `block_on` the runtime worker or a callback stack waiting for Dart.

C: existing `mqtt_client_connect` is unchanged (clean session true,
reconnect off). Additive `mqtt_client_connect_ex` accepts `clean_session`
(0 or 1) and an optional `MqttReconnectPolicy*` (null = default off).

## What this is not

See [bounded in-process QoS1 recovery](qos1-recovery.md) for packet ownership,
DUP replay, explicit new-session failure and ACK+EOF limits. This is not full
session recovery. MQTT5 terminates on a drop with accepted outstanding work
rather than stranding promises in a recovery loop; idle MQTT5 reconnect is supported.

No disk outbox. No cross-process QoS recovery. The expectation set is not
durable persistence. Broker `PUBACK`/`SUBACK` is not subscriber or
application delivery. This is not formal concurrency proof and not mobile
verification.
