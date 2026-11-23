> Current integration: the owned Rust production driver supersedes the previous
> EventLoop adapter discussed below. QoS2 publish/subscribe are implemented;
> Will QoS2 remains Unsupported. See [current matrix](qos2-readiness.md),
> [driver](owned-driver.md) and [capacity audit](websocket-memory.md). Historical
> statements about unsupported QoS2 or inaccessible WS constructors below describe
> the previous adapter, not the current production route.

# MQTT 5.0 baseline over TCP and host TLS

The official `rumqttc` dependency remains pinned to **0.25.1**, without a fork,
patch, vendor copy, or dependency upgrade. Its MQTT5 setter requires keepalive
at least 5 seconds:
[official tagged source](https://github.com/bytebeamio/rumqtt/blob/rumqttc-0.25.1/rumqttc/src/v5/mod.rs#L236).
The official main branch has the same restriction when checked; see
[releases](https://github.com/bytebeamio/rumqtt/releases).

## Explicit selection; default zero is still unsupported for MQTT5

The default is still MQTT3.1.1 with `keepAlivePeriod=Duration.zero`. MQTT5 does
**not** work with its default-zero keepalive. Selecting MQTT5 with 0..4 seconds
returns existing typed `Unsupported` (6) before network activity or the upstream
panicking setter. It never rewrites 0 to 5 or silently uses the engine's 60s
default. For successful MQTT5, explicitly choose whole seconds **5..65535**.
Fractional seconds and values outside the wire range remain `InvalidArgument`.

```dart
await client.connect(options: const MqttConnectOptions(
  protocol: MqttProtocol.v5,
  keepAlivePeriod: Duration(seconds: 5),
  // useTls: true, // chain + hostname verification, no insecure switch
));
```

Rust uses `Options.protocol=5` and `Options.keep_alive`. Both existing C
`mqtt_client_connect` and `mqtt_client_connect_ex` accept protocol=5 and an
explicit supported keep_alive_seconds value. No C signatures, numeric error
codes, event numbers, event layouts, or Dart method signatures changed.
MQTT3.1 remains unsupported; QoS2 is still typed Unsupported before sending.

MQTT5 uses Clean Start=true and no session-expiry property. The existing
`cleanSession` / `clean_session` option is a MQTT3.1.1 setting: setting it false
with MQTT5 is Unsupported rather than inventing a persistent-session policy.
No cleanStart/sessionExpiry API is introduced.

## What is tested

CONNACK-completed connect and refusal; subscribe/SUBACK; QoS0 sent without a
fabricated ACK; QoS1 sent/PUBACK; inbound binary QoS0/1 with manual ACK only after
native admission; unsubscribe/UNSUBACK; explicit disconnect; local cancellation
and late SUBACK; optional bounded drain/timeout and immediate force-close.

Real loopback **Mosquitto 2.0.21** integration exercises MQTT5 TCP and verified
TLS with explicit keepalive 5, binary QoS0/1 echo and unsubscribe, plus untrusted
CA and hostname failures. Deterministic MQTT5 wire fixtures cover negative ACKs,
first-CONNACK timeout, version/keepalive encoding, no-packet rejection,
reconnect and lifecycle races. **aMQTT 0.12.2 is MQTT3.1.1-only**, used only for
that protocol's existing tests. None of this is mobile verification.

## Refusal and settlement

CONNACK BadUserNamePassword/NotAuthorized maps to `BadCredentials` (19); other
CONNACK refusals map to `Refused` (8). A negative PUBACK/SUBACK/UNSUBACK maps to
terminal `Refused` (8), never success or Acknowledged. Raw reason strings and
properties are not exposed or logged. PUBACK NoMatchingSubscribers and UNSUBACK
NoSubscriptionExisted are successful protocol completions, not delivery claims.
First-CONNACK timeout maps to `Timeout` (13), and never enters reconnect.

On a terminal native Failure, Dart fails all pending local waits once. Native C
callers consume the terminal Failure rather than waiting for further operation
successes. Cancel only abandons one local waiter: it never withdraws a packet.
Drain retains accepted control completions; force-close/finalizer/owner-exit
remain immediate and non-draining. No Dart callback handshake or worker block_on.

## Reconnect and recovery limits

Reconnect remains off by default. Enabled idle MQTT5 reconnect uses the same
bounded attempts, jitter and events as [reconnect](reconnect.md), with the
expectation set resubscribed after a new clean connection. If a MQTT5 session
drops with accepted outstanding work, it fails terminally instead of transparently
reconnecting and stranding the promises. The application chooses whether to
retry; an already-sent packet might have been processed. Negative ACKs are also
terminal, not retried.

MQTT3.1.1 has a separate [bounded in-process QoS1 recovery policy](qos1-recovery.md).
It does not extend MQTT5 clean-start sessions or promise full session recovery.
There is no disk outbox, cross-process QoS recovery, or end-to-end delivery
guarantee. The expectation set is not durable persistence. A process kill is a
new session. iOS is not a background-persistent connection. Keepalive zero does
not disconnect and does not keep a connection alive.

## Outside this baseline

WS/WSS host transport is separately tested: see [WebSocket](websocket.md).
No QoS2, enhanced authentication, outbound MQTT5 advanced properties,
property inspection API, session expiry, or durable recovery. Basic Last Will
can be carried without MQTT5 Will properties. TLS uses the same verified
transport/custom-CA-exclusive policy as MQTT3.1.1. Credentials keep their existing
plaintext opt-in and redaction rules. No insecure/skip-verify fallback exists.
