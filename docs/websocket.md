> Current integration: the owned Rust production driver supersedes the previous
> EventLoop adapter discussed below. QoS2 publish/subscribe are implemented;
> Will QoS2 remains Unsupported. See [current matrix](qos2-readiness.md),
> [driver](owned-driver.md) and [capacity audit](websocket-memory.md). Historical
> statements about unsupported QoS2 or inaccessible WS constructors below describe
> the previous adapter, not the current production route.

# MQTT over native WebSocket / WSS

Linux host evidence only. This is not mobile WSS, Flutter Web, a production
plugin, or a full MQTT session-recovery claim.

## Additive API and defaults

- Rust `Options.use_websocket` (default false), `Options.websocket_path`
  (default `/mqtt`). `Options.use_tls` selects WSS instead of WS.
- Dart `MqttConnectOptions.useWebSocket` (default false), `webSocketPath`
  (default `/mqtt`); existing `useTls` selects WSS.
- C `mqtt_client_set_websocket(handle, enabled, path, path_len)` configures
  subsequent connects on that handle, not an active connection or reconnect
  supervisor. `enabled=1, null+0` uses `/mqtt`; provided bytes are copied
  immediately (max 2048). `enabled=0` requires null+0 and resets to TCP/TLS.
  Existing connect/connect_ex parameters, codes, event layouts and APIs remain.
  A newly created handle still uses TCP/TLS unless explicitly configured.

```dart
await client.connect(options: const MqttConnectOptions(
  useWebSocket: true,
  webSocketPath: '/mqtt',
  useTls: true,
  // protocol: MqttProtocol.v5,
  // keepAlivePeriod: Duration(seconds: 5), // required for MQTT 5 success
));
```

Four unambiguous combinations: useWebSocket=false + useTls=false/true means
TCP/TLS; useWebSocket=true + useTls=false/true means WS/WSS. No scheme inference,
TLS downgrade, plaintext fallback or automatic port change. The caller's port
is used explicitly.

For WS/WSS, `server` must be an ASCII DNS name or IPv4 literal, not a URL,
userinfo, host:port, or bracketed address. IPv6 WS endpoints are deliberately
rejected in this baseline because the pinned upstream constructs an unbracketed
socket address. Path is an absolute URI-safe ASCII path <=2048 bytes, starting
with one `/`, with valid `%HH` escapes. No query, fragment, whitespace, control
characters, backslash, arbitrary headers or URL authentication. Nondefault path
without enabling WS is rejected, not silently ignored. Invalid inputs are
InvalidArgument(1) before network. URI construction is exactly
`ws[s]://server:port/path` (one path slash, no inserted credentials). WS host/path are
redacted from Rust Debug; path is redacted from Dart options toString; errors never include URL/headers.

## Protocol and TLS constraints

MQTT 3.1.1 default keepalive0 is encoded as zero even on WS/WSS. MQTT 5 default 0
and 1..4 seconds stay Unsupported(6), before networking or the upstream setter;
explicit whole seconds 5..65535 are admitted, success tested at5s. No 0->5/60
rewrite. MQTT 5 remains Clean Start=true/no expiry; cleanSession=false is
Unsupported. MQTT 3.1, QoS2 and advanced MQTT 5/session APIs remain unsupported.

Production framing uses the official rumqttc 0.25.1 `websocket` feature and its
async-tungstenite/ws_stream_tungstenite stack, not hand-written WS framing.
The client requests `Sec-WebSocket-Protocol: mqtt` and rejects missing/wrong
response subprotocols or rejected HTTP upgrades before sending MQTT CONNECT.
Handshake rejection maps to Network(7). First-connect failure never retries,
even with reconnect enabled. Native connection/handshake timeout is5s.

WSS uses the same verified rustls ClientConfig as TLS: cert chain + hostname,
custom CA exclusively or host system roots, mTLS certificate/key pair held in
memory, no insecure switch. Nested WSS TLS errors preserve certificate-untrusted,
hostname-mismatch and expiry mappings. Password over WS still requires explicit
allowPlaintextCredentials; WSS uses the existing secure-credential path. MQTT
credentials are never placed in HTTP URL/headers by this API.

## Tested host matrix and exact broker boundary

| Protocol | Transport | Keepalive tested | Evidence |
| --- | --- | --- | --- |
| MQTT 3.1.1 | TCP/TLS | default 0 + prior explicit tests | existing aMQTT0.12.2 regressions |
| MQTT 5 | TCP/TLS | explicit5 | existing Mosquitto2.0.21 regressions |
| MQTT 3.1.1 | WS/WSS | default 0 | native Mosquitto2.0.21 WebSocket listener |
| MQTT 5 | WS/WSS | explicit5 | native Mosquitto2.0.21 WebSocket listener |

The installed Mosquitto build successfully uses `protocol websockets` via
libwebsockets. Tests connect directly to this native listener, including WSS
certificate/key configuration. **No WS-to-TCP relay is used.** Each new matrix
cell exercises CONNACK, SUBACK, binary QoS0/1 publish and inbound echo, QoS1 ACK,
UNSUBACK and bounded drain. WSS trust/hostname/expiry failures are tested for
both engines; verified mTLS succeeds on both. aMQTT is only MQTT 3.1.1 evidence.

Dart's library-backed WebSocket fixtures are deterministic MQTT fakes, not
brokers. They verify custom path and subprotocol, binary packets for both
protocols, HTTP403/404, missing/wrong subprotocol with no MQTT packets, negative
MQTT 5 PUBACK, local cancellation then late SUBACK, drain/force-close, idle MQTT 5
reconnect, and drain/disconnect/force-close stopping a WS backoff.

## Lifecycle and limitations

Existing bounded native command/event queues, serial MQTT command processing,
manual inbound QoS1 ACK and control completion retention are unchanged. WS
framing additionally has the upstream library's own buffer/frame/message limits;
these are not the same as the smaller MQTT payload/event limits. Pinned
tungstenite defaults allow 64MiB messages and 16MiB frames, with 128KiB read/write
buffer targets and no finite max-write-buffer cap. This API does not expose WS
size tuning; it does not claim a strict aggregate WS-memory bound. No new disk
queue or unbounded application outbox is introduced. NativeFinalizer/owner-exit
and ordinary close remain immediate/non-draining. Cancel only abandons a local
wait; it never withdraws a packet. Drain never starts or continues reconnect.

Reconnect defaults off; enabled reconnect remains attempt-bounded/jittered.
Shared MQTT 3.1.1 [QoS1 recovery policy](qos1-recovery.md) is fault-tested over TCP;
this WS/WSS roundtrip evidence is not transport-specific fault-recovery proof. MQTT 5 drops with
accepted outstanding work fail terminally rather than pretending to recover.
The inherited MQTT 3.1.1 resubscribe fixture can race its peer's immediate close;
that known flaky test is not suppressed or claimed fixed by transport support.

No disk outbox, cross-process QoS recovery, durable expectation set, subscriber
or end-to-end delivery guarantee, mobile evidence, TSan/formal proof or Flutter
Web support. A process kill is a new session. iOS is not a background-persistent
connection. Keepalive0 does not disconnect or keep the connection alive.

## Resource policy

WS/WSS admitted subscription expectations are limited to 128 keys and 262144 UTF-8
topic bytes; exceeding a new-key quota returns QueueFull5 before admission. This
is partial metadata hardening, **not an aggregate WS memory cap**. Upstream
frame/message/automatic-control/write/unconsumed buffers remain separate from MQTT
limits; see [the resource analysis](websocket-memory.md) for tested bounds and gaps.
