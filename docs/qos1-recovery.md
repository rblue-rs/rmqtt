> Current integration: the owned Rust production driver supersedes the previous
> EventLoop adapter discussed below. QoS2 publish/subscribe are implemented;
> Will QoS2 remains Unsupported. See [current matrix](qos2-readiness.md),
> [driver](owned-driver.md) and [capacity audit](websocket-memory.md). Historical
> statements about unsupported QoS2 or inaccessible WS constructors below describe
> the previous adapter, not the current production route.

# Bounded in-process QoS1 recovery

Automatic reconnect remains OFF by default. Enable a finite policy and use
MQTT3.1.1 `cleanSession=false` (`Options.clean_session=false` in Rust;
`mqtt_client_connect_ex` clean_session=0 in C) for persistent-session recovery.
There are no new public APIs, events, error codes, ABI layouts, or dependencies.
The Flutter package is `rmqtt_flutter`; publish tickets retain their existing API.

## Ownership and outcomes

The supervisor retains one active operation and the official rumqttc0.25.1 engine
(including its packet allocator), not a new engine per retry. The existing FIFO
still admits at most32 waiting commands plus one active operation, each publish
payload at most65536 bytes. Offline admission is Busy, not an unbounded outbox.

- After a successful connection, a later transport failure can resume an active
  QoS1 publish only for MQTT3.1.1 clean=false and CONNACK session_present=1.
  The same nonzero packet identifier and bytes are retransmitted with DUP=1.
  A request definitely still unsent (id0 in the engine's drained channel) is sent
  normally once, with DUP=0. Only an actual yielded matching PUBACK completes
  Acknowledged. Sent and Acknowledged stages are emitted at most once per operation.
- The allocator is retained across reconnect; a completed id is not accidentally
  reset to1 for the next queued publish. Standard eventual packet-id reuse still
  occurs after completion/wrap; malicious indefinitely delayed ACKs are not a
  new application deduplication guarantee. Unsolicited/invalid ACKs may cause a
  transport failure, never completion of a different current ticket.
- If a reconnect reports session_present=0 while an interrupted active publish
  exists, fail the whole accepted work set with terminal Network(7), and close
  that connection before replay. An interrupted clean-session operation also
  fails terminally on the drop. The application decides whether to retry;
  the broker might already have processed the original publish. No false claim
  of non-delivery, silent id substitution, or promise left waiting forever.
- Never-dispatched commands remain in the bounded application FIFO and can run
  after successful reconnect and subscription reconciliation. If an interrupted
  active operation causes terminal failure, all remaining work fails with it.
  Interrupted QoS0 fails terminally too. Interrupted subscribe/unsubscribe waiters
  receive existing Subscribed/Unsubscribed completion with code Network(7), not
  success; reconnect may continue. Their expectation decisions remain admission
  decisions, not broker persistence. A cancelled/timed-out waiter stays cancelled/
  timed out; this late error cannot settle a different operation.
- ACK followed immediately by EOF can be decoded internally but never yielded by
  upstream. We clear those stale buffered events. If the engine has already
  removed the publish and cannot prove pending ownership, return terminal
  Network(7), NOT a fabricated receipt or an unsafe replay promise. If an ACK
  was actually yielded before the drop, its completed stage stays completed.
- MQTT5 remains CleanStart=true/no expiry, no persistent-session API. A drop with
  accepted active/queued work is terminal; idle finite reconnect remains available.

Local Future timeout/cancel only abandons local waiting. It does not withdraw a
sent packet or skip its protocol handshake; a publish ticket can still receive
its eventual real PUBACK after another local Future wait timed out.

## Lifecycle and subscription fences

First connect failure never loops. Finite attempts, equal-jitter exponential
backoff, one socket attempt, Disconnected/Reconnecting/Reconnected and no full
resubscribe for persistent session_present=1 are unchanged. A successful CONNACK alone does not reset the attempt budget while the same
interrupted QoS1 publish remains owned. Completion/different work allows the normal
fresh budget for a later outage; exhaustion is terminal Disconnected (Dart fails
remaining tickets with the existing disconnected StateError). New/clean idle sessions
resubscribe admitted expectations; interrupted active work instead fails as above.

Explicit disconnect/force-close/finalizer/owner exit stop reconnect. A drain during
backoff with interrupted work reports Failed, not Drained. A drain during replay
waits for PUBACK then DISCONNECT or reports its deadline/failure. These paths do
not reconnect after the cutoff; close/shutdown never wait for Dart on a runtime
worker. Force-close/finalizer/owner exit remain immediate and non-draining.

## Evidence boundary

Independent scripted MQTT3.1.1 TCP peers verify packet id/DUP/bytes across repeated
drops, queued-work release, stale ACK isolation, clean/new-session failure, finite
exhaustion, queue capacity, cutoff, replay drain deadline and ACK+EOF settlement.
A native Mosquitto2.0.21 TCP broker behind a documented MQTT-aware fault relay
preserves the real broker session; the relay discards its first actual PUBACK and
captures identical id/DUP1 replay and final PUBACK. The relay is not a new broker.
Dart/native tests cover ticket once-only completion, repeated drops, local wait
timeout and terminal session-loss failures. Official engine state tests cover
unsent id0 ownership and decoded-but-unyielded PUBACK failure.

TLS/WS/WSS use this shared v3 engine policy, but recovery-specific fault injection
here is TCP-only. Their existing host roundtrip/TLS-negative regressions are not
new transport-specific recovery evidence. MQTT5 negative PUBACK and outstanding
drop regressions remain separate. No mobile/background-iOS/Flutter Web evidence,
formal concurrency proof, or independent code review is claimed.

No disk outbox, cross-process QoS recovery, durable subscription expectation set,
application exactly-once or end-to-end delivery. Process death loses local tickets.
QoS2 remains Unsupported; its independent diagnostic gates remain red. Upstream
WS aggregate buffering still lacks a strict cap. Keepalive3.1.1 default0 and MQTT5
Unsupported0/explicit5..65535 whole seconds are unchanged; no TLS verification bypass.
