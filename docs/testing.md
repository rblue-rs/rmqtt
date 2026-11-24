> Current integration: the owned Rust production driver supersedes the previous
> EventLoop adapter discussed below. QoS2 publish/subscribe are implemented;
> Will QoS2 remains Unsupported. See [current matrix](qos2-readiness.md),
> [driver](owned-driver.md) and [capacity audit](websocket-memory.md). Historical
> statements about unsupported QoS2 or inaccessible WS constructors below describe
> the previous adapter, not the current production route.

Current bounded QoS1 recovery evidence/policy is in [QoS1 recovery](qos1-recovery.md).
Earlier gate descriptions below are historical snapshots, not current recovery claims.

# Linux host testing and validation limits

Experimental Linux-host-tested MQTT3.1.1 and MQTT5 over TCP/TLS and WS/WSS.
Component evidence: **Rust 121/121, Dart 114/114, C ASan/UBSan smoke 1/1**,
Python build-support **3/3**. The full host gate is timing-sensitive: an
inherited resubscribe fixture can fail when its peer closes immediately after
SUBACK; this test is not suppressed or claimed fixed. This is not a stable ABI, physical-device support, release readiness,
or a security certification. A local pass is not a GitHub Actions result.

## Reproduce

Install Rust 1.100.0 with rustfmt/clippy (`rust-toolchain.toml`), a C compiler,
Dart 3.14.0, Flutter 3.50.x (required for `dart pub get` because the Dart
package declares a Flutter FFI plugin), libclang for regeneration, and a Python
virtual environment containing `amqtt==0.12.2`, plus Mosquitto (tested 2.0.21).
`MQTT5_BROKER` may override its default `/usr/sbin/mosquitto` path. Then at the repository root:

```sh
MQTT_TEST_PYTHON="${VIRTUAL_ENV}/bin/python" scripts/ci-host.sh
```

`scripts/ci-host.sh` runs `dart pub get`, `scripts/test-host.sh` (fmt, clippy
`-D warnings`, `cargo test --locked`, C smoke with ASan/UBSan, `dart analyze
--fatal-infos`, `dart test --concurrency=1`) and `scripts/check-bindings.sh`.
`CARGO`, `DART`, `CC`, `MQTT_TEST_PYTHON`, `FLUTTER_ROOT`, `LIBCLANG_ROOT`, and
`C_INCLUDE_DIR` can override locations. `C_INCLUDE_DIR` defaults to
`cc -print-file-name=include` when unset.

The fixture starts and stops an independent temporary aMQTT broker on 127.0.0.1
with an ephemeral port. TLS and mTLS tests generate temporary certificates in a
temp directory via `scripts/gen_test_certs.py`. No real password, private key,
public broker, or external account is stored in the repository. Build outputs
and machine-specific logs are not part of this source snapshot.

## Recorded Linux result

Linux x86_64, 2026-11-23 (Asia/Shanghai), Rust 1.100.0, Dart 3.14.0, Flutter
3.50.1, Python 3.13, amqtt 0.12.2, rumqttc 0.25.1:

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `cargo test --workspace --locked` | 121 passed / 0 failed / 0 ignored |
| C11 executable with ASan/UBSan and aMQTT | PASS (1 fixture) |
| `dart analyze --fatal-infos` | no issues |
| `dart test --concurrency=1` | 114 passed / 0 failed |
| `scripts/check-bindings.sh` | header and Dart bindings hash-identical after regeneration |
| `scripts/ci-host.sh` | latest exit 0; inherited resubscribe race also reproduced on earlier runs (exit101) |

Host TLS coverage includes a custom CA or host system roots (a custom CA
replaces system roots), hostname and expiry checks, untrusted-CA and
TLS-to-plaintext failures that are not retried, and rejection of empty or
invalid CA PEM without falling back to plaintext. Host mTLS coverage includes
an in-memory client certificate/key pair, missing or untrusted client
certificates (`Tls` 14, once), unparsable PEM (`InvalidClientCertificate` 20),
and a non-matching pair (`CertificateKeyMismatch` 21). Credential tests cover
passwords on TLS, the explicit `allowPlaintextCredentials` switch on plaintext
TCP (`PlaintextCredentials` 18), and `BadCredentials` 19 without retry. MQTT
3.1.1 Last Will (topic, payload, QoS 0/1, retain) is covered; QoS 2 Will is
`Unsupported`. Passwords, certificates, and private keys are redacted from
Debug, Dart `toString`, errors, and events. These are Linux-host results, not
mobile trust-store results.

Existing ownership, close/poll, late ACK, keepalive-zero, QoS 1 redelivery, and
backpressure coverage remains. Owner-exit probes observe `mqtt_client_new`
returning `NotInitialized` (2) in a bounded native-dispatch window **before**
any substituting `mqtt_runtime_shutdown`; a successful temporary probe handle is
closed and freed immediately. Shutdown is then checked only for idempotence.
That does not weaken the original assertions, and it is not a drain API. Local once-only settlement is not MQTT
exactly-once delivery. C sanitizer flags instrument the C executable, not the
full Rust dependency tree.

## Continuous integration

`.github/workflows/host-tests.yml` is the Linux host job on `ubuntu-24.04`. It
installs the pinned tools above, with no repository secrets, then runs the same
steps as `scripts/ci-host.sh`. The README Host tests badge tracks this workflow
on `main` only; it is not a claim that every workflow, an iOS build, an
emulator, a simulator, or a physical device has passed. A local pass is not a
GitHub Actions result. There is no macOS host-tests job: the C smoke test and
Dart native library path in `scripts/test-host.sh` are Linux `.so` / rpath
specific.

A separate `.github/workflows/ios-buildable.yml` job is documented in
[device-smoke.md](device-smoke.md). It is not covered by the Host tests badge.

## Not verified

No whole-Rust ASan, TSan, Miri, fuzzing, or broad multi-broker interoperability
certification is claimed. No mobile WSS/Flutter Web, MQTT5 advanced-properties API, disk outbox, or cross-process durable
recovery is claimed. Automatic reconnect has Linux-host tests only and is off
by default. Bounded graceful close has Linux-host evidence only. Android `arm64-v8a` has local
cross-build / debug-APK evidence only; iOS `arm64` project files are prepared
but not built on Linux. No emulator, simulator, or physical-device TLS result
exists. See [mobile gates](device-smoke.md) and [ownership](ownership.md).

## Build portability regression checks

`scripts/test-host.sh` also runs `scripts/test_build_support.py`. Its certificate
check verifies the generated leaf signature, server/client usage, loopback SAN,
and the fixed 2020 expiry interval. Dated issuance uses `openssl ca` so OpenSSL
3.0 (Ubuntu 24.04) and newer releases exercise the same expired-certificate test;
no TLS validation or failure assertion is bypassed.

The iOS orchestration regression uses fake tools on Linux, plus a real host
Cargo staticlib-selection probe. Neither is an iOS compile or link result.
`build-libs.sh ios-arm64` now selects only `staticlib` and explicitly exports
`IPHONEOS_DEPLOYMENT_TARGET=15.0` for Rust and native dependency compilers,
matching the existing podspec and example minimum (not raising it). The actual
iOS library/app must still build on macOS/CI; local Linux checks are not proof
of GitHub or iOS success.

## Optional graceful-close regression coverage

[Drain close](drain-close.md) is tested through Rust, additive C exports and Dart:
real aMQTT accepted QoS1/control success; gated PUBACK timeout and first-deadline
coalescing; full native command queue; new-work rejection; immediate force-close;
per-caller cancellation; connection loss; paused-stream/inbound suppression;
owner exit with a 300s pending drain; invalid and idle/connecting starts. C status
validates null outputs, retains results until free, and exercises 64 concurrent
admission/force-close races with independent output memory. The old close/poll,
cancellation, ownership and disconnect regressions are unchanged and remain green.
No mobile drain evidence, hard realtime guarantee, whole-Rust sanitizer, TSan, or
formal concurrency proof is implied.

## Optional reconnect and expectation-set regression coverage

[Reconnect](reconnect.md) is tested on the Linux host through Rust, additive C
exports, and Dart: default-off drop still fails without reconnect events; first
connect refusal/timeout does not enter the loop; a later broker drop emits
`Disconnected` then `Reconnecting`; equal-jitter backoff and a required
`max_attempts` ceiling end in a terminal `Disconnected`; at most one connection
is opened at a time; explicit disconnect, drain, and force-close stop the loop;
`clean_session` is visible on the CONNECT wire; duplicate subscribe does not
crash; `session_present=0` or a clean session resubscribes the expectation set;
`session_present=1` with a persistent session does not automatically
full-resubscribe; a locally accepted subscribe that has not yet been SUBACK'd
is still in the set. Existing default-off, drain, close, TLS, and ownership
regressions remain green.

No mobile reconnect evidence, disk outbox, cross-process QoS recovery, TSan,
or formal concurrency proof is implied. In-flight QoS 1 hold/retransmit across
a `session_present=1` reconnect is not implemented in this slice.


## MQTT5 baseline

See [MQTT5](mqtt5.md) for the protocol/keepalive matrix. Real Mosquitto 2.0.21
integration covers TCP and verified TLS QoS0/1 binary echo, subscribe/unsubscribe,
and TLS trust/hostname failures. Deterministic MQTT5 fixtures cover negative ACKs,
first-CONNACK timeout, cancellation/late ACK, drain/timeout/force-close, idle
reconnect and terminal failure with outstanding work. aMQTT 0.12.2 remains the
MQTT3.1.1-only fixture; it does not verify MQTT5.


## WebSocket transport

The [WS/WSS matrix](websocket.md) is tested directly against native Mosquitto2.0.21
WebSocket listeners for MQTT3.1.1 and MQTT5, including binary QoS0/1, inbound,
subscribe/unsubscribe and WSS trust/hostname/expiry/mTLS. No relay is used.
Deterministic library-backed Dart WS fixtures verify path/subprotocol rejection,
local cancellation/late ACK, force-close/drain, negative MQTT5 ACK and bounded
reconnect shutdown interactions. Host support is not mobile WSS or Flutter Web.
The inherited resubscribe fixture flake remains documented, not suppressed.


## QoS2 readiness is not a passed feature

[QoS2 readiness](qos2-readiness.md) remains blocked: the separately invoked strict
state and wire release probes exit1 (6/18 and6/12 passed respectively). Default
host CI does not run these executables; it runs all existing QoS0/1 regressions
and public safety guards for Unsupported QoS2. A green host gate is not a QoS2
success claim. No QoS2 broker/protocol/transport support is claimed.

## Reconciliation fixture lifetime

The deterministic TCP resubscribe fixtures keep the reconnected socket alive until
the application observes Subscribed and SubscriptionsResubscribed, then use an
explicit MQTT DISCONNECT as the teardown fence. A raw PINGREQ/PINGRESP test verifies
that the fixture remains live after writing SUBACK. Negative session-present checks
capture all packets through the ordered DISCONNECT, rather than interpreting an
empty timed read or EOF as proof that no SUBSCRIBE was sent.

This fixes test-fixture premature teardown, not MQTT ACK recovery on a real network
failure. In pinned rumqttc0.25.1, bulk read can return ConnectionAborted when EOF
follows a decoded ACK before buffered events are yielded. Closing immediately after
a SUBACK write is not a guarantee that the application observed the ACK. No upstream
engine code is patched and no QoS/session-recovery guarantee is added.

Run the focused suite with `cargo test -p mqtt-core --test resubscribe`. Observation
bounds are failure guards; fixture ordering is enforced by protocol/event barriers,
not longer sleeps. The independent QoS2 diagnostic gates and known QoS1 recovery
limitations are unaffected by fixture stability.
