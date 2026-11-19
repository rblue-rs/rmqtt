# rmqtt — experimental Rust / C / Dart MQTT client

Repository name: `rmqtt`; **not a published package or a production Flutter plugin**.
The Rust workspace intentionally retains its internal `mqtt-*` crate names.
The existing crates.io package named `rmqtt` is a separate MQTT broker: this project
is not that package and must not be published to crates.io under that occupied name.

## What actually works on the Linux host

- Rust `rumqttc = 0.25.1`, shared Tokio runtime, real MQTT **3.1.1 over TCP**.
- CONNACK-completed connect, SUBACK-completed subscribe, UNSUBACK unsubscribe.
- QoS 0/1 publishing with Queued / Sent / Acknowledged events; no fake QoS 0 ACK.
- Dart `MqttClient`, generated `ffigen` -> generated `cbindgen` C ABI -> Rust ->
  independent temporary loopback aMQTT broker, including binary messages.
- Disconnect/reconnect, permanent force-close, numeric handles (no raw handle UAF),
  caller-owned buffers and buffer-size retry, final runtime shutdown refusing live clients.
- Bounded native commands/events; overflow terminates, never silently evicts control
  events. Pausing any Dart stream listener stops polling and applies backpressure.
- Dart `MqttOperation<T>` with immediate operationId/future/cancel and additive
  beginConnect / beginSubscribe / beginUnsubscribe / beginDisconnect; existing
  Future methods still work. Cancellation only abandons local waiting, not MQTT packets.
- Independent local waits for coalesced disconnect callers, a shared first deadline,
  bounded join bookkeeping, and late-ACK isolation after timeout/cancellation.
- Close seals the native handle permanently: later poll returns Closed, never old
  messages or Empty. This is force close, not graceful draining.
- Default `keepAlivePeriod = Duration.zero` is actually encoded as zero in CONNECT.

## Explicitly not implemented / not validated

**No TLS/mTLS, MQTT 5 engine, WS/WSS, reconnect policy, persistent sessions,
subscription reconciliation, will/credentials, graceful draining close,
NativeFinalizer, mobile plugin packaging or mobile tests yet.** MQTT 5 returns
`MqttException(6)` / Rust `Error::Unsupported`, including its default-zero case;
it never rewrites 0 to 5 or 60 and never invokes the panicking v5 setter.
Unsupported features are absent from this experimental Dart options object;
there is no silently ignored TLS/reconnect flag. This is NOT the complete planned API.

C ABI is **experimental, not yet frozen**. In particular see
[the safe polling bridge decision](docs/ffi-safety.md): the original borrowed
callback buffer + `NativeCallable.listener` combination is unsafe. The prototype
uses polling instead of silently implementing that use-after-free design.

## Current verification status

**The host gate is not green.** The latest Linux run has 22/22 Rust tests passing
and 38 Dart tests passing, with one known failure: exiting the owner isolate
without explicit close leaks its native handle/runtime lease. The standalone
cleanup probe returns Busy(4) at final shutdown instead of success. The failure
is intentionally asserted by the default suite, not skipped. Explicit close is
mandatory; no isolate-exit cleanup guarantee is made. The 15 operation-handle
tests pass. C ASan/UBSan smoke and binding regeneration checks pass; see
[testing details](docs/testing.md). No mobile support result is claimed.

## Reproduce on Linux

Install Rust with rustfmt/clippy, a C compiler, Dart, libclang (for regeneration),
and a Python venv with `amqtt==0.12.2`. `cargo build -p mqtt-c` generates the C
header using cbindgen. `cd examples/flutter/mqtt_flutter && dart pub get`.

```sh
# Optional: point LIBCLANG_ROOT to a directory containing lib/libclang.so.
# Optional: set C_INCLUDE_DIR to `cc -print-file-name=include` if standard C
# headers are not found by a separately installed libclang.
LIBCLANG_ROOT=/path/to/llvm C_INCLUDE_DIR=/path/to/c/include \
  scripts/generate-bindings.sh
MQTT_TEST_PYTHON=/path/to/venv/bin/python scripts/test-host.sh
```

Overrides: `CARGO`, `DART`, `CC`, `MQTT_TEST_PYTHON`. The test broker binds only
127.0.0.1 on an ephemeral port and is shut down by the fixture. No public broker,
remote repository, release, network credential, or external account is required.
The C smoke test uses ASan/UBSan on the C executable, not on Rust dependencies.

See [semantics and current limitations](docs/semantics.md),
[host testing](docs/testing.md), and [mobile validation gates](docs/device-smoke.md).
No Android/iOS cross-build, simulator, or physical-device result is claimed.
