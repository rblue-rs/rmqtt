# rmqtt — experimental Rust / C / Dart MQTT client

Repository name: `rmqtt`; **not a published package or a production Flutter plugin**.
The Rust workspace intentionally retains its internal `mqtt-*` crate names.
The existing crates.io package named `rmqtt` is a separate MQTT broker: this project
is not that package and must not be published to crates.io under that occupied name.

## What actually works on the Linux host

- Rust `rumqttc = 0.25.1`, shared Tokio runtime, real MQTT **3.1.1 over TCP** and **host TLS**, including **host mTLS**. MQTT **5.0 TCP/TLS baseline** requires explicit
  keepalive 5..65535 seconds; default-zero MQTT5 stays Unsupported. See [MQTT5](docs/mqtt5.md).
- MQTT3.1.1 and MQTT5 [WS/WSS on the Linux host](docs/websocket.md), using native
  Mosquitto WebSocket listeners (no relay); opt-in useWebSocket with /mqtt path.
- Username/password on TLS, or on plaintext TCP only with an explicit
  `allowPlaintextCredentials` switch. Without that switch, a password on
  plaintext is rejected before connect and is never silently dropped.
- Client certificate and private key are passed in memory as PEM. TLS must
  be enabled; a certificate and key must form a pair. They are never written
  to disk, and keys are redacted from logs, errors, Debug, and Dart `toString`.
- MQTT 3.1.1 Last Will (`topic`, `payload`, QoS 0/1, `retain`). QoS 2 will is
  `Unsupported`. MQTT 5 will properties are not implemented.
- CONNACK-completed connect, SUBACK-completed subscribe, UNSUBACK unsubscribe.
- QoS 0/1 publishing with Queued / Sent / Acknowledged events; no fake QoS 0 ACK.
- Dart `MqttClient`, generated `ffigen` -> generated `cbindgen` C ABI -> Rust ->
  independent temporary loopback aMQTT broker, including binary messages.
- Disconnect/reconnect, permanent force-close, numeric handles (no raw handle UAF),
  caller-owned buffers and buffer-size retry, final runtime shutdown refusing live clients.
- Optional bounded [`drainClose` / `beginDrainClose`](docs/drain-close.md) waits for
  accepted outgoing protocol work; ordinary `close()` stays immediate force-close.
- Optional automatic reconnect, default **off**, with a required attempt ceiling
  and equal-jitter backoff. See [reconnect](docs/reconnect.md). MQTT 3.1.1
  `cleanSession` is explicit (default true). An in-process subscription
  expectation set is used only after a successful connection later drops.
- Bounded native commands/events; overflow terminates, never silently evicts control
  events. Pausing a stream normally stops polling; explicit drain polls only retained
  control completions and discards inbound messages.
- Additive `MqttOperation<T>` / beginX APIs expose immediate operationId/future/cancel;
  existing Future methods delegate. Cancel abandons only local waiting, not MQTT packets.
- Weak adaptive polling, bounded stream backpressure, matching-allocator native buffer
  cleanup, and native-only scalar owner-exit notifications. NativeFinalizer is GC fallback.
- Default `keepAlivePeriod = Duration.zero` is encoded as zero for MQTT3.1.1;
  MQTT5 with zero is typed Unsupported before network activity.

## Explicitly not implemented / not validated

Host TLS (MQTT 3.1.1 and 5.0) verifies the certificate chain and hostname. No custom CA
PEM means the **host** system roots are used. A caller-supplied CA PEM is trusted
exclusively; system roots are not mixed in. TLS cannot silently fall back to
plaintext. There is no skip-verify / onBadCertificate / insecure switch.
mTLS is supported on the Linux host: a client certificate chain and private
key may be supplied in memory. Providing them without TLS, or providing a
certificate and key that are not a pair, is a typed error and is not ignored.
Certificates, private keys, and passwords are not logged and are redacted from
options Debug and Dart `toString`. Close best-effort clears in-memory copies;
this is not a secure-erase guarantee. This is a Linux-host TLS/mTLS result,
not a claim about Android or iOS trust stores.

**No mobile WSS/Flutter Web, disk outbox, cross-process QoS recovery, or mobile verification.**
MQTT5 baseline succeeds with explicit supported nonzero keepalive only; 0..4s
returns `MqttException(6)` / Rust `Error::Unsupported` without invoking the
panicking upstream setter. No 0-to-5/60 rewrite. No MQTT5 session-expiry,
advanced-properties API or enhanced auth. Automatic reconnect remains off by
default and is not durable persistence. MQTT3.1.1 persistent-session QoS1 resumes the same packet id with DUP after
a drop; new/clean sessions fail interrupted work explicitly. See
[QoS1 recovery policy](docs/qos1-recovery.md). MQTT5 drops with accepted outstanding
work fail terminally.
See [MQTT5 limits](docs/mqtt5.md). This is NOT the complete planned API.

C ABI is **experimental, not yet frozen**. In particular see
[the safe polling bridge decision](docs/ffi-safety.md): the original borrowed
callback buffer + `NativeCallable.listener` combination is unsafe. The prototype
uses polling instead of silently implementing that use-after-free design.

## Resource ownership limits

Prefer explicit close. Owner-exit cleanup has Linux tests for Isolate.run termination,
natural quiescent exit and both VM kill priorities **after initialization/connection
has completed**. GC fallback is tested under allocation pressure, not a timeliness
promise. Kill during partial setup/connection allocation, process exit/crash/OS kill,
unloading the engine/library and mobile hot restart have no cleanup guarantee.
A live poll timer can keep an isolate alive; returning from an entrypoint is not
itself a request to close a live connection. See [ownership](docs/ownership.md).

## Reproduce on Linux

Install Rust with rustfmt/clippy, a C compiler, Dart, libclang (for regeneration),
Mosquitto (tested 2.0.21), and a Python venv with `amqtt==0.12.2`. `cargo build -p mqtt-c` generates the C
header into Cargo OUT_DIR using crate-local cbindgen config;
`scripts/generate-bindings.sh` explicitly exports it and regenerates Dart bindings. `cd examples/flutter/mqtt_flutter && dart pub get`.

```sh
# Optional: point LIBCLANG_ROOT to a directory containing lib/libclang.so.
# Optional: set C_INCLUDE_DIR to `cc -print-file-name=include` if standard C
# headers are not found by a separately installed libclang.
LIBCLANG_ROOT=/path/to/llvm C_INCLUDE_DIR=/path/to/c/include \
  scripts/generate-bindings.sh
MQTT_TEST_PYTHON=/path/to/venv/bin/python scripts/test-host.sh
```

Overrides: `CARGO`, `DART`, `CC`, `MQTT_TEST_PYTHON`, `MQTT5_BROKER` (default `/usr/sbin/mosquitto`). The test broker binds only
127.0.0.1 on an ephemeral port and is shut down by the fixture. TLS tests generate
temporary certificates under a temp directory via `scripts/gen_test_certs.py`;
private keys are not stored in the repository. No public broker,
remote repository, release, network credential, or external account is required.
The C smoke test uses ASan/UBSan on the C executable, not on Rust dependencies.

## Continuous integration

[![Host tests](https://github.com/rblue-rs/rmqtt/actions/workflows/host-tests.yml/badge.svg?branch=main)](https://github.com/rblue-rs/rmqtt/actions/workflows/host-tests.yml?query=branch%3Amain)

`.github/workflows/host-tests.yml` runs the Linux host gate on `ubuntu-24.04`.
It installs pinned tools (Rust 1.100.0 from `rust-toolchain.toml`, Dart 3.14.0,
Python 3.13, `amqtt==0.12.2`, plus Mosquitto, a C compiler and libclang), runs
`scripts/test-host.sh`, then `scripts/check-bindings.sh` so the committed C
header and Dart bindings match a fresh cbindgen/ffigen regeneration. The
workflow uses no repository secrets.

The same steps can be run locally with the same pinned versions:

```sh
# Optional: LIBCLANG_ROOT if libclang is not in a system LLVM prefix.
# C_INCLUDE_DIR defaults to `cc -print-file-name=include` when unset.
MQTT_TEST_PYTHON=/path/to/venv/bin/python scripts/ci-host.sh
```

The Host tests badge reports that Linux workflow on `main` only. It is not a
claim that every workflow, an iOS build, an emulator, a simulator, or a
physical device has passed. A local `scripts/ci-host.sh` result is not a
remote CI result.

## Mobile packaging and buildable evidence

`examples/flutter/mqtt_flutter` contains the confirmed **rmqtt_flutter** Flutter FFI
package plus a minimal sample app in `example/`. The repository directory is retained
for build-script compatibility; Rust package names and C ABI/library names are unchanged.
The plugin bundles a hash-checked native source snapshot, licenses, and package-local
build hooks, not prebuilt native binaries. See its [source-package guide](examples/flutter/mqtt_flutter/README.md). It is not a published plugin and not
a physical-device verified client.

- Default Android ABI: `arm64-v8a` (`aarch64-linux-android`).
- Default iOS device ABI: `arm64` (`aarch64-apple-ios`).
- Native libraries are built with `panic=abort`. A Rust panic aborts the
  process and is not turned into a Dart exception on these targets.
- iOS does not promise always-on background MQTT.
- Package-local `tool/build-native.sh` builds the bundled source and stages
  `libmqtt_c.so` or `libmqtt_c.a`; the legacy repository build script remains for
  earlier full-checkout instructions. Artifacts are not committed or bundled.
- `publish_to: none` and Cargo `publish = false` remain release guards. Local package
  extraction/build/load checks do not validate mobile build hooks or establish release
  readiness; Rust registry naming/versioned dependencies remain unresolved.

A Linux host can produce Android buildable evidence when Flutter 3.50.x and
Android NDK 28.2.13676358 are installed (`flutter build apk --debug --target-platform android-arm64`). iOS
library and unsigned `flutter build ios --no-codesign` builds require macOS
and Xcode; `.github/workflows/ios-buildable.yml` is a separate workflow from
the Linux host-tests job and is not covered by the Host tests badge. An APK, a
static library, or an unsigned iOS build is not an emulator, simulator, or
physical-device TLS result.

See [mobile validation gates](docs/device-smoke.md) for the evidence table.

See [semantics and current limitations](docs/semantics.md),
[host testing](docs/testing.md), and [mobile validation gates](docs/device-smoke.md).


QoS2 publish/subscribe now use the experimental owned production Rust driver for
MQTT311/5 over host TCP/TLS/native WS/WSS; Will QoS2 remains Unsupported.
See [actual matrix](docs/qos2-readiness.md) and [driver integration](docs/owned-driver.md).
Finite WS frame/message/write/HTTP bounds are reachable in production, but the
[full client-owned aggregate capacity audit](docs/websocket-memory.md) is not yet
complete. No application exactly-once, total-RSS or new mobile claim follows.
