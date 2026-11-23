# rmqtt_flutter (unreleased)

Single experimental Flutter FFI plugin for the Rust MQTT client in
https://github.com/rblue-rs/rmqtt . Package name: **rmqtt_flutter**; Rust component
names and C symbols are unchanged. `publish_to: none` deliberately prevents release.
No pub.dev release, mobile runtime validation, or Flutter Web support is implied.

## Source distribution and prerequisites

The package includes `native/`: the Rust workspace, locked registry dependencies,
C/Dart API DL glue and its BSD license, C header/config, and project Apache license.
No native binaries, passwords, certificates/private keys or dependency source caches
are bundled. Builds require a writable package-local native/target directory and
mobile staging directories; read-only package-cache installation is not validated.
Internet access to obtain Cargo/pub dependencies is needed on a fresh
machine; offline builds require a prepopulated normal dependency cache. This is not
a vendored/offline SDK distribution.

Prerequisites: Rust toolchain pinned by `native/rust-toolchain.toml` (tested 1.100.0),
C compiler/linker and the crypto dependencies' build prerequisites; Flutter SDK
(manifest >=3.50.0), Dart SDK (manifest >=3.4.0), Bash and Cargo. The lockfiles/dev
tools currently require newer Dart than the manifest floor: only Flutter 3.50.1 /
Dart 3.14.0 are tested. Minimum supported SDK versions are not established; do not
interpret unchanged manifest bounds as verified minimal SDK compatibility.

For a local source-package host verification, from this directory:

```sh
dart pub get --offline # omit --offline if dependencies are not cached
bash tool/build-native.sh host --offline
MQTT_NATIVE_LIBRARY="$PWD/native/target/debug/libmqtt_c.so" \
  MQTT_TEST_PORT=18883 dart test tool/host_smoke_test.dart
```

The smoke test requires an already-running local MQTT 3.1.1 broker on the selected
port (e.g. Mosquitto). Linux `.so` is host evidence only, not a supported Flutter
desktop plugin declaration. macOS/Windows host builds/tests are not verified. Native
library loading on iOS remains process-linked; host loading requires the explicit
`MQTT_NATIVE_LIBRARY` path (no automatic download or global installation).

The Android Gradle hook calls the package-local build script every build (Cargo
handles incremental work), rather than silently skipping missing Rust source.
Android configuration remains arm64-v8a, minSdk24 / compileSdk36 / Java17, with a
preinstalled NDK (the existing default is 28.2.13676358) and Rust target
`aarch64-linux-android`. The CocoaPods script builds only the existing iPhoneOS
arm64 static-library slice, iOS15+, using macOS/Xcode and a preinstalled
`aarch64-apple-ios` target. Neither hook installs SDKs/targets. iOS simulator and
other ABIs fail explicitly; they are not provided by this package. Mobile hooks
are source-level orchestration, not build/run evidence. Existing independent
mobile-validation work still requires integration; it is not folded into this tree.

```dart
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
```

## Scope and limitations

- MQTT 3.1.1 and MQTT 5 host TCP/TLS/WS/WSS paths exist. MQTT 5 default keepalive0 is
  typed Unsupported before traffic; explicit whole seconds5..65535 are admitted
  (success tested at5). MQTT 3.1.1 keepalive remains0 by default;0 does not maintain
  liveness or request disconnect. TLS certificate/hostname verification stays on.
- MQTT 5 CleanStart=true, no session-expiry API or public persistent-session promise.
- QoS0/1/2 publish/subscribe use the owned production Rust driver. Dart exactlyOnce
  is appended at ordinal2; whenAcknowledged waits PUBCOMP. Will QoS2 remains
  Unsupported. Upstream EventLoop diagnostics remain RED references, not this driver.
  Protocol handshakes are not application delivery or end-to-end exactly-once.
- Reconnect defaults off and is finite when enabled. MQTT 3.1.1 persistent sessions
  resume in-process QoS1 with the same id and DUP; clean/new sessions explicitly fail
  interrupted tickets (Network7), rather than silently replaying or stranding them. Host resubscribe fixtures wait for
  explicit disconnect after application ACK observation; ACK immediately followed by
  a real connection drop may yield terminal failure, never a fabricated receipt.
  MQTT 5 remains clean-start-only and terminally fails interrupted accepted work.
  Fault-injected recovery is host TCP tested; other host transports have baseline
  roundtrip evidence, not transport-specific recovery proof.
  WS/WSS now use finite131080-byte frame/message limits,262160-byte logical write
  maximum and finite HTTP/control/read accumulation. A complete full-client aggregate
  allocation ceiling is not yet claimed; frontend metadata/encoding and all transient
  capacity terms still require the complete ledger.
- Cancellation abandons local waiting; it does not retract a sent MQTT packet.
  Force close/finalizer/owner exit remain immediate; optional drain is bounded.
- No disk outbox, cross-process recovery, end-to-end exactly-once, background-persistent
  iOS, mobile WSS or Flutter Web guarantee. Mobile TLS validation is incomplete.

## Maintainers

`native/` is a generated source snapshot, not a second implementation. Edit Rust in
the repository workspace; regenerate with `python3 scripts/sync-native.py` and verify
with `--check`. C header generation writes Cargo OUT_DIR; the explicit repository
`scripts/generate-bindings.sh` exports the header and regenerates ffigen bindings.
Package-local ffigen input is `native/bindings/c/include/mqtt_client.h`.

Package/install checks do not establish release readiness. Rust registry names,
registry versioned dependencies, native distribution/platform validation,
independent review and a complete redistribution notice audit remain outstanding.

### WebSocket resource limits

WS/WSS retained subscription expectations allow at most128 distinct topics and
262144 UTF-8 topic bytes. New topics exceeding a quota fail with native code 5
(QueueFull) before admission; updating existing topics remains allowed. Admitted
unsubscribe frees quota; cancellation only stops local waiting and does not undo
accepted expectations. Close clears retained keys. TCP/TLS behavior is unchanged.
This is not an aggregate WS memory cap: upstream frame/message, automatic control
write backlog and unconsumed-message buffers are not covered by MQTT payload
limits. Arbitrary hostile control floods are not proven safe. No hard RSS/process
limit, mobile WSS or Flutter Web claim is added.
