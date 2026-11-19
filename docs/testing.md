# Linux host testing and current failure

This is an experimental Linux-host-tested MQTT3.1.1 TCP slice, not a mobile
release. **The complete host gate is RED.** Do not treat selected green tests as
release readiness or hide the known failing cleanup regression.

## Reproduce

Install Rust with rustfmt/clippy, a C compiler, Dart, libclang for regeneration,
and a Python virtual environment containing amqtt==0.12.2. In
`examples/flutter/mqtt_flutter`, run `dart pub get`, then at repository root:

```sh
MQTT_TEST_PYTHON="${VIRTUAL_ENV}/bin/python" scripts/test-host.sh
```

CARGO, DART, CC and MQTT_TEST_PYTHON override executable locations. The fixture
starts a temporary independent aMQTT broker on127.0.0.1 and an ephemeral port,
then stops it. No external account, real password, key or public broker is needed.
For bindings, build mqtt-c with cbindgen installed through its build dependency
and run scripts/generate-bindings.sh with libclang. Optional LIBCLANG_ROOT and
C_INCLUDE_DIR select LLVM and standard headers. Generated files are committed;
raw logs, local machine configuration, caches and build outputs are not.

## Recorded result — 2026-11-19 (Asia/Shanghai)

Linux x86_64, Rust1.100.0, Dart3.14.0, aMQTT0.12.2:

| Check | Exit | Result |
| --- | --- | --- |
| cargo fmt --all -- --check |0| PASS |
| cargo clippy --workspace --all-targets --locked -- -D warnings |0| PASS |
| cargo test --workspace --locked --no-fail-fast |0|22 PASS,0 FAIL,0 ignored |
| cargo test -p mqtt-c --locked --no-fail-fast |0|5 PASS (already included in22) |
| C11 smoke executable with ASan/UBSan and aMQTT |0|1 executable PASS |
| dart analyze --fatal-infos |0| No issues |
| dart test --concurrency=1 |1|39 tests:38 PASS,1 FAIL (owner-isolate exit cleanup) |
| operation_handle_test.dart |0|15 PASS; three consecutive focused runs |
| operation_handle_test.dart + lifecycle_test.dart |0|28 PASS |
| standalone isolate_exit_probe.dart |1| final shutdown expected0, actual Busy(4) |
| scripts/test-host.sh |1| only the owner-isolate cleanup regression fails |
| forced cbindgen build + ffigen regeneration |0| identical header/bindings hashes |

## Known failure is not skipped

`ffi_ownership_test.dart` runs `test/probes/isolate_exit_probe.dart` in a child OS
process. An owner isolate exits without close, leaking its native handle/runtime
lease; final shutdown returns Busy(4) instead of0. The parent expects successful
cleanup and therefore fails. No skip, ignore or should-fail annotation is used.
Do not run the deliberately leaking probe inside a long-lived production process.
Normal explicit close remains mandatory; no Finalizer or hot-restart/forced-death
reclamation guarantee is implemented.

## Coverage and limits

Close/poll tests cover Closed rather than Empty after close, retained-message
suppression after BufferTooSmall, and late producer rejection. Other tests cover
CONNACK/SUBACK/UNSUBACK gating, keepalive0 on wire and explicit1-second PING,
QoS1 redelivery, bounded command/event overflow, input/output ownership and binary
buffer retry, reconnect after disconnect, and live-client runtime shutdown refusal.

The15 new operation-handle tests cover immediate ids and pre-native local cancel,
late ACK isolation, independent first/joiner disconnect cancellation, repeated raw
cancel(id), first deadline preservation, reentrant close/cancel, committed results
before synchronous listeners, and bounded cancelled-join bookkeeping. Existing
publish-ticket tests retain Queued/Sent/ACK and QoS0 no-ACK behavior. Local once-only
settlement is not MQTT exactly-once delivery.

Peers: bounded scripted loopback wire peers and independent aMQTT0.12.2. A second
Mosquitto broker was not tested. C ASan/UBSan instruments the C executable, not
Rust dependencies. No TLS/mTLS, successful MQTT5, WS/WSS, auto reconnect, durable
recovery, drain, Miri/fuzz/TSan, full-Rust sanitizers, remote CI, Android/iOS cross-build,
Flutter plugin packaging, simulator, physical-device, signing or background result
is claimed. See device-smoke.md before making mobile claims.
