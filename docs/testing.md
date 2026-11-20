# Linux host testing and validation limits

Experimental Linux-host-tested MQTT3.1.1 TCP only. The complete host gate passes:
**Rust24/24, Dart51/51, C ASan/UBSan smoke1/1**, exit0. This is not stable ABI,
mobile support, release readiness or a security certification.

## Reproduce

Install Rust with rustfmt/clippy, a C compiler, Dart, libclang for regeneration,
and a Python virtual environment containing amqtt==0.12.2. Run `dart pub get`
in examples/flutter/mqtt_flutter, then at repository root:

```sh
MQTT_TEST_PYTHON="${VIRTUAL_ENV}/bin/python" scripts/test-host.sh
cargo test --workspace --locked --no-fail-fast
scripts/generate-bindings.sh
```

CARGO, DART, CC, MQTT_TEST_PYTHON override executable locations. LIBCLANG_ROOT
and C_INCLUDE_DIR select LLVM/C headers for ffigen. The fixture starts and stops
an independent temporary aMQTT broker on127.0.0.1 with an ephemeral port. No real
password, public broker or external account is required. Logs/caches/build output
and local machine configuration are not distributed with this source snapshot.
Official Dart API DL glue is vendored with its license; building C does not
require linking a Dart VM library. Generated bindings are not hand-maintained.

## Recorded Linux result

Linux x86_64, Rust1.100.0, Dart3.14.0, amqtt0.12.2, rumqttc0.25.1/Tokio1.54.1:

| Check | Exit | Result |
| --- | --- | --- |
| cargo fmt --all -- --check |0|PASS|
| cargo clippy --workspace --all-targets --locked -- -D warnings |0|PASS|
| cargo test --workspace --locked --no-fail-fast |0|24pass/0fail/0ignored|
| C11 executable with ASan/UBSan and aMQTT |0|1 fixture passed|
| dart analyze --fatal-infos |0|No issues|
| dart test --concurrency=1 |0|51pass/0fail|
| operation_handle_test.dart + lifecycle_test.dart |0|28pass (15+13), included in51|
| standalone isolate_exit_probe.dart |0|shutdown expected0/actual0|
| scripts/test-host.sh |0|Complete host gate passes|
| clean cbindgen build + ffigen |0|Stable generated header/bindings hashes|

The existing owner-isolate regression now passes: the assertion still expects
successful cleanup; no skip, ignore, inverted expectation or should-fail marker
was introduced. Seven subprocess modes cover completed-setup Isolate.run exit,
quiescent natural return after repeated close, init-only exits, both VM kill
priorities, concurrent mixed owners, and GC pressure. GC scheduling has no deadline
promise. Some exit probes call shutdown before observing NotInitialized, so they
alone do not prove automatic pairing of a single owner init ref; observer-only
coverage remains a useful follow-up test-strengthening opportunity.

Native tests cover64-owner and64-buffer quotas, retiring buffers while deallocation
is blocked,32 rounds of poll/release/finalizer races with exactly3 frees per buffer
set, and safe last-ref background release from a runtime worker. Output buffers
used by race tests remain independently valid. This is not permission to read a
caller allocation after release.

Pause/backpressure retains queued SUBACK, Queued/Sent/PUBACK and124 messages before
terminal Overflow (128normal slots); existing message-only128-slot coverage also
remains. Payload65537 is rejected;65536 is sent and loopback received intact.
Wire cap131080 includes topic/framing while application payload cap stays65536.
Three scheduling/listener tests cover idle backoff, reentrant wake, and listener17
rejection. Existing close/poll Closed semantics, late ACK/timeout/cancel isolation,
keepalive0 encoding, QoS0 no-ACK and QoS1 redelivery remain covered. Local once-only
settlement is not MQTT exactly-once delivery.

## Finite memory observation

One Linux VM/JIT sample used128 sequential owners,64 explicit closes and64 implicit
exits. RSS after1/16/32/64/96/128 owners was184053760/185348096/185749504/186818560/
186114048/186892288 bytes, peak254337024. Shutdown returned0 and subsequent
client_new returned NotInitialized2. This is an observation, not a leak proof,
RSS ceiling or cleanup-latency guarantee. Deterministic native free/quota counting
is separate evidence. See [ownership](ownership.md) for allocation accounting.

## Not verified

C sanitizer flags instrument the C executable, not the full Rust dependency tree.
No whole-Rust ASan, TSan, Miri, fuzzing, OOM injection, remote CI, second Mosquitto,
TLS/mTLS, successful MQTT5, WS/WSS, auto reconnect, durable recovery or drain result
is claimed. No Android/iOS cross-build, static-plugin multi-link, AOT finalizer,
Flutter hot restart, engine teardown, simulator or physical-device result exists.
Initialization/registration-midway kill and process/VM crash are not guaranteed;
Tokio blocking/DNS work may outlive background shutdown. Explicit close remains
preferred. See [mobile gates](device-smoke.md) and [ownership](ownership.md).
