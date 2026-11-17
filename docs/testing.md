# Host testing and validation limits

This is an experimental Linux-host-tested vertical slice, not a mobile release.

## Reproduce

Install Rust (rustfmt/clippy), a C compiler, Dart and a Python virtual environment
with `amqtt==0.12.2`. Run `dart pub get` in `examples/flutter/mqtt_flutter`.

```sh
MQTT_TEST_PYTHON=/path/to/venv/bin/python scripts/test-host.sh
```

`CARGO`, `DART`, `CC` and `MQTT_TEST_PYTHON` can override executable locations.
The script starts a temporary independent aMQTT broker on 127.0.0.1 and an
ephemeral port, runs its checks, and stops the broker. No external MQTT account,
public broker, real password, certificate, or private key is bundled.

For binding regeneration install libclang and run `scripts/generate-bindings.sh`;
optional `LIBCLANG_ROOT` and `C_INCLUDE_DIR` override LLVM/standard header locations.
Generated C and Dart bindings are included deliberately; build outputs and raw
machine-specific test logs are not part of the public source distribution.

## Recorded host result

2026-11-17 (Asia/Shanghai), Linux x86_64, Rust 1.100.0 and Dart 3.14.0:

- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `cargo test --workspace --locked`: 13 tests passed.
- `cargo build -p mqtt-c --locked`: passed on Linux.
- C11 smoke executable with ASan/UBSan: passed against real aMQTT.
- `dart analyze --fatal-infos`: no issues.
- `dart test --concurrency=1`: 8 tests passed, including real Dart/C/Rust/broker
  messaging, three Dart isolates, listener-triggered close, late PUBACK and
  bounded pause/backpressure overflow.
- C/Dart binding regeneration reproduced identical content.

Wire-level tests also check CONNACK/SUBACK gating, keepalive=0 on CONNECT,
absence of default protocol PING over an observation window, explicit 1-second
PING, QoS1 PUBACK gating, bounded requests and fatal overflow wakeup.

These are recorded local results, not a claim that remote CI has run. C executable
sanitizers do not mean Rust dependencies were sanitizer-instrumented or that
arbitrary invalid C pointers can be made safe.

## Not run / not implemented

No TLS/mTLS certificate or hostname-validation test, successful MQTT5 engine,
WS/WSS, automatic reconnect/session recovery, graceful close drain, fuzz/Miri/TSan,
Android/iOS cross-build, Flutter packaging, simulator, physical device, signing,
background-execution, remote CI or independent production security review is
claimed. See `device-smoke.md` and `semantics.md` before using the API.
