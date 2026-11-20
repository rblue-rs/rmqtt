# Linux host testing and validation limits

Experimental Linux-host-tested MQTT 3.1.1 over TCP and host TLS/mTLS. The
complete host gate passes: **Rust 76/76, Dart 76/76, C ASan/UBSan smoke 1/1**,
exit 0. This is not a stable ABI, physical-device support, release readiness,
or a security certification. A local pass is not a GitHub Actions result.

## Reproduce

Install Rust 1.100.0 with rustfmt/clippy (`rust-toolchain.toml`), a C compiler,
Dart 3.14.0, Flutter 3.50.x (required for `dart pub get` because the Dart
package declares a Flutter FFI plugin), libclang for regeneration, and a Python
virtual environment containing `amqtt==0.12.2`. Then at the repository root:

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

Linux x86_64, 2026-11-21 (Asia/Shanghai), Rust 1.100.0, Dart 3.14.0, Flutter
3.50.1, Python 3.13, amqtt 0.12.2, rumqttc 0.25.1:

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo clippy --workspace --all-targets -- -D warnings` | PASS |
| `cargo test --workspace --locked` | 76 passed / 0 failed / 0 ignored |
| C11 executable with ASan/UBSan and aMQTT | PASS (1 fixture) |
| `dart analyze --fatal-infos` | no issues |
| `dart test --concurrency=1` | 76 passed / 0 failed |
| `scripts/check-bindings.sh` | header and Dart bindings hash-identical after regeneration |
| `scripts/ci-host.sh` / `scripts/test-host.sh` | exit 0 |

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
backpressure coverage remains. Local once-only settlement is not MQTT
exactly-once delivery. C sanitizer flags instrument the C executable, not the
full Rust dependency tree.

## Continuous integration — not yet observed on GitHub

`.github/workflows/host-tests.yml` is the Linux host job on `ubuntu-24.04`. It
installs the pinned tools above, with no repository secrets, then runs the same
steps as `scripts/ci-host.sh`. A local pass is not a GitHub Actions result. No
workflow badge is published until the job has actually run green on GitHub.
There is no macOS host-tests job: the C smoke test and Dart native library path
in `scripts/test-host.sh` are Linux `.so` / rpath specific.

A separate `.github/workflows/ios-buildable.yml` job is documented in
[device-smoke.md](device-smoke.md) and is also pending a real macOS/CI run.

## Not verified

No whole-Rust ASan, TSan, Miri, fuzzing, or second-broker Mosquitto result is
claimed. No successful MQTT 5 engine, WS/WSS, automatic reconnect, durable
recovery, or draining close is claimed. Android `arm64-v8a` has local
cross-build / debug-APK evidence only; iOS `arm64` project files are prepared
but not built on Linux. No emulator, simulator, or physical-device TLS result
exists. See [mobile gates](device-smoke.md) and [ownership](ownership.md).
