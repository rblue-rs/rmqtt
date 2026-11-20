#!/usr/bin/env bash
# Local equivalent of .github/workflows/host-tests.yml (Linux job).
# Pin the same tool versions used by that workflow, then run the host gate
# and the binding-regeneration check. This is not a claim that GitHub Actions
# has already run.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
CARGO="${CARGO:-cargo}"
DART="${DART:-dart}"
PYTHON="${MQTT_TEST_PYTHON:-python3}"
export MQTT_TEST_PYTHON="$PYTHON"
export CARGO DART

# The Dart package pubspec depends on the Flutter SDK so that Android/iOS
# plugin packaging can be declared. Host tests still use `dart test`.
if [[ -z "${FLUTTER_ROOT:-}" ]] && command -v flutter >/dev/null 2>&1; then
  flutter_bin="$(command -v flutter)"
  maybe="$(cd "$(dirname "$flutter_bin")/.." && pwd)"
  if [[ -d "$maybe/packages/flutter" ]]; then
    FLUTTER_ROOT="$maybe"
  fi
fi
if [[ -n "${FLUTTER_ROOT:-}" ]]; then
  export FLUTTER_ROOT
  export PATH="$FLUTTER_ROOT/bin:$PATH"
fi
if [[ -z "${FLUTTER_ROOT:-}" || ! -d "${FLUTTER_ROOT}/packages/flutter" ]]; then
  echo "error: Flutter SDK 3.50.x is required for dart pub get (FLUTTER_ROOT or flutter on PATH)." >&2
  exit 1
fi

echo "== tool versions (expected: Rust 1.100.0, Dart 3.14.0, amqtt 0.12.2) =="
"$CARGO" --version
"$CARGO" fmt --version
if "$CARGO" clippy --version >/dev/null 2>&1; then
  "$CARGO" clippy --version
else
  rustup run 1.100.0 cargo clippy --version
fi
rustc --version
"$DART" --version
flutter --version | head -n 4
echo "FLUTTER_ROOT=${FLUTTER_ROOT:-}"
"$PYTHON" --version
"$PYTHON" -c "import amqtt,sys; print('amqtt', getattr(amqtt, '__version__', 'unknown'))"
"${CC:-cc}" --version | head -n 1
echo "LIBCLANG_ROOT=${LIBCLANG_ROOT:-}"
echo "C_INCLUDE_DIR=${C_INCLUDE_DIR:-}"

# Fail early if the pinned toolchain does not match, so a green local run
# cannot be confused with a different compiler.
rustc_ver="$(rustc --version)"
case "$rustc_ver" in
  *"1.100.0"*) ;;
  *)
    echo "error: rustc must be 1.100.0 (from rust-toolchain.toml); got: $rustc_ver" >&2
    exit 1
    ;;
esac
dart_ver="$("$DART" --version 2>&1 || true)"
case "$dart_ver" in
  *"3.14.0"*) ;;
  *)
    echo "error: dart must be 3.14.0; got: $dart_ver" >&2
    exit 1
    ;;
esac
amqtt_ver="$("$PYTHON" -c "import amqtt; print(getattr(amqtt, '__version__', ''))")"
if [[ "$amqtt_ver" != "0.12.2" ]]; then
  echo "error: amqtt must be 0.12.2; got: $amqtt_ver" >&2
  exit 1
fi

(cd "$ROOT/examples/flutter/mqtt_flutter" && "$DART" pub get)
"$ROOT/scripts/test-host.sh"
"$ROOT/scripts/check-bindings.sh"
echo "== ci-host: all Linux host-test steps passed locally =="
