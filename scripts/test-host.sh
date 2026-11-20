#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
CARGO="${CARGO:-cargo}"
DART="${DART:-dart}"
PYTHON="${MQTT_TEST_PYTHON:-python3}"
export MQTT_TEST_PYTHON="$PYTHON"
"$PYTHON" scripts/test_build_support.py
"$CARGO" fmt --all -- --check
"$CARGO" clippy --workspace --all-targets -- -D warnings
"$CARGO" test --workspace --locked
"$CARGO" build -p mqtt-c --locked
# Linux host fixture; deliberately no Android/iOS claims.
"${CC:-cc}" -std=c11 -Wall -Wextra -Werror -fsanitize=address,undefined \
 -I bindings/c/include tests/c/smoke.c -L target/debug -lmqtt_c \
 -Wl,-rpath,"$ROOT/target/debug" -o target/c-smoke
"$PYTHON" scripts/with_broker.py target/c-smoke
cd examples/flutter/mqtt_flutter
"$DART" analyze --fatal-infos
MQTT_NATIVE_LIBRARY="$ROOT/target/debug/libmqtt_c.so" \
 "$PYTHON" "$ROOT/scripts/with_broker.py" "$DART" test --concurrency=1
