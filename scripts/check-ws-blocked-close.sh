#!/usr/bin/env bash
# Linux host-only public FFI lifecycle proof; opt-in diagnostics, not product ABI.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
dart="${DART:-dart}"
cargo build --offline -p mqtt-c --features ws-test-observer
libraries="${CARGO_TARGET_DIR:-$root/target}/proof-libraries"
mkdir -p "$libraries"
cp "${CARGO_TARGET_DIR:-$root/target}/debug/libmqtt_c.so" "$libraries/libmqtt_c_observer.so"
# Restore normal feature-off shared-library output before running the separate copy.
cargo build --offline -p mqtt-c
cd examples/flutter/mqtt_flutter
MQTT_NATIVE_LIBRARY="$libraries/libmqtt_c_observer.so" "$dart" test test/probes/ws_blocked_close_probe.dart --concurrency=1
