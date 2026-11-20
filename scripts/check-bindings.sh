#!/usr/bin/env bash
# Regenerate the committed C header and Dart ffigen bindings, then fail if
# either file differs. Used by the Linux host-tests workflow and by
# scripts/ci-host.sh. Does not require git.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
CARGO="${CARGO:-cargo}"
HEADER="bindings/c/include/mqtt_client.h"
BINDINGS="examples/flutter/mqtt_flutter/lib/src/ffi_bindings.dart"
before_header="$(sha256sum "$HEADER")"
before_bindings="$(sha256sum "$BINDINGS")"
"$CARGO" build -p mqtt-c --locked
"$ROOT/scripts/generate-bindings.sh"
after_header="$(sha256sum "$HEADER")"
after_bindings="$(sha256sum "$BINDINGS")"
status=0
if [[ "$before_header" != "$after_header" ]]; then
  echo "Generated C header differs from the committed copy:" >&2
  echo "  before: $before_header" >&2
  echo "  after:  $after_header" >&2
  status=1
fi
if [[ "$before_bindings" != "$after_bindings" ]]; then
  echo "Generated Dart bindings differ from the committed copy:" >&2
  echo "  before: $before_bindings" >&2
  echo "  after:  $after_bindings" >&2
  status=1
fi
if [[ "$status" -ne 0 ]]; then
  echo "Committed bindings are stale. Re-run scripts/generate-bindings.sh (and cargo build -p mqtt-c) and commit the result." >&2
  exit "$status"
fi
echo "Binding regeneration is consistent:"
echo "  $after_header"
echo "  $after_bindings"
