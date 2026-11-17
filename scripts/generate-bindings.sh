#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/examples/flutter/mqtt_flutter"
# System LLVM installs work with ffigen.yaml directly. This override is optional.
config=ffigen.yaml
if [[ -n "${LIBCLANG_ROOT:-}" ]]; then
  config="$(mktemp "$PWD/.ffigen-local-XXXXXX.yaml")"
  trap 'rm -f "$config"' EXIT
  cp ffigen.yaml "$config"
  printf '\nllvm-path:\n  - "%s"\n' "$LIBCLANG_ROOT" >> "$config"
fi
opts=()
if [[ -n "${C_INCLUDE_DIR:-}" ]]; then opts=(--compiler-opts "-I$C_INCLUDE_DIR"); fi
"${DART:-dart}" run ffigen --config "$config" "${opts[@]}"
