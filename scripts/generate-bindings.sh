#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/examples/flutter/mqtt_flutter"
# System LLVM installs work with ffigen.yaml directly. This override is optional.
if [[ -z "${C_INCLUDE_DIR:-}" ]]; then
  detected="$(${CC:-cc} -print-file-name=include 2>/dev/null || true)"
  if [[ -n "$detected" && "$detected" != "include" && -d "$detected" ]]; then
    C_INCLUDE_DIR="$detected"
  fi
fi
if [[ -z "${LIBCLANG_ROOT:-}" ]]; then
  for cand in /usr/lib/llvm-19 /usr/lib/llvm-18 /usr/lib/llvm-17 /usr/lib/llvm-16 /usr/lib/llvm-15; do
    if [[ -e "$cand/lib/libclang.so" || -e "$cand/lib/libclang.so.1" ]]; then
      LIBCLANG_ROOT="$cand"
      break
    fi
  done
fi
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
