#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# Build scripts emit headers in OUT_DIR; only this explicit developer command exports.
metadata="$(mktemp)"
"${CARGO:-cargo}" build -p mqtt-c --locked --message-format=json > "$metadata"
python3 - "$metadata" "$ROOT/bindings/c/include/mqtt_client.h" <<'PY_HEADER'
import json
from pathlib import Path
import shutil
import sys
headers = []
for line in Path(sys.argv[1]).read_text().splitlines():
    item = json.loads(line)
    if item.get('reason') == 'build-script-executed':
        path = Path(item['out_dir']) / 'mqtt_client.h'
        if path.is_file():
            headers.append(path)
if len(headers) != 1:
    raise SystemExit('Expected one generated mqtt_client.h, got ' + str(headers))
shutil.copyfile(headers[0], sys.argv[2])
PY_HEADER
rm -f "$metadata"
python3 "$ROOT/scripts/sync-native.py"
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
