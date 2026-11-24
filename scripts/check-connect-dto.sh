#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root/examples/flutter/mqtt_flutter"
"${DART:-dart}" test "$root/scripts/probes/connect_dto_probe.dart" --concurrency=1
