#!/usr/bin/env bash
# Host VM-only mirrors fixture stays outside Flutter source/analyzer target.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root/examples/flutter/mqtt_flutter"
"${DART:-dart}" test "$root/scripts/probes/library_trace_probe.dart" --concurrency=1
