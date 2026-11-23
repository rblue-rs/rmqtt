# Independent WS resource probes

This unpublished nested diagnostic workspace uses the exact existing official
upstream versions. It changes no production dependency/features or WS framing.
Only the client-owned WS subscription metadata quota is production enforcement;
full WS aggregate memory remains unimplemented. See ../../docs/websocket-memory.md.

Build/run from repository root:

```sh
cargo build --manifest-path scripts/ws-resource-probe/Cargo.toml --locked --offline
cargo run --manifest-path scripts/ws-resource-probe/Cargo.toml --locked --offline
cargo run --manifest-path scripts/ws-resource-probe/Cargo.toml --locked --offline -- --bounded
cargo run --manifest-path scripts/ws-resource-probe/Cargo.toml --locked --offline -- --bounded --mutate-write
WS_PROBE_MUTATE_FRAME=1 cargo run --manifest-path scripts/ws-resource-probe/Cargo.toml --locked --offline -- --bounded
WS_PROBE_MUTATE_MESSAGE=1 cargo run --manifest-path scripts/ws-resource-probe/Cargo.toml --locked --offline -- --bounded
cargo run --manifest-path scripts/ws-resource-probe/Cargo.toml --locked --offline -- --layers
cargo run --manifest-path scripts/ws-resource-probe/Cargo.toml --locked --offline -- --layers --bounded
```

Defaults deliberately fail a2MiB requested-live **fixture** reference budget;
that reference is NOT a product limit or release claim. Counterfactual config is
not passed into rumqttc. Its selected-fixture success is not an aggregate proof.
WS_PROBE_PINGS sets the generated finite PING count (default 32768).

Allocator wrapper forwards System pointers/Layout unchanged and counts requested
active Rust bytes plus largest allocation request, including Vec spare capacity;
no RSS samples. Realloc accounting uses post-call live size, not old+new overlap
inside the allocator. Synthetic wire bytes are generated with fixed-size metadata,
not an allocated multi-megabyte peer payload. Warm stdio/masking RNG/notifier paths
outside object-lifetime capture; source-owned state is measured from construction
through drop and release checked. Allocator metadata, fragmentation/realloc
transients, process/runtime/TLS/OS memory are not a hard-cap claim. Async bridge
oversize may terminate with EOF (MQTT Network7), not an in-band io capacity error.

Raw adversary (requires the project's Python certificate-test dependencies):

```sh
python scripts/ws-resource-probe/adversary.py \
  --binary scripts/ws-resource-probe/target/debug/ws-resource-probe \
  --output "$TMPDIR/ws-observations"
```

Choose a fresh output directory. It tests MQTT 3.1.1/5 x WS/WSS over loopback with
certificate+hostname verification. The Python peer is a scripted raw protocol
fixture, NOT a broker or a production framing implementation. It supports real
MQTT PINGREQ and positive readiness/replacement/DISCONNECT fences. Force-close may
produce TCP reset, not graceful DISCONNECT. Slow-consumer failure is synchronized
by observed peer EOF before the application drains its128 queued messages. Cases
include header oversize, large frame/fragment totals, one-byte WS messages,64
controls, queue saturation, blocked output, close/drop/drain during reassembly and
count/UTF-8-byte quota edges. Observed resource logs are not universal upper bounds.
Normal native broker success is covered separately by the host WS broker tests.
