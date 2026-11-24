# FFI ownership and safety limits

## Accepted bridge: caller-owned polling

[ADR 0001](adr/0001-ffi-event-bridge.md) selects caller-owned synchronous polling,
not a native-to-Dart callback. A borrowed buffer freed when a
`NativeCallable.listener` trampoline returns is unsafe: Dart may run later.
Native must not wait for Dart callback execution to repair that lifetime.
Reference: https://api.dart.dev/dart-ffi/NativeCallable/NativeCallable.listener.html

- `mqtt_client_poll` copies metadata/topic/payload into caller-owned buffers.
  Metadata contains scalars and lengths, not borrowed Rust references.
- Native copies accepted inputs before return. Poll itself retains no output pointers;
  a separate ownership registration retains caller-buffer addresses only for cleanup.
- Empty preserves all output bytes. BufferTooSmall updates required metadata,
  preserves the event for retry, and leaves topic/payload buffers untouched.
- Handles are nonzero monotonically increasing numeric keys behind a mutex, not
  Rust pointers; stale valid operations return Closed. Free requires close.
- Force close seals the handle immediately, discards queued events and prevents
  later producer events from escaping via poll. Repeated close/free are harmless.
- Dart release order is stop polling -> native close -> native free -> caller buffers.
  Optional explicit drain retains control completions before this release order;
  it never adds a callback handshake. See [drain close](drain-close.md).
- One isolate owns each wrapper, timer, buffers and poll consumer. Independent
  clients may live in separate isolates; do not share raw wrapper/buffer pointers.
- Explicit close is preferred. Weak Timer callbacks do not root an unreachable client.
  NativeFinalizer is GC/group-shutdown fallback; a native port receives only a scalar
  owner-exit id. Cleanup removes each token once, then close/free/matching nativeFree
  outside the registry lock. It never re-enters Dart or blocks runtime destruction
  on a worker. Retiring resources still consume quota until actual deallocation.
- Completed connection/registration exit paths have Linux tests. Partial setup kill,
  process/VM crash, hot restart, forced library unload and mobile are not guaranteed.
  Native notification waiting is bounded separately from explicit runtime shutdown.
  Tokio background shutdown may leave blocking/DNS work running until it returns.
  See [ownership](ownership.md) for the exact boundary and memory accounting.

## Bounds and concurrency

Native has 32 pending commands plus one active protocol operation, 128 events
plus a terminal fatal slot, a 64KiB payload maximum and at most 64 handles.
Overflow terminates the connection, preserves accepted events, and never silently
evicts control events. Dart polls at most64 events per turn and admits16 listeners
per stream. Normally pausing a listener halts the client's poll and can cause overflow.
Explicit drain instead polls finite retained control events even if streams pause,
discarding inbound delivery. No application consumption is awaited.
Coalesced disconnect completion ids and Dart retained join continuations are each
bounded at32. Cancelled Dart joins retain their slot until the physical attempt ends.

Poll does not wait for network/events, but mutex acquisition can contend. This is
not lock-free or wait-free. The C table mutex linearizes concurrent handle calls;
callers still must not race output buffers. Bounds do not imply small worst-case memory.

## Panic and pointer limitations

Unwinding panics map to Internal(11). Abort/OOM, invalid pointers, undefined
behavior and segfaults are not caught. Callers must satisfy readable/writable,
length, alignment and nonoverlap preconditions in the generated header. Detecting
null/invalid lengths does not validate arbitrary addresses. C smoke ASan/UBSan
instruments the C executable, not the entire Rust dependency tree. This is an
experimental ABI, not a release or security certification.
