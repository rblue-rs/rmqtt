# ADR 0001 — caller-owned polling event bridge

Status: **Accepted**. Experimental ABI; not frozen across versions.
Scope: Linux-host-validated MQTT3.1.1 TCP slice. Android/iOS remain intended
platforms, not validated platforms.

## Decision

Use synchronous caller-owned polling. Native never calls Dart or waits for a Dart
callback. A `NativeCallable.listener` trampoline returns before Dart necessarily
runs; therefore freeing a borrowed event buffer at trampoline return can cause
use-after-free. Do not restore that borrowed-buffer listener design.

Owned events with explicit release and scalar wake notifications are not offered
in this implementation. They would require separate lifetime/quota design and
must not be required for correctness of the current polling path.

## Contract

- C metadata is repr(C), fixed-width scalars and size_t lengths. There are no
  Rust enums, references or allocator-owned event pointers across the boundary.
  cbindgen/ffigen generate the header/bindings; do not hand-edit generated files.
- Native copies accepted inputs synchronously. Caller output buffers stay valid,
  aligned, writable and nonoverlapping for the entire poll. Native retains none.
- Empty leaves output untouched. BufferTooSmall writes required metadata only,
  retaining the same event. Success copies one event and then removes it.
- Handles are monotonically increasing nonzero u64 table keys, never reused in
  the process. At most64 entries; free is idempotent and a live nonclosed handle
  cannot be freed. Closed handles reject valid operations except close/free.
- Native op_id is nonzero, strictly increasing per client and must never wrap or
  be reused, even after attempted enqueue failures. ACKs correlate by operation
  and packet id. Dart integer-exhaustion guarding remains an implementation gap.
- Same-handle calls serialize through the C handle-table mutex. One Dart isolate
  owns each wrapper and poll consumer. Poll copies before user listeners run.
- Force close immediately seals the handle. Queued events are discarded; late
  producers cannot escape through poll. Release order: stop poll, native close,
  native free, caller buffers. No drain or delivery guarantee is introduced.
- Disconnect leaves the client reusable; concurrent callers share one physical
  attempt and its first deadline. Each Dart caller has an independent wait.
  Cancelling a caller never withdraws MQTT packets or cancels another wait or the
  physical disconnect deadline. Timeout force-stops transport where documented.
- Dart MqttOperation<T> exposes an immediate operationId, future and idempotent
  cancel; beginX methods are additive. Existing Future methods delegate. Error
  types remain StateError / TimeoutException / MqttOperationCancelled or native
  MqttException(code); no native cancel/reset export or Cancelled code exists.
- Runtime is process-wide, with paired initialization references. Last shutdown
  refuses live client leases; runtime-worker shutdown is Busy. Never wait for a
  Dart callback or block_on a runtime worker during destruction.
- Bounds:32 pending commands, one active protocol operation,128 events plus a
  terminal fatal slot,64KiB payloads,16 Dart listeners per stream. Native distinct
  disconnect completion ids and Dart retained join continuations are each bounded
  at32; cancelled joins retain their slot until the shared attempt terminates.
- catch_unwind maps unwinding panics to Internal, not abort/OOM/UB or bad pointers.

## Limitations

Explicit close is mandatory. Exiting an owner isolate without close currently
leaks a handle/runtime lease; the child-process cleanup test remains failing.
No Finalizer, forced-isolate-exit or hot-restart reclamation guarantee exists.
Local once-only Future settlement is not MQTT exactly-once delivery. QoS0 has no
acknowledgement, Sent is not remote receipt, and PUBACK is not application processing.

Keep rumqttc0.25.1 / Tokio / rustls / cbindgen / ffigen without an upstream fork.
Default keepAlivePeriod is0. MQTT5 is typed Unsupported, never silently rewritten
to a different keepalive. No automatic reconnect, durable outbox, cross-process
QoS recovery, TLS or mobile validation is implied. See [semantics](../semantics.md)
and [testing](../testing.md) for current behavior and the known failing gate.
