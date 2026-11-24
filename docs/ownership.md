# Poll ownership, isolate cleanup and backpressure

Experimental Linux-host behavior only. The public Dart API remains unchanged.
Explicit close is the primary deterministic path; force close is not drain.
Optional [drainClose](drain-close.md) is a separate bounded explicit operation;
owner exit/finalizers never use it.

## Ownership

`lib/src/native_client.dart` owns one numeric handle and three independent calloc
allocations: MqttEvent,65535 topic bytes,65536 payload bytes. The native ownership
registration receives `calloc.nativeFree`, so allocation and deallocation use the
same native allocator, including platforms where that is not libc free. Native
retains only this cleanup record, not a view into an event. Poll synchronously
copies queued events into caller-owned destinations; Dart copies payload bytes
before exposing them to listeners. No borrowed queue pointer crosses the bridge.

A Finalizable NativeClient and a static NativeFinalizer hold the registration.
Explicit release detaches first, then removes the numeric token once. Native
cleanup serializes against poll via the existing handle-table mutex and performs
close -> free -> matching deallocation. Owner-exit and client/owner finalizers may
race; stale tokens are no-ops, not freed pointer dereferences. A retiring allocation
continues consuming its quota until deallocation actually finishes.

`lib/src/event_pump.dart` holds a weak callback, not a strong root to MqttClient.
There is at most one scheduled timer. Idle/paused turns back off5..80ms; work,
API requests and resume reset scheduling. These are implementation delays, not a
latency/throughput SLA. Close stops the pump before releasing native resources.

## Owner exits and runtime refs

NativeFinalizer guarantees finalization by normal **isolate-group** shutdown, not
synchronous completion at each individual isolate exit. It must not be the sole
owner-exit mechanism. A runtime owner registers Isolate.addOnExitListener with a
process-wide **native port**, carrying only a numeric owner id. The receiver runs
native code, never a Dart cleanup callback. No pointers or MQTT events travel on
this control channel. Initialization completes an event-loop registration turn
before creating client resources; it does not make native code wait for Dart.
That turn is not a documented VM acknowledgment protocol: readiness is a tested
precondition, not a guarantee against every exit/listener-registration race.

Each owner acquires exactly one runtime init reference. An exit notification or
owner Finalizer removes that owner and all its registrations once. Inaccessible
owner cleanup closes/free clients, releases buffers and uses Tokio background
shutdown for the last reference, without block_on or waiting on a runtime worker.
Explicit shutdown retains the ordinary live-lease and worker Busy guards and the
existing bounded shutdown behavior. Do not pair plain-C runtime_shutdown calls
with Dart-owned init references. Plain-C callers continue pairing their own init.

The plain-C shutdown entry allows up to2seconds for a native exit receiver racing
another isolate's final result to finish, before checking runtime leases. This
wait uses only a native condition variable, not a Dart callback or network drain.
If native notification dispatch is delayed beyond that bound, shutdown may still
return Busy and can be retried; it is not proof of universal finalizer scheduling.

Tokio shutdown_background does not wait for blocking work. Resolver/blocking tasks
may outlive the callback until they return; no immediate thread/RSS reclamation,
network packet withdrawal, broker receipt or graceful drainage is guaranteed.

| Exit path | Supported boundary / limitation |
| --- | --- |
| explicit close, repeated close | deterministic local release; pending waits settle once |
| Isolate.run / Isolate.exit | tested after connect completes and ownership setup is registered |
| natural entrypoint return | tested after close stops timers, even without explicit runtime shutdown |
| init-only owner exit | tested after ensureInitialized completes, including concurrent owners |
| VM kill immediate / beforeNextEvent | tested after connect readiness; no Dart finally is required |
| unreachable client / GC | fallback tested with pressure; GC/finalizer timing is not promised |
| kill during partial initialization, reconnect allocation or incomplete registration | not guaranteed; do not infer arbitrary-kill safety |
| process crash, OS kill, dart:io exit, VM abort, forced engine/library teardown | cleanup is not guaranteed |
| Flutter hot restart, Android/iOS lifecycle | not verified; no mobile guarantee |

A running poll timer can keep an isolate alive. Merely returning while keeping a
live MQTT connection is not equivalent to the VM actually terminating its owner.

Host exit-cleanup probes observe this pairing first: after the owner isolate has
exited, they retry `mqtt_client_new` until it returns `NotInitialized` (2), or
until a two-second native-dispatch window elapses. A temporary handle created
while the runtime is still up is closed and freed immediately. Only then do the
probes check that `mqtt_runtime_shutdown` is already idempotent. They must not
call shutdown first, because that would consume a single leftover owner init
ref and still look green. Public close remains force-close; this is not a drain
API.

## Backpressure and limits

| Condition | Behavior |
| --- | --- |
| any subscription paused | stop the entire client's poll; native bounded queues absorb only their capacity |
| resume | restart polling; retained events are delivered in order |
| native queue full | preserve already queued messages/control completions; one terminal Overflow and disconnect |
| force close | discard the queue and reject subsequent operations as Closed; no drain promise |
| listener17 | reject with StateError; no unbounded Dart listener set |
| payload65537 | InvalidArgument; not silently truncated |
| payload65536 | tested end-to-end; wire packet bound includes topic and MQTT framing |

Bounds are64 native handles,64 owner registrations,64 accepted caller-buffer sets
(including retiring ones),32 pending commands,128 normal events plus one fatal slot,
and16 listeners per stream. Linux MqttEvent is40bytes, so one caller-buffer set is
131111bytes;64 sets account for8391104bytes. Temporary pre-transfer allocations,
Rust/VM objects, networking and application-retained Dart copies are additional.
Each queued event's payload is at most65536bytes and topic at most65535bytes;
128 such raw pairs can total16777088bytes per client before allocator/object costs.
A command queue can additionally hold32 payloads (2097152bytes), plus an active
command, clones and the four-slot upstream request channel. Application-created
pending Futures/retained messages are not capped by the event-queue limit. Multiplying
across64 clients is substantial; these are not small-memory or full-process RSS
upper-bound claims. MQTT wire packet cap is131080bytes to include bounded framing.

The native quota regression stalls a deallocator and verifies retiring buffers
still count, then races poll, release and both fallback callbacks. The controlled
pause test retains SUBACK and Queued/Sent/PUBACK, then124 messages, then Overflow:
128 normal slots total. Another test still preserves all128 message-only events.

See [testing](testing.md) for recorded totals and Linux RSS observations. Local once-only settlement is not MQTT exactly-once delivery. Host TLS/mTLS is documented separately; auto reconnect is off by default and is not persistence or physical-device validation.

## Official basis

- https://api.dart.dev/dart-ffi/NativeFinalizer-class.html
- https://api.dart.dev/dart-ffi/NativeFinalizer/NativeFinalizer.html
- https://api.dart.dev/dart-isolate/Isolate/addOnExitListener.html
- https://api.dart.dev/dart-isolate/Isolate/exit.html
- https://api.dart.dev/dart-isolate/Isolate/kill.html
- https://docs.rs/tokio/1.54.1/tokio/runtime/struct.Runtime.html#method.shutdown_background

The vendored official Dart API DL glue retains its BSD-style license. The exit
port is a Dart-to-native scalar control path, not a native-to-Dart event callback.
