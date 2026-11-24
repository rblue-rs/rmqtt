# Production native WS/WSS resource limits and aggregate audit status

Reachable finite constructors now apply to MQTT311 and5, WS and WSS in the owned
production driver. This removes the old private-hook blocker. It is not a total
RSS/process limit, and a completed full-client aggregate ceiling is NOT claimed.

| Owned layer | Enforced bound |
| --- | --- |
| User publish |65536 payload /65535 UTF-8 topic bytes |
| Root command channel |32 plus serial active work; engine request channel4 |
| Application event queue |128 plus out-of-band terminal failure; each Message topic/payload independently bounded; Overflow12 closes transport |
| WS desired subscription set |128 keys /262144 UTF-8 bytes, QueueFull5 before admission |
| QoS2 incoming unfinished transactions |128 ids /1048576 topic+payload bytes, exact boxed payload/topic retention |
| MQTT declared packet |131080 bytes, checked from Remaining Length before accumulating its body |
| WS declared frame / reassembled message |131080 bytes each; independent frame and fragmented total limits |
| WS read / write targets |8192 bytes each; raw read8192, forced Pending after4 ready reads |
| WS maximum write buffer |262160 logical bytes; backing Vec growth/realloc slack is additional and must be counted |
| MQTT unconsumed accumulation |Preallocated262160 bytes: prior packet tail plus one bounded WS message; checked before extension |
| HTTP response input |16384 bytes through the handshake wrapper; finite parse buffer/read-ahead accounting still part of audit |
| Current outbound transport write |393216-byte CONNECT envelope; WS chunks at most131080, persistent offset/start/flush state |
| Protocol writes / control fairness |Owned outbox at most4; iterative processing, explicit yield after32 Ping/Pong controls; finite automatic write buffer |
| Socket write / connection attempt |Absolute5s timeout; close/drain/owner abort does not wait indefinitely for network progress |

Rust options and admitted String/Vec commands discard caller-provided spare
capacity before retention. Native C argument validation borrows before the owner
mutex; owned copies happen under that mutex rather than allocating an unbounded
number of waiting FFI copies. Caller-supplied preexisting input allocations and
returned/caller-retained events must be distinguished from incremental owned
retention and normalization transients.

## Actual evidence and what it does NOT prove

Independent PUBLIC Client tests cover declared overlimit frame without a body,
fragmented/tiny-frame totals, exact128 slow-consumer failure,20,000 controls with a
read fence and close, and a real stalled TCP receive window through WS/WSS in both
protocols. The latter admits72 messages / flushes39 in the recorded fixture then
terminates on the5s write deadline, retaining only the bounded unsent window; this
is not a claim that every host socket has those kernel limits. TLS and OS buffers
are outside those native requested-byte observations.

The private selected allocator oracle retains its original2MiB fixture threshold
and maximum-allocation assertion. Its success is supplementary, NOT an aggregate
client cap. Default CI green is not memory proof.

## Compositional ledger: remaining work before an aggregate claim

Must sum every options copy (including CONNECT encoding), normalized command,
active/engine/pending/outbox wire clone, application event including one peek/FFI
copy, desired/confirmed/resubscribe snapshot and BTree metadata, QoS2 state,
BytesMut/codec property-vector amplification, frame/message/write capacities and
reallocation overlap, HTTP parse/header buffers and native frontend buffers.
Individual finite lengths do not alone establish that sum. In particular Dart
UTF-8 conversion currently occurs before some byte-length rejection, and bursts
of asynchronous frontend control launches need a retained-metadata admission audit.
Those cannot be hidden by calling all Dart library allocations “runtime”.

The external scope must separately identify caller-owned data and returned values,
Dart VM object/runtime overhead, Tokio/DNS/task stacks, OS socket buffers,
TLS/certificate/crypto/provider allocations, allocator rounding/bookkeeping and
fragmentation. No hard numeric aggregate ceiling is advertised until that complete
ledger and its allocation/transient bounds are checked. These are precise remaining
integration/proof tasks, NOT a request for another engine-strategy approval.

The unchanged `scripts/ws-resource-probe` examines the former upstream-default
bridge and its counterfactuals. Its default RED result is a labeled upstream
reference, not a failing production-driver gate and not rewritten as green.

### Dart control launch admission
Connect, subscribe, unsubscribe and joined-disconnect share32 retained launch
slots. Admission precedes completion/map registration and Future scheduling.
Local cancellation/deadline/map removal does not itself release a slot: the
original launch continuation releases it once. Native wire transaction ownership
is separately bounded and can outlive a local wait. QueueFull remains code5 and
beginX still returns an independently identified handle. A rejected caller-owned
lazy error Future has no client/launch reference and schedules no work until the
caller listens; listeners execute in the caller zone. Convenience methods forward
that Future without an extra async state machine. Caller-retained handles/Futures
and user-created listeners are not client-owned queues. This is NOT an aggregate
heap bound: byte backing capacities, copies, other frontend paths and blocked-write
closure still need the complete ownership ledger and proof.

### Will ownership and binary field boundary
Will payloads are at most65535 bytes (MQTT's two-byte Binary Data field),
not the65536-byte ordinary PUBLISH limit. Rust validation and C borrowed-input
validation reject65536 with InvalidArgument1 before encoding/network/copy. Dart
MqttWill validates before making a detached fixed-length payload snapshot; a tiny
Uint8List view cannot keep an arbitrarily large caller buffer through that object.
Mutating the original caller list after construction no longer changes the Will.
The payload getter still exposes the bounded snapshot, not an immutable list.
Will QoS2 remains Unsupported; publish payload65536 is unchanged.

### Retiring supervisor lifetime admission
All native transports/protocols share64 admitted/live/retiring supervisor generations
per linked core. Close/free/owner exit request abort immediately but do not return
a generation permit until the actual captured future (including never-polled task)
and its owned allocation drop. Replacements need a fresh permit while old generations
retire. Exhaustion returns existing QueueFull(5), without background retry/waiting.
This can transiently reject a new connect after closing/freeing handles while runtime
workers are delayed; retry is caller policy. It is not a64-process-object/RSS cap.
See client-owned-capacity-ledger.md for shared registry/synchronous input parameters.
