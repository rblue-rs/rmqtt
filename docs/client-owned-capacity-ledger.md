# WS/WSS client-owned capacity ledger — final inventory, qualified model

**No unconditional whole-client byte cap is claimed.** This inventory closes the
previous open-ended investigation into the exact three residual obligations below.
Ordinary native311/5 WS/WSS use the owned driver, not rumqttc EventLoop. Public
blocked-write close and deterministic ownerexit are proven separately; GC-triggered
finalizer remains unproven and is not a memory-accounting blocker by itself.

## Meaning of bytes and ownership
Requested backing/capacity, not RSS or allocator usable-size. Every own Vec/String,
frame/message/HTTP/codec copy and map storage is included. Exclude ONLY OS/kernel,
TLS-provider crypto/config/parsed cert/root caches, VM/Tokio execution stack/runtime
baseline and allocator headers/rounding. Opaque library-held input graphs and own
SDK Futures are NOT excluded. Caller-returned handles/added listeners are external
parameters only when library is no longer retaining them; pending listener records
inside result cells are charged as K_user typed records.

Pinned audit: Rust1.100.0, Dart3.14.0 native VM, bytes1.12.1, tungstenite0.26.2,
async-tungstenite0.29.1, tokio1.54.1, rumqttc0.25.1 public codecs. Other compiler/VM
layouts require a recheck, not silently the same physical-byte claim.

## Constants / retained and transient native envelope
P=131080 MQTT/frame/message; A=131071 max topic+payload;
C=393216 max CONNECT/send; H=16384 HTTP raw input;
S=591866 total normalized Options byte contents;
D=1048576 inbound QoS2 dedup boxed bytes; E=262144 subscription topic bytes.
Rust input normalization boxes variable fields; by-value offered spare backing E_in
is included in synchronous handoff peak, then detached. E_in is NOT retained after
admission and is not a fixed-size bound before the library can inspect input.

|component, including concurrent copies|conservative backing term|bytes|
|---|---:|---:|
|native FIFO128 entries|128A|16777088|
|command32 +active1 +engine rx4 +handoff/clone slack3|40A|5242840|
|engine outbox4, including CONNECT (NOT merely4P)|4C|1572864|
|pending publish/QoS2 original/replay temporary copies|8P|1048640|
|transport MQTT accumulation, WS input/pins/reallocation, collector, output copies|48P|6291840|
|codec property vectors/strings/input/old+new allocation peak, four decode/encode contexts conservatively charged|1024P|134225920|
|actor/engine/C options and normalization/construction transient copies|12S|7102392|
|inbound dedup boxes plus decode/delivery duplicate copies|4D|4194304|
|expectations128 +resub snapshot/deque/active/construction copies|8E|2097152|
|HTTP buffer/request/response/tail and header string backing copies|16H|262144|
|C peek-event clone, fixed native bridge, current Dart handoff input copy slack|4A|524284|
|TOTAL of this native backing MODEL| |179339468 (171.032MiB)|

This is a deliberately loose **source-audit model**, not a configured179339468-byte
allocation counter or a proven complete Dart-client ceiling. Typed metadata below
must be added. The model's growth assumptions/proof obligations are explicit,
not a sum pretending every length equals capacity.

### Growth/pinning induction (locked sources)
Transport.data is allocated2P and cannot extend past2P; returned packet<=P coexists.
Pending write<=C, each WS chunk<=P; engine wire, chunk and formatted out-buffer
coexist. Std Vec growth in this pinned toolchain requests max(2*old,required,min).

FrameCodec.read_frame checks declared<=P BEFORE reserve. At a reserve site its
remaining logical input<P (otherwise complete frame returned); extra request<=P.
bytes reserve_inner unique Vec/Arc cases reclaim if offset>=len and capacity covers
len+additional. If offset<len, required underlying size<3P. Once original backing
>=3P neither case requires growth; a last geometric growth from below3P stays<6P.
Shared nonunique replacement requests max(len+additional, original_capacity_repr),
whose representation is floor/clamped, never an arbitrary upward amplification.
Use8P per complete underlying allocation (includes8192 initial/read slack).
Current input +old pinned PONG/frame +replacement transient: conservatively24P.
Do NOT charge a tiny PONG as125 bytes: it can pin an8P allocation. Only one
additional_send Frame, not a Pong queue. Binary/text incomplete collector grows
AFTER aggregate<=P check: capacity<=2P; old+new realloc<=3P. Output length<=2P
before reserve/format: capacity<=4P, growth overlap<=6P. Remaining48P slack covers
MQTT2P, returned/current packet, chunk/PendingWrite and these overlaps; C pending
wire is charged also in write/options slack, deliberately double counted.

Codec byte/property lengths are checked against wireP before reading fields.
Each property/list element consumes>=1 wire byte: total elements<=P. Property
Vec elements in used packet codecs are<=64 bytes on pinned64bit (largest used
repeated item is two String headers,48 bytes). All vector old+new growth<=3 times
the element budget plus small minimum capacities. Charge256P per codec context
for property elements/content/input/converted packet and peak overlap; four
contexts1024P is conservative (actual actor executes one packet path at a time).
Metadata amplification is included, not assumed equal to payload length.

HTTP raw input is cappedH; handshake read buffer grows geometrically, parser has
MAX_HEADERS=124; request URI uses host<=253 and path<=2048. Error response body/tail
is also within raw budget. HeaderMap/URI objects/slots are metadata below, NOT
excluded. No unlimited HTTP body read is authorized by Content-Length.

### Typed native metadata (not hidden as runtime)
Add: <=256 Event slots including deque reallocation overlap; <=8 Write slots;
<=64 disconnect u64 slots; BTree dedup<=129 nodes and expectations<=129 nodes
(each node includes up to11 keys/values and12 child pointers, pinned std B=6);
<=256 resub tuple slots +active; core and engine MPSC blocks/Arc/Notify/handles;
HTTP124 header slots+HeaderMap capacity; byte Shared/Frame/Message/codec fixed
structs. A typed allocation model counts each of these at its compiler layout,
plus realloc old/new blocks. Numeric standard-private layouts must not be guessed.
Supervisor generation quota Q=64 per linked native core, shared across ALL clients,
protocols and transports. A generation is admitted before normalized options/session/
channel/future capture, and stays charged until the actual captured supervisor future
and its Box are destroyed. An abort request, close/free or replacement does NOT
return its permit. Let L=live generations, R=retiring/never-polled aborted generations:
L+R<=Q, including discarded C handles and direct Rust callers. Saturation returns
existing QueueFull5 without creating another future; no worker wait in close/free.
Each generation adds one Pin<Box<supervisor future>> plus the Tokio task allocation,
wrapper/permit field and JoinHandle. These are M_fixed_actor, NOT runtime baseline.
One poll pump timer, one drain timer, common32 control launch continuations/timeouts
apply to each active Dart facade, not to arbitrary retired native generations.
Global ownership service and shared runtime have per-owner baseline, not perpacket.

## Dart/native overlap and retained graph checklist
- Bridge fixed topic65535 +payload65536 +C event struct, plus peek clone and FIFO
  original during copy. Current Dart decoded String/Uint8List, UTF8 encode/decode
  intermediate and arena coexist: conservatively per string encoding3*UTF16limit,
  decoded String<=2*units backing on native VM, plus old/new construction storage.
- Native input topics/payload normalized; options primary fields detached; Dart
  Will owns independent<=65535 fixed typed bytes. Ordinary publish65536 unchanged.
- Payload accepted native tickets: queued32 +active1 +engine-request4 +pending
  progress in native FIFO128 +one FFI crossing gives conservative166 ticket
  records (overcount shared phases).<=498 passive outcome cells plus32 drains.
  This is a COUNT; each retained SDK/control metadata object is separately charged.
- Common launches32, operation/start/control maps each<=32; timeout32 plus physical
  disconnect and pump/drain state; drain waiters32. Streams3*16 listeners, one
 16-reference snapshot per synchronous emission; no unbounded native/Dart message
  pause queue. During drain native inbound messages are dropped and only bounded
  admitted controls progress. User-added pending Future listeners cost K_user
  completer/list records; returned-handle retention costs K_handles externally.
- Rejected/cached discarded result views schedule0 internal jobs. Accepted path
  eager observer counts remain behind common32. Library-created traces in passive
  cells, rejected views and admitted control completers now use ONE constant
 61-character library-origin representation: no input trace.toString or retained
  caller/VM trace graph there. Original error object identity and SDK callback
  errors in caller zones are preserved. Inner SDK async error transients: R3 below.

## Shared library-owned attribution
CLIENTS HashMap in mqtt-c/lib.rs and resources/owners maps+sets in ownership.rs
are NOT OS/runtime exclusions. At most64 active entries per registry does NOT mean
64 buckets: historical churn/tombstones grow capacities. Audit observed capacity224
(256 raw buckets). Use actual historical raw bucket parameters B_clients,
B_resources,B_owners; charge old+new growth overlap, not current length. With
H(B,s,a)<=B*s+B+16+a requested-layout model, G includes each table's key/value size,
alignment/control bytes, overlap, <=64*133118 endpoint byte contents and <=64 cleanup
vector entries. Owner-service request data and temporary cleanup Vecs are charged.
No fixed high-water bucket constant is certified merely from entry count.

Synchronous pre-admission input/construction is separate U*B_pre_admission+E_in:
mqtt_client_new constructs Client/Options before checking CCLIENTS registry lock;
unrestricted concurrent native calls require caller concurrency parameter U. Existing
Dart launch32 is per facade, not a global native-call cap. Caller-provided Rust spare
capacity E_in is inspected/detached synchronously and is not retained by supervisor.

### Reviewed native typed model and aggregate expression
Pinned x86_64 Rust1.100 requested layouts: String/Vec24, Options312, Client64,
Event72, Command64, engine Request56, Write32, Entry384/(u64,Entry)392,
Resources48/(usize,Resources)56, Message56/Frame48, dedup BTree node488,
expectations node384. These are compiler-specific audit observations, not C ABI.
M_N =256*72+8*32+64*8+129*(488+384)+257*32
     +M_channels+M_HTTP+M_fixed_actor/transport/Arc/Notify.
First five terms =139912. Conservative four blocks/channel add
4*(32*64+32)+4*(32*56+32)=15616, plus channel headers/Arc/semaphore.
M_fixed includes captured supervisor future BOX and Tokio task/header/JoinHandle,
not just the small wrapper; source layout is compiler-dependent and must be measured
or kept a typed parameter. All retiring futures carry their full per-generation term.

HTTP HeaderMap COLLISION path may allocate1024 raw indices even with<=124 headers;
ordinary256 assumption is insufficient. HeaderName32/HeaderValue40. Charge
M_HTTP<=2*[1536*sizeof(Pos)+1152*sizeof(Bucket<HeaderValue>)
          +192*sizeof(ExtraValue<HeaderValue>)]
       +124*sizeof(httparse::Header)+request/URI/fixed.
The factor2 covers old/new overlap; Pos/Bucket/ExtraValue remain typed private-layout
parameters of the pinned http crate, not guessed logical header lengths.

Native_requested <= Q*(N_buf+M_N) +G(B_clients,B_resources,B_owners)
                    +U*B_pre_admission+E_in,
where N_buf=179339468 is ONLY the conservative backing-storage model above,
NOT an enforced whole-client/process byte cap. Duplicated per-client endpoint/event
storage is conservatively charged again; this expression intentionally overcounts.
R is bounded by Q, not folded into runtime baseline. Registry historical bucket
parameters and synchronous concurrency are explicit, not a disguised RSS promise.
Dart's separate logical-owned model adds <=32 DTOs*856057=27393824 backing bytes,
<=262140 facade endpoint logical bytes, current inbound<=196606 logical bytes,
conversion scratch and its own metadata/maps/cells/tasks/timers plus K_user records.
These Dart VM logical/typed models are NOT native requested-allocation measurements.

## EXACT residual obligations / readiness
R1 — RESOLVED for retained operation inputs. Public connect prepares a plain
_OwnedConnectInput synchronously after reserving common32 admission, before any
retained launch context. It reconstructs plain MqttConnectOptions/MqttWill/policy,
owned UTF8-decoded Strings, detached Will bytes and primitive Duration values.
A separate _launchPrepared method receives ONLY owned input and method tearoff;
original capture closure/options/subclass baggage cannot enter its lexical context.
Subscribe/unsubscribe topic/timeouts and disconnect timeout use same boundary.
Rejected QueueFull does not invoke getters or copy. Snapshot getter exceptions
return original error in caller-owned failed handle, not client maps. Reentrant
getter close is checked before any waiter registration. Public poison-getter +
Will-mutation wire test passes; host DTO field/type/view test makes no GC claim.
Retained connect DTO upper content model:2*(3*65536+2*65535+65535+2048)+65535
=856057 backing bytes (native-VM two-byte Strings +Will typed bytes), plus fixed
DTO/policy/Duration object metadata; common32 makes27393824 bytes. UTF8 encoder,
decoder, replacement copies and synchronous offered input E_in are additional
transient terms. This Dart term is NOT included in native171.032MiB proposal.

R2 — Native backing growth/metadata model certification. The algebra above supplies
a finite requested-backing candidate171.032MiB +typed metadata, but growth cases,
MPSC reclamation/node counts, HTTP HeaderMap layouts and the166 ticket/event
correspondence need independent checked source/model assertions on pinned tools
before claiming a complete enforced bound. Current tests prove selected limits,
not all model constants. Global G(B_clients,B_resources,B_owners) must remain explicit. No new
engine decision is needed; this is finite verification/accounting, not RSS work.

R3 — OWN FORWARDING BOUNDARIES RESOLVED under standard initialized SDK/FFI
operation. All library-authored throws/rethrows in facade now use
Error.throwWithStackTrace(error, shared finite library trace), including FIRST
throw. This prevents Error.stackTrace itself retaining raw origin graph, not just
outer completion trace. Inner async _connect trace is checked BEFORE outer
forwarding; raw-throw mutation fails (820 chars vs<=96), restored version passes.
Retained passive/rejection/control completer state already uses same constant.
Native network/protocol/TLS failures enter Dart as integer-code MqttException,
timeouts as fixed-message +owned Duration, local cancellation as scalar op id.
No network-controlled arbitrary Dart error/callback is materialized.

Explicit SDK assumption: valid initialized native bindings, ordinary SDK honoring
provided StackTrace, no VM/OOM/programming failures or caller Zone.errorCallback
replacement. We cannot control or byte-cap VM's internal awaiter/debug execution
history; it is execution-runtime scope, not a claimed lifetime/size fact. No own
field or forwarding closure stores raw trace now. Exceptions from public input
getters are synchronously returned unchanged in caller-owned failed result with
no retained client registration. User callback errors in caller zones remain
original; they are not blanket caught, converted or suppressed.

Independent final model review identified one lifecycle blocker: retired unpolled
futures were unbounded. generation.rs now holds Q64 permits until actual future Drop;
focused worker-gated PUBLIC ABI regression supplies RED/GREEN evidence. Registry,
HTTP collision and typed-layout qualifications above incorporate the other findings.
NOT claiming an enforced scalar whole-client byte total. Remaining final review/gates:

1. Check8P BytesMut backing induction, <=3 pinned/reallocation allocations,
   Vec growth/HTTP/collector/outgoing copy overlap against locked sources/tests.
2. Verify codec repeated-element<=64-byte layout/property counts, HTTP124 headers,
   encoder temporaries and metadata amplification including old/new capacities.
3. Derive actual MPSC blocks, VecDeque/BTree/HashMap layouts and high-water nodes;
   charge these library metadata, not runtime exclusions.
4. Certify166 ticket/event correspondence, Dart32 DTO856057-byte term +temporary
   encode/decode copies, fixed metadata/tasks/timers and K_user/K_handles boundary.
5. Attribute shared G(B_clients,B_resources,B_owners) historical registry capacity and lower-level
   owner-service requests explicitly; no hidden own buffers in OS/runtime terms.
After that review, run full immutable-tree gates/package production checks.
No new engine/features needed; actual GC timing and totalRSS remain out of scope.

## Reproducible source anchors
Production: core/lib.rs submit/events/session/expectations; core/budget.rs;
core/engine.rs request/outbox/one pending; owned_driver/state.rs boxed dedup;
owned_driver/transport.rs constructors/flush/packet; core/driver_codec.rs.
Frontend: rmqtt_flutter.dart _begin/_connect/_request/_DeferredResult/_completer;
event_pump.dart; native_client.dart. Shared: mqtt-c/lib.rs and ownership.rs.
Locked upstream: tungstenite0.26.2 protocol/frame/mod.rs158-278, message.rs115-145,
handshake/{client,machine,headers}.rs; bytes1.12.1 bytes_mut.rs626-805,1592-1605.
Canonical URLs: https://docs.rs/crate/tungstenite/0.26.2/source/src/protocol/frame/mod.rs
and https://docs.rs/crate/bytes/1.12.1/source/src/bytes_mut.rs .
