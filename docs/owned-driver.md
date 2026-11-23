# Owned Rust production driver: experimental host integration

Both supported protocol selections route through `engine.rs`, `driver_codec.rs`
and `owned_driver/{state,transport}.rs` in ordinary production builds. Only test
peers/instrumentation are cfg(test). There is no alternate untested old adapter.
The existing actor owns admission32, cancel-wait/frontends, expected subscriptions,
reconnect budget, drain and owner teardown; the driver owns one serial transaction,
shared packet ids, codec boundary, socket/read accumulation and persistent writes.

Public codecs and async-tungstenite/tokio-rustls public constructors are used.
Existing locked bytes1.12.1, futures0.3.34, async-tungstenite0.29.1 and
tokio-rustls0.26.6 moved from test-only to production dependencies; no version bump,
Git dependency, upstream engine copy, registry patch or changed TLS verifier.
These crates retain their lock checksums and upstream MIT/Apache-2.0 licensing.
No new crate-name/publication decision is made.

Writes retain offsets and WS start/flush state across cancellation, with an
absolute5s write deadline. Large CONNECT fits a393216-byte outbound envelope and
is split across WS binary messages of at most131080 bytes; pending MQTT operations
are not fragmented into independent logical transactions. MQTT3.1.1 default
keepalive0 stays zero. MQTT5 default0..4 remains Unsupported; explicit5..65535 stays
accepted, with broker ServerKeepAlive handling. First connection failure never
starts reconnect. Post-CONNACK generic TLS I/O failure can consume a finite retry;
fresh TLS negotiation always revalidates trust/hostname/expiry/client certificates.
Authentication/certificate failures are not ignored. Credentials remain redacted.

[QoS2 matrix](qos2-readiness.md) includes in-process MQTT311 persistent ownership.
MQTT5 accepted outstanding work fails on interruption; idle reconnect is separate.
SUB/UNSUB admitted expectation reconciliation and no full resubscribe on
sessionPresent1 retain their existing contract. QoS0 never fabricates an ACK.

[WS resource limits](websocket-memory.md) are production settings now, not private
counterfactuals. A final full client-owned aggregate heap ceiling is NOT yet claimed:
the compositional capacity audit must include frontend encoding/admission bursts,
metadata, copies and realloc transients, not just selected allocation fixtures.
No runtime/OS/TLS-provider/process-RSS, Flutter Web or mobile guarantee is implied.
