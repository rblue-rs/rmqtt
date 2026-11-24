# Owned production-driver QoS2 integration

The actor now uses an owned Rust connection/state driver and public rumqttc packet
codecs, not rumqttc EventLoop/MqttState or a private-field/vendor patch. This is
local experimental source, not a publication or mobile-conformance claim.

| Public combination | Current behavior |
| --- | --- |
| MQTT3.1.1 TCP/TLS/native WS/WSS, QoS2 publish/subscribe | Owned handshake; outbound success only matching PUBCOMP after PUBREC/PUBREL |
| MQTT3.1.1 persistent in-process reconnect | Same id; PUBLISH DUP1 before PUBREC, PUBREL replay after PUBREC; finite ticket retry budget |
| MQTT3.1.1 clean/new session with interrupted accepted work | Terminal failure, not silent replay into a new session |
| MQTT5 TCP/TLS/native WS/WSS, CleanStart=true/no expiry | Owned QoS2; negative PUBREC/PUBCOMP terminal Refused8; interrupted accepted work terminal |
| MQTT5 persistent sessions; enhanced auth; Will QoS2 | Unsupported6, not added |

Inbound QoS2 retains at most128 unfinished ids and1048576 topic+payload bytes,
delivers once on PUBREL, repeats PUBREC for a matching duplicate PUBLISH and
PUBCOMP for duplicate PUBREL. A broker may legally reuse a completed id for a new
publication: this is protocol-transaction suppression, not content deduplication
or subscriber application exactly-once. Unknown ids never complete an owned ticket.
Shared serial id allocation covers QoS1/2, SUBSCRIBE and UNSUBSCRIBE; QoS0 uses no id.

Cancellation/timeout abandons the local Dart wait, not an admitted wire handshake.
Dart exactlyOnce is appended at ordinal2. whenAcknowledged is PUBCOMP for QoS2,
PUBACK for QoS1, absent for QoS0. Existing ABI exports/layouts/numeric codes are
unchanged. Terminal retry exhaustion retains the existing final double-disconnect
contract; the frontend fails the outstanding future, never inventing an ACK.

## Actual production-path tests

`cargo test -p mqtt-core --test owned_public --test owned_faults --test owned_memory`
uses independent raw peers and PUBLIC Client paths. Cases cover duplicates,
wrong ids, both replay phases, clean/lost session and retry exhaustion, v5 negative
reasons, PUBCOMP-aware drain/deadline/force close, fragmented WS data, control flood,
slow application consumption and actual blocked socket writers. Separate native
Mosquitto positive cells exercise both protocols across TCP/TLS/WS/WSS with binary
QoS2 publish/subscription. Dart raw peers verify PUBCOMP-only futures, local cancel
and finite retry settlement through the production C library. These are host
evidence, not a full independent conformance certification or transport-fault proof
for every possible schedule. Final verification results belong to the accompanying
source report; this document does not turn an unrun test into a pass.

## Upstream reference probes remain RED, intentionally not rewritten

The unchanged `qos2_readiness`, `qos2_wire_readiness` and
`qos2_adapter_readiness` examples directly exercise rumqttc0.25.1 EventLoop/state,
NOT this owned production engine. Prior state6/18 and wire6/12 outcomes remain
upstream-reference evidence. They are not green production readiness gates.

Reference rules: MQTT3.1.1 sections4.3.3/4.4 and MQTT5:
https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html

See [owned driver](owned-driver.md), [WS capacity scope](websocket-memory.md).
No disk outbox, cross-process session storage, application exactly-once, browser
backend or additional mobile evidence is provided. Keepalive/TLS/reconnect safety
policies remain in force. Will QoS2 remains separately rejected.
