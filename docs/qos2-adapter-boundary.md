# Historical upstream adapter boundary

This investigation concerned the previous rumqttc0.25.1 EventLoop adapter. The
current experimental production actor instead owns its packet state and transport;
see [current integration](owned-driver.md). The old architectural blocker is no
longer a reason to stop that approved implementation.

The unchanged `qos2_adapter_readiness` example still measures public upstream
Network/EventLoop capabilities, not the new driver. Neither swallowing an
EventLoop error nor enabling an enum repairs duplicate-handshake/session ownership.
V5 private network/state fields are not accessed. No registry cache is patched;
there is no fork, copied dependency engine or Git dependency.

Public packet codecs are reused, with independently owned id allocation, QoS2
phases, inbound retention, reconnect replay, connection and finite WS configuration.
The old upstream state/wire references remain RED and are labeled as references,
not changed into passing expected-failure production tests.

Official source references:
https://docs.rs/rumqttc/0.25.1/src/rumqttc/eventloop.rs.html
https://docs.rs/rumqttc/0.25.1/src/rumqttc/v5/eventloop.rs.html
