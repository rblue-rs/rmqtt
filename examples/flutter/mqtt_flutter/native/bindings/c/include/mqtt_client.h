/* EXPERIMENTAL ABI: host vertical slice, not a stable release. */

#ifndef MQTT_CLIENT_EXPERIMENTAL_H
#define MQTT_CLIENT_EXPERIMENTAL_H

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Opaque VM handle. Only valid in a current Dart isolate/FFI scope.
 */
typedef struct _Dart_Handle _Dart_Handle;

/**
 * Additive reconnect policy for `mqtt_client_connect_ex`. `enabled` must be 0 or 1.
 * A null policy pointer means disabled (the default).
 */
typedef struct MqttReconnectPolicy {
  uint8_t enabled;
  uint32_t max_attempts;
  uint32_t base_delay_ms;
  uint32_t max_delay_ms;
} MqttReconnectPolicy;

/**
 * Event metadata. Buffers are copied into caller memory by mqtt_client_poll.
 */
typedef struct MqttEvent {
  uint32_t kind;
  int32_t code;
  uint64_t operation_id;
  uint16_t packet_id;
  uint8_t qos;
  uint8_t session_present;
  size_t topic_len;
  size_t payload_len;
} MqttEvent;

typedef struct _Dart_Handle *Dart_Handle;

/**
 * Native allocator deallocator, never a Dart trampoline.
 */
typedef void (*MqttNativeFree)(void*);

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

int32_t mqtt_runtime_init(void);

/**
 * Pair a plain-C init reference; do not consume Dart owner-managed references.
 * Allows up to two seconds for native-only owner-exit cleanup notification races,
 * then applies the live-client/worker guard. Not a message drain or Dart handshake.
 */
int32_t mqtt_runtime_shutdown(void);

/**
 * # Safety
 * Input pointers must reference readable lengths; out must be aligned/writable.
 */
int32_t mqtt_client_new(const uint8_t *host,
                        size_t host_len,
                        const uint8_t *id,
                        size_t id_len,
                        uint16_t port,
                        uint64_t *out);

/**
 * Configure WebSocket for subsequent connect calls on this handle only.
 * Does not mutate an existing connection. use_tls on connect selects WSS.
 * enabled=0 resets to TCP/TLS and requires null+0 path; enabled=1 with
 * null+0 selects /mqtt. Otherwise path is copied and validated (max2048 bytes).
 * # Safety
 * A non-null path must reference path_len readable bytes.
 */
int32_t mqtt_client_set_websocket(uint64_t handle,
                                  uint8_t enabled,
                                  const uint8_t *path,
                                  size_t path_len);

/**
 * # Safety
 * `ca_pem` must reference `ca_pem_len` readable bytes when non-null.
 * A null pointer is allowed only when `ca_pem_len` is zero, which means no
 * custom CA: if `use_tls` is 1, the host system roots are used. `use_tls`
 * must be 0 or 1. Providing a CA without TLS is rejected.
 * Username and password pointers are copied immediately; null + zero length
 * means the field is absent. `allow_plaintext_credentials` and `will_retain`
 * must be 0 or 1. A will is absent when `will_topic` is null and
 * `will_topic_len` is zero.
 * `client_cert_pem` and `client_key_pem` are copied immediately; null + zero
 * length means the field is absent. Both must be provided together and only
 * when `use_tls` is 1. They are not written to disk.
 */
int32_t mqtt_client_connect(uint64_t handle,
                            uint64_t op,
                            uint8_t protocol,
                            uint16_t keep_alive_seconds,
                            uint8_t use_tls,
                            const uint8_t *ca_pem,
                            size_t ca_pem_len,
                            const uint8_t *username,
                            size_t username_len,
                            const uint8_t *password,
                            size_t password_len,
                            uint8_t allow_plaintext_credentials,
                            const uint8_t *will_topic,
                            size_t will_topic_len,
                            const uint8_t *will_payload,
                            size_t will_payload_len,
                            uint8_t will_qos,
                            uint8_t will_retain,
                            const uint8_t *client_cert_pem,
                            size_t client_cert_pem_len,
                            const uint8_t *client_key_pem,
                            size_t client_key_pem_len);

/**
 * Additive connect: MQTT 3.1.1 `clean_session` (0 or 1) and an optional reconnect policy.
 * A null `reconnect` pointer uses the default disabled policy. Existing
 * `mqtt_client_connect` remains clean_session=1 with reconnect off.
 *
 * # Safety
 * Same pointer rules as `mqtt_client_connect`. `reconnect` may be null; a
 * non-null pointer must be aligned and readable for `MqttReconnectPolicy`.
 */
int32_t mqtt_client_connect_ex(uint64_t handle,
                               uint64_t op,
                               uint8_t protocol,
                               uint16_t keep_alive_seconds,
                               uint8_t use_tls,
                               const uint8_t *ca_pem,
                               size_t ca_pem_len,
                               const uint8_t *username,
                               size_t username_len,
                               const uint8_t *password,
                               size_t password_len,
                               uint8_t allow_plaintext_credentials,
                               const uint8_t *will_topic,
                               size_t will_topic_len,
                               const uint8_t *will_payload,
                               size_t will_payload_len,
                               uint8_t will_qos,
                               uint8_t will_retain,
                               const uint8_t *client_cert_pem,
                               size_t client_cert_pem_len,
                               const uint8_t *client_key_pem,
                               size_t client_key_pem_len,
                               uint8_t clean_session,
                               const struct MqttReconnectPolicy *reconnect);

/**
 * # Safety
 * topic must reference topic_len readable bytes.
 */
int32_t mqtt_client_subscribe(uint64_t handle,
                              uint64_t op,
                              const uint8_t *topic,
                              size_t topic_len,
                              uint8_t qos);

/**
 * # Safety
 * topic and payload must reference their readable lengths; null allowed iff length=0.
 */
int32_t mqtt_client_publish(uint64_t handle,
                            uint64_t op,
                            const uint8_t *topic,
                            size_t topic_len,
                            const uint8_t *payload,
                            size_t payload_len,
                            uint8_t qos,
                            uint8_t retain);

/**
 * # Safety
 * topic must reference topic_len readable bytes.
 */
int32_t mqtt_client_unsubscribe(uint64_t handle,
                                uint64_t op,
                                const uint8_t *topic,
                                size_t topic_len);

int32_t mqtt_client_disconnect(uint64_t handle, uint64_t op);

/**
 * Begin optional graceful close without blocking. timeout_ms must be 1..300000.
 * First accepted deadline wins; repeats join the same irreversible attempt.
 * New operations return Closed after acceptance; connecting/disconnecting -> Busy.
 * Poll retained control completions, not inbound messages. QoS1 waits for PUBACK;
 * success is not subscriber delivery. Caller must still close/free the handle.
 */
int32_t mqtt_client_drain_begin(uint64_t handle, uint32_t timeout_ms);

/**
 * Nonblocking out-of-band drain result, available even after force close until
 * free. 0 not-started, 1 pending, 2 drained, 3 timed-out, 4 failed, 5 forced.
 * Drained includes engine Outgoing Disconnect, not proof of broker receipt.
 * # Safety
 * out_status must point to aligned writable u32. Errors leave it unchanged.
 */
int32_t mqtt_client_drain_status(uint64_t handle, uint32_t *out_status);

/**
 * Poll without blocking. BUFFER_TOO_SMALL (9) writes metadata but retains event.
 * EMPTY (10) does not change caller memory. Native never retains caller pointers.
 * # Safety
 * out must be writable/aligned. Non-null buffers must be writable for capacities;
 * all output regions must be non-overlapping. Null buffers require capacity zero.
 */
int32_t mqtt_client_poll(uint64_t handle,
                         struct MqttEvent *out,
                         uint8_t *topic,
                         size_t topic_capacity,
                         uint8_t *payload,
                         size_t payload_capacity);

/**
 * Prototype force-close, idempotent; aborts pending work. No graceful drain promise.
 * After a successful close, the handle stays Closed until free; poll and other
 * operations except repeated close/free return Closed and cannot deliver events.
 */
int32_t mqtt_client_close(uint64_t handle);

/**
 * Idempotent numeric-handle release. Must close first. Never dereferences a handle.
 */
int32_t mqtt_client_free(uint64_t handle);

/**
 * Initialize official Dart API DL and one process-wide native exit receiver.
 * # Safety
 * api must be NativeApi.initializeApiDLData from the current Dart VM.
 */
int32_t mqtt_dart_initialize(void *api);

/**
 * Return the native-only exit receiver's SendPort as a Dart handle.
 * # Safety
 * Requires successful mqtt_dart_initialize and a current Dart isolate/FFI scope.
 */
Dart_Handle mqtt_dart_exit_send_port(void);

/**
 * Acquire one runtime init reference for an isolate owner (maximum 64 owners).
 * # Safety
 * out must be aligned/writable. Dart users establish an exit listener before
 * claiming owner-exit support; plain C users must explicitly release owners.
 */
int32_t mqtt_dart_owner_new(size_t *out);

/**
 * Pair an owner's runtime reference. Busy while it has registered clients.
 */
int32_t mqtt_dart_owner_shutdown(size_t owner);

/**
 * NativeFinalizer fallback for an inaccessible owner; encoded key, not a pointer.
 * No Dart calls or blocking runtime destruction; repeated cleanup is harmless.
 */
void mqtt_dart_owner_finalize(void *token);

/**
 * Transfer ownership of all three full-sized caller buffers and one native handle.
 * # Safety
 * Buffers must be distinct allocations from the supplied native free function,
 * sized/aligned for MqttEvent, 65535 topic bytes and 65536 payload bytes. Free must
 * not be a Dart trampoline. On success caller must use client_release/finalize,
 * never independently free buffers. out must be writable/aligned. No native event
 * pointer is returned; these remain caller-owned poll copy destinations.
 */
int32_t mqtt_dart_client_register(size_t owner,
                                  uint64_t handle,
                                  struct MqttEvent *event,
                                  uint8_t *topic,
                                  uint8_t *payload,
                                  MqttNativeFree free,
                                  size_t *out);

/**
 * Once-only close -> free -> matching buffer deallocator; stale tokens are harmless.
 */
int32_t mqtt_dart_client_release(size_t token);

/**
 * NativeFinalizer callback: token is an encoded registry key, never dereferenced.
 * Safe on arbitrary native threads; does not call Dart or block on a Tokio worker.
 */
void mqtt_dart_client_finalize(void *token);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* MQTT_CLIENT_EXPERIMENTAL_H */
