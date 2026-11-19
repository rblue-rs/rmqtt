/* EXPERIMENTAL ABI: host vertical slice, not a stable release. */

#ifndef MQTT_CLIENT_EXPERIMENTAL_H
#define MQTT_CLIENT_EXPERIMENTAL_H

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

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

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

int32_t mqtt_runtime_init(void);

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

int32_t mqtt_client_connect(uint64_t handle,
                            uint64_t op,
                            uint8_t protocol,
                            uint16_t keep_alive_seconds);

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

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* MQTT_CLIENT_EXPERIMENTAL_H */
