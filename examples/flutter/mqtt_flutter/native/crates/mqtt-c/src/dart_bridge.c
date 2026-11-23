/* Scalar Dart -> native owner-exit notification. Never invokes Dart code. */
#include "dart_api_dl.h"
#include <stddef.h>
#include <stdint.h>
static void (*owner_exit)(uint64_t) = NULL;
static void receive_exit(Dart_Port_DL port, Dart_CObject* object) {
  (void)port;
  int64_t id = 0;
  if (object->type == Dart_CObject_kInt64) id = object->value.as_int64;
  if (object->type == Dart_CObject_kInt32) id = object->value.as_int32;
  if (id > 0) owner_exit((uint64_t)id);
}
int64_t rmqtt_dart_initialize(void* api, void (*cleanup)(uint64_t)) {
  if (Dart_InitializeApiDL(api) != 0 || Dart_NewNativePort_DL == NULL ||
      Dart_NewSendPort_DL == NULL) return 0;
  owner_exit = cleanup;
  return Dart_NewNativePort_DL("mqtt-owner-exit", receive_exit, false);
}
Dart_Handle rmqtt_dart_send_port(int64_t port) {
  return Dart_NewSendPort_DL(port);
}
