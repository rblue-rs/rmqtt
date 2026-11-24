// Shared observation helper for ownership probes.
// A leftover owner init ref must be visible as mqtt_client_new == 0.
// Calling mqtt_runtime_shutdown first would consume that ref and hide it.
import 'dart:ffi';

import 'package:ffi/ffi.dart';
import 'package:rmqtt_flutter/src/ffi_bindings.dart';

class ObserveResult {
  final int code;
  final int elapsedMilliseconds;
  const ObserveResult(this.code, this.elapsedMilliseconds);
}

/// Probe whether the runtime is already down. A successful temporary handle
/// is closed and freed immediately so this call is not a substitute teardown.
int tryClientNew(MqttBindings native, {String clientId = 'observe'}) {
  return using((arena) {
    final hostBytes = '127.0.0.1'.codeUnits;
    final idBytes = clientId.codeUnits;
    final host = arena<Uint8>(hostBytes.length);
    final id = arena<Uint8>(idBytes.length);
    final out = arena<Uint64>();
    host.asTypedList(hostBytes.length).setAll(0, hostBytes);
    id.asTypedList(idBytes.length).setAll(0, idBytes);
    final code = native.mqtt_client_new(
      host,
      hostBytes.length,
      id,
      idBytes.length,
      1883,
      out,
    );
    if (code == 0) {
      native.mqtt_client_close(out.value);
      native.mqtt_client_free(out.value);
    }
    return code;
  });
}

/// Wait until `mqtt_client_new` returns 2 (NotInitialized), or until [window]
/// elapses. Retries only while the runtime is still up (code 0). Does not call
/// shutdown or any equivalent teardown.
Future<ObserveResult> waitForNotInitialized(
  MqttBindings native, {
  Duration window = const Duration(seconds: 2),
}) async {
  final sw = Stopwatch()..start();
  var code = tryClientNew(native);
  while (true) {
    if (code == 2 || code != 0 || sw.elapsed >= window) {
      return ObserveResult(code, sw.elapsedMilliseconds);
    }
    await Future<void>.delayed(const Duration(milliseconds: 2));
    code = tryClientNew(native);
  }
}
