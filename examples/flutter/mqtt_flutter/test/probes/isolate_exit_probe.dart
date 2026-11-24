// Run in its own OS process. After the owner isolate exits without close or
// shutdown, observe NotInitialized before any substituting runtime shutdown.
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';

import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:rmqtt_flutter/src/ffi_bindings.dart';

import 'observe_not_initialized.dart';

Future<void> main() async {
  await Isolate.run(() async {
    final client = MqttClient(
      server: '127.0.0.1',
      clientId: 'isolate-exit-probe',
      port: int.parse(Platform.environment['MQTT_TEST_PORT']!),
    );
    await client.connect();
    // Deliberately no close/shutdown; Isolate.run terminates the owning isolate.
  });
  final native = MqttBindings(
    DynamicLibrary.open(Platform.environment['MQTT_NATIVE_LIBRARY']!),
  );
  final observed = await waitForNotInitialized(native);
  if (observed.code != 2) {
    stdout.writeln(
      'automatic cleanup after owner isolate exit: expected 2 (NotInitialized) before shutdown, actual ${observed.code} after ${observed.elapsedMilliseconds}ms',
    );
    exitCode = 1;
    return;
  }
  for (var i = 0; i < 32; i++) {
    final code = native.mqtt_runtime_shutdown();
    if (code != 0) {
      stdout.writeln(
        'shutdown after observed NotInitialized: expected 0, actual $code at $i',
      );
      exitCode = 1;
      return;
    }
  }
  stdout.writeln(
    'NotInitialized observed after owner isolate exit before shutdown in ${observed.elapsedMilliseconds}ms; shutdown idempotent',
  );
}
