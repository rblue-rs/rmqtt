// Run in its own OS process: intentionally leaks the prototype's native handle.
// Desired behavior is cleanup after owner isolate exit; exit 1 records the gap.
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';
import 'package:mqtt_flutter/mqtt_flutter.dart';
import 'package:mqtt_flutter/src/ffi_bindings.dart';

Future<void> main() async {
  await Isolate.run(() async {
    final client = MqttClient(
        server: '127.0.0.1',
        clientId: 'isolate-exit-probe',
        port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
    await client.connect();
    // Deliberately no close/shutdown; Isolate.run terminates the owning isolate.
  });
  final native = MqttBindings(
      DynamicLibrary.open(Platform.environment['MQTT_NATIVE_LIBRARY']!));
  final code = native.mqtt_runtime_shutdown();
  stdout.writeln(
      'Owner-isolate cleanup: shutdown after owner isolate exit: expected 0, actual $code');
  exitCode = code == 0 ? 0 : 1;
}
