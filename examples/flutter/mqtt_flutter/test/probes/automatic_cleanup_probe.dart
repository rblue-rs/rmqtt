// Observe automatic owner-ref pairing without a substituting plain-C shutdown.
// Each round reinitializes and replaces the exit-port listener, then the owner
// isolate exits. NotInitialized must be visible before any runtime_shutdown.
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';

import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:rmqtt_flutter/src/ffi_bindings.dart';

import 'observe_not_initialized.dart';

Future<void> cycle(int round) async {
  for (var j = 0; j < 8; j++) {
    await MqttClient.ensureInitialized();
    await MqttClient.shutdownRuntime();
  }
  await MqttClient.ensureInitialized();
  if (round.isEven) {
    final client = MqttClient(
      server: '127.0.0.1',
      clientId: 'automatic-$round',
      port: int.parse(Platform.environment['MQTT_TEST_PORT']!),
    );
    await client.connect();
    // Deliberately do not close or shutdown. Isolate.run exits its owner.
  }
}

Future<void> main() async {
  final native = MqttBindings(
    DynamicLibrary.open(Platform.environment['MQTT_NATIVE_LIBRARY']!),
  );
  var peakMs = 0;
  for (var round = 0; round < 32; round++) {
    await Isolate.run(() => cycle(round));
    final observed = await waitForNotInitialized(native);
    if (observed.code != 2) {
      stderr.writeln(
        'automatic cleanup round=$round code=${observed.code} after ${observed.elapsedMilliseconds}ms',
      );
      exit(1);
    }
    if (observed.elapsedMilliseconds > peakMs) {
      peakMs = observed.elapsedMilliseconds;
    }
  }
  stdout.writeln(
    'PASS 32 owner exits after 256 init/shutdown/reinitialize cycles; NotInitialized observed without plain-C shutdown; maxObservedMs=$peakMs',
  );
}
