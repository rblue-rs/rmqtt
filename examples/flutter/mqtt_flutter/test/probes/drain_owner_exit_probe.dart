import 'dart:async';
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:rmqtt_flutter/src/ffi_bindings.dart';
import '../ack_timeout_test.dart' show Wire;
import 'observe_not_initialized.dart';

Future<void> runOwner(int port) => Isolate.run(() => owner(port));
Future<void> owner(int port) async {
  final c = MqttClient(server: '127.0.0.1', clientId: 'drain-exit', port: port);
  await c.connect();
  final t =
      c.publishMessage('x', Uint8List.fromList([1]), qos: MqttQos.atLeastOnce);
  await t.whenSent;
  c.beginDrainClose(timeout: const Duration(seconds: 300));
  // Isolate exits while drain is pending: must force-close, not wait 300s.
}

Future<void> main() async {
  final listener = await ServerSocket.bind('127.0.0.1', 0);
  final port = listener.port;
  Socket? accepted;
  final received = Completer<void>();
  final serve = () async {
    final s = accepted = await listener.first;
    final wire = Wire(s);
    await wire.packet();
    s.add([0x20, 2, 0, 0]);
    await s.flush();
    await wire.packet();
    received.complete(); // Deliberately never PUBACK.
  }();
  await runOwner(port);
  await received.future.timeout(const Duration(seconds: 3));
  final native = MqttBindings(
      DynamicLibrary.open(Platform.environment['MQTT_NATIVE_LIBRARY']!));
  final observation = await waitForNotInitialized(native);
  if (observation.code != 2) {
    throw StateError('draining owner leaked runtime: ${observation.code}');
  }
  accepted?.destroy();
  await listener.close();
  await serve;
  print('PASS pending drain owner reclaimed before shutdown');
}
