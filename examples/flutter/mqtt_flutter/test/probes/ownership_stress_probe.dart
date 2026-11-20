// Standalone Linux-host observation. No fixed RSS/performance promise.
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';
import 'package:ffi/ffi.dart';
import 'package:mqtt_flutter/mqtt_flutter.dart';
import 'package:mqtt_flutter/src/ffi_bindings.dart';

Future<void> main() async {
  final samples = <Map<String, int>>[];
  final port = int.parse(Platform.environment['MQTT_TEST_PORT']!);
  for (var i = 0; i < 128; i++) {
    await Isolate.run(() async {
      final c =
          MqttClient(server: '127.0.0.1', clientId: 'churn-$i', port: port);
      await c.connect();
      if (i.isEven) {
        await c.close();
        await c.close();
        await MqttClient.shutdownRuntime();
      }
      // Odd owners deliberately exit without close or shutdown.
    });
    if ([0, 15, 31, 63, 95, 127].contains(i)) {
      samples.add({
        'owners_completed': i + 1,
        'rss': ProcessInfo.currentRss,
        'max_rss': ProcessInfo.maxRss
      });
    }
  }
  final ffi = MqttBindings(
      DynamicLibrary.open(Platform.environment['MQTT_NATIVE_LIBRARY']!));
  final code = ffi.mqtt_runtime_shutdown();
  if (code != 0) throw StateError('final native shutdown=$code');
  final notInitialized = using((a) {
    final out = a<Uint64>(), host = a<Uint8>(9), id = a<Uint8>(5);
    host.asTypedList(9).setAll(0, utf8.encode('localhost'));
    id.asTypedList(5).setAll(0, utf8.encode('check'));
    final code = ffi.mqtt_client_new(host, 9, id, 5, 1883, out);
    if (code == 0) {
      ffi.mqtt_client_close(out.value);
      ffi.mqtt_client_free(out.value);
    }
    return code;
  });
  if (notInitialized != 2) {
    throw StateError(
        'unpaired init ref: client_new=$notInitialized, expected2');
  }
  stdout.writeln(jsonEncode({
    'owners': 128,
    'explicit_close': 64,
    'implicit_exit': 64,
    'native_shutdown': code,
    'post_shutdown_client_new': notInitialized,
    'caller_buffer_bytes_per_client': sizeOf<MqttEvent>() + 65535 + 65536,
    'samples': samples
  }));
}
