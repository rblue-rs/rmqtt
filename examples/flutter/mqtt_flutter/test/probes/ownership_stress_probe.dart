// Standalone Linux-host observation. No fixed RSS/performance promise.
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';

import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:rmqtt_flutter/src/ffi_bindings.dart';

import 'observe_not_initialized.dart';

Future<void> main() async {
  final samples = <Map<String, int>>[];
  final port = int.parse(Platform.environment['MQTT_TEST_PORT']!);
  for (var i = 0; i < 128; i++) {
    await Isolate.run(() async {
      final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'churn-$i',
        port: port,
      );
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
        'max_rss': ProcessInfo.maxRss,
      });
    }
  }
  final ffi = MqttBindings(
    DynamicLibrary.open(Platform.environment['MQTT_NATIVE_LIBRARY']!),
  );
  // Observe automatic cleanup first so a leftover owner init ref cannot be
  // consumed by a substituting mqtt_runtime_shutdown.
  final observed = await waitForNotInitialized(ffi);
  if (observed.code != 2) {
    throw StateError(
      'unpaired init ref before shutdown: client_new=${observed.code}, expected2',
    );
  }
  final code = ffi.mqtt_runtime_shutdown();
  if (code != 0) throw StateError('final native shutdown=$code');
  final notInitialized = tryClientNew(ffi, clientId: 'check');
  if (notInitialized != 2) {
    throw StateError(
      'unpaired init ref after shutdown: client_new=$notInitialized, expected2',
    );
  }
  stdout.writeln(
    jsonEncode({
      'owners': 128,
      'explicit_close': 64,
      'implicit_exit': 64,
      'pre_shutdown_client_new': observed.code,
      'pre_shutdown_observed_ms': observed.elapsedMilliseconds,
      'native_shutdown': code,
      'post_shutdown_client_new': notInitialized,
      'caller_buffer_bytes_per_client': sizeOf<MqttEvent>() + 65535 + 65536,
      'samples': samples,
    }),
  );
}
