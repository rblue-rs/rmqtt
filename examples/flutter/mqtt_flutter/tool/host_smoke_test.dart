// Extracted-source Linux verification, not a Flutter desktop/mobile support claim.
import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';

void main() {
  test(
    'native ABI loads; defaults and unsupported V5 zero stay unchanged',
    () async {
      final c = MqttClient(server: '127.0.0.1', clientId: 'package-zero-$pid');
      try {
        expect(const MqttConnectOptions().keepAlivePeriod, Duration.zero);
        expect(const MqttConnectOptions().reconnectPolicy.enabled, isFalse);
        await expectLater(
          c.connect(
            options: const MqttConnectOptions(protocol: MqttProtocol.v5),
          ),
          throwsA(isA<MqttException>().having((e) => e.code, 'code', 6)),
        );
        expect(MqttQos.values.map((q) => q.index), [0, 1, 2]);
      } finally {
        await c.close();
        await MqttClient.shutdownRuntime();
      }
    },
  );
  test(
    'extracted native source supports binary QoS0/1 broker roundtrip',
    () async {
      final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'package-roundtrip-$pid',
        port: int.parse(Platform.environment['MQTT_TEST_PORT']!),
      );
      final received = <MqttReceivedMessage>[];
      final sub = c.messages.listen(received.add);
      try {
        await c.connect(timeout: const Duration(seconds: 5));
        await c.subscribe('package/smoke');
        final q1 = c.publishMessage(
          'package/smoke',
          Uint8List.fromList([0, 255, 7]),
          qos: MqttQos.atLeastOnce,
        );
        await q1.whenQueued;
        await q1.whenSent;
        await q1.whenAcknowledged;
        final q0 = c.publishMessage(
          'package/smoke',
          Uint8List.fromList([255, 0, 8]),
        );
        await q0.whenSent;
        final until = DateTime.now().add(const Duration(seconds: 5));
        while (received.length < 2 && DateTime.now().isBefore(until)) {
          await Future<void>.delayed(const Duration(milliseconds: 10));
        }
        expect(received.map((m) => m.payload.toList()).toList(), [
          [0, 255, 7],
          [255, 0, 8],
        ]);
        await c.unsubscribe('package/smoke');
        await c.disconnect();
      } finally {
        await c.close();
        await sub.cancel();
        await MqttClient.shutdownRuntime();
      }
    },
  );
}
