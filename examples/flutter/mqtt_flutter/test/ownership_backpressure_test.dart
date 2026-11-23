import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';
import 'ack_timeout_test.dart' show Wire;

void main() {
  test('pause preserves queued SUBACK and PUBACK before terminal Overflow',
      () async {
    final listener = await ServerSocket.bind('127.0.0.1', 0);
    Socket? socket;
    final full = Completer<void>();
    final broker = () async {
      final s = socket = await listener.first;
      final wire = Wire(s);
      expect((await wire.packet()).first, 0x10);
      s.add([0x20, 2, 0, 0]);
      await s.flush();
      final sub = await wire.packet();
      expect(sub.first, 0x82);
      s.add([0x90, 3, sub[1], sub[2], 1]);
      await s.flush();
      final pub = await wire.packet();
      expect(pub.first & 6, 2);
      final length = (pub[1] << 8) | pub[2];
      s.add([0x40, 2, ...pub.sublist(length + 3, length + 5)]);
      for (var i = 0; i < 130; i++) {
        s.add([0x30, 4, 0, 1, 120, i]);
      }
      await s.flush();
      await expectLater(wire.packet(),
          throwsA(isA<StateError>().having((e) => e.message, 'EOF', 'EOF')));
      full.complete();
    }();
    final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'control-before-overflow',
        port: listener.port);
    final delivered = <int>[], stages = <MqttPublishStage>[];
    final overflow = Completer<void>();
    final messages = c.messages.listen((m) => delivered.add(m.payload.single));
    final updates = c.publishUpdates.listen((u) => stages.add(u.stage));
    final events = c.events.listen((e) {
      if (e.error?.code == 12 && !overflow.isCompleted) overflow.complete();
    });
    try {
      await c.connect();
      messages.pause();
      final sub = c.beginSubscribe('x');
      final ticket = c.publishMessage('x', Uint8List.fromList([42]),
          qos: MqttQos.atLeastOnce);
      await full.future.timeout(const Duration(seconds: 5));
      expect(delivered, isEmpty);
      expect(stages, isEmpty);
      messages.resume();
      await sub.future;
      await ticket.whenAcknowledged;
      await overflow.future.timeout(const Duration(seconds: 5));
      expect(stages, [
        MqttPublishStage.queued,
        MqttPublishStage.sent,
        MqttPublishStage.acknowledged
      ]);
      expect(
          delivered,
          List.generate(
              124, (i) => i)); // 128 slots minus4 retained control events.
      expect(c.connectionState, MqttConnectionState.disconnected);
      await broker;
    } finally {
      await c.close();
      await messages.cancel();
      await updates.cancel();
      await events.cancel();
      socket?.destroy();
      await listener.close();
      await MqttClient.shutdownRuntime();
    }
  });
  test('payload over64KiB is rejected without corrupting the connection',
      () async {
    final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'payload-limit',
        port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
    try {
      await c.connect();
      final bad = c.publishMessage('x', Uint8List(65537));
      await expectLater(
          bad.whenQueued,
          throwsA(isA<MqttException>()
              .having((e) => e.code, 'InvalidArgument', 1)));
      await c.subscribe('payload-boundary');
      final incoming = c.messages.first;
      final good = c.publishMessage('payload-boundary', Uint8List(65536));
      await good.whenSent;
      expect(
          (await incoming.timeout(const Duration(seconds: 5))).payload.length,
          65536);
      expect(good.whenAcknowledged, isNull);
      expect(c.connectionState, MqttConnectionState.connected);
    } finally {
      await c.close();
      await MqttClient.shutdownRuntime();
    }
  });
}
