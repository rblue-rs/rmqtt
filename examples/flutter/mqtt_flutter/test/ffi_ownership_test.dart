import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:mqtt_flutter/mqtt_flutter.dart';
import 'package:test/test.dart';
import 'ack_timeout_test.dart' show Wire;

void main() {
  test('received bytes survive subsequent polls and native close', () async {
    final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'owned-$pid',
        port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
    final messages = <MqttReceivedMessage>[];
    final received = Completer<void>();
    final sub = c.messages.listen((m) {
      messages.add(m);
      if (messages.length == 2) received.complete();
    });
    try {
      await c.connect();
      await c.subscribe('audit/ownership');
      for (final bytes in [
        [0, 255, 0],
        [9, 8, 7]
      ]) {
        final ticket = c.publishMessage(
            'audit/ownership', Uint8List.fromList(bytes),
            qos: MqttQos.atLeastOnce);
        await ticket.whenAcknowledged;
      }
      await received.future.timeout(const Duration(seconds: 5));
      await c.close();
      expect(messages[0].payload, [0, 255, 0]);
      expect(messages[1].payload, [9, 8, 7]);
      expect(messages[0].topic, 'audit/ownership');
    } finally {
      await c.close();
      await sub.cancel();
      await MqttClient.shutdownRuntime();
    }
  });

  test('force close settles pending publish and rejects later operations',
      () async {
    final listener = await ServerSocket.bind('127.0.0.1', 0);
    final sent = Completer<void>(), release = Completer<void>();
    Socket? accepted;
    final broker = () async {
      final s = accepted = await listener.first;
      final wire = Wire(s);
      await wire.packet();
      s.add([0x20, 2, 0, 0]);
      await s.flush();
      final p = await wire.packet();
      expect(p[0] & 6, 2);
      sent.complete();
      // Deliberately withhold PUBACK. No real broker/session recovery promised.
      await release.future;
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'close-pending', port: listener.port);
    try {
      await c.connect();
      final ticket = c.publishMessage('x', Uint8List.fromList([1]),
          qos: MqttQos.atLeastOnce);
      await ticket.whenQueued;
      await ticket.whenSent;
      await sent.future.timeout(const Duration(seconds: 5));
      final failed = expectLater(ticket.whenAcknowledged, throwsStateError);
      await c.close();
      await failed.timeout(const Duration(seconds: 5));
      await c.close();
      await expectLater(c.subscribe('x'), throwsStateError);
      await expectLater(c.disconnect(), throwsStateError);
      expect(c.connectionState, MqttConnectionState.closed);
    } finally {
      if (!release.isCompleted) release.complete();
      await c.close();
      accepted?.destroy();
      await listener.close();
      await broker;
      await MqttClient.shutdownRuntime();
    }
  });
  test('owner isolate exit must release its native handle', () async {
    // Isolation is essential: the prototype intentionally leaks in this probe.
    // Keep the desired cleanup assertion RED, without poisoning this test runner.
    final result = await Process.run(Platform.resolvedExecutable, [
      'run',
      'test/probes/isolate_exit_probe.dart'
    ]).timeout(const Duration(seconds: 15));
    expect(result.exitCode, 0,
        reason: 'owner-isolate leak: ${result.stdout}${result.stderr}');
  });
}
