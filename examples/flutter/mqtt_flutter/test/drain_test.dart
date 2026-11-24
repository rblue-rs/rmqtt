import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';
import 'ack_timeout_test.dart' show Wire;

void main() {
  test(
      'real broker drains accepted QoS1 and controls, rejects new work, caches result',
      () async {
    final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'drain-real-$pid',
        port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
    final updates = <MqttPublishStage>[];
    final sub = c.publishUpdates.listen((e) => updates.add(e.stage));
    try {
      await c.connect();
      final subscribe = c.subscribe('drain/topic');
      final t = c.publishMessage('drain/topic', Uint8List.fromList([1]),
          qos: MqttQos.atLeastOnce);
      final result = c.drainClose(timeout: const Duration(seconds: 3));
      final repeat = c.drainClose(timeout: const Duration(milliseconds: 1));
      expect(() => c.publishMessage('x', Uint8List(0)), throwsStateError);
      await expectLater(c.subscribe('x'), throwsStateError);
      await expectLater(c.disconnect(), throwsStateError);
      expect(await result, MqttDrainResult.drained);
      expect(await repeat, MqttDrainResult.drained);
      await subscribe;
      await t.whenAcknowledged;
      expect(updates, contains(MqttPublishStage.acknowledged));
      expect(c.connectionState, MqttConnectionState.closed);
      expect(await c.drainClose(timeout: const Duration(seconds: 1)),
          MqttDrainResult.drained);
    } finally {
      await c.close();
      await sub.cancel();
      await MqttClient.shutdownRuntime();
    }
  });

  for (final mode in ['timeout', 'force', 'cancel', 'loss', 'paused']) {
    test('gated wire drain $mode is bounded and deterministic', () async {
      final listener = await ServerSocket.bind('127.0.0.1', 0);
      final sent = Completer<List<int>>(), release = Completer<void>();
      Socket? socket;
      final server = () async {
        final s = socket = await listener.first;
        final wire = Wire(s);
        await wire.packet();
        s.add([0x20, 2, 0, 0]);
        await s.flush();
        final p = await wire.packet();
        sent.complete(p);
        await release.future;
        if (mode == 'loss') {
          s.destroy();
          return;
        }
        if (mode == 'cancel' || mode == 'paused') {
          final n = p[1] * 256 + p[2];
          s.add([0x40, 2, p[n + 3], p[n + 4]]);
          await s.flush();
          expect(await wire.packet(), [0xe0]);
        }
        // Lifetime cleanup is asserted through final closed state below.
      }();
      final c = MqttClient(
          server: '127.0.0.1', clientId: 'drain-$mode', port: listener.port);
      final messages = <MqttReceivedMessage>[];
      final events = c.messages.listen(messages.add);
      try {
        await c.connect();
        final ticket = c.publishMessage('x', Uint8List.fromList([7]),
            qos: MqttQos.atLeastOnce);
        // Observe expected failures early without changing what they assert.
        final ack = ticket.whenAcknowledged!
            .then<Object?>((_) => null, onError: (Object e) => e);
        await sent.future.timeout(const Duration(seconds: 3));
        if (mode == 'paused') events.pause();
        final first = c.beginDrainClose(
            timeout: mode == 'timeout'
                ? const Duration(milliseconds: 150)
                : const Duration(seconds: 3));
        final joined = c.beginDrainClose(timeout: const Duration(seconds: 10));
        if (mode == 'timeout') {
          final extra = List.generate(30,
              (_) => c.beginDrainClose(timeout: const Duration(seconds: 10)));
          await expectLater(
              c.beginDrainClose(timeout: const Duration(seconds: 10)).future,
              throwsA(isA<MqttException>().having((e) => e.code, 'code', 5)));
          for (final caller in extra) {
            await caller.cancel();
            expect(await caller.future, MqttDrainResult.cancelled);
          }
        }
        if (mode == 'force') await c.close();
        if (mode == 'cancel') {
          await first.cancel();
          expect(await first.future, MqttDrainResult.cancelled);
          // Cancelling never reopens admission or retracts the sent publish.
          expect(() => c.publishMessage('x', Uint8List(0)), throwsStateError);
        }
        if (mode == 'paused') {
          // Incoming delivery is excluded from drain even if listeners pause.
          socket!.add([0x30, 4, 0, 1, 120, 99]);
          await socket!.flush();
        }
        release.complete();
        final expected = switch (mode) {
          'timeout' => MqttDrainResult.timedOut,
          'force' => MqttDrainResult.forced,
          'loss' => MqttDrainResult.failed,
          _ => MqttDrainResult.drained,
        };
        expect(
            await joined.future.timeout(const Duration(seconds: 3)), expected);
        if (mode != 'cancel') expect(await first.future, expected);
        if (expected == MqttDrainResult.drained) {
          expect(await ack, isNull);
        } else {
          expect(await ack, isNotNull);
        }
        expect(c.connectionState, MqttConnectionState.closed);
        events.resume();
        expect(messages, isEmpty);
        await server;
      } finally {
        if (!release.isCompleted) release.complete();
        await c.close();
        await events.cancel();
        socket?.destroy();
        await listener.close();
        await MqttClient.shutdownRuntime();
      }
    }, timeout: const Timeout(Duration(seconds: 10)));
  }

  test('drain validation and never-connected close are explicit', () async {
    final c = MqttClient(server: '127.0.0.1', clientId: 'idle');
    expect(
        () => c.beginDrainClose(timeout: Duration.zero), throwsArgumentError);
    expect(() => c.beginDrainClose(timeout: const Duration(microseconds: 1)),
        throwsArgumentError);
    expect(() => c.beginDrainClose(timeout: const Duration(seconds: 301)),
        throwsArgumentError);
    expect(await c.drainClose(timeout: const Duration(seconds: 1)),
        MqttDrainResult.drained);
    await c.close();
  });

  test('connecting drain returns Busy without changing connection admission',
      () async {
    final listener = await ServerSocket.bind('127.0.0.1', 0);
    final received = Completer<void>(), release = Completer<void>();
    Socket? accepted;
    final server = () async {
      final s = accepted = await listener.first;
      await Wire(s).packet();
      received.complete();
      await release.future;
      s.add([0x20, 2, 0, 0]);
      await s.flush();
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'drain-connect', port: listener.port);
    try {
      final connecting = c.connect();
      await received.future;
      await expectLater(c.drainClose(timeout: const Duration(seconds: 1)),
          throwsA(isA<MqttException>().having((e) => e.code, 'code', 4)));
      release.complete();
      await connecting;
      expect(c.connectionState, MqttConnectionState.connected);
      await server;
    } finally {
      if (!release.isCompleted) release.complete();
      await c.close();
      accepted?.destroy();
      await listener.close();
      await MqttClient.shutdownRuntime();
    }
  });

  test('owner exit force-reclaims a pending drain without a hidden wait',
      () async {
    final result = await Process.run(Platform.resolvedExecutable,
        ['run', 'test/probes/drain_owner_exit_probe.dart']);
    expect(result.exitCode, 0, reason: '${result.stdout}${result.stderr}');
  }, timeout: const Timeout(Duration(seconds: 15)));
}
