import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';
import 'ack_timeout_test.dart' show Wire;

final closedError = isA<MqttException>().having((e) => e.code, 'code', 3);
int jobs(void Function() action) {
  var count = 0;
  runZoned(action,
      zoneSpecification: ZoneSpecification(scheduleMicrotask: (s, p, z, f) {
    count++;
    p.scheduleMicrotask(z, f);
  }));
  return count;
}

void main() {
  for (final qos in MqttQos.values) {
    test('discard10000 rejected ticket phases schedules0 QoS${qos.index}',
        () async {
      await MqttClient.ensureInitialized();
      final c = MqttClient(server: '127.0.0.1', clientId: 'lazy-reject');
      try {
        expect(jobs(() {
          for (var i = 0; i < 10000; i++) {
            final t = c.publishMessage('x', Uint8List(0), qos: qos);
            t.whenQueued;
            t.whenSent;
            t.whenAcknowledged;
          }
        }), 0);
      } finally {
        await c.close();
        await MqttClient.shutdownRuntime();
      }
    });
  }
  test('discard10000 cached drain handles and aliases schedules0', () async {
    final c = MqttClient(server: '127.0.0.1', clientId: 'lazy-cache');
    expect(jobs(() {
      for (var i = 0; i < 10000; i++) {
        c.cancel(123);
      }
    }), 0);
    await c.drainClose(timeout: const Duration(seconds: 1));
    expect(jobs(() {
      for (var i = 0; i < 10000; i++) {
        c.beginDrainClose(timeout: const Duration(seconds: 1));
        c.drainClose(timeout: const Duration(seconds: 1));
        c.close();
      }
    }), 0);
    await c.close();
  });
  test(
      'rejected ticket errors remain observable late across zones on every phase',
      () async {
    await MqttClient.ensureInitialized();
    final c = MqttClient(server: '127.0.0.1', clientId: 'lazy-errors');
    final ticket =
        c.publishMessage('x', Uint8List(0), qos: MqttQos.exactlyOnce);
    final done = Completer<void>();
    runZonedGuarded(() {
      unawaited(() async {
        for (final f in [
          ticket.whenQueued,
          ticket.whenSent,
          ticket.whenAcknowledged!
        ]) {
          await expectLater(f, throwsA(closedError));
          expect(
              await f.then<int>((_) => 0,
                  onError: (Object e) => (e as MqttException).code),
              3);
          await f.catchError((Object e) {});
          var completed = 0;
          await expectLater(f.whenComplete(() {
            completed++;
          }), throwsA(closedError));
          expect(completed, 1);
          await expectLater(f.asStream(), emitsError(closedError));
          await expectLater(
              f.timeout(const Duration(seconds: 1)), throwsA(closedError));
        }
        done.complete();
      }());
    }, (Object e, StackTrace st) {
      if (!done.isCompleted) done.completeError(e, st);
    });
    try {
      await done.future.timeout(const Duration(seconds: 5));
    } finally {
      await c.close();
      await MqttClient.shutdownRuntime();
    }
  });
  test(
      'pending phase cancellation errors reach caller zone and do not cancel wire',
      () async {
    final listener = await ServerSocket.bind('127.0.0.1', 0);
    Socket? socket;
    final ready = Completer<void>(),
        release = Completer<void>(),
        done = Completer<void>();
    final broker = () async {
      final peer = socket = await listener.first;
      final w = Wire(peer);
      await w.packet();
      peer.add([32, 2, 0, 0]);
      await peer.flush();
      final p = await w.packet();
      ready.complete();
      await release.future;
      final n = p[1] * 256 + p[2];
      peer.add([0x40, 2, p[n + 3], p[n + 4]]);
      await peer.flush();
      expect(await w.packet(), [0xe0]);
      await done.future;
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'lazy-cancel', port: listener.port);
    try {
      await c.connect();
      final ticket =
          c.publishMessage('x', Uint8List(0), qos: MqttQos.atLeastOnce);
      await ticket.whenSent;
      await ready.future;
      final observed = Completer<void>();
      runZonedGuarded(() {
        unawaited(() async {
          await expectLater(
              ticket.whenAcknowledged, throwsA(isA<MqttOperationCancelled>()));
          observed.complete();
        }());
      }, (Object e, StackTrace st) {
        observed.completeError(e, st);
      });
      await c.cancel(ticket.operationId);
      await observed.future.timeout(const Duration(seconds: 5));
      await ticket.whenQueued;
      await ticket.whenSent;
      release.complete();
      await c.disconnect();
    } finally {
      if (!release.isCompleted) release.complete();
      done.complete();
      await c.close();
      socket?.destroy();
      await listener.close();
      await MqttClient.shutdownRuntime();
    }
    await broker;
  });
  test('user listener errors are not suppressed or duplicated', () async {
    await MqttClient.ensureInitialized();
    final c = MqttClient(server: '127.0.0.1', clientId: 'lazy-user-error');
    final t = c.publishMessage('x', Uint8List(0), qos: MqttQos.exactlyOnce);
    final seen = Completer<void>();
    var calls = 0;
    runZonedGuarded(() {
      unawaited(t.whenSent.then<void>((_) {}, onError: (Object e) {
        throw StateError('caller-error');
      }));
    }, (Object e, StackTrace st) {
      calls++;
      expect(e, isA<StateError>());
      if (!seen.isCompleted) seen.complete();
    });
    try {
      await seen.future.timeout(const Duration(seconds: 5));
      await Future<void>(() {});
      expect(calls, 1);
    } finally {
      await c.close();
      await MqttClient.shutdownRuntime();
    }
  });
  test('10000 native QueueFull ticket rejects enqueue0 internal tasks',
      () async {
    final listener = await ServerSocket.bind('127.0.0.1', 0);
    Socket? socket;
    final ready = Completer<void>();
    final broker = () async {
      final peer = socket = await listener.first;
      final w = Wire(peer);
      await w.packet();
      peer.add([32, 2, 0, 0]);
      await peer.flush();
      await w.packet();
      ready.complete();
      await expectLater(w.packet(), throwsA(isA<StateError>()));
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'lazy-full', port: listener.port);
    try {
      await c.connect();
      final held =
          c.publishMessage('x', Uint8List(0), qos: MqttQos.atLeastOnce);
      await held.whenSent;
      await ready.future;
      expect(jobs(() {
        for (var i = 0; i < 10000; i++) {
          c.publishMessage('x', Uint8List(0), qos: MqttQos.exactlyOnce);
        }
      }), 0);
      final rejected =
          c.publishMessage('x', Uint8List(0), qos: MqttQos.exactlyOnce);
      for (final f in [
        rejected.whenQueued,
        rejected.whenSent,
        rejected.whenAcknowledged!
      ]) {
        await expectLater(
            f, throwsA(isA<MqttException>().having((e) => e.code, 'code', 5)));
      }
    } finally {
      await c.close();
      socket?.destroy();
      await listener.close();
      await MqttClient.shutdownRuntime();
    }
    await broker;
  });
  test(
      'success phase listeners before and after completion and cancel preserve QoS1/2',
      () async {
    final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'lazy-success',
        port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
    try {
      await c.connect();
      for (final qos in [MqttQos.atLeastOnce, MqttQos.exactlyOnce]) {
        final completed = Completer<void>();
        final updates = c.publishUpdates.listen((e) {
          if (e.stage == MqttPublishStage.acknowledged &&
              !completed.isCompleted) {
            completed.complete();
          }
        });
        final ticket = c.publishMessage(
            'lazy/success', Uint8List.fromList([0, 255]),
            qos: qos);
        final early = ticket.whenQueued.then((_) {});
        await completed.future.timeout(const Duration(seconds: 5));
        await early;
        await ticket.whenQueued;
        await ticket.whenSent;
        await ticket.whenAcknowledged;
        var completions = 0;
        for (final phase in [
          ticket.whenQueued,
          ticket.whenSent,
          ticket.whenAcknowledged!
        ]) {
          await phase.whenComplete(() {
            completions++;
          });
        }
        expect(completions, 3);
        await c.cancel(ticket.operationId);
        await ticket.whenAcknowledged;
        await updates.cancel();
      }
      await c.drainClose(timeout: const Duration(seconds: 2));
      final done = Completer<void>();
      runZonedGuarded(() {
        unawaited(() async {
          final result =
              await c.drainClose(timeout: const Duration(seconds: 2));
          expect(result, MqttDrainResult.drained);
          final op = c.beginDrainClose(timeout: const Duration(seconds: 2));
          await op.cancel();
          expect(await op.future, MqttDrainResult.drained);
          done.complete();
        }());
      }, (Object e, StackTrace st) {
        done.completeError(e, st);
      });
      await done.future;
    } finally {
      await c.close();
      await MqttClient.shutdownRuntime();
    }
  });
  test(
      'discard rejected drain burst stays0 and local cancellation leaves handshake',
      () async {
    final listener = await ServerSocket.bind('127.0.0.1', 0);
    Socket? socket;
    final received = Completer<void>(),
        ack = Completer<void>(),
        done = Completer<void>();
    final broker = () async {
      final s = socket = await listener.first;
      final w = Wire(s);
      await w.packet();
      s.add([32, 2, 0, 0]);
      await s.flush();
      final p = await w.packet();
      received.complete();
      await ack.future;
      final n = p[1] * 256 + p[2];
      s.add([0x40, 2, p[n + 3], p[n + 4]]);
      await s.flush();
      expect(await w.packet(), [0xe0]);
      await done.future;
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'lazy-drain', port: listener.port);
    try {
      await c.connect();
      final t = c.publishMessage('x', Uint8List(0), qos: MqttQos.atLeastOnce);
      await t.whenSent;
      await received.future;
      final waits = List.generate(
          32, (_) => c.beginDrainClose(timeout: const Duration(seconds: 5)));
      expect(jobs(() {
        for (var i = 0; i < 10000; i++) {
          c.beginDrainClose(timeout: const Duration(seconds: 5));
          c.drainClose(timeout: const Duration(seconds: 5));
        }
      }), 0);
      final rejected = c.beginDrainClose(timeout: const Duration(seconds: 5));
      await expectLater(rejected.future,
          throwsA(isA<MqttException>().having((e) => e.code, 'code', 5)));
      final rejectedAlias = c.drainClose(timeout: const Duration(seconds: 5));
      final observed = Completer<void>();
      runZonedGuarded(() {
        unawaited(() async {
          final failure = isA<MqttException>().having((e) => e.code, 'code', 5);
          for (final f in [rejected.future, rejectedAlias]) {
            var completions = 0;
            await expectLater(f.whenComplete(() {
              completions++;
            }), throwsA(failure));
            expect(completions, 1);
            expect(
                await f.then<int>((_) => 0,
                    onError: (Object e) => (e as MqttException).code),
                5);
            await expectLater(f.asStream(), emitsError(failure));
          }
          observed.complete();
        }());
      }, (Object e, StackTrace st) {
        observed.completeError(e, st);
      });
      await observed.future;
      await rejected.cancel();
      await waits.first.cancel();
      expect(await waits.first.future, MqttDrainResult.cancelled);
      ack.complete();
      await t.whenAcknowledged;
      for (final op in waits.skip(1)) {
        expect(await op.future, MqttDrainResult.drained);
      }
    } finally {
      if (!ack.isCompleted) ack.complete();
      done.complete();
      await c.close();
      socket?.destroy();
      await listener.close();
      await MqttClient.shutdownRuntime();
    }
    await broker;
  });
}
