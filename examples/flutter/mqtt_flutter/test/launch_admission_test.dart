import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';
import 'ack_timeout_test.dart' show Wire;

Future<int> code(Future<Object?> future) async {
  try {
    await future;
    return 0;
  } on MqttException catch (e) {
    return e.code;
  } on MqttOperationCancelled {
    return -1;
  } on StateError {
    return -2;
  }
}

void main() {
  test('10000 rejected begin launches schedule no client-owned work', () async {
    await MqttClient.ensureInitialized();
    final c = MqttClient(server: '127.0.0.1', clientId: 'launch-reject');
    final scheduled = <int>[0];
    final first = <MqttOperation<MqttUnsubscribeAck>>[];
    final rejected = <MqttOperation<MqttUnsubscribeAck>>[];
    final wrappers = <Future<Object?>>[];
    try {
      runZoned(() {
        for (var i = 0; i < 32; i++) {
          first.add(c.beginUnsubscribe('x'));
        }
        final warm = c.beginUnsubscribe('x');
        rejected.add(warm);
        final before = scheduled[0];
        for (var i = 0; i < 10000; i++) {
          final op = c.beginUnsubscribe('x');
          rejected.add(op);
        }
        for (var i = 0; i < 1000; i++) {
          wrappers.addAll([
            c.subscribe('x'),
            c.unsubscribe('x'),
            c.connect(),
            c.disconnect()
          ]);
        }
        expect(scheduled[0], before,
            reason: 'rejection must not schedule client-owned work');
      }, zoneSpecification:
          ZoneSpecification(scheduleMicrotask: (self, parent, zone, task) {
        scheduled[0]++;
        parent.scheduleMicrotask(zone, task);
      }));
      expect(await Future.wait(wrappers.map(code)), everyElement(5));
      final codes = await Future.wait(rejected.map((op) => code(op.future)));
      expect(codes, everyElement(5));
      expect(
          (await Future.wait(first.map((op) => code(op.future))))
              .every((n) => n != 5),
          isTrue);
      // Callback settlement is observed through its returned completion, not sleep.
      final again = List.generate(32, (_) => c.beginUnsubscribe('x'));
      expect(
          (await Future.wait(again.map((op) => code(op.future))))
              .every((n) => n != 5),
          isTrue);
    } finally {
      await c.close();
      await MqttClient.shutdownRuntime();
    }
  });
  test(
      'common mixed cap retains cancelled disconnect launches until physical fence',
      () async {
    final listener = await ServerSocket.bind('127.0.0.1', 0);
    Socket? socket;
    final sent = Completer<void>(),
        ack = Completer<void>(),
        done = Completer<void>();
    final broker = () async {
      final s = socket = await listener.first;
      final wire = Wire(s);
      await wire.packet();
      s.add([32, 2, 0, 0]);
      await s.flush();
      final packet = await wire.packet();
      expect(packet.first & 6, 2);
      sent.complete();
      await ack.future;
      final n = packet[1] * 256 + packet[2];
      s.add([0x40, 2, packet[n + 3], packet[n + 4]]);
      await s.flush();
      expect(await wire.packet(), [0xe0]);
      await done.future;
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'launch-fence', port: listener.port);
    final disconnected = Completer<void>();
    final events = c.events.listen((e) {
      if (e.state == MqttConnectionState.disconnected &&
          !disconnected.isCompleted) {
        disconnected.complete();
      }
    });
    try {
      await c.connect();
      final ticket =
          c.publishMessage('x', Uint8List(1), qos: MqttQos.atLeastOnce);
      await ticket.whenSent;
      await sent.future;
      final handles = List.generate(32, (_) => c.beginDisconnect());
      expect(await code(c.beginSubscribe('x').future), 5);
      expect(await code(c.beginUnsubscribe('x').future), 5);
      expect(await code(c.beginConnect().future), 5);
      for (final h in handles) {
        await h.cancel();
        expect(await code(h.future), -1);
      }
      expect(await code(c.beginDisconnect().future), 5);
      expect(await code(c.beginSubscribe('x').future), 5);
      ack.complete();
      await ticket.whenAcknowledged;
      await disconnected.future.timeout(const Duration(seconds: 5));
      // Event-loop fence: terminal-event continuations drain before this turn.
      await Future<void>(() {});
      await c.beginDisconnect().future;
      final fresh = List.generate(32, (_) => c.beginUnsubscribe('x'));
      expect(
          (await Future.wait(fresh.map((h) => code(h.future))))
              .every((n) => n != 5),
          isTrue);
    } finally {
      if (!ack.isCompleted) ack.complete();
      if (!done.isCompleted) done.complete();
      await c.close();
      await events.cancel();
      socket?.destroy();
      await listener.close();
      await MqttClient.shutdownRuntime();
    }
    await broker;
  });
  test(
      'shared deadline releases launch slots exactly once after physical close',
      () async {
    final listener = await ServerSocket.bind('127.0.0.1', 0);
    Socket? socket;
    final received = Completer<void>();
    final broker = () async {
      final s = socket = await listener.first;
      final w = Wire(s);
      await w.packet();
      s.add([32, 2, 0, 0]);
      await s.flush();
      await w.packet();
      received.complete();
      await expectLater(w.packet(), throwsA(isA<StateError>()));
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'launch-deadline', port: listener.port);
    try {
      await c.connect();
      final ticket =
          c.publishMessage('x', Uint8List(1), qos: MqttQos.atLeastOnce);
      await ticket.whenSent;
      await received.future;
      final waits = List.generate(32,
          (_) => c.beginDisconnect(timeout: const Duration(milliseconds: 100)));
      expect(await code(c.beginSubscribe('x').future), 5);
      await Future.wait(waits
          .map((h) => expectLater(h.future, throwsA(isA<TimeoutException>()))));
      await broker;
      final fresh = List.generate(32, (_) => c.beginUnsubscribe('x'));
      expect(await code(c.beginConnect().future), 5);
      expect(
          (await Future.wait(fresh.map((h) => code(h.future))))
              .every((n) => n != 5),
          isTrue);
      await c.beginDisconnect().future;
    } finally {
      await c.close();
      socket?.destroy();
      await listener.close();
      await MqttClient.shutdownRuntime();
    }
  });
  test('rejected Future interface recovery and listeners use caller error zone',
      () async {
    await MqttClient.ensureInitialized();
    final c = MqttClient(server: '127.0.0.1', clientId: 'launch-zone');
    final busy = List.generate(32, (_) => c.beginUnsubscribe('x'));
    final rejected = c.beginUnsubscribe('x').future;
    final done = Completer<void>();
    runZonedGuarded(() {
      unawaited(() async {
        final ack = await rejected
            .catchError((Object e) => const MqttUnsubscribeAck(42));
        expect(ack.operationId, 42);
        expect(await rejected.then<int>((_) => 0, onError: (Object e) => 5), 5);
        await expectLater(rejected.asStream(),
            emitsError(isA<MqttException>().having((e) => e.code, 'code', 5)));
        var completed = false;
        await expectLater(rejected.whenComplete(() {
          completed = true;
        }), throwsA(isA<MqttException>()));
        expect(completed, isTrue);
        await expectLater(rejected.timeout(const Duration(seconds: 1)),
            throwsA(isA<MqttException>().having((e) => e.code, 'code', 5)));
        done.complete();
      }());
    }, (Object e, StackTrace st) {
      if (!done.isCompleted) done.completeError(e, st);
    });
    try {
      await done.future.timeout(const Duration(seconds: 5));
      await Future.wait(busy.map((h) => code(h.future)));
    } finally {
      await c.close();
      await MqttClient.shutdownRuntime();
    }
  });
  test(
      'close settles capped launch burst without client-retained rejected tasks',
      () async {
    await MqttClient.ensureInitialized();
    final c = MqttClient(server: '127.0.0.1', clientId: 'launch-close');
    final handles = List.generate(32, (_) => c.beginUnsubscribe('x'));
    final extra = c.beginSubscribe('x');
    final closing = c.close();
    expect(await code(extra.future), 5);
    await Future.wait(handles.map((h) => code(h.future)));
    await closing;
    await c.close();
    expect(() => c.beginUnsubscribe('x'), throwsStateError);
    await MqttClient.shutdownRuntime();
  });
}
