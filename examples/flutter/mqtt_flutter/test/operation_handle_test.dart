import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';
import 'ack_timeout_test.dart' show Wire;
import 'lifecycle_test.dart' show Peer;

void main() {
  test('beginConnect handle is usable immediately; cancellation is local',
      () async {
    final p = await Peer.open();
    final ready = Completer<void>(),
        ack = Completer<void>(),
        done = Completer<void>();
    final broker = () async {
      final s = await p.listener.first;
      p.sockets.add(s);
      await Wire(s).packet();
      ready.complete();
      await ack.future;
      s.add([0x20, 2, 0, 0]);
      await s.flush();
      await done.future;
    }();
    final c = p.client('handle-connect');
    final connected = Completer<void>();
    final sub = c.events.listen((e) {
      if (e.state == MqttConnectionState.connected && !connected.isCompleted) {
        connected.complete();
      }
    });
    try {
      final MqttOperation<MqttConnectionStatus> op = c.beginConnect();
      expect(op.operationId, greaterThan(0));
      final failed =
          expectLater(op.future, throwsA(isA<MqttOperationCancelled>()));
      await op.cancel();
      await op.cancel();
      await failed;
      await ready.future;
      ack.complete();
      await connected.future.timeout(const Duration(seconds: 3));
      expect(c.connectionState, MqttConnectionState.connected);
      await expectLater(op.future, throwsA(isA<MqttOperationCancelled>()));
    } finally {
      if (!ack.isCompleted) ack.complete();
      done.complete();
      await c.close();
      await sub.cancel();
      await p.close();
      await MqttClient.shutdownRuntime();
    }
    await broker;
  });

  for (final unsubscribe in [false, true]) {
    test(
        'begin${unsubscribe ? 'Unsubscribe' : 'Subscribe'} exposes id, late ACK cannot settle another handle',
        () async {
      final p = await Peer.open();
      final ready = Completer<void>(),
          ack = Completer<void>(),
          done = Completer<void>();
      final broker = () async {
        final s = await p.listener.first;
        p.sockets.add(s);
        final w = Wire(s);
        await w.packet();
        s.add([0x20, 2, 0, 0]);
        await s.flush();
        final old = await w.packet();
        ready.complete();
        await ack.future;
        s.add(unsubscribe
            ? [0xb0, 2, old[1], old[2]]
            : [0x90, 3, old[1], old[2], 1]);
        await s.flush();
        final fresh = await w.packet();
        s.add([0x90, 3, fresh[1], fresh[2], 0]);
        await s.flush();
        await done.future;
      }();
      final c = p.client('handle-control-$unsubscribe');
      try {
        await c.connect();
        final MqttOperation<Object?> op =
            unsubscribe ? c.beginUnsubscribe('old') : c.beginSubscribe('old');
        final failed =
            expectLater(op.future, throwsA(isA<MqttOperationCancelled>()));
        await ready.future;
        await c.cancel(op.operationId);
        await op.cancel();
        await failed;
        final MqttOperation<MqttSubscribeAck> fresh =
            c.beginSubscribe('fresh', qos: MqttQos.atMostOnce);
        expect(fresh.operationId, greaterThan(op.operationId));
        ack.complete();
        final MqttSubscribeAck result = await fresh.future;
        expect(result.operationId, fresh.operationId);
        expect(result.qos, MqttQos.atMostOnce);
        await fresh.cancel();
        await fresh.cancel();
        await c.close();
        await fresh.cancel(); // terminal handle cancel is harmless after close
      } finally {
        if (!ack.isCompleted) ack.complete();
        done.complete();
        await c.close();
        await p.close();
        await MqttClient.shutdownRuntime();
      }
      await broker;
    });
  }

  for (final cancelFirst in [false, true]) {
    test(
        'shared disconnect cancels only ${cancelFirst ? 'first' : 'joining'} caller',
        () async {
      final p = await Peer.open();
      final ready = Completer<void>(),
          ack = Completer<void>(),
          done = Completer<void>();
      final broker = () async {
        final s = await p.listener.first;
        p.sockets.add(s);
        final w = Wire(s);
        await w.packet();
        s.add([0x20, 2, 0, 0]);
        await s.flush();
        final publish = await w.packet();
        ready.complete();
        await ack.future;
        final n = publish[1] * 256 + publish[2];
        s.add([0x40, 2, publish[n + 3], publish[n + 4]]);
        await s.flush();
        expect(await w.packet(), [0xe0]);
        await done.future;
      }();
      final c = p.client('handle-disconnect-$cancelFirst');
      try {
        await c.connect();
        final ticket =
            c.publishMessage('x', Uint8List(1), qos: MqttQos.atLeastOnce);
        await ticket.whenSent;
        await ready.future;
        final MqttOperation<void> a = c.beginDisconnect();
        final MqttOperation<void> b = c.beginDisconnect();
        expect(a.operationId, isNot(b.operationId));
        final MqttOperation<void> cancelled = cancelFirst ? a : b;
        final MqttOperation<void> survivor = cancelFirst ? b : a;
        final failed = expectLater(
            cancelled.future, throwsA(isA<MqttOperationCancelled>()));
        await cancelled.cancel();
        await c.cancel(cancelled.operationId);
        await c.cancel(cancelled
            .operationId); // repeated id cancellation must remain caller-local
        await failed;
        ack.complete();
        await survivor.future;
        expect(c.connectionState, MqttConnectionState.disconnected);
        await survivor.cancel();
        await cancelled.cancel();
      } finally {
        if (!ack.isCompleted) ack.complete();
        done.complete();
        await c.close();
        await p.close();
        await MqttClient.shutdownRuntime();
      }
      await broker;
    });
  }
  test('cancelling the first disconnect wait does not remove shared deadline',
      () async {
    final p = await Peer.open();
    final ready = Completer<void>(), done = Completer<void>();
    final broker = () async {
      final s = await p.listener.first;
      p.sockets.add(s);
      final w = Wire(s);
      await w.packet();
      s.add([0x20, 2, 0, 0]);
      await s.flush();
      await w.packet();
      ready.complete();
      await done.future;
    }();
    final c = p.client('handle-deadline');
    try {
      await c.connect();
      final ticket =
          c.publishMessage('x', Uint8List(1), qos: MqttQos.atLeastOnce);
      await ticket.whenSent;
      await ready.future;
      final watch = Stopwatch()..start();
      final a = c.beginDisconnect(timeout: const Duration(milliseconds: 100));
      final b = c.beginDisconnect(timeout: const Duration(seconds: 3));
      final failed =
          expectLater(a.future, throwsA(isA<MqttOperationCancelled>()));
      final timed = expectLater(b.future, throwsA(isA<TimeoutException>()));
      await a.cancel();
      await failed;
      await timed;
      expect(watch.elapsedMilliseconds, lessThan(1500));
      expect(c.connectionState, MqttConnectionState.disconnected);
      await a.cancel();
      await b.cancel();
      await expectLater(a.future, throwsA(isA<MqttOperationCancelled>()));
      await expectLater(b.future, throwsA(isA<TimeoutException>()));
    } finally {
      done.complete();
      await c.close();
      await p.close();
      await MqttClient.shutdownRuntime();
    }
    await broker;
  });

  test('reentrant beginDisconnect returns independent cancellable handle',
      () async {
    final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'handle-reentrant',
        port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
    MqttOperation<void>? joined;
    final sub = c.events.listen((e) {
      if (e.state == MqttConnectionState.disconnecting && joined == null) {
        joined = c.beginDisconnect();
        joined!.cancel();
      }
    });
    try {
      await c.connect();
      final outer = c.beginDisconnect();
      await outer.future;
      expect(joined!.operationId, isNot(outer.operationId));
      await expectLater(joined!.future, throwsA(isA<MqttOperationCancelled>()));
      await joined!.cancel();
      await outer.cancel();
      expect(c.connectionState, MqttConnectionState.disconnected);
    } finally {
      await c.close();
      await sub.cancel();
      await MqttClient.shutdownRuntime();
    }
  });

  test(
      'close from synchronous connecting listener settles returned handle once',
      () async {
    final c =
        MqttClient(server: '127.0.0.1', clientId: 'handle-close', port: 1883);
    final sub = c.events.listen((e) {
      if (e.state == MqttConnectionState.connecting) c.close();
    });
    try {
      final op = c.beginConnect();
      await expectLater(op.future, throwsStateError);
      await op.cancel();
      await op.cancel();
      expect(c.connectionState, MqttConnectionState.closed);
    } finally {
      await c.close();
      await sub.cancel();
      await MqttClient.shutdownRuntime();
    }
  });

  for (final unsubscribe in [false, true]) {
    test(
        'timed-out ${unsubscribe ? 'unsubscribe' : 'subscribe'} handle cannot be cancelled into another outcome',
        () async {
      final p = await Peer.open();
      final ready = Completer<void>(),
          ack = Completer<void>(),
          done = Completer<void>();
      final broker = () async {
        final s = await p.listener.first;
        p.sockets.add(s);
        final w = Wire(s);
        await w.packet();
        s.add([0x20, 2, 0, 0]);
        await s.flush();
        final packet = await w.packet();
        ready.complete();
        await ack.future;
        s.add(unsubscribe
            ? [0xb0, 2, packet[1], packet[2]]
            : [0x90, 3, packet[1], packet[2], 1]);
        await s.flush();
        await done.future;
      }();
      final c = p.client('handle-timeout-$unsubscribe');
      try {
        await c.connect();
        final MqttOperation<Object?> op = unsubscribe
            ? c.beginUnsubscribe('x', timeout: const Duration(milliseconds: 50))
            : c.beginSubscribe('x', timeout: const Duration(milliseconds: 50));
        final failed = expectLater(op.future, throwsA(isA<TimeoutException>()));
        await ready.future;
        await failed;
        await op.cancel();
        ack.complete();
        await Future<void>.delayed(const Duration(milliseconds: 30));
        await expectLater(op.future, throwsA(isA<TimeoutException>()));
        expect(c.connectionState, MqttConnectionState.connected);
      } finally {
        if (!ack.isCompleted) ack.complete();
        done.complete();
        await c.close();
        await p.close();
        await MqttClient.shutdownRuntime();
      }
      await broker;
    });
  }

  test(
      'native ACK committed before a synchronous message listener closes stays successful',
      () async {
    final p = await Peer.open();
    final done = Completer<void>();
    final broker = () async {
      final s = await p.listener.first;
      p.sockets.add(s);
      final w = Wire(s);
      await w.packet();
      s.add([0x20, 2, 0, 0]);
      await s.flush();
      final packet = await w.packet();
      s.add([0x90, 3, packet[1], packet[2], 1, 0x30, 4, 0, 1, 120, 33]);
      await s.flush();
      await done.future;
    }();
    final c = p.client('handle-ack-close');
    final closed = Completer<void>();
    final sub = c.messages.listen((m) {
      c.close();
      closed.complete();
    });
    try {
      await c.connect();
      final op = c.beginSubscribe('x');
      final result = await op.future;
      expect(result.operationId, op.operationId);
      await closed.future.timeout(const Duration(seconds: 3));
      await op.cancel();
      expect(c.connectionState, MqttConnectionState.closed);
    } finally {
      done.complete();
      await c.close();
      await sub.cancel();
      await p.close();
      await MqttClient.shutdownRuntime();
    }
    await broker;
  });
  test(
      'joined disconnect bookkeeping stays bounded even after local cancellations',
      () async {
    final p = await Peer.open();
    final ready = Completer<void>(),
        ack = Completer<void>(),
        done = Completer<void>();
    final broker = () async {
      final s = await p.listener.first;
      p.sockets.add(s);
      final w = Wire(s);
      await w.packet();
      s.add([0x20, 2, 0, 0]);
      await s.flush();
      final packet = await w.packet();
      ready.complete();
      await ack.future;
      final n = packet[1] * 256 + packet[2];
      s.add([0x40, 2, packet[n + 3], packet[n + 4]]);
      await s.flush();
      expect(await w.packet(), [0xe0]);
      await done.future;
    }();
    final c = p.client('handle-bound');
    final disconnected = Completer<void>();
    final sub = c.events.listen((e) {
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
      await ready.future;
      final handles = List.generate(32, (_) => c.beginDisconnect());
      for (final h in handles) {
        await h.cancel();
      }
      final rejected = c.beginDisconnect();
      await expectLater(
          rejected.future.timeout(const Duration(milliseconds: 100)),
          throwsA(isA<MqttException>().having((e) => e.code, 'code', 5)));
      ack.complete();
      await disconnected.future.timeout(const Duration(seconds: 3));
      // Underlying join futures release their slots in microtasks, without polling another event.
      await Future<void>.delayed(Duration.zero);
      await c.beginDisconnect().future;
    } finally {
      if (!ack.isCompleted) ack.complete();
      done.complete();
      await c.close();
      await sub.cancel();
      await p.close();
      await MqttClient.shutdownRuntime();
    }
    await broker;
  });
  test(
      'synchronous native rejection is terminal before immediate handle cancel',
      () async {
    final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'handle-rejection',
        port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
    try {
      await c.connect();
      final op = c.beginSubscribe('');
      await op.cancel();
      await expectLater(op.future,
          throwsA(isA<MqttException>().having((e) => e.code, 'code', 1)));
      expect(c.connectionState, MqttConnectionState.connected);
    } finally {
      await c.close();
      await MqttClient.shutdownRuntime();
    }
  });
  for (final disconnect in [false, true]) {
    test(
        '${disconnect ? 'DISCONNECT' : 'CONNACK'} committed before synchronous state-listener close stays successful',
        () async {
      final c = MqttClient(
          server: '127.0.0.1',
          clientId: 'handle-state-close-$disconnect',
          port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
      if (disconnect) await c.connect();
      final sub = c.events.listen((e) {
        if (e.state ==
            (disconnect
                ? MqttConnectionState.disconnected
                : MqttConnectionState.connected)) {
          unawaited(c.close());
        }
      });
      try {
        if (disconnect) {
          final first = c.beginDisconnect();
          final joined = c.beginDisconnect();
          await first.future;
          await joined.future;
          await first.cancel();
          await joined.cancel();
        } else {
          final op = c.beginConnect();
          expect((await op.future).sessionPresent, isFalse);
          await op.cancel();
        }
        expect(c.connectionState, MqttConnectionState.closed);
      } finally {
        await c.close();
        await sub.cancel();
        await MqttClient.shutdownRuntime();
      }
    });
  }
}
