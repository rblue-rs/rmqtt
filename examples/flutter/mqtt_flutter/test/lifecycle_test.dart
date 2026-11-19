import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:mqtt_flutter/mqtt_flutter.dart';
import 'package:test/test.dart';
import 'ack_timeout_test.dart' show Wire;

class Peer {
  final ServerSocket listener;
  final List<Socket> sockets = [];
  Peer(this.listener);
  static Future<Peer> open() async =>
      Peer(await ServerSocket.bind('127.0.0.1', 0));
  MqttClient client(String id) =>
      MqttClient(server: '127.0.0.1', clientId: id, port: listener.port);
  Future<void> close() async {
    for (final s in sockets) {
      s.destroy();
    }
    await listener.close();
  }
}

void main() {
  test(
      'concurrent disconnect is shared, and timeout leaves reusable disconnected client',
      () async {
    final p = await Peer.open();
    final received = Completer<void>(), release = Completer<void>();
    final accepts = StreamIterator<Socket>(p.listener);
    final broker = () async {
      await accepts.moveNext().timeout(const Duration(seconds: 5));
      final s = accepts.current;
      p.sockets.add(s);
      final w = Wire(s);
      await w.packet();
      s.add([0x20, 2, 0, 0]);
      await s.flush();
      await w.packet();
      received.complete(); // withhold PUBACK: disconnect cannot advance
      await accepts.moveNext().timeout(const Duration(seconds: 5));
      final fresh = accepts.current;
      p.sockets.add(fresh);
      await Wire(fresh).packet();
      fresh.add([0x20, 2, 0, 0]);
      await fresh.flush();
      await release.future;
    }();
    final c = p.client('disconnect-timeout');
    try {
      await c.connect();
      final t = c.publishMessage('x', Uint8List(1), qos: MqttQos.atLeastOnce);
      await t.whenSent;
      await received.future;
      final a = c.disconnect(timeout: const Duration(milliseconds: 40));
      final b = c.disconnect(timeout: const Duration(seconds: 2));
      final ea = expectLater(a, throwsA(isA<TimeoutException>()));
      final eb = expectLater(b, throwsA(isA<TimeoutException>()));
      await Future.wait([ea, eb]);
      expect(c.connectionState, MqttConnectionState.disconnected);
      await expectLater(t.whenAcknowledged, throwsA(isA<TimeoutException>()));
      await c.disconnect();
      await c.connect();
      expect(c.connectionState, MqttConnectionState.connected);
    } finally {
      release.complete();
      await c.close();
      await broker;
      await accepts.cancel();
      await p.close();
      await MqttClient.shutdownRuntime();
    }
  });

  test(
      'cancel publish settles local phases once, late PUBACK remains an update',
      () async {
    final p = await Peer.open();
    final received = Completer<void>(),
        release = Completer<void>(),
        done = Completer<void>();
    final broker = () async {
      final s = await p.listener.first;
      p.sockets.add(s);
      final w = Wire(s);
      await w.packet();
      s.add([0x20, 2, 0, 0]);
      await s.flush();
      final packet = await w.packet();
      received.complete();
      final n = packet[1] * 256 + packet[2];
      await release.future;
      s.add([0x40, 2, packet[n + 3], packet[n + 4]]);
      await s.flush();
      await done.future;
    }();
    final c = p.client('cancel-publish');
    final ack = Completer<void>();
    final sub = c.publishUpdates.listen((e) {
      if (e.stage == MqttPublishStage.acknowledged && !ack.isCompleted) {
        ack.complete();
      }
    });
    try {
      await c.connect();
      final t = c.publishMessage('x', Uint8List(1), qos: MqttQos.atLeastOnce);
      await t.whenSent;
      await received.future;
      final failed = expectLater(t.whenAcknowledged, throwsStateError);
      await c.cancel(t.operationId);
      await failed;
      release.complete();
      await ack.future.timeout(const Duration(seconds: 2));
      await expectLater(t.whenAcknowledged, throwsStateError);
      await c.cancel(t.operationId); // terminal cancel is harmless
      expect(c.connectionState, MqttConnectionState.connected);
    } finally {
      if (!release.isCompleted) release.complete();
      done.complete();
      await c.close();
      await broker;
      await sub.cancel();
      await p.close();
      await MqttClient.shutdownRuntime();
    }
  });

  test('late CONNACK after close in connecting event cannot resurrect client',
      () async {
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'reentrant-close', port: 1883);
    final sub = c.events.listen((e) {
      if (e.state == MqttConnectionState.connecting) c.close();
    });
    try {
      await expectLater(c.connect(), throwsStateError);
      expect(c.connectionState, MqttConnectionState.closed);
    } finally {
      await c.close();
      await sub.cancel();
      await MqttClient.shutdownRuntime();
    }
  });
  for (final unsubscribe in [false, true]) {
    test(
        '${unsubscribe ? 'UNSUBACK' : 'SUBACK'} timeout cannot settle a newer operation',
        () async {
      final p = await Peer.open();
      final sent = Completer<void>(),
          release = Completer<void>(),
          done = Completer<void>();
      final broker = () async {
        final s = await p.listener.first;
        p.sockets.add(s);
        final w = Wire(s);
        await w.packet();
        s.add([0x20, 2, 0, 0]);
        await s.flush();
        final old = await w.packet();
        sent.complete();
        await release.future;
        s.add(unsubscribe
            ? [0xb0, 2, old[1], old[2]]
            : [0x90, 3, old[1], old[2], 1]);
        await s.flush();
        final newer = await w.packet();
        expect(newer[0] & 0xf0, 0x80);
        s.add([0x90, 3, newer[1], newer[2], 0]);
        await s.flush();
        await done.future;
      }();
      final c = p.client('late-control-$unsubscribe');
      try {
        await c.connect();
        final Future<Object?> old = unsubscribe
            ? c.unsubscribe('old', timeout: const Duration(milliseconds: 40))
            : c.subscribe('old', timeout: const Duration(milliseconds: 40));
        final expired = expectLater(old, throwsA(isA<TimeoutException>()));
        await sent.future;
        await expired;
        final newer = c.subscribe('new', qos: MqttQos.atMostOnce);
        release.complete();
        final result = await newer;
        expect(result.qos, MqttQos.atMostOnce);
        await expectLater(old, throwsA(isA<TimeoutException>()));
        expect(c.connectionState, MqttConnectionState.connected);
      } finally {
        if (!release.isCompleted) release.complete();
        done.complete();
        await c.close();
        await broker;
        await p.close();
        await MqttClient.shutdownRuntime();
      }
    });
  }

  test('force close settles all pending ticket phases once, QoS0 has no ACK',
      () async {
    final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'close-all',
        port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
    final updates = <MqttPublishUpdate>[];
    final sub = c.publishUpdates.listen(updates.add);
    try {
      await c.connect();
      final a = c.publishMessage('lifecycle/all', Uint8List(1),
          qos: MqttQos.atLeastOnce);
      final b = c.publishMessage('lifecycle/all', Uint8List(1));
      expect(b.whenAcknowledged, isNull);
      final failures = [
        a.whenQueued,
        a.whenSent,
        a.whenAcknowledged!,
        b.whenQueued,
        b.whenSent
      ].map((f) => expectLater(f, throwsStateError)).toList();
      await c.close();
      await Future.wait(failures);
      await c.close();
      final count = updates.length;
      await Future<void>.delayed(const Duration(milliseconds: 40));
      expect(updates.length, count);
      expect(c.connectionState, MqttConnectionState.closed);
      await expectLater(c.cancel(a.operationId), throwsStateError);
    } finally {
      await c.close();
      await sub.cancel();
      await MqttClient.shutdownRuntime();
    }
  });

  test(
      'connect timeout destroys old transport and a later connection is isolated',
      () async {
    final p = await Peer.open();
    final accepts = StreamIterator<Socket>(p.listener);
    final firstReceived = Completer<void>(),
        release = Completer<void>(),
        done = Completer<void>();
    final broker = () async {
      await accepts.moveNext();
      final old = accepts.current;
      p.sockets.add(old);
      await Wire(old).packet();
      firstReceived.complete();
      await release.future;
      // The old connection may already be closed. Never use its ACK for the new one.
      try {
        old.add([0x20, 2, 0, 0]);
        await old.flush();
      } catch (_) {}
      await accepts.moveNext();
      final fresh = accepts.current;
      p.sockets.add(fresh);
      await Wire(fresh).packet();
      fresh.add([0x20, 2, 0, 0]);
      await fresh.flush();
      await done.future;
    }();
    final c = p.client('connect-timeout');
    try {
      final old = c.connect(timeout: const Duration(milliseconds: 50));
      final failed = expectLater(old, throwsA(isA<TimeoutException>()));
      await firstReceived.future;
      await failed;
      expect(c.connectionState, MqttConnectionState.disconnected);
      final fresh = c.connect();
      release.complete();
      await fresh;
      expect(c.connectionState, MqttConnectionState.connected);
      await expectLater(old, throwsA(isA<TimeoutException>()));
    } finally {
      if (!release.isCompleted) release.complete();
      done.complete();
      await c.close();
      await broker;
      await accepts.cancel();
      await p.close();
      await MqttClient.shutdownRuntime();
    }
  });

  test(
      'shutdown with live client is Busy and duplicate connect does not open another lease',
      () async {
    final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'runtime-busy',
        port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
    try {
      await c.connect();
      await expectLater(c.connect(), throwsStateError);
      await expectLater(MqttClient.shutdownRuntime(), throwsStateError);
      await c.disconnect();
      await c.disconnect();
      await c.connect();
    } finally {
      await c.close();
      await MqttClient.shutdownRuntime();
      await MqttClient.shutdownRuntime();
    }
  });

  test('reentrant disconnect joins the first deadline', () async {
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
    final c = p.client('reentrant-disconnect');
    Future<void>? joined;
    final sub = c.events.listen((e) {
      if (e.state.name == 'disconnecting' && joined == null) {
        // Prevent recursive event handling by installing a placeholder first.
        joined = Future<void>.value();
        joined = c.disconnect(timeout: const Duration(milliseconds: 1));
        joined!.catchError((Object _) {});
      }
    });
    try {
      await c.connect();
      final t = c.publishMessage('x', Uint8List(1), qos: MqttQos.atLeastOnce);
      await t.whenSent;
      await ready.future;
      final watch = Stopwatch()..start();
      await expectLater(
          c.disconnect(timeout: const Duration(milliseconds: 100)),
          throwsA(isA<TimeoutException>()));
      expect(watch.elapsedMilliseconds, greaterThanOrEqualTo(70));
      await expectLater(joined!, throwsA(isA<TimeoutException>()));
    } finally {
      done.complete();
      await c.close();
      await broker;
      await sub.cancel();
      await p.close();
      await MqttClient.shutdownRuntime();
    }
  });
  for (final cancel in [false, true]) {
    test(
        '${cancel ? 'cancel' : 'close'} settles pending connect without late ACK reuse',
        () async {
      final p = await Peer.open();
      final ready = Completer<void>(),
          release = Completer<void>(),
          done = Completer<void>();
      final broker = () async {
        final s = await p.listener.first;
        p.sockets.add(s);
        await Wire(s).packet();
        ready.complete();
        await release.future;
        if (cancel) {
          s.add([0x20, 2, 0, 0]);
          await s.flush();
        }
        await done.future;
      }();
      final c = p.client('connect-settle-$cancel');
      final connected = Completer<void>();
      final sub = c.events.listen((e) {
        if (e.state == MqttConnectionState.connected &&
            !connected.isCompleted) {
          connected.complete();
        }
      });
      try {
        final pending = c.connect();
        final failed = expectLater(pending, throwsStateError);
        await ready.future;
        // This fixture's first native request is deterministically op 1.
        // Production code should use an id it owns, not infer ids from timing.
        if (cancel) {
          await c.cancel(1);
        } else {
          await c.close();
        }
        await failed;
        release.complete();
        if (cancel) {
          await connected.future.timeout(const Duration(seconds: 2));
          expect(c.connectionState, MqttConnectionState.connected);
        } else {
          expect(c.connectionState, MqttConnectionState.closed);
        }
        await expectLater(pending, throwsStateError);
      } finally {
        if (!release.isCompleted) release.complete();
        done.complete();
        await c.close();
        await broker;
        await sub.cancel();
        await p.close();
        await MqttClient.shutdownRuntime();
      }
    });
  }

  test(
      'cancel before polling fails pending phases but does not retract wire PUBLISH',
      () async {
    final p = await Peer.open();
    final done = Completer<void>(), received = Completer<void>();
    final broker = () async {
      final s = await p.listener.first;
      p.sockets.add(s);
      final w = Wire(s);
      await w.packet();
      s.add([0x20, 2, 0, 0]);
      await s.flush();
      final packet = await w.packet();
      expect(packet[0] & 0xf0, 0x30);
      received.complete();
      await done.future;
    }();
    final c = p.client('cancel-all-phases');
    try {
      await c.connect();
      final t = c.publishMessage('x', Uint8List(1), qos: MqttQos.atLeastOnce);
      final settled = [t.whenQueued, t.whenSent, t.whenAcknowledged!]
          .map((f) => expectLater(f, throwsA(isA<MqttOperationCancelled>())))
          .toList();
      await c.cancel(t.operationId);
      await Future.wait(settled);
      await received.future.timeout(const Duration(seconds: 2));
      await c.cancel(t.operationId);
    } finally {
      done.complete();
      await c.close();
      await broker;
      await p.close();
      await MqttClient.shutdownRuntime();
    }
  });

  test(
      'disconnect while connecting aborts attempt but does not close Dart client',
      () async {
    final p = await Peer.open();
    final ready = Completer<void>(), done = Completer<void>();
    final broker = () async {
      final s = await p.listener.first;
      p.sockets.add(s);
      await Wire(s).packet();
      ready.complete();
      await done.future;
    }();
    final c = p.client('disconnect-connecting');
    try {
      final connection = c.connect();
      final failed = expectLater(connection, throwsStateError);
      await ready.future;
      await c.disconnect();
      await failed;
      expect(c.connectionState, MqttConnectionState.disconnected);
      await c.disconnect();
    } finally {
      done.complete();
      await c.close();
      await broker;
      await p.close();
      await MqttClient.shutdownRuntime();
    }
  });
}
