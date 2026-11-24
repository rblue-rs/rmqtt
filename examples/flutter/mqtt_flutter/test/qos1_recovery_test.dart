import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';

import 'reconnect_test.dart' show readMqtt, writeConnack;

int packetId(List<int> p) {
  final n = (p[1] << 8) | p[2];
  return (p[3 + n] << 8) | p[4 + n];
}

void main() {
  test(
    'persistent QoS1 ticket survives repeated drops and a local wait timeout',
    () async {
      await MqttClient.ensureInitialized();
      final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      final incoming = StreamIterator<Socket>(listener);
      final sockets = <Socket>[];
      Future<Socket> accept() async {
        expect(
          await incoming.moveNext().timeout(const Duration(seconds: 3)),
          isTrue,
        );
        final s = incoming.current;
        sockets.add(s);
        return s;
      }

      final drop = Completer<void>();
      final replayReady = Completer<void>();
      final allowAck = Completer<void>();
      final server = () async {
        var s = await accept();
        await readMqtt(s);
        writeConnack(s);
        final original = await readMqtt(s);
        expect(original[0], 0x32);
        final id = packetId(original);
        await drop.future;
        s.destroy();
        for (var i = 0; i < 2; i++) {
          s = await accept();
          await readMqtt(s);
          writeConnack(s, sessionPresent: true);
          final replay = await readMqtt(s);
          expect(replay[0], 0x3a);
          expect(replay.sublist(1), original.sublist(1));
          if (i == 0) {
            s.destroy();
          } else {
            replayReady.complete();
            await allowAck.future;
            s.add([0x40, 2, id >> 8, id & 255]);
            expect((await readMqtt(s))[0], 0xe0);
          }
        }
      }();
      final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'qos1-resume',
        port: listener.port,
      );
      final updates = <MqttPublishUpdate>[];
      final subscription = c.publishUpdates.listen(updates.add);
      try {
        await c.connect(
          options: const MqttConnectOptions(
            cleanSession: false,
            reconnectPolicy: MqttReconnectPolicy(
              enabled: true,
              baseDelay: Duration(milliseconds: 10),
              maxDelay: Duration(milliseconds: 10),
            ),
          ),
        );
        final ticket = c.publishMessage(
          'binary',
          Uint8List.fromList([0, 255, 128]),
          qos: MqttQos.atLeastOnce,
        );
        await ticket.whenSent.timeout(const Duration(seconds: 3));
        final id = ticket.packetId;
        drop.complete();
        await replayReady.future.timeout(const Duration(seconds: 3));
        // Abandoning this local Future wait does not withdraw the wire packet/ticket.
        await expectLater(
          ticket.whenAcknowledged!.timeout(const Duration(milliseconds: 1)),
          throwsA(isA<TimeoutException>()),
        );
        allowAck.complete();
        await ticket.whenAcknowledged!.timeout(const Duration(seconds: 3));
        expect(ticket.packetId, id);
        await c.disconnect();
        await server.timeout(const Duration(seconds: 3));
        expect(
          updates.where((u) => u.stage == MqttPublishStage.sent).length,
          1,
        );
        expect(
          updates.where((u) => u.stage == MqttPublishStage.acknowledged).length,
          1,
        );
      } finally {
        await c.close();
        for (final s in sockets) {
          s.destroy();
        }
        await incoming.cancel();
        await listener.close();
        await subscription.cancel();
        await MqttClient.shutdownRuntime();
      }
    },
  );
  for (final clean in [false, true]) {
    test(
      'interrupted QoS1 ticket fails explicitly for clean=$clean / no session',
      () async {
        await MqttClient.ensureInitialized();
        final listener = await ServerSocket.bind(
          InternetAddress.loopbackIPv4,
          0,
        );
        final sockets = <Socket>[];
        var connections = 0;
        final sub = listener.listen((s) async {
          sockets.add(s);
          connections++;
          await readMqtt(s);
          writeConnack(s);
          if (connections == 1) {
            expect((await readMqtt(s))[0], 0x32);
            s.destroy();
          }
        });
        final c = MqttClient(
          server: '127.0.0.1',
          clientId: 'lost-session',
          port: listener.port,
        );
        try {
          await c.connect(
            options: MqttConnectOptions(
              cleanSession: clean,
              reconnectPolicy: const MqttReconnectPolicy(
                enabled: true,
                baseDelay: Duration(milliseconds: 10),
                maxDelay: Duration(milliseconds: 10),
              ),
            ),
          );
          final ticket = c.publishMessage(
            'x',
            Uint8List.fromList([0, 255]),
            qos: MqttQos.atLeastOnce,
          );
          await expectLater(
            ticket.whenAcknowledged!.timeout(const Duration(seconds: 3)),
            throwsA(isA<MqttException>().having((e) => e.code, 'Network', 7)),
          );
          expect(connections, clean ? 1 : 2);
        } finally {
          await c.close();
          for (final s in sockets) {
            s.destroy();
          }
          await sub.cancel();
          await listener.close();
          await MqttClient.shutdownRuntime();
        }
      },
    );
  }

  for (final cancel in [false, true]) {
    test(
        'interrupted subscribe preserves expectation and settles only original waiter cancel=$cancel',
        () async {
      await MqttClient.ensureInitialized();
      final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      final incoming = StreamIterator<Socket>(listener);
      final sockets = <Socket>[];
      Future<Socket> accept() async {
        expect(await incoming.moveNext().timeout(const Duration(seconds: 3)),
            isTrue);
        final s = incoming.current;
        sockets.add(s);
        return s;
      }

      final seen = Completer<void>();
      final drop = Completer<void>();
      final reconciled = Completer<void>();
      final server = () async {
        var s = await accept();
        await readMqtt(s);
        writeConnack(s);
        final original = await readMqtt(s);
        expect(original[0], 0x82);
        seen.complete();
        await drop.future;
        s.destroy();
        s = await accept();
        await readMqtt(s);
        writeConnack(s);
        final restored = await readMqtt(s);
        expect(restored[0], 0x82);
        // Old-id SUBACK cannot complete the resubscribe or a new operation.
        s.add([0x90, 3, original[1], original[2], 1]);
        s.add([0x90, 3, restored[1], restored[2], 1]);
        final next = await readMqtt(s);
        expect(next[0], 0x82);
        s.add([0x90, 3, original[1], original[2], 1]);
        s.add([0x90, 3, next[1], next[2], 1]);
        expect((await readMqtt(s))[0], 0xe0);
      }();
      final c = MqttClient(
          server: '127.0.0.1',
          clientId: 'control-recovery',
          port: listener.port);
      final sub = c.events.listen((e) {
        if (e.kind == MqttClientEventKind.subscriptionsResubscribed &&
            !reconciled.isCompleted) {
          reconciled.complete();
        }
      });
      try {
        await c.connect(
            options: const MqttConnectOptions(
                cleanSession: false,
                reconnectPolicy: MqttReconnectPolicy(
                    enabled: true,
                    baseDelay: Duration(milliseconds: 10),
                    maxDelay: Duration(milliseconds: 10))));
        final op = c.beginSubscribe('held');
        final expected = expectLater(
            op.future,
            throwsA(cancel
                ? isA<MqttOperationCancelled>()
                : isA<MqttException>().having((e) => e.code, 'Network', 7)));
        await seen.future.timeout(const Duration(seconds: 3));
        if (cancel) {
          await op.cancel();
        }
        drop.complete();
        await expected.timeout(const Duration(seconds: 3));
        await reconciled.future.timeout(const Duration(seconds: 3));
        final next =
            await c.subscribe('next').timeout(const Duration(seconds: 3));
        expect(next.operationId, isNot(op.operationId));
        if (cancel) {
          await expectLater(op.future, throwsA(isA<MqttOperationCancelled>()));
        }
        await c.disconnect();
        await server.timeout(const Duration(seconds: 3));
      } finally {
        await c.close();
        for (final s in sockets) {
          s.destroy();
        }
        await incoming.cancel();
        await listener.close();
        await sub.cancel();
        await MqttClient.shutdownRuntime();
      }
    });
  }

  for (final qos in [MqttQos.atLeastOnce, MqttQos.exactlyOnce]) {
    test(
        '${qos.name} unacked native ticket exhausts budget despite successful CONNACKs',
        () async {
      await MqttClient.ensureInitialized();
      final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      final incoming = StreamIterator<Socket>(listener);
      final sockets = <Socket>[];
      final server = () async {
        for (var attempt = 0; attempt < 3; attempt++) {
          expect(await incoming.moveNext().timeout(const Duration(seconds: 3)),
              isTrue);
          final s = incoming.current;
          sockets.add(s);
          await readMqtt(s);
          writeConnack(s, sessionPresent: attempt != 0);
          final p = await readMqtt(s);
          expect(p[0], (0x30 | (qos.index << 1)) | (attempt == 0 ? 0 : 8));
          s.destroy();
        }
      }();
      final c = MqttClient(
          server: '127.0.0.1',
          clientId: 'finite-${qos.name}',
          port: listener.port);
      try {
        await c.connect(
            options: const MqttConnectOptions(
                cleanSession: false,
                reconnectPolicy: MqttReconnectPolicy(
                    enabled: true,
                    maxAttempts: 2,
                    baseDelay: Duration(milliseconds: 10),
                    maxDelay: Duration(milliseconds: 10))));
        final ticket =
            c.publishMessage('x', Uint8List.fromList([0, 255]), qos: qos);
        await expectLater(
            ticket.whenAcknowledged!.timeout(const Duration(seconds: 3)),
            throwsA(isA<StateError>()));
        await server.timeout(const Duration(seconds: 3));
        expect(sockets.length, 3);
      } finally {
        await c.close();
        for (final s in sockets) {
          s.destroy();
        }
        await incoming.cancel();
        await listener.close();
        await MqttClient.shutdownRuntime();
      }
    });
  }
}
