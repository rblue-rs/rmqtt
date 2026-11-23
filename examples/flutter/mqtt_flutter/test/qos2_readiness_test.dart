import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';
import 'ack_timeout_test.dart' show Wire;

void main() {
  test('QoS2 is appended without changing existing ordinals', () {
    expect(MqttQos.values.map((q) => q.index), [0, 1, 2]);
    expect(MqttQos.values.last.name, 'exactlyOnce');
  });
  for (final proto in [MqttProtocol.v311, MqttProtocol.v5]) {
    for (final cancel in [false, true]) {
      test('public QoS2 PUBCOMP-only ${proto.name} cancel=$cancel', () async {
        final listener = await ServerSocket.bind('127.0.0.1', 0);
        final complete = Completer<void>();
        Socket? peer;
        final broker = () async {
          final s = peer = await listener.first;
          final w = Wire(s);
          await w.packet();
          s.add(
              proto == MqttProtocol.v5 ? [0x20, 3, 0, 0, 0] : [0x20, 2, 0, 0]);
          await s.flush();
          final p = await w.packet();
          expect(p[0] & 6, 4);
          final n = (p[1] << 8) | p[2];
          final id = p.sublist(n + 3, n + 5);
          for (var i = 0; i < 2; i++) {
            s.add([0x50, 2, ...id]);
            await s.flush();
            expect(await w.packet(), [
              0x62,
              ...id,
              if (proto == MqttProtocol.v5) ...[0, 0]
            ]);
          }
          // A wrong-ID COMP followed by an application packet is a read fence.
          s.add([0x70, 2, id[0], id[1] + 1]);
          final body = [
            0,
            1,
            102,
            if (proto == MqttProtocol.v5) 0,
            103,
            97,
            116,
            101
          ];
          s.add([0x30, body.length, ...body]);
          await s.flush();
          await complete.future;
          s.add([0x70, 2, ...id, 0x70, 2, ...id]);
          await s.flush();
          expect(await w.packet(),
              [0xe0]); // DISCONNECT fence, not an ACK+EOF race.
        }();
        final c = MqttClient(
            server: '127.0.0.1',
            clientId: 'dart-qos2-${proto.name}-$cancel',
            port: listener.port);
        var acknowledged = 0;
        final update = Completer<void>();
        final sub = c.publishUpdates.listen((e) {
          if (e.stage == MqttPublishStage.acknowledged) {
            acknowledged++;
            if (!update.isCompleted) update.complete();
          }
        });
        try {
          await c.connect(
              options: MqttConnectOptions(
                  protocol: proto,
                  keepAlivePeriod: proto == MqttProtocol.v5
                      ? const Duration(seconds: 5)
                      : Duration.zero));
          final fence = c.messages.first;
          final t = c.publishMessage('x', Uint8List.fromList([0, 255]),
              qos: MqttQos.exactlyOnce);
          await t.whenSent;
          expect(t.whenAcknowledged, isNotNull);
          var done = false;
          final ack = t.whenAcknowledged!;
          ack.then((_) {
            done = true;
          }, onError: (Object e) {});
          expect((await fence.timeout(const Duration(seconds: 5))).payload,
              [103, 97, 116, 101]);
          expect(done, isFalse);
          expect(acknowledged, 0);
          if (cancel) {
            final failure = expectLater(ack, throwsStateError);
            await c.cancel(t.operationId);
            await failure;
          }
          complete.complete();
          await update.future.timeout(const Duration(seconds: 3));
          if (!cancel) await ack;
          await c.disconnect();
          await broker;
          expect(acknowledged, 1);
          if (cancel) await expectLater(ack, throwsStateError);
        } finally {
          if (!complete.isCompleted) complete.complete();
          await c.close();
          peer?.destroy();
          await listener.close();
          await broker.catchError((Object _) {});
          await sub.cancel();
          await MqttClient.shutdownRuntime();
        }
      });
    }
  }

  for (final phase in [0, 1]) {
    test(
        'v5 negative QoS2 ${phase == 0 ? 'PUBREC' : 'PUBCOMP'} fails ticket and closes',
        () async {
      final listener = await ServerSocket.bind('127.0.0.1', 0);
      Socket? peer;
      final broker = () async {
        final s = peer = await listener.first;
        final w = Wire(s);
        await w.packet();
        s.add([0x20, 3, 0, 0, 0]);
        await s.flush();
        final p = await w.packet();
        expect(p[0] & 6, 4);
        final n = (p[1] << 8) | p[2];
        final id = p.sublist(n + 3, n + 5);
        if (phase == 1) {
          s.add([0x50, 2, ...id]);
          await s.flush();
          expect(await w.packet(), [0x62, ...id, 0, 0]);
        }
        s.add(
            [phase == 0 ? 0x50 : 0x70, 4, ...id, phase == 0 ? 0x97 : 0x92, 0]);
        await s.flush();
        expect(await w.input.moveNext().timeout(const Duration(seconds: 3)),
            isFalse);
      }();
      final c = MqttClient(
          server: '127.0.0.1',
          clientId: 'negative-qos2-$phase',
          port: listener.port);
      try {
        await c.connect(
            options: const MqttConnectOptions(
                protocol: MqttProtocol.v5,
                keepAlivePeriod: Duration(seconds: 5)));
        final t = c.publishMessage('q', Uint8List.fromList([0, 255]),
            qos: MqttQos.exactlyOnce);
        await expectLater(
            t.whenAcknowledged!.timeout(const Duration(seconds: 3)),
            throwsA(isA<MqttException>().having((e) => e.code, 'Refused', 8)));
        await broker;
        expect(c.connectionState, MqttConnectionState.disconnected);
      } finally {
        await c.close();
        peer?.destroy();
        await listener.close();
        await broker.catchError((Object _) {});
        await MqttClient.shutdownRuntime();
      }
    });
  }
}
