// Library-backed WS fixture; no broker/mobile/aggregate memory claim.
import 'dart:async';
import 'dart:io';

import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';

import 'websocket_wire_test.dart' as fixture;

void main() {
  for (final v5 in [false, true]) {
    test(
      'WS v5=$v5 expectation quota preserves cancelled duplicate and late ACK',
      () async {
        final http = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
        final held = Completer<void>();
        final release = Completer<void>();
        final server = () async {
          final ws = await fixture.upgrade(await http.first);
          final r = fixture.Reader(ws);
          Future<List<int>> packet() async {
            while (true) {
              final p = await r.packet();
              if (p[0] != 0xc0) return p;
              fixture.send(ws, [0xd0, 0]);
            }
          }

          String topic(List<int> p) {
            final start = v5 ? 4 : 3;
            final n = (p[start] << 8) | p[start + 1];
            return String.fromCharCodes(p.sublist(start + 2, start + 2 + n));
          }

          expect((await packet())[0], 0x10);
          fixture.connack(ws, v5);
          for (var i = 0; i < 128; i++) {
            final p = await packet();
            expect(p[0], 0x82);
            expect(topic(p), 't/$i');
            fixture.suback(ws, p, v5);
          }
          final duplicate = await packet();
          expect(topic(duplicate), 't/0'); // rejects must never reach this wire
          held.complete();
          await release.future;
          fixture.suback(ws, duplicate, v5); // late after local cancellation
          final u = await packet();
          expect(u[0], 0xa2);
          expect(topic(u), 't/0');
          fixture.send(
            ws,
            v5 ? [0xb0, 4, u[1], u[2], 0, 0] : [0xb0, 2, u[1], u[2]],
          );
          final replacement = await packet();
          expect(topic(replacement), 'replacement');
          fixture.suback(ws, replacement, v5);
          expect(await packet(), [0xe0]); // positive no-unwanted-packet fence
          await ws.close();
        }();
        final c = MqttClient(
          server: '127.0.0.1',
          clientId: 'quota-$v5',
          port: http.port,
        );
        try {
          await c.connect(options: fixture.options(v5));
          for (var i = 0; i < 128; i++) {
            await c.subscribe('t/$i');
          }
          await expectLater(
            c.subscribe('forbidden'),
            throwsA(isA<MqttException>().having((e) => e.code, 'code', 5)),
          );
          final duplicate = c.beginSubscribe('t/0');
          final cancelled = expectLater(
            duplicate.future,
            throwsA(isA<MqttOperationCancelled>()),
          );
          await held.future.timeout(const Duration(seconds: 4));
          await duplicate.cancel();
          await cancelled;
          release.complete();
          await c.unsubscribe('t/0');
          await c.subscribe('replacement');
          await c.disconnect();
          await server;
        } finally {
          if (!release.isCompleted) release.complete();
          await c.close();
          await http.close(force: true);
          await MqttClient.shutdownRuntime();
        }
      },
    );
  }
}
