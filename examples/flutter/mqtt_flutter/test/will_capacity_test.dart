import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';
import 'ack_timeout_test.dart' show Wire;

void main() {
  test('tiny Will view does not retain caller backing allocation', () {
    final backing = Uint8List(32 * 1024 * 1024);
    backing[3] = 7;
    final view = Uint8List.view(backing.buffer, 3, 1);
    final will = MqttWill(topic: 'dummy', payload: view);
    expect(will.payload.buffer.lengthInBytes, 1);
    expect(will.payload.offsetInBytes, 0);
    backing[3] = 9;
    expect(will.payload.single, 7);
  });
  test(
      'Will65536 InvalidArgument before construction copy, publish limit unchanged',
      () {
    expect(() => MqttWill(topic: 'dummy', payload: Uint8List(65536)),
        throwsA(isA<MqttException>().having((e) => e.code, 'code', 1)));
    expect(MqttWill(topic: 'dummy', payload: Uint8List(65535)).payload.length,
        65535);
  });
  for (final v5 in [false, true]) {
    test('public Will65535 wire length and body match protocol ${v5 ? 5 : 4}',
        () async {
      final listener = await ServerSocket.bind('127.0.0.1', 0);
      Socket? socket;
      final done = Completer<void>();
      final broker = () async {
        final s = socket = await listener.first;
        final wire = Wire(s);
        final p = await wire.packet();
        expect(p.first, 0x10);
        var pos = 11;
        int u16() {
          final n = p[pos] * 256 + p[pos + 1];
          pos += 2;
          return n;
        }

        void properties() {
          var n = 0, m = 1;
          while (true) {
            final b = p[pos++];
            n += (b & 127) * m;
            m *= 128;
            if (b < 128) break;
          }
          pos += n;
        }

        if (v5) properties();
        final id = u16();
        pos += id;
        if (v5) properties();
        final topic = u16();
        pos += topic;
        expect(u16(), 65535);
        expect(p.length - pos, 65535);
        s.add(v5 ? [32, 3, 0, 0, 0] : [32, 2, 0, 0]);
        await s.flush();
        expect(await wire.packet(), [0xe0]);
        await done.future;
      }();
      final c = MqttClient(
          server: '127.0.0.1', clientId: 'will-bound', port: listener.port);
      try {
        await c.connect(
            options: MqttConnectOptions(
                protocol: v5 ? MqttProtocol.v5 : MqttProtocol.v311,
                keepAlivePeriod:
                    v5 ? const Duration(seconds: 5) : Duration.zero,
                will: MqttWill(topic: 'dummy', payload: Uint8List(65535))));
        await c.disconnect();
      } finally {
        done.complete();
        await c.close();
        socket?.destroy();
        await listener.close();
        await MqttClient.shutdownRuntime();
      }
      await broker;
    });
  }
}
