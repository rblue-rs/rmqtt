import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';
import 'reconnect_test.dart' show readMqtt;

const options = MqttConnectOptions(
    protocol: MqttProtocol.v5, keepAlivePeriod: Duration(seconds: 5));

void main() {
  test('v5 zero and sub-five keepalive are Unsupported without packets',
      () async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    var accepts = 0;
    final sub = listener.listen((s) {
      accepts++;
      s.destroy();
    });
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'v5-zero', port: listener.port);
    try {
      for (var secs = 0; secs < 5; secs++) {
        await expectLater(
            c.connect(
                options: MqttConnectOptions(
                    protocol: MqttProtocol.v5,
                    keepAlivePeriod: Duration(seconds: secs))),
            throwsA(isA<MqttException>().having((e) => e.code, 'code', 6)));
      }
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(accepts, 0);
      expect(const MqttConnectOptions().keepAlivePeriod, Duration.zero);
    } finally {
      await c.close();
      await sub.cancel();
      await listener.close();
      await MqttClient.shutdownRuntime();
    }
  });

  test('v5 connect subscribe binary publish inbound unsubscribe disconnect',
      () async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final server = () async {
      final s = await listener.first.timeout(const Duration(seconds: 3));
      final connect = await readMqtt(s);
      expect(connect[7], 5);
      expect(connect.sublist(9, 11), [0, 5]);
      s.add([0x20, 3, 0, 0, 0]);
      final sub = await readMqtt(s);
      s.add([0x90, 4, sub[1], sub[2], 0, 1]);
      for (final q in [0, 1]) {
        final p = await readMqtt(s);
        expect((p[0] >> 1) & 3, q);
        if (q == 1) {
          final n = (p[1] << 8) | p[2];
          s.add([0x40, 2, p[3 + n], p[4 + n]]);
        }
      }
      s.add([0x32, 9, 0, 1, 116, 0, 77, 0, 1, 0, 255]);
      expect(await readMqtt(s), [0x40, 0, 77]);
      final u = await readMqtt(s);
      s.add([0xb0, 4, u[1], u[2], 0, 0]);
      expect((await readMqtt(s))[0], 0xe0);
      s.destroy();
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'v5-dart', port: listener.port);
    final message = c.messages.first;
    try {
      expect((await c.connect(options: options)).sessionPresent, isFalse);
      await c.subscribe('t');
      for (final q in [MqttQos.atMostOnce, MqttQos.atLeastOnce]) {
        final ticket =
            c.publishMessage('t', Uint8List.fromList([1, 0, 255]), qos: q);
        await ticket.whenSent;
        if (q == MqttQos.atLeastOnce) {
          await ticket.whenAcknowledged!;
        }
      }
      expect((await message).payload, [1, 0, 255]);
      await c.unsubscribe('t');
      await c.disconnect();
      await server;
    } finally {
      await c.close();
      await listener.close();
      await MqttClient.shutdownRuntime();
    }
  });

  test('v5 cancel local subscribe then late SUBACK still allows bounded drain',
      () async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final sent = Completer<void>();
    final release = Completer<void>();
    final server = () async {
      final s = await listener.first.timeout(const Duration(seconds: 3));
      await readMqtt(s);
      s.add([0x20, 3, 0, 0, 0]);
      final sub = await readMqtt(s);
      sent.complete();
      await release.future;
      s.add([0x90, 4, sub[1], sub[2], 0, 1]);
      expect((await readMqtt(s))[0], 0xe0);
      s.destroy();
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'v5-cancel', port: listener.port);
    try {
      await c.connect(options: options);
      final op = c.beginSubscribe('t');
      final abandoned =
          expectLater(op.future, throwsA(isA<MqttOperationCancelled>()));
      await sent.future;
      await op.cancel();
      await abandoned;
      final drained = c.drainClose(timeout: const Duration(seconds: 1));
      release.complete();
      expect(await drained, MqttDrainResult.drained);
      await server;
    } finally {
      await c.close();
      await listener.close();
      await MqttClient.shutdownRuntime();
    }
  });

  test('v5 negative PUBACK fails ticket once instead of acknowledged',
      () async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final stop = Completer<void>();
    final server = () async {
      final s = await listener.first.timeout(const Duration(seconds: 3));
      await readMqtt(s);
      s.add([0x20, 3, 0, 0, 0]);
      final p = await readMqtt(s);
      final n = (p[1] << 8) | p[2];
      s.add([0x40, 4, p[3 + n], p[4 + n], 0x87, 0]);
      await stop.future;
      s.destroy();
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'v5-negative', port: listener.port);
    try {
      await c.connect(options: options);
      final ticket = c.publishMessage('t', Uint8List.fromList([1]),
          qos: MqttQos.atLeastOnce);
      final failed = expectLater(ticket.whenAcknowledged!,
          throwsA(isA<MqttException>().having((e) => e.code, 'code', 8)));
      await ticket.whenSent;
      await failed;
    } finally {
      stop.complete();
      await server;
      await c.close();
      await listener.close();
      await MqttClient.shutdownRuntime();
    }
  });
  test('v5 unacked publish fails on drop even with reconnect enabled',
      () async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final drop = Completer<void>();
    final server = () async {
      final socket = await listener.first.timeout(const Duration(seconds: 3));
      await readMqtt(socket);
      socket.add([0x20, 3, 0, 0, 0]);
      await readMqtt(socket);
      await drop.future;
      socket.destroy();
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'v5-drop', port: listener.port);
    final events = <MqttClientEvent>[];
    final sub = c.events.listen(events.add);
    try {
      await c.connect(
          options: const MqttConnectOptions(
              protocol: MqttProtocol.v5,
              keepAlivePeriod: Duration(seconds: 5),
              reconnectPolicy: MqttReconnectPolicy(enabled: true)));
      final ticket = c.publishMessage('t', Uint8List.fromList([1]),
          qos: MqttQos.atLeastOnce);
      final failed = expectLater(ticket.whenAcknowledged!,
          throwsA(isA<MqttException>().having((e) => e.code, 'code', 7)));
      await ticket.whenSent;
      drop.complete();
      await failed;
      await server;
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(events.where((e) => e.kind == MqttClientEventKind.reconnecting),
          isEmpty);
    } finally {
      await c.close();
      await sub.cancel();
      await listener.close();
      await MqttClient.shutdownRuntime();
    }
  });

  test('v5 force-close settles unacked ticket without draining', () async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final release = Completer<void>();
    final server = () async {
      final socket = await listener.first.timeout(const Duration(seconds: 3));
      await readMqtt(socket);
      socket.add([0x20, 3, 0, 0, 0]);
      await readMqtt(socket);
      await release.future;
      socket.destroy();
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'v5-force', port: listener.port);
    try {
      await c.connect(options: options);
      final ticket = c.publishMessage('t', Uint8List.fromList([1]),
          qos: MqttQos.atLeastOnce);
      final failed =
          expectLater(ticket.whenAcknowledged!, throwsA(isA<StateError>()));
      await ticket.whenSent;
      await c.close();
      await failed;
    } finally {
      release.complete();
      await server;
      await c.close();
      await listener.close();
      await MqttClient.shutdownRuntime();
    }
  });
}
