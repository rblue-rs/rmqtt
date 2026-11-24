// Deterministic MQTT gates; WebSocket framing is Dart's library, not a broker.
import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';

class Reader {
  final StreamIterator<dynamic> iter;
  final List<int> bytes = [];
  Reader(WebSocket ws) : iter = StreamIterator<dynamic>(ws);
  Future<int> byte() async {
    while (bytes.isEmpty) {
      if (!await iter.moveNext().timeout(const Duration(seconds: 4))) {
        throw StateError('WS closed');
      }
      expect(iter.current, isA<List<int>>());
      bytes.addAll(iter.current as List<int>);
    }
    return bytes.removeAt(0);
  }

  Future<List<int>> packet() async {
    final h = await byte();
    var size = 0, shift = 0;
    while (true) {
      final b = await byte();
      size += (b & 127) << shift;
      if (b & 128 == 0) {
        break;
      }
      shift += 7;
    }
    return [h, for (var n = 0; n < size; n++) await byte()];
  }
}

void send(WebSocket s, List<int> p) => s.add(Uint8List.fromList(p));
void connack(WebSocket s, bool v5) =>
    send(s, v5 ? [0x20, 3, 0, 0, 0] : [0x20, 2, 0, 0]);
void suback(WebSocket s, List<int> p, bool v5) =>
    send(s, v5 ? [0x90, 4, p[1], p[2], 0, 1] : [0x90, 3, p[1], p[2], 1]);
MqttConnectOptions options(bool v5) => MqttConnectOptions(
    useWebSocket: true,
    webSocketPath: '/custom/mqtt',
    protocol: v5 ? MqttProtocol.v5 : MqttProtocol.v311,
    keepAlivePeriod: v5 ? const Duration(seconds: 5) : Duration.zero);
Future<WebSocket> upgrade(HttpRequest req) async {
  expect(req.uri.path, '/custom/mqtt');
  expect(req.headers.value('Sec-WebSocket-Protocol'), 'mqtt');
  return WebSocketTransformer.upgrade(req, protocolSelector: (_) => 'mqtt');
}

void main() {
  for (final v5 in [false, true]) {
    test('WS v5=$v5 binary exchange custom path', () async {
      final http = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final server = () async {
        final ws = await upgrade(await http.first);
        final r = Reader(ws);
        final p = await r.packet();
        expect(p[7], v5 ? 5 : 4);
        expect(p.sublist(9, 11), v5 ? [0, 5] : [0, 0]);
        connack(ws, v5);
        suback(ws, await r.packet(), v5);
        for (final q in [0, 1]) {
          final p = await r.packet();
          expect((p[0] >> 1) & 3, q);
          if (q == 1) {
            final n = (p[1] << 8) | p[2];
            send(ws, [0x40, 2, p[n + 3], p[n + 4]]);
          }
        }
        send(
            ws,
            v5
                ? [0x32, 9, 0, 1, 116, 0, 77, 0, 1, 0, 255]
                : [0x32, 8, 0, 1, 116, 0, 77, 1, 0, 255]);
        expect(await r.packet(), [0x40, 0, 77]);
        final u = await r.packet();
        send(ws, v5 ? [0xb0, 4, u[1], u[2], 0, 0] : [0xb0, 2, u[1], u[2]]);
        expect((await r.packet())[0], 0xe0);
        await ws.close();
      }();
      final c =
          MqttClient(server: '127.0.0.1', clientId: 'ws-$v5', port: http.port);
      final msg = c.messages.first;
      try {
        await c.connect(options: options(v5));
        await c.subscribe('t');
        for (final q in [MqttQos.atMostOnce, MqttQos.atLeastOnce]) {
          final t =
              c.publishMessage('t', Uint8List.fromList([1, 0, 255]), qos: q);
          await t.whenSent;
          if (q == MqttQos.atLeastOnce) {
            await t.whenAcknowledged;
          }
        }
        expect((await msg).payload, [1, 0, 255]);
        await c.unsubscribe('t');
        await c.disconnect();
        await server;
      } finally {
        await c.close();
        await http.close(force: true);
        await MqttClient.shutdownRuntime();
      }
    });
  }
  for (final mode in ['http', 'path', 'missing', 'wrong']) {
    test('WS rejects $mode before MQTT and never first-fail reconnects',
        () async {
      final http = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      var requests = 0, packets = 0;
      final done = Completer<void>();
      final listener = http.listen((req) async {
        requests++;
        try {
          if (mode == 'http' || mode == 'path') {
            req.response.statusCode = mode == 'path' ? 404 : 403;
            await req.response.close();
          } else {
            if (mode == 'wrong') {
              req.response.headers.set('Sec-WebSocket-Protocol', 'not-mqtt');
            }
            final ws = await WebSocketTransformer.upgrade(req);
            await for (final _ in ws) {
              packets++;
            }
          }
          if (!done.isCompleted) {
            done.complete();
          }
        } catch (e, st) {
          if (!done.isCompleted) {
            done.completeError(e, st);
          }
        }
      });
      final c = MqttClient(
          server: '127.0.0.1', clientId: 'ws-reject', port: http.port);
      final events = <MqttClientEvent>[];
      final sub = c.events.listen(events.add);
      try {
        await expectLater(
            c.connect(
                options: const MqttConnectOptions(
                    useWebSocket: true,
                    webSocketPath: '/custom/mqtt',
                    reconnectPolicy: MqttReconnectPolicy(
                        enabled: true,
                        maxAttempts: 2,
                        baseDelay: Duration(milliseconds: 20),
                        maxDelay: Duration(milliseconds: 20)))),
            throwsA(isA<MqttException>().having((e) => e.code, 'code', 7)));
        await done.future.timeout(const Duration(seconds: 3));
        await Future<void>.delayed(const Duration(milliseconds: 100));
        expect(requests, 1);
        expect(packets, 0);
        expect(events.where((e) => e.kind == MqttClientEventKind.reconnecting),
            isEmpty);
      } finally {
        await c.close();
        await sub.cancel();
        await listener.cancel();
        await http.close(force: true);
        await MqttClient.shutdownRuntime();
      }
    });
  }
  for (final force in [false, true]) {
    test('WS cancellation late SUBACK force=$force', () async {
      final http = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final seen = Completer<void>(), release = Completer<void>();
      final server = () async {
        final ws = await upgrade(await http.first);
        final r = Reader(ws);
        await r.packet();
        connack(ws, false);
        final p = await r.packet();
        seen.complete();
        await release.future;
        if (!force) {
          suback(ws, p, false);
          expect((await r.packet())[0], 0xe0);
        }
        await ws.close();
      }();
      final c = MqttClient(
          server: '127.0.0.1', clientId: 'ws-cancel', port: http.port);
      try {
        await c.connect(options: options(false));
        final op = c.beginSubscribe('t');
        final failed =
            expectLater(op.future, throwsA(isA<MqttOperationCancelled>()));
        await seen.future;
        await op.cancel();
        await failed;
        if (force) {
          await c.close();
          release.complete();
        } else {
          final result = c.drainClose(timeout: const Duration(seconds: 1));
          release.complete();
          expect(await result, MqttDrainResult.drained);
        }
        await server;
      } finally {
        await c.close();
        await http.close(force: true);
        await MqttClient.shutdownRuntime();
      }
    });
  }
  test('WS MQTT5 negative PUBACK terminal Refused', () async {
    final http = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    final release = Completer<void>();
    final server = () async {
      final ws = await upgrade(await http.first);
      final r = Reader(ws);
      await r.packet();
      connack(ws, true);
      final p = await r.packet();
      final n = (p[1] << 8) | p[2];
      send(ws, [0x40, 4, p[n + 3], p[n + 4], 0x87, 0]);
      await release.future;
      await ws.close();
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'ws-negative', port: http.port);
    try {
      await c.connect(options: options(true));
      final t = c.publishMessage('t', Uint8List(1), qos: MqttQos.atLeastOnce);
      final failed = expectLater(t.whenAcknowledged!,
          throwsA(isA<MqttException>().having((e) => e.code, 'code', 8)));
      await t.whenSent;
      await failed;
    } finally {
      release.complete();
      await server;
      await c.close();
      await http.close(force: true);
      await MqttClient.shutdownRuntime();
    }
  });
  test('WS idle MQTT5 reconnect preserves disconnect event', () async {
    final http = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    final drop = Completer<void>(), stop = Completer<void>();
    var requests = 0;
    final tasks = <Future<void>>[];
    final listener = http.listen((req) {
      requests++;
      final number = requests;
      tasks.add(() async {
        final ws = await WebSocketTransformer.upgrade(req,
            protocolSelector: (_) => 'mqtt');
        final r = Reader(ws);
        await r.packet();
        connack(ws, true);
        if (number == 1) {
          await drop.future;
        } else {
          await stop.future;
        }
        await ws.close();
      }());
    });
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'ws-reconnect', port: http.port);
    final events = <MqttClientEvent>[];
    final sub = c.events.listen(events.add);
    try {
      await c.connect(
          options: const MqttConnectOptions(
              useWebSocket: true,
              protocol: MqttProtocol.v5,
              keepAlivePeriod: Duration(seconds: 5),
              reconnectPolicy: MqttReconnectPolicy(
                  enabled: true,
                  maxAttempts: 2,
                  baseDelay: Duration(milliseconds: 20),
                  maxDelay: Duration(milliseconds: 20))));
      drop.complete();
      final end = DateTime.now().add(const Duration(seconds: 3));
      while (!events.any((e) => e.kind == MqttClientEventKind.reconnected)) {
        expect(DateTime.now().isBefore(end), true);
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }
      expect(requests, 2);
      expect(
          events.any((e) => e.state == MqttConnectionState.disconnected), true);
    } finally {
      stop.complete();
      await c.close();
      await Future.wait(tasks);
      await sub.cancel();
      await listener.cancel();
      await http.close(force: true);
      await MqttClient.shutdownRuntime();
    }
  });
  for (final action in ['drain', 'force', 'disconnect']) {
    test('WS $action during backoff prevents next connection', () async {
      final http = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final drop = Completer<void>();
      var requests = 0;
      final server = Completer<void>();
      final listener = http.listen((req) async {
        requests++;
        final ws = await WebSocketTransformer.upgrade(req,
            protocolSelector: (_) => 'mqtt');
        final r = Reader(ws);
        await r.packet();
        connack(ws, false);
        await drop.future;
        await ws.close();
        if (!server.isCompleted) {
          server.complete();
        }
      });
      final c =
          MqttClient(server: '127.0.0.1', clientId: 'ws-stop', port: http.port);
      final events = <MqttClientEvent>[];
      final sub = c.events.listen(events.add);
      try {
        await c.connect(
            options: const MqttConnectOptions(
                useWebSocket: true,
                reconnectPolicy: MqttReconnectPolicy(
                    enabled: true,
                    maxAttempts: 2,
                    baseDelay: Duration(milliseconds: 500),
                    maxDelay: Duration(milliseconds: 500))));
        drop.complete();
        final end = DateTime.now().add(const Duration(seconds: 3));
        while (!events.any((e) => e.kind == MqttClientEventKind.reconnecting)) {
          expect(DateTime.now().isBefore(end), true);
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
        if (action == 'force') {
          await c.close();
        } else if (action == 'drain') {
          expect(await c.drainClose(timeout: const Duration(seconds: 1)),
              MqttDrainResult.drained);
        } else {
          await c.disconnect();
        }
        await Future<void>.delayed(const Duration(milliseconds: 600));
        expect(requests, 1);
        await server.future;
      } finally {
        await c.close();
        await sub.cancel();
        await listener.cancel();
        await http.close(force: true);
        await MqttClient.shutdownRuntime();
      }
    });
  }
}
