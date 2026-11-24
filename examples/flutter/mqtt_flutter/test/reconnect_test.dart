import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';

class _ByteSource {
  final List<int> _buf = [];
  final StreamIterator<Uint8List> _iter;
  _ByteSource(Socket socket) : _iter = StreamIterator(socket);

  Future<int> byte() async {
    while (_buf.isEmpty) {
      if (!await _iter.moveNext().timeout(const Duration(seconds: 3))) {
        throw StateError('socket closed');
      }
      _buf.addAll(_iter.current);
    }
    return _buf.removeAt(0);
  }

  Future<List<int>> packet() async {
    final h = await byte();
    var len = 0;
    var shift = 0;
    while (true) {
      final b = await byte();
      len += (b & 127) << shift;
      if (b & 128 == 0) {
        break;
      }
      shift += 7;
    }
    final out = <int>[h];
    for (var i = 0; i < len; i++) {
      out.add(await byte());
    }
    return out;
  }
}

final _readers = <Socket, _ByteSource>{};

Future<List<int>> readMqtt(Socket socket) {
  return _readers.putIfAbsent(socket, () => _ByteSource(socket)).packet();
}

bool cleanSessionFlag(List<int> connect) => (connect[8] & 0x02) != 0;

List<(String, int)> subscribeFilters(List<int> packet) {
  var i = 3;
  final out = <(String, int)>[];
  while (i + 2 <= packet.length) {
    final len = (packet[i] << 8) | packet[i + 1];
    i += 2;
    final topic = String.fromCharCodes(packet.sublist(i, i + len));
    i += len;
    out.add((topic, packet[i]));
    i += 1;
  }
  return out;
}

void writeConnack(Socket socket, {bool sessionPresent = false, int code = 0}) {
  socket.add([0x20, 2, sessionPresent ? 1 : 0, code]);
}

void writeSuback(Socket socket, List<int> subscribe) {
  final idHi = subscribe[1], idLo = subscribe[2];
  final filters = subscribeFilters(subscribe);
  socket
      .add([0x90, 2 + filters.length, idHi, idLo, ...filters.map((f) => f.$2)]);
}

void main() {
  test('defaults keep reconnect off and clean session true', () {
    const options = MqttConnectOptions();
    expect(options.cleanSession, isTrue);
    expect(options.reconnectPolicy.enabled, isFalse);
    expect(options.reconnectPolicy.maxAttempts, 8);
    expect(options.keepAlivePeriod, Duration.zero);
    expect(options.toString(), contains('password: null'));
    expect(options.toString(), isNot(contains('SECRET')));
    expect(options.toString(), contains('cleanSession'));
    expect(options.toString(), contains('reconnectPolicy'));
  });

  test('first connect refusal with policy enabled does not emit reconnecting',
      () async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    var extraAccepts = 0;
    final incoming = listener.asBroadcastStream();
    incoming.skip(1).listen((socket) {
      extraAccepts++;
      socket.destroy();
    });
    final server = () async {
      final socket = await incoming.first.timeout(const Duration(seconds: 3));
      await readMqtt(socket);
      writeConnack(socket, code: 3);
      await socket.close();
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'dart-first-fail', port: listener.port);
    final events = <MqttClientEvent>[];
    final sub = c.events.listen(events.add);
    try {
      await expectLater(
          c.connect(
              options: const MqttConnectOptions(
                  reconnectPolicy: MqttReconnectPolicy(
                      enabled: true,
                      maxAttempts: 5,
                      baseDelay: Duration(milliseconds: 20),
                      maxDelay: Duration(milliseconds: 20))),
              timeout: const Duration(seconds: 5)),
          throwsA(isA<MqttException>().having((e) => e.code, 'code', 8)));
      await Future<void>.delayed(const Duration(milliseconds: 250));
      expect(
          events.where((e) =>
              e.kind == MqttClientEventKind.reconnecting ||
              e.kind == MqttClientEventKind.reconnected),
          isEmpty);
      expect(extraAccepts, 0);
    } finally {
      await c.close();
      await sub.cancel();
      await listener.close();
      await server;
      await MqttClient.shutdownRuntime();
    }
  });

  test('drop after success emits disconnected, reconnecting, reconnected',
      () async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final incoming = listener.asBroadcastStream();
    final server = () async {
      final first = await incoming.first.timeout(const Duration(seconds: 3));
      await readMqtt(first);
      writeConnack(first);
      await first.close();
      final second = await incoming.first.timeout(const Duration(seconds: 3));
      await readMqtt(second);
      writeConnack(second);
      await Future<void>.delayed(const Duration(milliseconds: 80));
      await second.close();
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'dart-re', port: listener.port);
    final events = <MqttClientEvent>[];
    final sub = c.events.listen(events.add);
    try {
      await c.connect(
          options: const MqttConnectOptions(
              reconnectPolicy: MqttReconnectPolicy(
                  enabled: true,
                  maxAttempts: 3,
                  baseDelay: Duration(milliseconds: 20),
                  maxDelay: Duration(milliseconds: 20))),
          timeout: const Duration(seconds: 5));
      MqttClientEvent? reconnected;
      final deadline = DateTime.now().add(const Duration(seconds: 5));
      while (reconnected == null && DateTime.now().isBefore(deadline)) {
        await Future<void>.delayed(const Duration(milliseconds: 10));
        for (final e in events) {
          if (e.kind == MqttClientEventKind.reconnected) {
            reconnected = e;
          }
        }
      }
      expect(
          events.map((e) => e.kind),
          containsAllInOrder([
            MqttClientEventKind.state,
            MqttClientEventKind.state,
            MqttClientEventKind.state,
            MqttClientEventKind.reconnecting,
            MqttClientEventKind.reconnected,
          ]));
      expect(reconnected, isNotNull);
      expect(c.connectionState, MqttConnectionState.connected);
      expect(c.connectionStatus?.sessionPresent, isFalse);
      expect(reconnected!.status?.sessionPresent, isFalse);
    } finally {
      await c.close();
      await sub.cancel();
      await listener.close();
      await server;
      await MqttClient.shutdownRuntime();
    }
  });

  test('session_present=0 resubscribes the expectation set', () async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final incoming = listener.asBroadcastStream();
    final restored = Completer<List<(String, int)>>();
    final server = () async {
      final first = await incoming.first.timeout(const Duration(seconds: 3));
      await readMqtt(first);
      writeConnack(first);
      final sub = await readMqtt(first);
      expect((sub[0] & 0xf0), 0x80);
      writeSuback(first, sub);
      await Future<void>.delayed(const Duration(milliseconds: 50));
      await first.close();
      final second = await incoming.first.timeout(const Duration(seconds: 3));
      await readMqtt(second);
      writeConnack(second);
      final packet = await readMqtt(second).timeout(const Duration(seconds: 2));
      expect((packet[0] & 0xf0), 0x80);
      writeSuback(second, packet);
      restored.complete(subscribeFilters(packet));
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'dart-set', port: listener.port);
    final events = <MqttClientEvent>[];
    final sub = c.events.listen(events.add);
    try {
      await c.connect(
          options: const MqttConnectOptions(
              cleanSession: true,
              reconnectPolicy: MqttReconnectPolicy(
                  enabled: true,
                  maxAttempts: 3,
                  baseDelay: Duration(milliseconds: 20),
                  maxDelay: Duration(milliseconds: 20))),
          timeout: const Duration(seconds: 5));
      await c.subscribe('keep/a');
      final deadline = DateTime.now().add(const Duration(seconds: 5));
      while (DateTime.now().isBefore(deadline) &&
          !events.any(
              (e) => e.kind == MqttClientEventKind.subscriptionsResubscribed)) {
        await Future<void>.delayed(const Duration(milliseconds: 10));
      }
      expect(
          events.any(
              (e) => e.kind == MqttClientEventKind.subscriptionsResubscribed),
          isTrue);
      expect(await restored.future.timeout(const Duration(seconds: 3)),
          [('keep/a', 1)]);
    } finally {
      await c.close();
      await sub.cancel();
      await listener.close();
      await server;
      await MqttClient.shutdownRuntime();
    }
  });

  test('cleanSession false is visible and session_present=1 aligns', () async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final incoming = listener.asBroadcastStream();
    final extras = Completer<List<(String, int)>>();
    final server = () async {
      final first = await incoming.first.timeout(const Duration(seconds: 3));
      final connect = await readMqtt(first);
      expect(cleanSessionFlag(connect), isFalse);
      writeConnack(first);
      final sub = await readMqtt(first);
      writeSuback(first, sub);
      await Future<void>.delayed(const Duration(milliseconds: 50));
      await first.close();
      final second = await incoming.first.timeout(const Duration(seconds: 3));
      await readMqtt(second);
      writeConnack(second, sessionPresent: true);
      final got = <(String, int)>[];
      try {
        final packet =
            await readMqtt(second).timeout(const Duration(milliseconds: 250));
        if ((packet[0] & 0xf0) == 0x80) {
          got.addAll(subscribeFilters(packet));
          writeSuback(second, packet);
        }
      } on TimeoutException {
        // No SUBSCRIBE arrived in the observation window.
      }
      extras.complete(got);
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'dart-persist', port: listener.port);
    final events = <MqttClientEvent>[];
    final subEv = c.events.listen(events.add);
    try {
      await c.connect(
          options: const MqttConnectOptions(
              cleanSession: false,
              reconnectPolicy: MqttReconnectPolicy(
                  enabled: true,
                  maxAttempts: 3,
                  baseDelay: Duration(milliseconds: 20),
                  maxDelay: Duration(milliseconds: 20))),
          timeout: const Duration(seconds: 5));
      await c.subscribe('held/filter');
      final deadline = DateTime.now().add(const Duration(seconds: 5));
      while (DateTime.now().isBefore(deadline) &&
          !events
              .any((e) => e.kind == MqttClientEventKind.subscriptionsAligned)) {
        await Future<void>.delayed(const Duration(milliseconds: 10));
      }
      expect(
          events.any((e) => e.kind == MqttClientEventKind.subscriptionsAligned),
          isTrue);
      expect(c.connectionStatus?.sessionPresent, isTrue);
      expect(await extras.future.timeout(const Duration(seconds: 3)), isEmpty);
    } finally {
      await c.close();
      await subEv.cancel();
      await listener.close();
      await server;
      await MqttClient.shutdownRuntime();
    }
  });

  test('drain during reconnect prevents further attempts', () async {
    final listener = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final incoming = listener.asBroadcastStream();
    var second = false;
    final server = () async {
      final first = await incoming.first.timeout(const Duration(seconds: 3));
      await readMqtt(first);
      writeConnack(first);
      await first.close();
      try {
        await incoming.first.timeout(const Duration(milliseconds: 400));
        second = true;
      } on TimeoutException {
        // Reconnect attempt did not start, as required after drain.
      } on StateError {
        // Listener closed after the test finished; treat as no second accept.
      }
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'dart-drain', port: listener.port);
    final events = <MqttClientEvent>[];
    final sub = c.events.listen(events.add);
    try {
      await c.connect(
          options: const MqttConnectOptions(
              reconnectPolicy: MqttReconnectPolicy(
                  enabled: true,
                  maxAttempts: 5,
                  baseDelay: Duration(milliseconds: 300),
                  maxDelay: Duration(milliseconds: 300))),
          timeout: const Duration(seconds: 5));
      final deadline = DateTime.now().add(const Duration(seconds: 3));
      while (DateTime.now().isBefore(deadline) &&
          !events.any((e) => e.kind == MqttClientEventKind.reconnecting)) {
        await Future<void>.delayed(const Duration(milliseconds: 10));
      }
      expect(events.any((e) => e.kind == MqttClientEventKind.reconnecting),
          isTrue);
      final drain =
          await c.drainClose(timeout: const Duration(milliseconds: 200));
      expect(
          drain,
          anyOf(MqttDrainResult.drained, MqttDrainResult.failed,
              MqttDrainResult.forced));
      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(events.any((e) => e.kind == MqttClientEventKind.reconnected),
          isFalse);
      expect(second, isFalse);
    } finally {
      await c.close();
      await sub.cancel();
      await listener.close();
      await server;
      await MqttClient.shutdownRuntime();
    }
  });
}
