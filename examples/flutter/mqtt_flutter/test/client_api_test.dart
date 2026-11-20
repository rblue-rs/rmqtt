import 'dart:convert';
import 'dart:io';
import 'dart:async';
import 'dart:isolate';
import 'dart:typed_data';
import 'package:mqtt_flutter/mqtt_flutter.dart';
import 'package:test/test.dart';

void main() {
  test('default keepalive remains zero', () {
    expect(const MqttConnectOptions().keepAlivePeriod, Duration.zero);
    expect(const MqttConnectOptions().useTls, isFalse);
    expect(const MqttConnectOptions().caPem, isNull);
    expect(const MqttConnectOptions().clientCertPem, isNull);
    expect(const MqttConnectOptions().clientKeyPem, isNull);
  });
  test('Dart -> C ABI -> Rust -> broker and reusable disconnect', () async {
    final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'dart-$pid',
        port: int.parse(Platform.environment['MQTT_TEST_PORT'] ?? '18883'));
    final received = <MqttReceivedMessage>[];
    final sub = c.messages.listen(received.add);
    try {
      final status = await c.connect(timeout: const Duration(seconds: 5));
      expect(status.sessionPresent, isFalse);
      final ack = await c.subscribe('slice/dart');
      expect(ack.qos, MqttQos.atLeastOnce);
      final t = c.publishMessage(
          'slice/dart', Uint8List.fromList(utf8.encode('hello ffi')),
          qos: MqttQos.atLeastOnce);
      await t.whenQueued;
      await t.whenSent;
      await t.whenAcknowledged;
      final deadline = DateTime.now().add(const Duration(seconds: 5));
      while (received.isEmpty && DateTime.now().isBefore(deadline)) {
        await Future<void>.delayed(const Duration(milliseconds: 10));
      }
      expect(received.single.topic, 'slice/dart');
      expect(utf8.decode(received.single.payload), 'hello ffi');
      final q0 =
          c.publishMessage('slice/dart', Uint8List.fromList([0, 255, 0]));
      expect(q0.whenAcknowledged, isNull);
      await q0.whenSent;
      await c.unsubscribe('slice/dart');
      await c.disconnect();
      await c.connect();
      await c.close();
      expect(() => c.publishMessage('x', Uint8List(0)),
          throwsA(isA<StateError>()));
    } finally {
      await c.close();
      await sub.cancel();
      await MqttClient.shutdownRuntime();
    }
  });
  test('unsupported V5 zero is typed, and V311 remains usable', () async {
    final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'dart-v5',
        port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
    try {
      await expectLater(
          c.connect(
              options: const MqttConnectOptions(protocol: MqttProtocol.v5)),
          throwsA(isA<MqttException>().having((e) => e.code, 'code', 6)));
      await c.connect();
    } finally {
      await c.close();
      await MqttClient.shutdownRuntime();
    }
  });
  test('reject fractional keepalive rather than truncate', () async {
    final c = MqttClient(server: '127.0.0.1', clientId: 'fractional');
    await expectLater(
        c.connect(
            options: const MqttConnectOptions(
                keepAlivePeriod: Duration(milliseconds: 500))),
        throwsArgumentError);
    await c.close();
  });
  test('close from a message listener stops delivery without deadlock',
      () async {
    final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'dart-close',
        port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
    final done = Completer<void>();
    var count = 0;
    final listener = c.messages.listen((_) {
      count++;
      c.close().then((_) => done.complete());
    });
    try {
      await c.connect();
      await c.subscribe('slice/close');
      final t = c.publishMessage('slice/close', Uint8List.fromList([1]));
      await t.whenSent;
      await done.future.timeout(const Duration(seconds: 5));
      expect(c.connectionState, MqttConnectionState.closed);
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(count, 1);
    } finally {
      await c.close();
      await listener.cancel();
      await MqttClient.shutdownRuntime();
    }
  });
  test('shared native runtime across three Dart isolates', () async {
    final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'dart-main',
        port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
    try {
      await c.connect();
      await Future.wait(
          List.generate(3, (i) => Isolate.run(() => isolateClient(i))));
      final t = c.publishMessage('slice/isolate', Uint8List.fromList([1]),
          qos: MqttQos.atLeastOnce);
      await t.whenAcknowledged;
    } finally {
      await c.close();
      await MqttClient.shutdownRuntime();
    }
  });
}

Future<void> isolateClient(int index) async {
  final c = MqttClient(
      server: '127.0.0.1',
      clientId: 'isolate-$index',
      port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
  try {
    await c.connect();
    final t = c.publishMessage('slice/isolate', Uint8List.fromList([index]),
        qos: MqttQos.atLeastOnce);
    await t.whenAcknowledged;
  } finally {
    await c.close();
    await MqttClient.shutdownRuntime();
  }
}
