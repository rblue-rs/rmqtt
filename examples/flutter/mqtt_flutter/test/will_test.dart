import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';
import 'package:mqtt_flutter/mqtt_flutter.dart';
import 'package:test/test.dart';

String repoRoot() {
  var dir = Directory.current;
  for (var i = 0; i < 8; i++) {
    if (File('${dir.path}/scripts/with_broker.py').existsSync()) {
      return dir.path;
    }
    dir = dir.parent;
  }
  throw StateError('cannot locate repo root from ${Directory.current.path}');
}

String pythonBin() => Platform.environment['MQTT_TEST_PYTHON'] ?? 'python3';

class ServedBroker {
  ServedBroker(this.process, this.port);
  final Process process;
  final int port;

  Future<void> stop() async {
    process.kill(ProcessSignal.sigterm);
    try {
      await process.exitCode.timeout(const Duration(seconds: 5));
    } on TimeoutException {
      process.kill(ProcessSignal.sigkill);
      await process.exitCode;
    }
  }
}

Future<ServedBroker> startBroker(String root) async {
  final ready = File(
      '${Directory.systemTemp.path}/mqtt-will-$pid-${DateTime.now().microsecondsSinceEpoch}');
  if (ready.existsSync()) {
    ready.deleteSync();
  }
  final process = await Process.start(
    pythonBin(),
    ['$root/scripts/with_broker.py', '--serve', ready.path],
    environment: Platform.environment,
    workingDirectory: root,
  );
  final errors = StringBuffer();
  process.stderr.transform(utf8.decoder).listen(errors.write);
  var exited = false;
  process.exitCode.then((_) => exited = true);
  final deadline = DateTime.now().add(const Duration(seconds: 15));
  while (!ready.existsSync()) {
    if (exited) {
      throw StateError('broker exited early');
    }
    if (DateTime.now().isAfter(deadline)) {
      process.kill(ProcessSignal.sigkill);
      throw StateError('broker ready timeout');
    }
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
  var port = 0;
  for (final line in ready.readAsLinesSync()) {
    final parts = line.split('=');
    if (parts.length == 2 && (parts[0] == 'port' || parts[0] == 'plain_port')) {
      port = int.parse(parts[1]);
    }
  }
  if (port == 0) {
    process.kill(ProcessSignal.sigkill);
    throw StateError('broker ready file missing port');
  }
  return ServedBroker(process, port);
}

MqttClient client(String id, int port) =>
    MqttClient(server: '127.0.0.1', clientId: id, port: port);

Future<MqttReceivedMessage> waitMessage(
    List<MqttReceivedMessage> received) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (received.isEmpty && DateTime.now().isBefore(deadline)) {
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  expect(received, isNotEmpty);
  return received.first;
}

void main() {
  late String root;
  late ServedBroker broker;

  setUpAll(() async {
    root = repoRoot();
    broker = await startBroker(root);
  });

  tearDownAll(() async {
    await broker.stop();
    await MqttClient.shutdownRuntime();
  });

  test('will QoS 0 is delivered after an abnormal disconnect', () async {
    final subscriber = client('dart-will-sub-0', broker.port);
    final received = <MqttReceivedMessage>[];
    final sub = subscriber.messages.listen(received.add);
    final owner = client('dart-will-owner-0', broker.port);
    try {
      await subscriber.connect(timeout: const Duration(seconds: 5));
      await subscriber.subscribe('test/dart/will0');
      await owner.connect(
          options: MqttConnectOptions(
              will: MqttWill(
                  topic: 'test/dart/will0',
                  payload: Uint8List.fromList(utf8.encode('gone-0')))),
          timeout: const Duration(seconds: 5));
      await owner.close();
      final message = await waitMessage(received);
      expect(message.topic, 'test/dart/will0');
      expect(utf8.decode(message.payload), 'gone-0');
      expect(message.qos, MqttQos.atMostOnce);
    } finally {
      await owner.close();
      await subscriber.close();
      await sub.cancel();
    }
  });

  test('will QoS 1 is delivered after an abnormal disconnect', () async {
    final subscriber = client('dart-will-sub-1', broker.port);
    final received = <MqttReceivedMessage>[];
    final sub = subscriber.messages.listen(received.add);
    final owner = client('dart-will-owner-1', broker.port);
    try {
      await subscriber.connect(timeout: const Duration(seconds: 5));
      await subscriber.subscribe('test/dart/will1');
      await owner.connect(
          options: MqttConnectOptions(
              will: MqttWill(
                  topic: 'test/dart/will1',
                  payload: Uint8List.fromList(utf8.encode('gone-1')),
                  qos: MqttQos.atLeastOnce)),
          timeout: const Duration(seconds: 5));
      await owner.close();
      final message = await waitMessage(received);
      expect(message.topic, 'test/dart/will1');
      expect(utf8.decode(message.payload), 'gone-1');
      expect(message.qos, MqttQos.atLeastOnce);
    } finally {
      await owner.close();
      await subscriber.close();
      await sub.cancel();
    }
  });

  test('retained will is delivered to a later subscriber', () async {
    final owner = client('dart-will-owner-retain', broker.port);
    try {
      await owner.connect(
          options: MqttConnectOptions(
              will: MqttWill(
                  topic: 'test/dart/will-retain',
                  payload: Uint8List.fromList(utf8.encode('still-here')),
                  qos: MqttQos.atLeastOnce,
                  retain: true)),
          timeout: const Duration(seconds: 5));
      await owner.close();
      await Future<void>.delayed(const Duration(milliseconds: 100));
      final subscriber = client('dart-will-sub-retain', broker.port);
      final received = <MqttReceivedMessage>[];
      final sub = subscriber.messages.listen(received.add);
      try {
        await subscriber.connect(timeout: const Duration(seconds: 5));
        await subscriber.subscribe('test/dart/will-retain');
        final message = await waitMessage(received);
        expect(message.topic, 'test/dart/will-retain');
        expect(utf8.decode(message.payload), 'still-here');
      } finally {
        await subscriber.close();
        await sub.cancel();
      }
    } finally {
      await owner.close();
    }
  });

  test('empty will topic is rejected', () async {
    final c = client('dart-will-empty', broker.port);
    try {
      await expectLater(
          c.connect(
              options: MqttConnectOptions(
                  will: MqttWill(topic: '', payload: Uint8List.fromList([1])))),
          throwsA(isA<MqttException>().having((e) => e.code, 'code', 1)));
    } finally {
      await c.close();
    }
  });
}
