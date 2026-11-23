import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';

String repoRoot() {
  var dir = Directory.current;
  for (var i = 0; i < 8; i++) {
    if (File('${dir.path}/scripts/with_broker.py').existsSync() &&
        File('${dir.path}/scripts/gen_test_certs.py').existsSync()) {
      return dir.path;
    }
    dir = dir.parent;
  }
  throw StateError('cannot locate repo root from ${Directory.current.path}');
}

String pythonBin() => Platform.environment['MQTT_TEST_PYTHON'] ?? 'python3';

class ServedBroker {
  ServedBroker(this.process, this.tlsPort, this.plainPort);
  final Process process;
  final int tlsPort;
  final int plainPort;

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

Future<ServedBroker> startTlsBroker(
    String root, String certPath, String keyPath) async {
  final ready = File(
      '${Directory.systemTemp.path}/mqtt-tls-ready-$pid-${DateTime.now().microsecondsSinceEpoch}');
  if (ready.existsSync()) {
    ready.deleteSync();
  }
  final process = await Process.start(
    pythonBin(),
    ['$root/scripts/with_broker.py', '--serve', ready.path],
    environment: {
      ...Platform.environment,
      'MQTT_TEST_TLS_CERT': certPath,
      'MQTT_TEST_TLS_KEY': keyPath,
    },
    workingDirectory: root,
  );
  final errors = StringBuffer();
  process.stderr.transform(utf8.decoder).listen(errors.write);
  var exited = false;
  process.exitCode.then((_) => exited = true);
  final deadline = DateTime.now().add(const Duration(seconds: 15));
  while (!ready.existsSync()) {
    if (exited) {
      throw StateError('TLS broker exited early: $errors');
    }
    if (DateTime.now().isAfter(deadline)) {
      process.kill(ProcessSignal.sigkill);
      throw StateError('TLS broker ready timeout: $errors');
    }
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
  var tlsPort = 0;
  var plainPort = 0;
  for (final line in ready.readAsLinesSync()) {
    final parts = line.split('=');
    if (parts.length != 2) continue;
    final value = int.parse(parts[1]);
    if (parts[0] == 'port' || parts[0] == 'tls_port') {
      tlsPort = value;
    } else if (parts[0] == 'plain_port') {
      plainPort = value;
    }
  }
  if (tlsPort == 0 || plainPort == 0) {
    process.kill(ProcessSignal.sigkill);
    throw StateError(
        'broker ready file missing ports: ${ready.readAsStringSync()}');
  }
  return ServedBroker(process, tlsPort, plainPort);
}

MqttClient client(String id, int port) =>
    MqttClient(server: '127.0.0.1', clientId: id, port: port);

MqttConnectOptions tlsOptions([String? caPem]) =>
    MqttConnectOptions(useTls: true, caPem: caPem);

void assertNoSecrets(String text, List<String> secrets) {
  for (final secret in secrets) {
    expect(text.contains(secret), isFalse, reason: 'secret material leaked');
  }
  expect(text.contains('BEGIN CERTIFICATE'), isFalse);
  expect(text.contains('BEGIN PRIVATE KEY'), isFalse);
  expect(text.contains('BEGIN RSA PRIVATE KEY'), isFalse);
  expect(text.contains('-----BEGIN'), isFalse);
}

void main() {
  late String root;
  late Directory certDir;
  late String caPem;
  late String otherCaPem;
  late ServedBroker trusted;
  final secrets = <String>[];

  setUpAll(() async {
    root = repoRoot();
    certDir = Directory.systemTemp.createTempSync('mqtt-tls-dart-');
    final generated = await Process.run(
        pythonBin(), ['$root/scripts/gen_test_certs.py', certDir.path]);
    expect(generated.exitCode, 0, reason: generated.stderr.toString());
    expect(generated.stdout.toString().contains('BEGIN '), isFalse);
    caPem = File('${certDir.path}/ca.pem').readAsStringSync();
    otherCaPem = File('${certDir.path}/other-ca.pem').readAsStringSync();
    for (final name in [
      'ca.pem',
      'ca.key',
      'other-ca.pem',
      'other-ca.key',
      'server-127.pem',
      'server-127.key',
      'server-expired.pem',
      'server-expired.key',
      'server-wrong-host.pem',
      'server-wrong-host.key',
    ]) {
      secrets.add(File('${certDir.path}/$name').readAsStringSync());
    }
    trusted = await startTlsBroker(root, '${certDir.path}/server-127.pem',
        '${certDir.path}/server-127.key');
  });

  tearDownAll(() async {
    await trusted.stop();
    if (certDir.existsSync()) {
      certDir.deleteSync(recursive: true);
    }
    await MqttClient.shutdownRuntime();
  });

  Future<void> expectTlsFailure({
    required String id,
    required int port,
    required String? ca,
    required int code,
  }) async {
    final c = client(id, port);
    var settled = 0;
    Object? error;
    try {
      final op = c.beginConnect(
          options: tlsOptions(ca), timeout: const Duration(seconds: 5));
      await op.future.then((_) {
        settled++;
      }, onError: (Object e, StackTrace _) {
        error = e;
        settled++;
      });
      expect(settled, 1);
      expect(error, isA<MqttException>().having((e) => e.code, 'code', code));
      assertNoSecrets('$error ${error.toString()} ${tlsOptions(ca)}', secrets);
      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(settled, 1);
    } finally {
      await c.close();
    }
  }

  test('options redact CA PEM and default to plaintext', () {
    const defaults = MqttConnectOptions();
    expect(defaults.useTls, isFalse);
    expect(defaults.caPem, isNull);
    expect(defaults.keepAlivePeriod, Duration.zero);
    final options = MqttConnectOptions(useTls: true, caPem: caPem);
    assertNoSecrets('$options', secrets);
  });

  test('trusted CA and matching hostname connect and exchange QoS 1', () async {
    final c = client('dart-tls-ok', trusted.tlsPort);
    final received = <MqttReceivedMessage>[];
    final sub = c.messages.listen(received.add);
    try {
      final status = await c.connect(
          options: tlsOptions(caPem), timeout: const Duration(seconds: 5));
      expect(status.sessionPresent, isFalse);
      final ack = await c.subscribe('test/dart-tls');
      expect(ack.qos, MqttQos.atLeastOnce);
      final ticket = c.publishMessage(
          'test/dart-tls', Uint8List.fromList(utf8.encode('hello-tls')),
          qos: MqttQos.atLeastOnce);
      await ticket.whenQueued;
      await ticket.whenSent;
      await ticket.whenAcknowledged;
      final deadline = DateTime.now().add(const Duration(seconds: 5));
      while (received.isEmpty && DateTime.now().isBefore(deadline)) {
        await Future<void>.delayed(const Duration(milliseconds: 10));
      }
      expect(received.single.topic, 'test/dart-tls');
      expect(utf8.decode(received.single.payload), 'hello-tls');
    } finally {
      await c.close();
      await sub.cancel();
    }
  });

  test('untrusted CA fails once without retry', () async {
    await expectTlsFailure(
        id: 'dart-tls-untrusted',
        port: trusted.tlsPort,
        ca: otherCaPem,
        code: 15);
  });

  test('system roots do not trust the fixture CA', () async {
    await expectTlsFailure(
        id: 'dart-tls-system', port: trusted.tlsPort, ca: null, code: 15);
  });

  test('TLS to a plaintext port fails once', () async {
    // A silent MQTT broker can wait for CONNECT instead of failing TLS.
    // Close accepted TCP connections immediately from another process.
    final closer = await Process.start(pythonBin(), [
      '-c',
      'import socket,sys\n'
          's=socket.socket(); s.bind(("127.0.0.1",0)); s.listen(8)\n'
          'sys.stdout.write(str(s.getsockname()[1])+"\\n"); sys.stdout.flush()\n'
          'while True:\n'
          ' c,_=s.accept(); c.close()\n'
    ]);
    final portLine = await closer.stdout
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .first
        .timeout(const Duration(seconds: 5));
    try {
      await expectTlsFailure(
          id: 'dart-tls-plain', port: int.parse(portLine), ca: caPem, code: 14);
    } finally {
      closer.kill(ProcessSignal.sigterm);
      await closer.exitCode.timeout(const Duration(seconds: 2), onTimeout: () {
        closer.kill(ProcessSignal.sigkill);
        return -1;
      });
    }
  });

  test('hostname mismatch fails once', () async {
    final broker = await startTlsBroker(
        root,
        '${certDir.path}/server-wrong-host.pem',
        '${certDir.path}/server-wrong-host.key');
    try {
      await expectTlsFailure(
          id: 'dart-tls-hostname', port: broker.tlsPort, ca: caPem, code: 16);
    } finally {
      await broker.stop();
    }
  });

  test('expired certificate fails once', () async {
    final broker = await startTlsBroker(
        root,
        '${certDir.path}/server-expired.pem',
        '${certDir.path}/server-expired.key');
    try {
      await expectTlsFailure(
          id: 'dart-tls-expired', port: broker.tlsPort, ca: caPem, code: 17);
    } finally {
      await broker.stop();
    }
  });

  test('empty custom CA is rejected rather than falling back', () async {
    final c = client('dart-tls-empty', trusted.tlsPort);
    try {
      await expectLater(
          c.connect(options: const MqttConnectOptions(useTls: true, caPem: '')),
          throwsA(isA<MqttException>().having((e) => e.code, 'code', 1)));
    } finally {
      await c.close();
    }
  });
}
