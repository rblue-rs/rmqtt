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
  ServedBroker(this.process, this.tlsPort);
  final Process process;
  final int tlsPort;

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

Future<ServedBroker> startMtlsBroker(String root, String certDir) async {
  final ready = File(
      '${Directory.systemTemp.path}/mqtt-mtls-ready-$pid-${DateTime.now().microsecondsSinceEpoch}');
  if (ready.existsSync()) {
    ready.deleteSync();
  }
  final process = await Process.start(
    pythonBin(),
    ['$root/scripts/with_broker.py', '--serve', ready.path],
    environment: {
      ...Platform.environment,
      'MQTT_TEST_TLS_CERT': '$certDir/server-127.pem',
      'MQTT_TEST_TLS_KEY': '$certDir/server-127.key',
      'MQTT_TEST_TLS_CA': '$certDir/ca.pem',
      'MQTT_TEST_REQUIRE_CLIENT_CERT': '1',
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
      throw StateError('mTLS broker exited early: $errors');
    }
    if (DateTime.now().isAfter(deadline)) {
      process.kill(ProcessSignal.sigkill);
      throw StateError('mTLS broker ready timeout: $errors');
    }
    await Future<void>.delayed(const Duration(milliseconds: 20));
  }
  var tlsPort = 0;
  for (final line in ready.readAsLinesSync()) {
    final parts = line.split('=');
    if (parts.length != 2) continue;
    if (parts[0] == 'port' || parts[0] == 'tls_port') {
      tlsPort = int.parse(parts[1]);
    }
  }
  if (tlsPort == 0) {
    process.kill(ProcessSignal.sigkill);
    throw StateError(
        'broker ready file missing tls port: ${ready.readAsStringSync()}');
  }
  return ServedBroker(process, tlsPort);
}

MqttClient client(String id, int port) =>
    MqttClient(server: '127.0.0.1', clientId: id, port: port);

MqttConnectOptions mtlsOptions({
  required String caPem,
  String? clientCertPem,
  String? clientKeyPem,
}) =>
    MqttConnectOptions(
        useTls: true,
        caPem: caPem,
        clientCertPem: clientCertPem,
        clientKeyPem: clientKeyPem);

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
  late String clientCert;
  late String clientKey;
  late String otherCert;
  late String otherKey;
  late ServedBroker broker;
  final secrets = <String>[];

  setUpAll(() async {
    root = repoRoot();
    certDir = Directory.systemTemp.createTempSync('mqtt-mtls-dart-');
    final generated = await Process.run(
        pythonBin(), ['$root/scripts/gen_test_certs.py', certDir.path]);
    expect(generated.exitCode, 0, reason: generated.stderr.toString());
    expect(generated.stdout.toString().contains('BEGIN '), isFalse);
    caPem = File('${certDir.path}/ca.pem').readAsStringSync();
    clientCert = File('${certDir.path}/client-ok.pem').readAsStringSync();
    clientKey = File('${certDir.path}/client-ok.key').readAsStringSync();
    otherCert = File('${certDir.path}/client-other.pem').readAsStringSync();
    otherKey = File('${certDir.path}/client-other.key').readAsStringSync();
    for (final name in [
      'ca.pem',
      'ca.key',
      'other-ca.pem',
      'other-ca.key',
      'server-127.pem',
      'server-127.key',
      'client-ok.pem',
      'client-ok.key',
      'client-other.pem',
      'client-other.key',
    ]) {
      secrets.add(File('${certDir.path}/$name').readAsStringSync());
    }
    broker = await startMtlsBroker(root, certDir.path);
  });

  tearDownAll(() async {
    await broker.stop();
    if (certDir.existsSync()) {
      certDir.deleteSync(recursive: true);
    }
    await MqttClient.shutdownRuntime();
  });

  Future<void> expectConnectFailure({
    required String id,
    required MqttConnectOptions options,
    required int code,
  }) async {
    final c = client(id, broker.tlsPort);
    var settled = 0;
    Object? error;
    try {
      final op =
          c.beginConnect(options: options, timeout: const Duration(seconds: 5));
      await op.future.then((_) {
        settled++;
      }, onError: (Object e, StackTrace _) {
        error = e;
        settled++;
      });
      expect(settled, 1);
      expect(error, isA<MqttException>().having((e) => e.code, 'code', code));
      assertNoSecrets('$error ${error.toString()} $options', secrets);
      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(settled, 1);
    } finally {
      await c.close();
    }
  }

  test('options redact client certificate and key', () {
    const defaults = MqttConnectOptions();
    expect(defaults.useTls, isFalse);
    expect(defaults.clientCertPem, isNull);
    expect(defaults.clientKeyPem, isNull);
    expect(defaults.keepAlivePeriod, Duration.zero);
    final options = mtlsOptions(
        caPem: caPem, clientCertPem: clientCert, clientKeyPem: clientKey);
    assertNoSecrets('$options', secrets);
  });

  test('correct client certificate connects and exchanges QoS 1', () async {
    final c = client('dart-mtls-ok', broker.tlsPort);
    final received = <MqttReceivedMessage>[];
    final sub = c.messages.listen(received.add);
    try {
      final status = await c.connect(
          options: mtlsOptions(
              caPem: caPem, clientCertPem: clientCert, clientKeyPem: clientKey),
          timeout: const Duration(seconds: 5));
      expect(status.sessionPresent, isFalse);
      final ack = await c.subscribe('test/dart-mtls');
      expect(ack.qos, MqttQos.atLeastOnce);
      final ticket = c.publishMessage(
          'test/dart-mtls', Uint8List.fromList(utf8.encode('hello-mtls')),
          qos: MqttQos.atLeastOnce);
      await ticket.whenQueued;
      await ticket.whenSent;
      await ticket.whenAcknowledged;
      final deadline = DateTime.now().add(const Duration(seconds: 5));
      while (received.isEmpty && DateTime.now().isBefore(deadline)) {
        await Future<void>.delayed(const Duration(milliseconds: 10));
      }
      expect(received.single.topic, 'test/dart-mtls');
      expect(utf8.decode(received.single.payload), 'hello-mtls');
    } finally {
      await c.close();
      await sub.cancel();
    }
  });

  test('missing client certificate fails once without retry', () async {
    await expectConnectFailure(
        id: 'dart-mtls-missing', options: mtlsOptions(caPem: caPem), code: 14);
  });

  test('untrusted client certificate fails once without retry', () async {
    await expectConnectFailure(
        id: 'dart-mtls-untrusted',
        options: mtlsOptions(
            caPem: caPem, clientCertPem: otherCert, clientKeyPem: otherKey),
        code: 14);
  });

  test('mismatched certificate and key is rejected before connect', () async {
    await expectConnectFailure(
        id: 'dart-mtls-mismatch',
        options: mtlsOptions(
            caPem: caPem, clientCertPem: clientCert, clientKeyPem: otherKey),
        code: 21);
  });

  test('invalid client certificate PEM is rejected', () async {
    await expectConnectFailure(
        id: 'dart-mtls-bad-cert',
        options: mtlsOptions(
            caPem: caPem,
            clientCertPem: 'this is not a certificate',
            clientKeyPem: clientKey),
        code: 20);
  });

  test('invalid client key PEM is rejected', () async {
    await expectConnectFailure(
        id: 'dart-mtls-bad-key',
        options: mtlsOptions(
            caPem: caPem,
            clientCertPem: clientCert,
            clientKeyPem: 'this is not a private key'),
        code: 20);
  });

  test('client identity without TLS is rejected rather than ignored', () async {
    final c = client('dart-mtls-plain', broker.tlsPort);
    try {
      await expectLater(
          c.connect(
              options: MqttConnectOptions(
                  clientCertPem: clientCert, clientKeyPem: clientKey)),
          throwsA(isA<ArgumentError>()));
    } finally {
      await c.close();
    }
  });
}
