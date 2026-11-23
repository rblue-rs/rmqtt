import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';

const _user = 'dart-test-user';
const _secret = 'dart-fixture-secret-9c2b';

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
  final int? tlsPort;
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

Future<ServedBroker> startBroker({
  required String root,
  String? username,
  String? password,
  String? certPath,
  String? keyPath,
}) async {
  final ready = File(
      '${Directory.systemTemp.path}/mqtt-auth-ready-$pid-${DateTime.now().microsecondsSinceEpoch}');
  if (ready.existsSync()) {
    ready.deleteSync();
  }
  final environment = {...Platform.environment};
  if (username != null) {
    environment['MQTT_TEST_AUTH_USERNAME'] = username;
    environment['MQTT_TEST_AUTH_PASSWORD'] = password!;
  }
  if (certPath != null) {
    environment['MQTT_TEST_TLS_CERT'] = certPath;
    environment['MQTT_TEST_TLS_KEY'] = keyPath!;
  }
  final process = await Process.start(
    pythonBin(),
    ['$root/scripts/with_broker.py', '--serve', ready.path],
    environment: environment,
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
  final readyText = ready.readAsStringSync();
  expect(readyText.toLowerCase().contains('password'), isFalse);
  var tlsPort = 0;
  var plainPort = 0;
  for (final line in readyText.split('\n')) {
    final parts = line.split('=');
    if (parts.length != 2) continue;
    final value = int.parse(parts[1]);
    if (parts[0] == 'plain_port') {
      plainPort = value;
    } else if (parts[0] == 'tls_port') {
      tlsPort = value;
    } else if (parts[0] == 'port') {
      if (certPath != null) {
        tlsPort = value;
      } else {
        plainPort = value;
      }
    }
  }
  if (plainPort == 0 || (certPath != null && tlsPort == 0)) {
    process.kill(ProcessSignal.sigkill);
    throw StateError('broker ready file missing ports');
  }
  return ServedBroker(process, certPath == null ? null : tlsPort, plainPort);
}

MqttClient client(String id, int port) =>
    MqttClient(server: '127.0.0.1', clientId: id, port: port);

void assertNoSecret(Object? value) {
  final text = value.toString();
  expect(text.contains(_secret), isFalse, reason: 'secret material leaked');
}

void main() {
  late String root;
  late ServedBroker authBroker;

  setUpAll(() async {
    root = repoRoot();
    authBroker =
        await startBroker(root: root, username: _user, password: _secret);
  });

  tearDownAll(() async {
    await authBroker.stop();
    await MqttClient.shutdownRuntime();
  });

  test('defaults have no credentials and toString redacts a password', () {
    const defaults = MqttConnectOptions();
    expect(defaults.username, isNull);
    expect(defaults.password, isNull);
    expect(defaults.allowPlaintextCredentials, isFalse);
    expect(defaults.will, isNull);
    expect(defaults.keepAlivePeriod, Duration.zero);
    const options = MqttConnectOptions(
        username: _user, password: _secret, allowPlaintextCredentials: true);
    assertNoSecret(options);
    expect(options.toString().contains('<redacted>'), isTrue);
    expect(options.toString().contains(_user), isTrue);
  });

  test('correct plaintext credentials connect when explicitly allowed',
      () async {
    final c = client('dart-auth-ok', authBroker.plainPort);
    try {
      final status = await c.connect(
          options: const MqttConnectOptions(
              username: _user,
              password: _secret,
              allowPlaintextCredentials: true),
          timeout: const Duration(seconds: 5));
      expect(status.sessionPresent, isFalse);
    } finally {
      await c.close();
    }
  });

  test('wrong credentials fail once and settle the Future once', () async {
    final c = client('dart-auth-bad', authBroker.plainPort);
    var settled = 0;
    Object? error;
    try {
      final op = c.beginConnect(
          options: const MqttConnectOptions(
              username: _user,
              password: 'dart-fixture-wrong',
              allowPlaintextCredentials: true),
          timeout: const Duration(seconds: 5));
      await op.future.then((_) {
        settled++;
      }, onError: (Object e, StackTrace _) {
        error = e;
        settled++;
      });
      expect(settled, 1);
      expect(error, isA<MqttException>().having((e) => e.code, 'code', 19));
      assertNoSecret(error);
      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(settled, 1);
    } finally {
      await c.close();
    }
  });

  test('TLS may send a password without the plaintext allow switch', () async {
    final certDir = Directory.systemTemp.createTempSync('mqtt-auth-dart-');
    try {
      final generated = await Process.run(
          pythonBin(), ['$root/scripts/gen_test_certs.py', certDir.path]);
      expect(generated.exitCode, 0);
      expect(generated.stdout.toString().contains('BEGIN '), isFalse);
      final caPem = File('${certDir.path}/ca.pem').readAsStringSync();
      final broker = await startBroker(
          root: root,
          username: _user,
          password: _secret,
          certPath: '${certDir.path}/server-127.pem',
          keyPath: '${certDir.path}/server-127.key');
      final c = client('dart-auth-tls', broker.tlsPort!);
      try {
        final status = await c.connect(
            options: MqttConnectOptions(
                useTls: true, caPem: caPem, username: _user, password: _secret),
            timeout: const Duration(seconds: 5));
        expect(status.sessionPresent, isFalse);
        assertNoSecret(MqttConnectOptions(
            useTls: true, caPem: caPem, username: _user, password: _secret));
      } finally {
        await c.close();
        await broker.stop();
      }
    } finally {
      certDir.deleteSync(recursive: true);
    }
  });

  test('plaintext password without allow is rejected before connect', () async {
    final c = client('dart-auth-denied', authBroker.plainPort);
    var settled = 0;
    Object? error;
    try {
      final op = c.beginConnect(
          options: const MqttConnectOptions(username: _user, password: _secret),
          timeout: const Duration(seconds: 5));
      await op.future.then((_) {
        settled++;
      }, onError: (Object e, StackTrace _) {
        error = e;
        settled++;
      });
      expect(settled, 1);
      expect(error, isA<MqttException>().having((e) => e.code, 'code', 18));
      assertNoSecret(error);
      assertNoSecret(
          const MqttConnectOptions(username: _user, password: _secret));
    } finally {
      await c.close();
    }
  });
}
