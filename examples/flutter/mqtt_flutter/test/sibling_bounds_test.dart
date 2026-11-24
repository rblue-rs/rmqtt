import 'dart:async';
import 'dart:convert';
import 'mtls_test.dart' as mtls;
import 'dart:io';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';

final invalid = isA<MqttException>().having((e) => e.code, 'code', 1);
MqttConnectOptions options(String field, String value) => switch (field) {
      'username' => MqttConnectOptions(username: value),
      'password' => MqttConnectOptions(
          username: 'dummy', password: value, allowPlaintextCredentials: true),
      'caPem' => MqttConnectOptions(useTls: true, caPem: value),
      'clientCertPem' => MqttConnectOptions(
          useTls: true, clientCertPem: value, clientKeyPem: 'dummy'),
      'clientKeyPem' => MqttConnectOptions(
          useTls: true, clientCertPem: 'dummy', clientKeyPem: value),
      'willTopic' =>
        MqttConnectOptions(will: MqttWill(topic: value, payload: Uint8List(0))),
      _ => MqttConnectOptions(useWebSocket: true, webSocketPath: value),
    };
void main() {
  final limits = {
    'username': 65535,
    'password': 65535,
    'caPem': 65536,
    'clientCertPem': 65536,
    'clientKeyPem': 65536,
    'willTopic': 65535,
    'webSocketPath': 2048
  };
  for (final entry in limits.entries) {
    test('preallocation source contract ${entry.key}', () {
      final source = File('lib/rmqtt_flutter.dart').readAsStringSync();
      final expression =
          entry.key == 'willTopic' ? 'will.topic' : 'options.${entry.key}!';
      final pathExpression =
          entry.key == 'webSocketPath' ? 'options.webSocketPath' : expression;
      expect(source, contains('_boundedUtf8($pathExpression, ${entry.value})'));
      expect(source, isNot(contains('utf8.encode($pathExpression)')));
      expect(source.indexOf('text.length > maxBytes'),
          lessThan(source.indexOf('final encoded = utf8.encode(text)')));
      expect(source.indexOf('encoded.length > maxBytes'),
          greaterThan(source.indexOf('final encoded = utf8.encode(text)')));
    });
    for (final variant in ['ascii', 'multibyte', 'utf16']) {
      test('${entry.key} over bound $variant InvalidArgument', () async {
        final value = variant == 'ascii'
            ? 'a' * (entry.value + 1)
            : variant == 'utf16'
                ? '中' * (entry.value + 1)
                : '中' * (entry.value ~/ 3 + 1);
        final c = MqttClient(server: '127.0.0.1', clientId: 'bounds');
        try {
          await expectLater(
              c.connect(options: options(entry.key, value)), throwsA(invalid));
        } finally {
          await c.close();
          await MqttClient.shutdownRuntime();
        }
      });
    }
  }
  test('malformed dummy PEM keeps existing syntax error codes at byte boundary',
      () async {
    for (final field in ['caPem', 'clientCertPem', 'clientKeyPem']) {
      for (final value in ['a' * 65536, '中' * 21845 + 'a']) {
        final c = MqttClient(server: '127.0.0.1', clientId: 'pem-dummy');
        try {
          await expectLater(
              c.connect(options: options(field, value)),
              throwsA(isA<MqttException>()
                  .having((e) => e.code, 'code', field == 'caPem' ? 1 : 20)));
        } finally {
          await c.close();
          await MqttClient.shutdownRuntime();
        }
      }
    }
  });
  test(
      'public credentials and Will topic exact maxima with wire-representable payload connect',
      () async {
    for (final value in ['a' * 65535, '中' * 21845]) {
      final c = MqttClient(
          server: '127.0.0.1',
          clientId: 'dummy-max',
          port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
      try {
        await c.connect(
            options: MqttConnectOptions(
                username: value,
                password: value,
                allowPlaintextCredentials: true,
                will: MqttWill(topic: value, payload: Uint8List(65535))));
        await c.disconnect();
      } finally {
        await c.close();
        await MqttClient.shutdownRuntime();
      }
    }
  });
  test(
      'valid CA/client cert/key at exact65536 ASCII and multibyte bytes retain mTLS',
      () async {
    final dir = await Directory.systemTemp.createTemp('mqtt-sibling-certs-');
    addTearDown(() async {
      if (await dir.exists()) await dir.delete(recursive: true);
    });
    final generated = await Process.run(mtls.pythonBin(),
        ['${mtls.repoRoot()}/scripts/gen_test_certs.py', dir.path]);
    expect(generated.exitCode, 0);
    final ca = File('${dir.path}/ca.pem').readAsStringSync();
    final cert = File('${dir.path}/client-ok.pem').readAsStringSync();
    final key = File('${dir.path}/client-ok.key').readAsStringSync();
    final broker = await mtls.startMtlsBroker(mtls.repoRoot(), dir.path);
    String padded(String value, bool multi) {
      final gap = 65536 - utf8.encode(value).length;
      return value + (multi ? '中' * (gap ~/ 3) + ' ' * (gap % 3) : ' ' * gap);
    }

    try {
      for (final field in ['ca', 'cert', 'key']) {
        for (final multi in [false, true]) {
          final c = MqttClient(
              server: '127.0.0.1',
              clientId: 'valid-pem-bound',
              port: broker.tlsPort);
          try {
            await c.connect(
                options: MqttConnectOptions(
                    useTls: true,
                    caPem: field == 'ca' ? padded(ca, multi) : ca,
                    clientCertPem: field == 'cert' ? padded(cert, multi) : cert,
                    clientKeyPem: field == 'key' ? padded(key, multi) : key));
            await c.disconnect();
          } finally {
            await c.close();
            await MqttClient.shutdownRuntime();
          }
        }
      }
    } finally {
      await broker.stop();
      await dir.delete(recursive: true);
    }
  });
  test('Will payload over maximum rejected before arena copy', () async {
    final c = MqttClient(server: '127.0.0.1', clientId: 'payload');
    try {
      await expectLater(
          () async => c.connect(
              options: MqttConnectOptions(
                  will: MqttWill(topic: 'dummy', payload: Uint8List(65537)))),
          throwsA(invalid));
    } finally {
      await c.close();
      await MqttClient.shutdownRuntime();
    }
  });
  test('ASCII WS path exact maximum reaches observed HTTP request', () async {
    final server = await HttpServer.bind('127.0.0.1', 0);
    final observed = Completer<void>();
    final listener = server.listen((req) async {
      expect(req.uri.path.length, 2048);
      req.response.statusCode = 403;
      await req.response.close();
      observed.complete();
    });
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'path-dummy', port: server.port);
    try {
      await expectLater(
          c.connect(
              options: MqttConnectOptions(
                  useWebSocket: true, webSocketPath: '/${'a' * 2047}')),
          throwsA(isA<MqttException>().having((e) => e.code, 'code', 7)));
      await observed.future.timeout(const Duration(seconds: 5));
    } finally {
      await c.close();
      await listener.cancel();
      await server.close(force: true);
      await MqttClient.shutdownRuntime();
    }
  });
  test('Unicode WS path remains invalid even below byte maximum', () async {
    final c = MqttClient(server: '127.0.0.1', clientId: 'path');
    try {
      await expectLater(
          c.connect(
              options: const MqttConnectOptions(
                  useWebSocket: true, webSocketPath: '/中')),
          throwsA(invalid));
    } finally {
      await c.close();
      await MqttClient.shutdownRuntime();
    }
  });
}
