import 'dart:io';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';

final invalid = isA<MqttException>().having((e) => e.code, 'code', 1);
void main() {
  for (final field in ['server', 'clientId']) {
    for (final text in ['a' * 65536, '中' * 65536, '😀' * 32768]) {
      test(
          '$field rejects excess UTF16 units ${text.codeUnitAt(0)} without connect',
          () {
        expect(
            () => MqttClient(
                server: field == 'server' ? text : 'localhost',
                clientId: field == 'clientId' ? text : 'bounds'),
            throwsA(invalid));
      });
    }
    test('$field accepts the ASCII UTF16 boundary', () async {
      final c = MqttClient(
          server: field == 'server' ? 'a' * 65535 : 'localhost',
          clientId: field == 'clientId' ? 'a' * 65535 : 'bounds');
      await c.close();
    });
    test('$field rejects UTF8 byte excess below UTF16 bound', () async {
      final c = MqttClient(
          server: field == 'server' ? '中' * 21846 : 'localhost',
          clientId: field == 'clientId' ? '中' * 21846 : 'bounds');
      try {
        await expectLater(c.connect(), throwsA(invalid));
      } finally {
        await c.close();
        await MqttClient.shutdownRuntime();
      }
    });
  }
  test(
      'guard source contract: topic and endpoint reject before encoding and arena',
      () {
    final api = File('lib/rmqtt_flutter.dart').readAsStringSync();
    final topic = api.substring(api.indexOf('int _withTopic('),
        api.indexOf('MqttPublishTicket publishMessage('));
    expect(topic.indexOf('text.length > 65535'), greaterThanOrEqualTo(0));
    expect(topic.indexOf('text.length > 65535'),
        lessThan(topic.indexOf('utf8.encode(text)')));
    expect(topic.indexOf('bytes.length > 65535'),
        lessThan(topic.indexOf('arena<Uint8>')));
    final native = File('lib/src/native_client.dart').readAsStringSync();
    expect(native.indexOf('server.length > 65535'), greaterThanOrEqualTo(0));
    expect(native.indexOf('server.length > 65535'),
        lessThan(native.indexOf('utf8.encode(server)')));
    expect(native.indexOf('clientId.length > 65535'),
        lessThan(native.indexOf('utf8.encode(clientId)')));
    expect(native.indexOf('host.length > 65535'), greaterThanOrEqualTo(0));
    expect(native.indexOf('host.length > 65535'),
        lessThan(native.indexOf('a<Uint8>(host.length)')));
    expect(native.indexOf('id.length > 65535'),
        lessThan(native.indexOf('a<Uint8>(id.length)')));
  });
  for (final id in ['a' * 65535, '中' * 21845, '😀' * 16383 + 'abc']) {
    test('public clientId exact UTF8 boundary ${id.codeUnitAt(0)}', () async {
      final c = MqttClient(
          server: '127.0.0.1',
          clientId: id,
          port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
      try {
        await c.connect();
        await c.disconnect();
      } finally {
        await c.close();
        await MqttClient.shutdownRuntime();
      }
    });
  }
  test(
      'public boundary smoke: topic ASCII/multibyte accepted; oversize InvalidArgument',
      () async {
    final c = MqttClient(
        server: '127.0.0.1',
        clientId: 'text-bounds',
        port: int.parse(Platform.environment['MQTT_TEST_PORT']!));
    try {
      await c.connect();
      for (final topic in ['a' * 65535, '中' * 21845, '😀' * 16383 + 'abc']) {
        await c.subscribe(topic);
        final ticket = c.publishMessage(topic, Uint8List.fromList([0, 255]),
            qos: MqttQos.exactlyOnce);
        await ticket.whenAcknowledged;
        await c.unsubscribe(topic);
      }
      for (final topic in ['a' * 65536, '中' * 21846, '😀' * 16384]) {
        await expectLater(c.subscribe(topic), throwsA(invalid));
        await expectLater(c.unsubscribe(topic), throwsA(invalid));
        final t =
            c.publishMessage(topic, Uint8List(0), qos: MqttQos.exactlyOnce);
        await Future.wait([
          expectLater(t.whenQueued, throwsA(invalid)),
          expectLater(t.whenSent, throwsA(invalid)),
          expectLater(t.whenAcknowledged!, throwsA(invalid))
        ]);
      }
    } finally {
      await c.close();
      await MqttClient.shutdownRuntime();
    }
  });
}
