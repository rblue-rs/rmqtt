import 'dart:io';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';

void main() {
  test('websocket opt-in defaults and redaction', () {
    const options = MqttConnectOptions();
    expect(options.useWebSocket, false);
    expect(options.webSocketPath, '/mqtt');
    expect(
        const MqttConnectOptions(
                useWebSocket: true, webSocketPath: '/secret-path')
            .toString(),
        isNot(contains('secret-path')));
  });
  test('invalid WS path and v5 default zero never open network', () async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    var accepts = 0;
    final sub = server.listen((s) {
      accepts++;
      s.destroy();
    });
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'ws-invalid', port: server.port);
    try {
      for (final path in ['', 'invalid']) {
        await expectLater(
            c.connect(
                options: MqttConnectOptions(
                    useWebSocket: true, webSocketPath: path)),
            throwsA(isA<MqttException>().having((e) => e.code, 'code', 1)));
      }
      for (final tls in [false, true]) {
        await expectLater(
            c.connect(
                options: MqttConnectOptions(
                    useWebSocket: true, useTls: tls, protocol: MqttProtocol.v5)),
            throwsA(isA<MqttException>().having((e) => e.code, 'code', 6)));
      }
      expect(accepts, 0);
    } finally {
      await c.close();
      await sub.cancel();
      await server.close();
      await MqttClient.shutdownRuntime();
    }
  });
}
