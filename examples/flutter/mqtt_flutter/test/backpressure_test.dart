import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:mqtt_flutter/mqtt_flutter.dart';
import 'package:test/test.dart';

void main() {
  test(
      'paused listener backpressures bounded native events and observes Overflow on resume',
      () async {
    final port = int.parse(Platform.environment['MQTT_TEST_PORT']!);
    final receiver = MqttClient(
        server: '127.0.0.1', clientId: 'paused-receiver', port: port);
    final sender =
        MqttClient(server: '127.0.0.1', clientId: 'paused-sender', port: port);
    var count = 0;
    final overflow = Completer<void>();
    final messages = receiver.messages.listen((_) {
      count++;
    });
    final events = receiver.events.listen((e) {
      if (e.error?.code == 12 && !overflow.isCompleted) overflow.complete();
    });
    try {
      await receiver.connect();
      await receiver.subscribe('slice/paused');
      await sender.connect();
      messages.pause();
      for (var i = 0; i < 150; i++) {
        final t = sender.publishMessage('slice/paused', Uint8List.fromList([i]),
            qos: MqttQos.atLeastOnce);
        await t.whenAcknowledged;
      }
      expect(count, 0);
      messages.resume();
      await overflow.future.timeout(const Duration(seconds: 5));
      expect(count, 128);
      expect(receiver.connectionState, MqttConnectionState.disconnected);
    } finally {
      await receiver.close();
      await sender.close();
      await messages.cancel();
      await events.cancel();
      await MqttClient.shutdownRuntime();
    }
  });
}
