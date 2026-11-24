import 'dart:io';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';
import 'ack_timeout_test.dart' show Wire;

class PoisonOptions extends MqttConnectOptions {
  bool poison = false;
  int usernameReads = 0;
  final backing = Uint8List(8 * 1024 * 1024);
  final MqttWill capturedWill;
  PoisonOptions(this.capturedWill) : super(allowPlaintextCredentials: true);
  @override
  String? get username {
    usernameReads++;
    if (poison) throw StateError('original options retained');
    return 'dummy-user';
  }

  @override
  String? get password => 'dummy-pass';
  @override
  MqttWill? get will => capturedWill;
}

void main() {
  test('public connect retains snapshot, never rereads poisoned caller options',
      () async {
    final listener = await ServerSocket.bind('127.0.0.1', 0);
    Socket? peer;
    bool shutting = false;
    final server = () async {
      peer = await listener.first;
      final w = Wire(peer!);
      final packet = await w.packet();
      expect(packet[0], 0x10);
      expect(packet, containsAllInOrder('dummy-user'.codeUnits));
      expect(packet, containsAllInOrder('dummy-pass'.codeUnits));
      expect(packet, containsAllInOrder([0x71, 0x72, 0x73]));
      peer!.add([32, 2, 0, 0]);
      await peer!.flush();
      expect(await w.packet(), [0xe0]);
    }()
        .catchError((Object e) {
      if (!shutting) throw e;
    });
    final payload = Uint8List.fromList([0x71, 0x72, 0x73]);
    final will = MqttWill(topic: 'will/snapshot', payload: payload);
    final options = PoisonOptions(will);
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'snapshot-public', port: listener.port);
    try {
      final op = c.beginConnect(options: options);
      options.poison = true;
      will.payload.fillRange(0, 3, 0);
      await op.future;
      expect(options.usernameReads, 1);
      await c.disconnect();
      await server;
    } finally {
      shutting = true;
      await c.close();
      peer?.destroy();
      await listener.close();
      await server;
      await MqttClient.shutdownRuntime();
    }
  });
}
