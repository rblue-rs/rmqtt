import 'dart:async';
import 'dart:io';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';

class Wire {
  final StreamIterator<Uint8List> input;
  final List<int> pending = [];
  Wire(Socket socket) : input = StreamIterator(socket);
  Future<int> byte() async {
    while (pending.isEmpty) {
      if (!await input.moveNext().timeout(const Duration(seconds: 5))) {
        throw StateError('EOF');
      }
      pending.addAll(input.current);
    }
    return pending.removeAt(0);
  }

  Future<List<int>> packet() async {
    final head = await byte();
    var length = 0, shift = 0;
    while (true) {
      final b = await byte();
      length += (b & 127) << shift;
      if (b & 128 == 0) break;
      shift += 7;
    }
    final p = <int>[head];
    for (var i = 0; i < length; i++) {
      p.add(await byte());
    }
    return p;
  }
}

void main() {
  test('a timeout abandons waiting, not an already sent QoS1 publish',
      () async {
    final listener = await ServerSocket.bind('127.0.0.1', 0);
    final published = Completer<void>(), ack = Completer<void>();
    Socket? accepted;
    final broker = () async {
      final s = accepted = await listener.first;
      final wire = Wire(s);
      await wire.packet();
      s.add([0x20, 2, 0, 0]);
      await s.flush();
      final p = await wire.packet();
      expect(p[0] & 6, 2);
      final n = (p[1] << 8) | p[2];
      final id = p.sublist(n + 3, n + 5);
      expect(p.sublist(n + 5), [0, 255]);
      published.complete();
      await ack.future;
      s.add([0x40, 2, ...id]);
      await s.flush();
    }();
    final c = MqttClient(
        server: '127.0.0.1', clientId: 'late-ack', port: listener.port);
    final stages = <MqttPublishStage>[];
    final updates = c.publishUpdates.listen((e) => stages.add(e.stage));
    try {
      await c.connect();
      final ticket = c.publishMessage('x', Uint8List.fromList([0, 255]),
          qos: MqttQos.atLeastOnce);
      await ticket.whenQueued;
      await ticket.whenSent;
      await published.future;
      await expectLater(
          ticket.whenAcknowledged!.timeout(const Duration(milliseconds: 30)),
          throwsA(isA<TimeoutException>()));
      expect(stages, [MqttPublishStage.queued, MqttPublishStage.sent]);
      ack.complete();
      await ticket.whenAcknowledged;
      await broker;
      expect(stages.last, MqttPublishStage.acknowledged);
    } finally {
      if (!ack.isCompleted) ack.complete();
      await c.close();
      accepted?.destroy();
      await listener.close();
      await updates.cancel();
      await MqttClient.shutdownRuntime();
    }
  });
}
