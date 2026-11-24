import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';
import '../tls_test.dart' as tls;

class Probe {
  final lib = DynamicLibrary.open(Platform.environment['MQTT_NATIVE_LIBRARY']!);
  late final read =
      lib.lookupFunction<Uint64 Function(Uint32), int Function(int)>(
          'mqtt_test_ws_probe');
  late final reset = lib.lookupFunction<Void Function(), void Function()>(
      'mqtt_test_ws_probe_reset');
}

Future<void> until(bool Function() predicate, String reason) async {
  final end = DateTime.now().add(const Duration(seconds: 2));
  while (!predicate()) {
    if (DateTime.now().isAfter(end)) fail(reason);
    await Future<void>(() {});
  }
}

void main() {
  late Directory certs;
  setUpAll(() async {
    certs = await Directory.systemTemp.createTemp('mqtt-blocked-wss-');
    final result = await Process.run(tls.pythonBin(),
        ['${tls.repoRoot()}/scripts/gen_test_certs.py', certs.path]);
    expect(result.exitCode, 0, reason: result.stderr.toString());
  });
  tearDownAll(() async {
    await certs.delete(recursive: true);
  });
  for (final secure in [false, true]) {
    for (final v5 in [false, true]) {
      test(
          'public ${secure ? 'WSS' : 'WS'} v${v5 ? 5 : 311} forceclose during observed write Pending',
          () async {
        final probe = Probe();
        probe.reset();
        final args = [
          '${tls.repoRoot()}/scripts/ws_nonreading_peer.py',
          if (secure) '${certs.path}/server-127.pem',
          if (secure) '${certs.path}/server-127.key'
        ];
        final peer = await Process.start(tls.pythonBin(), args);
        final lines = StreamIterator(peer.stdout
            .transform(utf8.decoder)
            .transform(const LineSplitter()));
        final errorText = StringBuffer();
        final errors = peer.stderr.transform(utf8.decoder).listen((s) {
          final left = 4096 - errorText.length;
          if (left > 0) {
            errorText.write(s.substring(0, s.length < left ? s.length : left));
          }
        });
        expect(await lines.moveNext(), true);
        final port = int.parse(lines.current);
        final c = MqttClient(
            server: '127.0.0.1', clientId: 'blocked-$secure-$v5', port: port);
        final tickets = <MqttPublishTicket>[];
        final observations = <Future<void>>[];
        var errorsSeen = 0;
        var phaseSettled = 0;
        var controlsSettled = 0;
        try {
          await c.connect(
              options: MqttConnectOptions(
                  useWebSocket: true,
                  useTls: secure,
                  caPem: secure
                      ? File('${certs.path}/ca.pem').readAsStringSync()
                      : null,
                  protocol: v5 ? MqttProtocol.v5 : MqttProtocol.v311,
                  keepAlivePeriod:
                      v5 ? const Duration(seconds: 5) : Duration.zero,
                  reconnectPolicy: const MqttReconnectPolicy(
                      enabled: true, maxAttempts: 2)));
          expect(await lines.moveNext(), true);
          expect(lines.current, 'nonreading');
          final body = Uint8List(65536);
          final deadline = DateTime.now().add(const Duration(seconds: 3));
          for (var i = 0; i < 512 && probe.read(1) == 0; i++) {
            tickets.add(c.publishMessage('x', body));
            await Future<void>(() {});
            if (DateTime.now().isAfter(deadline)) {
              fail('actual writer Pending not reached');
            }
          }
          expect(probe.read(1), 1,
              reason:
                  'must observe actual Sink poll_flush Pending BEFORE close');
          expect(probe.read(2), greaterThan(0));
          expect(probe.read(6), greaterThan(0));
          tickets.add(c.publishMessage('q1', body, qos: MqttQos.atLeastOnce));
          tickets.add(c.publishMessage('q2', body, qos: MqttQos.exactlyOnce));
          final control = c.beginSubscribe('queued-control');
          observations.add(control.future.then<void>((_) {
            controlsSettled++;
          }, onError: (Object e) {
            expect(
                e,
                anyOf(
                    isA<MqttException>().having((e) => e.code, 'code', 5),
                    isA<StateError>().having(
                        (e) => e.message, 'message', 'Client is closed')));
            controlsSettled++;
          }));
          for (final ticket in tickets) {
            for (final phase in [
              ticket.whenQueued,
              ticket.whenSent,
              if (ticket.whenAcknowledged != null) ticket.whenAcknowledged!
            ]) {
              observations.add(phase.then<void>((_) {
                phaseSettled++;
              }, onError: (Object e) {
                expect(
                    e,
                    anyOf(
                        isA<MqttException>().having((e) => e.code, 'code', 5),
                        isA<StateError>().having(
                            (e) => e.message, 'message', 'Client is closed')));
                errorsSeen++;
                phaseSettled++;
              }));
            }
          }
          final watch = Stopwatch()..start();
          await c.close().timeout(const Duration(seconds: 1));
          expect(watch.elapsed, lessThan(const Duration(seconds: 1)));
          expect(probe.read(3), greaterThan(0),
              reason: 'native close entry also sampled Pending');
          await until(() => probe.read(0) == 0 && probe.read(7) == 0,
              'transport or actor still retained after close');
          expect(probe.read(4), 1);
          expect(probe.read(5), 1);
          expect(probe.read(6), 0);
          await Future.wait(observations);
          expect(controlsSettled, 1);
          expect(phaseSettled, observations.length - 1);
          expect(errorsSeen, greaterThan(0));
          final created = probe.read(5);
          await c.close();
          await expectLater(c.connect(), throwsStateError);
          expect(probe.read(5), created);
          print(
              'proof secure=$secure v5=$v5 pendingPolls=${probe.read(2)} sentAttempts=${tickets.length} closeMicros=${watch.elapsedMicroseconds} liveTransport=${probe.read(0)} liveActor=${probe.read(7)} drops=${probe.read(4)}');
        } finally {
          await c.close();
          peer.stdin.writeln('exit');
          await peer.stdin.close();
          expect(await peer.exitCode.timeout(const Duration(seconds: 3)), 0,
              reason: errorText.toString());
          await lines.cancel();
          await errors.cancel();
          await MqttClient.shutdownRuntime();
        }
      });
    }
  }
  for (final secure in [false, true]) {
    for (final v5 in [false, true]) {
      test(
          'ownerexit ${secure ? 'WSS' : 'WS'} v${v5 ? 5 : 311} while actual write Pending (not GC)',
          () async {
        final probe = Probe();
        probe.reset();
        await MqttClient.ensureInitialized();
        final peer = await Process.start(tls.pythonBin(), [
          '${tls.repoRoot()}/scripts/ws_nonreading_peer.py',
          if (secure) '${certs.path}/server-127.pem',
          if (secure) '${certs.path}/server-127.key'
        ]);
        final lines = StreamIterator(peer.stdout
            .transform(utf8.decoder)
            .transform(const LineSplitter()));
        final errorText = StringBuffer();
        final err = peer.stderr.transform(utf8.decoder).listen((s) {
          final left = 4096 - errorText.length;
          if (left > 0) {
            errorText.write(s.substring(0, s.length < left ? s.length : left));
          }
        });
        final inbox = ReceivePort();
        final messages = StreamIterator(inbox);
        Isolate? worker;
        try {
          expect(await lines.moveNext(), true);
          final port = int.parse(lines.current);
          worker = await Isolate.spawn(
              blockedOwner,
              [
                inbox.sendPort,
                port,
                secure,
                v5,
                secure ? File('${certs.path}/ca.pem').readAsStringSync() : null
              ],
              onError: inbox.sendPort);
          expect(await messages.moveNext().timeout(const Duration(seconds: 4)),
              true);
          final control = messages.current as SendPort;
          expect(await messages.moveNext().timeout(const Duration(seconds: 4)),
              true);
          expect(messages.current, 'pending');
          expect(probe.read(1), 1);
          control.send('exit');
          await until(() => probe.read(0) == 0 && probe.read(7) == 0,
              'ownerexit did not release blocked actor/transport');
          expect(probe.read(3), greaterThan(0));
          expect(probe.read(4), 1);
          expect(probe.read(5), 1);
          expect(probe.read(6), 0);
          print(
              'ownerexit secure=$secure v5=$v5 pendingPolls=${probe.read(2)} closePending=${probe.read(3)} liveTransport=${probe.read(0)} liveActor=${probe.read(7)} drops=${probe.read(4)}');
        } finally {
          worker?.kill(priority: Isolate.immediate);
          peer.stdin.writeln('exit');
          await peer.stdin.close();
          expect(await peer.exitCode.timeout(const Duration(seconds: 3)), 0,
              reason: errorText.toString());
          await lines.cancel();
          await err.cancel();
          await messages.cancel();
          inbox.close();
          await MqttClient.shutdownRuntime();
        }
      });
    }
  }
}

// Owner exits without Client.close: native runtime-owner onExit cleanup is the subject.
Future<void> blockedOwner(List<Object?> args) async {
  final send = args[0] as SendPort;
  final control = ReceivePort();
  send.send(control.sendPort);
  final probe = Probe();
  final secure = args[2] as bool, v5 = args[3] as bool;
  final c = MqttClient(
      server: '127.0.0.1', clientId: 'blocked-owner', port: args[1] as int);
  await c.connect(
      options: MqttConnectOptions(
          useWebSocket: true,
          useTls: secure,
          caPem: args[4] as String?,
          protocol: v5 ? MqttProtocol.v5 : MqttProtocol.v311,
          keepAlivePeriod: v5 ? const Duration(seconds: 5) : Duration.zero,
          reconnectPolicy:
              const MqttReconnectPolicy(enabled: true, maxAttempts: 2)));
  final body = Uint8List(65536);
  final end = DateTime.now().add(const Duration(seconds: 3));
  for (var i = 0; i < 512 && probe.read(1) == 0; i++) {
    c.publishMessage('x', body);
    await Future<void>(() {});
    if (DateTime.now().isAfter(end)) {
      throw StateError('owner writer never Pending');
    }
  }
  if (probe.read(1) != 1) throw StateError('no actual Pending');
  send.send('pending');
  await control.first;
  control.close();
  Isolate.exit();
}
