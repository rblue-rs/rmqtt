import 'dart:async';
import 'dart:ffi';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:rmqtt_flutter/src/ffi_bindings.dart';

import 'observe_not_initialized.dart';

int get port => int.parse(Platform.environment['MQTT_TEST_PORT']!);
MqttClient client(String id) =>
    MqttClient(server: '127.0.0.1', clientId: id, port: port);
Future<void> owner(int id, bool explicit) async {
  final c = client('owner-$id');
  await c.connect();
  if (explicit) {
    await c.close();
    await c.close();
    await MqttClient.shutdownRuntime();
  }
}

void killOwner(List<Object> args) async {
  final c = client('kill-owner');
  await c.connect();
  final keep = ReceivePort();
  (args[0] as SendPort).send(true);
  await keep.first;
  await c.close();
}

void naturalOwner(SendPort ready) async {
  final c = client('natural-owner');
  await c.connect();
  await c.close();
  await c.close();
  // No shutdownRuntime; returning with no live timers is a natural isolate exit.
  ready.send(true);
}

Future<WeakReference<MqttClient>> unreachable() async {
  final c = client('gc-owner');
  await c.connect();
  return WeakReference(c);
}

Future<void> main(List<String> args) async {
  final mode = args.single;
  if (mode == 'exit') {
    await Isolate.run(() => owner(0, false));
  } else if (mode == 'multi') {
    await Future.wait(
      List.generate(8, (i) => Isolate.run(() => owner(i, i.isEven))),
    );
  } else if (mode == 'natural') {
    final ready = ReceivePort(), dead = ReceivePort();
    await Isolate.spawn(naturalOwner, ready.sendPort, onExit: dead.sendPort);
    await ready.first.timeout(const Duration(seconds: 5));
    await dead.first.timeout(const Duration(seconds: 5));
    ready.close();
    dead.close();
  } else if (mode == 'init-exit') {
    await Future.wait(
      List.generate(
        8,
        (_) => Isolate.run(() async {
          await MqttClient.ensureInitialized();
        }),
      ),
    );
  } else if (mode == 'kill' || mode == 'kill-default') {
    final ready = ReceivePort(), dead = ReceivePort();
    final isolate = await Isolate.spawn(killOwner, <Object>[
      ready.sendPort,
    ], onExit: dead.sendPort);
    await ready.first.timeout(const Duration(seconds: 5));
    isolate.kill(
      priority: mode == 'kill' ? Isolate.immediate : Isolate.beforeNextEvent,
    );
    await dead.first.timeout(const Duration(seconds: 5));
    ready.close();
    dead.close();
  } else if (mode == 'gc') {
    final weak = await unreachable();
    final ring = <Uint8List>[];
    for (var i = 0; i < 1000 && weak.target != null; i++) {
      ring.add(Uint8List(256 * 1024)..[0] = i & 255);
      if (ring.length > 8) ring.removeAt(0);
      await Future<void>.delayed(const Duration(milliseconds: 2));
    }
    if (weak.target != null) {
      stderr.writeln('client retained after GC pressure');
      exit(1);
    }
    // The client lived in this still-running main isolate. After GC, the
    // static runtime owner still holds this isolate's init ref. Dart
    // shutdownRuntime releases that living owner, retrying Busy until the
    // client finalizer drops the lease. It is not a substituting native
    // shutdown for an already-exited owner.
    for (var i = 0; ; i++) {
      try {
        await MqttClient.shutdownRuntime();
        break;
      } on StateError {
        if (i == 100) rethrow;
        await Future<void>.delayed(const Duration(milliseconds: 10));
      }
    }
  } else {
    throw ArgumentError.value(mode);
  }
  final native = MqttBindings(
    DynamicLibrary.open(Platform.environment['MQTT_NATIVE_LIBRARY']!),
  );
  // Observe automatic cleanup first. A substituting mqtt_runtime_shutdown
  // would consume one leftover owner init ref and hide the mismatch.
  final observed = await waitForNotInitialized(native);
  if (observed.code != 2) {
    stderr.writeln(
      '$mode leaked init refs: new=${observed.code} expected2 before shutdown after ${observed.elapsedMilliseconds}ms',
    );
    exit(1);
  }
  // Shutdown is checked only for idempotence after NotInitialized was observed.
  for (var i = 0; i < 32; i++) {
    final code = native.mqtt_runtime_shutdown();
    if (code != 0) {
      stderr.writeln('$mode shutdown[$i] expected0 actual$code');
      exitCode = 1;
      return;
    }
  }
  stdout.writeln(
    '$mode PASS: NotInitialized observed before shutdown in ${observed.elapsedMilliseconds}ms',
  );
}
