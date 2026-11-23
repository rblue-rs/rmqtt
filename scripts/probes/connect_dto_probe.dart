import 'dart:async';
import 'dart:mirrors';
import 'dart:typed_data';

import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';

class ExtraDuration extends Duration {
  final baggage = Uint8List(1024 * 1024);
  ExtraDuration(int micros) : super(microseconds: micros);
}

class ExtraPolicy extends MqttReconnectPolicy {
  final baggage = Uint8List(1024 * 1024);
  ExtraPolicy() : super(enabled: false);
  @override
  Duration get baseDelay => ExtraDuration(1000000);
}

class ViewWill extends MqttWill {
  final Uint8List view;
  ViewWill(this.view) : super(topic: 'will/owned', payload: Uint8List(0));
  @override
  Uint8List get payload => view;
}

class ExtraOptions extends MqttConnectOptions {
  final baggage = Uint8List(1024 * 1024);
  final MqttWill value;
  ExtraOptions(this.value)
    : super(
        username: 'dummy',
        password: 'dummy',
        allowPlaintextCredentials: true,
      );
  @override
  MqttWill? get will => value;
  @override
  MqttReconnectPolicy get reconnectPolicy => ExtraPolicy();
  @override
  Duration get keepAlivePeriod => ExtraDuration(0);
}

class BombOptions extends MqttConnectOptions {
  int reads = 0;
  @override
  Duration get keepAlivePeriod {
    reads++;
    throw StateError('must not read rejected input');
  }
}

class GetterFailure implements Exception {
  @override
  String toString() => throw StateError('do not convert');
}

class ThrowOptions extends MqttConnectOptions {
  final Object failure;
  ThrowOptions(this.failure);
  @override
  String? get username => throw failure;
}

void main() {
  final lib = reflectClass(MqttClient).owner as LibraryMirror;
  test('actual connect DTO has only plain bounded retained fields and detached Will view', () {
    final storage = Uint8List(8 * 1024 * 1024), view = Uint8List(3);
    storage.setRange(4096, 4099, [1, 2, 3]);
    final source = ExtraOptions(
      ViewWill(Uint8List.view(storage.buffer, 4096, 3)),
    );
    final cls =
        lib.declarations[MirrorSystem.getSymbol('_OwnedConnectInput', lib)]
            as ClassMirror;
    final dto = cls.newInstance(#capture, [source, ExtraDuration(1200000)]);
    final owned = dto.getField(#options).reflectee as MqttConnectOptions;
    expect(owned.runtimeType, MqttConnectOptions);
    expect(identical(owned, source), isFalse);
    expect(owned.reconnectPolicy.runtimeType, MqttReconnectPolicy);
    expect(owned.reconnectPolicy.baseDelay.runtimeType, Duration);
    expect(owned.keepAlivePeriod.runtimeType, Duration);
    expect(dto.getField(#timeout).reflectee.runtimeType, Duration);
    expect(owned.will!.runtimeType, MqttWill);
    expect(owned.will!.payload.buffer.lengthInBytes, 3);
    expect(owned.will!.payload, [1, 2, 3]);
    storage.fillRange(4096, 4099, 0);
    expect(owned.will!.payload, [1, 2, 3]);
    expect(view.length, 3);
  });
  test('QueueFull happens before any snapshot getter or copy', () async {
    final c = MqttClient(server: '127.0.0.1', clientId: 'full-owned');
    reflect(c).setField(MirrorSystem.getSymbol('_activeLaunches', lib), 32);
    final source = BombOptions();
    final op = c.beginConnect(options: source);
    await expectLater(
      op.future,
      throwsA(isA<MqttException>().having((e) => e.code, 'code', 5)),
    );
    expect(source.reads, 0);
    reflect(c).setField(MirrorSystem.getSymbol('_activeLaunches', lib), 0);
    await c.close();
  });
  test(
    'getter failure remains original caller error without client registration',
    () async {
      final c = MqttClient(server: '127.0.0.1', clientId: 'bad-getter');
      final error = GetterFailure();
      final op = c.beginConnect(options: ThrowOptions(error));
      await op.future.then<void>(
        (_) {
          fail('unexpected success');
        },
        onError: (Object e, StackTrace s) {
          expect(identical(e, error), isTrue);
          expect(s.toString().length, lessThanOrEqualTo(96));
        },
      );
      expect(
        reflect(c)
            .getField(MirrorSystem.getSymbol('_activeLaunches', lib))
            .reflectee,
        0,
      );
      expect(
        (reflect(c)
                    .getField(MirrorSystem.getSymbol('_controlWaiters', lib))
                    .reflectee
                as Map)
            .isEmpty,
        isTrue,
      );
      await c.close();
    },
  );
  test(
    'inner async controlled throw has finite trace before outer forwarding',
    () async {
      final c = MqttClient(server: '127.0.0.1', clientId: 'inner-trace');
      final future =
          reflect(c).invoke(MirrorSystem.getSymbol('_connect', lib), [
                1,
                const MqttConnectOptions(
                  keepAlivePeriod: Duration(microseconds: -1),
                ),
                null,
              ]).reflectee
              as Future<dynamic>;
      await future.then<void>(
        (_) {
          fail('unexpected success');
        },
        onError: (Object e, StackTrace s) {
          expect(e, isA<ArgumentError>());
          expect(s.toString().length, lessThanOrEqualTo(96));
          expect(
            (e as Error).stackTrace.toString().length,
            lessThanOrEqualTo(96),
          );
        },
      );
      await c.close();
    },
  );
}
