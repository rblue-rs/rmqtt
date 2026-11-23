import 'dart:async';
import 'dart:mirrors';
import 'package:rmqtt_flutter/rmqtt_flutter.dart';
import 'package:test/test.dart';

class HostileTrace implements StackTrace {
  int stringCalls = 0;
  final backing = List<int>.filled(1024 * 1024, 0);
  @override
  String toString() {
    stringCalls++;
    throw StateError('must not stringify trace');
  }
}

class HostileError implements Exception {
  @override
  String toString() => throw StateError('must not stringify error');
}

void main() {
  final lib = reflectClass(MqttClient).owner as LibraryMirror;
  test(
      'passive outcome never retains arbitrary trace backing or stringifies it',
      () async {
    final cls = lib.declarations[MirrorSystem.getSymbol('_DeferredResult', lib)]
        as ClassMirror;
    final cell = cls.newInstance(const Symbol(''), []).reflectee;
    final trace = HostileTrace(), error = HostileError();
    reflect(cell).invoke(#completeError, [error, trace]);
    final stored =
        reflect(cell).getField(MirrorSystem.getSymbol('_trace', lib)).reflectee;
    expect(identical(stored, trace), isFalse);
    final f = cell as Future<dynamic>;
    await f.then<void>((_) {
      fail('unexpected success');
    }, onError: (Object e, StackTrace s) {
      expect(identical(e, error), isTrue);
      expect(s.toString().length, lessThanOrEqualTo(96));
    });
    expect(trace.stringCalls, 0);
  });
  test('rejected view does not retain arbitrary trace', () async {
    final cls =
        lib.declarations[MirrorSystem.getSymbol('_RejectedLaunchFuture', lib)]
            as ClassMirror;
    final trace = HostileTrace(), error = HostileError();
    final f = cls.newInstance(const Symbol(''), [error, trace]).reflectee
        as Future<dynamic>;
    var observed = false;
    await f.then<void>((_) {
      fail('unexpected success');
    }, onError: (Object e, StackTrace s) {
      observed = true;
      expect(identical(e, error), isTrue);
      expect(identical(s, trace), isFalse);
      expect(s.toString().length, lessThanOrEqualTo(96));
    });
    expect(observed, isTrue);
    expect(trace.stringCalls, 0);
  });
  test(
      'admitted control completer normalizes supplied trace without conversion',
      () async {
    final c =
        lib.invoke(MirrorSystem.getSymbol('_completer', lib), []).reflectee
            as Completer<dynamic>;
    final trace = HostileTrace(), error = HostileError();
    final seen = c.future.then<void>((_) {
      fail('unexpected success');
    }, onError: (Object e, StackTrace s) {
      expect(identical(e, error), isTrue);
      expect(identical(s, trace), isFalse);
      expect(s.toString().length, lessThanOrEqualTo(96));
    });
    c.completeError(error, trace);
    await seen;
    expect(trace.stringCalls, 0);
  });
}
