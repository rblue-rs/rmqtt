import 'dart:async';
import 'package:mqtt_flutter/src/event_pump.dart';
import 'package:test/test.dart';

void main() {
  test('idle polling backs off, wake resets, and close stops scheduling',
      () async {
    var calls = 0;
    final ready = Completer<void>();
    bool poll() {
      if (++calls == 5) ready.complete();
      return false;
    }

    final pump = EventPump(poll);
    await ready.future;
    expect(pump.currentDelay.inMilliseconds, 80);
    pump.wake();
    expect(pump.currentDelay.inMilliseconds, 5);
    pump.close();
    final count = calls;
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(calls, count);
    expect(
        poll, isNotNull); // Keep the weak callback reachable until this point.
  });
  test('wake during a tick cannot leave an uncancellable second timer',
      () async {
    final first = Completer<void>();
    late EventPump pump;
    var calls = 0;
    bool poll() {
      calls++;
      pump.wake();
      if (!first.isCompleted) first.complete();
      return true;
    }

    pump = EventPump(poll);
    await first.future;
    pump.close();
    await Future<void>.delayed(const Duration(milliseconds: 30));
    expect(calls, 1);
    expect(poll, isNotNull);
  });
  test('listener cap rejects listener17 and a resumed listener signals wake',
      () async {
    final stream = EventStream<int>();
    var wakes = 0;
    stream.onResume = () => wakes++;
    final subscriptions =
        List.generate(16, (_) => stream.stream.listen((_) {}));
    final failed = Completer<void>();
    final extra = stream.stream.listen((_) {}, onError: (Object e) {
      expect(e, isA<StateError>());
      failed.complete();
    });
    await failed.future;
    subscriptions.first.pause();
    expect(stream.paused, isTrue);
    subscriptions.first.resume();
    expect(stream.paused, isFalse);
    expect(wakes, 1);
    stream.close();
    for (final s in subscriptions) {
      await s.cancel();
    }
    await extra.cancel();
  });
}
