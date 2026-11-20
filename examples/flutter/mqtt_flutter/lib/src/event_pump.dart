// Internal weak poll scheduling and bounded stream backpressure.
import 'dart:async';

final class EventPump {
  final WeakReference<bool Function()> _poll;
  Timer? _timer;
  bool _closed = false;
  Duration _delay = const Duration(milliseconds: 5);
  Duration get currentDelay => _delay;
  EventPump(bool Function() poll) : _poll = WeakReference(poll) {
    _schedule();
  }
  void _schedule() {
    if (!_closed) _timer = Timer(_delay, _tick);
  }

  void _tick() {
    if (_closed) return;
    _timer = null;
    final poll = _poll.target;
    if (poll == null) {
      close();
      return;
    }
    final work = poll();
    if (_closed || _timer != null) return;
    _delay = Duration(
        milliseconds: work ? 5 : (_delay.inMilliseconds * 2).clamp(5, 80));
    _schedule();
  }

  void wake() {
    if (_closed) return;
    _timer?.cancel();
    _delay = const Duration(milliseconds: 5);
    _schedule();
  }

  void close() {
    _closed = true;
    _timer?.cancel();
    _timer = null;
  }
}

/// Pause applies backpressure to the native poller; no unbounded Dart pause queue.
/// While paused, native bounded queues can terminate the connection with Overflow.
class EventStream<T> {
  final Map<MultiStreamController<T>, bool> _listeners = {};
  bool _closed = false;
  void Function()? onResume;
  bool get paused => _listeners.values.any((v) => v);
  late final Stream<T> stream = Stream<T>.multi((c) {
    if (_closed) {
      c.closeSync();
      return;
    }
    if (_listeners.length >= 16) {
      c.addErrorSync(StateError('Too many listeners'));
      c.closeSync();
      return;
    }
    _listeners[c] = false;
    c.onPause = () {
      _listeners[c] = true;
    };
    c.onResume = () {
      _listeners[c] = false;
      onResume?.call();
    };
    c.onCancel = () {
      _listeners.remove(c);
    };
  }, isBroadcast: true);
  void add(T value) {
    for (final c in _listeners.keys.toList()) {
      c.addSync(value);
    }
  }

  void close() {
    _closed = true;
    for (final c in _listeners.keys.toList()) {
      c.closeSync();
    }
    _listeners.clear();
  }
}
