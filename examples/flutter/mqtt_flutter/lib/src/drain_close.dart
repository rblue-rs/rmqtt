part of '../rmqtt_flutter.dart';

/// Terminal result of one graceful-close wait. Cancelled abandons only that
/// caller's waiting; the shared native drain still reaches a bounded close.
enum MqttDrainResult { drained, timedOut, failed, forced, cancelled }

/// Optional bounded graceful close; existing [MqttClient.close] is force-close.
extension MqttDrainClose on MqttClient {
  /// Drain native accepted outbound work through Sent (QoS0), PUBACK (QoS1),
  /// SUBACK/UNSUBACK, then engine Outgoing Disconnect. Not subscriber delivery.
  /// Inbound messages are discarded, not drained. First caller fixes deadline.
  Future<MqttDrainResult> drainClose({required Duration timeout}) =>
      beginDrainClose(timeout: timeout).future;

  /// Each caller can cancel its own wait without stopping the shared drain.
  /// New work is rejected synchronously once the first start is accepted.
  /// timeout must be an exact integer number of milliseconds in 1..300000.
  MqttOperation<MqttDrainResult> beginDrainClose({required Duration timeout}) {
    final micros = timeout.inMicroseconds;
    if (micros < 1000 || micros > 300000000 || micros % 1000 != 0) {
      throw ArgumentError.value(
          timeout, 'timeout', 'requires 1..300000 whole milliseconds');
    }
    final id = ++_operation;
    final completion = _DeferredResult<MqttDrainResult>();
    final cached = _drainResult;
    if (cached != null) {
      completion.complete(cached);
    } else if (_drainWaiters.length >= 32) {
      completion.completeError(const MqttException(5));
    } else {
      try {
        if (!_drainStarted) {
          _openGuard();
          if (_state == MqttConnectionState.connecting ||
              _state == MqttConnectionState.disconnecting) {
            throw const MqttException(4);
          }
          if (_handle != 0) {
            MqttClient._check(MqttClient._ffi
                .mqtt_client_drain_begin(_handle, timeout.inMilliseconds));
          }
          _drainStarted = true;
          _state = MqttConnectionState.draining;
          // Publish the waiter before callbacks may reenter close/cancel.
          _drainWaiters[id] = completion;
          if (_handle == 0) {
            _finishDrain(MqttDrainResult.drained);
          } else {
            _drainTimer = Timer.periodic(
                const Duration(milliseconds: 2), (_) => _tickDrain());
            _pump?.wake();
            _events.add(MqttClientEvent(_state));
          }
        } else {
          _drainWaiters[id] = completion;
        }
      } catch (error, stack) {
        completion.completeError(error, stack);
      }
    }
    return MqttOperation<MqttDrainResult>._(
        id,
        completion.future,
        completion.isCompleted
            ? _cancelRejectedLaunch
            : () async {
                _cancelDrainWaiter(id);
              });
  }

  void _cancelDrainWaiter(int id) {
    final waiter = _drainWaiters.remove(id);
    if (waiter != null && !waiter.isCompleted) {
      waiter.complete(MqttDrainResult.cancelled);
    }
  }

  MqttDrainResult? _nativeDrainResult() => using((arena) {
        final status = arena<Uint32>();
        MqttClient._check(
            MqttClient._ffi.mqtt_client_drain_status(_handle, status));
        return switch (status.value) {
          2 => MqttDrainResult.drained,
          3 => MqttDrainResult.timedOut,
          4 => MqttDrainResult.failed,
          5 => MqttDrainResult.forced,
          _ => null,
        };
      });

  void _tickDrain() {
    if (_closed || _drainResult != null) return;
    try {
      // Drain only retained control completions, even if streams are paused.
      // A full poll batch gets another turn before final native release.
      // Snapshot terminal status FIRST. If the actor completes between an
      // empty poll and a later status read, newly queued completions would be
      // lost on release. Once status is terminal there are no later producers.
      final result = _nativeDrainResult();
      if (_poll()) return;
      if (_closed) return;
      if (result != null) _finishDrain(result);
    } catch (_) {
      _finishDrain(MqttDrainResult.failed);
    }
  }

  void _finishDrain(MqttDrainResult result) {
    if (_drainResult != null) return;
    _drainResult = result;
    _drainTimer?.cancel();
    _drainTimer = null;
    final waiters = _drainWaiters.values.toList();
    _drainWaiters.clear();
    // Existing force-close performs the final once-only ownership release.
    // Result is committed first so reentrant close cannot rewrite it.
    close();
    for (final waiter in waiters) {
      if (!waiter.isCompleted) waiter.complete(result);
    }
  }
}
