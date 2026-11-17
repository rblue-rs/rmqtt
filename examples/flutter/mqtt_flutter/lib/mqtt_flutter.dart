/// Experimental host-tested slice. Not yet an Android/iOS Flutter plugin.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:typed_data';
import 'package:ffi/ffi.dart';
import 'src/ffi_bindings.dart';

enum MqttQos { atMostOnce, atLeastOnce }

enum MqttProtocol { v311, v5 }

enum MqttConnectionState { disconnected, connecting, connected, closed }

enum MqttPublishStage { queued, sent, acknowledged }

class MqttException implements Exception {
  final int code;
  const MqttException(this.code);
  @override
  String toString() => 'MqttException(code: $code)';
}

class MqttConnectOptions {
  final Duration keepAlivePeriod;
  final MqttProtocol protocol;
  const MqttConnectOptions(
      {this.keepAlivePeriod = Duration.zero,
      this.protocol = MqttProtocol.v311});
}

class MqttConnectionStatus {
  final bool sessionPresent;
  const MqttConnectionStatus(this.sessionPresent);
}

class MqttSubscribeAck {
  final int operationId;
  final MqttQos qos;
  const MqttSubscribeAck(this.operationId, this.qos);
}

class MqttUnsubscribeAck {
  final int operationId;
  const MqttUnsubscribeAck(this.operationId);
}

class MqttReceivedMessage {
  final String topic;
  final Uint8List payload;
  final MqttQos qos;
  MqttReceivedMessage(this.topic, this.payload, this.qos);
}

class MqttClientEvent {
  final MqttConnectionState state;
  final MqttException? error;
  const MqttClientEvent(this.state, [this.error]);
}

class MqttPublishUpdate {
  final int operationId;
  final MqttPublishStage stage;
  final int? packetId;
  const MqttPublishUpdate(this.operationId, this.stage, this.packetId);
}

class MqttPublishTicket {
  final int operationId;
  final MqttQos qos;
  int? _packetId;
  int? get packetId => _packetId;
  final Completer<void> _queued = _completer<void>();
  final Completer<void> _sent = _completer<void>();
  final Completer<void>? _ack;
  MqttPublishTicket._(this.operationId, this.qos)
      : _ack = qos == MqttQos.atLeastOnce ? _completer<void>() : null;
  Future<void> get whenQueued => _queued.future;
  Future<void> get whenSent => _sent.future;

  /// Broker PUBACK, not end-to-end delivery. QoS 0 has no acknowledgement.
  Future<void>? get whenAcknowledged => _ack?.future;
  void _fail(Object error) {
    for (final c in [_queued, _sent, _ack]) {
      if (c != null && !c.isCompleted) c.completeError(error);
    }
  }
}

Completer<T> _completer<T>() {
  final c = Completer<T>();
  // Callers may choose to await only one phase; keep other phase errors observed.
  c.future.then<void>((_) {}, onError: (Object _, StackTrace __) {});
  return c;
}

/// Pause applies backpressure to the native poller; no unbounded Dart pause queue.
/// While paused, native bounded queues can terminate the connection with Overflow.
class _EventStream<T> {
  final Map<MultiStreamController<T>, bool> _listeners = {};
  bool _closed = false;
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

class MqttClient {
  final String server;
  final String clientId;
  final int port;
  static MqttBindings? _bindings;
  static bool _initialized = false;
  static int _live = 0;
  static MqttBindings get _ffi => _bindings ??= MqttBindings(Platform.isIOS
      ? DynamicLibrary.process()
      : DynamicLibrary.open(Platform.environment['MQTT_NATIVE_LIBRARY'] ??
          (Platform.isMacOS
              ? 'libmqtt_c.dylib'
              : Platform.isWindows
                  ? 'mqtt_c.dll'
                  : 'libmqtt_c.so')));
  static void _check(int code) {
    if (code != 0) throw MqttException(code);
  }

  static Future<void> ensureInitialized() async {
    if (!_initialized) {
      _check(_ffi.mqtt_runtime_init());
      _initialized = true;
    }
  }

  static Future<void> shutdownRuntime() async {
    if (_live != 0) {
      throw StateError('Close every client before shutdownRuntime');
    }
    if (_initialized) {
      _check(_ffi.mqtt_runtime_shutdown());
      _initialized = false;
    }
  }

  MqttClient({required this.server, required this.clientId, this.port = 1883}) {
    if (server.isEmpty || clientId.isEmpty || port < 1 || port > 65535) {
      throw ArgumentError('Invalid MQTT endpoint');
    }
  }
  final _messages = _EventStream<MqttReceivedMessage>();
  final _events = _EventStream<MqttClientEvent>();
  final _updates = _EventStream<MqttPublishUpdate>();
  Stream<MqttReceivedMessage> get messages => _messages.stream;
  Stream<MqttClientEvent> get events => _events.stream;
  Stream<MqttPublishUpdate> get publishUpdates => _updates.stream;
  MqttConnectionState _state = MqttConnectionState.disconnected;
  MqttConnectionState get connectionState => _state;
  MqttConnectionStatus? _status;
  MqttConnectionStatus? get connectionStatus => _status;
  bool _closed = false;
  bool _polling = false;
  int _handle = 0;
  int _operation = 0;
  Timer? _timer;
  Pointer<MqttEvent> _event = nullptr;
  Pointer<Uint8> _topic = nullptr;
  Pointer<Uint8> _payload = nullptr;
  final Map<int, Completer<Object?>> _operations = {};
  final Map<int, MqttPublishTicket> _tickets = {};
  void _openGuard() {
    if (_closed) throw StateError('Client is closed');
  }

  int _next() {
    _openGuard();
    return ++_operation;
  }

  Future<void> _create() async {
    if (_handle != 0) return;
    await ensureInitialized();
    _openGuard();
    using((arena) {
      final host = utf8.encode(server), id = utf8.encode(clientId);
      final hp = arena<Uint8>(host.length),
          ip = arena<Uint8>(id.length),
          out = arena<Uint64>();
      hp.asTypedList(host.length).setAll(0, host);
      ip.asTypedList(id.length).setAll(0, id);
      _check(_ffi.mqtt_client_new(hp, host.length, ip, id.length, port, out));
      _handle = out.value;
    });
    _live++;
    _event = calloc<MqttEvent>();
    _topic = calloc<Uint8>(65535);
    _payload = calloc<Uint8>(65536);
    _timer = Timer.periodic(const Duration(milliseconds: 5), (_) => _poll());
  }

  Future<Object?> _request(int op, int Function() send, Duration? timeout) {
    final c = _completer<Object?>();
    _operations[op] = c;
    try {
      _check(send());
    } catch (_) {
      _operations.remove(op);
      rethrow;
    }
    return c.future.timeout(timeout ?? const Duration(seconds: 10));
  }

  Future<MqttConnectionStatus> connect(
      {MqttConnectOptions options = const MqttConnectOptions(),
      Duration? timeout}) async {
    _openGuard();
    if (_state != MqttConnectionState.disconnected) {
      throw StateError('Already connecting/connected');
    }
    final micros = options.keepAlivePeriod.inMicroseconds;
    if (micros < 0 ||
        micros % Duration.microsecondsPerSecond != 0 ||
        options.keepAlivePeriod.inSeconds > 65535) {
      throw ArgumentError('Keepalive requires whole seconds in 0..65535');
    }
    _state = MqttConnectionState.connecting;
    _events.add(MqttClientEvent(_state));
    try {
      await _create();
      _openGuard();
      final op = _next();
      return (await _request(
          op,
          () => _ffi.mqtt_client_connect(
              _handle,
              op,
              options.protocol == MqttProtocol.v311 ? 4 : 5,
              options.keepAlivePeriod.inSeconds),
          timeout)) as MqttConnectionStatus;
    } catch (error) {
      if (!_closed) _state = MqttConnectionState.disconnected;
      if (error is TimeoutException) _releaseNative(error);
      rethrow;
    }
  }

  Future<MqttSubscribeAck> subscribe(String topic,
      {MqttQos qos = MqttQos.atLeastOnce, Duration? timeout}) async {
    final op = _next();
    return (await _request(
        op,
        () => _withTopic(
            topic,
            (ptr, len) =>
                _ffi.mqtt_client_subscribe(_handle, op, ptr, len, qos.index)),
        timeout)) as MqttSubscribeAck;
  }

  Future<MqttUnsubscribeAck> unsubscribe(String topic,
      {Duration? timeout}) async {
    final op = _next();
    return (await _request(
        op,
        () => _withTopic(topic,
            (ptr, len) => _ffi.mqtt_client_unsubscribe(_handle, op, ptr, len)),
        timeout)) as MqttUnsubscribeAck;
  }

  int _withTopic(String text, int Function(Pointer<Uint8>, int) f) =>
      using((arena) {
        final bytes = utf8.encode(text);
        if (bytes.length > 65535) throw const MqttException(1);
        final p = arena<Uint8>(bytes.length + 1);
        p.asTypedList(bytes.length).setAll(0, bytes);
        return f(p, bytes.length);
      });
  MqttPublishTicket publishMessage(String topic, Uint8List payload,
      {MqttQos qos = MqttQos.atMostOnce, bool retain = false}) {
    final op = _next(), ticket = MqttPublishTicket._(_operation, qos);
    _tickets[op] = ticket;
    try {
      if (payload.length > 65536) throw const MqttException(1);
      _check(_withTopic(
          topic,
          (p, len) => using((arena) {
                final data = arena<Uint8>(payload.length + 1);
                data.asTypedList(payload.length).setAll(0, payload);
                return _ffi.mqtt_client_publish(_handle, op, p, len, data,
                    payload.length, qos.index, retain ? 1 : 0);
              })));
    } catch (error) {
      _tickets.remove(op);
      ticket._fail(error);
    }
    return ticket;
  }

  Future<void> disconnect({Duration? timeout}) async {
    _openGuard();
    if (_state == MqttConnectionState.disconnected) return;
    final op = _next();
    await _request(op, () => _ffi.mqtt_client_disconnect(_handle, op), timeout);
  }

  void _poll() {
    if (_closed || _polling || _handle == 0) return;
    _polling = true;
    try {
      for (var i = 0; i < 64 && !_closed && _handle != 0; i++) {
        if (_messages.paused || _events.paused || _updates.paused) break;
        final code = _ffi.mqtt_client_poll(
            _handle, _event, _topic, 65535, _payload, 65536);
        if (code == 10) break;
        _check(code);
        final e = _event.ref, op = e.operation_id;
        final kind = e.kind, error = e.code;
        if (kind == 6) {
          _messages.add(MqttReceivedMessage(
              utf8.decode(_topic.asTypedList(e.topic_len)),
              Uint8List.fromList(_payload.asTypedList(e.payload_len)),
              MqttQos.values[e.qos]));
        } else if (kind == 8) {
          _state = MqttConnectionState.disconnected;
          final failure = MqttException(error);
          _failPending(failure);
          _events.add(MqttClientEvent(_state, failure));
        } else if (kind >= 3 && kind <= 5) {
          final t = _tickets[op];
          final stage = MqttPublishStage.values[kind - 3];
          if (t != null) {
            if (kind >= 4 && e.packet_id != 0) t._packetId = e.packet_id;
            final c = kind == 3
                ? t._queued
                : kind == 4
                    ? t._sent
                    : t._ack;
            if (c != null && !c.isCompleted) c.complete();
            if (kind == 5 || (kind == 4 && t.qos == MqttQos.atMostOnce)) {
              _tickets.remove(op);
            }
          }
          _updates.add(MqttPublishUpdate(
              op, stage, e.packet_id == 0 ? null : e.packet_id));
        } else {
          Object? value;
          if (kind == 1) {
            _status = MqttConnectionStatus(e.session_present != 0);
            value = _status;
            _state = MqttConnectionState.connected;
            _events.add(MqttClientEvent(_state));
          }
          if (kind == 2) value = MqttSubscribeAck(op, MqttQos.values[e.qos]);
          if (kind == 9) value = MqttUnsubscribeAck(op);
          if (kind == 7) {
            _state = MqttConnectionState.disconnected;
            _events.add(MqttClientEvent(_state));
          }
          final c = _operations.remove(op);
          if (c != null && !c.isCompleted) {
            if (error == 0) {
              c.complete(value);
            } else {
              c.completeError(MqttException(error));
            }
          }
        }
      }
    } catch (error) {
      _failPending(error);
      _releaseNative(error);
      _state = MqttConnectionState.disconnected;
    } finally {
      _polling = false;
    }
  }

  void _failPending(Object error) {
    for (final c in _operations.values) {
      if (!c.isCompleted) c.completeError(error);
    }
    _operations.clear();
    for (final t in _tickets.values) {
      t._fail(error);
    }
    _tickets.clear();
  }

  void _releaseNative(Object error) {
    _timer?.cancel();
    _timer = null;
    if (_handle != 0) {
      _check(_ffi.mqtt_client_close(_handle));
      _check(_ffi.mqtt_client_free(_handle));
      _handle = 0;
      _live--;
    }
    if (_event != nullptr) {
      calloc.free(_event);
      calloc.free(_topic);
      calloc.free(_payload);
      _event = nullptr;
      _topic = nullptr;
      _payload = nullptr;
    }
    _failPending(error);
  }

  /// Prototype force-close: stops polling and rejects pending work, not graceful drain.
  Future<void> close({Duration? timeout}) async {
    if (_closed) return;
    _closed = true;
    _state = MqttConnectionState.closed;
    _releaseNative(StateError('Client is closed'));
    _messages.close();
    _events.close();
    _updates.close();
  }
}
