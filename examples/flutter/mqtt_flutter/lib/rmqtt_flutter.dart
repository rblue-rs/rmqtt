/// Experimental host-tested slice. Not yet an Android/iOS Flutter plugin.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:typed_data';
import 'package:ffi/ffi.dart';
import 'src/ffi_bindings.dart' hide MqttReconnectPolicy;
import 'src/ffi_bindings.dart' as native show MqttReconnectPolicy;
import 'src/native_client.dart';
import 'src/event_pump.dart';

part 'src/drain_close.dart';

enum MqttQos { atMostOnce, atLeastOnce, exactlyOnce }

enum MqttProtocol { v311, v5 }

enum MqttConnectionState {
  disconnected,
  connecting,
  connected,
  closed,
  disconnecting,
  draining,
  reconnecting
}

/// Additive event classification. Existing callers can keep using [state].
enum MqttClientEventKind {
  state,
  reconnecting,
  reconnected,
  subscriptionsAligned,
  subscriptionsResubscribed
}

enum MqttPublishStage { queued, sent, acknowledged }

class MqttException implements Exception {
  final int code;
  const MqttException(this.code);
  @override
  String toString() => 'MqttException(code: $code)';
}

/// Local waiting was abandoned. No MQTT packet is withdrawn.
class MqttOperationCancelled extends StateError {
  final int operationId;
  MqttOperationCancelled(this.operationId)
      : super('Operation $operationId cancelled');
}

/// A caller-owned local wait. Cancelling never withdraws an MQTT packet.
/// Each beginX call has its own id and Future, including joined disconnects.
class MqttOperation<T> {
  final int operationId;
  final Future<T> future;
  final Future<void> Function() _cancel;
  MqttOperation._(this.operationId, this.future, this._cancel);

  /// Idempotent, including after completion/timeout/close. Only this wait ends.
  Future<void> cancel() => _cancel();
}

// Future views create standard Futures only on caller attachment, in that
// caller's zone. Returning or inspecting a view does not enqueue library jobs.
abstract class _CallerFuture<T> implements Future<T> {
  const _CallerFuture();
  Future<T> _listen();
  @override
  Stream<T> asStream() => _listen().asStream();
  @override
  Future<T> catchError(Function onError, {bool Function(Object)? test}) =>
      _listen().catchError(onError, test: test);
  @override
  Future<R> then<R>(FutureOr<R> Function(T value) onValue,
          {Function? onError}) =>
      _listen().then<R>(onValue, onError: onError);
  @override
  Future<T> timeout(Duration timeLimit, {FutureOr<T> Function()? onTimeout}) =>
      _listen().timeout(timeLimit, onTimeout: onTimeout);
  @override
  Future<T> whenComplete(FutureOr<void> Function() action) =>
      _listen().whenComplete(action);
}

// Library-owned outcomes never retain a VM/caller StackTrace backing graph.
// This is a shared finite representation, not a truncated toString conversion.
Never _libraryThrow(Object error) =>
    Error.throwWithStackTrace(error, _libraryTrace);
const _libraryTrace = _LibraryTrace();

final class _LibraryTrace implements StackTrace {
  const _LibraryTrace();
  @override
  String toString() =>
      'rmqtt_flutter: library operation outcome (origin stack omitted)';
}

final class _LibraryCompleter<T> implements Completer<T> {
  final Completer<T> _inner = Completer<T>();
  @override
  Future<T> get future => _inner.future;
  @override
  bool get isCompleted => _inner.isCompleted;
  @override
  void complete([FutureOr<T>? value]) => _inner.complete(value);
  @override
  void completeError(Object error, [StackTrace? stackTrace]) =>
      _inner.completeError(error, _libraryTrace);
}

final class _RejectedLaunchFuture<T> extends _CallerFuture<T> {
  final Object _failure;
  StackTrace get _trace => _libraryTrace;
  const _RejectedLaunchFuture(
      [this._failure = const MqttException(5), StackTrace? ignoredTrace]);
  @override
  Future<T> _listen() => Future<T>.error(_failure, _trace);
}

// Store protocol outcome without manufacturing observer jobs. Actual ordinary
// Futures/completers are created only for caller-attached listeners, in their zone.
final class _DeferredResult<T> extends _CallerFuture<T> {
  bool isCompleted = false;
  T? _value;
  Object? _failure;
  StackTrace? _trace;
  List<Completer<T>>? _listeners;
  Future<T> get future => this;
  @override
  Future<T> _listen() {
    if (isCompleted) {
      return _failure == null
          ? Future<T>.value(_value)
          : Future<T>.error(_failure!, _trace);
    }
    final listener = Completer<T>();
    (_listeners ??= []).add(listener);
    return listener.future;
  }

  void complete([T? value]) {
    if (isCompleted) _libraryThrow(StateError('Outcome already completed'));
    isCompleted = true;
    _value = value;
    final listeners = _listeners;
    _listeners = null;
    if (listeners != null) {
      for (final listener in listeners) {
        listener.complete(value);
      }
    }
  }

  void completeError(Object error, [StackTrace? stack]) {
    if (isCompleted) _libraryThrow(StateError('Outcome already completed'));
    isCompleted = true;
    _failure = error;
    _trace = _libraryTrace;
    final listeners = _listeners;
    _listeners = null;
    if (listeners != null) {
      for (final listener in listeners) {
        listener.completeError(error, _trace);
      }
    }
  }
}

Future<void> _cancelRejectedLaunch() async {}

// Do not introduce an async forwarding state machine on the full-admission path.
// Preserve Future errors (including closed-client errors) for convenience APIs.
Future<T> _operationFuture<T>(MqttOperation<T> Function() begin) {
  try {
    return begin().future;
  } catch (error, stack) {
    return _RejectedLaunchFuture<T>(error, stack);
  }
}

class _ControlWaiter {
  final bool sharedDisconnect;
  final void Function(Object, StackTrace?) fail;
  bool cancelled = false;
  _ControlWaiter(this.sharedDisconnect, this.fail);
}

class MqttWill {
  final String topic;
  final Uint8List payload;
  final MqttQos qos;
  final bool retain;
  MqttWill(
      {required this.topic,
      required Uint8List payload,
      this.qos = MqttQos.atMostOnce,
      this.retain = false})
      : payload = _snapshotWillPayload(payload);

  @override
  String toString() =>
      'MqttWill(topic: $topic, payloadLength: ${payload.length}, qos: $qos, retain: $retain)';
}

class MqttConnectOptions {
  final Duration keepAlivePeriod;
  final MqttProtocol protocol;

  /// Use TLS with certificate chain and hostname verification.
  /// There is no skip-verify / insecure switch.
  final bool useTls;

  /// MQTT in binary WebSocket frames; useTls selects WSS. Not Flutter Web.
  final bool useWebSocket;

  /// Absolute URI path only, no query/fragment/userinfo. Redacted from toString.
  final String webSocketPath;

  /// PEM-encoded CA certificates. `null` with [useTls] trusts host system
  /// roots. A provided value is trusted exclusively; system roots are not mixed
  /// in. Empty or invalid PEM is rejected rather than falling back.
  final String? caPem;
  final String? username;
  final String? password;

  /// When true, a password may be sent on plaintext TCP. Default false.
  /// TLS connections may send a password without this switch.
  final bool allowPlaintextCredentials;
  final MqttWill? will;

  /// PEM-encoded client certificate chain. Must be paired with
  /// [clientKeyPem] and [useTls].
  final String? clientCertPem;

  /// PEM-encoded client private key. Must be paired with [clientCertPem]
  /// and [useTls]. Held only in memory; redacted from [toString].
  final String? clientKeyPem;

  /// MQTT 3.1.1 Clean Session. Default true, matching prior implicit behavior.
  final bool cleanSession;
  final MqttReconnectPolicy reconnectPolicy;
  const MqttConnectOptions(
      {this.keepAlivePeriod = Duration.zero,
      this.protocol = MqttProtocol.v311,
      this.useTls = false,
      this.useWebSocket = false,
      this.webSocketPath = "/mqtt",
      this.caPem,
      this.username,
      this.password,
      this.allowPlaintextCredentials = false,
      this.will,
      this.clientCertPem,
      this.clientKeyPem,
      this.cleanSession = true,
      this.reconnectPolicy = const MqttReconnectPolicy()});

  @override
  String toString() =>
      'MqttConnectOptions(keepAlivePeriod: $keepAlivePeriod, protocol: $protocol, useTls: $useTls, useWebSocket: $useWebSocket, webSocketPath: <redacted>, caPem: ${caPem == null ? 'null' : '<redacted>'}, username: $username, password: ${password == null ? 'null' : '<redacted>'}, allowPlaintextCredentials: $allowPlaintextCredentials, will: $will, clientCertPem: ${clientCertPem == null ? 'null' : '<redacted>'}, clientKeyPem: ${clientKeyPem == null ? 'null' : '<redacted>'}, cleanSession: $cleanSession, reconnectPolicy: $reconnectPolicy)';
}

/// Automatic reconnect policy. Disabled by default. Enabling requires a finite
/// [maxAttempts] ceiling. Delays use equal jitter internally.
class MqttReconnectPolicy {
  final bool enabled;
  final int maxAttempts;
  final Duration baseDelay;
  final Duration maxDelay;
  const MqttReconnectPolicy(
      {this.enabled = false,
      this.maxAttempts = 8,
      this.baseDelay = const Duration(seconds: 1),
      this.maxDelay = const Duration(seconds: 30)});

  @override
  String toString() =>
      'MqttReconnectPolicy(enabled: $enabled, maxAttempts: $maxAttempts, baseDelay: $baseDelay, maxDelay: $maxDelay)';
}

// Capture runs synchronously after admission. No source/capture callback escapes.
String _ownedText(String source, int limit) =>
    utf8.decode(_boundedUtf8(source, limit));
Duration? _ownedDuration(Duration? source) =>
    source == null ? null : Duration(microseconds: source.inMicroseconds);

final class _OwnedConnectInput {
  final MqttConnectOptions options;
  final Duration? timeout;
  _OwnedConnectInput(this.options, this.timeout);
  factory _OwnedConnectInput.capture(
      MqttConnectOptions source, Duration? timeout) {
    final keepAlive = _ownedDuration(source.keepAlivePeriod)!;
    final protocol = source.protocol,
        tls = source.useTls,
        ws = source.useWebSocket;
    final path = source.webSocketPath,
        ca = source.caPem,
        user = source.username,
        password = source.password;
    final cert = source.clientCertPem, key = source.clientKeyPem;
    final allow = source.allowPlaintextCredentials, clean = source.cleanSession;
    final sourceWill = source.will, sourcePolicy = source.reconnectPolicy;
    if (ca != null && !tls) {
      _libraryThrow(ArgumentError('Custom CA requires useTls'));
    }
    if ((cert != null || key != null) && !tls) {
      _libraryThrow(ArgumentError('Client certificate requires useTls'));
    }
    final policy = MqttReconnectPolicy(
        enabled: sourcePolicy.enabled,
        maxAttempts: sourcePolicy.maxAttempts,
        baseDelay: _ownedDuration(sourcePolicy.baseDelay)!,
        maxDelay: _ownedDuration(sourcePolicy.maxDelay)!);
    final will = sourceWill == null
        ? null
        : MqttWill(
            topic: _ownedText(sourceWill.topic, 65535),
            payload: _snapshotWillPayload(sourceWill.payload),
            qos: sourceWill.qos,
            retain: sourceWill.retain);
    return _OwnedConnectInput(
        MqttConnectOptions(
            keepAlivePeriod: keepAlive,
            protocol: protocol,
            useTls: tls,
            useWebSocket: ws,
            webSocketPath: _ownedText(path, 2048),
            caPem: ca == null ? null : _ownedText(ca, 65536),
            username: user == null ? null : _ownedText(user, 65535),
            password: password == null ? null : _ownedText(password, 65535),
            clientCertPem: cert == null ? null : _ownedText(cert, 65536),
            clientKeyPem: key == null ? null : _ownedText(key, 65536),
            allowPlaintextCredentials: allow,
            cleanSession: clean,
            will: will,
            reconnectPolicy: policy),
        _ownedDuration(timeout));
  }
}

final class _OwnedTopicInput {
  final String topic;
  final MqttQos qos;
  final Duration? timeout;
  _OwnedTopicInput(String topic, this.qos, Duration? timeout)
      : topic = _ownedText(topic, 65535),
        timeout = _ownedDuration(timeout);
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
  final MqttClientEventKind kind;
  final MqttConnectionStatus? status;
  const MqttClientEvent(this.state, [this.error])
      : kind = MqttClientEventKind.state,
        status = null;
  const MqttClientEvent.withKind(this.state, this.kind,
      {this.error, this.status});
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
  final _DeferredResult<void> _queued = _DeferredResult<void>();
  final _DeferredResult<void> _sent = _DeferredResult<void>();
  final _DeferredResult<void>? _ack;
  MqttPublishTicket._(this.operationId, this.qos)
      : _ack = qos != MqttQos.atMostOnce ? _DeferredResult<void>() : null;
  Future<void> get whenQueued => _queued.future;
  Future<void> get whenSent => _sent.future;

  /// Broker PUBACK (QoS 1) or PUBCOMP (QoS 2), not end-to-end delivery.
  /// QoS 0 has no acknowledgement.
  Future<void>? get whenAcknowledged => _ack?.future;
  void _fail(Object error) {
    for (final c in [_queued, _sent, _ack]) {
      if (c != null && !c.isCompleted) c.completeError(error);
    }
  }
}

Completer<T> _completer<T>() {
  final c = _LibraryCompleter<T>();
  // Only admitted control/request/shared-disconnect paths use eager observers.
  // Rejected handles and publish/drain phase cells must stay passive.
  c.future.then<void>((_) {}, onError: (Object _, StackTrace __) {});
  return c;
}

class MqttClient implements Finalizable {
  final String server;
  final String clientId;
  final int port;
  static MqttBindings? _bindings;
  static NativeRuntimeOwner? _runtimeOwner;
  static Future<void>? _initializing;
  static MqttBindings get _ffi => _bindings ??= MqttBindings(Platform.isIOS
      ? DynamicLibrary.process()
      : DynamicLibrary.open(Platform.environment['MQTT_NATIVE_LIBRARY'] ??
          (Platform.isMacOS
              ? 'libmqtt_c.dylib'
              : Platform.isWindows
                  ? 'mqtt_c.dll'
                  : 'libmqtt_c.so')));
  static void _check(int code) {
    if (code != 0) _libraryThrow(MqttException(code));
  }

  static Future<void> ensureInitialized() {
    final pending = _initializing;
    if (pending != null) return pending;
    if (_runtimeOwner != null) return Future.value();
    final future = () async {
      final owner = NativeRuntimeOwner.create(_ffi, _check);
      _runtimeOwner = owner;
      await owner.ready();
    }();
    _initializing = future;
    future.then<void>((_) {
      _initializing = null;
    }, onError: (Object e, StackTrace s) {
      _initializing = null;
    });
    return future;
  }

  static Future<void> shutdownRuntime() async {
    final pending = _initializing;
    if (pending != null) await pending;
    final owner = _runtimeOwner;
    if (owner == null) return;
    final code = owner.shutdown();
    if (code == 4) {
      _libraryThrow(StateError('Close every client before shutdownRuntime'));
    }
    _check(code);
    _runtimeOwner = null;
  }

  MqttClient({required this.server, required this.clientId, this.port = 1883}) {
    if (server.isEmpty || clientId.isEmpty || port < 1 || port > 65535) {
      _libraryThrow(ArgumentError('Invalid MQTT endpoint'));
    }
    // UTF8 uses at least one byte per UTF16 code unit (also for replacements).
    // Reject before retaining an unusable endpoint or encoding it later.
    if (server.length > 65535 || clientId.length > 65535) {
      _libraryThrow(const MqttException(1));
    }
    for (final stream in [_messages, _events, _updates]) {
      stream.onResume = () => _pump?.wake();
    }
  }
  final _messages = EventStream<MqttReceivedMessage>();
  final _events = EventStream<MqttClientEvent>();
  final _updates = EventStream<MqttPublishUpdate>();
  Stream<MqttReceivedMessage> get messages => _messages.stream;
  Stream<MqttClientEvent> get events => _events.stream;
  Stream<MqttPublishUpdate> get publishUpdates => _updates.stream;
  MqttConnectionState _state = MqttConnectionState.disconnected;
  MqttConnectionState get connectionState => _state;
  MqttConnectionStatus? _status;
  MqttConnectionStatus? get connectionStatus => _status;
  bool _drainStarted = false;
  bool _reconnectEnabled = false;
  MqttDrainResult? _drainResult;
  Timer? _drainTimer;
  final Map<int, _DeferredResult<MqttDrainResult>> _drainWaiters = {};
  bool _closed = false;
  bool _polling = false;
  NativeClient? _native;
  int get _handle => _native?.handle ?? 0;
  int _operation = 0;
  EventPump? _pump;
  late final bool Function() _pollCallback = _poll;
  int _generation = 0;
  Future<void>? _disconnectFuture;
  int? _disconnectPhysicalId;
  // A launch keeps its slot until its ORIGINAL continuation settles.
  int _activeLaunches = 0;
  static const int _launchLimit = 32;
  final Map<int, Timer> _timeouts = {};
  final Map<int, _ControlWaiter> _controlWaiters = {};
  final Map<int, _ControlWaiter> _starting = {};
  Pointer<MqttEvent> get _event => _native?.event ?? nullptr;
  Pointer<Uint8> get _topic => _native?.topic ?? nullptr;
  Pointer<Uint8> get _payload => _native?.payload ?? nullptr;
  final Map<int, Completer<Object?>> _operations = {};
  final Map<int, MqttPublishTicket> _tickets = {};
  void _openGuard() {
    if (_closed || _drainStarted) {
      _libraryThrow(StateError('Client is closed or draining'));
    }
  }

  int _next() {
    _openGuard();
    return ++_operation;
  }

  Future<void> _create(int generation) async {
    if (_handle != 0) return;
    await ensureInitialized();
    _openGuard();
    if (generation != _generation) {
      _libraryThrow(StateError('Connection attempt stopped'));
    }
    _native = NativeClient.create(
        _ffi, _check, _runtimeOwner!.id, server, clientId, port);
    _pump = EventPump(_pollCallback);
  }

  MqttOperation<T> _prepare<I, T>(
      I Function() capture, Future<T> Function(int, I) run,
      {bool sharedDisconnect = false}) {
    final op = _next();
    if (_activeLaunches >= _launchLimit) {
      return MqttOperation<T>._(
          op, _RejectedLaunchFuture<T>(), _cancelRejectedLaunch);
    }
    _activeLaunches++;
    try {
      final input = capture();
      _openGuard(); // A caller getter may have synchronously closed/reentered.
      return _launchPrepared(op, input, run, sharedDisconnect);
    } catch (error) {
      _activeLaunches--;
      return MqttOperation<T>._(
          op, _RejectedLaunchFuture<T>(error), _cancelRejectedLaunch);
    }
  }

  // A distinct lexical context: it cannot capture the original capture callback.
  MqttOperation<T> _launchPrepared<I, T>(
      int op, I input, Future<T> Function(int, I) run, bool sharedDisconnect) {
    final completion = _completer<T>();
    final waiter = _ControlWaiter(sharedDisconnect, (error, stack) {
      if (!completion.isCompleted) completion.completeError(error, stack);
    });
    // Register before launching: synchronous listeners can close/reenter.
    _controlWaiters[op] = waiter;
    if (!sharedDisconnect) _starting[op] = waiter;
    Future<T>.sync(() => run(op, input)).then<void>((value) {
      _activeLaunches--;
      _controlWaiters.remove(op);
      _starting.remove(op);
      if (!completion.isCompleted) completion.complete(value);
    }, onError: (Object error, StackTrace stack) {
      _activeLaunches--;
      _controlWaiters.remove(op);
      _starting.remove(op);
      waiter.fail(error, stack);
    });
    return MqttOperation<T>._(op, completion.future, () async {
      if (_closed || !_controlWaiters.containsKey(op)) return;
      await cancel(op);
    });
  }

  // A native result/deadline wins before a later synchronous listener can close.
  // All registered disconnect callers commit with their shared attempt.
  void _commitControl(int op) {
    if (_disconnectPhysicalId == op) {
      _controlWaiters.removeWhere((_, waiter) => waiter.sharedDisconnect);
    } else {
      _controlWaiters.remove(op);
    }
  }

  Future<Object?> _request(int op, int Function() send, Duration? timeout,
      {void Function(Object)? onTimeout}) {
    final c = _completer<Object?>();
    final starting = _starting.remove(op);
    _operations[op] = c;
    try {
      _check(send());
      _pump?.wake();
      if (starting?.cancelled ?? false) {
        _operations.remove(op);
        c.completeError(MqttOperationCancelled(op));
        return c.future;
      }
      _timeouts[op] = Timer(timeout ?? const Duration(seconds: 10), () {
        _timeouts.remove(op);
        if (_operations.remove(op) != c || c.isCompleted) return;
        final error = TimeoutException('Operation $op timed out', timeout);
        _commitControl(op);
        c.completeError(error);
        onTimeout?.call(error);
      });
    } catch (error) {
      _operations.remove(op);
      _commitControl(op);
      c.completeError(error);
    }
    return c.future;
  }

  /// Abandon local waiting only. Late protocol progress can still be observed.
  /// Unknown/already terminal ids are harmless; a closed client rejects cancel.
  Future<void> cancel(int operationId) async {
    if (_drainWaiters.containsKey(operationId)) {
      _cancelDrainWaiter(operationId);
      return;
    }
    // Existing accepted operations remain locally cancellable during drain.
    if (_closed) _libraryThrow(StateError('Client is closed'));
    if (operationId <= 0) {
      _libraryThrow(ArgumentError.value(operationId, 'operationId'));
    }
    final error = MqttOperationCancelled(operationId);
    final waiter = _controlWaiters.remove(operationId);
    if (waiter != null) {
      waiter.cancelled = true;
      waiter.fail(error, null);
      // The first caller's deadline/transport belongs to the shared attempt.
      if (waiter.sharedDisconnect) return;
    }
    // The physical attempt can still use the first caller's id after that
    // caller has left. Repeated cancel(id) must not reach its native waiter.
    if (_disconnectPhysicalId == operationId) return;
    _timeouts.remove(operationId)?.cancel();
    final c = _operations.remove(operationId);
    if (c != null && !c.isCompleted) c.completeError(error);
    _tickets.remove(operationId)?._fail(error);
  }

  Future<MqttConnectionStatus> connect(
          {MqttConnectOptions options = const MqttConnectOptions(),
          Duration? timeout}) =>
      _operationFuture(() => beginConnect(options: options, timeout: timeout));

  MqttOperation<MqttConnectionStatus> beginConnect(
          {MqttConnectOptions options = const MqttConnectOptions(),
          Duration? timeout}) =>
      _prepare(
          () => _OwnedConnectInput.capture(options, timeout), _connectOwned);

  Future<MqttConnectionStatus> _connectOwned(
          int op, _OwnedConnectInput input) =>
      _connect(op, input.options, input.timeout);

  Future<MqttConnectionStatus> _connect(
      int op, MqttConnectOptions options, Duration? timeout) async {
    _openGuard();
    if (_state != MqttConnectionState.disconnected) {
      _libraryThrow(StateError('Already connecting/connected'));
    }
    final micros = options.keepAlivePeriod.inMicroseconds;
    if (micros < 0 ||
        micros % Duration.microsecondsPerSecond != 0 ||
        options.keepAlivePeriod.inSeconds > 65535) {
      _libraryThrow(
          ArgumentError('Keepalive requires whole seconds in 0..65535'));
    }
    if (options.caPem != null && !options.useTls) {
      _libraryThrow(ArgumentError('Custom CA requires useTls'));
    }
    if ((options.clientCertPem != null || options.clientKeyPem != null) &&
        !options.useTls) {
      _libraryThrow(ArgumentError('Client certificate requires useTls'));
    }
    if (options.useTls && options.caPem != null) {
      final bytes = _boundedUtf8(options.caPem!, 65536);
      if (bytes.isEmpty || bytes.contains(0) || bytes.length > 65536) {
        _libraryThrow(const MqttException(1));
      }
    }
    final hasClientCert = options.clientCertPem != null;
    final hasClientKey = options.clientKeyPem != null;
    if (hasClientCert != hasClientKey) {
      _libraryThrow(const MqttException(21));
    }
    if (hasClientCert) {
      final certBytes = _boundedUtf8(options.clientCertPem!, 65536);
      final keyBytes = _boundedUtf8(options.clientKeyPem!, 65536);
      if (certBytes.isEmpty ||
          certBytes.contains(0) ||
          certBytes.length > 65536 ||
          keyBytes.isEmpty ||
          keyBytes.contains(0) ||
          keyBytes.length > 65536) {
        _libraryThrow(const MqttException(1));
      }
    }
    if (options.password != null && options.username == null) {
      _libraryThrow(const MqttException(1));
    }
    if (options.password != null &&
        !options.useTls &&
        !options.allowPlaintextCredentials) {
      _libraryThrow(const MqttException(18));
    }
    if (options.username != null &&
        (options.username!.isEmpty || options.username!.contains('\u0000'))) {
      _libraryThrow(const MqttException(1));
    }
    if (options.password != null && options.password!.contains('\u0000')) {
      _libraryThrow(const MqttException(1));
    }
    final will = options.will;
    if (will != null) {
      if (will.topic.isEmpty ||
          will.topic.contains('\u0000') ||
          will.topic.contains('#') ||
          will.topic.contains('+') ||
          will.payload.length > 65535) {
        _libraryThrow(const MqttException(1));
      }
    }
    final policy = options.reconnectPolicy;
    if (policy.enabled) {
      final baseUs = policy.baseDelay.inMicroseconds;
      final maxUs = policy.maxDelay.inMicroseconds;
      if (policy.maxAttempts < 1 ||
          policy.maxAttempts > 10000 ||
          baseUs < 1000 ||
          maxUs < baseUs ||
          policy.maxDelay > const Duration(seconds: 300) ||
          baseUs % 1000 != 0 ||
          maxUs % 1000 != 0) {
        _libraryThrow(ArgumentError(
            'Reconnect policy requires a finite attempt ceiling and whole-millisecond delays'));
      }
    }
    final generation = _generation;
    _reconnectEnabled = policy.enabled;
    _state = MqttConnectionState.connecting;
    _events.add(MqttClientEvent(_state));
    try {
      await _create(generation);
      _openGuard();
      return (await _request(
          op,
          () => using((arena) {
                Pointer<Uint8> writeBytes(List<int> bytes) {
                  if (bytes.isEmpty) {
                    return nullptr;
                  }
                  final ptr = arena<Uint8>(bytes.length);
                  ptr.asTypedList(bytes.length).setAll(0, bytes);
                  return ptr;
                }

                Pointer<Uint8> pemPtr = nullptr;
                var pemLen = 0;
                if (options.useTls && options.caPem != null) {
                  final bytes = _boundedUtf8(options.caPem!, 65536);
                  pemPtr = writeBytes(bytes);
                  pemLen = bytes.length;
                }
                final usernameBytes = options.username == null
                    ? null
                    : _boundedUtf8(options.username!, 65535);
                final passwordBytes = options.password == null
                    ? null
                    : _boundedUtf8(options.password!, 65535);
                final clientCertBytes = options.clientCertPem == null
                    ? null
                    : _boundedUtf8(options.clientCertPem!, 65536);
                final clientKeyBytes = options.clientKeyPem == null
                    ? null
                    : _boundedUtf8(options.clientKeyPem!, 65536);
                final will = options.will;
                final willTopic =
                    will == null ? null : _boundedUtf8(will.topic, 65535);
                final passwordPtr =
                    passwordBytes == null ? nullptr : writeBytes(passwordBytes);
                final clientCertPtr = clientCertBytes == null
                    ? nullptr
                    : writeBytes(clientCertBytes);
                final clientKeyPtr = clientKeyBytes == null
                    ? nullptr
                    : writeBytes(clientKeyBytes);
                try {
                  final policyPtr = arena<native.MqttReconnectPolicy>();
                  policyPtr.ref
                    ..enabled = options.reconnectPolicy.enabled ? 1 : 0
                    ..max_attempts = options.reconnectPolicy.maxAttempts
                    ..base_delay_ms =
                        options.reconnectPolicy.baseDelay.inMilliseconds
                    ..max_delay_ms =
                        options.reconnectPolicy.maxDelay.inMilliseconds;
                  if (options.webSocketPath.isEmpty ||
                      (!options.useWebSocket &&
                          options.webSocketPath != '/mqtt')) {
                    return 1;
                  }
                  final path = options.useWebSocket
                      ? _boundedUtf8(options.webSocketPath, 2048)
                      : <int>[];
                  final wsCode = _ffi.mqtt_client_set_websocket(
                      _handle,
                      options.useWebSocket ? 1 : 0,
                      path.isEmpty ? nullptr : writeBytes(path),
                      path.length);
                  if (wsCode != 0) return wsCode;
                  return _ffi.mqtt_client_connect_ex(
                      _handle,
                      op,
                      options.protocol == MqttProtocol.v311 ? 4 : 5,
                      options.keepAlivePeriod.inSeconds,
                      options.useTls ? 1 : 0,
                      pemPtr,
                      pemLen,
                      usernameBytes == null
                          ? nullptr
                          : writeBytes(usernameBytes),
                      usernameBytes?.length ?? 0,
                      passwordPtr,
                      passwordBytes?.length ?? 0,
                      options.allowPlaintextCredentials ? 1 : 0,
                      willTopic == null ? nullptr : writeBytes(willTopic),
                      willTopic?.length ?? 0,
                      will == null ? nullptr : writeBytes(will.payload),
                      will?.payload.length ?? 0,
                      will?.qos.index ?? 0,
                      (will?.retain ?? false) ? 1 : 0,
                      clientCertPtr,
                      clientCertBytes?.length ?? 0,
                      clientKeyPtr,
                      clientKeyBytes?.length ?? 0,
                      options.cleanSession ? 1 : 0,
                      policyPtr);
                } finally {
                  void zero(Pointer<Uint8> ptr, List<int>? bytes) {
                    if (ptr != nullptr && bytes != null && bytes.isNotEmpty) {
                      ptr
                          .asTypedList(bytes.length)
                          .fillRange(0, bytes.length, 0);
                    }
                  }

                  zero(clientKeyPtr, clientKeyBytes);
                  zero(clientCertPtr, clientCertBytes);
                  zero(passwordPtr, passwordBytes);
                }
              }),
          timeout)) as MqttConnectionStatus;
    } catch (error) {
      if (error is TimeoutException && generation == _generation) {
        _releaseNative(error);
      }
      if (!_closed &&
          error is! MqttOperationCancelled &&
          generation == _generation) {
        _state = MqttConnectionState.disconnected;
      }
      if (!_closed && error is TimeoutException) {
        _state = MqttConnectionState.disconnected;
      }
      _libraryThrow(error);
    }
  }

  Future<MqttSubscribeAck> subscribe(String topic,
          {MqttQos qos = MqttQos.atLeastOnce, Duration? timeout}) =>
      _operationFuture(() => beginSubscribe(topic, qos: qos, timeout: timeout));

  MqttOperation<MqttSubscribeAck> beginSubscribe(String topic,
          {MqttQos qos = MqttQos.atLeastOnce, Duration? timeout}) =>
      _prepare(() => _OwnedTopicInput(topic, qos, timeout), _subscribeOwned);

  Future<MqttSubscribeAck> _subscribeOwned(int op, _OwnedTopicInput input) =>
      _subscribe(op, input.topic, input.qos, input.timeout);
  Future<MqttSubscribeAck> _subscribe(
      int op, String topic, MqttQos qos, Duration? timeout) async {
    return (await _request(
        op,
        () => _withTopic(
            topic,
            (ptr, len) =>
                _ffi.mqtt_client_subscribe(_handle, op, ptr, len, qos.index)),
        timeout)) as MqttSubscribeAck;
  }

  Future<MqttUnsubscribeAck> unsubscribe(String topic, {Duration? timeout}) =>
      _operationFuture(() => beginUnsubscribe(topic, timeout: timeout));

  MqttOperation<MqttUnsubscribeAck> beginUnsubscribe(String topic,
          {Duration? timeout}) =>
      _prepare(() => _OwnedTopicInput(topic, MqttQos.atMostOnce, timeout),
          _unsubscribeOwned);

  Future<MqttUnsubscribeAck> _unsubscribeOwned(
          int op, _OwnedTopicInput input) =>
      _unsubscribe(op, input.topic, input.timeout);
  Future<MqttUnsubscribeAck> _unsubscribe(
      int op, String topic, Duration? timeout) async {
    return (await _request(
        op,
        () => _withTopic(topic,
            (ptr, len) => _ffi.mqtt_client_unsubscribe(_handle, op, ptr, len)),
        timeout)) as MqttUnsubscribeAck;
  }

  int _withTopic(String text, int Function(Pointer<Uint8>, int) f) =>
      using((arena) {
        // Cheap bound before UTF8 allocates; actual bytes still decide below.
        if (text.length > 65535) _libraryThrow(const MqttException(1));
        final bytes = utf8.encode(text);
        if (bytes.length > 65535) _libraryThrow(const MqttException(1));
        final p = arena<Uint8>(bytes.length + 1);
        p.asTypedList(bytes.length).setAll(0, bytes);
        return f(p, bytes.length);
      });
  MqttPublishTicket publishMessage(String topic, Uint8List payload,
      {MqttQos qos = MqttQos.atMostOnce, bool retain = false}) {
    final op = _next(), ticket = MqttPublishTicket._(_operation, qos);
    _tickets[op] = ticket;
    try {
      if (payload.length > 65536) _libraryThrow(const MqttException(1));
      _check(_withTopic(
          topic,
          (p, len) => using((arena) {
                final data = arena<Uint8>(payload.length + 1);
                data.asTypedList(payload.length).setAll(0, payload);
                return _ffi.mqtt_client_publish(_handle, op, p, len, data,
                    payload.length, qos.index, retain ? 1 : 0);
              })));
      _pump?.wake();
    } catch (error) {
      _tickets.remove(op);
      ticket._fail(error);
    }
    return ticket;
  }

  /// Concurrent callers share one attempt and its first caller's deadline.
  /// Timeout force-stops the transport, but leaves this Dart client reusable.
  Future<void> disconnect({Duration? timeout}) =>
      _operationFuture(() => beginDisconnect(timeout: timeout));

  /// Each caller can cancel its own wait without cancelling other callers.
  /// The first attempt's deadline remains active even if all callers cancel.
  MqttOperation<void> beginDisconnect({Duration? timeout}) =>
      _prepare(() => _ownedDuration(timeout), _joinDisconnect,
          sharedDisconnect: true);

  Future<void> _joinDisconnect(int op, Duration? timeout) async {
    _openGuard();
    final existing = _disconnectFuture;
    if (existing != null) return existing;
    if (_state == MqttConnectionState.disconnected) return;
    final completion = _completer<void>();
    final attempt = completion.future;
    // Publish the shared attempt before any synchronous listener can reenter.
    _disconnectFuture = attempt;
    _disconnectPhysicalId = op;
    try {
      await _disconnect(op, timeout);
      completion.complete();
    } catch (error, stack) {
      completion.completeError(error, stack);
    } finally {
      if (identical(_disconnectFuture, attempt)) {
        _disconnectFuture = null;
        _disconnectPhysicalId = null;
      }
    }
    return attempt;
  }

  Future<void> _disconnect(int op, Duration? timeout) async {
    if (_state == MqttConnectionState.connecting) {
      _releaseNative(StateError('Connection attempt disconnected'),
          preserveDisconnectWaiters: true);
      _state = MqttConnectionState.disconnected;
      _events.add(MqttClientEvent(_state));
      return;
    }
    _state = MqttConnectionState.disconnecting;
    // Register before emitting to support reentrant close/cancel listeners.
    final pending =
        _request(op, () => _ffi.mqtt_client_disconnect(_handle, op), timeout,
            onTimeout: (error) {
      _releaseNative(error);
      if (!_closed) {
        _state = MqttConnectionState.disconnected;
        _events.add(MqttClientEvent(_state));
      }
    });
    _events.add(MqttClientEvent(_state));
    try {
      await pending;
    } catch (error) {
      if (!_closed &&
          error is! MqttOperationCancelled &&
          _state == MqttConnectionState.disconnecting) {
        _releaseNative(error);
        _state = MqttConnectionState.disconnected;
        _events.add(MqttClientEvent(_state));
      }
      _libraryThrow(error);
    }
  }

  bool _poll() {
    if (_closed || _polling || _handle == 0) return false;
    var work = false;
    _polling = true;
    try {
      for (var i = 0; i < 64 && !_closed && _handle != 0; i++) {
        if (!_drainStarted &&
            (_messages.paused || _events.paused || _updates.paused)) {
          break;
        }
        final code = _ffi.mqtt_client_poll(
            _handle, _event, _topic, 65535, _payload, 65536);
        if (code == 10) break;
        _check(code);
        work = true;
        final e = _event.ref, op = e.operation_id;
        final kind = e.kind, error = e.code;
        if (kind == 6) {
          if (_drainStarted) continue;
          _messages.add(MqttReceivedMessage(
              utf8.decode(_topic.asTypedList(e.topic_len)),
              Uint8List.fromList(_payload.asTypedList(e.payload_len)),
              MqttQos.values[e.qos]));
        } else if (kind == 8) {
          _state = MqttConnectionState.disconnected;
          final failure = MqttException(error);
          _failPending(failure);
          _events.add(MqttClientEvent(_state, failure));
        } else if (kind == 10) {
          _state = MqttConnectionState.reconnecting;
          _events.add(MqttClientEvent.withKind(
              _state, MqttClientEventKind.reconnecting));
        } else if (kind == 11) {
          _status = MqttConnectionStatus(e.session_present != 0);
          _state = MqttConnectionState.connected;
          _events.add(MqttClientEvent.withKind(
              _state, MqttClientEventKind.reconnected,
              status: _status));
        } else if (kind == 12) {
          _events.add(MqttClientEvent.withKind(
              _state, MqttClientEventKind.subscriptionsAligned));
        } else if (kind == 13) {
          _events.add(MqttClientEvent.withKind(
              _state, MqttClientEventKind.subscriptionsResubscribed));
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
          }
          if (kind == 2) value = MqttSubscribeAck(op, MqttQos.values[e.qos]);
          if (kind == 9) value = MqttUnsubscribeAck(op);
          final c = _operations.remove(op);
          _commitControl(op);
          _timeouts.remove(op)?.cancel();
          if (kind == 7) {
            final from = _state;
            _state = MqttConnectionState.disconnected;
            final transient =
                _reconnectEnabled && from == MqttConnectionState.connected;
            if (!transient) {
              _failPending(StateError('Client disconnected'),
                  preserveDisconnectWaiters: true);
            }
          }
          if (c != null && !c.isCompleted) {
            if (error == 0) {
              c.complete(value);
            } else {
              c.completeError(MqttException(error));
            }
          }
          if (kind == 1 || kind == 7) _events.add(MqttClientEvent(_state));
        }
      }
    } catch (error) {
      if (_drainStarted) {
        _finishDrain(MqttDrainResult.failed);
      } else {
        _failPending(error);
        _releaseNative(error);
        _state = MqttConnectionState.disconnected;
      }
    } finally {
      _polling = false;
    }
    return work;
  }

  void _failPending(Object error, {bool preserveDisconnectWaiters = false}) {
    for (final entry in _controlWaiters.entries.toList()) {
      if (preserveDisconnectWaiters && entry.value.sharedDisconnect) continue;
      _controlWaiters.remove(entry.key);
      entry.value.fail(error, null);
    }
    for (final t in _timeouts.values) {
      t.cancel();
    }
    _timeouts.clear();
    for (final c in _operations.values) {
      if (!c.isCompleted) c.completeError(error);
    }
    _operations.clear();
    for (final t in _tickets.values) {
      t._fail(error);
    }
    _tickets.clear();
  }

  void _releaseNative(Object error, {bool preserveDisconnectWaiters = false}) {
    _generation++;
    _pump?.close();
    _pump = null;
    final native = _native;
    if (native != null) {
      _check(native.release());
      _native = null;
    }
    _failPending(error, preserveDisconnectWaiters: preserveDisconnectWaiters);
  }

  /// Prototype force-close: stops polling and rejects pending work, not graceful drain.
  Future<void> close() async {
    if (_closed) return;
    if (_drainStarted && _drainResult == null) {
      // Preserve an already committed native result if force-close races it.
      MqttDrainResult? result;
      try {
        if (_handle != 0) result = _nativeDrainResult();
      } catch (_) {}
      _finishDrain(result ?? MqttDrainResult.forced);
      return;
    }
    _closed = true;
    _state = MqttConnectionState.closed;
    _releaseNative(StateError('Client is closed'));
    _messages.close();
    _events.close();
    _updates.close();
  }
}

// Every sibling connect text must cross this boundary before arena copying.
// Keep native validation (syntax, pairing, trust, protocol) authoritative.
Uint8List _boundedUtf8(String text, int maxBytes) {
  if (text.length > maxBytes) _libraryThrow(const MqttException(1));
  final encoded = utf8.encode(text);
  if (encoded.length > maxBytes) _libraryThrow(const MqttException(1));
  return encoded;
}

// Detach a tiny caller view from its potentially arbitrary-sized backing buffer.
// Validate before copying; ordinary publish payloads still allow65536 bytes.
Uint8List _snapshotWillPayload(Uint8List payload) {
  if (payload.length > 65535) _libraryThrow(const MqttException(1));
  return Uint8List.fromList(payload);
}
