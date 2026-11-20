// Internal allocation/ownership boundary; not re-exported by the public library.
import 'dart:convert';
import 'dart:ffi';
import 'dart:isolate';
import 'package:ffi/ffi.dart';
import 'ffi_bindings.dart';

final class NativeRuntimeOwner implements Finalizable {
  final MqttBindings ffi;
  final int id;
  final SendPort exitPort;
  static NativeFinalizer? _finalizer;
  NativeRuntimeOwner._(this.ffi, this.id, this.exitPort);
  static NativeRuntimeOwner create(MqttBindings ffi, void Function(int) check) {
    check(ffi.mqtt_dart_initialize(NativeApi.initializeApiDLData));
    final port = ffi.mqtt_dart_exit_send_port() as SendPort;
    final id = using((a) {
      final out = a<Size>();
      check(ffi.mqtt_dart_owner_new(out));
      return out.value;
    });
    final owner = NativeRuntimeOwner._(ffi, id, port);
    (_finalizer ??= NativeFinalizer(ffi.addresses.mqtt_dart_owner_finalize))
        .attach(owner, Pointer<Void>.fromAddress(id), detach: owner);
    Isolate.current.addOnExitListener(port, response: id);
    return owner;
  }

  // Owner support starts only after initialization's event-loop registration turn.
  Future<void> ready() => Future<void>.delayed(Duration.zero);
  int shutdown() {
    final code = ffi.mqtt_dart_owner_shutdown(id);
    if (code == 0) {
      _finalizer!.detach(this);
      Isolate.current.removeOnExitListener(exitPort);
    }
    return code;
  }
}

final class NativeClient implements Finalizable {
  final MqttBindings ffi;
  final int handle;
  final Pointer<MqttEvent> event;
  final Pointer<Uint8> topic, payload;
  final int token;
  bool _released = false;
  static NativeFinalizer? _finalizer;
  NativeClient._(
      this.ffi, this.handle, this.event, this.topic, this.payload, this.token);
  factory NativeClient.create(MqttBindings ffi, void Function(int) check,
      int owner, String server, String clientId, int port) {
    final handle = using((a) {
      final host = utf8.encode(server), id = utf8.encode(clientId);
      final hp = a<Uint8>(host.length),
          ip = a<Uint8>(id.length),
          out = a<Uint64>();
      hp.asTypedList(host.length).setAll(0, host);
      ip.asTypedList(id.length).setAll(0, id);
      check(ffi.mqtt_client_new(hp, host.length, ip, id.length, port, out));
      return out.value;
    });
    Pointer<MqttEvent> event = nullptr;
    Pointer<Uint8> topic = nullptr, payload = nullptr;
    var transferred = false;
    var token = 0;
    try {
      event = calloc<MqttEvent>();
      topic = calloc<Uint8>(65535);
      payload = calloc<Uint8>(65536);
      token = using((a) {
        final out = a<Size>();
        check(ffi.mqtt_dart_client_register(
            owner, handle, event, topic, payload, calloc.nativeFree, out));
        return out.value;
      });
      transferred = true;
      final client = NativeClient._(ffi, handle, event, topic, payload, token);
      (_finalizer ??= NativeFinalizer(ffi.addresses.mqtt_dart_client_finalize))
          .attach(client, Pointer<Void>.fromAddress(token),
              detach: client,
              externalSize: sizeOf<MqttEvent>() + 65535 + 65536);
      return client;
    } catch (_) {
      if (transferred) {
        ffi.mqtt_dart_client_release(token);
      } else {
        ffi.mqtt_client_close(handle);
        ffi.mqtt_client_free(handle);
        calloc.free(event);
        calloc.free(topic);
        calloc.free(payload);
      }
      rethrow;
    }
  }
  int release() {
    if (_released) return 0;
    _released = true;
    _finalizer!.detach(this);
    return ffi.mqtt_dart_client_release(token);
  }
}
