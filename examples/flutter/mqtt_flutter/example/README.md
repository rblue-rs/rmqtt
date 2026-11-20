# mqtt_flutter example

Minimal Flutter app that depends on the experimental `mqtt_flutter` FFI plugin
skeleton. It does not connect to a broker and does not ship credentials or
keys.

On Android this sample is limited to `arm64-v8a`. Build the native library
with `examples/flutter/tools/build-libs.sh android-arm64` (or let the plugin
Gradle hook invoke that script), then `flutter build apk --debug` from this
directory. A debug APK is a **buildable** artifact, not an emulator or
physical-device result.

iOS device `arm64` library and `flutter build ios --no-codesign` require
macOS and Xcode. That path is not runnable on a Linux host. `--no-codesign`
is not physical-device verification. iOS does not promise always-on
background MQTT.

Mobile Rust artifacts use `panic=abort`. A panic aborts the process.
