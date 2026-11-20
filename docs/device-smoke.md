# Mobile validation gates

Evidence is recorded by layer. A Linux-host test, a cross-compiled library, an
unsigned iOS build, a debug APK, or a simulator/emulator run is **not** a
physical-device TLS result. iOS does not promise an always-on background MQTT
connection.

Mobile Rust artifacts are built with `panic=abort`. On those targets
`catch_unwind` cannot convert a panic into `MQTT_ERR_INTERNAL`; the process
aborts. Host tests that exercise unwind behavior apply only to
`panic=unwind` Linux builds.

## Android `arm64-v8a` (`aarch64-linux-android`)

| Layer | Status |
| --- | --- |
| Rust cross-build of `mqtt-c` (`panic=abort`) | Recorded in the local build log when the NDK is available; otherwise NOT RUN |
| Flutter debug APK (`flutter build apk --debug --target-platform android-arm64`, ABI `arm64-v8a` only) | Recorded in the local build log when Flutter and the Android NDK/SDK are available; otherwise NOT RUN |
| Emulator run | NOT RUN |
| Physical-device TLS, subscribe, QoS 1 publish/receive | NOT RUN (later stage) |

Default packaging ABI is `arm64-v8a` only. Other Android ABIs are not part of
this skeleton. A successful APK build does not imply the host system TLS roots,
or a custom CA, were exercised on a device.

Reproduce the cross-build and APK (when the Android NDK and Flutter 3.50.x are
installed):

```sh
export ANDROID_SDK_ROOT=/path/to/android-sdk
export ANDROID_NDK_HOME="$ANDROID_SDK_ROOT/ndk/28.2.13676358"
examples/flutter/tools/build-libs.sh android-arm64
cd examples/flutter/mqtt_flutter/example
flutter build apk --debug --target-platform android-arm64
```

The plugin Gradle hook can invoke `build-libs.sh` if `libmqtt_c.so` is missing.
Do not commit the `.so`. Record the Rust, NDK, Flutter, and JDK versions and
the target triple with any buildable claim.

Recorded on 2026-11-21 (Asia/Shanghai) on a Linux x86_64 host, as a
cross-compile only:

| Item | Value |
| --- | --- |
| Rust | 1.100.0 |
| Flutter | 3.50.1 (Dart 3.14.0) |
| JDK | OpenJDK 21.0.13.1 |
| Android NDK | 28.2.13676358 (clang 19.0.1), API 24 |
| Command | `build-libs.sh android-arm64` then `flutter build apk --debug --target-platform android-arm64` |
| Native library | `libmqtt_c.so`, ELF64 AArch64 shared object, LOAD alignment 16 KiB, `panic=abort` |
| APK | `rs.rmqtt.mqtt_flutter_example` 0.1.0, minSdk 24, `native-code: arm64-v8a`, contains `lib/arm64-v8a/libmqtt_c.so` |
| Emulator | NOT RUN |
| Physical device | NOT RUN |

This is buildable evidence for `arm64-v8a`. It is not an emulator or
physical-device TLS result, and it is not a claim about the Android system
trust store.

## iOS device `arm64` (`aarch64-apple-ios`)

| Layer | Status |
| --- | --- |
| Rust static library (`panic=abort`) | NOT RUN on Linux; requires macOS and Xcode. A GitHub `macos-15` workflow exists in `.github/workflows/ios-buildable.yml` and is pending a real CI or Mac run. |
| Flutter unsigned iOS build (`flutter build ios --debug --no-codesign`) | Pending CI/Mac verification. `--no-codesign` is not a physical-device result. |
| Simulator run | NOT RUN |
| Physical-device TLS, subscribe, QoS 1 publish/receive | NOT RUN (later stage) |
| Background always-on MQTT | Not promised |

Reproduce on macOS with Xcode and Flutter 3.50.x:

```sh
examples/flutter/tools/build-libs.sh ios-arm64
cd examples/flutter/mqtt_flutter/example
flutter build ios --debug --no-codesign
```

The staged artifact is `examples/flutter/mqtt_flutter/ios/Frameworks/libmqtt_c.a`.
Do not commit the `.a`. A Linux checkout can only prepare the Xcode/podspec
project files.

## What a later physical-device TLS run must record

Target ABI, SDK/NDK or Xcode version, device model and OS, signing method,
broker, CA trust and hostname checks, foreground/background transitions,
network loss, and close behavior. Simulator or emulator success is not that
record.
