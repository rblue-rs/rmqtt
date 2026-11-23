#!/usr/bin/env bash
# Build only bundled source; never install SDKs/targets or silently use a prebuilt binary.
set -euo pipefail
PKG="$(cd "$(dirname "$0")/.." && pwd)"
NATIVE="$PKG/native"
if [[ ! -f "$NATIVE/Cargo.lock" || ! -f "$NATIVE/crates/mqtt-c/src/lib.rs" ]]; then
  echo "error: missing bundled native source; use a complete source package" >&2
  exit 1
fi
kind="${1:-}"
offline=()
if [[ "${2:-}" == '--offline' ]]; then offline=(--offline); elif [[ $# -gt 1 ]]; then
  echo 'error: only optional --offline is accepted' >&2; exit 2
fi
if [[ $# -gt 2 ]]; then echo 'error: too many arguments' >&2; exit 2; fi
cd "$NATIVE"
CARGO="${CARGO:-cargo}"
# Keep outputs in this package, including when invoked from CocoaPods/Gradle.
export CARGO_TARGET_DIR="$NATIVE/target"
abort=(--config 'profile.release.panic="abort"')
require_target() {
  rustup target list --installed | grep -Fxq "$1" || {
    echo "error: preinstall Rust target $1 (no automatic downloads)" >&2; exit 1;
  }
}
case "$kind" in
  host)
    "$CARGO" build -p mqtt-c --locked "${offline[@]}"
    echo "Native library built under $CARGO_TARGET_DIR/debug (set MQTT_NATIVE_LIBRARY for host tests)."
    ;;
  android-arm64)
    triple=aarch64-linux-android
    require_target "$triple"
    ndk="${ANDROID_NDK_HOME:-${ANDROID_NDK_ROOT:-}}"
    if [[ -z "$ndk" ]]; then
      sdk="${ANDROID_SDK_ROOT:-${ANDROID_HOME:-}}"
      ndk="$sdk/ndk/28.2.13676358"
    fi
    if [[ ! -d "$ndk/toolchains/llvm/prebuilt" ]]; then
      echo 'error: set ANDROID_NDK_HOME to a preinstalled Android NDK' >&2; exit 1
    fi
    export ANDROID_NDK_HOME="$ndk" ANDROID_NDK_ROOT="$ndk"
    prebuilts=("$ndk"/toolchains/llvm/prebuilt/*)
    if [[ ${#prebuilts[@]} -ne 1 ]]; then echo 'error: ambiguous NDK host toolchain' >&2; exit 1; fi
    clang="${prebuilts[0]}/bin/aarch64-linux-android${ANDROID_API_LEVEL:-24}-clang"
    if [[ ! -x "$clang" ]]; then echo 'error: Android clang missing' >&2; exit 1; fi
    export CC="$clang" CXX="${clang}++" AR="${prebuilts[0]}/bin/llvm-ar"
    export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="$clang"
    export RUSTFLAGS="${RUSTFLAGS:-} -C link-arg=-Wl,-z,max-page-size=16384"
    "$CARGO" build -p mqtt-c --locked --release --target "$triple" "${abort[@]}" "${offline[@]}"
    dest="$PKG/android/src/main/jniLibs/arm64-v8a"
    mkdir -p "$dest"
    cp "$CARGO_TARGET_DIR/$triple/release/libmqtt_c.so" "$dest/libmqtt_c.so"
    ;;
  ios-arm64)
    if [[ "$(uname -s)" != Darwin ]]; then
      echo 'error: ios-arm64 requires macOS/Xcode and an iPhoneOS SDK' >&2; exit 3
    fi
    if [[ "${PLATFORM_NAME:-iphoneos}" != iphoneos || "${ARCHS:-arm64}" != arm64 ]]; then
      echo 'error: only iPhoneOS arm64 is configured; simulator/other slices unsupported' >&2; exit 3
    fi
    xcrun --sdk iphoneos --show-sdk-path >/dev/null
    require_target aarch64-apple-ios
    export IPHONEOS_DEPLOYMENT_TARGET=15.0
    "$CARGO" rustc -p mqtt-c --lib --locked --release --target aarch64-apple-ios \
      "${abort[@]}" "${offline[@]}" --crate-type staticlib
    mkdir -p "$PKG/ios/Frameworks"
    cp "$CARGO_TARGET_DIR/aarch64-apple-ios/release/libmqtt_c.a" "$PKG/ios/Frameworks/libmqtt_c.a"
    ;;
  *) echo 'usage: build-native.sh host|android-arm64|ios-arm64 [--offline]' >&2; exit 2 ;;
esac
