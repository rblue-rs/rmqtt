#!/usr/bin/env bash
# Cross-compile mqtt-c for the default mobile ABIs and stage plugin artifacts.
#
#   android-arm64  -> aarch64-linux-android, packaged as arm64-v8a libmqtt_c.so
#   ios-arm64      -> aarch64-apple-ios static library (macOS + Xcode only)
#
# Release builds use panic=abort. catch_unwind in the C ABI cannot catch a
# panic on these artifacts; the process aborts. This script only produces
# buildable libraries. It is not an emulator, simulator, or physical-device
# result, and it does not claim a mobile trust store or background session.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$ROOT"
CARGO="${CARGO:-cargo}"
TARGET_ARG="${1:-}"

usage() {
  echo "usage: $0 android-arm64|ios-arm64" >&2
  exit 2
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "error: missing command: $1" >&2; exit 1; }
}

# Cargo's --config flag must appear before the subcommand.
CARGO_RELEASE_ABORT=(--config 'profile.release.panic="abort"')

build_android_arm64() {
  local ndk triple api prebuilt clang ar dest out
  triple="aarch64-linux-android"
  api="${ANDROID_API_LEVEL:-24}"
  if [[ -z "${ANDROID_NDK_HOME:-}${ANDROID_NDK_ROOT:-}" ]]; then
    if [[ -n "${ANDROID_SDK_ROOT:-}" && -d "${ANDROID_SDK_ROOT}/ndk/28.2.13676358" ]]; then
      ANDROID_NDK_HOME="${ANDROID_SDK_ROOT}/ndk/28.2.13676358"
    elif [[ -n "${ANDROID_HOME:-}" && -d "${ANDROID_HOME}/ndk/28.2.13676358" ]]; then
      ANDROID_NDK_HOME="${ANDROID_HOME}/ndk/28.2.13676358"
    fi
  fi
  ndk="${ANDROID_NDK_HOME:-${ANDROID_NDK_ROOT:-}}"
  if [[ -z "$ndk" || ! -d "$ndk" ]]; then
    echo "error: set ANDROID_NDK_HOME or ANDROID_SDK_ROOT with NDK 28.2.13676358" >&2
    exit 1
  fi
  export ANDROID_NDK_HOME="$ndk"
  export ANDROID_NDK_ROOT="$ndk"
  prebuilt="$(echo "$ndk"/toolchains/llvm/prebuilt/*)"
  if [[ ! -d "$prebuilt" ]]; then
    echo "error: NDK LLVM prebuilt not found under $ndk" >&2
    exit 1
  fi
  clang="$prebuilt/bin/${triple}${api}-clang"
  ar="$prebuilt/bin/llvm-ar"
  if [[ ! -x "$clang" ]]; then
    echo "error: missing Android clang: $clang" >&2
    exit 1
  fi
  rustup target add "$triple"
  export AR="$ar"
  export CC="$clang"
  export CXX="${clang}++"
  export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="$clang"
  # 16 KiB ELF segment alignment for newer Android.
  export RUSTFLAGS="${RUSTFLAGS:-} -C link-arg=-Wl,-z,max-page-size=16384"
  echo "== android-arm64 =="
  echo "triple=$triple api=$api panic=abort"
  echo "ndk=$ndk"
  rustc --version
  "$clang" --version | head -n 1
  "$CARGO" build -p mqtt-c --locked --release --target "$triple" \
    "${CARGO_RELEASE_ABORT[@]}"
  out="$ROOT/target/${triple}/release/libmqtt_c.so"
  dest="$ROOT/examples/flutter/mqtt_flutter/android/src/main/jniLibs/arm64-v8a"
  mkdir -p "$dest"
  cp -f "$out" "$dest/libmqtt_c.so"
  echo "wrote $dest/libmqtt_c.so"
  if command -v llvm-readobj >/dev/null 2>&1; then
    llvm-readobj -h "$dest/libmqtt_c.so" | head -n 20
  elif command -v readelf >/dev/null 2>&1; then
    readelf -h "$dest/libmqtt_c.so"
  elif command -v file >/dev/null 2>&1; then
    file "$dest/libmqtt_c.so"
  fi
}

build_ios_arm64() {
  local dest out
  if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "NOT RUN: ios-arm64 requires macOS and Xcode; this host is $(uname -s) $(uname -m)."
    exit 3
  fi
  if ! xcrun --sdk iphoneos --show-sdk-path >/dev/null 2>&1; then
    echo "error: Xcode iPhoneOS SDK not available" >&2
    exit 1
  fi
  rustup target add aarch64-apple-ios
  echo "== ios-arm64 (aarch64-apple-ios, panic=abort) =="
  rustc --version
  xcrun --sdk iphoneos --show-sdk-version
  # Request a static library for DynamicLibrary.process() on iOS.
  "$CARGO" build -p mqtt-c --locked --release --target aarch64-apple-ios \
    "${CARGO_RELEASE_ABORT[@]}" \
    --config 'lib.crate-type=["staticlib"]'
  out="$ROOT/target/aarch64-apple-ios/release/libmqtt_c.a"
  dest="$ROOT/examples/flutter/mqtt_flutter/ios/Frameworks"
  mkdir -p "$dest"
  cp -f "$out" "$dest/libmqtt_c.a"
  echo "wrote $dest/libmqtt_c.a"
  lipo -info "$dest/libmqtt_c.a" || file "$dest/libmqtt_c.a"
}

case "$TARGET_ARG" in
  android-arm64) build_android_arm64 ;;
  ios-arm64) build_ios_arm64 ;;
  *) usage ;;
esac
