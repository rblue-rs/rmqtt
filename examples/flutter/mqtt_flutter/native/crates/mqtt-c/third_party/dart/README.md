# Dart API DL glue

Unmodified public embedding API DL files from Dart SDK3.14.0, with its BSD-style
license. Source: https://github.com/dart-lang/sdk/tree/3.14.0/runtime/include
The official dart_api_dl.c loader performs API major-version validation. Our
small native bridge uses a native port for scalar owner-exit notifications,
never native-to-Dart event callbacks. No Dart SDK installation is needed to build
this vendored glue; mobile ABI/build validation remains separate.
