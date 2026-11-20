import 'dart:io';
import 'package:test/test.dart';

void main() {
  for (final mode in [
    'exit',
    'natural',
    'init-exit',
    'kill',
    'kill-default',
    'multi',
    'gc'
  ]) {
    test(
        'owner $mode releases resources and runtime references without Dart cleanup',
        () async {
      final result = await Process.run(Platform.resolvedExecutable, [
        '--new_gen_semi_max_size=1',
        '--old_gen_heap_size=128',
        'run',
        'test/probes/owner_lifecycle_probe.dart',
        mode
      ]);
      expect(result.exitCode, 0, reason: '${result.stdout}${result.stderr}');
    }, timeout: const Timeout(Duration(seconds: 20)));
  }
}
