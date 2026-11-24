#!/usr/bin/env python3
"""Source-package contracts; mobile hook checks are orchestration, not mobile builds."""
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
PKG = ROOT / 'examples/flutter/mqtt_flutter'


class SourcePackageTests(unittest.TestCase):
    def test_confirmed_flutter_name_and_imports(self):
        self.assertRegex((PKG / 'pubspec.yaml').read_text(), r'(?m)^name: rmqtt_flutter$')
        self.assertTrue((PKG / 'lib/rmqtt_flutter.dart').is_file())
        for path in PKG.rglob('*.dart'):
            if '.dart_tool' not in path.parts:
                self.assertNotIn('package:mqtt_flutter/', path.read_text(), str(path))
        self.assertIn('rmqtt_flutter:', (PKG / 'example/pubspec.yaml').read_text())

    def test_library_parts_follow_canonical_entry(self):
        self.assertEqual((PKG / 'lib/src/drain_close.dart').read_text().splitlines()[0],
                         "part of '../rmqtt_flutter.dart';")

    def test_metadata_license_and_no_publish(self):
        for name in ('LICENSE', 'README.md', 'CHANGELOG.md', 'NOTICE'):
            self.assertTrue((PKG / name).is_file(), name)
        self.assertEqual((PKG / 'LICENSE').read_bytes(), (ROOT / 'LICENSE').read_bytes())
        self.assertIn('publish_to: none', (PKG / 'pubspec.yaml').read_text())
        self.assertIn('repository: https://github.com/rblue-rs/rmqtt',
                      (PKG / 'pubspec.yaml').read_text())
        for manifest in (ROOT / 'crates').glob('*/Cargo.toml'):
            self.assertIn('publish = false', manifest.read_text())

    def test_bundled_workspace_contains_native_build_inputs(self):
        for path in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'LICENSE',
                     'crates/mqtt-c/third_party/dart/LICENSE',
                     'crates/mqtt-c/third_party/dart/dart_api_dl.c',
                     'crates/mqtt-c/src/dart_bridge.c', 'crates/mqtt-c/cbindgen.toml',
                     'bindings/c/include/mqtt_client.h'):
            self.assertTrue((PKG / 'native' / path).is_file(), path)
        for name in ('mqtt-api', 'mqtt-runtime', 'mqtt-core', 'mqtt-c'):
            self.assertTrue((PKG / f'native/crates/{name}/src/lib.rs').is_file())

    def test_source_mirror_matches_manifest_and_canonical_bytes(self):
        manifest = PKG / 'native/SOURCES.json'
        self.assertTrue(manifest.is_file(), 'generated source manifest missing')
        entries = json.loads(manifest.read_text())
        for name, digest in entries.items():
            actual = (PKG / 'native' / name).read_bytes()
            self.assertEqual(hashlib.sha256(actual).hexdigest(), digest, name)
            self.assertEqual(actual, (ROOT / name).read_bytes(), name)
        subprocess.run(['python3', str(ROOT / 'scripts/sync-native.py'), '--check'],
                       check=True, capture_output=True)

    def test_snapshot_excludes_credentials_test_material_and_build_outputs(self):
        native = PKG / 'native'
        self.assertTrue(native.is_dir())
        for path in native.rglob('*'):
            rel = path.relative_to(native)
            if 'target' in rel.parts:  # Local build output is excluded by .pubignore.
                continue
            self.assertFalse(path.is_symlink(), str(rel))
            self.assertFalse(set(rel.parts) & {'.git', '.dart_tool', 'tests', 'examples'})
            self.assertNotIn(path.suffix, ('.pem', '.key', '.so', '.a', '.dylib', '.crate'))

    def test_binding_input_is_package_local(self):
        config = (PKG / 'ffigen.yaml').read_text()
        self.assertIn('native/bindings/c/include/mqtt_client.h', config)
        self.assertNotIn('../', config)
        build = (ROOT / 'crates/mqtt-c/build.rs').read_text()
        self.assertIn('OUT_DIR', build)
        self.assertNotIn('join("../..")', build)
        self.assertTrue((ROOT / 'crates/mqtt-c/cbindgen.toml').is_file())

    def test_android_hook_cannot_silently_skip_missing_sources(self):
        gradle = (PKG / 'android/build.gradle').read_text()
        self.assertIn('../tool/build-native.sh', gradle)
        self.assertNotIn('onlyIf', gradle)
        self.assertNotIn('../../../tools', gradle)
        self.assertTrue((PKG / 'tool/build-native.sh').is_file())

    def test_ios_pod_name_and_package_local_build_hook(self):
        spec = PKG / 'ios/rmqtt_flutter.podspec'
        self.assertTrue(spec.is_file())
        value = spec.read_text()
        self.assertRegex(value, r"s.name\s*= 'rmqtt_flutter'")
        self.assertIn('build-native.sh', value)
        self.assertIn(':file =>', value)
        self.assertIn("s.platform = :ios, '15.0'", value)
        self.assertIn('script_phase', value)

    def test_build_fails_before_cargo_if_sources_are_missing(self):
        script = PKG / 'tool/build-native.sh'
        self.assertTrue(script.is_file())
        with tempfile.TemporaryDirectory(prefix='mqtt-package-missing-') as tmp:
            target = Path(tmp) / 'tool/build-native.sh'
            target.parent.mkdir()
            shutil.copyfile(script, target)
            result = subprocess.run(['bash', str(target), 'host', '--offline'],
                                    capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('missing bundled native source', result.stderr)

    def test_pub_exclusions_keep_native_source_but_not_outputs(self):
        ignore = PKG / '.pubignore'
        self.assertTrue(ignore.is_file())
        value = ignore.read_text()
        for item in ('native/target/', '.dart_tool/', '*.so', '*.a', '*.pem', '*.key'):
            self.assertIn(item, value)
        self.assertNotRegex(value, r'(?m)^native/$')


if __name__ == '__main__':
    unittest.main()
