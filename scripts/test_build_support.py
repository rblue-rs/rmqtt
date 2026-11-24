#!/usr/bin/env python3
"""Regression checks for TLS fixtures and mobile build orchestration."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent


class CertificateFixtureTests(unittest.TestCase):
    def test_expired_fixture_is_signed_expired_and_keeps_san(self):
        with tempfile.TemporaryDirectory(prefix="mqtt-fixture-") as tmp:
            out = Path(tmp)
            subprocess.run([sys.executable, str(ROOT / "scripts/gen_test_certs.py"), tmp],
                           check=True, capture_output=True)
            def openssl(*args):
                return subprocess.run(["openssl", *args], text=True, capture_output=True)
            for name, purpose in (("server-127", "sslserver"), ("client-ok", "sslclient")):
                result = openssl("verify", "-CAfile", str(out / "ca.pem"),
                                 "-purpose", purpose, str(out / f"{name}.pem"))
                self.assertEqual(result.returncode, 0, result.stderr)
            expired = openssl("verify", "-CAfile", str(out / "ca.pem"),
                              str(out / "server-expired.pem"))
            self.assertNotEqual(expired.returncode, 0)
            self.assertIn("error 10", expired.stderr)
            signature = openssl("verify", "-no_check_time", "-CAfile", str(out / "ca.pem"),
                                "-purpose", "sslserver", str(out / "server-expired.pem"))
            self.assertEqual(signature.returncode, 0, signature.stderr)
            dates = openssl("x509", "-in", str(out / "server-expired.pem"), "-noout", "-dates")
            self.assertIn("Jan  1 00:00:00 2020", dates.stdout)
            self.assertIn("Jan  2 00:00:00 2020", dates.stdout)
            san = openssl("x509", "-in", str(out / "server-expired.pem"),
                          "-noout", "-ext", "subjectAltName")
            self.assertIn("127.0.0.1", san.stdout)


class IosBuildConfigurationTests(unittest.TestCase):
    def test_cargo_override_emits_only_staticlib(self):
        # Real Cargo semantics on the host, not an Apple compile/link test.
        with tempfile.TemporaryDirectory(prefix="mqtt-crate-") as tmp:
            root = Path(tmp)
            (root / "src").mkdir()
            (root / "Cargo.toml").write_text(
                '[package]\nname="crate_type_probe"\nversion="0.1.0"\nedition="2021"\n'
                '[lib]\ncrate-type=["cdylib", "staticlib", "rlib"]\n')
            (root / "src/lib.rs").write_text('pub fn probe() -> u32 { 1 }\n')
            env = dict(os.environ, CARGO_TARGET_DIR=str(root / 'output'))
            result = subprocess.run([os.environ.get('CARGO', 'cargo'), 'rustc', '--lib',
                                     '--release', '--offline', '--crate-type', 'staticlib'],
                                    cwd=root, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            output = root / 'output/release'
            self.assertTrue((output / 'libcrate_type_probe.a').exists())
            for suffix in ('.so', '.dylib', '.rlib'):
                self.assertFalse((output / ('libcrate_type_probe' + suffix)).exists())

    def test_static_library_uses_existing_ios_15_floor(self):
        # Orchestration check only. These fake tools are NOT an iOS build.
        with tempfile.TemporaryDirectory(prefix="mqtt-tools-") as tmp:
            tools = Path(tmp)
            def tool(name, body):
                p = tools / name
                p.write_text("#!/usr/bin/env python3\n" + body)
                p.chmod(0o755)
            tool("uname", "print('Darwin')\n")
            tool("xcrun", "print('18.5')\n")
            tool("rustup", "pass\n")
            tool("rustc", "print('rustc orchestration only')\n")
            capture = tools / "invocation.json"
            tool("cargo", "import os,sys,json\n"
                 "json.dump({'args':sys.argv[1:], 'floor':os.getenv('IPHONEOS_DEPLOYMENT_TARGET')}, "
                 "open(os.environ['CAPTURE'], 'w'))\nsys.exit(73)\n")
            env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ['PATH'],
                       CARGO=str(tools / 'cargo'), CAPTURE=str(capture))
            env.pop('IPHONEOS_DEPLOYMENT_TARGET', None)
            result = subprocess.run(['bash', str(ROOT / 'examples/flutter/tools/build-libs.sh'),
                                     'ios-arm64'], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 73, result.stderr)
            call = json.loads(capture.read_text())
            self.assertEqual(call['floor'], '15.0')
            self.assertEqual(call['args'][0], 'rustc')
            self.assertIn('--lib', call['args'])
            self.assertIn('--crate-type', call['args'])
            self.assertEqual(call['args'][call['args'].index('--crate-type') + 1], 'staticlib')
            self.assertNotIn('lib.crate-type=["staticlib"]', call['args'])
            self.assertIn("s.platform = :ios, '15.0'", (ROOT /
                'examples/flutter/mqtt_flutter/ios/rmqtt_flutter.podspec').read_text())


if __name__ == '__main__':
    unittest.main()
