#!/usr/bin/env python3
# req: VER-003, DRV-014
"""Fail-closed orchestration tests; CI also reproduces ADR-0167 with real Rust."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
CHECK = ROOT / 'scripts/downstream-adapter-check'
NAMES = ('riffdb-openfga', 'riffdb-better-auth', 'riffdb-mlflow')


class DownstreamCheck(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        binary = self.root / 'target/release/riffdb'
        binary.parent.mkdir(parents=True)
        binary.write_text('#!/bin/sh\n[ "$*" = "application check" ] || exit 2\n'
                          'case "$PWD" in *"$FAILING_ADAPTER") exit 1;; esac\nexit 0\n')
        binary.chmod(0o755)
        cargo = self.root / 'cargo'
        metadata = json.dumps({'target_directory': str(self.root / 'target')})
        cargo.write_text('#!/bin/sh\nif [ "$1" = metadata ]; then\n'
                         + "printf '%s\\n' '" + metadata + "'\nfi\n")
        cargo.chmod(0o755)
        self.env = dict(os.environ, PATH=str(self.root) + os.pathsep + os.environ['PATH'],
                        FAILING_ADAPTER='none')

    def adapters(self, names=NAMES):
        for name in names:
            (self.root / name).mkdir()
            (self.root / name / 'riffdb.toml').write_text('')

    def check(self, *args):
        return subprocess.run([str(CHECK), '--repo-root', str(self.root), *args],
                              cwd=ROOT, env=self.env, capture_output=True, text=True)

    def test_missing_adapter_fails_even_when_another_compiles(self):
        self.adapters(NAMES[:1])
        result = self.check()
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('riffdb-better-auth is absent', result.stderr)

    def test_every_adapter_is_checked_and_failure_propagates(self):
        self.adapters()
        self.env['FAILING_ADAPTER'] = 'riffdb-better-auth'
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('riffdb-openfga\tcompiles', result.stdout)
        self.assertIn('riffdb-better-auth\tfailed', result.stderr)
        self.assertIn('riffdb-mlflow\tcompiles', result.stdout)

    def test_all_adapters_must_compile(self):
        self.adapters()
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.count('\tcompiles'), len(NAMES))

    def test_absent_manifest_fails(self):
        self.adapters()
        (self.root / NAMES[1] / 'riffdb.toml').unlink()
        result = self.check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('no riffdb.toml', result.stderr)

    def test_unknown_adapter_cannot_turn_into_a_path(self):
        result = self.check('--repo', '../elsewhere')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('unknown adapter', result.stderr)

    def test_missing_flag_value_is_usage_error(self):
        result = self.check('--repo')
        self.assertEqual(result.returncode, 2)

    def test_invalid_pin_sets_fail_before_fetch_or_build(self):
        checkout = self.root / 'checkout'
        scripts = checkout / 'scripts'
        scripts.mkdir(parents=True)
        subprocess.run(['git', 'init', '--quiet', str(checkout)], check=True)
        shutil.copy2(CHECK, scripts / CHECK.name)
        valid = json.loads((ROOT / 'scripts/downstream-adapters.json').read_text())
        for mutation in ('branch', 'empty', 'duplicate', 'foreign_repository'):
            with self.subTest(mutation=mutation):
                pins = json.loads(json.dumps(valid))
                if mutation == 'branch':
                    pins['adapters'][0]['revision'] = 'main'
                elif mutation == 'empty':
                    pins['adapters'] = []
                elif mutation == 'duplicate':
                    pins['adapters'].append(pins['adapters'][0])
                else:
                    pins['adapters'][0]['repository'] = 'file:///unreviewed'
                (scripts / 'downstream-adapters.json').write_text(json.dumps(pins))
                result = subprocess.run([str(scripts / CHECK.name)], cwd=checkout,
                                        env=self.env, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('invalid immutable adapter pins', result.stderr)


if __name__ == '__main__':
    unittest.main()
