# req: PERF-018
"""Reused binaries must match both their build and local qualification."""
from copy import deepcopy
import unittest

from run_retained import verify_retained


class RetainedIdentity(unittest.TestCase):
    def setUp(self):
        self.freeze = {'revision': 'source', 'inputs': {'source.bundle': 'bundle', 'observer.patch': 'observer'}}
        self.receipt = {'source_freeze': deepcopy(self.freeze), 'daemon_sha256': 'daemon', 'runner_sha256': 'runner'}
        self.gate = {'source_revision': 'source', 'observer_patch_sha256': 'observer',
                     'local_functional_passed': True, 'daemon_sha256': 'daemon', 'runner_sha256': 'runner'}

    def verify(self, daemon='daemon', runner='runner'):
        verify_retained(self.freeze, self.receipt, self.gate, daemon, runner)

    def test_exact_binary_and_source_identity_passes(self):
        self.verify()

    def test_either_substituted_binary_is_refused(self):
        for daemon, runner in [('other', 'runner'), ('daemon', 'other')]:
            with self.subTest(daemon=daemon, runner=runner), self.assertRaises(AssertionError):
                self.verify(daemon, runner)

    def test_build_identity_cannot_replace_qualification_identity(self):
        self.receipt['daemon_sha256'] = 'other'
        with self.assertRaises(AssertionError):
            self.verify(daemon='other')

    def test_changed_source_or_observer_is_refused(self):
        for name in ('source.bundle', 'observer.patch'):
            with self.subTest(name=name):
                freeze = deepcopy(self.freeze)
                freeze['inputs'][name] = 'other'
                with self.assertRaises(AssertionError):
                    verify_retained(freeze, self.receipt, self.gate, 'daemon', 'runner')

    def test_failed_qualification_is_refused(self):
        self.gate['local_functional_passed'] = False
        with self.assertRaises(AssertionError):
            self.verify()


if __name__ == '__main__':
    unittest.main()
