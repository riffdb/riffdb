# req: PERF-018
"""A short local pass cannot open the full diagnostic's measurement gate."""
from copy import deepcopy
import unittest
from build_run import verify_local_qualification


def fixture():
    freeze = {'revision': 'source', 'inputs': {'source.bundle': 'bundle', 'observer.patch': 'observer'}}
    receipt = {'source_freeze': deepcopy(freeze), 'daemon_sha256': 'daemon', 'runner_sha256': 'runner'}
    gate = {'source_revision': 'source', 'observer_patch_sha256': 'observer',
            'local_functional_passed': True, 'daemon_sha256': 'daemon', 'runner_sha256': 'runner',
            'capacity_qualification': {
                'workload': {'dataset': 'full', 'clients': 32, 'load': 'write_only', 'warmup_seconds': 15, 'measurement_seconds': 90},
                'report_scale': {'profile': 'full', 'approximate_row_count': 19220},
                'measured_elapsed_ns': 90_000_000_000, 'complete_analysis_passed': True,
                'io_record_limit': 16_777_216,
                'coverage': {'final': 1, 'groups': 2, 'reserved_groups': 2, 'checkpoints': 1, 'reserved_checkpoints': 1, 'io': 10, 'reserved_io': 10, 'dropped_groups': 0, 'dropped_checkpoints': 0, 'dropped_io': 0}}}
    return freeze, receipt, gate


class FullQualificationGate(unittest.TestCase):
    def verify(self, edit=None, daemon='daemon', runner='runner'):
        freeze, receipt, gate = fixture()
        if edit: edit(freeze, receipt, gate)
        verify_local_qualification(freeze, receipt, gate, daemon, runner)

    def test_full_qualification_with_both_exact_binaries_passes(self):
        self.verify()

    def test_old_boolean_only_qualification_is_refused(self):
        with self.assertRaises(AssertionError):
            self.verify(lambda f,r,g:g.pop('capacity_qualification'))

    def test_short_duration_smoke_scale_or_different_load_is_refused(self):
        for name, value in [('dataset','smoke'),('clients',1),('load','interactive'),('warmup_seconds',2),('measurement_seconds',20)]:
            with self.subTest(name=name), self.assertRaises(AssertionError):
                self.verify(lambda f,r,g:g['capacity_qualification']['workload'].update({name:value}))

    def test_short_actual_measurement_or_missing_completion_is_refused(self):
        for name, value in [('measured_elapsed_ns',20_000_000_000),('complete_analysis_passed',False),('io_record_limit',4_194_304)]:
            with self.subTest(name=name), self.assertRaises(AssertionError):
                self.verify(lambda f,r,g:g['capacity_qualification'].update({name:value}))
        with self.assertRaises(AssertionError):
            self.verify(lambda f,r,g:g['capacity_qualification']['report_scale'].update(profile='smoke'))

    def test_every_drop_and_incomplete_population_is_refused(self):
        for name, value in [('final',0),('dropped_io',1),('dropped_groups',1),('dropped_checkpoints',1),('reserved_io',11),('checkpoints',0)]:
            with self.subTest(name=name), self.assertRaises(AssertionError):
                self.verify(lambda f,r,g:g['capacity_qualification']['coverage'].update({name:value}))

    def test_either_substituted_binary_is_refused(self):
        for daemon, runner in [('other','runner'),('daemon','other')]:
            with self.subTest(daemon=daemon, runner=runner), self.assertRaises(AssertionError):
                self.verify(daemon=daemon,runner=runner)

    def test_build_digest_cannot_replace_qualification_digest(self):
        with self.assertRaises(AssertionError):
            self.verify(lambda f,r,g:r.update(daemon_sha256='other'),daemon='other')

    def test_changed_source_observer_or_bundle_is_refused(self):
        for edit in [lambda f,r,g:f.update(revision='other'),lambda f,r,g:f['inputs'].update({'observer.patch':'other'}),lambda f,r,g:f['inputs'].update({'source.bundle':'other'})]:
            with self.subTest(edit=edit), self.assertRaises(AssertionError):self.verify(edit)

    def test_failed_local_run_is_refused(self):
        with self.assertRaises(AssertionError):self.verify(lambda f,r,g:g.update(local_functional_passed=False))

if __name__ == '__main__':unittest.main()
