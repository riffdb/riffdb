# req: PERF-018
from copy import deepcopy
import unittest
from qualification_gate import WORKLOAD,verify

class QualificationGate(unittest.TestCase):
    def setUp(self):
        self.freeze=dict(control='parent',candidate='child',inputs={'observer':'exact'})
        self.build=dict(source_freeze=deepcopy(self.freeze),runner_sha256='runner',arms={
            a:dict(revision=r,daemon_sha256=a) for a,r in [('control','parent'),('candidate','child')]})
        self.gate=dict(source_freeze=deepcopy(self.freeze),runner_sha256='runner',local_correctness_passed=True,performance_verdict=False,arms={})
        for a,r in [('control','parent'),('candidate','child')]:
            self.gate['arms'][a]=dict(revision=r,daemon_sha256=a,workload=deepcopy(WORKLOAD),correctness_clean=True,
                complete_analysis_passed=True,report_scale=dict(profile='full',approximate_row_count=19220),
                measured_elapsed_ns=90_000_000_000,samples=100,checkpoints=2,dropped_samples=0,exit_code=0)
    def check(self):verify(self.freeze,self.build,self.gate)
    def test_exact_two_arm_receipt_passes(self):self.check()
    def test_stale_or_identical_binary_is_refused(self):
        self.gate['arms']['candidate']['daemon_sha256']='control'
        with self.assertRaises(AssertionError):self.check()
        self.build['arms']['candidate']['daemon_sha256']='control'
        with self.assertRaises(AssertionError):self.check()
    def test_missing_arm_is_refused(self):
        self.gate['arms'].pop('control')
        with self.assertRaises(AssertionError):self.check()
    def test_short_smoke_or_incomplete_run_is_refused(self):
        original=deepcopy(self.gate)
        for key,value in [('measured_elapsed_ns',1_000_000_000),('samples',0),('checkpoints',0),('dropped_samples',1),('exit_code',1),('correctness_clean',False),('complete_analysis_passed',False)]:
            self.gate=deepcopy(original)
            self.gate['arms']['candidate'][key]=value
            with self.subTest(key=key),self.assertRaises(AssertionError):self.check()
        self.gate=deepcopy(original)
        self.gate['arms']['control']['workload']['dataset']='smoke'
        with self.assertRaises(AssertionError):self.check()
    def test_changed_source_or_runner_is_refused(self):
        self.gate['source_freeze']['candidate']='new-source'
        with self.assertRaises(AssertionError):self.check()
        self.gate['source_freeze']=deepcopy(self.freeze)
        self.gate['runner_sha256']='new-runner'
        with self.assertRaises(AssertionError):self.check()

if __name__=='__main__':unittest.main()
