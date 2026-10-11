# req: END-007, END-008, END-010
"""Independent observations must refuse omissions, rewinds and alternate bases."""
from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"scripts"))
from lib.ticketdesk_lifecycle_campaign_resource_reconciliation import (
    reconcile, read_closed, ResourceReconciliationError, DISK_FIELDS, DISK_BASIS, ALLOCATOR_BASIS,
)

MIB=1024**2
SECOND=10**9


class ResourceReconciliationTests(unittest.TestCase):
    def fixture(self):
        self.process_limits=dict(controller=256*MIB,server=128*MIB,load=128*MIB,
            observer=128*MIB,native_sampler=128*MIB,operator=128*MIB)
        self.limits={**dict.fromkeys(DISK_FIELDS,MIB),"rss_bytes":MIB,"allocator_bytes":1024*MIB}
        process=dict(rss_bytes=10,allocator_bytes=20,rss_peak_bytes=15,allocator_peak_bytes=25,
            reported_rss_peak_bytes=15,reported_allocator_peak_bytes=25,task_census_restarts=0,
            later_reported_rss_peak_bytes=15,later_reported_allocator_peak_bytes=25)
        roles={role:dict(incarnations=1,rss_peak_upper_bound_bytes=15,
            allocator_peak_upper_bound_bytes=256*MIB) for role in self.process_limits if role!="operator"}
        memory=dict(schema="riffdb.ticketdesk-lifecycle-campaign-roster-memory/v1",elapsed_ns=0,
            allocator_basis=ALLOCATOR_BASIS,rss_peak_basis="maximum_observed_asynchronous_kernel_high_water_counter",
            rss_observation_basis="asynchronous_linux_kernel_rss_counter",scope="verified_owned_descendant_tree_at_sample",
            processes={role:deepcopy(process) for role in roles},rss_bytes=50,allocator_bytes=100,
            rss_peak_upper_bound_bytes=75,allocator_peak_upper_bound_bytes=1024*MIB,roles=roles,
            incarnations=5,retired_incarnations=0,
            allocator_peak_basis="frozen_worker_limits_plus_one_serial_launcher_birth_bound",qualifies_resource_model=False)
        disk=dict(schema="riffdb.ticketdesk-lifecycle-disk-sample/v1",elapsed_ns=0,basis=DISK_BASIS,
            files_and_directories=12,directories=3,resources=dict.fromkeys(DISK_FIELDS,1),
            complete_resource_model_proved=False)
        self.memory=[dict(deepcopy(memory),elapsed_ns=elapsed*SECOND) for elapsed in range(0,21900,60)]
        self.disk=[dict(deepcopy(disk),elapsed_ns=elapsed*SECOND) for elapsed in range(0,21901,30)]
        self.terminal=dict(deepcopy(memory),elapsed_ns=21900*SECOND)

    def run_reconciliation(self):
        return reconcile(self.memory,self.disk,duration_s=21900,process_limits=self.process_limits,
            resource_limits=self.limits,terminal_memory=self.terminal)

    def test_complete_clock_preserves_separate_whole_file_and_memory_bounds(self):
        self.fixture();self.disk[10]['resources']['journal_bytes']=999
        result=self.run_reconciliation()
        self.assertEqual(len(result['windows']),73)
        self.assertEqual(result['windows'][1]['journal_bytes'],999)
        self.assertEqual(result['windows'][0]['database_bytes'],1)
        self.assertEqual(result['windows'][-1]['allocator_bytes'],1024*MIB)
        self.assertFalse(result['qualifies_complete_resource_model'])

    def test_missing_launcher_birth_bound_refuses_even_when_current_workers_fit(self):
        self.fixture()
        for row in [*self.memory,self.terminal]:
            row['allocator_peak_upper_bound_bytes']=sum(self.process_limits.values())
        with self.assertRaises(ResourceReconciliationError):self.run_reconciliation()

    def test_unknown_birth_ceiling_dropped_control_files_and_unbounded_limits_refuse(self):
        for fault in ('birth','control','limit','boolean','basis'):
            with self.subTest(fault=fault):
                self.fixture()
                if fault=='birth':self.memory[0]['allocator_peak_upper_bound_bytes']-=128*MIB
                if fault=='control':del self.disk[0]['resources']['control_and_evidence_bytes']
                if fault=='limit':self.disk[0]['resources']['control_and_evidence_bytes']=MIB+1
                if fault=='boolean':self.memory[0]['incarnations']=True
                if fault=='basis':self.disk[0]['basis']='sum_apparent_bytes_only'
                with self.assertRaises(ResourceReconciliationError):self.run_reconciliation()

    def test_global_gap_missing_window_and_unmeasured_terminal_refuse(self):
        for fault in ('gap','window','disk_terminal','memory_terminal','late_terminal'):
            with self.subTest(fault=fault):
                self.fixture()
                if fault=='gap':del self.memory[4]
                if fault=='window':self.disk=[row for row in self.disk if not 900*SECOND<=row['elapsed_ns']<1200*SECOND]
                if fault=='disk_terminal':self.disk.pop()
                if fault=='memory_terminal':self.terminal['elapsed_ns']=21899*SECOND
                if fault=='late_terminal':self.terminal['elapsed_ns']=21976*SECOND
                with self.assertRaises(ResourceReconciliationError):self.run_reconciliation()

    def test_reported_totals_lost_retirement_peaks_and_untracked_owner_refuse(self):
        for fault in ('sum','role_peak','drop_role','untracked'):
            with self.subTest(fault=fault):
                self.fixture()
                if fault=='sum':self.memory[0]['rss_bytes']+=1
                if fault=='role_peak':self.memory[0]['roles']['server']['rss_peak_upper_bound_bytes']=14
                if fault=='drop_role':del self.terminal['roles']['server']
                if fault=='untracked':self.memory[0]['processes']['unknown']=self.memory[0]['processes']['load']
                with self.assertRaises(ResourceReconciliationError):self.run_reconciliation()

    def test_closed_file_custody_hashes_exact_bytes_and_refuses_link_duplicate_and_growth(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);os.chmod(root,0o700);path=root/'memory-samples.jsonl'
            encoded=b'{"x":1}\n';path.write_bytes(encoded);os.chmod(path,0o600)
            rows,identity=read_closed(root,path.name,maximum_rows=2)
            self.assertEqual(rows,[{'x':1}]);self.assertEqual(identity['bytes'],len(encoded))
            import hashlib
            self.assertEqual(identity['sha256'],hashlib.sha256(encoded).hexdigest())
            os.link(path,root/'alias')
            with self.assertRaises(ResourceReconciliationError):read_closed(root,path.name,maximum_rows=2)
            (root/'alias').unlink();path.write_bytes(b'{"x":1,"x":2}\n')
            with self.assertRaises(ResourceReconciliationError):read_closed(root,path.name,maximum_rows=2)
            path.write_bytes(encoded*3)
            with self.assertRaises(ResourceReconciliationError):read_closed(root,path.name,maximum_rows=2)
            path.unlink();path.symlink_to(root/'missing')
            with self.assertRaises(ResourceReconciliationError):read_closed(root,path.name,maximum_rows=2)


if __name__=='__main__':unittest.main()
