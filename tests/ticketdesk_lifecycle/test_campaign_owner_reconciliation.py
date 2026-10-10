# req: END-007, END-008, END-010
"""Closed child receipts must prove signed exits, finite births and exact custody."""
from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"scripts"))
from lib.ticketdesk_lifecycle_campaign_owner_reconciliation import (
    decode, checked_usage, read_private, reconcile, OwnerReconciliationError, BASIS,
)

MIB=1024**2


class OwnerReconciliationTests(unittest.TestCase):
    def fixture(self):
        self.limits=dict(controller=256*MIB,server=256*MIB,load=128*MIB,
                         observer=128*MIB,native_sampler=128*MIB,operator=128*MIB)
        return dict(schema='riffdb.ticketdesk-lifecycle-persistent-usage/v1',launch_observed=True,
            exit_observed=True,exit_code=-9,elapsed_ns=20,
            launch_memory=dict(rss_bytes=10,allocator_bytes=20,reported_rss_peak_bytes=15,reported_allocator_peak_bytes=25),
            launch_memory_refusal=None,exit_rss_peak_bytes=64*MIB,exit_allocator_peak_bytes=None,
            exit_allocator_peak_upper_bound_bytes=256*MIB,worker_exit_allocator_peak_upper_bound_bytes=256*MIB,
            address_space_proof=dict(enforced=True,limit_bytes=256*MIB,launch_virtual_peak_bytes=32*MIB,
                inherited_hard_limit_bytes=256*MIB,lifetime_upper_bound_bytes=256*MIB),
            exit_rss_basis='linux_wait4_per_child_ru_maxrss',allocator_basis=BASIS,
            qualifies_complete_resource_model=False)

    def test_killed_owner_preserves_signed_exit_and_unknown_allocator_peak(self):
        original=self.fixture();value=decode(json.dumps(original).encode())
        self.assertEqual(checked_usage(value,'server',self.limits,expected_exit=-9,maximum_lifetime_ns=100),64*MIB)
        self.assertEqual(value['exit_code'],-9);self.assertIsNone(value['exit_allocator_peak_bytes'])

    def test_unknown_birth_forged_exit_and_missing_kernel_peak_refuse(self):
        for fault in ('birth','exit','peak','allocator','boolean','lifetime'):
            with self.subTest(fault=fault):
                value=self.fixture()
                if fault=='birth':value['address_space_proof']['lifetime_upper_bound_bytes']=None
                if fault=='exit':value['exit_code']=0
                if fault=='peak':value['exit_rss_peak_bytes']=None
                if fault=='allocator':value['exit_allocator_peak_bytes']=1
                if fault=='boolean':value['exit_observed']=1
                if fault=='lifetime':value['elapsed_ns']=101
                with self.assertRaises(OwnerReconciliationError):checked_usage(value,'server',self.limits,expected_exit=-9,maximum_lifetime_ns=100)

    def test_unavailable_launch_census_keeps_actual_wait4_peak_and_finite_birth(self):
        value=self.fixture();value['launch_memory']=None;value['launch_memory_refusal']='resource_process_unavailable'
        self.assertEqual(checked_usage(value,'server',self.limits,expected_exit=-9,maximum_lifetime_ns=100),64*MIB)
        value['address_space_proof']['enforced']=False
        with self.assertRaises(OwnerReconciliationError):checked_usage(value,'server',self.limits,expected_exit=-9,maximum_lifetime_ns=100)

    def test_duplicate_numbers_floats_and_unsigned_fields_refuse(self):
        for encoded in (b'{"x":1,"x":2}',b'{"x":1.0}',b'{"x":-65}',b'{"x":18446744073709551616}'):
            with self.subTest(encoded=encoded), self.assertRaises(OwnerReconciliationError):decode(encoded)
        value=self.fixture();value['elapsed_ns']=-9
        with self.assertRaises(OwnerReconciliationError):checked_usage(value,'server',self.limits,expected_exit=-9,maximum_lifetime_ns=100)

    def ledger(self,root):
        value=self.fixture()
        rows=[]
        for i,(command,elapsed,span) in enumerate((('command.run',100,20),('projection.query',1000,30),
                ('worker.reconcile',21900*10**9+2*10**9,10**9),
                ('worker.archive-reconcile',21900*10**9+3*10**9,2*10**9))):
            rows.append(dict(schema='riffdb.ticketdesk-lifecycle-campaign-operator/v1',index=i,
                command=command,elapsed_ns=elapsed,duration_ns=span,exit_code=0,
                outcome_known=True,qualifies_complete_campaign=False))
        def write(name,record):
            path=root/name
            if not path.parent.exists():path.parent.mkdir(mode=0o700)
            path.write_text(json.dumps(record)+'\n');os.chmod(path,0o600)
        for role,count,prefix,file in (('server',4,'server','server'),
                ('native_sampler',5,'sampler','native-observer'),
                ('load',1,'original-load','load'),('observer',1,'original-observer','observer')):
            for i in range(count):
                directory=prefix+'-'+str(i).zfill(3) if role in ('server','native_sampler') else prefix
                current=deepcopy(value);current['exit_code']=-9 if role=='server' and i==2 else 0
                current['address_space_proof']['limit_bytes']=self.limits[role]
                current['worker_exit_allocator_peak_upper_bound_bytes']=self.limits[role]
                write(directory+'/'+file+'.process-usage.json',current)
        for i,row in enumerate(rows):
            current=deepcopy(value);current['exit_code']=row['exit_code'];current['elapsed_ns']=1
            current['address_space_proof']['limit_bytes']=self.limits['operator']
            current['worker_exit_allocator_peak_upper_bound_bytes']=self.limits['operator']
            write(f'operator-{i:03}.process-usage.json',current)
        ledger=root/'campaign-operator.jsonl'
        ledger.write_text(''.join(json.dumps(row)+'\n' for row in rows));os.chmod(ledger,0o600)
        self.terminal={'roles':{role:dict(incarnations=count,rss_peak_upper_bound_bytes=10)
            for role,count in dict(controller=1,server=4,native_sampler=5,load=1,observer=1,operator=2).items()}}

    def test_dense_actual_owner_ledgers_bind_all_declared_replacements(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);os.chmod(root,0o700);self.ledger(root)
            value=reconcile(root,duration_s=21900,limits=self.limits,terminal_memory=self.terminal)
            self.assertEqual(value['children'],15);self.assertEqual(value['incarnations'],16)
            self.assertEqual(value['roles']['operator'],4);self.assertEqual(len(value['inputs']),16)
            self.assertTrue(value['launch_and_exit_inventory_reconciled'])
            self.assertFalse(value['qualifies_complete_resource_model'])

    def test_missing_extra_or_miscounted_owners_and_noncontiguous_calls_refuse(self):
        for fault in ('missing_server','extra_server','missing_operator','extra_operator','counts','index','exit','controller_peak','missing_peak'):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);os.chmod(root,0o700);self.ledger(root)
                if fault=='missing_server':(root/'server-003').rename(root/'hidden-server')
                if fault=='extra_server':(root/'server-004').mkdir(mode=0o700)
                if fault=='missing_operator':(root/'operator-002.process-usage.json').unlink()
                if fault=='extra_operator':
                    p=root/'operator-099.process-usage.json';p.write_text('{}\n');os.chmod(p,0o600)
                if fault=='counts':self.terminal['roles']['native_sampler']['incarnations']=4
                if fault=='controller_peak':self.terminal['roles']['controller']['rss_peak_upper_bound_bytes']=True
                if fault=='missing_peak':del self.terminal['roles']['controller']['rss_peak_upper_bound_bytes']
                if fault=='index':
                    p=root/'campaign-operator.jsonl';rows=[json.loads(line) for line in p.read_text().splitlines()]
                    rows[1]['index']=2;p.write_text(''.join(json.dumps(row)+'\n' for row in rows))
                if fault=='exit':
                    p=root/'server-002/server.process-usage.json';row=json.loads(p.read_text());row['exit_code']=0
                    p.write_text(json.dumps(row)+'\n')
                with self.assertRaises(OwnerReconciliationError):reconcile(root,duration_s=21900,
                    limits=self.limits,terminal_memory=self.terminal)

    def test_closed_file_custody_refuses_aliases_symlinks_and_unbounded_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);os.chmod(root,0o700);child=root/'server-000';child.mkdir(mode=0o700)
            path=child/'server.process-usage.json';path.write_bytes(b'{}\n');os.chmod(path,0o600)
            encoded,identity=read_private(root,'server-000/server.process-usage.json',maximum=2048)
            self.assertEqual(encoded,b'{}\n');self.assertEqual(identity['bytes'],3)
            os.link(path,child/'alias')
            with self.assertRaises(OwnerReconciliationError):read_private(root,'server-000/server.process-usage.json',maximum=2048)
            (child/'alias').unlink();path.write_bytes(b'x'*2049)
            with self.assertRaises(OwnerReconciliationError):read_private(root,'server-000/server.process-usage.json',maximum=2048)
            path.unlink();path.symlink_to(child/'missing')
            with self.assertRaises(OwnerReconciliationError):read_private(root,'server-000/server.process-usage.json',maximum=2048)


if __name__=='__main__':unittest.main()
