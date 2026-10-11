# req: END-003, END-007, END-008, END-010
"""Long archive validation is admitted only after the measured owners retire."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import test_campaign_operator as fixtures
from lib.ticketdesk_lifecycle_campaign_operator import CampaignOperator, OperatorError
from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess

DURATION = 28800
REPORT = {"schema": "riffdb.ticketdesk-lifecycle-archive-reconciliation/v1",
    "records_checked": 65857, "encoded_bytes": 123, "physical_bytes": 456,
    "payloads_reconciled": True, "policy_anchors_reconciled": True}


class CampaignTerminalArchiveTests(unittest.TestCase):
    fixture = fixtures.CampaignOperatorTests.fixture

    @staticmethod
    def retire_all(roster):
        for role, child in list(roster.children.items()):
            child.exit(0); roster.retire(role, child, expected_exit=0)
        (roster.proc_root / str(os.getpid()) / "task" / str(os.getpid()) / "children").write_text("")

    def test_terminal_archive_requires_elapsed_period_and_all_original_owners_retired(self):
        cases = [(False, DURATION*10**9), (True, DURATION*10**9-1)]
        for offline, elapsed in cases:
            with self.subTest(offline=offline), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); roster = self.fixture(root, json.dumps(REPORT).encode())
                if offline: self.retire_all(roster)
                with CampaignOperator(root, roster, ["fixture"]) as operator:
                    with self.assertRaisesRegex(OperatorError, "^campaign_archive_not_offline$"):
                        operator.run_final_archive("fixture-worker", {}, elapsed_ns=elapsed,
                            duration_s=DURATION, timeout_s=900)
                    self.assertEqual(operator.count, 0)

    def test_long_terminal_archive_never_extends_ordinary_fault_worker_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); roster = self.fixture(root, json.dumps(REPORT).encode())
            self.retire_all(roster)
            with CampaignOperator(root, roster, ["fixture"]) as operator:
                with self.assertRaisesRegex(OperatorError, "^campaign_operator_deadline$"):
                    operator.run_worker("fixture-worker", "archive-reconcile", {},
                        elapsed_ns=DURATION*10**9, timeout_s=61)
                self.assertEqual(operator.count, 0)
                self.assertTrue(operator.failed)

    def test_closed_roster_archive_preserves_exact_exit_proof_and_original_clock(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); roster = self.fixture(root, json.dumps(REPORT).encode())
            self.retire_all(roster)
            with CampaignOperator(root, roster, ["fixture"]) as operator:
                self.assertEqual(operator.run_final_archive("fixture-worker", {},
                    elapsed_ns=DURATION*10**9, duration_s=DURATION, timeout_s=900), REPORT)
                self.assertGreaterEqual(operator.last_ns, DURATION*10**9)
                self.assertEqual(roster.children, {})
                self.assertEqual(roster.retired, 5)
                self.assertGreater(roster.roles["operator"]["rss_peak_upper_bound_bytes"], 0)
            record = json.loads((root/"campaign-operator.jsonl").read_bytes())
            self.assertEqual(record["command"], "worker.archive-reconcile")
            self.assertEqual(record["elapsed_ns"], DURATION*10**9)
            self.assertGreater(record["duration_ns"], 0)
            self.assertLessEqual(operator.last_ns, record["elapsed_ns"]+record["duration_ns"])
            self.assertFalse(operator.complete_campaign_checks_proved)

    def test_actual_offline_archive_worker_retains_wait4_peak_after_every_owner_retires(self):
        scripts = str(Path(__file__).resolve().parents[2] / "scripts")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code = '''import json,os,select,subprocess,sys,time
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess
from lib.ticketdesk_lifecycle_campaign_process_roster import CampaignProcessRoster
from lib.ticketdesk_lifecycle_campaign_operator import CampaignOperator
root=Path(sys.argv[2]);children={}
try:
 for role in ('server','load','observer','native_sampler'):
  child=ObservedProcess([sys.executable,'-c','import sys;print("ready",flush=True);sys.stdin.buffer.read()'],
   address_space_limit_bytes=128*1024**2,observation=root/(role+'.json'),
   stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  children[role]=child
  assert select.select([child.stdout],[],[],10)[0] and child.stdout.readline()==b'ready\\n'
 limits={'controller':256*1024**2,**{role:128*1024**2 for role in (*children,'operator')}}
 roster=CampaignProcessRoster(os.getpid(),children,limits)
 for role,child in children.items():
  child.stdin.close();assert child.wait(timeout=5)==0
  roster.retire(role,child,expected_exit=0)
  child.stdout.close();child.stderr.close()
 executable=root/'worker'
 report={'schema':'riffdb.ticketdesk-lifecycle-archive-reconciliation/v1',
  'records_checked':65857,'encoded_bytes':123,'physical_bytes':456,
  'payloads_reconciled':True,'policy_anchors_reconciled':True}
 executable.write_text('#!'+sys.executable+'\\nimport json,sys\\nassert sys.argv[1:]==["archive-reconcile"]\\nprint('+repr(json.dumps(report))+')\\n')
 executable.chmod(0o700)
 with CampaignOperator(root,roster,['unused']) as operator:
  result=operator.run_final_archive(str(executable),{},elapsed_ns=28800*10**9,duration_s=28800,timeout_s=900)
  assert result==report and operator.last_ns>=28800*10**9
 assert not roster.children and roster.retired==5
 assert roster.roles['operator']['rss_peak_upper_bound_bytes']>0
 print(json.dumps({'actual_children':5,'retired_owners':5,'qualified':False}))
finally:
 for child in children.values():
  if child.poll() is None:child.kill();child.wait(timeout=5)
'''
            owner = ObservedProcess([sys.executable, "-c", code, scripts, str(root)],
                address_space_limit_bytes=256*1024**2, observation=root/"outer.json",
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                self.assertEqual(owner.wait(timeout=60), 0, owner.stderr.read(4096).decode())
                self.assertEqual(json.loads(owner.stdout.read(4096)),
                    {"actual_children":5,"retired_owners":5,"qualified":False})
            finally:
                if owner.poll() is None: owner.kill(); owner.wait(timeout=5)
                owner.stdout.close(); owner.stderr.close()
