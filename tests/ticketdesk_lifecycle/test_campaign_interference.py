# req: END-007, END-008, END-010
"""Controlled process census fixtures are not measured campaign evidence."""
import json
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from lib.ticketdesk_lifecycle_campaign_interference import CampaignInterferenceSamples,census,reconcile,InterferenceError,POLICY

SECOND=10**9


class CampaignInterferenceTests(unittest.TestCase):
    def fixture(self,root):
        proc=root/"proc";proc.mkdir(mode=0o700)
        boot=proc/"sys/kernel/random";boot.mkdir(parents=True)
        (boot/"boot_id").write_text("00000000-0000-0000-0000-000000000001\n")
        self.process(proc,10,"python3",1)
        roster=SimpleNamespace(_check=lambda:None,sample=lambda elapsed:None,
            owners={"controller":SimpleNamespace(pid=10)})
        return proc,roster

    def process(self,proc,pid,name,parent):
        path=proc/str(pid);path.mkdir(mode=0o700)
        fields=["S",str(parent),*(["0"]*17),"100",*(["0"]*32)]
        (path/"stat").write_text(f"{pid} ({name}) "+" ".join(fields)+"\n")

    def test_competing_database_owner_is_recorded_then_refuses_the_actual_census(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);proc,roster=self.fixture(root);self.process(proc,20,"riffdbd",1)
            with CampaignInterferenceSamples(root,roster,duration_s=21900,proc_root=proc) as samples:
                with self.assertRaisesRegex(InterferenceError,"campaign_interference_detected"):
                    samples.poll(0)
                row=json.loads((root/"campaign-interference.jsonl").read_text())
                self.assertEqual(len(row["competing_workloads"]),1)
                self.assertFalse(row["qualifies_complete_campaign"])

    def test_exact_owned_workers_are_allowed_but_foreign_compilers_and_benchmarks_are_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);proc,roster=self.fixture(root)
            self.process(proc,20,"riffdbd",10);self.process(proc,30,"rustc",1)
            self.process(proc,40,"checkpoint",1)
            value=census(proc,{10,20})
            self.assertEqual(value["owned_processes"],2)
            self.assertEqual({row["pid"] for row in value["competing_workloads"]},{30,40})

    def test_changed_boot_or_original_clock_gap_refuses_stickily(self):
        for fault in ("boot","gap","rewind","missing_owner"):
            with self.subTest(fault=fault),tempfile.TemporaryDirectory() as directory:
                root=Path(directory);proc,roster=self.fixture(root)
                with CampaignInterferenceSamples(root,roster,duration_s=21900,proc_root=proc) as samples:
                    samples.poll(SECOND)
                    elapsed=6*SECOND
                    if fault=="boot":(proc/"sys/kernel/random/boot_id").write_text("00000000-0000-0000-0000-000000000002\n")
                    if fault=="gap":elapsed=77*SECOND
                    if fault=="rewind":elapsed=0
                    if fault=="missing_owner":(proc/"10/stat").unlink()
                    with self.assertRaises(InterferenceError):samples.poll(elapsed)
                    with self.assertRaisesRegex(InterferenceError,"campaign_interference_state"):samples.poll(7*SECOND)

    def rows(self):
        return [dict(schema="riffdb.ticketdesk-lifecycle-interference/v1",elapsed_ns=i*SECOND,
            policy=POLICY,boot_identity_sha256="a"*64,processes_seen=10,owned_processes=5,
            competing_workloads=[],disappeared_during_census=0,qualifies_complete_campaign=False)
            for i in range(0,21900,60)]

    def test_independent_check_requires_every_window_and_preserves_the_disclosed_policy(self):
        rows=self.rows();value=reconcile(rows,duration_s=21900)
        self.assertEqual(len(value["window_samples"]),73)
        self.assertEqual(value["policy"],POLICY);self.assertFalse(value["qualifies_complete_campaign"])
        for fault in ("gap","competing","boot","boolean","policy","inventory"):
            with self.subTest(fault=fault):
                rows=self.rows()
                if fault=="gap":rows.pop(2)
                if fault=="competing":rows[2]["competing_workloads"]=[{}]
                if fault=="boot":rows[2]["boot_identity_sha256"]="b"*64
                if fault=="boolean":rows[2]["processes_seen"]=True
                if fault=="policy":rows[2]["policy"]="unknown"
                if fault=="inventory":rows[2]["owned_processes"]=11
                with self.assertRaises(InterferenceError):reconcile(rows,duration_s=21900)


if __name__=="__main__":unittest.main()
