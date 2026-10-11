# req: END-003, END-007, END-008, END-010
"""Arithmetic progress fixtures never claim an actual database campaign."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from lib.ticketdesk_lifecycle_campaign_progress_reconciliation import reconcile,read_rows,ProgressReconciliationError

SECOND=10**9
DURATION=28800
IDENTITY=dict(projection_id=1,projection_plan_hash="a"*64)


def observer(elapsed):
    commands=elapsed*100
    return dict(schema="riffdb.ticketdesk-lifecycle-observation/v1",elapsed_s=elapsed+300,
        events_checked=1024+commands//65536*1024+min(commands%65536,1024),duplicates=0,
        consumer_checkpoint=elapsed*100+1,live_head=elapsed*100+65536,live_updates=elapsed//20+1,
        live_renewals=elapsed//20,history_incarnation=1 if elapsed<5400 else 2,
        release_generation=1 if elapsed<5400 else 6,auxiliary_slots=2,
        consumer_paused=False,observer_quiesced=False)


def native(elapsed):
    base=0 if elapsed<3600 else (3600 if elapsed<5400 else 5400)
    return dict(schema="riffdb.ticketdesk-lifecycle-native-observation/v1",index=(elapsed-base)//60,
        contract_version=1 if elapsed<3600 else 2,sampled_head=elapsed*100+65536,
        statistics_head=elapsed*100+65536,outbox_backlog=0,outbox_degraded=False,
        history_incarnation=1 if elapsed<5400 else 2,applied_through=elapsed*100+65536,
        projection_generation=5 if elapsed<5400 else 1,projection_backlog=0,**IDENTITY,
        transport_attempts=3,retries=0,auxiliary_slots=1,elapsed_ns=elapsed*SECOND)


class CampaignProgressReconciliationTests(unittest.TestCase):
    def fixture(self):
        self.observers=[dict(schema="riffdb.ticketdesk-lifecycle-campaign-observer/v1",
            elapsed_ns=i*SECOND,observation_age_ns=0,observation=observer(i)) for i in range(0,DURATION,60)]
        self.native=[native(i) for i in range(0,DURATION,60)]
        self.options=dict(duration_s=DURATION,identity=IDENTITY,terminal_observer=observer(DURATION),
            terminal_native=native(DURATION))

    def test_independent_coverage_rejects_a_gap_inside_an_otherwise_covered_window(self):
        self.fixture();self.observers.pop(2)
        with self.assertRaisesRegex(ProgressReconciliationError,"campaign_progress_gap"):
            reconcile(self.observers,self.native,**self.options)

    def test_original_clock_and_exact_terminal_progress_preserve_sparse_matching_event_windows(self):
        self.fixture();value=reconcile(self.observers,self.native,**self.options)
        self.assertEqual(len(value["windows"]),96)
        self.assertEqual(value["exact_consumer_events"],observer(DURATION)["events_checked"])
        self.assertTrue(any(a["events_checked"]==b["events_checked"]
            for a,b in zip(value["windows"],value["windows"][1:])))
        self.assertFalse(value["qualifies_complete_campaign"])

    def test_native_gaps_and_owner_local_index_discontinuity_refuse(self):
        for fault in ("gap","index","incarnation","frontier","generation","boolean","extra"):
            with self.subTest(fault=fault):
                self.fixture()
                if fault=="gap":self.native.pop(2)
                if fault=="index":self.native[2]["index"]=9
                if fault=="incarnation":self.native[2]["history_incarnation"]=3
                if fault=="frontier":self.native[2]["sampled_head"]=1
                if fault=="generation":self.native[2]["projection_generation"]=1
                if fault=="boolean":self.native[2]["transport_attempts"]=True
                if fault=="extra":self.native[2]["extra"]=0
                with self.assertRaises(ProgressReconciliationError):reconcile(self.observers,self.native,**self.options)

    def test_rewound_or_stale_observer_and_missing_terminal_drain_refuse(self):
        for fault in ("events","age","live","event_count","backlog","terminal_time","terminal_incarnation"):
            with self.subTest(fault=fault):
                self.fixture()
                if fault=="events":self.observers[2]["observation"]["events_checked"]=0
                if fault=="age":self.observers[2]["observation_age_ns"]=5*SECOND+1
                if fault=="live":self.options["terminal_observer"]["live_renewals"]=0
                if fault=="event_count":self.options["terminal_observer"]["events_checked"]-=1
                if fault=="backlog":self.options["terminal_native"]["outbox_backlog"]=1
                if fault=="terminal_time":self.options["terminal_native"]["elapsed_ns"]-=1
                if fault=="terminal_incarnation":self.options["terminal_native"]["history_incarnation"]=1
                with self.assertRaises(ProgressReconciliationError):reconcile(self.observers,self.native,**self.options)

    def test_distinct_native_and_live_progress_must_continue_through_normal_windows(self):
        self.fixture()
        for row in self.observers:
            if row["elapsed_ns"]>=300*SECOND:row["observation"]["live_renewals"]=1
        with self.assertRaises(ProgressReconciliationError):reconcile(self.observers,self.native,**self.options)

    def test_closed_private_rows_bind_bytes_and_refuse_duplicate_or_partial_records(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);path=root/"progress.jsonl";path.write_bytes(b'{"value":1}\n');path.chmod(0o600)
            rows,identity=read_rows(root,"progress.jsonl",maximum=4096,rows=2)
            self.assertEqual(rows,[{"value":1}]);self.assertEqual(identity["bytes"],12)
            for data in (b'{"x":1,"x":2}\n',b'{"x":1}',b'{}\n'*3,b'x'*4097):
                path.write_bytes(data)
                with self.assertRaises(ValueError):read_rows(root,"progress.jsonl",maximum=4096,rows=2)


if __name__=="__main__":unittest.main()
