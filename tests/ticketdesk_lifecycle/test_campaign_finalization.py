# req: END-003, END-007, END-008, END-009, END-010
"""Closed terminal report fixtures supply no database or timing qualification."""
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from lib.ticketdesk_lifecycle_campaign_finalization import (
    checked_terminal, checked_observer, checked_projection, read_terminal,
)

DURATION=28800
IDENTITY={"projection_id":1,"projection_plan_hash":"a"*64}


def terminal():
    return dict(schema="riffdb.ticketdesk-lifecycle-terminal/v1", generation=6,
        contract_version=2,history_incarnation=2,completed_commands=DURATION*100,
        completed_reads=DURATION*400)


def projection():
    count=DURATION*100//65536*1024+min(DURATION*100%65536,1024)
    frontier={"type":"applied_through","sequence":"3000000"}
    return {"schema":"riffdb.cli.output/v1","command":"projection.query","ok":True,
        "result":{"status":"ready","observed_fence":{"identity":dict(
            contract_lineage="TicketDeskLifecycle",**IDENTITY),"generation":"7","frontier":frontier},
            "frontier":frontier,"rows":[{"group":[{"type":"uuid","value":"01900000-0000-7000-8001-000000000000"}],
                "values":{"fields":[{"field_id":1,"value":{"type":"i64","value":str(count)}}]}}]}}


class CampaignFinalizationTests(unittest.TestCase):
    def test_exact_completed_original_phase_is_required_without_numeric_booleans(self):
        options=dict(generation=6,version=2,incarnation=2,duration_s=DURATION)
        self.assertEqual(checked_terminal(terminal(),**options),terminal())
        changes=[{"generation":5},{"contract_version":1},{"history_incarnation":1},
                 {"completed_commands":DURATION*100-1},{"completed_reads":DURATION*400-1},
                 {"completed_commands":True},{"extra":True}]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ValueError):
                checked_terminal(dict(terminal(),**change),**options)
        with self.assertRaises(ValueError):checked_terminal(terminal(),**dict(options,generation=True))

    def test_terminal_file_requires_bounded_immutable_regular_custody_and_closed_json(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);runtime=SimpleNamespace(root=root,generation=6,version=2,incarnation=2,duration=DURATION)
            path=root/"load.terminal";path.write_text(json.dumps(terminal())+"\n")
            self.assertEqual(read_terminal(runtime),terminal())
            for encoded in ('{"schema":"x","schema":"x"}\n','x'*4097,json.dumps(terminal())):
                path.write_text(encoded)
                with self.assertRaises(ValueError):read_terminal(runtime)
            path.unlink();path.symlink_to(root/"absent")
            with self.assertRaises(OSError):read_terminal(runtime)

    def test_consumer_target_uses_one_organization_and_requires_actual_live_progress(self):
        completed=65536+123
        value=dict(events_checked=1024+1024+123,duplicates=0,release_generation=6,
            history_incarnation=2,observer_quiesced=False,consumer_paused=False,
            live_head=90000,live_updates=3,live_renewals=4)
        options=dict(completed=completed,generation=6,incarnation=2,head=90000)
        self.assertTrue(checked_observer(value,**options))
        for change in ({"events_checked":value["events_checked"]-1},{"live_head":89999},
                       {"live_updates":0},{"live_renewals":0}):
            self.assertFalse(checked_observer(dict(value,**change),**options))
        for change in ({"events_checked":65536+completed},{"duplicates":1},
                       {"history_incarnation":1},{"observer_quiesced":True},
                       {"live_updates":True},{"consumer_paused":True}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                checked_observer(dict(value,**change),**options)

    def test_projection_reconciles_actual_count_identity_and_public_fence(self):
        options=dict(head=3000000,completed=DURATION*100)
        checked_projection(projection(),IDENTITY,**options)
        for kind in ("count","organization","frontier","plan","cursor","boolean_id","boolean_field","leading_zero"):
            value=deepcopy(projection());result=value["result"]
            if kind=="count":result["rows"][0]["values"]["fields"][0]["value"]["value"]="0"
            elif kind=="organization":result["rows"][0]["group"][0]["value"]="01900000-0000-7000-8001-000000000001"
            elif kind=="frontier":result["frontier"]["sequence"]="2999999"
            elif kind=="plan":result["observed_fence"]["identity"]["projection_plan_hash"]="b"*64
            elif kind=="cursor":result["next_cursor"]="unexpected"
            elif kind=="boolean_id":result["observed_fence"]["identity"]["projection_id"]=True
            elif kind=="boolean_field":result["rows"][0]["values"]["fields"][0]["field_id"]=True
            else:result["observed_fence"]["generation"]="07"
            with self.subTest(kind=kind), self.assertRaises(ValueError):checked_projection(value,IDENTITY,**options)

    def test_projection_malformed_shapes_refuse_before_reading_nested_values(self):
        for value in ({},dict(projection(),result=None),dict(projection(),result={}),
                      dict(projection(),result=dict(projection()["result"],observed_fence=[]))):
            with self.subTest(value_type=type(value).__name__), self.assertRaises(ValueError):
                checked_projection(value,IDENTITY,head=3000000,completed=DURATION*100)
