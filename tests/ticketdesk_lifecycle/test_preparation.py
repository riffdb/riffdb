# req: END-003, END-007, END-008
"""Preparation observations remain independent of producer success booleans."""
import copy
import importlib.machinery
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[2] / "scripts/ticketdesk-lifecycle-smoke"
loader = importlib.machinery.SourceFileLoader("preparation", str(SOURCE))
spec = importlib.util.spec_from_loader(loader.name, loader)
preparation = importlib.util.module_from_spec(spec)
loader.exec_module(preparation)


def observation():
    return {"schema": "riffdb.cli.output/v1", "command": "projection.query", "ok": True,
            "result": {"status": "ready", "rows": [{"group": [
                {"type": "uuid", "value": "01900000-0000-7000-8001-000000000000"}],
                "values": {"fields": [{"field_id": 1, "value": {"type": "i64", "value": "1"}}]}}],
                "observed_fence": {"identity": {"contract_lineage": "TicketDeskLifecycle",
                    "projection_id": 1, "projection_plan_hash": "7da25538637dd8c1d35f650daed706a83743a7bf6d7deb18191e7972170bc0d6"},
                    "generation": "1", "frontier": {"type": "applied_through", "sequence": "7"}},
                "frontier": {"type": "applied_through", "sequence": "7"}}}


class ProjectionReconciliation(unittest.TestCase):
    def test_ready_does_not_prove_the_count_or_partition_or_identity(self):
        good = observation()
        preparation.validate_projection(good, 1, 7, 1, good["result"]["observed_fence"]["identity"]["projection_plan_hash"])
        for mutation in ("count", "partition", "identity", "frontier", "field"):
            with self.subTest(mutation=mutation):
                value = copy.deepcopy(good)
                result = value["result"]
                if mutation == "count":
                    result["rows"][0]["values"]["fields"][0]["value"]["value"] = "2"
                elif mutation == "partition":
                    result["rows"][0]["group"][0]["value"] = "01900000-0000-7000-8001-000000000001"
                elif mutation == "identity":
                    result["observed_fence"]["identity"]["projection_id"] = 2
                elif mutation == "frontier":
                    result["frontier"]["sequence"] = "6"
                else:
                    result["rows"][0]["values"]["fields"][0]["field_id"] = 2
                with self.assertRaises(preparation.SmokeError):
                    preparation.validate_projection(value, 1, 7, 1, good["result"]["observed_fence"]["identity"]["projection_plan_hash"])

    def test_partial_population_round_counts_only_organization_zero_updates(self):
        value = observation()
        plan_hash = value["result"]["observed_fence"]["identity"]["projection_plan_hash"]
        value["result"]["rows"][0]["values"]["fields"][0]["value"]["value"] = "1024"
        preparation.validate_projection(value, 1, 7, 30000, plan_hash)
        value["result"]["rows"][0]["values"]["fields"][0]["value"]["value"] = "1025"
        preparation.validate_projection(value, 1, 7, 65537, plan_hash)
        with self.assertRaises(preparation.SmokeError):
            preparation.validate_projection(value, 1, 7, 65536, plan_hash)


class ConsumerOutageReconciliation(unittest.TestCase):
    def sample(self, *, paused=True, events=1000, checkpoint=2000, head=3000, updates=20):
        return {"consumer_paused": paused, "events_checked": events,
                "consumer_checkpoint": checkpoint, "live_head": head,
                "live_updates": updates, "duplicates": 0, "history_incarnation": 1,
                "auxiliary_slots": 2}

    def test_pause_requires_frozen_consumer_and_independent_live_progress(self):
        oracle = preparation.ConsumerOutageOracle()
        oracle.observe(self.sample(), paused=True)
        oracle.observe(self.sample(head=4000, updates=30), paused=True)
        oracle.finish_pause()
        for changed in (self.sample(events=1001), self.sample(checkpoint=2001),
                        self.sample(paused=False)):
            with self.subTest(changed=changed):
                with self.assertRaises(preparation.SmokeError):
                    oracle.observe(changed, paused=True)

    def test_a_missing_or_inactive_watch_cannot_prove_an_outage(self):
        oracle = preparation.ConsumerOutageOracle()
        with self.assertRaises(preparation.SmokeError):
            oracle.finish_pause()
        oracle.observe(self.sample(), paused=True)
        oracle.observe(self.sample(), paused=True)
        with self.assertRaises(preparation.SmokeError):
            oracle.finish_pause()

    def test_recovery_must_drain_real_missing_events_with_live_frontier_preserved(self):
        oracle = preparation.ConsumerOutageOracle()
        oracle.observe(self.sample(), paused=True)
        oracle.observe(self.sample(head=4000, updates=30), paused=True)
        oracle.finish_pause()
        self.assertFalse(oracle.recovered(self.sample(paused=False, head=4000, updates=30), 2048))
        self.assertTrue(oracle.recovered(self.sample(paused=False, events=2048,
                        checkpoint=5000, head=6000, updates=40), 2048))
        with self.assertRaises(preparation.SmokeError):
            oracle.recovered(self.sample(paused=False, events=2049), 2048)

    def test_only_windows_overlapping_the_measured_outage_and_drain_are_exempt(self):
        outage = {"start_elapsed_ns": 6000000000, "pause_elapsed_ns": 300000000000,
                  "recovery_elapsed_ns": 14000000000}
        self.assertTrue(preparation.outage_window(0, 300, outage))
        self.assertTrue(preparation.outage_window(300, 600, outage))
        self.assertFalse(preparation.outage_window(600, 900, outage))
        outage["start_elapsed_ns"] = 300000000000
        self.assertFalse(preparation.outage_window(0, 300, outage))
        self.assertTrue(preparation.outage_window(300, 600, outage))
        for field in ("pause_elapsed_ns", "recovery_elapsed_ns"):
            invalid = dict(outage)
            invalid[field] = 1000000000000
            with self.assertRaises(preparation.SmokeError):
                preparation.outage_window(0, 300, invalid)


class ChildOwnership(unittest.TestCase):
    def test_invalid_or_oversized_load_output_stops_the_owned_child(self):
        for output in ('{"schema":"unknown"}', 'x' * 8192):
            with self.subTest(bytes=len(output)), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                worker = root / "fixture-worker"
                worker.write_text("#!/usr/bin/env python3\nimport os, signal\n"
                    "from pathlib import Path\n"
                    "Path(__file__).with_name('pid').write_text(str(os.getpid()))\n"
                    f"print({output!r}, flush=True)\nsignal.pause()\n")
                worker.chmod(0o700)
                with self.assertRaises(preparation.SmokeError):
                    preparation.observed_load(root, str(worker), dict(os.environ), 1,
                                              None, preparation.queue.Queue(maxsize=1), False)
                pid = int((root / "pid").read_text())
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)


if __name__ == "__main__":
    unittest.main()
