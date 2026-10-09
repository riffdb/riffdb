# req: END-003, END-007, END-008
"""Preparation observations remain independent of producer success booleans."""
import copy
import importlib.machinery
import importlib.util
import json
import os
import select
import signal
import subprocess
import sys
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

SOURCE = Path(__file__).resolve().parents[2] / "scripts/ticketdesk-lifecycle-smoke"
sys.path.insert(0, str(SOURCE.parent))
loader = importlib.machinery.SourceFileLoader("preparation", str(SOURCE))
spec = importlib.util.spec_from_loader(loader.name, loader)
preparation = importlib.util.module_from_spec(spec)
loader.exec_module(preparation)


class DriverShutdownDiagnostics(unittest.TestCase):
    def stop(self, root, *, status, timed_out=False):
        driver = Mock()
        driver.poll.return_value = None
        driver.returncode = status
        driver.wait.side_effect = ([subprocess.TimeoutExpired("owned-driver", 15), status]
                                  if timed_out else [status])
        with patch.object(preparation.time, "monotonic_ns", side_effect=[100, 500]):
            preparation.stop_driver(root, driver)
        return driver

    def test_clean_shutdown_records_only_closed_numeric_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            driver = self.stop(root, status=0)
            value = json.loads((root / "driver-shutdown.json").read_text())
            self.assertEqual(value, {
                "schema": "riffdb.ticketdesk-lifecycle-driver-shutdown/v1",
                "timed_out": False, "exit_code": 0, "elapsed_ns": 400})
            driver.terminate.assert_called_once_with()
            driver.kill.assert_not_called()
            self.assertNotIn(str(root), json.dumps(value))

    def test_signal_exit_and_timeout_remain_failures_after_diagnostic_publication(self):
        for status, timed_out in [(-15, False), (1, False), (-9, True), (0, True)]:
            with self.subTest(status=status, timed_out=timed_out), \
                    tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                with self.assertRaisesRegex(preparation.SmokeError, "^driver_shutdown$"):
                    self.stop(root, status=status, timed_out=timed_out)
                value = json.loads((root / "driver-shutdown.json").read_text())
                self.assertEqual(value["timed_out"], timed_out)
                self.assertEqual(value["exit_code"], status)
                self.assertEqual(value["elapsed_ns"], 400)


class DriverFailurePreservation(unittest.TestCase):
    def test_startup_refusal_is_not_replaced_by_cleanup_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            driver = Mock()
            driver.poll.return_value = 1
            driver.returncode = 1
            driver.wait.return_value = 1
            with self.assertRaisesRegex(preparation.SmokeError, "^driver_startup$"):
                with preparation.owned_driver(root, driver):
                    raise preparation.SmokeError("driver_startup")
            value = json.loads((root / "driver-shutdown.json").read_text())
            self.assertEqual(value["exit_code"], 1)
            self.assertFalse(value["timed_out"])

    def test_successful_body_still_fails_if_driver_cleanup_refuses(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            driver = Mock()
            driver.poll.return_value = 1
            driver.returncode = 1
            driver.wait.return_value = 1
            with self.assertRaisesRegex(preparation.SmokeError, "^driver_shutdown$"):
                with preparation.owned_driver(root, driver):
                    pass


class PreparationDurationBoundaries(unittest.TestCase):
    def invoke_invalid_owned_root(self, duration, faults):
        arguments = ["ticketdesk-lifecycle-smoke", "--root", "/", "--server", sys.executable,
                     "--cli", sys.executable, "--worker", sys.executable, "--observe",
                     "--population-probe-seconds", str(duration), *faults]
        with patch.object(sys, "argv", arguments):
            preparation.main()

    def test_extended_outage_preparation_reaches_owned_root_validation(self):
        for duration in (900, 1500):
            for fault in ("--sink-outage", "--consumer-outage"):
                with self.subTest(duration=duration, fault=fault):
                    with self.assertRaisesRegex(preparation.SmokeError, "^root$"):
                        self.invoke_invalid_owned_root(duration, [fault])

    def test_extended_duration_remains_outage_only_and_bounded(self):
        for duration, faults in ((1500, []), (1501, ["--sink-outage"]),
                                 (86400, ["--sink-outage"])):
            with self.subTest(duration=duration, faults=faults):
                with self.assertRaisesRegex(preparation.SmokeError, "^probe_duration$"):
                    self.invoke_invalid_owned_root(duration, faults)

    def test_worst_permitted_recovery_leaves_a_whole_normal_window(self):
        outage = {"start_elapsed_ns": 5000000000, "pause_elapsed_ns": 302000000000,
                  "recovery_elapsed_ns": 600000000000}
        self.assertTrue(all(preparation.outage_window(start, start + 300, outage)
                            for start in (0, 300, 600)))
        self.assertTrue(preparation.outage_window(900, 1200, outage))
        self.assertFalse(preparation.outage_window(1200, 1500, outage))
        outage["recovery_elapsed_ns"] += 1
        with self.assertRaisesRegex(preparation.SmokeError, "^consumer_outage_timing$"):
            preparation.outage_window(1200, 1500, outage)


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


class ProjectionSampling(unittest.TestCase):
    def test_samples_real_frontier_without_waiting_for_it_to_catch_up(self):
        projection = observation()
        health = {"schema": "riffdb.cli.output/v1", "command": "server.health", "ok": True,
                  "result": {"last_commit_sequence": "10"}}
        identity = projection["result"]["observed_fence"]["identity"]
        calls = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def invoke(root, name, arguments, **kwargs):
                calls.append(arguments)
                value = health if arguments[-2:] == ["server", "health"] else projection
                (root / (name + ".stdout")).write_text(json.dumps(value))

            with patch.object(preparation, "invoke", invoke):
                sampler = preparation.ProjectionSampler(root, ["cli"], identity, 300)
                sample = sampler.sample(1, 1000000000)
            self.assertEqual(sample["projection_backlog"], 3)
            self.assertEqual(sample["sampled_head"], 10)
            self.assertEqual(sample["applied_through"], 7)
            self.assertEqual(len(calls), 2)
            self.assertNotIn("--after", calls[1])
            self.assertNotIn("--wait-nanos", calls[1])

    def test_every_load_window_needs_an_actual_projection_observation(self):
        identity = observation()["result"]["observed_fence"]["identity"]
        with tempfile.TemporaryDirectory() as directory:
            sampler = preparation.ProjectionSampler(Path(directory), ["cli"], identity, 900)
            with self.assertRaises(preparation.SmokeError):
                sampler.validate_coverage()
            sampler.coverage = [1, 1, 0]
            with self.assertRaises(preparation.SmokeError):
                sampler.validate_coverage()
            sampler.coverage = [1, 1, 1]
            sampler.validate_coverage()

    def test_stopping_during_an_admitted_sample_joins_the_owned_observer(self):
        projection = observation()
        identity = projection["result"]["observed_fence"]["identity"]
        entered, release = threading.Event(), threading.Event()
        calls = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def invoke(root, name, arguments, **kwargs):
                calls.append(arguments)
                if len(calls) == 1:
                    entered.set()
                    self.assertTrue(release.wait(5))
                value = ({"schema": "riffdb.cli.output/v1", "command": "server.health",
                          "ok": True, "result": {"last_commit_sequence": "10"}}
                         if len(calls) == 1 else projection)
                (root / (name + ".stdout")).write_text(json.dumps(value))

            with patch.object(preparation, "invoke", invoke):
                sampler = preparation.ProjectionSampler(root, ["cli"], identity, 300)
                sampler.start(time.monotonic())
                try:
                    self.assertTrue(entered.wait(5))
                    sampler.stopped.set()
                finally:
                    release.set()
                    sampler.stop()
            self.assertFalse(sampler.thread.is_alive())
            rows = (root / "projection-samples.jsonl").read_text().splitlines()
            self.assertEqual(len(rows), 1)
            self.assertEqual(json.loads(rows[0])["projection_backlog"], 3)
            sampler.validate_coverage()

    def test_failed_native_observation_stops_the_load_instead_of_becoming_zero(self):
        identity = observation()["result"]["observed_fence"]["identity"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(preparation, "invoke", side_effect=preparation.SmokeError("fixture")):
                sampler = preparation.ProjectionSampler(root, ["cli"], identity, 300)
                sampler.start(time.monotonic())
                sampler.thread.join(timeout=5)
                self.assertFalse(sampler.thread.is_alive())
                with self.assertRaises(preparation.SmokeError):
                    sampler.poll()
                with self.assertRaises(preparation.SmokeError):
                    sampler.stop()
            self.assertEqual((root / "projection-samples.jsonl").read_bytes(), b"")


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


class SinkOutageReconciliation(unittest.TestCase):
    def health(self, *, ready=False, frontier=100):
        return {"schema": "riffdb.cli.output/v1", "command": "server.health", "ok": True,
                "result": {"last_commit_sequence": str(frontier), "components": [
                    {"component": "outbox", "status": "healthy" if ready else "degraded"}]}}

    def live(self, *, head=100, updates=10):
        return {"live_head": head, "live_updates": updates, "auxiliary_slots": 2,
                "history_incarnation": 1, "observer_quiesced": False}

    def test_denied_access_alone_cannot_prove_a_live_production_outage(self):
        oracle = preparation.SinkOutageOracle()
        with self.assertRaises(preparation.SmokeError):
            oracle.finish_pause()
        with self.assertRaises(preparation.SmokeError):
            oracle.observe(self.health(ready=True), self.live())
        oracle.observe(self.health(), self.live())
        oracle.observe(self.health(), self.live())
        with self.assertRaises(preparation.SmokeError):
            oracle.finish_pause()

    def test_recovery_requires_actual_drained_health_and_preserved_live_progress(self):
        oracle = preparation.SinkOutageOracle()
        oracle.observe(self.health(), self.live())
        oracle.observe(self.health(frontier=200), self.live(head=200, updates=20))
        oracle.finish_pause()
        self.assertFalse(oracle.recovered(self.health(frontier=200)))
        self.assertTrue(oracle.recovered(self.health(ready=True, frontier=200)))
        with self.assertRaises(preparation.SmokeError):
            oracle.recovered(self.health(ready=True, frontier=99))

    def test_changed_incarnation_or_stopped_live_query_cannot_count_as_progress(self):
        for changed in ({"history_incarnation": 2}, {"observer_quiesced": True},
                        {"auxiliary_slots": 3}, {"live_head": 0}):
            with self.subTest(changed=changed):
                oracle = preparation.SinkOutageOracle()
                oracle.observe(self.health(), self.live())
                live = self.live(head=200, updates=20)
                live.update(changed)
                with self.assertRaises(preparation.SmokeError):
                    oracle.observe(self.health(frontier=200), live)

    def test_sink_health_requires_the_native_canonical_decimal_sequence(self):
        for frontier in (True, 100, 1.0, None, "0100", "-1", str(2**64)):
            with self.subTest(frontier=frontier):
                oracle = preparation.SinkOutageOracle()
                oracle.observe(self.health(), self.live())
                health = self.health(frontier=200)
                health["result"]["last_commit_sequence"] = frontier
                with self.assertRaises(preparation.SmokeError):
                    oracle.observe(health, self.live(head=200, updates=20))
                with self.assertRaises(preparation.SmokeError):
                    oracle.recovered(health)


class ChildOwnership(unittest.TestCase):
    def test_observed_load_collects_owned_process_memory_without_an_extra_request_thread(self):
        for native in (False, True):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                children = []
                try:
                    for _role in (("server", "observer", "native_sampler") if native else ("server", "observer")):
                        child = subprocess.Popen([sys.executable, "-c",
                            "import sys; print('ready', flush=True); sys.stdin.buffer.read()"],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        children.append(child)
                        self.assertTrue(select.select([child.stdout], [], [], 5)[0])
                        self.assertEqual(child.stdout.readline(), b"ready\n")
                    worker = root / "fixture-worker"
                    worker.write_text("#!/usr/bin/env python3\nimport signal, sys\n"
                        "def done(*_args):\n"
                        " print('{\"schema\":\"riffdb.ticketdesk-lifecycle-load-stopped/v1\"}', flush=True)\n"
                        " sys.exit(0)\n"
                        "signal.signal(signal.SIGUSR1, done)\n"
                        "print('{\"schema\":\"riffdb.ticketdesk-lifecycle-load-ready/v1\"}', flush=True)\n"
                        "signal.pause()\n")
                    worker.chmod(0o700)
                    original = preparation.PreparationMemorySamples.poll
                    released = False

                    def release_after_observation(samples, elapsed_ns):
                        nonlocal released
                        original(samples, elapsed_ns)
                        if samples.count == 1 and not released:
                            released = True
                            os.kill(samples.inventory.owners["load"].pid, signal.SIGUSR1)

                    # These child fixtures produce process memory, not native pool diagnostics.
                    # Keep the real process-memory oracle; pool semantics have independent tests.
                    with patch.object(preparation.PreparationMemorySamples, "poll", release_after_observation), \
                            patch.object(preparation, "ProviderLog", autospec=True) as provider_log, \
                            patch.object(preparation, "SourceBytesLog", autospec=True) as source_log, \
                            patch.object(preparation, "LogicalBytesLog", autospec=True) as logical_log, \
                            patch.object(preparation, "PhysicalBytesLog", autospec=True) as physical_log:
                        provider_log.return_value.finish.return_value = {}
                        source_log.return_value.finish.return_value = {"qualifies_complete_resource_model": False}
                        logical_log.return_value.finish.return_value = {"qualifies_complete_resource_model": False}
                        physical_log.return_value.finish.return_value = {"qualifies_complete_resource_model": False}
                        sampler = Mock(spec=preparation.NativeProjectionSampler) if native else None
                        if sampler is not None:
                            sampler.native = Mock(process=children[2], samples=Mock(last={"history_incarnation": 7}))
                        preparation.observed_load(root, str(worker), dict(os.environ), 1,
                            children[1], preparation.queue.Queue(maxsize=1), False, server=children[0],
                            projection=sampler, collect_source_bytes=native)
                        self.assertTrue(provider_log.return_value.poll.called)
                        provider_log.return_value.__exit__.assert_called_once()
                        if native:
                            self.assertEqual(source_log.call_args.args[2](), 7)
                            self.assertTrue(source_log.return_value.poll.called)
                            source_log.return_value.__exit__.assert_called_once()
                            summary = json.loads((root / "source-byte-observation.json").read_text())
                            self.assertFalse(summary["qualifies_complete_resource_model"])
                            self.assertEqual(physical_log.call_args.args[2](), 7)
                            self.assertTrue(physical_log.return_value.poll.called)
                            physical_log.return_value.__exit__.assert_called_once()
                            self.assertFalse(json.loads((root / "physical-byte-observation.json").read_text())["qualifies_complete_resource_model"])
                            self.assertEqual(logical_log.call_args.args[2](), 7)
                            self.assertTrue(logical_log.return_value.poll.called)
                            logical_log.return_value.__exit__.assert_called_once()
                            summary = json.loads((root / "logical-byte-observation.json").read_text())
                            self.assertFalse(summary["qualifies_complete_resource_model"])
                        else:
                            source_log.assert_not_called()
                            logical_log.assert_not_called()
                    records = [json.loads(line) for line in (root / "memory-samples.jsonl").read_bytes().splitlines()]
                    self.assertEqual(len(records), 1)
                    roles = {"controller", "server", "load", "observer"} | ({"native_sampler"} if native else set())
                    self.assertEqual(records[0]["processes"].keys(), roles)
                    if native:
                        self.assertEqual(records[0]["scope"], "verified_owned_descendant_tree_at_sample")
                        self.assertGreater(records[0]["rss_peak_upper_bound_bytes"], 0)
                    self.assertIs(records[0]["qualifies_resource_model"], False)
                    self.assertTrue(all(value["rss_bytes"] > 0 for value in records[0]["processes"].values()))
                finally:
                    for child in children:
                        child.communicate(timeout=5)

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


    def test_resource_refusal_survives_cleanup_failure_and_reaps_the_load_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            worker = root / "fixture-worker"
            worker.write_text("#!/usr/bin/env python3\nimport os,signal\n"
                "from pathlib import Path\n"
                "Path(__file__).with_name('pid').write_text(str(os.getpid()))\n"
                "print('{\"schema\":\"riffdb.ticketdesk-lifecycle-load-ready/v1\"}', flush=True)\n"
                "signal.pause()\n")
            worker.chmod(0o700)
            refusal = preparation.ResourceError("resource_process_inventory",
                reason="task_census_exhausted", role="server", census_attempts=3)
            server, observer = Mock(pid=124), Mock(pid=126)
            server.poll.return_value = observer.poll.return_value = None
            projection = Mock()
            projection.stop.side_effect = preparation.SmokeError("projection_sample_shutdown")
            with patch.object(preparation, "PreparationMemorySamples", autospec=True) as memory, \
                    patch.object(preparation, "ProviderLog", autospec=True):
                memory.return_value.poll.side_effect = refusal
                with self.assertRaisesRegex(preparation.SmokeError, "^projection_sample_shutdown$"):
                    preparation.observed_load(root, str(worker), dict(os.environ), 1,
                        observer, preparation.queue.Queue(maxsize=1), False,
                        server=server, projection=projection)
                memory.return_value.__exit__.assert_called_once()
            encoded = (root / "resource-refusal.json").read_text()
            report = json.loads(encoded)
            self.assertEqual(report["code"], "resource_process_inventory")
            self.assertEqual(report["reason"], "task_census_exhausted")
            self.assertEqual(report["role"], "server")
            self.assertEqual(report["census_attempts"], 3)
            self.assertFalse(report["qualifies_resource_model"])
            self.assertLessEqual(len(encoded), 512)
            self.assertNotIn(str(root), encoded)
            pid = int((root / "pid").read_text())
            self.assertNotIn(str(pid), encoded)
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)


class ArchiveReadiness(unittest.TestCase):
    def test_only_one_explicit_healthy_outbox_component_proves_delivery_drain(self):
        good = {"schema": "riffdb.cli.output/v1", "command": "server.health", "ok": True,
                "result": {"components": [{"component": "outbox", "status": "healthy"}]}}
        self.assertTrue(preparation.outbox_drained(good))
        for status in ("degraded", "unavailable"):
            value = copy.deepcopy(good)
            value["result"]["components"][0]["status"] = status
            self.assertFalse(preparation.outbox_drained(value))
        for components in ([], [{"component": "projection", "status": "healthy"}],
                           [None],
                           good["result"]["components"] * 2,
                           [{"component": "outbox", "status": "ready"}]):
            value = copy.deepcopy(good)
            value["result"]["components"] = components
            with self.assertRaises(preparation.SmokeError):
                preparation.outbox_drained(value)
        with self.assertRaises(preparation.SmokeError):
            preparation.outbox_drained(None)


class PhysicalInventory(unittest.TestCase):
    def census(self, deleted=0, recycled=0):
        return dict(deleted=deleted, recycled=recycled, held_references=0, held_readers=0, reclaim_us=0)

    def test_complete_native_shutdown_census_survives_a_bounded_day_of_diagnostics(self):
        from lib.ticketdesk_lifecycle_inventory import InventoryMonitor
        # Diagnostic volume is unrelated to whether physical reclamation occurred.
        # Each line is small; the complete bounded log exceeds the old 1-MiB cap.
        unrelated = b"[riffdbd-diag] trigger=other available=true\n" * 30000
        extent = (b"riffdb-retention-extents-v1\tdeleted=3\trecycled=0\t"
                  b"held_references=0\theld_readers=0\treclaim_us=3\n")
        terminal = b"riffdb-shutdown-release-v1\tonline_retention=cancelled\n"
        retired = b"".join(
            f"riffdb-retention-extent-retired-v1\troot=8\tgeneration={generation}\tdeleted=1\trecycled=0\n".encode()
            for generation in (1, 2, 3))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "server.stderr").write_bytes(unrelated + retired + extent + terminal)
            observed = InventoryMonitor(root).finish(root)
            self.assertEqual(observed["census"]["deleted"], 3)
            self.assertEqual(observed["physically_removed_generations"], [])
            self.assertEqual(observed["native_retired_generations"], [1, 2, 3])

    def test_shutdown_log_bounds_and_immutable_custody_remain_required(self):
        from lib import ticketdesk_lifecycle_inventory as inventory
        valid = (b"riffdb-retention-extents-v1\tdeleted=0\trecycled=0\t"
                 b"held_references=0\theld_readers=0\treclaim_us=3\n"
                 b"riffdb-shutdown-release-v1\tonline_retention=cancelled\n")
        for mutation in ("oversize", "long_line", "partial", "symlink", "replaced"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                path = root / "server.stderr"
                path.write_bytes(valid)
                if mutation == "oversize":
                    with path.open("r+b") as output:
                        output.truncate(inventory.MAX_LOG_BYTES + 1)
                elif mutation == "long_line":
                    path.write_bytes(b"x" * inventory.MAX_LOG_LINE_BYTES + b"\n" + valid)
                elif mutation == "partial":
                    path.write_bytes(valid[:-1])
                elif mutation == "symlink":
                    path.rename(root / "alternate")
                    path.symlink_to(root / "alternate")
                if mutation == "replaced":
                    real_fstat = os.fstat
                    calls = 0

                    def replace_before_final_fstat(descriptor):
                        nonlocal calls
                        calls += 1
                        if calls == 2:
                            path.rename(root / "alternate")
                            path.write_bytes(valid)
                        return real_fstat(descriptor)

                    with patch.object(inventory.os, "fstat", side_effect=replace_before_final_fstat):
                        with self.assertRaises(inventory.InventoryError):
                            inventory.shutdown_diagnostics(path)
                else:
                    with self.assertRaises(inventory.InventoryError):
                        inventory.shutdown_diagnostics(path)

    def test_shutdown_log_line_count_cannot_exhaust_the_scan_budget(self):
        from lib import ticketdesk_lifecycle_inventory as inventory
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.stderr"
            path.write_bytes(b"\n" * (inventory.MAX_LOG_LINES + 1))
            with self.assertRaises(inventory.InventoryError):
                inventory.shutdown_diagnostics(path)

    def test_refused_or_missing_retention_lane_cannot_pass_preparation(self):
        from lib.ticketdesk_lifecycle_inventory import InventoryError, InventoryMonitor
        extent = (b"riffdb-retention-extents-v1\tdeleted=0\trecycled=0\t"
                  b"held_references=0\theld_readers=0\treclaim_us=3\n")
        terminal = b"riffdb-shutdown-release-v1\tonline_retention=cancelled\n"
        for suffix in (b"", terminal.replace(b"cancelled", b"refused"),
                       terminal.replace(b"cancelled", b"holding"),
                       terminal.replace(b"cancelled", b"unknown"),
                       terminal.replace(b"\n", b"\tonline_retention=cancelled\n"),
                       terminal + terminal.replace(b"cancelled", b"refused"),
                       terminal * 3):
            with self.subTest(suffix=suffix), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "server.stderr").write_bytes(extent + suffix)
                with self.assertRaises(InventoryError):
                    InventoryMonitor(root).finish(root)
                # The measured extent observations survive the refusal.
                self.assertEqual(json.loads((root / "extent-observation.json").read_bytes())
                                 ["physically_removed_generations"], [])

    def test_shutdown_boundaries_repeat_one_census_without_double_counting(self):
        from lib.ticketdesk_lifecycle_inventory import InventoryError, InventoryMonitor
        line = (b"riffdb-retention-extents-v1\tdeleted=1\trecycled=0\t"
                b"held_references=0\theld_readers=0\treclaim_us=3\n")
        for lines, accepted in ((line, True), (line * 2, True),
                                (line * 3, False),
                                (line + line.replace(b"deleted=1", b"deleted=2"), False)):
            with self.subTest(accepted=accepted), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                terminal = b"riffdb-shutdown-release-v1\tonline_retention=cancelled\n"
                retired = b"riffdb-retention-extent-retired-v1\troot=8\tgeneration=7\tdeleted=1\trecycled=0\n"
                (root / "server.stderr").write_bytes(retired + lines + terminal * 2)
                monitor = InventoryMonitor(root)
                if accepted:
                    observed = monitor.finish(root)
                    self.assertEqual(observed["census"]["deleted"], 1)
                    self.assertEqual(observed["physically_removed_generations"], [])
                else:
                    with self.assertRaises(InventoryError):
                        monitor.finish(root)

    def test_only_observed_disappearance_backed_by_production_counts_is_reclamation(self):
        from lib.ticketdesk_lifecycle_inventory import ExtentInventory, InventoryError
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            file = root / "ticketdesk.redb.riffjournal.sealed.00000000000000000001"
            file.write_bytes(b"synthetic observer test")
            inventory = ExtentInventory(root)
            inventory.sample()
            self.assertEqual(inventory.observation(self.census())["physically_removed_generations"], [])
            file.unlink()
            inventory.sample()
            with self.assertRaises(InventoryError):
                inventory.observation(self.census())
            self.assertEqual(inventory.observation(self.census(recycled=1))["physically_removed_generations"], [1])
            file.write_bytes(b"reappeared generation")
            with self.assertRaises(InventoryError):
                inventory.sample()

    def test_changed_identity_or_a_link_cannot_be_published_as_a_sealed_file(self):
        from lib.ticketdesk_lifecycle_inventory import ExtentInventory, InventoryError, InventoryMonitor
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            monitor = InventoryMonitor(root)
            monitor.close()  # Startup failure must still permit daemon cleanup.
            file = root / "ticketdesk.redb.riffjournal.sealed.00000000000000000001"
            file.write_bytes(b"synthetic observer test")
            inventory = ExtentInventory(root)
            inventory.sample()
            file.write_bytes(b"changed length")
            with self.assertRaises(InventoryError):
                inventory.sample()
            self.assertEqual(inventory.samples, 1)
            file.unlink()
            file.symlink_to(root / "absent")
            with self.assertRaises(InventoryError):
                ExtentInventory(root).sample()


if __name__ == "__main__":
    unittest.main()
