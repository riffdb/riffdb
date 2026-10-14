# req: END-003, END-007, END-008, END-009, END-010
"""Worker protocol tests include an actual owned stderr handover, not a DB proof."""
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from lib.ticketdesk_lifecycle_campaign_runtime import CampaignError, CampaignRuntime, SECOND, WorkerOutput
from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess


def observation():
    return dict(schema="riffdb.ticketdesk-lifecycle-observation/v1", elapsed_s=1,
        events_checked=1024, duplicates=0, consumer_checkpoint=4, live_head=5, live_updates=1,
        history_incarnation=1, live_renewals=0, release_generation=1, auxiliary_slots=2,
        consumer_paused=False, observer_quiesced=False)


class CampaignRuntimeTests(unittest.TestCase):
    def prelude(self, *, latest):
        runtime = CampaignRuntime.__new__(CampaignRuntime)
        runtime.origin = runtime.roster = None
        runtime.outputs = {'observer': SimpleNamespace(failure=None, latest=latest)}
        runtime.children = {'observer': SimpleNamespace(poll=lambda: None)}
        return runtime

    def test_preload_drain_waits_for_a_fresh_complete_batch_without_refreshing_its_timestamp(self):
        value = observation()
        for stamp in (None, 3 * SECOND, 9 * SECOND):
            latest = None if stamp is None else (stamp, value)
            runtime = self.prelude(latest=latest)
            with self.subTest(stamp=stamp), patch('lib.ticketdesk_lifecycle_campaign_runtime.time.monotonic_ns', return_value=10 * SECOND):
                actual = runtime.prelude_observer()
                self.assertEqual(actual, value if stamp == 9 * SECOND else None)
                self.assertEqual(runtime.outputs['observer'].latest, latest)
        runtime = self.prelude(latest=(9 * SECOND, value))
        with patch('lib.ticketdesk_lifecycle_campaign_runtime.time.monotonic_ns', return_value=10 * SECOND):
            actual = runtime.prelude_observer()
            actual['events_checked'] = 0
            self.assertEqual(value['events_checked'], 1024)

    def test_preload_wait_cannot_admit_a_failed_or_dead_observer_or_run_after_offers_start(self):
        for mode in ('failed', 'dead', 'offered_clock', 'roster', 'future'):
            runtime = self.prelude(latest=(11 * SECOND if mode == 'future' else 9 * SECOND, observation()))
            if mode == 'failed': runtime.outputs['observer'].failure = ValueError('private payload')
            elif mode == 'dead': runtime.children['observer'].poll = lambda: 1
            elif mode == 'offered_clock': runtime.origin = 0
            elif mode == 'roster': runtime.roster = object()
            with self.subTest(mode=mode), patch('lib.ticketdesk_lifecycle_campaign_runtime.time.monotonic_ns', return_value=10 * SECOND):
                with self.assertRaises(CampaignError): runtime.prelude_observer()

    def test_measured_observer_still_refuses_a_stale_sample_without_waiting(self):
        runtime = self.prelude(latest=(3 * SECOND, observation()))
        runtime.interference = runtime.observer_samples = None
        with patch('lib.ticketdesk_lifecycle_campaign_runtime.time.monotonic_ns', return_value=10 * SECOND):
            with self.assertRaisesRegex(CampaignError, '^campaign_observer_gap$'):
                runtime.observer()

    def test_actual_population_drain_waits_for_fresh_observer_before_starting_offers(self):
        runtime = self.prelude(latest=(3 * SECOND, observation()))
        runtime.artifacts = Path('artifacts')
        runtime.base = ['operator']
        runtime.interference = runtime.observer_samples = None
        runtime.start_server = Mock(return_value=Mock())
        runtime.start_observer, runtime.start_sampler, runtime.start_load = Mock(), Mock(), Mock()
        runtime.worker_environment = Mock(return_value={})
        health = dict(schema='riffdb.cli.output/v1', command='server.health', ok=True,
            result=dict(components=[dict(component='outbox', status='healthy')]))
        runtime.prelude_call = Mock(side_effect=[dict(schema='riffdb.ticketdesk-lifecycle-seed/v1',
            organizations=64, tickets=65536, max_in_flight=16), health, health])
        def observed_during_drain(_duration):
            runtime.start_load.assert_not_called()
            runtime.outputs['observer'].latest = (9 * SECOND, observation())
        with patch('lib.ticketdesk_lifecycle_campaign_runtime.time.monotonic_ns', return_value=10 * SECOND), \
             patch('lib.ticketdesk_lifecycle_campaign_runtime.time.monotonic', return_value=100), \
             patch('lib.ticketdesk_lifecycle_campaign_runtime.threading.Event.wait', side_effect=observed_during_drain):
            runtime.prepare_population()
        self.assertEqual(runtime.prelude_call.call_count, 3)
        runtime.start_load.assert_called_once()

    def test_original_observer_counters_cannot_rewind_when_history_advances(self):
        output = WorkerOutput.__new__(WorkerOutput)
        output.prior_observation = None
        first = observation()
        output.observation(first)
        second = dict(first, elapsed_s=2, history_incarnation=2, release_generation=2)
        output.observation(second)
        with self.assertRaisesRegex(CampaignError, "campaign_observer_rewind"):
            output.observation(dict(second, elapsed_s=3, events_checked=1023))

    def test_original_observer_shape_and_counter_types_are_closed(self):
        first = observation()
        cases = [dict(first, extra=True), dict(first, duplicates=1), dict(first, auxiliary_slots=3)]
        cases += [dict(first, **{key: True}) for key, value in first.items() if type(value) is int]
        cases += [dict(first, **{key: 1}) for key, value in first.items() if type(value) is bool]
        for value in cases:
            with self.subTest(value=value):
                output = WorkerOutput.__new__(WorkerOutput)
                output.prior_observation = None
                with self.assertRaises(CampaignError): output.observation(value)

    def test_recovery_admission_requires_the_actual_new_process_stderr_object(self):
        for stale in (False, True):
            with self.subTest(stale=stale), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                actual = root / "server.stderr"
                code = ('import sys; print("armed",flush=True); '
                        'assert sys.stdin.buffer.readline()==b"go\\n"; '
                        'sys.stderr.write("riffdb-recovery-admission-v1\\tready=1\\n"); '
                        'sys.stderr.flush(); sys.stdin.buffer.read()')
                with actual.open("xb") as errors:
                    child = ObservedProcess([sys.executable, "-u", "-c", code],
                        address_space_limit_bytes=256 * 1024**2, observation=root / "child.json",
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errors)
                output = None
                try:
                    self.assertTrue(select.select([child.stdout], [], [], 10)[0])
                    self.assertEqual(child.stdout.readline(), b"armed\n")
                    output = WorkerOutput(child, root, "server")
                    runtime = CampaignRuntime.__new__(CampaignRuntime)
                    runtime.children, runtime.outputs = {"server": child}, {"server": output}
                    runtime.server_log = actual
                    runtime.origin = time.monotonic_ns()
                    runtime.active_archive_outage = None
                    class Memory:
                        def poll(self, *_arguments, **_options): pass
                    runtime.memory = runtime.disk = runtime.interference = Memory()
                    if stale:
                        runtime.server_log = root / "stale.stderr"
                        runtime.server_log.write_bytes(b"riffdb-recovery-admission-v1\tready=1\n")
                    child.stdin.write(b"go\n"); child.stdin.flush()
                    if stale:
                        with self.assertRaisesRegex(CampaignError, "campaign_recovery_log_owner"):
                            runtime.wait_recovery_admission(10 * SECOND)
                    else:
                        runtime.wait_recovery_admission(10 * SECOND)
                        self.assertFalse(output.ready_seen)
                    self.assertIsNone(child.poll())
                finally:
                    child.stdin.close()
                    try: child.wait(timeout=5)
                    except subprocess.TimeoutExpired: child.kill(); child.wait(timeout=5)
                    if output is not None: output.thread.join(5)
                    child.stdout.close()
