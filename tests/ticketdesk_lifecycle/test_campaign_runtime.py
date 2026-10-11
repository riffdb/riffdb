# req: END-003, END-007, END-008, END-009, END-010
"""Worker protocol tests include an actual owned stderr handover, not a DB proof."""
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import time
import unittest

from lib.ticketdesk_lifecycle_campaign_runtime import CampaignError, CampaignRuntime, SECOND, WorkerOutput
from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess


def observation():
    return dict(schema="riffdb.ticketdesk-lifecycle-observation/v1", elapsed_s=1,
        events_checked=1024, duplicates=0, consumer_checkpoint=4, live_head=5, live_updates=1,
        history_incarnation=1, live_renewals=0, release_generation=1, auxiliary_slots=2,
        consumer_paused=False, observer_quiesced=False)


class CampaignRuntimeTests(unittest.TestCase):
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
