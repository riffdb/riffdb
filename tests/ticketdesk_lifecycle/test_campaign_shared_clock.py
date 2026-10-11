# req: END-007, END-008, END-010
"""A native offered origin is authoritative; no new controller clock is credited."""
from contextlib import ExitStack
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from lib.ticketdesk_lifecycle_campaign_runtime import CampaignError, CampaignRuntime, WorkerOutput
from lib.ticketdesk_lifecycle_load_stream import CampaignLoadStream, LoadStreamError

NOW = 5_000_000_000
ORIGIN = NOW - 1_000_000


def ready(origin=ORIGIN):
    return dict(schema='riffdb.ticketdesk-lifecycle-load-ready/v2', workers=28,
                queue_slots=32768, origin_monotonic_ns=origin)


def encoded(value):
    return io.BytesIO(json.dumps(value).encode()+b'\n')


class StopAfterClock(Exception): pass


class SharedClockTests(unittest.TestCase):
    def test_shared_ready_preserves_the_exact_native_monotonic_origin(self):
        stream = CampaignLoadStream(duration_s=28800, require_shared_clock=True)
        value = ready()
        self.assertEqual(stream.read(encoded(value)), value)
        self.assertEqual(stream.origin_ns, ORIGIN)

    def test_shared_ready_refuses_legacy_zero_extra_or_invalid_clock_values(self):
        cases = [dict(schema='riffdb.ticketdesk-lifecycle-load-ready/v1', workers=28, queue_slots=32768),
                 dict(ready(), extra=1)]
        cases += [ready(origin) for origin in (0, -1, True, 1.5, 2**64)]
        for value in cases:
            with self.subTest(value=value):
                stream = CampaignLoadStream(duration_s=28800, require_shared_clock=True)
                with self.assertRaises(LoadStreamError): stream.read(encoded(value))
                with self.assertRaisesRegex(LoadStreamError, 'load_stream_failed'):
                    stream.read(encoded(ready()))

    def test_preparation_ready_remains_the_original_closed_v1_shape(self):
        value = dict(schema='riffdb.ticketdesk-lifecycle-load-ready/v1', workers=28, queue_slots=32768)
        stream = CampaignLoadStream(duration_s=28800)
        self.assertEqual(stream.read(encoded(value)), value)
        self.assertIsNone(stream.origin_ns)
        with self.assertRaises(LoadStreamError):
            CampaignLoadStream(duration_s=28800).read(encoded(ready()))

    def run_start(self, origin, expected):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            root = Path(directory)
            runtime = CampaignRuntime.__new__(CampaignRuntime)
            runtime.root, runtime.artifacts, runtime.duration = root, root/'artifacts', 28800
            runtime.outputs, runtime.children = {}, {}
            runtime.worker_environment = lambda **changes: changes
            launched = []
            runtime.launch = lambda *args, **kwargs: launched.append((args, kwargs)) or object()
            output = SimpleNamespace(origin_ns=origin, wait_ready=lambda _timeout: None)
            stack.enter_context(patch('lib.ticketdesk_lifecycle_campaign_runtime.WorkerOutput', return_value=output))
            stack.enter_context(patch('lib.ticketdesk_lifecycle_campaign_runtime.time.monotonic_ns', return_value=NOW))
            stack.enter_context(patch('lib.ticketdesk_lifecycle_campaign_runtime.CampaignProcessRoster',
                                      side_effect=StopAfterClock))
            with self.assertRaises(expected): runtime.start_load()
            return runtime, launched

    def test_runtime_uses_the_published_origin_instead_of_sampling_a_new_one(self):
        runtime, launched = self.run_start(ORIGIN, StopAfterClock)
        self.assertEqual(runtime.origin, ORIGIN, 'controller must use origin offered by Rust worker')
        self.assertEqual(launched[0][1]['environment']['RIFFDB_LIFECYCLE_SHARED_CLOCK'], '1')

    def test_runtime_refuses_missing_future_stale_or_noninteger_native_origin(self):
        for value in (None, True, 0, NOW+1, NOW-1_000_000_001):
            with self.subTest(value=value): self.run_start(value, CampaignError)
        runtime, _ = self.run_start(NOW-1_000_000_000, StopAfterClock)
        self.assertEqual(runtime.origin, NOW-1_000_000_000)

    def test_reader_hands_the_native_origin_to_the_controller_before_ready(self):
        import subprocess
        import sys
        from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code = ('import json,sys,time; print(json.dumps(dict('
                    'schema="riffdb.ticketdesk-lifecycle-load-ready/v2",workers=28,queue_slots=32768,'
                    'origin_monotonic_ns=time.monotonic_ns())),flush=True); sys.stdin.buffer.read()')
            before = __import__('time').monotonic_ns()
            with (root/'child.stderr').open('xb') as errors:
                child = ObservedProcess([sys.executable,'-u','-c',code],
                    address_space_limit_bytes=256*1024**2, observation=root/'child.json',
                    stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=errors)
            output = None
            try:
                output = WorkerOutput(child,root,'load',28800)
                output.wait_ready(10)
                after = __import__('time').monotonic_ns()
                self.assertLessEqual(before, output.origin_ns)
                self.assertLessEqual(output.origin_ns, after)
                self.assertEqual(output.origin_ns, output.parser.origin_ns)
            finally:
                child.stdin.close()
                try: child.wait(timeout=5)
                except subprocess.TimeoutExpired: child.kill(); child.wait(timeout=5)
                if output is not None: output.thread.join(5)
                child.stdout.close()
