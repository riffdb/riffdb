# req: END-003, END-007, END-008
"""A persistent native sampler must retain exact identities and continuity."""
import io
import json
import os
import tempfile
from unittest.mock import patch
from pathlib import Path
import subprocess
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from lib.ticketdesk_lifecycle_native_observer import (
    NativeSamples, NativeObservationError, NativeObserverProcess, read_message,
)


class NativeSampleContinuity(unittest.TestCase):
    identity = {"projection_id": 7, "projection_plan_hash": "a" * 64}

    def sample(self, **changes):
        value = {"schema": "riffdb.ticketdesk-lifecycle-native-observation/v1", "index": 0,
                 "contract_version": 1, "sampled_head": 100, "statistics_head": 101,
                 "outbox_backlog": 5, "outbox_degraded": True, "history_incarnation": 1,
                 "applied_through": 97, "projection_generation": 4, "projection_backlog": 3,
                 "projection_id": 7, "projection_plan_hash": "a" * 64,
                 "transport_attempts": 3, "retries": 0, "auxiliary_slots": 1}
        value.update(changes)
        return value

    def test_nonzero_backlogs_and_rpc_accounting_remain_actual_measurements(self):
        samples = NativeSamples(self.identity)
        first = self.sample()
        self.assertEqual(samples.observe(first, version=1), first)
        second = self.sample(index=1, contract_version=2, sampled_head=105, statistics_head=106,
                             applied_through=105, projection_backlog=0, outbox_backlog=0,
                             outbox_degraded=False)
        self.assertEqual(samples.observe(second, version=2), second)
        self.assertEqual(samples.transport_attempts, 6)

    def test_missing_unknown_or_substituted_fields_refuse_evidence(self):
        for change in ({"projection_id": 8}, {"projection_plan_hash": "b" * 64},
                       {"schema": "another/v1"}, {"unexpected": 0}, {"contract_version": 2},
                       {"statistics_head": 99}, {"projection_backlog": 0},
                       {"history_incarnation": 0}, {"transport_attempts": 2}, {"retries": 1},
                       {"auxiliary_slots": 2}, {"outbox_degraded": 1}):
            with self.subTest(change=change), self.assertRaises(NativeObservationError):
                NativeSamples(self.identity).observe(self.sample(**change), version=1)
        value = self.sample()
        del value["outbox_backlog"]
        with self.assertRaises(NativeObservationError):
            NativeSamples(self.identity).observe(value, version=1)

    def test_gaps_repeats_and_incarnation_or_frontier_regressions_refuse_evidence(self):
        for change in ({"index": 0}, {"index": 2}, {"history_incarnation": 2},
                       {"sampled_head": 99, "statistics_head": 101, "projection_backlog": 2},
                       {"statistics_head": 100}, {"projection_generation": 3},
                       {"applied_through": 96, "projection_backlog": 4}):
            samples = NativeSamples(self.identity)
            samples.observe(self.sample(), version=1)
            value = self.sample(index=1)
            value.update(change)
            with self.subTest(change=change), self.assertRaises(NativeObservationError):
                samples.observe(value, version=1)
            self.assertEqual(samples.transport_attempts, 3)

    def test_numbers_and_retained_observations_are_bounded(self):
        for key in ("index", "sampled_head", "statistics_head", "outbox_backlog",
                    "history_incarnation", "applied_through", "projection_generation"):
            for value in (True, -1, 1.5, "1", 2**64):
                with self.subTest(key=key, value=value), self.assertRaises(NativeObservationError):
                    NativeSamples(self.identity).observe(self.sample(**{key: value}), version=1)
        samples = NativeSamples(self.identity)
        value = self.sample()
        result = samples.observe(value, version=1)
        result["sampled_head"] = 999
        value["statistics_head"] = 999
        self.assertEqual(samples.last["sampled_head"], 100)
        self.assertEqual(samples.last["statistics_head"], 101)


class NativeProcessOwnership(unittest.TestCase):
    def script(self, root, *, ready_slots=1, ignore_stop=False):
        path = root / "sampler-fixture.py"
        value = NativeSampleContinuity().sample()
        path.write_text("import json, sys\n"
            + "print(json.dumps({'schema':'riffdb.ticketdesk-lifecycle-native-observer-ready/v1', 'auxiliary_slots':"
            + repr(ready_slots) + "}),flush=True)\n"
            + "value=" + repr(value) + "\n"
            + "for line in sys.stdin:\n"
            + " if line=='stop\\n':\n"
            + ("  continue\n" if ignore_stop else "  break\n")
            + " if line=='sample 1\\n':\n"
            + "  print(json.dumps(value),flush=True); value['index']+=1\n")
        return [sys.executable, str(path)]

    def test_actual_child_handshake_samples_and_normal_join_use_no_timing_sleep(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with NativeObserverProcess(root, self.script(root), dict(os.environ), NativeSampleContinuity.identity) as owner:
                process = owner.process
                first = owner.sample(0)
                second = owner.sample(5_000_000_000)
                self.assertEqual((first['outbox_backlog'], second['projection_backlog']), (5, 3))
                self.assertEqual(owner.samples.transport_attempts, 6)
            self.assertEqual(process.returncode, 0)
            self.assertFalse(owner.reader.is_alive())
            usage = json.loads((root / 'native-observer.process-usage.json').read_bytes())
            self.assertTrue(usage['exit_observed'])
            self.assertEqual(usage['exit_code'], 0)
            self.assertGreater(usage['exit_rss_peak_bytes'], 0)
            self.assertIsNone(usage['exit_allocator_peak_bytes'])
            records = [json.loads(line) for line in (root / 'native-observations.jsonl').read_bytes().splitlines()]
            self.assertEqual([record['index'] for record in records], [0, 1])

    def test_actual_bounded_parent_routes_the_replacement_sampler_through_its_roster(self):
        from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess
        scripts = str(Path(__file__).resolve().parents[2] / "scripts")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            command = self.script(root)
            code = '''import json,os,select,subprocess,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess
from lib.ticketdesk_lifecycle_campaign_process_roster import CampaignProcessRoster
from lib.ticketdesk_lifecycle_native_observer import NativeObserverProcess
root=Path(sys.argv[2]);command=json.loads(sys.argv[3]);identity=json.loads(sys.argv[4])
owned=[];roster=None;first=None;replacement=None
arguments=[sys.executable,'-c','import sys;print("ready",flush=True);sys.stdin.buffer.read()']
try:
 initial=root/'initial';initial.mkdir()
 first=NativeObserverProcess(initial,command,dict(os.environ),identity,
                             address_space_limit_bytes=128*1024**2)
 original=first.process;owned.append(original);children={'native_sampler':original}
 for role in ('server','load','observer'):
  child=ObservedProcess(arguments,observation=root/(role+'.json'),
      address_space_limit_bytes=128*1024**2,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  owned.append(child);children[role]=child
  assert select.select([child.stdout],[],[],10)[0] and child.stdout.readline()==b'ready\\n'
 limits={'controller':256*1024**2,**{role:128*1024**2 for role in children}}
 roster=CampaignProcessRoster(os.getpid(),children,limits)
 roster.sample(0);first.sample(0);first.close()
 roster.retire('native_sampler',original,expected_exit=0)
 successor=root/'successor';successor.mkdir()
 replacement=NativeObserverProcess(successor,command,dict(os.environ),identity,roster=roster)
 current=replacement.process;owned.append(current)
 assert roster.children['native_sampler'] is current
 assert current.usage['address_space_proof']['limit_bytes']==128*1024**2
 replacement.sample(1);roster.sample(1);replacement.close()
 roster.retire('native_sampler',current,expected_exit=0)
 for role in ('server','load','observer'):
  child=children[role];child.stdin.close();assert child.wait(timeout=10)==0
  roster.retire(role,child,expected_exit=0)
 final=roster.sample(2)
 assert final['roles']['native_sampler']['incarnations']==2
 assert final['retired_incarnations']==5 and len(final['processes'])==1
 assert current.usage['exit_rss_peak_bytes']>0 and current.usage['exit_observed']
 print(json.dumps({'sampler_incarnations':2,'retired':5,'finite_exit_peak':True}))
finally:
 for owner in (first,replacement):
  if owner is not None and owner.process is not None:owner.close(failed=True)
 pending=owned+([] if roster is None else list(roster.children.values()))
 for child in {id(child):child for child in pending}.values():
  if child.poll() is None:child.kill();child.wait(timeout=5)
  for stream in (child.stdin,child.stdout,child.stderr):
   if stream is not None:stream.close()
'''
            outer = ObservedProcess([sys.executable, "-c", code, scripts, str(root),
                json.dumps(command), json.dumps(NativeSampleContinuity.identity)],
                observation=root / "controller.json", address_space_limit_bytes=256 * 1024**2,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                self.assertEqual(outer.wait(timeout=60), 0, outer.stderr.read().decode())
                self.assertEqual(json.loads(outer.stdout.read()),
                    dict(sampler_incarnations=2, retired=5, finite_exit_peak=True))
            finally:
                if outer.poll() is None: outer.kill(); outer.wait(timeout=5)
                outer.stdout.close(); outer.stderr.close()

    def test_boolean_ready_slot_refuses_and_joins_the_actual_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(NativeObservationError, '^native_sample_ready$'):
                NativeObserverProcess(root, self.script(root, ready_slots=True), dict(os.environ), NativeSampleContinuity.identity)
            self.assertFalse((root / 'native-observations.jsonl').exists())

    def test_invalid_roster_and_competing_limit_refuse_before_opening_or_launching(self):
        from lib.ticketdesk_lifecycle_campaign_process_roster import CampaignProcessRoster
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "must-not-exist"
            for roster, limit in ((object(), None),
                                  (CampaignProcessRoster.__new__(CampaignProcessRoster), 128 * 1024**2)):
                with self.subTest(limit=limit), self.assertRaisesRegex(NativeObservationError, "^native_sample_owner$"):
                    NativeObserverProcess(root, ["must-not-launch"], {}, NativeSampleContinuity.identity,
                                          roster=roster, address_space_limit_bytes=limit)
            self.assertFalse(root.exists())

    def test_expired_graceful_shutdown_remains_a_failure_after_forced_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            owner = NativeObserverProcess(root, self.script(root, ignore_stop=True), dict(os.environ), NativeSampleContinuity.identity)
            process = owner.process
            owner.sample(0)
            with patch('lib.ticketdesk_lifecycle_native_observer.SHUTDOWN_TIMEOUT_S', 0):
                with self.assertRaisesRegex(NativeObservationError, '^native_sample_shutdown$'):
                    owner.close()
            self.assertIsNotNone(process.returncode)
            self.assertFalse(owner.reader.is_alive())
            self.assertIsNone(owner.process)
            self.assertIsNone(owner.output)

    def test_bounded_json_rejects_duplicates_floats_constants_and_truncation(self):
        for encoded in (b'{"index":0,"index":1}\n', b'{"index":1.5}\n',
                        b'{"index":NaN}\n', b'{"index":18446744073709551616}\n',
                        b'{"index":0}', b'x'*4097+b'\n'):
            with self.subTest(bytes=len(encoded)), self.assertRaises(NativeObservationError):
                read_message(io.BytesIO(encoded))


if __name__ == "__main__":
    unittest.main()
