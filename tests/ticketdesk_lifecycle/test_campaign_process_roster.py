# req: END-007, END-008, END-010
"""Replacement must retain exit peaks and preserve the exact live process tree."""
from copy import deepcopy
from pathlib import Path
import sys
import os
import subprocess
import threading
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess
from lib.ticketdesk_lifecycle_campaign_process_roster import CampaignProcessRoster
from lib.ticketdesk_lifecycle_resources import CampaignMemorySamples, ResourceError

MIB = 1024**2


class FixtureProcess(ObservedProcess):
    """Numeric kernel/proof fixture; actual launch and wait4 have separate tests."""

    def __init__(self, pid, *, limit=128 * MIB, birth=256 * MIB):
        self._fixture_pid = pid
        self.code = None
        self.record = dict(launch_observed=True, exit_observed=False, exit_code=None,
            exit_rss_peak_bytes=None, exit_allocator_peak_upper_bound_bytes=None,
            address_space_proof=dict(enforced=True, limit_bytes=limit,
                                    lifetime_upper_bound_bytes=birth))

    @property
    def pid(self): return self._fixture_pid

    @property
    def usage(self): return deepcopy(self.record)

    def poll(self): return self.code

    def exit(self, code=0, *, rss=64 * MIB):
        self.code = code
        self.record.update(exit_observed=True, exit_code=code, exit_rss_peak_bytes=rss,
            exit_allocator_peak_upper_bound_bytes=self.record["address_space_proof"]["lifetime_upper_bound_bytes"])


class CampaignRosterTests(unittest.TestCase):
    def fixture(self, root, *, extra_roles=()):
        self.controller = os.getpid()
        ceiling = patch("lib.ticketdesk_lifecycle_campaign_process_roster.resource.getrlimit",
                        return_value=(256 * MIB, 256 * MIB))
        ceiling.start(); self.addCleanup(ceiling.stop)
        self.children = {"server": FixtureProcess(124), "load": FixtureProcess(125),
                         "observer": FixtureProcess(126), "native_sampler": FixtureProcess(127)}
        self.limits = {"controller": 256 * MIB,
                       **{role: 128 * MIB for role in (*self.children, *extra_roles)}, "operator": 128 * MIB}
        self.tree(root, [child.pid for child in self.children.values()])
        return CampaignProcessRoster(self.controller, self.children, self.limits, proc_root=root)

    def tree(self, root, children):
        for pid in [self.controller, *children]:
            process = root / str(pid)
            (process / "task" / str(pid)).mkdir(parents=True, exist_ok=True)
            (process / "task" / str(pid) / "children").write_text(
                " ".join(map(str, children)) if pid == self.controller else "")
            fields = ["S"] + ["0"] * 18 + ["9"] + ["0"] * 30
            (process / "stat").write_text(f"{pid} (fixture) " + " ".join(fields) + "\n")
            (process / "status").write_text(f"State:\tS\nPid:\t{pid}\n"
                "VmRSS:\t13312 kB\nVmSize:\t103424 kB\nVmHWM:\t19456 kB\nVmPeak:\t130048 kB\n")

    def test_replacement_keeps_exit_peak_without_summing_sequential_incarnations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); roster = self.fixture(root)
            first = roster.sample(0)
            old = self.children["server"]
            old.exit(-9, rss=90 * MIB)
            roster.retire("server", old, expected_exit=-9)
            self.tree(root, [125, 126, 127])
            during = roster.sample(10)
            self.assertEqual(during["rss_bytes"], 4 * 13 * MIB)
            self.assertEqual(during["roles"]["server"]["rss_peak_upper_bound_bytes"], 90 * MIB)
            replacement = FixtureProcess(128)
            self.tree(root, [125, 126, 127, 128])
            with patch("lib.ticketdesk_lifecycle_campaign_process_roster.CampaignProcessRoster._spawn", return_value=replacement):
                roster.launch("server", ["fixture"])
            final = roster.sample(20)
            self.assertEqual(final["incarnations"], 6)
            self.assertEqual(final["retired_incarnations"], 1)
            self.assertEqual(final["roles"]["server"]["incarnations"], 2)
            self.assertEqual(final["rss_peak_upper_bound_bytes"], 90 * MIB + 4 * 19 * MIB)
            self.assertEqual(final["allocator_peak_upper_bound_bytes"], first["allocator_peak_upper_bound_bytes"])
            self.assertFalse(final["qualifies_resource_model"])
            self.assertNotIn("pid", str(final))

    def test_live_overlap_unexpected_exit_and_unknown_birth_proof_refuse(self):
        for fault in ("overlap", "unexpected_exit", "unknown_birth", "unknown_rss", "changed_limit"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); roster = self.fixture(root); old = self.children["server"]
                with self.assertRaises(ResourceError):
                    if fault == "overlap": roster.launch("server", ["fixture"])
                    elif fault == "unknown_birth":
                        old.record["address_space_proof"]["lifetime_upper_bound_bytes"] = None
                        roster.sample(0)
                    elif fault == "changed_limit":
                        old.record["address_space_proof"]["limit_bytes"] *= 2
                        roster.sample(0)
                    else:
                        old.exit(-9, rss=None if fault == "unknown_rss" else 64 * MIB)
                        roster.retire("server", old, expected_exit=0 if fault == "unexpected_exit" else -9)

    def test_replaced_pid_untracked_child_and_rewind_remain_failures(self):
        for fault in ("replacement", "untracked", "rewind"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); roster = self.fixture(root); roster.sample(10)
                if fault == "replacement":
                    p = root / "124" / "stat"; p.write_text(p.read_text().replace(" 9 ", " 10 "))
                if fault == "untracked":
                    (root / str(self.controller) / "task" / str(self.controller) / "children").write_text("124 125 126 127 128")
                with self.assertRaises(ResourceError): roster.sample(9 if fault == "rewind" else 20)

    def test_retirement_requires_the_exact_owner_and_actual_exit_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); roster = self.fixture(root)
            with self.assertRaises(ResourceError):
                roster.retire("server", self.children["server"], expected_exit=0)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); roster = self.fixture(root)
            imposter = FixtureProcess(124); imposter.exit()
            with self.assertRaises(ResourceError): roster.retire("server", imposter, expected_exit=0)

    def test_returned_role_records_do_not_mutate_retained_peaks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); roster = self.fixture(root)
            first = roster.sample(0); first["roles"]["server"]["rss_peak_upper_bound_bytes"] = 1
            self.assertEqual(roster.sample(1)["roles"]["server"]["rss_peak_upper_bound_bytes"], 19 * MIB)

    def test_continuous_sampling_keeps_exit_peaks_and_global_coverage_across_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); roster = self.fixture(root)
            with CampaignMemorySamples.from_roster(root, roster, 21900) as samples:
                samples.poll(0)
                original = self.children["server"]
                original.exit(-9, rss=90 * MIB)
                roster.retire("server", original, expected_exit=-9)
                self.tree(root, [125, 126, 127])
                samples.poll(60 * 10**9)
                replacement = FixtureProcess(128)
                self.tree(root, [125, 126, 127, 128])
                with patch.object(roster, "_spawn", return_value=replacement):
                    roster.launch("server", ["fixture"])
                samples.poll(120 * 10**9)
                self.assertEqual(samples.count, 3)
                self.assertEqual(samples.coverage, [3] + [0] * 72)
                for elapsed in range(180, 21901, 60):
                    samples.poll(elapsed * 10**9)
                samples.finish()
            import json
            rows = [json.loads(line) for line in samples.path.read_bytes().splitlines()]
            self.assertEqual(len(rows), 365)
            self.assertNotIn("server", rows[1]["processes"])
            self.assertEqual(rows[1]["roles"]["server"]["rss_peak_upper_bound_bytes"], 90 * MIB)
            self.assertEqual(rows[2]["roles"]["server"]["incarnations"], 2)
            self.assertEqual(rows[-1]["rss_peak_upper_bound_bytes"], 90 * MIB + 4 * 19 * MIB)
            self.assertEqual(samples.coverage, [5] * 73)
            self.assertTrue(all(row["qualifies_resource_model"] is False for row in rows))

    def test_replacement_cannot_reset_the_continuous_sample_gap_or_hide_a_failed_census(self):
        for fault in ("gap", "untracked"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); roster = self.fixture(root)
                with CampaignMemorySamples.from_roster(root, roster, 21900) as samples:
                    samples.poll(0)
                    original = self.children["server"]
                    original.exit(-9)
                    roster.retire("server", original, expected_exit=-9)
                    self.tree(root, [125, 126, 127, 128])
                    if fault == "gap":
                        with patch.object(roster, "_spawn", return_value=FixtureProcess(128)):
                            roster.launch("server", ["fixture"])
                    with self.assertRaisesRegex(ResourceError,
                            "^resource_sample_gap$" if fault == "gap" else "^resource_process_inventory$"):
                        samples.poll(75 * 10**9 + 1 if fault == "gap" else 60 * 10**9)
                    self.assertEqual(samples.count, 1)
                    self.assertEqual(samples.coverage, [1] + [0] * 72)

    def test_continuous_roster_sampling_preserves_the_existing_duration_and_record_limits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); roster = self.fixture(root)
            for duration in (True, 21600, 21901, 86700):
                with self.subTest(duration=duration), self.assertRaises(ResourceError):
                    CampaignMemorySamples.from_roster(root, roster, duration)
            with self.assertRaises(ResourceError):
                CampaignMemorySamples.from_roster(root, object(), 21900)
            with CampaignMemorySamples.from_roster(root, roster, 86400) as samples:
                for elapsed in range(0, 86401, 60):
                    samples.poll(elapsed * 10**9)
                samples.finish()
                self.assertEqual(samples.count, 1440)
                self.assertEqual(samples.coverage, [5] * 288)
            self.assertTrue(all(len(line) + 1 <= 4096
                                for line in samples.path.read_bytes().splitlines()))
            self.assertEqual(samples.path.stat().st_mode & 0o777, 0o600)

    def test_all_declared_roles_fit_one_bounded_record_and_keep_transient_exit_peaks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); roster = self.fixture(root, extra_roles=("typescript_driver",))
            self.tree(root, [124, 125, 126, 127, 128, 129])
            operator, driver = FixtureProcess(128), FixtureProcess(129)
            for role, child in (("operator", operator), ("typescript_driver", driver)):
                with patch.object(roster, "_spawn", return_value=child):
                    roster.launch(role, ["fixture"])
            with CampaignMemorySamples.from_roster(root, roster, 21900) as samples:
                samples.poll(0)
                operator.exit(0, rss=110 * MIB); driver.exit(0, rss=99 * MIB)
                roster.retire("operator", operator, expected_exit=0)
                roster.retire("typescript_driver", driver, expected_exit=0)
                self.tree(root, [124, 125, 126, 127])
                samples.poll(60 * 10**9)
            import json
            rows = [json.loads(line) for line in samples.path.read_bytes().splitlines()]
            self.assertEqual(len(rows[0]["processes"]), 7)
            self.assertEqual(rows[1]["rss_peak_upper_bound_bytes"], (5 * 19 + 110 + 99) * MIB)
            self.assertEqual(rows[1]["retired_incarnations"], 2)
            self.assertTrue(all(len(line) + 1 <= 4096
                                for line in samples.path.read_bytes().splitlines()))

    def test_short_child_exit_is_retained_until_explicit_expected_exit_retirement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); roster = self.fixture(root)
            child = FixtureProcess(128); child.exit(0, rss=31 * MIB)
            with patch.object(roster, "_spawn", return_value=child):
                self.assertIs(roster.launch("operator", ["fixture"]), child)
            roster.retire("operator", child, expected_exit=0)
            final = roster.sample(0)
            self.assertEqual(final["roles"]["operator"]["rss_peak_upper_bound_bytes"], 31 * MIB)
            self.assertEqual(final["retired_incarnations"], 1)

    def test_failed_evidence_fences_the_roster_and_another_thread_cannot_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); roster = self.fixture(root)
            failures = []
            def other_thread():
                try: roster.launch("operator", ["fixture"])
                except ResourceError as error: failures.append(str(error))
            thread = threading.Thread(target=other_thread); thread.start(); thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(failures, ["resource_roster_thread"])
            self.children["server"].record["address_space_proof"] = None
            with self.assertRaises(ResourceError): roster.sample(0)
            with self.assertRaisesRegex(ResourceError, "^resource_roster_failed$"): roster.sample(1)

    def test_incarnation_count_and_frozen_role_limits_are_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); roster = self.fixture(root)
            roster.count = 64
            with patch.object(roster, "_spawn", return_value=FixtureProcess(128)):
                with self.assertRaises(ResourceError): roster.launch("operator", ["fixture"])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); self.fixture(root)
            self.limits["server"] = 512 * MIB
            with self.assertRaises(ResourceError):
                CampaignProcessRoster(self.controller, self.children, self.limits, proc_root=root)

    def test_actual_owned_children_replacement_and_short_operator_keep_complete_exit_records(self):
        scripts = str(Path(__file__).resolve().parents[2] / "scripts")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code = '''import json,os,select,subprocess,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess
from lib.ticketdesk_lifecycle_campaign_process_roster import CampaignProcessRoster
root=Path(sys.argv[2]); owned=[]
arguments=[sys.executable,'-c','import sys;print("ready",flush=True);sys.stdin.buffer.read()']
def options(name):
 return dict(observation=root/(name+'.json'),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
def ready(child):
 assert select.select([child.stdout],[],[],10)[0]
 assert child.stdout.readline()==b'ready\\n'
try:
 children={}
 for role in ('server','load','observer','native_sampler'):
  child=ObservedProcess(arguments,address_space_limit_bytes=128*1024**2,**options(role))
  owned.append(child);ready(child);children[role]=child
 limits={'controller':256*1024**2,**{role:128*1024**2 for role in (*children,'operator')}}
 roster=CampaignProcessRoster(os.getpid(),children,limits)
 first=roster.sample(0)
 original=children['server'];original.kill();assert original.wait(timeout=5)==-9
 roster.retire('server',original,expected_exit=-9)
 replacement=roster.launch('server',arguments,**options('replacement'))
 owned.append(replacement);ready(replacement);children['server']=replacement
 second=roster.sample(1)
 operator=roster.launch('operator',[sys.executable,'-c','raise SystemExit(0)'],
                       observation=root/'operator.json',stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 owned.append(operator);assert operator.wait(timeout=10)==0
 roster.retire('operator',operator,expected_exit=0)
 for role,child in children.items():
  child.stdin.close();assert child.wait(timeout=10)==0;roster.retire(role,child,expected_exit=0)
 final=roster.sample(2)
 assert final['retired_incarnations']==6 and final['incarnations']==7
 assert len(final['processes'])==1 and not final['qualifies_resource_model']
 assert second['roles']['server']['incarnations']==2
 assert final['allocator_peak_upper_bound_bytes']==first['allocator_peak_upper_bound_bytes']
 print(json.dumps({'retired':final['retired_incarnations'],'owner_records':len(owned),
                   'peak_retained':final['roles']['server']['rss_peak_upper_bound_bytes']>0}))
finally:
 for child in owned:
  if child.poll() is None:child.kill();child.wait(timeout=5)
  for stream in (child.stdin,child.stdout,child.stderr):
   if stream is not None:stream.close()
'''
            outer = ObservedProcess([sys.executable, "-c", code, scripts, str(root)],
                observation=root / "controller.json", address_space_limit_bytes=256 * MIB,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                self.assertEqual(outer.wait(timeout=60), 0, outer.stderr.read().decode())
                import json
                self.assertEqual(json.loads(outer.stdout.read()),
                                 {"retired": 6, "owner_records": 6, "peak_retained": True})
            finally:
                if outer.poll() is None: outer.kill(); outer.wait(timeout=5)
                outer.stdout.close(); outer.stderr.close()


if __name__ == "__main__": unittest.main()
