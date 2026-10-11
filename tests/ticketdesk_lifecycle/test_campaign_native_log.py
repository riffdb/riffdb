# req: END-003, END-007, END-008, END-010
"""Numeric log fixtures prove custody rules; real processes prove stderr binding."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import test_campaign_native_epochs as epochs_fixture
import test_provider_budget as providers

SECOND = 10**9


class CampaignNativeLog(unittest.TestCase):
    @contextmanager
    def fixture(self):
        helper = epochs_fixture.NativeCampaignEpochs()
        with helper.fixture() as (root, processes, roster, epochs):
            path = root / "server.stderr"
            path.write_bytes(providers.line())
            self.bind_fd(roster, roster.children["server"], path)
            epochs.bind_log(path, 0)
            yield root, helper, processes, roster, epochs, path

    def bind_fd(self, roster, child, path):
        descriptor = roster.proc_root / str(child.pid) / "fd" / "2"
        descriptor.parent.mkdir(parents=True, exist_ok=True)
        if descriptor.exists(): descriptor.unlink()
        os.link(path, descriptor)

    def append(self, path, data):
        with path.open("ab") as output: output.write(data)

    def test_initial_log_excludes_preload_and_preserves_partial_record_until_complete(self):
        with self.fixture() as (_, _, _, _, epochs, path):
            self.assertTrue(epochs.poll_log(0))
            self.assertEqual(epochs.samples["provider"].count, 1)
            self.append(path, providers.line()[:-1])
            self.assertFalse(epochs.poll_log(SECOND))
            self.assertEqual(epochs.samples["provider"].count, 1)
            self.append(path, b"\n")
            self.assertTrue(epochs.poll_log(2 * SECOND))
            self.assertEqual(epochs.samples["provider"].count, 2)
            self.assertEqual(epochs.samples["provider"].last_ns, 2 * SECOND)

    def test_retired_owner_is_drained_before_replacement_startup_is_read_from_zero(self):
        with self.fixture() as (root, helper, processes, roster, epochs, path):
            self.append(path, providers.line(owned_bytes=20000, released_allocations=9))
            helper.replace(processes, roster)
            self.assertTrue(epochs.poll_log(SECOND, retired_kind="graceful_restart"))
            self.assertEqual(epochs.samples["provider"].peak, 24608)
            epochs.transition("graceful_restart", 2 * SECOND, version=1, incarnation=1,
                              identity=epochs_fixture.IDENTITY)
            successor = root / "successor.stderr"
            successor.write_bytes(providers.line(released_allocations=0, released_owners=0))
            self.bind_fd(roster, roster.children["server"], successor)
            epochs.bind_log(successor, 3 * SECOND)
            self.assertTrue(epochs.poll_log(4 * SECOND))
            self.assertEqual(epochs.samples["provider"].count, 3)
            self.assertEqual(epochs.samples["provider"].peak, 24608)
            self.assertEqual(epochs.samples["provider"].last["released_allocations"], 0)
            self.assertEqual(epochs.samples["provider"].coverage, [3] + [0] * 72)

    def test_replacement_cannot_skip_old_tail_or_observe_before_new_binding(self):
        for fault in ("undrained", "unbound"):
            with self.subTest(fault=fault), self.fixture() as (_, helper, processes, roster, epochs, _):
                helper.replace(processes, roster)
                if fault == "unbound":
                    epochs.poll_log(SECOND, retired_kind="graceful_restart")
                    epochs.transition("graceful_restart", 2 * SECOND, version=1, incarnation=1,
                                      identity=epochs_fixture.IDENTITY)
                with self.assertRaisesRegex(ValueError, "^campaign_epoch_log$"):
                    if fault == "undrained":
                        epochs.transition("graceful_restart", SECOND, version=1, incarnation=1,
                                          identity=epochs_fixture.IDENTITY)
                    else: epochs.observe("provider", providers.line(), 3 * SECOND)

    def test_repeated_component_waits_for_a_new_actual_poll_clock(self):
        with self.fixture() as (_, _, _, _, epochs, path):
            self.append(path, providers.line() + providers.line(released_allocations=4))
            self.assertFalse(epochs.poll_log(SECOND))
            self.assertEqual(epochs.samples["provider"].count, 2)
            self.assertTrue(epochs.poll_log(2 * SECOND))
            self.assertEqual(epochs.samples["provider"].count, 3)
            self.assertEqual(epochs.samples["provider"].last_ns, 2 * SECOND)

    def test_wrong_exit_or_unretired_owner_cannot_authorize_terminal_log_collection(self):
        for fault in ("unretired", "wrong_kind"):
            with self.subTest(fault=fault), self.fixture() as (_, helper, processes, roster, epochs, _):
                if fault == "unretired":
                    roster.children["server"].exit()
                else: helper.replace(processes, roster)
                with self.assertRaises(ValueError):
                    epochs.poll_log(SECOND, retired_kind="process_kill")
                self.assertEqual(epochs.samples["provider"].count, 1)

    def test_alias_replacement_truncation_wrong_stderr_and_log_bounds_refuse(self):
        for fault in ("symlink", "inode", "truncate", "stderr", "oversized", "line", "many"):
            with self.subTest(fault=fault), self.fixture() as (root, _, _, roster, epochs, path):
                other = root / "other.stderr"
                other.write_bytes(b"")
                if fault == "symlink": path.unlink(); path.symlink_to(other)
                elif fault == "inode": path.unlink(); path.write_bytes(b"")
                elif fault == "truncate": path.write_bytes(b"")
                elif fault == "stderr": self.bind_fd(roster, roster.children["server"], other)
                elif fault == "oversized":
                    with path.open("ab") as output: output.truncate(16 * 1024**2 + 1)
                elif fault == "line": self.append(path, b"x" * 4097 + b"\n")
                else: self.append(path, b"unrelated\n" * 65)
                with self.assertRaises(ValueError) as error: epochs.poll_log(SECOND)
                self.assertLess(len(str(error.exception)), 64)
                self.assertNotIn(str(root), str(error.exception))

    def test_killed_partial_tail_stays_private_and_never_supplies_a_sample(self):
        with self.fixture() as (root, helper, processes, roster, epochs, path):
            partial = providers.line()[:-1]
            self.append(path, partial)
            before = path.read_bytes()
            helper.replace(processes, roster, kind="process_kill")
            self.assertTrue(epochs.poll_log(SECOND, retired_kind="process_kill"))
            self.assertEqual(epochs.samples["provider"].count, 1)
            self.assertEqual(path.read_bytes(), before)
            epochs.transition("process_kill", 2 * SECOND, version=1, incarnation=1,
                              identity=epochs_fixture.IDENTITY)
            records = [json.loads(row) for row in
                       (root / "native-observation-epochs.jsonl").read_bytes().splitlines()]
            self.assertEqual(records[-2]["terminal_partial_bytes"], len(partial))
            self.assertFalse(records[-2]["qualifies_complete_resource_model"])

    def test_retirement_does_not_reset_original_global_gap(self):
        with self.fixture() as (_, helper, processes, roster, epochs, path):
            self.append(path, providers.line())
            helper.replace(processes, roster)
            with self.assertRaisesRegex(ValueError, "^provider_budget_sample_gap$"):
                epochs.poll_log(75 * SECOND + 1, retired_kind="graceful_restart")

    def test_graceful_partial_tail_and_malformed_complete_record_remain_failures(self):
        for fault in ("partial", "malformed"):
            with self.subTest(fault=fault), self.fixture() as (_, helper, processes, roster, epochs, path):
                self.append(path, providers.line()[:-1] if fault == "partial"
                            else providers.line(available="false"))
                helper.replace(processes, roster)
                with self.assertRaises(ValueError):
                    epochs.poll_log(SECOND, retired_kind="graceful_restart")
                self.assertEqual(epochs.samples["provider"].count, 1)

    def test_fresh_owner_cannot_reuse_a_retired_log_inode(self):
        with self.fixture() as (_, helper, processes, roster, epochs, path):
            helper.replace(processes, roster)
            epochs.poll_log(SECOND, retired_kind="graceful_restart")
            epochs.transition("graceful_restart", 2 * SECOND, version=1, incarnation=1,
                              identity=epochs_fixture.IDENTITY)
            self.bind_fd(roster, roster.children["server"], path)
            with self.assertRaisesRegex(ValueError, "^campaign_epoch_log$"):
                epochs.bind_log(path, 3 * SECOND)

    def test_actual_bounded_parent_binds_both_stderr_files_and_retains_exit_peaks(self):
        from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess
        import test_source_bytes_continuation as sources
        import test_logical_bytes as logicals
        import test_physical_bytes as physicals
        import test_native_observer as natives
        project = Path(__file__).resolve().parents[2]
        rows = dict(provider=providers.line(), source=sources.line(root=10, incarnation=1),
                    logical=logicals.line(root=10, incarnation=1),
                    physical=physicals.line(root=10, incarnation=1))
        fresh = dict(provider=providers.line(released_allocations=0, released_owners=0),
                     source=sources.line(root=2, incarnation=1),
                     logical=logicals.line(root=2, incarnation=1),
                     physical=physicals.line(root=2, incarnation=1))
        payload = dict(initial={k:v.hex() for k,v in rows.items()},
            fresh=b"".join(fresh.values()).hex(),
            terminal=providers.line(owned_bytes=20000, released_allocations=9).hex(),
            native=natives.NativeSampleContinuity().sample(), identity=epochs_fixture.IDENTITY)
        code = '''import json,os,select,subprocess,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess
from lib.ticketdesk_lifecycle_campaign_process_roster import CampaignProcessRoster
from lib.ticketdesk_lifecycle_campaign_native_epochs import CampaignNativeEpochs
root=Path(sys.argv[2]);data=json.loads(sys.argv[3]);owned=[];roster=None
dummy=[sys.executable,'-c','import sys;print("ready",flush=True);sys.stdin.buffer.read()']
server_code='import sys;sys.stderr.buffer.write(bytes.fromhex(sys.argv[1]));sys.stderr.flush();print("ready",flush=True);sys.stdin.buffer.read();sys.stderr.buffer.write(bytes.fromhex(sys.argv[2]));sys.stderr.flush()'
def ready(child):
 assert select.select([child.stdout],[],[],10)[0] and child.stdout.readline()==b'ready\\n'
try:
 children={};initial=root/'initial.stderr'
 for role in ('server','load','observer','native_sampler'):
  if role=='server':
   with initial.open('xb') as errors:
    child=ObservedProcess([sys.executable,'-c',server_code,data['initial']['provider'],data['terminal']],
       address_space_limit_bytes=128*1024**2,stdin=subprocess.PIPE,stdout=subprocess.PIPE,
       stderr=errors,observation=root/(role+'.json'))
  else:
   child=ObservedProcess(dummy,address_space_limit_bytes=128*1024**2,
       stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
       observation=root/(role+'.json'))
  owned.append(child);children[role]=child;ready(child)
 limits={'controller':256*1024**2,**{role:128*1024**2 for role in children}}
 roster=CampaignProcessRoster(os.getpid(),children,limits)
 with CampaignNativeEpochs(root,roster,21900,data['identity']) as epochs:
  # Values are explicit numeric collector fixtures, not native database evidence.
  for name,row in data['initial'].items():epochs.observe(name,bytes.fromhex(row),0)
  epochs.observe_native(data['native'],0);epochs.bind_log(initial,0)
  for role in ('server','native_sampler'):
   child=children[role];child.stdin.close();assert child.wait(timeout=10)==0
   roster.retire(role,child,expected_exit=0)
  assert epochs.poll_log(10**9,retired_kind='graceful_restart')
  assert epochs.samples['provider'].count==2 and epochs.samples['provider'].peak==24608
  successor=root/'successor.stderr'
  with successor.open('xb') as errors:
   server=roster.launch('server',[sys.executable,'-c',server_code,data['fresh'],data['terminal']],
       stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=errors,observation=root/'server-2.json')
  owned.append(server);ready(server)
  sampler=roster.launch('native_sampler',dummy,stdin=subprocess.PIPE,stdout=subprocess.PIPE,
       stderr=subprocess.PIPE,observation=root/'sampler-2.json')
  owned.append(sampler);ready(sampler)
  epochs.transition('graceful_restart',2*10**9,version=1,incarnation=1,identity=data['identity'])
  epochs.bind_log(successor,3*10**9);assert epochs.poll_log(4*10**9)
  assert epochs.samples['provider'].count==3 and epochs.samples['provider'].peak==24608
  assert all(epochs.samples[name].count==2 for name in ('source','logical','physical'))
  assert not epochs.complete_campaign_checks_proved
 for role,child in list(roster.children.items()):
  child.stdin.close();assert child.wait(timeout=10)==0;roster.retire(role,child,expected_exit=0)
 final=roster.sample(5*10**9)
 assert final['retired_incarnations']==6 and len(final['processes'])==1
 assert final['roles']['server']['incarnations']==2
 assert all(child.usage['exit_observed'] and child.usage['exit_rss_peak_bytes']>0 for child in owned)
 print(json.dumps({'stderr_owners':2,'retired':6,'fixture_values_only':True}))
finally:
 for child in owned:
  if child.poll() is None:
   if child.stdin is not None and not child.stdin.closed:child.stdin.close()
   try:child.wait(timeout=10)
   except subprocess.TimeoutExpired:child.kill();child.wait(timeout=5)
  for stream in (child.stdin,child.stdout,child.stderr):
   if stream is not None:stream.close()
'''
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child = ObservedProcess([sys.executable, "-c", code, str(project / "scripts"),
                str(root), json.dumps(payload)], address_space_limit_bytes=256 * 1024**2,
                observation=root / "controller.json", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                self.assertEqual(child.wait(timeout=60), 0, child.stderr.read().decode())
                self.assertEqual(json.loads(child.stdout.read()),
                    dict(stderr_owners=2, retired=6, fixture_values_only=True))
            finally:
                if child.poll() is None: child.kill(); child.wait(timeout=5)
                child.stdout.close(); child.stderr.close()


if __name__ == "__main__": unittest.main()
