# req: END-007, END-008, END-010
"""Administrative children retain exact exit proof before the next census.

Numeric fixtures test refusal and output semantics; the bounded-parent test
uses real children, pipes and wait4. Neither supplies database campaign proof.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from lib.ticketdesk_lifecycle_campaign_operator import CampaignOperator, OperatorError
from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess
from lib.ticketdesk_lifecycle_resources import ResourceError
import test_campaign_process_roster as fixtures


class CompletedFixture(fixtures.FixtureProcess):
    def wait(self, timeout=None):
        return self.code


class CampaignOperatorTests(unittest.TestCase):
    def fixture(self, root, encoded=None, *, exit_code=0):
        helper = fixtures.CampaignRosterTests()
        self.addCleanup(helper.doCleanups)
        roster = helper.fixture(root / "proc")
        child = CompletedFixture(128)
        child.exit(exit_code, rss=31 * 1024**2)
        self.child = child
        self.encoded = encoded if encoded is not None else json.dumps({
            "schema": "riffdb.cli.output/v1", "command": "server.health",
            "ok": True, "result": {"fixture": True}}).encode()

        def spawn(arguments, **options):
            # Pipe contents are fixtures; no actual CLI is claimed here.
            child._output = tempfile.TemporaryFile()
            child._output.write(self.encoded); child._output.seek(0)
            child._errors = tempfile.TemporaryFile()
            self.addCleanup(child._output.close)
            self.addCleanup(child._errors.close)
            return child

        self.spawn = patch.object(roster, "_spawn", side_effect=spawn)
        self.spawn.start(); self.addCleanup(self.spawn.stop)
        return roster

    def test_completed_short_child_is_retired_before_the_census_and_result_is_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); roster = self.fixture(root)
            with CampaignOperator(root, roster, ["fixture"]) as operator:
                result = operator.run("server.health", ["server", "health"], elapsed_ns=0, timeout_s=5)
                self.assertEqual(result["result"], {"fixture": True})
                self.assertNotIn("operator", roster.children)
                self.assertEqual(roster.retired, 1)
                self.assertEqual(roster.roles["operator"]["rss_peak_upper_bound_bytes"], 31 * 1024**2)
                self.assertFalse(operator.complete_campaign_checks_proved)
            record = json.loads((root / "campaign-operator.jsonl").read_bytes())
            self.assertEqual(record["command"], "server.health")
            self.assertEqual(record["exit_code"], 0)
            self.assertNotIn("fixture", str(record))

    def test_wrong_command_refusal_duplicate_json_and_oversized_output_fail_closed(self):
        valid = {"schema": "riffdb.cli.output/v1", "command": "server.health", "ok": True, "result": {}}
        cases = [b'{"ok":true,"ok":true}', b'{', b'x' * (1024**2 + 1)]
        for key, value in (("command", "capability.bootstrap"), ("ok", False), ("schema", "unknown")):
            cases.append(json.dumps(dict(valid, **{key: value})).encode())
        for encoded in cases:
            with self.subTest(size=len(encoded)), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); roster = self.fixture(root, encoded)
                with CampaignOperator(root, roster, ["fixture"]) as operator:
                    with self.assertRaises(OperatorError) as error:
                        operator.run("server.health", ["server", "health"], elapsed_ns=0, timeout_s=5)
                    self.assertNotIn(encoded[:20].decode(errors="ignore"), str(error.exception))
                    self.assertNotIn("operator", roster.children)
                    with self.assertRaisesRegex(OperatorError, "^campaign_operator_failed$"):
                        operator.run("server.health", [], elapsed_ns=1, timeout_s=5)

    def test_unexpected_exit_does_not_become_an_expected_retirement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); roster = self.fixture(root, exit_code=1)
            with CampaignOperator(root, roster, ["fixture"]) as operator:
                with self.assertRaises(ResourceError):
                    operator.run("server.health", [], elapsed_ns=0, timeout_s=5)
            self.assertTrue(roster.failed)
            self.assertEqual(roster.retired, 0)
            self.assertEqual(self.child.usage["exit_code"], 1)

    def test_maintenance_uncertainty_requires_the_exact_poll_protocol_and_exit_code(self):
        uncertain = {"schema": "riffdb.cli.output/v1", "command": "backup.create", "ok": False,
            "error": {"type": "uncertain", "code": "outcome_unknown", "message": "fixture",
                "recovery_action": "poll_maintenance_operation",
                "maintenance_operation_id": "01900000-0000-7000-8001-000000000001"}}
        for command in ("backup.create", "backup.restore"):
            with self.subTest(command=command), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                value = dict(uncertain, command=command)
                roster = self.fixture(root, json.dumps(value).encode(), exit_code=3)
                with CampaignOperator(root, roster, ["fixture"]) as operator:
                    self.assertEqual(operator.run(command, [], elapsed_ns=0, timeout_s=5), value)
                record = json.loads((root / "campaign-operator.jsonl").read_bytes())
                self.assertEqual(record["exit_code"], 3)
                self.assertFalse(record["outcome_known"])
                self.assertEqual(roster.retired, 1)
        cases = [dict(uncertain, command="server.health"), dict(uncertain, ok=True)]
        for change in ({"recovery_action": "retry"}, {"code": "storage_unavailable"},
                       {"type": "public"}, {"maintenance_operation_id": "unknown"},
                       {"maintenance_operation_id": "01900000-0000-4000-8001-000000000001"}):
            cases.append(dict(uncertain, error=dict(uncertain["error"], **change)))
        for value in cases:
            with self.subTest(hostile=True), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); roster = self.fixture(root, json.dumps(value).encode(), exit_code=3)
                with CampaignOperator(root, roster, ["fixture"]) as operator:
                    with self.assertRaises(ResourceError):
                        operator.run("backup.create", [], elapsed_ns=0, timeout_s=5)
                self.assertEqual(roster.retired, 0)

    def test_invalid_deadline_command_clock_and_argument_bounds_refuse_before_launch(self):
        cases = [dict(timeout_s=61), dict(timeout_s=0), dict(timeout_s=True),
                 dict(command="unknown"), dict(elapsed_ns=-1), dict(arguments=["x"] * 129),
                 dict(arguments=["x" * 65537]), dict(arguments=["\x00"])]
        for change in cases:
            with self.subTest(change=list(change)), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); roster = self.fixture(root)
                with CampaignOperator(root, roster, ["fixture"]) as operator:
                    options = dict(command="server.health", arguments=[], elapsed_ns=0, timeout_s=5)
                    options.update(change)
                    with self.assertRaises(OperatorError): operator.run(**options)
                    self.assertEqual(roster.retired, 0)

    def test_actual_bounded_child_output_exit_peak_and_failure_cleanup(self):
        scripts = str(Path(__file__).resolve().parents[2] / "scripts")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code = '''import json,os,select,subprocess,sys,time
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess
from lib.ticketdesk_lifecycle_campaign_process_roster import CampaignProcessRoster
from lib.ticketdesk_lifecycle_campaign_operator import CampaignOperator,OperatorError
root=Path(sys.argv[2]);children={}
try:
 for role in ('server','load','observer','native_sampler'):
  child=ObservedProcess([sys.executable,'-c','import sys;print("ready",flush=True);sys.stdin.buffer.read()'],
   address_space_limit_bytes=128*1024**2,observation=root/(role+'.json'),
   stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
  children[role]=child
  assert select.select([child.stdout],[],[],10)[0] and child.stdout.readline()==b'ready\\n'
 limits={'controller':256*1024**2,**{role:128*1024**2 for role in (*children,'operator')}}
 roster=CampaignProcessRoster(os.getpid(),children,limits)
 program='import json;print(json.dumps({"schema":"riffdb.cli.output/v1","command":"server.health","ok":True,"result":{"numeric_fixture":True}}))'
 origin=time.monotonic_ns()
 with CampaignOperator(root,roster,[sys.executable,'-c',program]) as operator:
  for i in range(3):
   assert operator.run('server.health',[],elapsed_ns=time.monotonic_ns()-origin,timeout_s=5)['result']=={'numeric_fixture':True}
 assert roster.retired==3 and roster.roles['operator']['incarnations']==3
 assert roster.roles['operator']['rss_peak_upper_bound_bytes']>0
 failure=root/'failure';failure.mkdir()
 # The output reader bounds retained bytes and kills a still-running producer.
 with CampaignOperator(failure,roster,[sys.executable,'-c','import sys;sys.stdout.buffer.write(b"x"*(2*1024**2));sys.stdout.flush();sys.stdin.buffer.read()']) as operator:
  try:operator.run('server.health',[],elapsed_ns=time.monotonic_ns()-origin,timeout_s=5)
  except OperatorError as error:assert str(error)=='campaign_operator_output'
  else:raise AssertionError('oversized child accepted')
 assert 'operator' not in roster.children
 assert (failure/'operator-000.stdout').stat().st_size<=1024**2
 assert roster.sample(time.monotonic_ns()-origin)['retired_incarnations']==4
 print(json.dumps({'actual_children':8,'retired_operators':4,'qualified':False}))
finally:
 for role,child in children.items():
  if child.poll() is None:child.stdin.close()
  assert child.wait(timeout=5)==0
  for stream in (child.stdout,child.stderr):stream.close()
'''
            owner = ObservedProcess([sys.executable, "-c", code, scripts, str(root)],
                address_space_limit_bytes=256 * 1024**2, observation=root / "outer.json",
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                self.assertEqual(owner.wait(timeout=60), 0, owner.stderr.read(4096).decode())
                self.assertEqual(json.loads(owner.stdout.read(4096)),
                    {"actual_children": 8, "retired_operators": 4, "qualified": False})
            finally:
                if owner.poll() is None: owner.kill(); owner.wait(timeout=5)
                owner.stdout.close(); owner.stderr.close()


CompletedFixture.stdout = property(lambda self: self._output)
CompletedFixture.stderr = property(lambda self: self._errors)
CompletedFixture.stdin = property(lambda self: None)

if __name__ == "__main__":
    unittest.main()
