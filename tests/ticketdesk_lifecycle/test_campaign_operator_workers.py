# req: END-007, END-008, END-009, END-010
"""Fixed worker reports and narrowly admitted read-only maintenance uncertainty.

Fixtures test protocol refusal; actual worker ownership is tested separately
through the same bounded parent used by the operator's real-process test.
"""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import test_campaign_operator as fixtures
from lib.ticketdesk_lifecycle_campaign_operator import CampaignOperator, OperatorError
from lib.ticketdesk_lifecycle_resources import ResourceError

OPERATION = "01900000-0000-7000-8001-000000000001"


def unavailable():
    return {"schema": "riffdb.cli.output/v1", "command": "backup.operation", "ok": False,
        "error": {"type": "client", "code": "transport_unavailable",
                  "message": "the gRPC transport is unavailable"}}


def offline_unavailable():
    return dict(unavailable(), error={"type": "public", "code": "storage_unavailable",
        "message": "storage is temporarily unavailable", "recovery_action": "retry"})


class CampaignOperatorWorkerTests(unittest.TestCase):
    fixture = fixtures.CampaignOperatorTests.fixture

    def test_original_registered_maintenance_operation_can_be_polled_through_transport_handover(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            value = {"schema": "riffdb.cli.output/v1", "command": "backup.restore", "ok": False,
                "error": {"type": "uncertain", "code": "outcome_unknown", "message": "fixture",
                    "recovery_action": "poll_maintenance_operation", "maintenance_operation_id": OPERATION}}
            roster = self.fixture(root, json.dumps(value).encode(), exit_code=3)
            with CampaignOperator(root, roster, ["fixture"]) as operator:
                self.assertEqual(operator.run("backup.restore", ["backup", "restore", "fixture"],
                    elapsed_ns=0, timeout_s=5), value)
                # The next fixture represents another actual protocol output,
                # not a retry of the restore mutation.
                self.child.exit(1, rss=31 * 1024**2)
                self.encoded = json.dumps(unavailable()).encode()
                self.assertEqual(operator.run("backup.operation", ["backup", "operation", OPERATION],
                    elapsed_ns=operator.last_ns + 1, timeout_s=5), unavailable())
                tls = unavailable()
                tls["error"] = {"type":"client", "code":"tls_connection_failed",
                                "message":"the verified TLS channel could not connect"}
                self.encoded = json.dumps(tls).encode()
                self.assertEqual(operator.run("backup.operation", ["backup", "operation", OPERATION],
                    elapsed_ns=operator.last_ns + 1, timeout_s=5), tls)
                self.encoded = json.dumps(offline_unavailable()).encode()
                self.assertEqual(operator.run("backup.operation", ["backup", "operation", OPERATION],
                    elapsed_ns=operator.last_ns + 1, timeout_s=5), offline_unavailable())
            rows = [json.loads(line) for line in (root / "campaign-operator.jsonl").read_bytes().splitlines()]
            self.assertEqual([row["exit_code"] for row in rows], [3, 1, 1, 1])
            self.assertTrue(all(row["outcome_known"] is False for row in rows))
            self.assertEqual(roster.retired, 4)

    def test_offline_poll_refusal_cannot_authorize_another_operation_or_changed_error(self):
        cases = [(False, "backup.operation", ["backup", "operation", OPERATION], offline_unavailable()),
            (True, "backup.operation", ["backup", "operation", "01900000-0000-7000-8001-000000000002"], offline_unavailable()),
            (True, "backup.restore", ["backup", "restore", "fixture"],
                dict(offline_unavailable(), command="backup.restore"))]
        for key, value in (("type", "client"), ("code", "outcome_unknown"),
                ("message", "other"), ("recovery_action", "poll_maintenance_operation"),
                ("extra", True)):
            cases.append((True, "backup.operation", ["backup", "operation", OPERATION],
                dict(offline_unavailable(), error=dict(offline_unavailable()["error"], **{key: value}))))
        missing = offline_unavailable()
        del missing["error"]["recovery_action"]
        cases.append((True, "backup.operation", ["backup", "operation", OPERATION], missing))
        for known, command, arguments, value in cases:
            with self.subTest(command=command, known=known, value=value), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); roster = self.fixture(root, json.dumps(value).encode(), exit_code=1)
                with CampaignOperator(root, roster, ["fixture"]) as operator:
                    if known: operator.pending_maintenance.add(OPERATION)
                    with self.assertRaises(ResourceError):
                        operator.run(command, arguments, elapsed_ns=0, timeout_s=5)
                self.assertEqual(roster.retired, 0)

    def test_unregistered_different_operation_and_non_transport_errors_remain_fatal(self):
        cases = [(False, "backup.operation", ["backup", "operation", OPERATION], unavailable())]
        cases.append((True, "backup.operation", ["backup", "operation", "01900000-0000-7000-8001-000000000002"], unavailable()))
        cases.append((True, "backup.restore", ["backup", "restore", "fixture"], dict(unavailable(), command="backup.restore")))
        for change in ({"code": "tls_connection_failed"}, {"code": "storage_unavailable"}, {"code": "deadline"}, {"type": "public"}, {"message": "other"}, {"extra": True}):
            cases.append((True, "backup.operation", ["backup", "operation", OPERATION],
                dict(unavailable(), error=dict(unavailable()["error"], **change))))
        for known, command, arguments, value in cases:
            with self.subTest(command=command, known=known), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); roster = self.fixture(root, json.dumps(value).encode(), exit_code=1)
                with CampaignOperator(root, roster, ["fixture"]) as operator:
                    if known: operator.pending_maintenance.add(OPERATION)
                    with self.assertRaises(ResourceError):
                        operator.run(command, arguments, elapsed_ns=0, timeout_s=5)
                self.assertEqual(roster.retired, 0)

    def test_worker_reports_are_closed_and_refuse_numeric_booleans(self):
        values = {
            "rotation-smoke": {"schema": "riffdb.ticketdesk-lifecycle-rotation-smoke/v1",
                "revoked_reads_refused": True, "revoked_replay_refused": True,
                "same_principal_replay": True, "state_reconciled": True},
            "successor-authorization": {"schema": "riffdb.ticketdesk-lifecycle-successor-authorization/v1",
                "contract_version": 2, "ungranted_command_refused": True},
            "reconcile": {"schema": "riffdb.ticketdesk-lifecycle-state-reconciliation/v1",
                "tickets_checked": 65536, "updates": 321, "max_in_flight": 16},
            "archive-reconcile": {"schema": "riffdb.ticketdesk-lifecycle-archive-reconciliation/v1",
                "records_checked": 65857, "encoded_bytes": 123, "physical_bytes": 456,
                "payloads_reconciled": True, "policy_anchors_reconciled": True},
        }
        for kind, value in values.items():
            cases = [(value, True), (dict(value, extra=True), False)]
            for key, item in value.items():
                if type(item) is bool: cases.append((dict(value, **{key: 1}), False))
                elif type(item) is int: cases.append((dict(value, **{key: True}), False))
            for report, accepted in cases:
                with self.subTest(kind=kind, accepted=accepted), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory); roster = self.fixture(root, json.dumps(report).encode())
                    with CampaignOperator(root, roster, ["fixture"]) as operator:
                        if accepted:
                            self.assertEqual(operator.run_worker("fixture-worker", kind, {}, elapsed_ns=0, timeout_s=5), report)
                        else:
                            with self.assertRaises(OperatorError):
                                operator.run_worker("fixture-worker", kind, {}, elapsed_ns=0, timeout_s=5)
                    self.assertEqual(roster.retired, 1)

    def test_worker_environment_and_kind_are_bounded_before_launch(self):
        for kind, environment in (("seed", {}), ("reconcile", {"bad": 1}),
                ("reconcile", {"bad\0": "value"}), ("reconcile", {"x": "y" * 8193}),
                ("reconcile", {str(i): "x" for i in range(257)})):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); roster = self.fixture(root)
                with CampaignOperator(root, roster, ["fixture"]) as operator:
                    with self.assertRaises(OperatorError):
                        operator.run_worker("fixture-worker", kind, environment, elapsed_ns=0, timeout_s=5)
                    self.assertEqual(roster.retired, 0)

    def test_per_call_credential_replaces_one_native_cli_flag_without_retaining_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); credential = root / "private.credential"; credential.write_bytes(b"fixture")
            roster = self.fixture(root)
            with CampaignOperator(root, roster, ["fixture", "--credential-file", "original"]) as operator:
                operator.run_as(str(credential), "server.health", ["server", "health"], elapsed_ns=0, timeout_s=5)
                launched = roster._spawn.call_args.args[0]
                self.assertEqual(launched, ["fixture", "--credential-file", str(credential), "server", "health"])
                self.assertEqual(operator.base, ["fixture", "--credential-file", "original"])
            self.assertNotIn(str(credential), (root / "campaign-operator.jsonl").read_text())
            self.assertNotIn("original", (root / "campaign-operator.jsonl").read_text())

    def test_actual_worker_uses_the_serial_operator_owner_and_retains_wait4_peak(self):
        import subprocess
        import sys
        from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess
        scripts = str(Path(__file__).resolve().parents[2] / "scripts")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code = '''import json,os,select,subprocess,sys,time
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess
from lib.ticketdesk_lifecycle_campaign_process_roster import CampaignProcessRoster
from lib.ticketdesk_lifecycle_campaign_operator import CampaignOperator
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
 executable=root/'worker'
 executable.write_text('#!'+sys.executable+'\\nimport json,os,sys\\nassert sys.argv[1:]==["successor-authorization"]\\nassert os.environ["WORKER_FIXTURE"]=="bounded"\\nprint(json.dumps({"schema":"riffdb.ticketdesk-lifecycle-successor-authorization/v1","contract_version":2,"ungranted_command_refused":True}))\\n')
 executable.chmod(0o700)
 origin=time.monotonic_ns()
 with CampaignOperator(root,roster,['unused']) as operator:
  result=operator.run_worker(str(executable),'successor-authorization',{'WORKER_FIXTURE':'bounded'},elapsed_ns=time.monotonic_ns()-origin,timeout_s=5)
  assert result['ungranted_command_refused'] is True
 assert roster.retired==1 and 'operator' not in roster.children
 assert roster.roles['operator']['rss_peak_upper_bound_bytes']>0
 assert roster.sample(time.monotonic_ns()-origin)['incarnations']==6
 print(json.dumps({'actual_children':5,'retired_operators':1,'qualified':False}))
finally:
 for child in children.values():
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
                    {"actual_children": 5, "retired_operators": 1, "qualified": False})
            finally:
                if owner.poll() is None: owner.kill(); owner.wait(timeout=5)
                owner.stdout.close(); owner.stderr.close()
