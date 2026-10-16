# req: END-007, END-008, END-009, END-010
"""Virtual process guards; these tests do not claim native qualification."""
from pathlib import Path
import unittest

from lib.ticketdesk_lifecycle_campaign_actions import CampaignActions
from lib.ticketdesk_lifecycle_campaign_maintenance import CampaignMaintenance, MaintenanceError, SECOND
from lib.ticketdesk_lifecycle_campaign_operator import OperatorError
from lib.ticketdesk_lifecycle_campaign_runtime import CampaignError

OPERATION = "01900000-0000-7000-8001-000000000001"


class VirtualGuardedOperator:
    """Apply the real caller-selected process limit to deterministic elapsed work."""
    def __init__(self):
        self.root = Path("fixture")
        self.operator = self
        self.last_ns = self.now_ns = 0
        self.duration_ns = 8 * SECOND
        self.calls = []
        self.failed = False

    def elapsed(self):
        return self.now_ns

    def remaining(self, deadline):
        if self.now_ns >= deadline:
            raise CampaignError("campaign_fault_deadline")
        return (deadline - self.now_ns) / SECOND

    def run_as(self, _credential, command, arguments, *, elapsed_ns, timeout_s):
        if self.failed:
            raise OperatorError("campaign_operator_failed")
        self.calls.append((command, list(arguments), elapsed_ns, timeout_s))
        if self.duration_ns > timeout_s * SECOND:
            self.last_ns = self.now_ns = elapsed_ns + int(timeout_s * SECOND)
            self.failed = True
            raise OperatorError("campaign_operator_deadline")
        self.last_ns = self.now_ns = elapsed_ns + self.duration_ns
        if command in {"backup.create", "backup.operation"}:
            result = dict(status="found" if command == "backup.operation" else "accepted",
                maintenance_operation_id=OPERATION, kind="create_backup", backup_name="campaign",
                input_hash="ab" * 32,
                phase="succeeded" if command == "backup.operation" else "accepted")
        else:
            result = {"status": "ready"}
        return dict(schema="riffdb.cli.output/v1", command=command, ok=True, result=result)


class CampaignCliDeadlineTests(unittest.TestCase):
    def test_initial_pending_start_can_finish_eight_seconds_inside_original_deadline(self):
        runtime = VirtualGuardedOperator()
        owner = CampaignMaintenance(runtime, "fixture", "create", "campaign",
            started_ns=0, deadline_ns=60 * SECOND)
        self.assertFalse(owner.start(0))
        self.assertEqual(runtime.now_ns, 8 * SECOND)
        self.assertEqual(owner.deadline_ns, 60 * SECOND)
        self.assertEqual([call[0] for call in runtime.calls], ["backup.create"])

    def test_initial_pending_status_observes_original_operation_without_resubmission(self):
        runtime = VirtualGuardedOperator()
        runtime.duration_ns = SECOND
        owner = CampaignMaintenance(runtime, "fixture", "create", "campaign",
            started_ns=0, deadline_ns=60 * SECOND)
        self.assertFalse(owner.start(0))
        runtime.duration_ns = 8 * SECOND
        self.assertTrue(owner.poll(runtime.now_ns))
        self.assertEqual(runtime.now_ns, 9 * SECOND)
        self.assertEqual(owner.polls, 1)
        self.assertEqual(runtime.calls[1][:2],
            ("backup.operation", ["backup", "operation", OPERATION]))
        self.assertEqual([call[0] for call in runtime.calls], ["backup.create", "backup.operation"])

    def test_original_replay_call_can_finish_eight_seconds_without_new_clock(self):
        runtime = VirtualGuardedOperator()
        result = CampaignActions(runtime, None).call("command.run", ["command", "run"], 60 * SECOND)
        self.assertTrue(result["ok"])
        self.assertEqual(runtime.now_ns, 8 * SECOND)
        self.assertEqual(len(runtime.calls), 1)

    def test_maintenance_remaining_fault_budget_still_fails_sticky(self):
        runtime = VirtualGuardedOperator()
        runtime.now_ns = 55 * SECOND
        owner = CampaignMaintenance(runtime, "fixture", "create", "campaign",
            started_ns=0, deadline_ns=60 * SECOND)
        with self.assertRaisesRegex(OperatorError, "campaign_operator_deadline"):
            owner.start(runtime.now_ns)
        self.assertEqual(runtime.calls[0][3], 5)
        self.assertEqual(runtime.now_ns, 60 * SECOND)
        with self.assertRaisesRegex(MaintenanceError, "campaign_maintenance_failed"):
            owner.poll(runtime.now_ns)
        self.assertEqual(len(runtime.calls), 1)

    def test_explicit_five_second_deploy_deadline_is_not_extended(self):
        runtime = VirtualGuardedOperator()
        runtime.now_ns = SECOND
        with self.assertRaisesRegex(OperatorError, "campaign_operator_deadline"):
            CampaignActions(runtime, None).call("application.deploy", ["application", "deploy"], 5 * SECOND)
        self.assertEqual(runtime.calls[0][3], 4)
        self.assertEqual(runtime.now_ns, 5 * SECOND)
        with self.assertRaisesRegex(CampaignError, "campaign_fault_deadline"):
            runtime.remaining(5 * SECOND)
        self.assertEqual(len(runtime.calls), 1)

    def test_cli_guard_remains_thirty_seconds_even_when_fault_has_more_room(self):
        runtime = VirtualGuardedOperator()
        runtime.duration_ns = 31 * SECOND
        with self.assertRaisesRegex(OperatorError, "campaign_operator_deadline"):
            CampaignActions(runtime, None).call("server.health", ["server", "health"], 60 * SECOND)
        self.assertEqual(runtime.calls[0][3], 30)
        self.assertEqual(runtime.now_ns, 30 * SECOND)
        self.assertEqual(len(runtime.calls), 1)
