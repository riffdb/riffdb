# req: END-007, END-008, END-009, END-010
"""Virtual-clock action tests; no native recovery or timing qualification."""
from pathlib import Path
import unittest

from lib.ticketdesk_lifecycle_campaign_actions import CampaignActions
from lib.ticketdesk_lifecycle_campaign_maintenance import CampaignMaintenance, SECOND
from lib.ticketdesk_lifecycle_campaign_runtime import CampaignError

OPERATION = "01900000-0000-7000-8001-000000000001"


class Runtime:
    root = Path("fixture")

    def __init__(self, *, complete_at_ns, started_ns=0):
        self.now_ns = started_ns
        self.complete_at_ns = complete_at_ns
        self.operator = self
        self.last_ns = started_ns
        self.calls = []

    def elapsed(self):
        return self.now_ns

    def remaining(self, deadline):
        if self.now_ns >= deadline:
            raise CampaignError("campaign_fault_deadline")
        return (deadline - self.now_ns) / SECOND

    def run_as(self, credential, command, arguments, *, elapsed_ns, timeout_s):
        self.calls.append((command, arguments, elapsed_ns, timeout_s))
        self.now_ns = self.last_ns = elapsed_ns + SECOND // 10
        polling = command == "backup.operation"
        return {"schema": "riffdb.cli.output/v1", "command": command, "ok": True,
                "result": {"status": "found" if polling else "accepted",
                    "maintenance_operation_id": OPERATION, "kind": "restore_backup",
                    "backup_name": "campaign-source", "input_hash": "ab" * 32,
                    "phase": "succeeded" if polling and self.now_ns >= self.complete_at_ns
                             else "accepted"}}


def action(runtime):
    actions = CampaignActions(runtime, None)

    def wait(condition, deadline, **_options):
        # Advance a virtual clock to the action's actual selected condition.
        # No sleep, subprocess, alternative polling algorithm or native owner.
        for _ in range(6001):
            runtime.remaining(deadline)
            if condition():
                return
            runtime.now_ns += SECOND // 100
        raise AssertionError("virtual wait bound exhausted")

    actions.wait = wait
    return actions


class CampaignMaintenanceScheduleTests(unittest.TestCase):
    def test_actual_restore_action_observes_late_success_on_original_clock(self):
        runtime = Runtime(complete_at_ns=53 * SECOND, started_ns=7 * SECOND)
        owner = action(runtime).maintenance("restore", 0, 60 * SECOND)
        self.assertIsInstance(owner, CampaignMaintenance)
        self.assertTrue(owner.complete)
        self.assertEqual(owner.deadline_ns, 60 * SECOND)
        self.assertGreaterEqual(runtime.now_ns, 53 * SECOND)
        self.assertLess(runtime.now_ns, 60 * SECOND)
        self.assertLessEqual(owner.polls, 16)
        self.assertEqual(runtime.calls[0][:2],
                         ("backup.restore", ["backup", "restore", "campaign-source"]))
        self.assertTrue(all(command == "backup.operation"
            and arguments == ["backup", "operation", OPERATION]
            for command, arguments, _now, _timeout in runtime.calls[1:]))

    def test_pending_operation_exhausts_same_sixteen_polls_without_resubmission(self):
        runtime = Runtime(complete_at_ns=61 * SECOND)
        with self.assertRaisesRegex(CampaignError, "campaign_maintenance_poll_bound"):
            action(runtime).maintenance("restore", 0, 60 * SECOND)
        self.assertEqual(len(runtime.calls), 17)
        self.assertEqual(sum(command == "backup.restore"
                             for command, *_ in runtime.calls), 1)
        self.assertGreater(runtime.now_ns, 53 * SECOND)
        self.assertLess(runtime.now_ns, 60 * SECOND)
        self.assertTrue(all(0 < timeout <= 5 for *_rest, timeout in runtime.calls))
