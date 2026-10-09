# req: END-007, END-008, END-009, END-010
"""Protocol fixtures prove refusal, not actual backup or restore success."""
from copy import deepcopy
import unittest

from lib.ticketdesk_lifecycle_campaign_maintenance import CampaignMaintenance, MaintenanceError, SECOND

OPERATION = "01900000-0000-7000-8001-000000000001"


def output(phase="accepted", *, polling=False):
    return {"schema": "riffdb.cli.output/v1", "command": "backup.operation" if polling else "backup.create",
            "ok": True, "result": {"status": "found" if polling else "accepted",
                "maintenance_operation_id": OPERATION, "kind": "create_backup", "backup_name": "campaign",
                "input_hash": "ab" * 32, "phase": phase}}


class Operator:
    def __init__(self):
        self.calls, self.value, self.last_ns, self.duration_ns = [], output(), None, SECOND

    def run_as(self, credential, command, arguments, *, elapsed_ns, timeout_s):
        self.calls.append((command, arguments))
        self.last_ns = elapsed_ns + self.duration_ns
        return deepcopy(self.value)

    @staticmethod
    def _maintenance_uncertain(command, value):
        from lib.ticketdesk_lifecycle_campaign_operator import CampaignOperator
        return CampaignOperator._maintenance_uncertain(command, value)

    @staticmethod
    def _poll_unavailable(command, arguments, value):
        from lib.ticketdesk_lifecycle_campaign_operator import CampaignOperator
        owner = type("Owner", (), {"pending_maintenance": {OPERATION}})()
        return CampaignOperator._poll_unavailable(owner, command, arguments, value)


class CampaignMaintenanceTests(unittest.TestCase):
    def owner(self):
        operator = Operator()
        return operator, CampaignMaintenance(operator, "fixture", "create", "campaign",
                                             started_ns=0, deadline_ns=60 * SECOND)

    def test_returning_after_original_deadline_is_sticky_failure(self):
        operator, owner = self.owner()
        operator.duration_ns = 60 * SECOND
        operator.value = output("succeeded")
        with self.assertRaisesRegex(MaintenanceError, "campaign_maintenance_deadline"):
            owner.start(0)
        with self.assertRaisesRegex(MaintenanceError, "campaign_maintenance_failed"):
            owner.poll(60 * SECOND)
        self.assertEqual(len(operator.calls), 1)

    def test_one_original_mutation_and_only_its_read_only_polls(self):
        operator, owner = self.owner()
        self.assertFalse(owner.start(0))
        for index, phase in enumerate(("draining", "offline", "artifact_published", "validating", "succeeded"), 1):
            operator.value = output(phase, polling=True)
            self.assertEqual(owner.poll(index * SECOND), phase == "succeeded")
        self.assertEqual(operator.calls[0], ("backup.create", ["backup", "create", "campaign"]))
        self.assertTrue(all(call == ("backup.operation", ["backup", "operation", OPERATION])
                            for call in operator.calls[1:]))
        with self.assertRaises(MaintenanceError):
            owner.start(7 * SECOND)
        self.assertEqual(len(operator.calls), 6)

    def test_identity_phase_and_closed_result_refusals_are_sticky(self):
        changes = ({"maintenance_operation_id": "01900000-0000-7000-8001-000000000002"},
                   {"input_hash": "cd" * 32}, {"backup_name": "other"}, {"kind": "restore_backup"},
                   {"status": []}, {"phase": []}, {"phase": "accepted"},
                   {"archive_restore": {}}, {"failure": "storage_unavailable"})
        for change in changes:
            with self.subTest(change=change):
                operator, owner = self.owner()
                operator.value = output("offline")
                owner.start(0)
                operator.value = output("succeeded", polling=True)
                operator.value["result"].update(change)
                with self.assertRaises(MaintenanceError):
                    owner.poll(SECOND)
                with self.assertRaisesRegex(MaintenanceError, "campaign_maintenance_failed"):
                    owner.poll(2 * SECOND)
                self.assertEqual(len(operator.calls), 2)

    def test_exact_read_only_transport_unknown_does_not_resubmit_mutation(self):
        operator, owner = self.owner()
        operator.value = {"schema": "riffdb.cli.output/v1", "command": "backup.create", "ok": False,
            "error": {"type": "uncertain", "code": "outcome_unknown", "message": "fixture",
                "recovery_action": "poll_maintenance_operation", "maintenance_operation_id": OPERATION}}
        self.assertFalse(owner.start(0))
        for index, (code, message) in enumerate((("transport_unavailable", "the gRPC transport is unavailable"),
                ("tls_connection_failed", "the verified TLS channel could not connect")), 1):
            operator.value = {"schema": "riffdb.cli.output/v1", "command": "backup.operation", "ok": False,
                              "error": {"type": "client", "code": code, "message": message}}
            self.assertFalse(owner.poll(index * SECOND))
        operator.value = output("succeeded", polling=True)
        self.assertTrue(owner.poll(3 * SECOND))
        self.assertEqual([call[0] for call in operator.calls], ["backup.create"] + ["backup.operation"] * 3)

    def test_poll_population_is_finite_and_uses_the_same_operation(self):
        operator, owner = self.owner()
        owner.start(0)
        operator.value = output("accepted", polling=True)
        for index in range(1, 17):
            self.assertFalse(owner.poll(index * SECOND))
        with self.assertRaises(MaintenanceError):
            owner.poll(17 * SECOND)
        self.assertEqual(len(operator.calls), 17)
