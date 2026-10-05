# req: END-003, END-007, END-008
"""Backlog evidence must come from the selected native projection fence."""
import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from lib.ticketdesk_lifecycle_observations import ObservationError, projection_backlog


class ActualProjectionFence(unittest.TestCase):
    identity = {"contract_lineage": "TicketDeskLifecycle", "projection_id": 1,
                "projection_plan_hash": "a" * 64}

    def health(self, head=7):
        return {"schema": "riffdb.cli.output/v1", "command": "server.health", "ok": True,
                "result": {"last_commit_sequence": head}}

    def projection(self, frontier="5"):
        fence = {"type": "applied_through", "sequence": frontier}
        return {"schema": "riffdb.cli.output/v1", "command": "projection.query", "ok": True,
                "result": {"status": "ready", "frontier": fence,
                    "observed_fence": {"identity": copy.deepcopy(self.identity),
                        "generation": "1", "frontier": copy.deepcopy(fence)}}}

    def test_backlog_is_measured_in_application_sequences_not_assumed_zero(self):
        self.assertEqual(projection_backlog(self.health(), self.projection(), self.identity), 2)
        self.assertEqual(projection_backlog(self.health(), self.projection("7"), self.identity), 0)
        # The query may observe commands committed after the earlier health RPC.
        self.assertEqual(projection_backlog(self.health(), self.projection("9"), self.identity), 0)

    def test_unavailable_or_partial_observations_never_become_zero_backlog(self):
        for mutation in ("absent", "pending", "failed", "empty_frontier", "unbound"):
            with self.subTest(mutation=mutation):
                value = self.projection()
                if mutation == "absent":
                    value = None
                elif mutation == "pending":
                    value["result"]["status"] = "pending"
                elif mutation == "failed":
                    value["ok"] = False
                elif mutation == "empty_frontier":
                    value["result"]["frontier"] = {"type": "before_first"}
                else:
                    del value["result"]["observed_fence"]
                with self.assertRaises(ObservationError):
                    projection_backlog(self.health(), value, self.identity)

    def test_another_projection_or_inconsistent_fence_cannot_satisfy_the_selected_query(self):
        for mutation in ("lineage", "id", "plan", "frontier", "generation"):
            with self.subTest(mutation=mutation):
                value = self.projection()
                fence = value["result"]["observed_fence"]
                if mutation == "lineage":
                    fence["identity"]["contract_lineage"] = "AnotherLineage"
                elif mutation == "id":
                    fence["identity"]["projection_id"] = 2
                elif mutation == "plan":
                    fence["identity"]["projection_plan_hash"] = "b" * 64
                elif mutation == "frontier":
                    fence["frontier"]["sequence"] = "6"
                else:
                    fence["generation"] = "0"
                with self.assertRaises(ObservationError):
                    projection_backlog(self.health(), value, self.identity)

    def test_noncanonical_or_unbounded_numbers_cannot_be_native_evidence(self):
        for frontier in (True, 5.0, "05", "-1", str(2**64), "1" * 100):
            with self.subTest(frontier=frontier), self.assertRaises(ObservationError):
                projection_backlog(self.health(), self.projection(frontier), self.identity)
        for head in (True, -1, 1.0, 2**64, None):
            with self.subTest(head=head), self.assertRaises(ObservationError):
                projection_backlog(self.health(head), self.projection(), self.identity)


if __name__ == "__main__":
    unittest.main()
