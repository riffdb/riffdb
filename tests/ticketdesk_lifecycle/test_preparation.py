# req: END-003, END-007, END-008
"""Preparation observations remain independent of producer success booleans."""
import copy
import importlib.machinery
import importlib.util
from pathlib import Path
import unittest

SOURCE = Path(__file__).resolve().parents[2] / "scripts/ticketdesk-lifecycle-smoke"
loader = importlib.machinery.SourceFileLoader("preparation", str(SOURCE))
spec = importlib.util.spec_from_loader(loader.name, loader)
preparation = importlib.util.module_from_spec(spec)
loader.exec_module(preparation)


def observation():
    return {"schema": "riffdb.cli.output/v1", "command": "projection.query", "ok": True,
            "result": {"status": "ready", "rows": [{"group": [
                {"type": "uuid", "value": "01900000-0000-7000-8001-000000000000"}],
                "values": {"fields": [{"field_id": 1, "value": {"type": "i64", "value": "1"}}]}}],
                "observed_fence": {"identity": {"contract_lineage": "TicketDeskLifecycle",
                    "projection_id": 1, "projection_plan_hash": "7da25538637dd8c1d35f650daed706a83743a7bf6d7deb18191e7972170bc0d6"},
                    "generation": "1", "frontier": {"type": "applied_through", "sequence": "7"}},
                "frontier": {"type": "applied_through", "sequence": "7"}}}


class ProjectionReconciliation(unittest.TestCase):
    def test_ready_does_not_prove_the_count_or_partition_or_identity(self):
        good = observation()
        preparation.validate_projection(good, 1, 7, 1, good["result"]["observed_fence"]["identity"]["projection_plan_hash"])
        for mutation in ("count", "partition", "identity", "frontier", "field"):
            with self.subTest(mutation=mutation):
                value = copy.deepcopy(good)
                result = value["result"]
                if mutation == "count":
                    result["rows"][0]["values"]["fields"][0]["value"]["value"] = "2"
                elif mutation == "partition":
                    result["rows"][0]["group"][0]["value"] = "01900000-0000-7000-8001-000000000001"
                elif mutation == "identity":
                    result["observed_fence"]["identity"]["projection_id"] = 2
                elif mutation == "frontier":
                    result["frontier"]["sequence"] = "6"
                else:
                    result["rows"][0]["values"]["fields"][0]["field_id"] = 2
                with self.assertRaises(preparation.SmokeError):
                    preparation.validate_projection(value, 1, 7, 1, good["result"]["observed_fence"]["identity"]["projection_plan_hash"])

    def test_partial_population_round_counts_only_organization_zero_updates(self):
        value = observation()
        plan_hash = value["result"]["observed_fence"]["identity"]["projection_plan_hash"]
        value["result"]["rows"][0]["values"]["fields"][0]["value"]["value"] = "1024"
        preparation.validate_projection(value, 1, 7, 30000, plan_hash)
        value["result"]["rows"][0]["values"]["fields"][0]["value"]["value"] = "1025"
        preparation.validate_projection(value, 1, 7, 65537, plan_hash)
        with self.assertRaises(preparation.SmokeError):
            preparation.validate_projection(value, 1, 7, 65536, plan_hash)


if __name__ == "__main__":
    unittest.main()
