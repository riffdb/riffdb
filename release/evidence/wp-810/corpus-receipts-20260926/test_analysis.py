# req: SIM-004, REC-001
import copy
import json
from pathlib import Path
import unittest

from analyze import MOVED, check_repair


class RepairEvidence(unittest.TestCase):
    def setUp(self):
        summary = json.loads((Path(__file__).parent / "summary.json").read_text())
        self.rows = copy.deepcopy(summary["audit_populations"])

    def test_all_coordinates_and_six_causal_movements_are_retained(self):
        self.assertEqual(set(check_repair(**self.rows)), MOVED)
        self.assertEqual(len(self.rows["fixed"]), 21)

    def test_repair_may_not_change_a_numeric_threshold(self):
        self.rows["fixed"]["0x51c2c067"]["expectation_verdicts"][0] = "+TornDecisionsAtLeast(29)"
        with self.assertRaisesRegex(AssertionError, "weakened a predicate"):
            check_repair(**self.rows)

    def test_repair_may_not_change_campaign_execution(self):
        self.rows["fixed"]["0x51c2c067"]["report"]["torn_decisions"] += 1
        with self.assertRaisesRegex(AssertionError, "changed campaign execution"):
            check_repair(**self.rows)

    def test_historical_coordinate_may_not_be_deleted(self):
        del self.rows["fixed"]["0x51c2c067"]
        with self.assertRaises(AssertionError):
            check_repair(**self.rows)

    def test_unrelated_causal_child_is_refused(self):
        self.rows["current"]["0x51c2c067"]["report"]["torn_decisions"] += 1
        with self.assertRaisesRegex(AssertionError, "differ from the causal child"):
            check_repair(**self.rows)


if __name__ == "__main__":
    unittest.main()
