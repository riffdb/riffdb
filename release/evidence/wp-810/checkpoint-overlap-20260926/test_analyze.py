import unittest

from analyze import classify


def group(identity, sequence, flush, start, end):
    return [identity, sequence, sequence, 1, start, start, end,
            flush, start, end, end, end, 1, 1, 1, 0, 1]


def checkpoint(identity, start, materialized, installed):
    return [identity, start, materialized, installed, 1, 1, 1, 2, 0]


# req: PERF-018
class OverlapTests(unittest.TestCase):
    def test_shared_flush_counts_once_and_excludes_unmeasured_groups(self):
        result = classify([group(1, 1, 1, 10, 30), group(2, 2, 1, 10, 30),
                           group(3, 3, 2, 40, 50)], [checkpoint(1, 5, 35, 38)], [1, 2])
        self.assertEqual(result["measured_groups"], 2)
        self.assertEqual(result["measured_flushes"], 1)
        self.assertEqual(result["phase_counts"]["materializing"], 1)

    def test_touching_endpoints_do_not_overlap(self):
        result = classify([group(1, 1, 1, 1, 10), group(2, 2, 2, 20, 30)],
                          [checkpoint(1, 10, 15, 20)], [1, 2])
        self.assertEqual(result["phase_counts"]["no_checkpoint"], 2)

    def test_materialized_boundary_separates_phases(self):
        result = classify([group(1, 1, 1, 10, 20), group(2, 2, 2, 20, 30)],
                          [checkpoint(1, 5, 20, 35)], [1, 2])
        self.assertEqual(result["phase_counts"]["materializing"], 1)
        self.assertEqual(result["phase_counts"]["post_materialization"], 1)

    def test_spanning_interval_keeps_disjoint_remainder(self):
        result = classify([group(1, 1, 1, 1, 100_000_001)],
                          [checkpoint(1, 10, 20, 30), checkpoint(2, 40, 50, 60)], [1, 1])
        row = result["slow_flushes"][0]
        self.assertEqual(result["over_50_ms"]["both"], 1)
        self.assertEqual(row["materializing_overlap_ns"], 20)
        self.assertEqual(row["post_materialization_overlap_ns"], 20)
        self.assertEqual(row["outside_checkpoint_ns"], 99_999_960)

    def test_inconsistent_shared_flush_refuses(self):
        with self.assertRaisesRegex(ValueError, "shared flush"):
            classify([group(1, 1, 1, 10, 30), group(2, 2, 1, 10, 31)],
                     [checkpoint(1, 5, 35, 38)], [1, 2])

    def test_missing_checkpoint_completion_refuses(self):
        with self.assertRaisesRegex(ValueError, "incomplete"):
            classify([group(1, 1, 1, 10, 30)], [checkpoint(1, 5, 0, 0)], [1, 1])

    def test_overlapping_checkpoint_lifetimes_refuse(self):
        with self.assertRaisesRegex(ValueError, "overlapping checkpoints"):
            classify([group(1, 1, 1, 10, 30)],
                     [checkpoint(1, 5, 20, 35), checkpoint(2, 34, 40, 50)], [1, 1])


if __name__ == "__main__":
    unittest.main()
