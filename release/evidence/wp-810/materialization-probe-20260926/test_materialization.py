# req: PERF-018
"""Reject incomplete evidence and verify interval arithmetic independently."""
import copy
from pathlib import Path
import tempfile
import unittest

from analyze_materialization import (Intervals, check_coverage, checkpoint_completion_coverage, checkpoint_sync_context, describe_interval,
                                     parse_log, phase_indexes, validate_process)


def fixture():
    # One physical flush publishes two command groups.
    return {
        "checkpoint": [[1, 10, 80, 100, 3, 3, 900, 2, 0, 20, 30, 60]],
        "group": [[1, 1, 2, 2, 5, 50, 70, 1, 40, 72, 75, 76, 2, 1, 1, 0, 0, 35, 38, 39],
                  [2, 3, 3, 1, 6, 50, 70, 1, 0, 0, 78, 79, 2, 1, 0, 0, 0, 35, 38, 39]],
        "io": [[1, 1, 2, 1, 45, 55, 8192, 0, 1],
               [2, 1, 3, 2, 60, 78, 0, 0, 1],
               [3, 0, 0, 4, 52, 62, 4096, 0, 1]],
        "coverage": [{"final": 1, "groups": 2, "reserved_groups": 2,
                      "checkpoints": 1, "reserved_checkpoints": 1,
                      "io": 3, "reserved_io": 3,
                      "dropped_groups": 0, "dropped_checkpoints": 0, "dropped_io": 0}],
    }


class MaterializationAnalysis(unittest.TestCase):
    def test_resize_sync_sequence_keeps_checkpoint_and_phase_identity(self):
        rows = [[1,1,2,3,10,20,4096,0,1], [2,0,0,4,11,21,4,0,1],
                [3,1,2,2,22,30,0,0,1], [4,1,3,2,40,50,0,0,1]]
        growth, commit = checkpoint_sync_context(rows)
        self.assertTrue(growth["immediately_preceded_by_resize"])
        self.assertEqual(growth["previous_same_phase_io"], rows[0])
        self.assertFalse(commit["immediately_preceded_by_resize"])

    def test_completion_after_load_is_distinct_from_preclose_census(self):
        c = {"failures": 0, "starts": 1, "materialized": 0, "installed": 0}
        result = checkpoint_completion_coverage(c, fixture()["checkpoint"], 79)
        self.assertEqual(result["preclose_installed"], 0)
        self.assertEqual(result["final_installed"], 1)
        with self.assertRaisesRegex(ValueError, "within load period"):
            checkpoint_completion_coverage(c, fixture()["checkpoint"], 90)

    def test_shared_flush_deduplicated_and_nonblocking_wait_retained(self):
        coverage, flushes = validate_process(fixture())
        self.assertEqual(coverage["groups"], 2)
        self.assertEqual(flushes, {1: (35, 38, 39, 50, 70)})

    def test_union_prevents_concurrent_io_double_count(self):
        p = fixture()
        phases, engine = phase_indexes(p["checkpoint"], p["io"])
        result = describe_interval(50, 70, phases, engine)
        self.assertEqual(result["checkpoint_phase_overlap_ns"]["replay"], 10)
        self.assertEqual(result["checkpoint_phase_overlap_ns"]["durable_commit"], 10)
        self.assertEqual(result["engine_io_overlap_ns"]["all_engine_io"], 20)
        self.assertEqual(result["engine_io_overlap_ns"]["checkpoint_io"], 15)
        self.assertEqual(result["without_observed_engine_io_ns"], 0)

    def test_half_open_boundaries_and_explicit_remainder(self):
        index = Intervals([(10, 20), (15, 25), (30, 40)])
        self.assertEqual(index.overlap(25, 30), 0)
        self.assertEqual(index.overlap(0, 100), 25)
        result = describe_interval(0, 100, {}, {"all_engine_io": index})
        self.assertEqual(result["outside_checkpoint_ns"], 100)
        self.assertEqual(result["without_observed_engine_io_ns"], 75)

    def test_missing_final_incomplete_reservation_and_overflow_refused(self):
        for key, value in (("final", 0), ("reserved_io", 4), ("dropped_io", 1)):
            with self.subTest(key=key):
                p = fixture()
                p["coverage"][-1][key] = value
                with self.assertRaises(ValueError):
                    check_coverage(p)

    def test_wrong_phase_unknown_checkpoint_and_io_failure_refused(self):
        for column, value in ((1, 2), (2, 3), (7, 1)):
            with self.subTest(column=column):
                p = fixture()
                p["io"][0][column] = value
                with self.assertRaises(ValueError):
                    validate_process(p)

    def test_inconsistent_flush_early_publication_and_duplicate_commands_refused(self):
        for row, column, value in ((1, 6, 71), (0, 10, 65), (1, 1, 2)):
            with self.subTest(column=column):
                p = fixture()
                p["group"][row][column] = value
                with self.assertRaises(ValueError):
                    validate_process(p)

    def test_checkpoint_overlap_and_reversed_lifecycle_refused(self):
        p = fixture()
        p["checkpoint"][0][11] = 90
        with self.assertRaises(ValueError):
            validate_process(p)
        p = fixture()
        second = copy.copy(p["checkpoint"][0])
        second[0] = 2
        p["checkpoint"].append(second)
        p["coverage"][-1].update(checkpoints=2, reserved_checkpoints=2)
        with self.assertRaises(ValueError):
            validate_process(p)

    def test_parser_refuses_skipped_rows_and_wrong_width(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace"
            for row in ("2,0,0,1,1,2,8,0,0", "1,0,0,1,1,2,8,0"):
                path.write_text("riffdb-wp810-io-v1\tpid=1\t" + row + "\n")
                with self.assertRaises(ValueError):
                    parse_log(path)

    def test_parser_keeps_processes_separate_and_retains_last_footer(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace"
            path.write_text("riffdb-wp810-io-v1\tpid=1\t1,0,0,1,1,2,8,0,0\n"
                            "riffdb-wp810-io-v1\tpid=2\t1,0,0,1,1,2,8,0,0\n"
                            "riffdb-wp810-coverage-v1\tpid=1\tfinal=0\n"
                            "riffdb-wp810-coverage-v1\tpid=1\tfinal=1\n")
            processes = parse_log(path)
            self.assertEqual(len(processes), 2)
            self.assertEqual(len(processes[1]["io"]), 1)
            self.assertEqual(processes[1]["coverage"][-1]["final"], 1)


if __name__ == "__main__":
    unittest.main()
