# req: PERF-018
"""Semantic checks for rank migration and conditional-distribution weighting."""
import hashlib
from pathlib import Path
import tempfile
import unittest
from decompose import exposure_split, load_rows, percentile, subtract_and_requantile, wait_partition, weighted_p99


def samples(false, true):
    return [dict(elapsed_ns=n, drain_required=flag) for flag, values in ((False,false),(True,true)) for n in values]


class DecompositionTests(unittest.TestCase):
    def test_removing_intervals_reorders_tail_instead_of_subtracting_percentiles(self):
        rows = [dict(elapsed_ns=10, wait=0) for _ in range(98)]
        rows += [dict(elapsed_ns=100, wait=90), dict(elapsed_ns=101, wait=1)]
        self.assertEqual(percentile([r['elapsed_ns'] for r in rows]), 100)
        self.assertEqual(percentile([r['wait'] for r in rows]), 1)
        self.assertEqual(subtract_and_requantile(rows, lambda r:r['wait']), 10)
        self.assertNotEqual(100-1, 10)

    def test_prevalence_alone_can_move_p99_with_unchanged_conditional_latencies(self):
        off = samples([1]*199, [100])
        on = samples([1]*197, [100]*3)
        result = exposure_split(off,on)
        self.assertEqual(result['matrix_p99_ns'], [[1,100],[1,100]])
        self.assertEqual(result['exposure_average_ns'], 99)
        self.assertEqual(result['within_state_average_ns'], 0)
        self.assertIsNone(result['causal_archive_specific_residual_ns'])

    def test_unchanged_prevalence_assigns_movement_to_conditional_distributions(self):
        result = exposure_split(samples([1]*98,[100]*2), samples([1]*98,[150]*2))
        self.assertEqual(result['exposure_average_ns'], 0)
        self.assertEqual(result['within_state_average_ns'], 50)
        self.assertIsNone(result['causal_archive_specific_residual_ns'])

    def test_unequal_population_sizes_recover_each_empirical_endpoint(self):
        off = samples(list(range(100)),list(range(500,503)))
        on = samples(list(range(40)),list(range(700,707)))
        result = exposure_split(off,on)
        self.assertEqual(result['matrix_p99_ns'][0][0], percentile([r['elapsed_ns'] for r in off]))
        self.assertEqual(result['matrix_p99_ns'][1][1], percentile([r['elapsed_ns'] for r in on]))

    def test_exact_mass_at_99_percent_does_not_round_into_the_upper_stratum(self):
        self.assertEqual(weighted_p99(samples([1],[100]),1,100),1)
        self.assertEqual(weighted_p99(samples([1],[100]),2,100),100)

    def test_interval_outside_request_is_refused_without_saturating(self):
        with self.assertRaises(AssertionError):
            subtract_and_requantile([dict(elapsed_ns=1)], lambda _:2)

    def test_wait_partition_clips_sync_to_its_overlap_with_the_wait(self):
        group=[0]*17
        group[5],group[6],group[8],group[9]=13,20,10,24
        self.assertEqual(wait_partition(group),(3,7,4))
        group[8],group[9]=15,18
        self.assertEqual(wait_partition(group),(0,3,0))
        group[8],group[9]=10,12
        self.assertEqual(wait_partition(group),(2,0,0))

    def test_host_invalid_source_is_refused_before_reading_requests(self):
        with tempfile.TemporaryDirectory() as tmp, self.assertRaises(AssertionError):
            load_rows(Path(tmp), dict(qualified_pair=False))

    def test_changed_joined_requests_are_refused_by_the_banked_digest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'01-disabled').mkdir()
            (root/'01-disabled/joined-requests.json').write_text('[1]')
            expected=dict(cell='01-disabled',host_valid=True,
                preflight=dict(valid=True),postflight=dict(valid=True),
                source_file_digests={'joined-requests.json':hashlib.sha256(b'[]').hexdigest()})
            with self.assertRaises(AssertionError):
                load_rows(root,dict(qualified_pair=True,cells=[expected,{}]))

    def test_missing_conditional_population_is_refused(self):
        with self.assertRaises(AssertionError):
            weighted_p99(samples([1,2],[]),1,2)


if __name__ == '__main__':
    unittest.main()
