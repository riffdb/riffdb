# req: END-007, END-008, END-010
"""Physical allocation preserves native protection categories and unknowns."""
import json
import sys
import tempfile
from pathlib import Path
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
from lib.ticketdesk_lifecycle_physical_bytes import PhysicalBytesError,PhysicalBytesLog,PhysicalBytesSamples,parse_sample


def line(**changes):
    fields=dict(root=10,incarnation=7,watermark=50,minimum_resume=100,active_generation=4,
        sealed_extents=3,eligible_allocated_bytes=512,protected_allocated_bytes=1024,
        reference_protected_allocated_bytes=1024,reader_protected_allocated_bytes=0,
        reader_held=0,capture_age_us=200000,pages=4,diagnostic_pin_excluded=1)
    fields.update(changes)
    return b'riffdb-retention-physical-bytes-v1\t'+b'\t'.join(f'{key}={value}'.encode() for key,value in fields.items())+b'\n'


class PhysicalBytesEvidence(unittest.TestCase):
    def test_allocated_and_protected_components_are_preserved_without_an_aggregate_claim(self):
        value=parse_sample(line())
        self.assertEqual(value['eligible_allocated_bytes'],512)
        self.assertEqual(value['reference_protected_allocated_bytes'],1024)
        self.assertEqual(value['diagnostic_pin_excluded'],1)
        self.assertNotIn('eligible_history_bytes',value)
        self.assertIsNone(parse_sample(b'unrelated diagnostic\n'))

    def test_noncanonical_incomplete_expired_refused_or_inconsistent_records_refuse(self):
        examples=[line()[:-1],line().replace(b'v1',b'v2',1),line().replace(b'root=10',b'root=10\troot=10'),
            line().replace(b'pages=4',b'unknown=4'),b'riffdb-retention-physical-bytes-refusal-v1\tkind=LimitExceeded\n']
        for changes in (dict(root=0),dict(root=11),dict(root=True),dict(root='010'),dict(root=-1),dict(root=1.5),
            dict(root=2**64),dict(incarnation=0),dict(minimum_resume=0),dict(active_generation=3),
            dict(sealed_extents=4097,active_generation=9000),dict(eligible_allocated_bytes=511),
            dict(protected_allocated_bytes=0),dict(eligible_allocated_bytes=2**64-512),
            dict(reader_held=2),dict(reader_held=1),dict(reader_protected_allocated_bytes=512),
            dict(diagnostic_pin_excluded=0),dict(pages=3),dict(pages=133),dict(capture_age_us=30000000)):
            examples.append(line(**changes))
        for encoded in examples:
            with self.subTest(encoded=encoded[:60]),self.assertRaises(PhysicalBytesError):parse_sample(encoded)

    def test_complete_empty_or_reader_protected_prefix_remains_a_distinct_component(self):
        empty=parse_sample(line(active_generation=1,sealed_extents=0,eligible_allocated_bytes=0,
            protected_allocated_bytes=0,reference_protected_allocated_bytes=0,pages=1))
        self.assertEqual(empty['sealed_extents'],0)
        held=parse_sample(line(eligible_allocated_bytes=0,protected_allocated_bytes=1536,
            reader_protected_allocated_bytes=512,reader_held=1))
        self.assertEqual(held['reader_protected_allocated_bytes'],512)

    def test_each_window_is_complete_and_bound_to_the_independent_incarnation(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with PhysicalBytesSamples(root,600,lambda:7) as samples:
                for second in range(0,600,60):samples.observe(line(root=10+second),second*1000000000)
                summary=samples.finish(600000000000)
                self.assertEqual(summary['samples'],10)
                self.assertFalse(summary['qualifies_complete_resource_model'])
                self.assertEqual(summary['basis'],'native_original_sealed_prefix_allocated_blocks')
            rows=[json.loads(row) for row in (root/'physical-byte-samples.jsonl').read_bytes().splitlines()]
            self.assertEqual(rows[-1]['elapsed_ns'],540000000000)
            self.assertEqual((root/'physical-byte-samples.jsonl').stat().st_mode & 0o777,0o600)

    def test_gaps_missing_windows_unknown_identity_and_rewound_roots_refuse(self):
        with tempfile.TemporaryDirectory() as directory:
            with PhysicalBytesSamples(Path(directory),300,lambda:7) as samples:
                samples.observe(line(),0)
                for encoded,elapsed in ((line(incarnation=8),1000000000),(line(root=8),1000000000),
                    (line(watermark=49),1000000000),(line(minimum_resume=99),1000000000),
                    (line(),96000000000)):
                    with self.assertRaises(PhysicalBytesError):samples.observe(encoded,elapsed)
                with self.assertRaises(PhysicalBytesError):samples.finish(300000000000)
        with tempfile.TemporaryDirectory() as directory:
            with PhysicalBytesSamples(Path(directory),300,lambda:None) as samples:
                with self.assertRaises(PhysicalBytesError):samples.observe(line(),0)

    def test_log_preserves_partial_records_and_refuses_replacement(self):
        from lib.ticketdesk_lifecycle_provider_budget import ProviderBudgetError
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);path=root/'server.stderr';path.write_bytes(line(root=999))
            with PhysicalBytesLog(root,60,lambda:7) as log:
                with path.open('ab') as output:output.write(line()[:-1])
                log.poll(1000000000);self.assertEqual(log.samples.count,0)
                with path.open('ab') as output:output.write(b'\n')
                log.poll(2000000000);self.assertEqual(log.samples.count,1)
                self.assertEqual(log.finish(60000000000)['samples'],1)
                path.rename(root/'old.stderr');path.write_bytes(line())
                with self.assertRaises(ProviderBudgetError):log.poll(3000000000)


if __name__=='__main__':unittest.main()
