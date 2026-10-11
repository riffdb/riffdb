# req: END-007, END-008, END-010
"""Logical bytes preserve native byte values and fail closed on missing evidence."""
from pathlib import Path
import json
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"scripts"))
from lib.ticketdesk_lifecycle_logical_bytes import LogicalBytesError, LogicalBytesLog, LogicalBytesSamples, parse_sample

def line(**changes):
    fields=dict(root=2,incarnation=1,watermark=0,head=602,ceiling=601,through=601,rows=600,canonical_history_value_bytes=100000,removed_key_bytes=4800,capture_age_us=1000,pages=3)
    fields.update(changes)
    return b"riffdb-retention-logical-bytes-v1\t"+b"\t".join(f"{key}={value}".encode() for key,value in fields.items())+b"\n"

class LogicalBytesEvidence(unittest.TestCase):
    def test_actual_component_bytes_are_preserved_without_aggregate_claim(self):
        value=parse_sample(line())
        self.assertIsNotNone(value,"a source-only reader silently discards logical accounting")
        self.assertEqual(value["canonical_history_value_bytes"],100000)
        self.assertEqual(value["removed_key_bytes"],4800)
        self.assertNotIn("eligible_history_bytes",value)

    def test_unknown_partial_stale_refused_duplicate_overflow_and_bad_numbers_fail_closed(self):
        examples=[line()[:-1],line().replace(b"v1",b"v2",1),
                  line().replace(b"root=2",b"root=2\troot=2"),
                  line().replace(b"pages=3",b"unknown=3"),
                  b"riffdb-retention-logical-bytes-refusal-v1\tkind=LimitExceeded\n"]
        for changes in (dict(root=0),dict(root=3),dict(incarnation=0),dict(watermark=602),
                        dict(ceiling=603),dict(through=602),dict(through=0),dict(rows=0),
                        dict(rows=602),dict(rows=1),dict(removed_key_bytes=4799),
                        dict(canonical_history_value_bytes=0),dict(canonical_history_value_bytes=2**64-1),
                        dict(pages=2),dict(pages=602),dict(capture_age_us=30000000),
                        dict(root=True),dict(root=-1),dict(root="02"),dict(root=2**64),dict(root=1.5)):
            examples.append(line(**changes))
        for encoded in examples:
            with self.subTest(encoded=encoded[:60]),self.assertRaises(LogicalBytesError):
                parse_sample(encoded)
        self.assertIsNone(parse_sample(b"another diagnostic with private content\n"))

    def test_complete_empty_protected_prefix_and_grouped_rows_preserve_native_counts(self):
        empty=line(watermark=8,head=10,ceiling=5,through=8,rows=0,canonical_history_value_bytes=0,removed_key_bytes=0,pages=1)
        self.assertEqual(parse_sample(empty)["canonical_history_value_bytes"],0)
        grouped=line(head=603,ceiling=602,through=602,rows=3,removed_key_bytes=24,pages=1)
        self.assertEqual(parse_sample(grouped)["rows"],3)
        for encoded in (empty.replace(b"removed_key_bytes=0",b"removed_key_bytes=8"),
                        grouped.replace(b"rows=3",b"rows=2")):
            with self.assertRaises(LogicalBytesError):parse_sample(encoded)

    def test_fresh_coverage_uses_independent_incarnation_and_never_qualifies_aggregate(self):
        with tempfile.TemporaryDirectory() as directory:
            with LogicalBytesSamples(directory,600,lambda:1) as samples:
                for seconds in range(0,600,60):samples.observe(line(root=2+seconds),seconds*1000000000)
                result=samples.finish(600000000000)
            self.assertEqual(result["samples"],10)
            self.assertFalse(result["qualifies_complete_resource_model"])
            records=[json.loads(value) for value in samples.path.read_text().splitlines()]
            self.assertEqual(records[0]["canonical_history_value_bytes"],100000)
            self.assertNotIn("eligible_history_bytes",result)
        for incarnation in (None,2,True):
            with tempfile.TemporaryDirectory() as directory:
                with LogicalBytesSamples(directory,60,lambda:incarnation) as samples:
                    with self.assertRaisesRegex(LogicalBytesError,"^logical_bytes_incarnation$"):
                        samples.observe(line(),0)

    def test_missing_gapped_or_rewound_samples_cannot_become_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            with LogicalBytesSamples(directory,300,lambda:1) as samples:
                with self.assertRaises(LogicalBytesError):samples.finish(300000000000)
                samples.observe(line(root=4,watermark=1),1000000000)
                for encoded,elapsed in ((line(root=2),2000000000),(line(root=6,watermark=0),2000000000),
                                        (line(root=6,head=601),2000000000),(line(root=6,watermark=1),97000000000)):
                    with self.assertRaises(LogicalBytesError):samples.observe(encoded,elapsed)
                # New holds may lower eligibility while durable head and watermark stay monotonic.
                samples.observe(line(root=6,watermark=1,ceiling=5,through=5,rows=4,canonical_history_value_bytes=999,removed_key_bytes=32,pages=1),61000000000)
                with self.assertRaises(LogicalBytesError):samples.finish(300000000000)

    def test_log_skips_seed_preserves_partial_lines_and_refuses_replacement(self):
        from lib.ticketdesk_lifecycle_provider_budget import ProviderBudgetError
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            path=root/"server.stderr"
            path.write_bytes(line(root=999))
            with LogicalBytesLog(root,60,lambda:1) as log:
                with path.open("ab") as output:output.write(line()[:-1])
                log.poll(1000000000)
                self.assertEqual(log.samples.count,0)
                with path.open("ab") as output:output.write(b"\n")
                log.poll(2000000000)
                self.assertEqual(log.samples.count,1)
                self.assertEqual(log.finish(60000000000)["samples"],1)
                path.rename(root/"old.stderr")
                path.write_bytes(line())
                with self.assertRaises(ProviderBudgetError):log.poll(3000000000)
