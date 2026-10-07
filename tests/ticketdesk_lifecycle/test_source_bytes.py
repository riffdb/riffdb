# req: END-007, END-008, END-010
"""Source receipt bytes must stay distinct from total eligible history."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from lib.ticketdesk_lifecycle_source_bytes import SourceBytesError, SourceBytesLog, SourceBytesSamples, parse_sample


def line(**changes):
    fields = dict(root=2, incarnation=1, resume=3, ceiling=603, rows=600,
                  canonical_receipt_bytes=100000, key_bytes=4800,
                  capture_age_us=1000, collection_us=1000, pages=3)
    fields.update(changes)
    return b"riffdb-retention-source-bytes-v1\t" + b"\t".join(
        f"{key}={value}".encode() for key, value in fields.items()) + b"\n"


class SourceBytesEvidence(unittest.TestCase):
    def test_actual_canonical_bytes_are_preserved_without_total_history_claim(self):
        value = parse_sample(line())
        self.assertEqual(value["canonical_receipt_bytes"], 100000)
        self.assertEqual(value["key_bytes"], 4800)
        self.assertNotIn("eligible_history_bytes", value)

    def test_unknown_partial_stale_overflow_duplicate_and_bad_numbers_refuse(self):
        examples = [line()[:-1], line().replace(b"v1", b"v2", 1),
                    line().replace(b"root=2", b"root=2\troot=2"),
                    line().replace(b"pages=3", b"unknown=3")]
        for changes in (dict(root=0), dict(root=3), dict(incarnation=0),
                        dict(rows=256), dict(key_bytes=4799), dict(pages=2),
                        dict(canonical_receipt_bytes=0), dict(pages=601),
                        dict(capture_age_us=30000000), dict(collection_us=1001),
                        dict(root=True), dict(root=-1), dict(root="02"),
                        dict(root=2**64), dict(rows=2**63), dict(root=1.5)):
            examples.append(line(**changes))
        for encoded in examples:
            with self.subTest(encoded=encoded[:60]), self.assertRaises(SourceBytesError):
                parse_sample(encoded)
        self.assertIsNone(parse_sample(b"another diagnostic with private data\n"))

    def test_complete_empty_prefix_can_be_zero_but_not_a_nonempty_prefix(self):
        value = parse_sample(line(resume=603, rows=0, canonical_receipt_bytes=0,
                                  key_bytes=0, pages=1))
        self.assertEqual(value["canonical_receipt_bytes"], 0)
        with self.assertRaises(SourceBytesError):
            parse_sample(line(resume=603, rows=0, canonical_receipt_bytes=1,
                              key_bytes=0, pages=1))

    def test_fresh_coverage_is_bound_to_independently_observed_incarnation(self):
        with tempfile.TemporaryDirectory() as directory:
            with SourceBytesSamples(Path(directory), 600, lambda: 1) as samples:
                for seconds in range(0, 600, 60):
                    samples.observe(line(root=2+seconds), seconds*1000000000)
                result = samples.finish(600000000000)
            self.assertEqual(result["samples"], 10)
            self.assertFalse(result["qualifies_complete_resource_model"])
            records = [json.loads(value) for value in samples.path.read_text().splitlines()]
            self.assertEqual(records[0]["canonical_receipt_bytes"], 100000)
            self.assertNotIn("eligible_history_bytes", result)
        for incarnation in (None, 2, True):
            with tempfile.TemporaryDirectory() as directory:
                with SourceBytesSamples(Path(directory), 60, lambda: incarnation) as samples:
                    with self.assertRaisesRegex(SourceBytesError, "^source_bytes_incarnation$"):
                        samples.observe(line(), 0)

    def test_missing_gapped_or_rewound_observations_cannot_become_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            with SourceBytesSamples(Path(directory), 300, lambda: 1) as samples:
                with self.assertRaises(SourceBytesError):
                    samples.finish(300000000000)
                samples.observe(line(root=4), 1000000000)
                for encoded, elapsed in ((line(root=2), 2000000000),
                                         (line(root=6), 97000000000)):
                    with self.assertRaises(SourceBytesError):
                        samples.observe(encoded, elapsed)
                with self.assertRaises(SourceBytesError):
                    samples.finish(300000000000)

    def test_log_excludes_seed_lines_preserves_partial_and_refuses_replacement(self):
        from lib.ticketdesk_lifecycle_provider_budget import ProviderBudgetError
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "server.stderr"
            path.write_bytes(line(root=999))
            with SourceBytesLog(root, 60, lambda: 1) as log:
                with path.open("ab") as output:
                    output.write(line()[:-1])
                log.poll(1000000000)
                self.assertEqual(log.samples.count, 0)
                with path.open("ab") as output:
                    output.write(b"\n")
                log.poll(2000000000)
                self.assertEqual(log.samples.count, 1)
                self.assertEqual(log.finish(60000000000)["samples"], 1)
                path.rename(root / "old.stderr")
                path.write_bytes(line())
                with self.assertRaises(ProviderBudgetError):
                    log.poll(3000000000)


if __name__ == "__main__":
    unittest.main()
