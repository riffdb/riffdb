# req: END-007, END-008, END-010
"""V2 never reports qualified reuse as scans performed in the current capture."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from lib.ticketdesk_lifecycle_source_bytes import (
    SourceBytesError, SourceBytesSamples, parse_sample, parse_sample_v2)
from test_source_bytes import line as v1


def line(**changes):
    fields = dict(root=2, incarnation=1, resume=3, ceiling=603, rows=600,
                  canonical_receipt_bytes=100000, key_bytes=4800,
                  capture_age_us=1000, collection_us=1000, pages=1,
                  reused_rows=550, checked_rows=100, subtracted_rows=50,
                  publication_kind=0, publication_hash_a=0, publication_hash_b=0,
                  publication_hash_c=0, publication_hash_d=0)
    fields.update(changes)
    return b"riffdb-retention-source-bytes-v2\t" + b"\t".join(
        f"{key}={value}".encode() for key, value in fields.items()) + b"\n"


class ContinuedSourceBytes(unittest.TestCase):
    def test_reuse_and_checked_cut_subtraction_are_separate_from_actual_pages(self):
        sample = parse_sample_v2(line())
        self.assertEqual(sample["rows"], 600)
        self.assertEqual(sample["checked_rows"], 100)
        self.assertEqual(sample["pages"], 1)
        sample = parse_sample_v2(line(reused_rows=600, checked_rows=0,
                                      subtracted_rows=0, pages=0))
        self.assertEqual(sample["canonical_receipt_bytes"], 100000)
        self.assertEqual(sample["pages"], 0)
        # The original V1 reader and its field/page meaning remain frozen.
        with self.assertRaises(SourceBytesError):
            parse_sample(line())
        self.assertEqual(parse_sample(v1())["pages"], 3)

    def test_incomplete_or_inconsistent_reuse_cannot_become_known_bytes(self):
        for changes in (dict(reused_rows=549), dict(subtracted_rows=51),
                        dict(checked_rows=0), dict(pages=0), dict(pages=101),
                        dict(reused_rows=2**64-1), dict(checked_rows=2**64),
                        dict(capture_age_us=30000000), dict(collection_us=1001),
                        dict(resume=604), dict(canonical_receipt_bytes=0),
                        dict(key_bytes=4799), dict(root=3), dict(publication_kind=2),
                        dict(publication_hash_a=1)):
            with self.subTest(changes=changes), self.assertRaises(SourceBytesError):
                parse_sample_v2(line(**changes))
        for malformed in (line()[:-1], line().replace(b"v2", b"v3", 1),
                          line()+b"extra", line().replace(b"root=2", b"root=2\troot=2")):
            with self.subTest(encoded=malformed[:60]), self.assertRaises(SourceBytesError):
                parse_sample_v2(malformed)

    def test_frozen_v2_basis_and_version_are_retained_in_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            with SourceBytesSamples(Path(directory), 60, lambda: 1) as samples:
                samples.observe(line(), 0)
                with self.assertRaisesRegex(SourceBytesError, "^source_bytes_version$"):
                    samples.observe(v1(root=4), 1000000000)
                summary = samples.finish(60000000000)
            self.assertEqual(summary["schema"], "riffdb.ticketdesk-lifecycle-source-byte-observation/v2")
            self.assertEqual(summary["basis"], "qualified_canonical_source_receipts_and_keys")
            record = json.loads(samples.path.read_text())
            self.assertEqual(record["checked_rows"], 100)
            self.assertFalse(summary["qualifies_complete_resource_model"])


if __name__ == "__main__":
    unittest.main()
