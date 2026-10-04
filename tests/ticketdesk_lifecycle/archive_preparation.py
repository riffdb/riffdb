# req: END-003, END-007, END-008
"""Integration-only archive evidence checks against a supplied private database."""
import os
from pathlib import Path
import struct
import subprocess
import sys
import unittest


WORKER, ROOT = map(Path, sys.argv[1:])
sys.argv[1:] = []


class ActualArchiveEvidence(unittest.TestCase):
    def test_valid_archive_refuses_substituted_events_and_incarnation_witnesses(self):
        updates = ROOT / "updates.witness"
        original = updates.read_bytes()
        seed = (ROOT / "seed.witness").read_bytes()
        self.assertEqual(len(original), 48)
        self.assertEqual(len(seed), 48)
        environment = dict(os.environ, RIFFDB_LIFECYCLE_WITNESS_ROOT=str(ROOT))
        try:
            for offset, value, code in (
                (24, struct.unpack(">Q", seed[24:32])[0], "archive_reconciliation"),
                (40, 2, "archive_missing_event"),
            ):
                with self.subTest(field_offset=offset):
                    encoded = bytearray(original)
                    encoded[offset:offset + 8] = struct.pack(">Q", value)
                    updates.write_bytes(encoded)
                    result = subprocess.run([str(WORKER), "archive-reconcile"],
                        env=environment, capture_output=True, timeout=30, check=False)
                    self.assertEqual(result.returncode, 1)
                    self.assertEqual(result.stderr, ("ticketdesk-lifecycle-worker: " + code + "\n").encode())
                    self.assertEqual(result.stdout, b"")
        finally:
            updates.write_bytes(original)


if __name__ == "__main__":
    unittest.main()
