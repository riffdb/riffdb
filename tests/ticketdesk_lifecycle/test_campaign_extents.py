# req: END-003, END-007, END-008, END-010
"""Filesystem and native-record fixtures prove attribution, never DB reclamation."""
import os
from pathlib import Path
import tempfile
import unittest

from lib.ticketdesk_lifecycle_campaign_extents import MeasuredExtents, MeasuredExtentError

SECOND = 10**9
DURATION = 28800


def sealed(root, generation):
    path = root / ("ticketdesk.redb.riffjournal.sealed." + str(generation).zfill(20))
    path.write_bytes(b"immutable fixture")
    return path


def record(generation, root=2):
    return (f"riffdb-retention-extent-retired-v1\troot={root}\tgeneration={generation}"
            "\tdeleted=1\trecycled=0\n").encode()


class CampaignExtentTests(unittest.TestCase):
    def test_only_measured_observed_native_retirements_in_this_incarnation_count(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [sealed(root, generation) for generation in (10, 11, 12, 13)]
            tracker = MeasuredExtents(root, duration_s=DURATION, incarnation=2)
            tracker.poll(0)
            for index, path in enumerate(paths[:3], 1):
                path.unlink(); tracker.poll(index * SECOND)
            # File 13 disappears only after the offered period has ended.
            # Advance the original clock with complete bounded coverage.
            for elapsed in range(60, DURATION, 60): tracker.poll(elapsed * SECOND)
            paths[3].unlink(); tracker.close(DURATION * SECOND)
            log = root / "closed.stderr"
            # Generation 1 is a startup retirement that was never observed.
            log.write_bytes(b"ordinary bounded diagnostic\n" + record(1) +
                            b"".join(record(generation, 4) for generation in (10, 11, 12, 13)))
            result = tracker.reconcile([(log, False)])
            self.assertEqual(result["history_incarnation"], 2)
            self.assertEqual(result["observed_native_retired_generations"], [10, 11, 12])
            self.assertEqual(result["measured_removal_elapsed_ns"], [SECOND, 2*SECOND, 3*SECOND])
            self.assertEqual(result["unconfirmed_disappearances"], 0)
            self.assertFalse(result["qualifies_complete_resource_model"])

    def test_unobserved_native_record_cannot_create_a_measured_retirement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); tracker = MeasuredExtents(root, duration_s=DURATION, incarnation=1)
            tracker.close(0)
            log = root / "closed.stderr"; log.write_bytes(record(1))
            self.assertEqual(tracker.reconcile([(log, False)])["observed_native_retired_generations"], [])

    def test_unexplained_disappearance_fails_sticky(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); path = sealed(root, 7)
            tracker = MeasuredExtents(root, duration_s=DURATION, incarnation=1); tracker.poll(0)
            path.unlink(); tracker.close(SECOND)
            log = root / "closed.stderr"; log.write_bytes(b"ordinary\n")
            with self.assertRaisesRegex(MeasuredExtentError, "^campaign_extent_unexplained_disappearance$"):
                tracker.reconcile([(log, False)])
            self.assertTrue(tracker.failed)
            log.write_bytes(record(7))
            with self.assertRaisesRegex(MeasuredExtentError, "^campaign_extent_logs$"):
                tracker.reconcile([(log, False)])

    def test_process_local_root_reset_preserves_persisted_generation_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); paths=[sealed(root,generation) for generation in (7,8)]
            tracker=MeasuredExtents(root,duration_s=DURATION,incarnation=1);tracker.poll(0)
            paths[0].unlink();tracker.poll(SECOND)
            paths[1].unlink();tracker.close(2*SECOND)
            first,second=root/"first.stderr",root/"second.stderr"
            first.write_bytes(record(7,100));second.write_bytes(record(8,2))
            result=tracker.reconcile([(first,False),(second,False)])
            self.assertEqual(result["observed_native_retired_generations"],[7,8])

    def test_late_native_retirement_of_final_present_file_supplies_no_measured_credit(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);sealed(root,7)
            tracker=MeasuredExtents(root,duration_s=DURATION,incarnation=2);tracker.close(0)
            log=root/"closed.stderr";log.write_bytes(record(7))
            self.assertEqual(tracker.reconcile([(log,False)])["observed_native_retired_generations"],[])

    def test_only_final_crash_truncation_is_unknown_and_never_a_record(self):
        for killed, encoded, accepted in ((True, record(1) + b"riffdb-retention-extent-retired-v1\troot=", True),
                (False, record(1) + b"truncated", False),
                (True, b"riffdb-retention-extent-retired-v9\troot=2\n", False)):
            with self.subTest(killed=killed, accepted=accepted), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); tracker = MeasuredExtents(root, duration_s=DURATION, incarnation=1)
                tracker.close(0); log = root / "closed.stderr"; log.write_bytes(encoded)
                if accepted:
                    self.assertEqual(tracker.reconcile([(log, killed)])["observed_native_retired_generations"], [])
                else:
                    with self.assertRaises(MeasuredExtentError): tracker.reconcile([(log, killed)])

    def test_duplicate_generation_root_rewind_hardlinks_and_oversized_lines_refuse(self):
        for kind in ("duplicate", "rewind", "hardlink", "oversized", "repeat_log", "symlink"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); tracker = MeasuredExtents(root, duration_s=DURATION, incarnation=1)
                tracker.close(0); log = root / "closed.stderr"
                encoded = {"duplicate":record(1)+record(1), "rewind":record(1, 4)+record(2, 2),
                           "oversized":b"x"*4097+b"\n"}.get(kind, record(1))
                log.write_bytes(encoded)
                if kind == "hardlink": os.link(log, root / "alias")
                if kind == "symlink":
                    alias = root / "alias"; alias.symlink_to(log); log = alias
                logs = [(log, False)] * (2 if kind == "repeat_log" else 1)
                with self.assertRaises(MeasuredExtentError): tracker.reconcile(logs)
                self.assertTrue(tracker.failed)

    def test_clock_gap_and_reused_generation_are_sticky_failures(self):
        for kind in ("gap", "reappeared"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory); path = sealed(root, 1)
                tracker = MeasuredExtents(root, duration_s=DURATION, incarnation=1); tracker.poll(0)
                if kind == "gap":
                    with self.assertRaisesRegex(MeasuredExtentError, "^campaign_extent_gap$"):
                        tracker.poll(76*SECOND)
                else:
                    path.unlink(); tracker.poll(SECOND); sealed(root, 1)
                    with self.assertRaises(ValueError): tracker.poll(2*SECOND)
                self.assertTrue(tracker.failed)
                with self.assertRaisesRegex(MeasuredExtentError, "^campaign_extent_closed$"): tracker.poll(3*SECOND)
