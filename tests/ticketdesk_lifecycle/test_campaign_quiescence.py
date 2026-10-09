# req: END-007, END-008, END-009, END-010
"""Real worker control files; these tests do not qualify a campaign."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from lib.ticketdesk_lifecycle_campaign_quiescence import QuiescentRelease, QuiescenceError

SECOND = 1000000000


class WorkerHandover(unittest.TestCase):
    def acknowledgements(self, root, *, generation=1, version=1, incarnation=1):
        (root / "load.quiesced").write_text(json.dumps({
            "schema": "riffdb.ticketdesk-lifecycle-quiesced/v1",
            "generation": generation, "completed_commands": 71, "completed_reads": 281,
        }) + "\n")
        (root / "observer.quiesced").write_text(json.dumps({
            "schema": "riffdb.ticketdesk-lifecycle-observer-quiesced/v1",
            "generation": generation, "contract_version": version,
            "history_incarnation": incarnation, "events_checked": 19,
            "duplicates": 0, "consumer_checkpoint": 37, "live_head": 41,
        }) + "\n")

    def release(self, root):
        return QuiescentRelease(root, generation=1, version=1, incarnation=1)

    def resumed_acknowledgement(self, root, *, commands=72, version=1, incarnation=1):
        (root / "load.resumed").write_text(json.dumps({
            "schema": "riffdb.ticketdesk-lifecycle-resumed/v1", "generation": 2,
            "contract_version": version, "history_incarnation": incarnation,
            "completed_commands": commands, "completed_reads": 282,
        }) + "\n")

    def test_both_original_workers_must_acknowledge_before_publishing_or_resuming(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            control = self.release(root)
            control.begin(10 * SECOND)
            self.assertTrue((root / "load.quiesce").exists())
            self.assertTrue((root / "observer.quiesce").exists())
            self.assertFalse(control.poll(11 * SECOND))
            self.acknowledgements(root)
            (root / "observer.quiesced").unlink()
            self.assertFalse(control.poll(12 * SECOND))
            self.acknowledgements(root)
            self.assertTrue(control.poll(13 * SECOND))
            self.assertFalse((root / "load.phase").exists())

    def test_published_release_uses_the_exact_shared_worker_protocol_before_unpausing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "next.credential").write_bytes(b"private fixture\n")
            control = self.release(root)
            control.begin(0)
            self.acknowledgements(root)
            control.publish(SECOND, credential="next.credential", version=2, incarnation=1)
            self.assertEqual((root / "load.phase").read_bytes(), b"2 2 1 next.credential\n")
            self.assertTrue((root / "load.quiesce").exists())
            self.assertTrue((root / "observer.quiesce").exists())
            self.assertEqual(control.snapshot["load"]["completed_commands"], 71)
            control.snapshot["load"]["completed_commands"] = 0
            self.assertEqual(control.snapshot["load"]["completed_commands"], 71)
            control.resume(2 * SECOND)
            self.assertFalse((root / "load.quiesce").exists())
            self.assertFalse((root / "observer.quiesce").exists())
            self.assertTrue((root / "load.quiesced").exists())
            self.assertTrue((root / "observer.quiesced").exists())
            self.resumed_acknowledgement(root, version=2)
            control.workers_resumed(3 * SECOND, generation=2, incarnation=1,
                                    duplicates=0)
            self.assertFalse((root / "load.quiesced").exists())
            self.assertFalse((root / "observer.quiesced").exists())

    def test_stale_acknowledgement_or_changed_incarnation_fences_before_phase_publication(self):
        for change in ({"generation": 2}, {"version": 2}, {"incarnation": 2}):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                control = self.release(root)
                control.begin(0)
                self.acknowledgements(root, **change)
                with self.assertRaisesRegex(QuiescenceError, "^quiescence_identity$"):
                    control.poll(SECOND)
                with self.assertRaisesRegex(QuiescenceError, "^quiescence_failed$"):
                    control.poll(2 * SECOND)
                self.assertFalse((root / "load.phase").exists())
                self.assertTrue((root / "load.quiesce").exists())

    def test_missing_acknowledgement_duplicate_events_and_unbounded_records_refuse(self):
        for change in ("duplicates", "large", "duplicate_key", "recursion"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                control = self.release(root)
                control.begin(0)
                self.acknowledgements(root)
                path = root / "observer.quiesced"
                if change == "duplicates":
                    value = json.loads(path.read_text())
                    value["duplicates"] = 1
                    path.write_text(json.dumps(value))
                elif change == "large":
                    path.write_bytes(b" " * 4097)
                elif change == "duplicate_key":
                    path.write_text('{"generation":1,"generation":1}')
                else:
                    path.write_text("[" * 1100 + "0" + "]" * 1100)
                with self.assertRaises(QuiescenceError):
                    control.poll(SECOND)
                self.assertTrue((root / "observer.quiesce").exists())

    def test_restore_marker_requires_one_exact_incarnation_advance(self):
        for incarnation, restore in ((1, True), (2, False), (3, True)):
            with self.subTest(incarnation=incarnation, restore=restore), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "next.credential").write_bytes(b"private fixture\n")
                control = self.release(root)
                control.begin(0)
                self.acknowledgements(root)
                with self.assertRaisesRegex(QuiescenceError, "^quiescence_release$"):
                    control.publish(SECOND, credential="next.credential", version=1,
                                    incarnation=incarnation, restore=restore)
                self.assertFalse((root / "load.phase").exists())
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "next.credential").write_bytes(b"private fixture\n")
            control = self.release(root)
            control.begin(0)
            self.acknowledgements(root)
            control.publish(SECOND, credential="next.credential", version=1,
                            incarnation=2, restore=True)
            self.assertTrue((root / "observer.restore").exists())
            self.assertEqual((root / "load.phase").read_bytes(), b"2 1 2 next.credential\n")
            control.resume(2 * SECOND)
            self.assertTrue((root / "observer.restore").exists())
            self.resumed_acknowledgement(root, incarnation=2)
            control.workers_resumed(3 * SECOND, generation=2, incarnation=2,
                                    duplicates=0)
            self.assertFalse((root / "observer.restore").exists())

    def test_acknowledgements_stay_until_both_new_generation_workers_make_progress(self):
        for change in ({"generation": 1}, {"completed_commands": 71}, {"duplicates": 1}):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "next.credential").write_bytes(b"private fixture\n")
                control = self.release(root)
                control.begin(0)
                self.acknowledgements(root)
                control.publish(SECOND, credential="next.credential", version=1, incarnation=1)
                control.resume(2 * SECOND)
                self.resumed_acknowledgement(root, commands=change.get("completed_commands", 72))
                evidence = dict(generation=2, incarnation=1, duplicates=0)
                evidence.update({key: value for key, value in change.items()
                                 if key != "completed_commands"})
                with self.assertRaises(QuiescenceError):
                    control.workers_resumed(3 * SECOND, **evidence)
                self.assertTrue((root / "load.quiesced").exists())
                self.assertTrue((root / "observer.quiesced").exists())

    def test_clock_rewind_timeout_and_premature_resume_keep_workers_paused(self):
        for mode in ("rewind", "timeout", "resume"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                control = self.release(root)
                control.begin(10 * SECOND)
                with self.assertRaises(QuiescenceError):
                    if mode == "rewind":
                        control.poll(10 * SECOND - 1)
                    elif mode == "timeout":
                        control.poll(70 * SECOND + 1)
                    else:
                        control.resume(11 * SECOND)
                self.assertTrue((root / "load.quiesce").exists())
                self.assertTrue((root / "observer.quiesce").exists())

    def test_restart_can_stop_observation_while_offered_load_exercises_actual_interruption(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "next.credential").write_bytes(b"private fixture\n")
            control = self.release(root)
            control.begin(0, pause_load=False)
            self.assertFalse((root / "load.quiesce").exists())
            self.assertTrue((root / "observer.quiesce").exists())
            self.acknowledgements(root)
            (root / "load.quiesced").unlink()
            self.assertFalse(control.poll(SECOND))
            self.assertEqual(control.observer_snapshot["generation"], 1)
            # The caller interrupts/restarts the actual server while load keeps
            # submitting. Only afterward may it drain load for phase handover.
            control.pause_load(2 * SECOND)
            self.assertTrue((root / "load.quiesce").exists())
            self.acknowledgements(root)
            control.publish(3 * SECOND, credential="next.credential", version=1, incarnation=1)
            control.resume(4 * SECOND)
            self.resumed_acknowledgement(root)
            control.workers_resumed(5 * SECOND, generation=2, incarnation=1,
                                    duplicates=0)
            self.assertFalse((root / "observer.quiesced").exists())

    def test_missing_native_resume_acknowledgement_cannot_confirm_the_first_write(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "next.credential").write_bytes(b"private fixture\n")
            control = self.release(root)
            control.begin(0)
            self.acknowledgements(root)
            control.publish(SECOND, credential="next.credential", version=1, incarnation=1)
            control.resume(2 * SECOND)
            with self.assertRaises(QuiescenceError):
                control.workers_resumed(3 * SECOND, generation=2, incarnation=1, duplicates=0)
            self.assertTrue((root / "load.quiesced").exists())

    def test_partial_pause_cannot_publish_a_phase_from_an_unrequested_load_acknowledgement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "next.credential").write_bytes(b"private fixture\n")
            control = self.release(root)
            control.begin(0, pause_load=False)
            self.acknowledgements(root)
            with self.assertRaises(QuiescenceError):
                control.publish(SECOND, credential="next.credential", version=1, incarnation=1)
            self.assertFalse((root / "load.phase").exists())
            self.assertTrue((root / "observer.quiesce").exists())


if __name__ == "__main__":
    unittest.main()
