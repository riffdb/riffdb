# req: END-007, END-008, END-010
"""Real file observations prove accounting; fixture bytes are not a database."""
import os
from pathlib import Path
import tempfile
import unittest

from lib.ticketdesk_lifecycle_campaign_disk import CampaignDiskSamples, DiskError, FIELDS, SECOND, file_bytes
from lib.ticketdesk_lifecycle_faults import LocalArchiveOutage


class CampaignDiskTests(unittest.TestCase):
    def fixture(self, root):
        for name in ("archive", "data", "backups", "projections"):
            (root / name).mkdir(mode=0o700)
        for name in ("archive/archive.redb", "data/ticketdesk.redb", "data/ticketdesk.redb.riffjournal",
                     "backups/manifest.riffdb", "projections/view"):
            path = root / name
            path.write_bytes(b"fixture")
            path.chmod(0o600)
        return CampaignDiskSamples(root, 21900, dict.fromkeys(FIELDS, 16 * 1024**2))

    def test_counts_preserved_source_and_projection_copies_as_backup_without_claiming_reclamation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.fixture(root) as owner:
                first = owner.poll(0)
                original = first["resources"]
                (root / "data").rename(root / "retained-pre-restore-data")
                (root / "data").mkdir(mode=0o700)
                (root / "projections").rename(root / "retained-pre-restore-projections")
                (root / "projections").mkdir(mode=0o700)
                next_row = owner.poll(30 * SECOND)
                self.assertEqual(next_row["resources"]["backup_bytes"], original["backup_bytes"]
                    + original["database_bytes"] + original["journal_bytes"] + original["derived_bytes"])
                self.assertEqual(next_row["resources"]["database_bytes"], 0)
                self.assertEqual(next_row["resources"]["journal_bytes"], 0)
                self.assertFalse(next_row["complete_resource_model_proved"])

    def test_same_open_archive_file_is_counted_through_the_actual_permission_fault(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.fixture(root) as owner, LocalArchiveOutage(root / "archive") as outage:
                first = owner.poll(0)
                outage.pause()
                during = owner.poll(30 * SECOND, archive_outage=outage)
                self.assertEqual(during["resources"]["sink_archive_bytes"], first["resources"]["sink_archive_bytes"])
                outage.resume()
                after = owner.poll(60 * SECOND)
                self.assertEqual(after["resources"]["sink_archive_bytes"], first["resources"]["sink_archive_bytes"])

    def test_unexpected_archive_artifact_and_replaced_archive_identity_fail_closed(self):
        for replacement in (False, True):
            with self.subTest(replacement=replacement), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                with self.fixture(root) as owner:
                    owner.poll(0)
                    if replacement:
                        (root / "archive/archive.redb").unlink()
                        (root / "archive/archive.redb").write_bytes(b"other")
                    else:
                        (root / "archive/other").write_bytes(b"other")
                    with self.assertRaises(DiskError): owner.poll(30 * SECOND)
                    with self.assertRaisesRegex(DiskError, "campaign_disk_failed"): owner.poll(31 * SECOND)

    def test_symlinks_and_hard_links_cannot_hide_or_duplicate_owned_resource_files(self):
        for hard_link in (False, True):
            with self.subTest(hard_link=hard_link), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                with self.fixture(root) as owner:
                    source, target = root / "data/ticketdesk.redb", root / "data/other"
                    if hard_link: os.link(source, target)
                    else: target.symlink_to(source)
                    with self.assertRaisesRegex(DiskError, "campaign_disk_file"): owner.poll(0)

    def test_apparent_sparse_length_is_retained_as_a_conservative_file_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sparse"
            with path.open("wb") as output: output.truncate(1024**2)
            info = path.stat()
            self.assertEqual(file_bytes(info), max(info.st_size, info.st_blocks * 512))
            self.assertGreaterEqual(file_bytes(info), 1024**2)

    def test_global_gap_limit_cannot_reset_on_a_new_server_or_history(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.fixture(Path(directory)) as owner:
                owner.poll(0)
                with self.assertRaisesRegex(DiskError, "campaign_disk_gap"): owner.poll(76 * SECOND)
                with self.assertRaisesRegex(DiskError, "campaign_disk_failed"): owner.poll(77 * SECOND)

    def test_terminal_record_requires_every_window_and_preserves_window_maxima(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.fixture(Path(directory)) as owner:
                for elapsed in range(0, 21900, 30): owner.poll(elapsed * SECOND)
                row = owner.poll(21900 * SECOND, terminal=True)
                self.assertTrue(all(owner.coverage))
                self.assertEqual(len(owner.windows), 73)
                self.assertGreater(owner.windows[0]["database_bytes"], 0)
                self.assertFalse(row["complete_resource_model_proved"])
                with self.assertRaises(DiskError): owner.poll(21901 * SECOND)
