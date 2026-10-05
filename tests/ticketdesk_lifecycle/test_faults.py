# req: END-003, END-007, END-008
"""Fault preparation must retain the real archive, including during cleanup."""
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from lib.ticketdesk_lifecycle_faults import FaultError, LocalArchiveOutage


class ArchiveCustodyFault(unittest.TestCase):
    def archive(self, root):
        directory = root / "archive"
        directory.mkdir(mode=0o700)
        file = directory / "archive.redb"
        file.write_bytes(b"observer fixture, never database evidence")
        file.chmod(0o600)
        return directory, file

    def test_outage_changes_access_and_restores_the_same_objects_and_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory, file = self.archive(Path(temporary))
            identities = (directory.stat().st_ino, file.stat().st_ino)
            with LocalArchiveOutage(directory) as outage:
                outage.pause()
                self.assertEqual(directory.stat().st_mode & 0o777, 0)
                with self.assertRaises(FaultError):
                    outage.pause()
                outage.resume()
                with self.assertRaises(FaultError):
                    outage.resume()
            self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
            self.assertEqual((directory.stat().st_ino, file.stat().st_ino), identities)
            self.assertEqual(file.read_bytes(), b"observer fixture, never database evidence")

    def test_exception_restores_the_pinned_directory_without_changing_a_replacement(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory, file = self.archive(root)
            retained = root / "retained"
            with self.assertRaisesRegex(RuntimeError, "fixture"):
                with LocalArchiveOutage(directory) as outage:
                    outage.pause()
                    directory.rename(retained)
                    directory.mkdir(mode=0o500)
                    raise RuntimeError("fixture")
            self.assertEqual(retained.stat().st_mode & 0o777, 0o700)
            self.assertEqual(directory.stat().st_mode & 0o777, 0o500)
            self.assertEqual((retained / file.name).read_bytes(),
                             b"observer fixture, never database evidence")

    def test_substitution_cannot_be_called_recovery(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory, _ = self.archive(root)
            with LocalArchiveOutage(directory) as outage:
                outage.pause()
                directory.rename(root / "retained")
                directory.mkdir(mode=0o700)
                with self.assertRaises(FaultError):
                    outage.resume()

    def test_links_and_nonprivate_permissions_are_refused_before_any_change(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory, file = self.archive(root)
            directory.chmod(0o750)
            with self.assertRaises(FaultError):
                LocalArchiveOutage(directory)
            self.assertEqual(directory.stat().st_mode & 0o777, 0o750)
            directory.chmod(0o700)
            link = root / "linked"
            link.symlink_to(directory, target_is_directory=True)
            with self.assertRaises(FaultError):
                LocalArchiveOutage(link)
            extra = root / "extra"
            os.link(file, extra)
            with self.assertRaises(FaultError):
                LocalArchiveOutage(directory)
            extra.unlink()
            file.unlink()
            file.symlink_to(extra)
            with self.assertRaises(FaultError):
                LocalArchiveOutage(directory)


if __name__ == "__main__":
    unittest.main()
