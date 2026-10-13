# req: END-001, END-007, END-008
"""Actual controller file witnesses; no native or timing qualification."""
import hashlib
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from lib.ticketdesk_lifecycle_campaign_restore_inputs import (
    AcknowledgedPrefix, SelectedBackup, RestoreInputError,
)
from lib.ticketdesk_lifecycle_campaign_restore_clock import FIXED


def record(ordinal, sequence, version=1, incarnation=1):
    return b''.join(n.to_bytes(8, 'big') for n in (ordinal, sequence, version, incarnation))


class RestoreInputTests(unittest.TestCase):
    def witness(self, root, *, rows=None):
        rows = rows if rows is not None else record(0, 11) + record(1, 10, 2)
        prefix = b'RDBTW001' + (30000).to_bytes(8, 'big') + rows
        path = root / 'updates.witness'
        with path.open('wb') as output:
            output.write(prefix)
            output.truncate(16 + 32 * 30000)
        return prefix

    def capture(self, root, **changes):
        options = dict(commands=2, duration_s=300, frontier=11, version=2,
                       incarnation=1, checkpoint=lambda: None)
        options.update(changes)
        return AcknowledgedPrefix(root, **options)

    def backup(self, root):
        path = root / 'backups' / 'campaign-source'
        path.mkdir(parents=True)
        for name in FIXED | {'journal.sealed.00000000000000000001.riffextent'}:
            (path / name).write_bytes(b'controller fixture only\n')
        return path

    def test_prefix_covers_every_acknowledgement_without_hashing_future_preallocated_slots(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            prefix = self.witness(root)
            owner = self.capture(root)
            try:
                self.assertEqual(owner.prefix_bytes, 80)
                self.assertEqual(owner.sha256, hashlib.sha256(prefix).hexdigest())
                self.assertEqual(owner.maximum_sequence, 11)
                self.assertEqual(owner.verify_prefix(2, lambda: None), owner.sha256)
            finally:
                owner.close()
            with self.assertRaises(RestoreInputError):
                owner.verify_prefix(2, lambda: None)

    def test_all_rows_are_checked_with_parallel_commit_order_and_exact_contract_bounds(self):
        for rows in (record(1, 11) + record(1, 10), record(0, 12) + record(1, 10),
                     record(0, 11) + record(1, 0), record(0, 11) + record(1, 10, 3),
                     record(0, 11) + record(1, 10, 1, 2)):
            with self.subTest(rows=rows.hex()), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                self.witness(root, rows=rows)
                with self.assertRaisesRegex(RestoreInputError, 'restore_input_witness_record'):
                    self.capture(root)

    def test_new_acknowledgement_mutation_replacement_hardlink_and_truncation_refuse(self):
        for mode in ('ack', 'write', 'replace', 'link', 'truncate', 'directory'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary) / 'case'
                root.mkdir()
                self.witness(root)
                owner = self.capture(root)
                try:
                    path = root / 'updates.witness'
                    if mode == 'write':
                        old = path.stat()
                        with path.open('r+b') as out:
                            out.seek(16)
                            out.write(record(0, 10))
                        os.utime(path, ns=(old.st_atime_ns, old.st_mtime_ns))
                    elif mode == 'replace':
                        path.rename(root / 'old.witness')
                        self.witness(root)
                    elif mode == 'link':
                        os.link(path, root / 'linked.witness')
                    elif mode == 'truncate':
                        with path.open('r+b') as out:
                            out.truncate(48)
                    elif mode == 'directory':
                        root.rename(root.with_name('old-case'))
                        root.mkdir()
                        self.witness(root)
                    with self.assertRaises(RestoreInputError):
                        owner.verify_prefix(3 if mode == 'ack' else 2, lambda: None)
                    with self.assertRaises(RestoreInputError):
                        owner.verify_prefix(2, lambda: None)
                finally:
                    owner.close()

    def test_preallocation_header_symlink_and_acknowledgement_count_bounds_refuse(self):
        for mode in ('header', 'size', 'symlink', 'count', 'boolean'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                self.witness(root)
                path = root / 'updates.witness'
                if mode == 'header':
                    with path.open('r+b') as out:
                        out.write(b'bad-hdr!')
                elif mode == 'size':
                    with path.open('r+b') as out:
                        out.truncate(80)
                elif mode == 'symlink':
                    path.rename(root / 'other')
                    path.symlink_to('other')
                with self.assertRaises((RestoreInputError, OSError)):
                    self.capture(root, commands=30001 if mode == 'count' else
                                 True if mode == 'boolean' else 2)

    def test_selected_backup_binds_original_bytes_inventory_and_descriptors_until_loss(self):
        for mode in ('unchanged', 'extra', 'missing', 'replace', 'hardlink', 'mutate', 'parent'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                path = self.backup(root)
                owner = SelectedBackup(root, 'campaign-source', checkpoint=lambda: None)
                try:
                    if mode == 'unchanged':
                        self.assertEqual(owner.verify_selection(),
                            hashlib.sha256((path / 'manifest.riffdb').read_bytes()).hexdigest())
                        self.assertEqual(len(owner.artifacts), 5)
                        continue
                    selected = path / 'database.redb'
                    if mode == 'extra':
                        (path / 'unknown').write_bytes(b'fixture')
                    elif mode == 'missing':
                        selected.unlink()
                    elif mode == 'replace':
                        selected.unlink()
                        selected.write_bytes(b'controller fixture only\n')
                    elif mode == 'hardlink':
                        os.link(selected, root / 'link')
                    elif mode == 'mutate':
                        old = selected.stat()
                        selected.write_bytes(b'x' * old.st_size)
                        os.utime(selected, ns=(old.st_atime_ns, old.st_mtime_ns))
                    elif mode == 'parent':
                        path.parent.rename(root / 'old-backups')
                        self.backup(root)
                    with self.assertRaises((RestoreInputError, OSError)):
                        owner.verify_selection()
                    with self.assertRaises(RestoreInputError):
                        owner.verify_selection()
                finally:
                    owner.close()

    def test_cancellation_releases_pinned_files_without_admitting_partial_witness(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.witness(root)
            def cancelled():
                raise InterruptedError('fixture cancellation')
            before = len(list(Path('/proc/self/fd').iterdir()))
            with self.assertRaises(InterruptedError):
                self.capture(root, checkpoint=cancelled)
            self.assertEqual(before, len(list(Path('/proc/self/fd').iterdir())))


if __name__ == '__main__':
    unittest.main()
