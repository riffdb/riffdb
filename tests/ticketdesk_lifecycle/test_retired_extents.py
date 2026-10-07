# req: END-007, END-008, END-010
"""An aggregate count cannot identify a durably retired physical generation."""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from lib import ticketdesk_lifecycle_inventory as inventory


def record(generation=7, root=8, deleted=1, recycled=0):
    return (f'riffdb-retention-extent-retired-v1\troot={root}\tgeneration={generation}'
            f'\tdeleted={deleted}\trecycled={recycled}\n').encode()


class RetirementWitnesses(unittest.TestCase):
    def test_closed_record_identifies_exactly_one_completed_retirement(self):
        self.assertEqual(inventory.parse_retirement(record()),
                         dict(root=8, generation=7, deleted=1, recycled=0))
        self.assertEqual(inventory.parse_retirement(record(deleted=0, recycled=1))['recycled'], 1)
        self.assertIsNone(inventory.parse_retirement(b'unrelated diagnostic\n'))
        for value in (record(root=7), record(generation=0), record(deleted=0),
                      record(recycled=1), record(generation=2**64), record().replace(b'7', b'07'),
                      record()[:-1], record()+b'extra\n', record().replace(b'root=8',b'root=8\troot=8'),
                      record().replace(b'-v1',b'-v2')):
            with self.subTest(value=value), self.assertRaises(inventory.InventoryError):
                inventory.parse_retirement(value)

    def test_native_retirements_match_counts_and_sampled_disappearances(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            monitor=inventory.InventoryMonitor(root)
            file=root/'ticketdesk.redb.riffjournal.sealed.00000000000000000007'
            file.write_bytes(b'isolated inventory fixture')
            monitor.inventory.sample();file.unlink();monitor.inventory.sample()
            census=(b'riffdb-retention-extents-v1\tdeleted=2\trecycled=1\t'
                    b'held_references=0\theld_readers=0\treclaim_us=3\n')
            terminal=b'riffdb-shutdown-release-v1\tonline_retention=cancelled\n'
            (root/'server.stderr').write_bytes(record(7)+record(9,10)+record(8,12,0,1)+census+terminal)
            observed=monitor.finish(root)
            self.assertEqual(observed['native_retired_generations'],[7,8,9])
            self.assertEqual(observed['physically_removed_generations'],[7])
            self.assertFalse(observed['qualifies_complete_resource_model'])

    def test_duplicate_missing_unmatched_or_rewound_witnesses_refuse(self):
        for encoded in (record()+record(), record(7,10)+record(8,8), b'', record(deleted=0,recycled=1)):
            with self.subTest(encoded=encoded),tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                census=(b'riffdb-retention-extents-v1\tdeleted=1\trecycled=0\t'
                        b'held_references=0\theld_readers=0\treclaim_us=3\n')
                (root/'server.stderr').write_bytes(encoded+census+b'riffdb-shutdown-release-v1\tonline_retention=cancelled\n')
                with self.assertRaises(inventory.InventoryError):inventory.InventoryMonitor(root).finish(root)

    def test_a_live_file_or_disappearance_of_another_generation_refuses(self):
        for still_live in (True, False):
            with self.subTest(still_live=still_live),tempfile.TemporaryDirectory() as directory:
                root=Path(directory)
                monitor=inventory.InventoryMonitor(root)
                file=root/'ticketdesk.redb.riffjournal.sealed.00000000000000000007'
                file.write_bytes(b'isolated inventory fixture')
                monitor.inventory.sample()
                if not still_live:file.unlink();monitor.inventory.sample()
                census=(b'riffdb-retention-extents-v1\tdeleted=1\trecycled=0\t'
                        b'held_references=0\theld_readers=0\treclaim_us=3\n')
                (root/'server.stderr').write_bytes(record(7 if still_live else 8)+census+
                    b'riffdb-shutdown-release-v1\tonline_retention=cancelled\n')
                with self.assertRaises(inventory.InventoryError):monitor.finish(root)


if __name__=='__main__':unittest.main()
