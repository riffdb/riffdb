# req: END-007, END-008, END-010
"""A stopped native producer must refuse before the end of a long invocation."""
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from lib import ticketdesk_lifecycle_source_bytes as source
from lib import ticketdesk_lifecycle_logical_bytes as logical
from lib import ticketdesk_lifecycle_physical_bytes as physical
from test_source_bytes import line as source_line
from test_logical_bytes import line as logical_line
from test_physical_bytes import line as physical_line

CASES = (
    (source.SourceBytesLog, source.SourceBytesError, source_line, 'source_bytes'),
    (logical.LogicalBytesLog, logical.LogicalBytesError, logical_line, 'logical_bytes'),
    (physical.PhysicalBytesLog, physical.PhysicalBytesError, physical_line, 'physical_bytes'),
)
SECOND = 1000000000


class NativeByteSilence(unittest.TestCase):
    def test_no_initial_native_sample_refuses_at_the_existing_gap_boundary(self):
        for kind, error, _line, code in CASES:
            with self.subTest(code=code), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / 'server.stderr').write_bytes(b'')
                with kind(root, 1500, lambda: 1) as log:
                    log.poll(95 * SECOND)
                    with self.assertRaisesRegex(error, '^' + code + '_sample_gap$'):
                        log.poll(95 * SECOND + 1)
                    self.assertEqual(log.samples.count, 0)

    def test_silent_producer_after_one_sample_refuses_without_waiting_for_a_new_line(self):
        for kind, error, line, code in CASES:
            with self.subTest(code=code), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                path = root / 'server.stderr'
                path.write_bytes(b'')
                with kind(root, 1500, lambda: 1) as log:
                    with path.open('ab') as output:
                        output.write(line(incarnation=1))
                    log.poll(3 * SECOND)
                    log.poll(98 * SECOND)
                    with self.assertRaisesRegex(error, '^' + code + '_sample_gap$'):
                        log.poll(98 * SECOND + 1)
                    self.assertEqual(log.samples.count, 1)
                    self.assertEqual(len(log.samples.path.read_bytes().splitlines()), 1)

    def test_new_actual_samples_extend_only_the_observed_gap_clock(self):
        for kind, _error, line, code in CASES:
            with self.subTest(code=code), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                path = root / 'server.stderr'
                path.write_bytes(b'')
                with kind(root, 600, lambda: 1) as log:
                    for elapsed in range(0, 600, 60):
                        with path.open('ab') as output:
                            output.write(line(root=2 + elapsed, incarnation=1))
                        log.poll(elapsed * SECOND)
                    log.poll(600 * SECOND)
                    value = log.finish(600 * SECOND)
                    self.assertEqual(value['samples'], 10)
                    self.assertFalse(value['qualifies_complete_resource_model'])


if __name__ == '__main__':
    unittest.main()
