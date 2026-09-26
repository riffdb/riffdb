# req: PERF-018
"""A failed diagnostic must retain its output and remove only its RAM spool."""
from pathlib import Path
from types import SimpleNamespace
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

import build_run


class Retention(unittest.TestCase):
    def exercise(self, exit_code, cell_deadline_seconds=900):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cell = root / 'cell'
            ram = root / 'private-ram-spool'
            cell.mkdir()
            ram.mkdir()
            command = [sys.executable, '-c', f'print("retained diagnostic", flush=True); raise SystemExit({exit_code})']
            with patch.object(build_run, 'SOURCE', root), \
                 patch.object(build_run, 'remaining', return_value=1800), \
                 patch.object(build_run.subprocess, 'check_output', return_value='tmpfs\n'), \
                 patch.object(build_run.os, 'statvfs', return_value=SimpleNamespace(f_bavail=3*2**30, f_frsize=1)), \
                 patch.object(build_run.tempfile, 'mkdtemp', return_value=str(ram)):
                if exit_code:
                    with self.assertRaisesRegex(AssertionError, 'cell failed'):
                        build_run.run_in_ram(command, cell, os.environ.copy(), cell_deadline_seconds)
                else:
                    build_run.run_in_ram(command, cell, os.environ.copy(), cell_deadline_seconds)
            self.assertEqual((cell / 'run.log').read_text(), 'retained diagnostic\n')
            self.assertFalse(ram.exists())
            self.assertTrue(cell.exists())

    def test_success_retains_ram_output(self):
        self.exercise(0)

    def test_failure_retains_ram_output_without_retry(self):
        self.exercise(7)

    def test_bounded_setup_budget_keeps_failed_output(self):
        self.exercise(7, cell_deadline_seconds=3600)

    def test_unfrozen_deadline_is_refused_before_launch(self):
        with self.assertRaisesRegex(AssertionError, 'unfrozen cell deadline'):
            build_run.run_in_ram([], Path('.'), {}, 3601)


if __name__ == '__main__':
    unittest.main()
