#!/usr/bin/env python3
# req: DRV-014, VER-003
"""Exercise the installed Go package check against current and stale artifacts."""
import contextlib
import io
from pathlib import Path
import runpy
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[2]
BUILD = runpy.run_path(str(ROOT / 'scripts/build-driver-distributions'))
ARRIVAL = runpy.run_path(str(ROOT / 'scripts/driver-package-arrival'))


class GoPackageArrival(unittest.TestCase):
    def test_current_artifact_passes_and_old_protocol_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix='riffdb-go-arrival.') as temporary:
            root = Path(temporary)
            distribution = root / 'distribution'
            BUILD['go_proxy'](distribution, 1735689600)
            current = root / 'current'
            current.mkdir()
            ARRIVAL['smoke_go'](distribution, current)

            archive = distribution / 'go-proxy/riffdb.dev/application/@v/v0.1.0.zip'
            with zipfile.ZipFile(archive) as source:
                entries = [(entry, source.read(entry.filename)) for entry in source.infolist()]
            with zipfile.ZipFile(archive, 'w') as destination:
                for entry, data in entries:
                    if entry.filename.endswith('/runtime.go'):
                        old = b'ProtocolVersion   = uint32(4)'
                        self.assertEqual(data.count(old), 1)
                        data = data.replace(old, b'ProtocolVersion   = uint32(3)')
                    destination.writestr(entry, data)
            stale = root / 'stale'
            stale.mkdir()
            diagnostics = io.StringIO()
            with contextlib.redirect_stderr(diagnostics):
                with self.assertRaisesRegex(SystemExit, 'command failed: go test'):
                    ARRIVAL['smoke_go'](distribution, stale)
            self.assertIn('unexpected protocol', diagnostics.getvalue())


if __name__ == '__main__':
    unittest.main()
