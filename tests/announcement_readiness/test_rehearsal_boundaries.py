# req: DX-003, DX-005, DX-006, DX-007, DX-009, DX-012, ID-005, VER-001, END-007, END-008
"""Adversarial artifact and process boundaries, independent of live qualification."""
import io
import ast
import os
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from processes import Process, private_environment, run
from public_assets import archive_identity, checksum_entries


class RehearsalBoundaries(unittest.TestCase):
    def test_core_conformance_has_no_framework_or_adapter_dependency(self):
        root = Path(__file__).resolve().parents[2]
        allowed = sys.stdlib_module_names | {"diagnostics", "prepare", "processes", "public_assets"}
        sources = [root / "scripts/announcement-readiness", *Path(__file__).parent.glob("*.py")]
        for source in sources:
            tree = ast.parse(source.read_text())
            for node in ast.walk(tree):
                names = [item.name for item in node.names] if isinstance(node, ast.Import) else [node.module] if isinstance(node, ast.ImportFrom) else []
                for name in names:
                    self.assertIn(name.split(".")[0], allowed, str(source.relative_to(root)))
        self.assertEqual(list((root / "fixtures/announcement-readiness").rglob("go.mod")), [])
        self.assertEqual(list((root / "fixtures/announcement-readiness").rglob("package.json")), [])

    def test_anonymous_checksums_refuse_duplicates_paths_and_invalid_digests(self):
        self.assertEqual(checksum_entries(b"a"*64 + b"  candidate.tar.gz\n"), {"candidate.tar.gz": "a"*64})
        self.assertEqual(checksum_entries(b"a"*64 + b"  conformance/go-v1.json\n"), {"conformance/go-v1.json": "a"*64})
        for content in (b"a"*64 + b"  ../secret\n", b"invalid  candidate.tar.gz\n",
                        (b"a"*64 + b"  candidate.tar.gz\n")*2):
            with self.assertRaises(ValueError):
                checksum_entries(content)

    def test_archive_inspection_refuses_links_traversal_and_duplicate_members(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "archive.tar.gz"
            for kind in ("link", "traversal", "duplicate"):
                with tarfile.open(path, "w:gz") as archive:
                    member = tarfile.TarInfo("candidate/REVISION")
                    member.size = 41
                    archive.addfile(member, io.BytesIO(b"a"*40 + b"\n"))
                    if kind == "link":
                        member = tarfile.TarInfo("candidate/bin/riffdb")
                        member.type = tarfile.SYMTYPE
                        member.linkname = "/secret-canary"
                    elif kind == "traversal":
                        member = tarfile.TarInfo("candidate/../secret-canary")
                    archive.addfile(member, io.BytesIO(b"a"*41) if member.size else None)
                with self.assertRaises(ValueError):
                    archive_identity(path)

    def test_application_child_environment_does_not_inherit_operator_authority(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.environ["RIFFDB_CREDENTIAL_FILE"] = "secret-authority-canary"
            try:
                stdout, _ = run([sys.executable, "-c", "import os; assert 'RIFFDB_CREDENTIAL_FILE' not in os.environ; print('isolated')"],
                                cwd=root, env=private_environment(root))
                self.assertEqual(stdout, b"isolated\n")
            finally:
                del os.environ["RIFFDB_CREDENTIAL_FILE"]

    def test_child_output_is_bounded_before_retention(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child = Process([sys.executable, "-c", "import os; os.write(1,b'x'*(9*1024*1024))"],
                            cwd=root, env=private_environment(root))
            with self.assertRaisesRegex(RuntimeError, "process_output_bound"):
                child.wait(timeout=10)
            self.assertLessEqual(len(child.output[0]), 8*1024*1024)


if __name__ == "__main__":
    unittest.main()
