# req: DX-003, DX-006, DX-007, DX-009, DX-012, ID-005, VER-001, END-007, END-008
"""Closed installed-artifact and announcement gates; no source-build fallback."""
import hashlib
import importlib.machinery
import importlib.util
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
LOADER = importlib.machinery.SourceFileLoader("announcement_readiness", str(ROOT / "scripts/announcement-readiness"))
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
READINESS = importlib.util.module_from_spec(SPEC)
LOADER.exec_module(READINESS)


class CandidateInventory(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="riffdb-candidate-secret-path-canary-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "bin").mkdir()
        (self.root / "VERSION").write_text("0.2.0\n")
        (self.root / "REVISION").write_text("a" * 40 + "\n")
        for binary in ["riffdb", "riffdbd", "riffdb-driverd", "riffdb-mcp"]:
            (self.root / "bin" / binary).write_bytes(b"synthetic executable inventory fixture\n")
            (self.root / "bin" / binary).chmod(0o755)
        self.checksums()

    def checksums(self):
        files = sorted(path for path in self.root.rglob("*") if path.is_file() and path.name != "SHA256SUMS")
        (self.root / "SHA256SUMS").write_text("".join(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(self.root).as_posix()}\n" for path in files))

    def test_exact_product_inventory_and_revision_are_required(self):
        result = READINESS.inspect_product_bundle(self.root, "0.2.0", "a" * 40)
        self.assertEqual(result["binaries"], ["riffdb", "riffdb-driverd", "riffdb-mcp", "riffdbd"])
        self.assertTrue(result["checksums_verified"])
        self.assertEqual(result["source_revision"], "a" * 40)
        files = sorted(path for path in self.root.rglob("*") if path.is_file() and path.name != "SHA256SUMS")
        (self.root / "SHA256SUMS").write_text("".join(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  ./{path.relative_to(self.root).as_posix()}\n" for path in files))
        dot_prefixed = READINESS.inspect_product_bundle(self.root, "0.2.0", "a" * 40)
        self.assertTrue(dot_prefixed["checksums_verified"])

    def test_missing_driver_refuses_without_source_build_or_hidden_repair(self):
        (self.root / "bin/riffdb-driverd").unlink()
        self.checksums()
        with self.assertRaisesRegex(READINESS.CandidateError, "product_binary_inventory"):
            READINESS.inspect_product_bundle(self.root, "0.2.0", "a" * 40)
        self.assertFalse((self.root / "bin/riffdb-driverd").exists())

    def test_checksum_tampering_and_wrong_revision_are_distinct_value_free_causes(self):
        with self.assertRaisesRegex(READINESS.CandidateError, "source_revision_mismatch") as failure:
            READINESS.inspect_product_bundle(self.root, "0.2.0", "b" * 40)
        self.assertNotIn("secret-path-canary", str(failure.exception))
        (self.root / "bin/riffdbd").write_bytes(b"tampered")
        with self.assertRaisesRegex(READINESS.CandidateError, "artifact_checksum_mismatch"):
            READINESS.inspect_product_bundle(self.root, "0.2.0", "a" * 40)

    def test_links_extra_executables_and_escaping_manifest_paths_refuse(self):
        (self.root / "bin/riffdb").unlink()
        (self.root / "bin/riffdb").symlink_to(self.root / "bin/riffdbd")
        with self.assertRaisesRegex(READINESS.CandidateError, "artifact_file_type"):
            READINESS.inspect_product_bundle(self.root, "0.2.0", "a" * 40)
        (self.root / "bin/riffdb").unlink()
        (self.root / "bin/riffdb").write_bytes(b"fixture")
        (self.root / "bin/riffdb").chmod(0o755)
        (self.root / "bin/unsafe-helper").write_bytes(b"fixture")
        self.checksums()
        with self.assertRaisesRegex(READINESS.CandidateError, "product_binary_inventory"):
            READINESS.inspect_product_bundle(self.root, "0.2.0", "a" * 40)
        (self.root / "bin/unsafe-helper").unlink()
        self.checksums()
        with (self.root / "SHA256SUMS").open("a") as manifest:
            manifest.write("0" * 64 + "  ../secret-path-canary\n")
        with self.assertRaisesRegex(READINESS.CandidateError, "artifact_manifest_path"):
            READINESS.inspect_product_bundle(self.root, "0.2.0", "a" * 40)

    def test_local_gate_requires_success_for_every_original_track(self):
        checks = {track: True for track in READINESS.TRACKS}
        self.assertEqual(len(checks), 5)
        self.assertTrue(READINESS.local_gate(checks))
        for track in READINESS.TRACKS:
            missing = dict(checks)
            del missing[track]
            self.assertFalse(READINESS.local_gate(missing))
            missing[track] = False
            self.assertFalse(READINESS.local_gate(missing))
        checks["unknown_track"] = True
        self.assertFalse(READINESS.local_gate(checks))

    def test_publication_gate_requires_availability_identity_and_complete_checksums(self):
        complete = {check: True for check in READINESS.PUBLIC_CHECKS}
        self.assertTrue(READINESS.public_gate(complete))
        for check in complete:
            refused = dict(complete)
            refused[check] = False
            self.assertFalse(READINESS.public_gate(refused))
        self.assertFalse(READINESS.public_gate({"anonymous_downloads": True}))

    def test_external_behavior_evidence_is_complete_and_bound_without_loading_an_adapter(self):
        import json
        contract = json.loads((ROOT / "fixtures/announcement-readiness/behavior-contract-v1.json").read_text())
        evidence = {"contract_version": 1, "product_revision": "a"*40,
                    "cases": {case["id"]: True for case in contract["cases"]}}
        self.assertTrue(READINESS.backend_behavior_evidence_is_complete_and_exactly_bound(evidence, "a"*40))
        self.assertFalse(READINESS.backend_behavior_evidence_is_complete_and_exactly_bound(evidence, "b"*40))
        for name in list(evidence["cases"]):
            refused = {**evidence, "cases": {**evidence["cases"], name: False}}
            self.assertFalse(READINESS.backend_behavior_evidence_is_complete_and_exactly_bound(refused, "a"*40))
        del evidence["cases"][next(iter(evidence["cases"]))]
        self.assertFalse(READINESS.backend_behavior_evidence_is_complete_and_exactly_bound(evidence, "a"*40))


if __name__ == "__main__":
    unittest.main()
