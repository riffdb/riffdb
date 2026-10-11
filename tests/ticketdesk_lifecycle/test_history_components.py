# req: END-007, END-008, END-010
"""Synthetic numeric checks cannot qualify actual campaign resources."""
from pathlib import Path
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from lib.ticketdesk_lifecycle_history_components import HistoryComponentsError, read_observations, reconcile

SECOND = 1000000000
DURATION = 28800
FINAL_START = DURATION - 21600


def records():
    result = {"logical": [], "source": [], "physical": []}
    for index, second in enumerate(range(0, DURATION, 60)):
        common = dict(root=2 + 2 * index, incarnation=7, elapsed_ns=second * SECOND,
                      capture_age_us=1000, pages=1, qualifies_complete_resource_model=False)
        result["logical"].append(dict(common,
            schema="riffdb.ticketdesk-lifecycle-logical-byte-sample/v1",
            basis="canonical_logical_history_and_removed_keys",
            watermark=100, head=101, ceiling=101, through=101, rows=1,
            canonical_history_value_bytes=1048576, removed_key_bytes=8))
        result["source"].append(dict(common,
            schema="riffdb.ticketdesk-lifecycle-source-byte-sample/v1",
            basis="canonical_source_receipts_and_keys", resume=100, ceiling=101,
            rows=1, canonical_receipt_bytes=100, key_bytes=8, collection_us=500))
        result["physical"].append(dict(common,
            schema="riffdb.ticketdesk-lifecycle-physical-byte-sample/v1",
            basis="native_original_sealed_prefix_allocated_blocks",
            watermark=100, minimum_resume=101, active_generation=2, sealed_extents=1,
            eligible_allocated_bytes=512, protected_allocated_bytes=0,
            reference_protected_allocated_bytes=0, reader_protected_allocated_bytes=0,
            reader_held=0, diagnostic_pin_excluded=1, pages=2))
    return result


class HistoryComponents(unittest.TestCase):
    def test_final_windows_preserve_three_separate_bases_and_claim_no_complete_model(self):
        result = reconcile(records(), duration_s=DURATION, incarnation=7)
        self.assertEqual(len(result["windows"]), 72)
        self.assertEqual(result["windows"][0]["start_s"], FINAL_START)
        self.assertEqual(result["windows"][-1]["end_s"], DURATION)
        self.assertEqual(result["windows"][0]["components"],
                         dict(logical=1048584, source=108, physical=512))
        self.assertNotIn("eligible_history_bytes", result)
        self.assertFalse(result["qualifies_complete_resource_model"])

    def test_final_source_v2_basis_is_frozen_without_claiming_reused_rows_as_scans(self):
        value = records()
        for row in value["source"]:
            row.update(schema="riffdb.ticketdesk-lifecycle-source-byte-sample/v2",
                       basis="qualified_canonical_source_receipts_and_keys",
                       reused_rows=1, checked_rows=0, subtracted_rows=0, pages=0,
                       publication_kind=1, publication_hash_a=7, publication_hash_b=0,
                       publication_hash_c=0, publication_hash_d=0)
        result = reconcile(value, duration_s=DURATION, incarnation=7)
        self.assertEqual(result["basis"]["source"], "qualified_canonical_source_receipts_and_keys")
        self.assertEqual(result["windows"][0]["components"]["source"], 108)
        value["source"][1] = records()["source"][1]
        with self.assertRaisesRegex(HistoryComponentsError, "^history_components_basis$"):
            reconcile(value, duration_s=DURATION, incarnation=7)

    def test_declining_logical_bytes_cannot_hide_increasing_physical_or_source_bytes(self):
        for growing in ("physical", "source"):
            value = records()
            for index in range(len(value["logical"])):
                window = index // 5
                value["logical"][index]["canonical_history_value_bytes"] -= window * 2048
                if growing == "physical":
                    value[growing][index]["eligible_allocated_bytes"] += window * 512
                else:
                    value[growing][index]["canonical_receipt_bytes"] += window * 512
            with self.subTest(growing=growing), self.assertRaisesRegex(
                    HistoryComponentsError, "^history_components_growth$"):
                reconcile(value, duration_s=DURATION, incarnation=7)

    def test_final_cleanup_cannot_hide_growth_across_the_quiet_period(self):
        value = records()
        for row in value["source"]:
            window = row["elapsed_ns"] // (300 * SECOND) - FINAL_START // 300
            if 40 <= window < 71:
                row["canonical_receipt_bytes"] += 200
        with self.assertRaisesRegex(HistoryComponentsError, "^history_components_growth$"):
            reconcile(value, duration_s=DURATION, incarnation=7)

    def test_a_window_uses_its_maximum_without_mixing_native_roots_or_bases(self):
        value = records()
        for index in range(len(value["logical"])):
            value["logical"][index]["canonical_history_value_bytes"] -= index % 5
        result = reconcile(value, duration_s=DURATION, incarnation=7)
        self.assertTrue(all(window["components"]["logical"] == 1048584
                            for window in result["windows"]))

    def test_missing_windows_and_stale_samples_never_become_zero_backlog(self):
        value = records()
        value["source"] = [row for row in value["source"]
                           if not FINAL_START <= row["elapsed_ns"] // SECOND < FINAL_START + 300]
        with self.assertRaises(HistoryComponentsError):
            reconcile(value, duration_s=DURATION, incarnation=7)
        value = records()
        value["physical"] = value["physical"][::2]
        with self.assertRaisesRegex(HistoryComponentsError, "^history_components_gap$"):
            reconcile(value, duration_s=DURATION, incarnation=7)

    def test_mutated_native_accounting_identity_or_metadata_refuses(self):
        for kind, field, replacement in (
            ("physical", "reader_held", 1), ("source", "key_bytes", 7),
            ("logical", "root", 3), ("source", "incarnation", 8),
            ("logical", "basis", "canonical_source_receipts_and_keys"),
            ("physical", "qualifies_complete_resource_model", True),
            ("logical", "canonical_history_value_bytes", True),
            ("physical", "elapsed_ns", -1),
        ):
            value = records()
            value[kind][-1][field] = replacement
            with self.subTest(kind=kind, field=field), self.assertRaises(HistoryComponentsError):
                reconcile(value, duration_s=DURATION, incarnation=7)
        value = records()
        value["physical"][-1]["unknown"] = 1
        with self.assertRaises(HistoryComponentsError):
            reconcile(value, duration_s=DURATION, incarnation=7)

    def test_extra_missing_duplicate_rewound_or_oversized_inputs_refuse(self):
        for change in (lambda value: value.pop("logical"),
                       lambda value: value.update(unknown=[]),
                       lambda value: value["source"].append(value["source"][-1]),
                       lambda value: value["physical"].reverse(),
                       lambda value: value.update(logical=value["logical"] * 5)):
            value = records()
            change(value)
            with self.assertRaises(HistoryComponentsError):
                reconcile(value, duration_s=DURATION, incarnation=7)
        for duration in (0, 21600, 28801, 86700, True):
            with self.assertRaises(HistoryComponentsError):
                reconcile(records(), duration_s=duration, incarnation=7)


def files(root):
    encoded = {}
    for kind, rows in records().items():
        encoded[kind] = b"".join(json.dumps(row, sort_keys=True).encode() + b"\n" for row in rows)
        path = root / (kind + "-byte-samples.jsonl")
        path.write_bytes(encoded[kind])
        path.chmod(0o600)
    return encoded


class OriginalObservationCustody(unittest.TestCase):
    def test_actual_file_bytes_are_hashed_and_the_cli_keeps_qualification_unmet(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            encoded = files(root)
            actual, inputs = read_observations(root)
            for kind in encoded:
                self.assertEqual(inputs[kind], dict(sha256=hashlib.sha256(encoded[kind]).hexdigest(),
                                                   bytes=len(encoded[kind]), samples=480))
            self.assertEqual(reconcile(actual, duration_s=DURATION, incarnation=7)["final_samples"],
                             dict(logical=360, source=360, physical=360))
            output = root / "checked.json"
            command = [sys.executable, str(Path(__file__).resolve().parents[2] / "scripts" /
                       "check-ticketdesk-lifecycle-history-components"), "--root", str(root),
                       "--duration-s", str(DURATION), "--incarnation", "7", "--output", str(output)]
            result = subprocess.run(command, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stdout)
            report = json.loads(output.read_bytes())
            self.assertEqual(report["inputs"], inputs)
            self.assertFalse(report["qualifies_complete_resource_model"])
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            before = output.read_bytes()
            result = subprocess.run(command, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 1)
            self.assertNotIn(str(root).encode(), result.stdout + result.stderr)
            self.assertEqual(output.read_bytes(), before)

    def test_symlinks_hardlinks_truncation_duplicate_json_and_nonprivate_inputs_refuse(self):
        for kind in ("symlink", "hardlink", "truncated", "duplicate", "permissions", "missing"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                files(root)
                path = root / "source-byte-samples.jsonl"
                if kind == "symlink":
                    path.rename(root / "original")
                    path.symlink_to(root / "original")
                elif kind == "hardlink":
                    os.link(path, root / "alias")
                elif kind == "truncated":
                    path.write_bytes(path.read_bytes()[:-1])
                elif kind == "duplicate":
                    path.write_bytes(path.read_bytes().replace(b'{"basis":', b'{"root":2,"basis":', 1))
                elif kind == "permissions":
                    path.chmod(0o644)
                else:
                    path.unlink()
                with self.assertRaises(HistoryComponentsError):
                    read_observations(root)

    def test_a_prior_input_changed_during_later_file_collection_refuses_the_whole_cohort(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            files(root)
            real_stat = os.stat
            changed = False

            def stat_after_change(name, *args, **kwargs):
                nonlocal changed
                if name == "source-byte-samples.jsonl" and not changed:
                    changed = True
                    with (root / "logical-byte-samples.jsonl").open("ab") as output:
                        output.write(b"\n")
                return real_stat(name, *args, **kwargs)

            with patch("lib.ticketdesk_lifecycle_history_components.os.stat", stat_after_change):
                with self.assertRaisesRegex(HistoryComponentsError, "^history_components_changed$"):
                    read_observations(root)
            self.assertTrue(changed)


if __name__ == "__main__":
    unittest.main()
