# req: END-007, END-008, END-009, END-010
"""Hostile-evidence tests, never a measured TicketDesk campaign."""

import hashlib
import importlib.machinery
import importlib.util
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[2] / "scripts/check-ticketdesk-lifecycle"
LOADER = importlib.machinery.SourceFileLoader("ticketdesk_lifecycle", str(SCRIPT))
SPEC = importlib.util.spec_from_loader(LOADER.name, LOADER)
CHECK = importlib.util.module_from_spec(SPEC)
LOADER.exec_module(CHECK)


def fixture():
    """Synthetic in-memory checker input; never written as measured evidence."""
    manifest = {
        "schema": CHECK.MANIFEST_SCHEMA,
        "target": dict(CHECK.TARGET),
        "artifacts": {name: "a" * 64 for name in CHECK.ARTIFACTS},
        "faults": [
            {"kind": kind, "start_s": 1800 + i * 2400,
             "duration_s": 300 if kind in CHECK.OUTAGES else 1}
            for i, kind in enumerate(sorted(CHECK.ACTIONS))
        ],
        "resource_limits": {name: 8 * 1024**3 for name in CHECK.RESOURCES},
    }
    receipt = {
        "schema": CHECK.RECEIPT_SCHEMA,
        "classification": "rehearsal",
        "evidence_kind": "measured",
        "manifest_sha256": "b" * 64,
        "source_revision": "a" * 40,
        "artifacts": {name: {"path": name + ".json", "sha256": "a" * 64}
                      for name in CHECK.ARTIFACTS},
        "checks": {name: True for name in CHECK.CHECKS},
        "actions": [dict(action, recovery_s=30, reconciled=True)
                    for action in manifest["faults"]],
        "reclaimed_generations": [2, 3, 4],
        "windows": [],
    }
    for start in range(0, 86400, 300):
        receipt["windows"].append({
            "start_s": start, "end_s": start + 300,
            "offered_commands": 30000, "offered_reads": 120000,
            "completed_commands": 30000, "completed_reads": 120000,
            "transport_attempts": 150000, "retries": 0,
            "unresolved_commands": 0, "unexpected_errors": 0,
            "p95_us": 10000, "p99_us": 20000, "max_in_flight": 32,
            "eligible_history_bytes": 1024, "consumer_backlog": 0,
            "outbox_backlog": 0, "projection_backlog": 0,
            "interference": False,
            "resources": {name: 1024 for name in CHECK.RESOURCES},
        })
    return manifest, receipt


class EvidenceTests(unittest.TestCase):
    def rejected(self, mutate, code):
        manifest, receipt = fixture()
        mutate(manifest, receipt)
        with self.assertRaisesRegex(CHECK.EvidenceError, "^" + code + "$"):
            CHECK.validate(manifest, receipt)

    def test_complete_synthetic_input_exercises_checker_only(self):
        CHECK.validate(*fixture())

    def test_short_rehearsal_preserves_six_final_fault_free_hours(self):
        manifest, receipt = fixture()
        receipt["windows"] = receipt["windows"][:144]
        CHECK.validate(manifest, receipt, short_rehearsal=True)
        with self.assertRaisesRegex(CHECK.EvidenceError, "^duration$"):
            CHECK.validate(manifest, receipt)

    def test_short_rehearsal_refuses_faults_in_final_six_hours(self):
        manifest, receipt = fixture()
        receipt["windows"] = receipt["windows"][:120]
        with self.assertRaisesRegex(CHECK.EvidenceError, "^actions$"):
            CHECK.validate(manifest, receipt, short_rehearsal=True)

    def test_short_rehearsal_still_requires_reclamation_and_service_levels(self):
        manifest, receipt = fixture()
        receipt["windows"] = receipt["windows"][:144]
        receipt["reclaimed_generations"] = [2, 3]
        with self.assertRaisesRegex(CHECK.EvidenceError, "^reclamation$"):
            CHECK.validate(manifest, receipt, short_rehearsal=True)
        receipt["reclaimed_generations"] = [2, 3, 4]
        receipt["windows"][-1]["p99_us"] = 250000
        with self.assertRaisesRegex(CHECK.EvidenceError, "^service_level$"):
            CHECK.validate(manifest, receipt, short_rehearsal=True)

    def test_short_rehearsal_cannot_hide_history_growth_before_hour_eighteen(self):
        manifest, receipt = fixture()
        receipt["windows"] = receipt["windows"][:144]
        receipt["windows"][-1]["eligible_history_bytes"] = 2048
        with self.assertRaisesRegex(CHECK.EvidenceError, "^backlog_growth$"):
            CHECK.validate(manifest, receipt, short_rehearsal=True)

    def test_twenty_four_hours_never_qualifies_alpha(self):
        self.rejected(lambda m, r: r.update(classification="alpha"), "classification")

    def test_logical_pruning_without_physical_reclamation_is_rejected(self):
        self.rejected(lambda m, r: r.update(reclaimed_generations=[]), "reclamation")

    def test_duplicate_generation_does_not_count_as_repeated_reclamation(self):
        self.rejected(lambda m, r: r.update(reclaimed_generations=[2, 2, 2]), "reclamation")

    def test_incomplete_and_gapped_observation_is_rejected(self):
        self.rejected(lambda m, r: r["windows"].pop(), "duration")
        self.rejected(lambda m, r: r["windows"][3].update(start_s=901), "window")

    def test_sustained_rates_cannot_be_hidden_by_fast_other_windows(self):
        self.rejected(lambda m, r: r["windows"][-1].update(
            completed_commands=29999, transport_attempts=149999), "service_level")

    def test_final_six_hours_cannot_accumulate_eligible_history(self):
        self.rejected(lambda m, r: r["windows"][-1].update(eligible_history_bytes=2048), "backlog_growth")

    def test_final_cleanup_cannot_hide_a_growing_history_trend(self):
        def mutate(manifest, receipt):
            for index, window in enumerate(receipt["windows"][-72:-1]):
                window["eligible_history_bytes"] += index * 1024
        self.rejected(mutate, "backlog_growth")

    def test_retained_archive_does_not_excuse_memory_growth(self):
        self.rejected(lambda m, r: r["windows"][-1]["resources"].update(rss_bytes=9 * 1024**3), "resource_limit")

    def test_faults_cannot_be_omitted_or_relabelled_after_the_run(self):
        self.rejected(lambda m, r: r["actions"].pop(), "actions")
        self.rejected(lambda m, r: r["actions"][0].update(start_s=900), "actions")

    def test_outage_recovery_and_restart_have_distinct_limits(self):
        def slow(kind, seconds):
            def mutate(manifest, receipt):
                next(a for a in receipt["actions"] if a["kind"] == kind)["recovery_s"] = seconds
            return mutate
        self.rejected(slow("sink_outage", 601), "recovery")
        self.rejected(slow("process_kill", 61), "recovery")
        self.rejected(slow("process_kill", 60), "recovery")

    def test_restart_recovery_includes_the_action_duration(self):
        manifest, receipt = fixture()
        for action in manifest["faults"] + receipt["actions"]:
            if action["kind"] == "graceful_restart":
                action["duration_s"] = 30
        CHECK.validate(manifest, receipt)
        next(action for action in receipt["actions"]
             if action["kind"] == "graceful_restart")["recovery_s"] = 31
        with self.assertRaisesRegex(CHECK.EvidenceError, "^recovery$"):
            CHECK.validate(manifest, receipt)

    def test_hidden_retry_and_unresolved_command_are_rejected(self):
        self.rejected(lambda m, r: r["windows"][-1].update(transport_attempts=150001), "attempt_accounting")
        self.rejected(lambda m, r: r["windows"][-1].update(unresolved_commands=1), "service_level")

    def test_interference_missing_reconciliation_and_artifact_drift_fail(self):
        self.rejected(lambda m, r: r["windows"][0].update(interference=True), "interference")
        self.rejected(lambda m, r: r["checks"].update(restore_reconciled=False), "checks")
        self.rejected(lambda m, r: r["artifacts"]["server"].update(sha256="c" * 64), "artifact_identity")

    def test_unknown_fields_and_boolean_counters_fail_closed(self):
        self.rejected(lambda m, r: r.update(private_payload="not permitted"), "shape")
        self.rejected(lambda m, r: r["windows"][0].update(retries=True), "integer")

    def test_measured_label_is_required_and_targets_cannot_drift(self):
        self.rejected(lambda m, r: r.update(evidence_kind="synthetic"), "classification")
        self.rejected(lambda m, r: m["target"].update(commands_per_second=1), "target")

    def test_bounded_reader_rejects_duplicates_and_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.json"
            source.write_text('{"same":1,"same":2}')
            with self.assertRaises(CHECK.EvidenceError):
                CHECK.read_object(source)
            source.write_text('{}')
            link = root / "link.json"
            link.symlink_to(source)
            with self.assertRaises(CHECK.EvidenceError):
                CHECK.read_object(link)

    def test_excessive_json_integer_refuses_without_a_raw_exception(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.json"
            source.write_text('{"counter":' + "9" * 5000 + '}')
            with self.assertRaises(CHECK.EvidenceError):
                CHECK.read_object(source)

    def test_artifact_paths_cannot_escape_evidence_root(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(CHECK.EvidenceError):
                CHECK.verify_artifact(Path(directory), "../outside", "a" * 64)

    def test_artifacts_are_hashed_from_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = b"bounded fixture"
            (root / "artifact").write_bytes(data)
            CHECK.verify_artifact(root, "artifact", hashlib.sha256(data).hexdigest())
            with self.assertRaises(CHECK.EvidenceError):
                CHECK.verify_artifact(root, "artifact", "a" * 64)


if __name__ == "__main__":
    unittest.main()
