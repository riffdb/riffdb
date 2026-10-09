# req: END-003, END-007, END-008, END-010
"""Declared owner changes cannot reset global evidence bounds or hide rewinds."""
from contextlib import contextmanager
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import test_campaign_process_roster as owners
import test_provider_budget as providers
import test_source_bytes_continuation as sources
import test_logical_bytes as logicals
import test_physical_bytes as physicals
import test_native_observer as natives
from lib.ticketdesk_lifecycle_campaign_native_epochs import CampaignNativeEpochs, EpochError
from lib.ticketdesk_lifecycle_native_observer import NativeObserverProcess, NativeSamples

SECOND = 10**9
IDENTITY = natives.NativeSampleContinuity.identity


class NativeCampaignEpochs(unittest.TestCase):
    @contextmanager
    def fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            helper = owners.CampaignRosterTests()
            try:
                roster = helper.fixture(root)
                self.next_pid = 128
                with CampaignNativeEpochs(root, roster, 21900, IDENTITY) as epochs:
                    self.emit(epochs, 0)
                    epochs.observe_native(natives.NativeSampleContinuity().sample(), 0)
                    yield root, helper, roster, epochs
            finally:
                helper.doCleanups()

    def emit(self, epochs, elapsed, *, root=10, incarnation=1, changes=None):
        changes = changes or {}
        rows = dict(provider=providers.line,
                    source=lambda **kw: sources.line(root=root, incarnation=incarnation, **kw),
                    logical=lambda **kw: logicals.line(root=root, incarnation=incarnation, **kw),
                    physical=lambda **kw: physicals.line(root=root, incarnation=incarnation, **kw))
        for name, line in rows.items():
            epochs.observe(name, line(**changes.get(name, {})), elapsed)

    def replace(self, helper, roster, *, kind="graceful_restart"):
        roles = ("native_sampler",) if kind == "additive_deploy" else ("native_sampler", "server")
        for role in roles:
            old = roster.children[role]
            code = -9 if kind == "process_kill" and role == "server" else 0
            old.exit(code)
            roster.retire(role, old, expected_exit=code)
        for role in roles:
            child = owners.FixtureProcess(self.next_pid)
            self.next_pid += 1
            helper.tree(roster.proc_root, [c.pid for c in roster.children.values()] + [child.pid])
            with patch.object(roster, "_spawn", return_value=child):
                roster.launch(role, ["fixture"])

    def test_actual_declared_restart_keeps_global_counts_gaps_coverage_and_peak(self):
        with self.fixture() as (root, helper, roster, epochs):
            self.replace(helper, roster)
            epochs.transition("graceful_restart", 10 * SECOND, version=1, incarnation=1,
                              identity=IDENTITY)
            self.emit(epochs, 60 * SECOND, root=2,
                      changes={"provider": dict(released_allocations=0, released_owners=0,
                                                owned_bytes=4096, allocations=3)})
            epochs.observe_native(natives.NativeSampleContinuity().sample(), 60 * SECOND)
            for sample in epochs.samples.values():
                self.assertEqual(sample.count, 2)
                self.assertEqual(sample.coverage, [2] + [0] * 72)
                self.assertEqual(sample.last_ns, 60 * SECOND)
            self.assertEqual(epochs.samples["provider"].peak, 12800)
            self.assertEqual(epochs.native_count, 2)
            self.assertEqual(epochs.native_transport_attempts, 6)
            self.assertFalse(epochs.complete_campaign_checks_proved)
            rows = [json.loads(row) for row in (root / "native-observation-epochs.jsonl").read_bytes().splitlines()]
            self.assertEqual([row["kind"] for row in rows], ["initial", "graceful_restart"])
            self.assertEqual(rows[-1]["samples"], dict(provider=1, source=1, logical=1, physical=1))
            self.assertEqual(rows[-1]["server_incarnation"], 2)
            self.assertEqual(rows[-1]["sampler_incarnation"], 2)
            self.assertTrue(all(len(row) + 1 <= 4096 for row in
                                (root / "native-observation-epochs.jsonl").read_bytes().splitlines()))
            self.assertEqual((root / "native-observation-epochs.jsonl").stat().st_mode & 0o777, 0o600)

    def test_process_kill_requires_the_exact_killed_server_and_normally_closed_sampler(self):
        with self.fixture() as (_, helper, roster, epochs):
            self.replace(helper, roster, kind="process_kill")
            epochs.transition("process_kill", SECOND, version=1, incarnation=1, identity=IDENTITY)
            self.emit(epochs, 60 * SECOND, root=2)
        with self.fixture() as (_, helper, roster, epochs):
            self.replace(helper, roster)
            with self.assertRaisesRegex(EpochError, "^campaign_epoch_exit$"):
                epochs.transition("process_kill", SECOND, version=1, incarnation=1, identity=IDENTITY)

    def test_restore_changes_only_the_declared_history_lineage_and_keeps_global_time(self):
        with self.fixture() as (_, helper, roster, epochs):
            self.replace(helper, roster, kind="backup_restore")
            epochs.transition("backup_restore", 10 * SECOND, version=1, incarnation=2, identity=IDENTITY)
            self.emit(epochs, 60 * SECOND, root=2, incarnation=2,
                      changes={"source": dict(resume=1, ceiling=601),
                               "physical": dict(watermark=0, minimum_resume=1)})
            epochs.observe_native(natives.NativeSampleContinuity().sample(history_incarnation=2), 60 * SECOND)
            self.assertEqual(epochs.samples["source"].last["incarnation"], 2)
            self.assertEqual(epochs.samples["source"].last_ns, 60 * SECOND)

    def test_additive_deploy_changes_sampler_identity_without_resetting_the_live_provider_pool(self):
        with self.fixture() as (_, helper, roster, epochs):
            self.replace(helper, roster, kind="additive_deploy")
            identity = dict(projection_id=8, projection_plan_hash="b" * 64)
            epochs.transition("additive_deploy", SECOND, version=2, incarnation=1, identity=identity)
            epochs.observe_native(natives.NativeSampleContinuity().sample(
                contract_version=2, projection_id=8, projection_plan_hash="b" * 64,
                applied_through=1, projection_generation=1, projection_backlog=99), 2 * SECOND)
            with self.assertRaisesRegex(ValueError, "^provider_budget_counter_rewind$"):
                epochs.observe("provider", providers.line(released_allocations=0), 60 * SECOND)

    def test_same_lineage_restart_cannot_rewind_durable_cursors_or_heads(self):
        cases = [("source", sources.line(root=2, resume=2, ceiling=602)),
                 ("logical", logicals.line(root=2, head=601)),
                 ("physical", physicals.line(root=2, incarnation=1, minimum_resume=99)),
                 ("physical", physicals.line(root=2, incarnation=1, active_generation=3,
                                              sealed_extents=2))]
        for name, encoded in cases:
            with self.subTest(name=name), self.fixture() as (_, helper, roster, epochs):
                self.replace(helper, roster)
                epochs.transition("graceful_restart", SECOND, version=1, incarnation=1, identity=IDENTITY)
                with self.assertRaisesRegex(EpochError, "^campaign_epoch_frontier$"):
                    epochs.observe(name, encoded, 60 * SECOND)
                self.assertEqual(epochs.samples[name].count, 1)

    def test_restore_cannot_rewind_the_already_observed_command_head(self):
        with self.fixture() as (_, helper, roster, epochs):
            self.replace(helper, roster, kind="backup_restore")
            epochs.transition("backup_restore", SECOND, version=1, incarnation=2, identity=IDENTITY)
            with self.assertRaisesRegex(EpochError, "^campaign_epoch_frontier$"):
                epochs.observe_native(natives.NativeSampleContinuity().sample(
                    history_incarnation=2, sampled_head=99, projection_backlog=2), 2 * SECOND)

    def test_declaring_a_restart_without_reaping_and_replacing_exact_owners_refuses(self):
        for fault in ("unchanged", "sampler_only", "server_only", "skipped_owner"):
            with self.subTest(fault=fault), self.fixture() as (_, helper, roster, epochs):
                if fault == "sampler_only": self.replace(helper, roster, kind="additive_deploy")
                elif fault != "unchanged":
                    self.replace(helper, roster)
                    if fault == "server_only": roster.children["native_sampler"] = epochs.sampler
                    if fault == "skipped_owner": self.replace(helper, roster)
                with self.assertRaises(ValueError):
                    epochs.transition("graceful_restart", SECOND, version=1, incarnation=1, identity=IDENTITY)

    def test_transition_cannot_reset_a_global_gap_even_when_the_new_owner_is_valid(self):
        for name, elapsed, encoded in (("provider", 75 * SECOND + 1, providers.line()),
                                      ("source", 95 * SECOND + 1, sources.line(root=2)),
                                      ("logical", 95 * SECOND + 1, logicals.line(root=2)),
                                      ("physical", 95 * SECOND + 1, physicals.line(root=2, incarnation=1))):
            with self.subTest(name=name), self.fixture() as (_, helper, roster, epochs):
                self.replace(helper, roster)
                epochs.transition("graceful_restart", SECOND, version=1, incarnation=1, identity=IDENTITY)
                with self.assertRaisesRegex(ValueError, "sample_gap$"):
                    epochs.observe(name, encoded, elapsed)
                self.assertEqual(epochs.samples[name].count, 1)

    def test_eof_enforces_original_gaps_and_failed_evidence_fences_future_observations(self):
        with self.fixture() as (_, _, _, epochs):
            with self.assertRaisesRegex(ValueError, "sample_gap$"):
                epochs.check_gaps(75 * SECOND + 1)
            with self.assertRaisesRegex(EpochError, "^campaign_epoch_failed$"):
                epochs.observe("provider", providers.line(), 76 * SECOND)

    def test_wrong_version_lineage_identity_and_legacy_source_basis_refuse(self):
        cases = [dict(version=2), dict(incarnation=2), dict(identity=dict(
            projection_id=8, projection_plan_hash="b" * 64)), dict(kind="unknown")]
        for changes in cases:
            with self.subTest(changes=changes), self.fixture() as (_, helper, roster, epochs):
                self.replace(helper, roster)
                arguments = dict(kind="graceful_restart", elapsed_ns=SECOND, version=1,
                                 incarnation=1, identity=IDENTITY)
                arguments.update(changes)
                with self.assertRaises(EpochError): epochs.transition(**arguments)
        with self.fixture() as (_, _, _, epochs):
            with self.assertRaisesRegex(EpochError, "^campaign_epoch_source_version$"):
                epochs.observe("source", sources.v1(), SECOND)

    def test_global_native_indexes_and_accounting_survive_sampler_replacement(self):
        with self.fixture() as (_, helper, roster, epochs):
            self.replace(helper, roster)
            epochs.transition("graceful_restart", SECOND, version=1, incarnation=1, identity=IDENTITY)
            epochs.observe_native(natives.NativeSampleContinuity().sample(), 2 * SECOND)
            with self.assertRaisesRegex(ValueError, "^native_sample_continuity$"):
                epochs.observe_native(natives.NativeSampleContinuity().sample(), 3 * SECOND)
            self.assertEqual(epochs.native_count, 2)
            self.assertEqual(epochs.native_transport_attempts, 6)

    def test_replacement_cannot_erase_sample_bound_or_incomplete_epoch(self):
        with self.fixture() as (_, helper, roster, epochs):
            self.replace(helper, roster)
            epochs.transition("graceful_restart", SECOND, version=1, incarnation=1, identity=IDENTITY)
            epochs.samples["provider"].count = 2048
            with self.assertRaisesRegex(ValueError, "^provider_budget_sample_bound$"):
                epochs.observe("provider", providers.line(), 60 * SECOND)
        with self.fixture() as (_, helper, roster, epochs):
            self.replace(helper, roster)
            epochs.transition("graceful_restart", SECOND, version=1, incarnation=1, identity=IDENTITY)
            self.replace(helper, roster)
            with self.assertRaisesRegex(EpochError, "^campaign_epoch_incomplete$"):
                epochs.transition("graceful_restart", 2 * SECOND, version=1, incarnation=1, identity=IDENTITY)

    def test_terminal_coverage_cannot_be_replaced_with_an_epoch_receipt(self):
        with self.fixture() as (_, helper, roster, epochs):
            self.replace(helper, roster)
            epochs.transition("graceful_restart", SECOND, version=1, incarnation=1, identity=IDENTITY)
            with self.assertRaises(ValueError): epochs.finish(21900 * SECOND)

    def test_complete_global_component_coverage_remains_nonqualifying_after_a_restart(self):
        with self.fixture() as (_, helper, roster, epochs):
            self.replace(helper, roster)
            epochs.transition("graceful_restart", SECOND, version=1, incarnation=1, identity=IDENTITY)
            for elapsed in range(60, 21900, 60):
                self.emit(epochs, elapsed * SECOND, root=2)
                epochs.observe_native(natives.NativeSampleContinuity().sample(index=elapsed // 60 - 1),
                                      elapsed * SECOND)
            result = epochs.finish(21900 * SECOND)
            self.assertFalse(result["qualifies_complete_resource_model"])
            self.assertEqual(result["native_samples"], 365)
            self.assertEqual(result["native_transport_attempts"], 1095)
            self.assertTrue(all(sample.coverage == [5] * 73 for sample in epochs.samples.values()))
            self.assertTrue(all(sample.count == 365 for sample in epochs.samples.values()))

    def test_native_limit_and_duration_bound_cannot_be_reset_by_a_new_epoch(self):
        with self.fixture() as (_, helper, roster, epochs):
            self.replace(helper, roster)
            epochs.transition("graceful_restart", SECOND, version=1, incarnation=1, identity=IDENTITY)
            epochs.native_count = 86400
            with self.assertRaisesRegex(EpochError, "^campaign_epoch_bound$"):
                epochs.observe_native(natives.NativeSampleContinuity().sample(), 2 * SECOND)
        with self.fixture() as (root, _, roster, _):
            for duration in (True, 21600, 21901, 86700):
                with self.subTest(duration=duration), self.assertRaisesRegex(EpochError, "^campaign_epoch_duration$"):
                    CampaignNativeEpochs(root, roster, duration, IDENTITY)

    def test_untracked_replacement_and_relabelled_history_never_supply_owner_proof(self):
        with self.fixture() as (_, helper, roster, epochs):
            self.replace(helper, roster)
            helper.tree(roster.proc_root, [c.pid for c in roster.children.values()] + [999])
            with self.assertRaisesRegex(ValueError, "^resource_process_inventory$"):
                epochs.transition("graceful_restart", SECOND, version=1, incarnation=1, identity=IDENTITY)
        with self.fixture() as (_, helper, roster, epochs):
            self.replace(helper, roster, kind="backup_restore")
            epochs.transition("backup_restore", SECOND, version=1, incarnation=2, identity=IDENTITY)
            with self.assertRaisesRegex(ValueError, "^source_bytes_incarnation$"):
                epochs.observe("source", sources.line(root=2, incarnation=1), 60 * SECOND)

    def test_native_collection_calls_the_exact_sampler_and_keeps_its_original_elapsed_field(self):
        with self.fixture() as (_, _, roster, epochs):
            owner = NativeObserverProcess.__new__(NativeObserverProcess)
            owner.process = roster.children["native_sampler"]
            owner.samples = NativeSamples(IDENTITY)
            value = dict(natives.NativeSampleContinuity().sample(index=1), elapsed_ns=SECOND)
            with patch.object(owner, "sample", return_value=value) as sample:
                self.assertEqual(epochs.sample_native(owner, SECOND), value)
                sample.assert_called_once_with(SECOND, version=1)
            self.assertEqual(epochs.native_count, 2)
            self.assertEqual(epochs.native_transport_attempts, 6)
            self.assertEqual(value["elapsed_ns"], SECOND)

    def test_wrong_sampler_or_returned_clock_never_becomes_current_epoch_evidence(self):
        for fault in ("owner", "identity", "clock"):
            with self.subTest(fault=fault), self.fixture() as (_, _, roster, epochs):
                owner = NativeObserverProcess.__new__(NativeObserverProcess)
                owner.process = (owners.FixtureProcess(999) if fault == "owner"
                                 else roster.children["native_sampler"])
                owner.samples = NativeSamples(dict(projection_id=8, projection_plan_hash="b" * 64)
                                              if fault == "identity" else IDENTITY)
                value = dict(natives.NativeSampleContinuity().sample(index=1), elapsed_ns=0)
                with patch.object(owner, "sample", return_value=value) as sample:
                    with self.assertRaisesRegex(EpochError,
                            "^campaign_epoch_clock$" if fault == "clock" else "^campaign_epoch_owner$"):
                        epochs.sample_native(owner, SECOND)
                    if fault != "clock": sample.assert_not_called()
                self.assertEqual(epochs.native_count, 1)


if __name__ == "__main__": unittest.main()
