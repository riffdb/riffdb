# req: END-001, END-008, REC-001
"""Controller refusal tests; these do not prove native recovery or campaign timing."""
from pathlib import Path
from types import SimpleNamespace
import sys
import tempfile
import unittest
from unittest.mock import Mock
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from lib.ticketdesk_lifecycle_campaign_actions import CampaignActions
from lib.ticketdesk_lifecycle_campaign_runtime import CampaignError, SECOND
from test_campaign_restore_clock import prepared, lose, LOSS
from test_campaign_restore_inputs import record
import test_campaign_quiescence as handover
from lib.ticketdesk_lifecycle_campaign_quiescence import QuiescentRelease
from lib.ticketdesk_lifecycle_campaign_maintenance import CampaignMaintenance
from lib.ticketdesk_lifecycle_campaign_restore_clock import FIXED


class RestorePreparationTests(unittest.TestCase):
    def test_actual_preparation_binds_quiescence_public_frontier_operation_and_original_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            now = SimpleNamespace(value=LOSS - 60 * SECOND)
            runtime = SimpleNamespace(root=root, version=2, incarnation=1, generation=5, duration=28800,
                faults=[dict(kind='backup_restore', start_s=5400, duration_s=45)],
                elapsed=lambda: now.value, tick=Mock(), stop_sampler=Mock(), stop_server=Mock())
            runtime.remaining = lambda deadline: (deadline - now.value) / SECOND
            with (root / 'updates.witness').open('wb') as out:
                out.write(b'RDBTW001' + (28800 * 100).to_bytes(8, 'big') + record(0, 65537) + record(1, 65538, 2))
                out.truncate(16 + 32 * 28800 * 100)
            actions = CampaignActions(runtime, None)
            def quiesce(started, deadline, *, restore_clock):
                release = QuiescentRelease(root, generation=5, version=2, incarnation=1,
                                           restore_clock=restore_clock)
                release.begin(started)
                handover.WorkerHandover().acknowledgements(root, generation=5, version=2)
                path = root / 'load.quiesced'
                value = json.loads(path.read_text())
                value['completed_commands'] = 2
                path.write_text(json.dumps(value))
                now.value += SECOND // 10
                release.poll(now.value)
                return release
            def health(*_args):
                now.value += SECOND // 10
                return dict(ok=True, result=dict(status='ready', active_contract_version='2',
                    history_incarnation='1', last_commit_sequence='65538'))
            def create(kind, started, deadline):
                self.assertEqual(kind, 'create')
                path = root / 'backups/campaign-source'
                path.mkdir(parents=True)
                for name in FIXED:
                    (path / name).write_bytes(b'controller fixture only\n')
                class Operator:
                    last_ns = started
                    def run_as(self, _credential, command, _arguments, **_options):
                        now.value = self.last_ns = started + 10 * SECOND
                        return dict(schema='riffdb.cli.output/v1', command=command, ok=True,
                            result=dict(status='terminal', kind='create_backup', backup_name='campaign-source',
                                maintenance_operation_id='01900000-0000-7000-8000-000000000001',
                                input_hash='a' * 64, phase='succeeded'))
                owner = CampaignMaintenance(Operator(), 'fixture.credential', kind, 'campaign-source',
                                            started_ns=started, deadline_ns=deadline)
                owner.start(started)
                return owner
            actions.quiesce, actions.call, actions.maintenance = quiesce, health, create
            try:
                self.assertTrue(actions.prepare_restore_if_due(now.value))
                self.assertEqual(actions.restore_clock.preparation['started_ns'], LOSS - 60 * SECOND)
                self.assertEqual(actions.restore_clock.preparation['acknowledged_frontier'], 65538)
                self.assertEqual(actions.restore_clock.preparation['acknowledged_commands'], 2)
                self.assertEqual(actions.restore_clock.state, 'backup_durable')
                self.assertLess(actions.restore_clock.backup['durable_at_ns'], LOSS)
                self.assertTrue((root / 'load.quiesce').exists())
                self.assertTrue((root / 'campaign-restore-preparation.json').is_file())
                runtime.stop_sampler.assert_not_called()
                runtime.stop_server.assert_not_called()
                now.value = LOSS
                self.assertFalse(actions.prepare_restore_if_due(now.value))
            finally:
                actions.close()

    def runtime(self, root):
        for name in ('data', 'projections'):
            (root / name).mkdir()
        return SimpleNamespace(root=root, incarnation=1, version=2,
            identity={}, server_log=root / 'server.stderr', epochs=Mock(),
            elapsed=lambda: LOSS, remaining=lambda _deadline: 60.0,
            stop_sampler=Mock(), stop_server=Mock(), start_server=Mock(),
            close_extent_incarnation=Mock(), begin_extent_incarnation=Mock(),
            wait_recovery_admission=Mock(), start_sampler=Mock(), tick=Mock())

    def test_missing_prefault_backup_refuses_before_sampler_stop_or_postloss_creation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = self.runtime(root)
            actions = CampaignActions(runtime, None)
            actions.maintenance = Mock()
            with self.assertRaisesRegex(CampaignError, '^campaign_restore_backup_required$'):
                actions.replace('backup_restore', 5400 * SECOND, 5460 * SECOND, None)
            runtime.stop_sampler.assert_not_called()
            actions.maintenance.assert_not_called()
            self.assertTrue((root / 'data').is_dir())
            self.assertFalse((root / 'retained-pre-restore-data').exists())

    def test_public_restore_uses_selected_backup_without_creating_another_after_loss(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = self.runtime(Path(temporary))
            actions = CampaignActions(runtime, None)
            actions.maintenance = Mock()
            actions.restore_clock, _, _ = prepared()
            deadline = lose(actions.restore_clock)
            actions.replace('backup_restore', LOSS, deadline, None)
            actions.maintenance.assert_called_once_with('restore', LOSS, deadline)
            self.assertEqual(runtime.incarnation, 2)
            self.assertTrue((runtime.root / 'retained-pre-restore-data').is_dir())

    def test_postloss_action_cannot_reset_or_replace_the_selected_recovery_clock(self):
        for state in ('not_lost', 'different_start', 'different_deadline'):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as temporary:
                runtime = self.runtime(Path(temporary))
                actions = CampaignActions(runtime, None)
                actions.maintenance = Mock()
                actions.restore_clock, _, _ = prepared()
                deadline = LOSS + 60 * SECOND
                if state != 'not_lost':
                    lose(actions.restore_clock)
                started = LOSS + int(state == 'different_start')
                deadline += int(state == 'different_deadline')
                with self.assertRaisesRegex(CampaignError, '^campaign_restore_backup_required$'):
                    actions.replace('backup_restore', started, deadline, None)
                runtime.stop_sampler.assert_not_called()
                actions.maintenance.assert_not_called()


if __name__ == '__main__':
    unittest.main()
