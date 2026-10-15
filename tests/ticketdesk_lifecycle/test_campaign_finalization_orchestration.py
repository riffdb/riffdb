# req: END-003, END-007, END-008, END-009, END-010
"""Synthetic owner orchestration, never native or elapsed campaign qualification.

Call the actual finalizer with explicit original owners and terminal reports.
No sleeps, shortened offered period, replacement owners or measured receipts
are used. Native report validation has its own semantic tests.
"""
import json
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import MagicMock, Mock

from lib.ticketdesk_lifecycle_campaign_entrypoint import schedule
from lib.ticketdesk_lifecycle_campaign_finalization import CampaignFinalization
from lib.ticketdesk_lifecycle_campaign_runtime import CampaignError, SECOND
from lib.ticketdesk_lifecycle_campaign_timeline import CampaignTimeline
from test_campaign_finalization import DURATION, IDENTITY, projection, terminal


class FinalizationOrchestrationTests(unittest.TestCase):
    def fixture(self, root, *, fault=None):
        events = []
        now = DURATION * SECOND
        runtime = SimpleNamespace(root=root, artifacts=root/'artifacts', duration=DURATION,
            generation=6, version=2, incarnation=2, credential='application-v2.credential',
            identity=dict(IDENTITY), active_archive_outage=None, elapsed=lambda: now)
        runtime.remaining = lambda deadline: (deadline-now)/SECOND
        (root/'load.terminal').write_text(json.dumps(terminal())+'\n')
        (root/'load.phase').write_bytes(b'6 2 2 application-v2.credential\n')
        (root/'updates.witness').write_bytes(b'synthetic original witness; not native evidence\n')
        original = {role: SimpleNamespace(stdin=None, stdout=None, wait=Mock(return_value=0),
            terminate=Mock()) for role in ('load', 'observer', 'native_sampler', 'server')}
        runtime.children = dict(original)
        runtime.roster = SimpleNamespace(children=dict(original), sample=Mock(return_value={'fixture': True}))

        def retire(role, child, *, expected_exit):
            self.assertIs(child, original[role])
            self.assertIs(runtime.roster.children[role], child)
            self.assertEqual(expected_exit, 0)
            events.append('retire:'+role)
            if fault != 'retained_owner' or role != 'server':
                del runtime.roster.children[role]
        runtime.roster.retire = retire
        stopped = dict(schema='riffdb.ticketdesk-lifecycle-load-stopped/v1',
            completed_updates=DURATION*100, completed_reads=DURATION*400,
            post_duration_updates=int(fault == 'post_duration_work'), post_duration_reads=0)
        runtime.outputs = {role: SimpleNamespace(join=Mock()) for role in ('load', 'observer', 'server')}
        runtime.outputs['load'].terminal = stopped
        runtime.outputs['load'].parser = SimpleNamespace(complete=fault != 'incomplete_load')

        def load_exit(*, timeout):
            self.assertGreater(timeout, 0)
            self.assertEqual((root/'load.terminal-release').read_bytes(),
                b'6 2 2 application-v2.credential\n')
            self.assertEqual(events, ['replay', 'projection', 'finish_timeline'])
            events.append('exit:load')
            return 0
        original['load'].wait.side_effect = load_exit
        endpoint = dict(history_incarnation=2, sampled_head=3000000,
            outbox_backlog=0, outbox_degraded=False, projection_backlog=0)
        runtime.native = SimpleNamespace(process=original['native_sampler'],
            sample=Mock(return_value=endpoint), close=Mock(side_effect=lambda: events.append('close:sampler')))
        completed = DURATION*100
        observed = dict(events_checked=1024+completed//65536*1024+min(completed%65536,1024),
            duplicates=0, release_generation=6, history_incarnation=2,
            observer_quiesced=False, consumer_paused=False, live_head=3000000,
            live_updates=1, live_renewals=1)
        runtime.observer = Mock(return_value=observed)
        runtime.epochs = MagicMock()
        runtime.epochs.native_last = dict(sampled_head=2999999)
        runtime.epochs.finish.return_value = {'synthetic_components': True}
        runtime.memory = MagicMock()
        runtime.tick = Mock()
        runtime.worker_environment = lambda **values: dict(values)
        runtime.close_extent_incarnation = Mock()
        retired = [2, 3] if fault == 'too_few_retirements' else [2, 3, 4]
        extents = [dict(history_incarnation=1),
            dict(history_incarnation=2, observed_native_retired_generations=retired)]
        runtime.reconcile_extent_incarnations = Mock(return_value=extents)
        archive_fd = os.open(root/'synthetic-archive', os.O_WRONLY|os.O_CREAT|os.O_EXCL, 0o600)
        runtime.disk = SimpleNamespace(poll=Mock(return_value={'synthetic_disk': True}),
            limits={'control_and_evidence_bytes': 4096},
            _snapshot=Mock(return_value=({'control_and_evidence_bytes': 1}, {}, {})),
            output=Mock(), archive=archive_fd)
        self.addCleanup(lambda: self.close_if_open(runtime.disk.archive))

        def state_worker(executable, kind, environment, **options):
            self.assertEqual(executable, str(root/'artifacts/lifecycle'))
            self.assertEqual(kind, 'reconcile')
            self.assertEqual(environment, dict(RIFFDB_LIFECYCLE_COMPLETED_COMMANDS=str(completed),
                RIFFDB_LIFECYCLE_RECONCILIATION_WITNESS=str(root/'updates.witness')))
            self.assertNotIn('load', runtime.children)
            self.assertNotIn('load', runtime.roster.children)
            self.assertTrue((root/'updates.witness').is_file())
            events.append('reconcile:state')
            return dict(schema='riffdb.ticketdesk-lifecycle-state-reconciliation/v1',
                tickets_checked=65536, updates=completed-int(fault == 'state_count'), max_in_flight=16)

        def archive_worker(executable, environment, **options):
            self.assertEqual(executable, str(root/'artifacts/lifecycle'))
            self.assertEqual(environment, {})
            self.assertEqual(runtime.children, {})
            self.assertEqual(runtime.roster.children, {})
            self.assertEqual(options['duration_s'], DURATION)
            self.assertEqual(options['elapsed_ns'], now)
            self.assertEqual(options['timeout_s'], 900)
            events.append('reconcile:archive')
            return dict(schema='riffdb.ticketdesk-lifecycle-archive-reconciliation/v1',
                records_checked=65536+completed-int(fault == 'archive_count'),
                encoded_bytes=1, physical_bytes=1, payloads_reconciled=True, policy_anchors_reconciled=True)
        runtime.operator = SimpleNamespace(run_worker=Mock(side_effect=state_worker),
            run_final_archive=Mock(side_effect=archive_worker))
        timeline = CampaignTimeline(schedule(), duration_s=DURATION)
        for plan in schedule():
            start = plan['start_s']*SECOND
            timeline.start(plan['kind'], start)
            timeline.release(plan['kind'], start+plan['duration_s']*SECOND)
            timeline.finish(plan['kind'], start+(plan['duration_s']+1)*SECOND, reconciled=True)
        finish_timeline = timeline.finish_campaign

        def finish(elapsed):
            events.append('finish_timeline')
            finish_timeline(elapsed)
        timeline.finish_campaign = finish

        def wait(condition, deadline):
            self.assertEqual(deadline, (DURATION+55)*SECOND)
            self.assertTrue(condition(), 'explicit synthetic terminal condition must already hold')

        def projection_query(command, arguments, deadline):
            self.assertEqual(command, 'projection.query')
            self.assertEqual(arguments[-6:], ['--after', '3000000', '--wait-nanos', '30000000000', '--limit', '1'])
            self.assertFalse((root/'load.terminal-release').exists())
            self.assertEqual(runtime.children, original)
            self.assertEqual(runtime.roster.children, original)
            events.append('projection')
            return projection()
        actions = SimpleNamespace(wait=wait, replay=Mock(side_effect=lambda _deadline: events.append('replay')),
            call=Mock(side_effect=projection_query), restore_receipt={'synthetic_restore': True},
            restore_clock=SimpleNamespace(complete=fault != 'missing_restore'),
            restore_read_admission={'synthetic_restore_reads': True})
        return CampaignFinalization(runtime, actions, timeline), runtime, original, events

    @staticmethod
    def close_if_open(descriptor):
        if descriptor is None: return
        try: os.close(descriptor)
        except OSError: pass

    def test_actual_finalizer_releases_original_load_then_closes_all_owners_before_offline_archive(self):
        with tempfile.TemporaryDirectory() as temporary:
            finalizer, runtime, original, events = self.fixture(Path(temporary))
            result = finalizer.finish()
            self.assertTrue(finalizer.complete)
            self.assertFalse(finalizer.failed)
            self.assertFalse(finalizer.complete_campaign_checks_proved)
            self.assertFalse(result['qualifies_complete_campaign'])
            self.assertEqual(result['terminal'], terminal())
            self.assertEqual(result['actions'], finalizer.timeline.actions)
            self.assertEqual(len(result['actions']), 7)
            self.assertEqual(events, ['replay', 'projection', 'finish_timeline', 'exit:load',
                'retire:load', 'reconcile:state', 'retire:observer', 'close:sampler',
                'retire:native_sampler', 'retire:server', 'reconcile:archive'])
            self.assertEqual(runtime.children, {})
            self.assertEqual(runtime.roster.children, {})
            self.assertEqual(runtime.elapsed(), DURATION*SECOND)
            self.assertIsNone(runtime.native)
            self.assertIsNone(runtime.disk.archive)
            self.assertIsNone(runtime.disk.output)
            self.assertTrue((runtime.root/'campaign-post-validation-disk.json').is_file())
            original['server'].terminate.assert_called_once_with()
            runtime.memory.__exit__.assert_called_once_with(None, None, None)
            runtime.epochs.__exit__.assert_called_once_with(None, None, None)
            with self.assertRaisesRegex(CampaignError, '^campaign_finalization_closed$'):
                finalizer.finish()

    def test_terminal_protocol_or_state_failure_prevents_offline_archive_and_success(self):
        cases = {'post_duration_work': 'campaign_terminal_load',
            'incomplete_load': 'campaign_terminal_load',
            'state_count': 'campaign_state_reconciliation',
            'retained_owner': 'campaign_archive_not_offline'}
        for fault, code in cases.items():
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                finalizer, runtime, _, _ = self.fixture(Path(temporary), fault=fault)
                with self.assertRaisesRegex(CampaignError, '^'+code+'$'):
                    finalizer.finish()
                self.assertTrue(finalizer.failed)
                self.assertFalse(finalizer.complete)
                runtime.operator.run_final_archive.assert_not_called()
                self.assertFalse((runtime.root/'campaign-post-validation-disk.json').exists())
                with self.assertRaisesRegex(CampaignError, '^campaign_finalization_closed$'):
                    finalizer.finish()

    def test_archive_retirement_or_restore_proof_failure_never_returns_a_completed_component(self):
        cases = {'archive_count': 'campaign_archive_reconciliation',
            'too_few_retirements': 'campaign_measured_retirement_gate',
            'missing_restore': 'campaign_restore_clock_incomplete'}
        for fault, code in cases.items():
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                finalizer, runtime, _, _ = self.fixture(Path(temporary), fault=fault)
                with self.assertRaisesRegex(CampaignError, '^'+code+'$'):
                    finalizer.finish()
                self.assertTrue(finalizer.failed)
                self.assertFalse(finalizer.complete)
                runtime.operator.run_final_archive.assert_called_once()
                self.assertEqual(runtime.children, {})
                self.assertFalse(finalizer.complete_campaign_checks_proved)
