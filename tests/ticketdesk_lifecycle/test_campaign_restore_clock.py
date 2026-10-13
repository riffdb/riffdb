# req: END-001, END-008
"""Synthetic clock oracles, never actual backup or campaign qualification."""
from copy import deepcopy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from lib.ticketdesk_lifecycle_campaign_maintenance import CampaignMaintenance
from lib.ticketdesk_lifecycle_campaign_restore_clock import (
    RestoreRecoveryClock, RestoreClockError, RECOVERY_PHASES, FIXED, SECOND,
)

LOSS = 5400 * SECOND
HASH = 'a' * 64


class Operator:
    last_ns = LOSS - 20 * SECOND

    def run_as(self, credential, command, arguments, **options):
        return dict(schema='riffdb.cli.output/v1', command=command, ok=True,
            result=dict(status='terminal', maintenance_operation_id='01900000-0000-7000-8000-000000000001',
                kind='create_backup', backup_name='campaign-source', input_hash='b' * 64,
                phase='succeeded'))


def prepared(planned=LOSS, *, before_acknowledgements=None):
    clock = RestoreRecoveryClock(planned)
    clock.begin_preparation(planned - 30 * SECOND)
    if before_acknowledgements is not None:
        before_acknowledgements(clock)
    clock.bind_acknowledgements(planned - 30 * SECOND, acknowledged_commands=530000,
        acknowledged_frontier=595536, acknowledgement_witness_sha256=HASH)
    operator=Operator()
    operator.last_ns=planned-20*SECOND
    owner = CampaignMaintenance(operator, 'private.credential', 'create', 'campaign-source',
        started_ns=planned - 25 * SECOND, deadline_ns=planned)
    owner.start(planned - 25 * SECOND)
    inventory = {name: dict(sha256=HASH, bytes=128) for name in FIXED}
    clock.backup_durable(planned - 10 * SECOND, owner,
        manifest_sha256=HASH, artifacts=inventory)
    return clock, owner, inventory


def lose(clock, now=LOSS, **changes):
    return clock.primary_loss(now, **dict(dict(acknowledged_commands=530000,
        acknowledgement_witness_sha256=HASH, selected_manifest_sha256=HASH), **changes))


class RestoreClockTests(unittest.TestCase):
    def test_preparation_and_durable_backup_precede_one_exact_sixty_second_clock(self):
        clock, _, inventory = prepared()
        inventory['database.redb']['sha256'] = 'c' * 64
        self.assertEqual(lose(clock), LOSS + 60 * SECOND)
        for index, name in enumerate(RECOVERY_PHASES):
            clock.phase_completed(name, LOSS + (index + 1) * 5 * SECOND)
        receipt = clock.finish(LOSS + 40 * SECOND)
        self.assertEqual(receipt['preparation']['started_ns'], LOSS - 30 * SECOND)
        self.assertEqual(receipt['backup']['durable_at_ns'], LOSS - 10 * SECOND)
        self.assertEqual(receipt['backup']['bytes'], 512)
        self.assertEqual(receipt['backup']['artifacts']['database.redb']['sha256'], HASH)
        self.assertEqual(receipt['deadline_ns'] - receipt['loss_ns'], 60 * SECOND)
        self.assertFalse(receipt['qualifies_complete_campaign'])

    def test_actual_campaign_can_reconcile_before_original_workers_acknowledge_resumption(self):
        clock, _, _ = prepared()
        lose(clock)
        order=('target_setup','target_admission','restore','readiness',
               'original_replay','state_reconciliation','first_write')
        for index,phase in enumerate(order):
            clock.phase_completed(phase,LOSS+(index+1)*SECOND)
        receipt=clock.finish(LOSS+8*SECOND)
        self.assertEqual(tuple(p['kind'] for p in receipt['phases']),order)
        self.assertEqual(receipt['deadline_ns'],LOSS+60*SECOND)

    def test_fractional_scheduled_loss_cannot_overflow_the_bounded_deadline(self):
        planned=2**64-1-60*SECOND
        clock, _, _=prepared(planned)
        with self.assertRaisesRegex(RestoreClockError,'restore_clock_number'):
            lose(clock,planned+1)
        self.assertIsNone(clock.loss_ns)
        with self.assertRaisesRegex(RestoreClockError,'restore_clock_failed'):
            clock.observe(planned+1)

    def test_target_setup_transfer_unknown_polls_readiness_and_write_share_the_deadline(self):
        for phase in RECOVERY_PHASES:
            with self.subTest(phase=phase):
                clock, _, _ = prepared()
                lose(clock)
                for earlier in RECOVERY_PHASES[:RECOVERY_PHASES.index(phase)]:
                    clock.phase_completed(earlier, LOSS + SECOND)
                self.assertEqual(clock.observe(LOSS + 59 * SECOND), SECOND)
                with self.assertRaisesRegex(RestoreClockError, 'restore_clock_deadline'):
                    clock.phase_completed(phase, LOSS + 60 * SECOND + 1)
                with self.assertRaisesRegex(RestoreClockError, 'restore_clock_failed'):
                    clock.observe(LOSS + 59 * SECOND)

    def test_missing_backup_late_preparation_and_rescheduled_loss_refuse(self):
        clock = RestoreRecoveryClock(LOSS)
        with self.assertRaises(RestoreClockError):
            lose(clock)
        for now in (LOSS - SECOND, LOSS + SECOND):
            clock, _, _ = prepared()
            with self.assertRaisesRegex(RestoreClockError, 'restore_clock_loss_schedule'):
                lose(clock, now)
        clock, _, _ = prepared()
        with self.assertRaisesRegex(RestoreClockError, 'restore_clock_loss_schedule'):
            lose(clock, LOSS + 1)
            lose(clock, LOSS + 2)

    def test_new_acknowledgement_or_changed_witness_or_selection_refuses_loss(self):
        for change in (dict(acknowledged_commands=530001),
                       dict(acknowledgement_witness_sha256='d' * 64),
                       dict(selected_manifest_sha256='d' * 64)):
            clock, _, _ = prepared()
            with self.assertRaises(RestoreClockError):
                lose(clock, **change)
            self.assertIsNone(clock.loss_ns)

    def test_backdated_clock_incomplete_reconciliation_and_reordered_success_refuse(self):
        clock, _, _ = prepared()
        lose(clock)
        with self.assertRaisesRegex(RestoreClockError, 'restore_clock_rewind'):
            clock.observe(LOSS - 1)
        clock, _, _ = prepared()
        lose(clock)
        with self.assertRaisesRegex(RestoreClockError, 'restore_clock_incomplete'):
            clock.finish(LOSS + SECOND)
        clock, _, _ = prepared()
        lose(clock)
        with self.assertRaisesRegex(RestoreClockError, 'restore_clock_phase_order'):
            clock.phase_completed('readiness', LOSS + SECOND)

    def test_durability_inventory_and_resource_bounds_cannot_be_asserted_by_a_boolean(self):
        _, owner, good = prepared()
        cases = [True, None]
        for operation in cases:
            clock = RestoreRecoveryClock(LOSS)
            clock.begin_preparation(LOSS - 30 * SECOND)
            clock.bind_acknowledgements(LOSS - 30 * SECOND, acknowledged_commands=530000,
                acknowledged_frontier=595536, acknowledgement_witness_sha256=HASH)
            with self.assertRaisesRegex(RestoreClockError, 'restore_clock_durable_operation'):
                clock.backup_durable(LOSS - 10 * SECOND, operation,
                    manifest_sha256=HASH, artifacts=good)
        for mutation in ('missing', 'name', 'checksum', 'bytes', 'manifest'):
            clock = RestoreRecoveryClock(LOSS)
            clock.begin_preparation(LOSS - 30 * SECOND)
            clock.bind_acknowledgements(LOSS - 30 * SECOND, acknowledged_commands=530000,
                acknowledged_frontier=595536, acknowledgement_witness_sha256=HASH)
            bad = deepcopy(good)
            if mutation == 'missing': del bad['journal.riffextent']
            elif mutation == 'name': bad['../escape'] = dict(sha256=HASH, bytes=1)
            elif mutation == 'checksum': bad['database.redb']['sha256'] = 'wrong'
            elif mutation == 'bytes': bad['database.redb']['bytes'] = 32 * 1024**3
            else: bad['manifest.riffdb']['sha256'] = 'c' * 64
            with self.assertRaises(RestoreClockError):
                clock.backup_durable(LOSS - 10 * SECOND, owner,
                    manifest_sha256=HASH, artifacts=bad)


if __name__ == '__main__':
    unittest.main()
