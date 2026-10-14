# req: END-001, END-007, END-008
"""Independent closed evidence checks, never a native or measured campaign."""
from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from lib.ticketdesk_lifecycle_campaign_restore_evidence import (
    FILES, RULES, RestoreEvidenceError, protocol_snapshot, validate_protocol,
    validate_clock, public_operation_witnesses, first_write_witness,
    validate_read_admission,
)
from lib.ticketdesk_lifecycle_campaign_maintenance import REQUIRED
from lib.ticketdesk_lifecycle_campaign_restore_clock import SECOND
from test_evidence import fixture, restore_clock_fixture, restore_read_admission_fixture, CHECK


def campaign_clock():
    plan = dict(kind='backup_restore', start_s=5400, duration_s=45)
    action = dict(plan, recovery_s=10, reconciled=True)
    clock = restore_clock_fixture(plan, action)
    for phase, second in zip(clock['phases'], (1, 2, 20, 21, 22, 23, 45)):
        phase['completed_ns'] = clock['loss_ns'] + second * SECOND
    return plan, action, clock


class RestoreEvidenceTests(unittest.TestCase):
    def test_read_admission_cannot_hide_a_write_gap_or_readiness_pause_cost(self):
        _, _, clock = campaign_clock()
        original = restore_read_admission_fixture(clock)
        validate_read_admission(original, clock, expected_generation=5, expected_version=2)
        for mode in ('write', 'regression', 'generation', 'version', 'early', 'late_read', 'late_pause', 'boolean'):
            value = deepcopy(original)
            if mode == 'write': value['reading_ack']['completed_commands'] += 1
            elif mode == 'regression': value['quiesced_ack']['completed_reads'] = 0
            elif mode == 'generation': value['reading_ack']['generation'] = 4
            elif mode == 'version': value['quiesced_ack']['contract_version'] = 1
            elif mode == 'early': value['requested_ns'] = clock['backup']['durable_at_ns'] - 1
            elif mode == 'late_read': value['reading_ns'] = clock['loss_ns']
            elif mode == 'late_pause': value['quiesced_ns'] = clock['loss_ns'] + 2 * SECOND
            else: value['initial']['completed_commands'] = True
            with self.subTest(mode=mode), self.assertRaises(RestoreEvidenceError):
                validate_read_admission(value, clock)

    def test_first_write_requires_retained_original_worker_acknowledgement_in_the_restored_phase(self):
        for mode in ('valid', 'no_new_ack', 'old_phase', 'late', 'boolean'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                root.chmod(0o700)
                _, _, clock = campaign_clock()
                acknowledgement = dict(schema='riffdb.ticketdesk-lifecycle-resumed/v1', generation=6,
                    contract_version=2, history_incarnation=2, completed_commands=101, completed_reads=400)
                value = dict(schema='riffdb.ticketdesk-lifecycle-restore-first-write/v1',
                    elapsed_ns=clock['phases'][-1]['completed_ns'], acknowledgement=acknowledgement)
                if mode == 'no_new_ack': acknowledgement['completed_commands'] = 100
                elif mode == 'old_phase': acknowledgement['history_incarnation'] = 1
                elif mode == 'late': value['elapsed_ns'] += SECOND
                elif mode == 'boolean': acknowledgement['completed_commands'] = True
                path = root / 'campaign-restore-first-write.json'
                path.write_text(json.dumps(value) + '\n')
                path.chmod(0o600)
                if mode == 'valid':
                    self.assertEqual(first_write_witness(root, clock)['observation'], value)
                else:
                    with self.assertRaises(RestoreEvidenceError):
                        first_write_witness(root, clock)

    def test_protocol_binds_each_source_before_offers_and_refuses_different_rules_or_methods(self):
        root = Path(__file__).resolve().parents[2]
        protocol = protocol_snapshot(root)
        validate_protocol(protocol)
        self.assertEqual(protocol['source_files'].keys(), FILES)
        for mode in ('method', 'acknowledgement_method', 'rule', 'source', 'boolean'):
            value = deepcopy(protocol)
            if mode in {'method', 'acknowledgement_method'}:
                value[mode] = 'old_combined_clock'
            elif mode == 'rule':
                value['rules']['primary_loss_limit_seconds'] = 61
            elif mode == 'source':
                value['source_files'].pop(next(iter(FILES)))
            else:
                value['rules']['primary_loss_limit_seconds'] = True
            with self.subTest(mode=mode), self.assertRaises(RestoreEvidenceError):
                validate_protocol(value)

    def test_single_clock_preserves_preparation_visibility_and_fixed_release(self):
        plan, action, clock = campaign_clock()
        self.assertEqual(validate_clock(clock, protocol_sha256='a' * 64, plan=plan, action=action),
                         (5370, 5400))
        for mode in ('method', 'hash', 'deadline', 'late_backup', 'late_ready',
                     'early_write', 'missing_phase', 'ack_count', 'hidden_pause'):
            value = deepcopy(clock)
            if mode == 'method': value['method'] = 'combined_clock_v1'
            elif mode == 'hash': value['protocol_sha256'] = 'f' * 64
            elif mode == 'deadline': value['deadline_ns'] += SECOND
            elif mode == 'late_backup': value['backup']['durable_at_ns'] = value['loss_ns']
            elif mode == 'late_ready': value['phases'][3]['completed_ns'] = value['loss_ns'] + 46 * SECOND
            elif mode == 'early_write': value['phases'][-1]['completed_ns'] -= SECOND
            elif mode == 'missing_phase': value['phases'].pop()
            elif mode == 'ack_count': value['preparation']['acknowledged_commands'] = True
            else: value['preparation']['started_ns'] -= 31 * SECOND
            with self.subTest(mode=mode), self.assertRaises(RestoreEvidenceError):
                validate_clock(value, protocol_sha256='a' * 64, plan=plan, action=action)

    def test_preparation_window_is_visible_without_excusing_an_ordinary_neighbor(self):
        manifest, receipt = fixture()
        plan = next(p for p in manifest['faults'] if p['kind'] == 'backup_restore')
        clock = receipt['restore_clock']
        clock['preparation']['started_ns'] = (plan['start_s'] - 60) * SECOND
        index = plan['start_s'] // 300 - 1
        receipt['windows'][index]['p99_us'] = 250000
        CHECK.validate(manifest, receipt)
        receipt['windows'][index - 1]['p99_us'] = 250000
        with self.assertRaisesRegex(CHECK.EvidenceError, '^service_level$'):
            CHECK.validate(manifest, receipt)

    def public_files(self, root, clock):
        rows = []
        def emit(command, seconds, *, kind=None, status=None, phase=None, unknown=False):
            index = len(rows)
            row = dict(schema='riffdb.ticketdesk-lifecycle-campaign-operator/v1', index=index,
                command=command, elapsed_ns=clock['loss_ns'] + int(seconds * SECOND),
                duration_ns=SECOND // 10, exit_code=int(unknown), outcome_known=not unknown,
                qualifies_complete_campaign=False)
            rows.append(row)
            if command.startswith('worker.'):
                return
            if command == 'server.health':
                result = dict(status='ready', last_commit_sequence=str(clock['preparation']['acknowledged_frontier']))
            else:
                result = dict(status=status, kind=kind + '_backup', backup_name='campaign-source',
                    phase=phase, input_hash=('c' if kind == 'create' else 'e') * 64,
                    maintenance_operation_id='01900000-0000-7000-8000-00000000000' +
                                             ('1' if kind == 'create' else '2'))
                self.assertEqual(result.keys(), REQUIRED)
            value = dict(schema='riffdb.cli.output/v1', command=command, ok=not unknown)
            if unknown:
                value['error'] = dict(type='public', code='storage_unavailable',
                    message='storage is temporarily unavailable', recovery_action='retry')
            else:
                value['result'] = result
            path = root / ('operator-' + str(index).zfill(3) + '.stdout')
            path.write_text(json.dumps(value) + '\n')
            path.chmod(0o600)
        emit('server.health', -28)
        emit('backup.create', -25, kind='create', status='accepted', phase='draining')
        emit('backup.operation', -15, kind='create', status='found', phase='succeeded')
        emit('server.health', -14)
        emit('backup.restore', 2.1, kind='restore', status='accepted', phase='draining')
        emit('backup.operation', 10, kind='restore', status='found', phase='offline', unknown=True)
        emit('backup.operation', 19.8, kind='restore', status='found', phase='succeeded')
        emit('worker.reconcile', 28801)
        emit('worker.archive-reconcile', 28802)
        return rows

    def test_claimed_backup_and_restore_require_original_public_terminal_operation_outputs(self):
        for mode in ('valid', 'unknown_never_succeeded', 'different_id', 'frontier', 'late_create'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                root.chmod(0o700)
                _, _, clock = campaign_clock()
                rows = self.public_files(root, clock)
                if mode == 'late_create':
                    rows[1]['elapsed_ns'] = clock['loss_ns']
                elif mode in {'unknown_never_succeeded', 'different_id', 'frontier'}:
                    index = 3 if mode == 'frontier' else 6
                    path = root / f'operator-{index:03}.stdout'
                    value = json.loads(path.read_text())
                    if mode == 'frontier': value['result']['last_commit_sequence'] = '1'
                    elif mode == 'different_id': value['result']['maintenance_operation_id'] = clock['backup']['maintenance_operation_id']
                    else: value['result']['phase'] = 'validating'
                    path.write_text(json.dumps(value) + '\n')
                path = root / 'campaign-operator.jsonl'
                path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
                path.chmod(0o600)
                if mode == 'valid':
                    proof = public_operation_witnesses(root, clock, duration_s=28800)
                    self.assertTrue(all(o['complete'] for o in proof['operations'].values()))
                    self.assertEqual(proof['operations']['restore']['polls'], 2)
                else:
                    with self.assertRaises(ValueError):
                        public_operation_witnesses(root, clock, duration_s=28800)


if __name__ == '__main__':
    unittest.main()
