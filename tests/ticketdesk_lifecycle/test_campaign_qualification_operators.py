# req: END-003, END-007, END-008, END-009, END-010
"""Original operator-ledger fixtures, never real workload or campaign proof."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from lib import ticketdesk_lifecycle_campaign_qualification_operators as qualification
from lib.ticketdesk_lifecycle_campaign_entrypoint import schedule
from lib.ticketdesk_lifecycle_campaign_qualification_prerequisites import PrerequisiteError
from test_campaign_finalization import DURATION, IDENTITY, projection

SECOND = 10**9
ORIGINAL = '01900000-0000-7000-8000-000000000001'
REPLACEMENT = '01900000-0000-7000-8000-000000000002'


def fixture(root, *, fault=None):
    def write(name, value):
        path = root/name; path.write_text(json.dumps(value)+'\n'); path.chmod(0o600)
    def public(command, result):
        return dict(schema='riffdb.cli.output/v1', command=command, ok=True, result=result)
    def binding(capability):
        return dict(status='created', transition=dict(identity=dict(capability_id=capability, revision='1'),
            administration_sequence='1'), credential_retained=True)
    write('bind-application.stdout', public('role.bind', binding(ORIGINAL)))
    write('original-command.json', dict(idempotency_key='ticketdesk-lifecycle-update-0',
        organization_id='01900000-0000-7000-8001-000000000000',
        ticket_id='01900000-0000-7000-8004-000000000000', expected_revision=0, status='InProgress'))
    replay = dict(status='replayed', commit_sequence='65537', contract_version='1', plan_hash='a'*64,
        outcome_type='Updated', outcome={'status':'InProgress'}, provenance_uri='synthetic-provenance',
        outcome_uri='synthetic-outcome', durability_mode='group')
    state = lambda count: dict(schema='riffdb.ticketdesk-lifecycle-state-reconciliation/v1',
        tickets_checked=65536, updates=count, max_in_flight=16)
    rotation = dict(schema='riffdb.ticketdesk-lifecycle-rotation-smoke/v1', revoked_reads_refused=True,
        revoked_replay_refused=True, same_principal_replay=True, state_reconciled=True)
    archive = dict(schema='riffdb.ticketdesk-lifecycle-archive-reconciliation/v1', records_checked=65536+DURATION*100,
        encoded_bytes=1, physical_bytes=1, payloads_reconciled=True, policy_anchors_reconciled=True)
    pending = []
    def add(second, command, result):
        pending.append((second*SECOND, command, result if command.startswith('worker.') else public(command, result)))
    def replay_at(second): add(second, 'command.run', deepcopy(replay))
    for plan in schedule():
        kind, start = plan['kind'], plan['start_s']
        if kind in {'consumer_outage', 'sink_outage'}: continue
        if kind in {'capability_rotation', 'additive_deploy'}: replay_at(start+1)
        if kind == 'capability_rotation':
            add(start+2, 'role.bind', binding(REPLACEMENT))
            add(start+3, 'capability.revoke', dict(status='revoked',
                transition=dict(identity=dict(capability_id=ORIGINAL, revision='2'), administration_sequence='3')))
            add(start+4, 'worker.rotation-smoke', rotation)
        elif kind == 'additive_deploy':
            add(start+2, 'event.retire', {'result':'applied'})
            add(start+3, 'application.deploy', {'contract_version':'2'})
            add(start+4, 'role.bind', binding(REPLACEMENT))
            add(start+5, 'event.consume', {'events':[]})
            add(start+6, 'event.seek', {'result':'applied'})
            add(start+7, 'worker.successor-authorization',
                dict(schema='riffdb.ticketdesk-lifecycle-successor-authorization/v1',
                    contract_version=2, ungranted_command_refused=True))
        if kind == 'backup_restore': add(start+1, 'backup.restore', {'synthetic_public_operation':True})
        replay_at(start+8)
        add(start+9, 'worker.reconcile', state(534000 if kind == 'backup_restore' else start*100))
    add(5341, 'server.health', {'synthetic_source_frontier':True})
    add(5342, 'backup.create', {'synthetic_public_operation':True})
    add(5343, 'server.health', {'synthetic_source_frontier':True})
    replay_at(DURATION+1)
    pending.append(((DURATION+2)*SECOND, 'projection.query', projection()))
    add(DURATION+3, 'worker.reconcile', state(DURATION*100))
    add(DURATION+4, 'worker.archive-reconcile', archive)
    pending.sort(key=lambda item:item[0])
    if fault == 'missing_replay': pending.pop(next(i for i, row in enumerate(pending) if row[1] == 'command.run'))
    rows = []
    for index, (elapsed, command, value) in enumerate(pending):
        if command == 'command.run' and elapsed > 4800*SECOND:
            if fault == 'replay_sequence': value['result']['commit_sequence'] = '65538'
            if fault == 'replay_provenance': value['result']['provenance_uri'] = 'mixed-original-provenance'
            if fault == 'durability': value['result']['durability_mode'] = 'memory'
        if command == 'capability.revoke' and fault == 'revoke_identity':
            value['result']['transition']['identity']['capability_id'] = REPLACEMENT
        if command == 'worker.rotation-smoke' and fault == 'rotation_refusal': value['revoked_reads_refused'] = False
        if command == 'worker.reconcile' and elapsed > DURATION*SECOND and fault == 'terminal_state': value['updates'] -= 1
        if command == 'worker.archive-reconcile' and fault == 'terminal_archive': value['records_checked'] -= 1
        write(f'operator-{index:03}.stdout', value)
        rows.append(dict(schema='riffdb.ticketdesk-lifecycle-campaign-operator/v1', index=index,
            command=command, elapsed_ns=elapsed, duration_ns=1000, exit_code=0,
            outcome_known=True, qualifies_complete_campaign=False))
    ledger = root/'campaign-operator.jsonl'
    ledger.write_text(''.join(json.dumps(row)+'\n' for row in rows)); ledger.chmod(0o600)
    actions = [dict(plan, recovery_s=1, reconciled=True) for plan in schedule()]
    finalization = dict(actions=actions, native_endpoint={'sampled_head':3000000},
        state=state(DURATION*100), archive=dict(archive))
    clock = dict(preparation={'acknowledged_commands':534000}, loss_ns=5400*SECOND,
        phases=[dict(kind='original_replay', completed_ns=5409*SECOND),
            dict(kind='state_reconciliation', completed_ns=5410*SECOND)])
    return finalization, clock, rows


class QualificationOperatorTests(unittest.TestCase):
    def check(self, root, final, clock):
        # This test isolates actual operator decoding, phase joins and public
        # witnesses. Actual bounded ACK-file reading is covered separately.
        with patch.object(qualification, 'original_witness', return_value=(65537, {'sha256':'a'*64, 'bytes':48})):
            return qualification.verify_operator_witnesses(root, final, duration_s=DURATION,
                projection_identity=IDENTITY, restore_clock=clock)

    def test_exact_original_public_outputs_cover_every_phase_and_final_state_archive(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); final, clock, rows = fixture(root)
            result = self.check(root, final, clock)
            self.assertEqual(result['original_replays'], 8)
            self.assertEqual(result['original_sequence'], 65537)
            self.assertEqual(result['final_state']['updates'], DURATION*100)
            self.assertEqual(result['final_archive']['records_checked'], 65536+DURATION*100)
            self.assertEqual(len(result['inputs']), len(rows)+4)
            self.assertFalse(result['qualifies_complete_campaign'])

    def test_missing_mixed_nondurable_or_refused_original_witness_never_becomes_success(self):
        for fault in ('missing_replay', 'replay_sequence', 'replay_provenance', 'durability',
                'revoke_identity', 'rotation_refusal', 'terminal_state', 'terminal_archive'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary); final, clock, _ = fixture(root, fault=fault)
                with self.assertRaises(PrerequisiteError): self.check(root, final, clock)

    def test_missing_duplicate_or_changed_original_operator_bytes_fail_closed(self):
        for fault in ('missing', 'duplicate', 'late_restore'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary); final, clock, rows = fixture(root)
                if fault == 'missing': (root/'operator-000.stdout').unlink()
                elif fault == 'duplicate': (root/'operator-000.stdout').write_bytes(b'{"ok":true,"ok":true}\n')
                else: clock['phases'][-1]['completed_ns'] = 5409*SECOND
                with self.assertRaises(PrerequisiteError): self.check(root, final, clock)

    def test_actual_original_acknowledgement_header_and_bytes_are_bound_without_a_large_fixture(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); path = root/'updates.witness'
            raw = b'RDBTW001'+(100).to_bytes(8, 'big')
            raw += b''.join(value.to_bytes(8, 'big') for value in (0, 65537, 1, 1))
            path.write_bytes(raw+b'\0'*(16+3200-len(raw))); path.chmod(0o600)
            sequence, proof = qualification.original_witness(root, 1)
            self.assertEqual(sequence, 65537); self.assertEqual(proof['bytes'], 3216)
            path.write_bytes(path.read_bytes()[:-1])
            with self.assertRaises(PrerequisiteError): qualification.original_witness(root, 1)
