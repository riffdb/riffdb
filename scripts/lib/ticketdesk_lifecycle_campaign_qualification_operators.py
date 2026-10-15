# req: END-003, END-007, END-008, END-009, END-010
"""Re-read original public and native worker witnesses from the closed ledger."""
from collections import Counter
import hashlib
import os
import stat

from .ticketdesk_lifecycle_campaign_actions import REPLAY_FIELDS
from .ticketdesk_lifecycle_campaign_finalization import checked_projection
from .ticketdesk_lifecycle_campaign_operator import CampaignOperator
from .ticketdesk_lifecycle_campaign_owner_reconciliation import read_private, decode, operators
from .ticketdesk_lifecycle_campaign_qualification_prerequisites import closed_inputs, exact, require

SECOND = 10**9


def original_witness(root, duration_s):
    descriptor = os.open(root/'updates.witness', os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor, 'rb') as source:
        before = os.fstat(source.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_uid == os.geteuid() and before.st_nlink == 1
            and stat.S_IMODE(before.st_mode) == 0o600 and before.st_size == 16+32*duration_s*100,
            'qualification_original_witness')
        prefix = source.read(48)
        source.seek(0); checksum = hashlib.file_digest(source, 'sha256').hexdigest()
        after = os.fstat(source.fileno())
    identity = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    require(identity(before) == identity(after) == identity((root/'updates.witness').lstat())
        and prefix[:16] == b'RDBTW001'+(duration_s*100).to_bytes(8, 'big')
        and int.from_bytes(prefix[16:24], 'big') == 0
        and int.from_bytes(prefix[32:40], 'big') == int.from_bytes(prefix[40:48], 'big') == 1,
        'qualification_original_witness')
    sequence = int.from_bytes(prefix[24:32], 'big')
    require(sequence > 0, 'qualification_original_witness')
    return sequence, dict(sha256=checksum, bytes=before.st_size)


@closed_inputs
def verify_operator_witnesses(root, finalization, *, duration_s, projection_identity, restore_clock):
    rows, ledger = operators(root, duration_s)
    proofs = {'operator_ledger':ledger}; observed = []; original = None
    sequence, proofs['updates_witness'] = original_witness(root, duration_s)
    raw, proofs['original_command'] = read_private(root, 'original-command.json', maximum=4096)
    exact(decode(raw), dict(idempotency_key='ticketdesk-lifecycle-update-0',
        organization_id='01900000-0000-7000-8001-000000000000',
        ticket_id='01900000-0000-7000-8004-000000000000', expected_revision=0, status='InProgress'),
        'qualification_original_command')
    raw, proofs['original_binding'] = read_private(root, 'bind-application.stdout', maximum=65536)
    binding = decode(raw)
    require(binding.get('schema') == 'riffdb.cli.output/v1' and binding.get('command') == 'role.bind'
        and binding.get('ok') is True and binding['result']['status'] == 'created',
        'qualification_original_capability')
    original_capability = binding['result']['transition']['identity']['capability_id']
    require(type(original_capability) is str and 0 < len(original_capability) <= 64,
        'qualification_original_capability')
    for row in rows:
        name = 'operator-'+str(row['index']).zfill(3)+'.stdout'
        raw, proofs[name] = read_private(root, name, maximum=1024**2)
        value = decode(raw); command = row['command']
        require(type(value) is dict, 'qualification_operator_witness')
        if command.startswith('worker.'):
            kind = command.removeprefix('worker.')
            require(row['exit_code'] == 0 and CampaignOperator._worker_result(kind, value),
                'qualification_worker_witness')
            result = value
        else:
            require(value.keys() == {'schema', 'command', 'ok', 'result' if row['exit_code'] == 0 else 'error'}
                and value['schema'] == 'riffdb.cli.output/v1' and value['command'] == command
                and value['ok'] is (row['exit_code'] == 0), 'qualification_operator_witness')
            if row['exit_code'] != 0:
                require(command in {'backup.create', 'backup.restore', 'backup.operation'},
                    'qualification_operator_witness')
                observed.append((row, value)); continue
            result = value['result']
            require(type(result) is dict, 'qualification_operator_witness')
        if command == 'command.run':
            require(result.get('status') == 'replayed' and result.get('commit_sequence') == str(sequence)
                and result.get('contract_version') == '1' and result.get('outcome_type') == 'Updated'
                and result.get('durability_mode') == 'group' and all(key in result for key in REPLAY_FIELDS)
                and all(type(result[key]) is str and 0 < len(result[key]) <= 4096
                    for key in ('plan_hash', 'provenance_uri', 'outcome_uri')),
                'qualification_original_replay')
            retained = {key:result[key] for key in REPLAY_FIELDS}
            if original is not None: exact(retained, original, 'qualification_original_replay')
            original = retained
        elif command == 'capability.revoke':
            require(result.get('status') == 'revoked'
                and result['transition']['identity']['capability_id'] == original_capability,
                'qualification_original_capability')
        elif command == 'role.bind':
            require(result.get('status') == 'created'
                and result['transition']['identity']['capability_id'] != original_capability,
                'qualification_original_capability')
        elif command == 'application.deploy':
            exact(result.get('contract_version'), '2', 'qualification_additive_deploy')
        elif command in {'event.retire', 'event.seek', 'event.nack'}:
            exact(result, {'result':'applied'}, 'qualification_consumer_handover')
        elif command == 'event.consume':
            require(type(result.get('events')) is list and len(result['events']) <= 1,
                'qualification_consumer_handover')
        elif command == 'projection.query':
            checked_projection(value, projection_identity,
                head=finalization['native_endpoint']['sampled_head'], completed=duration_s*100)
        observed.append((row, result))
    counts = Counter(row['command'] for row in rows)
    expected = {'command.run':8, 'worker.reconcile':6, 'worker.rotation-smoke':1,
        'worker.successor-authorization':1, 'worker.archive-reconcile':1, 'projection.query':1,
        'role.bind':2, 'capability.revoke':1, 'application.deploy':1,
        'event.retire':1, 'event.consume':1, 'event.seek':1, 'server.health':2,
        'backup.create':1, 'backup.restore':1}
    require(all(counts[name] == count for name, count in expected.items())
        and counts['event.nack'] <= 1 and counts['backup.operation'] <= 32
        and counts.keys() <= expected.keys()|{'event.nack', 'backup.operation'},
        'qualification_operator_inventory')
    for action in finalization['actions']:
        kind = action['kind']
        if kind in {'consumer_outage', 'sink_outage'}: continue
        start = action['start_s']*SECOND
        selected = [(row, value) for row, value in observed if start <= row['elapsed_ns'] < start+60*SECOND]
        commands = Counter(row['command'] for row, _ in selected)
        require(commands['command.run'] == (2 if kind in {'capability_rotation', 'additive_deploy'} else 1)
            and commands['worker.reconcile'] == 1, 'qualification_fault_witness')
        state = next((row, value) for row, value in selected if row['command'] == 'worker.reconcile')
        require(0 < state[1]['updates'] <= state[0]['elapsed_ns']//SECOND*100,
            'qualification_fault_state')
        if kind == 'capability_rotation':
            require(commands['worker.rotation-smoke'] == commands['capability.revoke'] == commands['role.bind'] == 1,
                'qualification_rotation_witness')
        elif kind == 'additive_deploy':
            require(commands['worker.successor-authorization'] == commands['application.deploy'] == commands['event.retire']
                == commands['event.consume'] == commands['event.seek'] == commands['role.bind'] == 1,
                'qualification_deploy_witness')
        elif kind == 'backup_restore':
            exact(state[1]['updates'], restore_clock['preparation']['acknowledged_commands'],
                'qualification_restore_state')
            state_end = state[0]['elapsed_ns']+state[0]['duration_ns']
            state_phase = next(p['completed_ns'] for p in restore_clock['phases'] if p['kind'] == 'state_reconciliation')
            require(state_end <= state_phase < restore_clock['loss_ns']+45*SECOND,
                'qualification_restore_state')
            replay = next(row for row, _ in selected if row['command'] == 'command.run')
            replay_phase = next(p['completed_ns'] for p in restore_clock['phases'] if p['kind'] == 'original_replay')
            require(replay['elapsed_ns']+replay['duration_ns'] <= replay_phase <= state_phase,
                'qualification_restore_state')
    final = [(row, value) for row, value in observed if row['elapsed_ns'] >= duration_s*SECOND]
    require([row['command'] for row, _ in final] == ['command.run', 'projection.query',
        'worker.reconcile', 'worker.archive-reconcile'], 'qualification_terminal_witness')
    exact(final[-2][1], finalization['state'], 'qualification_terminal_state')
    exact(final[-1][1], finalization['archive'], 'qualification_terminal_archive')
    exact(final[-2][1]['updates'], duration_s*100, 'qualification_terminal_state')
    exact(final[-1][1]['records_checked'], 65536+duration_s*100, 'qualification_terminal_archive')
    return dict(original_sequence=sequence, original_replay=original, original_replays=counts['command.run'],
        final_state=final[-2][1], final_archive=final[-1][1], inputs=proofs,
        qualifies_complete_campaign=False)
