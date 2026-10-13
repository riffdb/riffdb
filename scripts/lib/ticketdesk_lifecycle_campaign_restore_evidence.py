# req: END-001, END-007, END-008
"""Freeze and independently check the accepted primary-loss evidence method.

No previous combined-clock attempt is converted into a result under this
protocol. Preparation remains part of offered workload and resource evidence.
"""
import hashlib
import os
from pathlib import Path
import re
import stat

from .ticketdesk_lifecycle_campaign_restore_clock import (
    SECOND, RECOVERY_PHASE_ORDERS, artifact_binding,
)
from .ticketdesk_lifecycle_campaign_maintenance import UUID7, REQUIRED, PHASES, MAX_POLLS
from .ticketdesk_lifecycle_campaign_owner_reconciliation import read_private, decode, operators
from .ticketdesk_lifecycle_campaign_operator import CampaignOperator
from .ticketdesk_lifecycle_campaign_quiescence import RESUMED_KEYS

RULES = dict(primary_loss_limit_seconds=60, preparation_limit_seconds=60,
    backup_before_loss=True, unchanged_fault_placement=True,
    acknowledgement_gap_allowed=False, exact_replay_and_state_inside_deadline=True,
    preparation_windows_visible=True, other_fault_clocks_unchanged=True)
FILES = frozenset({'scripts/check-ticketdesk-lifecycle',
    'adr/0292-recovery-clock-begins-at-primary-loss.md'} | {
    'scripts/lib/ticketdesk_lifecycle_campaign_' + name + '.py' for name in (
        'actions', 'execution', 'quiescence', 'maintenance', 'entrypoint',
        'finalization', 'reconciliation', 'restore_clock', 'restore_inputs', 'restore_evidence')})


class RestoreEvidenceError(ValueError):
    """Closed refusal without private evidence contents."""


def require(condition, code):
    if not condition:
        raise RestoreEvidenceError(code)


def shape(value, keys):
    require(type(value) is dict and value.keys() == set(keys), 'restore_evidence_shape')


def number(value):
    require(type(value) is int and 0 <= value < 2**64, 'restore_evidence_number')
    return value


def digest(value):
    require(type(value) is str and re.fullmatch(r'[0-9a-f]{64}', value) is not None,
            'restore_evidence_digest')
    return value


def protocol_snapshot(repository):
    sources = {}
    for name in sorted(FILES):
        descriptor = os.open(Path(repository) / name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, 'rb') as source:
            before = os.fstat(source.fileno())
            require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= 1024**2,
                    'restore_protocol_source')
            encoded = source.read(1024**2 + 1)
            after = os.fstat(source.fileno())
        identity = lambda info: (info.st_dev, info.st_ino, info.st_size,
                                 info.st_mtime_ns, info.st_ctime_ns)
        require(len(encoded) == before.st_size and identity(before) == identity(after)
                == identity((Path(repository) / name).lstat()), 'restore_protocol_source_changed')
        sources[name] = hashlib.sha256(encoded).hexdigest()
    return dict(schema='riffdb.ticketdesk-lifecycle-restore-protocol/v1',
        method='primary_loss_v1', acknowledgement_method='acknowledged_prefix_v1',
        rules=dict(RULES), source_files=sources)


def validate_protocol(value):
    shape(value, {'schema', 'method', 'acknowledgement_method', 'rules', 'source_files'})
    require(value['schema'] == 'riffdb.ticketdesk-lifecycle-restore-protocol/v1'
            and value['method'] == 'primary_loss_v1'
            and value['acknowledgement_method'] == 'acknowledged_prefix_v1',
            'restore_evidence_method')
    shape(value['rules'], RULES)
    require(all(type(value['rules'][key]) is type(expected)
                and value['rules'][key] == expected for key, expected in RULES.items()),
            'restore_evidence_rules')
    shape(value['source_files'], FILES)
    for checksum in value['source_files'].values():
        digest(checksum)


def validate_clock(value, *, protocol_sha256, plan, action):
    shape(value, {'schema', 'method', 'acknowledgement_method', 'protocol_sha256',
        'planned_loss_ns', 'preparation', 'backup', 'loss_ns', 'deadline_ns',
        'completed_ns', 'phases', 'qualifies_complete_campaign'})
    require(value['schema'] == 'riffdb.ticketdesk-lifecycle-restore-clock/v2'
            and value['method'] == 'primary_loss_v1'
            and value['acknowledgement_method'] == 'acknowledged_prefix_v1'
            and value['qualifies_complete_campaign'] is False
            and digest(value['protocol_sha256']) == digest(protocol_sha256), 'restore_evidence_method')
    planned, loss = number(value['planned_loss_ns']), number(value['loss_ns'])
    deadline, completed = number(value['deadline_ns']), number(value['completed_ns'])
    require(plan['kind'] == action['kind'] == 'backup_restore'
            and planned == number(plan['start_s']) * SECOND
            and planned <= loss < planned + SECOND and deadline == loss + 60 * SECOND
            and loss <= completed <= deadline
            and completed <= (action['start_s'] + action['duration_s'] + action['recovery_s']) * SECOND,
            'restore_evidence_clock')
    preparation, backup = value['preparation'], value['backup']
    shape(preparation, {'started_ns', 'acknowledgements_bound_ns', 'acknowledged_commands',
        'acknowledged_frontier', 'acknowledgement_witness_sha256'})
    start = number(preparation['started_ns'])
    bound = number(preparation['acknowledgements_bound_ns'])
    require(planned - 60 * SECOND <= start <= bound < planned
            and 0 < number(preparation['acknowledged_commands']) <= bound // SECOND * 100
            and number(preparation['acknowledged_frontier']) >= preparation['acknowledged_commands'],
            'restore_evidence_preparation')
    digest(preparation['acknowledgement_witness_sha256'])
    shape(backup, {'name', 'maintenance_operation_id', 'input_hash', 'manifest_sha256',
        'operation_started_ns', 'operation_completed_ns', 'operation_deadline_ns',
        'artifacts', 'bytes', 'durable_at_ns'})
    require(backup['name'] == 'campaign-source'
            and type(backup['maintenance_operation_id']) is str
            and re.fullmatch(UUID7, backup['maintenance_operation_id']) is not None,
            'restore_evidence_operation')
    digest(backup['input_hash'])
    require(bound <= number(backup['operation_started_ns'])
            <= number(backup['operation_completed_ns']) <= number(backup['durable_at_ns']) < planned
            and backup['operation_started_ns'] < number(backup['operation_deadline_ns']) <= planned,
            'restore_evidence_backup_before_loss')
    try:
        _, size = artifact_binding(backup['artifacts'], backup['manifest_sha256'])
    except (ValueError, TypeError) as error:
        raise RestoreEvidenceError('restore_evidence_backup') from error
    require(number(backup['bytes']) == size, 'restore_evidence_backup')
    phases = value['phases']
    require(type(phases) is list and len(phases) == 7, 'restore_evidence_phases')
    previous = loss
    names = []
    for phase in phases:
        shape(phase, {'kind', 'completed_ns'})
        instant = number(phase['completed_ns'])
        require(previous <= instant <= completed, 'restore_evidence_phases')
        names.append(phase['kind'])
        previous = instant
        if phase['kind'] == 'first_write':
            require(instant >= loss + number(plan['duration_s']) * SECOND,
                    'restore_evidence_release')
        else:
            require(instant < loss + plan['duration_s'] * SECOND, 'restore_evidence_release')
    require(tuple(names) == RECOVERY_PHASE_ORDERS[1], 'restore_evidence_phases')
    return start // SECOND, planned // SECOND


def public_operation_witnesses(root, clock, *, duration_s):
    """Re-read the actual serialized public requests and terminal observations."""
    rows, journal = operators(root, duration_s)
    inputs, operations, health = {}, {}, []
    current = None
    observer = object.__new__(CampaignOperator)
    restore_end = next(p['completed_ns'] for p in clock['phases'] if p['kind'] == 'restore')
    for row in rows:
        command = row['command']
        if command not in {'backup.create', 'backup.restore', 'backup.operation', 'server.health'}:
            continue
        name = 'operator-' + str(row['index']).zfill(3) + '.stdout'
        raw, inputs[name] = read_private(root, name, maximum=1024**2)
        value = decode(raw)
        shape(value, {'schema', 'command', 'ok', 'result' if row['exit_code'] == 0 else 'error'})
        require(value['schema'] == 'riffdb.cli.output/v1' and value['command'] == command
                and value['ok'] is (row['exit_code'] == 0), 'restore_evidence_public_output')
        end = row['elapsed_ns'] + row['duration_ns']
        if command == 'server.health':
            result = value.get('result')
            require(type(result) is dict and result.get('status') == 'ready'
                    and result.get('last_commit_sequence')
                    == str(clock['preparation']['acknowledged_frontier'])
                    and end < clock['loss_ns'], 'restore_evidence_source_frontier')
            health.append((row['elapsed_ns'], end))
            continue
        if command != 'backup.operation':
            kind = command.split('.')[1]
            require(kind not in operations and (current is None or operations[current]['complete']),
                    'restore_evidence_public_mutation')
            current = kind
            operations[kind] = dict(operation=None, input_hash=None, phase=None, polls=0,
                complete=False, started_ns=row['elapsed_ns'], completed_ns=None)
        require(current is not None, 'restore_evidence_public_poll')
        owner = operations[current]
        if command == 'backup.operation':
            owner['polls'] += 1
            require(not owner['complete'] and owner['polls'] <= MAX_POLLS,
                    'restore_evidence_public_poll')
        if value['ok'] is False:
            if command == 'backup.operation':
                observer.pending_maintenance = {owner['operation']}
                require(observer._poll_unavailable(command,
                    ['backup', 'operation', owner['operation']], value), 'restore_evidence_public_unknown')
            else:
                require(CampaignOperator._maintenance_uncertain(command, value),
                        'restore_evidence_public_unknown')
                owner['operation'] = value['error']['maintenance_operation_id']
        else:
            result = value['result']
            shape(result, REQUIRED)
            operation, checksum, phase = result['maintenance_operation_id'], result['input_hash'], result['phase']
            require(type(operation) is str and re.fullmatch(UUID7, operation) is not None
                    and result['status'] in ({'found'} if command == 'backup.operation' else
                                             {'accepted', 'already_accepted', 'terminal'})
                    and result['kind'] == current + '_backup' and result['backup_name'] == 'campaign-source'
                    and type(phase) is str and phase in PHASES
                    and (owner['operation'] is None or operation == owner['operation'])
                    and (owner['input_hash'] is None or checksum == owner['input_hash'])
                    and (owner['phase'] is None or PHASES[phase] >= PHASES[owner['phase']]),
                    'restore_evidence_public_identity')
            digest(checksum)
            owner.update(operation=operation, input_hash=checksum, phase=phase,
                         complete=phase == 'succeeded', completed_ns=end if phase == 'succeeded' else None)
        if current == 'create':
            require(clock['preparation']['acknowledgements_bound_ns'] <= row['elapsed_ns']
                    and end < clock['loss_ns'], 'restore_evidence_public_clock')
        else:
            require(clock['loss_ns'] <= row['elapsed_ns'] <= end <= restore_end,
                    'restore_evidence_public_clock')
    require(operations.keys() == {'create', 'restore'} and all(o['complete'] for o in operations.values()),
            'restore_evidence_public_incomplete')
    create, restore = operations['create'], operations['restore']
    backup = clock['backup']
    require(create['operation'] == backup['maintenance_operation_id']
            and create['input_hash'] == backup['input_hash']
            and backup['operation_started_ns'] <= create['started_ns']
            and create['completed_ns'] <= backup['durable_at_ns']
            and create['operation'] != restore['operation']
            and len(health) == 2 and health[0][1] <= create['started_ns']
            and create['completed_ns'] <= health[1][0] <= health[1][1] <= backup['durable_at_ns'],
            'restore_evidence_public_binding')
    return dict(journal=journal, inputs=inputs, operations=operations)


def first_write_witness(root, clock):
    raw, proof = read_private(root, 'campaign-restore-first-write.json', maximum=4096)
    value = decode(raw)
    shape(value, {'schema', 'elapsed_ns', 'acknowledgement'})
    require(value['schema'] == 'riffdb.ticketdesk-lifecycle-restore-first-write/v1',
            'restore_evidence_first_write')
    instant = number(value['elapsed_ns'])
    first_write = next(p['completed_ns'] for p in clock['phases'] if p['kind'] == 'first_write')
    require(clock['loss_ns'] <= instant <= first_write <= clock['deadline_ns'],
            'restore_evidence_first_write')
    acknowledgement = value['acknowledgement']
    shape(acknowledgement, RESUMED_KEYS)
    for key in RESUMED_KEYS - {'schema'}:
        number(acknowledgement[key])
    require(acknowledgement['schema'] == 'riffdb.ticketdesk-lifecycle-resumed/v1'
            and acknowledgement['generation'] == 6 and acknowledgement['contract_version'] == 2
            and acknowledgement['history_incarnation'] == 2
            and clock['preparation']['acknowledged_commands'] < acknowledgement['completed_commands']
                <= instant // SECOND * 100
            and acknowledgement['completed_reads'] <= instant // SECOND * 400,
            'restore_evidence_first_write')
    return dict(input=proof, observation=value)
