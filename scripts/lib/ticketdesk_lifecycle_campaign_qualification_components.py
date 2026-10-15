# req: END-001, END-007, END-008, END-010, REC-001
"""Verify original shipping-build, native-crash and saved-restore observations.

These are prerequisite component proofs, never an actual scheduled fault or
elapsed campaign. Reuse the original private observation schemas and clocks.
"""
from .ticketdesk_lifecycle_campaign_qualification_prerequisites import (
    closed_inputs, decode, digest, exact, number, read_original, require, verify_steps,
)

SECOND = 10**9


@closed_inputs
def verify_restore(receipt, files, proofs, revision, binaries):
    require(type(files) is dict and files.keys() == {'observation', 'helper', 'log', 'acknowledgements'},
        'qualification_prerequisite_restore')
    raw, proofs['restore_observation'] = read_original(files['observation'],
        expected_sha256=receipt['whole_observation_receipt_sha256'])
    observed = decode(raw, elapsed_seconds=True)
    exact(observed, receipt['whole_observation'], 'qualification_prerequisite_restore')
    require(observed.get('status') == 'passed' and observed.get('revision') == revision
        and type(observed.get('helper_exit')) is int and observed['helper_exit'] == 0,
        'qualification_prerequisite_restore')
    exact(observed.get('artifacts_sha256'), binaries, 'qualification_prerequisite_restore')
    _, proofs['restore_helper'] = read_original(files['helper'], expected_sha256=observed['helper_sha256'])
    _, proofs['restore_helper_log'] = read_original(files['log'], maximum=16*1024**2,
        expected_sha256=observed['log_sha256'], allow_empty=True)
    observation = observed.get('observation')
    require(type(observation) is dict and observation.get('schema') == 'riffdb.ticketdesk-populated-public-restore-diagnostic/v2'
        and observation.get('revision') == revision and observation.get('phase') == 'complete'
        and observation.get('recovery_clock_method') == observed.get('recovery_clock_method') == 'primary_loss_v1',
        'qualification_prerequisite_restore')
    for parent, key, expected in ((receipt, 'whole_gate_deadline_unchanged_ns', 60*SECOND),
            (observed, 'whole_gate_deadline_ns', 60*SECOND),
            (observation, 'pre_release_deadline_ns', 45*SECOND),
            (observation, 'post_backup_acknowledged_commands', 0)):
        exact(parent.get(key), expected, 'qualification_prerequisite_restore_clock')
    times = [number(observation[key]) for key in ('restore_succeeded_ns', 'restored_readiness_ns',
        'pre_release_reconciled_ns', 'fixed_release_ns', 'first_successful_write_ns',
        'whole_backup_restore_first_write_ns', 'whole_backup_restore_reconciled_ns')]
    require(0 < times[0] <= times[1] <= times[2] < 45*SECOND <= times[3] < 46*SECOND
        and times[3] <= times[4] <= times[5] <= times[6] <= 60*SECOND,
        'qualification_prerequisite_restore_clock')
    for key in ('whole_backup_restore_fault_proved', 'state_reconciled', 'application_role_renewed_by_public_operator',
            'original_acknowledgement_witness_bound', 'first_write_exact_replay',
            'original_acknowledged_identity_replayed_after_restore', 'retained_backup_and_archive_roots_used',
            'existing_replacement_inputs_durable_before_primary_loss', 'native_restore_materialization_remains_inside_recovery_clock',
            'selected_backup_durably_complete_before_primary_loss', 'empty_target_before_start'):
        require(observation.get(key) is True, 'qualification_prerequisite_restore_witness')
    require(observation.get('live_archive_copied') is False
        and observation.get('complete_campaign_checks_proved') is False,
        'qualification_prerequisite_restore_witness')
    digest(observation.get('original_acknowledgement_witness_sha256'))
    raw, proofs['restore_acknowledgements'] = read_original(files['acknowledgements'], maximum=128*1024**2,
        expected_sha256=observation['original_acknowledgement_witness_sha256'])
    commands = number(observation.get('acknowledged_commands_before_loss'), maximum=8640000)
    require(0 < commands and raw[:8] == b'RDBTW001' and len(raw) >= 16,
        'qualification_prerequisite_restore_witness')
    capacity = int.from_bytes(raw[8:16], 'big')
    require(commands <= capacity <= 8640000 and len(raw) == 16+32*capacity,
        'qualification_prerequisite_restore_witness')
    for index in range(commands):
        row = raw[16+32*index:48+32*index]
        ordinal, sequence, version, incarnation = (int.from_bytes(row[i:i+8], 'big') for i in (0, 8, 16, 24))
        require(ordinal == index and sequence > 0 and version in (1, 2) and incarnation == 1,
            'qualification_prerequisite_restore_witness')
    require(int.from_bytes(raw[32:40], 'big') == 1, 'qualification_prerequisite_restore_witness')
    return dict(pre_release_reconciled_ns=times[2], whole_reconciled_ns=times[6],
        acknowledged_commands=commands, recovery_clock_method='primary_loss_v1')


@closed_inputs
def verify_components(receipts, inputs, proofs, revision, binaries):
    files = inputs['component_logs']
    require(type(files) is dict and files.keys() == {'build', 'restore', 'crash'},
        'qualification_prerequisite_components')
    build, restore, crash = (receipts[name+'_receipt'] for name in ('build', 'restore', 'crash'))
    logs = {name:verify_steps(receipts[name+'_receipt'], files[name], proofs, name+'_log')
        for name in ('build', 'restore', 'crash')}
    require(build.get('ordinary_shipping_profile') is True
        and 'ordinary-four-binary-build' in logs['build']
        and b'Finished `release` profile [optimized]' in logs['build']['ordinary-four-binary-build'],
        'qualification_prerequisite_shipping_build')
    require('primary-loss-whole-restore' in logs['restore'], 'qualification_prerequisite_restore')
    require(type(crash.get('actual_original_crash_cases_passed')) is int
        and crash['actual_original_crash_cases_passed'] == 10
        and 'all-ten-original-offline-process-crashes' in logs['crash']
        and b'offline_maintenance_matrix: 10 passed' in logs['crash']['all-ten-original-offline-process-crashes'],
        'qualification_prerequisite_crashes')
    require(crash.get('source_manifest_sha256') == proofs['source_manifest']['sha256'],
        'qualification_prerequisite_lineage')
    restored = verify_restore(restore, inputs['restore_proof'], proofs, revision, binaries)
    return dict(ordinary_shipping_build=True, original_offline_crash_cases=10, saved_restore=restored,
        qualifies_complete_campaign=False)
