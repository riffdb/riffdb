# req: END-003, END-007, END-008, END-009, END-010
"""Join original complete observations into the existing evidence-contract receipt.

This performs no workload, fault, backup, clock sampling or recovery. Every
original input is re-read and checked; an unqualified producer summary or a
successful controller exit cannot establish any qualification predicate.

This is separate offline evidence verification, invoked only after the original
admitted controller has exited. The caller enforces the verifier's own frozen
controller memory ceiling and writes outputs outside the closed campaign root.
It does not introduce another measured campaign actor or extend its windows.
"""
import io
from pathlib import Path

from .ticketdesk_lifecycle_campaign_qualification_prerequisites import (
    closed_inputs, exact, number, require, verify_prerequisites,
)
from .ticketdesk_lifecycle_campaign_qualification_operators import verify_operator_witnesses
from .ticketdesk_lifecycle_campaign_owner_reconciliation import read_private, decode, reconcile as owners
from .ticketdesk_lifecycle_campaign_resource_reconciliation import read_closed, reconcile as resources
from .ticketdesk_lifecycle_campaign_progress_reconciliation import read_rows, reconcile as progress
from .ticketdesk_lifecycle_campaign_interference import reconcile as interference
from .ticketdesk_lifecycle_campaign_resource_model import artifacts, DISK_LIMITS, PROCESS_LIMITS, PUBLIC_LIMITS
from .ticketdesk_lifecycle_history_components import read_observations, reconcile as history
from .ticketdesk_lifecycle_campaign_reconciliation import (
    epoch_transitions, provider_epochs, native_epoch_cuts, window_observations,
)
from .ticketdesk_lifecycle_campaign_restore_evidence import (
    protocol_snapshot, validate_protocol, validate_clock, validate_read_admission,
    public_operation_witnesses, first_write_witness,
)
from .ticketdesk_lifecycle_campaign_restore_inputs import AcknowledgedPrefix, SelectedBackup
from .ticketdesk_lifecycle_campaign_extents import MeasuredExtents
from .ticketdesk_lifecycle_campaign_finalization import checked_terminal
from .ticketdesk_lifecycle_campaign_timeline import contract
from .ticketdesk_lifecycle_load_stream import CampaignLoadStream
from .ticketdesk_lifecycle_resources import ALLOCATOR_BASIS

SECOND = 10**9


@closed_inputs
def corroborate_extents(root, measured, *, duration_s, clock):
    """Recheck original measured numbers against native logs, without sampling."""
    require(type(measured) is list and len(measured) == 2, 'qualification_retirement_incarnations')
    proofs = {}; result = []
    restored = next(row['completed_ns'] for row in clock['phases'] if row['kind'] == 'restore')
    for incarnation, value in enumerate(measured, 1):
        require(type(value) is dict and value.keys() == {'schema', 'history_incarnation',
            'observed_native_retired_generations', 'measured_removal_elapsed_ns',
            'unconfirmed_disappearances', 'qualifies_complete_resource_model'}
            and type(value['history_incarnation']) is int and value['history_incarnation'] == incarnation
            and value['schema'] == 'riffdb.ticketdesk-lifecycle-measured-extents/v1'
            and value['qualifies_complete_resource_model'] is False,
            'qualification_retirement_incarnations')
        generations, times = value['observed_native_retired_generations'], value['measured_removal_elapsed_ns']
        require(type(generations) is list and type(times) is list and len(generations) == len(times) <= 4096
            and all(type(v) is int and 0 < v < 2**64 for v in generations)
            and generations == sorted(set(generations))
            and all(type(t) is int and (0 if incarnation == 1 else restored) <= t < duration_s*SECOND for t in times)
            and (incarnation == 2 or all(t <= restored for t in times)),
            'qualification_retirement_observations')
        exact(value['unconfirmed_disappearances'], 0, 'qualification_retirement_observations')
        # The original measured disappearance list is a numeric observation,
        # not a retrospective inventory. Reuse the native corroborator only;
        # no directory scan or invented disappearance is performed here.
        tracker = object.__new__(MeasuredExtents)
        tracker.incarnation = incarnation; tracker.closed = True; tracker.failed = False
        tracker.observed = dict(zip(generations, times))
        logs = []
        for index in (range(3) if incarnation == 1 else (3,)):
            name = f'server-{index:03}/server.stderr'
            _, proofs[name] = read_private(root, name, maximum=16*1024**2)
            logs.append((root/name, index == 2))
        checked = tracker.reconcile(logs)
        exact(checked, value, 'qualification_retirement_native_join'); result.append(checked)
    require(len(result[-1]['observed_native_retired_generations']) >= 3, 'qualification_retirement_gate')
    return result, proofs


@closed_inputs
def verify_closed_campaign(root, repository, prerequisites, *, closed_sha256, post_validation_sha256):
    root = Path(root); inputs = {}
    raw, closed_input = read_private(root, 'campaign-closed-reconciliation.json', maximum=4*1024**2)
    require(closed_input['sha256'] == closed_sha256, 'qualification_closed_identity')
    closed = decode(raw)
    require(type(closed) is dict and closed.keys() == {'schema', 'duration_s', 'origin_monotonic_ns', 'inputs',
        'owners', 'resources', 'progress', 'interference', 'history', 'provider', 'measured_extents', 'windows',
        'restore_clock', 'restore_preparation_interval_s', 'restore_public_operations', 'restore_first_write',
        'restore_read_admission', 'consumer_backlog_basis', 'eligible_history_basis', 'qualifies_complete_campaign'}
        and closed.get('schema') == 'riffdb.ticketdesk-lifecycle-closed-reconciliation/v2'
        and closed.get('qualifies_complete_campaign') is False,
        'qualification_closed_shape')
    duration = prerequisites['duration_s']
    exact(closed['duration_s'], duration, 'qualification_closed_duration')
    expected = closed['inputs']
    require(type(expected) is dict and expected.keys() == {'finalization', 'restore_protocol', 'restore_clock',
        'restore_preparation', 'restore_read_admission', 'load', 'memory', 'disk', 'observer', 'native',
        'terminal_observer', 'terminal_native', 'interference', 'history', 'provider', 'epochs'},
        'qualification_closed_inputs')
    def read(key, name, maximum):
        raw, inputs[key] = read_private(root, name, maximum=maximum)
        exact(inputs[key], expected[key], 'qualification_closed_input_identity')
        return decode(raw)
    final = read('finalization', 'campaign-execution.json', 1024**2)
    require(type(final) is dict and final.keys() == {'schema', 'terminal', 'native_endpoint', 'components',
        'terminal_memory', 'terminal_disk', 'state', 'archive', 'measured_extents', 'actions',
        'restore_clock', 'restore_read_admission', 'qualifies_complete_campaign'}
        and final['schema'] == 'riffdb.ticketdesk-lifecycle-finalization/v2'
        and final['qualifies_complete_campaign'] is False, 'qualification_finalization')
    checked_terminal(final['terminal'], generation=6, version=2, incarnation=2, duration_s=duration)
    raw, inputs['original_terminal'] = read_private(root, 'load.terminal', maximum=4096)
    exact(decode(raw), final['terminal'], 'qualification_original_terminal')
    for key, name in (('original_phase', 'load.phase'), ('terminal_release', 'load.terminal-release')):
        raw, inputs[key] = read_private(root, name, maximum=256)
        exact(raw, b'6 2 2 application-v2.credential\n', 'qualification_original_terminal_release')
    raw, inputs['descriptor'] = read_private(root, 'descriptor.stdout', maximum=4096)
    descriptor = decode(raw)
    identity = {key:descriptor[key] for key in ('projection_id', 'projection_plan_hash')}
    campaign_binaries = artifacts(root, root/'artifacts')
    require({name:value['sha256'] for name, value in campaign_binaries.items()}
        == prerequisites['bindings']['ordinary_artifact_sha256'], 'qualification_campaign_binaries')
    protocol = read('restore_protocol', 'campaign-restore-protocol.json', 65536)
    validate_protocol(protocol)
    exact(protocol, protocol_snapshot(repository), 'qualification_restore_protocol')
    require(inputs['restore_protocol']['sha256'] == prerequisites['manifest']['artifacts']['restore_protocol'],
        'qualification_restore_protocol')
    clock = read('restore_clock', 'campaign-restore-clock.json', 1024**2)
    exact(clock, final['restore_clock'], 'qualification_restore_clock')
    plan = next(row for row in prerequisites['manifest']['faults'] if row['kind'] == 'backup_restore')
    action = next(row for row in final['actions'] if row['kind'] == 'backup_restore')
    preparation = validate_clock(clock, protocol_sha256=inputs['restore_protocol']['sha256'], plan=plan, action=action)
    value = read('restore_preparation', 'campaign-restore-preparation.json', 1024**2)
    exact(value, dict(schema='riffdb.ticketdesk-lifecycle-restore-preparation/v2', method=clock['method'],
        acknowledgement_method=clock['acknowledgement_method'], preparation=clock['preparation'],
        backup=clock['backup'], qualifies_complete_campaign=False), 'qualification_restore_preparation')
    operations = public_operation_witnesses(root, clock, duration_s=duration)
    first_write = first_write_witness(root, clock)
    reads = read('restore_read_admission', 'campaign-restore-read-admission.json', 4096)
    exact(reads, final['restore_read_admission'], 'qualification_restore_reads')
    validate_read_admission(reads, clock, expected_generation=5, expected_version=2)
    with_prefix = AcknowledgedPrefix(root, commands=clock['preparation']['acknowledged_commands'],
        duration_s=duration, frontier=clock['preparation']['acknowledged_frontier'], version=2,
        incarnation=1, checkpoint=lambda: None)
    try:
        require(with_prefix.sha256 == clock['preparation']['acknowledgement_witness_sha256'],
            'qualification_restore_acknowledgements')
    finally: with_prefix.close()
    backup = SelectedBackup(root, clock['backup']['name'], checkpoint=lambda: None)
    try:
        exact(backup.artifacts, clock['backup']['artifacts'], 'qualification_restore_backup')
        require(backup.manifest_sha256 == clock['backup']['manifest_sha256'], 'qualification_restore_backup')
    finally: backup.close()
    raw, inputs['load'] = read_private(root, 'original-load/load.stdout', maximum=290*4096)
    exact(inputs['load'], expected['load'], 'qualification_closed_input_identity')
    parser = CampaignLoadStream(duration_s=duration, require_shared_clock=True)
    source = io.BytesIO(raw); load = []
    while (value := parser.read(source)) is not None: load.append(value)
    require(parser.complete and parser.origin_ns == closed['origin_monotonic_ns']
        and load[-1]['post_duration_updates'] == load[-1]['post_duration_reads'] == 0,
        'qualification_original_load')
    memory, inputs['memory'] = read_closed(root, 'memory-samples.jsonl', maximum_rows=2048)
    disk, inputs['disk'] = read_closed(root, 'disk-samples.jsonl', maximum_rows=4096)
    limits = {**PUBLIC_LIMITS, 'control_and_evidence_bytes':DISK_LIMITS['control_and_evidence_bytes']}
    resource_result = resources(memory, disk, duration_s=duration, process_limits=PROCESS_LIMITS,
        resource_limits=limits, terminal_memory=final['terminal_memory'])
    owner_result = owners(root, duration_s=duration, limits=PROCESS_LIMITS, terminal_memory=final['terminal_memory'])
    rows = {}
    for key, name, count in (('observer','campaign-observer-observations.jsonl',86475),
            ('native','campaign-native-observations.jsonl',4096),
            ('interference','campaign-interference.jsonl',17281),
            ('provider','provider-budget-samples.jsonl',2048), ('epochs','native-observation-epochs.jsonl',12)):
        rows[key], inputs[key] = read_rows(root, name, maximum=count*4096, rows=count)
    terminal_observer = read('terminal_observer', 'campaign-terminal-observer.json', 4096)
    terminal_native = read('terminal_native', 'campaign-terminal-native.json', 4096)
    exact(terminal_native, final['native_endpoint'], 'qualification_native_endpoint')
    progress_result = progress(rows['observer'], rows['native'], duration_s=duration, identity=identity,
        terminal_observer=terminal_observer, terminal_native=terminal_native)
    interference_result = interference(rows['interference'], duration_s=duration)
    histories, inputs['history'] = read_observations(root)
    history_result = history(histories, duration_s=duration, incarnation=2)
    epochs = epoch_transitions(rows['epochs'])
    provider_result = provider_epochs(rows['provider'], epochs, duration_s=duration, native_count=len(rows['native']))
    native_epoch_cuts(rows['native'], histories, epochs, duration_s=duration)
    by_kind = {action['kind']:action for action in final['actions']}
    for epoch in epochs[1:]:
        start = by_kind[epoch['kind']]['start_s']*SECOND
        limit = 240 if epoch['kind'] == 'backup_restore' else 60
        require(start <= epoch['elapsed_ns'] < start+limit*SECOND, 'qualification_epoch_fault_clock')
    require(next(phase['completed_ns'] for phase in clock['phases'] if phase['kind'] == 'readiness')
        <= epochs[-1]['elapsed_ns']
        <= next(phase['completed_ns'] for phase in clock['phases'] if phase['kind'] == 'original_replay'),
        'qualification_epoch_restore_clock')
    summary = final['components']
    require(summary['epochs'] == 5 and summary['native_samples'] == len(rows['native'])
        and summary['native_transport_attempts'] == sum(row['transport_attempts'] for row in rows['native'])
        and all(summary['components'][kind]['samples'] == len(values)
            for kind, values in {**histories, 'provider':rows['provider']}.items())
        and summary['components']['provider']['peak_charged_bytes'] == provider_result['peak_charged_bytes'],
        'qualification_collector_summary')
    extents, extent_inputs = corroborate_extents(root, final['measured_extents'], duration_s=duration, clock=clock)
    windows = window_observations(load[1:-1], progress_result['windows'], resource_result['windows'],
        histories['logical'], duration_s=duration)
    for key in expected:
        exact(inputs[key], expected[key], 'qualification_closed_input_identity')
    exact(closed['consumer_backlog_basis'],
        'conservative_offered_organization_zero_matches_less_observed_acknowledgements', 'qualification_closed_basis')
    exact(closed['eligible_history_basis'], 'canonical_logical_history_and_removed_keys_only',
        'qualification_closed_basis')
    for key, value in dict(owners=owner_result, resources=resource_result, progress=progress_result,
            interference=interference_result, history=history_result, provider=provider_result,
            measured_extents=extents, windows=windows, restore_clock=clock,
            restore_preparation_interval_s=preparation, restore_public_operations=operations,
            restore_first_write=first_write, restore_read_admission=reads).items():
        # The JSON closed result stores this pair as an array.
        exact(closed[key], list(value) if key == 'restore_preparation_interval_s' else value,
            'qualification_closed_reconciliation')
    operator_result = verify_operator_witnesses(root, final, duration_s=duration,
        projection_identity=identity, restore_clock=clock)
    raw, post_input = read_private(root, 'campaign-post-validation-resources.json', maximum=4096)
    require(post_input['sha256'] == post_validation_sha256, 'qualification_post_validation_identity')
    post = decode(raw)
    require(type(post) is dict and post.keys() == {'schema', 'measured', 'elapsed_ns', 'controller',
        'files', 'qualifies_complete_campaign'} and post['schema'] == 'riffdb.ticketdesk-lifecycle-post-validation-resources/v1'
        and post['measured'] is False and post['qualifies_complete_campaign'] is False
        and number(post['elapsed_ns']) >= duration*SECOND and type(post['controller']) is dict
        and type(post['files']) is dict and post['files'].keys() == DISK_LIMITS.keys(),
        'qualification_post_validation')
    controller = post['controller']
    require(controller.keys() == {'rss_bytes', 'allocator_bytes', 'allocator_basis', 'rss_peak_bytes',
        'allocator_peak_bytes', 'reported_rss_peak_bytes', 'reported_allocator_peak_bytes', 'task_census_restarts'}
        and controller['allocator_basis'] == ALLOCATOR_BASIS, 'qualification_post_validation_bounds')
    for key in controller.keys()-{'allocator_basis'}: number(controller[key])
    require(all(number(post['files'][key]) <= limit for key, limit in DISK_LIMITS.items())
        and 0 < controller['rss_bytes'] <= controller['allocator_bytes'] <= controller['allocator_peak_bytes']
            <= PROCESS_LIMITS['controller']
        and controller['rss_bytes'] <= controller['reported_rss_peak_bytes'] <= controller['rss_peak_bytes']
            <= controller['reported_allocator_peak_bytes'] == controller['allocator_peak_bytes'],
        'qualification_post_validation_bounds')
    exact(read_private(root, 'campaign-closed-reconciliation.json', maximum=4*1024**2)[1], closed_input,
        'qualification_closed_changed')
    return dict(finalization=final, windows=windows, restore_clock=clock, restore_read_admission=reads,
        extents=extents, owners=owner_result, operator_witnesses=operator_result,
        inputs=inputs, extent_inputs=extent_inputs, closed_input=closed_input, post_validation_input=post_input,
        qualifies_complete_campaign=False)


@closed_inputs
def assemble(root, frozen_root, repository, *, manifest_sha256, bindings_sha256, inputs,
        closed_sha256, post_validation_sha256):
    """Return the accepted receipt shape after all independent joins succeed."""
    prerequisites = verify_prerequisites(frozen_root, repository,
        manifest_sha256=manifest_sha256, bindings_sha256=bindings_sha256, inputs=inputs)
    campaign = verify_closed_campaign(root, repository, prerequisites,
        closed_sha256=closed_sha256, post_validation_sha256=post_validation_sha256)
    # These constants represent completed validation above; none is copied from
    # a producer's checks dictionary or success/status-only summary.
    checks = dict(source_clean=True, host_valid=True, durability_verified=True, rust_workload=True,
        typescript_smoke=True, four_language_conformance=True, full_ci=True, events_active=True,
        consumer_active=True, projection_active=True, live_query_active=True, state_reconciled=True,
        restore_reconciled=True, idempotency_reconciled=True, provenance_reconciled=True,
        tenant_isolation=True, revocation_enforced=True, archive_reconciled=True, resource_model_verified=True)
    require(checks.keys() == contract.CHECKS, 'qualification_predicate_inventory')
    receipt = dict(schema=contract.RECEIPT_SCHEMA, classification='rehearsal', evidence_kind='measured',
        manifest_sha256=manifest_sha256, source_revision=prerequisites['source_revision'],
        artifacts={name:dict(path=prerequisites['bindings']['artifact_paths'][name], sha256=checksum)
            for name, checksum in prerequisites['manifest']['artifacts'].items()}, checks=checks,
        actions=campaign['finalization']['actions'],
        reclaimed_generations=campaign['extents'][-1]['observed_native_retired_generations'],
        windows=campaign['windows'], restore_clock=campaign['restore_clock'],
        restore_read_admission=campaign['restore_read_admission'])
    contract.validate(prerequisites['manifest'], receipt, short_rehearsal=prerequisites['duration_s'] == 28800)
    # Re-read frozen/source prerequisites after the measured-input join, too.
    repeated = verify_prerequisites(frozen_root, repository,
        manifest_sha256=manifest_sha256, bindings_sha256=bindings_sha256, inputs=inputs)
    exact(repeated, prerequisites, 'qualification_prerequisites_changed')
    return receipt, dict(scope='offline_after_original_campaign_controller_exit',
        prerequisites=prerequisites['inputs'], campaign=campaign['inputs'],
        owners=campaign['owners']['inputs'], operators=campaign['operator_witnesses']['inputs'],
        extents=campaign['extent_inputs'], closed=campaign['closed_input'],
        post_validation=campaign['post_validation_input'])
