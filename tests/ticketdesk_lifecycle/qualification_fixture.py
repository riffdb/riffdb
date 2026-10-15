# req: END-003, END-007, END-008, END-009, END-010
"""Synthetic producer-shaped files; no database, elapsed run or qualification.

Build original inputs consumed by the actual independent closed reconcilers.
Only native work is represented by fixture bytes and observed numeric reports.
Every Git observation, file read, hash, parser and join runs normally in tests.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import struct
from types import SimpleNamespace

from lib import ticketdesk_lifecycle_campaign_reconciliation as producer
from lib.ticketdesk_lifecycle_campaign_runtime import CampaignRuntime
from lib.ticketdesk_lifecycle_campaign_resource_model import artifacts, DISK_LIMITS, PROCESS_LIMITS, PUBLIC_LIMITS
from lib.ticketdesk_lifecycle_campaign_restore_clock import FIXED
from lib.ticketdesk_lifecycle_campaign_restore_inputs import AcknowledgedPrefix, SelectedBackup
from lib.ticketdesk_lifecycle_campaign_extents import MeasuredExtents
from lib.ticketdesk_lifecycle_campaign_restore_evidence import protocol_snapshot
from lib.ticketdesk_lifecycle_provider_budget import parse_sample
from lib.ticketdesk_lifecycle_resources import ALLOCATOR_BASIS
from lib.ticketdesk_lifecycle_campaign_interference import POLICY
from test_campaign_qualification_prerequisites import fixture as prerequisite_fixture
from test_campaign_qualification_operators import fixture as operator_fixture
from test_campaign_finalization import terminal
from test_campaign_progress_reconciliation import native, observer, IDENTITY
from test_campaign_resource_reconciliation import ResourceReconciliationTests
from test_campaign_owner_reconciliation import OwnerReconciliationTests
from test_load_stream import ready, window, stopped
from test_provider_budget import line
from test_history_components import records

SECOND = 10**9
DURATION = 28800
MIB = 1024**2


def write(root, name, value, *, raw=False):
    path = root/name; directory = root
    for part in path.parent.relative_to(root).parts:
        directory /= part; directory.mkdir(mode=0o700, exist_ok=True)
    data = value if raw else json.dumps(value, sort_keys=True).encode()+b'\n'
    path.write_bytes(data); path.chmod(0o600)
    return data


def rows(root, name, values):
    write(root, name, b''.join(json.dumps(row, sort_keys=True).encode()+b'\n' for row in values), raw=True)


def fixture(base):
    prerequisites = prerequisite_fixture(base, actual_repository=True)
    root = base/'campaign'; root.mkdir(mode=0o700)
    (root/'artifacts').mkdir(mode=0o700)
    for name in ('riffdbd', 'riffdb', 'lifecycle', 'riffdb-driverd'):
        write(root, 'artifacts/'+name, (prerequisites.frozen/'ordinary-artifacts'/name).read_bytes(), raw=True)
        (root/'artifacts'/name).chmod(0o700)
    final, _, ledger = operator_fixture(root)
    data = root/'data'; data.mkdir(mode=0o700)
    first = MeasuredExtents(data, duration_s=DURATION, incarnation=1)
    for elapsed in range(0, 5400, 60): first.poll(elapsed*SECOND)
    first.close(5400*SECOND)
    second = MeasuredExtents(data, duration_s=DURATION, incarnation=2)
    for generation in (3, 4, 5):
        write(data, f'ticketdesk.redb.riffjournal.sealed.{generation:020}', b'synthetic sealed extent\n', raw=True)
    second.poll(5402*SECOND)
    for index, generation in enumerate((3, 4, 5)):
        (data/f'ticketdesk.redb.riffjournal.sealed.{generation:020}').unlink()
        second.poll((5403+index)*SECOND)
    for elapsed in range(5460, DURATION, 60): second.poll(elapsed*SECOND)
    second.close(DURATION*SECOND)
    for index in range(4):
        log = b'synthetic native diagnostic\n'
        if index == 3:
            log += b''.join(f'riffdb-retention-extent-retired-v1\troot=2\tgeneration={g}\tdeleted=1\trecycled=0\n'.encode()
                for g in (3, 4, 5))
        write(root, f'server-{index:03}/server.stderr', log, raw=True)
    extents = [first.reconcile([(root/f'server-{i:03}/server.stderr', i == 2) for i in range(3)]),
        second.reconcile([(root/'server-003/server.stderr', False)])]
    # Write complete private ACK records, using the actual fixed-capacity format.
    commands = 534000
    with (root/'updates.witness').open('wb') as out:
        out.write(b'RDBTW001'+(DURATION*100).to_bytes(8, 'big'))
        for start in range(0, DURATION*100, 4096):
            out.write(b''.join(struct.pack('>QQQQ', i, 65537+i, 1 if i < 360000 else 2, 1 if i < commands else 2)
                for i in range(start, min(start+4096, DURATION*100))))
    (root/'updates.witness').chmod(0o600)
    prefix = AcknowledgedPrefix(root, commands=commands, duration_s=DURATION, frontier=65536+commands,
        version=2, incarnation=1, checkpoint=lambda: None)
    prefix_hash = prefix.sha256; prefix.close()
    for name in FIXED: write(root, 'backups/campaign-source/'+name, ('synthetic native backup '+name+'\n').encode(), raw=True)
    backup = SelectedBackup(root, 'campaign-source', checkpoint=lambda: None)
    backup_artifacts, backup_hash = deepcopy(backup.artifacts), backup.manifest_sha256; backup.close()
    loss = 5400*SECOND
    clock = dict(schema='riffdb.ticketdesk-lifecycle-restore-clock/v2', method='primary_loss_v1',
        acknowledgement_method='acknowledged_prefix_v1', protocol_sha256='0'*64,
        planned_loss_ns=loss, loss_ns=loss, deadline_ns=loss+60*SECOND, completed_ns=loss+46*SECOND,
        preparation=dict(started_ns=5340*SECOND, acknowledgements_bound_ns=5341*SECOND+1000,
            acknowledged_commands=commands, acknowledged_frontier=65536+commands,
            acknowledgement_witness_sha256=prefix_hash),
        backup=dict(name='campaign-source', maintenance_operation_id='01900000-0000-7000-8000-000000000001',
            input_hash='a'*64, manifest_sha256=backup_hash, operation_started_ns=5342*SECOND,
            operation_completed_ns=5342*SECOND+1000, operation_deadline_ns=loss, durable_at_ns=5344*SECOND,
            artifacts=backup_artifacts, bytes=sum(value['bytes'] for value in backup_artifacts.values())),
        phases=[dict(kind=kind, completed_ns=value) for kind, value in (
            ('target_setup', loss+SECOND//10), ('target_admission', loss+SECOND//5),
            ('restore', loss+SECOND+1000), ('readiness', loss+2*SECOND),
            ('original_replay', loss+8*SECOND+1000), ('state_reconciliation', loss+10*SECOND),
            ('first_write', loss+45*SECOND+1000))], qualifies_complete_campaign=False)
    for row in ledger:
        command = row['command']; name = f'operator-{row["index"]:03}.stdout'
        value = json.loads((root/name).read_text())
        if command == 'server.health':
            value['result'] = dict(status='ready', last_commit_sequence=str(65536+commands),
                active_contract_version='2', history_incarnation='1')
        elif command in {'backup.create', 'backup.restore'}:
            kind = command.split('.')[1]
            value['result'] = dict(status='terminal', maintenance_operation_id=(clock['backup']['maintenance_operation_id']
                if kind == 'create' else '01900000-0000-7000-8000-000000000002'),
                kind=kind+'_backup', backup_name='campaign-source', input_hash='a'*64 if kind == 'create' else 'b'*64,
                phase='succeeded')
        write(root, name, value)
    protocol = protocol_snapshot(prerequisites.source)
    clock['protocol_sha256'] = hashlib.sha256(write(root, 'campaign-restore-protocol.json', protocol)).hexdigest()
    write(root, 'campaign-restore-clock.json', clock)
    write(root, 'campaign-restore-preparation.json', dict(schema='riffdb.ticketdesk-lifecycle-restore-preparation/v2',
        method=clock['method'], acknowledgement_method=clock['acknowledgement_method'],
        preparation=clock['preparation'], backup=clock['backup'], qualifies_complete_campaign=False))
    def ack(schema, read_count):
        return dict(schema=schema, generation=5, contract_version=2, history_incarnation=1,
            completed_commands=commands, completed_reads=read_count)
    read_admission = dict(schema='riffdb.ticketdesk-lifecycle-restore-read-admission/v1',
        initial=dict(schema='riffdb.ticketdesk-lifecycle-quiesced/v1', generation=5,
            completed_commands=commands, completed_reads=commands*4),
        requested_ns=5345*SECOND, reading_ns=5346*SECOND, pause_requested_ns=loss, quiesced_ns=loss+SECOND//20,
        reading_ack=ack('riffdb.ticketdesk-lifecycle-restore-reads-active/v1', commands*4+1),
        quiesced_ack=ack('riffdb.ticketdesk-lifecycle-restore-readers-quiesced/v1', commands*4+2),
        qualifies_complete_campaign=False)
    write(root, 'campaign-restore-read-admission.json', read_admission)
    first_write = dict(schema='riffdb.ticketdesk-lifecycle-resumed/v1', generation=6, contract_version=2,
        history_incarnation=2, completed_commands=commands+1, completed_reads=commands*4+2)
    write(root, 'campaign-restore-first-write.json', dict(schema='riffdb.ticketdesk-lifecycle-restore-first-write/v1',
        elapsed_ns=loss+45*SECOND+1000, acknowledgement=first_write))
    write(root, 'descriptor.stdout', dict(IDENTITY, reactive_v1='c'*64, reactive_v2='d'*64))
    load = [dict(ready(), schema='riffdb.ticketdesk-lifecycle-load-ready/v2', origin_monotonic_ns=1)]
    load.extend(window(i) for i in range(96)); load.append(stopped(96))
    for index, delta in ((17, -6000), (18, 6000)):
        for key in ('completed_commands', 'transport_attempts', 'settled_submission_attempts'): load[index+1][key] += delta
    rows(root, 'original-load/load.stdout', load)
    history = records(); providers = []; natives = []; observers = []; epochs = []
    starts = [0, 60, 70, 80, 90]
    transitions = [0, 3607*SECOND+SECOND//2, 4207*SECOND+SECOND//2, 4807*SECOND+SECOND//2, 5407*SECOND+SECOND//2]
    for epoch, (kind, cut, elapsed) in enumerate(zip(('initial', 'additive_deploy', 'graceful_restart', 'process_kill', 'backup_restore'), starts, transitions)):
        epochs.append(dict(schema='riffdb.ticketdesk-lifecycle-native-epoch/v1', epoch=epoch, kind=kind, elapsed_ns=elapsed,
            server_incarnation=max(1,epoch), sampler_incarnation=epoch+1, contract_version=1 if epoch == 0 else 2,
            history_incarnation=2 if epoch == 4 else 1, samples=dict.fromkeys(('provider','source','logical','physical'),cut),
            native_samples=cut, native_transport_attempts=cut*3, provider_peak_charged_bytes=12800 if epoch else 0,
            qualifies_complete_resource_model=False))
    for index, second_s in enumerate(range(0, DURATION, 60)):
        epoch = max(i for i, cut in enumerate(starts) if cut <= index)
        elapsed = transitions[epoch] if index == starts[epoch] else second_s*SECOND
        value = native(second_s)
        value.update(elapsed_ns=elapsed, index=index-starts[epoch], contract_version=epochs[epoch]['contract_version'],
            history_incarnation=epochs[epoch]['history_incarnation'])
        if 1800 <= second_s < 2100:
            value.update(outbox_degraded=True, outbox_backlog=(second_s-1800)*100+1)
        natives.append(value)
        paused = next((start for start, end in ((3000,3045), (3600,3645), (4200,4245),
            (4800,4845), (5340,5445)) if start <= second_s < end), None)
        current = observer(second_s if paused is None else paused)
        current.update(elapsed_s=second_s+300, observer_quiesced=paused is not None,
            release_generation=1+sum(second_s >= release for release in (3045,3645,4245,4845,5445)),
            history_incarnation=1 if second_s < 5445 else 2)
        if 600 <= second_s <= 900:
            held = observer(600)
            current.update(consumer_paused=True, events_checked=held['events_checked'],
                consumer_checkpoint=held['consumer_checkpoint'])
        observers.append(dict(schema='riffdb.ticketdesk-lifecycle-campaign-observer/v1', elapsed_ns=elapsed,
            observation_age_ns=0, observation=current))
        for component in history.values(): component[index].update(elapsed_ns=elapsed, incarnation=epochs[epoch]['history_incarnation'])
        counter = index-(starts[epoch] if epoch >= 2 else 0)
        providers.append(dict(parse_sample(line(released_allocations=counter, released_owners=counter)),
            schema='riffdb.ticketdesk-lifecycle-provider-sample/v1', elapsed_ns=elapsed,
            basis='native_exact_provider_aggregate_charge', qualifies_complete_resource_model=False))
    for value in history['source']:
        value.update(schema='riffdb.ticketdesk-lifecycle-source-byte-sample/v2', basis='qualified_canonical_source_receipts_and_keys',
            reused_rows=1, checked_rows=0, subtracted_rows=0, pages=0,
            publication_kind=1, publication_hash_a=7, publication_hash_b=0, publication_hash_c=0, publication_hash_d=0)
    rows(root, 'campaign-observer-observations.jsonl', observers)
    rows(root, 'campaign-native-observations.jsonl', natives)
    for kind, values in history.items(): rows(root, kind+'-byte-samples.jsonl', values)
    rows(root, 'provider-budget-samples.jsonl', providers)
    epoch_stream = []
    def epoch_log(kind, epoch, **extra):
        return dict({key:epochs[epoch][key] for key in ('schema','epoch','elapsed_ns','server_incarnation',
            'sampler_incarnation','qualifies_complete_resource_model')}, kind=kind, **extra)
    for index, value in enumerate(epochs):
        if index >= 2:
            retired = epoch_log('retired_log', index-1, bytes=100, terminal_partial_bytes=0)
            retired['elapsed_ns'] = value['elapsed_ns']-1; epoch_stream.append(retired)
        epoch_stream.append(value)
        if index != 1: epoch_stream.append(epoch_log('bound_log', index, excluded_initial_bytes=0))
    rows(root, 'native-observation-epochs.jsonl', epoch_stream)
    terminal_native = native(DURATION); terminal_native['index'] = len(natives)-starts[-1]
    terminal_native['elapsed_ns'] = (DURATION+1)*SECOND
    terminal_observer = observer(DURATION)
    write(root, 'campaign-terminal-native.json', terminal_native)
    write(root, 'campaign-terminal-observer.json', terminal_observer)
    memory_template = ResourceReconciliationTests(); memory_template.fixture()
    template = memory_template.memory[0]
    allocation_ceiling = sum(PROCESS_LIMITS.values())+7*1024**3
    def memory_at(elapsed):
        value = deepcopy(template); value['elapsed_ns'] = elapsed
        value['allocator_peak_upper_bound_bytes'] = allocation_ceiling
        role_counts = dict(controller=1, server=1+sum(elapsed >= stamp for stamp in transitions[2:]),
            native_sampler=1+sum(elapsed >= stamp for stamp in transitions[1:]), load=1, observer=1)
        count = sum(row['elapsed_ns']+row['duration_ns'] <= elapsed for row in ledger)
        if count: role_counts['operator'] = count
        value['roles'] = {role:dict(incarnations=count, rss_peak_upper_bound_bytes=64*MIB,
            allocator_peak_upper_bound_bytes=PROCESS_LIMITS['controller']) for role, count in role_counts.items()}
        value['incarnations'] = sum(role_counts.values()); value['retired_incarnations'] = value['incarnations']-len(value['processes'])
        value['rss_peak_upper_bound_bytes'] = sum(row['rss_peak_upper_bound_bytes'] for row in value['roles'].values())
        return value
    memory = [memory_at(second_s*SECOND) for second_s in range(0,DURATION,60)]
    terminal_memory = memory_at((DURATION+2)*SECOND+SECOND//2)
    rows(root, 'memory-samples.jsonl', memory)
    disk = [dict(deepcopy(memory_template.disk[0]), elapsed_ns=second_s*SECOND) for second_s in range(0,DURATION+1,30)]
    rows(root, 'disk-samples.jsonl', disk)
    usage_template = OwnerReconciliationTests(); usage = usage_template.fixture()
    def usage_for(role, exit_code, elapsed=1):
        value = deepcopy(usage); value.update(exit_code=exit_code, elapsed_ns=elapsed,
            exit_allocator_peak_upper_bound_bytes=PROCESS_LIMITS['controller'],
            worker_exit_allocator_peak_upper_bound_bytes=PROCESS_LIMITS[role])
        value['address_space_proof'].update(limit_bytes=PROCESS_LIMITS[role],
            inherited_hard_limit_bytes=PROCESS_LIMITS['controller'], lifetime_upper_bound_bytes=PROCESS_LIMITS['controller'])
        return value
    for role, count, prefix, file in (('server',4,'server','server'), ('native_sampler',5,'sampler','native-observer'),
            ('load',1,'original-load','load'), ('observer',1,'original-observer','observer')):
        for index in range(count):
            directory = f'{prefix}-{index:03}' if count > 1 else prefix
            write(root, directory+'/'+file+'.process-usage.json', usage_for(role, -9 if role == 'server' and index == 2 else 0))
    for index in range(len(ledger)): write(root, f'operator-{index:03}.process-usage.json', usage_for('operator',0))
    rows(root, 'campaign-interference.jsonl', [dict(schema='riffdb.ticketdesk-lifecycle-interference/v1',
        elapsed_ns=second_s*SECOND, policy=POLICY, boot_identity_sha256='a'*64, processes_seen=5, owned_processes=5,
        competing_workloads=[], disappeared_during_census=0, qualifies_complete_campaign=False)
        for second_s in range(0,DURATION,60)])
    components = dict(components={kind:dict(samples=480) for kind in (*history,'provider')}, epochs=5,
        native_samples=480, native_transport_attempts=1440, qualifies_complete_resource_model=False)
    components['components']['provider']['peak_charged_bytes'] = 12800
    final.update(schema='riffdb.ticketdesk-lifecycle-finalization/v2', terminal=terminal(), native_endpoint=terminal_native,
        components=components, terminal_memory=terminal_memory, terminal_disk=disk[-1], measured_extents=extents,
        restore_clock=clock, restore_read_admission=read_admission, qualifies_complete_campaign=False)
    write(root, 'campaign-execution.json', final)
    write(root, 'load.terminal', final['terminal'])
    write(root, 'load.phase', b'6 2 2 application-v2.credential\n', raw=True)
    write(root, 'load.terminal-release', b'6 2 2 application-v2.credential\n', raw=True)
    runtime = object.__new__(CampaignRuntime)
    runtime.root, runtime.duration, runtime.origin, runtime.repository = root, DURATION, 1, prerequisites.source
    runtime.children = {}; runtime.roster = SimpleNamespace(children={}); runtime.epochs = SimpleNamespace(complete=True)
    runtime.outputs = {'load':SimpleNamespace(parser=SimpleNamespace(complete=True))}
    runtime.artifacts = root/'artifacts'; runtime.resource_model = {'artifacts':artifacts(root, root/'artifacts')}
    runtime.restore_protocol_sha256 = clock['protocol_sha256']
    runtime.faults = [{key:action[key] for key in ('kind','start_s','duration_s')} for action in final['actions']]
    runtime.identity = dict(IDENTITY); runtime.reconcile_extent_incarnations = lambda:extents
    closed = producer.reconcile(runtime, final)
    closed_raw = write(root, 'campaign-closed-reconciliation.json', closed)
    post = dict(schema='riffdb.ticketdesk-lifecycle-post-validation-resources/v1', measured=False,
        elapsed_ns=(DURATION+5)*SECOND, controller=dict(rss_bytes=10, allocator_bytes=20, allocator_basis=ALLOCATOR_BASIS,
            rss_peak_bytes=15, allocator_peak_bytes=25, reported_rss_peak_bytes=15, reported_allocator_peak_bytes=25,
            task_census_restarts=0), files=dict.fromkeys(DISK_LIMITS,1), qualifies_complete_campaign=False)
    post_raw = write(root, 'campaign-post-validation-resources.json', post)
    return SimpleNamespace(root=root, prerequisites=prerequisites, closed=closed, final=final,
        closed_sha256=hashlib.sha256(closed_raw).hexdigest(), post_validation_sha256=hashlib.sha256(post_raw).hexdigest())
