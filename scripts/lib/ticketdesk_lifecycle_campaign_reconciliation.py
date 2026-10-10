# req: END-003, END-007, END-008, END-009, END-010
"""Reconcile closed original-clock evidence after the actual controller exits.

This combines observations without granting source, host, durability, CI or
conformance credit. Those independently frozen prerequisites still belong to
the entrypoint. A missing input is never replaced by a successful assertion.
"""
from .ticketdesk_lifecycle_campaign_owner_reconciliation import read_private, decode, reconcile as owners
from .ticketdesk_lifecycle_campaign_resource_reconciliation import read_closed, reconcile as resources
from .ticketdesk_lifecycle_campaign_progress_reconciliation import read_rows, reconcile as progress
from .ticketdesk_lifecycle_campaign_interference import reconcile as interference
from .ticketdesk_lifecycle_campaign_resource_model import artifacts, DISK_LIMITS, PROCESS_LIMITS, PUBLIC_LIMITS
from .ticketdesk_lifecycle_history_components import read_observations, reconcile as history
from .ticketdesk_lifecycle_provider_budget import FIELDS as PROVIDER_FIELDS, PREFIX, parse_sample
from .ticketdesk_lifecycle_campaign_runtime import CampaignRuntime
from .ticketdesk_lifecycle_load_stream import CampaignLoadStream
from .ticketdesk_lifecycle_history_components import native_record
import io

SECOND=10**9
KINDS=('initial','additive_deploy','graceful_restart','process_kill','backup_restore')
EPOCH_FIELDS=frozenset({'schema','epoch','kind','elapsed_ns','server_incarnation','sampler_incarnation',
    'contract_version','history_incarnation','samples','native_samples','native_transport_attempts',
    'provider_peak_charged_bytes','qualifies_complete_resource_model'})
PROVIDER_RECORD_FIELDS=PROVIDER_FIELDS|{'schema','elapsed_ns','charged_bytes','basis',
    'qualifies_complete_resource_model'}


class CampaignReconciliationError(ValueError):
    """Closed refusal without private paths or diagnostic contents."""


def require(condition,code):
    if not condition:raise CampaignReconciliationError(code)


def number(value):
    require(type(value) is int and 0<=value<2**64,'campaign_reconciliation_number')
    return value


def provider_epochs(rows,epochs,*,duration_s,native_count):
    require(type(duration_s) is int and duration_s%300==0 and 73<=duration_s//300<=288,
            'campaign_reconciliation_duration')
    require(type(rows) is list and 0<len(rows)<=2048 and type(epochs) is list and len(epochs)==5,
            'campaign_reconciliation_epochs')
    previous=None
    for index,row in enumerate(epochs):
        require(type(row) is dict and row.keys()==EPOCH_FIELDS
                and row['schema']=='riffdb.ticketdesk-lifecycle-native-epoch/v1'
                and row['kind']==KINDS[index] and row['qualifies_complete_resource_model'] is False,
                'campaign_reconciliation_epochs')
        for key in EPOCH_FIELDS-{'schema','kind','samples','qualifies_complete_resource_model'}:
            number(row[key])
        require(row['epoch']==index and row['sampler_incarnation']==index+1
                and row['server_incarnation']==max(1,index)
                and row['contract_version']==(1 if index==0 else 2)
                and row['history_incarnation']==(2 if index==4 else 1),
                'campaign_reconciliation_epochs')
        counts=row['samples']
        require(type(counts) is dict and counts.keys()=={'provider','source','logical','physical'},
                'campaign_reconciliation_epochs')
        for value in counts.values():number(value)
        require(row['elapsed_ns']<duration_s*SECOND
                and row['native_samples']<native_count and counts['provider']<len(rows),
                'campaign_reconciliation_epochs')
        if previous is None:
            require(row['elapsed_ns']==row['native_samples']==row['native_transport_attempts']==0
                    and all(value==0 for value in counts.values()),'campaign_reconciliation_epochs')
        else:
            require(row['elapsed_ns']>previous['elapsed_ns']
                    and row['native_samples']>previous['native_samples']
                    and row['native_transport_attempts']>previous['native_transport_attempts']
                    and all(counts[key]>previous['samples'][key] for key in counts)
                    and row['provider_peak_charged_bytes']>=previous['provider_peak_charged_bytes'],
                    'campaign_reconciliation_epochs')
        previous=row
    peak=0;prior=None;prior_ns=None;epoch=0;coverage=[0]*(duration_s//300)
    for index,row in enumerate(rows):
        require(type(row) is dict and row.keys()==PROVIDER_RECORD_FIELDS
                and row['schema']=='riffdb.ticketdesk-lifecycle-provider-sample/v1'
                and row['basis']=='native_exact_provider_aggregate_charge'
                and row['qualifies_complete_resource_model'] is False,
                'campaign_reconciliation_provider')
        elapsed=number(row['elapsed_ns']);number(row['charged_bytes'])
        encoded=PREFIX+b'available=true '+b' '.join(key.encode()+b'='+str(number(row[key])).encode()
            for key in sorted(PROVIDER_FIELDS))+b'\n'
        value=parse_sample(encoded)
        require(value['charged_bytes']==row['charged_bytes'],'campaign_reconciliation_provider')
        require(elapsed<duration_s*SECOND and elapsed-(prior_ns or 0)<=75*SECOND
                and (prior_ns is None or elapsed>prior_ns),'campaign_reconciliation_provider_gap')
        if epoch+1<len(epochs) and index==epochs[epoch+1]['samples']['provider']:
            epoch+=1
            if epochs[epoch]['server_incarnation']!=epochs[epoch-1]['server_incarnation']:prior=None
        require(elapsed>=epochs[epoch]['elapsed_ns']
                and (epoch+1==len(epochs) or elapsed<=epochs[epoch+1]['elapsed_ns']),
                'campaign_reconciliation_provider_epoch')
        if prior is not None:
            require(all(value[key]>=prior[key] for key in ('released_allocations','released_owners')),
                    'campaign_reconciliation_provider_rewind')
        peak=max(peak,value['charged_bytes']);coverage[elapsed//(300*SECOND)]+=1
        prior,prior_ns=value,elapsed
    require(epoch==4 and all(coverage) and duration_s*SECOND-prior_ns<=75*SECOND,
            'campaign_reconciliation_provider_gap')
    return dict(samples=len(rows),peak_charged_bytes=peak,window_samples=coverage)


def reconcile(runtime,result):
    require(isinstance(runtime,CampaignRuntime) and runtime.origin is not None
            and not runtime.children and runtime.roster is not None and not runtime.roster.children
            and runtime.epochs.complete and runtime.outputs['load'].parser.complete,
            'campaign_reconciliation_state')
    root,duration=runtime.root,runtime.duration
    raw,final_input=read_private(root,'campaign-execution.json',maximum=1024**2)
    require(decode(raw)==result and type(result) is dict
            and result.get('schema')=='riffdb.ticketdesk-lifecycle-finalization/v1'
            and result.get('qualifies_complete_campaign') is False,
            'campaign_reconciliation_finalization')
    require(artifacts(root,runtime.artifacts)==runtime.resource_model['artifacts'],
            'campaign_reconciliation_artifacts')
    inputs={'finalization':final_input}
    raw,inputs['load']=read_private(root,'original-load/load.stdout',maximum=290*4096)
    parser=CampaignLoadStream(duration_s=duration,require_shared_clock=True)
    source=io.BytesIO(raw);load_rows=[]
    while (value:=parser.read(source)) is not None:load_rows.append(value)
    require(parser.complete and parser.origin_ns==runtime.origin
            and load_rows[-1]['post_duration_updates']==load_rows[-1]['post_duration_reads']==0,
            'campaign_reconciliation_load')
    memory_rows,inputs['memory']=read_closed(root,'memory-samples.jsonl',maximum_rows=2048)
    disk_rows,inputs['disk']=read_closed(root,'disk-samples.jsonl',maximum_rows=4096)
    limits={**PUBLIC_LIMITS,'control_and_evidence_bytes':DISK_LIMITS['control_and_evidence_bytes']}
    resource_result=resources(memory_rows,disk_rows,duration_s=duration,process_limits=PROCESS_LIMITS,
        resource_limits=limits,terminal_memory=result['terminal_memory'])
    owner_result=owners(root,duration_s=duration,limits=PROCESS_LIMITS,terminal_memory=result['terminal_memory'])
    observer_rows,inputs['observer']=read_rows(root,'campaign-observer-observations.jsonl',
        maximum=86475*4096,rows=86475)
    native_rows,inputs['native']=read_rows(root,'campaign-native-observations.jsonl',maximum=4096*4096,rows=4096)
    raw,inputs['terminal_observer']=read_private(root,'campaign-terminal-observer.json',maximum=4096)
    terminal_observer=decode(raw)
    raw,inputs['terminal_native']=read_private(root,'campaign-terminal-native.json',maximum=4096)
    terminal_native=decode(raw)
    require(terminal_native==result['native_endpoint'],'campaign_reconciliation_terminal')
    progress_result=progress(observer_rows,native_rows,duration_s=duration,identity=runtime.identity,
        terminal_observer=terminal_observer,terminal_native=terminal_native)
    interference_rows,inputs['interference']=read_rows(root,'campaign-interference.jsonl',
        maximum=17281*4096,rows=17281)
    interference_result=interference(interference_rows,duration_s=duration)
    history_rows,history_inputs=read_observations(root)
    inputs['history']=history_inputs
    history_result=history(history_rows,duration_s=duration,incarnation=2)
    provider_rows,inputs['provider']=read_rows(root,'provider-budget-samples.jsonl',maximum=2048*4096,rows=2048)
    epoch_rows,inputs['epochs']=read_rows(root,'native-observation-epochs.jsonl',maximum=8*4096,rows=8)
    provider_result=provider_epochs(provider_rows,epoch_rows,duration_s=duration,native_count=len(native_rows))
    summary=result['components']
    require(summary['epochs']==5 and summary['native_samples']==len(native_rows)
            and summary['native_transport_attempts']==sum(row['transport_attempts'] for row in native_rows)
            and all(summary['components'][kind]['samples']==len(rows)
                    for kind,rows in {**history_rows,'provider':provider_rows}.items())
            and summary['components']['provider']['peak_charged_bytes']==provider_result['peak_charged_bytes'],
            'campaign_reconciliation_collector_summary')
    extents=runtime.reconcile_extent_incarnations()
    require(extents==result['measured_extents'] and len(extents)==2
            and extents[-1]['history_incarnation']==2
            and len(extents[-1]['observed_native_retired_generations'])>=3,
            'campaign_reconciliation_retirements')
    windows=window_observations(load_rows[1:-1],progress_result['windows'],resource_result['windows'],
        history_rows['logical'],duration_s=duration)
    return dict(schema='riffdb.ticketdesk-lifecycle-closed-reconciliation/v1',duration_s=duration,
        origin_monotonic_ns=runtime.origin,inputs=inputs,owners=owner_result,resources=resource_result,
        progress=progress_result,interference=interference_result,history=history_result,
        provider=provider_result,measured_extents=extents,windows=windows,
        consumer_backlog_basis='conservative_offered_organization_zero_matches_less_observed_acknowledgements',
        eligible_history_basis='canonical_logical_history_and_removed_keys_only',
        qualifies_complete_campaign=False)


def window_observations(load,progress,resources,logical,*,duration_s):
    require(type(duration_s) is int and duration_s%300==0 and 73<=duration_s//300<=288,
            'campaign_reconciliation_duration')
    count=duration_s//300
    require(type(load) is list and type(progress) is list and type(resources) is list
            and len(load)==len(progress)==len(resources)==count
            and type(logical) is list and 0<len(logical)<=2048,'campaign_reconciliation_windows')
    maxima=[None]*count
    for row in logical:
        value=native_record('logical',row);elapsed=number(row['elapsed_ns'])
        require(elapsed<duration_s*SECOND,'campaign_reconciliation_windows')
        amount=number(value['canonical_history_value_bytes']+value['removed_key_bytes'])
        index=elapsed//(300*SECOND);maxima[index]=max(maxima[index] or 0,amount)
    require(all(value is not None for value in maxima),'campaign_reconciliation_windows')
    windows=[];commands=reads=0
    for index,(worker,observer,resource) in enumerate(zip(load,progress,resources)):
        end=(index+1)*300
        require(worker['start_s']==index*300 and worker['end_s']==end
                and worker['offered_commands']==30000 and worker['offered_reads']==120000,
                'campaign_reconciliation_load')
        commands+=number(worker['completed_commands']);reads+=number(worker['completed_reads'])
        require(commands<=end*100 and reads<=end*400,'campaign_reconciliation_load')
        offered=end*100
        matches=1024+offered//65536*1024+min(offered%65536,1024)
        require(number(observer['events_checked'])<=matches,'campaign_reconciliation_consumer')
        windows.append(dict(start_s=index*300,end_s=end,offered_commands=30000,offered_reads=120000,
            completed_commands=worker['completed_commands'],completed_reads=worker['completed_reads'],
            transport_attempts=worker['transport_attempts'],retries=worker['retries'],
            unresolved_commands=end*100-commands,unexpected_errors=0,p95_us=worker['p95_us'],p99_us=worker['p99_us'],
            max_in_flight=worker['load_peak_in_flight']+worker['reserved_auxiliary_slots'],
            eligible_history_bytes=maxima[index],consumer_backlog=matches-observer['events_checked'],
            outbox_backlog=observer['outbox_backlog'],projection_backlog=observer['projection_backlog'],
            interference=False,resources={key:resource[key] for key in PUBLIC_LIMITS}))
    require(commands==duration_s*100 and reads==duration_s*400,'campaign_reconciliation_load')
    return windows
