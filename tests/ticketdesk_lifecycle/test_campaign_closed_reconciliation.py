# req: END-003, END-007, END-008, END-010
"""Closed observations cannot erase owner epochs, offered debt or separate bases."""
import unittest
from copy import deepcopy

from lib.ticketdesk_lifecycle_campaign_reconciliation import (
    CampaignReconciliationError, provider_epochs, window_observations,
    epoch_transitions, native_epoch_cuts,
)
from lib.ticketdesk_lifecycle_provider_budget import parse_sample
from test_provider_budget import line
from test_load_stream import window
from test_history_components import records, DURATION
from lib.ticketdesk_lifecycle_campaign_resource_model import PUBLIC_LIMITS
from test_campaign_progress_reconciliation import native

SECOND=10**9


def providers():
    rows=[]
    for index,second in enumerate(range(0,DURATION,60)):
        counter=index if index<20 else index-(index//10)*10 if index<40 else index-40
        value=parse_sample(line(released_allocations=counter,released_owners=counter))
        rows.append(dict(value,schema='riffdb.ticketdesk-lifecycle-provider-sample/v1',elapsed_ns=second*SECOND,
            basis='native_exact_provider_aggregate_charge',qualifies_complete_resource_model=False))
    epochs=[]
    for index,kind in enumerate(('initial','additive_deploy','graceful_restart','process_kill','backup_restore')):
        count=index*10
        epochs.append(dict(schema='riffdb.ticketdesk-lifecycle-native-epoch/v1',epoch=index,kind=kind,
            elapsed_ns=count*60*SECOND,server_incarnation=max(1,index),sampler_incarnation=index+1,
            contract_version=1 if index==0 else 2,history_incarnation=2 if index==4 else 1,
            samples=dict.fromkeys(('provider','source','logical','physical'),count),native_samples=count,
            native_transport_attempts=count*3,provider_peak_charged_bytes=12800 if index else 0,
            qualifies_complete_resource_model=False))
    return rows,epochs


def observations():
    workers=[window(index) for index in range(DURATION//300)]
    progress=[]
    for index in range(len(workers)):
        offered=(index+1)*30000
        matching=1024+offered//65536*1024+min(offered%65536,1024)
        progress.append(dict(events_checked=matching,outbox_backlog=0,projection_backlog=0))
    resources=[dict.fromkeys(PUBLIC_LIMITS,1) for _ in workers]
    return workers,progress,resources,records()['logical']


def epoch_observations():
    provider,epochs=providers();history=records();samples=[]
    for index,second in enumerate(range(0,DURATION,60)):
        epoch=min(index//10,4)
        value=native(second)
        value.update(index=index-epochs[epoch]['native_samples'],
            contract_version=epochs[epoch]['contract_version'],
            history_incarnation=epochs[epoch]['history_incarnation'])
        samples.append(value)
        for rows in history.values():rows[index]['incarnation']=epochs[epoch]['history_incarnation']
    for row in history['source']:
        row.update(schema='riffdb.ticketdesk-lifecycle-source-byte-sample/v2',
            basis='qualified_canonical_source_receipts_and_keys',
            reused_rows=1,checked_rows=0,subtracted_rows=0,pages=0,
            publication_kind=1,publication_hash_a=7,publication_hash_b=0,
            publication_hash_c=0,publication_hash_d=0)
    stream=[]
    def log(kind,epoch,**values):
        base={key:epochs[epoch][key] for key in ('schema','epoch','elapsed_ns',
            'server_incarnation','sampler_incarnation','qualifies_complete_resource_model')}
        return dict(base,kind=kind,**values)
    for epoch,row in enumerate(epochs):
        if epoch>=2:
            retired=log('retired_log',epoch-1,bytes=100,terminal_partial_bytes=1 if epoch==3 else 0)
            retired['elapsed_ns']=row['elapsed_ns']-1;stream.append(retired)
        stream.append(deepcopy(row))
        if epoch!=1:stream.append(log('bound_log',epoch,excluded_initial_bytes=10 if epoch==0 else 0))
    return provider,epochs,samples,history,stream


class ClosedReconciliationTests(unittest.TestCase):
    def test_actual_mixed_epoch_stream_binds_every_native_and_history_prefix(self):
        provider,epochs,samples,history,stream=epoch_observations()
        self.assertEqual(len(stream),12)
        self.assertEqual(epoch_transitions(stream),epochs)
        provider_epochs(provider,epochs,duration_s=DURATION,native_count=len(samples))
        native_epoch_cuts(samples,history,epochs,duration_s=DURATION)

    def test_unrecorded_sampler_reset_or_inexact_epoch_prefix_refuses(self):
        for fault in ('reset','cut','attempts','version','incarnation','time','history_cut',
                      'history_incarnation','history_root','source_version'):
            with self.subTest(fault=fault):
                _,epochs,samples,history,_=epoch_observations()
                if fault=='reset':samples[5]['index']=0
                if fault=='cut':epochs[1]['native_samples']+=1
                if fault=='attempts':epochs[1]['native_transport_attempts']+=1
                if fault=='version':samples[5]['contract_version']=2
                if fault=='incarnation':samples[5]['history_incarnation']=2
                if fault=='time':samples[10]['elapsed_ns']-=1
                if fault=='history_cut':epochs[1]['samples']['source']+=2
                if fault=='history_incarnation':history['physical'][5]['incarnation']=2
                if fault=='history_root':history['logical'][11]['root']=2
                if fault=='source_version':history['source'][5]=records()['source'][5]
                with self.assertRaises(CampaignReconciliationError):
                    native_epoch_cuts(samples,history,epochs,duration_s=DURATION)

    def test_missing_duplicate_reordered_or_inconsistent_log_custody_refuses(self):
        for fault in ('missing','duplicate','order','owner','exclude','partial','bytes','shape','clock'):
            with self.subTest(fault=fault):
                *_,stream=epoch_observations()
                if fault=='missing':stream.pop(1)
                if fault=='duplicate':stream.insert(2,deepcopy(stream[1]))
                if fault=='order':stream[3],stream[4]=stream[4],stream[3]
                if fault=='owner':stream[3]['server_incarnation']=2
                if fault=='exclude':stream[-1]['excluded_initial_bytes']=1
                if fault=='partial':stream[3]['terminal_partial_bytes']=1
                if fault=='bytes':stream[3]['bytes']=0
                if fault=='shape':stream[1]['extra']=0
                if fault=='clock':stream[3]['elapsed_ns']=0
                with self.assertRaises(CampaignReconciliationError):epoch_transitions(stream)

    def test_epoch_provider_peak_is_the_exact_observed_prefix(self):
        for fault in ('initial','under','over'):
            with self.subTest(fault=fault):
                rows,epochs=providers()
                if fault=='initial':epochs[0]['provider_peak_charged_bytes']=1
                else:epochs[1]['provider_peak_charged_bytes']+=-1 if fault=='under' else 1
                with self.assertRaises(CampaignReconciliationError):
                    provider_epochs(rows,epochs,duration_s=DURATION,native_count=len(rows))

    def test_only_actual_server_replacement_permits_provider_counter_reset(self):
        rows,epochs=providers()
        value=provider_epochs(rows,epochs,duration_s=DURATION,native_count=len(rows))
        self.assertEqual(value['samples'],480)
        self.assertEqual(value['peak_charged_bytes'],12800)
        self.assertEqual(value['window_samples'],[5]*96)

    def test_sampler_replacement_at_deploy_cannot_erase_the_same_provider_pool(self):
        rows,epochs=providers()
        rows[10]['released_allocations']=rows[10]['released_owners']=0
        with self.assertRaisesRegex(CampaignReconciliationError,'campaign_reconciliation_provider_rewind'):
            provider_epochs(rows,epochs,duration_s=DURATION,native_count=len(rows))

    def test_rewind_away_from_replacement_or_shifted_epoch_cut_refuses(self):
        for fault in ('rewind','cut','owner','epoch','gap'):
            with self.subTest(fault=fault):
                rows,epochs=providers()
                if fault=='rewind':rows[15]['released_allocations']=0
                if fault=='cut':epochs[2]['samples']['provider']+=1
                if fault=='owner':epochs[2]['server_incarnation']=1
                if fault=='epoch':epochs.pop()
                if fault=='gap':rows[5]['elapsed_ns']+=16*SECOND
                with self.assertRaises(CampaignReconciliationError):
                    provider_epochs(rows,epochs,duration_s=DURATION,native_count=len(rows))

    def test_report_keeps_one_history_basis_and_org_scoped_consumer_accounting(self):
        args=observations()
        value=window_observations(*args,duration_s=DURATION)
        self.assertEqual(len(value),96)
        self.assertTrue(all(row['eligible_history_bytes']==1048584 for row in value))
        self.assertTrue(all(row['consumer_backlog']==0 for row in value))
        self.assertTrue(all(row['max_in_flight']==32 for row in value))
        self.assertEqual(value[-1]['resources'],dict.fromkeys(PUBLIC_LIMITS,1))

    def test_fault_window_command_debt_survives_until_actual_later_completion(self):
        workers,progress,resources,logical=observations()
        workers[0]['completed_commands']-=1;workers[0]['transport_attempts']-=1;workers[0]['settled_submission_attempts']-=1
        workers[1]['completed_commands']+=1;workers[1]['transport_attempts']+=1;workers[1]['settled_submission_attempts']+=1
        value=window_observations(workers,progress,resources,logical,duration_s=DURATION)
        self.assertEqual(value[0]['unresolved_commands'],1)
        self.assertEqual(value[1]['unresolved_commands'],0)

    def test_missing_history_coverage_impossible_consumer_or_unsettled_original_load_refuses(self):
        for fault in ('history','consumer','unsettled','future_completion'):
            with self.subTest(fault=fault):
                workers,progress,resources,logical=observations()
                if fault=='history':logical=[row for row in logical if row['elapsed_ns']>=300*SECOND]
                if fault=='consumer':progress[0]['events_checked']+=1
                if fault=='unsettled':workers[-1]['completed_commands']-=1
                if fault=='future_completion':workers[0]['completed_commands']+=1
                with self.assertRaises(CampaignReconciliationError):
                    window_observations(workers,progress,resources,logical,duration_s=DURATION)
