# req: END-003, END-007, END-008, END-010
"""Closed observations cannot erase owner epochs, offered debt or separate bases."""
import unittest

from lib.ticketdesk_lifecycle_campaign_reconciliation import (
    CampaignReconciliationError, provider_epochs, window_observations,
)
from lib.ticketdesk_lifecycle_provider_budget import parse_sample
from test_provider_budget import line
from test_load_stream import window
from test_history_components import records, DURATION
from lib.ticketdesk_lifecycle_campaign_resource_model import PUBLIC_LIMITS

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


class ClosedReconciliationTests(unittest.TestCase):
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
