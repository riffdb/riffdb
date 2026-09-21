from pathlib import Path
import json,sys

def reconcile(root):
 c=json.loads((root/'correlated.json').read_text());report=json.loads((root/'report.json').read_text())
 e=report['curve'][0]['server_stage_evidence'];j=e['writer_journal_stages'];p=e['writer_publication_stages'];f=e['writer_flush_census']
 groups=[]
 for line in (root/'run.log').read_text().splitlines():
  if line.startswith(f"riffdb-correlation-group-v1\tpid={c['load_pid']}\t"):
   groups.append([int(x) for x in line.split('\t')[2].split(',')])
 flushes={}
 for g in groups:
  assert flushes.setdefault(g[7],g[5:7])==g[5:7]
 sync_us=[(b-a)//1000 for a,b in flushes.values()]
 wait_us=[(g[9]-g[8])//1000 for g in groups]
 assert len(flushes)==j['physical_flushes']==f['physical_flushes']
 assert sum(sync_us)==j['sync_sum_us'] and max(sync_us)==j['sync_max_us']
 assert sum(g[14] for g in groups)==p['receipt_wait_calls']
 assert sum(wait_us)<=p['receipt_block_sum_us'] and max(wait_us)<=p['receipt_block_max_us']
 assert p['receipt_wait_errors']==0
 v=dict(all_groups=len(groups),unique_command_flushes=len(flushes),trace_sync_sum_us=sum(sync_us),census_sync_sum_us=j['sync_sum_us'],trace_sync_max_us=max(sync_us),census_sync_max_us=j['sync_max_us'],trace_receipt_wait_calls=sum(g[14] for g in groups),census_receipt_wait_calls=p['receipt_wait_calls'],trace_first_wait_sum_us=sum(wait_us),census_all_wait_sum_us=p['receipt_block_sum_us'],all_wait_minus_first_wait_us=p['receipt_block_sum_us']-sum(wait_us),trace_first_wait_max_us=max(wait_us),census_all_wait_max_us=p['receipt_block_max_us'],journal=j,publication=p,flush=f,limits='Consistency of the same instrumented boundaries; not an independent observer-overhead or continuous wall-clock measurement. Wait/sync overlap, groups share request populations; totals cannot be added to request wall.')
 (root/'reconciliation.json').write_text(json.dumps(v,indent=2)+'\n')
 return v
if __name__=='__main__':print(json.dumps(reconcile(Path(sys.argv[1])),indent=2))
