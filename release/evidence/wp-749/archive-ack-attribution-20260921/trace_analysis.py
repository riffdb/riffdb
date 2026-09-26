from pathlib import Path
import json,sys
from correlate import distribution

def analyze(root):
 c=json.loads((root/'correlated.json').read_text());e=json.loads((root/'report.json').read_text())['curve'][0]['server_stage_evidence']
 groups={}
 for line in (root/'run.log').read_text().splitlines():
  if line.startswith(f"riffdb-correlation-group-v1\tpid={c['load_pid']}\t"):
   g=list(map(int,line.split('\t')[2].split(',')));groups[g[0]]=g
 rows=json.loads((root/'joined-requests.json').read_text());tail=[r for r in rows if r['elapsed_ns']>=c['base']['raw_percentiles']['p99_ns']]
 intervals={'before_sync':[],'inside_sync':[],'after_sync':[]}
 for r in tail:
  g=groups[r['group_id']];a=max(0,min(g[5],g[9])-g[8]);b=max(0,min(g[6],g[9])-max(g[5],g[8]));d=max(0,g[9]-max(g[6],g[8]));assert a+b+d==g[9]-g[8]
  intervals['before_sync'].append(a);intervals['inside_sync'].append(b);intervals['after_sync'].append(d)
 minimum=e['writer_journal_stages']['positional_write_max_us']*1000
 candidates=[g for g in groups.values() if g[5]-g[4]>=minimum]
 proof=dict(max_positional_write_us=e['writer_journal_stages']['positional_write_max_us'],candidate_count=len(candidates),candidate_group_ids=[g[0] for g in candidates],unique=False,method='A successful command-bearing positional-write interval lies inside submission-to-sync-start. Census gives floor(microseconds); reject every group with a shorter enclosing interval. Require one frame per flush before a unique assignment.',max_frames_per_flush=e['writer_flush_census']['max_frames_per_flush'])
 if len(candidates)==1 and proof['max_frames_per_flush']==1:
  g=candidates[0];rr=[r for r in rows if r['group_id']==g[0]]
  proof.update(unique=True,group_id=g[0],first_sequence=g[1],last_sequence=g[2],checkpoint_id=g[13],drain_required=g[12]==1,submit_to_sync_ns=g[5]-g[4],sync_ns=g[6]-g[5],first_wait_ns=g[9]-g[8],measured_requests=rr)
 result=dict(host_valid=json.loads((root/'postflight.json').read_text())['valid'],tail_first_wait_partition={k:distribution(v) for k,v in intervals.items()},maximum_positional_write_enclosure=proof,limits='Actual retained trace and timing-bound consistency only. A host-invalid arm cannot support an archive-presence performance or causal contrast. Kernel/device cause of blocking writes is not instrumented.')
 (root/'trace-analysis.json').write_text(json.dumps(result,indent=2)+'\n');return result
if __name__=='__main__':
 v=analyze(Path(sys.argv[1]));print(json.dumps(v,indent=2))
