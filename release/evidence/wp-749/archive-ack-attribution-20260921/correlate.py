from pathlib import Path
import json,sys,math
from validate_cell import validate

def distribution(values):
 values=sorted(values)
 if not values:return {'count':0}
 return dict(count=len(values),mean_ns=sum(values)/len(values),max_ns=values[-1],**{f'p{p}_ns':values[math.ceil(len(values)*p/100)-1] for p in (50,95,99)})
def correlate(root):
 rawpath=root/'raw/raw-request-tail.json';reportpath=root/'report.json';logpath=root/'run.log'
 base=validate(reportpath,rawpath,logpath)
 raw=json.loads(rawpath.read_text());report=json.loads(reportpath.read_text())
 assert raw['missing_commit_sequences']==0
 samples=[]
 for worker in raw['workers']:
  assert len(worker['samples_ns'])==len(worker['commit_sequences'])
  samples += [(s,n,worker['worker'],i) for i,(s,n) in enumerate(zip(worker['commit_sequences'],worker['samples_ns']))]
 sequences={s for s,_,_,_ in samples};assert len(sequences)==len(samples) and min(sequences)>0
 bypid={}
 for line in logpath.read_text().splitlines():
  if not line.startswith('riffdb-correlation-'):continue
  parts=line.split('\t');pid=int(parts[1].split('=')[1]);entry=bypid.setdefault(pid,dict(groups=[],checkpoints=[],coverage=[]))
  if parts[0]=='riffdb-correlation-group-v1':entry['groups'].append(list(map(int,parts[2].split(','))))
  elif parts[0]=='riffdb-correlation-checkpoint-v1':entry['checkpoints'].append(list(map(int,parts[2].split(','))))
  else:entry['coverage'].append({k:int(v) for k,v in (part.split('=') for part in parts[2:])})
 candidates=[]
 for pid,data in bypid.items():
  lookup={}
  for g in data['groups']:
   assert len(g)==17 and g[3]==g[2]-g[1]+1 and g[1]>0
   for s in range(g[1],g[2]+1):
    assert s not in lookup,(pid,'overlapping sequence',s)
    lookup[s]=g
  if sequences<=lookup.keys():candidates.append((pid,data,lookup))
 assert len(candidates)==1,('ambiguous load process',[(pid,len(d['groups'])) for pid,d in bypid.items()])
 pid,data,lookup=candidates[0]
 coverage=data['coverage'][-1];assert coverage['final']==1
 assert coverage['groups']==coverage['reserved_groups']==len(data['groups'])
 assert coverage['checkpoints']==coverage['reserved_checkpoints']==len(data['checkpoints'])
 assert coverage['dropped_groups']==coverage['dropped_checkpoints']==0
 assert [g[0] for g in data['groups']]==list(range(1,len(data['groups'])+1))
 cps={c[0]:c for c in data['checkpoints']}
 assert len(cps)>0 and [c[0] for c in data['checkpoints']]==list(range(1,len(cps)+1))
 for c in cps.values():assert len(c)==9 and 0<c[1]<=c[2]<=c[3] and c[8]==0
 evidence=report['curve'][0]['server_stage_evidence'];writer=evidence['writer'];counts=evidence['writer_checkpoint_census']
 assert len(data['groups'])==writer['physical_commits']
 assert sum(g[3] for g in data['groups'])==writer['logical_commands_committed']
 assert len(cps)==counts['starts'] and counts['failures']==0
 assert sum(c[4] for c in cps.values())==counts['selected_transitions']
 assert sum(c[5] for c in cps.values())==counts['selected_commands']
 assert sum(c[6] for c in cps.values())==counts['selected_bytes']
 assert min(c[4] for c in cps.values())==counts['min_transitions'] and max(c[4] for c in cps.values())==counts['max_transitions']
 assert min(c[6] for c in cps.values())==counts['min_bytes'] and max(c[6] for c in cps.values())==counts['max_bytes']
 for bit,key in enumerate(('transitions','bytes','physical','reanchor')):assert sum(bool(c[7] & (1<<bit)) for c in cps.values())==counts['trigger_counts_nonexclusive'][key]
 # The ordinary checkpoint census is sampled before graceful close; the final
 # trace is drained after close finishes any already registered checkpoint.
 assert counts['materialized']<=len(cps) and counts['installed']<=len(cps)
 flushes={}
 for g in data['groups']:
  assert g[15]==0 and 0<g[4]<=g[5]<=g[6]<=g[10]<=g[11]
  assert g[7]>0 and g[12] in (1,2)
  if g[14]:assert 0<g[8]<=g[9]<=g[10]
  else:assert g[8]==g[9]==0
  assert g[13] in cps or g[13]==0
  assert g[16] in cps or g[16]==0
  if g[12]==1:assert g[13]>0
  prev=flushes.setdefault(g[7],g[5:7]);assert prev==g[5:7]
  if g[13]:assert cps[g[13]][1]<=g[10] and g[4]<=cps[g[13]][3]
 def detail(sample):
  s,n,w,i=sample;g=lookup[s];c=cps.get(g[13])
  return dict(commit_sequence=s,elapsed_ns=n,worker=w,worker_ordinal=i,group_id=g[0],commands=g[3],drain_required=g[12]==1,checkpoint_id=g[13],checkpoint_at_submission=g[16],first_wait_ns=g[9]-g[8],sync_ns=g[6]-g[5],publication_ns=g[11]-g[10],submit_to_publication_ns=g[11]-g[4],flush_id=g[7],wait_calls=g[14],sync_wait_overlap_ns=max(0,min(g[6],g[9])-max(g[5],g[8])),checkpoint_registered_ns=(c[3]-c[1]) if c else None,checkpoint_materialize_ns=(c[2]-c[1]) if c else None)
 rows=[detail(s) for s in samples];rows.sort(key=lambda r:(-r['elapsed_ns'],r['worker'],r['worker_ordinal']))
 p99=base['raw_percentiles']['p99_ns'];tail=[r for r in rows if r['elapsed_ns']>=p99]
 groups={lookup[s][0]:lookup[s] for s in sequences}
 result=dict(base=base,load_pid=pid,coverage=coverage,generation_counts={pid:len(d['groups']) for pid,d in bypid.items()},measurement_sequence_range=[min(sequences),max(sequences)],measured_groups=len(groups),checkpoints=data['checkpoints'],checkpoint_census_before_close=counts,slowest_20=rows[:20],p99_tail_count=len(tail),p99_tail_drain_count=sum(r['drain_required'] for r in tail),all_drain_count=sum(r['drain_required'] for r in rows),request_latency_by_drain={str(flag):distribution([r['elapsed_ns'] for r in rows if r['drain_required']==flag]) for flag in (False,True)},tail_intervals={k:distribution([r[k] for r in tail]) for k in ('first_wait_ns','sync_ns','publication_ns','submit_to_publication_ns','sync_wait_overlap_ns')},group_intervals={str(flag):{k:distribution([g[b]-g[a] for g in groups.values() if (g[12]==1)==flag]) for k,a,b in [('first_wait_ns',8,9),('sync_ns',5,6),('publication_ns',10,11),('submit_to_publication_ns',4,11)]} for flag in (False,True)},limits='One instrumented run; no counterfactual or production delta. Client and server clocks are not compared; sequence joins only. Wait, sync and publication may overlap across groups and are not additive. Writer census accounted wall is not independent wall; retain reconciliation limits.')
 (root/'correlated.json').write_text(json.dumps(result,indent=2)+'\n')
 (root/'joined-requests.json').write_text(json.dumps(rows)+'\n')
 return result
if __name__=='__main__':
 r=correlate(Path(sys.argv[1]));print(json.dumps({k:v for k,v in r.items() if k not in ('base','checkpoints','slowest_20')},indent=2))
