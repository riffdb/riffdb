from pathlib import Path
import hashlib,re
FIELDS={'seed','generator_version','plan_digest','commands_applied','batches_committed','crashes','recoveries','recovery_window_crashes','torn_decisions','max_torn_in_one_recovery','in_flight_commit_present','in_flight_commit_absent','in_flight_admit_present','in_flight_admit_absent','initialization_rolled_back','initialization_survived','catalog_activation_rolled_back','catalog_activation_survived','oracle_comparisons','final_frontier'}
def parse(path):
 assert path.stat().st_size<16*1024*1024
 result={}
 for line in path.read_text().splitlines():
  if not line.startswith('corpus-audit\t'):continue
  parts=line.split('\t');seed=parts[1].split('=',1)[1]
  assert seed not in result
  if len(parts)==3:
   assert parts[2]=='completed (pinned as a wedge)'
   result[seed]={'historical_engine_defect':'completed'}
   continue
  meta=dict(p.split('=',1) for p in parts[2:9])
  assert set(meta)=={'rotation','torn','absent','present','admit','window','init'}
  verdicts=next(p for p in parts if p.startswith(('+','-','=','!'))).split()
  report_part=next(p for p in parts if p.startswith('report=CampaignReport { '))
  report={k:int(v) for k,v in re.findall(r'([a-z_]+): (\d+)',report_part)}
  assert set(report)==FIELDS,(seed,set(report)^FIELDS)
  assert report['seed']==int(seed,16)
  assert int(meta['torn'])==report['torn_decisions']
  assert int(meta['absent'])==report['in_flight_commit_absent']
  assert int(meta['present'])==report['in_flight_commit_present']
  assert int(meta['admit'])==report['in_flight_admit_absent']+report['in_flight_admit_present']
  assert int(meta['window'])==report['recovery_window_crashes']
  assert int(meta['init'])==report['initialization_rolled_back']+report['initialization_survived']
  assert report['final_frontier']==report['commands_applied']==18
  rotation=next(p.split('=',1)[1] for p in parts if p.startswith('rotation='))
  assert rotation in ('none','restored','rotated')
  holds=all(v.startswith(('+','=')) for v in verdicts)
  mismatch=holds if rotation=='rotated' else not holds
  result[seed]={'rotation':rotation,'expectation_verdicts':verdicts,'exact_territory_holds':holds,'receipt_mismatch':mismatch,'report':report}
 assert len(result)==21,len(result)
 return result

def digest(path):
 with path.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
if __name__=='__main__':
 import json,sys
 for name in sys.argv[1:]:
  rows=parse(Path(name))
  print(name)
  for seed,r in rows.items():
   if r.get('receipt_mismatch'):print(seed,r['rotation'],r['expectation_verdicts'])
