from pathlib import Path
import hashlib,json,math,struct
STAGES=['loop_drain_ready','loop_work_recv','loop_admit_gate','loop_drain_all_pre','unit_execute','post_submit_enqueue','post_drain_all','post_feedback','loop_residual','exec_queue_telemetry','exec_finish_group','exec_admission','exec_admission_lower','exec_compatibility','exec_conflict_acquire','exec_batch_begin','exec_snapshot_capture','exec_evaluate','exec_detach','exec_prepare_detached','exec_stage_group','exec_audit_prepare','exec_apply','exec_seal','exec_alternate_path','exec_future_box','exec_serial_setup','exec_pool_marshal','exec_stage_serial','exec_group_finalize','exec_drive_unnamed','exec_outer_residual','drive_total','serial_acquire','serial_input_facts','serial_dependency_read','serial_lease','serial_admission','serial_snapshot','serial_evaluate','serial_residual']
STAGES += ['append_authorize','append_begin','append_current','append_epoch','append_build_stage','stage_graph_input','stage_build_records','stage_attach','epoch_derive_indexes','epoch_read_affected','epoch_reserve_assign','attach_match_reserved','attach_storage_stage']
def sha(p):
 with Path(p).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
def bucket_upper(n):
 if n<=16:return n
 base=1<<(n.bit_length()-1);width=base//16
 return base+((n-base)//width+1)*width-1

def validate(report_path,raw_path,log_path):
 report=json.loads(report_path.read_text());raw=json.loads(raw_path.read_text());log=log_path.read_text()
 assert report['correctness']['clean'] is True and len(report['curve'])==1
 assert raw['schema']=='private.wp749-correlated-request-tail/v1' and raw['valid'] is True and raw['dropped_samples']==0
 assert len(raw['workers'])==32
 values=[];identified=[];digest=hashlib.sha256()
 for worker,row in enumerate(raw['workers']):
  assert row['worker']==worker and row['dropped_samples']==0 and 0<len(row['samples_ns'])<=32768
  digest.update(struct.pack('<QQ',worker,len(row['samples_ns'])))
  for ordinal,ns in enumerate(row['samples_ns']):
   assert type(ns) is int and 1<=ns<2**64
   values.append(ns);identified.append((ns,worker,ordinal));digest.update(struct.pack('<Q',ns))
 p=report['curve'][0];h=report['backends'][0]['aggregate']['latency'];w=p['server_stage_evidence']['writer']
 assert len(values)==raw['sample_count']==raw['expected_sample_count']==h['sample_count']==p['logical_ops']
 assert min(values)==h['min_ns'] and max(values)==h['max_ns'] and sum(values)//len(values)==h['mean_ns']
 ordered=sorted(values);raw_percentiles={}
 for percentile in [50,95,99]:
  exact=ordered[(len(values)*percentile+99)//100-1];raw_percentiles[f'p{percentile}_ns']=exact
  assert bucket_upper(exact)==h[f'p{percentile}_ns']==p[f'aggregate_p{percentile}_ns']
 top=[{'elapsed_ns':ns,'worker':worker,'worker_ordinal':ordinal} for ns,worker,ordinal in sorted(identified,key=lambda t:(-t[0],t[1],t[2]))[:20]]
 assert top==[{k:v for k,v in row.items() if k!='commit_sequence'} for row in raw['slowest_20']]
 censuses=[]
 for line in log.splitlines():
  if not line.startswith('riffdb-writer-batch-stages-v1\t'):continue
  fields=dict(part.split('=',1) for part in line.split('\t')[1:])
  assert fields['labels'].split(',')==STAGES
  windows=[]
  for encoded in fields['windows'].split(';'):
   parts=[int(v) for v in encoded.split(',')];assert len(parts)==len(STAGES)+3 and all(v>=0 for v in parts)
   windows.append({'count':parts[0],'stage_ns':parts[1:1+len(STAGES)],'commands':parts[1+len(STAGES)],'submitted_units':parts[2+len(STAGES)]})
  assert 1<=len(windows)<=64 and sum(v['count'] for v in windows)==int(fields['total'])
  assert all(v['count']==256 for v in windows[:-1])
  assert windows[-1]['count']<=256 or len(windows)==64
  censuses.append({'total_iterations':int(fields['total']),'commands':sum(v['commands'] for v in windows),'submitted_units':sum(v['submitted_units'] for v in windows),'windows':windows})
 expected_commands=w['storage_queue_duration_us']['count']
 matches=[c for c in censuses if c['commands']==expected_commands]
 assert len(matches)==1,('ambiguous/missing load census',expected_commands,[(c['total_iterations'],c['commands']) for c in censuses])
 census=matches[0]
 totals={stage:sum(window['stage_ns'][i] for window in census['windows']) for i,stage in enumerate(STAGES)}
 level0={stage:totals[stage] for stage in STAGES[:9]};level1={stage:totals[stage] for stage in STAGES[9:32] if stage!='exec_alternate_path'}
 # Report signed reconciliation; saturated producer residuals are not a proof.
 level1_difference=totals['unit_execute']-sum(level1.values())
 serial_difference=totals['exec_alternate_path']-sum(totals[stage] for stage in STAGES[33:41])
 accounted=sum(level0.values())
 summary={'sample_count':len(values),'raw_array_digest_format':'SHA256 of each worker id and count as little-endian u64, followed by that worker\'s u64 nanosecond values, worker order ascending','raw_arrays_sha256':digest.hexdigest(),'raw_file_sha256':sha(raw_path),'report_sha256':sha(report_path),'log_sha256':sha(log_path),'raw_percentiles':raw_percentiles,'histogram_percentiles':{f'p{q}_ns':h[f'p{q}_ns'] for q in [50,95,99]},'raw_min_ns':min(values),'raw_max_ns':max(values),'raw_mean_ns':sum(values)//len(values),'slowest_20':top,'slowest_20_range_ns':[top[-1]['elapsed_ns'],top[0]['elapsed_ns']],'slowest_20_unique_values':len(set(t['elapsed_ns'] for t in top)),'generation_command_counts':[c['commands'] for c in censuses],'writer_census':{'iterations':census['total_iterations'],'commands':census['commands'],'command_count_match':'writer storage-queue observations; observe_commands and queue telemetry traverse the same group','logical_commands_committed':w['logical_commands_committed'],'submitted_units':census['submitted_units'],'window_width_iterations':256,'retained_windows':len(census['windows']),'last_window_iterations':census['windows'][-1]['count'],'level0_ns':level0,'level0_accounted_iteration_ns':accounted,'level0_percent_of_accounted':{k:v/accounted*100 for k,v in level0.items()},'level0_us_per_iteration':{k:v/census['total_iterations']/1000 for k,v in level0.items()},'level1_ns':level1,'level1_parent_minus_children_ns':level1_difference,'level2_parent_minus_children_ns':serial_difference,'stage_totals_ns':totals,'windows':census['windows'],'limits':'No per-stage call counts, independent continuous writer-wall span or per-request attribution. Level-0 accounted sum is not a separately measured denominator. Saturating residuals and W1/W2 limitations retained.'}}
 return summary
