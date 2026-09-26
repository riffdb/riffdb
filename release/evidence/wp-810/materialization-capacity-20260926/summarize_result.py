# req: PERF-018
"""Summarize a validated WP-810 diagnostic; overlap is not device causation."""
from pathlib import Path
from collections import defaultdict
import argparse
import hashlib
import json
from analyze_materialization import Intervals, distributions

def summarize(cell):
    analysis_path = cell / 'materialization-analysis.json'
    for name, limit in (('materialization-analysis.json', 64 << 20),
                        ('joined-requests.json', 512 << 20), ('run.log', 2048 << 20)):
        assert (cell / name).stat().st_size <= limit, 'evidence exceeds input budget'
    a = json.loads(analysis_path.read_text())
    assert a['coverage']['final'] == 1
    assert all(a['coverage']['dropped_' + k] == 0 for k in ('groups', 'checkpoints', 'io'))
    assert hashlib.file_digest((cell/'run.log').open('rb'), 'sha256').hexdigest() == a['log_sha256']
    joined = json.loads((cell/'joined-requests.json').read_text())
    assert 0 < len(joined) == a['populations']['requests'] <= 32 * 32768
    by_process = defaultdict(lambda: {'groups': [], 'checkpoints': []})
    with (cell/'run.log').open() as stream:
        for line in stream:
            if not line.startswith(('riffdb-wp810-group-v1\t', 'riffdb-wp810-checkpoint-v1\t')):
                continue
            assert len(line) < 4096
            label, pid, payload = line.rstrip().split('\t')
            assert len(by_process) < 32 or pid in by_process
            key = 'groups' if 'group' in label else 'checkpoints'
            rows = by_process[pid][key]
            assert len(rows) < (65536 if key == 'groups' else 512)
            rows.append([int(x) for x in payload.split(',')])
    lo, hi = min(r['sequence'] for r in joined), max(r['sequence'] for r in joined)
    candidates = [p for p in by_process.values() if p['groups'] and p['groups'][0][1] <= lo <= hi <= p['groups'][-1][2]]
    assert len(candidates) == 1
    groups, cps = candidates[0]['groups'], candidates[0]['checkpoints']
    assert len(groups) == a['coverage']['groups'] and len(cps) == a['coverage']['checkpoints']
    measured = {r['group'] for r in joined}
    flushes = {g[7]: g for g in groups if g[0] in measured}
    assert len(flushes) == a['populations']['measured_flushes']
    assert distributions([g[6]-g[5] for g in flushes.values()]) == a['measured_sync']
    cp_sync = Intervals([(x['start_ns'], x['end_ns']) for x in a['checkpoint_sync_context']])
    cp_active = Intervals([(c[1], c[3]) for c in cps])
    materializing = Intervals([(c[1], c[2]) for c in cps])
    aggregates = []
    for threshold in (10, 50, 100):
        rows = [e['sync'] for e in a['slow_syncs_over_10ms'] if e['sync']['duration_ns'] > threshold * 1_000_000]
        overlap = [cp_sync.overlap(e['start_ns'], e['end_ns']) for e in rows]
        aggregates.append(dict(threshold_ms=threshold, count=len(rows), duration_ns=sum(e['duration_ns'] for e in rows),
            checkpoint_sync_overlap_ns=sum(overlap),
            checkpoint_sync_any=sum(n > 0 for n in overlap),
            checkpoint_sync_full=sum(n == e['duration_ns'] for n,e in zip(overlap,rows)),
            checkpoint_phase_overlap_ns={k: sum(e['checkpoint_phase_overlap_ns'][k] for e in rows) for k in rows[0]['checkpoint_phase_overlap_ns']},
            without_observed_engine_io_ns=sum(e['without_observed_engine_io_ns'] for e in rows)))
    longest = max(flushes.values(),key=lambda g: g[6]-g[5])
    longest_groups = [g for g in groups if g[7]==longest[7]]
    longest_group_ids={g[0] for g in longest_groups}
    relevant_requests=[r for r in joined if r['group'] in longest_group_ids]
    witness=dict(flush=longest[7], journal_sync_start_ns=longest[5], journal_sync_end_ns=longest[6],
        journal_sync_ns=longest[6]-longest[5], journal_write_ns=longest[19]-longest[18],
        groups=[dict(group=g[0],first_sequence=g[1],last_sequence=g[2],commands=g[3],
                     first_wait_start_ns=g[8],first_wait_end_ns=g[9],first_wait_ns=g[9]-g[8],
                     queue_ns=g[17]-g[4],publication_ns=g[11]-g[10]) for g in longest_groups],
        checkpoint_syncs=[x for x in a['checkpoint_sync_context'] if x['start_ns'] < longest[6] and x['end_ns'] > longest[5]],
        joined_request_durations=distributions([r['elapsed_ns'] for r in relevant_requests]))
    without_cp=[g[6]-g[5] for g in flushes.values() if cp_active.overlap(g[5],g[6])==0]
    without_materialization=[g[6]-g[5] for g in flushes.values() if materializing.overlap(g[5],g[6])==0]
    slowest=a['slowest_20_joined_requests'][0]
    sg=groups[slowest['group']-1]
    queue_overlap=[]
    all_flushes={g[7]:g for g in groups}
    for f,g in all_flushes.items():
        overlap=max(0,min(sg[17],g[6])-max(sg[4],g[5]))
        if overlap: queue_overlap.append(dict(flush=f,sync_ns=g[6]-g[5],overlap_with_queue_ns=overlap))
    return dict(schema='riffdb.wp810-materialization-derived/v1',
        analysis_sha256=hashlib.file_digest(analysis_path.open('rb'),'sha256').hexdigest(),
        scope='One validated instrumented diagnostic; checkpoint sync overlap is association, not kernel/device causation or mitigation effect.',
        slow_journal_syncs=aggregates,
        measured_sync_without_registered_checkpoint=distributions(without_cp),
        measured_sync_without_materialization=distributions(without_materialization),
        longest_sync_witness=witness,
        slowest_request_queue_witness=dict(request=slowest,own_sync_ns=sg[6]-sg[5],prior_sync_queue_overlap=queue_overlap),
        checkpoint_growth_syncs=[x for x in a['checkpoint_sync_context'] if x['immediately_preceded_by_resize']],
        all_phase_totals_include_shutdown=True,
        max_installation_checkpoint=max(cps,key=lambda c:c[3]-c[2]))

if __name__ == '__main__':
    p=argparse.ArgumentParser();p.add_argument('cell',type=Path);p.add_argument('output',type=Path)
    args=p.parse_args();args.output.write_text(json.dumps(summarize(args.cell),indent=2)+'\n')
