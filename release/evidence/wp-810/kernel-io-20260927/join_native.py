"""Validate journal ordinal identity and a common clock-offset interval."""
from pathlib import Path
import json
from analyze_kernel import parse_trace,summarize

def pair_calls(flushes,calls):
    ordered=sorted(flushes.items())
    assert len(calls)==len(flushes)>0
    assert all(a['end_ns']<=b['start_ns'] for a,b in zip(calls,calls[1:]))
    assert all(a[1][1]<=b[1][0] for a,b in zip(ordered,ordered[1:]))
    low=max(k['end_ns']-u[1] for k,(_,u) in zip(calls,ordered))
    high=min(k['start_ns']-u[0] for k,(_,u) in zip(calls,ordered))
    assert low<=high,('no common enclosing clock offset',low,high)
    return {f:k for k,(f,_) in zip(calls,ordered)},high-low

def join(root):
    cell=root/'results/01-materialization'
    app=json.loads((cell/'materialization-analysis.json').read_text())
    native=parse_trace(root/'kernel.log',root/'kernel.stderr')
    pid=json.loads((root/'kernel-analysis.json').read_text())['load_pid']
    groups=[]
    with (cell/'run.log').open() as stream:
        for line in stream:
            if line.startswith(f'riffdb-wp810-group-v1\tpid={pid}\t'):
                assert len(groups)<65536
                groups.append([int(x) for x in line.rstrip().split('\t')[2].split(',')])
    assert len(groups)==app['coverage']['groups']
    flushes={}
    for g in groups:
        pair=(g[5],g[6])
        assert g[7] not in flushes or flushes[g[7]]==pair
        flushes[g[7]]=pair
    calls=sorted([s for s in native['syncs'] if s['pid']==pid and s['role']==1],key=lambda s:s['start_ns'])
    assert len(calls)==len(flushes)==app['journal_census']['physical_flushes']
    mapping,width=pair_calls(flushes,calls)
    joined_path=cell/'joined-requests.json';assert joined_path.stat().st_size<=512*1024*1024
    requests=json.loads(joined_path.read_text());measured={r['flush'] for r in requests}
    assert len(measured)==app['populations']['measured_flushes']
    scoped={**native,'syncs':[mapping[f] for f in sorted(measured)]}
    summary=summarize(scoped,pid)
    longest=max(measured,key=lambda f:mapping[f]['duration_ns'])
    receipt_groups=[dict(group=g[0],commands=g[3],first_wait_ns=g[9]-g[8],queue_ns=g[17]-g[4]) for g in groups if g[7]==longest]
    summary['join']=dict(method='All ordered journal syscalls pair one-to-one with application flushes; one offset interval encloses every kernel call within its corresponding application sync. No client clock subtraction.',paired_flushes=len(calls),clock_offset_interval_width_ns=width,measured_flushes=len(measured),longest_measured_flush=longest,receipt_groups=receipt_groups)
    return summary
if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);p.add_argument('output',type=Path);a=p.parse_args();a.output.write_text(json.dumps(join(a.root),indent=2)+'\n')
