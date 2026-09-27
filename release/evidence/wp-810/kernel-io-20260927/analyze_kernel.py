"""Bounded kernel trace validation. Block issue/complete records are not paired."""
from pathlib import Path
from collections import Counter,defaultdict
import argparse,json,re

LIMIT=512*1024*1024

def union_size(rows):
    total=0;last=0
    for lo,hi in sorted(rows):
        assert lo<=hi
        total+=max(0,hi-max(lo,last));last=max(last,hi)
    return total

def parse_trace(path,stderr):
    assert path.stat().st_size<=LIMIT,'kernel log bound'
    assert not stderr.read_text().strip(),'kernel tracer stderr is nonempty'
    syncs={};subs=defaultdict(list);ext={};jbd=[];blocks=Counter();counts={};seen=Counter();stacks={}
    header=footer=None;stack_key=None;stack=[];rows=0
    with path.open() as stream:
        for line in stream:
            assert len(line)<=8192,'line bound';line=line.rstrip()
            if stack_key is not None:
                if line=='ENDSTACK':
                    assert stack_key not in stacks,'duplicate stack'
                    stacks[stack_key]=stack;stack_key=None;stack=[]
                else:
                    assert len(stack)<24,'stack bound';stack.append(line.strip())
                continue
            if not line: continue
            if line.startswith('@counts['):
                m=re.fullmatch(r'@counts\[(\d+)\]: (\d+)',line);assert m,'bad count'
                key,value=map(int,m.groups());assert key not in counts,'duplicate count';counts[key]=value;continue
            p=line.split('\t');kind=p[0]
            if kind=='STACK':
                assert len(p)==5;stack_key=tuple(map(int,p[1:]));continue
            rows+=1;assert rows<=4_000_000,'record bound'
            if kind in ('HEADER','FOOTER'):
                assert len(p)==3 and p[1]=='1';ns=int(p[2])
                if kind=='HEADER': assert header is None;header=ns
                else: assert footer is None;footer=ns
            elif kind=='SYNC':
                assert len(p)==11
                pid,tid,role,fd,seq,start,end,ret,dev,ino=map(int,p[1:]);key=(pid,tid,seq)
                assert key not in syncs and role in (1,2) and ret==0 and dev and ino and 0<start<=end,'invalid sync'
                syncs[key]=dict(pid=pid,tid=tid,sequence=seq,role=role,fd=fd,start_ns=start,end_ns=end,dev=dev,inode=ino)
                seen[2]+=1;assert len(syncs)<=100_000,'sync bound'
            elif kind=='SUB':
                assert len(p)==9
                pid,tid,seq,stage,start,end,ret,tx=map(int,p[1:]);assert stage in (1,2,3) and ret==0 and 0<start<=end,'invalid subcall'
                subs[(pid,tid,seq)].append(dict(stage=stage,start_ns=start,end_ns=end,transaction=tx))
                assert len(subs[(pid,tid,seq)])<=16,'subcall bound';seen[stage*2+4]+=1
            elif kind=='EXT4':
                assert len(p)==9
                pid,tid,seq,start,end,dev,ino,ret=map(int,p[1:]);key=(pid,tid,seq)
                assert key not in ext and ret==0 and 0<start<=end,'invalid ext4'
                ext[key]=(start,end,dev,ino);seen[4]+=1
            elif kind=='JBD':
                assert len(p)==5
                stage,ns,dev,tx=map(int,p[1:]);assert stage in (1,2,3,4)
                jbd.append((stage,ns,dev,tx));seen[stage+10]+=1;assert len(jbd)<=500_000,'jbd bound'
            elif kind=='BLOCK':
                assert len(p)==8
                stage,ns,dev,sector,n=map(int,p[1:6]);flags=p[6];err=int(p[7]);assert stage in (1,2) and ns>0 and err==0,'invalid block event'
                blocks[(stage,flags)]+=1;seen[stage+14]+=1
            else: raise AssertionError('unknown/error record: '+kind)
    assert stack_key is None and header and footer and header<footer,'incomplete trace'
    for a,b in ((1,2),(3,4),(5,6),(7,8),(9,10)):
        assert counts.get(a,0)==counts.get(b,0)==seen[b],('entry/return/retention mismatch',a,b)
    for n in range(11,17): assert counts.get(n,0)==seen[n],('retention mismatch',n)
    assert set(ext)==set(syncs),'missing ext4 operation'
    assert set(subs)<=set(syncs),'orphan subcall'
    for key,s in syncs.items():
        assert header<=s['start_ns']<=s['end_ns']<=footer
        lo,hi,dev,ino=ext[key];assert s['start_ns']<=lo<=hi<=s['end_ns'] and (dev,ino)==(s['dev'],s['inode']),'ext4 containment'
        for row in subs[key]: assert lo<=row['start_ns']<=row['end_ns']<=hi,'subcall outside ext4'
        duration=s['end_ns']-s['start_ns'];intervals=[(r['start_ns'],r['end_ns']) for r in subs[key]]
        s.update(duration_ns=duration,subcalls=subs[key],unattributed_ns=duration-union_size(intervals))
    return dict(syncs=list(syncs.values()),jbd=jbd,stacks={','.join(map(str,k)):v for k,v in stacks.items()},counts=counts,block_counts=[dict(stage=k[0],flags=k[1],count=v) for k,v in sorted(blocks.items())])

def summarize(trace,pid=None):
    result=[]
    for role in (1,2):
        rows=[r for r in trace['syncs'] if r['role']==role and (pid is None or r['pid']==pid)]
        totals=[]
        for threshold in (0,10,50,100):
            selected=[r for r in rows if r['duration_ns']>threshold*1_000_000]
            totals.append(dict(threshold_ms=threshold,count=len(selected),duration_ns=sum(r['duration_ns'] for r in selected),
                subcall_union_ns={str(stage):sum(union_size([(s['start_ns'],s['end_ns']) for s in r['subcalls'] if s['stage']==stage]) for r in selected) for stage in (1,2,3)},
                unattributed_ns=sum(r['unattributed_ns'] for r in selected)))
        result.append(dict(role=role,thresholds=totals,longest=sorted(rows,key=lambda r:-r['duration_ns'])[:20]))
    return dict(schema='riffdb.wp810-kernel-io/v1',counts=trace['counts'],block_counts=trace['block_counts'],
        roles=result,stacks=trace['stacks'],scope='Native sync subcalls, inclusive time with union-based residual. Block issue/completion populations are retained independently; partial completion and boundary events prevent simple latency pairing.')

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('log',type=Path);p.add_argument('stderr',type=Path);p.add_argument('output',type=Path);p.add_argument('--pid',type=int)
    args=p.parse_args();t=parse_trace(args.log,args.stderr);args.output.write_text(json.dumps(summarize(t,args.pid),indent=2)+'\n')
