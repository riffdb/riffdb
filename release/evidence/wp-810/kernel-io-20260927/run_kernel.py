"""One retained-binary kernel diagnostic; no automatic retry."""
from pathlib import Path
import datetime,hashlib,json,os,shutil,signal,subprocess,sys,tempfile,time
import build_run as run
from isolate_host import prepare,restore
from analyze_materialization import analyze
from analyze_kernel import parse_trace,summarize
ROOT=Path(__file__).resolve().parent

def write(name,value):run.write(ROOT/name,value)
def kill_group(p):
    if p is None or p.poll() is not None:return
    subprocess.run(['sudo','-n','kill','-INT','--',f'-{p.pid}'],check=True)
    try: p.wait(timeout=20)
    except subprocess.TimeoutExpired:
        subprocess.run(['sudo','-n','kill','-KILL','--',f'-{p.pid}'],check=True);p.wait(timeout=10)

def main():
    tracer=None;ram=None;status=dict(started_at=run.now(),pid=os.getpid(),state='preparing')
    deadline=time.monotonic()+1800
    run.remaining=lambda:max(0,deadline-time.monotonic())
    try:
        assert os.getpriority(os.PRIO_PROCESS,0)==0
        for flag in ('sha_ni','aes','pclmulqdq','sse4_2'):assert flag in Path('/proc/cpuinfo').read_text().split()
        write('host-inventory.json',dict(kernel=subprocess.check_output(['uname','-r'],text=True).strip(),
            bpftrace=subprocess.check_output(['bpftrace','--version'],text=True).strip(),
            filesystem=json.loads(subprocess.check_output(['findmnt','-J','-T',str(ROOT)],text=True))))
        freeze=json.loads((ROOT/'source-freeze.json').read_text());receipt=json.loads((ROOT/'build-complete.json').read_text());gate=json.loads((ROOT/'local-functional-qualified.json').read_text())
        for name,digest in json.loads((ROOT/'inputs.json').read_text()).items():assert run.sha(ROOT/name)==digest,name
        run.verify_local_qualification(freeze,receipt,gate,run.sha(ROOT/'binaries/riffdbd'),run.sha(ROOT/'binaries/runner'))
        assert os.major(os.stat(ROOT).st_dev)==259
        env=run.environment();env.update(RIFFDB_APP_BASELINE_ARCHIVE_COLLECTION='disabled')
        source=ROOT/'source'
        subprocess.run(['git','clone','--no-checkout',str(ROOT/'source.bundle'),str(source)],check=True,timeout=60)
        subprocess.run(['git','checkout','--detach',freeze['revision']],cwd=source,check=True,timeout=60)
        subprocess.run(['git','apply','--check',str(ROOT/'observer.patch')],cwd=source,check=True,timeout=30)
        subprocess.run(['git','apply',str(ROOT/'observer.patch')],cwd=source,check=True,timeout=30)
        prepare()
        ram=Path(tempfile.mkdtemp(prefix='wp810-kernel-',dir='/dev/shm'))
        write('trace-spool.json',dict(directory=str(ram),limit_bytes=512*1024*1024))
        with (ram/'kernel.log').open('x') as out,(ROOT/'kernel.stderr').open('x') as err:
            tracer=subprocess.Popen(['sudo','-n','bpftrace','-q','-B','line',str(ROOT/'kernel_trace.bt')],stdout=out,stderr=err,start_new_session=True)
            ready=time.monotonic()+60
            while 'HEADER\t' not in (ram/'kernel.log').read_text():
                assert tracer.poll() is None,'tracer exited before ready'
                assert time.monotonic()<ready,'tracer startup timeout';time.sleep(.1)
            # Keep the same observer process present at both host-validity endpoints.
            status.update(state='measuring',tracer_pid=tracer.pid);write('measurement-started.json',status)
            original=run.run_in_ram
            def traced_cell(command,cell,environment,cell_deadline_seconds=900):
                # Reuse the qualified spool/retention and enforce a second log bound.
                import threading
                stopped=threading.Event();failure=[]
                def watch():
                    while not stopped.wait(.5):
                        if tracer.poll() is not None or (ram/'kernel.log').stat().st_size>512*1024*1024:
                            failure.append(dict(tracer_returncode=tracer.poll(),kernel_log_bytes=(ram/'kernel.log').stat().st_size))
                            write('trace-watchdog-failure.json',failure[-1])
                            os.kill(os.getpid(),signal.SIGTERM);return
                watcher=threading.Thread(target=watch,daemon=True);watcher.start()
                try:original(command,cell,environment,cell_deadline_seconds)
                finally:stopped.set();watcher.join(timeout=2)
                assert not failure,failure
            run.run_in_ram=traced_cell
            # The original analyzer runs after postflight. Stop the kernel observer
            # then, before its own result writes can enter the trace.
            original_validity=run.validity
            def validity(path,environment,baseline=None):
                original_validity(path,environment,baseline)
                if baseline is not None:kill_group(tracer)
            run.validity=validity
            run.measure(env,receipt)
        assert tracer.returncode==0
        shutil.copy2(ram/'kernel.log',ROOT/'kernel.log')
        native=parse_trace(ROOT/'kernel.log',ROOT/'kernel.stderr')
        cell=ROOT/'results/01-materialization'
        raw=json.loads((cell/'raw/raw-request-tail.json').read_text());seqs=[x for w in raw['workers'] for x in w['commit_sequences']]
        low,high=min(seqs),max(seqs);ranges={}
        with (cell/'run.log').open() as stream:
            for line in stream:
                if line.startswith('riffdb-wp810-group-v1\t'):
                    _,p,row=line.rstrip().split('\t');v=[int(x) for x in row.split(',')];pid=int(p[4:])
                    ranges.setdefault(pid,[v[1],v[2]])[1]=v[2]
        candidates=[pid for pid,(lo,hi) in ranges.items() if lo<=low<=high<=hi];assert len(candidates)==1
        result=summarize(native,candidates[0]);result['load_pid']=candidates[0]
        write('kernel-analysis.json',result)
        status.update(state='complete',kernel_log_sha256=run.sha(ROOT/'kernel.log'),load_pid=candidates[0])
    except BaseException as error:
        status.update(state='failed',error_type=type(error).__name__,reason=str(error));raise
    finally:
        kill_group(tracer)
        if ram is not None:
            if (ram/'kernel.log').exists() and not (ROOT/'kernel.log').exists():shutil.copy2(ram/'kernel.log',ROOT/'kernel.log')
            shutil.rmtree(ram)
        restore();status.update(finished_at=run.now(),isolation_restored=True)
        write('supervisor-status.json',status)

if __name__=='__main__':
    signal.signal(signal.SIGTERM,run.interrupted)
    main()
