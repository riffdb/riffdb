from pathlib import Path
import datetime, hashlib, json, os, shutil, signal, subprocess, sys, time

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT/'source'

def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def sha(path):
    with Path(path).open('rb') as f: return hashlib.file_digest(f, 'sha256').hexdigest()
def write(path, data):
    with Path(path).open('x') as f: f.write(json.dumps(data, indent=2)+'\n')

def environment():
    env = {k:v for k,v in os.environ.items() if not k.startswith(('RIFFDB_', 'CARGO_', 'RUST'))}
    env.update(PATH=str(Path.home()/'.cargo/bin')+':/usr/local/bin:/usr/bin:/bin', LC_ALL='C', TZ='UTC',
        RUSTFLAGS='-C target-cpu=x86-64', CARGO_BUILD_JOBS='6', CARGO_INCREMENTAL='0', RUSTC_WRAPPER='',
        RUSTUP_TOOLCHAIN='1.98.1', CARGO_PROFILE_RELEASE_LTO='thin', CARGO_PROFILE_RELEASE_CODEGEN_UNITS='1',
        CARGO_TARGET_DIR=str(ROOT/'build-target'))
    return env

def execute(command, cwd, env, log_name, maximum=3000):
    print('starting', log_name, now(), flush=True)
    started=time.monotonic()
    with (ROOT/log_name).open('x') as output:
        child=subprocess.Popen(command,cwd=cwd,env=env,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
        try: code=child.wait(timeout=maximum)
        except BaseException:
            os.killpg(child.pid,signal.SIGTERM)
            try: child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid,signal.SIGKILL)
                child.wait()
            write(ROOT/(log_name+'.interrupted.json'),dict(at=now(),elapsed_seconds=time.monotonic()-started))
            raise
    write(ROOT/(log_name+'.execution.json'),dict(command=command,exit_code=code,elapsed_seconds=time.monotonic()-started,finished_at=now()))
    print('finished',log_name,code,now(),flush=True)
    assert code==0,(log_name,code)

def build(env, freeze):
    for name,digest in freeze['inputs'].items(): assert sha(ROOT/name)==digest,name
    for flag in ('sha_ni','aes','pclmulqdq','sse4_2'): assert flag in Path('/proc/cpuinfo').read_text().split(),flag
    compiler=subprocess.check_output(['rustc','-Vv'],env=env,text=True)
    assert compiler.startswith('rustc 1.98.1 ')
    execute(['git','clone','--no-checkout',str(ROOT/'source.bundle'),str(SOURCE)],ROOT,env,'clone.log')
    execute(['git','checkout','--detach',freeze['source_revision']],SOURCE,env,'checkout.log')
    assert subprocess.check_output(['git','status','--porcelain'],cwd=SOURCE,text=True)==''
    execute(['cargo','build','--locked','--release','-p','riffdb-server','--bin','riffdbd'],SOURCE,env,'daemon-build.log')
    binaries=ROOT/'binaries'
    binaries.mkdir()
    shutil.copy2(ROOT/'build-target/release/riffdbd',binaries/'riffdbd')
    execute(['git','apply','--check',str(ROOT/'runner-raw-samples.patch')],SOURCE,env,'patch-check.log')
    execute(['git','apply',str(ROOT/'runner-raw-samples.patch')],SOURCE,env,'patch-apply.log')
    assert subprocess.check_output(['git','diff','--name-only','--','crates'],cwd=SOURCE,text=True)==''
    execute(['cargo','test','--locked','--manifest-path','examples/app-baseline/Cargo.toml','-p','riffdb-app-baseline-core','raw_tail'],SOURCE,env,'raw-sample-tests.log')
    execute(['cargo','build','--locked','--release','--manifest-path','examples/app-baseline/Cargo.toml','-p','riffdb-app-baseline','--bin','riffdb-app-baseline'],SOURCE,env,'runner-build.log')
    shutil.copy2(ROOT/'build-target/release/riffdb-app-baseline',binaries/'runner')
    receipt=dict(completed_at=now(),source_freeze=freeze,compiler=compiler,daemon_sha256=sha(binaries/'riffdbd'),runner_sha256=sha(binaries/'runner'),
        locks={p:sha(SOURCE/p) for p in ['Cargo.lock','examples/app-baseline/Cargo.lock']},
        build_environment={k:v for k,v in env.items() if k.startswith(('CARGO_','RUST'))},
        daemon_source_unmodified=True,runner_patch_sha256=sha(ROOT/'runner-raw-samples.patch'))
    write(ROOT/'build-complete.json',receipt)
    return receipt

def validity(path,env,baseline=None):
    cmd=[str(SOURCE/'scripts/app-baseline-host-validity')]
    if baseline: cmd+=['--baseline-report',str(baseline)]
    with path.open('x') as log: code=subprocess.run(cmd,cwd=SOURCE,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=30).returncode
    assert code==0 and json.loads(path.read_text())['valid'] is True,path.name

def measure(env,receipt):
    from analyze import analyze,compare
    from isolate_host import prepare,restore
    out=ROOT/'results'
    out.mkdir()
    assert os.readlink('/proc/self/ns/pid')==subprocess.check_output(['sudo','-n','readlink','/proc/1/ns/pid'],text=True).strip()
    try:
        prepare()
        for ordinal,mode in enumerate(('disabled','enabled'),1):
            cell=out/f'{ordinal:02}-{mode}'
            cell.mkdir()
            for name in ('raw','tmp'): (cell/name).mkdir()
            runner=ROOT/'binaries/runner'
            daemon=ROOT/'binaries/riffdbd'
            assert sha(daemon)==receipt['daemon_sha256'] and sha(runner)==receipt['runner_sha256']
            cellenv={k:v for k,v in env.items() if not k.startswith(('CARGO_','RUST'))}
            cellenv.update(RUSTUP_TOOLCHAIN='1.98.1',RIFFDB_REDB_COMMIT_PROFILE='standard',RIFFDB_TMP_ROOT=str(cell/'tmp'),
                RIFFDB_APP_BASELINE_RIFFDBD_BIN=str(daemon),RIFFDB_APP_BASELINE_RUNNER_BIN=str(runner),
                RIFFDB_APP_BASELINE_ARCHIVE_COLLECTION=mode,RIFFDB_APP_BASELINE_RAW_TAIL_DIR=str(cell/'raw'))
            cmd=[str(SOURCE/'benchmarks/run-app-baseline'),'--full','--load','write_only','--load-clients','32',
                '--load-duration-secs','90','--load-warmup-secs','15','--reps','1','--skip-postgres',
                '--database-root',str(ROOT/'perf-db'),'--output',str(cell/'report.json')]
            write(cell/'attempt.json',dict(ordinal=ordinal,archive=mode,started_at=now(),command=cmd,
                daemon_sha256=sha(daemon),runner_sha256=sha(runner),source_revision=receipt['source_freeze']['source_revision']))
            time.sleep(10) # measurement cooldown only
            validity(cell/'preflight.json',cellenv)
            started=time.monotonic()
            execute(cmd,SOURCE,cellenv,str(cell.relative_to(ROOT)/'run.log'),maximum=900)
            write(cell/'execution.json',dict(exit_code=0,elapsed_seconds=time.monotonic()-started,finished_at=now(),
                daemon_sha256=sha(daemon),runner_sha256=sha(runner)))
            time.sleep(5)
            validity(cell/'postflight.json',cellenv,cell/'preflight.json')
            result=analyze(cell,mode=='enabled')
            print('cell verified',mode,result['raw_percentiles'],flush=True)
        compare(out)
        write(out/'complete.json',dict(completed_at=now(),cells=2,automatic_gate_verdict=False))
    finally:
        restore()

def interrupted(signum, frame): raise TimeoutError('supervisor requested stop')
def main():
    signal.signal(signal.SIGTERM,interrupted)
    write(ROOT/'started.json',dict(at=now(),pid=os.getpid()))
    env=environment()
    receipt=build(env,json.loads((ROOT/'source-freeze.json').read_text()))
    measure(env,receipt)

if __name__=='__main__':
    code=1
    try:
        main()
        code=0
    except BaseException as error:
        write(ROOT/'stopped.json',dict(at=now(),error_type=type(error).__name__,reason=str(error),automatic_retry=False))
        raise
    finally:
        write(ROOT/'finished.json',dict(at=now(),exit_code=code))
