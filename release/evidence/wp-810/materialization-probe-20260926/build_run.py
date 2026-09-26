from pathlib import Path
import datetime, hashlib, json, os, shutil, signal, subprocess, sys, tempfile, time

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / 'source'
UTC = datetime.timezone.utc

def now():
    return datetime.datetime.now(UTC).isoformat()

def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

def write(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')

def verify_qualified_binary_digests(receipt, gate, daemon_digest, runner_digest):
    assert gate['local_functional_passed'] is True
    assert daemon_digest == receipt['daemon_sha256'] == gate['daemon_sha256']
    assert runner_digest == receipt['runner_sha256'] == gate['runner_sha256']

def remaining():
    lease = json.loads((ROOT / 'lease.json').read_text())
    return (datetime.datetime.fromisoformat(lease['expires_at']) - datetime.datetime.now(UTC)).total_seconds()

def environment():
    env = {k:v for k,v in os.environ.items() if not k.startswith(('RIFFDB_', 'CARGO_', 'RUST'))}
    env.update(PATH=str(Path.home()/'.cargo/bin')+':/usr/local/bin:/usr/bin:/bin', LC_ALL='C', TZ='UTC',
               RUSTFLAGS='-C target-cpu=x86-64', CARGO_BUILD_JOBS='6', CARGO_INCREMENTAL='0',
               RUSTC_WRAPPER='', RUSTUP_TOOLCHAIN='1.98.1', CARGO_PROFILE_RELEASE_LTO='thin',
               CARGO_PROFILE_RELEASE_CODEGEN_UNITS='1', CARGO_TARGET_DIR=str(ROOT/'build-target'))
    return env

def execute(command, cwd, env, log_name, maximum=2400):
    assert remaining() > 120, 'exclusive interval nearly exhausted'
    print('starting', log_name, now(), flush=True)
    started = time.monotonic()
    with (ROOT/log_name).open('x') as output:
        child = subprocess.Popen(command, cwd=cwd, env=env, stdout=output, stderr=subprocess.STDOUT,
                                 start_new_session=True)
        try:
            code = child.wait(timeout=min(maximum, int(remaining())-60))
        except BaseException:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
            raise
    write(ROOT/(log_name+'.execution.json'), dict(command=command, exit_code=code,
          elapsed_seconds=time.monotonic()-started, finished_at=now()))
    assert code == 0, (log_name, code)
    print('finished', log_name, now(), flush=True)

def build(env, freeze):
    for flag in ('sha_ni', 'aes', 'pclmulqdq', 'sse4_2'):
        assert flag in Path('/proc/cpuinfo').read_text().split(), flag
    toolchain = subprocess.check_output(['rustc', '-Vv'], env=env, text=True)
    assert toolchain.startswith('rustc 1.98.1 ')
    for name, digest in freeze['inputs'].items():
        assert sha(ROOT/name) == digest, name
    execute(['git','clone','--no-checkout',str(ROOT/'source.bundle'),str(SOURCE)],ROOT,env,'clone.log')
    execute(['git','checkout','--detach',freeze['revision']],SOURCE,env,'checkout.log')
    execute(['git','apply','--check',str(ROOT/'observer.patch')],SOURCE,env,'observer-check.log')
    execute(['git','apply',str(ROOT/'observer.patch')],SOURCE,env,'observer-apply.log')
    receipt = dict(started_at=now(),toolchain=toolchain,source_freeze=freeze,
                   build_environment={k:v for k,v in env.items() if k.startswith(('CARGO_','RUST'))})
    write(ROOT/'build-started.json',receipt)
    binaries=ROOT/'binaries'
    binaries.mkdir()
    execute(['cargo','build','--locked','--release','-p','riffdb-server','--bin','riffdbd'],SOURCE,env,'daemon-build.log')
    shutil.copy2(ROOT/'build-target/release/riffdbd',binaries/'riffdbd')
    execute(['cargo','build','--locked','--release','--manifest-path','examples/app-baseline/Cargo.toml',
             '-p','riffdb-app-baseline','--bin','riffdb-app-baseline'],SOURCE,env,'runner-build.log')
    shutil.copy2(ROOT/'build-target/release/riffdb-app-baseline',binaries/'runner')
    receipt.update(completed_at=now(),daemon_sha256=sha(binaries/'riffdbd'),runner_sha256=sha(binaries/'runner'))
    write(ROOT/'build-complete.json',receipt)
    return receipt


def validity(path,env,baseline=None):
    cmd=[str(SOURCE/'scripts/app-baseline-host-validity')]
    if baseline:
        cmd+=['--baseline-report',str(baseline)]
    with path.open('x') as out:
        result=subprocess.run(cmd,cwd=SOURCE,env=env,stdout=out,stderr=subprocess.STDOUT,timeout=30)
    assert result.returncode==0 and json.loads(path.read_text())['valid'] is True,path.name


def run_in_ram(command,cell,env,cell_deadline_seconds=900):
    assert cell_deadline_seconds in (900,3600), 'unfrozen cell deadline'
    assert subprocess.check_output(['findmnt','-n','-o','FSTYPE','--target','/dev/shm'],text=True).strip()=='tmpfs'
    available=os.statvfs('/dev/shm')
    assert available.f_bavail*available.f_frsize >= 1024*1024*1024,'insufficient tmpfs headroom'
    ram=Path(tempfile.mkdtemp(prefix='wp810-materialization-',dir='/dev/shm'))
    write(cell/'ram-output.json',dict(directory=str(ram),filesystem='tmpfs',maximum_bytes=768*1024*1024))
    child=None
    try:
        with (ram/'run.log').open('x') as out:
            child=subprocess.Popen(command,cwd=SOURCE,env=env,stdout=out,stderr=subprocess.STDOUT,start_new_session=True)
            deadline=time.monotonic()+min(cell_deadline_seconds,remaining()-90)
            while True:
                try:
                    code=child.wait(timeout=1)
                    break
                except subprocess.TimeoutExpired:
                    assert time.monotonic()<deadline,'cell deadline exceeded'
                    assert (ram/'run.log').stat().st_size<=768*1024*1024,'log budget exceeded'
            assert code==0,('cell failed',code)
    finally:
        if child is not None and child.poll() is None:
            os.killpg(child.pid,signal.SIGTERM)
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid,signal.SIGKILL)
                child.wait()
        if (ram/'run.log').exists():
            shutil.copy2(ram/'run.log',cell/'run.log')
        # Remove only this task's exact mkdtemp directory after copying it.
        shutil.rmtree(ram)


def measure(env,receipt,cell_deadline_seconds=900):
    from analyze_materialization import analyze
    assert cell_deadline_seconds in (900,3600), 'unfrozen cell deadline'
    assert remaining()>cell_deadline_seconds+300,'insufficient reservation for complete fixed diagnostic'
    assert os.readlink('/proc/self/ns/pid')==subprocess.check_output(['sudo','-n','readlink','/proc/1/ns/pid'],text=True).strip()
    cell=ROOT/'results/01-materialization'
    cell.mkdir(parents=True)
    for name in ('raw','tmp'):
        (cell/name).mkdir()
    daemon,runner=ROOT/'binaries/riffdbd',ROOT/'binaries/runner'
    assert sha(daemon)==receipt['daemon_sha256'] and sha(runner)==receipt['runner_sha256']
    cellenv={k:v for k,v in env.items() if not k.startswith(('CARGO_','RUST'))}
    cellenv.update(RUSTUP_TOOLCHAIN='1.98.1',RIFFDB_REDB_COMMIT_PROFILE='standard',
                   RIFFDB_TMP_ROOT=str(cell/'tmp'),RIFFDB_APP_BASELINE_RIFFDBD_BIN=str(daemon),
                   RIFFDB_APP_BASELINE_RUNNER_BIN=str(runner),RIFFDB_APP_BASELINE_ARCHIVE_COLLECTION='disabled',
                   RIFFDB_WRITER_BATCH_DIAGNOSTICS='1',RIFFDB_WRITER_CORRELATION='1',
                   RIFFDB_APP_BASELINE_RAW_TAIL_DIR=str(cell/'raw'))
    command=[str(SOURCE/'benchmarks/run-app-baseline'),'--full','--load','write_only','--load-clients','32',
             '--load-duration-secs','90','--load-warmup-secs','15','--reps','1','--skip-postgres',
             '--database-root',str(ROOT/'perf-db'),'--output',str(cell/'report.json')]
    write(cell/'attempt.json',dict(started_at=now(),command=command,source_revision=receipt['source_freeze']['revision'],
                                  daemon_sha256=sha(daemon),runner_sha256=sha(runner),
                                  cell_deadline_seconds=cell_deadline_seconds))
    time.sleep(10) # measurement cooldown; never correctness synchronization
    validity(cell/'preflight.json',cellenv)
    started=time.monotonic()
    run_in_ram(command,cell,cellenv,cell_deadline_seconds)
    write(cell/'execution.json',dict(exit_code=0,elapsed_seconds=time.monotonic()-started,finished_at=now(),
                                   daemon_sha256=sha(daemon),runner_sha256=sha(runner)))
    time.sleep(5)
    validity(cell/'postflight.json',cellenv,cell/'preflight.json')
    result=analyze(cell)
    print('diagnostic complete',result['coverage'],flush=True)
    write(ROOT/'complete.json',dict(completed_at=now(),status='One diagnostic retained; no mitigation or package-closure verdict.'))


def interrupted(signum,frame):
    raise TimeoutError('supervisor requested stop')


def main():
    signal.signal(signal.SIGTERM,interrupted)
    env=environment()
    freeze=json.loads((ROOT/'source-freeze.json').read_text())
    receipt=build(env,freeze)
    measure(env,receipt)


if __name__=='__main__':
    try:
        main()
    except BaseException as error:
        write(ROOT/'stopped.json',dict(at=now(),error_type=type(error).__name__,reason=str(error),automatic_retries=False))
        raise
