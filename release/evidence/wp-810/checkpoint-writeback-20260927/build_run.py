from pathlib import Path
import datetime, hashlib, json, os, shutil, signal, subprocess, sys, time

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

def remaining():
    lease = json.loads((ROOT / 'task-window.json').read_text())
    return (datetime.datetime.fromisoformat(lease['deadline_at']) - datetime.datetime.now(UTC)).total_seconds()

def environment():
    env = {k:v for k,v in os.environ.items() if not k.startswith(('RIFFDB_', 'CARGO_', 'RUST'))}
    env.update(PATH=str(Path.home()/'.cargo/bin')+':/usr/local/bin:/usr/bin:/bin', LC_ALL='C', TZ='UTC',
               RUSTFLAGS='-C target-cpu=x86-64', CARGO_BUILD_JOBS='6', CARGO_INCREMENTAL='0',
               RUSTC_WRAPPER='', RUSTUP_TOOLCHAIN='1.98.1', CARGO_PROFILE_RELEASE_LTO='thin',
               CARGO_PROFILE_RELEASE_CODEGEN_UNITS='1', CARGO_TARGET_DIR=str(ROOT/'build-target'))
    return env

def execute(command, cwd, env, log_name, maximum=2400):
    assert remaining() > 120, 'bounded task deadline nearly exhausted'
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
    execute(['git', 'clone', '--no-checkout', str(ROOT/'source.bundle'), str(SOURCE)], ROOT, env, 'clone.log')
    binaries = ROOT/'binaries'
    binaries.mkdir()
    receipt = dict(started_at=now(), toolchain=toolchain, source_freeze=freeze,
                   build_environment={k:v for k,v in env.items() if k.startswith(('CARGO_', 'RUST'))}, arms={})
    write(ROOT/'build-started.json', receipt)
    for arm, revision in [('control', freeze['control']), ('candidate', freeze['candidate'])]:
        if arm == 'candidate':
            execute(['git', 'apply', '--reverse', str(ROOT/'common-observation.patch')], SOURCE, env, 'remove-control-observer.log')
        execute(['git', 'checkout', '--detach', revision], SOURCE, env, arm+'-checkout.log')
        execute(['git', 'apply', '--check', str(ROOT/'common-observation.patch')], SOURCE, env, arm+'-observer-check.log')
        execute(['git', 'apply', str(ROOT/'common-observation.patch')], SOURCE, env, arm+'-observer-apply.log')
        execute(['cargo', 'build', '--locked', '--release', '-p', 'riffdb-server', '--bin', 'riffdbd'], SOURCE, env, arm+'-build.log')
        destination = binaries/('riffdbd-'+arm)
        shutil.copy2(ROOT/'build-target/release/riffdbd', destination)
        receipt['arms'][arm] = dict(revision=revision, daemon_sha256=sha(destination))
    assert receipt['arms']['control']['daemon_sha256'] != receipt['arms']['candidate']['daemon_sha256'], 'identical server binaries'
    execute(['cargo', 'build', '--locked', '--release', '--manifest-path', 'examples/app-baseline/Cargo.toml',
             '-p', 'riffdb-app-baseline', '--bin', 'riffdb-app-baseline'], SOURCE, env, 'runner-build.log')
    shutil.copy2(ROOT/'build-target/release/riffdb-app-baseline', binaries/'runner')
    receipt.update(completed_at=now(), runner_sha256=sha(binaries/'runner'))
    write(ROOT/'build-complete.json', receipt)
    return receipt

def validity(path, env, baseline=None):
    command = [str(SOURCE/'scripts/app-baseline-host-validity')]
    if baseline:
        command += ['--baseline-report', str(baseline)]
    with path.open('x') as output:
        result = subprocess.run(command, cwd=SOURCE, env=env, stdout=output, stderr=subprocess.STDOUT, timeout=30)
    assert result.returncode == 0 and json.loads(path.read_text())['valid'] is True, path.name

def measure(env, receipt):
    from analyze import analyze_cell, compare
    results = ROOT/'results'
    results.mkdir()
    write(results/'protocol.json', json.loads((ROOT/'measurement-protocol.json').read_text()))
    # Building is complete before any host validity interval or measured cell.
    assert os.readlink('/proc/self/ns/pid') == subprocess.check_output(['sudo', '-n', 'readlink', '/proc/1/ns/pid'], text=True).strip()
    for ordinal, arm in enumerate(('control', 'candidate', 'candidate', 'control', 'control', 'candidate'), 1):
        assert remaining() > 900, 'insufficient bounded task deadline for another complete cell'
        cell = results/f'{ordinal:02}-{arm}'
        cell.mkdir()
        (cell/'raw').mkdir()
        (cell/'tmp').mkdir()
        daemon = ROOT/'binaries'/('riffdbd-'+arm)
        runner = ROOT/'binaries/runner'
        assert sha(daemon) == receipt['arms'][arm]['daemon_sha256']
        assert sha(runner) == receipt['runner_sha256']
        revision = receipt['arms'][arm]['revision']
        if subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=SOURCE, text=True).strip() != revision:
            execute(['git', 'apply', '--reverse', str(ROOT/'common-observation.patch')], SOURCE, env, f'{ordinal:02}-source-unpatch.log')
            execute(['git', 'checkout', '--detach', revision], SOURCE, env, f'{ordinal:02}-source-checkout.log')
            execute(['git', 'apply', str(ROOT/'common-observation.patch')], SOURCE, env, f'{ordinal:02}-source-observer.log')
        cellenv = {k:v for k,v in env.items() if not k.startswith(('CARGO_', 'RUST'))}
        cellenv.update(RUSTUP_TOOLCHAIN='1.98.1', RIFFDB_REDB_COMMIT_PROFILE='standard',
                       RIFFDB_TMP_ROOT=str(cell/'tmp'), RIFFDB_APP_BASELINE_RIFFDBD_BIN=str(daemon),
                       RIFFDB_APP_BASELINE_RUNNER_BIN=str(runner), RIFFDB_APP_BASELINE_ARCHIVE_COLLECTION='disabled',
                       RIFFDB_WRITER_BATCH_DIAGNOSTICS='1', RIFFDB_APP_BASELINE_RAW_TAIL_DIR=str(cell/'raw'))
        command = [str(SOURCE/'benchmarks/run-app-baseline'), '--full', '--load', 'write_only', '--load-clients', '32',
                   '--load-duration-secs', '90', '--load-warmup-secs', '15', '--reps', '1', '--skip-postgres',
                   '--database-root', str(ROOT/'perf-db'), '--output', str(cell/'report.json')]
        write(cell/'attempt.json', dict(arm=arm, ordinal=ordinal, pair=(ordinal+1)//2, started_at=now(), command=command, source_revision=revision,
                                      daemon_sha256=sha(daemon), runner_sha256=sha(runner)))
        time.sleep(10) # measurement cooldown, never correctness synchronization
        validity(cell/'preflight.json', cellenv)
        started = time.monotonic()
        execute(command, SOURCE, cellenv, str(cell.relative_to(ROOT)/'run.log'), maximum=900)
        write(cell/'execution.json', dict(exit_code=0, elapsed_seconds=time.monotonic()-started,
                                          daemon_sha256=sha(daemon), runner_sha256=sha(runner), finished_at=now()))
        time.sleep(5)
        validity(cell/'postflight.json', cellenv, cell/'preflight.json')
        result = analyze_cell(cell)
        print('cell complete', ordinal, arm, result['raw_percentiles'], flush=True)
    compare(results)
    write(results/'complete.json', dict(completed_at=now(), status='All six fixed cells retained; maintainer interpretation required.'))

def interrupted(signum, frame):
    raise TimeoutError('supervisor requested stop')

def main():
    signal.signal(signal.SIGTERM, interrupted)
    env = environment()
    freeze = json.loads((ROOT/'source-freeze.json').read_text())
    receipt = build(env, freeze)
    write(ROOT/'awaiting-local-qualification.json', dict(at=now(), source_freeze=freeze))
    while not (ROOT/'local-functional-qualified.json').exists():
        assert remaining() > 1200, 'bounded task deadline leaves no complete comparison'
        assert not (ROOT/'cancel-before-measurement.json').exists(), 'local checks refused'
        time.sleep(2)
    from qualification_gate import verify
    gate = json.loads((ROOT/'local-functional-qualified.json').read_text())
    verify(freeze, receipt, gate)
    write(ROOT/'measurement-gate-passed.json', dict(at=now(), gate=gate))
    measure(env, receipt)

if __name__ == '__main__':
    try:
        main()
    except BaseException as error:
        write(ROOT/'stopped.json', dict(at=now(), error_type=type(error).__name__, reason=str(error), automatic_retries=False))
        raise
