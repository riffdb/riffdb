"""Full local functional qualification of remote-built executables, with no latency verdict."""
from pathlib import Path
import argparse,json,os,signal,subprocess,time
import build_run as run
from analyze import analyze_cell
from qualification_gate import WORKLOAD, verify

def main():
    parser=argparse.ArgumentParser()
    for key in ('source','binaries','output','freeze'): parser.add_argument('--'+key,required=True,type=Path)
    args=parser.parse_args()
    source,binaries,output,freeze_path=(getattr(args,k).resolve() for k in ('source','binaries','output','freeze'))
    freeze=json.loads(freeze_path.read_text())
    build=json.loads((binaries/'build-complete.json').read_text())
    assert freeze==build['source_freeze']
    assert run.sha(Path(__file__).parent/'common-observation.patch')==freeze['inputs']['common-observation.patch']
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==freeze['candidate']
    for file in ('benchmarks/run-app-baseline','Cargo.lock'):
        for arm in ('control','candidate'):
            assert subprocess.check_output(['git','show',freeze[arm]+':'+file],cwd=source)==(source/file).read_bytes()
    runner=binaries/'runner'
    assert run.sha(runner)==build['runner_sha256']
    proof_path=Path(__file__).parent/'local-correctness-qualified.json'
    proof=json.loads(proof_path.read_text())
    assert proof['source_revision']==freeze['candidate']
    assert proof['storage_acceptance_exit_code']==proof['coordinator_exit_code']==0
    output.mkdir()
    gate=dict(source_freeze=freeze,runner_sha256=run.sha(runner),local_correctness_passed=True,performance_verdict=False,arms={})
    gate['local_correctness_proof']=proof
    gate['local_correctness_proof_sha256']=run.sha(proof_path)
    for arm in ('control','candidate'):
        cell=output/arm
        cell.mkdir()
        for name in ('raw','tmp'): (cell/name).mkdir()
        daemon=binaries/('riffdbd-'+arm)
        assert run.sha(daemon)==build['arms'][arm]['daemon_sha256']
        command=[str(source/'benchmarks/run-app-baseline'),'--full','--load','write_only','--load-clients','32',
                 '--load-duration-secs','90','--load-warmup-secs','15','--reps','1','--skip-postgres',
                 '--database-root',str(cell/'db'),'--output',str(cell/'report.json')]
        env={k:v for k,v in os.environ.items() if not k.startswith(('CARGO_','RUST','RIFFDB_'))}
        env.update(PATH=str(Path.home()/'.cargo/bin')+':/usr/local/bin:/usr/bin:/bin',RUSTUP_TOOLCHAIN='1.98.1',
            RUSTC_WRAPPER='',RIFFDB_REDB_COMMIT_PROFILE='standard',RIFFDB_TMP_ROOT=str(cell/'tmp'),
            RIFFDB_APP_BASELINE_RIFFDBD_BIN=str(daemon),RIFFDB_APP_BASELINE_RUNNER_BIN=str(runner),
            RIFFDB_APP_BASELINE_ARCHIVE_COLLECTION='disabled',RIFFDB_WRITER_BATCH_DIAGNOSTICS='1',
            RIFFDB_APP_BASELINE_RAW_TAIL_DIR=str(cell/'raw'))
        run.write(cell/'attempt.json',dict(arm=arm,command=command,source_revision=freeze[arm],
            daemon_sha256=run.sha(daemon),runner_sha256=run.sha(runner),started_at=run.now(),deadline_seconds=900))
        started=time.monotonic()
        with (cell/'run.log').open('x') as log:
            child=subprocess.Popen(command,cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            try: code=child.wait(timeout=900)
            except BaseException:
                os.killpg(child.pid,signal.SIGTERM)
                try: child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid,signal.SIGKILL);child.wait()
                raise
        run.write(cell/'execution.json',dict(exit_code=code,daemon_sha256=run.sha(daemon),runner_sha256=run.sha(runner),
                  elapsed_seconds=time.monotonic()-started,finished_at=run.now()))
        assert code==0, (arm,code)
        result=analyze_cell(cell,local=True)
        report=json.loads((cell/'report.json').read_text())
        raw=json.loads((cell/'raw/raw-request-tail.json').read_text())
        gate['arms'][arm]=dict(revision=freeze[arm],daemon_sha256=run.sha(daemon),workload=WORKLOAD,
            correctness_clean=report['correctness']['clean'],complete_analysis_passed=True,report_scale=report['scale'],
            measured_elapsed_ns=result['measured_elapsed_ns'],samples=result['sample_count'],
            checkpoints=result['checkpoint']['starts'],dropped_samples=raw['dropped_samples'],exit_code=code,
            report_sha256=run.sha(cell/'report.json'),analysis_sha256=run.sha(cell/'analysis.json'),log_sha256=run.sha(cell/'run.log'))
        print(arm, 'qualified',gate['arms'][arm]['samples'],'samples',flush=True)
    verify(freeze,build,gate)
    gate['completed_at']=run.now()
    run.write(output/'local-functional-qualified.json',gate)

if __name__=='__main__':
    try: main()
    except BaseException as error:
        print(json.dumps(dict(qualification_refused=True,error_type=type(error).__name__,reason=str(error),automatic_retry=False)),flush=True)
        raise
