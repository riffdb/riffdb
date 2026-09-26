"""Full workload capacity qualification; local timing is not performance evidence."""
from pathlib import Path
import argparse
import json
import os
import signal
import subprocess
import time

import build_run as run
from analyze_materialization import analyze


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--binaries', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--freeze', required=True, type=Path)
    args = parser.parse_args()
    source, binaries, output = (p.resolve() for p in (args.source, args.binaries, args.output))
    freeze = json.loads(args.freeze.read_text())
    build = json.loads((binaries / 'build-complete.json').read_text())
    assert build['source_freeze'] == freeze
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=source, text=True).strip() == freeze['revision']
    for name in ('benchmarks/run-app-baseline', 'Cargo.lock'):
        assert subprocess.check_output(['git', 'show', freeze['revision'] + ':' + name], cwd=source) == (source / name).read_bytes()
    assert run.sha(Path(__file__).parent / 'observer.patch') == freeze['inputs']['observer.patch']
    assert os.getpriority(os.PRIO_PROCESS, 0) == 0
    daemon, runner = binaries / 'riffdbd', binaries / 'runner'
    assert run.sha(daemon) == build['daemon_sha256']
    assert run.sha(runner) == build['runner_sha256']
    output.mkdir()
    cell = output / 'cell'
    cell.mkdir()
    for name in ('raw', 'tmp'):
        (cell / name).mkdir()
    workload = dict(dataset='full', clients=32, load='write_only', warmup_seconds=15, measurement_seconds=90)
    command = [str(source / 'benchmarks/run-app-baseline'), '--full', '--load', 'write_only',
               '--load-clients', '32', '--load-duration-secs', '90', '--load-warmup-secs', '15',
               '--reps', '1', '--skip-postgres', '--database-root', str(cell / 'db'),
               '--output', str(cell / 'report.json')]
    receipt = dict(started_at=run.now(), source_revision=freeze['revision'],
                   observer_patch_sha256=freeze['inputs']['observer.patch'],
                   daemon_sha256=build['daemon_sha256'], runner_sha256=build['runner_sha256'],
                   command=command, workload=workload, cell_deadline_seconds=900,
                   scope='Local full-workload capacity and correctness proof only; no performance verdict.')
    run.write(cell / 'attempt.json', receipt)
    env = {k: v for k, v in os.environ.items() if not k.startswith(('CARGO_', 'RUST', 'RIFFDB_'))}
    env.update(PATH=str(Path.home() / '.cargo/bin') + ':/usr/local/bin:/usr/bin:/bin',
               RUSTUP_TOOLCHAIN='1.98.1', RUSTC_WRAPPER='', RIFFDB_REDB_COMMIT_PROFILE='standard',
               RIFFDB_TMP_ROOT=str(cell / 'tmp'), RIFFDB_APP_BASELINE_RIFFDBD_BIN=str(daemon),
               RIFFDB_APP_BASELINE_RUNNER_BIN=str(runner), RIFFDB_APP_BASELINE_ARCHIVE_COLLECTION='disabled',
               RIFFDB_WRITER_BATCH_DIAGNOSTICS='1', RIFFDB_WRITER_CORRELATION='1',
               RIFFDB_APP_BASELINE_RAW_TAIL_DIR=str(cell / 'raw'))
    # Reuse the bounded RAM spool and failure retention. This deadline is local;
    # it does not create or extend a remote host reservation.
    started = time.monotonic()
    local_deadline = started + 1020
    run.SOURCE = source
    run.remaining = lambda: max(0, local_deadline - time.monotonic())
    signal.signal(signal.SIGTERM, run.interrupted)
    try:
        run.run_in_ram(command, cell, env, cell_deadline_seconds=900)
        run.write(cell / 'execution.json', dict(exit_code=0, finished_at=run.now(),
                                                elapsed_seconds=time.monotonic() - started))
        result = analyze(cell, local=True)
        report = json.loads((cell / 'report.json').read_text())
        point = report['curve'][0]
        gate = dict(source_revision=freeze['revision'], observer_patch_sha256=freeze['inputs']['observer.patch'],
                    daemon_sha256=run.sha(daemon), runner_sha256=run.sha(runner),
                    local_functional_passed=True, completed_at=run.now(), performance_verdict=False,
                    capacity_qualification=dict(workload=workload, report_scale=report['scale'],
                        measured_elapsed_ns=point['measured_elapsed_ns'], complete_analysis_passed=True,
                        io_record_limit=16_777_216, coverage=result['coverage'], populations=result['populations'],
                        report_sha256=run.sha(cell / 'report.json'),
                        analysis_sha256=run.sha(cell / 'materialization-analysis.json'),
                        log_sha256=run.sha(cell / 'run.log')))
        run.verify_local_qualification(freeze, build, gate, run.sha(daemon), run.sha(runner))
        run.write(output / 'local-functional-qualified.json', gate)
        print(json.dumps(dict(coverage=result['coverage'], populations=result['populations']), indent=2))
    except BaseException as error:
        run.write(output / 'qualification-refused.json', dict(at=run.now(), error_type=type(error).__name__,
                  reason=str(error), automatic_retry=False, shortened_replacement_allowed=False))
        raise


if __name__ == '__main__':
    main()
