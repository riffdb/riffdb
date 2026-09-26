"""One bounded diagnostic using the already-built, locally qualified binaries."""
import json
import signal
import subprocess

import build_run as run


def verify_retained(freeze, receipt, gate, daemon_digest, runner_digest):
    prior = receipt['source_freeze']
    assert prior['revision'] == freeze['revision'] == gate['source_revision']
    for name in ('source.bundle', 'observer.patch'):
        assert prior['inputs'][name] == freeze['inputs'][name], name
    assert gate['observer_patch_sha256'] == freeze['inputs']['observer.patch']
    assert gate['local_functional_passed'] is True
    assert daemon_digest == receipt['daemon_sha256'] == gate['daemon_sha256']
    assert runner_digest == receipt['runner_sha256'] == gate['runner_sha256']


def main():
    signal.signal(signal.SIGTERM, run.interrupted)
    root = run.ROOT
    freeze = json.loads((root / 'source-freeze.json').read_text())
    receipt = json.loads((root / 'retained-build-complete.json').read_text())
    gate = json.loads((root / 'local-functional-qualified.json').read_text())
    env = run.environment()
    assert run.remaining() > 3900, 'insufficient exclusive interval'
    for flag in ('sha_ni', 'aes', 'pclmulqdq', 'sse4_2'):
        assert flag in run.Path('/proc/cpuinfo').read_text().split(), flag
    assert subprocess.check_output(['rustc', '-Vv'], env=env, text=True).startswith('rustc 1.98.1 ')
    for name, digest in freeze['inputs'].items():
        assert run.sha(root / name) == digest, name
    verify_retained(freeze, receipt, gate, run.sha(root / 'binaries/riffdbd'), run.sha(root / 'binaries/runner'))
    run.execute(['git', 'clone', '--no-checkout', str(root / 'source.bundle'), str(run.SOURCE)], root, env, 'clone.log')
    run.execute(['git', 'checkout', '--detach', freeze['revision']], run.SOURCE, env, 'checkout.log')
    run.execute(['git', 'apply', '--check', str(root / 'observer.patch')], run.SOURCE, env, 'observer-check.log')
    run.execute(['git', 'apply', str(root / 'observer.patch')], run.SOURCE, env, 'observer-apply.log')
    run.write(root / 'retained-binaries-verified.json', dict(at=run.now(), prior_build=receipt, execution_freeze=freeze, gate=gate))
    run.measure(env, receipt, cell_deadline_seconds=3600)


if __name__ == '__main__':
    try:
        main()
    except BaseException as error:
        run.write(run.ROOT / 'stopped.json', dict(at=run.now(), error_type=type(error).__name__, reason=str(error), automatic_retries=False))
        raise
