from pathlib import Path
import json, signal, time
import build_run as run

ROOT = Path(__file__).resolve().parent
signal.signal(signal.SIGTERM, run.interrupted)
try:
    freeze = json.loads((ROOT / 'source-freeze.json').read_text())
    env = run.environment()
    receipt = run.build(env, freeze)
    run.write(ROOT / 'awaiting-local-qualification.json', dict(at=run.now(), source_revision=freeze['revision']))
    while not (ROOT / 'local-functional-qualified.json').exists():
        assert run.remaining() > 1200, 'no time left for local qualification and the full diagnostic'
        assert not (ROOT / 'cancel-before-measurement.json').exists(), 'local qualification refused; no workload started'
        time.sleep(2)
    gate = json.loads((ROOT / 'local-functional-qualified.json').read_text())
    assert gate['source_revision'] == freeze['revision']
    assert gate['observer_patch_sha256'] == freeze['inputs']['observer.patch']
    assert gate['local_functional_passed'] is True
    assert not (ROOT / 'cancel-before-measurement.json').exists()
    run.write(ROOT / 'measurement-gate-passed.json', dict(at=run.now(), gate=gate))
    run.measure(env, receipt)
except BaseException as error:
    run.write(ROOT / 'stopped.json', dict(at=run.now(), error_type=type(error).__name__, reason=str(error), automatic_retries=False))
    raise
