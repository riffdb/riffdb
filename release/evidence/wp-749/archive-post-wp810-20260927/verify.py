#!/usr/bin/env python3
"""Verify exported identities and arithmetic; raw replay uses analyze.py."""
from pathlib import Path
import hashlib
import json
import math

ROOT = Path(__file__).resolve().parent


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    for line in (ROOT/'SHA256SUMS').read_text().splitlines():
        expected, name = line.split('  ', 1)
        assert digest(ROOT/name) == expected, name
    build = json.loads((ROOT/'build.json').read_text())
    freeze = json.loads((ROOT/'source-freeze.json').read_text())
    comparison = json.loads((ROOT/'comparison.json').read_text())
    custody = json.loads((ROOT/'custody.json').read_text())
    assert build['source_revision'] == freeze['source_revision']
    assert build['daemon_source_unmodified'] is True
    assert custody['isolation_restored'] is True and custody['finished']['exit_code'] == 0
    for name, expected in freeze['inputs'].items():
        if (ROOT/name).exists():
            assert digest(ROOT/name) == expected, name
    cells = []
    for name, arm, enabled in [('01-disabled', 'disabled', False), ('02-enabled', 'enabled', True)]:
        root = ROOT/name
        value = json.loads((root/'analysis.json').read_text())
        assert value == comparison[arm]
        execution = json.loads((root/'execution.json').read_text())
        assert execution['exit_code'] == 0
        for field in ['daemon_sha256', 'runner_sha256']:
            assert execution[field] == build[field]
        assert value['archive']['enabled'] is enabled
        assert value['checkpoint']['failures'] == 0
        assert value['publication']['instrument_version'] == 2
        assert value['publication']['receipt_wait_errors'] == 0
        pre = json.loads((root/'preflight.json').read_text())
        post = json.loads((root/'postflight.json').read_text())
        assert pre['valid'] is post['valid'] is post['whole_cell']['valid'] is True
        assert post['baseline_report_sha256'] == digest(root/'preflight.json')
        assert pre['boot_identity_sha256'] == post['boot_identity_sha256']
        assert pre['hardware_identity'] == post['hardware_identity']
        for field in ['preflight.json', 'postflight.json', 'execution.json']:
            assert digest(root/field) == value['input_sha256'][field]
        retained = custody['inputs'][name]
        assert retained['analysis_sha256'] == digest(root/'analysis.json')
        assert retained['raw_samples_sha256'] == value['input_sha256']['raw/raw-request-tail.json']
        assert retained['raw_report_sha256'] == value['input_sha256']['report.json']
        assert retained['run_log_sha256'] == value['input_sha256']['run.log']
        cells.append(value)
    off, on = cells
    assert off['raw_arrays_sha256'] != on['raw_arrays_sha256']
    for q, a in off['raw_percentiles'].items():
        change = 100 * (on['raw_percentiles'][q] / a - 1)
        assert math.isclose(change, comparison['latency_change_percent'][q], abs_tol=1e-12)
    throughput = 100 * (on['throughput_ops_s'] / off['throughput_ops_s'] - 1)
    assert math.isclose(throughput, comparison['throughput_change_percent'], abs_tol=1e-12)
    print('Export digests, identical binaries, host receipts, raw-input bindings and paired arithmetic verified.')
    print('The archive acknowledgement-latency gate remains unmet; this verifier does not award a pass.')


if __name__ == '__main__':
    main()
