#!/usr/bin/env python3
"""Reconcile exported WP-749 cells and recompute the fixed-set arithmetic."""
import hashlib
import json
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parent
METRICS = ['throughput_ops_s', 'p50_ns', 'p95_ns', 'p99_ns', 'mean_commit_us']


def summarize(path):
    receipt = json.loads(path.read_text())
    rows = []
    for cell in receipt['cells']:
        for key in ['daemon_sha256', 'runner_sha256']:
            assert cell[key] == receipt[key]
        if 'report_extract' in cell:
            data = (path.parent / cell['report_extract']).read_bytes()
            assert hashlib.sha256(data).hexdigest() == cell['report_extract_sha256']
        if not cell['cell_checks_passed']:
            continue
        row = cell['validated_summary']
        assert row['report_sha256'] == cell['original_report_sha256']
        report = json.loads(data)
        assert report['correctness']['clean']
        point = report['curve'][0]
        assert sum(point['outcomes'].values()) == point['logical_ops']
        assert point['outcomes']['error'] == point['outcomes']['unavailable'] == 0
        assert row['throughput_ops_s'] == point['throughput_ops_s']
        for metric in ['p50_ns', 'p95_ns', 'p99_ns']:
            assert row[metric] == point['aggregate_' + metric]
        writer = point['server_stage_evidence']['writer']
        assert row['mean_commit_us'] == writer['commit_duration_us']['sum'] / writer['commit_duration_us']['count']
        assert row['archive'] == point['server_stage_evidence']['archive_collection']
        assert cell['preflight']['valid'] and cell['postflight']['valid']
        assert cell['postflight']['baseline_report_sha256'] == cell['preflight_original_sha256']
        rows.append(row)
    phases = {}
    for load in ['write_only', 'interactive']:
        selected = [r for r in rows if r['load'] == load]
        arms = {}
        for mode in ['disabled', 'enabled']:
            arm = [r for r in selected if r['mode'] == mode]
            if not arm:
                continue
            assert len({r['pair'] for r in arm}) == len(arm) <= 5
            metrics = {}
            for name in METRICS:
                values = sorted(r[name] for r in arm)
                metric = {'values': values, 'median': statistics.median(values)}
                if len(values) == 5:
                    metric['central_three_spread'] = values[3] / values[1]
                    if name in ['p50_ns', 'p95_ns']:
                        metric['stability_passed'] = values[3] / values[1] <= 1.20
                metrics[name] = metric
            arms[mode] = {'n': len(arm), 'metrics': metrics}
        pairs = []
        for pair in range(1, 6):
            paired = {r['mode']: r for r in selected if r['pair'] == pair}
            if set(paired) == {'disabled', 'enabled'}:
                pairs.append({'pair': pair, 'enabled_over_disabled_percent': {
                    metric: 100 * (paired['enabled'][metric] / paired['disabled'][metric] - 1)
                    for metric in METRICS}})
        contrast = None
        if len(pairs) == 5:
            contrast = {metric: 100 * (arms['enabled']['metrics'][metric]['median'] /
                        arms['disabled']['metrics'][metric]['median'] - 1) for metric in METRICS}
        phases[load] = {'arms': arms, 'complete_pairs': pairs, 'median_contrast_percent': contrast}
    return {'attempt': receipt['host_attempt'], 'attempted_cells': len(receipt['cells']),
            'cells_passing_checks': len(rows), 'complete': receipt['terminal']['complete'],
            'phases': phases, 'qualification_verdict': 'Not assigned by this arithmetic.'}


if __name__ == '__main__':
    print(json.dumps([summarize(p) for p in sorted(ROOT.glob('attempt-*/attempt.json'))], indent=2))
