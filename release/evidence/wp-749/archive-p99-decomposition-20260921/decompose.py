"""Descriptive P99 decomposition of the retained, host-valid WP-749 pair.

This cannot identify a causal archive-specific residual. It standardizes the
observed own-group drain prevalence and re-quantiles request-level subtractions.
"""
from pathlib import Path
import argparse
import hashlib
import json

MAX_REQUESTS = 32 * 32768
MAX_FILE_BYTES = 512 * 1024 * 1024


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def read_json(path):
    assert path.stat().st_size <= MAX_FILE_BYTES, 'input exceeds retained-evidence bound'
    return json.loads(path.read_text())


def percentile(values, numerator=99, denominator=100):
    assert values and 0 < numerator <= denominator
    ordered = sorted(values)
    return ordered[(len(ordered) * numerator + denominator - 1) // denominator - 1]


def weighted_p99(rows, target_true, target_total):
    """Keep both conditional empirical CDFs; replace only drain prevalence.

    Integer weights use a common denominator to avoid roundoff at rank edges.
    Each true row has mass target_true / target_total / source_true, and
    similarly for false. All sample weights, including multiplicity, are kept.
    """
    source_true = sum(row['drain_required'] for row in rows)
    source_false = len(rows) - source_true
    assert source_true > 0 and source_false > 0
    assert 0 <= target_true <= target_total and target_total > 0
    weights = {
        True: target_true * source_false,
        False: (target_total - target_true) * source_true,
    }
    total = target_total * source_true * source_false
    cumulative = 0
    for row in sorted(rows, key=lambda item: item['elapsed_ns']):
        cumulative += weights[row['drain_required']]
        if cumulative * 100 >= total * 99:
            return row['elapsed_ns']
    raise AssertionError('weighted CDF did not reach its quantile')


def exposure_split(off, on):
    prevalence = [(sum(row['drain_required'] for row in arm), len(arm)) for arm in (off, on)]
    matrix = [[weighted_p99(arm, count, total) for count, total in prevalence] for arm in (off, on)]
    a, b = matrix[0]
    c, d = matrix[1]
    assert a == percentile([r['elapsed_ns'] for r in off])
    assert d == percentile([r['elapsed_ns'] for r in on])
    exposure_steps = [b - a, d - c]
    conditional_steps = [c - a, d - b]
    assert exposure_steps[0] + conditional_steps[1] == d - a
    assert conditional_steps[0] + exposure_steps[1] == d - a
    result = dict(
        prevalence=[dict(drain_requests=count, requests=total) for count, total in prevalence],
        matrix_p99_ns=matrix,
        matrix_rows='conditional request latency distributions: disabled, enabled',
        matrix_columns='own-group drain prevalence: disabled, enabled',
        raw_difference_ns=d-a,
        exposure_change_by_order_ns=exposure_steps,
        within_state_change_by_order_ns=conditional_steps,
        exposure_average_ns=sum(exposure_steps)/2,
        within_state_average_ns=sum(conditional_steps)/2,
        average_rule='Equal average of the two possible factor-change orders; both exact paths retained.',
        causal_archive_specific_residual_ns=None,
        interpretation='Observed own-group drain composition versus conditional latency distribution change. Within-state change includes changed checkpoint/journal severity, queueing and any archive effects; it is not an identified archive-specific cost.',
    )
    assert result['exposure_average_ns'] + result['within_state_average_ns'] == result['raw_difference_ns']
    return result


def subtract_and_requantile(rows, component):
    residuals = []
    for row in rows:
        removed = component(row)
        assert 0 <= removed <= row['elapsed_ns'], 'component exceeds enclosing request'
        residuals.append(row['elapsed_ns'] - removed)
    return percentile(residuals)


def wait_partition(group):
    before = max(0, min(group[5], group[9])-group[8])
    inside = max(0, min(group[6], group[9])-max(group[5], group[8]))
    after = max(0, group[9]-max(group[6], group[8]))
    assert before+inside+after == group[9]-group[8]
    return before, inside, after


def load_rows(root, source):
    assert source['qualified_pair'] is True
    assert len(source['cells']) == 2
    arms = []
    for name, expected in zip(('01-disabled', '02-enabled'), source['cells']):
        assert expected['cell'] == name and expected['host_valid'] is True
        assert expected['preflight']['valid'] is True and expected['postflight']['valid'] is True
        cell = root/name
        for filename in ('joined-requests.json', 'raw/raw-request-tail.json', 'run.log', 'report.json', 'preflight.json', 'postflight.json'):
            assert digest(cell/filename) == expected['source_file_digests'][filename], filename
        assert read_json(cell/'execution.json') == expected['execution']
        assert expected['execution']['exit_code'] == 0
        for binary in ('daemon_sha256', 'runner_sha256'):
            assert expected['execution'][binary] == source[binary]
        groups = {}
        prefix = f"riffdb-correlation-group-v1\tpid={expected['attribution']['load_pid']}\t"
        for line in (cell/'run.log').read_text().splitlines():
            if line.startswith(prefix):
                group = list(map(int,line.split('\t')[2].split(',')))
                assert len(group) == 17 and group[0] not in groups
                groups[group[0]] = group
        assert 0 < len(groups) <= 65536
        rows = read_json(cell/'joined-requests.json')
        assert len(rows) == expected['attribution']['base']['sample_count']
        assert 0 < len(rows) <= MAX_REQUESTS
        assert len({row['commit_sequence'] for row in rows}) == len(rows)
        for row in rows:
            group = groups[row['group_id']]
            before, inside, after = wait_partition(group)
            assert inside == row['sync_wait_overlap_ns']
            assert before+inside+after == row['first_wait_ns']
            row['before_sync_in_first_wait_ns'] = before
            assert type(row['drain_required']) is bool
            assert 0 <= row['sync_wait_overlap_ns'] <= row['first_wait_ns'] <= row['submit_to_publication_ns'] <= row['elapsed_ns']
        assert percentile([row['elapsed_ns'] for row in rows]) == expected['attribution']['base']['raw_percentiles']['p99_ns']
        arms.append(rows)
    return arms


def analyze(root, source_path):
    source = read_json(source_path)
    arms = load_rows(root, source)
    split = exposure_split(*arms)
    components = {
        'own_checkpoint_pre_sync_wait': lambda r: r['before_sync_in_first_wait_ns'] if r['drain_required'] else 0,
        'own_checkpoint_sync_overlap': lambda r: r['sync_wait_overlap_ns'] if r['drain_required'] else 0,
        'own_checkpoint_first_wait': lambda r: r['first_wait_ns'] if r['drain_required'] else 0,
        'own_group_submission_through_publication': lambda r: r['submit_to_publication_ns'],
    }
    stripped = {}
    for label, component in components.items():
        off, on = [subtract_and_requantile(rows, component) for rows in arms]
        stripped[label] = dict(disabled_p99_ns=off, enabled_p99_ns=on, difference_ns=on-off)
    rank_samples = []
    for rows in arms:
        ordered = sorted(rows, key=lambda r: (r['elapsed_ns'], r['commit_sequence']))
        row = ordered[(len(ordered)*99+99)//100-1]
        rank_samples.append({key: row[key] for key in ('commit_sequence', 'group_id', 'elapsed_ns', 'first_wait_ns', 'before_sync_in_first_wait_ns', 'sync_wait_overlap_ns', 'submit_to_publication_ns', 'checkpoint_id', 'drain_required')})
    observation = source['cells'][1]['trace_analysis']['maximum_positional_write_enclosure']
    assert observation['unique'] is True
    return dict(
        schema='riffdb.wp749.archive-p99-decomposition/v1',
        source_summary_sha256=digest(source_path),
        source_revision=source['source_revision'],
        execution_receipt_digests={name:digest(root/name/'execution.json') for name in ('01-disabled','02-enabled')},
        source_file_digests={cell['cell']:cell['source_file_digests'] for cell in source['cells']},
        request_counts=[len(rows) for rows in arms],
        percentile_method='Empirical nearest rank ceil(99*N/100); weighted CDF uses exact integer masses.',
        exposure_standardization=split,
        request_subtraction_sensitivity=stripped,
        pre_sync_subtraction_change_in_contrast_ns=split['raw_difference_ns']-stripped['own_checkpoint_pre_sync_wait']['difference_ns'],
        sensitivity_interaction_warning='Do not add exposure standardization and interval subtraction effects: they are different interventions on the same observed distributions, not disjoint time components.',
        subtraction_limits='Descriptive sensitivity only: subtract the actual contained interval from each request, then re-sort. Keep all other durations and observed dependencies fixed. This is neither a replay nor the request latency after removing the mechanism; predecessor-induced queueing is not removed.',
        raw_p99_rank_samples=rank_samples,
        enabled_write_observation={key:observation[key] for key in ('group_id', 'first_sequence', 'last_sequence', 'checkpoint_id', 'drain_required', 'max_positional_write_us', 'first_wait_ns', 'sync_ns', 'submit_to_sync_ns')},
        identification_limit='The traces contain client elapsed duration and commit-sequence joins, not aligned request admission/queue intervals or counterfactual checkpoint schedules. A shared mechanism can change severity in either arm. The within-state residual cannot be divided causally into shared versus archive-specific time from these observations.',
        gate_disposition='No small, named archive-specific residual established; the existing unchanged-acknowledgement-latency gate remains unmet. This analysis does not authorize another measurement or an amendment.',
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path)
    parser.add_argument('--source-summary', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    result = analyze(args.results, args.source_summary)
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result['exposure_standardization'], indent=2))
