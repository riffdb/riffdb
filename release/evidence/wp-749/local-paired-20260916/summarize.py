"""Recompute this fixed local WP-749 disclosure from the retained raw reports."""
from pathlib import Path
import hashlib
import json
import statistics
import sys

root = Path(sys.argv[1]) if len(sys.argv) == 2 else Path(__file__).parent
protocol = json.loads((root / 'protocol.json').read_text())
assert hashlib.sha256((root / 'run.py').read_bytes()).hexdigest() == protocol['runner_script_sha256']
identity = protocol['identities']
results = {}
for cell in protocol['cells']:
    name = cell['name']
    raw = (root / f'{name}.json').read_bytes()
    report = json.loads(raw)
    execution = json.loads((root / f'{name}-execution.json').read_text())
    candidate = report['qualification_candidate']
    variant = identity['control' if name == 'prefix-control' else 'current']
    assert execution['exit_code'] == 0, name
    assert candidate['source_revision'] == variant['source_revision'], name
    assert candidate['source_tree_clean'] is True, name
    assert candidate['cargo_lock_sha256'] == variant['root_lock_sha256'], name
    assert candidate['riffdbd_sha256'] == variant['daemon_sha256'], name
    assert candidate['runner_sha256'] == identity['harness']['runner_sha256'], name
    assert report['correctness'] == {
        'clean': True, 'failures': [], 'server_shutdown_error': None
    }, name
    eligibility = report['evidence_eligibility']
    assert eligibility['eligible'] and eligibility['stable'], name
    assert not eligibility['comparison_complete'], name
    assert not eligibility['reason_codes'], name
    host = report['host_validity']
    assert host['preflight']['valid'] and host['postflight']['valid'], name
    generations = report['backends']
    assert len(generations) == 3, name
    archives = []
    for generation in generations:
        assert generation['profile'] == 'write_only', name
        assert generation['clients'] == 32, name
        assert generation['duration_ms'] == 90000, name
        assert generation['warmup_ms'] == 15000, name
        assert generation['resource_delta']['process_identity_stable'], name
        outcomes = generation['aggregate']['outcomes']
        assert outcomes['success'] == outcomes['logical_operations'] > 0, name
        archive = generation['server_stage_evidence']['archive_collection']
        mode = cell['archive_mode']
        if mode is None:
            assert archive is None, name
        else:
            assert archive['enabled'] == (mode == 'enabled'), name
            assert not archive['collector_terminal_failure_observed'], name
            assert archive['caught_up_at_shutdown'] == 'not_claimed', name
            if mode == 'enabled':
                assert archive['archived_application_sequence'] > archive['backup_application_sequence'], name
                assert archive['archived_history_transactions'] > 0, name
                assert len(archive['manifest_digest']) == 64, name
            else:
                assert archive['archived_application_sequence'] is None, name
                assert archive['archived_history_transactions'] == 0, name
                assert archive['manifest_digest'] is None, name
        archives.append(archive)
    summary = report['load_rep_summaries']['riffdb_public_grpc@clients=32']
    metrics = {key: summary[key] for key in [
        'throughput_ops_s', 'aggregate_p50_ns', 'aggregate_p95_ns', 'aggregate_p99_ns'
    ]}
    resources = {}
    for key in ['process_scope_committed_commands', 'cpu_ticks', 'rss_bytes_peak_sampled',
                'durable_bytes_growth_per_process_scope_committed_command',
                'process_write_bytes_per_process_scope_committed_command']:
        values = [generation['resource_delta'][key] for generation in generations]
        resources[key] = {'values': values, 'median': statistics.median(values)}
    results[name] = {
        'report_sha256': hashlib.sha256(raw).hexdigest(),
        'elapsed_seconds': execution['elapsed_seconds'],
        'metrics': metrics,
        'resources': resources,
        'resource_scope': generations[0]['resource_delta']['normalization_scope'],
        'archive_collection': archives,
        'evidence_eligibility': eligibility,
        'hardware': host['preflight']['hardware_identity'],
        'environment': report['environment'],
        'host_whole_cell': host['postflight']['whole_cell'],
    }
comparisons = {}
for label, before, after in [('prefix', 'prefix-control', 'prefix-current'),
                             ('archive', 'archive-disabled', 'archive-enabled')]:
    comparisons[label] = {'before': before, 'after': after, 'median_ratios': {}}
    for key in results[before]['metrics']:
        ratio = results[after]['metrics'][key]['median'] / results[before]['metrics'][key]['median']
        comparisons[label]['median_ratios'][key] = {
            'after_divided_by_before': ratio, 'change_percent': (ratio - 1) * 100
        }
print(json.dumps({
    'purpose': protocol['purpose'],
    'interpretation': 'Local fixed-order disclosure only; no added cross-variant threshold, causal isolation, cloud qualification, full catch-up or no-regression claim.',
    'cells': results, 'comparisons': comparisons
}, indent=2))
