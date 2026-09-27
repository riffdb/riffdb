from pathlib import Path
import hashlib, json, struct

def sha(path):
    with Path(path).open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()

def bucket_upper(n):
    if n <= 16:
        return n
    base = 1 << (n.bit_length() - 1)
    width = base // 16
    return base + ((n - base) // width + 1) * width - 1

def analyze(cell, enabled):
    r = json.loads((cell / 'report.json').read_text())
    raw = json.loads((cell / 'raw/raw-request-tail.json').read_text())
    assert r['correctness']['clean'] is True and len(r['curve']) == 1
    assert r['scale']['profile'] == 'full' and r['scale']['approximate_row_count'] == 19220
    p = r['curve'][0]
    assert p['clients'] == 32 and p['profile'] == 'write_only'
    assert p['outcomes']['error'] == p['outcomes']['unavailable'] == p['outcomes']['conflict'] == 0
    assert p['outcomes']['success'] == p['logical_ops']
    assert 90_000_000_000 <= p['measured_elapsed_ns'] < 95_000_000_000
    assert raw['schema'] == 'private.wp810-request-tail/v1'
    assert raw['valid'] is True and raw['dropped_samples'] == 0
    assert len(raw['workers']) == 32
    values, identified = [], []
    digest = hashlib.sha256()
    for worker, row in enumerate(raw['workers']):
        samples = row['samples_ns']
        assert row['worker'] == worker and row['dropped_samples'] == 0
        assert 0 < len(samples) <= 32768
        digest.update(struct.pack('<QQ', worker, len(samples)))
        for ordinal, ns in enumerate(samples):
            assert type(ns) is int and 0 < ns < 2**64
            values.append(ns)
            identified.append((ns, worker, ordinal))
            digest.update(struct.pack('<Q', ns))
    h = r['backends'][0]['aggregate']['latency']
    assert len(values) == raw['sample_count'] == raw['expected_sample_count'] == h['sample_count'] == p['logical_ops']
    assert min(values) == h['min_ns'] and max(values) == h['max_ns']
    assert sum(values) // len(values) == h['mean_ns']
    ordered = sorted(values)
    quantiles = {}
    for q in (50, 95, 99):
        n = ordered[(len(ordered) * q + 99) // 100 - 1]
        quantiles[f'p{q}_ns'] = n
        assert bucket_upper(n) == h[f'p{q}_ns'] == p[f'aggregate_p{q}_ns']
    top = [dict(elapsed_ns=n, worker=w, worker_ordinal=o)
           for n, w, o in sorted(identified, key=lambda t: (-t[0], t[1], t[2]))[:20]]
    assert top == raw['slowest_20']
    e = p['server_stage_evidence']
    archive = e['archive_collection']
    assert archive['enabled'] is enabled
    assert archive['backup_application_sequence'] == 19220
    assert archive['collector_terminal_failure_observed'] is False
    if enabled:
        assert archive['archived_application_sequence'] > 19220
        assert archive['archived_history_transactions'] > 0
        assert len(bytes.fromhex(archive['manifest_digest'])) == 32
    else:
        assert archive['archived_application_sequence'] is None
        assert archive['manifest_digest'] is None and archive['archived_history_transactions'] == 0
    checkpoint = e['writer_checkpoint_census']
    publication = e['writer_publication_stages']
    assert checkpoint['starts'] > 0 and checkpoint['failures'] == 0
    assert publication['instrument_version'] == 2 and publication['receipt_wait_errors'] == 0
    for name in ('preflight.json', 'postflight.json'):
        assert json.loads((cell / name).read_text())['valid'] is True
    assert json.loads((cell / 'execution.json').read_text())['exit_code'] == 0
    committed = e['writer']['logical_commands_committed']
    lag = 19220 + committed - archive['archived_application_sequence'] if enabled else None
    assert lag is None or lag >= 0
    result = dict(raw_percentiles=quantiles, sample_count=len(values), raw_arrays_sha256=digest.hexdigest(),
        slowest_20=top, raw_mean_ns=sum(values)//len(values), throughput_ops_s=p['throughput_ops_s'],
        measured_elapsed_ns=p['measured_elapsed_ns'], archive=archive, archive_lag_application_commands=lag,
        lag_method='backup application frontier plus process-generation committed commands minus confirmed archived frontier; includes warmup',
        checkpoint=checkpoint, publication=publication, journal=e['writer_journal_stages'],
        flush=e['writer_flush_census'], frames=e['writer_frame_census'], writer=e['writer'],
        resource_delta=p['resource_delta'],
        input_sha256={name:sha(cell/name) for name in ['report.json','raw/raw-request-tail.json','run.log','preflight.json','postflight.json','execution.json']},
        counter_scope='Counters include warmup. Raw acknowledgements are measured-only. Overlapping timers are not added or subtracted; busy plus idle is not writer wall.')
    (cell/'analysis.json').write_text(json.dumps(result, indent=2)+'\n')
    return result

def compare(root):
    off = analyze(root/'01-disabled', False)
    on = analyze(root/'02-enabled', True)
    assert off['raw_arrays_sha256'] != on['raw_arrays_sha256']
    identities = [json.loads((root/name/'execution.json').read_text()) for name in ['01-disabled','02-enabled']]
    assert len({x['daemon_sha256'] for x in identities}) == 1
    assert len({x['runner_sha256'] for x in identities}) == 1
    result = dict(disabled=off, enabled=on,
        latency_change_percent={q:100*(on['raw_percentiles'][q]/off['raw_percentiles'][q]-1) for q in off['raw_percentiles']},
        throughput_change_percent=100*(on['throughput_ops_s']/off['throughput_ops_s']-1),
        verdict='Evidence only; no automatic gate pass or attribution. Fixed order and one pair do not calibrate latency variance.')
    (root/'comparison.json').write_text(json.dumps(result, indent=2)+'\n')
    return result

if __name__ == '__main__':
    import sys
    compare(Path(sys.argv[1]))
