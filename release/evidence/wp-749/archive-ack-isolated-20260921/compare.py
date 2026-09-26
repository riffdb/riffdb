from pathlib import Path
import json
from correlate import correlate, distribution

def summarize(cell, enabled):
    result = correlate(cell)
    report = json.loads((cell/'report.json').read_text())
    point = report['curve'][0]
    assert point['outcomes']['error'] == point['outcomes']['unavailable'] == 0
    archive = point['server_stage_evidence']['archive_collection']
    assert archive['enabled'] is enabled
    if enabled:
        assert archive['archived_application_sequence'] > archive['backup_application_sequence']
        assert archive['manifest_digest'] and not archive['collector_terminal_failure_observed']
    else:
        assert archive['archived_application_sequence'] is None and archive['manifest_digest'] is None
    rows = json.loads((cell/'joined-requests.json').read_text())
    tail = [r for r in rows if r['elapsed_ns'] >= result['base']['raw_percentiles']['p99_ns']]
    for r in rows:
        r['outside_own_group_ns'] = r['elapsed_ns'] - r['submit_to_publication_ns']
        assert r['outside_own_group_ns'] >= 0, 'own group interval exceeds its request duration'
    cps = result['checkpoints']
    details = dict(result,
        throughput_ops_s=point['throughput_ops_s'], archive=archive,
        client_tail=distribution([r['elapsed_ns'] for r in tail]),
        client_tail_outside_own_group=distribution([r['outside_own_group_ns'] for r in tail]),
        all_requests_outside_own_group=distribution([r['outside_own_group_ns'] for r in rows]),
        checkpoint_registered=distribution([c[3]-c[1] for c in cps]),
        checkpoint_materialize=distribution([c[2]-c[1] for c in cps]),
        checkpoint_install_after_materialize=distribution([c[3]-c[2] for c in cps]),
        tail_drain_percent=100*result['p99_tail_drain_count']/result['p99_tail_count'],
        all_drain_percent=100*result['all_drain_count']/len(rows),
        request_population='Measured successful write-only generated-client command calls, including response decode.',
        limits='One paired instrumented diagnostic, fixed off/on order; no estimate of observer overhead or order effects. Own-group server intervals are nested in client acknowledgement; remainder is reported, not attributed. Wait/sync overlap and group/flush sharing prohibit summation as writer wall. Census totals are not independently measured continuous wall. Correlation alone does not prove a resource-contention cause.')
    (cell/'attribution.json').write_text(json.dumps(details,indent=2)+'\n')
    return details

def compare(root):
    for cell in ('01-disabled', '02-enabled'):
        for endpoint in ('preflight.json', 'postflight.json'):
            validity = json.loads((root/cell/endpoint).read_text())
            assert validity['valid'] is True, (cell, endpoint, 'host-invalid pair is non-qualifying')
    off = summarize(root/'01-disabled', False)
    on = summarize(root/'02-enabled', True)
    assert off['base']['raw_arrays_sha256'] != on['base']['raw_arrays_sha256'], 'identical sample arrays'
    identities=[]
    for cell in ('01-disabled','02-enabled'):
        receipt=json.loads((root/cell/'execution.json').read_text())
        assert receipt['exit_code']==0
        identities.append((receipt['daemon_sha256'],receipt['runner_sha256']))
    assert identities[0]==identities[1], 'arms did not run the same binaries'
    output=dict(arms={'disabled':off,'enabled':on},
        raw_percentile_change_percent={k:100*(on['base']['raw_percentiles'][k]/off['base']['raw_percentiles'][k]-1) for k in ('p50_ns','p95_ns','p99_ns')},
        throughput_change_percent=100*(on['throughput_ops_s']/off['throughput_ops_s']-1),
        daemon_sha256=identities[0][0],runner_sha256=identities[0][1],
        verdict='Evidence only; no automatic mechanism, latency-parity or package-completion verdict.')
    (root/'comparison.json').write_text(json.dumps(output,indent=2)+'\n')
    return output

if __name__=='__main__':
    import sys
    r=compare(Path(sys.argv[1]))
    print(json.dumps({k:v for k,v in r.items() if k!='arms'},indent=2))
