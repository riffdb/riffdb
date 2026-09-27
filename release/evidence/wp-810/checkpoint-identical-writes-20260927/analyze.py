from pathlib import Path
import json, statistics
from validate_cell import validate, sha

def analyze_cell(cell, local=False):
    result = validate(cell/'report.json', cell/'raw/raw-request-tail.json', cell/'run.log')
    report = json.loads((cell/'report.json').read_text())
    point = report['curve'][0]
    evidence = point['server_stage_evidence']
    assert point['outcomes']['error'] == point['outcomes']['unavailable'] == 0
    assert point['clients'] == 32 and point['profile'] == 'write_only'
    assert evidence['archive_collection']['enabled'] is False
    assert evidence['writer_checkpoint_census']['failures'] == 0
    assert evidence['writer_publication_stages']['instrument_version'] == 2
    assert evidence['writer_publication_stages']['receipt_wait_errors'] == 0
    assert evidence['writer_checkpoint_census']['starts'] > 0
    if not local:
        for name in ('preflight.json', 'postflight.json'):
            assert json.loads((cell/name).read_text())['valid'] is True
    result.update(throughput_ops_s=point['throughput_ops_s'],
                  measured_elapsed_ns=point['measured_elapsed_ns'], logical_ops=point['logical_ops'],
                  writer=evidence['writer'], checkpoint=evidence['writer_checkpoint_census'],
                  journal=evidence['writer_journal_stages'], publication=evidence['writer_publication_stages'],
                  flush=evidence['writer_flush_census'], frames=evidence['writer_frame_census'],
                  identity=json.loads((cell/'execution.json').read_text()),
                  scope='Writer/checkpoint/wait counters include warmup; raw client latency and throughput are measured-only. Journal and publication timers overlap the writer and each other.')
    (cell/'analysis.json').write_text(json.dumps(result, indent=2)+'\n')
    return result

def compare(root):
    names = ['01-control', '02-candidate', '03-candidate', '04-control', '05-control', '06-candidate']
    cells = {name: analyze_cell(root/name) for name in names}
    for arm in ('control', 'candidate'):
        assert len({v['identity']['daemon_sha256'] for k,v in cells.items() if k.endswith(arm)}) == 1
    assert len({v['identity']['daemon_sha256'] for v in cells.values()}) == 2
    assert len({v['identity']['runner_sha256'] for v in cells.values()}) == 1
    assert len({v['raw_arrays_sha256'] for v in cells.values()}) == 6, 'reused sample arrays'
    pairs = []
    for control, candidate in [('01-control','02-candidate'), ('04-control','03-candidate'), ('05-control','06-candidate')]:
        a,b = cells[control],cells[candidate]
        pairs.append(dict(control=control, candidate=candidate,
            latency_change_percent={q:100*(b['raw_percentiles'][q]/a['raw_percentiles'][q]-1) for q in ('p50_ns','p95_ns','p99_ns')},
            throughput_change_percent=100*(b['throughput_ops_s']/a['throughput_ops_s']-1)))
    result = dict(cells=cells, pairs=pairs,
        median_paired_latency_change_percent={q:statistics.median(p['latency_change_percent'][q] for p in pairs) for q in ('p50_ns','p95_ns','p99_ns')},
        median_paired_throughput_change_percent=statistics.median(p['throughput_change_percent'] for p in pairs),
        verdict='Evidence only. No automatic package closure, observer-overhead claim, or WP-749 archive verdict.')
    (root/'comparison.json').write_text(json.dumps(result, indent=2)+'\n')
    return result

if __name__ == '__main__':
    import sys
    compare(Path(sys.argv[1]))
