"""Publish selected numerical evidence without private host/build inventory."""
from pathlib import Path
import hashlib
import json
import statistics
import sys


def summarize(root):
    comparison = json.loads((root / 'results/comparison.json').read_text())
    build = json.loads((root / 'build-complete.json').read_text())
    cells = []
    for name, cell in comparison['cells'].items():
        journal, publication = cell['journal'], cell['publication']
        checks = {}
        for phase in ('preflight', 'postflight'):
            path = root / 'results' / name / f'{phase}.json'
            checks[phase] = {
                'valid': json.loads(path.read_text())['valid'],
                'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            assert checks[phase]['valid'] is True
        row = {key: cell[key] for key in (
            'throughput_ops_s', 'logical_ops', 'measured_elapsed_ns',
            'raw_percentiles', 'raw_mean_ns', 'raw_max_ns', 'sample_count',
            'raw_arrays_sha256', 'raw_file_sha256', 'report_sha256', 'log_sha256',
            'slowest_20', 'slowest_20_range_ns', 'slowest_20_unique_values',
            'independent_wall', 'checkpoint', 'journal', 'publication',
        )}
        row.update(name=name, host_checks=checks,
                   writer_level0_ns=cell['writer_census']['level0_ns'],
                   mean_journal_sync_us=journal['sync_sum_us'] / journal['physical_flushes'],
                   mean_journal_queue_us=journal['queue_sum_us'] / journal['queue_observations'],
                   mean_receipt_block_us=publication['receipt_block_sum_us'] / publication['receipt_wait_calls'])
        cells.append(row)
    by_name = {cell['name']: cell for cell in cells}
    pairs = comparison['pairs']
    means = ('mean_journal_sync_us', 'mean_journal_queue_us', 'mean_receipt_block_us')
    for pair in pairs:
        control, candidate = by_name[pair['control']], by_name[pair['candidate']]
        pair['wait_mean_change_percent'] = {
            key: 100 * (candidate[key] / control[key] - 1) for key in means
        }
    return {
        'schema': 'riffdb.wp810-identical-write-result/v1',
        'status': 'six valid cells; measured mitigation awaiting maintainer disposition',
        'host_specifications': '8 vCPU AMD EPYC 9B14; hardware SHA-2, AES, carry-less multiply and SSE4.2',
        'toolchain': build['toolchain'],
        'build_environment': {key: build['build_environment'][key] for key in (
            'RUSTFLAGS', 'CARGO_BUILD_JOBS', 'CARGO_INCREMENTAL', 'RUSTC_WRAPPER',
            'RUSTUP_TOOLCHAIN', 'CARGO_PROFILE_RELEASE_LTO', 'CARGO_PROFILE_RELEASE_CODEGEN_UNITS',
        )},
        'binaries': build['arms'],
        'runner_sha256': build['runner_sha256'],
        'observer_patch_sha256': build['source_freeze']['inputs']['common-observation.patch'],
        'all_six_raw_arrays_distinct': len({c['raw_arrays_sha256'] for c in cells}) == 6,
        'pairs': pairs,
        'median_paired_latency_change_percent': comparison['median_paired_latency_change_percent'],
        'median_paired_throughput_change_percent': comparison['median_paired_throughput_change_percent'],
        'median_paired_wait_mean_change_percent': {
            key: statistics.median(p['wait_mean_change_percent'][key] for p in pairs) for key in means
        },
        'cells': cells,
        'limitations': [
            'Client percentiles and throughput are measured-only; writer, checkpoint and journal counters include warmup.',
            'Journal and publication timers overlap writer time and each other; they are never summed into wall time.',
            'Throughput is within the recorded 2-4 percent host variance; that variance is not a P95 or P99 noise band.',
            'P99 falls substantially in two pairs and is nearly flat in one. Its host repeatability is not qualified.',
            'Long journal waits persist. The paired observer cannot causally join each request to native I/O or count production writes omitted.',
            'The pinned-engine duplicate-write census is a separate workload and establishes no production byte-reduction percentage.',
            'No merge, WP-810 completion, or WP-749 archive-gate pass is recorded by this report.',
        ],
    }


if __name__ == '__main__':
    Path(sys.argv[2]).write_text(json.dumps(summarize(Path(sys.argv[1])), indent=2) + '\n')
