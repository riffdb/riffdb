"""Both exact built arms must qualify; local timings are not release evidence."""
WORKLOAD = dict(dataset='full', clients=32, load='write_only', warmup_seconds=15, measurement_seconds=90)

def verify(freeze, build, gate):
    assert build['source_freeze'] == freeze == gate['source_freeze']
    assert gate['local_correctness_passed'] is True and gate['performance_verdict'] is False
    assert set(gate['arms']) == {'control', 'candidate'}
    assert build['arms']['control']['daemon_sha256'] != build['arms']['candidate']['daemon_sha256']
    assert gate['runner_sha256'] == build['runner_sha256']
    for arm, row in gate['arms'].items():
        assert row['revision'] == freeze[arm] == build['arms'][arm]['revision']
        assert row['daemon_sha256'] == build['arms'][arm]['daemon_sha256']
        assert row['workload'] == WORKLOAD
        assert row['correctness_clean'] is True and row['complete_analysis_passed'] is True
        assert row['report_scale']['profile'] == 'full' and row['report_scale']['approximate_row_count'] == 19220
        assert row['measured_elapsed_ns'] >= 89_000_000_000
        assert row['samples'] > 0 and row['checkpoints'] > 0 and row['dropped_samples'] == 0
        assert row['exit_code'] == 0
