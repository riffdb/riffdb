#![forbid(unsafe_code)]
//! WP-800. Does the staging cost of a vector field scale with its payload?
//!
//! The attribution puts about 60 percent of the added writer time in serial
//! staging, which is where the evaluated command body is put onto the open
//! batch, and the body is about 1,940 bytes larger with a four-component
//! vector. If that is the whole story, staging time per added byte should be
//! roughly constant as the vector widens. If it climbs, something is doing
//! per-element or repeated work and the cost is not simply carriage.
//!
//! Throughput is not measurable on this workstation. Per-command stage charges
//! are, which is what this reads.
use std::collections::BTreeMap;

use riffdb_perf_surface::daemon::riffdbd_binary;
use riffdb_perf_surface::measure::{VariantMeasurement, measure_variant};
use riffdb_perf_surface::{Mechanism, contract_source};

/// Renders the vector variant with a different embedding width.
fn vector_source_with_dimension(dimension: usize) -> String {
    let source = contract_source(&[Mechanism::Vector]);
    assert!(
        source.contains("embedding(4,") && source.contains("input embedding: vector<4>"),
        "the vector variant declares a four-component embedding to rewrite"
    );
    source
        .replace("embedding(4,", &format!("embedding({dimension},"))
        .replace(
            "input embedding: vector<4>",
            &format!("input embedding: vector<{dimension}>"),
        )
}

fn stage_ns_per_command(measurement: &VariantMeasurement, stage: &str) -> f64 {
    let line = measurement
        .stdout_tail
        .iter()
        .find(|line| line.starts_with("riffdb-writer-batch-stages-v1"))
        .expect("writer batch census");
    let mut labels: Vec<String> = Vec::new();
    let mut windows: Vec<&str> = Vec::new();
    for field in line.split('\t') {
        if let Some(value) = field.strip_prefix("labels=") {
            labels = value.split(',').map(str::to_owned).collect();
        } else if let Some(value) = field.strip_prefix("windows=") {
            windows = value.split(';').filter(|w| !w.is_empty()).collect();
        }
    }
    let index = labels
        .iter()
        .position(|label| label == stage)
        .expect("named stage");
    let mut total = 0_u64;
    let mut commands = 0_u64;
    for window in windows {
        let values: Vec<u64> = window
            .split(',')
            .map(|value| value.parse().expect("census value"))
            .collect();
        total = total.saturating_add(values[index + 1]);
        commands = commands.saturating_add(values[labels.len() + 1]);
    }
    assert!(commands > 0, "the census saw commands");
    total as f64 / commands as f64
}

#[test]
#[ignore = "diagnostic: does vector staging cost scale with payload width?"]
fn vector_staging_cost_scales_with_the_payload_it_carries() {
    assert!(
        std::env::var("RIFFDB_WRITER_BATCH_DIAGNOSTICS").as_deref() == Ok("1"),
        "run with RIFFDB_WRITER_BATCH_DIAGNOSTICS=1"
    );
    let binary = riffdbd_binary().expect("riffdbd");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("rt");

    // Three repetitions per width. Staging varies by about 8 percent of its
    // mean between runs, so a single pair cannot tell a flat cost from one that
    // grew by less than that.
    let mut samples: BTreeMap<usize, Vec<(f64, f64, f64)>> = BTreeMap::new();
    for rep in 0..3 {
        for dimension in [0_usize, 4, 16, 64] {
            let (name, source) = if dimension == 0 {
                ("base".to_owned(), contract_source(&[]))
            } else {
                (
                    format!("vector{dimension}"),
                    vector_source_with_dimension(dimension),
                )
            };
            let dir =
                std::env::temp_dir().join(format!("ps-vecdim-{}-{name}-{rep}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            let measurement = runtime
                .block_on(measure_variant(&binary, &dir, &name, &source, 2_000, 32))
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            let _ = std::fs::remove_dir_all(&dir);
            samples.entry(dimension).or_default().push((
                measurement.frame_bytes_per_document(),
                stage_ns_per_command(&measurement, "exec_stage_serial"),
                stage_ns_per_command(&measurement, "unit_execute"),
            ));
        }
    }

    let mean = |values: &[f64]| values.iter().sum::<f64>() / values.len() as f64;
    let column = |rows: &[(f64, f64, f64)], pick: fn(&(f64, f64, f64)) -> f64| {
        rows.iter().map(pick).collect::<Vec<_>>()
    };
    let base = &samples[&0];
    let base_bytes = mean(&column(base, |row| row.0));
    let base_staging = mean(&column(base, |row| row.1));
    let base_execute = mean(&column(base, |row| row.2));
    println!(
        "VECDIM base bytes={base_bytes:.0} staging={base_staging:.0}ns execute={base_execute:.0}ns"
    );
    for (dimension, rows) in samples.iter().filter(|(d, _)| **d != 0) {
        let staging: Vec<f64> = column(rows, |row| row.1);
        let added_bytes = mean(&column(rows, |row| row.0)) - base_bytes;
        let added_staging = mean(&staging) - base_staging;
        let spread = staging.iter().copied().fold(f64::MIN, f64::max)
            - staging.iter().copied().fold(f64::MAX, f64::min);
        println!(
            "VECDIM dim={dimension:<3} bytes=+{added_bytes:<8.0} staging=+{added_staging:<9.0}ns \
             (spread {spread:.0}) execute=+{:<9.0}ns ns_per_added_byte={:.3}",
            mean(&column(rows, |row| row.2)) - base_execute,
            if added_bytes > 0.0 {
                added_staging / added_bytes
            } else {
                0.0
            }
        );
    }
}
