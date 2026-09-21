#![forbid(unsafe_code)]
//! WP-800. Where the write-throughput cost of declaring a vector field goes.
//!
//! The cost is 19.95 percent at 32 clients on the C3D bench host, reproduced
//! over twelve repetitions with a standard deviation of 1.72
//! (`docs/performance/wp-791-derived-sinks-c3d-2026-09.md`). It is not
//! measurable on this workstation at all: six runs of the same shape put it
//! between 1.1 and 25.2 percent, with and without the census, so a throughput
//! attribution taken here would be fitting noise.
//!
//! What this probe asks is narrower and answerable anywhere: does the writer
//! batch-stage census resolve the difference between the two variants, and does
//! it resolve it the same way twice? A stage decomposition that moves as much as
//! the effect it explains is not an instrument, whatever host it runs on.
use std::collections::BTreeMap;

use riffdb_perf_surface::daemon::riffdbd_binary;
use riffdb_perf_surface::measure::{VariantMeasurement, measure_variant};
use riffdb_perf_surface::{Mechanism, contract_source};

/// Per-command nanoseconds for each labelled stage, from one daemon's census.
fn stage_ns_per_command(measurement: &VariantMeasurement) -> BTreeMap<String, f64> {
    let line = measurement
        .stdout_tail
        .iter()
        .find(|line| line.starts_with("riffdb-writer-batch-stages-v1"))
        .unwrap_or_else(|| {
            panic!(
                "{}: no writer batch census; is RIFFDB_WRITER_BATCH_DIAGNOSTICS=1 set?",
                measurement.name
            )
        });
    let mut labels: Vec<String> = Vec::new();
    let mut windows: Vec<&str> = Vec::new();
    for field in line.split('\t') {
        if let Some(value) = field.strip_prefix("labels=") {
            labels = value.split(',').map(str::to_owned).collect();
        } else if let Some(value) = field.strip_prefix("windows=") {
            windows = value.split(';').filter(|w| !w.is_empty()).collect();
        }
    }
    assert!(!labels.is_empty(), "census carries its stage labels");

    // Each window is count, then one total per stage, then commands and
    // submitted units. Summing across windows gives the whole run.
    let mut totals = vec![0_u64; labels.len()];
    let mut commands = 0_u64;
    for window in windows {
        let values: Vec<u64> = window
            .split(',')
            .map(|value| value.parse().expect("census value"))
            .collect();
        assert_eq!(values.len(), labels.len() + 3, "census window shape");
        for (index, total) in totals.iter_mut().enumerate() {
            *total = total.saturating_add(values[index + 1]);
        }
        commands = commands.saturating_add(values[labels.len() + 1]);
    }
    assert!(commands > 0, "the census saw commands");
    labels
        .into_iter()
        .zip(totals)
        .map(|(label, total)| (label, total as f64 / commands as f64))
        .collect()
}

#[test]
#[ignore = "diagnostic: does the writer census resolve the vector write cost?"]
fn the_writer_census_attribution_of_a_vector_field_is_reproducible() {
    assert!(
        std::env::var("RIFFDB_WRITER_BATCH_DIAGNOSTICS").as_deref() == Ok("1"),
        "run with RIFFDB_WRITER_BATCH_DIAGNOSTICS=1"
    );
    let binary = riffdbd_binary().expect("riffdbd");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("rt");

    let mut runs: Vec<BTreeMap<String, f64>> = Vec::new();
    let mut throughputs: Vec<f64> = Vec::new();
    for rep in 0..3 {
        let mut measured = BTreeMap::new();
        for (name, mechanisms) in [("base", Vec::new()), ("vector", vec![Mechanism::Vector])] {
            let dir = std::env::temp_dir()
                .join(format!("ps-vec-attr-{}-{name}-{rep}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            let source = contract_source(&mechanisms);
            let measurement = runtime
                .block_on(measure_variant(&binary, &dir, name, &source, 2_000, 32))
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            let _ = std::fs::remove_dir_all(&dir);
            measured.insert(name.to_owned(), measurement);
        }
        let base = &measured["base"];
        let vector = &measured["vector"];
        throughputs
            .push(vector.documents_per_second() / base.documents_per_second() * 100.0 - 100.0);
        let base_stages = stage_ns_per_command(base);
        let vector_stages = stage_ns_per_command(vector);
        let mut delta = BTreeMap::new();
        for (label, base_ns) in &base_stages {
            let vector_ns = vector_stages.get(label).copied().unwrap_or(0.0);
            delta.insert(label.clone(), vector_ns - base_ns);
        }
        runs.push(delta);
    }

    println!("VECATTR throughput deltas: {throughputs:?}");
    let labels: Vec<String> = runs[0].keys().cloned().collect();
    let mut ranked: Vec<(String, f64, f64, f64)> = labels
        .into_iter()
        .map(|label| {
            let values: Vec<f64> = runs.iter().map(|run| run[&label]).collect();
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let min = values.iter().copied().fold(f64::MAX, f64::min);
            let max = values.iter().copied().fold(f64::MIN, f64::max);
            (label, mean, min, max)
        })
        .collect();
    ranked.sort_by(|left, right| right.1.abs().partial_cmp(&left.1.abs()).expect("finite"));
    for (label, mean, min, max) in ranked.iter().take(22) {
        println!("VECATTR {label:<28} mean={mean:>10.0}ns min={min:>10.0} max={max:>10.0}");
    }
}
