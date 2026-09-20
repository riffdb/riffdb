#![forbid(unsafe_code)]

//! Attributes per-write cost to one contract mechanism at a time.

use std::path::PathBuf;

use riffdb_perf_surface::daemon::riffdbd_binary;
use riffdb_perf_surface::measure::{VariantMeasurement, measure_variant};
use riffdb_perf_surface::{contract_source, variants};

fn main() -> std::process::ExitCode {
    let documents: u64 = std::env::var("PERF_SURFACE_DOCUMENTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2_000);
    let concurrency: usize = std::env::var("PERF_SURFACE_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);

    let Some(binary) = riffdbd_binary() else {
        eprintln!(
            "no riffdbd binary: build it with `cargo build --release --bin riffdbd` \
             or set RIFFDB_PERF_SURFACE_RIFFDBD_BIN"
        );
        return std::process::ExitCode::FAILURE;
    };

    let root = std::env::var("PERF_SURFACE_RUN_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("perf-surface-run"));
    let _ = std::fs::remove_dir_all(&root);

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("runtime: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };

    let mut measurements = Vec::new();
    for (name, mechanisms) in variants() {
        let run_dir = root.join(&name);
        let source = contract_source(&mechanisms);
        match runtime.block_on(measure_variant(
            &binary,
            &run_dir,
            &name,
            &source,
            documents,
            concurrency,
        )) {
            Ok(measurement) => {
                report_row(&measurement);
                measurements.push(measurement);
            }
            Err(error) => {
                eprintln!("variant {name} failed: {error}");
                return std::process::ExitCode::FAILURE;
            }
        }
        let _ = std::fs::remove_dir_all(&run_dir);
    }

    report_deltas(&measurements);
    std::process::ExitCode::SUCCESS
}

fn report_row(measurement: &VariantMeasurement) {
    println!(
        "perf-surface {name} documents={documents} clients={clients} \
         docs_per_s={rate:.0} \
         commits={commits} docs_per_commit={batch:.1} \
         frame_bytes_per_doc={frame:.1} \
         segment_bytes_per_doc={segment:.1} \
         writer_busy_us_per_doc={busy:.1} \
         commit_us_per_doc={commit:.1} \
         final_apply_us_per_doc={apply:.1}",
        name = measurement.name,
        documents = measurement.documents,
        clients = measurement.concurrency,
        rate = measurement.documents_per_second(),
        commits = measurement.committed_commands,
        batch = measurement.documents_per_commit(),
        frame = measurement.frame_bytes_per_document(),
        segment = measurement.segment_bytes_per_document(),
        busy = measurement.writer_busy_us_per_document(),
        commit = measurement.commit_us_per_document(),
        apply = measurement.final_apply_us_per_document(),
    );
}

/// Every mechanism against the baseline. The byte deltas are the attribution;
/// the throughput delta is reported beside them and is only meaningful when it
/// clears this host's run-to-run spread.
fn report_deltas(measurements: &[VariantMeasurement]) {
    let Some(base) = measurements.iter().find(|row| row.name == "base") else {
        return;
    };
    println!();
    for row in measurements.iter().filter(|row| row.name != "base") {
        let frame = row.frame_bytes_per_document() - base.frame_bytes_per_document();
        let segment = row.segment_bytes_per_document() - base.segment_bytes_per_document();
        let rate = if base.documents_per_second() > 0.0 {
            (row.documents_per_second() / base.documents_per_second() - 1.0) * 100.0
        } else {
            0.0
        };
        println!(
            "perf-surface delta {name} vs base: \
             frame_bytes_per_doc={frame:+.1} \
             segment_bytes_per_doc={segment:+.1} \
             writer_busy_us_per_doc={busy:+.1} \
             final_apply_us_per_doc={apply:+.1} \
             throughput={rate:+.1}%",
            name = row.name,
            busy = row.writer_busy_us_per_document() - base.writer_busy_us_per_document(),
            apply = row.final_apply_us_per_document() - base.final_apply_us_per_document(),
        );
    }
}
