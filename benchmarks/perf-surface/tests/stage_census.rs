#![forbid(unsafe_code)]
//! Captures stage attribution for the base and projection variants so the
//! projection's cost can be located rather than only measured.
use riffdb_perf_surface::daemon::riffdbd_binary;
use riffdb_perf_surface::measure::measure_variant;
use riffdb_perf_surface::{Mechanism, contract_source};

#[test]
#[ignore = "diagnostic: run with RIFFDB_WRITER_BATCH_DIAGNOSTICS=1"]
fn capture_stage_census() {
    let binary = riffdbd_binary().expect("riffdbd");
    let documents: u64 = std::env::var("PERF_SURFACE_DOCUMENTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);
    let concurrency: usize = std::env::var("PERF_SURFACE_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("rt");
    for (name, mechanisms) in [
        ("base", Vec::new()),
        ("projection", vec![Mechanism::Projection]),
    ] {
        let dir = std::env::temp_dir().join(format!("ps-stage-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let source = contract_source(&mechanisms);
        let m = runtime
            .block_on(measure_variant(
                &binary,
                &dir,
                name,
                &source,
                documents,
                concurrency,
            ))
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        println!("STAGES {name} docs_per_s={:.0}", m.documents_per_second());
        for line in &m.stdout_tail {
            if line.starts_with("riffdb-writer-batch-stages-v1") {
                println!("STAGELINE {name} {line}");
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
