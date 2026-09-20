#![forbid(unsafe_code)]
//! Answers one question the project has never had an answer to: does declaring
//! a vector field register a columnar source, and what does the engine report
//! after a write-only workload?
//!
//! WP-777 keeps every admitted source cold until a projected query demands it,
//! so writes alone must NOT activate one. A run that shows a cold source and
//! zero activations is the expected result and the prerequisite for measuring
//! activation.
//!
//! The first process of each pair reports no source at all, and that is not a
//! failed declaration: `ColumnarRuntime` resolves its source set at startup
//! from the active catalog, so a contract deployed into a running daemon is
//! invisible to the engine until the process restarts. The reopen below is
//! what gives the declaration a startup to be seen at. `columnar_activation`
//! carries the rest of that route, through to a served query.
use riffdb_perf_surface::daemon::riffdbd_binary;
use riffdb_perf_surface::measure::measure_variant;
use riffdb_perf_surface::{Mechanism, contract_source};

#[test]
#[ignore = "diagnostic: reports columnar source state for the vector variant"]
fn columnar_source_state_for_the_vector_variant() {
    let binary = riffdbd_binary().expect("riffdbd");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("rt");
    for (name, mechanisms) in [("base", Vec::new()), ("vector", vec![Mechanism::Vector])] {
        let dir = std::env::temp_dir().join(format!("ps-columnar-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let source = contract_source(&mechanisms);
        let measured = runtime
            .block_on(measure_variant(&binary, &dir, name, &source, 200, 8))
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        println!(
            "COLUMNAR {name} docs_per_s={:.0}",
            measured.documents_per_second()
        );
        let mut reported = false;
        for line in &measured.stdout_tail {
            for field in line.split_whitespace() {
                if field.starts_with("columnar_") {
                    println!("COLUMNARFIELD {name} {field}");
                    reported = true;
                }
            }
        }
        if !reported {
            println!("COLUMNARFIELD {name} (no columnar counters in daemon output)");
        }
        // The counters live in the startup census, which runs before any
        // contract is deployed, so a single process can never report on a
        // source its own run admitted. Reopen the same database and read the
        // census of a process that starts with the contract already present.
        let reopened = riffdb_perf_surface::daemon::Daemon::start(&binary, &dir)
            .unwrap_or_else(|error| panic!("{name} reopen: {error}"));
        // The startup census is written to stderr, not stdout, so it must be
        // read before shutdown consumes the handle.
        let mut lines = reopened.stderr_tail(400);
        lines.extend(
            reopened
                .shutdown()
                .unwrap_or_else(|error| panic!("{name} reopen shutdown: {error}")),
        );
        let mut seen = false;
        for line in &lines {
            for field in line.split_whitespace() {
                if field.starts_with("columnar_") {
                    println!("REOPENED {name} {field}");
                    seen = true;
                }
            }
        }
        if !seen {
            println!("REOPENED {name} (still no columnar counters)");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
