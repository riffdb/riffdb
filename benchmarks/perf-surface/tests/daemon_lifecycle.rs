#![forbid(unsafe_code)]

//! The harness is worthless if it cannot start and stop the real daemon, so
//! that is proven before anything is measured through it.

use riffdb_perf_surface::daemon::{Daemon, riffdbd_binary};

#[test]
fn the_daemon_starts_becomes_ready_and_shuts_down_cleanly() {
    // Deliberately fatal rather than skipped. This crate is run on purpose, and
    // a harness test that passes when it measured nothing is worse than one
    // that fails: it reports success for a daemon it never started.
    let binary = riffdbd_binary().expect(
        "no riffdbd binary: build it with `cargo build --release --bin riffdbd` \
         or set RIFFDB_PERF_SURFACE_RIFFDBD_BIN",
    );

    let run_dir = std::env::temp_dir().join(format!("perf-surface-lifecycle-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&run_dir);

    let daemon = Daemon::start(&binary, &run_dir).expect("daemon starts and becomes ready");
    let endpoint = daemon.endpoint();
    assert!(
        endpoint.starts_with("http://127.0.0.1:"),
        "endpoint should be a loopback address, got {endpoint}"
    );
    assert!(
        !endpoint.ends_with(":0"),
        "the daemon must report the port it actually bound, got {endpoint}"
    );

    let stdout = daemon.shutdown().expect("daemon shuts down cleanly");

    // The shutdown census is what the measurement reads; if it stops being
    // emitted the harness must fail here rather than report empty attribution.
    assert!(
        stdout
            .iter()
            .any(|line| line.starts_with("riffdb-writer-frame-census-v1")),
        "shutdown must emit the writer frame census; got {} lines",
        stdout.len()
    );

    let _ = std::fs::remove_dir_all(&run_dir);
}
