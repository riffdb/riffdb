//! End-to-end dead-peer abort: kill -9 riffdbd mid-load and require fast failure.
//!
//! Requires a built `riffdbd`. Resolution order:
//! 1. `RIFFDB_APP_BASELINE_RIFFDBD_BIN`
//! 2. `target/{release,debug}/riffdbd` relative to the repo root
//!
//! When no binary is found the test soft-skips unless `RUN_RIFFDB_DEAD_PEER=1`
//! is set (then it fails). The app-baseline wrapper can export that env after
//! building riffdbd to make this test mandatory in CI.

#![forbid(unsafe_code)]

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use riffdb_app_baseline_core::{
    AppBackend, LoadConfig, LoadExecutionShape, SeedDataset, WorkloadProfile,
    run_closed_loop_load_with_abort,
};
use riffdb_app_baseline_riffdb::{
    RiffDbServerSession, ServerStartOptions, min_free_bytes_for_full,
};

fn resolve_riffdbd() -> Option<PathBuf> {
    if let Some(path) = env::var_os("RIFFDB_APP_BASELINE_RIFFDBD_BIN") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Some(path);
        }
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)?;
    for rel in ["target/release/riffdbd", "target/debug/riffdbd"] {
        let candidate = repo.join(rel);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

#[test]
fn kill_daemon_mid_load_aborts_within_two_seconds() {
    let Some(riffdbd) = resolve_riffdbd() else {
        if env::var_os("RUN_RIFFDB_DEAD_PEER").is_some() {
            panic!(
                "RUN_RIFFDB_DEAD_PEER=1 but riffdbd not found; build with \
                 `cargo +1.97.0 build -p riffdb-server --bin riffdbd` or set \
                 RIFFDB_APP_BASELINE_RIFFDBD_BIN"
            );
        }
        eprintln!(
            "dead_peer_abort: soft-skip (no riffdbd); set RUN_RIFFDB_DEAD_PEER=1 to require"
        );
        return;
    };

    let database_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("perf-db")
        .join("dead-peer-abort");
    let _ = std::fs::create_dir_all(&database_root);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    let session = runtime
        .block_on(RiffDbServerSession::start_with_options(
            &riffdbd,
            ServerStartOptions {
                database_root: Some(database_root.clone()),
                allow_tmpfs: false,
                min_free_bytes: min_free_bytes_for_full(false),
                ..ServerStartOptions::default()
            },
        ))
        .expect("start riffdbd session");

    let child_pid = session.child_pid();
    let session = Arc::new(Mutex::new(session));

    let dataset = SeedDataset::generate(riffdb_app_baseline_core::Scale::smoke());
    {
        let mut guard = session.lock().expect("lock");
        guard.backend.reset().expect("reset");
        guard.backend.seed(&dataset).expect("seed");
    }

    let kill_at = Arc::new(Mutex::new(None::<Instant>));
    let kill_at_writer = Arc::clone(&kill_at);
    let killer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(500));
        let at = Instant::now();
        let status = Command::new("kill")
            .args(["-9", &child_pid.to_string()])
            .status();
        *kill_at_writer.lock().expect("kill_at") = Some(at);
        assert!(
            status.as_ref().map(|s| s.success()).unwrap_or(false),
            "kill -9 {child_pid} failed: {status:?}"
        );
    });

    let abort = {
        let session = Arc::clone(&session);
        Arc::new(move || {
            let mut guard = session.lock().ok()?;
            guard.server_alive().err()
        }) as Arc<dyn Fn() -> Option<String> + Send + Sync>
    };

    let prototype = {
        let guard = session.lock().expect("lock");
        guard.backend.clone().with_command_attempt_budget(1)
    };

    let mut config = LoadConfig::smoke(WorkloadProfile::Interactive);
    config.clients = 4;
    config.duration = Duration::from_secs(12);
    config.warmup = Duration::from_millis(200);

    let load_started = Instant::now();
    let result = run_closed_loop_load_with_abort(
        "riffdb_public_grpc",
        config,
        LoadExecutionShape {
            transport_topology: "per_session_http2_connection",
            command_attempt_budget: 1,
        },
        &dataset,
        0,
        move || {
            let mut backend = prototype
                .fresh_session()
                .map_err(|error| error.to_string())?;
            backend.prewarm().map_err(|error| error.to_string())?;
            Ok(backend)
        },
        Some(abort),
    );
    let _ = killer.join();

    let kill_instant = kill_at
        .lock()
        .expect("kill_at")
        .expect("killer should have recorded kill time");
    let since_kill = kill_instant.elapsed();

    // Drop session (may already be dead); ignore shutdown errors.
    if let Ok(owned) = Arc::try_unwrap(session) {
        let _ = owned.into_inner().map(|s| s.shutdown());
    }

    let err = result.expect_err("load must fail after kill -9 of riffdbd");
    assert!(
        err.contains("backend died mid-load") || err.contains("exited mid-load"),
        "unexpected error text: {err}"
    );
    assert!(
        err.contains("signal=9") || err.contains("signal=SIGKILL") || err.contains("signal 9")
            || err.contains("exit"),
        "death message must mention exit/signal: {err}"
    );
    // stderr_tail may be empty if the process was SIGKILL'd before writing; still
    // require the death message scaffolding (database_root / free_bytes).
    assert!(
        err.contains("database_root=") || err.contains("stderr_tail"),
        "death message must include diagnostic fields: {err}"
    );
    assert!(
        since_kill <= Duration::from_secs(2),
        "abort took {since_kill:?} after kill (limit 2s); total load {:?}",
        load_started.elapsed()
    );
}
