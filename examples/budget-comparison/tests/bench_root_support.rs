//! Shared real-disk placement for budget-comparison integration tests.
//!
//! Naked `cargo test` previously landed multi-GB redb files on tmpfs via
//! `std::env::temp_dir()`. Roots now resolve under
//! `examples/budget-comparison/target/perf-db/` (or `RIFFDB_BENCH_DB_ROOT`).

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_bench_root::{
    BENCH_DB_ROOT_ENV, BenchDir, BenchRoot, BenchRootOptions, default_perf_db_root, sweep_stale,
};

static NEXT: AtomicU64 = AtomicU64::new(1);

/// Resolves the budget-comparison bench root (env → default under this package).
pub(crate) fn budget_bench_root() -> BenchRoot {
    let default_root =
        default_perf_db_root(Path::new(env!("CARGO_MANIFEST_DIR")), "budget-comparison");
    // Tests may run under constrained CI disks; require only a modest floor.
    // Production publish scripts still place roots on real NVMe via TMPDIR /
    // RIFFDB_BENCH_DB_ROOT redirection.
    let root = BenchRoot::resolve(BenchRootOptions {
        harness: "budget-comparison",
        cli_override: None,
        default_root,
        // Honor allow when the unified env forces a tmpfs (never preferred).
        allow_tmpfs: std::env::var_os(BENCH_DB_ROOT_ENV).is_some()
            || std::env::var_os("RIFFDB_BENCH_ALLOW_TMPFS").is_some(),
        min_free_bytes: 64 * 1024 * 1024,
    })
    .unwrap_or_else(|error| {
        // Last-resort: still avoid anonymous /tmp by using the package target tree
        // with allow_tmpfs so unit correctness tests can run on constrained hosts.
        let fallback = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("perf-db")
            .join("budget-comparison");
        BenchRoot::resolve(BenchRootOptions {
            harness: "budget-comparison",
            cli_override: Some(fallback),
            default_root: PathBuf::from("target/perf-db/budget-comparison"),
            allow_tmpfs: true,
            min_free_bytes: 0,
        })
        .unwrap_or_else(|fallback_error| {
            panic!("budget bench root unavailable: {error}; fallback failed: {fallback_error}");
        })
    });
    let _ = sweep_stale(&root);
    root
}

/// Creates a unique directory under the budget bench root; removes on drop.
pub(crate) fn unique_bench_dir(label: &str) -> BenchDir {
    let root = budget_bench_root();
    BenchDir::create(&root, label).unwrap_or_else(|error| {
        panic!("create bench dir ({label}): {error}");
    })
}

/// Unique file path under the bench root (caller owns cleanup if not using BenchDir).
pub(crate) fn unique_bench_path(label: &str) -> PathBuf {
    let root = budget_bench_root();
    root.path().join(format!(
        "{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
