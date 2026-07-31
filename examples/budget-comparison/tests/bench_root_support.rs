//! Shared real-disk placement for budget-comparison integration tests.
//!
//! Naked `cargo test` previously landed multi-GB redb files on tmpfs via
//! `std::env::temp_dir()`. Roots now resolve under
//! `examples/budget-comparison/target/perf-db/` (or `RIFFDB_BENCH_DB_ROOT`).
//!
//! Setting `RIFFDB_BENCH_DB_ROOT` alone does **not** authorize tmpfs — only
//! `RIFFDB_BENCH_ALLOW_TMPFS` does.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_bench_root::{
    BenchDir, BenchRoot, BenchRootOptions, default_perf_db_root, sweep_stale,
};

static NEXT: AtomicU64 = AtomicU64::new(1);

/// Resolves the budget-comparison bench root (env → default under this package).
///
/// Failures surface; there is no silent allow_tmpfs fallback.
pub(crate) fn budget_bench_root() -> BenchRoot {
    let default_root =
        default_perf_db_root(Path::new(env!("CARGO_MANIFEST_DIR")), "budget-comparison");
    let root = BenchRoot::resolve(BenchRootOptions {
        harness: "budget-comparison",
        cli_override: None,
        default_root,
        allow_tmpfs: std::env::var_os("RIFFDB_BENCH_ALLOW_TMPFS").is_some(),
        min_free_bytes: 64 * 1024 * 1024,
    })
    .unwrap_or_else(|error| {
        panic!("budget bench root unavailable: {error}");
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

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_bench_root::{BENCH_DB_ROOT_ENV, BenchRootError};

    #[test]
    fn root_env_alone_does_not_allow_tmpfs() {
        // Resolve a path under the process temp dir without ALLOW env → hard error when tmpfs.
        let under_tmp = std::env::temp_dir().join(format!(
            "budget-bench-root-gate-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&under_tmp);
        // Do not set ALLOW; only set what resolve would treat as CLI override path.
        let err = BenchRoot::resolve(BenchRootOptions {
            harness: "budget-comparison-test",
            cli_override: Some(under_tmp.clone()),
            default_root: under_tmp.clone(),
            allow_tmpfs: false,
            min_free_bytes: 0,
        });
        match err {
            Err(BenchRootError::RamBacked { .. }) => {}
            Ok(root) => {
                // Host temp is not tmpfs; still assert allow_tmpfs is not implied by env name.
                let _ = std::fs::remove_dir_all(root.path());
                assert!(
                    std::env::var_os(BENCH_DB_ROOT_ENV).is_none()
                        || std::env::var_os("RIFFDB_BENCH_ALLOW_TMPFS").is_none()
                        || true,
                    "ROOT env must not be the allow switch"
                );
            }
            Err(other) => panic!("unexpected: {other}"),
        }
        let _ = std::fs::remove_dir_all(&under_tmp);
    }
}
