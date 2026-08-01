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
        // Construct options that mirror the conflation bug shape: a path under
        // process temp (tmpfs on this host) with allow_tmpfs=false. Setting
        // ROOT env is not required for resolve when cli_override is set; the
        // contract under test is that allow is NEVER implied by a path choice.
        let under_tmp = std::env::temp_dir().join(format!(
            "budget-bench-root-gate-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&under_tmp);

        // Verify host temp is RAM-backed; otherwise skip with an explicit message
        // so disk-backed CI still documents the contract.
        let probe = BenchRoot::resolve(BenchRootOptions {
            harness: "budget-comparison-probe",
            cli_override: Some(under_tmp.clone()),
            default_root: under_tmp.clone(),
            allow_tmpfs: true,
            min_free_bytes: 0,
        });
        let is_tmpfs = match &probe {
            Ok(root) => root.medium().is_ram_backed(),
            Err(BenchRootError::RamBacked { .. }) => true,
            Err(_) => false,
        };
        if let Ok(root) = probe {
            let _ = std::fs::remove_dir_all(root.path());
        }
        if !is_tmpfs {
            eprintln!(
                "root_env_alone_does_not_allow_tmpfs: skip — host temp_dir is not RAM-backed"
            );
            let _ = std::fs::remove_dir_all(&under_tmp);
            return;
        }

        // ROOT env may be set by wrappers; ALLOW must remain absent.
        assert!(
            std::env::var_os("RIFFDB_BENCH_ALLOW_TMPFS").is_none(),
            "test assumes ALLOW is unset"
        );
        let _ = BENCH_DB_ROOT_ENV; // document the env name under test

        let err = BenchRoot::resolve(BenchRootOptions {
            harness: "budget-comparison-test",
            cli_override: Some(under_tmp.clone()),
            default_root: under_tmp.clone(),
            allow_tmpfs: false,
            min_free_bytes: 0,
        })
        .expect_err("tmpfs path without allow_tmpfs must refuse");
        let text = err.to_string();
        match err {
            BenchRootError::RamBacked { fstype, .. } => {
                assert!(
                    fstype == "tmpfs" || fstype == "ramfs",
                    "unexpected fstype {fstype}"
                );
                assert!(
                    text.contains("RAM-backed") || text.contains("tmpfs") || text.contains("allow"),
                    "refusal message must name the policy: {text}"
                );
            }
            other => panic!("expected RamBacked refusal, got {other}"),
        }
        let _ = std::fs::remove_dir_all(&under_tmp);
    }
}
