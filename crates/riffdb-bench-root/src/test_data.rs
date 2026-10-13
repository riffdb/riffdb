//! Placement for ordinary test scratch.
//!
//! Benchmarks need a real disk ([`crate::BenchRoot`] refuses tmpfs); ordinary
//! tests do not. Their databases, journals and crash-harness copies are
//! correctness fixtures, and on a disk a parallel suite's fsync-heavy writes
//! saturate the device for no assertion's benefit. [`test_data_root`] places
//! test scratch below [`TEST_DATA_ROOT_ENV`] when it is set and non-empty, and
//! below the caller's on-disk root otherwise. The variable is resolved once per
//! process so one run never mixes roots.
//!
//! `cargo nextest` sets the variable to a RAM-backed root through its
//! `test-scratch` setup script (`scripts/test-scratch-env`), because nextest
//! runs each test in its own process. A plain multi-threaded `cargo test`
//! process keeps the disk default: several maintenance tests there reopen a
//! database or archive while a concurrent test in the same process spawns a
//! crash child, and on tmpfs that interleaving is fast enough to turn rare
//! lock collisions into routine failures.
//!
//! Process-kill crash tests stay meaningful on tmpfs: a killed process loses
//! nothing the page cache holds, which is exactly what tmpfs keeps. Tests that
//! must observe a real device belong in benchmarks, which use
//! [`crate::BenchRoot`].

use std::path::PathBuf;
use std::sync::OnceLock;

/// Environment override for the test scratch root.
pub const TEST_DATA_ROOT_ENV: &str = "RIFFDB_TEST_DATA_ROOT";

/// Where this process's test scratch is placed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestDataPlacement {
    /// [`TEST_DATA_ROOT_ENV`] named the root.
    Override(PathBuf),
    /// No override; the caller's on-disk fallback applies.
    Fallback,
}

static PLACEMENT: OnceLock<TestDataPlacement> = OnceLock::new();

/// The placement this process uses, resolved on first call.
pub fn test_data_placement() -> &'static TestDataPlacement {
    PLACEMENT
        .get_or_init(|| resolve_placement(std::env::var_os(TEST_DATA_ROOT_ENV).map(PathBuf::from)))
}

/// The test scratch root: the override with `component` joined below it, or
/// `fallback` used as is. The directory is not created.
///
/// `component` keeps each suite's scratch apart under a shared override root
/// (for example `storage-redb-unit`); `fallback` is the caller's full on-disk
/// path.
#[must_use]
pub fn test_data_root(component: &str, fallback: impl Into<PathBuf>) -> PathBuf {
    match test_data_placement() {
        TestDataPlacement::Override(root) => root.join(component),
        TestDataPlacement::Fallback => fallback.into(),
    }
}

fn resolve_placement(env_override: Option<PathBuf>) -> TestDataPlacement {
    match env_override.filter(|root| !root.as_os_str().is_empty()) {
        Some(root) => TestDataPlacement::Override(root),
        None => TestDataPlacement::Fallback,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn override_wins_and_empty_or_absent_override_falls_back() {
        assert_eq!(
            resolve_placement(Some(PathBuf::from("/data/riffdb-tests"))),
            TestDataPlacement::Override(PathBuf::from("/data/riffdb-tests"))
        );
        assert_eq!(
            resolve_placement(Some(PathBuf::new())),
            TestDataPlacement::Fallback
        );
        assert_eq!(resolve_placement(None), TestDataPlacement::Fallback);
    }

    #[test]
    fn component_is_joined_below_the_override_and_fallback_is_used_as_is() {
        let fallback = PathBuf::from("/workspace/target/riffdb-test-data/storage-redb-unit");
        let root = test_data_root("storage-redb-unit", fallback.clone());
        match test_data_placement() {
            TestDataPlacement::Override(base) => {
                assert_eq!(root, base.join("storage-redb-unit"));
            }
            TestDataPlacement::Fallback => assert_eq!(root, fallback),
        }
    }
}
