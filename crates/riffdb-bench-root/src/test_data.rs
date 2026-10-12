//! RAM-backed placement for ordinary test scratch.
//!
//! Benchmarks need a real disk ([`crate::BenchRoot`] refuses tmpfs); ordinary
//! tests do not. Their databases, journals and crash-harness copies are
//! correctness fixtures, and on a disk the suite's fsync-heavy parallel writes
//! saturate the device for no assertion's benefit. [`test_data_root`] therefore
//! places test scratch on RAM-backed `/dev/shm` when it exists and has room,
//! and falls back to the caller's on-disk root otherwise.
//!
//! Precedence, resolved once per process so one run never mixes media:
//!
//! 1. [`TEST_DATA_ROOT_ENV`], when set and non-empty, used as given (set it to
//!    an on-disk path to force disk).
//! 2. [`RAM_TEST_DATA_ROOT`], when it is tmpfs/ramfs with at least
//!    [`RAM_TEST_DATA_MIN_FREE_BYTES`] free.
//! 3. The caller's fallback, normally a directory below the workspace
//!    `target/`.
//!
//! Process-kill crash tests stay meaningful on tmpfs: a killed process loses
//! nothing the page cache holds, which is exactly what tmpfs keeps. Tests that
//! must observe a real device belong in benchmarks, which use
//! [`crate::BenchRoot`].

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Environment override for the test scratch root.
pub const TEST_DATA_ROOT_ENV: &str = "RIFFDB_TEST_DATA_ROOT";

/// Default RAM-backed test scratch root on Linux.
pub const RAM_TEST_DATA_ROOT: &str = "/dev/shm/riffdb-tests";

/// Free space the RAM root must have before a process places scratch there.
pub const RAM_TEST_DATA_MIN_FREE_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Where this process's test scratch was placed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TestDataPlacement {
    /// [`TEST_DATA_ROOT_ENV`] named the root.
    Override(PathBuf),
    /// The RAM-backed default.
    Ram(PathBuf),
    /// No usable RAM root; the caller's on-disk fallback applies.
    Fallback,
}

static PLACEMENT: OnceLock<TestDataPlacement> = OnceLock::new();

/// The placement this process uses, resolved on first call.
pub fn test_data_placement() -> &'static TestDataPlacement {
    PLACEMENT.get_or_init(|| {
        resolve_placement(
            std::env::var_os(TEST_DATA_ROOT_ENV).map(PathBuf::from),
            Path::new(RAM_TEST_DATA_ROOT),
            RAM_TEST_DATA_MIN_FREE_BYTES,
        )
    })
}

/// The test scratch root: the override, the RAM root, or `fallback`, with
/// `component` joined below it. The directory is not created.
///
/// `component` keeps each suite's scratch apart under a shared RAM root
/// (for example `storage-redb-unit`); `fallback` is the caller's full on-disk
/// path, used as is.
#[must_use]
pub fn test_data_root(component: &str, fallback: impl Into<PathBuf>) -> PathBuf {
    match test_data_placement() {
        TestDataPlacement::Override(root) | TestDataPlacement::Ram(root) => root.join(component),
        TestDataPlacement::Fallback => fallback.into(),
    }
}

fn resolve_placement(
    env_override: Option<PathBuf>,
    ram_root: &Path,
    min_free_bytes: u64,
) -> TestDataPlacement {
    if let Some(root) = env_override.filter(|root| !root.as_os_str().is_empty()) {
        return TestDataPlacement::Override(root);
    }
    if ram_root_usable(ram_root, min_free_bytes) {
        return TestDataPlacement::Ram(ram_root.to_path_buf());
    }
    TestDataPlacement::Fallback
}

fn ram_root_usable(ram_root: &Path, min_free_bytes: u64) -> bool {
    // The mount (for example `/dev/shm`) must exist; the root below it is
    // created on demand so a fresh boot needs no setup.
    let Some(mount) = ram_root.parent().filter(|parent| parent.is_dir()) else {
        return false;
    };
    if !matches!(
        crate::classify_medium(mount),
        Ok(crate::StorageMedium::RamBacked { .. })
    ) {
        return false;
    }
    if std::fs::create_dir_all(ram_root).is_err() {
        return false;
    }
    crate::free_bytes_for(ram_root).is_ok_and(|free| free >= min_free_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn override_wins_and_empty_override_is_ignored() {
        let ram = Path::new("/nonexistent-riffdb-ram/riffdb-tests");
        assert_eq!(
            resolve_placement(Some(PathBuf::from("/data/riffdb-tests")), ram, 0),
            TestDataPlacement::Override(PathBuf::from("/data/riffdb-tests"))
        );
        assert_eq!(
            resolve_placement(Some(PathBuf::new()), ram, 0),
            TestDataPlacement::Fallback
        );
    }

    #[test]
    fn a_missing_or_disk_backed_ram_root_falls_back() {
        assert_eq!(
            resolve_placement(None, Path::new("/nonexistent-riffdb-ram/riffdb-tests"), 0),
            TestDataPlacement::Fallback
        );
        // The crate directory is on an ordinary filesystem in CI and locally.
        let disk = Path::new(env!("CARGO_MANIFEST_DIR")).join("riffdb-tests-never-created");
        if !matches!(
            crate::classify_medium(Path::new(env!("CARGO_MANIFEST_DIR"))),
            Ok(crate::StorageMedium::RamBacked { .. })
        ) {
            assert_eq!(
                resolve_placement(None, &disk, 0),
                TestDataPlacement::Fallback
            );
            assert!(!disk.exists());
        }
    }

    #[test]
    fn a_ram_root_without_enough_free_space_falls_back() {
        let shm = Path::new("/dev/shm");
        if !matches!(
            crate::classify_medium(shm),
            Ok(crate::StorageMedium::RamBacked { .. })
        ) {
            return;
        }
        let root = shm.join(format!("riffdb-bench-root-test-{}", std::process::id()));
        assert_eq!(
            resolve_placement(None, &root, 0),
            TestDataPlacement::Ram(root.clone())
        );
        assert_eq!(
            resolve_placement(None, &root, u64::MAX),
            TestDataPlacement::Fallback
        );
        let _ = std::fs::remove_dir(&root);
    }

    #[test]
    fn component_is_joined_below_a_shared_root_and_fallback_is_used_as_is() {
        let fallback = PathBuf::from("/workspace/target/riffdb-test-data/storage-redb-unit");
        let root = test_data_root("storage-redb-unit", fallback.clone());
        match test_data_placement() {
            TestDataPlacement::Override(base) | TestDataPlacement::Ram(base) => {
                assert_eq!(root, base.join("storage-redb-unit"));
            }
            TestDataPlacement::Fallback => assert_eq!(root, fallback),
        }
    }
}
