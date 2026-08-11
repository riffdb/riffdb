//! Whole-directory scratch lifecycle guard for test temp files.
//!
//! Test code must never join names onto `std::env::temp_dir()` and clean up
//! by hand: files leak on panic, on failed assertions, and on interrupted
//! runs, and lists of "files to remove" rot as the code under test grows new
//! side files. [`ScratchDir`] scopes every artifact of one test to a single
//! directory that is removed on `Drop` — pass, fail, or panic. Because
//! `SIGKILL`/Ctrl-C skips `Drop`, every directory name embeds the owning pid
//! and each construction first sweeps same-prefix directories whose pid is
//! dead (or whose mtime is older than 24 hours, guarding against pid reuse).
//! The sweep mirrors `riffdb-bench-root`'s stale-dir sweep, the house
//! precedent for crash-orphan hygiene.
//!
//! Two consequences of that design are deliberate and worth knowing:
//!
//! - **The 24-hour belt outranks liveness.** A directory whose mtime is more
//!   than 24 hours old is swept even if its owning pid is still alive; never
//!   sweeping live pids would reinstate the pid-reuse leak the belt closes.
//!   A harness that holds one scratch directory idle for over a day must
//!   touch it or disarm it with [`ScratchDir::keep`] (which also moves it
//!   out of sweep scope entirely).
//! - **Path length.** A scratch path costs `TMPDIR` plus roughly 34 bytes of
//!   scope name before the caller's own file names. Unix local-socket paths
//!   are capped at 100 bytes (`riffdb-config`'s
//!   `MAX_LOCAL_SOCKET_PATH_BYTES`), so socket tests should use a
//!   deliberately short-prefix `tempfile::TempDir` instead — see the
//!   `riffdb-sock-` scope in riffdb-server's daemon tests.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

/// Leading path component of every scratch directory this module creates.
const SCRATCH_COMPONENT: &str = "riffdb-scratch";

/// Age beyond which a same-prefix directory is swept even if its embedded pid
/// looks alive (pid-reuse belt, mirrored from `riffdb-bench-root`).
const MAX_STALE_AGE: Duration = Duration::from_secs(24 * 60 * 60);

static NEXT_SCRATCH: AtomicU64 = AtomicU64::new(0);

/// One per-test scratch directory: `<root>/riffdb-scratch-<prefix>-<pid>-<n>`.
///
/// Removed recursively on `Drop` unless [`ScratchDir::keep`] was called.
/// Creating one first sweeps sibling directories with the same prefix whose
/// owning process is dead, so artifacts orphaned by a killed run are bounded
/// to one interrupted session instead of accumulating forever.
#[derive(Debug)]
pub struct ScratchDir {
    path: PathBuf,
    keep: bool,
}

impl ScratchDir {
    /// Creates a scratch directory under the platform temporary directory
    /// (`TMPDIR` when set), sweeping dead same-prefix siblings first.
    ///
    /// `prefix` must be non-empty ASCII alphanumerics/`.`/`_`/`-` so swept
    /// names stay unambiguous.
    pub fn new(prefix: &str) -> io::Result<Self> {
        Self::new_in(std::env::temp_dir(), prefix)
    }

    /// Creates a scratch directory under `root` (e.g. `CARGO_TARGET_TMPDIR`),
    /// sweeping dead same-prefix siblings first.
    pub fn new_in(root: impl Into<PathBuf>, prefix: &str) -> io::Result<Self> {
        let root = root.into();
        validate_prefix(prefix)?;
        // Best-effort: a sweep failure must never fail the test being set up.
        let _swept = sweep_stale(&root, prefix);
        loop {
            let ordinal = NEXT_SCRATCH.fetch_add(1, Ordering::Relaxed);
            let path = root.join(format!(
                "{SCRATCH_COMPONENT}-{prefix}-{}-{ordinal}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path, keep: false }),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    fs::create_dir_all(&root)?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Absolute path of the scratch directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Convenience join onto the scratch directory.
    #[must_use]
    pub fn join(&self, name: impl AsRef<Path>) -> PathBuf {
        self.path.join(name)
    }

    /// Disarms cleanup so the directory outlives the guard (for
    /// keep-the-evidence debug flows such as `RIFFDB_KEEP_*` variables).
    ///
    /// The directory is also renamed with a `-kept` suffix, which the
    /// sweep's strict `<pid>-<ordinal>` tail parse rejects — without the
    /// rename, the next run of the same suite would see a dead pid and
    /// silently destroy the retained evidence. [`ScratchDir::path`] reflects
    /// the new name, so read it *after* calling `keep` when logging where
    /// the evidence lives.
    pub fn keep(&mut self) {
        self.keep = true;
        let mut kept = self.path.as_os_str().to_os_string();
        kept.push("-kept");
        let kept = PathBuf::from(kept);
        match fs::rename(&self.path, &kept) {
            Ok(()) => self.path = kept,
            Err(error) => eprintln!(
                "riffdb-testkit: failed to move kept scratch dir {} out of sweep scope: {error}",
                self.path.display()
            ),
        }
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        if self.keep {
            return;
        }
        let _ = fs::remove_dir_all(&self.path);
    }
}

/// Removes `<root>/riffdb-scratch-<prefix>-<pid>-<n>` directories whose pid
/// is dead or whose mtime is older than 24 hours (the module docs explain
/// why staleness outranks liveness). Returns how many were removed; racing
/// sweeps of one root are safe but the count may over-report, so treat it
/// as best-effort. Only exact same-prefix scratch names are ever touched —
/// in particular, directories renamed by [`ScratchDir::keep`] fail the tail
/// parse and always survive.
pub fn sweep_stale(root: &Path, prefix: &str) -> io::Result<usize> {
    validate_prefix(prefix)?;
    let family = format!("{SCRATCH_COMPONENT}-{prefix}-");
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    let now = SystemTime::now();
    let mut removed = 0_usize;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(pid) = name
            .strip_prefix(family.as_str())
            .and_then(extract_scratch_pid)
        else {
            continue;
        };
        if pid == std::process::id() {
            continue;
        }
        let pid_dead = !pid_is_alive(pid);
        let too_old = entry
            .metadata()
            .ok()
            .and_then(|meta| meta.modified().ok())
            .and_then(|mtime| now.duration_since(mtime).ok())
            .is_some_and(|age| age > MAX_STALE_AGE);
        if pid_dead || too_old {
            match fs::remove_dir_all(&path) {
                Ok(()) => {
                    eprintln!(
                        "riffdb-testkit: swept stale scratch dir {} \
                         (pid_dead={pid_dead} too_old={too_old})",
                        path.display()
                    );
                    removed = removed.saturating_add(1);
                }
                Err(error) => {
                    eprintln!(
                        "riffdb-testkit: failed to sweep {}: {error}",
                        path.display()
                    );
                }
            }
        }
    }
    Ok(removed)
}

fn validate_prefix(prefix: &str) -> io::Result<()> {
    let valid = prefix
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphanumeric())
        && prefix
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if valid {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid scratch prefix {prefix:?}"),
        ))
    }
}

/// Strict `<pid>-<ordinal>` tail parse; anything else is not ours to sweep.
fn extract_scratch_pid(tail: &str) -> Option<u32> {
    let (pid, ordinal) = tail.split_once('-')?;
    if ordinal.is_empty() || !ordinal.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if pid.is_empty() || !pid.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    pid.parse().ok()
}

fn pid_is_alive(pid: u32) -> bool {
    Path::new("/proc").join(pid.to_string()).exists()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Mutex;

    /// The guard's reason to exist: cleanup must happen on the panic path.
    /// With `Drop` neutered this test observes the leak and fails.
    #[test]
    fn panicking_test_body_still_removes_the_directory() {
        let observed = Mutex::new(Option::<PathBuf>::None);
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let scratch = ScratchDir::new("panic-proof").expect("create scratch");
            fs::write(scratch.join("evidence.bin"), b"payload").expect("write evidence");
            *observed.lock().expect("record path") = Some(scratch.path().to_path_buf());
            panic!("simulated test failure inside the guard scope");
        }));
        assert!(outcome.is_err(), "the guarded body must have panicked");
        let path = observed
            .into_inner()
            .expect("path mutex")
            .expect("path recorded before the panic");
        assert!(
            !path.exists(),
            "scratch directory {} must be removed on the panic path",
            path.display()
        );
    }

    #[test]
    fn side_files_grown_later_are_covered_by_directory_scoping() {
        let scratch = ScratchDir::new("side-files").expect("create scratch");
        let path = scratch.path().to_path_buf();
        // A whole-directory guard needs no file list: arbitrary new artifacts
        // are inside the scope by construction.
        fs::write(scratch.join("db.redb"), b"db").expect("db");
        fs::write(scratch.join("db.redb.riffjournal"), b"journal").expect("journal");
        fs::create_dir(scratch.join("nested")).expect("nested");
        fs::write(scratch.join("nested/marker"), b"marker").expect("marker");
        drop(scratch);
        assert!(
            !path.exists(),
            "directory scope must remove every side file"
        );
    }

    #[test]
    fn sweep_removes_dead_pid_dirs_and_keeps_live_and_foreign_ones() {
        let root = ScratchDir::new("sweep-root").expect("create sweep root");
        // Mirror riffdb-bench-root: a very high never-started pid reads as dead.
        let dead = root.join(format!("{SCRATCH_COMPONENT}-swp-999999-1"));
        let live = root.join(format!("{SCRATCH_COMPONENT}-swp-{}-2", std::process::id()));
        let foreign = root.join("keep-me");
        let malformed = root.join(format!("{SCRATCH_COMPONENT}-swp-notapid-3"));
        let other_prefix = root.join(format!("{SCRATCH_COMPONENT}-other-999999-4"));
        for dir in [&dead, &live, &foreign, &malformed, &other_prefix] {
            fs::create_dir(dir).expect("seed sweep candidate");
        }
        // The live dir belongs to "another" process for the sweep's purposes:
        // rewrite it under a pid that is alive but not ours (init, pid 1).
        let live_other = root.join(format!("{SCRATCH_COMPONENT}-swp-1-5"));
        fs::create_dir(&live_other).expect("seed live foreign candidate");

        let removed = sweep_stale(root.path(), "swp").expect("sweep");
        assert_eq!(removed, 1, "exactly the dead-pid dir is swept");
        assert!(!dead.exists(), "dead-pid dir must be swept");
        assert!(live.exists(), "own-pid dir must survive");
        assert!(live_other.exists(), "live foreign-pid dir must survive");
        assert!(foreign.exists(), "non-scratch names are never touched");
        assert!(malformed.exists(), "unparseable names are never touched");
        assert!(
            other_prefix.exists(),
            "other prefixes belong to other suites and must survive"
        );
    }

    /// The maintainer's actual complaint is leaks on *kill* — a path no
    /// `Drop` can cover. This pins the one mechanism that covers it: merely
    /// constructing a scratch dir must reap a dead session's leftovers.
    /// With the construction-time sweep call deleted, this test fails.
    #[test]
    fn constructing_a_scratch_dir_reaps_a_dead_sessions_orphan() {
        let root = ScratchDir::new("kill-pin-root").expect("create pin root");
        let orphan = root.join(format!("{SCRATCH_COMPONENT}-kill-pin-999999-1"));
        fs::create_dir(&orphan).expect("seed dead-pid orphan");
        fs::write(orphan.join("leaked.redb"), b"leak").expect("seed leaked database");

        let fresh = ScratchDir::new_in(root.path(), "kill-pin").expect("construct scratch");
        assert!(
            !orphan.exists(),
            "constructing a scratch dir must sweep the dead session's orphan"
        );
        assert!(fresh.path().exists(), "the fresh scope itself must exist");
    }

    #[test]
    fn keep_disarms_cleanup_for_evidence_flows() {
        let mut scratch = ScratchDir::new("keep").expect("create scratch");
        let before = scratch.path().to_path_buf();
        scratch.keep();
        let path = scratch.path().to_path_buf();
        assert_ne!(before, path, "keep must rename the directory");
        drop(scratch);
        assert!(path.exists(), "kept directory must survive its guard");
        fs::remove_dir_all(&path).expect("manual cleanup of kept directory");
    }

    /// Retained evidence must survive the *next* run's sweep: a kept
    /// directory's embedded pid goes dead when the retaining session exits,
    /// and without the `-kept` rename the sweep would silently destroy it.
    /// With the rename removed from `keep()`, this test fails.
    #[test]
    fn kept_directories_survive_a_subsequent_sweep_even_with_a_dead_pid() {
        let root = ScratchDir::new("keep-sweep-root").expect("create root");
        // Model a kept dir from an exited session: dead pid, renamed tail.
        let kept = root.join(format!("{SCRATCH_COMPONENT}-ks-999999-1-kept"));
        fs::create_dir(&kept).expect("seed kept dir");
        fs::write(kept.join("evidence.bin"), b"evidence").expect("seed evidence");
        // Control: the same dead pid without the rename must be reaped.
        let unkept = root.join(format!("{SCRATCH_COMPONENT}-ks-999999-2"));
        fs::create_dir(&unkept).expect("seed unkept control");

        let removed = sweep_stale(root.path(), "ks").expect("sweep");
        assert_eq!(removed, 1, "exactly the unkept control is swept");
        assert!(!unkept.exists(), "unkept dead-pid dir must be reaped");
        assert!(kept.exists(), "kept dir must survive the sweep");
        assert!(
            kept.join("evidence.bin").exists(),
            "retained evidence must be intact after the sweep"
        );

        // End to end: keep() itself must produce a name outside sweep scope.
        // Our own pid shields the dir while this process lives, so assert
        // the durable property directly: the kept name must fail the very
        // tail parse the sweep uses, or the next run would reap it once this
        // pid dies.
        let mut live = ScratchDir::new_in(root.path(), "ks").expect("create live scratch");
        live.keep();
        let live_path = live.path().to_path_buf();
        drop(live);
        assert!(live_path.exists(), "kept scratch must survive its guard");
        let name = live_path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("kept scratch name");
        assert_eq!(
            name.strip_prefix(&format!("{SCRATCH_COMPONENT}-ks-"))
                .and_then(extract_scratch_pid),
            None,
            "keep() must rename the directory so the sweep's tail parse \
             rejects it after this pid dies"
        );
    }

    #[test]
    fn hostile_prefixes_are_rejected_before_any_filesystem_work() {
        for prefix in ["", "a/b", "..", "a b", "a\u{7}b"] {
            assert!(
                ScratchDir::new(prefix).is_err(),
                "prefix {prefix:?} must be rejected"
            );
            assert!(
                sweep_stale(Path::new("/nonexistent"), prefix).is_err(),
                "sweep prefix {prefix:?} must be rejected"
            );
        }
    }
}
