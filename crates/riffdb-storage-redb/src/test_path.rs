#![forbid(unsafe_code)]

//! Repository-local scratch placement and lifecycle for this crate's unit
//! tests.
//!
//! Storage tests never allocate under the ambient temp directory (enforced by
//! `tests/architecture.rs`); everything lives below
//! `target/riffdb-test-data/storage-redb-unit`. [`ScopedDirectory`] scopes one
//! test's database and every side file it grows (journal, checkpoint, spare,
//! durable-format marker, …) to a single directory removed on `Drop` — pass,
//! fail, or panic — so cleanup never depends on a hand-maintained file list.
//! Directory names embed the owning pid; creation sweeps sibling scopes whose
//! pid is dead, bounding what interrupted runs can accumulate.
//!
//! This intentionally mirrors `riffdb_testkit::scratch::ScratchDir`; the
//! testkit itself depends on this crate, so the storage layer keeps a local
//! copy instead of importing it back (dependency cycle).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

/// Leading component of every unit-test scope directory.
const SCOPE_PREFIX: &str = "scope-";

/// Age beyond which a scope is swept even if its embedded pid looks alive
/// (pid-reuse belt, mirrored from `riffdb-bench-root`).
const MAX_STALE_AGE: Duration = Duration::from_secs(24 * 60 * 60);

static NEXT_SCOPE: AtomicU64 = AtomicU64::new(1);

pub(crate) fn root() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/riffdb-test-data/storage-redb-unit");
    fs::create_dir_all(&root).expect("create repository-local redb test root");
    fs::canonicalize(root).expect("canonicalize repository-local redb test root")
}

/// One unit test's scratch scope: `<root>/scope-<label>-<pid>-<n>`, removed
/// recursively on `Drop`.
#[derive(Debug)]
pub(crate) struct ScopedDirectory(PathBuf);

impl ScopedDirectory {
    /// Creates a fresh scope after sweeping dead-pid siblings.
    pub(crate) fn new(label: &str) -> Self {
        let root = root();
        sweep_stale_scopes(&root);
        let ordinal = NEXT_SCOPE.fetch_add(1, Ordering::Relaxed);
        let path = root.join(format!("{SCOPE_PREFIX}{label}-{}-{ordinal}", std::process::id()));
        fs::create_dir(&path).expect("create unit test scope directory");
        Self(path)
    }

    /// Joins a file name onto the scope.
    pub(crate) fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for ScopedDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Removes `scope-…-<pid>-<n>` siblings whose pid is dead or whose mtime is
/// older than 24 hours. Best-effort: failures only lose hygiene, never tests.
fn sweep_stale_scopes(root: &Path) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    let now = SystemTime::now();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Some(pid) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix(SCOPE_PREFIX))
            .and_then(extract_scope_pid)
        else {
            continue;
        };
        if pid == std::process::id() {
            continue;
        }
        let pid_dead = !Path::new("/proc").join(pid.to_string()).exists();
        let too_old = entry
            .metadata()
            .ok()
            .and_then(|meta| meta.modified().ok())
            .and_then(|mtime| now.duration_since(mtime).ok())
            .is_some_and(|age| age > MAX_STALE_AGE);
        if pid_dead || too_old {
            match fs::remove_dir_all(&path) {
                Ok(()) => eprintln!(
                    "riffdb-storage-redb: swept stale unit scope {} \
                     (pid_dead={pid_dead} too_old={too_old})",
                    path.display()
                ),
                Err(error) => eprintln!(
                    "riffdb-storage-redb: failed to sweep {}: {error}",
                    path.display()
                ),
            }
        }
    }
}

/// Strict `…-<pid>-<ordinal>` tail parse; anything else is not ours to sweep.
fn extract_scope_pid(tail: &str) -> Option<u32> {
    let mut parts = tail.rsplit('-');
    let ordinal = parts.next()?;
    let pid = parts.next()?;
    if ordinal.is_empty() || !ordinal.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    pid.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Mutex;

    /// Falsifiability for the guard itself: cleanup must happen on the panic
    /// path. With `Drop` neutered this observes the leak and fails.
    #[test]
    fn panicking_test_body_still_removes_the_scope() {
        let observed = Mutex::new(Option::<PathBuf>::None);
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let scope = ScopedDirectory::new("panic-proof");
            fs::write(scope.join("db.redb"), b"payload").expect("write evidence");
            fs::write(scope.join("db.redb.riffjournal"), b"side file").expect("side file");
            *observed.lock().expect("record path") = Some(scope.0.clone());
            panic!("simulated unit test failure inside the scope");
        }));
        assert!(outcome.is_err(), "the scoped body must have panicked");
        let path = observed
            .into_inner()
            .expect("path mutex")
            .expect("path recorded before the panic");
        assert!(
            !path.exists(),
            "unit scope {} must be removed on the panic path",
            path.display()
        );
    }

    #[test]
    fn sweep_removes_dead_pid_scopes_and_keeps_live_and_foreign_entries() {
        // Host the fixture inside a scope so this test cleans after itself;
        // the sweep under test runs against that inner root.
        let host = ScopedDirectory::new("sweep-host");
        let inner_root = host.join("inner-root");
        fs::create_dir(&inner_root).expect("inner root");
        // Mirror riffdb-bench-root: a very high never-started pid reads dead.
        let dead = inner_root.join("scope-dead-999999-1");
        let live = inner_root.join(format!("scope-live-{}-2", std::process::id()));
        let init = inner_root.join("scope-init-1-3");
        let foreign = inner_root.join("keep-me");
        let malformed = inner_root.join("scope-malformed-notapid-4");
        for dir in [&dead, &live, &init, &foreign, &malformed] {
            fs::create_dir(dir).expect("seed sweep candidate");
        }
        sweep_stale_scopes(&inner_root);
        assert!(!dead.exists(), "dead-pid scope must be swept");
        assert!(live.exists(), "own-pid scope must survive");
        assert!(init.exists(), "live foreign-pid scope must survive");
        assert!(foreign.exists(), "non-scope names are never touched");
        assert!(malformed.exists(), "unparseable names are never touched");
    }
}
