//! Fixed receiver scratch inventory. Initial construction is published by rename
//! only after its first durable checkpoint; abandoned construction has no ack.
#[path = "bootstrap_receiver_retirement.rs"]
mod retirement;
use super::{
    RedbBootstrapMaterializer, bootstrap_stage::RedbBootstrapStage, path_guard::PinnedDirectory,
};
use riffdb_storage_api::{
    ReplicationBootstrapManifestV1 as Manifest, StorageError, StorageErrorKind,
};
use std::{
    ffi::OsStr,
    fs::File,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

const LOCK: &str = "inventory.lock";
const TRANSFER: &str = "transfer";
const TRANSFER_CREATING: &str = "transfer.creating";
const CANDIDATE: &str = "candidate";
const CANDIDATE_CREATING: &str = "candidate.creating";
const TRANSFER_FILES: &[&str] = &["transfer.redb"];
const CANDIDATE_FILES: &[&str] = &[
    "construction.lock",
    "follower.redb",
    "follower.redb.riffdb-format-v1",
    "follower.redb.riffdb-format-v1.staging",
];

/// One configured receiver's private scratch area, separate from source scratch.
/// All names are fixed; no peer-selected path or generic deletion is exposed.
#[derive(Clone)]
pub struct RedbBootstrapReceiverRepository {
    pub(super) inner: Arc<ReceiverRepositoryInner>,
}
pub(super) struct ReceiverRepositoryInner {
    path: PathBuf,
    directory: PinnedDirectory,
    lock: File,
    admission: Mutex<()>,
}
impl RedbBootstrapReceiverRepository {
    /// Opens the closed inventory and holds its actual root lock. Existing
    /// unknown entries, symlinks, substituted paths and nonprivate roots refuse.
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let parent_path = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .map(Ok)
            .unwrap_or_else(|| std::env::current_dir().map_err(|_| unavailable()))?;
        let parent = PinnedDirectory::open(&parent_path)?;
        let name = path.file_name().ok_or_else(corrupt)?;
        let directory = match parent.child_directory(name)? {
            Some(directory) => directory,
            None => parent.create_private_child(name)?,
        };
        directory.verify_private()?;
        let entries = directory.bounded_entries(5)?;
        check_inventory(&entries)?;
        let lock = match directory.regular_file_length(OsStr::new(LOCK))? {
            Some(0) => directory.open_file_read_write(OsStr::new(LOCK))?.into_std(),
            None if entries.is_empty() => directory.create_new_file(OsStr::new(LOCK))?.into_std(),
            _ => return Err(corrupt()),
        };
        lock.try_lock().map_err(|_| unavailable())?;
        lock.sync_all().map_err(|_| unavailable())?;
        directory.sync()?;
        let value = Self {
            inner: Arc::new(ReceiverRepositoryInner {
                path: path.to_path_buf(),
                directory,
                lock,
                admission: Mutex::new(()),
            }),
        };
        value.inner.verify()?;
        Ok(value)
    }

    /// Recovers acknowledged pages, or reports that a source manifest is needed.
    /// Only the unpublished initial-transfer name may be reclaimed here.
    pub fn recover_transfer(&self) -> Result<Option<RedbBootstrapStage>, StorageError> {
        let _guard = self.inner.admission.lock().map_err(|_| unavailable())?;
        self.inner.verify()?;
        self.inner
            .discard_initial(TRANSFER_CREATING, TRANSFER, TRANSFER_FILES)?;
        if self
            .inner
            .directory
            .child_directory(OsStr::new(TRANSFER))?
            .is_none()
        {
            return Ok(None);
        }
        self.inner.directory.sync()?;
        self.inner.open_stage(None).map(Some)
    }

    /// Creates and publishes initial progress before any page acknowledgement.
    /// An exact retry reopens the original manifest; replacement is refused.
    pub fn begin_transfer(&self, manifest: Manifest) -> Result<RedbBootstrapStage, StorageError> {
        let _guard = self.inner.admission.lock().map_err(|_| unavailable())?;
        self.inner.verify()?;
        self.inner
            .discard_initial(TRANSFER_CREATING, TRANSFER, TRANSFER_FILES)?;
        if self
            .inner
            .directory
            .child_directory(OsStr::new(TRANSFER))?
            .is_some()
        {
            self.inner.directory.sync()?;
            return self.inner.open_stage(Some(manifest));
        }
        if self
            .inner
            .directory
            .child_directory(OsStr::new(CANDIDATE))?
            .is_some()
            || self
                .inner
                .directory
                .child_directory(OsStr::new(CANDIDATE_CREATING))?
                .is_some()
        {
            return Err(corrupt());
        }
        let stage = RedbBootstrapStage::create(&self.inner.path.join(TRANSFER_CREATING), manifest)?;
        drop(stage);
        crash_edge("transfer-initialized");
        self.inner.publish_initial(TRANSFER_CREATING, TRANSFER)?;
        self.inner.open_stage(Some(manifest))
    }

    /// Creates or resumes one candidate from this repository's complete transfer.
    /// Initial candidate creation is also renamed only after its durable progress
    /// exists. No source writer, replacement target or readiness is granted.
    pub fn materialize_transfer(
        &self,
        stage: RedbBootstrapStage,
        cancellation: &AtomicBool,
    ) -> Result<RedbBootstrapMaterializer, StorageError> {
        let _guard = self.inner.admission.lock().map_err(|_| unavailable())?;
        cancelled(cancellation)?;
        self.inner.verify()?;
        let directory = self
            .inner
            .directory
            .child_directory(OsStr::new(TRANSFER))?
            .ok_or_else(corrupt)?;
        if !stage.matches_repository_file(&directory)? {
            return Err(corrupt());
        }
        let manifest = stage.progress().manifest();
        let input = stage.into_materialization_input()?;
        self.inner
            .discard_initial(CANDIDATE_CREATING, CANDIDATE, CANDIDATE_FILES)?;
        let path = self.inner.path.join(CANDIDATE);
        if self
            .inner
            .directory
            .child_directory(OsStr::new(CANDIDATE))?
            .is_some()
        {
            let directory = self
                .inner
                .directory
                .child_directory(OsStr::new(CANDIDATE))?
                .ok_or_else(corrupt)?;
            exact_files(&directory, &CANDIDATE_FILES[..3])?;
            self.inner.directory.sync()?;
            return RedbBootstrapMaterializer::open_cancellable(&path, input, cancellation);
        }
        cancelled(cancellation)?;
        let materializer =
            RedbBootstrapMaterializer::create(&self.inner.path.join(CANDIDATE_CREATING), input)?;
        drop(materializer);
        crash_edge("candidate-initialized");
        cancelled(cancellation)?;
        self.inner.publish_initial(CANDIDATE_CREATING, CANDIDATE)?;
        let input = self
            .inner
            .open_stage(Some(manifest))?
            .into_materialization_input()?;
        RedbBootstrapMaterializer::open_cancellable(&path, input, cancellation)
    }
}
impl ReceiverRepositoryInner {
    pub(super) fn verify(&self) -> Result<(), StorageError> {
        self.directory.verify_private()?;
        if self.directory.regular_file_length(OsStr::new(LOCK))? != Some(0)
            || !self
                .directory
                .regular_file_matches(OsStr::new(LOCK), &self.lock)?
        {
            return Err(corrupt());
        }
        check_inventory(&self.directory.bounded_entries(5)?)
    }
    fn open_stage(
        self: &Arc<Self>,
        expected: Option<Manifest>,
    ) -> Result<RedbBootstrapStage, StorageError> {
        self.verify()?;
        let directory = self
            .directory
            .child_directory(OsStr::new(TRANSFER))?
            .ok_or_else(corrupt)?;
        exact_files(&directory, TRANSFER_FILES)?;
        let path = self.path.join(TRANSFER);
        let stage = match expected {
            Some(manifest) => RedbBootstrapStage::open(&path, manifest)?,
            None => RedbBootstrapStage::recover(&path)?,
        };
        self.verify()?;
        Ok(stage.with_receiver_repository(Arc::clone(self)))
    }
    fn publish_initial(&self, pending: &str, ready: &str) -> Result<(), StorageError> {
        self.verify()?;
        if self.directory.child_directory(OsStr::new(ready))?.is_some() {
            return Err(corrupt());
        }
        self.directory
            .rename(OsStr::new(pending), OsStr::new(ready))?;
        crash_edge(if ready == TRANSFER {
            "transfer-renamed"
        } else {
            "candidate-renamed"
        });
        self.directory.sync()?;
        self.verify()
    }
    fn discard_initial(
        &self,
        pending: &str,
        ready: &str,
        allowed: &[&str],
    ) -> Result<(), StorageError> {
        let Some(directory) = self.directory.child_directory(OsStr::new(pending))? else {
            // Finish an earlier uncertain unlink/rename even when its entry is
            // already absent on this retry.
            self.directory.sync()?;
            return self.verify();
        };
        if self.directory.child_directory(OsStr::new(ready))?.is_some() {
            return Err(corrupt());
        }
        directory.verify_private()?;
        let entries = directory.bounded_entries(allowed.len())?;
        if entries.iter().any(|(name, is_dir)| {
            *is_dir || !allowed.iter().any(|allowed| name == OsStr::new(allowed))
        }) {
            return Err(corrupt());
        }
        let mut files = Vec::with_capacity(entries.len());
        for (name, _) in entries {
            // Read-only descriptors plus redb's actual EX lock: no engine is
            // opened or repaired, and other hardlinks' bytes cannot be changed.
            let file = directory.open_file(&name)?.into_std();
            file.try_lock().map_err(|_| unavailable())?;
            files.push((name, file));
        }
        self.verify()?;
        directory.verify_private()?;
        for (name, file) in &files {
            if !directory.regular_file_matches(name, file)? {
                return Err(corrupt());
            }
        }
        for (name, file) in &files {
            self.verify()?;
            directory.verify_private()?;
            if !directory.regular_file_matches(name, file)? {
                return Err(corrupt());
            }
            directory.remove_file_if_present(name)?;
            crash_edge("initial-file-removed");
        }
        directory.sync()?;
        directory.remove_self_empty()?;
        crash_edge("initial-directory-removed");
        self.directory.sync()?;
        self.verify()
    }
}
fn exact_files(directory: &PinnedDirectory, names: &[&str]) -> Result<(), StorageError> {
    directory.verify_private()?;
    let entries = directory.bounded_entries(names.len())?;
    if entries.len() != names.len()
        || entries.iter().any(|(name, is_dir)| {
            *is_dir || !names.iter().any(|allowed| name == OsStr::new(allowed))
        })
    {
        return Err(corrupt());
    }
    Ok(())
}
fn check_inventory(entries: &[(std::ffi::OsString, bool)]) -> Result<(), StorageError> {
    if entries.iter().any(|(name, directory)| {
        if *directory {
            ![TRANSFER, TRANSFER_CREATING, CANDIDATE, CANDIDATE_CREATING]
                .iter()
                .any(|allowed| name == OsStr::new(allowed))
        } else {
            name != OsStr::new(LOCK)
        }
    }) {
        return Err(corrupt());
    }
    for (pending, ready) in [
        (TRANSFER_CREATING, TRANSFER),
        (CANDIDATE_CREATING, CANDIDATE),
    ] {
        if entries.iter().any(|(name, _)| name == OsStr::new(pending))
            && entries.iter().any(|(name, _)| name == OsStr::new(ready))
        {
            return Err(corrupt());
        }
    }
    Ok(())
}
fn cancelled(flag: &AtomicBool) -> Result<(), StorageError> {
    if flag.load(Ordering::Acquire) {
        Err(unavailable())
    } else {
        Ok(())
    }
}
fn corrupt() -> StorageError {
    StorageError::new(StorageErrorKind::CorruptData, None)
}
fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}
fn crash_edge(edge: &str) {
    #[cfg(test)]
    if std::env::var("RIFFDB_RECEIVER_REPOSITORY_EDGE").as_deref() == Ok(edge) {
        std::process::exit(95);
    }
    #[cfg(not(test))]
    let _ = edge;
}
