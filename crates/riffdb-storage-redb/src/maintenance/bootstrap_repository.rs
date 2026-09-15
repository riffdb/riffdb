//! Fixed-capacity persistent source scratch. Private artifacts are removable only
//! under file exclusion and after proving no source fence depends on them, or
//! after the exact bootstrap-to-follower hold handoff has completed durably.
use super::{bootstrap_source::*, path_guard::PinnedDirectory};
use crate::RedbOperationalPorts;
use riffdb_storage_api::{
    ChangelogCursorErrorV3 as Refusal, ChangelogHistoryPointV3,
    ReplicationBootstrapManifestV1 as Manifest, ReplicationSourceHoldIdV1 as HoldId, StorageError,
    StorageErrorKind,
};
use std::{
    ffi::OsStr,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

const MAX_ARTIFACTS: usize = 4;
const LOCK: &str = "inventory.lock";
const TRANSFER: &str = "transfer.redb";

/// Cloneable, narrow source factory; no application or generic maintenance writer.
/// The root engine-independent lock is retained by every in-flight artifact.
#[derive(Clone)]
pub struct RedbBootstrapRepository {
    pub(super) inner: Arc<RepositoryInner>,
}
pub(super) struct RepositoryInner {
    ports: RedbOperationalPorts,
    path: PathBuf,
    directory: PinnedDirectory,
    lock: File,
    admission: Mutex<()>,
}
impl RedbOperationalPorts {
    /// Opens the source's dedicated private artifact area. The configured path
    /// is composition-owned; peers supply only fixed-size IDs. One repository
    /// admits at most four on-disk artifacts, including abandoned ones.
    pub fn bootstrap_repository(
        &self,
        path: &Path,
    ) -> Result<RedbBootstrapRepository, StorageError> {
        let parent_path = match path.parent().filter(|p| !p.as_os_str().is_empty()) {
            Some(parent) => parent.to_path_buf(),
            None => std::env::current_dir().map_err(|_| unavailable())?,
        };
        let parent = PinnedDirectory::open(&parent_path)?;
        let name = path.file_name().ok_or_else(corrupt)?;
        let directory = match parent.child_directory(name)? {
            Some(directory) => directory,
            None => parent.create_private_child(name)?,
        };
        directory.verify_private()?;
        let lock = match directory.regular_file_length(OsStr::new(LOCK))? {
            None => directory.create_new_file(OsStr::new(LOCK))?.into_std(),
            Some(0) => directory.open_file_read_write(OsStr::new(LOCK))?.into_std(),
            Some(_) => return Err(corrupt()),
        };
        lock.try_lock().map_err(|_| unavailable())?;
        lock.sync_all().map_err(|_| unavailable())?;
        directory.sync()?;
        let inner = Arc::new(RepositoryInner {
            ports: RedbOperationalPorts {
                shared: Arc::clone(&self.shared),
            },
            path: path.to_path_buf(),
            directory,
            lock,
            admission: Mutex::new(()),
        });
        inner.inventory()?;
        Ok(RedbBootstrapRepository { inner })
    }
}
impl RepositoryInner {
    pub(super) fn verify(&self) -> Result<(), StorageError> {
        self.directory.verify_private()?;
        if self.directory.regular_file_length(OsStr::new(LOCK))? != Some(0)
            || !self
                .directory
                .regular_file_matches(OsStr::new(LOCK), &self.lock)?
        {
            return Err(corrupt());
        }
        Ok(())
    }
    fn inventory(&self) -> Result<Vec<HoldId>, StorageError> {
        self.verify()?;
        let mut ids = Vec::new();
        for (name, directory) in self.directory.bounded_entries(MAX_ARTIFACTS + 1)? {
            if name == OsStr::new(LOCK) && !directory {
                continue;
            }
            if !directory || ids.len() == MAX_ARTIFACTS {
                return Err(corrupt());
            }
            ids.push(decode_name(&name)?);
        }
        ids.sort_by_key(|id| *id.as_bytes());
        Ok(ids)
    }
    fn path(&self, id: HoldId) -> PathBuf {
        self.path.join(encode_name(id))
    }
    fn artifact(&self, id: HoldId) -> Result<Option<ArtifactLock>, StorageError> {
        self.verify()?;
        self.directory
            .child_directory(OsStr::new(&encode_name(id)))?
            .map(ArtifactLock::open)
            .transpose()
    }
    fn remove(&self, artifact: ArtifactLock) -> Result<(), StorageError> {
        self.verify()?;
        artifact.verify()?;
        crash_edge("repository-before-delete");
        if artifact.file.is_some() {
            artifact
                .directory
                .remove_file_if_present(OsStr::new(TRANSFER))?;
        }
        artifact.directory.sync()?;
        crash_edge("repository-file-deleted");
        self.verify()?;
        artifact.directory.remove_self_empty()?;
        crash_edge("repository-directory-deleted");
        self.directory.sync()?;
        crash_edge("repository-parent-synced");
        self.verify()?;
        Ok(())
    }
    fn reap_unheld(&self, id: HoldId) -> Result<bool, StorageError> {
        let Some(artifact) = self.artifact(id)? else {
            return Ok(true);
        };
        // Actual EX file lock is already held. A source builder cannot register
        // this artifact's hold while cleanup inspects the published hold table.
        if self.ports.bootstrap_id_is_held(id)? {
            return Ok(false);
        }
        self.remove(artifact)?;
        Ok(true)
    }
}
impl RedbBootstrapRepository {
    /// Advances an attached follower's durable fence without recreating scratch
    /// or registering an absent hold. Caller owns current policy and remote ack.
    pub fn acknowledge_follower(
        &self,
        id: HoldId,
        lineage: riffdb_storage_api::ChangelogLineageV3,
        acknowledged: ChangelogHistoryPointV3,
    ) -> Result<(), Refusal> {
        self.inner.verify()?;
        self.inner
            .ports
            .acknowledge_replication_follower_v3(id, lineage, acknowledged)?;
        self.inner.verify()?;
        Ok(())
    }

    /// Starts a new artifact or resumes the same already-held ID after a lost
    /// initial response. Unheld abandoned files are reclaimed under exclusion.
    pub fn begin(&self, id: HoldId) -> Result<RedbBootstrapSourceBuild, Refusal> {
        let _guard = self.inner.admission.lock().map_err(|_| unavailable())?;
        let mut ids = self.inner.inventory()?;
        if ids.contains(&id) {
            if !self.inner.reap_unheld(id)? {
                return self
                    .inner
                    .ports
                    .begin_replication_bootstrap_resume_v3(&self.inner.path(id), id)
                    .map(|build| build.with_repository(Arc::clone(&self.inner)));
            }
            ids.retain(|existing| *existing != id);
        }
        if self.inner.ports.bootstrap_id_is_held(id)? {
            return Err(Refusal::InvalidPosition);
        }
        if ids.len() == MAX_ARTIFACTS {
            for existing in ids {
                match self.inner.reap_unheld(existing) {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(error) if error.kind() == StorageErrorKind::Unavailable => {}
                    Err(error) => return Err(error.into()),
                }
            }
            if self.inner.inventory()?.len() == MAX_ARTIFACTS {
                return Err(unavailable().into());
            }
        }
        let build = self
            .inner
            .ports
            .begin_replication_bootstrap_v3(&self.inner.path(id), id)?;
        self.inner.verify()?;
        Ok(build.with_repository(Arc::clone(&self.inner)))
    }
    /// Reopens an exact ID for bounded full artifact verification. The caller
    /// must compare the completed manifest with the peer's original manifest.
    pub fn resume(&self, id: HoldId) -> Result<RedbBootstrapSourceBuild, Refusal> {
        let _guard = self.inner.admission.lock().map_err(|_| unavailable())?;
        if !self.inner.inventory()?.contains(&id) {
            return Err(Refusal::InvalidPosition);
        }
        self.inner
            .ports
            .begin_replication_bootstrap_resume_v3(&self.inner.path(id), id)
            .map(|build| build.with_repository(Arc::clone(&self.inner)))
    }
    /// Performs the exact durable source hold handoff, then removes only this
    /// artifact. An uncertain handoff preserves files; cleanup retries never
    /// release the follower fence or allocate another source receipt.
    pub fn attach(
        &self,
        manifest: Manifest,
        acknowledged: ChangelogHistoryPointV3,
    ) -> Result<(), Refusal> {
        let _guard = self.inner.admission.lock().map_err(|_| unavailable())?;
        self.inner.inventory()?;
        let id = manifest.fence().hold_id();
        let artifact = self.inner.artifact(id)?;
        if let Some(artifact) = &artifact {
            artifact.verify()?;
        }
        self.inner
            .ports
            .attach_replication_bootstrap_v3(manifest, acknowledged)?;
        crash_edge("repository-attached");
        if let Some(artifact) = artifact {
            self.inner.remove(artifact)?;
        }
        // An earlier cleanup may have removed the entry but failed its parent
        // sync. A successful exact retry must make that absence durable too.
        self.inner.directory.sync()?;
        self.inner.verify()?;
        Ok(())
    }
}

// EX uses the same standard file lock as redb's native backend. Cleanup never
// opens an engine for writing and cannot change another hardlink's file bytes.
struct ArtifactLock {
    directory: PinnedDirectory,
    file: Option<File>,
}
impl ArtifactLock {
    fn open(directory: PinnedDirectory) -> Result<Self, StorageError> {
        directory.verify_private()?;
        let entries = directory.bounded_entries(1)?;
        if entries
            .iter()
            .any(|(name, is_dir)| *is_dir || name != OsStr::new(TRANSFER))
        {
            return Err(corrupt());
        }
        let file = if entries.is_empty() {
            None
        } else {
            let file = directory
                .open_file_read_write(OsStr::new(TRANSFER))?
                .into_std();
            file.try_lock().map_err(|_| unavailable())?;
            Some(file)
        };
        let value = Self { directory, file };
        value.verify()?;
        Ok(value)
    }
    fn verify(&self) -> Result<(), StorageError> {
        self.directory.verify_private()?;
        if let Some(file) = &self.file {
            if !self
                .directory
                .regular_file_matches(OsStr::new(TRANSFER), file)?
            {
                return Err(corrupt());
            }
        } else if !self.directory.bounded_entries(0)?.is_empty() {
            return Err(corrupt());
        }
        Ok(())
    }
}
fn encode_name(id: HoldId) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    id.as_bytes()
        .iter()
        .flat_map(|byte| {
            [
                HEX[(byte >> 4) as usize] as char,
                HEX[(byte & 15) as usize] as char,
            ]
        })
        .collect()
}
fn decode_name(name: &OsStr) -> Result<HoldId, StorageError> {
    let name = name.to_str().ok_or_else(corrupt)?;
    if name.len() != 32
        || !name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(corrupt());
    }
    let mut id = [0; 16];
    for (out, pair) in id.iter_mut().zip(name.as_bytes().chunks_exact(2)) {
        let digit = |b: u8| if b <= b'9' { b - b'0' } else { b - b'a' + 10 };
        *out = digit(pair[0]) * 16 + digit(pair[1]);
    }
    HoldId::new(id).ok_or_else(corrupt)
}
fn corrupt() -> StorageError {
    StorageError::new(StorageErrorKind::CorruptData, None)
}
fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}
fn crash_edge(edge: &str) {
    #[cfg(test)]
    if std::env::var("RIFFDB_BOOTSTRAP_REPOSITORY_EDGE").as_deref() == Ok(edge) {
        std::process::exit(94);
    }
    #[cfg(not(test))]
    let _ = edge;
}

#[cfg(test)]
#[path = "bootstrap_repository_tests.rs"]
mod tests;
