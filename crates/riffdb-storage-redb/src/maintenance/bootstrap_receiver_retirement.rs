//! Retirement is authorized by an attached receiver and bound to its actual
//! validated engine owner. Candidate links go first; transfer evidence goes last.
use super::*;
use crate::maintenance::FollowerNamespace;
use riffdb_storage_api::ChangelogHistoryStateV3;

impl RedbBootstrapReceiverRepository {
    /// Recovers the original attachment manifest after validated live-follower
    /// startup. Partial retirement can leave only an empty transfer directory;
    /// that means attachment already succeeded and the normal follower report
    /// is required. This method grants no publication or serving authority.
    pub fn attachment_manifest(&self) -> Result<Option<Manifest>, StorageError> {
        let _guard = self.inner.admission.lock().map_err(|_| unavailable())?;
        self.inner.attachment_manifest()
    }

    pub(crate) fn retire_attached(
        &self,
        history: ChangelogHistoryStateV3,
        namespace: &mut FollowerNamespace,
    ) -> Result<(), StorageError> {
        let _guard = self.inner.admission.lock().map_err(|_| unavailable())?;
        let manifest = self.inner.attachment_manifest()?;
        namespace.verify()?;
        let candidate = self
            .inner
            .directory
            .child_directory(OsStr::new(CANDIDATE))?;
        let transfer = self.inner.directory.child_directory(OsStr::new(TRANSFER))?;
        if let Some(manifest) = manifest {
            let fence = manifest.fence().history();
            if history.lineage() != fence.lineage()
                || history.tail().sequence() < fence.tail().sequence()
                || (history.tail().sequence() == fence.tail().sequence()
                    && history.tail() != fence.tail())
            {
                return Err(corrupt());
            }
        }
        // Preflight BOTH inventories and acquire exclusion before any unlink.
        let transfer = transfer
            .map(|directory| Scratch::prepare(directory, false))
            .transpose()?;
        let candidate = candidate
            .map(|directory| Scratch::prepare(directory, true))
            .transpose()?;
        if let Some(candidate) = &candidate {
            namespace.verify_scratch(&candidate.directory)?;
        }
        if let Some(candidate) = candidate {
            candidate.remove(&self.inner, namespace, true)?;
        }
        if let Some(transfer) = transfer {
            transfer.remove(&self.inner, namespace, false)?;
        }
        // An absent entry can be the result of a previously uncertain unlink.
        self.inner.directory.sync()?;
        namespace.verify()?;
        self.inner.verify()
    }
}

impl ReceiverRepositoryInner {
    fn attachment_manifest(self: &Arc<Self>) -> Result<Option<Manifest>, StorageError> {
        self.verify()?;
        for name in [TRANSFER_CREATING, CANDIDATE_CREATING] {
            if self.directory.child_directory(OsStr::new(name))?.is_some() {
                return Err(corrupt());
            }
        }
        if let Some(directory) = self.directory.child_directory(OsStr::new(TRANSFER))? {
            directory.verify_private()?;
            if directory
                .regular_file_length(OsStr::new(TRANSFER_FILES[0]))?
                .is_some()
            {
                let stage = self.open_stage(None)?;
                let progress = stage.progress();
                if progress.page_count() != progress.manifest().page_count() {
                    return Err(corrupt());
                }
                return Ok(Some(progress.manifest()));
            }
            if !directory.bounded_entries(1)?.is_empty() {
                return Err(corrupt());
            }
        }
        if self
            .directory
            .child_directory(OsStr::new(CANDIDATE))?
            .is_some()
        {
            return Err(corrupt());
        }
        Ok(None)
    }
}

struct Scratch {
    directory: PinnedDirectory,
    files: Vec<(&'static str, File)>,
}
impl Scratch {
    fn prepare(directory: PinnedDirectory, candidate: bool) -> Result<Self, StorageError> {
        directory.verify_private()?;
        // The construction lock is removed last and remains held through rmdir.
        const CANDIDATE_ORDER: &[&str] = &[
            "follower.redb",
            "follower.redb.riffdb-format-v1",
            "construction.lock",
        ];
        let allowed = if candidate {
            CANDIDATE_ORDER
        } else {
            TRANSFER_FILES
        };
        let entries = directory.bounded_entries(allowed.len())?;
        if entries.iter().any(|(name, is_dir)| {
            *is_dir || !allowed.iter().any(|allowed| name == OsStr::new(allowed))
        }) {
            return Err(corrupt());
        }
        let mut files = Vec::with_capacity(entries.len());
        for &name in allowed {
            if entries.iter().any(|(entry, _)| entry == OsStr::new(name)) {
                let file = directory.open_file(OsStr::new(name))?.into_std();
                if !candidate || name == "construction.lock" {
                    file.try_lock().map_err(|_| unavailable())?;
                }
                files.push((name, file));
            }
        }
        if candidate
            && !files.is_empty()
            && directory.regular_file_length(OsStr::new("construction.lock"))? != Some(0)
        {
            return Err(corrupt());
        }
        Ok(Self { directory, files })
    }

    fn remove(
        self,
        root: &ReceiverRepositoryInner,
        namespace: &mut FollowerNamespace,
        candidate: bool,
    ) -> Result<(), StorageError> {
        for (name, file) in &self.files {
            root.verify()?;
            namespace.verify()?;
            self.directory.verify_private()?;
            if candidate {
                namespace.verify_scratch(&self.directory)?;
            }
            if !self
                .directory
                .regular_file_matches(OsStr::new(name), file)?
            {
                return Err(corrupt());
            }
            self.directory.remove_file_if_present(OsStr::new(name))?;
            crash_edge(match *name {
                "follower.redb" => "retiring-candidate-file",
                "follower.redb.riffdb-format-v1" => "retiring-candidate-marker",
                "construction.lock" => "retiring-candidate-lock",
                _ => "retiring-transfer-file",
            });
            // Persist each unlink before removing the lock that proves its
            // predecessor files are gone on a crash retry.
            self.directory.sync()?;
            crash_edge(match *name {
                "follower.redb" => "retired-candidate-file",
                "follower.redb.riffdb-format-v1" => "retired-candidate-marker",
                "construction.lock" => "retired-candidate-lock",
                _ => "retired-transfer-file",
            });
        }
        self.directory.sync()?;
        self.directory.remove_self_empty()?;
        crash_edge(if candidate {
            "retired-candidate-directory"
        } else {
            "retired-transfer-directory"
        });
        root.directory.sync()?;
        namespace.verify()?;
        root.verify()
    }
}
