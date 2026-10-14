//! The original three closed artifacts, with immutable pre-transfer seals.
//! Durable publication/restart retains these original seals and file identities.
use super::*;
use crate::maintenance::{ClosedPublicationCustody, PinnedDirectory};
use std::sync::Arc;

pub(crate) struct OwnedPublicationArtifacts {
    custody: ClosedPublicationCustody,
    #[cfg(unix)]
    backup_history: Option<Arc<super::owned_history::OwnedBackupHistory>>,
    artifacts: [MaterializationArtifact; 3],
    checksums: [BackupIntegrityChecksumV1; 3],
    continuity: [Option<ArtifactContinuity>; 3],
    readback_complete: bool,
    invalid: bool,
    test_controller: Option<crate::RedbMaintenanceTestController>,
    publishing: bool,
    pub(crate) history: riffdb_storage_api::ChangelogHistoryStateV3,
    pub(crate) retention_watermark: u64,
    pub(crate) incarnation: u64,
}

impl OwnedClosedMaterialization {
    pub(crate) fn retains_original_backup_owner(&self) -> Result<bool, StorageError> {
        self.verify()?;
        #[cfg(unix)]
        {
            Ok(self.materialization.backup_history.is_some())
        }
        #[cfg(not(unix))]
        {
            Ok(false)
        }
    }

    pub(crate) fn permits_publication_transfer(&self) -> Result<bool, StorageError> {
        self.verify()?;
        Ok(self.ordinary_source
            && self.publication_facts.is_some()
            && self.materialization.database.is_none()
            && self.materialization.deferred_extents.is_none()
            && self
                .materialization
                .validated_seal
                .as_ref()
                .is_some_and(|seal| seal.sealed.is_empty()))
    }

    pub(crate) fn into_publication_artifacts(
        self,
    ) -> Result<OwnedPublicationArtifacts, StorageError> {
        self.materialization.verify_full_seal()?;
        #[cfg(unix)]
        if let Some(backup) = &self.materialization.backup_history {
            backup.verify_full()?;
        }
        let facts = self.publication_facts.ok_or_else(corrupt)?;
        let owner = self.materialization;
        if !self.ordinary_source
            || owner.database.is_some()
            || owner.deferred_extents.is_some()
            || owner
                .validated_seal
                .as_ref()
                .is_none_or(|seal| !seal.sealed.is_empty())
        {
            owner.custody.invalidate();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let seals = owner.validated_seal.ok_or_else(corrupt)?;
        let mut artifacts = [owner.primary, owner.journal, owner.marker];
        // No destructor may erase a transferred artifact on an uncertain path.
        // Directory cleanup belongs to the later durable-intent protocol.
        for artifact in &mut artifacts {
            artifact.staged.mark_published();
        }
        let mut publication = OwnedPublicationArtifacts {
            custody: owner.custody.into_closed_publication()?,
            #[cfg(unix)]
            backup_history: owner.backup_history,
            artifacts,
            checksums: [seals.primary, seals.journal, seals.marker],
            continuity: seals.continuity,
            readback_complete: false,
            invalid: false,
            test_controller: None,
            publishing: false,
            history: facts.0,
            retention_watermark: facts.1,
            incarnation: self.history_incarnation,
        };
        publication.verify_continuity()?;
        Ok(publication)
    }
}

impl OwnedPublicationArtifacts {
    pub(crate) fn has_original_backup_owner(&mut self) -> Result<bool, StorageError> {
        self.verify_continuity()?;
        #[cfg(unix)]
        {
            Ok(self.backup_history.is_some())
        }
        #[cfg(not(unix))]
        {
            Ok(false)
        }
    }

    pub(crate) fn verify_published_owner(
        &mut self,
        intent: &crate::maintenance::PublicationIntent,
        target: &PinnedDirectory,
        names: &[OsString; 3],
    ) -> Result<(), StorageError> {
        let result = (|| {
            self.verify_continuity()?;
            if !self.publishing
                || !self.readback_complete
                || !self.has_original_backup_owner()?
                || self.history != intent.history
                || self.retention_watermark != intent.retention_watermark
                || self.incarnation != intent.incarnation
            {
                return Err(corrupt());
            }
            for (index, name) in names.iter().enumerate() {
                let artifact = &self.artifacts[index];
                let file = artifact.private_file()?;
                let original = &intent.artifacts[index];
                if original.kind != crate::maintenance::ArtifactKind::ORDERED[index]
                    || original.checksum != self.checksums[index]
                    || original.length != file.metadata().map_err(io_unavailable)?.len()
                    || artifact.path() != target.child_path(name)?
                    || !target.regular_file_matches(name, file)?
                {
                    return Err(corrupt());
                }
            }
            self.verify_continuity()
        })();
        if result.is_err() {
            self.invalid = true;
        }
        result
    }

    pub(crate) fn set_test_controller(
        &mut self,
        controller: Option<crate::RedbMaintenanceTestController>,
    ) {
        self.test_controller = controller;
    }

    fn hit(&self, point: crate::RedbMaintenanceFailpoint) -> Result<(), StorageError> {
        self.test_controller
            .as_ref()
            .map_or(Ok(()), |controller| controller.hit(point, true))
    }

    pub(crate) fn publication_intent(
        &mut self,
        operation: riffdb_types::OfflineMaintenanceOperationId,
        manifest: OfflineBackupManifestIdentityV1,
    ) -> Result<crate::maintenance::PublicationIntent, StorageError> {
        use crate::maintenance::{ArtifactKind, ArtifactSeal, PublicationIntent};
        self.verify_continuity()?;
        let seal = |index: usize, kind| -> Result<ArtifactSeal, StorageError> {
            Ok(ArtifactSeal {
                kind,
                length: self.artifacts[index]
                    .private_file()?
                    .metadata()
                    .map_err(io_unavailable)?
                    .len(),
                checksum: self.checksums[index].clone(),
            })
        };
        PublicationIntent::prepared(
            operation,
            manifest,
            self.incarnation,
            self.history,
            self.retention_watermark,
            [
                seal(0, ArtifactKind::Primary)?,
                seal(1, ArtifactKind::Journal)?,
                seal(2, ArtifactKind::FormatMarker)?,
            ],
        )
    }

    fn check_continuity(&self) -> Result<(), StorageError> {
        if self.invalid {
            return Err(corrupt());
        }
        self.custody.verify()?;
        #[cfg(unix)]
        if let Some(backup) = &self.backup_history {
            backup.verify()?;
        }
        for (artifact, expected) in self.artifacts.iter().zip(self.continuity) {
            artifact.verify()?;
            let file = artifact.private_file()?;
            let metadata = file.metadata().map_err(io_unavailable)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.nlink() != 1 {
                    return Err(corrupt());
                }
            }
            if expected.is_some() && ArtifactContinuity::read(file)? != expected {
                return Err(corrupt());
            }
        }
        Ok(())
    }

    pub(crate) fn verify_continuity(&mut self) -> Result<(), StorageError> {
        let result = self.check_continuity();
        if result.is_err() {
            self.invalid = true;
        }
        result
    }

    /// Full reads always compare against the original sealed checksums. A
    /// metadata change during our own rename never produces a replacement seal.
    fn verify_full(&mut self) -> Result<(), StorageError> {
        let result = (|| {
            self.verify_continuity()?;
            for (artifact, expected) in self.artifacts.iter().zip(&self.checksums) {
                if artifact.checksum()? != *expected {
                    return Err(corrupt());
                }
            }
            self.verify_continuity()
        })();
        if result.is_err() {
            self.invalid = true;
        }
        result
    }

    fn move_one(
        &mut self,
        index: usize,
        target: Arc<PinnedDirectory>,
        name: &OsStr,
    ) -> Result<(), StorageError> {
        let result = (|| {
            self.verify_continuity()?;
            let points = crate::maintenance::native_move_failpoints(
                crate::maintenance::ArtifactKind::ORDERED[index],
                self.publishing,
            );
            let controller = self.test_controller.clone();
            let hit = |point| {
                controller
                    .as_ref()
                    .map_or(Ok(()), |controller| controller.hit(point, true))
            };
            let artifact = &mut self.artifacts[index];
            let from_name = artifact
                .path()
                .file_name()
                .ok_or_else(corrupt)?
                .to_os_string();
            let before = ArtifactContinuity::read(artifact.private_file()?)?;
            let (parent, file) = artifact.identity.as_mut().ok_or_else(corrupt)?;
            if !parent.same_filesystem(&target)? {
                return Err(corrupt());
            }
            if index == 0 {
                self.custody.move_to(Arc::clone(&target), name)?;
            } else {
                parent.rename_to(&from_name, &target, name)?;
            }
            hit(points[0])?;
            // A completed move changes only the known directory entry. Retain
            // the creation FD and check both locations before updating the pin.
            if parent.regular_file_length(&from_name)?.is_some()
                || !target.regular_file_matches(name, file)?
            {
                return Err(corrupt());
            }
            parent.sync()?;
            hit(points[1])?;
            target.sync()?;
            hit(points[2])?;
            *parent = target;
            artifact.staged.path = parent.child_path(name)?;
            let after = ArtifactContinuity::read(file)?;
            if let (Some(before), Some(after)) = (before, after)
                && (before.device, before.inode, before.length, before.modified)
                    != (after.device, after.inode, after.length, after.modified)
            {
                return Err(corrupt());
            }
            self.continuity[index] = after;
            self.verify_continuity()
        })();
        if result.is_err() {
            self.invalid = true;
        }
        result
    }

    pub(crate) fn move_to_temporaries(
        &mut self,
        target: Arc<PinnedDirectory>,
        names: [&OsStr; 3],
    ) -> Result<(), StorageError> {
        self.readback_complete = false;
        self.publishing = false;
        self.verify_continuity()?;
        for (index, name) in names.into_iter().enumerate() {
            self.move_one(index, Arc::clone(&target), name)?;
        }
        // Complete all three independent readbacks before *any* final rename.
        self.verify_full()?;
        for artifact in &self.artifacts {
            artifact
                .private_file()?
                .sync_all()
                .map_err(io_unavailable)?;
        }
        target.sync()?;
        self.readback_complete = true;
        self.hit(crate::RedbMaintenanceFailpoint::NativeTransferReadback)?;
        Ok(())
    }

    pub(crate) fn publish(
        &mut self,
        target: Arc<PinnedDirectory>,
        names: [&OsStr; 3],
    ) -> Result<(), StorageError> {
        if !self.readback_complete {
            return Err(corrupt());
        }
        // Check continuity since readback, then publish the marker last.
        self.publishing = true;
        self.verify_continuity()?;
        for (index, name) in names.into_iter().enumerate() {
            self.move_one(index, Arc::clone(&target), name)?;
        }
        target.sync()?;
        self.verify_continuity()
    }
}

#[cfg(all(test, unix))]
#[path = "backup_publication_artifacts_tests.rs"]
mod tests;

pub(super) fn capture_publication_facts(
    database: &Database,
    manifest: &OfflineBackupManifestV1,
    incarnation: u64,
) -> Result<(riffdb_storage_api::ChangelogHistoryStateV3, u64), StorageError> {
    let read = database.begin_read().map_err(transaction_error)?;
    let history = crate::changelog_v3_roots::read_checkpoint_roots(&read)?.ok_or_else(corrupt)?;
    let watermark =
        crate::retention::load_watermark(&read)?.map_or(0, |value| value.watermark_sequence());
    if history.lineage().database_id() != manifest.database_id()
        || history.lineage().history_incarnation() != incarnation
        || history.tail().frontier().application() != manifest.last_commit_sequence()
        || watermark
            > manifest
                .last_commit_sequence()
                .map_or(0, |value| value.get())
    {
        return Err(corrupt());
    }
    Ok((history, watermark))
}
