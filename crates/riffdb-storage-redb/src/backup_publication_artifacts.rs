//! The original three closed artifacts, with immutable pre-transfer seals.
//! This module is qualified before durable publication/restart activation.
use super::*;
use crate::maintenance::{ClosedPublicationCustody, PinnedDirectory};
use std::sync::Arc;

pub(crate) struct OwnedPublicationArtifacts {
    custody: ClosedPublicationCustody,
    artifacts: [MaterializationArtifact; 3],
    checksums: [BackupIntegrityChecksumV1; 3],
    continuity: [Option<ArtifactContinuity>; 3],
    readback_complete: bool,
    invalid: bool,
}

impl OwnedClosedMaterialization {
    pub(crate) fn into_publication_artifacts(
        self,
    ) -> Result<OwnedPublicationArtifacts, StorageError> {
        self.materialization.verify_full_seal()?;
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
            artifacts,
            checksums: [seals.primary, seals.journal, seals.marker],
            continuity: seals.continuity,
            readback_complete: false,
            invalid: false,
        };
        publication.verify_continuity()?;
        Ok(publication)
    }
}

impl OwnedPublicationArtifacts {
    fn check_continuity(&self) -> Result<(), StorageError> {
        if self.invalid {
            return Err(corrupt());
        }
        self.custody.verify()?;
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

    fn verify_continuity(&mut self) -> Result<(), StorageError> {
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
            // A completed move changes only the known directory entry. Retain
            // the creation FD and check both locations before updating the pin.
            if parent.regular_file_length(&from_name)?.is_some()
                || !target.regular_file_matches(name, file)?
            {
                return Err(corrupt());
            }
            parent.sync()?;
            target.sync()?;
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
