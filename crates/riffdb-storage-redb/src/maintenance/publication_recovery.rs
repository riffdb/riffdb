//! Cold ownership of the actual intent-bound artifacts, never a validation proof.
//! Test-only until startup consumption and ordinary admission are connected.
use super::native_publication::IntentInventory;
use super::*;
use crate::backup::ArtifactContinuity;
use crate::maintenance::codec::publication_intent::{PublicationIntent, PublicationPhase};
use crate::native_file_custody::NativeFileCustody;
use std::io::{Seek, SeekFrom};
use std::sync::Arc;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Position {
    Stage,
    Temporary,
    Published,
}

struct RecoveredArtifact {
    file: File,
    parent: Arc<PinnedDirectory>,
    name: OsString,
    continuity: Option<ArtifactContinuity>,
    position: Position,
}

/// Reconstructed file custody may resume recorded moves, but never authorizes
/// an engine open, a complete-validation claim or application admission.
pub(in crate::maintenance) struct RecoveredPublicationArtifacts {
    intent: PublicationIntent,
    artifacts: [RecoveredArtifact; 3],
    target: Arc<PinnedDirectory>,
    final_names: [OsString; 3],
    stage: Option<Arc<PinnedDirectory>>,
    backup: PinnedDirectory,
    _native: NativeFileCustody,
    invalid: bool,
}

impl RedbMaintenanceStorage {
    pub(in crate::maintenance) fn prepare_native_publication_recovery(
        &self,
    ) -> Result<Option<RecoveredPublicationArtifacts>, StorageError> {
        self.verify_path_ownership()?;
        // Validate all ordinary versions and intent facts without deleting any
        // temporary. Actual artifact ownership must also pass before cleanup.
        let (receipts, _, _, _, _) = self.read_complete_inventory_mode(true, false)?;
        let inventory = IntentInventory::read(&self.receipts_directory_guard)?;
        let Some(intent) = inventory.current else {
            return Ok(None);
        };
        let receipt = receipts
            .receipts()
            .iter()
            .find(|r| r.operation_id() == intent.operation_id)
            .ok_or_else(corrupt)?;
        let backup_path = self.named_backup_directory(receipt.backup_name());
        let backup = PinnedDirectory::open(&backup_path)?;
        let (_, identity) = validate_immutable_backup(&backup_path)?;
        if identity != intent.manifest {
            return Err(corrupt());
        }
        backup.verify()?;
        let target = Arc::new(self.database_parent_guard.retained_clone()?);
        let stage = self
            .staged_directory_guard
            .child_directory(OsStr::new(&intent.operation_id.to_string()))?
            .map(Arc::new);
        let finals = artifact_names(&self.database_file)?;
        let temporaries = finals
            .each_ref()
            .map(|n| super::super::staged::target_temporary_name(n, intent.operation_id));
        let staged = artifact_names(Path::new(crate::backup::DATABASE_ARTIFACT_FILE_NAME))?;
        if let Some(stage) = &stage {
            if !stage.same_filesystem(&target)? {
                return Err(corrupt());
            }
            for (name, directory) in stage.bounded_entries(3)? {
                if directory || !staged.contains(&name) {
                    return Err(corrupt());
                }
            }
        } else if intent.phase == PublicationPhase::Prepared {
            return Err(corrupt());
        }
        let mut recovered = Vec::with_capacity(3);
        let mut native = None;
        for index in 0..3 {
            let mut position = None;
            let mut consider =
                |parent: &Arc<PinnedDirectory>, name: &OsStr, at| -> Result<(), StorageError> {
                    if parent.regular_file_length(name)?.is_some() {
                        if position.is_some() {
                            return Err(corrupt());
                        }
                        position = Some((Arc::clone(parent), name.to_os_string(), at));
                    }
                    Ok(())
                };
            if let Some(stage) = &stage {
                consider(stage, &staged[index], Position::Stage)?;
            }
            consider(&target, &temporaries[index], Position::Temporary)?;
            consider(&target, &finals[index], Position::Published)?;
            let (parent, name, position) = position.ok_or_else(corrupt)?;
            if intent.phase != PublicationPhase::Prepared && position != Position::Published {
                return Err(corrupt());
            }
            let file = parent.open_file_read_write(&name)?.into_std();
            check_single_link(&file)?;
            if index == 0 {
                // Take native and legacy exclusions before reading any seal.
                native = Some(NativeFileCustody::acquire(
                    file.try_clone().map_err(io_unavailable)?,
                )?);
            }
            recovered.push(RecoveredArtifact {
                continuity: ArtifactContinuity::read(&file)?,
                file,
                parent,
                name,
                position,
            });
        }
        let artifacts: [RecoveredArtifact; 3] = recovered.try_into().map_err(|_| invariant())?;
        if artifacts[2].position == Position::Published
            && artifacts.iter().any(|a| a.position != Position::Published)
        {
            return Err(corrupt());
        }
        let mut recovered = RecoveredPublicationArtifacts {
            intent,
            artifacts,
            target,
            final_names: finals,
            stage,
            backup,
            _native: native.ok_or_else(invariant)?,
            invalid: false,
        };
        recovered.verify_continuity()?;
        if recovered.intent.phase != PublicationPhase::StartupConsumed {
            recovered.verify_full()?;
        }
        self.verify_path_ownership()?;
        Ok(Some(recovered))
    }
}

fn artifact_names(primary: &Path) -> Result<[OsString; 3], StorageError> {
    let journal = crate::journal::journal_path(primary);
    let marker = crate::durable_format_marker_path(primary);
    Ok([
        primary.file_name().ok_or_else(corrupt)?.to_os_string(),
        journal.file_name().ok_or_else(corrupt)?.to_os_string(),
        marker.file_name().ok_or_else(corrupt)?.to_os_string(),
    ])
}

fn check_single_link(file: &File) -> Result<(), StorageError> {
    let metadata = file.metadata().map_err(io_unavailable)?;
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(corrupt());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(corrupt());
        }
    }
    Ok(())
}

impl RecoveredPublicationArtifacts {
    fn check_continuity(&self) -> Result<(), StorageError> {
        if self.invalid {
            return Err(corrupt());
        }
        self.target.verify()?;
        self.backup.verify()?;
        if let Some(stage) = &self.stage {
            stage.verify()?;
        }
        let staged = artifact_names(Path::new(crate::backup::DATABASE_ARTIFACT_FILE_NAME))?;
        if let Some(stage) = &self.stage {
            for (name, directory) in stage.bounded_entries(3)? {
                if directory
                    || !self.artifacts.iter().enumerate().any(|(index, artifact)| {
                        artifact.position == Position::Stage && name == staged[index]
                    })
                {
                    return Err(corrupt());
                }
            }
        }
        for (index, artifact) in self.artifacts.iter().enumerate() {
            if let Some(stage) = &self.stage
                && stage.regular_file_length(&staged[index])?.is_some()
                    != (artifact.position == Position::Stage)
            {
                return Err(corrupt());
            }
            let temporary = super::super::staged::target_temporary_name(
                &self.final_names[index],
                self.intent.operation_id,
            );
            if self.target.regular_file_length(&temporary)?.is_some()
                != (artifact.position == Position::Temporary)
                || self
                    .target
                    .regular_file_length(&self.final_names[index])?
                    .is_some()
                    != (artifact.position == Position::Published)
            {
                return Err(corrupt());
            }
            artifact.parent.verify()?;
            check_single_link(&artifact.file)?;
            if !artifact
                .parent
                .regular_file_matches(&artifact.name, &artifact.file)?
                || ArtifactContinuity::read(&artifact.file)? != artifact.continuity
            {
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

    fn verify_full(&mut self) -> Result<(), StorageError> {
        let result = (|| {
            self.verify_continuity()?;
            for (artifact, original) in self.artifacts.iter().zip(&self.intent.artifacts) {
                let mut reader = artifact.file.try_clone().map_err(io_unavailable)?;
                reader.seek(SeekFrom::Start(0)).map_err(io_unavailable)?;
                if reader.metadata().map_err(io_unavailable)?.len() != original.length
                    || crate::backup::sha256_reader(reader)? != original.checksum
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

    fn move_one(
        &mut self,
        index: usize,
        name: &OsStr,
        position: Position,
        controller: Option<&RedbMaintenanceTestController>,
    ) -> Result<(), StorageError> {
        let result = (|| {
            self.verify_continuity()?;
            let points = crate::maintenance::native_move_failpoints(
                crate::maintenance::ArtifactKind::ORDERED[index],
                position == Position::Published,
            );
            let hit = |point| controller.map_or(Ok(()), |controller| controller.hit(point, true));
            let artifact = &mut self.artifacts[index];
            let before = artifact.continuity;
            artifact
                .parent
                .rename_to(&artifact.name, &self.target, name)?;
            hit(points[0])?;
            if artifact
                .parent
                .regular_file_length(&artifact.name)?
                .is_some()
                || !self.target.regular_file_matches(name, &artifact.file)?
            {
                return Err(corrupt());
            }
            artifact.parent.sync()?;
            hit(points[1])?;
            self.target.sync()?;
            hit(points[2])?;
            let after = ArtifactContinuity::read(&artifact.file)?;
            if let (Some(before), Some(after)) = (before, after)
                && !before.matches_after_known_rename(after)
            {
                return Err(corrupt());
            }
            artifact.parent = Arc::clone(&self.target);
            artifact.name = name.to_os_string();
            artifact.position = position;
            artifact.continuity = after;
            self.verify_continuity()
        })();
        if result.is_err() {
            self.invalid = true;
        }
        result
    }

    pub(in crate::maintenance) fn resume_publication(
        mut self,
        storage: &RedbMaintenanceStorage,
    ) -> Result<PublicationIntent, StorageError> {
        if self.intent.phase == PublicationPhase::StartupConsumed {
            return Err(invariant());
        }
        self.verify_continuity()?;
        // Ownership and seals already passed before the first deletion. Retain
        // native exclusion and all descriptors through cleanup and continuation.
        storage.read_complete_inventory(true)?;
        if IntentInventory::read(&storage.receipts_directory_guard)?
            .current
            .as_ref()
            != Some(&self.intent)
        {
            return Err(corrupt());
        }
        let result: Result<PublicationIntent, StorageError> = (|| {
            let finals = artifact_names(&storage.database_file)?;
            let temporaries = finals
                .each_ref()
                .map(|n| super::super::staged::target_temporary_name(n, self.intent.operation_id));
            for (index, name) in temporaries.iter().enumerate() {
                if self.artifacts[index].position == Position::Stage {
                    self.move_one(
                        index,
                        name,
                        Position::Temporary,
                        storage.test_controller.as_ref(),
                    )?;
                }
            }
            // A cold Prepared state may already have primary/journal final.
            // Read all original seals again before publishing any remaining one.
            if self.intent.phase == PublicationPhase::Prepared {
                self.verify_full()?;
                for artifact in &self.artifacts {
                    artifact.file.sync_all().map_err(io_unavailable)?;
                }
                self.target.sync()?;
                storage.hit(RedbMaintenanceFailpoint::NativeTransferReadback, true)?;
                for (index, name) in finals.iter().enumerate() {
                    if self.artifacts[index].position != Position::Published {
                        self.move_one(
                            index,
                            name,
                            Position::Published,
                            storage.test_controller.as_ref(),
                        )?;
                    }
                }
                self.target.sync()?;
                self.verify_continuity()?;
                let mut published = self.intent.clone();
                published.advance(PublicationPhase::Published)?;
                storage.write_native_publication_intent(Some(&self.intent), &published)?;
                self.intent = published;
            }
            self.verify_continuity()?;
            Ok(self.intent.clone())
        })();
        // Even a rename followed by failed verification/sync may be durable.
        // Preserve all files and the intent; callers poll the same operation.
        result.map_err(|_| unknown())
    }
}

#[cfg(test)]
#[path = "publication_recovery_tests.rs"]
mod tests;
