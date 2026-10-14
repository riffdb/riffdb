//! Actual three-artifact publication under a durable original-seal intent.
//! Complete restart and startup fencing remain required before admission.
use super::*;
use crate::maintenance::codec::publication_intent::PublicationPhase;
use std::sync::Arc;

impl RedbSealedStagedRestore {
    pub(in crate::maintenance) fn permits_native_transfer(&self) -> Result<bool, StorageError> {
        self.verify_paths()?;
        if !cfg!(target_os = "linux")
            || self.native_archive_closed.is_some()
            || !self
                .stage_cleanup
                .directory_guard
                .same_filesystem(&self.configured_parent_guard)?
        {
            return Ok(false);
        }
        let Some(NativeStagedOwner::Closed(owner)) = &self.native_owner else {
            return Ok(false);
        };
        if !owner.permits_publication_transfer()? {
            return Ok(false);
        }
        for path in [
            &self.configured_database_file,
            &crate::journal::journal_path(&self.configured_database_file),
            &crate::durable_format_marker_path(&self.configured_database_file),
        ] {
            if self
                .configured_parent_guard
                .regular_file_length(path.file_name().ok_or_else(invariant)?)?
                .is_some()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(in crate::maintenance) fn publish_by_native_transfer(
        mut self,
        storage: &crate::RedbMaintenanceStorage,
    ) -> Result<OfflineRestoreResultV1, StorageError> {
        if !self.permits_native_transfer()? {
            return Err(invariant());
        }
        let Some(NativeStagedOwner::Closed(owner)) = self.native_owner.as_ref() else {
            return Err(invariant());
        };
        // The retained owner's consuming handoff below revalidates ALL original
        // artifacts against its pinned manifest, including sealed history.
        // Avoid a second complete path hash at this same publication boundary.
        // Without that actual owner, keep the complete original fallback check.
        let (manifest, identity) = if owner.retains_original_backup_owner()? {
            crate::backup::read_backup_manifest_binding(&self.backup_directory)?
        } else {
            validate_immutable_backup(&self.backup_directory)?
        };
        if manifest != self.manifest || identity != self.manifest_identity {
            return Err(corrupt());
        }
        self.verify_paths()?;
        let Some(NativeStagedOwner::Closed(owner)) = self.native_owner.take() else {
            return Err(invariant());
        };
        // The consuming handoff performs the full pre-transfer source check.
        let mut artifacts = (*owner).into_publication_artifacts()?;
        let prepared =
            artifacts.publication_intent(self.operation_id, self.manifest_identity.clone())?;
        let journal = crate::journal::journal_path(&self.configured_database_file);
        let marker = crate::durable_format_marker_path(&self.configured_database_file);
        let final_names = [
            self.configured_database_file
                .file_name()
                .ok_or_else(invariant)?
                .to_os_string(),
            journal.file_name().ok_or_else(invariant)?.to_os_string(),
            marker.file_name().ok_or_else(invariant)?.to_os_string(),
        ];
        let temporary_names = final_names
            .each_ref()
            .map(|name| target_temporary_name(name, self.operation_id));
        for name in final_names.iter().chain(&temporary_names) {
            if self
                .configured_parent_guard
                .regular_file_length(name)?
                .is_some()
            {
                return Err(corrupt());
            }
        }
        // A failed/uncertain intent rename may already leave Prepared durable.
        // Preserve the stage before attempting that write, not only after it.
        self.stage_cleanup.disarm();
        storage
            .write_native_publication_intent(None, &prepared)
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
        self.hit(RedbMaintenanceFailpoint::BeforeTargetPublication, true)?;
        let Self {
            configured_parent_guard,
            backup_directory_guard,
            stage_cleanup,
            artifact_identities,
            manifest,
            test_controller,
            ..
        } = self;
        let target = Arc::new(configured_parent_guard);
        artifacts.set_test_controller(test_controller.clone());
        let transfer: Result<crate::maintenance::PublicationIntent, StorageError> = (|| {
            artifacts.move_to_temporaries(
                Arc::clone(&target),
                temporary_names.each_ref().map(|n| n.as_os_str()),
            )?;
            // The above completes full original-seal readback of all three files.
            // No final target artifact has yet been published.
            backup_directory_guard.verify()?;
            stage_cleanup.verify()?;
            stage_cleanup.directory_guard.bounded_entries(0)?;
            artifacts.publish(
                Arc::clone(&target),
                final_names.each_ref().map(|n| n.as_os_str()),
            )?;
            let mut published = prepared.clone();
            published.advance(PublicationPhase::Published)?;
            storage.write_native_publication_intent(Some(&prepared), &published)?;
            if let Some(controller) = &test_controller {
                controller.hit(RedbMaintenanceFailpoint::AfterTargetPublication, true)?;
                controller.hit(RedbMaintenanceFailpoint::AfterTargetParentSync, true)?;
            }
            backup_directory_guard.verify()?;
            stage_cleanup.verify()?;
            stage_cleanup.directory_guard.bounded_entries(0)?;
            target.verify()?;
            for name in &temporary_names {
                if target.regular_file_length(name)?.is_some() {
                    return Err(corrupt());
                }
            }
            Ok(published)
        })();
        let published =
            transfer.map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
        // Retain the original independent outer identity FDs until publication
        // and Published durability have completed. The actual inner native owner
        // may then move into the one operation-local startup handoff (ADR-0299 D9).
        drop(artifact_identities);
        if artifacts
            .has_original_backup_owner()
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?
        {
            let retained = super::super::retained_publication::RetainedPublication::new(
                published,
                artifacts,
                stage_cleanup,
                backup_directory_guard,
                target,
                final_names,
            )
            .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
            storage
                .retain_verified_publication(retained)
                .map_err(|_| storage_error(StorageErrorKind::CommitStatusUnknown))?;
        }
        Ok(OfflineRestoreResultV1::Restored {
            manifest: Box::new(manifest),
        })
    }
}
