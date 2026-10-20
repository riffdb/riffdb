//! Owning-only post-maintenance handoff. No Source/cut/drain proof is minted.
use super::*;
use crate::engine_owner::source_reopen::{
    OpenedPredecessorSourceReadV1, PredecessorSourceConstructionV1,
};
use riffdb_types::{OwnedCapacityInspectionErrorV1, OwnedCapacityInspectionV1};

/// Every non-engine field of the original actual predecessor acquisition.
/// Construction is PRIVATE to the owning artifacts module's consuming method.
/// The original backup object is joined with this fixed owner before Native
/// retirement/open; no arbitrary engine/descriptor pair is a public input.
pub(crate) struct RetainedPredecessorOuterV1 {
    source_database: PathBuf,
    backup_directory: PathBuf,
    manifest: riffdb_storage_api::OfflineBackupManifestV1,
    primary: crate::native_file_custody::NativeFileCustody,
    cancellation: Arc<AtomicBool>,
    observation: FixedMaintenanceObservationV1,
}

impl MaintenanceValidatedPredecessorV1 {
    pub(crate) fn into_source_read_phase(
        self,
        construction: PredecessorSourceConstructionV1,
    ) -> Result<OpenedPredecessorSourceReadV1, StorageError> {
        // The sole construction permit comes directly from the consumed sealed
        // validated owner. Joined complete validation already retired TLS/cache.
        let Self {
            artifacts,
            observation,
        } = self;
        artifacts.into_source_read_phase(observation, construction)
    }
}

impl RetainedPredecessorOuterV1 {
    pub(crate) fn original_history(&self) -> riffdb_storage_api::ChangelogHistoryStateV3 {
        self.observation.history
    }

    // Only fields extracted from the SAME actual OfflinePredecessorArtifactsV1
    // can be supplied by its private parent/child owning implementation.
    pub(super) fn from_artifacts(
        source_database: PathBuf,
        backup_directory: PathBuf,
        manifest: riffdb_storage_api::OfflineBackupManifestV1,
        primary: crate::native_file_custody::NativeFileCustody,
        cancellation: Arc<AtomicBool>,
        observation: FixedMaintenanceObservationV1,
    ) -> Self {
        Self {
            source_database,
            backup_directory,
            manifest,
            primary,
            cancellation,
            observation,
        }
    }
    pub(crate) fn verify_backup_binding(
        &self,
        backup: &crate::backup::OwnedBackupHistory,
        marker: riffdb_storage_api::DurableFormatMarker,
    ) -> Result<(), StorageError> {
        self.check_cancellation()?;
        self.primary.verify_path_binding(&self.source_database)?;
        backup.verify()?;
        crate::backup::validate_catalog_cut_source_artifacts(
            &self.source_database,
            &self.backup_directory,
            &self.manifest,
        )?;
        // Original marker is checked against the fully validated observation,
        // never replaced by the new binary registry or a durable-ID adoption.
        if marker.registry_digest() != self.observation.original_registry {
            return Err(corrupt());
        }
        self.primary.verify_path_binding(&self.source_database)?;
        self.check_cancellation()
    }
    pub(crate) fn check_cancellation(&self) -> Result<(), StorageError> {
        if self.cancellation.load(std::sync::atomic::Ordering::Acquire) {
            Err(storage_error(StorageErrorKind::Unavailable))
        } else {
            Ok(())
        }
    }
    pub(crate) fn native_quote_capture_bytes(&self) -> Result<u64, StorageError> {
        crate::native_file_custody::NativeFileCustody::source_quote_capture_bytes()
    }
    pub(crate) fn native_retained_resource_bytes(&self) -> Result<u64, StorageError> {
        self.primary.source_retained_resource_bytes()
    }
    pub(crate) fn inspect_source_descriptor_capacity(
        &self,
        inspection: &mut OwnedCapacityInspectionV1<'_>,
    ) -> Result<(), OwnedCapacityInspectionErrorV1> {
        use crate::source_descriptor_capacity as owned;
        let Self {
            source_database,
            backup_directory,
            manifest,
            primary,
            cancellation,
            observation,
        } = self;
        owned::path(inspection, source_database)?;
        owned::path(inspection, backup_directory)?;
        inspection.visit(manifest)?;
        primary.inspect_source_firstparty_fields(inspection)?;
        owned::arc_block(inspection, cancellation)?;
        owned::atomic(inspection, cancellation)?;
        let FixedMaintenanceObservationV1 {
            database_id,
            open_session_id,
            history,
            original_registry,
            active_pointer_digest,
            legacy_controls,
            source_policies,
        } = observation;
        inspection.inline(database_id)?;
        inspection.inline(open_session_id)?;
        inspection.inline(history)?;
        inspection.inline(original_registry)?;
        inspection.inline(active_pointer_digest)?;
        let legacy_controls::AttestedLegacyControlsV1 {
            commitment,
            control_count,
            key_value_bytes,
            visited_rows,
        } = legacy_controls;
        inspection.inline(commitment)?;
        inspection.inline(control_count)?;
        inspection.inline(key_value_bytes)?;
        inspection.inline(visited_rows)?;
        let source_policies::SourcePolicyDrainObservationV1 {
            retired_policy_count,
            retired_policy_native_bytes,
        } = source_policies;
        inspection.inline(retired_policy_count)?;
        inspection.inline(retired_policy_native_bytes)
    }
}
