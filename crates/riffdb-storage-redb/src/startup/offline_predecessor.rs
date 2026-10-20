//! Closed, read-only predecessor maintenance phase for the offline catalog cut.
//! This cannot authorize Accepted, primary mutation, source closure or serving.
#![cfg_attr(not(test), allow(dead_code))]
use super::*;
use sha2::{Digest, Sha256};

mod artifacts;
mod legacy_controls;
mod source_policies;
use artifacts::OfflinePredecessorArtifactsV1;

pub(super) struct OfflineBackupScanOwnerV1<'owner> {
    artifacts: &'owner OfflinePredecessorArtifactsV1,
}
impl OfflineBackupScanOwnerV1<'_> {
    pub(super) fn admit_source_decode(
        &self,
        bytes: usize,
    ) -> Result<crate::locator_memory::LocatorCharge, StorageError> {
        self.artifacts
            .backup
            .engine()
            .admit_maintenance_decode(bytes)
    }
    pub(super) fn begin_read(&self) -> Result<ReadTransaction, StorageError> {
        self.artifacts.backup.begin_read()
    }
    pub(super) fn observation_matches(
        &self,
        observation: crate::backup::ImmutableBackupObservationV1,
    ) -> Result<bool, StorageError> {
        self.artifacts.backup.observation_matches(observation)
    }
    pub(super) fn verify_read_binding(&self, read: &ReadTransaction) -> Result<(), StorageError> {
        self.artifacts.backup.verify()?;
        OfflinePredecessorArtifactsV1::validate_source_absence(read)
    }
    pub(super) fn begin_history_scope(
        &self,
    ) -> Result<crate::history_stub::ImmutableBackupReadsV1<'_>, StorageError> {
        crate::history_stub::begin_immutable_backup_reads(&self.artifacts.backup)
    }
    pub(super) fn read_checkpoint_roots(
        &self,
    ) -> Result<Option<riffdb_storage_api::ChangelogHistoryStateV3>, StorageError> {
        crate::changelog_v3_roots::read_checkpoint_roots_owned_backup_v1(&self.artifacts.backup)
    }
    pub(super) fn original_registry(&self) -> riffdb_types::SchemaHash {
        self.artifacts.original_registry()
    }
    pub(super) fn validate_layout(&self, read: &ReadTransaction) -> Result<(), StorageError> {
        OfflinePredecessorArtifactsV1::validate_v2_layout(read)
    }
}

/// First phase only; actual primary exclusion and backup descriptors remain
/// private throughout. Its one joined validator thread owns all evidence graphs.
pub(crate) struct OfflinePredecessorMaintenanceV1 {
    artifacts: OfflinePredecessorArtifactsV1,
    inputs: StartupValidationInputs,
}

/// Fixed observations are meaningful only inside their actual session owner.
/// No constructor, clone, raw proof getter or conversion grants live authority.
struct FixedMaintenanceObservationV1 {
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    history: riffdb_storage_api::ChangelogHistoryStateV3,
    original_registry: riffdb_types::SchemaHash,
    active_pointer_digest: Option<[u8; 32]>,
    legacy_controls: legacy_controls::AttestedLegacyControlsV1,
    source_policies: source_policies::SourcePolicyDrainObservationV1,
}

/// Complete predecessor maintenance validation, but NOT the required Source64
/// active-plan input phase or the drain/cut transaction. It retains no compiled
/// catalog, evidence cache, command graph, decoder scratch or startup inputs.
pub(crate) struct MaintenanceValidatedPredecessorV1 {
    artifacts: OfflinePredecessorArtifactsV1,
    observation: FixedMaintenanceObservationV1,
}

impl OfflinePredecessorMaintenanceV1 {
    pub(crate) fn acquire(
        source: &Path,
        backup: &Path,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
    ) -> Result<Self, StorageError> {
        let artifacts = OfflinePredecessorArtifactsV1::acquire(source, backup, cancellation)?;
        Ok(Self { artifacts, inputs })
    }

    pub(crate) fn acquire_configured(
        source: &Path,
        backup: &Path,
        inputs: StartupValidationInputs,
        cancellation: Arc<AtomicBool>,
        domain: crate::source_working_domain::SourceWorkingDomain,
    ) -> Result<Self, StorageError> {
        let artifacts = OfflinePredecessorArtifactsV1::acquire_configured(
            source,
            backup,
            cancellation,
            domain,
        )?;
        Ok(Self { artifacts, inputs })
    }

    pub(crate) fn validate_complete_maintenance(
        self,
    ) -> Result<MaintenanceValidatedPredecessorV1, StorageError> {
        let Self { artifacts, inputs } = self;
        let observation = std::thread::scope(|threads| {
            let handle = std::thread::Builder::new()
                .name("riffdb-cut-validation".to_owned())
                .spawn_scoped(threads, || validate_on_owned_thread(&artifacts, inputs))
                .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
            handle.join().map_err(|_| invariant())?
        })?;
        // The joined thread has physically destroyed its TLS decode scratch and
        // every maintenance graph. Reauthenticate the unchanged original set
        // while primary exclusion remains continuously held, before handoff.
        artifacts.verify()?;
        artifacts.backup.verify_full()?;
        Ok(MaintenanceValidatedPredecessorV1 {
            artifacts,
            observation,
        })
    }
}

// Static test diagnostics localize failure without weakening or remapping it.
fn readonly_phase<T>(
    phase: &'static str,
    result: Result<T, StorageError>,
) -> Result<T, StorageError> {
    #[cfg(test)]
    if let Err(error) = &result {
        eprintln!("readonly_predecessor_phase={phase} kind={:?}", error.kind());
    }
    #[cfg(not(test))]
    let _ = phase;
    result
}

fn validate_on_owned_thread(
    artifacts: &OfflinePredecessorArtifactsV1,
    inputs: StartupValidationInputs,
) -> Result<FixedMaintenanceObservationV1, StorageError> {
    let mut history_scope = readonly_phase(
        "history_scope",
        crate::history_stub::begin_immutable_backup_reads(&artifacts.backup),
    )?;
    let mut session = readonly_phase(
        "evidence_begin",
        OfflinePredecessorEvidenceV1::begin(artifacts, inputs),
    )?;
    let structural_end =
        readonly_phase("structural_walk", session.scan.walk_complete_structural())?;
    let validation = readonly_phase(
        "catalog_history",
        riffdb_catalog::validate_catalog_history_read_only_v1(&mut session).map_err(|error| {
            storage_error(
                error
                    .storage_kind()
                    .unwrap_or(StorageErrorKind::CorruptData),
            )
        }),
    )?;
    let (outcome, historical_end) = validation.into_parts();
    let riffdb_catalog::ReadOnlyCatalogHistoryOutcomeV1::Ready(catalog) = outcome else {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    };
    if !catalog.matches(session.scan.database_id, session.scan.open_session_id)
        || structural_end.cursor != session.scan.next_structural
        || historical_end.cursor != session.scan.next_historical
        || !session.scan.structural_finished
        || !session.scan.historical_finished
        || session.scan.authoritative_finding_seen
        || session.scan.any_finding_seen
        || session.scan.saw_v1_index
    {
        return Err(corrupt());
    }
    session.scan.ensure_structural_continuity()?;
    let read = session
        .scan
        .validation_read
        .as_ref()
        .ok_or_else(invariant)?;
    let history = readonly_phase(
        "retained_history",
        crate::changelog_v3_roots::validate_retained_history_owned_backup_v1(&artifacts.backup),
    )?
    .ok_or_else(corrupt)?;
    if history != session.history {
        return Err(corrupt());
    }
    let active = read.open_table(CATALOG_ACTIVE).map_err(table_error)?;
    let active_pointer_digest = active
        .get(CATALOG_ACTIVE_KEY.as_slice())
        .map_err(precommit_storage_error)?
        .map(|row| Sha256::digest(row.value()).into());
    drop(active);
    // Only this complete original pinned session may attest historical controls.
    // This remains maintenance evidence, never drain/Source/cut authority.
    let legacy_controls = readonly_phase(
        "legacy_controls_attestation",
        legacy_controls::attest_after_complete_original_validation(&session),
    )?;
    let source_policies = readonly_phase(
        "source_policy_drain_observation",
        source_policies::observe_after_complete_original_validation(&session),
    )?;
    let observation = FixedMaintenanceObservationV1 {
        database_id: session.scan.database_id,
        open_session_id: session.scan.open_session_id,
        history,
        original_registry: artifacts.original_registry(),
        active_pointer_digest,
        legacy_controls,
        source_policies,
    };
    // No full catalog or population proof survives into the source phase.
    drop(catalog);
    drop(session);
    history_scope.finish()?;
    drop(history_scope);
    Ok(observation)
}

struct OfflinePredecessorEvidenceV1<'owner> {
    scan: EvidenceScanCoreV1<OfflineBackupScanOwnerV1<'owner>>,
    history: riffdb_storage_api::ChangelogHistoryStateV3,
}
impl<'owner> OfflinePredecessorEvidenceV1<'owner> {
    #[allow(clippy::mutable_key_type)]
    fn begin(
        artifacts: &'owner OfflinePredecessorArtifactsV1,
        inputs: StartupValidationInputs,
    ) -> Result<Self, StorageError> {
        let owner = OfflineBackupScanOwnerV1 { artifacts };
        let read = artifacts.backup.begin_read()?;
        OfflinePredecessorArtifactsV1::validate_v2_layout(&read)?;
        let snapshot = collect_startup_snapshot_with_admission(
            &read,
            None,
            RetainedRegistryAdmissionV1::OfflineWriter4(&owner),
        )?;
        let history = readonly_phase(
            "checkpoint_roots",
            crate::changelog_v3_roots::read_checkpoint_roots_owned_backup_v1(&artifacts.backup),
        )?
        .ok_or_else(corrupt)?;
        if history.lineage().catalog_digest()
            != riffdb_storage_api::AuthoritativeStateCatalogV2.digest()
            || history.lineage().database_id() != snapshot.retained_metadata.database_id()
            || history.lineage().history_incarnation()
                != snapshot.retained_metadata.history_incarnation()
            || artifacts.manifest().history_incarnation()
                != Some(history.lineage().history_incarnation())
        {
            return Err(storage_error(StorageErrorKind::IncompatibleFormat));
        }
        let meta = read.open_table(META).map_err(table_error)?;
        let admission = {
            let row = meta
                .get(crate::primary_admission_roots::key()?)
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            riffdb_storage_api::proto_codec::decode_replication_primary_admission_v1(row.value())
                .map_err(crate::error::codec_error)?
                .into_parts()
                .0
        };
        if admission.lineage() != history.lineage() || admission.fence().is_some() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        drop(meta);
        let database_id = snapshot.retained_metadata.database_id();
        let open_session_id = allocate_open_session()?;
        let durable_commit_epoch = artifacts.backup.observe()?;
        let full_structural_counts = snapshot.structural_counts;
        let structural_counts = snapshot.structural_counts;
        let additive_structural_counts = snapshot.additive_structural_counts;
        let structural_total = snapshot.structural_total;
        let binding_bundles = collect_bundle_bindings(&read)?;
        let publication_audits = build_publication_audit_cache_for_owner(&read, &owner)?;
        let reactive_publication_audit_decodes = publication_audits.decoded_rows;
        let validation_read = read;
        let scan = EvidenceScanCoreV1 {
            source_components: None,
            source_component_start: structural_total,
            catalog_validation_complete: false,
            cancellation: Some(artifacts.cancellation()),
            shared: owner,
            lease: None,
            purpose: EvidenceOpenPurpose::OfflineIntegrityScrub,
            durable_commit_epoch,
            database_id,
            open_session_id,
            retained_metadata: snapshot.retained_metadata,
            inputs,
            v3_activation_required: false,
            clean_close_fast: false,
            bounded_dirty_startup: false,
            verified_clean_lifecycle: None,
            clean_close_declined_reason: None,
            structural_counts,
            full_structural_counts,
            additive_structural_counts,
            structural_total,
            next_structural: StructuralEvidenceCursor::start(database_id, open_session_id),
            next_historical: HistoricalEvidenceCursor::start(database_id, open_session_id),
            last_historical_key: None,
            structural_finished: false,
            historical_finished: false,
            authoritative_finding_seen: false,
            any_finding_seen: false,
            saw_v1_index: false,
            validation_read: Some(validation_read),
            historical_tables: None,
            structural_cursors: None,
            command_history: command_history::CommandHistory::empty(),
            command_audit_bytes: 0,
            command_cache_built: false,
            command_history_rows_decoded: 0,
            recovery_owned_peak_bytes: 0,
            v2_source: true,
            publication_audits: Some(publication_audits),
            binding_bundles,
            entity_chains: None,
            prebuilt_entity_chains: None,
            prebuilt_commit_plans: None,
            reactive_publication_audit_decodes,
            pending_entity_orphan_findings: VecDeque::new(),
            historical_plan: None,
            checkpoint: None,
            sampled_window_rows_inspected: 0,
            checkpoint_verified: false,
            checkpoint_ignored_reason: None,
            walked_suffix_counts: [0; STRUCTURAL_TABLE_COUNT],
            walked_prefix_counts: [0; STRUCTURAL_TABLE_COUNT],
            terminal_execution_failure_rows: 0,
            segments: SegmentMemo::default(),
            bundles: crate::command_prefix::BundleMemo::default(),
        };
        Ok(Self { scan, history })
    }
}
// Retain actual owner affinity even for an otherwise unused evidence value.
impl Drop for OfflinePredecessorEvidenceV1<'_> {
    fn drop(&mut self) {}
}

impl riffdb_catalog::ReadOnlyCatalogHistoryEvidenceV1 for OfflinePredecessorEvidenceV1<'_> {
    type HistoricalEnd = RedbHistoricalEvidenceEnd;
    fn database_id(&self) -> DatabaseId {
        self.scan.database_id()
    }
    fn open_session_id(&self) -> OpenSessionId {
        self.scan.open_session_id()
    }
    fn read_historical_evidence(
        &mut self,
        cursor: HistoricalEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<HistoricalEvidencePage<Self::HistoricalEnd>, StorageError> {
        self.scan.read_historical_evidence(cursor, limit)
    }
    fn read_historical_bundle(
        &mut self,
        lineage: &ContractLineage,
        version: ContractVersion,
        hash: ContractBundleHash,
    ) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
        self.scan.read_historical_bundle(lineage, version, hash)
    }
    fn read_integrity_entity(
        &mut self,
        target: &riffdb_storage_api::EntityTarget,
    ) -> Result<Option<riffdb_storage_api::StoredEntityRecordV1>, StorageError> {
        self.scan.read_integrity_entity(target)
    }
    fn read_integrity_unique_occupancy(
        &mut self,
        target: &UniqueIndexTarget,
    ) -> Result<UniqueOccupancyKind, StorageError> {
        self.scan.read_integrity_unique_occupancy(target)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod nonempty_controls_tests;

mod cut_predecessor_entry;
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use cut_predecessor_entry::CatalogCutPredecessorV1;

mod source_phase;
pub(crate) use source_phase::RetainedPredecessorOuterV1;

impl MaintenanceValidatedPredecessorV1 {
    pub(crate) fn numerical_source_budget(
        &self,
    ) -> &riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkingBudgetV1 {
        self.artifacts.backup.engine().source_working_budget()
    }
}
