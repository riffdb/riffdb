//! Private staged-database implementation of commit-owned contract migration.

use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

use redb::{ReadableTable, ReadableTableMetadata, TableHandle};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AuditPrincipalV1, ContractMigrationArtifactsV1,
    ContractMigrationJournalStepV1, ContractMigrationOperationArtifactsV1,
    ContractMigrationReceiptV1, EncodedPageItem, MigrationBatch, MigrationCutover,
    MigrationCutoverApplied, MigrationJournalState, MigrationScanCursor, MigrationScanPage,
    MigrationStageError, MigrationStagePort, OfflineBackupManifestIdentityV1,
    ServiceAuditAppendIntentV1, StorageError, StorageErrorKind, StorageValueError,
    StoredAdministrationAuditRecordV1, StoredContractMigrationJournalV1,
    StoredContractMigrationRecordV1, StoredContractWriteRetirementV1,
};
use riffdb_types::{
    ApprovalId, BackupNameV1, ContractMigrationInputHash, ContractMigrationOperationId,
    MigrationBundleHash, RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetV1,
    ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1, Timestamp,
};
use sha2::{Digest, Sha256};

use crate::administration::{
    append_audit_record, read_administration_allocator, write_administration_allocator,
};
use crate::codec::{
    decode_active_catalog_pointer_v1, decode_entity_record_v1, encode_active_catalog_pointer_v1,
    encode_contract_bundle_v1, encode_entity_record_v1, encode_index_entry_v2,
};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::hooks::RedbTestOperation;
use crate::keys::{
    encode_contract_bundle_key, encode_contract_migration_operation_key,
    encode_contract_write_retirement_key, encode_entity_key, encode_index_entry_key,
};
use crate::layout::{
    CAPABILITIES, CAPABILITY_TOKENS, CATALOG_ACTIVE, CATALOG_ACTIVE_KEY, COMMITS, CONTRACT_BUNDLES,
    CONTRACT_MIGRATION_JOURNAL, CONTRACT_MIGRATIONS, CONTRACT_WRITE_RETIREMENTS, ENTITIES,
    EVENT_ROUTES, EVENTS, IDEMPOTENCY, IDEMPOTENCY_PENDING, META, OUTBOX, OUTBOX_STATUS,
    PROVENANCE, QUERY_MODULE_ACTIVE, QUERY_MODULES, SECONDARY_INDEXES,
};
use crate::store::{RedbOperationalPorts, RedbStore};

/// Projection-only mutation authority split from a private migration stage.
pub struct RedbMigrationProjectionPorts {
    shared: Arc<crate::store::SharedRedb>,
}

impl RedbMigrationProjectionPorts {
    fn operational(&self) -> RedbOperationalPorts {
        RedbOperationalPorts {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl riffdb_storage_api::AuthoritativeScanReader for RedbMigrationProjectionPorts {
    fn scan_index(
        &self,
        request: riffdb_storage_api::AuthoritativeIndexScanRequest,
    ) -> Result<riffdb_storage_api::AuthoritativeIndexScanPage, StorageError> {
        riffdb_storage_api::AuthoritativeScanReader::scan_index(&self.operational(), request)
    }

    fn scan_commits(
        &self,
        request: riffdb_storage_api::CommitScanRequest,
    ) -> Result<riffdb_storage_api::CommitScanPageV1, StorageError> {
        riffdb_storage_api::AuthoritativeScanReader::scan_commits(&self.operational(), request)
    }
}

impl riffdb_storage_api::PartitionEventRouteReader for RedbMigrationProjectionPorts {
    fn scan_partition_event_routes(
        &self,
        request: riffdb_storage_api::EventRouteScanRequestV1,
    ) -> Result<riffdb_storage_api::EventRouteScanV1, StorageError> {
        riffdb_storage_api::PartitionEventRouteReader::scan_partition_event_routes(
            &self.operational(),
            request,
        )
    }
}

impl riffdb_storage_api::ProjectionApplySnapshotReader for RedbMigrationProjectionPorts {
    fn read_apply_snapshot(
        &self,
        request: &riffdb_storage_api::ProjectionApplySnapshotRequest,
    ) -> Result<riffdb_storage_api::ProjectionApplySnapshot, StorageError> {
        riffdb_storage_api::ProjectionApplySnapshotReader::read_apply_snapshot(
            &self.operational(),
            request,
        )
    }
}

impl riffdb_storage_api::ProjectionMutationRepository for RedbMigrationProjectionPorts {
    fn apply_projection(
        &mut self,
        request: &riffdb_storage_api::ProjectionApplyRequestV1,
    ) -> Result<riffdb_storage_api::ProjectionApplyResult, StorageError> {
        riffdb_storage_api::ProjectionMutationRepository::apply_projection(
            &mut self.operational(),
            request,
        )
    }

    fn transition_projection_control(
        &mut self,
        operation: riffdb_storage_api::ProjectionControlOperation,
    ) -> Result<riffdb_storage_api::ProjectionControlResult, StorageError> {
        riffdb_storage_api::ProjectionMutationRepository::transition_projection_control(
            &mut self.operational(),
            operation,
        )
    }
}

impl riffdb_storage_api::ProjectionQueryReader for RedbMigrationProjectionPorts {
    fn query_projection(
        &self,
        request: &riffdb_storage_api::ProjectionQueryRequest,
    ) -> Result<riffdb_storage_api::ProjectionQueryResult, StorageError> {
        riffdb_storage_api::ProjectionQueryReader::query_projection(&self.operational(), request)
    }

    fn read_projection_status(
        &self,
        identity: &riffdb_types::ProjectionIdentity,
    ) -> Result<riffdb_storage_api::ProjectionStatus, StorageError> {
        riffdb_storage_api::ProjectionQueryReader::read_projection_status(
            &self.operational(),
            identity,
        )
    }
}

/// Read-only live-database authority used for complete preflight after drain.
pub struct RedbContractMigrationPreflight {
    ports: RedbOperationalPorts,
    active_bundle: riffdb_types::ContractBundleHash,
}

/// Move-only immutable-history proof joining a private stage to its predecessor.
pub struct RedbContractMigrationImmutableWitness {
    database_id: riffdb_types::DatabaseId,
    parent: riffdb_types::ContractBundleHash,
    digest: [u8; 32],
}

impl RedbContractMigrationPreflight {
    /// Binds preflight to the transaction-current active predecessor.
    pub fn new(
        ports: RedbOperationalPorts,
        active_bundle: riffdb_types::ContractBundleHash,
    ) -> Result<Self, StorageError> {
        let transaction = ports.begin_read()?;
        if read_active(&transaction)?.bundle_hash() != active_bundle {
            return Err(corrupt());
        }
        drop(transaction);
        Ok(Self {
            ports,
            active_bundle,
        })
    }

    /// Consumes preflight and closes its activated storage handle.
    #[must_use]
    pub fn into_ports(self) -> RedbOperationalPorts {
        self.ports
    }

    /// Seals transaction-current immutable history after complete preflight.
    pub fn immutable_witness(&self) -> Result<RedbContractMigrationImmutableWitness, StorageError> {
        let transaction = self.ports.begin_read()?;
        let database_id = read_database_id(&transaction)?;
        let digest = immutable_history_digest(&transaction)?;
        drop(transaction);
        Ok(RedbContractMigrationImmutableWitness {
            database_id,
            parent: self.active_bundle,
            digest,
        })
    }
}

impl MigrationStagePort for RedbContractMigrationPreflight {
    fn active_bundle_hash(&self) -> riffdb_types::ContractBundleHash {
        self.active_bundle
    }

    fn scan_migration_rows(
        &self,
        cursor: &MigrationScanCursor,
    ) -> Result<MigrationScanPage, MigrationStageError> {
        scan_rows(&self.ports, cursor)
    }

    fn migration_target_exists(
        &self,
        target: &riffdb_storage_api::EntityTarget,
    ) -> Result<bool, MigrationStageError> {
        target_exists(&self.ports, target)
    }

    fn has_unresolved_retiring_admissions(&self) -> Result<bool, MigrationStageError> {
        has_pending_admissions(&self.ports)
    }

    fn apply_migration_batch(&mut self, _batch: MigrationBatch) -> Result<(), MigrationStageError> {
        Err(MigrationStageError::Integrity)
    }

    fn build_migration_projection_candidates(
        &mut self,
        _projections: &[riffdb_types::ProjectionId],
    ) -> Result<(), MigrationStageError> {
        Err(MigrationStageError::Integrity)
    }

    fn validate_migration_stage(
        &self,
        _candidate: riffdb_types::ContractBundleHash,
        _retained_parent_lineage: &[riffdb_types::ContractBundleHash],
    ) -> Result<(), MigrationStageError> {
        Err(MigrationStageError::Integrity)
    }

    fn finalize_migration(
        &mut self,
        _cutover: MigrationCutover,
    ) -> Result<MigrationCutoverApplied, MigrationStageError> {
        Err(MigrationStageError::Integrity)
    }
}

/// Complete operation evidence retained by one protected staged database.
pub struct RedbContractMigrationContext {
    operation_id: ContractMigrationOperationId,
    input_hash: ContractMigrationInputHash,
    artifacts: ContractMigrationArtifactsV1,
    operation_artifacts: ContractMigrationOperationArtifactsV1,
    backup_name: BackupNameV1,
    backup_manifest: OfflineBackupManifestIdentityV1,
    principal: AuditPrincipalV1,
    approval_id: Option<ApprovalId>,
    request_id: RequestId,
    timestamp: Timestamp,
    ingress: ServiceIngressKindV1,
}

impl RedbContractMigrationContext {
    /// Reconstructs exact cutover evidence only from the protected receipt.
    pub fn from_receipt(receipt: &ContractMigrationReceiptV1) -> Result<Self, StorageValueError> {
        let backup_name = receipt
            .backup_name()
            .cloned()
            .ok_or(StorageValueError::InvalidShape)?;
        let backup_manifest = receipt
            .backup_manifest()
            .cloned()
            .ok_or(StorageValueError::InvalidShape)?;
        let admission = receipt.admission();
        Ok(Self {
            operation_id: receipt.operation_id(),
            input_hash: receipt.input_hash(),
            artifacts: receipt.artifacts(),
            operation_artifacts: receipt.operation_artifacts(),
            backup_name,
            backup_manifest,
            principal: admission.principal().clone(),
            approval_id: admission.approval_id().cloned(),
            request_id: admission.request_id(),
            timestamp: admission.accepted_at(),
            ingress: admission.ingress(),
        })
    }
}

/// Exclusive operational handle for a private same-filesystem migration stage.
pub struct RedbContractMigrationStage {
    ports: RedbOperationalPorts,
    context: RedbContractMigrationContext,
    immutable_history_digest: [u8; 32],
}

impl RedbContractMigrationStage {
    /// Joins checked stage ports to the exact accepted operation evidence.
    pub fn new(
        ports: RedbOperationalPorts,
        context: RedbContractMigrationContext,
    ) -> Result<Self, StorageError> {
        let transaction = ports.begin_read()?;
        let active = read_active(&transaction)?;
        if active.bundle_hash() != context.artifacts.parent()
            || context.backup_manifest.database_id() != read_database_id(&transaction)?
        {
            return Err(corrupt());
        }
        let immutable_history_digest = immutable_history_digest(&transaction)?;
        drop(transaction);
        Ok(Self {
            ports,
            context,
            immutable_history_digest,
        })
    }

    /// Reopens a partial private stage without weakening ordinary startup.
    ///
    /// The witness comes from the freshly validated drained predecessor.
    /// Storage rechecks all migration-mutable envelopes and proves immutable
    /// authoritative history remained byte-identical before releasing this
    /// migration-only authority.
    pub fn resume(
        store: RedbStore,
        witness: RedbContractMigrationImmutableWitness,
        context: RedbContractMigrationContext,
    ) -> Result<Self, StorageError> {
        if witness.database_id != context.backup_manifest.database_id()
            || witness.parent != context.artifacts.parent()
        {
            return Err(corrupt());
        }
        let ports = store.into_contract_migration_ports()?;
        let stage = Self::new(ports, context)?;
        if stage.immutable_history_digest != witness.digest {
            return Err(corrupt());
        }
        validate_entity_index_structure(&stage.ports)?;
        Ok(stage)
    }

    /// Consumes the private stage after a terminal coordinator result.
    #[must_use]
    pub fn into_ports(self) -> RedbOperationalPorts {
        self.ports
    }

    /// Splits a least-authority projection builder port over the same private stage.
    #[must_use]
    pub fn projection_ports(&self) -> RedbMigrationProjectionPorts {
        RedbMigrationProjectionPorts {
            shared: Arc::clone(&self.ports.shared),
        }
    }

    fn durable_journal(&self) -> Result<Option<StoredContractMigrationJournalV1>, StorageError> {
        let transaction = self.ports.begin_read()?;
        read_journal_read(&transaction, self.context.operation_id)
    }
}

impl MigrationStagePort for RedbContractMigrationStage {
    fn active_bundle_hash(&self) -> riffdb_types::ContractBundleHash {
        self.context.artifacts.parent()
    }

    fn scan_migration_rows(
        &self,
        cursor: &MigrationScanCursor,
    ) -> Result<MigrationScanPage, MigrationStageError> {
        scan_rows(&self.ports, cursor)
    }

    fn migration_target_exists(
        &self,
        target: &riffdb_storage_api::EntityTarget,
    ) -> Result<bool, MigrationStageError> {
        target_exists(&self.ports, target)
    }

    fn has_unresolved_retiring_admissions(&self) -> Result<bool, MigrationStageError> {
        has_pending_admissions(&self.ports)
    }

    fn migration_journal_state(
        &self,
        migration: MigrationBundleHash,
    ) -> Result<Option<MigrationJournalState>, MigrationStageError> {
        let Some(journal) = self.durable_journal().map_err(stage_error)? else {
            return Ok(None);
        };
        if journal.database_id() != self.context.backup_manifest.database_id()
            || journal.operation_id() != self.context.operation_id
            || journal.input_hash() != self.context.input_hash
            || journal.artifacts() != self.context.artifacts
            || journal.artifacts().migration() != migration
            || journal.step() == ContractMigrationJournalStepV1::Complete
        {
            return Err(MigrationStageError::Integrity);
        }
        MigrationJournalState::new(
            migration,
            journal.cursor().clone(),
            journal.checked_rows(),
            journal.changed_rows(),
            journal.batch_count(),
        )
        .map(Some)
    }

    fn migration_journal_step(
        &self,
        migration: MigrationBundleHash,
    ) -> Result<Option<ContractMigrationJournalStepV1>, MigrationStageError> {
        let Some(journal) = self.durable_journal().map_err(stage_error)? else {
            return Ok(None);
        };
        if journal.artifacts() != self.context.artifacts
            || journal.artifacts().migration() != migration
        {
            return Err(MigrationStageError::Integrity);
        }
        Ok(Some(journal.step()))
    }

    fn migration_frozen_frontier(&self) -> Option<riffdb_types::CommitSequence> {
        self.context.backup_manifest.included_application_frontier()
    }

    fn apply_migration_batch(&mut self, batch: MigrationBatch) -> Result<(), MigrationStageError> {
        if batch.migration() != self.context.artifacts.migration() {
            return Err(MigrationStageError::Integrity);
        }
        let access = self.ports.begin_write().map_err(stage_error)?;
        let transaction = access.transaction().map_err(stage_error)?;
        let prior =
            read_journal_write(transaction, self.context.operation_id).map_err(stage_error)?;
        validate_batch_progress(&batch, prior.as_ref())?;
        let mut entities = transaction
            .open_table(ENTITIES)
            .map_err(|_| MigrationStageError::Integrity)?;
        let mut indexes = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(|_| MigrationStageError::Integrity)?;
        for mutation in batch.mutations() {
            let key = encode_entity_key(mutation.expected().source().target().key());
            let current = entities
                .get(key)
                .map_err(|_| MigrationStageError::Integrity)?
                .ok_or(MigrationStageError::RowChanged)?;
            let decoded = decode_entity_record_v1(current.value()).map_err(stage_error)?;
            if !mutation.expected().matches(decoded.value()) {
                return Err(MigrationStageError::RowChanged);
            }
            drop(current);
            if let Some(post_image) = mutation.post_image() {
                let encoded = encode_entity_record_v1(post_image).map_err(stage_error)?;
                entities
                    .insert(key, encoded.as_bytes())
                    .map_err(|_| MigrationStageError::Integrity)?;
            }
            for index in mutation.rebuilt_indexes() {
                let encoded = encode_index_entry_v2(index).map_err(stage_error)?;
                if indexes
                    .insert(encode_index_entry_key(index.key()), encoded.as_bytes())
                    .map_err(|_| MigrationStageError::Integrity)?
                    .is_some()
                {
                    return Err(MigrationStageError::Integrity);
                }
            }
        }
        drop(indexes);
        drop(entities);
        let next = StoredContractMigrationJournalV1::new(
            self.context.backup_manifest.database_id(),
            self.context.operation_id,
            self.context.input_hash,
            self.context.artifacts,
            ContractMigrationJournalStepV1::Transforming,
            batch.checked_through().clone(),
            batch.checked_rows(),
            batch.changed_rows(),
            prior
                .as_ref()
                .map_or(1, |journal| journal.batch_count() + 1),
            self.context.backup_manifest.included_application_frontier(),
            Vec::new(),
            prior
                .as_ref()
                .map(StoredContractMigrationJournalV1::journal_hash),
        )
        .map_err(|_| MigrationStageError::Integrity)?;
        write_journal(transaction, &next).map_err(stage_error)?;
        access
            .commit_for(RedbTestOperation::ContractMigrationBatch)
            .map_err(stage_error)
    }

    fn checkpoint_migration_step(
        &mut self,
        step: ContractMigrationJournalStepV1,
        required_projections: &[riffdb_types::ProjectionId],
    ) -> Result<(), MigrationStageError> {
        if !matches!(
            step,
            ContractMigrationJournalStepV1::RebuildingProjections
                | ContractMigrationJournalStepV1::Validating
                | ContractMigrationJournalStepV1::ReadyForCutover
        ) || required_projections.windows(2).any(|ids| ids[0] >= ids[1])
        {
            return Err(MigrationStageError::Integrity);
        }
        let access = self.ports.begin_write().map_err(stage_error)?;
        let transaction = access.transaction().map_err(stage_error)?;
        let prior = read_journal_write(transaction, self.context.operation_id)
            .map_err(stage_error)?
            .ok_or(MigrationStageError::Integrity)?;
        let exact_successor = matches!(
            (prior.step(), step),
            (
                ContractMigrationJournalStepV1::Transforming,
                ContractMigrationJournalStepV1::RebuildingProjections
            ) | (
                ContractMigrationJournalStepV1::RebuildingProjections,
                ContractMigrationJournalStepV1::Validating
            ) | (
                ContractMigrationJournalStepV1::Validating,
                ContractMigrationJournalStepV1::ReadyForCutover
            )
        );
        if !exact_successor || prior.artifacts() != self.context.artifacts {
            return Err(MigrationStageError::Integrity);
        }
        let next = StoredContractMigrationJournalV1::new(
            prior.database_id(),
            prior.operation_id(),
            prior.input_hash(),
            prior.artifacts(),
            step,
            prior.cursor().clone(),
            prior.checked_rows(),
            prior.changed_rows(),
            prior.batch_count(),
            prior.frozen_application_frontier(),
            required_projections.to_vec(),
            Some(prior.journal_hash()),
        )
        .map_err(|_| MigrationStageError::Integrity)?;
        write_journal(transaction, &next).map_err(stage_error)?;
        access
            .commit_for(RedbTestOperation::ContractMigrationBatch)
            .map_err(stage_error)
    }

    fn build_migration_projection_candidates(
        &mut self,
        _projections: &[riffdb_types::ProjectionId],
    ) -> Result<(), MigrationStageError> {
        Err(MigrationStageError::Integrity)
    }

    fn validate_migration_stage(
        &self,
        _candidate: riffdb_types::ContractBundleHash,
        _retained_parent_lineage: &[riffdb_types::ContractBundleHash],
    ) -> Result<(), MigrationStageError> {
        self.validate_migration_stage_structure()
    }

    fn validate_migration_stage_structure(&self) -> Result<(), MigrationStageError> {
        let journal = self
            .durable_journal()
            .map_err(stage_error)?
            .ok_or(MigrationStageError::Integrity)?;
        if journal.step() < ContractMigrationJournalStepV1::Validating
            || journal.step() == ContractMigrationJournalStepV1::Complete
            || journal.artifacts() != self.context.artifacts
        {
            return Err(MigrationStageError::Integrity);
        }
        let transaction = self.ports.begin_read().map_err(stage_error)?;
        if immutable_history_digest(&transaction).map_err(stage_error)?
            != self.immutable_history_digest
        {
            return Err(MigrationStageError::Integrity);
        }
        drop(transaction);
        validate_entity_index_structure(&self.ports).map_err(stage_error)
    }

    fn finalize_migration(
        &mut self,
        cutover: MigrationCutover,
    ) -> Result<MigrationCutoverApplied, MigrationStageError> {
        if cutover.parent() != self.context.artifacts.parent()
            || cutover.candidate().bundle_hash() != self.context.artifacts.candidate()
            || cutover.migration() != self.context.artifacts.migration()
        {
            return Err(MigrationStageError::Integrity);
        }
        let access = self.ports.begin_write().map_err(stage_error)?;
        let transaction = access.transaction().map_err(stage_error)?;
        let journal = read_journal_write(transaction, self.context.operation_id)
            .map_err(stage_error)?
            .ok_or(MigrationStageError::Integrity)?;
        if journal.step() != ContractMigrationJournalStepV1::ReadyForCutover
            || journal.artifacts() != self.context.artifacts
            || journal.required_projections() != cutover.required_projections()
                && !journal.required_projections().is_empty()
        {
            return Err(MigrationStageError::Integrity);
        }
        let allocator = read_administration_allocator(transaction).map_err(stage_error)?;
        let allocation = allocator
            .allocate_one()
            .map_err(|_| MigrationStageError::SequenceExhausted)?;
        let sequence = allocation.assigned();
        let active = read_active_write(transaction).map_err(stage_error)?;
        if active.bundle_hash() != cutover.parent() {
            return Err(MigrationStageError::Integrity);
        }

        let bundle_key = encode_contract_bundle_key(
            cutover.candidate().lineage(),
            cutover.candidate().contract_version(),
        )
        .map_err(|_| MigrationStageError::Integrity)?;
        let encoded_bundle = encode_contract_bundle_v1(cutover.candidate()).map_err(stage_error)?;
        let mut bundles = transaction
            .open_table(CONTRACT_BUNDLES)
            .map_err(|_| MigrationStageError::Integrity)?;
        if let Some(existing) = bundles
            .insert(bundle_key.as_slice(), encoded_bundle.as_bytes())
            .map_err(|_| MigrationStageError::Integrity)?
            && existing.value() != encoded_bundle.as_bytes()
        {
            return Err(MigrationStageError::Integrity);
        }
        drop(bundles);

        let successor = ActiveCatalogPointerV1::from_bundle(cutover.candidate());
        let encoded_active = encode_active_catalog_pointer_v1(&successor).map_err(stage_error)?;
        let mut active_table = transaction
            .open_table(CATALOG_ACTIVE)
            .map_err(|_| MigrationStageError::Integrity)?;
        active_table
            .insert(CATALOG_ACTIVE_KEY.as_slice(), encoded_active.as_bytes())
            .map_err(|_| MigrationStageError::Integrity)?;
        drop(active_table);

        let retirement = StoredContractWriteRetirementV1::new(
            self.context.artifacts,
            self.context.operation_id,
            sequence,
        );
        let encoded_retirement =
            riffdb_storage_api::proto_codec::encode_contract_write_retirement_v1(retirement)
                .map_err(|_| MigrationStageError::Integrity)?;
        let mut retirements = transaction
            .open_table(CONTRACT_WRITE_RETIREMENTS)
            .map_err(|_| MigrationStageError::Integrity)?;
        if retirements
            .insert(
                encode_contract_write_retirement_key(cutover.parent()).as_slice(),
                encoded_retirement.as_bytes(),
            )
            .map_err(|_| MigrationStageError::Integrity)?
            .is_some()
        {
            return Err(MigrationStageError::Integrity);
        }
        drop(retirements);

        let record = StoredContractMigrationRecordV1::new(
            self.context.backup_manifest.database_id(),
            self.context.operation_id,
            self.context.input_hash,
            self.context.artifacts,
            self.context.operation_artifacts,
            self.context.backup_name.clone(),
            self.context.backup_manifest.manifest_checksum().clone(),
            self.context.principal.clone(),
            self.context.approval_id.clone(),
            self.context.backup_manifest.included_application_frontier(),
            self.context.backup_manifest.included_application_frontier(),
            journal.checked_rows(),
            journal.changed_rows(),
            journal.batch_count(),
            cutover.validation_digest(),
            sequence,
        )
        .map_err(|_| MigrationStageError::Integrity)?;
        let encoded_record =
            riffdb_storage_api::proto_codec::encode_contract_migration_record_v1(&record)
                .map_err(|_| MigrationStageError::Integrity)?;
        let mut migrations = transaction
            .open_table(CONTRACT_MIGRATIONS)
            .map_err(|_| MigrationStageError::Integrity)?;
        if migrations
            .insert(
                encode_contract_migration_operation_key(self.context.operation_id).as_slice(),
                encoded_record.as_bytes(),
            )
            .map_err(|_| MigrationStageError::Integrity)?
            .is_some()
        {
            return Err(MigrationStageError::Integrity);
        }
        drop(migrations);

        let targets = ServiceAuditTargetsV1::new([
            ServiceAuditTargetV1::ContractLineage(cutover.candidate().lineage().clone()),
            ServiceAuditTargetV1::ContractVersion {
                lineage: cutover.candidate().lineage().clone(),
                version: cutover.candidate().contract_version(),
            },
        ])
        .map_err(|_| MigrationStageError::Integrity)?;
        let intent = ServiceAuditAppendIntentV1::new(
            self.context.request_id,
            self.context.timestamp,
            ServiceOperationV1::ApplyContractMigration,
            ServiceAuditPhaseV1::Succeeded,
            self.context.principal.clone(),
            self.context.ingress,
            targets,
            self.context.approval_id.clone(),
            ServiceAuditLinkV1::ControlPlane {
                administration_sequence: sequence,
            },
        )
        .map_err(|_| MigrationStageError::Integrity)?;
        let audit = riffdb_storage_api::StoredServiceAuditRecordV1::from_intent(sequence, &intent);
        append_audit_record(
            transaction,
            &StoredAdministrationAuditRecordV1::Service(audit),
        )
        .map_err(stage_error)?;

        let complete = StoredContractMigrationJournalV1::new(
            journal.database_id(),
            journal.operation_id(),
            journal.input_hash(),
            journal.artifacts(),
            ContractMigrationJournalStepV1::Complete,
            journal.cursor().clone(),
            journal.checked_rows(),
            journal.changed_rows(),
            journal.batch_count(),
            journal.frozen_application_frontier(),
            cutover.required_projections().to_vec(),
            Some(journal.journal_hash()),
        )
        .map_err(|_| MigrationStageError::Integrity)?;
        write_journal(transaction, &complete).map_err(stage_error)?;
        write_administration_allocator(transaction, allocator, allocation.next())
            .map_err(stage_error)?;
        access
            .commit_for(RedbTestOperation::ContractMigrationCutover)
            .map_err(stage_error)?;
        Ok(MigrationCutoverApplied::new(sequence))
    }
}

fn scan_rows(
    ports: &RedbOperationalPorts,
    cursor: &MigrationScanCursor,
) -> Result<MigrationScanPage, MigrationStageError> {
    let transaction = ports.begin_read().map_err(stage_error)?;
    let table = transaction
        .open_table(ENTITIES)
        .map_err(|_| MigrationStageError::Integrity)?;
    let mut rows = Vec::with_capacity(riffdb_storage_api::MAX_MIGRATION_SCAN_ROWS + 1);
    let mut scan = match cursor.exclusive_lower_bound() {
        Some(target) => table
            .range::<&[u8]>((Excluded(encode_entity_key(target.key())), Unbounded))
            .map_err(|_| MigrationStageError::Integrity)?,
        None => table.iter().map_err(|_| MigrationStageError::Integrity)?,
    };
    for item in scan
        .by_ref()
        .take(riffdb_storage_api::MAX_MIGRATION_SCAN_ROWS + 1)
    {
        let (key, value) = item.map_err(|_| MigrationStageError::Integrity)?;
        let decoded = decode_entity_record_v1(value.value()).map_err(stage_error)?;
        if encode_entity_key(decoded.value().target().key()) != key.value() {
            return Err(MigrationStageError::Integrity);
        }
        rows.push(decoded.into_parts().0);
    }
    let has_more = rows.len() > riffdb_storage_api::MAX_MIGRATION_SCAN_ROWS;
    if has_more {
        rows.pop();
    }
    let next = has_more.then(|| {
        MigrationScanCursor::after(rows.last().expect("bounded nonempty page").target().clone())
    });
    MigrationScanPage::new(rows, next)
}

fn target_exists(
    ports: &RedbOperationalPorts,
    target: &riffdb_storage_api::EntityTarget,
) -> Result<bool, MigrationStageError> {
    let transaction = ports.begin_read().map_err(stage_error)?;
    let table = transaction
        .open_table(ENTITIES)
        .map_err(|_| MigrationStageError::Integrity)?;
    let Some(value) = table
        .get(encode_entity_key(target.key()))
        .map_err(|_| MigrationStageError::Integrity)?
    else {
        return Ok(false);
    };
    let decoded = decode_entity_record_v1(value.value()).map_err(stage_error)?;
    if decoded.value().target() != target {
        return Err(MigrationStageError::Integrity);
    }
    Ok(true)
}

fn has_pending_admissions(ports: &RedbOperationalPorts) -> Result<bool, MigrationStageError> {
    let transaction = ports.begin_read().map_err(stage_error)?;
    let table = transaction
        .open_table(IDEMPOTENCY_PENDING)
        .map_err(|_| MigrationStageError::Integrity)?;
    Ok(!table
        .is_empty()
        .map_err(|_| MigrationStageError::Integrity)?)
}

fn read_active(
    transaction: &redb::ReadTransaction,
) -> Result<ActiveCatalogPointerV1, StorageError> {
    let table = transaction
        .open_table(CATALOG_ACTIVE)
        .map_err(table_error)?;
    let value = table
        .get(CATALOG_ACTIVE_KEY.as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    decode_active_catalog_pointer_v1(value.value()).map(|item| item.into_parts().0)
}

fn read_active_write(
    transaction: &redb::WriteTransaction,
) -> Result<ActiveCatalogPointerV1, StorageError> {
    let table = transaction
        .open_table(CATALOG_ACTIVE)
        .map_err(table_error)?;
    let value = table
        .get(CATALOG_ACTIVE_KEY.as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    decode_active_catalog_pointer_v1(value.value()).map(|item| item.into_parts().0)
}

fn read_database_id(
    transaction: &redb::ReadTransaction,
) -> Result<riffdb_types::DatabaseId, StorageError> {
    let table = transaction
        .open_table(crate::layout::META)
        .map_err(table_error)?;
    let value = table
        .get(crate::layout::META_DATABASE_ID)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    crate::codec::decode_database_identity_v1(value.value()).map(|item| *item.value())
}

fn immutable_history_digest(transaction: &redb::ReadTransaction) -> Result<[u8; 32], StorageError> {
    let mut digest = Sha256::new();
    let meta = transaction.open_table(META).map_err(table_error)?;
    hash_component(&mut digest, META.name().as_bytes())?;
    for row in meta.iter().map_err(precommit_storage_error)? {
        let (key, value) = row.map_err(precommit_storage_error)?;
        hash_component(&mut digest, key.value().as_bytes())?;
        hash_component(&mut digest, value.value())?;
    }
    drop(meta);
    for definition in [
        CONTRACT_BUNDLES,
        QUERY_MODULES,
        QUERY_MODULE_ACTIVE,
        IDEMPOTENCY,
        IDEMPOTENCY_PENDING,
        COMMITS,
        PROVENANCE,
        EVENTS,
        EVENT_ROUTES,
        OUTBOX,
        OUTBOX_STATUS,
        CAPABILITIES,
        CAPABILITY_TOKENS,
    ] {
        hash_component(&mut digest, definition.name().as_bytes())?;
        let table = transaction.open_table(definition).map_err(table_error)?;
        for row in table.iter().map_err(precommit_storage_error)? {
            let (key, value) = row.map_err(precommit_storage_error)?;
            hash_component(&mut digest, key.value())?;
            hash_component(&mut digest, value.value())?;
        }
    }
    Ok(digest.finalize().into())
}

fn validate_entity_index_structure(ports: &RedbOperationalPorts) -> Result<(), StorageError> {
    let transaction = ports.begin_read()?;
    let entities = transaction.open_table(ENTITIES).map_err(table_error)?;
    for row in entities.iter().map_err(precommit_storage_error)? {
        let (key, value) = row.map_err(precommit_storage_error)?;
        let decoded = decode_entity_record_v1(value.value())?;
        if encode_entity_key(decoded.value().target().key()) != key.value() {
            return Err(corrupt());
        }
    }
    drop(entities);
    let indexes = transaction
        .open_table(SECONDARY_INDEXES)
        .map_err(table_error)?;
    for row in indexes.iter().map_err(precommit_storage_error)? {
        let (key, value) = row.map_err(precommit_storage_error)?;
        let decoded = crate::codec::decode_index_entry_v2(value.value())?;
        if encode_index_entry_key(decoded.value().key()) != key.value() {
            return Err(corrupt());
        }
    }
    Ok(())
}

fn hash_component(digest: &mut Sha256, bytes: &[u8]) -> Result<(), StorageError> {
    let length =
        u64::try_from(bytes.len()).map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
    digest.update(length.to_be_bytes());
    digest.update(bytes);
    Ok(())
}

fn read_journal_read(
    transaction: &redb::ReadTransaction,
    operation_id: ContractMigrationOperationId,
) -> Result<Option<StoredContractMigrationJournalV1>, StorageError> {
    let table = transaction
        .open_table(CONTRACT_MIGRATION_JOURNAL)
        .map_err(table_error)?;
    let key = encode_contract_migration_operation_key(operation_id);
    table
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
        .map(|value| {
            riffdb_storage_api::proto_codec::decode_contract_migration_journal_v1(value.value())
                .map(EncodedPageItem::into_parts)
                .map(|parts| parts.0)
                .map_err(crate::error::codec_error)
        })
        .transpose()
}

fn read_journal_write(
    transaction: &redb::WriteTransaction,
    operation_id: ContractMigrationOperationId,
) -> Result<Option<StoredContractMigrationJournalV1>, StorageError> {
    let table = transaction
        .open_table(CONTRACT_MIGRATION_JOURNAL)
        .map_err(table_error)?;
    let key = encode_contract_migration_operation_key(operation_id);
    table
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
        .map(|value| {
            riffdb_storage_api::proto_codec::decode_contract_migration_journal_v1(value.value())
                .map(EncodedPageItem::into_parts)
                .map(|parts| parts.0)
                .map_err(crate::error::codec_error)
        })
        .transpose()
}

fn write_journal(
    transaction: &redb::WriteTransaction,
    journal: &StoredContractMigrationJournalV1,
) -> Result<(), StorageError> {
    let encoded = riffdb_storage_api::proto_codec::encode_contract_migration_journal_v1(journal)
        .map_err(crate::error::codec_error)?;
    let key = encode_contract_migration_operation_key(journal.operation_id());
    transaction
        .open_table(CONTRACT_MIGRATION_JOURNAL)
        .map_err(table_error)?
        .insert(key.as_slice(), encoded.as_bytes())
        .map_err(precommit_storage_error)?;
    Ok(())
}

fn validate_batch_progress(
    batch: &MigrationBatch,
    prior: Option<&StoredContractMigrationJournalV1>,
) -> Result<(), MigrationStageError> {
    if prior.is_some_and(|journal| {
        journal.step() != ContractMigrationJournalStepV1::Transforming
            || batch.checked_rows() <= journal.checked_rows()
            || batch.changed_rows() < journal.changed_rows()
            || batch.checked_through() <= journal.cursor()
    }) || prior.is_none()
        && (batch.checked_rows() == 0 && batch.checked_through().exclusive_lower_bound().is_some()
            || batch.checked_rows() != 0
                && batch.checked_through().exclusive_lower_bound().is_none())
    {
        return Err(MigrationStageError::Integrity);
    }
    Ok(())
}

fn stage_error(error: StorageError) -> MigrationStageError {
    match error.kind() {
        StorageErrorKind::LimitExceeded => MigrationStageError::LimitExceeded,
        StorageErrorKind::SequenceExhausted => MigrationStageError::SequenceExhausted,
        _ => MigrationStageError::Integrity,
    }
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
