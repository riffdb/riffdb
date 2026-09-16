#![expect(
    clippy::expect_used,
    reason = "validated staged migrations retain the predecessor catalog row selected for replacement"
)]

//! Private staged-database implementation of commit-owned contract migration.

use std::sync::Arc;

use redb::{ReadableTable, TableHandle};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AuditPrincipalV1, ContractMigrationArtifactsV1,
    ContractMigrationJournalStepV1, ContractMigrationOperationArtifactsV1,
    ContractMigrationReceiptV1, EncodedPageItem, MigrationBatch, MigrationCutover,
    MigrationCutoverApplied, MigrationJournalState, MigrationScanCursor, MigrationScanPage,
    MigrationStageError, MigrationStagePort, OfflineBackupManifestIdentityV1,
    ServiceAuditAppendIntentV1, StorageError, StorageErrorKind, StorageValueError,
    StoredAdministrationAuditRecordV1, StoredContractMigrationJournalV1,
    StoredContractMigrationRecordV1, StoredContractWriteRetirementV1, StoredRetiredEntityRecordV1,
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
use crate::journal::JournalTable;
use crate::keys::{
    encode_contract_bundle_key, encode_contract_migration_operation_key,
    encode_contract_write_retirement_key, encode_entity_key, encode_index_entry_key,
    encode_retired_entity_key,
};
use crate::layout::{
    CAPABILITIES, CAPABILITY_TOKENS, CATALOG_ACTIVE, CATALOG_ACTIVE_KEY, COMMITS, CONTRACT_BUNDLES,
    CONTRACT_MIGRATION_JOURNAL, CONTRACT_MIGRATIONS, CONTRACT_WRITE_RETIREMENTS, ENTITIES,
    EVENT_ROUTES, EVENTS, IDEMPOTENCY, IDEMPOTENCY_PENDING, META, OUTBOX, OUTBOX_STATUS,
    PROVENANCE, QUERY_MODULE_ACTIVE, QUERY_MODULES, RETIRED_ENTITIES, SECONDARY_INDEXES,
};
use crate::store::{RedbOperationalPorts, RedbStore};

#[path = "migration_stage_history.rs"]
mod history;
use history::{capture_v3_history, hash_v3_prefix};

#[cfg(test)]
#[path = "migration_stage_v3_tests.rs"]
mod v3_tests;

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
    fn scan_entity_partition(
        &self,
        request: riffdb_storage_api::AuthoritativeEntityPartitionScanRequest,
    ) -> Result<riffdb_storage_api::AuthoritativeEntityPartitionScanPage, StorageError> {
        riffdb_storage_api::AuthoritativeScanReader::scan_entity_partition(
            &self.operational(),
            request,
        )
    }

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
    fn capture_apply_batch_snapshot(
        &self,
        identity: &riffdb_types::ProjectionIdentity,
    ) -> Result<Box<dyn riffdb_storage_api::ProjectionBatchSnapshot>, StorageError> {
        riffdb_storage_api::ProjectionApplySnapshotReader::capture_apply_batch_snapshot(
            &self.operational(),
            identity,
        )
    }

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
    fn apply_projection_batch(
        &mut self,
        request: &riffdb_storage_api::ProjectionApplyBatchV1,
    ) -> Result<riffdb_storage_api::ProjectionApplyBatchResult, StorageError> {
        riffdb_storage_api::ProjectionMutationRepository::apply_projection_batch(
            &mut self.operational(),
            request,
        )
    }

    fn resolve_projection_batch(
        &self,
        request: &riffdb_storage_api::ProjectionApplyBatchV1,
    ) -> Result<riffdb_storage_api::ProjectionApplyBatchResult, StorageError> {
        riffdb_storage_api::ProjectionMutationRepository::resolve_projection_batch(
            &self.operational(),
            request,
        )
    }

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
    v3_history: Option<riffdb_storage_api::ChangelogHistoryStateV3>,
}

impl RedbContractMigrationPreflight {
    /// Binds read-only preflight to a cloneable activated read handle.
    pub fn from_shared(
        ports: crate::RedbSharedPorts,
        active_bundle: riffdb_types::ContractBundleHash,
    ) -> Result<Self, StorageError> {
        Self::new(ports.operational(), active_bundle)
    }

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
        let v3_history = capture_v3_history(&transaction)?;
        let digest = immutable_history_digest(&transaction, v3_history)?;
        drop(transaction);
        Ok(RedbContractMigrationImmutableWitness {
            database_id,
            parent: self.active_bundle,
            digest,
            v3_history,
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
    immutable_v3_history: Option<riffdb_storage_api::ChangelogHistoryStateV3>,
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
        let immutable_v3_history = capture_v3_history(&transaction)?;
        let immutable_history_digest =
            immutable_history_digest(&transaction, immutable_v3_history)?;
        drop(transaction);
        Ok(Self {
            ports,
            context,
            immutable_history_digest,
            immutable_v3_history,
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
        let mut stage = Self::new(ports, context)?;
        let transaction = stage.ports.begin_read()?;
        if immutable_history_digest(&transaction, witness.v3_history)? != witness.digest {
            return Err(corrupt());
        }
        drop(transaction);
        stage.immutable_history_digest = witness.digest;
        stage.immutable_v3_history = witness.v3_history;
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

#[cfg(feature = "test-fixtures")]
mod fixture {
    #[cfg(test)]
    mod unique_tests {
        include!("../tests/support/migration_unique.rs");
    }
    use std::num::NonZeroU64;

    use redb::ReadableTable;
    use riffdb_storage_api::{
        BackupIntegrityChecksumV1, ContractMigrationArtifactFileV1,
        ContractMigrationOperationArtifactsV1, StoredContractBundleV1, StoredEntityRecordV1,
        StoredIndexEntryV2,
    };
    use riffdb_types::{
        ActorId, ActorKind, BackupNameV1, CapabilityId, ContractMigrationApplyConfirmation,
        ContractMigrationOperationKind, DatabaseId, ProjectionId, RequestId,
        contract_migration_input_hash,
    };

    use super::*;

    /// Read-only semantic image retained by the redb migration-stage fixture.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct RedbMigrationStageSnapshot {
        active_bundle: riffdb_types::ContractBundleHash,
        entities: Vec<StoredEntityRecordV1>,
        indexes: Vec<StoredIndexEntryV2>,
        retained_archive: Vec<StoredEntityRecordV1>,
        immutable_history: [u8; 32],
        journal: Option<MigrationJournalState>,
        predecessor_writes_retired: bool,
        migration_record: Option<StoredContractMigrationRecordV1>,
    }

    /// Feature-gated redb stage with cached, read-only assertions for migration gates.
    pub struct RedbMigrationStageFixture {
        stage: RedbContractMigrationStage,
        migration: MigrationBundleHash,
        snapshot: RedbMigrationStageSnapshot,
        unresolved_retiring_admission: bool,
        _scope: tempfile::TempDir,
    }

    impl RedbMigrationStageFixture {
        /// Creates one isolated real redb stage containing the exact predecessor rows.
        pub fn create(
            parent: &StoredContractBundleV1,
            candidate: riffdb_types::ContractBundleHash,
            migration: MigrationBundleHash,
            rows: Vec<StoredEntityRecordV1>,
        ) -> Result<Self, StorageError> {
            let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/riffdb-test-data/storage-redb-fixtures");
            std::fs::create_dir_all(&root).map_err(|_| invalid())?;
            let scope = tempfile::TempDir::new_in(root).map_err(|_| invalid())?;
            let path = scope.path().join("db.redb");
            let database_id =
                DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x76; 10])
                    .map_err(|_| invalid())?;
            let ports = crate::fixtures::contract_migration_stage_ports_fixture(
                &path,
                database_id,
                parent,
                &rows,
            )?;

            let artifacts =
                ContractMigrationArtifactsV1::new(parent.bundle_hash(), candidate, migration);
            let operation_artifact =
                ContractMigrationArtifactFileV1::new(1, [0x75; 32]).map_err(|_| invalid())?;
            let operation_id = ContractMigrationOperationId::from_unix_milliseconds_and_random(
                1_700_000_000_001,
                [0x75; 10],
            )
            .map_err(|_| invalid())?;
            let context = RedbContractMigrationContext {
                operation_id,
                input_hash: contract_migration_input_hash(
                    ContractMigrationOperationKind::Apply,
                    parent.bundle_hash(),
                    candidate,
                    migration,
                    ContractMigrationApplyConfirmation::AllowApplyContractMigration,
                ),
                artifacts,
                operation_artifacts: ContractMigrationOperationArtifactsV1::new(
                    operation_artifact,
                    operation_artifact,
                ),
                backup_name: BackupNameV1::new("wp756-redb-stage").map_err(|_| invalid())?,
                backup_manifest: OfflineBackupManifestIdentityV1::new(
                    BackupIntegrityChecksumV1::new(vec![0x75; 32]).map_err(|_| invalid())?,
                    database_id,
                    None,
                ),
                principal: AuditPrincipalV1::new(
                    ActorId::new("wp756-migration-gate").map_err(|_| invalid())?,
                    ActorKind::Human,
                    CapabilityId::from_bytes(uuid(0x73)).map_err(|_| invalid())?,
                    NonZeroU64::MIN,
                ),
                approval_id: None,
                request_id: RequestId::from_bytes(uuid(0x74)).map_err(|_| invalid())?,
                timestamp: Timestamp::new(1_700_000_000, 0).map_err(|_| invalid())?,
                ingress: ServiceIngressKindV1::InProcessTestComparison,
            };
            let stage = RedbContractMigrationStage::new(ports, context)?;
            let snapshot = inspect(&stage, migration)?;
            Ok(Self {
                stage,
                migration,
                snapshot,
                unresolved_retiring_admission: false,
                _scope: scope,
            })
        }

        /// Returns the exact immutable-history digest canary.
        #[must_use]
        pub const fn history_witness(&self) -> &[u8; 32] {
            &self.snapshot.immutable_history
        }

        /// Clones the complete inspected semantic state.
        #[must_use]
        pub fn snapshot(&self) -> RedbMigrationStageSnapshot {
            self.snapshot.clone()
        }

        /// Borrows the current migration journal.
        #[must_use]
        pub const fn journal(&self) -> Option<&MigrationJournalState> {
            self.snapshot.journal.as_ref()
        }

        /// Returns the inspected active bundle hash.
        #[must_use]
        pub const fn active_bundle_hash(&self) -> riffdb_types::ContractBundleHash {
            self.snapshot.active_bundle
        }

        /// Returns whether the predecessor write fence is durable.
        #[must_use]
        pub const fn predecessor_writes_retired(&self) -> bool {
            self.snapshot.predecessor_writes_retired
        }

        /// Borrows retained predecessor images.
        #[must_use]
        pub fn retained_archive(&self) -> &[StoredEntityRecordV1] {
            &self.snapshot.retained_archive
        }

        /// Borrows successor index entries.
        #[must_use]
        pub fn index_entries(&self) -> &[StoredIndexEntryV2] {
            &self.snapshot.indexes
        }

        /// Borrows permanent migration evidence after cutover.
        #[must_use]
        pub const fn migration_record(&self) -> Option<&StoredContractMigrationRecordV1> {
            self.snapshot.migration_record.as_ref()
        }

        /// Iterates current authoritative entity rows.
        pub fn entities(&self) -> impl ExactSizeIterator<Item = &StoredEntityRecordV1> {
            self.snapshot.entities.iter()
        }

        /// Inserts one pending-admission canary used by the fail-closed gate.
        pub fn add_unresolved_retiring_admission(&mut self) -> Result<(), StorageError> {
            self.unresolved_retiring_admission = true;
            Ok(())
        }

        fn refresh(&mut self) -> Result<(), MigrationStageError> {
            self.snapshot = inspect(&self.stage, self.migration).map_err(stage_error)?;
            Ok(())
        }
    }

    impl MigrationStagePort for RedbMigrationStageFixture {
        fn active_bundle_hash(&self) -> riffdb_types::ContractBundleHash {
            MigrationStagePort::active_bundle_hash(&self.stage)
        }

        fn scan_migration_rows(
            &self,
            cursor: &MigrationScanCursor,
        ) -> Result<MigrationScanPage, MigrationStageError> {
            self.stage.scan_migration_rows(cursor)
        }

        fn migration_target_exists(
            &self,
            target: &riffdb_storage_api::EntityTarget,
        ) -> Result<bool, MigrationStageError> {
            self.stage.migration_target_exists(target)
        }

        fn has_unresolved_retiring_admissions(&self) -> Result<bool, MigrationStageError> {
            if self.unresolved_retiring_admission {
                Ok(true)
            } else {
                self.stage.has_unresolved_retiring_admissions()
            }
        }

        fn migration_journal_state(
            &self,
            migration: MigrationBundleHash,
        ) -> Result<Option<MigrationJournalState>, MigrationStageError> {
            self.stage.migration_journal_state(migration)
        }

        fn migration_journal_step(
            &self,
            migration: MigrationBundleHash,
        ) -> Result<Option<ContractMigrationJournalStepV1>, MigrationStageError> {
            self.stage.migration_journal_step(migration)
        }

        fn migration_frozen_frontier(&self) -> Option<riffdb_types::CommitSequence> {
            self.stage.migration_frozen_frontier()
        }

        fn retained_migration_entity_count(
            &self,
            migration: MigrationBundleHash,
            entity_types: &[riffdb_types::EntityTypeId],
        ) -> Result<u64, MigrationStageError> {
            self.stage
                .retained_migration_entity_count(migration, entity_types)
        }

        fn apply_migration_batch(
            &mut self,
            batch: MigrationBatch,
        ) -> Result<(), MigrationStageError> {
            let result = MigrationStagePort::apply_migration_batch(&mut self.stage, batch);
            self.refresh()?;
            result
        }

        fn checkpoint_migration_step(
            &mut self,
            step: ContractMigrationJournalStepV1,
            required_projections: &[ProjectionId],
        ) -> Result<(), MigrationStageError> {
            let result = self
                .stage
                .checkpoint_migration_step(step, required_projections);
            self.refresh()?;
            result
        }

        fn build_migration_projection_candidates(
            &mut self,
            projections: &[ProjectionId],
        ) -> Result<(), MigrationStageError> {
            let result = self
                .stage
                .build_migration_projection_candidates(projections);
            self.refresh()?;
            result
        }

        fn validate_migration_stage(
            &self,
            candidate: riffdb_types::ContractBundleHash,
            retained_parent_lineage: &[riffdb_types::ContractBundleHash],
        ) -> Result<(), MigrationStageError> {
            self.stage
                .validate_migration_stage(candidate, retained_parent_lineage)
        }

        fn validate_migration_stage_structure(&self) -> Result<(), MigrationStageError> {
            self.stage.validate_migration_stage_structure()
        }

        fn migration_unique_validation(
            &self,
            artifacts: ContractMigrationArtifactsV1,
        ) -> Result<
            Option<Box<dyn riffdb_storage_api::MigrationUniqueValidation + '_>>,
            MigrationStageError,
        > {
            self.stage.migration_unique_validation(artifacts)
        }

        fn finalize_migration(
            &mut self,
            cutover: MigrationCutover,
        ) -> Result<MigrationCutoverApplied, MigrationStageError> {
            let result = MigrationStagePort::finalize_migration(&mut self.stage, cutover);
            self.refresh()?;
            result
        }
    }

    fn inspect(
        stage: &RedbContractMigrationStage,
        migration: MigrationBundleHash,
    ) -> Result<RedbMigrationStageSnapshot, StorageError> {
        let transaction = stage.ports.begin_read()?;
        let active_bundle = read_active(&transaction)?.bundle_hash();
        let entities_table = transaction.open_table(ENTITIES).map_err(|_| invalid())?;
        let mut entities = Vec::new();
        for entry in entities_table.iter().map_err(|_| invalid())? {
            let (_, value) = entry.map_err(|_| invalid())?;
            entities.push(decode_entity_record_v1(value.value())?.into_parts().0);
        }
        drop(entities_table);
        let indexes_table = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(|_| invalid())?;
        let mut indexes = Vec::new();
        for entry in indexes_table.iter().map_err(|_| invalid())? {
            let (_, value) = entry.map_err(|_| invalid())?;
            indexes.push(
                crate::codec::decode_index_entry_v2(value.value())?
                    .into_parts()
                    .0,
            );
        }
        drop(indexes_table);
        let retired_table = transaction
            .open_table(RETIRED_ENTITIES)
            .map_err(|_| invalid())?;
        let mut retained_archive = Vec::new();
        for entry in retired_table.iter().map_err(|_| invalid())? {
            let (_, value) = entry.map_err(|_| invalid())?;
            let retired =
                riffdb_storage_api::proto_codec::decode_retired_entity_record_v1(value.value())
                    .map_err(|_| invalid())?
                    .into_parts()
                    .0;
            retained_archive.push(
                decode_entity_record_v1(retired.original_entity_envelope())?
                    .into_parts()
                    .0,
            );
        }
        drop(retired_table);
        let journal = read_journal_read(&transaction, stage.context.operation_id)?
            .map(|row| {
                let mut state = MigrationJournalState::new(
                    migration,
                    row.cursor().clone(),
                    row.checked_rows(),
                    row.changed_rows(),
                    row.batch_count(),
                )
                .map_err(|_| invalid())?;
                if row.step() == ContractMigrationJournalStepV1::Complete {
                    state.mark_complete().map_err(|_| invalid())?;
                }
                Ok::<_, StorageError>(state)
            })
            .transpose()?;
        let predecessor_writes_retired = transaction
            .open_table(CONTRACT_WRITE_RETIREMENTS)
            .map_err(|_| invalid())?
            .get(encode_contract_write_retirement_key(stage.context.artifacts.parent()).as_slice())
            .map_err(|_| invalid())?
            .is_some();
        let migrations = transaction
            .open_table(CONTRACT_MIGRATIONS)
            .map_err(|_| invalid())?;
        let migration_record = migrations
            .iter()
            .map_err(|_| invalid())?
            .next()
            .transpose()
            .map_err(|_| invalid())?
            .map(|(_, value)| {
                riffdb_storage_api::proto_codec::decode_contract_migration_record_v1(value.value())
                    .map(|item| item.into_parts().0)
                    .map_err(|_| invalid())
            })
            .transpose()?;
        drop(migrations);
        let immutable_history = stage.immutable_history_digest;
        Ok(RedbMigrationStageSnapshot {
            active_bundle,
            entities,
            indexes,
            retained_archive,
            immutable_history,
            journal,
            predecessor_writes_retired,
            migration_record,
        })
    }

    fn uuid(seed: u8) -> [u8; 16] {
        let mut value = [seed; 16];
        value[6] = 0x70 | (seed & 0x0f);
        value[8] = 0x80 | (seed & 0x3f);
        value
    }

    fn invalid() -> StorageError {
        storage_error(StorageErrorKind::InvariantViolation)
    }
}

#[cfg(feature = "test-fixtures")]
pub use fixture::{RedbMigrationStageFixture, RedbMigrationStageSnapshot};

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

    fn migration_unique_validation(
        &self,
        artifacts: ContractMigrationArtifactsV1,
    ) -> Result<
        Option<Box<dyn riffdb_storage_api::MigrationUniqueValidation + '_>>,
        MigrationStageError,
    > {
        if artifacts != self.context.artifacts {
            return Err(MigrationStageError::Integrity);
        }
        self.validate_migration_stage_structure()?;
        // Holding this borrow prevents all authoritative stage mutations. The
        // split projection port can change only derived projection state.
        Ok(Some(Box::new(FinalUniqueValidation(self))))
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

    fn retained_migration_entity_count(
        &self,
        migration: MigrationBundleHash,
        entity_types: &[riffdb_types::EntityTypeId],
    ) -> Result<u64, MigrationStageError> {
        let transaction = self.ports.begin_read().map_err(stage_error)?;
        let table = transaction
            .open_table(RETIRED_ENTITIES)
            .map_err(|_| MigrationStageError::Integrity)?;
        let mut count = 0_u64;
        for entry in table.iter().map_err(|_| MigrationStageError::Integrity)? {
            let (_, value) = entry.map_err(|_| MigrationStageError::Integrity)?;
            let record =
                riffdb_storage_api::proto_codec::decode_retired_entity_record_v1(value.value())
                    .map_err(|_| MigrationStageError::Integrity)?;
            if record.value().migration() == migration
                && entity_types.contains(&record.value().original_target().entity_type_id())
            {
                count = count
                    .checked_add(1)
                    .ok_or(MigrationStageError::LimitExceeded)?;
            }
        }
        Ok(count)
    }

    fn apply_migration_batch(&mut self, batch: MigrationBatch) -> Result<(), MigrationStageError> {
        if batch.migration() != self.context.artifacts.migration() {
            return Err(MigrationStageError::Integrity);
        }
        let access = self
            .ports
            .begin_attributed_write(
                riffdb_storage_api::ChangelogAttributionV3::ContractMigrationBatch,
            )
            .map_err(stage_error)?;
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
        let mut retained = transaction
            .open_table(RETIRED_ENTITIES)
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
            let predecessor_envelope = current.value().to_vec();
            drop(current);
            if mutation.post_image().is_some() || mutation.retires_source() {
                if let Some(post_image) = mutation.post_image()
                    && post_image.target() != mutation.expected().source().target()
                {
                    let target_key = encode_entity_key(post_image.target().key());
                    if entities
                        .get(target_key)
                        .map_err(|_| MigrationStageError::Integrity)?
                        .is_some()
                    {
                        return Err(MigrationStageError::Integrity);
                    }
                }
                let retained_record = StoredRetiredEntityRecordV1::new(
                    self.context.operation_id,
                    batch.migration(),
                    mutation.expected().source().target().clone(),
                    predecessor_envelope,
                )
                .map_err(|_| MigrationStageError::Integrity)?;
                let retained_envelope =
                    riffdb_storage_api::proto_codec::encode_retired_entity_record_v1(
                        &retained_record,
                    )
                    .map_err(|_| MigrationStageError::Integrity)?;
                let retained_key = encode_retired_entity_key(
                    self.context.operation_id,
                    mutation.expected().source().target(),
                )
                .map_err(|_| MigrationStageError::Integrity)?;
                if retained
                    .insert(retained_key.as_slice(), retained_envelope.as_bytes())
                    .map_err(|_| MigrationStageError::Integrity)?
                    .is_some()
                {
                    return Err(MigrationStageError::Integrity);
                }
                entities
                    .remove(key)
                    .map_err(|_| MigrationStageError::Integrity)?
                    .ok_or(MigrationStageError::RowChanged)?;
                let entity_key = mutation.expected().source().target().key().as_bytes();
                let mut obsolete = Vec::new();
                for entry in indexes.iter().map_err(|_| MigrationStageError::Integrity)? {
                    let (index_key, _) = entry.map_err(|_| MigrationStageError::Integrity)?;
                    if index_key.value().ends_with(entity_key) {
                        obsolete.push(index_key.value().to_vec());
                    }
                }
                for index_key in obsolete {
                    indexes
                        .remove(index_key.as_slice())
                        .map_err(|_| MigrationStageError::Integrity)?;
                }
                if let Some(post_image) = mutation.post_image() {
                    let target_key = encode_entity_key(post_image.target().key());
                    let encoded = encode_entity_record_v1(post_image).map_err(stage_error)?;
                    if entities
                        .insert(target_key, encoded.as_bytes())
                        .map_err(|_| MigrationStageError::Integrity)?
                        .is_some()
                    {
                        return Err(MigrationStageError::Integrity);
                    }
                }
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
        drop(retained);
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
        let access = self
            .ports
            .begin_attributed_write(
                riffdb_storage_api::ChangelogAttributionV3::ContractMigrationBatch,
            )
            .map_err(stage_error)?;
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
            || journal.operation_id() != self.context.operation_id
            || journal.input_hash() != self.context.input_hash
            || journal.database_id() != self.context.backup_manifest.database_id()
            || journal.frozen_application_frontier() != self.migration_frozen_frontier()
        {
            return Err(MigrationStageError::Integrity);
        }
        let transaction = self.ports.begin_read().map_err(stage_error)?;
        if immutable_history_digest(&transaction, self.immutable_v3_history).map_err(stage_error)?
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
        let access = self
            .ports
            .begin_attributed_write(
                riffdb_storage_api::ChangelogAttributionV3::ContractMigrationCutover,
            )
            .map_err(stage_error)?;
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

/// Returns the exact exclusive lower bound `key` as an inclusive start key.
///
/// `key || 0x00` is the least byte string strictly greater than `key`: any
/// greater string either extends `key`, and is therefore at least
/// `key || 0x00`, or diverges upward at an earlier position and therefore
/// already exceeds `key || 0x00` at that same position.
fn exclusive_start(key: &[u8]) -> Vec<u8> {
    let mut start = Vec::with_capacity(key.len().saturating_add(1));
    start.extend_from_slice(key);
    start.push(0);
    start
}

fn scan_rows(
    ports: &RedbOperationalPorts,
    cursor: &MigrationScanCursor,
) -> Result<MigrationScanPage, MigrationStageError> {
    // ADR-0104 section 2: preflight is an operational scan and must read one
    // published checkpoint-plus-overlay view. Iterating the dereferenced
    // `ENTITIES` table reads the checkpoint alone and silently omits every
    // transition already durable in the journal suffix but not yet
    // checkpointed -- exactly the predecessor rows a fail-closed check exists
    // to refuse.
    let access = ports.begin_composite_read().map_err(stage_error)?;
    let start = cursor
        .exclusive_lower_bound()
        .map(|target| exclusive_start(encode_entity_key(target.key())))
        .unwrap_or_default();
    let page = access
        .read_range_to(
            JournalTable::Entities,
            &start,
            None,
            riffdb_storage_api::MAX_MIGRATION_SCAN_ROWS + 1,
        )
        .map_err(stage_error)?;
    let mut rows = Vec::with_capacity(riffdb_storage_api::MAX_MIGRATION_SCAN_ROWS + 1);
    for (key, value) in page {
        let decoded = decode_entity_record_v1(&value).map_err(stage_error)?;
        if encode_entity_key(decoded.value().target().key()) != key.as_ref() {
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
    // Same ADR-0104 section 2 rule as `scan_rows`: an overlay value can create
    // the target and an overlay tombstone is authoritative absence, so neither
    // answer may come from the checkpoint alone.
    let access = ports.begin_composite_read().map_err(stage_error)?;
    let Some(value) = access
        .read_value(JournalTable::Entities, encode_entity_key(target.key()))
        .map_err(stage_error)?
    else {
        return Ok(false);
    };
    let decoded = decode_entity_record_v1(&value).map_err(stage_error)?;
    if decoded.value().target() != target {
        return Err(MigrationStageError::Integrity);
    }
    Ok(true)
}

fn has_pending_admissions(ports: &RedbOperationalPorts) -> Result<bool, MigrationStageError> {
    // An unresolved admission that is still journal-suffix durable must hold
    // the migration closed, so emptiness is decided on the merged view rather
    // than by table metadata on the checkpoint root.
    let access = ports.begin_composite_read().map_err(stage_error)?;
    Ok(!access
        .read_range_to(JournalTable::IdempotencyPending, &[], None, 1)
        .map_err(stage_error)?
        .is_empty())
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
    transaction: &crate::store::OperationalWriteTransaction,
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

fn immutable_history_digest(
    transaction: &redb::ReadTransaction,
    v3_history: Option<riffdb_storage_api::ChangelogHistoryStateV3>,
) -> Result<[u8; 32], StorageError> {
    let mut digest = Sha256::new();
    hash_v3_prefix(transaction, v3_history, &mut digest)?;
    let meta = transaction.open_table(META).map_err(table_error)?;
    hash_component(&mut digest, META.name().as_bytes())?;
    for row in meta.iter().map_err(precommit_storage_error)? {
        let (key, value) = row.map_err(precommit_storage_error)?;
        // These two roots must advance with each batch. The exact original
        // prefix is checked above; every other metadata byte remains immutable.
        if [
            riffdb_storage_api::AuthoritativeNamespaceV1::ChangelogHistoryState,
            riffdb_storage_api::AuthoritativeNamespaceV1::NextChangelogTransaction,
        ]
        .into_iter()
        .any(|namespace| namespace.metadata_key() == Some(key.value()))
        {
            continue;
        }
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

struct FinalUniqueValidation<'a>(&'a RedbContractMigrationStage);

impl riffdb_storage_api::MigrationUniqueValidation for FinalUniqueValidation<'_> {
    fn validate_unique_owner(
        &self,
        entity: riffdb_types::EntityTypeId,
        prefix: &riffdb_storage_api::StructurallyDecodedIndexRangePrefixV1,
        expected: &riffdb_storage_api::StoredIndexEntryV2,
    ) -> Result<(), MigrationStageError> {
        if expected.schema_binding().bundle_hash() != self.0.context.artifacts.candidate()
            || expected.key().index_id() != prefix.index_id()
            || !expected.key().as_bytes().starts_with(prefix.as_bytes())
        {
            return Err(MigrationStageError::Integrity);
        }
        let upper = crate::reads::exclusive_prefix_end(prefix.as_bytes())
            .ok_or(MigrationStageError::Integrity)?;
        let access = self.0.ports.begin_composite_read().map_err(stage_error)?;
        let entries = access
            .read_range(JournalTable::SecondaryIndexes, prefix.as_bytes(), &upper, 2)
            .map_err(stage_error)?;
        for (key, value) in &entries {
            let decoded = crate::codec::decode_index_entry_v2(value).map_err(stage_error)?;
            if encode_index_entry_key(decoded.value().key()) != key.as_ref()
                || decoded.value().key().index_id() != prefix.index_id()
                || decoded.value().schema_binding() != expected.schema_binding()
            {
                return Err(MigrationStageError::Integrity);
            }
        }
        match entries.as_slice() {
            [(_, value)] => {
                let decoded = crate::codec::decode_index_entry_v2(value).map_err(stage_error)?;
                if decoded.value() != expected {
                    return Err(MigrationStageError::Integrity);
                }
                Ok(())
            }
            [_, _] => Err(MigrationStageError::UniqueConflict {
                entity,
                index: prefix.index_id(),
            }),
            _ => Err(MigrationStageError::Integrity),
        }
    }
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
    transaction: &crate::store::OperationalWriteTransaction,
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
    transaction: &crate::store::OperationalWriteTransaction,
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
        StorageErrorKind::SequenceExhausted | StorageErrorKind::HistoryPruned => {
            MigrationStageError::SequenceExhausted
        }
        _ => MigrationStageError::Integrity,
    }
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

#[cfg(test)]
mod tests {
    use super::exclusive_start;

    /// `exclusive_start` converts the migration cursor's exclusive lower bound
    /// into the inclusive start the overlay-aware range accessor takes. It is
    /// only correct if `key || 0x00` is the exact successor of `key`: strictly
    /// greater than `key`, and less than or equal to every other byte string
    /// that is greater than `key`. A wrong bound here silently skips or repeats
    /// a preflight page, which would re-open the same fail-closed hole one
    /// pagination step later.
    #[test]
    fn exclusive_start_is_the_exact_successor_of_its_key() {
        for key in [
            [].as_slice(),
            b"a".as_slice(),
            b"entity".as_slice(),
            &[0x00],
            &[0xff],
            &[0xff, 0xff, 0xff],
            &[0x10, 0x00, 0x20],
            &[0x41; 24],
        ] {
            let start = exclusive_start(key);
            assert!(start.as_slice() > key, "successor must exceed its key");

            // Nothing sorts strictly between `key` and its successor.
            let mut between = key.to_vec();
            between.push(0);
            assert_eq!(between, start);

            // Every candidate greater than `key` is at or after the successor.
            for candidate in [
                {
                    let mut value = key.to_vec();
                    value.push(0);
                    value.push(0);
                    value
                },
                {
                    let mut value = key.to_vec();
                    value.push(0xff);
                    value
                },
                {
                    let mut value = key.to_vec();
                    value.extend_from_slice(b"zzz");
                    value
                },
            ] {
                assert!(candidate.as_slice() > key);
                assert!(
                    candidate >= start,
                    "successor must not skip a key greater than its cursor"
                );
            }
        }

        // A key that diverges upward before the cursor ends still sorts after
        // the successor, so a fixed-length keyspace loses no row either.
        let key = [0x10, 0x20, 0x30];
        let start = exclusive_start(&key);
        for greater in [[0x10, 0x20, 0x31], [0x10, 0x21, 0x00], [0x11, 0x00, 0x00]] {
            assert!(greater.as_slice() > key.as_slice());
            assert!(greater.as_slice() > start.as_slice());
        }
    }
}
