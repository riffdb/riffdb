//! Durable identities and closed external state for contract migration.

use riffdb_types::{
    AdministrationSequence, ApprovalId, CommitSequence, ContractBundleHash,
    ContractMigrationInputHash, ContractMigrationJournalHash, ContractMigrationOperationId,
    ContractMigrationValidationDigest, DatabaseId, MigrationBundleHash, ProjectionId, RequestId,
    ServiceIngressKindV1, Timestamp,
};

use crate::{
    AuditPrincipalV1, BackupIntegrityChecksumV1, OfflineBackupManifestIdentityV1, StorageValueError,
};

/// Maximum receipt phase transitions retained for one operation.
pub const MAX_CONTRACT_MIGRATION_RECEIPT_TRANSITIONS_V1: usize = 32;
/// Maximum successor projection identities retained by a migration journal.
pub const MAX_CONTRACT_MIGRATION_PROJECTIONS_V1: usize = 4_096;

/// Exact SHA-256 identity of one protected operation artifact.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContractMigrationArtifactFileV1 {
    length: u64,
    sha256: [u8; 32],
}

impl ContractMigrationArtifactFileV1 {
    /// Constructs one nonempty immutable artifact identity.
    pub fn new(length: u64, sha256: [u8; 32]) -> Result<Self, StorageValueError> {
        if length == 0 {
            return Err(StorageValueError::Empty);
        }
        Ok(Self { length, sha256 })
    }

    /// Returns the exact artifact byte length.
    #[must_use]
    pub const fn length(self) -> u64 {
        self.length
    }

    /// Returns the exact SHA-256 checksum.
    #[must_use]
    pub const fn sha256(self) -> [u8; 32] {
        self.sha256
    }
}

/// Protected canonical successor and migration bundle artifact identities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContractMigrationOperationArtifactsV1 {
    candidate: ContractMigrationArtifactFileV1,
    migration: ContractMigrationArtifactFileV1,
}

impl ContractMigrationOperationArtifactsV1 {
    /// Constructs the exact immutable operation artifact pair.
    #[must_use]
    pub const fn new(
        candidate: ContractMigrationArtifactFileV1,
        migration: ContractMigrationArtifactFileV1,
    ) -> Self {
        Self {
            candidate,
            migration,
        }
    }

    /// Returns the canonical successor bundle file identity.
    #[must_use]
    pub const fn candidate(self) -> ContractMigrationArtifactFileV1 {
        self.candidate
    }

    /// Returns the canonical migration bundle file identity.
    #[must_use]
    pub const fn migration(self) -> ContractMigrationArtifactFileV1 {
        self.migration
    }
}

/// Exact parent, successor, and migration semantic identities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContractMigrationArtifactsV1 {
    parent: ContractBundleHash,
    candidate: ContractBundleHash,
    migration: MigrationBundleHash,
}

impl ContractMigrationArtifactsV1 {
    /// Constructs the exact artifact triple.
    #[must_use]
    pub const fn new(
        parent: ContractBundleHash,
        candidate: ContractBundleHash,
        migration: MigrationBundleHash,
    ) -> Self {
        Self {
            parent,
            candidate,
            migration,
        }
    }

    /// Returns the active predecessor bundle hash.
    #[must_use]
    pub const fn parent(self) -> ContractBundleHash {
        self.parent
    }

    /// Returns the successor bundle hash.
    #[must_use]
    pub const fn candidate(self) -> ContractBundleHash {
        self.candidate
    }

    /// Returns the migration bundle hash.
    #[must_use]
    pub const fn migration(self) -> MigrationBundleHash {
        self.migration
    }
}

/// Closed durable journal step.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ContractMigrationJournalStepV1 {
    /// Entity and index batches are being applied.
    Transforming,
    /// Required projection generations are being rebuilt.
    RebuildingProjections,
    /// Complete staged validation is running.
    Validating,
    /// The stage has validated and is ready for atomic cutover.
    ReadyForCutover,
    /// Final cutover is durably complete inside the stage.
    Complete,
}

/// Durable stage journal advanced atomically with each migration mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredContractMigrationJournalV1 {
    database_id: DatabaseId,
    operation_id: ContractMigrationOperationId,
    input_hash: ContractMigrationInputHash,
    artifacts: ContractMigrationArtifactsV1,
    step: ContractMigrationJournalStepV1,
    cursor: crate::MigrationScanCursor,
    checked_rows: u64,
    changed_rows: u64,
    batch_count: u64,
    frozen_application_frontier: Option<CommitSequence>,
    required_projections: Vec<ProjectionId>,
    previous_hash: Option<ContractMigrationJournalHash>,
    journal_hash: ContractMigrationJournalHash,
}

impl StoredContractMigrationJournalV1 {
    /// Constructs a journal and computes its exact chained hash.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_id: DatabaseId,
        operation_id: ContractMigrationOperationId,
        input_hash: ContractMigrationInputHash,
        artifacts: ContractMigrationArtifactsV1,
        step: ContractMigrationJournalStepV1,
        cursor: crate::MigrationScanCursor,
        checked_rows: u64,
        changed_rows: u64,
        batch_count: u64,
        frozen_application_frontier: Option<CommitSequence>,
        required_projections: Vec<ProjectionId>,
        previous_hash: Option<ContractMigrationJournalHash>,
    ) -> Result<Self, StorageValueError> {
        let mut value = Self {
            database_id,
            operation_id,
            input_hash,
            artifacts,
            step,
            cursor,
            checked_rows,
            changed_rows,
            batch_count,
            frozen_application_frontier,
            required_projections,
            previous_hash,
            journal_hash: ContractMigrationJournalHash::from_bytes([0; 32]),
        };
        value.journal_hash = value.computed_hash()?;
        Self::from_stored_parts(
            value.database_id,
            value.operation_id,
            value.input_hash,
            value.artifacts,
            value.step,
            value.cursor,
            value.checked_rows,
            value.changed_rows,
            value.batch_count,
            value.frozen_application_frontier,
            value.required_projections,
            value.previous_hash,
            value.journal_hash,
        )
    }

    /// Reconstructs a semantically checked durable journal.
    #[allow(clippy::too_many_arguments)]
    pub fn from_stored_parts(
        database_id: DatabaseId,
        operation_id: ContractMigrationOperationId,
        input_hash: ContractMigrationInputHash,
        artifacts: ContractMigrationArtifactsV1,
        step: ContractMigrationJournalStepV1,
        cursor: crate::MigrationScanCursor,
        checked_rows: u64,
        changed_rows: u64,
        batch_count: u64,
        frozen_application_frontier: Option<CommitSequence>,
        required_projections: Vec<ProjectionId>,
        previous_hash: Option<ContractMigrationJournalHash>,
        journal_hash: ContractMigrationJournalHash,
    ) -> Result<Self, StorageValueError> {
        if changed_rows > checked_rows
            || required_projections.len() > MAX_CONTRACT_MIGRATION_PROJECTIONS_V1
            || required_projections.windows(2).any(|ids| ids[0] >= ids[1])
            || checked_rows == 0 && cursor.exclusive_lower_bound().is_some()
            || checked_rows != 0 && cursor.exclusive_lower_bound().is_none()
            || batch_count == 0 && previous_hash.is_some()
            || step != ContractMigrationJournalStepV1::Transforming && batch_count == 0
        {
            return Err(StorageValueError::InvalidShape);
        }
        let value = Self {
            database_id,
            operation_id,
            input_hash,
            artifacts,
            step,
            cursor,
            checked_rows,
            changed_rows,
            batch_count,
            frozen_application_frontier,
            required_projections,
            previous_hash,
            journal_hash,
        };
        if value.computed_hash()? != journal_hash {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(value)
    }

    /// Returns the database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }
    /// Returns the caller-stable operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> ContractMigrationOperationId {
        self.operation_id
    }
    /// Returns the canonical semantic input hash.
    #[must_use]
    pub const fn input_hash(&self) -> ContractMigrationInputHash {
        self.input_hash
    }
    /// Returns the exact artifact triple.
    #[must_use]
    pub const fn artifacts(&self) -> ContractMigrationArtifactsV1 {
        self.artifacts
    }
    /// Returns the current typed step.
    #[must_use]
    pub const fn step(&self) -> ContractMigrationJournalStepV1 {
        self.step
    }
    /// Borrows the canonical exclusive scan cursor.
    #[must_use]
    pub const fn cursor(&self) -> &crate::MigrationScanCursor {
        &self.cursor
    }
    /// Returns cumulative checked rows.
    #[must_use]
    pub const fn checked_rows(&self) -> u64 {
        self.checked_rows
    }
    /// Returns cumulative changed rows.
    #[must_use]
    pub const fn changed_rows(&self) -> u64 {
        self.changed_rows
    }
    /// Returns committed batch count.
    #[must_use]
    pub const fn batch_count(&self) -> u64 {
        self.batch_count
    }
    /// Returns the frozen application frontier.
    #[must_use]
    pub const fn frozen_application_frontier(&self) -> Option<CommitSequence> {
        self.frozen_application_frontier
    }
    /// Borrows required projections in canonical ID order.
    #[must_use]
    pub fn required_projections(&self) -> &[ProjectionId] {
        &self.required_projections
    }
    /// Returns the preceding journal hash, if present.
    #[must_use]
    pub const fn previous_hash(&self) -> Option<ContractMigrationJournalHash> {
        self.previous_hash
    }
    /// Returns this journal state's chained hash.
    #[must_use]
    pub const fn journal_hash(&self) -> ContractMigrationJournalHash {
        self.journal_hash
    }

    /// Recomputes the exact domain-separated journal hash.
    pub fn computed_hash(&self) -> Result<ContractMigrationJournalHash, StorageValueError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1_u32.to_be_bytes());
        bytes.extend_from_slice(self.database_id.as_bytes());
        bytes.extend_from_slice(self.operation_id.as_bytes());
        bytes.extend_from_slice(self.input_hash.as_bytes());
        bytes.extend_from_slice(self.artifacts.parent().as_bytes());
        bytes.extend_from_slice(self.artifacts.candidate().as_bytes());
        bytes.extend_from_slice(self.artifacts.migration().as_bytes());
        bytes.push(match self.step {
            ContractMigrationJournalStepV1::Transforming => 1,
            ContractMigrationJournalStepV1::RebuildingProjections => 2,
            ContractMigrationJournalStepV1::Validating => 3,
            ContractMigrationJournalStepV1::ReadyForCutover => 4,
            ContractMigrationJournalStepV1::Complete => 5,
        });
        match self.cursor.exclusive_lower_bound() {
            None => bytes.push(0),
            Some(target) => {
                bytes.push(1);
                bytes.extend_from_slice(&target.entity_type_id().get().to_be_bytes());
                let key = target.key().as_bytes();
                bytes.extend_from_slice(
                    &u32::try_from(key.len())
                        .map_err(|_| StorageValueError::LimitExceeded)?
                        .to_be_bytes(),
                );
                bytes.extend_from_slice(key);
            }
        }
        bytes.extend_from_slice(&self.checked_rows.to_be_bytes());
        bytes.extend_from_slice(&self.changed_rows.to_be_bytes());
        bytes.extend_from_slice(&self.batch_count.to_be_bytes());
        match self.frozen_application_frontier {
            None => bytes.push(0),
            Some(frontier) => {
                bytes.push(1);
                bytes.extend_from_slice(&frontier.get().to_be_bytes());
            }
        }
        bytes.extend_from_slice(
            &u32::try_from(self.required_projections.len())
                .map_err(|_| StorageValueError::LimitExceeded)?
                .to_be_bytes(),
        );
        for projection in &self.required_projections {
            bytes.extend_from_slice(&projection.get().to_be_bytes());
        }
        match self.previous_hash {
            None => bytes.push(0),
            Some(hash) => {
                bytes.push(1);
                bytes.extend_from_slice(hash.as_bytes());
            }
        }
        Ok(riffdb_types::hash_contract_migration_journal(&bytes))
    }
}

/// Permanent successful migration evidence stored at cutover.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredContractMigrationRecordV1 {
    database_id: DatabaseId,
    operation_id: ContractMigrationOperationId,
    input_hash: ContractMigrationInputHash,
    artifacts: ContractMigrationArtifactsV1,
    operation_artifacts: ContractMigrationOperationArtifactsV1,
    source_backup_name: riffdb_types::BackupNameV1,
    source_backup_manifest: BackupIntegrityChecksumV1,
    principal: AuditPrincipalV1,
    approval_id: Option<ApprovalId>,
    predecessor_frontier: Option<CommitSequence>,
    successor_frontier: Option<CommitSequence>,
    checked_rows: u64,
    changed_rows: u64,
    batch_count: u64,
    validation_digest: ContractMigrationValidationDigest,
    administration_sequence: AdministrationSequence,
}

impl StoredContractMigrationRecordV1 {
    /// Constructs complete terminal migration evidence.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_id: DatabaseId,
        operation_id: ContractMigrationOperationId,
        input_hash: ContractMigrationInputHash,
        artifacts: ContractMigrationArtifactsV1,
        operation_artifacts: ContractMigrationOperationArtifactsV1,
        source_backup_name: riffdb_types::BackupNameV1,
        source_backup_manifest: BackupIntegrityChecksumV1,
        principal: AuditPrincipalV1,
        approval_id: Option<ApprovalId>,
        predecessor_frontier: Option<CommitSequence>,
        successor_frontier: Option<CommitSequence>,
        checked_rows: u64,
        changed_rows: u64,
        batch_count: u64,
        validation_digest: ContractMigrationValidationDigest,
        administration_sequence: AdministrationSequence,
    ) -> Result<Self, StorageValueError> {
        if changed_rows > checked_rows
            || batch_count == 0
            || predecessor_frontier != successor_frontier
        {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            database_id,
            operation_id,
            input_hash,
            artifacts,
            operation_artifacts,
            source_backup_name,
            source_backup_manifest,
            principal,
            approval_id,
            predecessor_frontier,
            successor_frontier,
            checked_rows,
            changed_rows,
            batch_count,
            validation_digest,
            administration_sequence,
        })
    }

    /// Returns the database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }
    /// Returns the operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> ContractMigrationOperationId {
        self.operation_id
    }
    /// Returns the semantic input hash.
    #[must_use]
    pub const fn input_hash(&self) -> ContractMigrationInputHash {
        self.input_hash
    }
    /// Returns exact semantic artifact identities.
    #[must_use]
    pub const fn artifacts(&self) -> ContractMigrationArtifactsV1 {
        self.artifacts
    }
    /// Returns exact protected operation-file identities.
    #[must_use]
    pub const fn operation_artifacts(&self) -> ContractMigrationOperationArtifactsV1 {
        self.operation_artifacts
    }
    /// Borrows the retained backup name.
    #[must_use]
    pub const fn source_backup_name(&self) -> &riffdb_types::BackupNameV1 {
        &self.source_backup_name
    }
    /// Borrows the retained backup manifest checksum.
    #[must_use]
    pub const fn source_backup_manifest(&self) -> &BackupIntegrityChecksumV1 {
        &self.source_backup_manifest
    }
    /// Borrows the exact authorizing principal.
    #[must_use]
    pub const fn principal(&self) -> &AuditPrincipalV1 {
        &self.principal
    }
    /// Borrows an approval identity, when supplied.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }
    /// Returns the predecessor application frontier.
    #[must_use]
    pub const fn predecessor_frontier(&self) -> Option<CommitSequence> {
        self.predecessor_frontier
    }
    /// Returns the successor application frontier.
    #[must_use]
    pub const fn successor_frontier(&self) -> Option<CommitSequence> {
        self.successor_frontier
    }
    /// Returns checked row count.
    #[must_use]
    pub const fn checked_rows(&self) -> u64 {
        self.checked_rows
    }
    /// Returns changed row count.
    #[must_use]
    pub const fn changed_rows(&self) -> u64 {
        self.changed_rows
    }
    /// Returns committed batch count.
    #[must_use]
    pub const fn batch_count(&self) -> u64 {
        self.batch_count
    }
    /// Returns the terminal validation digest.
    #[must_use]
    pub const fn validation_digest(&self) -> ContractMigrationValidationDigest {
        self.validation_digest
    }
    /// Returns the sole cutover administration sequence.
    #[must_use]
    pub const fn administration_sequence(&self) -> AdministrationSequence {
        self.administration_sequence
    }
}

/// Durable rejection fence for one retired predecessor writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoredContractWriteRetirementV1 {
    artifacts: ContractMigrationArtifactsV1,
    operation_id: ContractMigrationOperationId,
    administration_sequence: AdministrationSequence,
}

/// Exact permanent evidence authorizing one migrated catalog lineage edge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredContractMigrationEdgeV1 {
    retirement: StoredContractWriteRetirementV1,
    migration: StoredContractMigrationRecordV1,
}

impl StoredContractMigrationEdgeV1 {
    /// Joins the predecessor-keyed fence to its operation-keyed migration record.
    pub fn new(
        retirement: StoredContractWriteRetirementV1,
        migration: StoredContractMigrationRecordV1,
    ) -> Result<Self, StorageValueError> {
        if retirement.operation_id() != migration.operation_id()
            || retirement.artifacts() != migration.artifacts()
            || retirement.administration_sequence() != migration.administration_sequence()
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            retirement,
            migration,
        })
    }

    /// Returns the predecessor-keyed write fence.
    #[must_use]
    pub const fn retirement(&self) -> StoredContractWriteRetirementV1 {
        self.retirement
    }

    /// Borrows the complete permanent migration record.
    #[must_use]
    pub const fn migration(&self) -> &StoredContractMigrationRecordV1 {
        &self.migration
    }
}

impl StoredContractWriteRetirementV1 {
    /// Constructs exact predecessor retirement evidence.
    #[must_use]
    pub const fn new(
        artifacts: ContractMigrationArtifactsV1,
        operation_id: ContractMigrationOperationId,
        administration_sequence: AdministrationSequence,
    ) -> Self {
        Self {
            artifacts,
            operation_id,
            administration_sequence,
        }
    }
    /// Returns the semantic artifact identities.
    #[must_use]
    pub const fn artifacts(self) -> ContractMigrationArtifactsV1 {
        self.artifacts
    }
    /// Returns the operation identity.
    #[must_use]
    pub const fn operation_id(self) -> ContractMigrationOperationId {
        self.operation_id
    }
    /// Returns the cutover administration sequence.
    #[must_use]
    pub const fn administration_sequence(self) -> AdministrationSequence {
        self.administration_sequence
    }
}

/// A predecessor entity retained outside active namespaces.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredRetiredEntityRecordV1 {
    operation_id: ContractMigrationOperationId,
    migration: MigrationBundleHash,
    original_target: crate::EntityTarget,
    original_entity_envelope: Vec<u8>,
}

impl StoredRetiredEntityRecordV1 {
    /// Constructs a bounded retained predecessor record.
    pub fn new(
        operation_id: ContractMigrationOperationId,
        migration: MigrationBundleHash,
        original_target: crate::EntityTarget,
        original_entity_envelope: Vec<u8>,
    ) -> Result<Self, StorageValueError> {
        if original_entity_envelope.is_empty()
            || original_entity_envelope.len() > crate::MAX_DURABLE_ENCODED_CONTENT_BYTES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            operation_id,
            migration,
            original_target,
            original_entity_envelope,
        })
    }
    /// Returns the operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> ContractMigrationOperationId {
        self.operation_id
    }
    /// Returns the migration bundle identity.
    #[must_use]
    pub const fn migration(&self) -> MigrationBundleHash {
        self.migration
    }
    /// Borrows the original canonical target.
    #[must_use]
    pub const fn original_target(&self) -> &crate::EntityTarget {
        &self.original_target
    }
    /// Borrows the exact original entity envelope.
    #[must_use]
    pub fn original_entity_envelope(&self) -> &[u8] {
        &self.original_entity_envelope
    }
}

/// Closed external migration operation phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractMigrationReceiptPhaseV1 {
    /// The immutable operation artifacts and initial receipt are durable.
    Accepted,
    /// The selected database is draining.
    Draining,
    /// Complete read-only preflight is running.
    Preflight,
    /// The immutable operation backup is durable.
    BackupPublished,
    /// The protected stage is being materialized.
    Staging,
    /// Bounded row and index transforms are running.
    Transforming,
    /// Fresh projection generations are rebuilding.
    RebuildingProjections,
    /// The complete private stage is validating.
    ValidatingStage,
    /// The validated stage is being atomically published.
    Publishing,
    /// The published database is undergoing fresh validation.
    ValidatingPublished,
    /// Automatic rollback is restoring the operation backup.
    RollingBack,
    /// The migration completed and freshly validated.
    Succeeded,
    /// The operation failed before publication.
    FailedClosed,
    /// Published failure was automatically rolled back and validated.
    FailedRolledBack,
}

impl ContractMigrationReceiptPhaseV1 {
    /// Returns whether no further transition is permitted.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::FailedClosed | Self::FailedRolledBack
        )
    }
}

/// Value-free closed external failure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractMigrationReceiptFailureV1 {
    /// Protected artifact identity did not match.
    ArtifactMismatch,
    /// Predecessor state failed semantic validation.
    InvalidPredecessor,
    /// An unresolved retiring-version admission exists.
    PendingAdmission,
    /// A checked count, sequence, or version is exhausted.
    CapacityExhausted,
    /// Conservative disk requirements are unavailable.
    DiskUnavailable,
    /// Protected stage evidence is corrupt.
    StageCorrupt,
    /// Publication durability is uncertain.
    PublicationUncertain,
    /// Fresh validation rejected the published target.
    PublishedValidationFailed,
    /// Automatic rollback could not be proven complete.
    RollbackFailed,
}

/// One checked phase transition and optional terminal failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContractMigrationReceiptTransitionV1 {
    phase: ContractMigrationReceiptPhaseV1,
    failure: Option<ContractMigrationReceiptFailureV1>,
}

/// Exact authorization and request evidence retained before migration drain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractMigrationAdmissionV1 {
    principal: AuditPrincipalV1,
    approval_id: Option<ApprovalId>,
    request_id: RequestId,
    accepted_at: Timestamp,
    ingress: ServiceIngressKindV1,
}

impl ContractMigrationAdmissionV1 {
    /// Constructs the complete restart-stable admission evidence.
    #[must_use]
    pub const fn new(
        principal: AuditPrincipalV1,
        approval_id: Option<ApprovalId>,
        request_id: RequestId,
        accepted_at: Timestamp,
        ingress: ServiceIngressKindV1,
    ) -> Self {
        Self {
            principal,
            approval_id,
            request_id,
            accepted_at,
            ingress,
        }
    }

    /// Borrows the exact authenticated principal and capability revision.
    #[must_use]
    pub const fn principal(&self) -> &AuditPrincipalV1 {
        &self.principal
    }

    /// Borrows the optional human approval identity.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }

    /// Returns the original request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the server-supplied acceptance timestamp.
    #[must_use]
    pub const fn accepted_at(&self) -> Timestamp {
        self.accepted_at
    }

    /// Returns the authenticated ingress classification.
    #[must_use]
    pub const fn ingress(&self) -> ServiceIngressKindV1 {
        self.ingress
    }
}

impl ContractMigrationReceiptTransitionV1 {
    /// Constructs a non-failure phase transition.
    #[must_use]
    pub const fn phase(phase: ContractMigrationReceiptPhaseV1) -> Self {
        Self {
            phase,
            failure: None,
        }
    }
    /// Constructs a terminal failure transition.
    #[must_use]
    pub const fn failed(
        phase: ContractMigrationReceiptPhaseV1,
        failure: ContractMigrationReceiptFailureV1,
    ) -> Self {
        Self {
            phase,
            failure: Some(failure),
        }
    }
    /// Returns the phase.
    #[must_use]
    pub const fn receipt_phase(self) -> ContractMigrationReceiptPhaseV1 {
        self.phase
    }
    /// Returns the safe failure classification.
    #[must_use]
    pub const fn failure(self) -> Option<ContractMigrationReceiptFailureV1> {
        self.failure
    }
}

/// Checksummed external state for one accepted migration operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractMigrationReceiptV1 {
    database_id: DatabaseId,
    operation_id: ContractMigrationOperationId,
    input_hash: ContractMigrationInputHash,
    artifacts: ContractMigrationArtifactsV1,
    operation_artifacts: ContractMigrationOperationArtifactsV1,
    admission: ContractMigrationAdmissionV1,
    backup_name: Option<riffdb_types::BackupNameV1>,
    backup_manifest: Option<OfflineBackupManifestIdentityV1>,
    stage_identity: Option<[u8; 32]>,
    transitions: Vec<ContractMigrationReceiptTransitionV1>,
}

impl ContractMigrationReceiptV1 {
    /// Reconstructs a canonical receipt after validating the closed phase graph.
    #[allow(clippy::too_many_arguments)]
    pub fn from_canonical_parts(
        database_id: DatabaseId,
        operation_id: ContractMigrationOperationId,
        input_hash: ContractMigrationInputHash,
        artifacts: ContractMigrationArtifactsV1,
        operation_artifacts: ContractMigrationOperationArtifactsV1,
        admission: ContractMigrationAdmissionV1,
        backup_name: Option<riffdb_types::BackupNameV1>,
        backup_manifest: Option<OfflineBackupManifestIdentityV1>,
        stage_identity: Option<[u8; 32]>,
        transitions: Vec<ContractMigrationReceiptTransitionV1>,
    ) -> Result<Self, StorageValueError> {
        if transitions.is_empty()
            || transitions.len() > MAX_CONTRACT_MIGRATION_RECEIPT_TRANSITIONS_V1
            || transitions[0].receipt_phase() != ContractMigrationReceiptPhaseV1::Accepted
            || backup_name.is_some() != backup_manifest.is_some()
            || backup_manifest
                .as_ref()
                .is_some_and(|manifest| manifest.database_id() != database_id)
        {
            return Err(StorageValueError::InvalidShape);
        }
        for pair in transitions.windows(2) {
            if !valid_receipt_successor(pair[0].receipt_phase(), pair[1].receipt_phase()) {
                return Err(StorageValueError::InvalidShape);
            }
        }
        for transition in &transitions {
            let failure_phase = matches!(
                transition.receipt_phase(),
                ContractMigrationReceiptPhaseV1::FailedClosed
                    | ContractMigrationReceiptPhaseV1::FailedRolledBack
            );
            if failure_phase != transition.failure().is_some() {
                return Err(StorageValueError::InvalidShape);
            }
        }
        let current = transitions
            .last()
            .expect("a canonical receipt is nonempty")
            .receipt_phase();
        let after_backup = !matches!(
            current,
            ContractMigrationReceiptPhaseV1::Accepted
                | ContractMigrationReceiptPhaseV1::Draining
                | ContractMigrationReceiptPhaseV1::Preflight
                | ContractMigrationReceiptPhaseV1::FailedClosed
        );
        if after_backup && backup_name.is_none() {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            database_id,
            operation_id,
            input_hash,
            artifacts,
            operation_artifacts,
            admission,
            backup_name,
            backup_manifest,
            stage_identity,
            transitions,
        })
    }

    /// Returns the database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }
    /// Returns the caller-stable operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> ContractMigrationOperationId {
        self.operation_id
    }
    /// Returns the canonical semantic input hash.
    #[must_use]
    pub const fn input_hash(&self) -> ContractMigrationInputHash {
        self.input_hash
    }
    /// Returns semantic artifact identities.
    #[must_use]
    pub const fn artifacts(&self) -> ContractMigrationArtifactsV1 {
        self.artifacts
    }
    /// Returns protected operation-file identities.
    #[must_use]
    pub const fn operation_artifacts(&self) -> ContractMigrationOperationArtifactsV1 {
        self.operation_artifacts
    }
    /// Borrows exact restart-stable authorization and request evidence.
    #[must_use]
    pub const fn admission(&self) -> &ContractMigrationAdmissionV1 {
        &self.admission
    }
    /// Borrows the retained backup name when published.
    #[must_use]
    pub const fn backup_name(&self) -> Option<&riffdb_types::BackupNameV1> {
        self.backup_name.as_ref()
    }
    /// Borrows the exact backup manifest identity.
    #[must_use]
    pub const fn backup_manifest(&self) -> Option<&OfflineBackupManifestIdentityV1> {
        self.backup_manifest.as_ref()
    }
    /// Returns the protected stage identity.
    #[must_use]
    pub const fn stage_identity(&self) -> Option<[u8; 32]> {
        self.stage_identity
    }
    /// Borrows canonical phase transitions.
    #[must_use]
    pub fn transitions(&self) -> &[ContractMigrationReceiptTransitionV1] {
        &self.transitions
    }
    /// Returns the current phase.
    #[must_use]
    pub fn current_phase(&self) -> ContractMigrationReceiptPhaseV1 {
        self.transitions
            .last()
            .expect("a canonical receipt is nonempty")
            .receipt_phase()
    }

    /// Appends one checked non-failure phase transition without changing evidence.
    pub fn advance(
        &self,
        phase: ContractMigrationReceiptPhaseV1,
    ) -> Result<Self, StorageValueError> {
        self.advance_with(
            ContractMigrationReceiptTransitionV1::phase(phase),
            None,
            None,
        )
    }

    /// Binds the immutable automatic backup while advancing to `BackupPublished`.
    pub fn publish_backup(
        &self,
        backup_name: riffdb_types::BackupNameV1,
        backup_manifest: OfflineBackupManifestIdentityV1,
    ) -> Result<Self, StorageValueError> {
        self.advance_with(
            ContractMigrationReceiptTransitionV1::phase(
                ContractMigrationReceiptPhaseV1::BackupPublished,
            ),
            Some((backup_name, backup_manifest)),
            None,
        )
    }

    /// Binds the protected sibling stage while advancing to `Transforming`.
    pub fn begin_transforming(&self, stage_identity: [u8; 32]) -> Result<Self, StorageValueError> {
        self.advance_with(
            ContractMigrationReceiptTransitionV1::phase(
                ContractMigrationReceiptPhaseV1::Transforming,
            ),
            None,
            Some(stage_identity),
        )
    }

    /// Appends one terminal failure transition with its closed safe classification.
    pub fn fail(
        &self,
        phase: ContractMigrationReceiptPhaseV1,
        failure: ContractMigrationReceiptFailureV1,
    ) -> Result<Self, StorageValueError> {
        self.advance_with(
            ContractMigrationReceiptTransitionV1::failed(phase, failure),
            None,
            None,
        )
    }

    fn advance_with(
        &self,
        transition: ContractMigrationReceiptTransitionV1,
        backup: Option<(riffdb_types::BackupNameV1, OfflineBackupManifestIdentityV1)>,
        stage_identity: Option<[u8; 32]>,
    ) -> Result<Self, StorageValueError> {
        if self.current_phase().is_terminal()
            || backup.is_some() && self.backup_name.is_some()
            || stage_identity.is_some() && self.stage_identity.is_some()
        {
            return Err(StorageValueError::InvalidShape);
        }
        let mut transitions = self.transitions.clone();
        transitions.push(transition);
        let (backup_name, backup_manifest) = backup.map_or_else(
            || (self.backup_name.clone(), self.backup_manifest.clone()),
            |(name, manifest)| (Some(name), Some(manifest)),
        );
        Self::from_canonical_parts(
            self.database_id,
            self.operation_id,
            self.input_hash,
            self.artifacts,
            self.operation_artifacts,
            self.admission.clone(),
            backup_name,
            backup_manifest,
            stage_identity.or(self.stage_identity),
            transitions,
        )
    }
}

const fn valid_receipt_successor(
    current: ContractMigrationReceiptPhaseV1,
    next: ContractMigrationReceiptPhaseV1,
) -> bool {
    use ContractMigrationReceiptPhaseV1 as P;
    matches!(
        (current, next),
        (P::Accepted, P::Draining | P::FailedClosed)
            | (P::Draining, P::Preflight | P::FailedClosed)
            | (P::Preflight, P::BackupPublished | P::FailedClosed)
            | (P::BackupPublished, P::Staging | P::FailedClosed)
            | (P::Staging, P::Transforming | P::FailedClosed)
            | (
                P::Transforming,
                P::Transforming | P::RebuildingProjections | P::FailedClosed
            )
            | (
                P::RebuildingProjections,
                P::RebuildingProjections | P::ValidatingStage | P::FailedClosed
            )
            | (P::ValidatingStage, P::Publishing | P::FailedClosed)
            | (P::Publishing, P::ValidatingPublished | P::RollingBack)
            | (P::ValidatingPublished, P::Succeeded | P::RollingBack)
            | (P::RollingBack, P::FailedRolledBack)
    )
}
