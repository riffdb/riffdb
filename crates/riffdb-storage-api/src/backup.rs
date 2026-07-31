//! Checked value-level metadata for backend-private offline backup operations.
//!
//! This module defines no backup/restore handle, filesystem path, generic byte
//! sink, or on-disk encoding. ADR-0004 keeps execution handles private to the
//! concrete adapter; WP-070 owns the offline artifact and manifest encoding.

use std::num::NonZeroU32;

use riffdb_types::{
    ActorId, ActorKind, ApprovalId, BackupNameV1, CapabilityId, CommitSequence, ContractBundleHash,
    ContractLineage, ContractVersion, DatabaseId, OfflineMaintenanceInputHash,
    OfflineMaintenanceOperationId, OfflineMaintenanceOperationKind,
    OfflineMaintenanceReplacementConfirmation, offline_maintenance_input_hash,
};

use crate::{ActiveCatalogPointerV1, StorageError, StorageFormatVersion, StorageValueError};

/// Maximum bytes in one build-metadata scalar.
pub const MAX_BACKUP_BUILD_VALUE_BYTES: usize = 256;
/// Maximum enabled feature names retained in build metadata.
pub const MAX_BACKUP_BUILD_FEATURES: usize = 128;
/// Maximum bytes in one enabled feature name.
pub const MAX_BACKUP_BUILD_FEATURE_BYTES: usize = 128;
/// Maximum catalog bundle descriptors retained in one manifest.
pub const MAX_BACKUP_CATALOG_BUNDLES: usize = 65_535;
/// Maximum artifact checksums retained in one manifest.
pub const MAX_BACKUP_ARTIFACT_CHECKSUMS: usize = 65_535;
/// Maximum opaque adapter-owned bytes in one integrity checksum value.
pub const MAX_BACKUP_INTEGRITY_CHECKSUM_BYTES: usize = 256;
/// Maximum phase-history entries in one external maintenance receipt.
pub const MAX_OFFLINE_MAINTENANCE_RECEIPT_TRANSITIONS_V1: usize = 16;
/// Maximum receipts accepted in one bounded startup inventory.
pub const MAX_OFFLINE_MAINTENANCE_RECEIPTS_V1: usize = 65_535;

/// The admitted identity retained by one external maintenance receipt.
///
/// This is the narrow redacted audit identity required by ADR-0050. It contains
/// no bearer, token digest, policy object, filesystem path, or reusable
/// authorization decision.
#[derive(Clone, Eq, PartialEq)]
pub struct OfflineMaintenanceAdmissionV1 {
    principal_id: ActorId,
    actor_kind: ActorKind,
    capability_id: CapabilityId,
    approval_id: Option<ApprovalId>,
}

impl OfflineMaintenanceAdmissionV1 {
    /// Captures the exact admitted principal, capability, and applicable approval.
    #[must_use]
    pub const fn new(
        principal_id: ActorId,
        actor_kind: ActorKind,
        capability_id: CapabilityId,
        approval_id: Option<ApprovalId>,
    ) -> Self {
        Self {
            principal_id,
            actor_kind,
            capability_id,
            approval_id,
        }
    }

    /// Borrows the admitted principal identity.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Returns the admitted actor classification.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Returns the authorizing capability identity.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Borrows the validated approval identity, when applicable.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }
}

impl std::fmt::Debug for OfflineMaintenanceAdmissionV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("OfflineMaintenanceAdmissionV1([REDACTED])")
    }
}

/// The SHA-256 identity of one exact WP-070 manifest and its semantic frontier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfflineBackupManifestIdentityV1 {
    manifest_checksum: BackupIntegrityChecksumV1,
    database_id: DatabaseId,
    included_application_frontier: Option<CommitSequence>,
}

impl OfflineBackupManifestIdentityV1 {
    /// Binds exact manifest bytes to the database identity and included frontier.
    #[must_use]
    pub const fn new(
        manifest_checksum: BackupIntegrityChecksumV1,
        database_id: DatabaseId,
        included_application_frontier: Option<CommitSequence>,
    ) -> Self {
        Self {
            manifest_checksum,
            database_id,
            included_application_frontier,
        }
    }

    /// Borrows the concrete adapter-owned manifest checksum.
    #[must_use]
    pub const fn manifest_checksum(&self) -> &BackupIntegrityChecksumV1 {
        &self.manifest_checksum
    }

    /// Returns the database identity encoded by the manifest.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the last included application commit, when any.
    #[must_use]
    pub const fn included_application_frontier(&self) -> Option<CommitSequence> {
        self.included_application_frontier
    }
}

/// Closed durable phase of one external offline-maintenance receipt.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum OfflineMaintenanceReceiptPhaseV1 {
    /// The operation identity and admitted actor are durable.
    Accepted,
    /// Ordinary work is being drained under the fixed deadline.
    Draining,
    /// The database and all protected ports are offline.
    Offline,
    /// The immutable backup or restored target has been published.
    ArtifactPublished,
    /// Authoritative post-publication validation is in progress.
    Validating,
    /// Validation completed and the terminal receipt is durable.
    Succeeded,
    /// The operation stopped without claiming success.
    FailedClosed,
}

impl OfflineMaintenanceReceiptPhaseV1 {
    /// Returns the stable receipt-v1 encoding tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Accepted => 0x01,
            Self::Draining => 0x02,
            Self::Offline => 0x03,
            Self::ArtifactPublished => 0x04,
            Self::Validating => 0x05,
            Self::Succeeded => 0x06,
            Self::FailedClosed => 0x07,
        }
    }

    /// Decodes one receipt-v1 tag, rejecting zero and unknown values.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::Accepted),
            0x02 => Some(Self::Draining),
            0x03 => Some(Self::Offline),
            0x04 => Some(Self::ArtifactPublished),
            0x05 => Some(Self::Validating),
            0x06 => Some(Self::Succeeded),
            0x07 => Some(Self::FailedClosed),
            _ => None,
        }
    }

    /// Returns whether this phase closes the operation.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::FailedClosed)
    }
}

/// Closed safe failure retained by a failed-closed maintenance receipt.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum OfflineMaintenanceReceiptFailureV1 {
    /// Accepted work did not quiesce under the fixed drain deadline.
    QuiescenceFailed,
    /// A required immutable artifact could not be read or published.
    ArtifactUnavailable,
    /// Artifact inventory, checksum, manifest, or semantic identity was invalid.
    ArtifactInvalid,
    /// Fresh authorization against the staged database failed.
    StagedAuthorizationFailed,
    /// The concrete storage operation was unavailable.
    StorageUnavailable,
    /// Complete authoritative validation failed.
    ValidationFailed,
    /// Receipt durability could not be established.
    ReceiptUnavailable,
    /// A closed internal failure prevented safe continuation.
    InternalFailure,
}

impl OfflineMaintenanceReceiptFailureV1 {
    /// Returns the stable receipt-v1 encoding tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::QuiescenceFailed => 0x01,
            Self::ArtifactUnavailable => 0x02,
            Self::ArtifactInvalid => 0x03,
            Self::StagedAuthorizationFailed => 0x04,
            Self::StorageUnavailable => 0x05,
            Self::ValidationFailed => 0x06,
            Self::ReceiptUnavailable => 0x07,
            Self::InternalFailure => 0x08,
        }
    }

    /// Decodes one receipt-v1 tag, rejecting zero and unknown values.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::QuiescenceFailed),
            0x02 => Some(Self::ArtifactUnavailable),
            0x03 => Some(Self::ArtifactInvalid),
            0x04 => Some(Self::StagedAuthorizationFailed),
            0x05 => Some(Self::StorageUnavailable),
            0x06 => Some(Self::ValidationFailed),
            0x07 => Some(Self::ReceiptUnavailable),
            0x08 => Some(Self::InternalFailure),
            _ => None,
        }
    }
}

/// One checked append-only phase-history entry.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OfflineMaintenanceReceiptTransitionV1 {
    phase: OfflineMaintenanceReceiptPhaseV1,
    failure: Option<OfflineMaintenanceReceiptFailureV1>,
}

impl OfflineMaintenanceReceiptTransitionV1 {
    /// Constructs a nonterminal or successful transition.
    #[must_use]
    pub const fn phase(phase: OfflineMaintenanceReceiptPhaseV1) -> Self {
        Self {
            phase,
            failure: None,
        }
    }

    /// Constructs the one terminal failed-closed transition.
    #[must_use]
    pub const fn failed(failure: OfflineMaintenanceReceiptFailureV1) -> Self {
        Self {
            phase: OfflineMaintenanceReceiptPhaseV1::FailedClosed,
            failure: Some(failure),
        }
    }

    /// Returns the closed phase.
    #[must_use]
    pub const fn receipt_phase(self) -> OfflineMaintenanceReceiptPhaseV1 {
        self.phase
    }

    /// Returns the safe terminal failure, when failed closed.
    #[must_use]
    pub const fn failure(self) -> Option<OfflineMaintenanceReceiptFailureV1> {
        self.failure
    }

    fn has_valid_shape(self) -> bool {
        matches!(
            (self.phase, self.failure),
            (OfflineMaintenanceReceiptPhaseV1::FailedClosed, Some(_))
                | (
                    OfflineMaintenanceReceiptPhaseV1::Accepted
                        | OfflineMaintenanceReceiptPhaseV1::Draining
                        | OfflineMaintenanceReceiptPhaseV1::Offline
                        | OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
                        | OfflineMaintenanceReceiptPhaseV1::Validating
                        | OfflineMaintenanceReceiptPhaseV1::Succeeded,
                    None
                )
        )
    }
}

/// One complete checked semantic maintenance receipt.
///
/// The concrete adapter owns its external encoding and checksum. This value
/// owns only receipt semantics and can never contain a path or credential.
#[derive(Clone, Eq, PartialEq)]
pub struct OfflineMaintenanceReceiptV1 {
    operation_id: OfflineMaintenanceOperationId,
    operation_kind: OfflineMaintenanceOperationKind,
    backup_name: BackupNameV1,
    input_hash: OfflineMaintenanceInputHash,
    replacement_confirmation: OfflineMaintenanceReplacementConfirmation,
    admission: OfflineMaintenanceAdmissionV1,
    source_database_id: Option<DatabaseId>,
    staged_database_id: Option<DatabaseId>,
    manifest_identity: Option<OfflineBackupManifestIdentityV1>,
    /// History incarnation stamped onto the restored database after publication.
    published_history_incarnation: Option<u64>,
    transitions: Vec<OfflineMaintenanceReceiptTransitionV1>,
}

impl OfflineMaintenanceReceiptV1 {
    /// Creates the first accepted receipt after checking its semantic input.
    pub fn accepted(
        operation_id: OfflineMaintenanceOperationId,
        operation_kind: OfflineMaintenanceOperationKind,
        backup_name: BackupNameV1,
        input_hash: OfflineMaintenanceInputHash,
        replacement_confirmation: OfflineMaintenanceReplacementConfirmation,
        admission: OfflineMaintenanceAdmissionV1,
    ) -> Result<Self, StorageValueError> {
        Self::from_canonical_parts(
            operation_id,
            operation_kind,
            backup_name,
            input_hash,
            replacement_confirmation,
            admission,
            None,
            None,
            None,
            None,
            vec![OfflineMaintenanceReceiptTransitionV1::phase(
                OfflineMaintenanceReceiptPhaseV1::Accepted,
            )],
        )
    }

    /// Reconstructs one complete receipt through the same semantic validator.
    #[allow(clippy::too_many_arguments)]
    pub fn from_canonical_parts(
        operation_id: OfflineMaintenanceOperationId,
        operation_kind: OfflineMaintenanceOperationKind,
        backup_name: BackupNameV1,
        input_hash: OfflineMaintenanceInputHash,
        replacement_confirmation: OfflineMaintenanceReplacementConfirmation,
        admission: OfflineMaintenanceAdmissionV1,
        source_database_id: Option<DatabaseId>,
        staged_database_id: Option<DatabaseId>,
        manifest_identity: Option<OfflineBackupManifestIdentityV1>,
        published_history_incarnation: Option<u64>,
        transitions: Vec<OfflineMaintenanceReceiptTransitionV1>,
    ) -> Result<Self, StorageValueError> {
        if offline_maintenance_input_hash(operation_kind, &backup_name, replacement_confirmation)
            != input_hash
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        if operation_kind == OfflineMaintenanceOperationKind::CreateBackup
            && replacement_confirmation != OfflineMaintenanceReplacementConfirmation::NotProvided
        {
            return Err(StorageValueError::InvalidShape);
        }
        if published_history_incarnation.is_some_and(|value| value < 1) {
            return Err(StorageValueError::InvalidShape);
        }
        if published_history_incarnation.is_some()
            && operation_kind != OfflineMaintenanceOperationKind::RestoreBackup
        {
            return Err(StorageValueError::InvalidShape);
        }
        validate_receipt_history(operation_kind, source_database_id, &transitions)?;
        validate_receipt_evidence(
            operation_kind,
            source_database_id,
            staged_database_id,
            manifest_identity.as_ref(),
            transitions
                .last()
                .copied()
                .ok_or(StorageValueError::Empty)?
                .receipt_phase(),
        )?;
        Ok(Self {
            operation_id,
            operation_kind,
            backup_name,
            input_hash,
            replacement_confirmation,
            admission,
            source_database_id,
            staged_database_id,
            manifest_identity,
            published_history_incarnation,
            transitions,
        })
    }

    /// Returns the caller-stable operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> OfflineMaintenanceOperationId {
        self.operation_id
    }

    /// Returns the closed operation kind.
    #[must_use]
    pub const fn operation_kind(&self) -> OfflineMaintenanceOperationKind {
        self.operation_kind
    }

    /// Borrows the checked immutable backup name.
    #[must_use]
    pub const fn backup_name(&self) -> &BackupNameV1 {
        &self.backup_name
    }

    /// Returns the stable canonical semantic-input hash.
    #[must_use]
    pub const fn input_hash(&self) -> OfflineMaintenanceInputHash {
        self.input_hash
    }

    /// Returns the caller's exact replacement confirmation.
    #[must_use]
    pub const fn replacement_confirmation(&self) -> OfflineMaintenanceReplacementConfirmation {
        self.replacement_confirmation
    }

    /// Borrows the narrow admitted audit identity.
    #[must_use]
    pub const fn admission(&self) -> &OfflineMaintenanceAdmissionV1 {
        &self.admission
    }

    /// Returns the healthy source database identity, when one was established.
    #[must_use]
    pub const fn source_database_id(&self) -> Option<DatabaseId> {
        self.source_database_id
    }

    /// Returns the independently validated staged database identity, when any.
    #[must_use]
    pub const fn staged_database_id(&self) -> Option<DatabaseId> {
        self.staged_database_id
    }

    /// Borrows the exact immutable backup-manifest evidence, when known.
    #[must_use]
    pub const fn manifest_identity(&self) -> Option<&OfflineBackupManifestIdentityV1> {
        self.manifest_identity.as_ref()
    }

    /// Returns the incarnation recorded for the restored published database.
    #[must_use]
    pub const fn published_history_incarnation(&self) -> Option<u64> {
        self.published_history_incarnation
    }

    /// Returns the complete bounded append-only history.
    #[must_use]
    pub fn transitions(&self) -> &[OfflineMaintenanceReceiptTransitionV1] {
        &self.transitions
    }

    /// Returns the current closed phase.
    #[must_use]
    pub fn current_phase(&self) -> OfflineMaintenanceReceiptPhaseV1 {
        self.transitions
            .last()
            .map_or(OfflineMaintenanceReceiptPhaseV1::Accepted, |entry| {
                entry.receipt_phase()
            })
    }

    /// Records the source identity once without changing it on retries.
    pub fn record_source_database_id(
        &mut self,
        database_id: DatabaseId,
    ) -> Result<(), StorageValueError> {
        let prior = self.source_database_id;
        record_once(&mut self.source_database_id, database_id)?;
        if let Err(error) = self.validate_current() {
            self.source_database_id = prior;
            return Err(error);
        }
        Ok(())
    }

    /// Records the staged identity once without changing it on retries.
    pub fn record_staged_database_id(
        &mut self,
        database_id: DatabaseId,
    ) -> Result<(), StorageValueError> {
        if self.operation_kind != OfflineMaintenanceOperationKind::RestoreBackup {
            return Err(StorageValueError::InvalidShape);
        }
        let prior = self.staged_database_id;
        record_once(&mut self.staged_database_id, database_id)?;
        if let Err(error) = self.validate_current() {
            self.staged_database_id = prior;
            return Err(error);
        }
        Ok(())
    }

    /// Records exact immutable manifest evidence once.
    pub fn record_manifest_identity(
        &mut self,
        identity: OfflineBackupManifestIdentityV1,
    ) -> Result<(), StorageValueError> {
        let prior = self.manifest_identity.clone();
        match &self.manifest_identity {
            None => self.manifest_identity = Some(identity),
            Some(existing) if existing == &identity => {}
            Some(_) => return Err(StorageValueError::IdentityMismatch),
        }
        if let Err(error) = self.validate_current() {
            self.manifest_identity = prior;
            return Err(error);
        }
        Ok(())
    }

    /// Records the published history incarnation once (restore offline phase).
    pub fn record_published_incarnation(
        &mut self,
        incarnation: u64,
    ) -> Result<(), StorageValueError> {
        if self.operation_kind != OfflineMaintenanceOperationKind::RestoreBackup {
            return Err(StorageValueError::InvalidShape);
        }
        if incarnation < 1 {
            return Err(StorageValueError::InvalidShape);
        }
        let prior = self.published_history_incarnation;
        match self.published_history_incarnation {
            None => self.published_history_incarnation = Some(incarnation),
            Some(existing) if existing == incarnation => {}
            Some(_) => return Err(StorageValueError::IdentityMismatch),
        }
        if let Err(error) = self.validate_current() {
            self.published_history_incarnation = prior;
            return Err(error);
        }
        Ok(())
    }

    /// Appends one legal forward transition.
    pub fn advance(
        &mut self,
        transition: OfflineMaintenanceReceiptTransitionV1,
    ) -> Result<(), StorageValueError> {
        if self.transitions.len() == MAX_OFFLINE_MAINTENANCE_RECEIPT_TRANSITIONS_V1 {
            return Err(StorageValueError::LimitExceeded);
        }
        let prior = self
            .transitions
            .last()
            .copied()
            .ok_or(StorageValueError::Empty)?;
        if !valid_receipt_transition(
            self.operation_kind,
            self.source_database_id,
            prior.receipt_phase(),
            transition,
        ) {
            return Err(StorageValueError::InvalidShape);
        }
        self.transitions.push(transition);
        if let Err(error) = self.validate_current() {
            self.transitions.pop();
            return Err(error);
        }
        Ok(())
    }

    /// Returns whether this value is the same receipt or a valid monotonic extension.
    #[must_use]
    pub fn monotonically_extends(&self, prior: &Self) -> bool {
        self.operation_id == prior.operation_id
            && self.operation_kind == prior.operation_kind
            && self.backup_name == prior.backup_name
            && self.input_hash == prior.input_hash
            && self.replacement_confirmation == prior.replacement_confirmation
            && self.admission == prior.admission
            && option_extends(prior.source_database_id, self.source_database_id)
            && option_extends(prior.staged_database_id, self.staged_database_id)
            && option_ref_extends(
                prior.manifest_identity.as_ref(),
                self.manifest_identity.as_ref(),
            )
            && option_extends(
                prior.published_history_incarnation,
                self.published_history_incarnation,
            )
            && self.transitions.starts_with(&prior.transitions)
    }

    fn validate_current(&self) -> Result<(), StorageValueError> {
        validate_receipt_history(
            self.operation_kind,
            self.source_database_id,
            &self.transitions,
        )?;
        validate_receipt_evidence(
            self.operation_kind,
            self.source_database_id,
            self.staged_database_id,
            self.manifest_identity.as_ref(),
            self.current_phase(),
        )
    }
}

impl std::fmt::Debug for OfflineMaintenanceReceiptV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OfflineMaintenanceReceiptV1")
            .field("operation_id", &self.operation_id)
            .field("operation_kind", &self.operation_kind)
            .field("phase", &self.current_phase())
            .field("details", &"[REDACTED]")
            .finish()
    }
}

/// Bounded canonical startup inventory of all external maintenance receipts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfflineMaintenanceReceiptInventoryV1(Vec<OfflineMaintenanceReceiptV1>);

impl OfflineMaintenanceReceiptInventoryV1 {
    /// Sorts by operation ID and rejects duplicate or excessive receipts.
    pub fn new(mut receipts: Vec<OfflineMaintenanceReceiptV1>) -> Result<Self, StorageValueError> {
        if receipts.len() > MAX_OFFLINE_MAINTENANCE_RECEIPTS_V1 {
            return Err(StorageValueError::LimitExceeded);
        }
        receipts.sort_by_key(OfflineMaintenanceReceiptV1::operation_id);
        if receipts
            .windows(2)
            .any(|pair| pair[0].operation_id == pair[1].operation_id)
        {
            return Err(StorageValueError::Duplicate);
        }
        Ok(Self(receipts))
    }

    /// Borrows receipts in canonical operation-ID order.
    #[must_use]
    pub fn receipts(&self) -> &[OfflineMaintenanceReceiptV1] {
        &self.0
    }
}

/// Result of atomically creating one operation receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OfflineMaintenanceReceiptCreateResultV1 {
    /// The candidate became the first durable receipt for this operation.
    Created,
    /// A checked receipt with the same operation ID already exists.
    Existing(Box<OfflineMaintenanceReceiptV1>),
}

/// Result of atomically replacing one receipt with a monotonic extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfflineMaintenanceReceiptReplaceResultV1 {
    /// The exact candidate was already durable.
    AlreadyCurrent,
    /// The monotonic extension replaced its predecessor.
    Replaced,
}

/// Engine-neutral semantics for the private external maintenance receipt ledger.
pub trait OfflineMaintenanceReceiptPersistencePort {
    /// Creates the accepted receipt or returns the existing checked value.
    fn create_or_read_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV1,
    ) -> Result<OfflineMaintenanceReceiptCreateResultV1, StorageError>;

    /// Atomically installs one checked monotonic extension.
    fn replace_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV1,
    ) -> Result<OfflineMaintenanceReceiptReplaceResultV1, StorageError>;

    /// Reads one complete receipt by caller-stable operation identity.
    fn read_receipt(
        &mut self,
        operation_id: OfflineMaintenanceOperationId,
    ) -> Result<Option<OfflineMaintenanceReceiptV1>, StorageError>;

    /// Validates and returns the complete bounded receipt inventory.
    fn validate_receipt_inventory(
        &mut self,
    ) -> Result<OfflineMaintenanceReceiptInventoryV1, StorageError>;
}

/// Nonzero semantic version of the offline backup manifest.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BackupManifestVersion(NonZeroU32);

impl BackupManifestVersion {
    /// Initial POC semantic manifest version.
    pub const V1: Self = Self(NonZeroU32::MIN);

    /// Reconstructs a nonzero historical version for checked decoding.
    #[must_use]
    pub const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric semantic manifest version.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// One immutable contract bundle identity included by an offline backup.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BackupCatalogBundleV1 {
    lineage: ContractLineage,
    contract_version: ContractVersion,
    bundle_hash: ContractBundleHash,
}

impl BackupCatalogBundleV1 {
    /// Constructs an exact immutable bundle descriptor.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        contract_version: ContractVersion,
        bundle_hash: ContractBundleHash,
    ) -> Self {
        Self {
            lineage,
            contract_version,
            bundle_hash,
        }
    }

    /// Returns the exact contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the immutable contract version.
    #[must_use]
    pub const fn contract_version(&self) -> ContractVersion {
        self.contract_version
    }

    /// Returns the hash of the canonical immutable bundle.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }

    fn matches_active(&self, active: &ActiveCatalogPointerV1) -> bool {
        self.lineage == *active.lineage()
            && self.contract_version == active.contract_version()
            && self.bundle_hash == active.bundle_hash()
    }
}

/// Closed snapshot representation included by an offline backup.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum BackupSnapshotKindV1 {
    /// A complete consistent set of storage-engine data artifacts.
    StorageEngineData,
    /// A complete engine-independent logical snapshot verified by the adapter.
    VerifiedLogicalSnapshot,
}

/// Bounded adapter-owned checksum representation.
///
/// Storage API intentionally does not select a backup checksum algorithm or
/// byte encoding. The concrete offline backup format owns both and must verify
/// the exact value during restore.
#[derive(Clone, Eq, PartialEq)]
pub struct BackupIntegrityChecksumV1(Vec<u8>);

impl BackupIntegrityChecksumV1 {
    /// Constructs a nonempty bounded opaque checksum value.
    pub fn new(value: Vec<u8>) -> Result<Self, StorageValueError> {
        if value.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if value.len() > MAX_BACKUP_INTEGRITY_CHECKSUM_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self(value))
    }

    /// Borrows the complete adapter-owned checksum representation.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for BackupIntegrityChecksumV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BackupIntegrityChecksumV1")
            .field("bytes", &"[REDACTED]")
            .field("length", &self.0.len())
            .finish()
    }
}

/// One exact integrity checksum for a named backup artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupArtifactChecksumV1 {
    artifact_ordinal: NonZeroU32,
    checksum: BackupIntegrityChecksumV1,
}

impl BackupArtifactChecksumV1 {
    /// Associates one opaque checksum with its one-based snapshot artifact.
    #[must_use]
    pub const fn new(artifact_ordinal: NonZeroU32, checksum: BackupIntegrityChecksumV1) -> Self {
        Self {
            artifact_ordinal,
            checksum,
        }
    }

    /// Returns the one-based opaque artifact position in the complete snapshot.
    #[must_use]
    pub const fn artifact_ordinal(&self) -> NonZeroU32 {
        self.artifact_ordinal
    }

    /// Borrows the complete adapter-owned integrity checksum.
    #[must_use]
    pub const fn checksum(&self) -> &BackupIntegrityChecksumV1 {
        &self.checksum
    }
}

/// Bounded release/build identity retained in an offline backup manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupBuildMetadataV1 {
    semantic_version: String,
    git_revision: String,
    rust_version: String,
    executable_ir_version: u32,
    enabled_features: Vec<String>,
}

impl BackupBuildMetadataV1 {
    /// Constructs canonical build metadata from already captured values.
    pub fn new(
        semantic_version: impl Into<String>,
        git_revision: impl Into<String>,
        rust_version: impl Into<String>,
        executable_ir_version: u32,
        mut enabled_features: Vec<String>,
    ) -> Result<Self, StorageValueError> {
        let semantic_version = checked_build_value(semantic_version.into())?;
        let git_revision = checked_build_value(git_revision.into())?;
        let rust_version = checked_build_value(rust_version.into())?;
        if executable_ir_version == 0 {
            return Err(StorageValueError::InvalidShape);
        }
        if enabled_features.len() > MAX_BACKUP_BUILD_FEATURES {
            return Err(StorageValueError::LimitExceeded);
        }
        for feature in &enabled_features {
            checked_feature(feature)?;
        }
        enabled_features.sort_unstable();
        if enabled_features.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(StorageValueError::Duplicate);
        }
        Ok(Self {
            semantic_version,
            git_revision,
            rust_version,
            executable_ir_version,
            enabled_features,
        })
    }

    /// Returns the embedded RiffDB semantic version.
    #[must_use]
    pub fn semantic_version(&self) -> &str {
        &self.semantic_version
    }

    /// Returns the embedded source revision.
    #[must_use]
    pub fn git_revision(&self) -> &str {
        &self.git_revision
    }

    /// Returns the embedded Rust toolchain version.
    #[must_use]
    pub fn rust_version(&self) -> &str {
        &self.rust_version
    }

    /// Returns the executable contract-IR version without importing IR here.
    #[must_use]
    pub const fn executable_ir_version(&self) -> u32 {
        self.executable_ir_version
    }

    /// Returns enabled feature names in canonical order.
    #[must_use]
    pub fn enabled_features(&self) -> &[String] {
        &self.enabled_features
    }
}

/// Checked semantic summary for one complete offline consistent backup.
///
/// This is not the manifest byte encoding and does not make online backup or
/// point-in-time recovery available.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfflineBackupManifestV1 {
    manifest_version: BackupManifestVersion,
    storage_format_version: StorageFormatVersion,
    database_id: DatabaseId,
    snapshot_kind: BackupSnapshotKindV1,
    catalog_bundles: Vec<BackupCatalogBundleV1>,
    active_catalog: Option<ActiveCatalogPointerV1>,
    last_commit_sequence: Option<CommitSequence>,
    /// Present on post-fence backups; absent on pre-fence manifests.
    history_incarnation: Option<u64>,
    /// Whether the wire form includes the history presence tag.
    ///
    /// Post-fence encodings always include the tag (`true`): `None` is one
    /// presence byte `0`, `Some` is presence `1` + u64. Pre-fence dual-path
    /// reconstructs omit the field entirely (`false`).
    history_wire_tagged: bool,
    checksums: Vec<BackupArtifactChecksumV1>,
    build: BackupBuildMetadataV1,
    semantic_bytes: usize,
}

impl OfflineBackupManifestV1 {
    /// Validates canonical bundle/checksum inventories and active linkage.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage_format_version: StorageFormatVersion,
        database_id: DatabaseId,
        snapshot_kind: BackupSnapshotKindV1,
        mut catalog_bundles: Vec<BackupCatalogBundleV1>,
        active_catalog: Option<ActiveCatalogPointerV1>,
        last_commit_sequence: Option<CommitSequence>,
        history_incarnation: Option<u64>,
        mut checksums: Vec<BackupArtifactChecksumV1>,
        build: BackupBuildMetadataV1,
    ) -> Result<Self, StorageValueError> {
        if catalog_bundles.len() > MAX_BACKUP_CATALOG_BUNDLES
            || checksums.len() > MAX_BACKUP_ARTIFACT_CHECKSUMS
        {
            return Err(StorageValueError::LimitExceeded);
        }
        if checksums.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if active_catalog.is_none()
            && (!catalog_bundles.is_empty() || last_commit_sequence.is_some())
        {
            return Err(StorageValueError::InvalidShape);
        }
        if history_incarnation.is_some_and(|value| value < 1) {
            return Err(StorageValueError::InvalidShape);
        }

        catalog_bundles.sort_unstable();
        if catalog_bundles.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(StorageValueError::Duplicate);
        }
        if active_catalog.as_ref().is_some_and(|active| {
            !catalog_bundles
                .iter()
                .any(|bundle| bundle.matches_active(active))
        }) {
            return Err(StorageValueError::IdentityMismatch);
        }

        checksums.sort_by_key(|checksum| checksum.artifact_ordinal);
        if checksums
            .windows(2)
            .any(|pair| pair[0].artifact_ordinal == pair[1].artifact_ordinal)
        {
            return Err(StorageValueError::Duplicate);
        }

        // `new` always builds the post-fence wire form (presence-tagged field).
        let history_wire_tagged = true;
        let semantic_bytes = backup_manifest_semantic_bytes(
            &catalog_bundles,
            active_catalog.as_ref(),
            last_commit_sequence,
            history_incarnation,
            history_wire_tagged,
            &checksums,
            &build,
        )?;

        Ok(Self {
            manifest_version: BackupManifestVersion::V1,
            storage_format_version,
            database_id,
            snapshot_kind,
            catalog_bundles,
            active_catalog,
            last_commit_sequence,
            history_incarnation,
            history_wire_tagged,
            checksums,
            build,
            semantic_bytes,
        })
    }

    /// Reconstructs a pre-fence manifest whose wire form omits the history field.
    ///
    /// Used only by dual-path decode of historical backup bytes. History must be
    /// absent; size accounting matches field omission (zero bytes).
    #[allow(clippy::too_many_arguments)]
    pub fn new_pre_fence(
        storage_format_version: StorageFormatVersion,
        database_id: DatabaseId,
        snapshot_kind: BackupSnapshotKindV1,
        catalog_bundles: Vec<BackupCatalogBundleV1>,
        active_catalog: Option<ActiveCatalogPointerV1>,
        last_commit_sequence: Option<CommitSequence>,
        checksums: Vec<BackupArtifactChecksumV1>,
        build: BackupBuildMetadataV1,
    ) -> Result<Self, StorageValueError> {
        let mut candidate = Self::new(
            storage_format_version,
            database_id,
            snapshot_kind,
            catalog_bundles,
            active_catalog,
            last_commit_sequence,
            None,
            checksums,
            build,
        )?;
        candidate.history_wire_tagged = false;
        candidate.semantic_bytes = backup_manifest_semantic_bytes(
            &candidate.catalog_bundles,
            candidate.active_catalog.as_ref(),
            candidate.last_commit_sequence,
            None,
            false,
            &candidate.checksums,
            &candidate.build,
        )?;
        Ok(candidate)
    }

    /// Returns whether the durable wire form includes the history presence tag.
    #[must_use]
    pub const fn history_wire_tagged(&self) -> bool {
        self.history_wire_tagged
    }

    /// Returns the semantic backup-manifest version.
    #[must_use]
    pub const fn manifest_version(&self) -> BackupManifestVersion {
        self.manifest_version
    }

    /// Returns the included database storage-format version.
    #[must_use]
    pub const fn storage_format_version(&self) -> StorageFormatVersion {
        self.storage_format_version
    }

    /// Returns the permanent database identity preserved by restore.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns whether this backup contains engine artifacts or a logical snapshot.
    #[must_use]
    pub const fn snapshot_kind(&self) -> BackupSnapshotKindV1 {
        self.snapshot_kind
    }

    /// Returns all immutable bundle hashes in canonical identity order.
    #[must_use]
    pub fn catalog_bundles(&self) -> &[BackupCatalogBundleV1] {
        &self.catalog_bundles
    }

    /// Borrows the exact active contract relation, if deployed.
    #[must_use]
    pub const fn active_catalog(&self) -> Option<&ActiveCatalogPointerV1> {
        self.active_catalog.as_ref()
    }

    /// Returns the last included authoritative application commit, if any.
    #[must_use]
    pub const fn last_commit_sequence(&self) -> Option<CommitSequence> {
        self.last_commit_sequence
    }

    /// Returns the backup history incarnation when the artifact carries one.
    #[must_use]
    pub const fn history_incarnation(&self) -> Option<u64> {
        self.history_incarnation
    }

    /// Returns checksums in canonical artifact-name order.
    #[must_use]
    pub fn checksums(&self) -> &[BackupArtifactChecksumV1] {
        &self.checksums
    }

    /// Borrows the captured build metadata.
    #[must_use]
    pub const fn build(&self) -> &BackupBuildMetadataV1 {
        &self.build
    }

    /// Returns checked aggregate semantic bytes under the per-field maxima.
    ///
    /// This is overflow evidence, not a new normative artifact-size limit.
    #[must_use]
    pub const fn semantic_bytes(&self) -> usize {
        self.semantic_bytes
    }
}

/// Restore overwrite policy selected only by explicit operator action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfflineRestoreOverwritePolicyV1 {
    /// Refuse any nonempty target directory.
    RefuseNonEmpty,
    /// Operator explicitly selected destructive replacement.
    ExplicitlyAllowDestructive,
}

/// Closed result of a backend-private, source/target-bound offline restore.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OfflineRestoreResultV1 {
    /// The artifact was restored and its semantic manifest was decoded.
    ///
    /// This does not establish structural integrity, catalog validity, or
    /// readiness. The restored database must still enter the normal exclusive
    /// startup-evidence and catalog-validation path.
    Restored {
        /// Checked value-level summary decoded from the restored artifact.
        manifest: Box<OfflineBackupManifestV1>,
    },
    /// The target was nonempty and destructive replacement was not explicit.
    TargetNotEmpty,
}

/// Semantic operation implemented by a private, target-bound offline adapter.
///
/// The implementing receiver owns all destination and I/O details. No concrete
/// handle, path, sink, artifact bytes, encoding, or callback crosses this port.
pub trait OfflineBackupPersistencePort {
    /// Exclusively creates one complete consistent backup and returns its
    /// checked semantic manifest summary.
    fn create_offline_backup(
        &mut self,
        build: &BackupBuildMetadataV1,
    ) -> Result<OfflineBackupManifestV1, StorageError>;
}

/// Semantic operation implemented by a private, source/target-bound adapter.
///
/// A successful result still requires ordinary startup structural evidence and
/// catalog validation before any readiness claim or operational port release.
pub trait OfflineRestorePersistencePort {
    /// Exclusively restores the already-bound source into the already-bound
    /// target under the explicit overwrite policy.
    fn restore_offline_backup(
        &mut self,
        overwrite_policy: OfflineRestoreOverwritePolicyV1,
    ) -> Result<OfflineRestoreResultV1, StorageError>;
}

fn validate_receipt_history(
    operation_kind: OfflineMaintenanceOperationKind,
    source_database_id: Option<DatabaseId>,
    transitions: &[OfflineMaintenanceReceiptTransitionV1],
) -> Result<(), StorageValueError> {
    if transitions.is_empty() {
        return Err(StorageValueError::Empty);
    }
    if transitions.len() > MAX_OFFLINE_MAINTENANCE_RECEIPT_TRANSITIONS_V1 {
        return Err(StorageValueError::LimitExceeded);
    }
    if transitions[0]
        != OfflineMaintenanceReceiptTransitionV1::phase(OfflineMaintenanceReceiptPhaseV1::Accepted)
    {
        return Err(StorageValueError::InvalidShape);
    }
    if transitions.iter().any(|entry| !entry.has_valid_shape())
        || transitions.windows(2).any(|pair| {
            !valid_receipt_transition(
                operation_kind,
                source_database_id,
                pair[0].receipt_phase(),
                pair[1],
            )
        })
    {
        return Err(StorageValueError::InvalidShape);
    }
    Ok(())
}

fn valid_receipt_transition(
    operation_kind: OfflineMaintenanceOperationKind,
    source_database_id: Option<DatabaseId>,
    prior: OfflineMaintenanceReceiptPhaseV1,
    next: OfflineMaintenanceReceiptTransitionV1,
) -> bool {
    if !next.has_valid_shape() || prior.is_terminal() {
        return false;
    }
    if next.receipt_phase() == OfflineMaintenanceReceiptPhaseV1::FailedClosed {
        return true;
    }
    (prior == OfflineMaintenanceReceiptPhaseV1::Accepted
        && next.receipt_phase() == OfflineMaintenanceReceiptPhaseV1::Offline
        && operation_kind == OfflineMaintenanceOperationKind::RestoreBackup
        && source_database_id.is_none())
        || matches!(
            (prior, next.receipt_phase()),
            (
                OfflineMaintenanceReceiptPhaseV1::Accepted,
                OfflineMaintenanceReceiptPhaseV1::Draining
            ) | (
                OfflineMaintenanceReceiptPhaseV1::Draining,
                OfflineMaintenanceReceiptPhaseV1::Offline
            ) | (
                OfflineMaintenanceReceiptPhaseV1::Offline,
                OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
            ) | (
                OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
                OfflineMaintenanceReceiptPhaseV1::Validating
            ) | (
                OfflineMaintenanceReceiptPhaseV1::Validating,
                OfflineMaintenanceReceiptPhaseV1::Succeeded
            )
        )
}

fn validate_receipt_evidence(
    operation_kind: OfflineMaintenanceOperationKind,
    source_database_id: Option<DatabaseId>,
    staged_database_id: Option<DatabaseId>,
    manifest_identity: Option<&OfflineBackupManifestIdentityV1>,
    current_phase: OfflineMaintenanceReceiptPhaseV1,
) -> Result<(), StorageValueError> {
    if operation_kind == OfflineMaintenanceOperationKind::CreateBackup
        && staged_database_id.is_some()
    {
        return Err(StorageValueError::InvalidShape);
    }
    if let (Some(source), Some(manifest)) = (source_database_id, manifest_identity)
        && operation_kind == OfflineMaintenanceOperationKind::CreateBackup
        && source != manifest.database_id()
    {
        return Err(StorageValueError::IdentityMismatch);
    }
    if let (Some(staged), Some(manifest)) = (staged_database_id, manifest_identity)
        && staged != manifest.database_id()
    {
        return Err(StorageValueError::IdentityMismatch);
    }
    if matches!(
        current_phase,
        OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
            | OfflineMaintenanceReceiptPhaseV1::Validating
            | OfflineMaintenanceReceiptPhaseV1::Succeeded
    ) && manifest_identity.is_none()
    {
        return Err(StorageValueError::InvalidShape);
    }
    if current_phase == OfflineMaintenanceReceiptPhaseV1::Succeeded {
        match operation_kind {
            OfflineMaintenanceOperationKind::CreateBackup if source_database_id.is_none() => {
                return Err(StorageValueError::InvalidShape);
            }
            OfflineMaintenanceOperationKind::RestoreBackup if staged_database_id.is_none() => {
                return Err(StorageValueError::InvalidShape);
            }
            OfflineMaintenanceOperationKind::CreateBackup
            | OfflineMaintenanceOperationKind::RestoreBackup => {}
        }
    }
    Ok(())
}

fn record_once<T: Copy + Eq>(slot: &mut Option<T>, value: T) -> Result<(), StorageValueError> {
    match *slot {
        None => *slot = Some(value),
        Some(existing) if existing == value => {}
        Some(_) => return Err(StorageValueError::IdentityMismatch),
    }
    Ok(())
}

fn option_extends<T: Eq>(prior: Option<T>, next: Option<T>) -> bool {
    prior.is_none() || prior == next
}

fn option_ref_extends<T: Eq>(prior: Option<&T>, next: Option<&T>) -> bool {
    prior.is_none() || prior == next
}

fn checked_build_value(value: String) -> Result<String, StorageValueError> {
    if value.is_empty() {
        return Err(StorageValueError::Empty);
    }
    if value.len() > MAX_BACKUP_BUILD_VALUE_BYTES {
        return Err(StorageValueError::LimitExceeded);
    }
    if value.bytes().any(|byte| !(0x21..=0x7e).contains(&byte)) {
        return Err(StorageValueError::InvalidShape);
    }
    Ok(value)
}

fn checked_feature(value: &str) -> Result<(), StorageValueError> {
    if value.is_empty() {
        return Err(StorageValueError::Empty);
    }
    if value.len() > MAX_BACKUP_BUILD_FEATURE_BYTES {
        return Err(StorageValueError::LimitExceeded);
    }
    if value
        .bytes()
        .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'-' | b'_'))
    {
        return Err(StorageValueError::InvalidShape);
    }
    Ok(())
}

fn backup_manifest_semantic_bytes(
    catalog_bundles: &[BackupCatalogBundleV1],
    active_catalog: Option<&ActiveCatalogPointerV1>,
    last_commit_sequence: Option<CommitSequence>,
    history_incarnation: Option<u64>,
    history_wire_tagged: bool,
    checksums: &[BackupArtifactChecksumV1],
    build: &BackupBuildMetadataV1,
) -> Result<usize, StorageValueError> {
    let bundles = catalog_bundles.iter().try_fold(4usize, |total, bundle| {
        total
            .checked_add(framed_backup_bytes(catalog_identity_semantic_bytes(
                bundle.lineage.as_bytes().len(),
                bundle.contract_version.to_be_bytes().len(),
            )?)?)
            .ok_or(StorageValueError::SizeOverflow)
    })?;
    let checksums = checksums.iter().try_fold(4usize, |total, checksum| {
        let value = 4usize
            .checked_add(framed_backup_bytes(checksum.checksum.as_bytes().len())?)
            .ok_or(StorageValueError::SizeOverflow)?;
        total
            .checked_add(framed_backup_bytes(value)?)
            .ok_or(StorageValueError::SizeOverflow)
    })?;
    let features = build
        .enabled_features
        .iter()
        .try_fold(4usize, |total, feature| {
            total
                .checked_add(framed_backup_bytes(feature.len())?)
                .ok_or(StorageValueError::SizeOverflow)
        })?;
    let active = match active_catalog {
        Some(pointer) => 1usize
            .checked_add(catalog_identity_semantic_bytes(
                pointer.lineage().as_bytes().len(),
                pointer.contract_version().to_be_bytes().len(),
            )?)
            .ok_or(StorageValueError::SizeOverflow)?,
        None => 1,
    };
    checked_backup_sum([
        4,  // manifest version
        4,  // storage-format version
        16, // database ID
        1,  // snapshot kind
        bundles,
        active,
        1 + last_commit_sequence.map_or(0, |_| 8),
        // Match actual encoding for all three wire cases:
        // - pre-fence omitted field → 0
        // - post-fence tagged None (presence 0) → 1
        // - post-fence tagged Some → 1 + 8
        if history_wire_tagged {
            1 + history_incarnation.map_or(0, |_| 8)
        } else {
            0
        },
        checksums,
        framed_backup_bytes(build.semantic_version.len())?,
        framed_backup_bytes(build.git_revision.len())?,
        framed_backup_bytes(build.rust_version.len())?,
        4,
        features,
    ])
}

fn catalog_identity_semantic_bytes(
    lineage_bytes: usize,
    version_bytes: usize,
) -> Result<usize, StorageValueError> {
    checked_backup_sum([framed_backup_bytes(lineage_bytes)?, version_bytes, 32])
}

fn framed_backup_bytes(content_bytes: usize) -> Result<usize, StorageValueError> {
    4usize
        .checked_add(content_bytes)
        .ok_or(StorageValueError::SizeOverflow)
}

fn checked_backup_sum(parts: impl IntoIterator<Item = usize>) -> Result<usize, StorageValueError> {
    parts.into_iter().try_fold(0usize, |total, part| {
        total
            .checked_add(part)
            .ok_or(StorageValueError::SizeOverflow)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation_id(seed: u8) -> OfflineMaintenanceOperationId {
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [seed; 10])
            .expect("operation ID")
    }

    fn database_id(seed: u8) -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(2, [seed; 10]).expect("database ID")
    }

    fn admission() -> OfflineMaintenanceAdmissionV1 {
        OfflineMaintenanceAdmissionV1::new(
            ActorId::new("maintenance-operator").expect("actor ID"),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(3, [0x33; 10]).expect("capability ID"),
            Some(ApprovalId::new("change-42").expect("approval ID")),
        )
    }

    fn accepted_restore_receipt() -> OfflineMaintenanceReceiptV1 {
        let backup_name = BackupNameV1::new("before-upgrade").expect("backup name");
        let confirmation = OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget;
        OfflineMaintenanceReceiptV1::accepted(
            operation_id(0x11),
            OfflineMaintenanceOperationKind::RestoreBackup,
            backup_name.clone(),
            offline_maintenance_input_hash(
                OfflineMaintenanceOperationKind::RestoreBackup,
                &backup_name,
                confirmation,
            ),
            confirmation,
            admission(),
        )
        .expect("accepted receipt")
    }

    #[test]
    fn checksum_artifact_identity_is_opaque_and_one_based() {
        let value = BackupIntegrityChecksumV1::new(vec![0x55; 32]).expect("checksum");
        let checksum = BackupArtifactChecksumV1::new(NonZeroU32::MIN, value);

        assert_eq!(checksum.artifact_ordinal(), NonZeroU32::MIN);
        assert_eq!(checksum.checksum().as_bytes(), &[0x55; 32]);
    }

    #[test]
    fn build_features_are_canonicalized() {
        let metadata = BackupBuildMetadataV1::new(
            "0.1.0",
            "0123456789abcdef",
            "rustc-1.97.0",
            1,
            vec!["zeta".to_owned(), "alpha".to_owned()],
        )
        .expect("build metadata");

        assert_eq!(metadata.enabled_features(), &["alpha", "zeta"]);
    }

    #[test]
    fn absent_active_catalog_requires_empty_history_and_no_commits() {
        let checksum = BackupArtifactChecksumV1::new(
            NonZeroU32::MIN,
            BackupIntegrityChecksumV1::new(vec![0x55; 32]).expect("checksum"),
        );
        let build =
            BackupBuildMetadataV1::new("0.1.0", "0123456789abcdef", "rustc-1.97.0", 1, Vec::new())
                .expect("build metadata");

        assert_eq!(
            OfflineBackupManifestV1::new(
                StorageFormatVersion::V1,
                DatabaseId::from_bytes([
                    0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x03,
                ])
                .expect("valid UUIDv7"),
                BackupSnapshotKindV1::StorageEngineData,
                Vec::new(),
                None,
                Some(CommitSequence::first()),
                Some(1),
                vec![checksum],
                build,
            ),
            Err(StorageValueError::InvalidShape)
        );
    }

    #[test]
    fn receipt_history_is_closed_append_only_and_evidence_bound() {
        let mut receipt = accepted_restore_receipt();
        receipt
            .record_source_database_id(database_id(0x43))
            .expect("healthy source");
        assert_eq!(
            receipt.advance(OfflineMaintenanceReceiptTransitionV1::phase(
                OfflineMaintenanceReceiptPhaseV1::Offline
            )),
            Err(StorageValueError::InvalidShape)
        );
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(
                OfflineMaintenanceReceiptPhaseV1::Draining,
            ))
            .expect("draining");
        receipt
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(
                OfflineMaintenanceReceiptPhaseV1::Offline,
            ))
            .expect("offline");
        assert_eq!(
            receipt.advance(OfflineMaintenanceReceiptTransitionV1::phase(
                OfflineMaintenanceReceiptPhaseV1::ArtifactPublished
            )),
            Err(StorageValueError::InvalidShape),
            "publication requires exact manifest evidence"
        );

        let staged_id = database_id(0x44);
        receipt
            .record_staged_database_id(staged_id)
            .expect("staged identity");
        assert_eq!(
            receipt.record_manifest_identity(OfflineBackupManifestIdentityV1::new(
                BackupIntegrityChecksumV1::new(vec![0x44; 32]).expect("checksum"),
                database_id(0x45),
                None,
            )),
            Err(StorageValueError::IdentityMismatch)
        );
        assert!(
            receipt.manifest_identity().is_none(),
            "a rejected evidence update must not poison the receipt"
        );
        receipt
            .record_manifest_identity(OfflineBackupManifestIdentityV1::new(
                BackupIntegrityChecksumV1::new(vec![0x55; 32]).expect("checksum"),
                staged_id,
                Some(CommitSequence::first()),
            ))
            .expect("manifest identity");
        for phase in [
            OfflineMaintenanceReceiptPhaseV1::ArtifactPublished,
            OfflineMaintenanceReceiptPhaseV1::Validating,
            OfflineMaintenanceReceiptPhaseV1::Succeeded,
        ] {
            receipt
                .advance(OfflineMaintenanceReceiptTransitionV1::phase(phase))
                .expect("forward transition");
        }
        assert!(receipt.current_phase().is_terminal());
        assert_eq!(
            receipt.advance(OfflineMaintenanceReceiptTransitionV1::failed(
                OfflineMaintenanceReceiptFailureV1::InternalFailure
            )),
            Err(StorageValueError::InvalidShape)
        );
    }

    #[test]
    fn source_less_recovery_restore_can_enter_offline_without_inventing_a_drain() {
        let mut recovery = accepted_restore_receipt();
        recovery
            .advance(OfflineMaintenanceReceiptTransitionV1::phase(
                OfflineMaintenanceReceiptPhaseV1::Offline,
            ))
            .expect("recovery target is already offline");
        assert_eq!(
            recovery.record_source_database_id(database_id(0x46)),
            Err(StorageValueError::InvalidShape)
        );
        assert_eq!(
            recovery.source_database_id(),
            None,
            "a recovery receipt cannot retroactively claim a healthy source"
        );
    }

    #[test]
    fn receipt_successor_preserves_all_immutable_and_prior_evidence() {
        let prior = accepted_restore_receipt();
        let mut next = prior.clone();
        next.advance(OfflineMaintenanceReceiptTransitionV1::phase(
            OfflineMaintenanceReceiptPhaseV1::Draining,
        ))
        .expect("draining");
        assert!(next.monotonically_extends(&prior));
        assert!(!prior.monotonically_extends(&next));

        let different_name = BackupNameV1::new("different").expect("backup name");
        let confirmation = OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget;
        let different = OfflineMaintenanceReceiptV1::accepted(
            prior.operation_id(),
            OfflineMaintenanceOperationKind::RestoreBackup,
            different_name.clone(),
            offline_maintenance_input_hash(
                OfflineMaintenanceOperationKind::RestoreBackup,
                &different_name,
                confirmation,
            ),
            confirmation,
            admission(),
        )
        .expect("different receipt");
        assert!(!different.monotonically_extends(&prior));
    }

    #[test]
    fn receipt_reconstruction_rejects_hash_mismatch_and_phase_regression() {
        let receipt = accepted_restore_receipt();
        assert_eq!(
            OfflineMaintenanceReceiptV1::accepted(
                receipt.operation_id(),
                receipt.operation_kind(),
                receipt.backup_name().clone(),
                OfflineMaintenanceInputHash::from_bytes([0x99; 32]),
                receipt.replacement_confirmation(),
                admission(),
            ),
            Err(StorageValueError::IdentityMismatch)
        );
        assert_eq!(
            OfflineMaintenanceReceiptV1::from_canonical_parts(
                receipt.operation_id(),
                receipt.operation_kind(),
                receipt.backup_name().clone(),
                receipt.input_hash(),
                receipt.replacement_confirmation(),
                admission(),
                None,
                None,
                None,
                None,
                vec![
                    OfflineMaintenanceReceiptTransitionV1::phase(
                        OfflineMaintenanceReceiptPhaseV1::Accepted,
                    ),
                    OfflineMaintenanceReceiptTransitionV1::phase(
                        OfflineMaintenanceReceiptPhaseV1::Draining,
                    ),
                    OfflineMaintenanceReceiptTransitionV1::phase(
                        OfflineMaintenanceReceiptPhaseV1::Accepted,
                    ),
                ],
            ),
            Err(StorageValueError::InvalidShape)
        );
    }
}
