//! Archive-only V3 receipt semantics; external persistence belongs to the adapter.

use super::*;
use crate::ArchiveRestoreSelectionV3;
use riffdb_types::{ArchiveNameV1, ArchiveRestoreStopV1, DualFrontier, archive_restore_input_hash};

/// Exact archive restore request, immutable selection and bounded recovery history.
/// This is evidence metadata, not a replay, authorization or publication capability.
/// Ordinary create/restore V1 and retirement V2 semantics are independent and frozen.
#[derive(Clone, Eq, PartialEq)]
pub struct OfflineMaintenanceReceiptV3 {
    operation_id: OfflineMaintenanceOperationId,
    backup_name: BackupNameV1,
    archive_name: ArchiveNameV1,
    stop: ArchiveRestoreStopV1,
    input_hash: OfflineMaintenanceInputHash,
    replacement_confirmation: OfflineMaintenanceReplacementConfirmation,
    admission: OfflineMaintenanceAdmissionV1,
    source_database_id: Option<DatabaseId>,
    selection: Option<ArchiveRestoreSelectionV3>,
    staged_database_id: Option<DatabaseId>,
    restored_frontier: Option<DualFrontier>,
    published_history_incarnation: Option<u64>,
    transitions: Vec<OfflineMaintenanceReceiptTransitionV1>,
}

impl OfflineMaintenanceReceiptV3 {
    /// Admits an exact request. Source presence fixes the ordinary or source-less
    /// route and cannot change on retry. The caller must durably create the receipt
    /// before returning Accepted; this constructor establishes no durability.
    #[allow(clippy::too_many_arguments)]
    pub fn accepted_archive_restore(
        operation_id: OfflineMaintenanceOperationId,
        backup_name: BackupNameV1,
        archive_name: ArchiveNameV1,
        stop: ArchiveRestoreStopV1,
        input_hash: OfflineMaintenanceInputHash,
        replacement_confirmation: OfflineMaintenanceReplacementConfirmation,
        admission: OfflineMaintenanceAdmissionV1,
        source_database_id: Option<DatabaseId>,
    ) -> Result<Self, StorageValueError> {
        Self::from_canonical_parts(
            operation_id,
            backup_name,
            archive_name,
            stop,
            input_hash,
            replacement_confirmation,
            admission,
            source_database_id,
            None,
            None,
            None,
            None,
            vec![OfflineMaintenanceReceiptTransitionV1::phase(
                OfflineMaintenanceReceiptPhaseV1::Accepted,
            )],
        )
    }

    /// Reconstructs complete decoded evidence through the same semantic validator.
    #[allow(clippy::too_many_arguments)]
    pub fn from_canonical_parts(
        operation_id: OfflineMaintenanceOperationId,
        backup_name: BackupNameV1,
        archive_name: ArchiveNameV1,
        stop: ArchiveRestoreStopV1,
        input_hash: OfflineMaintenanceInputHash,
        replacement_confirmation: OfflineMaintenanceReplacementConfirmation,
        admission: OfflineMaintenanceAdmissionV1,
        source_database_id: Option<DatabaseId>,
        selection: Option<ArchiveRestoreSelectionV3>,
        staged_database_id: Option<DatabaseId>,
        restored_frontier: Option<DualFrontier>,
        published_history_incarnation: Option<u64>,
        transitions: Vec<OfflineMaintenanceReceiptTransitionV1>,
    ) -> Result<Self, StorageValueError> {
        let value = Self {
            operation_id,
            backup_name,
            archive_name,
            stop,
            input_hash,
            replacement_confirmation,
            admission,
            source_database_id,
            selection,
            staged_database_id,
            restored_frontier,
            published_history_incarnation,
            transitions,
        };
        value.validate()?;
        Ok(value)
    }

    /// Caller-stable operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> OfflineMaintenanceOperationId {
        self.operation_id
    }
    /// Archive restore retains the existing administrative restore operation kind.
    #[must_use]
    pub const fn operation_kind(&self) -> OfflineMaintenanceOperationKind {
        OfflineMaintenanceOperationKind::RestoreBackup
    }
    /// Checked original full-backup name.
    #[must_use]
    pub const fn backup_name(&self) -> &BackupNameV1 {
        &self.backup_name
    }
    /// Checked configured archive name; never a sink path or credential.
    #[must_use]
    pub const fn archive_name(&self) -> &ArchiveNameV1 {
        &self.archive_name
    }
    /// Exact request stop, independent of resolved artifact selection.
    #[must_use]
    pub const fn stop(&self) -> ArchiveRestoreStopV1 {
        self.stop
    }
    /// Canonical identity binding every request field.
    #[must_use]
    pub const fn input_hash(&self) -> OfflineMaintenanceInputHash {
        self.input_hash
    }
    /// Exact replacement confirmation retained for retries.
    #[must_use]
    pub const fn replacement_confirmation(&self) -> OfflineMaintenanceReplacementConfirmation {
        self.replacement_confirmation
    }
    /// Redacted admitted actor and capability identity, never reusable authority.
    #[must_use]
    pub const fn admission(&self) -> &OfflineMaintenanceAdmissionV1 {
        &self.admission
    }
    /// Healthy current database identity, absent only for the source-less route.
    #[must_use]
    pub const fn source_database_id(&self) -> Option<DatabaseId> {
        self.source_database_id
    }
    /// Frozen verified artifact selection, absent only before the offline selection step.
    #[must_use]
    pub const fn selection(&self) -> Option<&ArchiveRestoreSelectionV3> {
        self.selection.as_ref()
    }
    /// Independently validated replay candidate's database identity.
    #[must_use]
    pub const fn staged_database_id(&self) -> Option<DatabaseId> {
        self.staged_database_id
    }
    /// Actual restored frontier, distinct from the full-backup and archive frontiers.
    #[must_use]
    pub const fn restored_frontier(&self) -> Option<DualFrontier> {
        self.restored_frontier
    }
    /// Durable incarnation decision that the publication owner must stamp exactly.
    #[must_use]
    pub const fn published_history_incarnation(&self) -> Option<u64> {
        self.published_history_incarnation
    }
    /// Complete bounded append-only phase history.
    #[must_use]
    pub fn transitions(&self) -> &[OfflineMaintenanceReceiptTransitionV1] {
        &self.transitions
    }
    /// Current checked recovery phase.
    #[must_use]
    pub fn current_phase(&self) -> OfflineMaintenanceReceiptPhaseV1 {
        self.transitions
            .last()
            .map_or(OfflineMaintenanceReceiptPhaseV1::Accepted, |t| {
                t.receipt_phase()
            })
    }

    /// Records verified selection once. Ordinary restores must be offline;
    /// source-less candidates can bind their fixed selection before admission.
    pub fn record_selection(
        &mut self,
        selection: ArchiveRestoreSelectionV3,
    ) -> Result<(), StorageValueError> {
        let mut next = self.clone();
        next.selection = Some(selection);
        self.install(next)
    }

    /// Records complete validated replay evidence together. The offline owner must
    /// independently validate the candidate and later perform staged authorization.
    pub fn record_validated_restore(
        &mut self,
        staged: DatabaseId,
        frontier: DualFrontier,
    ) -> Result<(), StorageValueError> {
        let mut next = self.clone();
        next.staged_database_id = Some(staged);
        next.restored_frontier = Some(frontier);
        self.install(next)
    }

    /// Records the exact existing max(target, staged)+1 decision before stamping.
    /// The publication owner proves the target term; this value checks the bound
    /// against the original staged incarnation and forbids changes on retry.
    pub fn record_published_incarnation(
        &mut self,
        incarnation: u64,
    ) -> Result<(), StorageValueError> {
        let mut next = self.clone();
        next.published_history_incarnation = Some(incarnation);
        self.install(next)
    }

    /// Appends one legal phase without admitting a partial update on failure.
    pub fn advance(
        &mut self,
        transition: OfflineMaintenanceReceiptTransitionV1,
    ) -> Result<(), StorageValueError> {
        if self.transitions.len() >= MAX_OFFLINE_MAINTENANCE_RECEIPT_TRANSITIONS_V1 {
            return Err(StorageValueError::LimitExceeded);
        }
        let mut next = self.clone();
        next.transitions.push(transition);
        self.install(next)
    }

    /// Exact retry or monotonic extension, never a new selection under an old ID.
    #[must_use]
    pub fn monotonically_extends(&self, prior: &Self) -> bool {
        (!prior.current_phase().is_terminal() || self == prior)
            && self.operation_id == prior.operation_id
            && self.backup_name == prior.backup_name
            && self.archive_name == prior.archive_name
            && self.stop == prior.stop
            && self.input_hash == prior.input_hash
            && self.replacement_confirmation == prior.replacement_confirmation
            && self.admission == prior.admission
            && self.source_database_id == prior.source_database_id
            && option_ref_extends(prior.selection.as_ref(), self.selection.as_ref())
            && option_extends(prior.staged_database_id, self.staged_database_id)
            && option_extends(prior.restored_frontier, self.restored_frontier)
            && option_extends(
                prior.published_history_incarnation,
                self.published_history_incarnation,
            )
            && self.transitions.starts_with(&prior.transitions)
    }

    fn install(&mut self, next: Self) -> Result<(), StorageValueError> {
        next.validate()?;
        if !next.monotonically_extends(self) {
            return Err(StorageValueError::IdentityMismatch);
        }
        *self = next;
        Ok(())
    }

    fn validate(&self) -> Result<(), StorageValueError> {
        use OfflineMaintenanceReceiptPhaseV1::*;
        validate_receipt_history(
            self.operation_kind(),
            self.source_database_id,
            &self.transitions,
        )?;
        if self.input_hash
            != archive_restore_input_hash(
                &self.backup_name,
                &self.archive_name,
                self.stop,
                self.replacement_confirmation,
            )
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let visited = |phase| self.transitions.iter().any(|t| t.receipt_phase() == phase);
        let offline = visited(Offline);
        if self.selection.is_some()
            && !offline
            && (self.source_database_id.is_some() || visited(Draining))
        {
            return Err(StorageValueError::InvalidShape);
        }
        if self.staged_database_id.is_some() != self.restored_frontier.is_some() {
            return Err(StorageValueError::InvalidShape);
        }
        if let Some(selection) = &self.selection {
            selection.target_application(self.stop)?;
            if let Some(staged) = self.staged_database_id
                && staged != selection.lineage().database_id()
            {
                return Err(StorageValueError::IdentityMismatch);
            }
            if let Some(frontier) = self.restored_frontier {
                selection.validate_restored_frontier(self.stop, frontier)?;
            }
            if let Some(incarnation) = self.published_history_incarnation
                && (!offline
                    || self.restored_frontier.is_none()
                    || incarnation <= selection.lineage().history_incarnation())
            {
                return Err(StorageValueError::InvalidShape);
            }
        } else if self.restored_frontier.is_some() || self.published_history_incarnation.is_some() {
            return Err(StorageValueError::InvalidShape);
        }
        if visited(ArtifactPublished)
            && (self.selection.is_none()
                || self.restored_frontier.is_none()
                || self.published_history_incarnation.is_none())
        {
            return Err(StorageValueError::InvalidShape);
        }
        validate_receipt_evidence(
            self.operation_kind(),
            self.source_database_id,
            self.staged_database_id,
            self.selection
                .as_ref()
                .map(ArchiveRestoreSelectionV3::backup),
            self.current_phase(),
        )
    }
}

impl std::fmt::Debug for OfflineMaintenanceReceiptV3 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OfflineMaintenanceReceiptV3")
            .field("operation_id", &self.operation_id)
            .field("phase", &self.current_phase())
            .field("details", &"[REDACTED]")
            .finish()
    }
}

/// Bounded canonical inventory of archive-only external receipts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfflineMaintenanceReceiptInventoryV3(Vec<OfflineMaintenanceReceiptV3>);
impl OfflineMaintenanceReceiptInventoryV3 {
    /// Sorts exact operation identities and rejects duplicates or excess receipts.
    pub fn new(mut receipts: Vec<OfflineMaintenanceReceiptV3>) -> Result<Self, StorageValueError> {
        if receipts.len() > MAX_OFFLINE_MAINTENANCE_RECEIPTS_V1 {
            return Err(StorageValueError::LimitExceeded);
        }
        receipts.sort_by_key(OfflineMaintenanceReceiptV3::operation_id);
        if receipts
            .windows(2)
            .any(|pair| pair[0].operation_id() == pair[1].operation_id())
        {
            return Err(StorageValueError::Duplicate);
        }
        Ok(Self(receipts))
    }
    /// Canonical operation order, including terminal evidence.
    #[must_use]
    pub fn receipts(&self) -> &[OfflineMaintenanceReceiptV3] {
        &self.0
    }
}
/// Outcome of durable archive receipt admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OfflineMaintenanceReceiptCreateResultV3 {
    /// The accepted candidate became durable.
    Created,
    /// The original receipt is returned unchanged; admission must compare its input identity.
    Existing(Box<OfflineMaintenanceReceiptV3>),
}
/// Outcome of a durable monotonic receipt replacement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfflineMaintenanceReceiptReplaceResultV3 {
    /// The exact candidate is durable, including resolved parent synchronization.
    AlreadyCurrent,
    /// The checked extension replaced its predecessor durably.
    Replaced,
}
/// Archive receipt access under the same exclusive maintenance owner and inventory
/// as V1/V2 receipts and migrations. This port grants no database mutation authority.
pub trait OfflineArchiveReceiptPersistencePort {
    /// Creates an Accepted receipt or returns the exact original for retry checks.
    fn create_or_read_archive_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV3,
    ) -> Result<OfflineMaintenanceReceiptCreateResultV3, StorageError>;
    /// Atomically persists a checked monotonic extension, retaining selection exactly.
    fn replace_archive_receipt(
        &mut self,
        receipt: &OfflineMaintenanceReceiptV3,
    ) -> Result<OfflineMaintenanceReceiptReplaceResultV3, StorageError>;
    /// Reads one checksummed receipt without resolving an archive again.
    fn read_archive_receipt(
        &mut self,
        id: OfflineMaintenanceOperationId,
    ) -> Result<Option<OfflineMaintenanceReceiptV3>, StorageError>;
    /// Validates the complete bounded shared ledger before returning its V3 members.
    fn validate_archive_receipt_inventory(
        &mut self,
    ) -> Result<OfflineMaintenanceReceiptInventoryV3, StorageError>;
}
