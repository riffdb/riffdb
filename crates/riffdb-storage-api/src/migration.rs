//! Identity-only semantic ports for private contract-migration stages.

use std::error::Error;
use std::fmt;

use riffdb_types::{
    AdministrationSequence, CanonicalValueHash, CommitSequence, ContractBundleHash,
    ContractMigrationValidationDigest, EntityTypeId, IndexId, MigrationBundleHash, ProjectionId,
    hash_canonical_value,
};

use crate::{
    MAX_STAGED_WRITE_BYTES, StoredContractBundleV1, StoredEntityRecordV1, StoredIndexEntryV2,
};

/// Maximum authoritative rows returned by one migration scan.
pub const MAX_MIGRATION_SCAN_ROWS: usize = 256;
/// Maximum authoritative row mutations in one atomic migration batch.
pub const MAX_MIGRATION_BATCH_MUTATIONS: usize = 64;
/// Conservative journal/envelope reservation inside the existing write ceiling.
pub const MIGRATION_BATCH_JOURNAL_RESERVE_BYTES: usize = 4 * 1_024;
/// Maximum semantic row/index bytes admitted before journal/envelope reserve.
pub const MAX_MIGRATION_BATCH_WRITE_BYTES: usize =
    MAX_STAGED_WRITE_BYTES - MIGRATION_BATCH_JOURNAL_RESERVE_BYTES;

/// Exclusive ordered cursor following the last row already inspected.
#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct MigrationScanCursor(Option<crate::EntityTarget>);

impl MigrationScanCursor {
    /// Starts before the first canonical entity target.
    #[must_use]
    pub const fn start() -> Self {
        Self(None)
    }

    /// Starts after an exact canonical entity target.
    #[must_use]
    pub const fn after(target: crate::EntityTarget) -> Self {
        Self(Some(target))
    }

    /// Borrows the exclusive lower bound, if any.
    #[must_use]
    pub const fn exclusive_lower_bound(&self) -> Option<&crate::EntityTarget> {
        self.0.as_ref()
    }
}

/// Process-neutral semantic state advanced atomically with migration batches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationJournalState {
    migration: MigrationBundleHash,
    checked_through: MigrationScanCursor,
    checked_rows: u64,
    changed_rows: u64,
    batch_count: u64,
    complete: bool,
}

impl MigrationJournalState {
    /// Constructs a checked nonterminal journal post-image.
    pub fn new(
        migration: MigrationBundleHash,
        checked_through: MigrationScanCursor,
        checked_rows: u64,
        changed_rows: u64,
        batch_count: u64,
    ) -> Result<Self, MigrationStageError> {
        if checked_rows < changed_rows || batch_count == 0 {
            return Err(MigrationStageError::Integrity);
        }
        Ok(Self {
            migration,
            checked_through,
            checked_rows,
            changed_rows,
            batch_count,
            complete: false,
        })
    }

    /// Returns the exact migration identity.
    #[must_use]
    pub const fn migration(&self) -> MigrationBundleHash {
        self.migration
    }

    /// Borrows the exclusive scan cursor committed with the latest batch.
    #[must_use]
    pub const fn checked_through(&self) -> &MigrationScanCursor {
        &self.checked_through
    }

    /// Returns cumulative checked rows.
    #[must_use]
    pub const fn checked_rows(&self) -> u64 {
        self.checked_rows
    }

    /// Returns cumulative entity post-images changed.
    #[must_use]
    pub const fn changed_rows(&self) -> u64 {
        self.changed_rows
    }

    /// Returns committed atomic batch count.
    #[must_use]
    pub const fn batch_count(&self) -> u64 {
        self.batch_count
    }

    /// Returns whether successful final cutover completed.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.complete
    }

    /// Marks this exact stage journal complete at successful cutover.
    pub fn mark_complete(&mut self) -> Result<(), MigrationStageError> {
        if self.complete {
            return Err(MigrationStageError::Integrity);
        }
        self.complete = true;
        Ok(())
    }
}

/// Process-neutral permanent evidence installed by successful final cutover.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationRecordEvidence {
    parent: ContractBundleHash,
    candidate: ContractBundleHash,
    migration: MigrationBundleHash,
    administration_sequence: AdministrationSequence,
}

impl MigrationRecordEvidence {
    /// Constructs exact final-cutover evidence.
    #[must_use]
    pub const fn new(
        parent: ContractBundleHash,
        candidate: ContractBundleHash,
        migration: MigrationBundleHash,
        administration_sequence: AdministrationSequence,
    ) -> Self {
        Self {
            parent,
            candidate,
            migration,
            administration_sequence,
        }
    }

    /// Returns the exact predecessor bundle hash.
    #[must_use]
    pub const fn parent(self) -> ContractBundleHash {
        self.parent
    }

    /// Returns the exact activated successor bundle hash.
    #[must_use]
    pub const fn candidate(self) -> ContractBundleHash {
        self.candidate
    }

    /// Returns the exact migration artifact hash.
    #[must_use]
    pub const fn migration(self) -> MigrationBundleHash {
        self.migration
    }

    /// Returns the sole administration sequence assigned at cutover.
    #[must_use]
    pub const fn administration_sequence(self) -> AdministrationSequence {
        self.administration_sequence
    }
}

/// One bounded ordered page from the private staged database.
#[derive(Clone, Eq, PartialEq)]
pub struct MigrationScanPage {
    rows: Vec<StoredEntityRecordV1>,
    next: Option<MigrationScanCursor>,
}

impl MigrationScanPage {
    /// Constructs a page while enforcing its fixed row bound.
    pub fn new(
        rows: Vec<StoredEntityRecordV1>,
        next: Option<MigrationScanCursor>,
    ) -> Result<Self, MigrationStageError> {
        if rows.len() > MAX_MIGRATION_SCAN_ROWS
            || rows
                .windows(2)
                .any(|pair| pair[0].target() >= pair[1].target())
            || rows.is_empty() && next.is_some()
            || next.as_ref().is_some_and(|cursor| {
                cursor.exclusive_lower_bound() != rows.last().map(StoredEntityRecordV1::target)
            })
        {
            return Err(MigrationStageError::LimitExceeded);
        }
        Ok(Self { rows, next })
    }

    /// Borrows rows in canonical target order.
    #[must_use]
    pub fn rows(&self) -> &[StoredEntityRecordV1] {
        &self.rows
    }

    /// Borrows the next exclusive cursor, or returns exact end.
    #[must_use]
    pub const fn next(&self) -> Option<&MigrationScanCursor> {
        self.next.as_ref()
    }
}

impl fmt::Debug for MigrationScanPage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MigrationScanPage")
            .field("row_count", &self.rows.len())
            .field("next", &self.next)
            .finish()
    }
}

/// Exact transaction-current evidence for one migration source row.
#[derive(Clone, Eq, PartialEq)]
pub struct MigrationRowEvidence {
    source: StoredEntityRecordV1,
    fields_hash: CanonicalValueHash,
}

impl MigrationRowEvidence {
    /// Seals the exact version, binding, bytes, and a canonical content hash.
    #[must_use]
    pub fn from_source(source: StoredEntityRecordV1) -> Self {
        let fields_hash = hash_canonical_value(source.fields_encoded());
        Self {
            source,
            fields_hash,
        }
    }

    /// Borrows the complete source row evidence.
    #[must_use]
    pub const fn source(&self) -> &StoredEntityRecordV1 {
        &self.source
    }

    /// Returns the canonical source-fields hash.
    #[must_use]
    pub const fn fields_hash(&self) -> CanonicalValueHash {
        self.fields_hash
    }

    /// Rechecks a transaction-current row against all sealed evidence.
    #[must_use]
    pub fn matches(&self, current: &StoredEntityRecordV1) -> bool {
        self.source == *current
            && self.fields_hash == hash_canonical_value(current.fields_encoded())
    }
}

impl fmt::Debug for MigrationRowEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MigrationRowEvidence")
            .field("target", self.source.target())
            .field("entity_version", &self.source.entity_version())
            .field("schema_binding", self.source.schema_binding())
            .field("fields_hash", &self.fields_hash)
            .finish()
    }
}

/// One coordinator-prepared authoritative row and index transition.
#[derive(Clone, Eq, PartialEq)]
pub struct MigrationRowMutation {
    expected: MigrationRowEvidence,
    post_image: Option<StoredEntityRecordV1>,
    retire_source: bool,
    rebuilt_indexes: Vec<StoredIndexEntryV2>,
}

impl MigrationRowMutation {
    /// Constructs an exact mutation whose target and version transition agree.
    pub fn new(
        expected: MigrationRowEvidence,
        post_image: Option<StoredEntityRecordV1>,
        rebuilt_indexes: Vec<StoredIndexEntryV2>,
    ) -> Result<Self, MigrationStageError> {
        if post_image.as_ref().is_some_and(|post_image| {
            expected.source().target().entity_type_id() != post_image.target().entity_type_id()
                || expected.source().entity_version().checked_next()
                    != Some(post_image.entity_version())
        }) || post_image.is_none() && rebuilt_indexes.is_empty()
        {
            return Err(MigrationStageError::Integrity);
        }
        Ok(Self {
            expected,
            post_image,
            retire_source: false,
            rebuilt_indexes,
        })
    }

    /// Constructs a logical retirement that archives and removes the authoritative row.
    pub fn retire(expected: MigrationRowEvidence) -> Self {
        Self {
            expected,
            post_image: None,
            retire_source: true,
            rebuilt_indexes: Vec::new(),
        }
    }

    /// Borrows exact source evidence.
    #[must_use]
    pub const fn expected(&self) -> &MigrationRowEvidence {
        &self.expected
    }

    /// Borrows the complete successor-bound entity post-image.
    #[must_use]
    pub const fn post_image(&self) -> Option<&StoredEntityRecordV1> {
        self.post_image.as_ref()
    }

    /// Whether the source row is archived and removed instead of replaced.
    #[must_use]
    pub const fn retires_source(&self) -> bool {
        self.retire_source
    }

    /// Whether every predecessor index entry for the source target is removed.
    #[must_use]
    pub const fn removes_source_indexes(&self) -> bool {
        self.post_image.is_some() || self.retire_source
    }

    /// Borrows complete successor index post-images.
    #[must_use]
    pub fn rebuilt_indexes(&self) -> &[StoredIndexEntryV2] {
        &self.rebuilt_indexes
    }

    /// Returns the checked semantic write charge used by bounded batch packing.
    pub fn semantic_write_bytes(&self) -> Result<usize, MigrationStageError> {
        let entity_bytes = self
            .post_image
            .as_ref()
            .map_or(Ok(0), StoredEntityRecordV1::semantic_bytes)
            .map_err(|_| MigrationStageError::LimitExceeded)?;
        let retained_predecessor_bytes = if self.post_image.is_some() || self.retire_source {
            self.expected.source().semantic_bytes()
        } else {
            Ok(0)
        }
        .map_err(|_| MigrationStageError::LimitExceeded)?;
        let entity_bytes = entity_bytes
            .checked_add(retained_predecessor_bytes)
            .filter(|bytes| *bytes <= MAX_MIGRATION_BATCH_WRITE_BYTES)
            .ok_or(MigrationStageError::LimitExceeded)?;
        self.rebuilt_indexes
            .iter()
            .try_fold(entity_bytes, |total, entry| {
                total
                    .checked_add(
                        entry
                            .semantic_bytes()
                            .map_err(|_| MigrationStageError::LimitExceeded)?,
                    )
                    .filter(|bytes| *bytes <= MAX_MIGRATION_BATCH_WRITE_BYTES)
                    .ok_or(MigrationStageError::LimitExceeded)
            })
    }
}

impl fmt::Debug for MigrationRowMutation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MigrationRowMutation")
            .field("expected", &self.expected)
            .field(
                "post_image",
                &self.post_image.as_ref().map(StoredEntityRecordV1::target),
            )
            .field("retire_source", &self.retire_source)
            .field("rebuilt_index_count", &self.rebuilt_indexes.len())
            .finish()
    }
}

/// One bounded batch applied atomically with journal advancement.
pub struct MigrationBatch {
    migration: MigrationBundleHash,
    mutations: Vec<MigrationRowMutation>,
    checked_through: MigrationScanCursor,
    checked_rows: u64,
    changed_rows: u64,
}

impl MigrationBatch {
    /// Constructs a bounded coordinator batch. An empty mutation set may
    /// advance the journal across a checked page containing unchanged rows.
    pub fn new(
        migration: MigrationBundleHash,
        mutations: Vec<MigrationRowMutation>,
        checked_through: MigrationScanCursor,
        checked_rows: u64,
        changed_rows: u64,
    ) -> Result<Self, MigrationStageError> {
        if mutations.len() > MAX_MIGRATION_BATCH_MUTATIONS || checked_rows < changed_rows {
            return Err(MigrationStageError::LimitExceeded);
        }
        mutations.iter().try_fold(0_usize, |total, mutation| {
            total
                .checked_add(mutation.semantic_write_bytes()?)
                .filter(|bytes| *bytes <= MAX_MIGRATION_BATCH_WRITE_BYTES)
                .ok_or(MigrationStageError::LimitExceeded)
        })?;
        Ok(Self {
            migration,
            mutations,
            checked_through,
            checked_rows,
            changed_rows,
        })
    }

    /// Returns the exact migration artifact identity.
    #[must_use]
    pub const fn migration(&self) -> MigrationBundleHash {
        self.migration
    }

    /// Borrows ordered row mutations.
    #[must_use]
    pub fn mutations(&self) -> &[MigrationRowMutation] {
        &self.mutations
    }

    /// Borrows the journal cursor installed atomically with the batch.
    #[must_use]
    pub const fn checked_through(&self) -> &MigrationScanCursor {
        &self.checked_through
    }

    /// Returns cumulative checked rows at this journal point.
    #[must_use]
    pub const fn checked_rows(&self) -> u64 {
        self.checked_rows
    }

    /// Returns cumulative changed rows at this journal point.
    #[must_use]
    pub const fn changed_rows(&self) -> u64 {
        self.changed_rows
    }
}

/// Exact identities required for the final private-stage cutover.
pub struct MigrationCutover {
    parent: ContractBundleHash,
    candidate: StoredContractBundleV1,
    migration: MigrationBundleHash,
    required_projections: Vec<ProjectionId>,
    retained_parent_lineage: Vec<ContractBundleHash>,
    validation_digest: ContractMigrationValidationDigest,
}

impl MigrationCutover {
    /// Constructs a complete final-cutover request.
    #[must_use]
    pub const fn new(
        parent: ContractBundleHash,
        candidate: StoredContractBundleV1,
        migration: MigrationBundleHash,
        required_projections: Vec<ProjectionId>,
        retained_parent_lineage: Vec<ContractBundleHash>,
        validation_digest: ContractMigrationValidationDigest,
    ) -> Self {
        Self {
            parent,
            candidate,
            migration,
            required_projections,
            retained_parent_lineage,
            validation_digest,
        }
    }

    /// Returns the expected active predecessor hash.
    #[must_use]
    pub const fn parent(&self) -> ContractBundleHash {
        self.parent
    }

    /// Borrows the exact successor bundle.
    #[must_use]
    pub const fn candidate(&self) -> &StoredContractBundleV1 {
        &self.candidate
    }

    /// Returns the exact migration artifact hash.
    #[must_use]
    pub const fn migration(&self) -> MigrationBundleHash {
        self.migration
    }

    /// Borrows projection generations that must already be ready.
    #[must_use]
    pub fn required_projections(&self) -> &[ProjectionId] {
        &self.required_projections
    }

    /// Borrows exact predecessor lineage bindings allowed on unchanged rows.
    #[must_use]
    pub fn retained_parent_lineage(&self) -> &[ContractBundleHash] {
        &self.retained_parent_lineage
    }

    /// Returns the catalog-sealed complete validation identity.
    #[must_use]
    pub const fn validation_digest(&self) -> ContractMigrationValidationDigest {
        self.validation_digest
    }
}

/// Successful final-cutover evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationCutoverApplied {
    administration_sequence: AdministrationSequence,
}

impl MigrationCutoverApplied {
    /// Constructs storage-returned final-cutover evidence.
    #[must_use]
    pub const fn new(administration_sequence: AdministrationSequence) -> Self {
        Self {
            administration_sequence,
        }
    }

    /// Returns the sole administration sequence assigned by migration.
    #[must_use]
    pub const fn administration_sequence(self) -> AdministrationSequence {
        self.administration_sequence
    }
}

/// Closed failures from a private migration stage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationStageError {
    /// A fixed page, batch, byte, or count bound was exceeded.
    LimitExceeded,
    /// Transaction-current source evidence no longer matched.
    RowChanged,
    /// Artifact, journal, catalog, or stage state was inconsistent.
    Integrity,
    /// A required relationship target is absent.
    RelationshipMissing(EntityTypeId),
    /// A successor unique key collides.
    UniqueConflict {
        /// Entity whose successor unique key collides.
        entity: EntityTypeId,
        /// Backing successor index identity.
        index: IndexId,
    },
    /// The final administration sequence cannot be assigned.
    SequenceExhausted,
    /// An unresolved predecessor admission would be stranded by cutover.
    PendingAdmission,
}

impl fmt::Display for MigrationStageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("private migration stage operation failed")
    }
}

impl Error for MigrationStageError {}

/// Private-stage operations used only by the commit-owned coordinator.
pub trait MigrationStagePort {
    /// Returns the transaction-current active bundle hash.
    fn active_bundle_hash(&self) -> ContractBundleHash;

    /// Reads one bounded canonical page.
    fn scan_migration_rows(
        &self,
        cursor: &MigrationScanCursor,
    ) -> Result<MigrationScanPage, MigrationStageError>;

    /// Tests one exact relationship target without exposing general reads.
    fn migration_target_exists(
        &self,
        target: &crate::EntityTarget,
    ) -> Result<bool, MigrationStageError>;

    /// Reports unresolved admissions owned by a predecessor version being retired.
    fn has_unresolved_retiring_admissions(&self) -> Result<bool, MigrationStageError>;

    /// Reads exact restart progress for this artifact, when a batch already committed.
    fn migration_journal_state(
        &self,
        migration: MigrationBundleHash,
    ) -> Result<Option<MigrationJournalState>, MigrationStageError> {
        let _ = migration;
        Ok(None)
    }

    /// Reads the exact durable orchestration step for restart recovery.
    fn migration_journal_step(
        &self,
        migration: MigrationBundleHash,
    ) -> Result<Option<crate::ContractMigrationJournalStepV1>, MigrationStageError> {
        self.migration_journal_state(migration)
            .map(|journal| journal.map(|_| crate::ContractMigrationJournalStepV1::Transforming))
    }

    /// Returns the immutable application frontier captured before staging.
    fn migration_frozen_frontier(&self) -> Option<CommitSequence> {
        None
    }

    /// Counts predecessor images retained for logically retired entity types.
    fn retained_migration_entity_count(
        &self,
        _migration: MigrationBundleHash,
        _entity_types: &[EntityTypeId],
    ) -> Result<u64, MigrationStageError> {
        Ok(0)
    }

    /// Applies row/index mutations and journal advancement atomically.
    fn apply_migration_batch(&mut self, batch: MigrationBatch) -> Result<(), MigrationStageError>;

    /// Advances durable orchestration evidence without changing authoritative rows.
    fn checkpoint_migration_step(
        &mut self,
        _step: crate::ContractMigrationJournalStepV1,
        _required_projections: &[ProjectionId],
    ) -> Result<(), MigrationStageError> {
        Ok(())
    }

    /// Builds fresh required projection generations through the frozen frontier.
    fn build_migration_projection_candidates(
        &mut self,
        projections: &[ProjectionId],
    ) -> Result<(), MigrationStageError>;

    /// Performs complete semantic validation of the private stage before cutover.
    fn validate_migration_stage(
        &self,
        candidate: ContractBundleHash,
        retained_parent_lineage: &[ContractBundleHash],
    ) -> Result<(), MigrationStageError>;

    /// Performs storage-owned complete structural validation of the private stage.
    fn validate_migration_stage_structure(&self) -> Result<(), MigrationStageError> {
        self.validate_migration_stage(self.active_bundle_hash(), &[])
    }

    /// Atomically activates the successor, fences predecessor writes, and records migration.
    fn finalize_migration(
        &mut self,
        cutover: MigrationCutover,
    ) -> Result<MigrationCutoverApplied, MigrationStageError>;
}
