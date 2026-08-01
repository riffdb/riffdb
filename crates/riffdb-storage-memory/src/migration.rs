//! Deterministic private-stage reference model for contract migration.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use riffdb_storage_api::{
    AdministrationSequenceAllocator, ApplicationSequenceAllocator, MAX_MIGRATION_SCAN_ROWS,
    MigrationBatch, MigrationCutover, MigrationCutoverApplied, MigrationJournalState,
    MigrationRecordEvidence, MigrationScanCursor, MigrationScanPage, MigrationStageError,
    MigrationStagePort, StoredContractBundleV1, StoredEntityRecordV1, StoredIndexEntryV2,
};
use riffdb_types::{ContractBundleHash, ProjectionId};

const MAX_HISTORY_WITNESS_BYTES: usize = 16 * 1_024 * 1_024;

/// Opaque canary proving application sequencing and retained history do not change.
#[derive(Clone, Eq, PartialEq)]
pub struct MemoryMigrationHistoryWitness {
    application_sequence: ApplicationSequenceAllocator,
    retained_bytes: Vec<u8>,
}

impl MemoryMigrationHistoryWitness {
    /// Constructs one bounded immutable-history witness.
    pub fn new(
        application_sequence: ApplicationSequenceAllocator,
        retained_bytes: Vec<u8>,
    ) -> Result<Self, MigrationStageError> {
        if retained_bytes.len() > MAX_HISTORY_WITNESS_BYTES {
            return Err(MigrationStageError::LimitExceeded);
        }
        Ok(Self {
            application_sequence,
            retained_bytes,
        })
    }
}

impl fmt::Debug for MemoryMigrationHistoryWitness {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryMigrationHistoryWitness")
            .field("application_sequence", &self.application_sequence)
            .field("retained_bytes", &"[REDACTED]")
            .field("retained_length", &self.retained_bytes.len())
            .finish()
    }
}

/// Memory-backend name for the shared semantic migration journal.
pub type MemoryMigrationJournal = MigrationJournalState;
/// Memory-backend name for shared permanent migration evidence.
pub type MemoryMigrationRecord = MigrationRecordEvidence;

/// Exact state snapshot used for failure-atomicity assertions.
#[derive(Clone, Eq, PartialEq)]
pub struct MemoryMigrationSnapshot {
    active_bundle: StoredContractBundleV1,
    entities: Vec<StoredEntityRecordV1>,
    indexes: Vec<StoredIndexEntryV2>,
    retained_archive: Vec<StoredEntityRecordV1>,
    history: MemoryMigrationHistoryWitness,
    journal: Option<MemoryMigrationJournal>,
    retired_predecessor_writes: bool,
    projection_candidates: BTreeMap<ProjectionId, ApplicationSequenceAllocator>,
    migration_record: Option<MemoryMigrationRecord>,
    administration_sequence: AdministrationSequenceAllocator,
    unresolved_retiring_admissions: bool,
}

impl fmt::Debug for MemoryMigrationSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryMigrationSnapshot")
            .field("active_bundle", &self.active_bundle)
            .field("entity_count", &self.entities.len())
            .field("index_count", &self.indexes.len())
            .field("retained_archive_count", &self.retained_archive.len())
            .field("history", &self.history)
            .field("journal", &self.journal)
            .field(
                "retired_predecessor_writes",
                &self.retired_predecessor_writes,
            )
            .field("projection_candidates", &self.projection_candidates)
            .field("migration_record", &self.migration_record)
            .field("administration_sequence", &self.administration_sequence)
            .field(
                "unresolved_retiring_admissions",
                &self.unresolved_retiring_admissions,
            )
            .finish()
    }
}

/// Isolated volatile stage used by the migration reference model.
pub struct MemoryMigrationStage {
    state: MemoryMigrationSnapshot,
}

impl MemoryMigrationStage {
    /// Constructs a canonical stage whose initial rows are exact active-bundle rows.
    pub fn new(
        active_bundle: StoredContractBundleV1,
        mut entities: Vec<StoredEntityRecordV1>,
        history: MemoryMigrationHistoryWitness,
    ) -> Result<Self, MigrationStageError> {
        entities.sort_unstable_by(|left, right| left.target().cmp(right.target()));
        if entities
            .windows(2)
            .any(|pair| pair[0].target() == pair[1].target())
            || entities.iter().any(|row| {
                row.schema_binding().lineage() != active_bundle.lineage()
                    || row.schema_binding().contract_version() > active_bundle.contract_version()
            })
        {
            return Err(MigrationStageError::Integrity);
        }
        Ok(Self {
            state: MemoryMigrationSnapshot {
                active_bundle,
                entities,
                indexes: Vec::new(),
                retained_archive: Vec::new(),
                history,
                journal: None,
                retired_predecessor_writes: false,
                projection_candidates: BTreeMap::new(),
                migration_record: None,
                administration_sequence: AdministrationSequenceAllocator::initial(),
                unresolved_retiring_admissions: false,
            },
        })
    }

    /// Borrows immutable application history and its sequence allocator canary.
    #[must_use]
    pub const fn history_witness(&self) -> &MemoryMigrationHistoryWitness {
        &self.state.history
    }

    /// Clones the complete semantic state for failure-atomicity comparison.
    #[must_use]
    pub fn snapshot(&self) -> MemoryMigrationSnapshot {
        self.state.clone()
    }

    /// Borrows the current migration journal.
    #[must_use]
    pub const fn journal(&self) -> Option<&MemoryMigrationJournal> {
        self.state.journal.as_ref()
    }

    /// Returns the current exact active catalog hash.
    #[must_use]
    pub const fn active_bundle_hash(&self) -> ContractBundleHash {
        self.state.active_bundle.bundle_hash()
    }

    /// Returns whether predecessor writes are durably retired in this model.
    #[must_use]
    pub const fn predecessor_writes_retired(&self) -> bool {
        self.state.retired_predecessor_writes
    }

    /// Borrows the retained-data archive. Gate A never places rows here.
    #[must_use]
    pub fn retained_archive(&self) -> &[StoredEntityRecordV1] {
        &self.state.retained_archive
    }

    /// Borrows successor index entries built in the private stage.
    #[must_use]
    pub fn index_entries(&self) -> &[StoredIndexEntryV2] {
        &self.state.indexes
    }

    /// Returns whether a required fresh projection generation is ready.
    #[must_use]
    pub fn projection_candidate_ready(&self, projection: ProjectionId) -> bool {
        self.state.projection_candidates.contains_key(&projection)
    }

    /// Returns the permanent migration evidence after final cutover.
    #[must_use]
    pub const fn migration_record(&self) -> Option<&MemoryMigrationRecord> {
        self.state.migration_record.as_ref()
    }

    /// Iterates all canonical entity rows.
    pub fn entities(&self) -> impl ExactSizeIterator<Item = &StoredEntityRecordV1> {
        self.state.entities.iter()
    }

    /// Adds a synthetic unresolved predecessor admission for negative model tests.
    #[must_use]
    pub fn with_unresolved_retiring_admission(mut self) -> Self {
        self.state.unresolved_retiring_admissions = true;
        self
    }

    fn row_position(&self, target: &riffdb_storage_api::EntityTarget) -> Result<usize, usize> {
        self.state
            .entities
            .binary_search_by(|candidate| candidate.target().cmp(target))
    }
}

impl MigrationStagePort for MemoryMigrationStage {
    fn active_bundle_hash(&self) -> ContractBundleHash {
        self.active_bundle_hash()
    }

    fn scan_migration_rows(
        &self,
        cursor: &MigrationScanCursor,
    ) -> Result<MigrationScanPage, MigrationStageError> {
        let start = cursor.exclusive_lower_bound().map_or(0, |target| {
            self.state
                .entities
                .partition_point(|candidate| candidate.target() <= target)
        });
        let end = start
            .saturating_add(MAX_MIGRATION_SCAN_ROWS)
            .min(self.state.entities.len());
        let rows = self.state.entities[start..end].to_vec();
        let next = (end < self.state.entities.len()).then(|| {
            MigrationScanCursor::after(
                rows.last()
                    .expect("a nonterminal page is nonempty")
                    .target()
                    .clone(),
            )
        });
        MigrationScanPage::new(rows, next)
    }

    fn migration_target_exists(
        &self,
        target: &riffdb_storage_api::EntityTarget,
    ) -> Result<bool, MigrationStageError> {
        Ok(self.row_position(target).is_ok())
    }

    fn has_unresolved_retiring_admissions(&self) -> Result<bool, MigrationStageError> {
        Ok(self.state.unresolved_retiring_admissions)
    }

    fn migration_journal_state(
        &self,
        migration: riffdb_types::MigrationBundleHash,
    ) -> Result<Option<MigrationJournalState>, MigrationStageError> {
        if self
            .state
            .journal
            .as_ref()
            .is_some_and(|journal| journal.migration() != migration || journal.is_complete())
        {
            return Err(MigrationStageError::Integrity);
        }
        Ok(self.state.journal.clone())
    }

    fn apply_migration_batch(&mut self, batch: MigrationBatch) -> Result<(), MigrationStageError> {
        if self.state.retired_predecessor_writes
            || self.state.journal.as_ref().is_some_and(|journal| {
                journal.migration() != batch.migration() || journal.is_complete()
            })
        {
            return Err(MigrationStageError::Integrity);
        }

        let mut positions = Vec::with_capacity(batch.mutations().len());
        let mut targets = BTreeSet::new();
        let mut index_keys = BTreeSet::new();
        for mutation in batch.mutations() {
            if !targets.insert(mutation.expected().source().target().clone()) {
                return Err(MigrationStageError::Integrity);
            }
            let position = self
                .row_position(mutation.expected().source().target())
                .map_err(|_| MigrationStageError::RowChanged)?;
            if !mutation.expected().matches(&self.state.entities[position]) {
                return Err(MigrationStageError::RowChanged);
            }
            for entry in mutation.rebuilt_indexes() {
                if !index_keys.insert(entry.key().clone())
                    || self
                        .state
                        .indexes
                        .iter()
                        .any(|current| current.key() == entry.key())
                {
                    return Err(MigrationStageError::Integrity);
                }
            }
            positions.push(position);
        }
        if self.state.journal.as_ref().is_some_and(|journal| {
            batch.checked_rows() < journal.checked_rows()
                || batch.changed_rows() < journal.changed_rows()
                || batch.checked_through() <= journal.checked_through()
        }) {
            return Err(MigrationStageError::Integrity);
        }

        let next_batch_count = self
            .state
            .journal
            .as_ref()
            .map_or(1, |journal| journal.batch_count().saturating_add(1));
        for (position, mutation) in positions.into_iter().zip(batch.mutations()) {
            if let Some(post_image) = mutation.post_image() {
                self.state.entities[position] = post_image.clone();
            }
            for entry in mutation.rebuilt_indexes() {
                self.state.indexes.push(entry.clone());
            }
        }
        self.state
            .indexes
            .sort_unstable_by(|left, right| left.key().cmp(right.key()));
        self.state.journal = Some(MigrationJournalState::new(
            batch.migration(),
            batch.checked_through().clone(),
            batch.checked_rows(),
            batch.changed_rows(),
            next_batch_count,
        )?);
        Ok(())
    }

    fn build_migration_projection_candidates(
        &mut self,
        projections: &[ProjectionId],
    ) -> Result<(), MigrationStageError> {
        for projection in projections {
            self.state
                .projection_candidates
                .insert(*projection, self.state.history.application_sequence);
        }
        Ok(())
    }

    fn validate_migration_stage(
        &self,
        candidate: ContractBundleHash,
        retained_parent_lineage: &[ContractBundleHash],
    ) -> Result<(), MigrationStageError> {
        if self.state.retired_predecessor_writes
            || self.state.migration_record.is_some()
            || self
                .state
                .journal
                .as_ref()
                .is_none_or(MigrationJournalState::is_complete)
            || self.state.entities.iter().any(|row| {
                row.schema_binding().bundle_hash() != candidate
                    && !retained_parent_lineage.contains(&row.schema_binding().bundle_hash())
            })
        {
            return Err(MigrationStageError::Integrity);
        }
        Ok(())
    }

    fn validate_migration_stage_structure(&self) -> Result<(), MigrationStageError> {
        if self.state.retired_predecessor_writes
            || self.state.migration_record.is_some()
            || self
                .state
                .journal
                .as_ref()
                .is_none_or(MigrationJournalState::is_complete)
        {
            return Err(MigrationStageError::Integrity);
        }
        Ok(())
    }

    fn finalize_migration(
        &mut self,
        cutover: MigrationCutover,
    ) -> Result<MigrationCutoverApplied, MigrationStageError> {
        let journal = self
            .state
            .journal
            .as_ref()
            .ok_or(MigrationStageError::Integrity)?;
        if self.state.active_bundle.bundle_hash() != cutover.parent()
            || journal.migration() != cutover.migration()
            || self.state.entities.iter().any(|row| {
                row.schema_binding().bundle_hash() != cutover.candidate().bundle_hash()
                    && !cutover
                        .retained_parent_lineage()
                        .contains(&row.schema_binding().bundle_hash())
            })
        {
            return Err(MigrationStageError::Integrity);
        }
        let allocation = self
            .state
            .administration_sequence
            .allocate_one()
            .map_err(|_| MigrationStageError::SequenceExhausted)?;
        let record = MigrationRecordEvidence::new(
            cutover.parent(),
            cutover.candidate().bundle_hash(),
            cutover.migration(),
            allocation.assigned(),
        );
        self.state.active_bundle = cutover.candidate().clone();
        self.state.retired_predecessor_writes = true;
        self.state.migration_record = Some(record);
        self.state.administration_sequence = allocation.next();
        self.state
            .journal
            .as_mut()
            .expect("journal checked before infallible application")
            .mark_complete()?;
        Ok(MigrationCutoverApplied::new(allocation.assigned()))
    }
}
