//! Independent atomic-state model for authoritative command histories.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use riffdb_storage_api::{
    ApplicationSequenceAllocator, AtomicCommandRecordSet, ExpectedEntityState, IdempotencyIdentity,
    IdempotencyIdentityKey, IndexEntryMutationV1, IndexEpochPosition, PartitionIndexTarget,
    StoredAdmissionStateV1, StoredCommitRecordV1, StoredDurableEventV1, StoredEntityRecordV1,
    StoredExecutionFailedV1, StoredIndexEntryV2, StoredIndexEpochV1, StoredOutboxIntentV1,
    StoredOutcomeV1, StoredPendingAdmissionV1, StoredProvenanceRecordV1,
};
use riffdb_types::{CommitSequence, EventId, IndexEntryKey, ProvenanceId};

/// Closed defects detected by the independent command-history model.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelApplyError {
    /// An identity could not produce its already-bounded canonical storage key.
    InvalidIdentityKey,
    /// A pending admission already exists under the same identity.
    AdmissionAlreadyExists,
    /// The exact pending admission was absent or different.
    PendingMismatch,
    /// The record sequence did not equal the model allocator's next value.
    SequenceMismatch,
    /// A commit sequence was already present.
    DuplicateCommit,
    /// An entity's expected absence or version did not match current state.
    EntityPriorMismatch,
    /// A requested index deletion named no current entry.
    MissingIndexEntry,
    /// An epoch advance did not match the current exact-prefix position.
    EpochPriorMismatch,
    /// An immutable provenance identity was already present.
    DuplicateProvenance,
    /// A durable event identity was already present.
    DuplicateEvent,
    /// An authoritative outbox intent identity was already present.
    DuplicateOutboxIntent,
}

impl fmt::Display for ModelApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidIdentityKey => "invalid idempotency identity key",
            Self::AdmissionAlreadyExists => "admission identity already exists",
            Self::PendingMismatch => "pending admission does not match",
            Self::SequenceMismatch => "application sequence does not match",
            Self::DuplicateCommit => "commit sequence already exists",
            Self::EntityPriorMismatch => "entity prior state does not match",
            Self::MissingIndexEntry => "index deletion target is absent",
            Self::EpochPriorMismatch => "index epoch prior state does not match",
            Self::DuplicateProvenance => "provenance identity already exists",
            Self::DuplicateEvent => "event identity already exists",
            Self::DuplicateOutboxIntent => "outbox intent identity already exists",
        })
    }
}

impl Error for ModelApplyError {}

/// Atomic reference state for the authoritative records changed by commands.
///
/// This deliberately uses standard ordered maps and applies to a cloned state.
/// It shares semantic DTOs with an engine but no engine mutation code or table
/// layout, making it suitable for differential history assertions.
#[derive(Clone)]
pub struct AuthoritativeCommandModel {
    application_sequence: ApplicationSequenceAllocator,
    admissions: BTreeMap<IdempotencyIdentityKey, StoredAdmissionStateV1>,
    entities: BTreeMap<riffdb_storage_api::EntityTarget, StoredEntityRecordV1>,
    index_entries: BTreeMap<IndexEntryKey, StoredIndexEntryV2>,
    index_epochs: BTreeMap<PartitionIndexTarget, StoredIndexEpochV1>,
    outcomes: BTreeMap<CommitSequence, StoredOutcomeV1>,
    commits: BTreeMap<CommitSequence, StoredCommitRecordV1>,
    provenance: BTreeMap<ProvenanceId, StoredProvenanceRecordV1>,
    events: BTreeMap<EventId, StoredDurableEventV1>,
    outbox_intents: BTreeMap<EventId, StoredOutboxIntentV1>,
}

impl Default for AuthoritativeCommandModel {
    fn default() -> Self {
        Self {
            application_sequence: ApplicationSequenceAllocator::initial(),
            admissions: BTreeMap::new(),
            entities: BTreeMap::new(),
            index_entries: BTreeMap::new(),
            index_epochs: BTreeMap::new(),
            outcomes: BTreeMap::new(),
            commits: BTreeMap::new(),
            provenance: BTreeMap::new(),
            events: BTreeMap::new(),
            outbox_intents: BTreeMap::new(),
        }
    }
}

impl AuthoritativeCommandModel {
    /// Creates an empty model whose next application sequence is one.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts one pending admission without changing application sequencing.
    pub fn admit_pending(
        &mut self,
        pending: StoredPendingAdmissionV1,
    ) -> Result<(), ModelApplyError> {
        let key = identity_key(pending.identity())?;
        if self.admissions.contains_key(&key) {
            return Err(ModelApplyError::AdmissionAlreadyExists);
        }
        self.admissions
            .insert(key, StoredAdmissionStateV1::Pending(pending));
        Ok(())
    }

    /// Atomically replaces an equal pending admission with deterministic failure.
    pub fn terminalize_execution_failure(
        &mut self,
        failure: StoredExecutionFailedV1,
    ) -> Result<(), ModelApplyError> {
        let key = identity_key(failure.pending().identity())?;
        match self.admissions.get(&key) {
            Some(StoredAdmissionStateV1::Pending(current)) if current == failure.pending() => {}
            _ => return Err(ModelApplyError::PendingMismatch),
        }
        self.admissions
            .insert(key, StoredAdmissionStateV1::ExecutionFailed(failure));
        Ok(())
    }

    /// Applies one already checked record graph to a private clone, then publishes it.
    pub fn apply_command(
        &mut self,
        records: &AtomicCommandRecordSet,
    ) -> Result<(), ModelApplyError> {
        let mut next = self.clone();
        next.apply_command_in_place(records)?;
        *self = next;
        Ok(())
    }

    fn apply_command_in_place(
        &mut self,
        records: &AtomicCommandRecordSet,
    ) -> Result<(), ModelApplyError> {
        let sequence = records.assignment().assigned();
        if self.application_sequence != ApplicationSequenceAllocator::next(sequence)
            || records.next_application_sequence() != records.assignment().next_allocator()
        {
            return Err(ModelApplyError::SequenceMismatch);
        }
        if self.commits.contains_key(&sequence) {
            return Err(ModelApplyError::DuplicateCommit);
        }

        let pending = records.expected_pending();
        let admission_key = identity_key(pending.identity())?;
        match self.admissions.get(&admission_key) {
            Some(StoredAdmissionStateV1::Pending(current)) if current == pending => {}
            _ => return Err(ModelApplyError::PendingMismatch),
        }
        if self
            .provenance
            .contains_key(&records.provenance().provenance_id())
        {
            return Err(ModelApplyError::DuplicateProvenance);
        }

        for mutation in records.entities() {
            let current = self.entities.get(mutation.post_image().target());
            let matches = match (mutation.expected(), current) {
                (ExpectedEntityState::Absent, None) => true,
                (ExpectedEntityState::Present(expected), Some(record)) => {
                    record.entity_version() == expected
                }
                _ => false,
            };
            if !matches {
                return Err(ModelApplyError::EntityPriorMismatch);
            }
        }
        for advance in records.index_epochs() {
            let current = self
                .index_epochs
                .get(advance.target())
                .map_or(IndexEpochPosition::BeforeFirst, |record| {
                    IndexEpochPosition::Value(record.epoch())
                });
            if current != advance.prior() {
                return Err(ModelApplyError::EpochPriorMismatch);
            }
        }
        for event in records.events() {
            if self.events.contains_key(&event.event_id()) {
                return Err(ModelApplyError::DuplicateEvent);
            }
        }
        for intent in records.outbox_intents() {
            if self.outbox_intents.contains_key(&intent.event_id()) {
                return Err(ModelApplyError::DuplicateOutboxIntent);
            }
        }

        for mutation in records.entities() {
            self.entities.insert(
                mutation.post_image().target().clone(),
                mutation.post_image().clone(),
            );
        }
        for mutation in records.index_entries() {
            match mutation {
                IndexEntryMutationV1::Delete(key) => {
                    if self.index_entries.remove(key).is_none() {
                        return Err(ModelApplyError::MissingIndexEntry);
                    }
                }
                IndexEntryMutationV1::Put(record) => {
                    self.index_entries
                        .insert(record.key().clone(), record.clone());
                }
            }
        }
        for advance in records.index_epochs() {
            self.index_epochs
                .insert(advance.target().clone(), advance.post_image().clone());
        }
        self.admissions.insert(
            admission_key,
            StoredAdmissionStateV1::StoredOutcome(records.stored_outcome().clone()),
        );
        self.outcomes
            .insert(sequence, records.stored_outcome().clone());
        self.commits.insert(sequence, records.commit().clone());
        self.provenance.insert(
            records.provenance().provenance_id(),
            records.provenance().clone(),
        );
        for event in records.events() {
            self.events.insert(event.event_id(), event.clone());
        }
        for intent in records.outbox_intents() {
            self.outbox_intents
                .insert(intent.event_id(), intent.clone());
        }
        self.application_sequence = records.next_application_sequence();
        Ok(())
    }

    /// Returns the exact modeled application allocator state.
    #[must_use]
    pub const fn application_sequence(&self) -> ApplicationSequenceAllocator {
        self.application_sequence
    }

    /// Looks up one modeled admission state by its exact identity.
    pub fn admission(
        &self,
        identity: &IdempotencyIdentity,
    ) -> Result<Option<&StoredAdmissionStateV1>, ModelApplyError> {
        Ok(self.admissions.get(&identity_key(identity)?))
    }

    /// Looks up one modeled entity post-image.
    #[must_use]
    pub fn entity(
        &self,
        target: &riffdb_storage_api::EntityTarget,
    ) -> Option<&StoredEntityRecordV1> {
        self.entities.get(target)
    }

    /// Looks up one modeled index-entry post-image.
    #[must_use]
    pub fn index_entry(&self, key: &IndexEntryKey) -> Option<&StoredIndexEntryV2> {
        self.index_entries.get(key)
    }

    /// Looks up one modeled partition/index generation post-image.
    #[must_use]
    pub fn index_epoch(&self, target: &PartitionIndexTarget) -> Option<&StoredIndexEpochV1> {
        self.index_epochs.get(target)
    }

    /// Looks up one modeled terminal outcome by application sequence.
    #[must_use]
    pub fn outcome(&self, sequence: CommitSequence) -> Option<&StoredOutcomeV1> {
        self.outcomes.get(&sequence)
    }

    /// Looks up one modeled commit record by application sequence.
    #[must_use]
    pub fn commit(&self, sequence: CommitSequence) -> Option<&StoredCommitRecordV1> {
        self.commits.get(&sequence)
    }

    /// Looks up one modeled immutable provenance record.
    #[must_use]
    pub fn provenance(&self, id: ProvenanceId) -> Option<&StoredProvenanceRecordV1> {
        self.provenance.get(&id)
    }

    /// Looks up one modeled durable event.
    #[must_use]
    pub fn event(&self, id: EventId) -> Option<&StoredDurableEventV1> {
        self.events.get(&id)
    }

    /// Looks up one modeled authoritative outbox intent.
    #[must_use]
    pub fn outbox_intent(&self, id: EventId) -> Option<&StoredOutboxIntentV1> {
        self.outbox_intents.get(&id)
    }

    /// Returns the number of modeled commits.
    #[must_use]
    pub fn commit_count(&self) -> usize {
        self.commits.len()
    }
}

fn identity_key(identity: &IdempotencyIdentity) -> Result<IdempotencyIdentityKey, ModelApplyError> {
    identity
        .storage_key()
        .map_err(|_| ModelApplyError::InvalidIdentityKey)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_model_starts_before_sequence_one() {
        let model = AuthoritativeCommandModel::new();
        assert_eq!(
            model.application_sequence(),
            ApplicationSequenceAllocator::initial()
        );
        assert_eq!(model.commit_count(), 0);
    }

    #[test]
    fn model_errors_are_closed_and_payload_free() {
        let variants = [
            ModelApplyError::InvalidIdentityKey,
            ModelApplyError::AdmissionAlreadyExists,
            ModelApplyError::PendingMismatch,
            ModelApplyError::SequenceMismatch,
            ModelApplyError::DuplicateCommit,
            ModelApplyError::EntityPriorMismatch,
            ModelApplyError::MissingIndexEntry,
            ModelApplyError::EpochPriorMismatch,
            ModelApplyError::DuplicateProvenance,
            ModelApplyError::DuplicateEvent,
            ModelApplyError::DuplicateOutboxIntent,
        ];
        for error in variants {
            assert!(!error.to_string().is_empty());
        }
    }
}
