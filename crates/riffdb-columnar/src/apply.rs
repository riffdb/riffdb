//! Commit apply protocol with supersession deferral and frontier holdback (D4).

use std::collections::BTreeMap;
use std::sync::Arc;

use riffdb_storage_api::{
    AuthoritativePointReader, AuthoritativeScanReader, CommitScanPageV1, CommitScanRequest,
    CommittedEntityReferenceV2, EntityTarget, StorageScanLimit, StoredEntityRecordV1,
};
use riffdb_types::{CommitSequence, EntityVersion, FrontierPosition};

use crate::definition::RegisteredDefinition;
use crate::error::ColumnarError;
use crate::store::{
    ColumnarSnapshot, LiveRow, OrgKey, PrimaryKeyBytes, WorkingState, project_cells,
};

/// Progress reported after one `apply_available` pull.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyProgress {
    /// Last commit sequence fully processed (may lead the published frontier).
    pub processed: FrontierPosition,
    /// Visible frontier of the published snapshot.
    pub published_frontier: FrontierPosition,
    /// Number of entities waiting for their superseding commit.
    pub deferred_set_size: usize,
    /// Whether the scan reached the frozen ExactEnd fence.
    pub caught_up: bool,
}

/// Mutable apply machinery shared with the engine.
pub(crate) struct ApplyState {
    pub definition: RegisteredDefinition,
    pub working: WorkingState,
    pub published: Arc<ColumnarSnapshot>,
    pub deferred: BTreeMap<EntityTargetKey, EntityVersion>,
    /// When true (tests only), publish after each entity inside a commit.
    pub(crate) test_publish_mid_commit: bool,
    /// When true (tests only), apply raced-ahead live records immediately.
    pub(crate) test_skip_holdback: bool,
    /// When true (tests only), skip supersession tombstone on replace.
    pub(crate) test_skip_tombstone: bool,
    /// Test observability: total live rows in each published snapshot (all orgs).
    pub(crate) test_publish_row_counts: Vec<usize>,
}

/// Orderable wrapper for EntityTarget (keyed by entity key bytes).
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct EntityTargetKey {
    entity_type: u32,
    key: Vec<u8>,
}

impl EntityTargetKey {
    fn from_target(target: &EntityTarget) -> Self {
        Self {
            entity_type: target.entity_type_id().get(),
            key: target.key().as_bytes().to_vec(),
        }
    }
}

impl ApplyState {
    pub(crate) fn new(definition: RegisteredDefinition) -> Self {
        Self {
            definition,
            working: WorkingState::default(),
            published: Arc::new(ColumnarSnapshot::empty()),
            deferred: BTreeMap::new(),
            test_publish_mid_commit: false,
            test_skip_holdback: false,
            test_skip_tombstone: false,
            test_publish_row_counts: Vec::new(),
        }
    }

    /// Pulls available commits and applies them under the D4 protocol.
    pub(crate) fn apply_available(
        &mut self,
        reader: &(impl AuthoritativeScanReader + AuthoritativePointReader),
    ) -> Result<ApplyProgress, ColumnarError> {
        let limit = StorageScanLimit::new(64).ok_or(ColumnarError::Integrity("scan limit"))?;
        let mut scan = match self.working.processed {
            FrontierPosition::BeforeFirst => CommitScanRequest::initial(limit),
            FrontierPosition::AppliedThrough(sequence) => {
                CommitScanRequest::initial_after(sequence, limit)
            }
        };
        let caught_up;
        loop {
            let page = reader
                .scan_commits(scan)
                .map_err(|error| ColumnarError::Storage(error.to_string()))?;
            let inclusive_upper = page.inclusive_upper();
            if inclusive_upper < self.working.processed {
                return Err(ColumnarError::Integrity(
                    "scan upper before processed frontier",
                ));
            }
            for charged in page.records() {
                let commit = charged.value();
                let sequence = commit.commit_sequence();
                if FrontierPosition::AppliedThrough(sequence) <= self.working.processed {
                    continue;
                }
                if !is_exact_successor(self.working.processed, sequence) {
                    return Err(ColumnarError::Integrity("commit sequence gap"));
                }
                self.apply_commit(reader, commit.entity_references(), sequence)?;
            }
            match page {
                CommitScanPageV1::Page {
                    next_after,
                    inclusive_upper,
                    ..
                } => {
                    let FrontierPosition::AppliedThrough(upper) = inclusive_upper else {
                        return Err(ColumnarError::Integrity("page upper missing sequence"));
                    };
                    scan = CommitScanRequest::continuing(next_after, upper, limit)
                        .map_err(|_| ColumnarError::Integrity("continue request"))?;
                }
                CommitScanPageV1::ExactEnd { .. } => {
                    // Fence check: after ExactEnd, processed must equal the fence when
                    // the fence is AppliedThrough and we consumed all records, or both
                    // BeforeFirst when the log is empty.
                    match (self.working.processed, inclusive_upper) {
                        (p, u) if p == u => caught_up = true,
                        (FrontierPosition::BeforeFirst, FrontierPosition::BeforeFirst) => {
                            caught_up = true;
                        }
                        _ => {
                            if self.working.processed < inclusive_upper {
                                return Err(ColumnarError::Integrity(
                                    "exact-end fence not reached",
                                ));
                            }
                            caught_up = true;
                        }
                    }
                    break;
                }
            }
        }
        Ok(ApplyProgress {
            processed: self.working.processed,
            published_frontier: self.published.visible_frontier,
            deferred_set_size: self.deferred.len(),
            caught_up,
        })
    }

    fn apply_commit(
        &mut self,
        reader: &impl AuthoritativePointReader,
        references: &[CommittedEntityReferenceV2],
        sequence: CommitSequence,
    ) -> Result<(), ColumnarError> {
        let projected_type = self.definition.entity_type_id();
        for reference in references {
            if reference.target().entity_type_id() != projected_type {
                continue;
            }
            self.apply_entity_reference(reader, reference)?;
            if self.test_publish_mid_commit {
                // Neuter path: publish between entity effects of one commit.
                self.maybe_publish(FrontierPosition::AppliedThrough(sequence));
            }
        }
        self.working.processed = FrontierPosition::AppliedThrough(sequence);
        if !self.test_publish_mid_commit {
            self.maybe_publish(FrontierPosition::AppliedThrough(sequence));
        }
        Ok(())
    }

    fn apply_entity_reference(
        &mut self,
        reader: &impl AuthoritativePointReader,
        reference: &CommittedEntityReferenceV2,
    ) -> Result<(), ColumnarError> {
        let record = reader
            .read_entity(reference.target())
            .map_err(|error| ColumnarError::Storage(error.to_string()))?
            .ok_or(ColumnarError::Integrity(
                "entity absent for commit reference",
            ))?;

        if reference.matches(&record) {
            self.apply_matched_record(&record)?;
            let key = EntityTargetKey::from_target(reference.target());
            if let Some(deferred_version) = self.deferred.get(&key).copied()
                && record.entity_version().get() >= deferred_version.get()
            {
                self.deferred.remove(&key);
            }
            return Ok(());
        }

        let live_version = record.entity_version();
        if live_version.get() > reference.entity_version().get() {
            if self.test_skip_holdback {
                // Neuter: apply the raced-ahead record immediately.
                self.apply_matched_record(&record)?;
                return Ok(());
            }
            self.deferred.insert(
                EntityTargetKey::from_target(reference.target()),
                live_version,
            );
            return Ok(());
        }

        // Same version but hash mismatch, or live version behind the reference.
        Err(ColumnarError::Integrity(
            "entity reference does not match live record and is not a forward race",
        ))
    }

    fn apply_matched_record(&mut self, record: &StoredEntityRecordV1) -> Result<(), ColumnarError> {
        let fields = record.fields().fields();
        let (org_value, cells) = project_cells(
            fields,
            self.definition.projected_fields(),
            self.definition.org_scope_field(),
        )?;
        let org = OrgKey::from_value(&org_value)?;
        let key = PrimaryKeyBytes::from_entity_key_bytes(record.target().key().as_bytes().to_vec());
        let row = LiveRow {
            entity_version: record.entity_version(),
            cells,
        };

        // Idempotence: skip if an equal-or-newer version is already present in delta.
        if let Some(org_delta) = self.working.delta.get(&org)
            && let Some(RowState::Live(existing)) = org_delta.get(&key)
            && !crate::store::supersession_should_replace(
                existing.entity_version.get(),
                row.entity_version.get(),
            )
        {
            // Existing is newer than incoming — idempotent skip.
            return Ok(());
        }
        if let Some(org_delta) = self.working.delta.get(&org)
            && let Some(RowState::Live(existing)) = org_delta.get(&key)
            && existing.entity_version.get() == row.entity_version.get()
        {
            // Equal version replay.
            return Ok(());
        }

        let mask_prior = !self.test_skip_tombstone;
        self.working.upsert_live(org, key, row, mask_prior);
        Ok(())
    }

    fn maybe_publish(&mut self, candidate: FrontierPosition) {
        if self.test_skip_holdback {
            self.publish(candidate);
            return;
        }
        if self.deferred.is_empty() {
            self.publish(candidate);
        }
        // Else hold back: keep previous published snapshot.
    }

    fn publish(&mut self, frontier: FrontierPosition) {
        let snapshot = self.working.to_snapshot(frontier);
        let mut orgs: std::collections::BTreeSet<_> = snapshot.delta.keys().cloned().collect();
        for segment in &snapshot.segments {
            orgs.insert(segment.org.clone());
        }
        let mut total = 0usize;
        for org in &orgs {
            total = total.saturating_add(snapshot.merged_org(org).len());
        }
        self.test_publish_row_counts.push(total);
        self.published = Arc::new(snapshot);
    }
}

use crate::store::RowState;

const fn is_exact_successor(frontier: FrontierPosition, sequence: CommitSequence) -> bool {
    match frontier {
        FrontierPosition::BeforeFirst => sequence.get() == 1,
        FrontierPosition::AppliedThrough(previous) => match previous.checked_next() {
            Some(expected) => expected.get() == sequence.get(),
            None => false,
        },
    }
}

// FrontierPosition ordering for comparisons used above.
// riffdb_types::FrontierPosition derives PartialOrd if listed — verify.
// If not, we only use == and the storage API's PartialOrd on inclusive_upper.

#[cfg(test)]
mod successor_tests {
    use super::*;
    use riffdb_types::CommitSequence;

    #[test]
    fn exact_successor_from_before_first() {
        assert!(is_exact_successor(
            FrontierPosition::BeforeFirst,
            CommitSequence::new(1).expect("1")
        ));
        assert!(!is_exact_successor(
            FrontierPosition::BeforeFirst,
            CommitSequence::new(2).expect("2")
        ));
    }
}
