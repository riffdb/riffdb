//! API-neutral readers over one pinned in-memory state generation.

use riffdb_storage_api::{
    AuthoritativeEntityPartitionScanPage, AuthoritativeEntityPartitionScanRequest,
    AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest, AuthoritativePointReader,
    AuthoritativeScanReader, CommitScanPageV1, CommitScanRequest, EncodedPageItem, EntityTarget,
    FilteredAuthoritativeIndexScanPage, FilteredAuthoritativeIndexScanRequest,
    FilteredAuthoritativeScanReader, IdempotencyIdentity, OwnedSnapshotReader,
    PartitionIndexTarget, ReadSnapshot, SnapshotFenceReader, SnapshotIndexDirectionV1,
    SnapshotIndexRangePageV1, SnapshotIndexRangeRequestV1, SnapshotReader, SnapshotRequest,
    StorageError, StorageErrorKind, StoredCommitRecordV1, StoredDurableEventV1,
    StoredEntityRecordV1, StoredOutcomeV1, StoredProvenanceRecordV1, VectorEvidenceIndexPageV1,
    VectorEvidenceIndexRepository, VectorEvidenceIndexScanRequestV1, VectorHealthObservationV1,
    VectorObservationCountsV1, VectorObservationRepository, VectorObservationTargetV1,
};
use riffdb_types::{CommitSequence, ContractLineage, EventId, ProvenanceId};

use crate::state::MemoryIndexEntry;
use crate::store::MemoryOperationalPorts;

/// One owned immutable copy of the memory backend's bounded reference state.
pub struct MemoryOwnedSnapshot {
    reader: MemoryOperationalPorts,
}

impl OwnedSnapshotReader for MemoryOperationalPorts {
    type Snapshot<'a> = MemoryOwnedSnapshot;

    fn open_owned_snapshot(&self) -> Result<Self::Snapshot<'_>, StorageError> {
        self.read(|state| {
            Ok(MemoryOwnedSnapshot {
                reader: MemoryOperationalPorts::immutable_read_view(state.clone()),
            })
        })
    }
}

impl SnapshotFenceReader for MemoryOwnedSnapshot {
    fn application_frontier(&self) -> Result<Option<CommitSequence>, StorageError> {
        self.reader.read(|state| {
            Ok(state
                .commits
                .last()
                .map(StoredCommitRecordV1::commit_sequence))
        })
    }

    fn index_epoch(&self, target: &PartitionIndexTarget) -> Result<u64, StorageError> {
        self.reader.read(|state| {
            Ok(state
                .index_epochs
                .binary_search_by(|row| row.target().cmp(target))
                .ok()
                .map_or(0, |index| state.index_epochs[index].epoch().get()))
        })
    }

    fn scan_snapshot_index_range(
        &self,
        request: SnapshotIndexRangeRequestV1,
    ) -> Result<SnapshotIndexRangePageV1, StorageError> {
        self.reader.read(|state| {
            let wanted = usize::from(request.limit().get());
            let in_bounds = |entry: &&MemoryIndexEntry| {
                let key = entry.key().as_bytes();
                let lower =
                    key > request.lower() || (request.lower_inclusive() && key == request.lower());
                let upper =
                    key < request.upper() || (request.upper_inclusive() && key == request.upper());
                let after = request
                    .after()
                    .is_none_or(|after| match request.direction() {
                        SnapshotIndexDirectionV1::Forward => key > after,
                        SnapshotIndexDirectionV1::Reverse => key < after,
                    });
                lower && upper && after
            };
            let rows = state.index_entries.iter().filter(in_bounds);
            let ordered = match request.direction() {
                SnapshotIndexDirectionV1::Forward => rows.collect::<Vec<_>>(),
                SnapshotIndexDirectionV1::Reverse => rows.rev().collect::<Vec<_>>(),
            };
            let mut entries = Vec::with_capacity(wanted);
            let mut has_more = false;
            for row in ordered {
                let record = row
                    .current_record()
                    .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
                if entries.len() == wanted {
                    has_more = true;
                    break;
                }
                entries.push(EncodedPageItem::new(
                    record.clone(),
                    row.encoded_content_charge(),
                ));
            }
            SnapshotIndexRangePageV1::new(&request, entries, has_more).map_err(value_error)
        })
    }
}

impl AuthoritativePointReader for MemoryOwnedSnapshot {
    fn read_entity(
        &self,
        target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        AuthoritativePointReader::read_entity(&self.reader, target)
    }

    fn read_stored_outcome(
        &self,
        identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredOutcomeV1>, StorageError> {
        AuthoritativePointReader::read_stored_outcome(&self.reader, identity)
    }

    fn read_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<StoredCommitRecordV1>, StorageError> {
        AuthoritativePointReader::read_commit(&self.reader, sequence)
    }

    fn read_provenance(
        &self,
        provenance_id: ProvenanceId,
    ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
        AuthoritativePointReader::read_provenance(&self.reader, provenance_id)
    }

    fn read_durable_event(
        &self,
        event_id: EventId,
    ) -> Result<Option<StoredDurableEventV1>, StorageError> {
        AuthoritativePointReader::read_durable_event(&self.reader, event_id)
    }
}

impl SnapshotReader for MemoryOwnedSnapshot {
    fn read_snapshot(&self, request: SnapshotRequest) -> Result<ReadSnapshot, StorageError> {
        SnapshotReader::read_snapshot(&self.reader, request)
    }
}

impl AuthoritativeScanReader for MemoryOwnedSnapshot {
    fn scan_entity_partition(
        &self,
        request: AuthoritativeEntityPartitionScanRequest,
    ) -> Result<AuthoritativeEntityPartitionScanPage, StorageError> {
        AuthoritativeScanReader::scan_entity_partition(&self.reader, request)
    }

    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        AuthoritativeScanReader::scan_index(&self.reader, request)
    }

    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        AuthoritativeScanReader::scan_commits(&self.reader, request)
    }
}

impl FilteredAuthoritativeScanReader for MemoryOwnedSnapshot {
    fn scan_index_filtered(
        &self,
        request: FilteredAuthoritativeIndexScanRequest,
    ) -> Result<FilteredAuthoritativeIndexScanPage, StorageError> {
        FilteredAuthoritativeScanReader::scan_index_filtered(&self.reader, request)
    }
}

impl VectorObservationRepository for MemoryOwnedSnapshot {
    fn read_vector_observation(
        &self,
        target: &VectorObservationTargetV1,
    ) -> Result<Option<VectorObservationCountsV1>, StorageError> {
        VectorObservationRepository::read_vector_observation(&self.reader, target)
    }

    fn read_vector_health_observation(
        &self,
        lineage: &ContractLineage,
    ) -> Result<Option<VectorHealthObservationV1>, StorageError> {
        VectorObservationRepository::read_vector_health_observation(&self.reader, lineage)
    }
}

impl VectorEvidenceIndexRepository for MemoryOwnedSnapshot {
    fn scan_vector_evidence_index(
        &self,
        request: &VectorEvidenceIndexScanRequestV1,
    ) -> Result<VectorEvidenceIndexPageV1, StorageError> {
        VectorEvidenceIndexRepository::scan_vector_evidence_index(&self.reader, request)
    }
}

const fn invariant() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}

const fn limit_exceeded() -> StorageError {
    storage_error(StorageErrorKind::LimitExceeded)
}

const fn storage_error(kind: StorageErrorKind) -> StorageError {
    StorageError::new(kind, None)
}

const fn value_error(error: riffdb_storage_api::StorageValueError) -> StorageError {
    match error {
        riffdb_storage_api::StorageValueError::LimitExceeded
        | riffdb_storage_api::StorageValueError::SizeOverflow => limit_exceeded(),
        _ => invariant(),
    }
}
