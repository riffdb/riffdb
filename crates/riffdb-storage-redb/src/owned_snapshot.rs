//! API-neutral readers over one pinned composite read view.

use riffdb_storage_api::{
    AuthoritativeEntityPartitionScanPage, AuthoritativeEntityPartitionScanRequest,
    AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest, AuthoritativePointReader,
    AuthoritativeScanReader, CommitScanPageV1, CommitScanRequest, EncodedPageItem, EntityTarget,
    FilteredAuthoritativeIndexScanPage, FilteredAuthoritativeIndexScanRequest,
    FilteredAuthoritativeScanReader, IdempotencyIdentity, IndexEpochPosition,
    IndexPartitionFilterScope, IndexRangeEntry, MAX_COMMIT_SCAN_PAGE_BYTES,
    MAX_INDEX_SCAN_INSPECTED_BYTES, MAX_INDEX_SCAN_INSPECTED_ENTRIES, MAX_SCAN_PAGE_BYTES,
    OwnedSnapshotReader, PartitionIndexTarget, ReadSnapshot, SnapshotFenceReader,
    SnapshotIndexDirectionV1, SnapshotIndexRangePageV1, SnapshotIndexRangeRequestV1,
    SnapshotReader, SnapshotRequest, StorageError, StorageErrorKind, StoredCommitRecordV1,
    StoredDurableEventV1, StoredEntityRecordV1, StoredOutcomeV1, StoredProvenanceRecordV1,
    VectorEvidenceIndexPageV1, VectorEvidenceIndexRepository, VectorEvidenceIndexScanRequestV1,
    VectorHealthObservationV1, VectorObservationCountsV1, VectorObservationRepository,
    VectorObservationTargetV1,
};
use riffdb_types::{CommitSequence, ContractLineage, EventId, FrontierPosition, ProvenanceId};

use crate::codec::{
    IdempotencyRecordV1, decode_command_locator_v1, decode_durable_event_v1,
    decode_entity_record_v1, decode_idempotency_record_v1, decode_index_entry_v2,
    decode_index_epoch_v1, decode_provenance_record_v1, decode_vector_evidence_index_v1,
    decode_vector_health_observation_v1, decode_vector_observation_v1,
};
use crate::command_authority::{
    CommandAuthorityMember, command_member_at_access, commit_at_access,
    commits_in_physical_row_access,
};
use crate::error::storage_error;
use crate::journal::JournalTable;
use crate::keys::{
    decode_application_sequence_key, decode_index_entry_key, decode_vector_evidence_index_key,
    encode_application_sequence_key, encode_event_key, encode_idempotency_key,
    encode_partition_index_key, encode_provenance_key, encode_vector_evidence_index_key,
    encode_vector_evidence_index_prefix, encode_vector_health_observation_key,
    encode_vector_observation_key,
};
use crate::reads::{
    read_entity_record_access, read_epoch_position_access, read_snapshot_from_access,
};
use crate::shared_ports::RedbSharedPorts;
use crate::store::{RedbOperationalPorts, RedbReadAccess};

/// One move-only immutable redb view. No redb transaction or iterator escapes.
pub struct RedbOwnedSnapshot {
    access: RedbReadAccess,
}

impl OwnedSnapshotReader for RedbOperationalPorts {
    type Snapshot<'a> = RedbOwnedSnapshot;

    fn open_owned_snapshot(&self) -> Result<Self::Snapshot<'_>, StorageError> {
        Ok(RedbOwnedSnapshot {
            access: self.begin_composite_read()?,
        })
    }
}

impl OwnedSnapshotReader for RedbSharedPorts {
    type Snapshot<'a> = RedbOwnedSnapshot;

    fn open_owned_snapshot(&self) -> Result<Self::Snapshot<'_>, StorageError> {
        self.operational().open_owned_snapshot()
    }
}

impl SnapshotFenceReader for RedbOwnedSnapshot {
    fn application_frontier(&self) -> Result<Option<CommitSequence>, StorageError> {
        self.access.application_frontier()
    }

    fn index_epoch(&self, target: &PartitionIndexTarget) -> Result<u64, StorageError> {
        let key = encode_partition_index_key(target);
        let Some(encoded) = self.access.read_value(JournalTable::IndexEpochs, &key)? else {
            return Ok(0);
        };
        let epoch = decode_index_epoch_v1(&encoded)?.into_parts().0;
        if epoch.target() != target {
            return Err(corrupt());
        }
        Ok(epoch.epoch().get())
    }

    fn scan_snapshot_index_range(
        &self,
        request: SnapshotIndexRangeRequestV1,
    ) -> Result<SnapshotIndexRangePageV1, StorageError> {
        let wanted = usize::from(request.limit().get());
        let mut upper = request.upper().to_vec();
        if request.upper_inclusive() {
            upper.push(0);
        }
        let lower = request.lower();
        let raw = match request.direction() {
            SnapshotIndexDirectionV1::Forward => self.access.read_range(
                JournalTable::SecondaryIndexes,
                request.after().unwrap_or(lower),
                &upper,
                wanted.saturating_add(2),
            )?,
            SnapshotIndexDirectionV1::Reverse => self.access.read_range_reverse(
                JournalTable::SecondaryIndexes,
                lower,
                request.after().unwrap_or(upper.as_slice()),
                wanted.saturating_add(2),
            )?,
        };
        let mut entries = Vec::with_capacity(wanted);
        let mut has_more = false;
        for (physical, encoded) in raw {
            let key = decode_index_entry_key(&physical).map_err(|_| corrupt())?;
            if request.after().is_some_and(|after| after == key.as_bytes())
                || (!request.lower_inclusive() && key.as_bytes() == request.lower())
                || (!request.upper_inclusive() && key.as_bytes() == request.upper())
            {
                continue;
            }
            let decoded = decode_index_entry_v2(&encoded)?;
            if decoded.value().key() != &key {
                return Err(corrupt());
            }
            if entries.len() == wanted {
                has_more = true;
                break;
            }
            let (entry, charge) = decoded.into_parts();
            entries.push(EncodedPageItem::new(entry, charge));
        }
        SnapshotIndexRangePageV1::new(&request, entries, has_more).map_err(value_error)
    }
}

impl AuthoritativePointReader for RedbOwnedSnapshot {
    fn read_entity(
        &self,
        target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        read_entity_record_access(&self.access, target)
    }

    fn read_stored_outcome(
        &self,
        identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredOutcomeV1>, StorageError> {
        let key = identity.storage_key().map_err(|_| invariant())?;
        let encoded_key = encode_idempotency_key(&key);
        let encoded = match self
            .access
            .read_value(JournalTable::Idempotency, encoded_key)?
        {
            Some(encoded) => encoded,
            None => {
                let Some(locator) = self
                    .access
                    .read_value(JournalTable::IdempotencyLocators, encoded_key)?
                else {
                    return Ok(None);
                };
                let locator = decode_command_locator_v1(&locator)?.into_parts().0;
                let command = command_member_at_access(&self.access, locator.commit_sequence())?
                    .ok_or_else(corrupt)?;
                if command.base().outcome().identity() != identity {
                    return Err(corrupt());
                }
                return Ok(Some(command.base().outcome().clone()));
            }
        };
        match decode_idempotency_record_v1(&encoded)?.into_parts().0 {
            IdempotencyRecordV1::StoredOutcome(outcome) if outcome.identity() == identity => {
                Ok(Some(outcome))
            }
            IdempotencyRecordV1::ExecutionFailed(failure)
                if failure.pending().identity() == identity =>
            {
                Ok(None)
            }
            IdempotencyRecordV1::CommandLocator(locator) => {
                let command = command_member_at_access(&self.access, locator.commit_sequence())?
                    .ok_or_else(corrupt)?;
                if command.base().outcome().identity() != identity {
                    return Err(corrupt());
                }
                Ok(Some(command.base().outcome().clone()))
            }
            IdempotencyRecordV1::StoredOutcome(_) | IdempotencyRecordV1::ExecutionFailed(_) => {
                Err(corrupt())
            }
        }
    }

    fn read_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<StoredCommitRecordV1>, StorageError> {
        if let Some(command) = command_member_at_access(&self.access, sequence)? {
            if command.base().commit_sequence() != sequence {
                return Err(corrupt());
            }
            return Ok(Some(command.base().commit().clone()));
        }
        commit_at_access(&self.access, sequence)
    }

    fn read_provenance(
        &self,
        provenance_id: ProvenanceId,
    ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
        let encoded_key = encode_provenance_key(provenance_id);
        let Some(encoded) = self
            .access
            .read_value(JournalTable::Provenance, encoded_key.as_slice())?
        else {
            let Some(locator) = self
                .access
                .read_value(JournalTable::ProvenanceLocators, encoded_key.as_slice())?
            else {
                return Ok(None);
            };
            let locator = decode_command_locator_v1(&locator)?.into_parts().0;
            let command = command_member_at_access(&self.access, locator.commit_sequence())?
                .ok_or_else(corrupt)?;
            if command.base().provenance().provenance_id() != provenance_id {
                return Err(corrupt());
            }
            return Ok(Some(command.base().provenance().clone()));
        };
        let record = match decode_command_locator_v1(&encoded) {
            Ok(locator) => {
                let locator = locator.into_parts().0;
                let command = command_member_at_access(&self.access, locator.commit_sequence())?
                    .ok_or_else(corrupt)?;
                command.base().provenance().clone()
            }
            Err(_) => decode_provenance_record_v1(&encoded)?.into_parts().0,
        };
        if record.provenance_id() != provenance_id {
            return Err(corrupt());
        }
        Ok(Some(record))
    }

    fn read_durable_event(
        &self,
        event_id: EventId,
    ) -> Result<Option<StoredDurableEventV1>, StorageError> {
        if let Some(CommandAuthorityMember::CapsuleV2(capsule)) =
            command_member_at_access(&self.access, event_id.commit_sequence())?
        {
            let ordinal = usize::try_from(event_id.event_ordinal()).map_err(|_| corrupt())?;
            let event = capsule.events().get(ordinal).ok_or_else(corrupt)?;
            if event.event_id() != event_id {
                return Err(corrupt());
            }
            return Ok(Some(event.clone()));
        }
        let key = encode_event_key(event_id);
        let Some(encoded) = self.access.read_value(JournalTable::Events, &key)? else {
            return Ok(None);
        };
        let event = decode_durable_event_v1(&encoded)?.into_parts().0;
        if event.event_id() != event_id {
            return Err(corrupt());
        }
        Ok(Some(event))
    }
}

impl SnapshotReader for RedbOwnedSnapshot {
    fn read_snapshot(&self, request: SnapshotRequest) -> Result<ReadSnapshot, StorageError> {
        let observed = self.access.application_frontier()?;
        read_snapshot_from_access(request, observed, &self.access)
    }
}

impl AuthoritativeScanReader for RedbOwnedSnapshot {
    fn scan_entity_partition(
        &self,
        request: AuthoritativeEntityPartitionScanRequest,
    ) -> Result<AuthoritativeEntityPartitionScanPage, StorageError> {
        let application_head = self.access.application_frontier()?.map_or(
            FrontierPosition::BeforeFirst,
            FrontierPosition::AppliedThrough,
        );
        let upper = exclusive_prefix_end(request.prefix()).ok_or_else(corrupt)?;
        let start = request
            .after()
            .map_or(request.prefix(), riffdb_types::EntityKey::as_bytes);
        let scan = self.access.read_range(
            JournalTable::Entities,
            start,
            &upper,
            usize::from(request.limit().get()).saturating_add(2),
        )?;
        let wanted = usize::from(request.limit().get());
        let mut records = Vec::with_capacity(wanted);
        let mut has_more = false;
        for (physical_key, encoded) in scan {
            if request
                .after()
                .is_some_and(|after| physical_key.as_ref() == after.as_bytes())
            {
                continue;
            }
            if records.len() == wanted {
                has_more = true;
                break;
            }
            let record = decode_entity_record_v1(&encoded)?;
            if record.value().target().key().as_bytes() != physical_key.as_ref() {
                return Err(corrupt());
            }
            records.push(record);
        }
        AuthoritativeEntityPartitionScanPage::new(&request, application_head, records, has_more)
            .map_err(value_error)
    }

    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        let epoch = read_epoch_position_access(&self.access, request.target().generation_target())?;
        let prefix = request.target().prefix().as_bytes();
        let upper = exclusive_prefix_end(prefix).ok_or_else(corrupt)?;
        let start = request
            .after()
            .map_or(prefix, riffdb_types::IndexEntryKey::as_bytes);
        let scan = self.access.read_range(
            JournalTable::SecondaryIndexes,
            start,
            &upper,
            MAX_INDEX_SCAN_INSPECTED_ENTRIES.saturating_add(1),
        )?;
        let wanted = usize::from(request.limit().get());
        let mut entries = Vec::with_capacity(wanted);
        let mut encoded_bytes = 0usize;
        let mut has_more = false;
        for (candidate_index, (physical_key, encoded)) in scan.into_iter().enumerate() {
            if request
                .after()
                .is_some_and(|after| physical_key.as_ref() == after.as_bytes())
            {
                continue;
            }
            if candidate_index == MAX_INDEX_SCAN_INSPECTED_ENTRIES {
                has_more = true;
                break;
            }
            let key = decode_index_entry_key(&physical_key).map_err(|_| corrupt())?;
            let decoded = decode_index_entry_v2(&encoded)?;
            if decoded.value().key() != &key {
                return Err(corrupt());
            }
            if decoded.value().partition_key()
                != request.target().generation_target().partition_key()
            {
                continue;
            }
            if entries.len() == wanted {
                has_more = true;
                break;
            }
            let (stored, charge) = decoded.into_parts();
            let next_bytes = encoded_bytes
                .checked_add(charge.get())
                .ok_or_else(corrupt)?;
            if next_bytes > MAX_SCAN_PAGE_BYTES {
                if entries.is_empty() {
                    return Err(limit_exceeded());
                }
                has_more = true;
                break;
            }
            let row = IndexRangeEntry::new(key.index_id(), key, stored.covered_values().clone())
                .map_err(value_error)?;
            entries.push(EncodedPageItem::new(row, charge));
            encoded_bytes = next_bytes;
        }
        if has_more {
            let next_after = entries.last().ok_or_else(corrupt)?.value().key().clone();
            AuthoritativeIndexScanPage::page(&request, epoch, entries, next_after)
                .map_err(value_error)
        } else {
            AuthoritativeIndexScanPage::exact_end(&request, epoch, entries).map_err(value_error)
        }
    }

    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        let inclusive_upper = match request.inclusive_upper() {
            Some(sequence) => FrontierPosition::AppliedThrough(sequence),
            None => self.access.application_frontier()?.map_or(
                FrontierPosition::BeforeFirst,
                FrontierPosition::AppliedThrough,
            ),
        };
        let Some(first_expected) = request
            .after()
            .map_or(Some(CommitSequence::first()), CommitSequence::checked_next)
        else {
            return CommitScanPageV1::exact_end(request, inclusive_upper, Vec::new())
                .map_err(value_error);
        };
        let FrontierPosition::AppliedThrough(upper) = inclusive_upper else {
            return CommitScanPageV1::exact_end(request, inclusive_upper, Vec::new())
                .map_err(value_error);
        };
        if first_expected > upper {
            return CommitScanPageV1::exact_end(request, inclusive_upper, Vec::new())
                .map_err(value_error);
        }
        let first_key = encode_application_sequence_key(CommitSequence::first());
        let mut first_end = encode_application_sequence_key(first_expected).to_vec();
        first_end.push(0);
        let start = self
            .access
            .read_range_reverse(JournalTable::Commits, &first_key, &first_end, 1)?
            .into_iter()
            .next()
            .map(|(key, _)| key)
            .ok_or_else(corrupt)?;
        let mut end = encode_application_sequence_key(upper).to_vec();
        end.push(0);
        let scan = self.access.read_range(
            JournalTable::Commits,
            &start,
            &end,
            MAX_INDEX_SCAN_INSPECTED_ENTRIES.saturating_add(1),
        )?;
        let wanted = usize::from(request.limit().get());
        let mut records = Vec::with_capacity(wanted);
        let mut encoded_bytes = 0usize;
        let mut has_more = false;
        let mut expected = Some(first_expected);
        let mut expected_physical = None;
        'rows: for (row_index, (physical_key, encoded)) in scan.into_iter().enumerate() {
            if row_index == MAX_INDEX_SCAN_INSPECTED_ENTRIES {
                return Err(limit_exceeded());
            }
            let physical_sequence =
                decode_application_sequence_key(&physical_key).map_err(|_| corrupt())?;
            if expected_physical.is_some_and(|value| value != physical_sequence) {
                return Err(corrupt());
            }
            let logical =
                commits_in_physical_row_access(&self.access, &encoded, physical_sequence)?;
            let last_physical = logical
                .last()
                .ok_or_else(corrupt)?
                .value()
                .commit_sequence();
            expected_physical = last_physical.checked_next();
            for decoded in logical {
                let sequence = decoded.value().commit_sequence();
                if sequence < first_expected {
                    continue;
                }
                if sequence > upper {
                    break 'rows;
                }
                if Some(sequence) != expected {
                    return Err(corrupt());
                }
                if records.len() == wanted {
                    has_more = true;
                    break 'rows;
                }
                let next_bytes = encoded_bytes
                    .checked_add(decoded.encoded_content_charge().get())
                    .ok_or_else(corrupt)?;
                if next_bytes > MAX_COMMIT_SCAN_PAGE_BYTES {
                    if records.is_empty() {
                        return Err(limit_exceeded());
                    }
                    has_more = true;
                    break 'rows;
                }
                encoded_bytes = next_bytes;
                records.push(decoded);
                expected = sequence.checked_next();
            }
        }
        if !has_more && expected.is_some_and(|sequence| sequence <= upper) {
            return Err(corrupt());
        }
        if has_more {
            let next_after = records
                .last()
                .ok_or_else(corrupt)?
                .value()
                .commit_sequence();
            CommitScanPageV1::page(request, inclusive_upper, records, next_after)
                .map_err(value_error)
        } else {
            CommitScanPageV1::exact_end(request, inclusive_upper, records).map_err(value_error)
        }
    }
}

impl FilteredAuthoritativeScanReader for RedbOwnedSnapshot {
    fn scan_index_filtered(
        &self,
        request: FilteredAuthoritativeIndexScanRequest,
    ) -> Result<FilteredAuthoritativeIndexScanPage, StorageError> {
        if request.partition_filter().is_none() {
            return FilteredAuthoritativeIndexScanPage::exact_end(
                &request,
                IndexEpochPosition::BeforeFirst,
                Vec::new(),
            )
            .map_err(value_error);
        }
        let partition = match request.partition_filter().scope() {
            IndexPartitionFilterScope::Explicit(keys) if keys.len() == 1 => &keys[0],
            IndexPartitionFilterScope::All
            | IndexPartitionFilterScope::None
            | IndexPartitionFilterScope::Explicit(_) => return Err(invariant()),
        };
        let epoch = read_epoch_position_access(
            &self.access,
            &PartitionIndexTarget::new(partition.clone(), request.target().prefix().index_id()),
        )?;
        let prefix = request.target().prefix().as_bytes();
        let upper = exclusive_prefix_end(prefix).ok_or_else(corrupt)?;
        let start = request
            .after()
            .map_or(prefix, riffdb_types::IndexEntryKey::as_bytes);
        let scan = self.access.read_range(
            JournalTable::SecondaryIndexes,
            start,
            &upper,
            MAX_INDEX_SCAN_INSPECTED_ENTRIES.saturating_add(1),
        )?;
        let returned_limit = usize::from(request.limit().get());
        let mut returned = Vec::with_capacity(returned_limit);
        let mut returned_bytes = 0usize;
        let mut inspected_bytes = 0usize;
        let mut scanned_through = None;
        let mut exact_end = true;
        for (candidate_index, (physical_key, encoded)) in scan.into_iter().enumerate() {
            if request
                .after()
                .is_some_and(|after| physical_key.as_ref() == after.as_bytes())
            {
                continue;
            }
            if returned.len() == returned_limit
                || candidate_index == MAX_INDEX_SCAN_INSPECTED_ENTRIES
            {
                exact_end = false;
                break;
            }
            let key = decode_index_entry_key(&physical_key).map_err(|_| corrupt())?;
            let decoded = decode_index_entry_v2(&encoded)?;
            if decoded.value().key() != &key {
                return Err(corrupt());
            }
            let charge = decoded.encoded_content_charge();
            let next_inspected_bytes = inspected_bytes
                .checked_add(charge.get())
                .ok_or_else(limit_exceeded)?;
            if next_inspected_bytes > MAX_INDEX_SCAN_INSPECTED_BYTES {
                if scanned_through.is_none() {
                    return Err(corrupt());
                }
                exact_end = false;
                break;
            }
            let is_eligible = request.partition_filter().allows(
                decoded.value().schema_binding(),
                decoded.value().partition_key(),
            );
            let next_returned_bytes = returned_bytes
                .checked_add(charge.get())
                .ok_or_else(limit_exceeded)?;
            if is_eligible && next_returned_bytes > MAX_SCAN_PAGE_BYTES {
                if scanned_through.is_none() {
                    return Err(corrupt());
                }
                exact_end = false;
                break;
            }
            inspected_bytes = next_inspected_bytes;
            scanned_through = Some(key);
            if is_eligible {
                returned_bytes = next_returned_bytes;
                returned.push(decoded);
            }
        }
        if exact_end {
            FilteredAuthoritativeIndexScanPage::exact_end(&request, epoch, returned)
                .map_err(value_error)
        } else {
            let scanned_through = scanned_through.ok_or_else(invariant)?;
            FilteredAuthoritativeIndexScanPage::page(&request, epoch, returned, scanned_through)
                .map_err(value_error)
        }
    }
}

impl VectorObservationRepository for RedbOwnedSnapshot {
    fn read_vector_observation(
        &self,
        target: &VectorObservationTargetV1,
    ) -> Result<Option<VectorObservationCountsV1>, StorageError> {
        let key = encode_vector_observation_key(target).map_err(|_| invariant())?;
        self.access
            .read_value(JournalTable::VectorObservations, &key)?
            .map(|bytes| decode_vector_observation_v1(&bytes).map(|decoded| decoded.into_parts().0))
            .transpose()
    }

    fn read_vector_health_observation(
        &self,
        lineage: &ContractLineage,
    ) -> Result<Option<VectorHealthObservationV1>, StorageError> {
        let key = encode_vector_health_observation_key(lineage).map_err(|_| invariant())?;
        self.access
            .read_value(JournalTable::VectorObservations, &key)?
            .map(|bytes| {
                decode_vector_health_observation_v1(&bytes).map(|decoded| decoded.into_parts().0)
            })
            .transpose()
    }
}

impl VectorEvidenceIndexRepository for RedbOwnedSnapshot {
    fn scan_vector_evidence_index(
        &self,
        request: &VectorEvidenceIndexScanRequestV1,
    ) -> Result<VectorEvidenceIndexPageV1, StorageError> {
        let prefix =
            encode_vector_evidence_index_prefix(request.target()).map_err(|_| invariant())?;
        let upper = exclusive_prefix_end(&prefix).ok_or_else(invariant)?;
        let start = request
            .after()
            .map_or_else(
                || Ok(prefix.clone()),
                |after| encode_vector_evidence_index_key(request.target(), after),
            )
            .map_err(|_| invariant())?;
        let limit = usize::from(request.limit().get());
        let rows = self.access.read_range(
            JournalTable::VectorEvidenceIndex,
            &start,
            &upper,
            limit.saturating_add(2),
        )?;
        let mut entries = Vec::with_capacity(limit);
        let mut encoded_bytes = 0usize;
        let mut more = false;
        for (key, value) in rows {
            let (target, entity_key) =
                decode_vector_evidence_index_key(&key).map_err(|_| corrupt())?;
            if request.after().is_some_and(|after| after == &entity_key) {
                continue;
            }
            if entries.len() == limit {
                more = true;
                break;
            }
            let decoded = decode_vector_evidence_index_v1(&value)?;
            if decoded.value().target() != &target
                || decoded.value().entity_key() != &entity_key
                || &target != request.target()
            {
                return Err(corrupt());
            }
            encoded_bytes = encoded_bytes
                .checked_add(decoded.encoded_content_charge().get())
                .ok_or_else(limit_exceeded)?;
            entries.push(decoded.into_parts().0);
        }
        let continuation = more
            .then(|| entries.last().map(|entry| entry.entity_key().clone()))
            .flatten();
        VectorEvidenceIndexPageV1::new(
            request.target(),
            entries,
            continuation,
            !more,
            encoded_bytes,
        )
        .map_err(value_error)
    }
}

const fn invariant() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}

const fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

const fn limit_exceeded() -> StorageError {
    storage_error(StorageErrorKind::LimitExceeded)
}

fn exclusive_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    let position = upper.iter().rposition(|byte| *byte != u8::MAX)?;
    upper[position] = upper[position].saturating_add(1);
    upper.truncate(position + 1);
    Some(upper)
}

const fn value_error(_: riffdb_storage_api::StorageValueError) -> StorageError {
    corrupt()
}
