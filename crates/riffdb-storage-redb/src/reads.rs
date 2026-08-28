//! Owned authoritative reads over short redb read transactions.

use redb::{ReadOnlyTable, ReadableTableMetadata};
use riffdb_storage_api::{
    ApplicationSequenceAllocator, AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest,
    AuthoritativePointReader, AuthoritativeScanReader, CommandDerivedIndexKindV1,
    CommandDerivedMemberV1, CommitScanPageV1, CommitScanRequest, EncodedContentCharge,
    EncodedPageItem, EntityObservation, EntityTarget, EventRouteScanRequestV1, EventRouteScanV1,
    EventRouteUpperFenceV1, FilteredAuthoritativeIndexScanPage,
    FilteredAuthoritativeIndexScanRequest, FilteredAuthoritativeScanReader, IdempotencyIdentity,
    IndexEpochPosition, IndexPartitionFilterScope, IndexRangeEntry, MAX_COMMIT_SCAN_PAGE_BYTES,
    MAX_INDEX_SCAN_INSPECTED_BYTES, MAX_INDEX_SCAN_INSPECTED_ENTRIES, MAX_SCAN_PAGE_BYTES,
    PartitionEventRouteReader, PartitionIndexTarget, ReadSnapshot, ReadSnapshotBuilder,
    SnapshotReader, SnapshotRequest, StorageError, StorageErrorKind, StorageValueError,
    StoredCommitRecordV1, StoredDurableEventV1, StoredEntityRecordV1, StoredOutcomeV1,
    StoredProvenanceRecordV1,
};
use riffdb_types::{CommitSequence, EventId, FrontierPosition, ProvenanceId};

use crate::codec::{
    IdempotencyRecordV1, decode_application_sequence_allocator_v1, decode_command_locator_v1,
    decode_durable_event_v1, decode_entity_record_v1, decode_idempotency_record_v1,
    decode_index_entry_v2, decode_index_epoch_v1, decode_provenance_record_v1,
    encode_event_route_v1,
};
#[cfg(test)]
use crate::command_authority::command_authority_head;
use crate::command_authority::{
    command_member_at_access, commit_at_access, commits_in_physical_row_access,
};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::journal::JournalTable;
#[cfg(test)]
use crate::keys::encode_event_route_key;
use crate::keys::{
    decode_application_sequence_key, decode_index_entry_key, encode_application_sequence_key,
    encode_entity_key, encode_event_key, encode_idempotency_key, encode_partition_index_key,
    encode_provenance_key,
};
use crate::layout::META_APPLICATION_SEQUENCE;
#[cfg(test)]
use crate::layout::{EVENTS, META};
use crate::store::{RedbOperationalPorts, RedbReadAccess};

type BytesTable = ReadOnlyTable<&'static [u8], &'static [u8]>;

impl SnapshotReader for RedbOperationalPorts {
    fn read_snapshot(&self, request: SnapshotRequest) -> Result<ReadSnapshot, StorageError> {
        let mut snapshots = self.read_snapshot_group(vec![request])?;
        snapshots
            .pop()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
    }

    fn read_snapshot_group(
        &self,
        requests: Vec<SnapshotRequest>,
    ) -> Result<Vec<ReadSnapshot>, StorageError> {
        if requests.is_empty() || requests.len() > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
        {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let transaction = self.begin_composite_read()?;
        let observed_through = transaction.application_frontier()?;
        requests
            .into_iter()
            .map(|request| read_snapshot_from_access(request, observed_through, &transaction))
            .collect()
    }
}

fn read_snapshot_from_access(
    request: SnapshotRequest,
    observed_through: Option<CommitSequence>,
    access: &RedbReadAccess,
) -> Result<ReadSnapshot, StorageError> {
    let mut snapshot =
        ReadSnapshotBuilder::new(&request, observed_through).map_err(materialization_value)?;
    for target in request.binding_targets() {
        snapshot
            .push_binding(read_entity_observation_access(access, target)?)
            .map_err(materialization_value)?;
    }
    for target in request.root_validation_targets() {
        snapshot
            .push_root_validation(read_entity_observation_access(access, target)?)
            .map_err(materialization_value)?;
    }
    for target in request.cascade_targets() {
        snapshot
            .push_cascade_predecessor(read_entity_observation_access(access, target)?)
            .map_err(materialization_value)?;
    }
    for (position, target) in request.range_targets().iter().enumerate() {
        let epoch = read_epoch_position_access(access, target.generation_target())?;
        let mut range = snapshot
            .begin_range(target.clone(), epoch)
            .map_err(materialization_value)?;
        let prefix = target.prefix().as_bytes();
        let upper = exclusive_prefix_end(prefix).ok_or_else(corrupt)?;
        let entries = access.read_range(
            JournalTable::SecondaryIndexes,
            prefix,
            &upper,
            MAX_INDEX_SCAN_INSPECTED_ENTRIES.saturating_add(1),
        )?;
        let entry_limit = request
            .range_entry_limit(position)
            .ok_or_else(|| materialization_value(StorageValueError::IdentityMismatch))?;
        let mut retained = 0usize;
        for (physical_key, encoded) in entries {
            let key = decode_index_entry_key(&physical_key).map_err(|_| corrupt())?;
            let decoded = decode_index_entry_v2(&encoded)?;
            if decoded.value().key() != &key {
                return Err(corrupt());
            }
            if decoded.value().partition_key() != target.generation_target().partition_key() {
                continue;
            }
            if retained == entry_limit {
                break;
            }
            let row = IndexRangeEntry::new(
                key.index_id(),
                key,
                decoded.value().covered_values().clone(),
            )
            .map_err(corrupt_value)?;
            range.push_entry(row).map_err(materialization_value)?;
            retained += 1;
        }
        range.finish().map_err(materialization_value)?;
    }
    snapshot.finish().map_err(materialization_value)
}

impl AuthoritativePointReader for RedbOperationalPorts {
    fn read_entity(
        &self,
        target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        let transaction = self.begin_composite_read()?;
        read_entity_record_access(&transaction, target)
    }

    fn read_stored_outcome(
        &self,
        identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredOutcomeV1>, StorageError> {
        let key = identity
            .storage_key()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let encoded_key = encode_idempotency_key(&key);
        if let Some((segment, locator)) =
            self.command_derived_member(CommandDerivedIndexKindV1::Idempotency, encoded_key)?
        {
            if locator.member != CommandDerivedMemberV1::Command || locator.member_ordinal != 0 {
                return Err(corrupt());
            }
            let command = segment
                .commands()
                .get(usize::from(locator.command_ordinal))
                .ok_or_else(corrupt)?;
            if command.base().outcome().identity() != identity {
                return Err(corrupt());
            }
            return Ok(Some(command.base().outcome().clone()));
        }
        let transaction = self.begin_composite_read()?;
        let Some(encoded) = transaction.read_value(JournalTable::Idempotency, encoded_key)? else {
            // ADR-0165: outcomes are segment-owned, so IDEMPOTENCY is empty and
            // the durable path to the owning segment is the locator table. An
            // index miss plus an empty IDEMPOTENCY is NOT absence.
            let Some(encoded) =
                transaction.read_value(JournalTable::IdempotencyLocators, encoded_key)?
            else {
                return Ok(None);
            };
            let locator = decode_command_locator_v1(&encoded)?.into_parts().0;
            // Fail closed from here: the locator asserted the segment exists.
            let capsule = command_member_at_access(&transaction, locator.commit_sequence())?
                .ok_or_else(corrupt)?
                .into_base();
            if capsule.commit_sequence() != locator.commit_sequence()
                || capsule.outcome().identity() != identity
            {
                return Err(corrupt());
            }
            return Ok(Some(capsule.outcome().clone()));
        };
        let decoded = decode_idempotency_record_v1(&encoded)?;
        match decoded.into_parts().0 {
            IdempotencyRecordV1::StoredOutcome(outcome) if outcome.identity() == identity => {
                Ok(Some(outcome))
            }
            IdempotencyRecordV1::ExecutionFailed(failure)
                if failure.pending().identity() == identity =>
            {
                Ok(None)
            }
            IdempotencyRecordV1::StoredOutcome(_) | IdempotencyRecordV1::ExecutionFailed(_) => {
                Err(corrupt())
            }
            IdempotencyRecordV1::CommandLocator(locator) => {
                let capsule = command_member_at_access(&transaction, locator.commit_sequence())?
                    .ok_or_else(corrupt)?
                    .into_base();
                if capsule.commit_sequence() != locator.commit_sequence()
                    || capsule.outcome().identity() != identity
                {
                    return Err(corrupt());
                }
                Ok(Some(capsule.outcome().clone()))
            }
        }
    }

    fn read_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<StoredCommitRecordV1>, StorageError> {
        if let Some(command) = self.indexed_command_at(sequence)? {
            if command.commit_sequence() != sequence {
                return Err(corrupt());
            }
            return Ok(Some(command.base().commit().clone()));
        }
        let transaction = self.begin_composite_read()?;
        // Verified once at open; prune runs only under exclusive OFFLINE
        // access, so the watermark cannot change under a live handle.
        let watermark = self.shared.retention_watermark();
        let Some(record) = commit_at_access(&transaction, sequence)? else {
            if crate::retention::sequence_covered_by_watermark(sequence.get(), watermark) {
                return Err(storage_error(StorageErrorKind::HistoryPruned));
            }
            return Ok(None);
        };
        Ok(Some(record))
    }

    fn read_provenance(
        &self,
        provenance_id: ProvenanceId,
    ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
        let encoded_key = encode_provenance_key(provenance_id);
        if let Some((segment, locator)) = self.command_derived_member(
            CommandDerivedIndexKindV1::Provenance,
            encoded_key.as_slice(),
        )? {
            if locator.member != CommandDerivedMemberV1::Command || locator.member_ordinal != 0 {
                return Err(corrupt());
            }
            let command = segment
                .commands()
                .get(usize::from(locator.command_ordinal))
                .ok_or_else(corrupt)?;
            let record = command.base().provenance();
            if record.provenance_id() != provenance_id {
                return Err(corrupt());
            }
            return Ok(Some(record.clone()));
        }
        let transaction = self.begin_composite_read()?;
        let Some(encoded) =
            transaction.read_value(JournalTable::Provenance, encoded_key.as_slice())?
        else {
            // ADR-0165 locator table: PROVENANCE is empty because provenance is
            // segment-owned, so its absence is not the answer.
            let Some(encoded) =
                transaction.read_value(JournalTable::ProvenanceLocators, encoded_key.as_slice())?
            else {
                return Ok(None);
            };
            let locator = decode_command_locator_v1(&encoded)?.into_parts().0;
            let capsule = command_member_at_access(&transaction, locator.commit_sequence())?
                .ok_or_else(corrupt)?
                .into_base();
            let record = capsule.provenance();
            if capsule.commit_sequence() != locator.commit_sequence()
                || record.provenance_id() != provenance_id
            {
                return Err(corrupt());
            }
            return Ok(Some(record.clone()));
        };
        let record = match decode_command_locator_v1(&encoded) {
            Ok(locator) => {
                let locator = locator.into_parts().0;
                let capsule = command_member_at_access(&transaction, locator.commit_sequence())?
                    .ok_or_else(corrupt)?
                    .into_base();
                if capsule.commit_sequence() != locator.commit_sequence() {
                    return Err(corrupt());
                }
                capsule.provenance().clone()
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
        if let Some(command) = self.indexed_command_at(event_id.commit_sequence())? {
            let ordinal = usize::try_from(event_id.event_ordinal()).map_err(|_| corrupt())?;
            let event = command.events().get(ordinal).ok_or_else(corrupt)?;
            if event.event_id() != event_id {
                return Err(corrupt());
            }
            return Ok(Some(event.clone()));
        }
        let transaction = self.begin_composite_read()?;
        // Verified once at open (see read_commit).
        let watermark = self.shared.retention_watermark();
        let encoded_key = encode_event_key(event_id);
        let Some(encoded) = transaction.read_value(JournalTable::Events, &encoded_key)? else {
            // Segment-owned events have no physical EVENTS row, so an index miss
            // plus an empty table is NOT absence. `EVENTS` holds zero rows in
            // the current layout, which made this the whole answer whenever the
            // transient index was dormant: a durable event read as absent
            // (ADR-0156 §5 forbids converting cold state into absence).
            //
            // Unlike the idempotency and provenance keys, an event id carries
            // its commit sequence, so the owning segment is directly addressable
            // in `COMMITS` and needs no durable locator row. This is the same
            // fallback `read_commit` already performs.
            // Only a V2 capsule embeds its events; a historical V1 row always
            // has its own physical EVENTS row, so its absence here is genuine.
            if let Some(crate::command_authority::CommandAuthorityMember::CapsuleV2(capsule)) =
                crate::command_authority::command_member_at_access(
                    &transaction,
                    event_id.commit_sequence(),
                )?
            {
                if capsule.base().commit_sequence() != event_id.commit_sequence() {
                    return Err(corrupt());
                }
                let ordinal = usize::try_from(event_id.event_ordinal()).map_err(|_| corrupt())?;
                // Fail closed, never absent: the segment owning this sequence is
                // present, so a missing ordinal or mismatched identity is
                // corruption, not a missing event.
                let event = capsule.events().get(ordinal).ok_or_else(corrupt)?;
                if event.event_id() != event_id {
                    return Err(corrupt());
                }
                return Ok(Some(event.clone()));
            }
            if crate::retention::sequence_covered_by_watermark(
                event_id.commit_sequence().get(),
                watermark,
            ) {
                return Err(storage_error(StorageErrorKind::HistoryPruned));
            }
            return Ok(None);
        };
        let event = decode_durable_event_v1(&encoded)?.into_parts().0;
        if event.event_id() != event_id {
            return Err(corrupt());
        }
        Ok(Some(event))
    }
}

impl PartitionEventRouteReader for RedbOperationalPorts {
    fn scan_partition_event_routes(
        &self,
        request: EventRouteScanRequestV1,
    ) -> Result<EventRouteScanV1, StorageError> {
        let wanted = usize::from(request.limit().get().get());
        let (inclusive_upper, routes, mut has_more) = self.partition_event_route_page(
            request.partition_hash(),
            request.after(),
            request.inclusive_upper(),
            wanted.saturating_add(1),
        )?;
        let EventRouteUpperFenceV1::Inclusive(upper) = inclusive_upper else {
            return EventRouteScanV1::exact_end(request, inclusive_upper, Vec::new())
                .map_err(materialization_value);
        };
        let mut items = Vec::with_capacity(wanted);
        let mut encoded_bytes = 0usize;
        for route in routes {
            if items.len() == wanted {
                has_more = true;
                break;
            }
            let encoded = encode_event_route_v1(route)?;
            let charge = EncodedContentCharge::new(encoded.as_bytes().len()).ok_or_else(corrupt)?;
            let next_bytes = encoded_bytes
                .checked_add(charge.get())
                .ok_or_else(corrupt)?;
            if next_bytes > MAX_SCAN_PAGE_BYTES {
                has_more = true;
                break;
            }
            encoded_bytes = next_bytes;
            items.push(EncodedPageItem::new(route, charge));
        }

        if has_more {
            EventRouteScanV1::page(request, upper, items).map_err(materialization_value)
        } else {
            EventRouteScanV1::exact_end(request, inclusive_upper, items)
                .map_err(materialization_value)
        }
    }
}

impl AuthoritativeScanReader for RedbOperationalPorts {
    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        let transaction = self.begin_composite_read()?;
        let epoch = read_epoch_position_access(&transaction, request.target().generation_target())?;
        let prefix = request.target().prefix().as_bytes();
        let upper = exclusive_prefix_end(prefix).ok_or_else(corrupt)?;
        let start = request.after().map_or(prefix, |after| after.as_bytes());
        let scan = transaction.read_range(
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
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                has_more = true;
                break;
            }
            let row = IndexRangeEntry::new(key.index_id(), key, stored.covered_values().clone())
                .map_err(corrupt_value)?;
            entries.push(EncodedPageItem::new(row, charge));
            encoded_bytes = next_bytes;
        }

        if has_more {
            let next_after = entries.last().ok_or_else(corrupt)?.value().key().clone();
            AuthoritativeIndexScanPage::page(&request, epoch, entries, next_after)
                .map_err(corrupt_value)
        } else {
            AuthoritativeIndexScanPage::exact_end(&request, epoch, entries).map_err(corrupt_value)
        }
    }

    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        let transaction = self.begin_composite_read()?;
        let inclusive_upper = match request.inclusive_upper() {
            Some(sequence) => FrontierPosition::AppliedThrough(sequence),
            None => transaction.application_frontier()?.map_or(
                FrontierPosition::BeforeFirst,
                FrontierPosition::AppliedThrough,
            ),
        };
        let Some(first_expected) = request
            .after()
            .map_or(Some(CommitSequence::first()), CommitSequence::checked_next)
        else {
            return CommitScanPageV1::exact_end(request, inclusive_upper, Vec::new())
                .map_err(corrupt_value);
        };
        let FrontierPosition::AppliedThrough(upper) = inclusive_upper else {
            return CommitScanPageV1::exact_end(request, inclusive_upper, Vec::new())
                .map_err(corrupt_value);
        };
        if first_expected > upper {
            return CommitScanPageV1::exact_end(request, inclusive_upper, Vec::new())
                .map_err(corrupt_value);
        }
        let first_key = encode_application_sequence_key(CommitSequence::first());
        let mut first_end = encode_application_sequence_key(first_expected).to_vec();
        first_end.push(0);
        let start = transaction
            .read_range_reverse(JournalTable::Commits, &first_key, &first_end, 1)?
            .into_iter()
            .next()
            .map(|(key, _)| key)
            .ok_or_else(corrupt)?;
        let mut end = encode_application_sequence_key(upper).to_vec();
        end.push(0);
        let scan = transaction.read_range(
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
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            let physical_sequence =
                decode_application_sequence_key(&physical_key).map_err(|_| corrupt())?;
            if expected_physical.is_some_and(|value| value != physical_sequence) {
                return Err(corrupt());
            }
            let logical =
                commits_in_physical_row_access(&transaction, &encoded, physical_sequence)?;
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
                        return Err(storage_error(StorageErrorKind::LimitExceeded));
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
                .map_err(corrupt_value)
        } else {
            CommitScanPageV1::exact_end(request, inclusive_upper, records).map_err(corrupt_value)
        }
    }
}

impl FilteredAuthoritativeScanReader for RedbOperationalPorts {
    fn scan_index_filtered(
        &self,
        request: FilteredAuthoritativeIndexScanRequest,
    ) -> Result<FilteredAuthoritativeIndexScanPage, StorageError> {
        let transaction = self.begin_composite_read()?;
        if request.partition_filter().is_none() {
            return FilteredAuthoritativeIndexScanPage::exact_end(
                &request,
                IndexEpochPosition::BeforeFirst,
                Vec::new(),
            )
            .map_err(corrupt_value);
        }
        let partition = match request.partition_filter().scope() {
            IndexPartitionFilterScope::Explicit(keys) if keys.len() == 1 => &keys[0],
            IndexPartitionFilterScope::All
            | IndexPartitionFilterScope::None
            | IndexPartitionFilterScope::Explicit(_) => {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        };
        let epoch = read_epoch_position_access(
            &transaction,
            &PartitionIndexTarget::new(partition.clone(), request.target().prefix().index_id()),
        )?;

        let prefix = request.target().prefix().as_bytes();
        let upper = exclusive_prefix_end(prefix).ok_or_else(corrupt)?;
        let start = request.after().map_or(prefix, |after| after.as_bytes());
        let scan = transaction.read_range(
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
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
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
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
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
                .map_err(corrupt_value)
        } else {
            let scanned_through = scanned_through
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            FilteredAuthoritativeIndexScanPage::page(&request, epoch, returned, scanned_through)
                .map_err(corrupt_value)
        }
    }
}

fn read_entity_observation_access(
    access: &RedbReadAccess,
    target: &EntityTarget,
) -> Result<EntityObservation, StorageError> {
    Ok(read_entity_record_access(access, target)?.map_or_else(
        || EntityObservation::Absent(target.clone()),
        EntityObservation::Present,
    ))
}

pub(crate) fn read_entity_record_access(
    access: &RedbReadAccess,
    target: &EntityTarget,
) -> Result<Option<StoredEntityRecordV1>, StorageError> {
    let Some(encoded) =
        access.read_value(JournalTable::Entities, encode_entity_key(target.key()))?
    else {
        return Ok(None);
    };
    let record = decode_entity_record_v1(&encoded)?.into_parts().0;
    if record.target() != target {
        return Err(corrupt());
    }
    Ok(Some(record))
}

pub(crate) fn read_entity_record(
    table: &BytesTable,
    target: &EntityTarget,
) -> Result<Option<StoredEntityRecordV1>, StorageError> {
    let Some(encoded) = table
        .get(encode_entity_key(target.key()))
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    let record = decode_entity_record_v1(encoded.value())?.into_parts().0;
    if record.target() != target {
        return Err(corrupt());
    }
    Ok(Some(record))
}

fn read_epoch_position_access(
    access: &RedbReadAccess,
    target: &PartitionIndexTarget,
) -> Result<IndexEpochPosition, StorageError> {
    let key = encode_partition_index_key(target);
    let Some(encoded) = access.read_value(JournalTable::IndexEpochs, &key)? else {
        return Ok(IndexEpochPosition::BeforeFirst);
    };
    let epoch = decode_index_epoch_v1(&encoded)?.into_parts().0;
    if epoch.target() != target {
        return Err(corrupt());
    }
    Ok(IndexEpochPosition::Value(epoch.epoch()))
}

fn exclusive_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    let position = upper.iter().rposition(|byte| *byte != u8::MAX)?;
    upper[position] = upper[position].saturating_add(1);
    upper.truncate(position + 1);
    Some(upper)
}

#[cfg(test)]
pub(crate) fn read_commit_head(
    transaction: &redb::ReadTransaction,
    table: &BytesTable,
) -> Result<Option<CommitSequence>, StorageError> {
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    command_authority_head(table, &events)
}

/// Reads the snapshot-visible application frontier without decoding retained
/// command history. The allocator is advanced in the same authoritative redb
/// transaction as every entity/index post-image and command segment, so its
/// predecessor is the exact frontier of this read transaction.
pub(crate) fn read_snapshot_head(
    access: &RedbReadAccess,
) -> Result<Option<CommitSequence>, StorageError> {
    let encoded = access
        .read_value(JournalTable::Meta, META_APPLICATION_SEQUENCE.as_bytes())?
        .ok_or_else(corrupt)?;
    let allocator = *decode_application_sequence_allocator_v1(&encoded)?.value();
    let head = snapshot_head_from_allocator(allocator);
    match access {
        RedbReadAccess::Composite(view) => {
            if view.overlay().published_application() != head {
                return Err(corrupt());
            }
        }
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => {
            verify_snapshot_authority_presence(root, head)?;
        }
    }
    Ok(head)
}

const fn snapshot_head_from_allocator(
    allocator: ApplicationSequenceAllocator,
) -> Option<CommitSequence> {
    match allocator {
        ApplicationSequenceAllocator::Next(next) if next.get() == 1 => None,
        ApplicationSequenceAllocator::Next(next) => CommitSequence::new(next.get() - 1),
        ApplicationSequenceAllocator::Exhausted => CommitSequence::new(u64::MAX),
    }
}

/// Checks only whether the same snapshot contains a physical command-authority
/// witness. It deliberately does not read a retained segment value: complete
/// segment validation belongs to startup, recovery, history, and retention.
/// A fully pruned history is witnessed by its exact retention watermark.
fn verify_snapshot_authority_presence(
    root: &crate::checkpoint_root::CheckpointRoot,
    head: Option<CommitSequence>,
) -> Result<(), StorageError> {
    let commits = root
        .journal_byte_table(JournalTable::Commits)
        .ok_or_else(corrupt)?
        .map_err(table_error)?;
    if !commits.is_empty().map_err(precommit_storage_error)? {
        return if head.is_some() {
            Ok(())
        } else {
            Err(corrupt())
        };
    }

    let watermark = crate::retention::load_watermark(root)?
        .map(|watermark| watermark.watermark_sequence());
    match (head, watermark) {
        (None, None | Some(0)) => Ok(()),
        (Some(head), Some(watermark)) if head.get() == watermark => Ok(()),
        _ => Err(corrupt()),
    }
}

fn materialization_value(error: StorageValueError) -> StorageError {
    match error {
        StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => {
            storage_error(StorageErrorKind::LimitExceeded)
        }
        StorageValueError::Empty
        | StorageValueError::NonCanonicalOrder
        | StorageValueError::Duplicate
        | StorageValueError::IdentityMismatch
        | StorageValueError::InvalidShape => corrupt(),
    }
}

fn corrupt_value(_: StorageValueError) -> StorageError {
    corrupt()
}

const fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;
    use std::path::PathBuf;
    use std::sync::Arc;

    use riffdb_storage_api::{
        AuthoritativeIndexScanPage, DatabaseInitializationPort, DeclaredOutcome, DurabilityMode,
        DurableKeySchemaBindingV1, EventRoutePageLimit, ExecutablePlanRef, IdempotencyIdentity,
        IdempotencyKeyDigest, IndexPartitionFilter, IndexPartitionFilterScope,
        IndexRangePrefixBuilder, IndexRangeTarget, PartitionIndexTarget, ReadDependencies,
        StorageScanLimit, StoredCommitRecordV1, StoredDurableEventV1, StoredEventRouteV1,
        StoredIndexEntryV2, StoredIndexEpochV1, StoredReadDependenciesV1,
        StoredRetentionWatermarkV1, derive_event_hash_v1,
        proto_codec::encode_retention_watermark_v1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash,
        CanonicalRecord, CommandId, ContractBundleHash, ContractLineage, ContractVersion,
        DatabaseId, DigestKeyId, EntityKeyBuilder, EntityTypeId, EntityVersion, Environment,
        EventTypeId, IndexEntryKeyBuilder, IndexEpoch, IndexId, LogicalTime, OutcomeId,
        PartitionKeyBuilder, PlanHash, ProvenanceId, RequestId, SchemaHash, TenantScope, Timestamp,
        hash_partition_key,
    };

    use super::*;
    use crate::codec::{
        encode_application_sequence_allocator_v1, encode_commit_record_v1, encode_durable_event_v1,
        encode_entity_record_v1, encode_event_route_v1, encode_index_entry_v2,
        encode_index_epoch_v1,
    };
    use crate::layout::{
        COMMITS, ENTITIES, EVENT_ROUTES, EVENTS, INDEX_EPOCHS, META_RETENTION_WATERMARK,
        SECONDARY_INDEXES,
    };
    use crate::store::RedbStore;

    /// Whole-directory scope: the database and every side file it grows live
    /// in one [`crate::test_path::ScopedDirectory`] removed on drop — pass,
    /// fail, or panic.
    struct TestDatabasePath(
        PathBuf,
        // Held only so `Drop` removes the whole scope.
        #[allow(dead_code)] crate::test_path::ScopedDirectory,
    );

    impl TestDatabasePath {
        fn new(label: &str) -> Self {
            let scope = crate::test_path::ScopedDirectory::new(label);
            Self(scope.join("db.redb"), scope)
        }
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x11; 10])
            .expect("database ID")
    }

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn operational(label: &str) -> (TestDatabasePath, RedbOperationalPorts) {
        let path = TestDatabasePath::new(label);
        let mut store = RedbStore::open(&path.0).expect("open store");
        store
            .initialize_database(database_id())
            .expect("initialize store");
        let ports = RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        (path, ports)
    }

    fn plan() -> ExecutablePlanRef {
        ExecutablePlanRef::new(
            ContractLineage::new("budget").expect("lineage"),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([0x22; 32]),
            CommandId::new(1).expect("command ID"),
            PlanHash::from_bytes([0x33; 32]),
        )
    }

    fn entity_target(entity: u64) -> EntityTarget {
        let entity_type = EntityTypeId::new(7).expect("entity type");
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_u64(entity).expect("entity component");
        EntityTarget::new(entity_type, key.finish().expect("entity key")).expect("target")
    }

    fn idempotency_identity() -> IdempotencyIdentity {
        IdempotencyIdentity::new(
            database_id(),
            Environment::new("test").expect("environment"),
            TenantScope::Global,
            ActorId::new("operator").expect("actor"),
            ContractLineage::new("budget").expect("lineage"),
            CommandId::new(1).expect("command ID"),
            IdempotencyKeyDigest::from_hmac_bytes(DigestKeyId::new(1).expect("key ID"), [0x44; 32]),
        )
    }

    fn binding() -> DurableKeySchemaBindingV1 {
        DurableKeySchemaBindingV1::new(
            ContractLineage::new("budget").expect("lineage"),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([0x22; 32]),
        )
    }

    fn filtered_binding(lineage: &str) -> DurableKeySchemaBindingV1 {
        DurableKeySchemaBindingV1::new(
            ContractLineage::new(lineage).expect("lineage"),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([0x22; 32]),
        )
    }

    fn filtered_range() -> IndexRangeTarget {
        filtered_range_for(1)
    }

    fn filtered_range_for(partition: u64) -> IndexRangeTarget {
        let mut prefix = IndexRangePrefixBuilder::new(IndexId::new(7).expect("index"));
        prefix.push_u64(19).expect("prefix component");
        IndexRangeTarget::new(filtered_partition(partition), prefix.finish())
    }

    fn filtered_index_key(value: u64) -> riffdb_types::IndexEntryKey {
        let mut entity = EntityKeyBuilder::new(EntityTypeId::new(1).expect("entity type"));
        entity.push_u64(value).expect("entity component");
        let mut index = IndexEntryKeyBuilder::new(IndexId::new(7).expect("index"));
        index.push_u64(19).expect("index component");
        index
            .finish(entity.finish().expect("entity key"))
            .expect("index key")
    }

    fn filtered_partition(value: u64) -> riffdb_types::PartitionKey {
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
        partition.push_u64(value).expect("partition component");
        partition.finish().expect("partition")
    }

    fn filtered_row(value: u64, partition: u64, lineage: &str) -> StoredIndexEntryV2 {
        StoredIndexEntryV2::new(
            filtered_index_key(value),
            filtered_binding(lineage),
            CanonicalRecord::new(Vec::new()).expect("covered values"),
            filtered_partition(partition),
        )
        .expect("V2 row")
    }

    fn filtered_request(
        partition: u64,
        after: Option<riffdb_types::IndexEntryKey>,
        limit: u16,
    ) -> FilteredAuthoritativeIndexScanRequest {
        let filter = IndexPartitionFilter::new(
            ContractLineage::new("application-test").expect("lineage"),
            IndexPartitionFilterScope::Explicit(vec![filtered_partition(partition)]),
        )
        .expect("filter");
        FilteredAuthoritativeIndexScanRequest::new(
            filtered_range_for(partition),
            filter,
            after,
            StorageScanLimit::new(limit).expect("limit"),
        )
        .expect("request")
    }

    fn seed_filtered_rows(ports: &RedbOperationalPorts, rows: Vec<StoredIndexEntryV2>) {
        let target = filtered_range();
        let generations = rows
            .iter()
            .map(|row| {
                PartitionIndexTarget::new(row.partition_key().clone(), target.prefix().index_id())
            })
            .collect::<std::collections::BTreeSet<_>>();
        let access = ports.begin_write().expect("begin filtered seed");
        {
            let mut table = access
                .transaction()
                .expect("seed transaction")
                .open_table(SECONDARY_INDEXES)
                .expect("index table");
            for row in rows {
                let encoded = encode_index_entry_v2(&row).expect("encode V2 row");
                assert!(
                    table
                        .insert(row.key().as_bytes(), encoded.as_bytes())
                        .expect("insert V2 row")
                        .is_none()
                );
            }
        }
        {
            let mut table = access
                .transaction()
                .expect("seed transaction")
                .open_table(INDEX_EPOCHS)
                .expect("epoch table");
            for generation in generations {
                let stored_epoch = StoredIndexEpochV1::new(
                    generation.clone(),
                    filtered_binding("application-test"),
                    IndexEpoch::first(),
                );
                let encoded_epoch =
                    encode_index_epoch_v1(&stored_epoch).expect("encode index epoch");
                let key = encode_partition_index_key(&generation);
                assert!(
                    table
                        .insert(key.as_slice(), encoded_epoch.as_bytes())
                        .expect("insert index generation")
                        .is_none()
                );
            }
        }
        access.commit().expect("commit filtered seed");
    }

    fn stored_commit(sequence: CommitSequence) -> StoredCommitRecordV1 {
        let actor = AdmittedActorContext::new(
            ActorId::new("maintainer").expect("actor"),
            ActorKind::Human,
            TenantScope::Global,
            None,
        );
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_u64(1).expect("partition component");
        let partition_hash =
            hash_partition_key(partition.finish().expect("partition key").as_bytes());
        let sequence_byte = u8::try_from(sequence.get()).expect("small test sequence");
        StoredCommitRecordV1::new(
            sequence,
            RequestId::from_bytes(uuid_bytes(sequence_byte.wrapping_add(0x20)))
                .expect("request ID"),
            plan(),
            CanonicalInputHash::from_bytes([sequence_byte; 32]),
            actor,
            LogicalTime::new(Timestamp::new(i64::from(sequence_byte), 0).expect("timestamp")),
            partition_hash,
            Vec::new(),
            StoredReadDependenciesV1::from_live(
                &ReadDependencies::new(Vec::new()).expect("empty dependencies"),
            )
            .expect("stored dependencies"),
            Vec::new(),
            Vec::new(),
            DeclaredOutcome::new(
                OutcomeId::first(),
                CanonicalRecord::new(Vec::new()).expect("outcome fields"),
            )
            .expect("outcome"),
            ProvenanceId::from_bytes(uuid_bytes(sequence_byte.wrapping_add(0x40)))
                .expect("provenance ID"),
            Vec::new(),
            DurabilityMode::Sync,
        )
        .expect("stored commit")
    }

    fn seed_commit(ports: &RedbOperationalPorts, sequence: CommitSequence) {
        let encoded = encode_commit_record_v1(&stored_commit(sequence)).expect("encode commit");
        let next = sequence.checked_next().map_or(
            ApplicationSequenceAllocator::Exhausted,
            ApplicationSequenceAllocator::Next,
        );
        let encoded_allocator =
            encode_application_sequence_allocator_v1(next).expect("encode allocator");
        let key = encode_application_sequence_key(sequence);
        let access = ports.begin_write().expect("begin commit seed");
        {
            let mut table = access
                .transaction()
                .expect("seed transaction")
                .open_table(COMMITS)
                .expect("commit table");
            assert!(
                table
                    .insert(key.as_slice(), encoded.as_bytes())
                    .expect("insert commit")
                    .is_none()
            );
        }
        {
            let mut meta = access
                .transaction()
                .expect("seed transaction")
                .open_table(META)
                .expect("meta table");
            assert!(
                meta.insert(META_APPLICATION_SEQUENCE, encoded_allocator.as_bytes())
                    .expect("advance allocator")
                    .is_some()
            );
        }
        access.commit().expect("commit seed");
    }

    fn replace_application_allocator(
        ports: &RedbOperationalPorts,
        allocator: ApplicationSequenceAllocator,
    ) {
        let encoded =
            encode_application_sequence_allocator_v1(allocator).expect("encode allocator");
        let access = ports.begin_write().expect("begin allocator update");
        {
            let mut meta = access
                .transaction()
                .expect("allocator transaction")
                .open_table(META)
                .expect("meta table");
            assert!(
                meta.insert(META_APPLICATION_SEQUENCE, encoded.as_bytes())
                    .expect("replace allocator")
                    .is_some()
            );
        }
        access.commit().expect("commit allocator update");
    }

    fn event_partition_hash() -> riffdb_types::PartitionKeyHash {
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_u64(77).expect("partition component");
        hash_partition_key(partition.finish().expect("partition key").as_bytes())
    }

    fn seed_event_route(ports: &RedbOperationalPorts, sequence: CommitSequence) {
        let event_id = EventId::new(sequence, 0);
        let event_type_id = EventTypeId::new(7).expect("event type");
        let payload = CanonicalRecord::new(vec![(
            riffdb_types::FieldId::first(),
            riffdb_types::CanonicalValue::U64(sequence.get()),
        )])
        .expect("event payload");
        let event_hash =
            derive_event_hash_v1(event_id, event_type_id, &payload).expect("derive event hash");
        let event = StoredDurableEventV1::new(event_id, event_type_id, payload, event_hash)
            .expect("durable event");
        let route = StoredEventRouteV1::new(event_id, event_type_id, event_hash);
        let encoded_event = encode_durable_event_v1(&event).expect("encode event");
        let encoded_route = encode_event_route_v1(route).expect("encode event route");
        let event_key = encode_event_key(event_id);
        let route_key = encode_event_route_key(event_partition_hash(), event_id);
        let access = ports.begin_write().expect("begin event-route seed");
        {
            let mut events = access
                .transaction()
                .expect("seed transaction")
                .open_table(EVENTS)
                .expect("event table");
            assert!(
                events
                    .insert(event_key.as_slice(), encoded_event.as_bytes())
                    .expect("insert event")
                    .is_none()
            );
        }
        {
            let mut routes = access
                .transaction()
                .expect("seed transaction")
                .open_table(EVENT_ROUTES)
                .expect("event-route table");
            assert!(
                routes
                    .insert(route_key.as_slice(), encoded_route.as_bytes())
                    .expect("insert event route")
                    .is_none()
            );
        }
        access.commit().expect("commit event-route seed");
    }

    #[test]
    fn empty_reads_return_absence_and_exact_end() {
        let (_path, ports) = operational("empty");
        let target = entity_target(1);
        assert_eq!(ports.read_entity(&target).expect("entity read"), None);
        assert_eq!(
            ports
                .read_stored_outcome(&idempotency_identity())
                .expect("outcome read"),
            None
        );
        assert_eq!(
            ports
                .read_commit(CommitSequence::first())
                .expect("commit read"),
            None
        );

        let snapshot = ports
            .read_snapshot(
                SnapshotRequest::new(plan(), vec![target.clone()], Vec::new(), Vec::new())
                    .expect("snapshot request"),
            )
            .expect("snapshot");
        assert_eq!(snapshot.bindings(), &[EntityObservation::Absent(target)]);
        assert_eq!(snapshot.observed_through(), None);

        let commits = ports
            .scan_commits(CommitScanRequest::initial(
                StorageScanLimit::new(10).expect("limit"),
            ))
            .expect("commit scan");
        assert!(matches!(
            commits,
            CommitScanPageV1::ExactEnd {
                records,
                inclusive_upper: FrontierPosition::BeforeFirst,
            } if records.is_empty()
        ));
    }

    #[test]
    fn snapshot_frontier_allocator_mapping_is_closed_and_exact() {
        let second = CommitSequence::first().checked_next().expect("second");
        assert_eq!(
            snapshot_head_from_allocator(ApplicationSequenceAllocator::initial()),
            None
        );
        assert_eq!(
            snapshot_head_from_allocator(ApplicationSequenceAllocator::Next(second)),
            Some(CommitSequence::first())
        );
        assert_eq!(
            snapshot_head_from_allocator(ApplicationSequenceAllocator::Exhausted),
            CommitSequence::new(u64::MAX)
        );
    }

    #[test]
    fn snapshot_frontier_rejects_allocator_authority_presence_mismatch() {
        let (_path, ports) = operational("snapshot-frontier-presence");
        let second = CommitSequence::first().checked_next().expect("second");
        replace_application_allocator(&ports, ApplicationSequenceAllocator::Next(second));
        let access = ports.begin_read().expect("read transaction");
        assert!(matches!(
            read_snapshot_head(&access),
            Err(error) if error.kind() == StorageErrorKind::CorruptData
        ));
    }

    #[test]
    fn snapshot_frontier_does_not_decode_retained_command_history() {
        let (_path, ports) = operational("snapshot-frontier-constant");
        let first = CommitSequence::first();
        let access = ports.begin_write().expect("begin malformed history seed");
        {
            let mut commits = access
                .transaction()
                .expect("history transaction")
                .open_table(COMMITS)
                .expect("commit table");
            assert!(
                commits
                    .insert(
                        encode_application_sequence_key(first).as_slice(),
                        &[0x5a_u8; 4096][..],
                    )
                    .expect("insert opaque retained history")
                    .is_none()
            );
        }
        access.commit().expect("commit malformed history seed");
        replace_application_allocator(
            &ports,
            ApplicationSequenceAllocator::Next(first.checked_next().expect("second")),
        );

        let access = ports.begin_read().expect("read transaction");
        assert_eq!(
            read_snapshot_head(&access).expect("constant-time frontier"),
            Some(first)
        );
        let transaction = match &access {
            RedbReadAccess::Current(transaction) => transaction,
            RedbReadAccess::Durable(transaction) => transaction,
            RedbReadAccess::Composite(_) => panic!("test does not publish a composite view"),
        };
        let commits = transaction.open_table(COMMITS).expect("commit table");
        assert!(matches!(
            read_commit_head(transaction, &commits),
            Err(error) if error.kind() == StorageErrorKind::CorruptData
        ));
    }

    #[test]
    fn snapshot_frontier_accepts_exact_fully_pruned_authority_witness() {
        let (_path, ports) = operational("snapshot-frontier-pruned");
        let first = CommitSequence::first();
        replace_application_allocator(
            &ports,
            ApplicationSequenceAllocator::Next(first.checked_next().expect("second")),
        );
        let watermark = StoredRetentionWatermarkV1::new(
            first.get(),
            riffdb_storage_api::HISTORY_INCARNATION_INITIAL,
            Some(SchemaHash::from_bytes([0x42; 32])),
        )
        .expect("watermark");
        let encoded = encode_retention_watermark_v1(&watermark).expect("encode watermark");
        let access = ports.begin_write().expect("begin watermark seed");
        {
            let mut meta = access
                .transaction()
                .expect("watermark transaction")
                .open_table(META)
                .expect("meta table");
            meta.insert(META_RETENTION_WATERMARK, encoded.as_bytes())
                .expect("insert watermark");
        }
        access.commit().expect("commit watermark seed");

        let access = ports.begin_read().expect("read transaction");
        assert_eq!(
            read_snapshot_head(&access).expect("pruned frontier"),
            Some(first)
        );
    }

    #[test]
    fn commit_continuation_reuses_the_initial_frozen_head() {
        let (_path, ports) = operational("commit-fence");
        let first = CommitSequence::first();
        let second = first.checked_next().expect("second sequence");
        let third = second.checked_next().expect("third sequence");
        seed_commit(&ports, first);
        seed_commit(&ports, second);

        let limit = StorageScanLimit::new(1).expect("limit");
        let initial = ports
            .scan_commits(CommitScanRequest::initial(limit))
            .expect("initial commit page");
        assert_eq!(
            initial.inclusive_upper(),
            FrontierPosition::AppliedThrough(second)
        );
        let CommitScanPageV1::Page { next_after, .. } = initial else {
            panic!("the initial frozen range requires a continuation");
        };
        assert_eq!(next_after, first);

        seed_commit(&ports, third);
        let continuation = ports
            .scan_commits(
                CommitScanRequest::continuing(next_after, second, limit)
                    .expect("continuation request"),
            )
            .expect("continued commit page");
        assert_eq!(
            continuation.inclusive_upper(),
            FrontierPosition::AppliedThrough(second)
        );
        assert!(matches!(
            continuation,
            CommitScanPageV1::ExactEnd { records, .. }
                if records.len() == 1 && records[0].value().commit_sequence() == second
        ));

        let fresh = ports
            .scan_commits(CommitScanRequest::initial(
                StorageScanLimit::new(3).expect("fresh limit"),
            ))
            .expect("fresh commit page");
        assert_eq!(
            fresh.inclusive_upper(),
            FrontierPosition::AppliedThrough(third)
        );
        assert!(matches!(
            fresh,
            CommitScanPageV1::ExactEnd { records, .. } if records.len() == 3
        ));
    }

    #[test]
    fn event_route_continuation_reuses_the_initial_frozen_partition_head() {
        let (_path, ports) = operational("event-route-fence");
        let first = CommitSequence::first();
        let second = first.checked_next().expect("second sequence");
        let third = second.checked_next().expect("third sequence");
        seed_event_route(&ports, first);
        seed_event_route(&ports, second);

        let limit = EventRoutePageLimit::new(NonZeroU16::MIN).expect("route limit");
        let initial = ports
            .scan_partition_event_routes(EventRouteScanRequestV1::initial(
                event_partition_hash(),
                None,
                limit,
            ))
            .expect("initial route page");
        assert_eq!(
            initial.inclusive_upper(),
            EventRouteUpperFenceV1::Inclusive(EventId::new(second, 0))
        );
        let continuation = initial.continuation().expect("route continuation");
        assert_eq!(continuation.after(), EventId::new(first, 0));

        seed_event_route(&ports, third);
        let continued = ports
            .scan_partition_event_routes(EventRouteScanRequestV1::continuing(continuation, limit))
            .expect("continued route page");
        assert_eq!(
            continued.inclusive_upper(),
            EventRouteUpperFenceV1::Inclusive(EventId::new(second, 0))
        );
        assert!(matches!(
            continued,
            EventRouteScanV1::ExactEnd { items, .. }
                if items.len() == 1 && items[0].value().event_id() == EventId::new(second, 0)
        ));

        let fresh = ports
            .scan_partition_event_routes(EventRouteScanRequestV1::initial(
                event_partition_hash(),
                None,
                EventRoutePageLimit::new(NonZeroU16::new(3).expect("nonzero"))
                    .expect("fresh limit"),
            ))
            .expect("fresh route page");
        assert_eq!(
            fresh.inclusive_upper(),
            EventRouteUpperFenceV1::Inclusive(EventId::new(third, 0))
        );
        assert!(matches!(
            fresh,
            EventRouteScanV1::ExactEnd { items, .. } if items.len() == 3
        ));
    }

    #[test]
    fn index_scan_preserves_exact_envelope_charge() {
        let (_path, ports) = operational("index");
        let index_id = IndexId::new(3).expect("index ID");
        let mut index_key = IndexEntryKeyBuilder::new(index_id);
        index_key.push_u64(9).expect("index component");
        let key = index_key
            .finish(entity_target(1).key().clone())
            .expect("index key");
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_u64(1).expect("partition component");
        let partition = partition.finish().expect("partition key");
        let target = IndexRangeTarget::new(
            partition.clone(),
            IndexRangePrefixBuilder::new(index_id).finish(),
        );
        let stored = StoredIndexEntryV2::new(
            key.clone(),
            binding(),
            CanonicalRecord::new(Vec::new()).expect("covered values"),
            partition.clone(),
        )
        .expect("stored index entry");
        let encoded = encode_index_entry_v2(&stored).expect("encode index entry");
        let generation = PartitionIndexTarget::new(partition.clone(), index_id);
        let stored_epoch =
            StoredIndexEpochV1::new(generation.clone(), binding(), IndexEpoch::first());
        let encoded_epoch = encode_index_epoch_v1(&stored_epoch).expect("encode index epoch");
        let access = ports.begin_write().expect("begin write");
        {
            let mut table = access
                .transaction()
                .expect("transaction")
                .open_table(SECONDARY_INDEXES)
                .expect("index table");
            table
                .insert(key.as_bytes(), encoded.as_bytes())
                .expect("insert index row");
        }
        {
            let mut table = access
                .transaction()
                .expect("transaction")
                .open_table(INDEX_EPOCHS)
                .expect("epoch table");
            table
                .insert(
                    encode_partition_index_key(&generation).as_slice(),
                    encoded_epoch.as_bytes(),
                )
                .expect("insert index epoch");
        }
        access.commit().expect("commit index row");

        let request = AuthoritativeIndexScanRequest::new(
            target.clone(),
            None,
            StorageScanLimit::new(10).expect("limit"),
        )
        .expect("scan request");
        let page = ports.scan_index(request).expect("scan index");
        let AuthoritativeIndexScanPage::ExactEnd { entries, epoch } = page else {
            panic!("one-row index is an exact-end page");
        };
        assert_eq!(epoch, IndexEpochPosition::Value(IndexEpoch::first()));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].value().key(), &key);
        assert_eq!(
            entries[0].encoded_content_charge(),
            encoded.encoded_content_charge()
        );

        let snapshot = ports
            .read_snapshot(
                SnapshotRequest::new(plan(), Vec::new(), Vec::new(), vec![target])
                    .expect("snapshot request"),
            )
            .expect("snapshot");
        assert_eq!(snapshot.ranges().len(), 1);
        assert_eq!(
            snapshot.ranges()[0].epoch(),
            IndexEpochPosition::Value(IndexEpoch::first())
        );
        assert_eq!(snapshot.ranges()[0].entries()[0].key(), &key);
    }

    #[test]
    fn filtered_scan_requires_one_exact_partition_and_enforces_lineage() {
        let (_path, ports) = operational("filtered-partitions");
        seed_filtered_rows(
            &ports,
            vec![
                filtered_row(1, 1, "application-test"),
                filtered_row(2, 2, "foreign"),
                filtered_row(3, 3, "application-test"),
            ],
        );

        let explicit = ports
            .scan_index_filtered(filtered_request(3, None, 500))
            .expect("explicit scan");
        assert!(matches!(
            explicit,
            FilteredAuthoritativeIndexScanPage::ExactEnd { ref entries, .. }
                if entries.len() == 1 && entries[0].value().key() == &filtered_index_key(3)
        ));

        for scope in [
            IndexPartitionFilterScope::All,
            IndexPartitionFilterScope::None,
            IndexPartitionFilterScope::Explicit(vec![filtered_partition(1), filtered_partition(3)]),
        ] {
            let filter = IndexPartitionFilter::new(
                ContractLineage::new("application-test").expect("lineage"),
                scope,
            )
            .expect("structural filter");
            assert!(matches!(
                FilteredAuthoritativeIndexScanRequest::new(
                    filtered_range(),
                    filter,
                    None,
                    StorageScanLimit::new(500).expect("limit"),
                ),
                Err(StorageValueError::InvalidShape)
            ));
        }
    }

    #[test]
    fn sparse_filtered_scan_stops_after_500_physical_candidates() {
        let (_path, ports) = operational("filtered-sparse");
        seed_filtered_rows(
            &ports,
            (1_u64..=501)
                .map(|value| filtered_row(value, 1, "foreign"))
                .collect(),
        );

        let first = ports
            .scan_index_filtered(filtered_request(1, None, 500))
            .expect("first sparse page");
        let FilteredAuthoritativeIndexScanPage::Page {
            entries,
            scanned_through,
            epoch,
        } = first
        else {
            panic!("501 physical candidates require sparse progress");
        };
        assert!(entries.is_empty());
        assert_eq!(scanned_through, filtered_index_key(500));
        assert_eq!(epoch, IndexEpochPosition::Value(IndexEpoch::first()));

        let final_page = ports
            .scan_index_filtered(filtered_request(1, Some(scanned_through), 500))
            .expect("final sparse page");
        assert!(matches!(
            final_page,
            FilteredAuthoritativeIndexScanPage::ExactEnd { ref entries, epoch }
                if entries.is_empty()
                    && epoch == IndexEpochPosition::Value(IndexEpoch::first())
        ));
    }

    #[test]
    fn filtered_scan_return_limit_advances_only_through_inspected_rows() {
        let (_path, ports) = operational("filtered-limit");
        seed_filtered_rows(
            &ports,
            vec![
                filtered_row(1, 1, "application-test"),
                filtered_row(2, 1, "application-test"),
                filtered_row(3, 1, "application-test"),
            ],
        );

        let first = ports
            .scan_index_filtered(filtered_request(1, None, 2))
            .expect("bounded first page");
        let FilteredAuthoritativeIndexScanPage::Page {
            entries,
            scanned_through,
            ..
        } = first
        else {
            panic!("one eligible row remains");
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].value().key(), &filtered_index_key(1));
        assert_eq!(entries[1].value().key(), &filtered_index_key(2));
        assert_eq!(scanned_through, filtered_index_key(2));

        let final_page = ports
            .scan_index_filtered(filtered_request(1, Some(scanned_through), 2))
            .expect("bounded final page");
        assert!(matches!(
            final_page,
            FilteredAuthoritativeIndexScanPage::ExactEnd { ref entries, .. }
                if entries.len() == 1 && entries[0].value().key() == &filtered_index_key(3)
        ));
    }

    #[test]
    fn entity_key_payload_mismatch_is_corruption() {
        let (_path, ports) = operational("entity-mismatch");
        let stored_target = entity_target(1);
        let lookup_target = entity_target(2);
        let stored = StoredEntityRecordV1::new(
            stored_target,
            EntityVersion::new(1).expect("entity version"),
            ContractVersion::new(1).expect("contract version"),
            binding(),
            CanonicalRecord::new(Vec::new()).expect("fields"),
        )
        .expect("stored entity");
        let encoded = encode_entity_record_v1(&stored).expect("encode entity");
        let access = ports.begin_write().expect("begin write");
        {
            let mut table = access
                .transaction()
                .expect("transaction")
                .open_table(ENTITIES)
                .expect("entity table");
            table
                .insert(lookup_target.key().as_bytes(), encoded.as_bytes())
                .expect("insert mismatched entity");
        }
        access.commit().expect("commit mismatched entity");

        let error = ports
            .read_entity(&lookup_target)
            .expect_err("key/payload mismatch must fail");
        assert_eq!(error.kind(), StorageErrorKind::CorruptData);
    }
}
