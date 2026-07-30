//! Owned authoritative reads over short redb read transactions.

use std::ops::Bound::{Excluded, Included, Unbounded};

use redb::{ReadOnlyTable, ReadTransaction, ReadableTable};
use riffdb_storage_api::{
    AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest, AuthoritativePointReader,
    AuthoritativeScanReader, CommitScanPageV1, CommitScanRequest, EncodedPageItem,
    EntityObservation, EntityTarget, FilteredAuthoritativeIndexScanPage,
    FilteredAuthoritativeIndexScanRequest, FilteredAuthoritativeScanReader, IdempotencyIdentity,
    IndexEpochPosition, IndexRangeEntry, IndexRangeTarget, MAX_COMMIT_SCAN_PAGE_BYTES,
    MAX_INDEX_SCAN_INSPECTED_BYTES, MAX_INDEX_SCAN_INSPECTED_ENTRIES, MAX_SCAN_PAGE_BYTES,
    ReadSnapshot, ReadSnapshotBuilder, SnapshotReader, SnapshotRequest, StorageError,
    StorageErrorKind, StorageValueError, StoredCommitRecordV1, StoredDurableEventV1,
    StoredEntityRecordV1, StoredOutcomeV1, StoredProvenanceRecordV1,
};
use riffdb_types::{CommitSequence, EventId, FrontierPosition, ProvenanceId};

use crate::codec::{
    IdempotencyRecordV1, decode_commit_with_event_table, decode_durable_event_v1,
    decode_entity_record_v1, decode_idempotency_record_v1, decode_index_entry_v2,
    decode_index_epoch_v1, decode_provenance_record_v1,
};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::keys::{
    decode_application_sequence_key, decode_index_entry_key, encode_application_sequence_key,
    encode_entity_key, encode_event_key, encode_idempotency_key, encode_provenance_key,
};
use crate::layout::{
    COMMITS, ENTITIES, EVENTS, IDEMPOTENCY, INDEX_EPOCHS, PROVENANCE, SECONDARY_INDEXES,
};
use crate::store::RedbOperationalPorts;

type BytesTable = ReadOnlyTable<&'static [u8], &'static [u8]>;

impl SnapshotReader for RedbOperationalPorts {
    fn read_snapshot(&self, request: SnapshotRequest) -> Result<ReadSnapshot, StorageError> {
        let transaction = self.begin_read()?;
        let entities = transaction.open_table(ENTITIES).map_err(table_error)?;
        let index_entries = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(table_error)?;
        let index_epochs = transaction.open_table(INDEX_EPOCHS).map_err(table_error)?;
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;

        let observed_through = read_commit_head(&transaction, &commits)?;
        let mut snapshot =
            ReadSnapshotBuilder::new(&request, observed_through).map_err(materialization_value)?;

        for target in request.binding_targets() {
            snapshot
                .push_binding(read_entity_observation(&entities, target)?)
                .map_err(materialization_value)?;
        }
        for target in request.root_validation_targets() {
            snapshot
                .push_root_validation(read_entity_observation(&entities, target)?)
                .map_err(materialization_value)?;
        }
        for target in request.range_targets() {
            let epoch = read_epoch_position(&index_epochs, target)?;
            let mut range = snapshot
                .begin_range(target.clone(), epoch)
                .map_err(materialization_value)?;
            let prefix = target.prefix().as_bytes();
            let mut entries = index_entries
                .range(prefix..)
                .map_err(precommit_storage_error)?;
            for entry in &mut entries {
                let (physical_key, encoded) = entry.map_err(precommit_storage_error)?;
                if !physical_key.value().starts_with(prefix) {
                    break;
                }
                let key = decode_index_entry_key(physical_key.value()).map_err(|_| corrupt())?;
                let decoded = decode_index_entry_v2(encoded.value())?;
                if decoded.value().key() != &key {
                    return Err(corrupt());
                }
                let row = IndexRangeEntry::new(
                    key.index_id(),
                    key,
                    decoded.value().covered_values().clone(),
                )
                .map_err(corrupt_value)?;
                range.push_entry(row).map_err(materialization_value)?;
            }
            range.finish().map_err(materialization_value)?;
        }

        snapshot.finish().map_err(materialization_value)
    }
}

impl AuthoritativePointReader for RedbOperationalPorts {
    fn read_entity(
        &self,
        target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        let transaction = self.begin_read()?;
        let table = transaction.open_table(ENTITIES).map_err(table_error)?;
        read_entity_record(&table, target)
    }

    fn read_stored_outcome(
        &self,
        identity: &IdempotencyIdentity,
    ) -> Result<Option<StoredOutcomeV1>, StorageError> {
        let key = identity
            .storage_key()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let transaction = self.begin_read()?;
        let table = transaction.open_table(IDEMPOTENCY).map_err(table_error)?;
        let Some(encoded) = table
            .get(encode_idempotency_key(&key))
            .map_err(precommit_storage_error)?
        else {
            return Ok(None);
        };
        let decoded = decode_idempotency_record_v1(encoded.value())?;
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
        }
    }

    fn read_commit(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<StoredCommitRecordV1>, StorageError> {
        let transaction = self.begin_read()?;
        let table = transaction.open_table(COMMITS).map_err(table_error)?;
        let encoded_key = encode_application_sequence_key(sequence);
        let Some(encoded) = table
            .get(encoded_key.as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(None);
        };
        let record = decode_commit_in_snapshot(&transaction, encoded.value())?
            .into_parts()
            .0;
        if record.commit_sequence() != sequence {
            return Err(corrupt());
        }
        Ok(Some(record))
    }

    fn read_provenance(
        &self,
        provenance_id: ProvenanceId,
    ) -> Result<Option<StoredProvenanceRecordV1>, StorageError> {
        let transaction = self.begin_read()?;
        let table = transaction.open_table(PROVENANCE).map_err(table_error)?;
        let encoded_key = encode_provenance_key(provenance_id);
        let Some(encoded) = table
            .get(encoded_key.as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(None);
        };
        let record = decode_provenance_record_v1(encoded.value())?.into_parts().0;
        if record.provenance_id() != provenance_id {
            return Err(corrupt());
        }
        Ok(Some(record))
    }

    fn read_durable_event(
        &self,
        event_id: EventId,
    ) -> Result<Option<StoredDurableEventV1>, StorageError> {
        let transaction = self.begin_read()?;
        let table = transaction.open_table(EVENTS).map_err(table_error)?;
        let encoded_key = encode_event_key(event_id);
        let Some(encoded) = table
            .get(encoded_key.as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(None);
        };
        let event = decode_durable_event_v1(encoded.value())?.into_parts().0;
        if event.event_id() != event_id {
            return Err(corrupt());
        }
        Ok(Some(event))
    }
}

pub(crate) fn decode_commit_in_snapshot(
    transaction: &ReadTransaction,
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredCommitRecordV1>, StorageError> {
    let table = transaction.open_table(EVENTS).map_err(table_error)?;
    decode_commit_with_event_table(encoded, &table)
}

impl AuthoritativeScanReader for RedbOperationalPorts {
    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        let transaction = self.begin_read()?;
        let table = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(table_error)?;
        let index_epochs = transaction.open_table(INDEX_EPOCHS).map_err(table_error)?;
        let epoch = read_epoch_position(&index_epochs, request.target())?;
        let prefix = request.target().prefix().as_bytes();
        let mut scan = match request.after() {
            Some(after) => table
                .range::<&[u8]>((Excluded(after.as_bytes()), Unbounded))
                .map_err(precommit_storage_error)?,
            None => table.range(prefix..).map_err(precommit_storage_error)?,
        };
        let wanted = usize::from(request.limit().get());
        let mut entries = Vec::with_capacity(wanted);
        let mut encoded_bytes = 0usize;
        let mut has_more = false;

        for entry in &mut scan {
            let (physical_key, encoded) = entry.map_err(precommit_storage_error)?;
            if !physical_key.value().starts_with(prefix) {
                break;
            }
            let key = decode_index_entry_key(physical_key.value()).map_err(|_| corrupt())?;
            if entries.len() == wanted {
                has_more = true;
                break;
            }
            let decoded = decode_index_entry_v2(encoded.value())?;
            if decoded.value().key() != &key {
                return Err(corrupt());
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
        let transaction = self.begin_read()?;
        let table = transaction.open_table(COMMITS).map_err(table_error)?;
        let inclusive_upper = match request.inclusive_upper() {
            Some(sequence) => FrontierPosition::AppliedThrough(sequence),
            None => read_commit_head(&transaction, &table)?.map_or(
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
        let start = encode_application_sequence_key(first_expected);
        let end = encode_application_sequence_key(upper);
        let mut scan = table
            .range::<&[u8]>((Included(start.as_slice()), Included(end.as_slice())))
            .map_err(precommit_storage_error)?;
        let wanted = usize::from(request.limit().get());
        let mut records = Vec::with_capacity(wanted);
        let mut encoded_bytes = 0usize;
        let mut has_more = false;
        let mut expected = Some(first_expected);

        for entry in &mut scan {
            let (physical_key, encoded) = entry.map_err(precommit_storage_error)?;
            let sequence =
                decode_application_sequence_key(physical_key.value()).map_err(|_| corrupt())?;
            if Some(sequence) != expected {
                return Err(corrupt());
            }
            if records.len() == wanted {
                has_more = true;
                break;
            }
            let decoded = decode_commit_in_snapshot(&transaction, encoded.value())?;
            if decoded.value().commit_sequence() != sequence {
                return Err(corrupt());
            }
            let next_bytes = encoded_bytes
                .checked_add(decoded.encoded_content_charge().get())
                .ok_or_else(corrupt)?;
            if next_bytes > MAX_COMMIT_SCAN_PAGE_BYTES {
                if records.is_empty() {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
                has_more = true;
                break;
            }
            encoded_bytes = next_bytes;
            records.push(decoded);
            expected = sequence.checked_next();
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
        let transaction = self.begin_read()?;
        let table = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(table_error)?;
        let index_epochs = transaction.open_table(INDEX_EPOCHS).map_err(table_error)?;
        let epoch = read_epoch_position(&index_epochs, request.target())?;
        if request.partition_filter().is_none() {
            return FilteredAuthoritativeIndexScanPage::exact_end(&request, epoch, Vec::new())
                .map_err(corrupt_value);
        }

        let prefix = request.target().prefix().as_bytes();
        let mut scan = match request.after() {
            Some(after) => table
                .range::<&[u8]>((Excluded(after.as_bytes()), Unbounded))
                .map_err(precommit_storage_error)?,
            None => table.range(prefix..).map_err(precommit_storage_error)?,
        };
        let returned_limit = usize::from(request.limit().get());
        let mut returned = Vec::with_capacity(returned_limit);
        let mut returned_bytes = 0usize;
        let mut inspected_bytes = 0usize;
        let mut scanned_through = None;
        let mut exact_end = true;

        for (candidate_index, entry) in (&mut scan).enumerate() {
            let (physical_key, encoded) = entry.map_err(precommit_storage_error)?;
            if !physical_key.value().starts_with(prefix) {
                break;
            }
            if returned.len() == returned_limit
                || candidate_index == MAX_INDEX_SCAN_INSPECTED_ENTRIES
            {
                exact_end = false;
                break;
            }

            let key = decode_index_entry_key(physical_key.value()).map_err(|_| corrupt())?;
            let decoded = decode_index_entry_v2(encoded.value())?;
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

fn read_entity_observation(
    table: &BytesTable,
    target: &EntityTarget,
) -> Result<EntityObservation, StorageError> {
    Ok(read_entity_record(table, target)?.map_or_else(
        || EntityObservation::Absent(target.clone()),
        EntityObservation::Present,
    ))
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

fn read_epoch_position(
    table: &BytesTable,
    target: &IndexRangeTarget,
) -> Result<IndexEpochPosition, StorageError> {
    let prefix = target.prefix();
    let Some(encoded) = table
        .get(prefix.as_bytes())
        .map_err(precommit_storage_error)?
    else {
        return Ok(IndexEpochPosition::BeforeFirst);
    };
    let epoch = decode_index_epoch_v1(encoded.value())?.into_parts().0;
    if epoch.target().index_id() != prefix.index_id()
        || epoch.target().as_bytes() != prefix.as_bytes()
    {
        return Err(corrupt());
    }
    Ok(IndexEpochPosition::Value(epoch.epoch()))
}

pub(crate) fn read_commit_head(
    transaction: &ReadTransaction,
    table: &BytesTable,
) -> Result<Option<CommitSequence>, StorageError> {
    let Some((physical_key, encoded)) = table.last().map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let sequence = decode_application_sequence_key(physical_key.value()).map_err(|_| corrupt())?;
    let commit = decode_commit_in_snapshot(transaction, encoded.value())?;
    if commit.value().commit_sequence() != sequence {
        return Err(corrupt());
    }
    Ok(Some(sequence))
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
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use riffdb_storage_api::{
        AuthoritativeIndexScanPage, DatabaseInitializationPort, DeclaredOutcome, DurabilityMode,
        DurableKeySchemaBindingV1, ExecutablePlanRef, IdempotencyIdentity, IdempotencyKeyDigest,
        IndexPartitionFilter, IndexPartitionFilterScope, IndexRangePrefixBuilder, ReadDependencies,
        StorageScanLimit, StoredCommitRecordV1, StoredIndexEntryV2, StoredIndexEpochV1,
        StoredReadDependenciesV1, StructurallyDecodedIndexRangePrefixV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash,
        CanonicalRecord, CommandId, ContractBundleHash, ContractLineage, ContractVersion,
        DatabaseId, DigestKeyId, EntityKeyBuilder, EntityTypeId, EntityVersion, Environment,
        IndexEntryKeyBuilder, IndexEpoch, IndexId, LogicalTime, OutcomeId, PartitionKeyBuilder,
        PlanHash, ProvenanceId, RequestId, TenantScope, Timestamp, hash_partition_key,
    };

    use super::*;
    use crate::codec::{
        encode_commit_record_v1, encode_entity_record_v1, encode_index_entry_v2,
        encode_index_epoch_v1,
    };
    use crate::layout::{COMMITS, ENTITIES, INDEX_EPOCHS, SECONDARY_INDEXES};
    use crate::store::RedbStore;

    static NEXT_TEST_PATH: AtomicU64 = AtomicU64::new(1);

    struct TestDatabasePath(PathBuf);

    impl TestDatabasePath {
        fn new(label: &str) -> Self {
            let ordinal = NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "riffdb-redb-reads-{label}-{}-{ordinal}.redb",
                std::process::id()
            )))
        }
    }

    impl Drop for TestDatabasePath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
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
        let mut prefix = IndexRangePrefixBuilder::new(IndexId::new(7).expect("index"));
        prefix.push_u64(19).expect("prefix component");
        IndexRangeTarget::new(prefix.finish())
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
        scope: IndexPartitionFilterScope,
        after: Option<riffdb_types::IndexEntryKey>,
        limit: u16,
    ) -> FilteredAuthoritativeIndexScanRequest {
        let filter = IndexPartitionFilter::new(
            ContractLineage::new("application-test").expect("lineage"),
            scope,
        )
        .expect("filter");
        FilteredAuthoritativeIndexScanRequest::new(
            filtered_range(),
            filter,
            after,
            StorageScanLimit::new(limit).expect("limit"),
        )
        .expect("request")
    }

    fn seed_filtered_rows(ports: &RedbOperationalPorts, rows: Vec<StoredIndexEntryV2>) {
        let target = filtered_range();
        let stored_epoch = StoredIndexEpochV1::new(
            StructurallyDecodedIndexRangePrefixV1::from_live(target.prefix()),
            filtered_binding("application-test"),
            IndexEpoch::first(),
        );
        let encoded_epoch = encode_index_epoch_v1(&stored_epoch).expect("encode index epoch");
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
            assert!(
                table
                    .insert(target.prefix().as_bytes(), encoded_epoch.as_bytes())
                    .expect("insert index epoch")
                    .is_none()
            );
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
        access.commit().expect("commit seed");
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
    fn index_scan_preserves_exact_envelope_charge() {
        let (_path, ports) = operational("index");
        let index_id = IndexId::new(3).expect("index ID");
        let mut index_key = IndexEntryKeyBuilder::new(index_id);
        index_key.push_u64(9).expect("index component");
        let key = index_key
            .finish(entity_target(1).key().clone())
            .expect("index key");
        let target = IndexRangeTarget::new(IndexRangePrefixBuilder::new(index_id).finish());
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_u64(1).expect("partition component");
        let stored = StoredIndexEntryV2::new(
            key.clone(),
            binding(),
            CanonicalRecord::new(Vec::new()).expect("covered values"),
            partition.finish().expect("partition key"),
        )
        .expect("stored index entry");
        let encoded = encode_index_entry_v2(&stored).expect("encode index entry");
        let stored_epoch = StoredIndexEpochV1::new(
            StructurallyDecodedIndexRangePrefixV1::from_live(target.prefix()),
            binding(),
            IndexEpoch::first(),
        );
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
                .insert(target.prefix().as_bytes(), encoded_epoch.as_bytes())
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
    fn filtered_scan_enforces_all_explicit_none_and_lineage() {
        let (_path, ports) = operational("filtered-partitions");
        seed_filtered_rows(
            &ports,
            vec![
                filtered_row(1, 1, "application-test"),
                filtered_row(2, 2, "foreign"),
                filtered_row(3, 3, "application-test"),
            ],
        );

        let all = ports
            .scan_index_filtered(filtered_request(IndexPartitionFilterScope::All, None, 500))
            .expect("all scan");
        assert_eq!(all.epoch(), IndexEpochPosition::Value(IndexEpoch::first()));
        assert!(matches!(
            all,
            FilteredAuthoritativeIndexScanPage::ExactEnd { ref entries, .. }
                if entries.len() == 2
                    && entries[0].value().key() == &filtered_index_key(1)
                    && entries[1].value().key() == &filtered_index_key(3)
        ));

        let explicit = ports
            .scan_index_filtered(filtered_request(
                IndexPartitionFilterScope::Explicit(vec![filtered_partition(3)]),
                None,
                500,
            ))
            .expect("explicit scan");
        assert!(matches!(
            explicit,
            FilteredAuthoritativeIndexScanPage::ExactEnd { ref entries, .. }
                if entries.len() == 1 && entries[0].value().key() == &filtered_index_key(3)
        ));

        let none = ports
            .scan_index_filtered(filtered_request(IndexPartitionFilterScope::None, None, 500))
            .expect("none scan");
        assert!(matches!(
            none,
            FilteredAuthoritativeIndexScanPage::ExactEnd { ref entries, .. }
                if entries.is_empty()
        ));
    }

    #[test]
    fn sparse_filtered_scan_stops_after_500_physical_candidates() {
        let (_path, ports) = operational("filtered-sparse");
        seed_filtered_rows(
            &ports,
            (1_u64..=501)
                .map(|value| filtered_row(value, value, "foreign"))
                .collect(),
        );

        let first = ports
            .scan_index_filtered(filtered_request(IndexPartitionFilterScope::All, None, 500))
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
            .scan_index_filtered(filtered_request(
                IndexPartitionFilterScope::All,
                Some(scanned_through),
                500,
            ))
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
                filtered_row(2, 2, "application-test"),
                filtered_row(3, 3, "application-test"),
            ],
        );

        let first = ports
            .scan_index_filtered(filtered_request(IndexPartitionFilterScope::All, None, 2))
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
            .scan_index_filtered(filtered_request(
                IndexPartitionFilterScope::All,
                Some(scanned_through),
                2,
            ))
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
