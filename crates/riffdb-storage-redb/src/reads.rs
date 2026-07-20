//! Owned authoritative reads over short redb read transactions.

use std::ops::Bound::{Excluded, Unbounded};

use redb::{ReadOnlyTable, ReadableTable};
use riffdb_storage_api::{
    AuthoritativeIndexScanPage, AuthoritativeIndexScanRequest, AuthoritativePointReader,
    AuthoritativeScanReader, CommitScanPageV1, CommitScanRequest, EncodedPageItem,
    EntityObservation, EntityTarget, IdempotencyIdentity, IndexEpochPosition, IndexRangeEntry,
    IndexRangeTarget, MAX_COMMIT_SCAN_PAGE_BYTES, MAX_SCAN_PAGE_BYTES, ReadSnapshot,
    ReadSnapshotBuilder, SnapshotReader, SnapshotRequest, StorageError, StorageErrorKind,
    StorageValueError, StoredCommitRecordV1, StoredDurableEventV1, StoredEntityRecordV1,
    StoredOutcomeV1, StoredProvenanceRecordV1,
};
use riffdb_types::{CommitSequence, EventId, ProvenanceId};

use crate::codec::{
    IdempotencyRecordV1, decode_commit_record_v1, decode_durable_event_v1, decode_entity_record_v1,
    decode_idempotency_record_v1, decode_index_entry_v1, decode_index_epoch_v1,
    decode_provenance_record_v1,
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

        let observed_through = read_commit_head(&commits)?;
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
                let decoded = decode_index_entry_v1(encoded.value())?;
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
        let record = decode_commit_record_v1(encoded.value())?.into_parts().0;
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

impl AuthoritativeScanReader for RedbOperationalPorts {
    fn scan_index(
        &self,
        request: AuthoritativeIndexScanRequest,
    ) -> Result<AuthoritativeIndexScanPage, StorageError> {
        let transaction = self.begin_read()?;
        let table = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(table_error)?;
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
            let decoded = decode_index_entry_v1(encoded.value())?;
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
            AuthoritativeIndexScanPage::page(&request, entries, next_after).map_err(corrupt_value)
        } else {
            AuthoritativeIndexScanPage::exact_end(&request, entries).map_err(corrupt_value)
        }
    }

    fn scan_commits(&self, request: CommitScanRequest) -> Result<CommitScanPageV1, StorageError> {
        let Some(first_expected) = request
            .after()
            .map_or(Some(CommitSequence::first()), CommitSequence::checked_next)
        else {
            return CommitScanPageV1::exact_end(request, Vec::new()).map_err(corrupt_value);
        };
        let transaction = self.begin_read()?;
        let table = transaction.open_table(COMMITS).map_err(table_error)?;
        let start = encode_application_sequence_key(first_expected);
        let mut scan = table
            .range(start.as_slice()..)
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
            let decoded = decode_commit_record_v1(encoded.value())?;
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
            CommitScanPageV1::page(request, records, next_after).map_err(corrupt_value)
        } else {
            CommitScanPageV1::exact_end(request, records).map_err(corrupt_value)
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

fn read_entity_record(
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

fn read_commit_head(table: &BytesTable) -> Result<Option<CommitSequence>, StorageError> {
    let Some((physical_key, encoded)) = table.last().map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let sequence = decode_application_sequence_key(physical_key.value()).map_err(|_| corrupt())?;
    let commit = decode_commit_record_v1(encoded.value())?;
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
        AuthoritativeIndexScanPage, DatabaseInitializationPort, DurableKeySchemaBindingV1,
        ExecutablePlanRef, IdempotencyIdentity, IdempotencyKeyDigest, IndexRangePrefixBuilder,
        StorageScanLimit, StoredIndexEntryV1,
    };
    use riffdb_types::{
        ActorId, CanonicalRecord, CommandId, ContractBundleHash, ContractLineage, ContractVersion,
        DatabaseId, DigestKeyId, EntityKeyBuilder, EntityTypeId, EntityVersion, Environment,
        IndexEntryKeyBuilder, IndexId, PlanHash, TenantScope,
    };

    use super::*;
    use crate::codec::{encode_entity_record_v1, encode_index_entry_v1};
    use crate::layout::{ENTITIES, SECONDARY_INDEXES};
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
            .scan_commits(CommitScanRequest::new(
                None,
                StorageScanLimit::new(10).expect("limit"),
            ))
            .expect("commit scan");
        assert!(matches!(commits, CommitScanPageV1::ExactEnd { records } if records.is_empty()));
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
        let stored = StoredIndexEntryV1::new(
            key.clone(),
            binding(),
            CanonicalRecord::new(Vec::new()).expect("covered values"),
        )
        .expect("stored index entry");
        let encoded = encode_index_entry_v1(&stored).expect("encode index entry");
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
        access.commit().expect("commit index row");

        let target = IndexRangeTarget::new(IndexRangePrefixBuilder::new(index_id).finish());
        let request = AuthoritativeIndexScanRequest::new(
            target.clone(),
            None,
            StorageScanLimit::new(10).expect("limit"),
        )
        .expect("scan request");
        let page = ports.scan_index(request).expect("scan index");
        let AuthoritativeIndexScanPage::ExactEnd { entries } = page else {
            panic!("one-row index is an exact-end page");
        };
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
            IndexEpochPosition::BeforeFirst
        );
        assert_eq!(snapshot.ranges()[0].entries()[0].key(), &key);
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
