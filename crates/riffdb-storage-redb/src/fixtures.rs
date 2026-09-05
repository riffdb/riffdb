//! Test-only constructors for stopped-database compatibility fixtures.

use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, ApplicationSequenceAllocator, DatabaseInitializationPort, StorageError,
    StorageErrorKind, StoredCommitRecordV1, StoredContractBundleV1, StoredEntityRecordV1,
    StoredIndexEntryV1,
};

use crate::error::{
    codec_error, commit_error, database_error, precommit_storage_error, storage_error, table_error,
    transaction_error,
};
use crate::layout::{
    CATALOG_ACTIVE, CATALOG_ACTIVE_KEY, COMMITS, CONTRACT_BUNDLES, ENTITIES, META,
    META_APPLICATION_SEQUENCE, META_CLEAN_CLOSE_LIFECYCLE, META_VALIDATED_PREFIX_CHECKPOINT,
    SECONDARY_INDEXES,
};

/// Appends one exact authoritative entity/commit pair for server worker tests.
#[doc(hidden)]
pub fn append_columnar_worker_commit_fixture(
    ports: &crate::store::RedbOperationalPorts,
    row: &StoredEntityRecordV1,
    commit: &StoredCommitRecordV1,
) -> Result<(), StorageError> {
    let access = ports.begin_write()?;
    let transaction = access.transaction()?;
    let encoded_row = crate::codec::encode_entity_record_v1(row)?;
    transaction
        .open_table(ENTITIES)
        .map_err(table_error)?
        .insert(
            crate::keys::encode_entity_key(row.target().key()),
            encoded_row.as_bytes(),
        )
        .map_err(precommit_storage_error)?;
    let encoded_commit = crate::codec::encode_commit_record_v1(commit)?;
    let commit_key = crate::keys::encode_application_sequence_key(commit.commit_sequence());
    if transaction
        .open_table(COMMITS)
        .map_err(table_error)?
        .insert(commit_key.as_slice(), encoded_commit.as_bytes())
        .map_err(precommit_storage_error)?
        .is_some()
    {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let allocator = commit.commit_sequence().checked_next().map_or(
        ApplicationSequenceAllocator::Exhausted,
        ApplicationSequenceAllocator::Next,
    );
    let encoded_allocator = crate::codec::encode_application_sequence_allocator_v1(allocator)?;
    transaction
        .open_table(META)
        .map_err(table_error)?
        .insert(META_APPLICATION_SEQUENCE, encoded_allocator.as_bytes())
        .map_err(precommit_storage_error)?;
    access.commit_for(crate::hooks::RedbTestOperation::CommandBatch)
}

/// Seeds and opens least-authority ports for one isolated contract-migration fixture.
pub(crate) fn contract_migration_stage_ports_fixture(
    path: &Path,
    database_id: riffdb_types::DatabaseId,
    parent: &StoredContractBundleV1,
    rows: &[StoredEntityRecordV1],
) -> Result<crate::store::RedbOperationalPorts, StorageError> {
    let mut store = crate::RedbStore::open(path)?;
    store.initialize_database(database_id)?;
    let transaction = store
        .shared
        .database
        .begin_write()
        .map_err(transaction_error)?;
    let bundle_key =
        crate::keys::encode_contract_bundle_key(parent.lineage(), parent.contract_version())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let encoded_bundle = crate::codec::encode_contract_bundle_v1(parent)?;
    transaction
        .open_table(CONTRACT_BUNDLES)
        .map_err(table_error)?
        .insert(bundle_key.as_slice(), encoded_bundle.as_bytes())
        .map_err(precommit_storage_error)?;
    let active = ActiveCatalogPointerV1::from_bundle(parent);
    let encoded_active = crate::codec::encode_active_catalog_pointer_v1(&active)?;
    transaction
        .open_table(CATALOG_ACTIVE)
        .map_err(table_error)?
        .insert(CATALOG_ACTIVE_KEY.as_slice(), encoded_active.as_bytes())
        .map_err(precommit_storage_error)?;
    let mut entities = transaction.open_table(ENTITIES).map_err(table_error)?;
    for row in rows {
        let encoded = crate::codec::encode_entity_record_v1(row)?;
        entities
            .insert(
                crate::keys::encode_entity_key(row.target().key()),
                encoded.as_bytes(),
            )
            .map_err(precommit_storage_error)?;
    }
    drop(entities);
    transaction.commit().map_err(commit_error)?;
    Ok(crate::store::RedbOperationalPorts {
        shared: store.shared,
    })
}

/// Replaces every canonical V2 secondary-index row in a stopped database with
/// its canonical V1 migration source.
///
/// The normal writer remains V2-only. This function is absent from default
/// builds and must be called only after the owning process has stopped.
pub fn downgrade_all_index_rows_to_v1_fixture(path: &Path) -> Result<usize, StorageError> {
    let database = Database::create(path).map_err(database_error)?;
    let transaction = database.begin_write().map_err(transaction_error)?;
    let replacements = {
        let table = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(table_error)?;
        let mut replacements = Vec::new();
        for entry in table.iter().map_err(precommit_storage_error)? {
            let (physical_key, envelope) = entry.map_err(precommit_storage_error)?;
            let physical_key =
                riffdb_types::IndexEntryKey::from_bytes(physical_key.value().to_vec())
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            let current = crate::codec::decode_index_entry_v2(envelope.value())?;
            if current.value().key() != &physical_key {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            let legacy = StoredIndexEntryV1::new(
                physical_key.clone(),
                current.value().schema_binding().clone(),
                current.value().covered_values().clone(),
            )
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            let legacy =
                riffdb_storage_api::encode_index_entry_v1_fixture(&legacy).map_err(codec_error)?;
            replacements.push((physical_key, legacy));
        }
        replacements
    };

    {
        let mut table = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(table_error)?;
        for (physical_key, legacy) in &replacements {
            table
                .insert(physical_key.as_bytes(), legacy.as_bytes())
                .map_err(precommit_storage_error)?;
        }
    }
    // This test-only stopped-database rewrite happens after the source process
    // closed. It must invalidate that process's clean evidence so startup takes
    // the complete migration-discovery path.
    transaction
        .open_table(META)
        .map_err(table_error)?
        .remove(META_CLEAN_CLOSE_LIFECYCLE)
        .map_err(precommit_storage_error)?;
    transaction.commit().map_err(commit_error)?;
    Ok(replacements.len())
}

/// Reads the commit sequence S bound in a stopped database's durable
/// validated-prefix checkpoint, or `None` when no checkpoint row exists.
///
/// Read-only observation for stopped-database recovery tests. Must be called
/// only after the owning process has stopped.
pub fn read_validated_prefix_checkpoint_commit_sequence_fixture(
    path: &Path,
) -> Result<Option<u64>, StorageError> {
    let database = Database::open(path).map_err(database_error)?;
    let transaction = database.begin_read().map_err(transaction_error)?;
    let meta = transaction.open_table(META).map_err(table_error)?;
    let Some(encoded) = meta
        .get(META_VALIDATED_PREFIX_CHECKPOINT)
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    let checkpoint =
        riffdb_storage_api::proto_codec::decode_validated_prefix_checkpoint_v2(encoded.value())
            .map_err(codec_error)?
            .into_parts()
            .0;
    Ok(Some(checkpoint.base().checkpoint_commit_sequence()))
}

/// Reads the exact retained checkpoint envelope bytes from a stopped database.
pub fn read_validated_prefix_checkpoint_bytes_fixture(
    path: &Path,
) -> Result<Option<Vec<u8>>, StorageError> {
    let database = Database::open(path).map_err(database_error)?;
    let transaction = database.begin_read().map_err(transaction_error)?;
    let meta = transaction.open_table(META).map_err(table_error)?;
    Ok(meta
        .get(META_VALIDATED_PREFIX_CHECKPOINT)
        .map_err(precommit_storage_error)?
        .map(|encoded| encoded.value().to_vec()))
}
