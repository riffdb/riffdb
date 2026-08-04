//! Test-only constructors for stopped-database compatibility fixtures.

use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable};
use riffdb_storage_api::{StorageError, StorageErrorKind, StoredIndexEntryV1};

use crate::error::{
    codec_error, commit_error, database_error, precommit_storage_error, storage_error, table_error,
    transaction_error,
};
use crate::layout::{META, META_VALIDATED_PREFIX_CHECKPOINT, SECONDARY_INDEXES};

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
    transaction.commit().map_err(commit_error)?;
    Ok(replacements.len())
}

/// Reads the commit sequence S bound in a stopped database's durable
/// validated-prefix checkpoint, or `None` when no checkpoint row exists.
///
/// Read-only observation for real-process tests (ADR-0019 Amendment 1): a
/// graceful shutdown's final engine commit binds S at the drained frontier, so
/// the bound sequence discriminates the shutdown write from an earlier
/// startup-finish checkpoint. Must be called only after the owning process has
/// stopped.
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
        riffdb_storage_api::proto_codec::decode_validated_prefix_checkpoint_v1(encoded.value())
            .map_err(codec_error)?
            .into_parts()
            .0;
    Ok(Some(checkpoint.checkpoint_commit_sequence()))
}
