//! Offline storage compaction over a closed database file (ADR-0311).

use std::fs::File;
use std::path::{Path, PathBuf};

use redb::ReadableDatabase;
use riffdb_storage_api::{HISTORY_INCARNATION_INITIAL, StorageError, StorageErrorKind};
use riffdb_types::DatabaseId;

use crate::codec;
use crate::error::{
    database_error, precommit_storage_error, storage_error, table_error, transaction_error,
};
use crate::layout::{META, META_DATABASE_ID, META_HISTORY_INCARNATION};
use crate::retention::{load_watermark, verify_tombstone_chain};
use crate::store::RedbStore;

/// Bounded maximum compaction passes per offline compaction invocation.
pub const MAX_COMPACTION_PASSES: usize = 64;

/// Result of an offline storage compaction run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageCompactionResultV1 {
    /// Authoritative database identity verified before and after compaction.
    pub database_id: DatabaseId,
    /// On-disk byte size of the primary database file before compaction.
    pub bytes_before: u64,
    /// On-disk byte size of the primary database file after compaction.
    pub bytes_after: u64,
    /// Physical bytes reclaimed (`bytes_before.saturating_sub(bytes_after)`).
    pub reclaimed_bytes: u64,
    /// Whether `redb::Database::compact()` relocated and freed pages.
    pub compacted: bool,
    /// Retention watermark sequence verified before and after compaction (`0` when unpruned).
    pub retention_watermark_sequence: u64,
    /// History incarnation verified before and after compaction.
    pub history_incarnation: u64,
}

/// Offline exclusive compaction maintenance over a closed database file.
pub struct RedbOfflineCompaction {
    database_path: PathBuf,
}

impl RedbOfflineCompaction {
    /// Binds one offline database file for exclusive storage compaction.
    #[must_use]
    pub fn bind(database_path: impl AsRef<Path>) -> Self {
        Self {
            database_path: database_path.as_ref().to_path_buf(),
        }
    }

    /// Initializes a minimal quiescent database for CLI compaction tests.
    #[doc(hidden)]
    pub fn initialize_quiescent_for_test(
        database_path: impl AsRef<Path>,
        database_id: DatabaseId,
    ) -> Result<(), StorageError> {
        use riffdb_storage_api::DatabaseInitializationPort as _;
        let mut store = RedbStore::open(database_path.as_ref())?;
        let _ = store.initialize_database(database_id)?;
        Ok(())
    }

    /// Compacts a closed database file after verifying format, journal rebase,
    /// retained V3 history, and tombstone chain integrity before and after compaction.
    pub fn compact(&self) -> Result<StorageCompactionResultV1, StorageError> {
        let (database_id, retention_watermark_sequence, history_incarnation) =
            self.verify_authoritative_state()?;

        let bytes_before = std::fs::metadata(&self.database_path)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?
            .len();

        let compacted = compact_redb_file(&self.database_path)?;
        self.compact_derived_sidecar_if_present()?;

        let (after_id, after_watermark, after_incarnation) = self.verify_authoritative_state()?;
        if after_id != database_id
            || after_watermark != retention_watermark_sequence
            || after_incarnation != history_incarnation
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }

        let bytes_after = std::fs::metadata(&self.database_path)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?
            .len();
        let reclaimed_bytes = bytes_before.saturating_sub(bytes_after);

        Ok(StorageCompactionResultV1 {
            database_id,
            bytes_before,
            bytes_after,
            reclaimed_bytes,
            compacted,
            retention_watermark_sequence,
            history_incarnation,
        })
    }

    fn verify_authoritative_state(&self) -> Result<(DatabaseId, u64, u64), StorageError> {
        let store = RedbStore::open(&self.database_path)?;
        let preparation = store.prepare_offline_retention()?;
        let transaction = preparation
            .database()
            .begin_read()
            .map_err(transaction_error)?;
        let meta = transaction.open_table(META).map_err(table_error)?;
        let encoded_id = meta
            .get(META_DATABASE_ID)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let database_id = *codec::decode_database_identity_v1(encoded_id.value())?.value();
        let encoded_incarnation = meta
            .get(META_HISTORY_INCARNATION)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let incarnation =
            *codec::decode_history_incarnation_v1(encoded_incarnation.value())?.value();
        if incarnation < HISTORY_INCARNATION_INITIAL {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        drop(meta);

        let watermark = load_watermark(&transaction)?;
        let (watermark_sequence, recorded_root, history_incarnation) = match watermark.as_ref() {
            None => (0, None, incarnation),
            Some(wm) => (
                wm.watermark_sequence(),
                wm.chain_root_registry_digest(),
                wm.history_incarnation(),
            ),
        };
        verify_tombstone_chain(&transaction, watermark_sequence, recorded_root)?;
        Ok((database_id, watermark_sequence, history_incarnation))
    }

    fn compact_derived_sidecar_if_present(&self) -> Result<(), StorageError> {
        let sidecar = crate::store::derived_store_path(&self.database_path);
        match std::fs::symlink_metadata(&sidecar) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(storage_error(StorageErrorKind::Unavailable)),
            Ok(metadata) if !metadata.file_type().is_file() => {
                Err(storage_error(StorageErrorKind::CorruptData))
            }
            Ok(_) => {
                let _ = compact_redb_file(&sidecar)?;
                Ok(())
            }
        }
    }
}

fn compact_redb_file(path: &Path) -> Result<bool, StorageError> {
    let mut builder = redb::Builder::new();
    builder.set_cache_size(crate::store::PRIMARY_REDB_CACHE_SIZE_BYTES);
    let mut database = builder.open(path).map_err(database_error)?;
    let mut compacted = false;
    for _ in 0..MAX_COMPACTION_PASSES {
        if database.compact().map_err(compaction_error)? {
            compacted = true;
        } else {
            break;
        }
    }
    drop(database);
    let file = File::open(path).map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    file.sync_all()
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    Ok(compacted)
}

fn compaction_error(error: redb::CompactionError) -> StorageError {
    match error {
        redb::CompactionError::Storage(error) => precommit_storage_error(error),
        redb::CompactionError::TransactionInProgress => {
            storage_error(StorageErrorKind::Unavailable)
        }
        _ => storage_error(StorageErrorKind::Unavailable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::EVENTS;
    use crate::retention::RedbOfflineRetention;
    use riffdb_storage_api::{ChangelogLineageV3, LeadershipEpochV1};
    use riffdb_types::DualFrontier;

    #[test]
    // req: STO-001, STO-010, STO-012, DX-014
    fn offline_storage_compact_reclaims_freed_pages_and_preserves_authoritative_state() {
        let scope = crate::test_path::ScopedDirectory::new("offline-storage-compact");
        let path = scope.join("db.redb");
        let id =
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x91; 10]).unwrap();

        let mut store = RedbStore::open(&path).unwrap();
        store.initialize_legacy_fixture(id).unwrap();
        crate::changelog_v3_activation::activate_validated(
            store.shared.database.begin_write().unwrap(),
            ChangelogLineageV3::new(id, 1, LeadershipEpochV1::initial()).unwrap(),
            DualFrontier::INITIAL,
        )
        .unwrap();
        drop(store);

        // 1. Allocate large pages in the lower region of the file.
        {
            let database = redb::Database::open(&path).unwrap();
            let payload = vec![0xAB_u8; 64 * 1024];
            let write = database.begin_write().unwrap();
            {
                let mut table = write.open_table(EVENTS).unwrap();
                for seq in 0_u64..64_u64 {
                    table
                        .insert(seq.to_be_bytes().as_slice(), payload.as_slice())
                        .unwrap();
                }
            }
            write.commit().unwrap();
        }

        // 2. Commit authoritative state while the lower 4 MiB is still allocated,
        // placing live B-tree pages at the high-water mark of the file.
        let retention = RedbOfflineRetention::bind(&path);
        retention
            .add_hold("compact-hold", 0, "before-compact")
            .unwrap();

        // 3. Free the lower 4 MiB pages; because live pages remain at the end of
        // the file, the file size remains > 4 MiB until offline compaction relocates
        // those live pages into the freed lower pages.
        {
            let database = redb::Database::open(&path).unwrap();
            let write = database.begin_write().unwrap();
            {
                let mut table = write.open_table(EVENTS).unwrap();
                for seq in 0_u64..64_u64 {
                    table.remove(seq.to_be_bytes().as_slice()).unwrap();
                }
            }
            write.commit().unwrap();
        }
        retention.remove_hold("compact-hold").unwrap();

        let status_before = retention.status().unwrap();
        let result = RedbOfflineCompaction::bind(&path).compact().unwrap();

        assert_eq!(result.database_id, id);
        assert!(result.compacted, "expected freed pages to be compacted");
        assert!(
            result.bytes_after < result.bytes_before,
            "expected compacted file size ({}) < before ({})",
            result.bytes_after,
            result.bytes_before
        );
        assert_eq!(
            result.reclaimed_bytes,
            result.bytes_before - result.bytes_after
        );
        assert_eq!(
            result.retention_watermark_sequence,
            status_before.watermark_sequence
        );
        assert_eq!(
            result.history_incarnation,
            status_before.history_incarnation
        );

        let status_after = retention.status().unwrap();
        assert_eq!(status_after, status_before);
    }
}
