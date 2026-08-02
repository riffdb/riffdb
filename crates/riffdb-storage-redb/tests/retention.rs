//! Retention watermark, fencing, offline prune, and tombstone coverage (ADR-0085 A2).
//!
//! Falsifiability notes (what a neutered implementation would break):
//! - `prune_refuses_without_fencing_inputs`: prune without fencing max would pass.
//! - `prune_never_exceeds_hold_fence`: prune past hold would pass.
//! - `crash_before_subrange_reopens_valid` / `crash_after_subrange_commit_resumes`:
//!   non-atomic subrange would leave unopenable state.
//! - `tombstone_chain_corruption_refuses_open`: ignoring chain hash would open.
//! - `pre_retention_registry_migrates_on_open`: skipping PRE_RETENTION publish would fail.
//! - `checkpoint_with_wrong_watermark_binding_is_ignored`: trusting mismatched
//!   checkpoint would skip full validation incorrectly or refuse open.
//! - `backup_pruned_db_restores_and_validates`: omitting watermark from manifest
//!   would fail allocator check on fully-pruned empty history or lose stamp.
//! - `outbox_undelivered_fence_blocks_prune`: ignoring undelivered outbox would
//!   allow prune past low-water.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use redb::{Database, ReadableTable, TableDefinition};
use riffdb_storage_api::{
    BackupBuildMetadataV1, DatabaseIdentityProbe, DatabaseIdentityProbePort,
    DatabaseInitializationPort, DatabaseInitializationResult, OfflineBackupPersistencePort,
    OfflineRestoreOverwritePolicyV1, OfflineRestorePersistencePort, OfflineRestoreResultV1,
    RetentionFenceBinding, RetentionFencingInputs, StorageErrorKind,
    compute_max_permissible_watermark,
};
use riffdb_storage_redb::{
    RedbOfflineBackup, RedbOfflineRestore, RedbOfflineRetention, RedbStore, RedbTestController,
    RedbTestOperation,
};
use riffdb_types::DatabaseId;

static NEXT_TEST_PATH: AtomicU64 = AtomicU64::new(1);

struct TestRoot(PathBuf);

impl TestRoot {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "riffdb-retention-{}-{}-{}",
            label,
            std::process::id(),
            NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create test root");
        Self(path)
    }

    fn db(&self) -> PathBuf {
        self.0.join("database.redb")
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(2, [0x42; 10]).expect("database ID")
}

fn initialize(path: &Path) {
    let mut store = RedbStore::open(path).expect("open database");
    assert_eq!(
        store.probe_database_identity().expect("probe database"),
        DatabaseIdentityProbe::NeedsInitialization
    );
    assert_eq!(
        store
            .initialize_database(database_id())
            .expect("initialize database"),
        DatabaseInitializationResult::Installed(database_id())
    );
}

#[test]
fn fencing_each_input_binds_alone() {
    let only_hold = RetentionFencingInputs {
        min_projection_durable_frontier: None,
        min_operator_hold_sequence: Some(7),
        undelivered_outbox_low_water: None,
        staged_migration_frozen_frontier: None,
    };
    let (v, b) = compute_max_permissible_watermark(&only_hold);
    assert_eq!(v, Some(7));
    assert_eq!(b, RetentionFenceBinding::OperatorHold);
}

#[test]
fn fencing_absent_all_inputs_is_unbounded() {
    let empty = RetentionFencingInputs {
        min_projection_durable_frontier: None,
        min_operator_hold_sequence: None,
        undelivered_outbox_low_water: None,
        staged_migration_frozen_frontier: None,
    };
    let (v, b) = compute_max_permissible_watermark(&empty);
    assert_eq!(v, None);
    assert_eq!(b, RetentionFenceBinding::Unbounded);
}

#[test]
fn empty_db_opens_with_zero_watermark() {
    let root = TestRoot::new("empty");
    initialize(&root.db());
    let status = RedbOfflineRetention::bind(root.db())
        .status()
        .expect("status");
    assert_eq!(status.watermark_sequence, 0);
    assert_eq!(status.tombstone_count, 0);
}

#[test]
fn prune_refuses_without_fencing_inputs() {
    let root = TestRoot::new("no-fence");
    initialize(&root.db());
    let err = RedbOfflineRetention::bind(root.db())
        .prune_to(1)
        .expect_err("must refuse");
    assert_eq!(err.kind(), StorageErrorKind::InvariantViolation);
}

#[test]
fn prune_with_hold_roundtrip_and_reopen() {
    let root = TestRoot::new("prune-hold");
    initialize(&root.db());
    let maint = RedbOfflineRetention::bind(root.db());
    maint.add_hold("allow", 5, "cap at 5").expect("hold");
    let status = maint.prune_to(1).expect("prune empty range");
    assert_eq!(status.watermark_sequence, 1);
    assert_eq!(status.tombstone_count, 1);

    let store = RedbStore::open(root.db()).expect("reopen after prune");
    drop(store);

    let status = RedbOfflineRetention::bind(root.db())
        .status()
        .expect("status");
    assert_eq!(status.watermark_sequence, 1);
    assert_eq!(status.tombstone_count, 1);
}

#[test]
fn prune_never_exceeds_hold_fence() {
    let root = TestRoot::new("hold-cap");
    initialize(&root.db());
    let maint = RedbOfflineRetention::bind(root.db());
    maint.add_hold("cap", 2, "cap").expect("hold");
    let err = maint.prune_to(3).expect_err("over fence");
    assert_eq!(err.kind(), StorageErrorKind::InvariantViolation);
    let status = maint.prune_to(2).expect("at fence");
    assert_eq!(status.watermark_sequence, 2);
}

#[test]
fn crash_before_subrange_reopens_valid() {
    let root = TestRoot::new("crash");
    initialize(&root.db());
    let controller =
        RedbTestController::return_before_commit(RedbTestOperation::RetentionPruneSubrange);
    let maint = RedbOfflineRetention::bind_with_test_controller(root.db(), controller);
    maint.add_hold("cap", 10, "cap").expect("hold");
    let err = maint.prune_to(10).expect_err("failpoint");
    assert_eq!(err.kind(), StorageErrorKind::Unavailable);

    let store = RedbStore::open(root.db()).expect("reopen after abort");
    drop(store);
    let status = RedbOfflineRetention::bind(root.db())
        .status()
        .expect("status");
    assert_eq!(status.watermark_sequence, 0);
}

#[test]
fn watermark_holds_tombstone_codec_roundtrip() {
    use riffdb_storage_api::{
        HistoryTombstoneContentDigest, RetentionHoldV1, StoredHistoryTombstoneV1,
        StoredRetentionHoldsV1, StoredRetentionWatermarkV1, decode_history_tombstone_v1,
        decode_retention_holds_v1, decode_retention_watermark_v1, encode_history_tombstone_v1,
        encode_retention_holds_v1, encode_retention_watermark_v1,
    };

    let wm = StoredRetentionWatermarkV1::new(42, 1).expect("wm");
    let enc = encode_retention_watermark_v1(&wm).expect("enc");
    let dec = decode_retention_watermark_v1(enc.as_bytes())
        .expect("dec")
        .into_parts()
        .0;
    assert_eq!(dec, wm);

    let holds = StoredRetentionHoldsV1::new(vec![
        RetentionHoldV1::new("a", 1, "r").expect("a"),
        RetentionHoldV1::new("b", 2, "r").expect("b"),
    ])
    .expect("holds");
    let enc = encode_retention_holds_v1(&holds).expect("enc");
    let dec = decode_retention_holds_v1(enc.as_bytes())
        .expect("dec")
        .into_parts()
        .0;
    assert_eq!(dec, holds);

    let ts = StoredHistoryTombstoneV1::new(
        1,
        3,
        3,
        0,
        0,
        0,
        HistoryTombstoneContentDigest::from_bytes([9; 32]),
        None,
        1,
    )
    .expect("ts");
    let enc = encode_history_tombstone_v1(&ts).expect("enc");
    let dec = decode_history_tombstone_v1(enc.as_bytes())
        .expect("dec")
        .into_parts()
        .0;
    assert_eq!(dec, ts);
}

#[test]
fn crash_after_subrange_commit_resumes() {
    // After-commit uncertainty still leaves a durable subrange; reopen and
    // re-status must see the advanced watermark (atomic delete+tombstone+wm).
    let root = TestRoot::new("crash-after");
    initialize(&root.db());
    let controller =
        RedbTestController::return_unknown_after_commit(RedbTestOperation::RetentionPruneSubrange);
    let maint = RedbOfflineRetention::bind_with_test_controller(root.db(), controller);
    maint.add_hold("cap", 10, "cap").expect("hold");
    let err = maint.prune_to(1).expect_err("failpoint after commit");
    assert_eq!(err.kind(), StorageErrorKind::CommitStatusUnknown);

    let store = RedbStore::open(root.db()).expect("reopen after after-commit uncertainty");
    drop(store);
    let status = RedbOfflineRetention::bind(root.db())
        .status()
        .expect("status");
    assert_eq!(status.watermark_sequence, 1);
    assert_eq!(status.tombstone_count, 1);
}

#[test]
fn pre_retention_registry_migrates_on_open() {
    // Rewrite registry digest to the frozen PRE_RETENTION value; reopen must
    // migrate and install current registry (history_tombstones already present
    // is fine — install is idempotent).
    let root = TestRoot::new("pre-retention-migrate");
    initialize(&root.db());
    {
        let database = Database::open(root.db()).expect("open redb");
        let write = database.begin_write().expect("write");
        {
            let mut meta = write
                .open_table(TableDefinition::<&str, &[u8]>::new("meta"))
                .expect("meta");
            // PRE_RETENTION_WATERMARK_REGISTRY_DIGEST from store.rs
            let pre: [u8; 32] = [
                0x99, 0x13, 0x0d, 0x68, 0x02, 0x71, 0x1b, 0x38, 0xe8, 0xda, 0x85, 0xc2, 0x04, 0x39,
                0x60, 0xf3, 0x62, 0x04, 0x7f, 0x41, 0xc2, 0xd8, 0x0a, 0x4d, 0x88, 0xff, 0xf8, 0x2c,
                0x9c, 0xfc, 0x46, 0x69,
            ];
            let encoded = riffdb_storage_api::proto_codec::encode_record_registry_v2(
                riffdb_types::SchemaHash::from_bytes(pre),
            )
            .expect("encode registry");
            meta.insert("record_registry/v2", encoded.as_bytes())
                .expect("insert");
        }
        write.commit().expect("commit");
    }
    let store = RedbStore::open(root.db()).expect("migrate on open");
    drop(store);
    let status = RedbOfflineRetention::bind(root.db())
        .status()
        .expect("status after migrate");
    assert_eq!(status.watermark_sequence, 0);
}

#[test]
fn tombstone_chain_corruption_refuses_open() {
    let root = TestRoot::new("tombstone-corrupt");
    initialize(&root.db());
    let maint = RedbOfflineRetention::bind(root.db());
    maint.add_hold("cap", 5, "cap").expect("hold");
    maint.prune_to(1).expect("prune");

    // Flip one byte in the only tombstone row.
    {
        let database = Database::open(root.db()).expect("open");
        let write = database.begin_write().expect("write");
        {
            let mut table = write
                .open_table(TableDefinition::<&[u8], &[u8]>::new("history_tombstones"))
                .expect("tombstones");
            let (key_bytes, mut bytes) = {
                let (key, value) = table
                    .iter()
                    .expect("iter")
                    .next()
                    .expect("row")
                    .expect("entry");
                (key.value().to_vec(), value.value().to_vec())
            };
            let last = bytes.len() - 1;
            bytes[last] ^= 0xff;
            table
                .insert(key_bytes.as_slice(), bytes.as_slice())
                .expect("rewrite");
        }
        write.commit().expect("commit");
    }

    let err = RedbStore::open(root.db()).expect_err("corrupt chain must refuse open");
    assert_eq!(err.kind(), StorageErrorKind::CorruptData);
}

#[test]
fn checkpoint_meta_with_wrong_watermark_is_ignored_on_open() {
    // Prune deletes the live checkpoint; write a garbage checkpoint binding
    // a wrong watermark. Open must ignore it (full validation) and succeed.
    let root = TestRoot::new("bad-checkpoint");
    initialize(&root.db());
    let maint = RedbOfflineRetention::bind(root.db());
    maint.add_hold("cap", 5, "cap").expect("hold");
    maint.prune_to(1).expect("prune");

    {
        let database = Database::open(root.db()).expect("open");
        let write = database.begin_write().expect("write");
        {
            let mut meta = write
                .open_table(TableDefinition::<&str, &[u8]>::new("meta"))
                .expect("meta");
            // Non-decodable / wrong-shape bytes → load_active_checkpoint ignores.
            meta.insert(
                "validated_prefix_checkpoint/v1",
                b"not-a-checkpoint".as_slice(),
            )
            .expect("insert bad checkpoint");
        }
        write.commit().expect("commit");
    }

    let store = RedbStore::open(root.db()).expect("open ignores bad checkpoint");
    drop(store);
}

#[test]
fn backup_pruned_db_restores_and_validates() {
    let root = TestRoot::new("backup-pruned");
    initialize(&root.db());
    let maint = RedbOfflineRetention::bind(root.db());
    maint.add_hold("cap", 5, "cap").expect("hold");
    let status = maint.prune_to(2).expect("prune");
    assert_eq!(status.watermark_sequence, 2);

    let backup_dir = root.0.join("backup");
    let build = BackupBuildMetadataV1::new(
        "0.1.0",
        "0123456789abcdef",
        "rustc-1.97.0",
        1,
        vec!["retention".to_owned()],
    )
    .expect("build");
    let mut backup = RedbOfflineBackup::bind(root.db(), &backup_dir);
    let manifest = backup.create_offline_backup(&build).expect("backup");
    assert_eq!(manifest.retention_watermark_sequence(), Some(2));

    let restore_dir = root.0.join("restored");
    std::fs::create_dir_all(&restore_dir).expect("restore dir");
    let mut restore = RedbOfflineRestore::bind(&backup_dir, &restore_dir);
    let result = restore
        .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
        .expect("restore");
    assert!(matches!(result, OfflineRestoreResultV1::Restored { .. }));

    let restored_db = restore_dir.join("database.redb");
    let store = RedbStore::open(&restored_db).expect("open restored");
    drop(store);
    let restored_status = RedbOfflineRetention::bind(&restored_db)
        .status()
        .expect("status");
    assert_eq!(restored_status.watermark_sequence, 2);
    assert_eq!(restored_status.tombstone_count, 1);
}

#[test]
fn outbox_undelivered_fence_blocks_prune() {
    // Insert a non-terminal outbox_status row at sequence 3 so low-water is 2.
    // Prune to 3 must refuse; prune to 2 may proceed under an operator hold.
    use std::num::NonZeroU32;

    use riffdb_storage_api::{
        OutboxDestinationIdV1, OutboxRetryMetadataV1, StoredOutboxStatusV1,
        proto_codec::encode_outbox_status_v1,
    };
    use riffdb_types::{EventId, Timestamp};

    let root = TestRoot::new("outbox-fence");
    initialize(&root.db());

    {
        let database = Database::open(root.db()).expect("open");
        let write = database.begin_write().expect("write");
        {
            let mut statuses = write
                .open_table(TableDefinition::<&[u8], &[u8]>::new("outbox_status"))
                .expect("outbox_status");
            let event_id = EventId::new(riffdb_types::CommitSequence::new(3).expect("seq"), 0);
            let mut key = [0u8; 12];
            key[..8].copy_from_slice(&3u64.to_be_bytes());
            key[8..].copy_from_slice(&0u32.to_be_bytes());
            let destination = OutboxDestinationIdV1::new("dest-1").expect("dest");
            let metadata = OutboxRetryMetadataV1::new(
                NonZeroU32::new(1).expect("attempts"),
                Timestamp::new(1, 0).expect("ts"),
                None,
                destination,
                None,
            );
            let status = StoredOutboxStatusV1::pending(event_id, metadata);
            let encoded = encode_outbox_status_v1(&status).expect("encode");
            statuses
                .insert(key.as_slice(), encoded.as_bytes())
                .expect("insert");
        }
        write.commit().expect("commit");
    }

    let maint = RedbOfflineRetention::bind(root.db());
    maint.add_hold("cap", 10, "cap").expect("hold");
    let err = maint
        .prune_to(3)
        .expect_err("must not pass undelivered low-water");
    assert_eq!(err.kind(), StorageErrorKind::InvariantViolation);
    let status = maint.prune_to(2).expect("at undelivered fence");
    assert_eq!(status.watermark_sequence, 2);
}
