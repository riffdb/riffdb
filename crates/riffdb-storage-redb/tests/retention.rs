//! Retention fencing, holds, audited projection administration, and format
//! plumbing (ADR-0085 A2). The populated-history prune end-to-end lives in
//! `tests/storage_recovery/storage_recovery_matrix.rs` — pruning an empty
//! database is impossible by construction (the durable application head
//! fences the watermark at 0).
//!
//! Falsifiability notes (what a neutered implementation would break):
//! - `prune_refuses_on_empty_history_even_under_holds`: dropping the durable
//!   application-head fencing input would let the empty prune pass.
//! - `projection_detach_and_reattach_are_audited`: skipping the audit append
//!   or the typed hold kind would break the audit/kind assertions.
//! - `pre_retention_registry_migrates_on_open`: skipping the PRE_RETENTION
//!   publish step would fail the reopen.
//! - `watermark_holds_tombstone_codec_roundtrip`: dropping the chain-root
//!   digest or hold-kind wire fields would break the roundtrips.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use riffdb_storage_api::{
    DatabaseIdentityProbe, DatabaseIdentityProbePort, DatabaseInitializationPort,
    DatabaseInitializationResult, RetentionAdministrationAction, RetentionFenceBinding,
    RetentionFencingInputs, RetentionHoldKind, StorageErrorKind, compute_max_permissible_watermark,
};
use riffdb_storage_redb::{RedbOfflineRetention, RedbStore};
use riffdb_types::{DatabaseId, ProjectionId, Timestamp};

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

fn timestamp() -> Timestamp {
    Timestamp::new(1_700_000_010, 0).expect("timestamp")
}

const fn no_inputs() -> RetentionFencingInputs {
    RetentionFencingInputs {
        durable_application_head: None,
        min_projection_durable_frontier: None,
        min_operator_hold_sequence: None,
        undelivered_outbox_low_water: None,
        staged_migration_frozen_frontier: None,
    }
}

#[test]
fn fencing_each_input_binds_alone() {
    let only_head = RetentionFencingInputs {
        durable_application_head: Some(3),
        ..no_inputs()
    };
    let (v, b) = compute_max_permissible_watermark(&only_head);
    assert_eq!(v, Some(3));
    assert_eq!(b, RetentionFenceBinding::DurableApplicationHead);

    let only_hold = RetentionFencingInputs {
        min_operator_hold_sequence: Some(7),
        ..no_inputs()
    };
    let (v, b) = compute_max_permissible_watermark(&only_hold);
    assert_eq!(v, Some(7));
    assert_eq!(b, RetentionFenceBinding::OperatorHold);
}

#[test]
fn fencing_absent_all_inputs_is_unbounded() {
    let (v, b) = compute_max_permissible_watermark(&no_inputs());
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
    // The durable application head is always collected: 0 for empty history.
    assert_eq!(status.max_permissible_watermark, Some(0));
    assert_eq!(
        status.fence_binding,
        RetentionFenceBinding::DurableApplicationHead
    );
}

#[test]
fn prune_refuses_on_empty_history_even_under_holds() {
    // Nothing ever committed: the durable head fences the watermark at 0.
    // Pruning an empty database is impossible by construction — a permissive
    // hold cannot override the head.
    let root = TestRoot::new("no-history");
    initialize(&root.db());
    let maintenance = RedbOfflineRetention::bind(root.db());
    let err = maintenance.prune_to(1).expect_err("must refuse");
    assert_eq!(err.kind(), StorageErrorKind::InvariantViolation);
    maintenance.add_hold("allow", 5, "cap at 5").expect("hold");
    let err = maintenance.prune_to(1).expect_err("must still refuse");
    assert_eq!(err.kind(), StorageErrorKind::InvariantViolation);
    let status = maintenance.status().expect("status");
    assert_eq!(status.watermark_sequence, 0);
    assert_eq!(status.tombstone_count, 0);
}

#[test]
fn watermark_holds_tombstone_codec_roundtrip() {
    use riffdb_storage_api::{
        HistoryTombstoneContentDigest, RetentionHoldV1, StoredHistoryTombstoneV1,
        StoredRetentionHoldsV1, StoredRetentionWatermarkV1, decode_history_tombstone_v1,
        decode_retention_holds_v1, decode_retention_watermark_v1, encode_history_tombstone_v1,
        encode_retention_holds_v1, encode_retention_watermark_v1,
    };
    use riffdb_types::SchemaHash;

    // Pruned watermark carries the recorded chain-root registry digest.
    let wm = StoredRetentionWatermarkV1::new(42, 1, Some(SchemaHash::from_bytes([0x5a; 32])))
        .expect("wm");
    let enc = encode_retention_watermark_v1(&wm).expect("enc");
    let dec = decode_retention_watermark_v1(enc.as_bytes())
        .expect("dec")
        .into_parts()
        .0;
    assert_eq!(dec, wm);
    assert_eq!(
        dec.chain_root_registry_digest(),
        Some(SchemaHash::from_bytes([0x5a; 32]))
    );
    // Unpruned watermark has no chain and no rooting digest.
    let zero = StoredRetentionWatermarkV1::new(0, 1, None).expect("zero wm");
    let enc = encode_retention_watermark_v1(&zero).expect("enc");
    let dec = decode_retention_watermark_v1(enc.as_bytes())
        .expect("dec")
        .into_parts()
        .0;
    assert_eq!(dec, zero);

    let holds = StoredRetentionHoldsV1::new(vec![
        RetentionHoldV1::new_projection_detach(ProjectionId::new(7).expect("id"), "budget")
            .expect("detach hold"),
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
    assert_eq!(dec.holds()[0].kind(), RetentionHoldKind::ProjectionDetach);
    assert_eq!(dec.holds()[1].kind(), RetentionHoldKind::Operator);

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
fn retention_administration_codec_roundtrip() {
    use riffdb_storage_api::{
        StoredRetentionAdministrationV1, decode_retention_administration_v1,
        encode_retention_administration_v1,
    };
    use riffdb_types::AdministrationSequence;

    let record = StoredRetentionAdministrationV1::new(
        AdministrationSequence::new(5).expect("sequence"),
        RetentionAdministrationAction::ProjectionDetach,
        ProjectionId::new(9).expect("projection"),
        "replay budget accepted",
        timestamp(),
    )
    .expect("record");
    let enc = encode_retention_administration_v1(&record).expect("enc");
    let dec = decode_retention_administration_v1(enc.as_bytes())
        .expect("dec")
        .into_parts()
        .0;
    assert_eq!(dec, record);
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

fn retention_audit_records(
    path: &Path,
) -> Vec<riffdb_storage_api::StoredRetentionAdministrationV1> {
    let database = Database::open(path).expect("open raw");
    let read = database.begin_read().expect("read");
    let table = read
        .open_table(TableDefinition::<&[u8], &[u8]>::new("audit"))
        .expect("audit");
    table
        .iter()
        .expect("iter")
        .filter_map(|entry| {
            let (_, value) = entry.expect("entry");
            riffdb_storage_api::proto_codec::decode_retention_administration_v1(value.value())
                .ok()
                .map(|item| item.into_parts().0)
        })
        .collect()
}

#[test]
fn projection_detach_and_reattach_are_audited() {
    let root = TestRoot::new("detach-audit");
    initialize(&root.db());
    let maintenance = RedbOfflineRetention::bind(root.db());
    let projection = ProjectionId::new(9).expect("projection");

    maintenance
        .detach_projection(projection, "replay budget accepted", timestamp())
        .expect("detach");
    let status = maintenance.status().expect("status");
    let holds = status.holds.holds();
    assert_eq!(holds.len(), 1);
    assert_eq!(holds[0].kind(), RetentionHoldKind::ProjectionDetach);
    assert_eq!(holds[0].hold_id(), "9");
    assert_eq!(holds[0].sequence(), 0);
    let records = retention_audit_records(&root.db());
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].action(),
        RetentionAdministrationAction::ProjectionDetach
    );
    assert_eq!(records[0].projection_id(), projection);
    assert_eq!(records[0].reason(), "replay budget accepted");

    // Idempotent re-detach appends NO second audit record.
    maintenance
        .detach_projection(projection, "again", timestamp())
        .expect("idempotent detach");
    assert_eq!(retention_audit_records(&root.db()).len(), 1);

    // The detach row is not a plain hold: replace and removal refuse.
    let err = maintenance
        .add_hold("9", 3, "collide")
        .expect_err("operator hold must not replace a detach row");
    assert_eq!(err.kind(), StorageErrorKind::InvariantViolation);
    let err = maintenance
        .remove_hold("9")
        .expect_err("plain removal must not silently reattach");
    assert_eq!(err.kind(), StorageErrorKind::InvariantViolation);

    // Audited reattach removes the detach row and appends its own record.
    maintenance
        .reattach_projection(projection, "budget restored", timestamp())
        .expect("reattach");
    let status = maintenance.status().expect("status after reattach");
    assert!(status.holds.holds().is_empty());
    let records = retention_audit_records(&root.db());
    assert_eq!(records.len(), 2);
    assert_eq!(
        records[1].action(),
        RetentionAdministrationAction::ProjectionReattach
    );

    // Reattaching a non-detached projection refuses.
    let err = maintenance
        .reattach_projection(projection, "twice", timestamp())
        .expect_err("must refuse");
    assert_eq!(err.kind(), StorageErrorKind::InvariantViolation);

    // The audited administration stream keeps the database valid on reopen.
    drop(RedbStore::open(root.db()).expect("reopen after audited actions"));
}
