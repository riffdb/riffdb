use std::num::NonZeroU16;

use riffdb_storage_api::{
    DurableKeySchemaBindingV1, IndexRangePrefixBuilder, LegacyStoredIndexEpochV1,
    StoredIndexEntryV2, StructurallyDecodedIndexRangePrefixV1,
    proto_codec::encode_legacy_index_epoch_v1_fixture,
};
use riffdb_types::{
    AggregateTypeId, CanonicalRecord, ContractBundleHash, ContractLineage, ContractVersion,
    EntityKeyBuilder, EntityTypeId, IndexEntryKeyBuilder, PartitionKeyBuilder,
};
use riffdb_types::{CommitSequence, EventId};

use crate::keys::{
    encode_application_sequence_key, encode_event_key, encode_index_range_prefix_key,
};

use super::*;

fn pin_predecessor_registry(
    transaction: &redb::WriteTransaction,
    predecessor: &riffdb_storage_api::CanonicalStoredEnvelopeV1,
) {
    let mut meta = transaction.open_table(META).expect("open metadata");
    meta.insert(META_RECORD_REGISTRY, predecessor.as_bytes())
        .expect("install predecessor registry");
    meta.remove(META_CHANGELOG_V2_ROTATION_RECEIPT)
        .expect("remove successor rotation receipt");
    drop(meta);
    assert!(
        transaction
            .open_table(ENTITY_CHAIN_HEADS)
            .expect("entity chain heads")
            .is_empty()
            .expect("head table length"),
        "a predecessor fixture must not retain successor chain heads"
    );
}

/// Warming the transient indexes must not block on the caller's own read.
///
/// The administration-audit read paths warm the command-derived index on
/// first use (see `ensure_command_audit_index`), and they do so *while their
/// caller holds a live read transaction* — `read_active_catalog` opens one,
/// then walks the whole administration stream through
/// `validate_administration_stream_readonly`. Warming takes the exclusive
/// mutation gate and, when a journal runtime exists, its barrier checkpoint
/// runs a real `begin_write` + `commit_durable`. This arm proves that
/// nesting is safe: redb admits a writer alongside live readers, so the
/// commit completes rather than waiting for a reader that is waiting for it.
///
/// The durable read frontier is installed deliberately, because it is the
/// precondition for the journal runtime to exist and therefore for the
/// barrier to take `begin_write` instead of its early return. Without it
/// this arm would exercise only the no-write path and prove nothing about
/// the nesting.
///
/// What this arm does NOT license: warming from a path that already holds
/// the mutation lease. `ExclusiveGate` is a non-reentrant FIFO ticket lock,
/// so a second acquire on the holding thread blocks forever. Warm-on-demand
/// belongs only on read paths, which take no ticket.
#[test]
fn warming_the_indexes_commits_under_the_callers_live_read_transaction() {
    let (sender, receiver) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let scope = crate::test_path::ScopedDirectory::new("warm-under-live-read");
        let path = scope.join("db.redb");
        let mut store = RedbStore::open(&path).expect("open");
        let database_id = DatabaseId::from_bytes({
            let mut bytes = [0x33; 16];
            bytes[6] = 0x71;
            bytes[8] = 0xa1;
            bytes
        })
        .expect("database");
        store.initialize_database(database_id).expect("initialize");
        {
            let mut frontier = store
                .shared
                .durable_read_frontier
                .write()
                .expect("durable read frontier lock");
            *frontier = Some(Arc::new(CheckpointRoot::new(
                store
                    .shared
                    .database
                    .begin_read()
                    .expect("durable frontier read"),
                store.shared.durable_commit_epoch.load(Ordering::Acquire),
            )));
        }
        drop(
            store
                .shared
                .journal_runtime()
                .expect("initialise the journal runtime"),
        );
        assert!(
            store
                .shared
                .journal_runtime
                .lock()
                .expect("journal runtime lock")
                .is_some(),
            "a live journal runtime is what makes the barrier take begin_write; \
                 without it this arm degrades to the no-write path"
        );
        assert!(
            matches!(
                *store
                    .shared
                    .transient_indexes
                    .read()
                    .expect("transient index lock"),
                TransientIndexState::Dormant
            ),
            "the arm must start from a cold index or it warms nothing"
        );

        // The caller's read transaction, held across the whole warm.
        let read = store
            .shared
            .begin_operational_read()
            .expect("caller read access");
        store
            .shared
            .ensure_transient_indexes_ready()
            .expect("warm the indexes under a live read transaction");
        assert!(
            matches!(
                *store
                    .shared
                    .transient_indexes
                    .read()
                    .expect("transient index lock"),
                TransientIndexState::Ready(_)
            ),
            "the warm must publish a populated index"
        );
        drop(read);
        sender.send(()).expect("report completion");
    });
    receiver
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect(
            "warming must not block on the caller's live read transaction; \
                 a timeout here is a deadlock, not a slow machine",
        );
    worker.join().expect("worker thread");
}

#[test]
fn pre_export_registry_installs_operation_table_before_publication() {
    let scope = crate::test_path::ScopedDirectory::new("pre-export-registry");
    let path = scope.join("db.redb");
    let mut store = RedbStore::open(&path).expect("open");
    let database_id = DatabaseId::from_bytes({
        let mut bytes = [0x21; 16];
        bytes[6] = 0x71;
        bytes[8] = 0xa1;
        bytes
    })
    .expect("database");
    store.initialize_database(database_id).expect("initialize");
    {
        let mut transaction = store
            .shared
            .database
            .begin_write()
            .expect("begin predecessor write");
        transaction
            .set_durability(Durability::Immediate)
            .expect("durability");
        transaction
            .delete_table(crate::layout::APPLICATION_EXPORT_OPERATIONS)
            .expect("remove successor table");
        transaction
            .delete_table(crate::layout::VECTOR_EVIDENCE)
            .expect("remove later successor table");
        transaction
            .delete_table(crate::layout::VECTOR_OBSERVATIONS)
            .expect("remove later observation table");
        transaction
            .delete_table(crate::layout::VECTOR_EVIDENCE_INDEX)
            .expect("remove later evidence index table");
        let predecessor = encode_record_registry_v2(SchemaHash::from_bytes(
            PRE_APPLICATION_EXPORT_REGISTRY_DIGEST,
        ))
        .expect("predecessor registry");
        transaction
            .open_table(META)
            .expect("metadata")
            .insert(META_RECORD_REGISTRY, predecessor.as_bytes())
            .expect("pin predecessor registry");
        transaction.commit().expect("commit predecessor fixture");
    }
    drop(store);

    let reopened = RedbStore::open(&path).expect("migrate predecessor");
    let transaction = reopened
        .shared
        .database
        .begin_read()
        .expect("read migrated database");
    assert!(
        transaction
            .open_table(crate::layout::APPLICATION_EXPORT_OPERATIONS)
            .expect("export operation table")
            .is_empty()
            .expect("table length")
    );
    let metadata = transaction.open_table(META).expect("metadata");
    let encoded = metadata
        .get(META_RECORD_REGISTRY)
        .expect("registry read")
        .expect("registry");
    assert_eq!(
        *decode_record_registry_v2(encoded.value())
            .expect("decode registry")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
}

#[test]
fn pre_vector_registry_installs_evidence_table_before_publication() {
    let scope = crate::test_path::ScopedDirectory::new("pre-vector-registry");
    let path = scope.join("db.redb");
    let mut store = RedbStore::open(&path).expect("open");
    let database_id = DatabaseId::from_bytes({
        let mut bytes = [0x22; 16];
        bytes[6] = 0x72;
        bytes[8] = 0xa2;
        bytes
    })
    .expect("database");
    store.initialize_database(database_id).expect("initialize");
    {
        let mut transaction = store
            .shared
            .database
            .begin_write()
            .expect("begin predecessor write");
        transaction
            .set_durability(Durability::Immediate)
            .expect("durability");
        transaction
            .delete_table(crate::layout::VECTOR_EVIDENCE)
            .expect("remove successor table");
        transaction
            .delete_table(crate::layout::VECTOR_OBSERVATIONS)
            .expect("remove later observation table");
        transaction
            .delete_table(crate::layout::VECTOR_EVIDENCE_INDEX)
            .expect("remove later evidence index table");
        let predecessor =
            encode_record_registry_v2(SchemaHash::from_bytes(PRE_VECTOR_EVIDENCE_REGISTRY_DIGEST))
                .expect("predecessor registry");
        transaction
            .open_table(META)
            .expect("metadata")
            .insert(META_RECORD_REGISTRY, predecessor.as_bytes())
            .expect("pin predecessor registry");
        transaction.commit().expect("commit predecessor fixture");
    }
    drop(store);

    let reopened = RedbStore::open(&path).expect("migrate predecessor");
    let transaction = reopened
        .shared
        .database
        .begin_read()
        .expect("read migrated database");
    assert!(
        transaction
            .open_table(crate::layout::VECTOR_EVIDENCE)
            .expect("vector evidence table")
            .is_empty()
            .expect("table length")
    );
    let metadata = transaction.open_table(META).expect("metadata");
    let encoded = metadata
        .get(META_RECORD_REGISTRY)
        .expect("registry read")
        .expect("registry");
    assert_eq!(
        *decode_record_registry_v2(encoded.value())
            .expect("decode registry")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
}

#[test]
fn pre_vector_observation_registry_installs_table_before_publication() {
    let scope = crate::test_path::ScopedDirectory::new("pre-vector-observation-registry");
    let path = scope.join("db.redb");
    let mut store = RedbStore::open(&path).expect("open");
    let database_id = DatabaseId::from_bytes({
        let mut bytes = [0x23; 16];
        bytes[6] = 0x73;
        bytes[8] = 0xa3;
        bytes
    })
    .expect("database");
    store.initialize_database(database_id).expect("initialize");
    {
        let mut transaction = store
            .shared
            .database
            .begin_write()
            .expect("begin predecessor write");
        transaction
            .set_durability(Durability::Immediate)
            .expect("durability");
        transaction
            .delete_table(crate::layout::VECTOR_OBSERVATIONS)
            .expect("remove successor table");
        transaction
            .delete_table(crate::layout::VECTOR_EVIDENCE_INDEX)
            .expect("remove successor evidence index table");
        let predecessor = encode_record_registry_v2(SchemaHash::from_bytes(
            PRE_VECTOR_OBSERVATION_REGISTRY_DIGEST,
        ))
        .expect("predecessor registry");
        transaction
            .open_table(META)
            .expect("metadata")
            .insert(META_RECORD_REGISTRY, predecessor.as_bytes())
            .expect("pin predecessor registry");
        transaction.commit().expect("commit predecessor fixture");
    }
    drop(store);

    let reopened = RedbStore::open(&path).expect("migrate predecessor");
    let transaction = reopened
        .shared
        .database
        .begin_read()
        .expect("read migrated database");
    assert!(
        transaction
            .open_table(crate::layout::VECTOR_OBSERVATIONS)
            .expect("vector observations table")
            .is_empty()
            .expect("table length")
    );
    assert!(
        transaction
            .open_table(crate::layout::VECTOR_EVIDENCE_INDEX)
            .expect("vector evidence index table")
            .is_empty()
            .expect("table length")
    );
    let metadata = transaction.open_table(META).expect("metadata");
    let encoded = metadata
        .get(META_RECORD_REGISTRY)
        .expect("registry read")
        .expect("registry");
    assert_eq!(
        *decode_record_registry_v2(encoded.value())
            .expect("decode registry")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
}

#[test]
fn pre_vector_health_registry_publishes_only_after_health_backfill() {
    let scope = crate::test_path::ScopedDirectory::new("pre-vector-health-registry");
    let path = scope.join("db.redb");
    let mut store = RedbStore::open(&path).expect("open");
    let database_id = DatabaseId::from_bytes({
        let mut bytes = [0x24; 16];
        bytes[6] = 0x74;
        bytes[8] = 0xa4;
        bytes
    })
    .expect("database");
    store.initialize_database(database_id).expect("initialize");
    {
        let mut transaction = store
            .shared
            .database
            .begin_write()
            .expect("begin predecessor write");
        transaction
            .set_durability(Durability::Immediate)
            .expect("durability");
        let predecessor = encode_record_registry_v2(SchemaHash::from_bytes(
            PRE_VECTOR_HEALTH_OBSERVATION_REGISTRY_DIGEST,
        ))
        .expect("predecessor registry");
        transaction
            .open_table(META)
            .expect("metadata")
            .insert(META_RECORD_REGISTRY, predecessor.as_bytes())
            .expect("pin predecessor registry");
        transaction.commit().expect("commit predecessor fixture");
    }
    drop(store);

    let reopened = RedbStore::open(&path).expect("migrate predecessor");
    let transaction = reopened
        .shared
        .database
        .begin_read()
        .expect("read migrated database");
    assert!(
        transaction
            .open_table(crate::layout::VECTOR_OBSERVATIONS)
            .expect("vector observation table")
            .is_empty()
            .expect("table length")
    );
    let metadata = transaction.open_table(META).expect("metadata");
    let encoded = metadata
        .get(META_RECORD_REGISTRY)
        .expect("registry read")
        .expect("registry");
    assert_eq!(
        *decode_record_registry_v2(encoded.value())
            .expect("decode registry")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
}

#[test]
fn pre_vector_projection_control_registry_installs_table_before_publication() {
    let scope = crate::test_path::ScopedDirectory::new("pre-vector-projection-control-registry");
    let path = scope.join("db.redb");
    let mut store = RedbStore::open(&path).expect("open");
    let database_id = DatabaseId::from_bytes({
        let mut bytes = [0x25; 16];
        bytes[6] = 0x75;
        bytes[8] = 0xa5;
        bytes
    })
    .expect("database");
    store.initialize_database(database_id).expect("initialize");
    {
        let mut transaction = store
            .shared
            .database
            .begin_write()
            .expect("begin predecessor write");
        transaction
            .set_durability(Durability::Immediate)
            .expect("durability");
        transaction
            .delete_table(crate::layout::VECTOR_PROJECTION_CONTROLS)
            .expect("remove successor table");
        let predecessor = encode_record_registry_v2(SchemaHash::from_bytes(
            PRE_VECTOR_PROJECTION_CONTROL_REGISTRY_DIGEST,
        ))
        .expect("predecessor registry");
        transaction
            .open_table(META)
            .expect("metadata")
            .insert(META_RECORD_REGISTRY, predecessor.as_bytes())
            .expect("pin predecessor registry");
        transaction.commit().expect("commit predecessor fixture");
    }
    drop(store);

    let reopened = RedbStore::open(&path).expect("migrate predecessor");
    let transaction = reopened
        .shared
        .database
        .begin_read()
        .expect("read migrated database");
    assert!(
        transaction
            .open_table(crate::layout::VECTOR_PROJECTION_CONTROLS)
            .expect("vector projection control table")
            .is_empty()
            .expect("table length")
    );
    let metadata = transaction.open_table(META).expect("metadata");
    let encoded = metadata
        .get(META_RECORD_REGISTRY)
        .expect("registry read")
        .expect("registry");
    assert_eq!(
        *decode_record_registry_v2(encoded.value())
            .expect("decode registry")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
}

// req: PRJ-007, PRJ-010, OQ-024, OQ-053
#[test]
fn pre_columnar_control_registry_installs_table_before_publication() {
    let scope = crate::test_path::ScopedDirectory::new("pre-columnar-control-registry");
    let path = scope.join("db.redb");
    let mut store = RedbStore::open(&path).expect("open");
    let database_id = DatabaseId::from_bytes({
        let mut bytes = [0x26; 16];
        bytes[6] = 0x76;
        bytes[8] = 0xa6;
        bytes
    })
    .expect("database");
    store.initialize_database(database_id).expect("initialize");
    {
        let mut transaction = store
            .shared
            .database
            .begin_write()
            .expect("begin predecessor write");
        transaction
            .set_durability(Durability::Immediate)
            .expect("durability");
        transaction
            .delete_table(crate::layout::COLUMNAR_PROJECTION_CONTROLS)
            .expect("remove successor table");
        let predecessor = encode_record_registry_v2(SchemaHash::from_bytes(
            PRE_COLUMNAR_PROJECTION_CONTROL_REGISTRY_DIGEST,
        ))
        .expect("predecessor registry");
        transaction
            .open_table(META)
            .expect("metadata")
            .insert(META_RECORD_REGISTRY, predecessor.as_bytes())
            .expect("pin predecessor registry");
        transaction.commit().expect("commit predecessor fixture");
    }
    drop(store);

    let reopened = RedbStore::open(&path).expect("migrate predecessor");
    let transaction = reopened
        .shared
        .database
        .begin_read()
        .expect("read migrated database");
    assert!(
        transaction
            .open_table(crate::layout::COLUMNAR_PROJECTION_CONTROLS)
            .expect("columnar projection control table")
            .is_empty()
            .expect("table length")
    );
    let metadata = transaction.open_table(META).expect("metadata");
    let encoded = metadata
        .get(META_RECORD_REGISTRY)
        .expect("registry read")
        .expect("registry");
    assert_eq!(
        *decode_record_registry_v2(encoded.value())
            .expect("decode registry")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
}

#[test]
fn async_checkpoint_starts_half_full_and_reserves_one_maximum_physical_frame() {
    assert_eq!(JOURNAL_CHECKPOINT_START_TRANSITIONS, 4_096);
    assert_eq!(JOURNAL_CHECKPOINT_START_BYTES, 16 * 1024 * 1024);
    let maximum_frame = crate::journal::extent_frame_bytes(crate::journal::MAX_JOURNAL_FRAME_BYTES)
        .expect("maximum physical frame");
    let start_physical =
        journal_checkpoint_start_physical_bytes().expect("physical start watermark");
    assert_eq!(
        start_physical.checked_add(maximum_frame),
        Some(crate::journal::EXTENT_DATA_BYTES)
    );
}

#[test]
fn command_fence_drains_for_direct_commits_and_in_flight_checkpoints() {
    assert!(!command_fence_requires_pipeline_drain(true, false));
    assert!(command_fence_requires_pipeline_drain(false, false));
    assert!(command_fence_requires_pipeline_drain(true, true));
    assert!(command_fence_requires_pipeline_drain(false, true));
}

#[test]
fn journal_capacity_turns_checkpoint_headroom_into_backpressure_boundary() {
    let exact = JournalCapacityCharge {
        checkpoint_transitions: 4_096,
        checkpoint_bytes: 16 * 1024 * 1024,
        suffix_transitions: 4_095,
        suffix_bytes: 16 * 1024 * 1024 - 1,
        suffix_physical_bytes: crate::journal::EXTENT_DATA_BYTES - 4_096,
        unpublished_transitions: 0,
        unpublished_bytes: 0,
    };
    assert!(exact.admits(1, 1, 4_096));
    assert!(!exact.admits(2, 1, 4_096));
    assert!(!exact.admits(1, 2, 4_096));
    assert!(!exact.admits(1, 1, 4_097));

    let unpublished = JournalCapacityCharge {
        checkpoint_transitions: 0,
        checkpoint_bytes: 0,
        suffix_transitions: 0,
        suffix_bytes: 0,
        suffix_physical_bytes: 0,
        unpublished_transitions: crate::journal::MAX_JOURNAL_TRANSITIONS,
        unpublished_bytes: crate::journal::MAX_JOURNAL_FRAME_BYTES,
    };
    assert!(!unpublished.admits(1, 1, 4_096));
}

#[test]
fn durability_epoch_totals_accept_exact_bounds_and_reject_each_successor() {
    let one = riffdb_storage_api::StagedBatchMetrics::new(NonZeroU16::MIN, 1, 1)
        .expect("one-command metrics");
    assert_eq!(
        checked_epoch_totals(
            riffdb_storage_api::MAX_STAGED_COMMANDS - 1,
            riffdb_storage_api::MAX_STAGED_WRITE_BYTES - 1,
            riffdb_storage_api::MAX_STAGED_WRITE_BYTES - 1,
            1,
            one,
        )
        .expect("exact epoch bounds"),
        (
            riffdb_storage_api::MAX_STAGED_COMMANDS,
            riffdb_storage_api::MAX_STAGED_WRITE_BYTES,
            riffdb_storage_api::MAX_STAGED_WRITE_BYTES,
        )
    );
    assert_eq!(
        checked_epoch_totals(riffdb_storage_api::MAX_STAGED_COMMANDS, 0, 0, 1, one,)
            .expect_err("command successor exceeds epoch")
            .kind(),
        StorageErrorKind::LimitExceeded
    );
    assert_eq!(
        checked_epoch_totals(0, riffdb_storage_api::MAX_STAGED_WRITE_BYTES, 0, 1, one,)
            .expect_err("semantic successor exceeds epoch")
            .kind(),
        StorageErrorKind::LimitExceeded
    );
    assert_eq!(
        checked_epoch_totals(0, 0, riffdb_storage_api::MAX_STAGED_WRITE_BYTES, 1, one,)
            .expect_err("encoded successor exceeds epoch")
            .kind(),
        StorageErrorKind::LimitExceeded
    );
}

#[test]
fn dropping_pristine_durability_epoch_is_a_proven_safe_cancellation() {
    let path = TestDatabasePath::new("empty-epoch-cancellation");
    let mut store = RedbStore::open(&path.0).expect("open store");
    store
        .initialize_database(database_id(0x31))
        .expect("initialize store");
    let ports = RedbDormantPorts {
        shared: store.shared,
    }
    .into_operational_after_catalog_validation()
    .expect("activate ports");

    let epoch = ports.begin_deferred_epoch().expect("begin pristine epoch");
    assert!(!epoch.has_unpublished_state());
    drop(epoch);

    ports
        .begin_write()
        .expect("pristine cancellation must not fence later writes")
        .abort()
        .expect("abort probe write");
    assert!(
        ports
            .pending_outbox_page(None, 1)
            .expect("pristine cancellation keeps transient indexes usable")
            .0
            .is_empty()
    );
}

#[test]
fn dropping_nonempty_durability_epoch_remains_fail_closed() {
    let path = TestDatabasePath::new("nonempty-epoch-drop");
    let mut store = RedbStore::open(&path.0).expect("open store");
    store
        .initialize_database(database_id(0x32))
        .expect("initialize store");
    let ports = RedbDormantPorts {
        shared: store.shared,
    }
    .into_operational_after_catalog_validation()
    .expect("activate ports");

    let mut epoch = ports.begin_deferred_epoch().expect("begin epoch");
    epoch.command_count = 1;
    assert!(epoch.has_unpublished_state());
    drop(epoch);

    let error = match ports.begin_write() {
        Ok(_) => panic!("nonempty dropped epoch must fence later writes"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    assert_eq!(
        ports
            .pending_outbox_page(None, 1)
            .expect_err("nonempty dropped epoch invalidates transient indexes")
            .kind(),
        StorageErrorKind::Unavailable
    );
}

/// Whole-directory scope: the database and every side file it grows
/// (journal, checkpoint, spare, durable-format marker, …) live in one
/// [`crate::test_path::ScopedDirectory`] removed on drop — pass, fail, or
/// panic — so cleanup never depends on a hand-maintained file list.
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

fn database_id(seed: u8) -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [seed; 10])
        .expect("valid deterministic UUIDv7")
}

#[test]
fn open_publishes_current_marker_and_refuses_unmarked_existing_bytes() {
    let current_path = TestDatabasePath::new("format-marker-current");
    drop(RedbStore::open(&current_path.0).expect("open new current database"));
    assert_eq!(
        crate::preflight_durable_format_path(&current_path.0),
        Ok(crate::RedbDurableFormatPreflight::OpenCurrent)
    );

    let predecessor_path = TestDatabasePath::new("format-marker-predecessor");
    let predecessor_bytes = b"predecessor bytes remain unchanged";
    std::fs::write(&predecessor_path.0, predecessor_bytes).expect("write predecessor bytes");
    let error = match RedbStore::open(&predecessor_path.0) {
        Ok(_) => panic!("predecessor needs upgrade"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), StorageErrorKind::IncompatibleFormat);
    assert_eq!(
        std::fs::read(&predecessor_path.0).expect("read predecessor"),
        predecessor_bytes
    );
    assert!(!crate::durable_format_marker_path(&predecessor_path.0).exists());
}

fn fixture_envelope_from(file: &str, record_type: &str) -> Vec<u8> {
    let line = file
        .lines()
        .find(|line| line.starts_with(record_type))
        .expect("fixture record exists");
    let encoded = line
        .split('\t')
        .nth(2)
        .expect("fixture includes envelope hex");
    assert_eq!(encoded.len() % 2, 0);
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("fixture hex is UTF-8");
            u8::from_str_radix(text, 16).expect("fixture hex is valid")
        })
        .collect()
}

fn wp373_fixture_envelope(record_type: &str) -> Vec<u8> {
    fixture_envelope_from(
        include_str!("../../../../fixtures/proto/durable-wire-vectors-v2.txt"),
        record_type,
    )
}

fn legacy_v1_fixture_envelope(record_type: &str) -> Vec<u8> {
    fixture_envelope_from(
        include_str!("../../../../fixtures/proto/durable-wire-vectors.txt"),
        record_type,
    )
}

fn install_pre_generation_fixture(path: &Path) {
    let mut store = RedbStore::open(path).expect("open generation fixture");
    store
        .initialize_database(database_id(0x1d))
        .expect("initialize generation fixture");
    let index_id = IndexId::new(7).expect("index ID");
    let mut entity = EntityKeyBuilder::new(EntityTypeId::first());
    entity.push_u64(9).expect("entity key component");
    let mut index = IndexEntryKeyBuilder::new(index_id);
    index.push_u64(11).expect("index component");
    let index_key = index
        .finish(entity.finish().expect("entity key"))
        .expect("index key");
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
    partition.push_u64(3).expect("partition component");
    let partition = partition.finish().expect("partition key");
    let binding = DurableKeySchemaBindingV1::new(
        ContractLineage::new("generation-migration").expect("lineage"),
        ContractVersion::new(1).expect("contract version"),
        ContractBundleHash::from_bytes([0x44; 32]),
    );
    let entry = StoredIndexEntryV2::new(
        index_key.clone(),
        binding.clone(),
        CanonicalRecord::new(Vec::new()).expect("covered values"),
        partition,
    )
    .expect("index entry");
    let encoded_entry = crate::codec::encode_index_entry_v2(&entry).expect("encode index");
    let mut live_prefix = IndexRangePrefixBuilder::new(index_id);
    live_prefix.push_u64(11).expect("legacy prefix component");
    let prefix = StructurallyDecodedIndexRangePrefixV1::from_live(&live_prefix.finish());
    let legacy = LegacyStoredIndexEpochV1::new(
        prefix.clone(),
        binding,
        IndexEpoch::new(4).expect("legacy generation"),
    );
    let encoded_legacy =
        encode_legacy_index_epoch_v1_fixture(&legacy).expect("encode legacy generation");
    let predecessor =
        encode_record_registry_v2(SchemaHash::from_bytes(PRE_INDEX_GENERATION_REGISTRY_DIGEST))
            .expect("encode predecessor registry");
    let transaction = store
        .shared
        .database
        .begin_write()
        .expect("begin generation fixture");
    transaction
        .open_table(SECONDARY_INDEXES)
        .expect("open indexes")
        .insert(index_key.as_bytes(), encoded_entry.as_bytes())
        .expect("insert index");
    transaction
        .open_table(INDEX_EPOCHS)
        .expect("open generations")
        .insert(
            encode_index_range_prefix_key(&prefix),
            encoded_legacy.as_bytes(),
        )
        .expect("insert legacy generation");
    pin_predecessor_registry(&transaction, &predecessor);
    transaction.commit().expect("commit generation fixture");
}

fn assert_generation_fixture_migrated(store: &RedbStore) {
    let read = store
        .shared
        .database
        .begin_read()
        .expect("read migrated generation fixture");
    let metadata = read.open_table(META).expect("open metadata");
    let registry = metadata
        .get(META_RECORD_REGISTRY)
        .expect("read registry")
        .expect("registry exists");
    assert_eq!(
        *decode_record_registry_v2(registry.value())
            .expect("decode registry")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
    drop(registry);
    drop(metadata);

    let epochs = read.open_table(INDEX_EPOCHS).expect("open generations");
    let rows = epochs
        .iter()
        .expect("iterate generations")
        .collect::<Result<Vec<_>, _>>()
        .expect("read generation rows");
    assert_eq!(rows.len(), 1);
    let (physical, encoded) = &rows[0];
    let target = decode_partition_index_key(physical.value()).expect("decode partition/index key");
    let generation = decode_index_epoch_v1(encoded.value())
        .expect("decode current generation")
        .into_parts()
        .0;
    assert_eq!(generation.target(), &target);
    assert_eq!(
        generation.epoch(),
        IndexEpoch::new(5).expect("generation five")
    );
}

#[test]
fn application_commit_profile_defaults_to_standard_and_can_be_hardened() {
    let standard_path = TestDatabasePath::new("standard-commit-profile");
    let standard = RedbStore::open(&standard_path.0).expect("open standard store");
    assert_eq!(
        standard.shared.application_commit_profile,
        RedbCommitProfile::Standard
    );
    assert!(!standard.shared.application_commit_profile.uses_two_phase());

    let hardened_path = TestDatabasePath::new("hardened-commit-profile");
    let hardened =
        RedbStore::open_with_commit_profile(&hardened_path.0, RedbCommitProfile::Hardened)
            .expect("open hardened store");
    assert_eq!(
        hardened.shared.application_commit_profile,
        RedbCommitProfile::Hardened
    );
    assert!(hardened.shared.application_commit_profile.uses_two_phase());
}

#[test]
fn empty_initialize_probe_and_reopen_preserve_one_database_identity() {
    let path = TestDatabasePath::new("identity");
    let expected = database_id(0x11);
    {
        let mut store = RedbStore::open(&path.0).expect("open empty store");
        assert_eq!(
            store.probe_database_identity().expect("probe empty store"),
            DatabaseIdentityProbe::NeedsInitialization
        );
        assert_eq!(
            store
                .initialize_database(expected)
                .expect("initialize database"),
            DatabaseInitializationResult::Installed(expected)
        );
        assert_eq!(
            store.probe_database_identity().expect("probe identity"),
            DatabaseIdentityProbe::Existing(expected)
        );
    }

    let reopened = RedbStore::open(&path.0).expect("reopen database");
    assert_eq!(
        reopened
            .probe_database_identity()
            .expect("probe reopened identity"),
        DatabaseIdentityProbe::Existing(expected)
    );
}

#[test]
fn mixed_v1_v2_framing_resumes_and_publishes_registry_last() {
    let path = TestDatabasePath::new("format-v2-migration");
    let expected = database_id(0x19);
    let mut store = RedbStore::open(&path.0).expect("open empty store");
    store
        .initialize_database(expected)
        .expect("initialize current database");

    let mut transaction = store
        .shared
        .database
        .begin_write()
        .expect("begin legacy fixture");
    transaction
        .set_durability(Durability::Immediate)
        .expect("set fixture durability");
    let mut metadata = transaction.open_table(META).expect("open metadata");
    let legacy_identity = {
        let identity = metadata
            .get(META_DATABASE_ID)
            .expect("read identity")
            .expect("identity exists");
        riffdb_storage_api::proto_codec::transcode_durable_record_to_v1(identity.value())
            .expect("identity transcodes")
            .into_bytes()
    };
    let legacy_format = riffdb_storage_api::proto_codec::transcode_durable_record_to_v1(
        encode_storage_format_version_v1(StorageFormatVersion::V1)
            .expect("legacy semantic format encodes")
            .as_bytes(),
    )
    .expect("format transcodes")
    .into_bytes();
    metadata
        .insert(META_DATABASE_ID, legacy_identity.as_slice())
        .expect("install legacy identity");
    metadata
        .insert(META_FORMAT_VERSION, legacy_format.as_slice())
        .expect("install legacy format");
    metadata
        .remove(META_RECORD_REGISTRY)
        .expect("remove final registry marker");
    metadata
        .remove(META_CHANGELOG_V2_ROTATION_RECEIPT)
        .expect("remove successor rotation receipt");
    drop(metadata);
    transaction.commit().expect("commit mixed fixture");
    drop(store);

    let reopened = RedbStore::open(&path.0).expect("resume format migration");
    assert_eq!(
        reopened
            .probe_database_identity()
            .expect("probe migrated identity"),
        DatabaseIdentityProbe::Existing(expected)
    );
    let read = reopened
        .shared
        .database
        .begin_read()
        .expect("read migrated metadata");
    let metadata = read.open_table(META).expect("open migrated metadata");
    for row in metadata.iter().expect("iterate migrated metadata") {
        let (key, value) = row.expect("read migrated row");
        if key.value() == META_INDEX_EPOCH_ROWS_REPAIRED {
            // Process marker is not a durable envelope.
            assert_eq!(value.value(), [1u8].as_slice());
            continue;
        }
        assert!(value.value().starts_with(b"RDB2"));
    }
    let registry = metadata
        .get(META_RECORD_REGISTRY)
        .expect("read registry")
        .expect("registry published");
    assert_eq!(
        *decode_record_registry_v2(registry.value())
            .expect("decode registry")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
}

#[test]
fn prefix_epochs_migrate_to_one_partition_index_generation_and_publish_last() {
    let path = TestDatabasePath::new("partition-index-generation-migration");
    install_pre_generation_fixture(&path.0);

    let reopened = RedbStore::open(&path.0).expect("migrate generation fixture");
    assert_generation_fixture_migrated(&reopened);
}

#[test]
fn prefix_epoch_migration_restarts_after_a_proven_precommit_failure() {
    let path = TestDatabasePath::new("partition-index-generation-restart");
    install_pre_generation_fixture(&path.0);
    let controller =
        RedbTestController::return_before_commit(RedbTestOperation::StorageFormatMigrationBatch);
    let error = match RedbStore::open_with_test_controller(&path.0, controller) {
        Ok(_) => panic!("armed migration must fail before commit"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);

    let reopened = RedbStore::open(&path.0).expect("restart generation migration");
    assert_generation_fixture_migrated(&reopened);
}

#[test]
fn wp373_event_copies_migrate_to_exact_references_before_registry_publication() {
    let path = TestDatabasePath::new("event-reference-migration");
    let mut store = RedbStore::open(&path.0).expect("open empty store");
    store
        .initialize_database(database_id(0x1b))
        .expect("initialize current database");

    let transaction = store
        .shared
        .database
        .begin_write()
        .expect("begin WP-373 fixture");
    let event_id = EventId::new(CommitSequence::first(), 0);
    let commit_key = encode_application_sequence_key(CommitSequence::first());
    let event_key = encode_event_key(event_id);
    let event = wp373_fixture_envelope("riffdb.storage.v1.StoredDurableEventV1");
    let commit = wp373_fixture_envelope("riffdb.storage.v1.StoredCommitRecordV1");
    let outbox = wp373_fixture_envelope("riffdb.storage.v1.StoredOutboxIntentV1");
    transaction
        .open_table(EVENTS)
        .expect("open events")
        .insert(event_key.as_slice(), event.as_slice())
        .expect("insert authoritative event");
    transaction
        .open_table(COMMITS)
        .expect("open commits")
        .insert(commit_key.as_slice(), commit.as_slice())
        .expect("insert historical commit");
    transaction
        .open_table(OUTBOX)
        .expect("open outbox")
        .insert(event_key.as_slice(), outbox.as_slice())
        .expect("insert historical outbox intent");
    let predecessor =
        encode_record_registry_v2(SchemaHash::from_bytes(PRE_EVENT_REFERENCE_REGISTRY_DIGEST))
            .expect("encode predecessor registry");
    pin_predecessor_registry(&transaction, &predecessor);
    transaction.commit().expect("commit WP-373 fixture");
    drop(store);

    let interrupted = RedbTestController::return_unknown_after_commit(
        RedbTestOperation::StorageFormatMigrationBatch,
    );
    assert_eq!(
        RedbStore::open_with_test_controller(&path.0, interrupted)
            .expect_err("injected postcommit uncertainty interrupts migration")
            .kind(),
        StorageErrorKind::CommitStatusUnknown
    );
    let migrated = RedbStore::open(&path.0).expect("migrate predecessor database");
    let read = migrated
        .shared
        .database
        .begin_read()
        .expect("read migrated database");
    let events = read.open_table(EVENTS).expect("open events");
    let commits = read.open_table(COMMITS).expect("open commits");
    let commit = commits
        .get(commit_key.as_slice())
        .expect("read commit")
        .expect("commit remains");
    // After the full migration chain, commits are current V3 entity references
    // (compact tag 17, revision 3); outbox remains V2 (tag 15, revision 2).
    assert_eq!(&commit.value()[..8], b"RDB2\x02\x11\0\x03");
    decode_commit_with_event_table(commit.value(), &events)
        .expect("migrated commit proves authoritative event");
    let outbox_table = read.open_table(OUTBOX).expect("open outbox");
    let outbox = outbox_table
        .get(event_key.as_slice())
        .expect("read outbox")
        .expect("outbox remains");
    assert_eq!(&outbox.value()[..8], b"RDB2\x02\x0f\0\x02");
    decode_outbox_with_event_table(outbox.value(), &events)
        .expect("migrated outbox proves authoritative event");
    let metadata = read.open_table(META).expect("open metadata");
    let registry = metadata
        .get(META_RECORD_REGISTRY)
        .expect("read registry")
        .expect("registry remains");
    assert_eq!(
        *decode_record_registry_v2(registry.value())
            .expect("decode registry")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
}

fn v2_commit_fixture_from_legacy_v1() -> (Vec<u8>, Vec<u8>, EventId, CommitSequence) {
    let legacy_commit = legacy_v1_fixture_envelope("riffdb.storage.v1.StoredCommitRecordV1");
    let (v2, event) = riffdb_storage_api::rewrap_legacy_commit_v1_as_v2_fixture(&legacy_commit)
        .expect("rewrap legacy commit as v2");
    let decoded = riffdb_storage_api::decode_commit_record_v2(
        v2.as_bytes(),
        // Event is rehydrated after table load; for identity extract decode event alone.
        vec![
            riffdb_storage_api::decode_durable_event_v1(event.as_bytes())
                .expect("decode event")
                .into_parts()
                .0,
        ],
    )
    .expect("v2 decodes with its event");
    let commit = decoded.into_parts().0;
    let event_id = commit.events()[0].event_id();
    (
        v2.into_bytes(),
        event.into_bytes(),
        event_id,
        commit.commit_sequence(),
    )
}

#[test]
fn entity_reference_migration_transcodes_v2_commits_and_is_idempotent() {
    let path = TestDatabasePath::new("entity-reference-migration");
    let mut store = RedbStore::open(&path.0).expect("open empty store");
    store
        .initialize_database(database_id(0x2e))
        .expect("initialize current database");

    let (v2, event, event_id, sequence) = v2_commit_fixture_from_legacy_v1();
    let commit_key = encode_application_sequence_key(sequence);
    let event_key = encode_event_key(event_id);

    let transaction = store
        .shared
        .database
        .begin_write()
        .expect("begin entity-reference fixture");
    transaction
        .open_table(EVENTS)
        .expect("open events")
        .insert(event_key.as_slice(), event.as_slice())
        .expect("insert event");
    transaction
        .open_table(COMMITS)
        .expect("open commits")
        .insert(commit_key.as_slice(), v2.as_slice())
        .expect("insert v2 commit");
    let predecessor =
        encode_record_registry_v2(SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST))
            .expect("encode predecessor registry");
    pin_predecessor_registry(&transaction, &predecessor);
    transaction
        .commit()
        .expect("commit entity-reference fixture");
    drop(store);

    // Crash-restart at page boundary: first open is interrupted after a batch.
    let interrupted = RedbTestController::return_unknown_after_commit(
        RedbTestOperation::StorageFormatMigrationBatch,
    );
    assert_eq!(
        RedbStore::open_with_test_controller(&path.0, interrupted)
            .expect_err("injected postcommit uncertainty interrupts migration")
            .kind(),
        StorageErrorKind::CommitStatusUnknown
    );

    let migrated = RedbStore::open(&path.0).expect("migrate predecessor database");
    let read = migrated
        .shared
        .database
        .begin_read()
        .expect("read migrated database");
    let events = read.open_table(EVENTS).expect("open events");
    let commits = read.open_table(COMMITS).expect("open commits");
    let commit = commits
        .get(commit_key.as_slice())
        .expect("read commit")
        .expect("commit remains");
    // Compact V3: magic RDB2, format 2, compact tag 17, revision 3.
    assert_eq!(&commit.value()[..8], b"RDB2\x02\x11\0\x03");
    decode_commit_with_event_table(commit.value(), &events)
        .expect("migrated commit proves entity references");
    let metadata = read.open_table(META).expect("open metadata");
    let registry = metadata
        .get(META_RECORD_REGISTRY)
        .expect("read registry")
        .expect("registry remains");
    assert_eq!(
        *decode_record_registry_v2(registry.value())
            .expect("decode registry")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
    drop(metadata);
    drop(commits);
    drop(events);
    drop(read);
    drop(migrated);

    // Second open is a no-op: all pages abort without rewrite.
    let again = RedbStore::open(&path.0).expect("second open is no-op");
    let read = again
        .shared
        .database
        .begin_read()
        .expect("read after no-op");
    let registry = read
        .open_table(META)
        .expect("meta")
        .get(META_RECORD_REGISTRY)
        .expect("read")
        .expect("registry");
    assert_eq!(
        *decode_record_registry_v2(registry.value())
            .expect("decode")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
}

#[test]
fn entity_reference_migration_from_pre_audit_request_index_converges() {
    let path = TestDatabasePath::new("entity-reference-from-audit");
    let mut store = RedbStore::open(&path.0).expect("open empty store");
    store
        .initialize_database(database_id(0x30))
        .expect("initialize");
    let (v2, event, event_id, sequence) = v2_commit_fixture_from_legacy_v1();
    let commit_key = encode_application_sequence_key(sequence);
    let event_key = encode_event_key(event_id);
    let transaction = store.shared.database.begin_write().expect("write");
    transaction
        .open_table(EVENTS)
        .expect("events")
        .insert(event_key.as_slice(), event.as_slice())
        .expect("insert event");
    transaction
        .open_table(COMMITS)
        .expect("commits")
        .insert(commit_key.as_slice(), v2.as_slice())
        .expect("insert v2 commit");
    // Enter the chain at the AuditRequestIndex step: terminal publish was
    // rewired from `current` to PRE_ENTITY_REFERENCE.
    let predecessor = encode_record_registry_v2(SchemaHash::from_bytes(
        PRE_AUDIT_REQUEST_INDEX_REGISTRY_DIGEST,
    ))
    .expect("encode pre-audit registry");
    pin_predecessor_registry(&transaction, &predecessor);
    transaction.commit().expect("commit fixture");
    drop(store);

    let migrated = RedbStore::open(&path.0).expect("migrate from PRE_AUDIT");
    let read = migrated.shared.database.begin_read().expect("read");
    let registry = read
        .open_table(META)
        .expect("meta")
        .get(META_RECORD_REGISTRY)
        .expect("get")
        .expect("registry");
    assert_eq!(
        *decode_record_registry_v2(registry.value())
            .expect("decode")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
    let commits = read.open_table(COMMITS).expect("commits");
    let commit = commits
        .get(commit_key.as_slice())
        .expect("get")
        .expect("commit");
    assert_eq!(&commit.value()[..8], b"RDB2\x02\x11\0\x03");
}

#[test]
fn entity_reference_migration_survives_damaged_events_row() {
    let path = TestDatabasePath::new("entity-reference-damaged-event");
    let mut store = RedbStore::open(&path.0).expect("open empty store");
    store
        .initialize_database(database_id(0x31))
        .expect("initialize");
    let (v2, _event, _event_id, sequence) = v2_commit_fixture_from_legacy_v1();
    let commit_key = encode_application_sequence_key(sequence);
    let transaction = store.shared.database.begin_write().expect("write");
    // Missing event row (commit still references it). Migration must not
    // join EVENTS; damage surfaces later as a structural finding. A garbage
    // EVENTS payload would also fail the general compact-row pass, which is
    // outside this migration step.
    transaction
        .open_table(COMMITS)
        .expect("commits")
        .insert(commit_key.as_slice(), v2.as_slice())
        .expect("insert v2 commit");
    let predecessor =
        encode_record_registry_v2(SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST))
            .expect("encode predecessor");
    pin_predecessor_registry(&transaction, &predecessor);
    transaction.commit().expect("commit fixture");
    drop(store);

    let migrated = RedbStore::open(&path.0).expect("migration must not join EVENTS");
    let read = migrated.shared.database.begin_read().expect("read");
    let registry = read
        .open_table(META)
        .expect("meta")
        .get(META_RECORD_REGISTRY)
        .expect("get")
        .expect("registry");
    assert_eq!(
        *decode_record_registry_v2(registry.value())
            .expect("decode")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
    let commit = read
        .open_table(COMMITS)
        .expect("commits")
        .get(commit_key.as_slice())
        .expect("get")
        .expect("commit");
    assert_eq!(&commit.value()[..8], b"RDB2\x02\x11\0\x03");
}

#[test]
fn entity_reference_migration_splits_501_rows_and_crash_restarts() {
    let path = TestDatabasePath::new("entity-reference-501");
    let mut store = RedbStore::open(&path.0).expect("open empty store");
    store
        .initialize_database(database_id(0x32))
        .expect("initialize");
    let (v2_template, event, event_id, _) = v2_commit_fixture_from_legacy_v1();
    let event_key = encode_event_key(event_id);
    let transaction = store.shared.database.begin_write().expect("write");
    transaction
        .open_table(EVENTS)
        .expect("events")
        .insert(event_key.as_slice(), event.as_slice())
        .expect("insert event");
    {
        let mut commits = transaction.open_table(COMMITS).expect("commits");
        // 501 rows forces at least one page split at FORMAT_MIGRATION_MAX_ROWS=500.
        for seq in 1u64..=501 {
            let sequence = CommitSequence::new(seq).expect("sequence");
            let key = encode_application_sequence_key(sequence);
            // Reuse the same V2 payload; migration rewrites by content not key.
            commits
                .insert(key.as_slice(), v2_template.as_slice())
                .expect("insert commit row");
        }
    }
    let predecessor =
        encode_record_registry_v2(SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST))
            .expect("encode predecessor");
    pin_predecessor_registry(&transaction, &predecessor);
    transaction.commit().expect("commit 501-row fixture");
    drop(store);

    let interrupted = RedbTestController::return_unknown_after_commit(
        RedbTestOperation::StorageFormatMigrationBatch,
    );
    assert_eq!(
        RedbStore::open_with_test_controller(&path.0, interrupted)
            .expect_err("page-boundary interrupt")
            .kind(),
        StorageErrorKind::CommitStatusUnknown
    );
    let migrated = RedbStore::open(&path.0).expect("resume migration converges");
    let read = migrated.shared.database.begin_read().expect("read");
    let registry = read
        .open_table(META)
        .expect("meta")
        .get(META_RECORD_REGISTRY)
        .expect("get")
        .expect("registry");
    assert_eq!(
        *decode_record_registry_v2(registry.value())
            .expect("decode")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
    let commits = read.open_table(COMMITS).expect("commits");
    let mut v3_count = 0usize;
    for row in commits.iter().expect("iter") {
        let (_, value) = row.expect("row");
        if value.value().starts_with(b"RDB2\x02\x11\0\x03") {
            v3_count += 1;
        }
    }
    assert_eq!(v3_count, 501, "all rows transcoded to V3");
    drop(commits);
    drop(read);
    drop(migrated);
    // Second open no-ops (all pages abort).
    RedbStore::open(&path.0).expect("second open no-op");
}

#[test]
fn entity_reference_migration_empty_database_path_converges() {
    let path = TestDatabasePath::new("entity-reference-empty");
    let mut store = RedbStore::open(&path.0).expect("open empty store");
    store
        .initialize_database(database_id(0x2f))
        .expect("initialize");
    let predecessor =
        encode_record_registry_v2(SchemaHash::from_bytes(PRE_ENTITY_REFERENCE_REGISTRY_DIGEST))
            .expect("encode predecessor");
    let transaction = store.shared.database.begin_write().expect("write");
    pin_predecessor_registry(&transaction, &predecessor);
    transaction.commit().expect("commit predecessor");
    drop(store);
    let migrated = RedbStore::open(&path.0).expect("empty path migrates");
    let registry = migrated
        .shared
        .database
        .begin_read()
        .expect("read")
        .open_table(META)
        .expect("meta")
        .get(META_RECORD_REGISTRY)
        .expect("get")
        .expect("registry");
    assert_eq!(
        *decode_record_registry_v2(registry.value())
            .expect("decode")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
}

#[test]
fn event_routes_rebuild_idempotently_before_registry_publication() {
    let path = TestDatabasePath::new("event-route-migration");
    let mut store = RedbStore::open(&path.0).expect("open empty store");
    store
        .initialize_database(database_id(0x1c))
        .expect("initialize current database");

    let event_id = EventId::new(CommitSequence::first(), 0);
    let commit_key = encode_application_sequence_key(CommitSequence::first());
    let event_key = encode_event_key(event_id);
    let event = wp373_fixture_envelope("riffdb.storage.v1.StoredDurableEventV1");
    let commit = wp373_fixture_envelope("riffdb.storage.v1.StoredCommitRecordV1");
    let predecessor =
        encode_record_registry_v2(SchemaHash::from_bytes(PRE_EVENT_ROUTE_REGISTRY_DIGEST))
            .expect("encode predecessor registry");
    let transaction = store
        .shared
        .database
        .begin_write()
        .expect("begin event-route fixture");
    transaction
        .open_table(EVENTS)
        .expect("open events")
        .insert(event_key.as_slice(), event.as_slice())
        .expect("insert authoritative event");
    transaction
        .open_table(COMMITS)
        .expect("open commits")
        .insert(commit_key.as_slice(), commit.as_slice())
        .expect("insert historical commit");
    pin_predecessor_registry(&transaction, &predecessor);
    transaction.commit().expect("commit predecessor fixture");
    drop(store);

    let interrupted = RedbTestController::return_unknown_after_commit(
        RedbTestOperation::StorageFormatMigrationBatch,
    );
    assert_eq!(
        RedbStore::open_with_test_controller(&path.0, interrupted)
            .expect_err("injected postcommit uncertainty interrupts route migration")
            .kind(),
        StorageErrorKind::CommitStatusUnknown
    );

    let migrated = RedbStore::open(&path.0).expect("resume event-route migration");
    let read = migrated
        .shared
        .database
        .begin_read()
        .expect("read migrated database");
    let events = read.open_table(EVENTS).expect("open events");
    let commits = read.open_table(COMMITS).expect("open commits");
    let encoded_commit = commits
        .get(commit_key.as_slice())
        .expect("read commit")
        .expect("commit remains");
    let decoded_commit = decode_commit_with_event_table(encoded_commit.value(), &events)
        .expect("decode authoritative commit")
        .into_parts()
        .0;
    let event = decoded_commit.events().first().expect("commit event");
    let route_key = encode_event_route_key(decoded_commit.partition_hash(), event_id);
    let routes = read.open_table(EVENT_ROUTES).expect("open event routes");
    let encoded_route = routes
        .get(route_key.as_slice())
        .expect("read route")
        .expect("rebuilt route exists");
    let route = decode_event_route_v1(encoded_route.value())
        .expect("decode route")
        .into_parts()
        .0;
    assert_eq!(route.event_id(), event.event_id());
    assert_eq!(route.event_type_id(), event.event_type_id());
    assert_eq!(route.event_hash(), event.event_hash());
    let metadata = read.open_table(META).expect("open metadata");
    let registry = metadata
        .get(META_RECORD_REGISTRY)
        .expect("read registry")
        .expect("registry remains");
    assert_eq!(
        *decode_record_registry_v2(registry.value())
            .expect("decode registry")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
}

#[test]
fn current_format_rejects_unknown_compact_identity_before_probe() {
    let path = TestDatabasePath::new("unknown-compact-tag");
    let mut store = RedbStore::open(&path.0).expect("open empty store");
    store
        .initialize_database(database_id(0x1a))
        .expect("initialize current database");
    let transaction = store
        .shared
        .database
        .begin_write()
        .expect("begin corruption fixture");
    let mut metadata = transaction.open_table(META).expect("open metadata");
    let mut identity = metadata
        .get(META_DATABASE_ID)
        .expect("read identity")
        .expect("identity exists")
        .value()
        .to_vec();
    identity[5] = u8::MAX;
    metadata
        .insert(META_DATABASE_ID, identity.as_slice())
        .expect("install unknown tag");
    drop(metadata);
    transaction.commit().expect("commit corruption fixture");
    drop(store);

    assert_eq!(
        RedbStore::open(&path.0)
            .expect_err("unknown compact identity must fail")
            .kind(),
        StorageErrorKind::IncompatibleFormat
    );
}

#[test]
fn initialization_recheck_returns_the_durable_winner() {
    let path = TestDatabasePath::new("winner");
    let mut first = RedbStore::open(&path.0).expect("open store");
    let mut second = first.reopen_for_test();
    let winner = database_id(0x22);
    let loser = database_id(0x33);

    assert_eq!(
        first.initialize_database(winner).expect("install winner"),
        DatabaseInitializationResult::Installed(winner)
    );
    assert_eq!(
        second.initialize_database(loser).expect("observe winner"),
        DatabaseInitializationResult::ConcurrentWinner(winner)
    );
}

#[test]
fn precommit_failure_is_proven_absent_and_postcommit_unknown_fences_writes() {
    let before_path = TestDatabasePath::new("before-commit");
    let before = RedbTestController::return_before_commit(RedbTestOperation::Initialization);
    let mut store = RedbStore::open_with_test_controller(&before_path.0, before)
        .expect("open controlled store");
    assert_eq!(
        store
            .initialize_database(database_id(0x44))
            .expect_err("injected precommit failure")
            .kind(),
        StorageErrorKind::Unavailable
    );
    assert_eq!(
        store.probe_database_identity().expect("probe after abort"),
        DatabaseIdentityProbe::NeedsInitialization
    );

    let after_path = TestDatabasePath::new("after-commit");
    let after = RedbTestController::return_unknown_after_commit(RedbTestOperation::Initialization);
    let mut store =
        RedbStore::open_with_test_controller(&after_path.0, after).expect("open controlled store");
    assert_eq!(
        store
            .initialize_database(database_id(0x55))
            .expect_err("injected uncertain response")
            .kind(),
        StorageErrorKind::CommitStatusUnknown
    );
    assert_eq!(
        store.probe_database_identity().expect("durable winner"),
        DatabaseIdentityProbe::Existing(database_id(0x55))
    );
    assert_eq!(
        store
            .initialize_database(database_id(0x66))
            .expect_err("fenced handle")
            .kind(),
        StorageErrorKind::Unavailable
    );

    drop(store);
    let reopened = RedbStore::open(&after_path.0).expect("reopen clears process-local fence");
    assert_eq!(
        reopened.probe_database_identity().expect("reopen identity"),
        DatabaseIdentityProbe::Existing(database_id(0x55))
    );
}

#[test]
fn postcommit_accelerator_failure_preserves_known_success_and_does_not_fence_core_writes() {
    let path = TestDatabasePath::new("postcommit-accelerator");
    let mut store = RedbStore::open(&path.0).expect("open store");
    store
        .initialize_database(database_id(0x77))
        .expect("initialize store");
    let dormant = RedbDormantPorts {
        shared: store.shared,
    };
    let ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate ports");
    let event_id = EventId::new(CommitSequence::first(), 0);

    for _ in 0..2 {
        ports
            .begin_write()
            .expect("begin known-success transaction")
            .commit_for_with_delta(
                RedbTestOperation::CommandBatch,
                Some(TransientIndexDelta::PendingOutboxInserted(vec![event_id])),
            )
            .expect("confirmed engine commit remains known success");
    }
    assert_eq!(
        ports
            .pending_outbox_page(None, 1)
            .expect_err("duplicate delta degrades only the outbox accelerator")
            .kind(),
        StorageErrorKind::Unavailable
    );
    ports
        .begin_write()
        .expect("core writes remain unfenced")
        .abort()
        .expect("abort proof transaction");
}

#[test]
fn current_digest_reopen_skips_index_epoch_scan_after_repair_marker() {
    let path = TestDatabasePath::new("epoch-repair-marker-skip");
    let mut store = RedbStore::open(&path.0).expect("open");
    store
        .initialize_database(database_id(0x73))
        .expect("initialize");
    // Fresh init installs the repair marker; reopen must not require a scan.
    assert!(
        !index_epoch_rows_may_need_legacy_repair(&store.shared).expect("gate"),
        "init marker must short-circuit the full secondary-index scan"
    );
    store
        .complete_partition_index_generation_migration()
        .expect("current digest no-op");
    assert!(
        !index_epoch_rows_may_need_legacy_repair(&store.shared).expect("gate again"),
        "reopen remains O(1)"
    );
}

#[test]
fn history_incarnation_migration_inserts_initial_and_is_idempotent() {
    let path = TestDatabasePath::new("history-incarnation-migrate");
    let mut store = RedbStore::open(&path.0).expect("open");
    store
        .initialize_database(database_id(0x71))
        .expect("initialize");

    // Simulate a pre-fence database: drop the key and roll the registry
    // digest back to PRE_HISTORY_INCARNATION.
    {
        let transaction = store.shared.database.begin_write().expect("begin write");
        {
            let mut meta = transaction.open_table(META).expect("meta");
            meta.remove(META_HISTORY_INCARNATION).expect("remove key");
            let predecessor = encode_record_registry_v2(SchemaHash::from_bytes(
                PRE_HISTORY_INCARNATION_REGISTRY_DIGEST,
            ))
            .expect("encode predecessor");
            meta.insert(META_RECORD_REGISTRY, predecessor.as_bytes())
                .expect("install predecessor");
            meta.remove(META_CHANGELOG_V2_ROTATION_RECEIPT)
                .expect("remove successor rotation receipt");
        }
        transaction.commit().expect("commit pre-fence fixture");
    }

    store
        .complete_partition_index_generation_migration()
        .expect("migrate history incarnation");

    let read = store.shared.database.begin_read().expect("read");
    let meta = read.open_table(META).expect("meta");
    let encoded = meta
        .get(META_HISTORY_INCARNATION)
        .expect("get")
        .expect("history key present after migration");
    let incarnation = *decode_history_incarnation_v1(encoded.value())
        .expect("decode")
        .value();
    assert_eq!(incarnation, HISTORY_INCARNATION_INITIAL);
    let registry = meta
        .get(META_RECORD_REGISTRY)
        .expect("registry get")
        .expect("registry present");
    assert_eq!(
        *decode_record_registry_v2(registry.value())
            .expect("decode registry")
            .value(),
        riffdb_storage_api::proto_codec::current_record_registry_digest()
    );
    drop(registry);
    drop(encoded);
    drop(meta);
    drop(read);

    // Second open / migration is a no-op for both digest and key value.
    store
        .complete_partition_index_generation_migration()
        .expect("idempotent migration");
    let read = store.shared.database.begin_read().expect("read again");
    let meta = read.open_table(META).expect("meta");
    let encoded = meta
        .get(META_HISTORY_INCARNATION)
        .expect("get")
        .expect("still present");
    assert_eq!(
        *decode_history_incarnation_v1(encoded.value())
            .expect("decode")
            .value(),
        HISTORY_INCARNATION_INITIAL
    );
}

#[test]
fn current_digest_still_runs_index_generation_row_repair() {
    // I4: short-circuit must not skip migrate_partition_index_generations
    // even when the registry digest is already current.
    let path = TestDatabasePath::new("current-digest-epoch-repair");
    let mut store = RedbStore::open(&path.0).expect("open");
    store
        .initialize_database(database_id(0x72))
        .expect("initialize");

    // Simulate a pre-marker database that published the current digest while
    // still holding legacy INDEX_EPOCHS rows: clear the init repair marker.
    {
        let transaction = store.shared.database.begin_write().expect("write");
        {
            let mut meta = transaction.open_table(META).expect("meta");
            meta.remove(META_INDEX_EPOCH_ROWS_REPAIRED)
                .expect("clear marker");
        }
        transaction.commit().expect("commit");
    }

    // Full downgrade_all_index_rows_to_v1_fixture shape: V2 index entry +
    // legacy epoch row, with the current registry digest left in place.
    let index_id = IndexId::new(9).expect("index");
    let mut index = IndexEntryKeyBuilder::new(index_id);
    index.push_u64(11).expect("index component");
    let mut entity = EntityKeyBuilder::new(EntityTypeId::first());
    entity.push_u64(1).expect("entity component");
    let index_key = index
        .finish(entity.finish().expect("entity key"))
        .expect("index key");
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
    partition.push_u64(3).expect("partition component");
    let partition = partition.finish().expect("partition key");
    let binding = DurableKeySchemaBindingV1::new(
        ContractLineage::new("repair-i4").expect("lineage"),
        ContractVersion::new(1).expect("contract version"),
        ContractBundleHash::from_bytes([0x45; 32]),
    );
    let entry = StoredIndexEntryV2::new(
        index_key.clone(),
        binding.clone(),
        CanonicalRecord::new(Vec::new()).expect("covered values"),
        partition,
    )
    .expect("index entry");
    let encoded_entry = crate::codec::encode_index_entry_v2(&entry).expect("encode index");
    let mut live_prefix = IndexRangePrefixBuilder::new(index_id);
    live_prefix.push_u64(11).expect("prefix component");
    let prefix = StructurallyDecodedIndexRangePrefixV1::from_live(&live_prefix.finish());
    let legacy = LegacyStoredIndexEpochV1::new(
        prefix.clone(),
        binding,
        IndexEpoch::new(3).expect("legacy generation"),
    );
    let encoded_legacy =
        encode_legacy_index_epoch_v1_fixture(&legacy).expect("encode legacy generation");
    {
        let transaction = store.shared.database.begin_write().expect("write");
        transaction
            .open_table(SECONDARY_INDEXES)
            .expect("indexes")
            .insert(index_key.as_bytes(), encoded_entry.as_bytes())
            .expect("insert index");
        transaction
            .open_table(INDEX_EPOCHS)
            .expect("epochs")
            .insert(
                encode_index_range_prefix_key(&prefix),
                encoded_legacy.as_bytes(),
            )
            .expect("insert legacy");
        transaction.commit().expect("commit legacy fixture");
    }

    // Registry is already current — this is the I4 short-circuit path.
    {
        let read = store.shared.database.begin_read().expect("read");
        let meta = read.open_table(META).expect("meta");
        let registry = meta
            .get(META_RECORD_REGISTRY)
            .expect("get")
            .expect("registry");
        assert_eq!(
            *decode_record_registry_v2(registry.value())
                .expect("decode")
                .value(),
            riffdb_storage_api::proto_codec::current_record_registry_digest()
        );
    }

    store
        .complete_partition_index_generation_migration()
        .expect("row repair on current digest");

    let read = store.shared.database.begin_read().expect("read");
    let epochs = read.open_table(INDEX_EPOCHS).expect("epochs");
    let rows = epochs
        .iter()
        .expect("iter")
        .collect::<Result<Vec<_>, _>>()
        .expect("rows");
    assert_eq!(rows.len(), 1);
    let (_key, value) = &rows[0];
    let generation = decode_index_epoch_v1(value.value())
        .expect("legacy row repaired to current encoding")
        .into_parts()
        .0;
    // Migration rewrites prefix-keyed legacy rows to partition-keyed current
    // encoding; the retained epoch is at least the legacy maximum.
    assert!(generation.epoch().get() >= 3);
}

// req: OUT-001, OUT-002, TXN-042, REC-004, PERF-019
#[test]
fn fresh_locator_preserving_immediate_permits_are_closed_exact_and_bounded() {
    use crate::hooks::RedbTestOperation;

    let preserving = [
        RedbTestOperation::Admission,
        RedbTestOperation::ExecutionFailure,
        RedbTestOperation::ServiceAudit,
        RedbTestOperation::CatalogAdministration,
        RedbTestOperation::QueryModuleAdministration,
        RedbTestOperation::ReactiveModuleAdministration,
        RedbTestOperation::CapabilityAdministration,
        RedbTestOperation::CapabilityBootstrap,
        RedbTestOperation::ProjectionMutation,
        RedbTestOperation::OutboxTransition,
        RedbTestOperation::EventConsumerTransition,
        RedbTestOperation::ColumnarProjectionControl,
        RedbTestOperation::ApplicationInstallationCampaign,
        RedbTestOperation::ApplicationExportOperation,
    ];
    assert!(
        preserving
            .into_iter()
            .all(|operation| { PreservingImmediateClass::from_operation(operation).is_some() })
    );
    for disabling in [
        RedbTestOperation::CommandBatch,
        RedbTestOperation::DeferredCommandBatch,
        RedbTestOperation::StorageFormatMigrationBatch,
        RedbTestOperation::ContractMigrationBatch,
        RedbTestOperation::ContractMigrationCutover,
        RedbTestOperation::Restore,
        RedbTestOperation::RetentionPruneSubrange,
    ] {
        assert!(PreservingImmediateClass::from_operation(disabling).is_none());
    }

    let insert_only = [
        PreservingImmediateClass::Admission,
        PreservingImmediateClass::ServiceAudit,
        PreservingImmediateClass::Catalog,
        PreservingImmediateClass::QueryModule,
        PreservingImmediateClass::ReactiveModule,
        PreservingImmediateClass::CapabilityAdministration,
        PreservingImmediateClass::CapabilityBootstrap,
        PreservingImmediateClass::ColumnarControl,
        PreservingImmediateClass::Installation,
        PreservingImmediateClass::Export,
    ];
    assert!(insert_only.into_iter().all(|class| {
        fresh_locator_action_allowed(class, FreshLocatorMutationKind::Insert)
            && !fresh_locator_action_allowed(class, FreshLocatorMutationKind::Delete)
    }));
    for class in [
        PreservingImmediateClass::ExecutionFailure,
        PreservingImmediateClass::Projection,
        PreservingImmediateClass::Outbox,
        PreservingImmediateClass::Consumer,
    ] {
        assert!(fresh_locator_action_allowed(
            class,
            FreshLocatorMutationKind::Insert
        ));
        assert!(fresh_locator_action_allowed(
            class,
            FreshLocatorMutationKind::Delete
        ));
    }

    let exact_tables = [
        (
            PreservingImmediateClass::Admission,
            "idempotency_pending",
            b"k".as_slice(),
        ),
        (
            PreservingImmediateClass::ExecutionFailure,
            "idempotency",
            b"k".as_slice(),
        ),
        (
            PreservingImmediateClass::ServiceAudit,
            "audit",
            b"k".as_slice(),
        ),
        (
            PreservingImmediateClass::Catalog,
            "contract_bundles",
            b"k".as_slice(),
        ),
        (
            PreservingImmediateClass::QueryModule,
            "query_modules",
            b"k".as_slice(),
        ),
        (
            PreservingImmediateClass::ReactiveModule,
            "reactive_modules",
            b"k".as_slice(),
        ),
        (
            PreservingImmediateClass::CapabilityAdministration,
            "capabilities",
            b"k".as_slice(),
        ),
        (
            PreservingImmediateClass::CapabilityBootstrap,
            "meta",
            crate::layout::META_CAPABILITY_BOOTSTRAP.as_bytes(),
        ),
        (
            PreservingImmediateClass::Projection,
            "projection_state",
            b"k".as_slice(),
        ),
        (
            PreservingImmediateClass::Outbox,
            "outbox_status",
            b"k".as_slice(),
        ),
        (
            PreservingImmediateClass::Consumer,
            "event_consumers",
            b"k".as_slice(),
        ),
        (
            PreservingImmediateClass::ColumnarControl,
            "columnar_projection_controls",
            b"k".as_slice(),
        ),
        (
            PreservingImmediateClass::Installation,
            "application_installation_campaigns",
            b"k".as_slice(),
        ),
        (
            PreservingImmediateClass::Export,
            "application_export_operations",
            b"k".as_slice(),
        ),
    ];
    assert!(
        exact_tables
            .iter()
            .all(|(class, table, key)| { fresh_locator_table_allowed(*class, table, key) })
    );
    for (class, table, key) in exact_tables {
        let expected = FreshLocatorMutationPermit {
            table: table.into(),
            key: key.into(),
            kind: FreshLocatorMutationKind::Insert,
        };
        let actual = FreshLocatorMutationPermit {
            table: table.into(),
            key: key.into(),
            kind: FreshLocatorMutationKind::Insert,
        };
        assert!(
            matching_fresh_locator_mutations(class, &[expected], &[actual]).is_some(),
            "the exact typed {table} mutation must preserve its named lane"
        );
    }
    for excluded in [
        "commits",
        "idempotency_locators",
        "provenance_locators",
        "audit_by_request_locators",
        "contract_migrations",
    ] {
        assert!(!fresh_locator_table_allowed(
            PreservingImmediateClass::Admission,
            excluded,
            b"extra"
        ));
    }
    assert!(!fresh_locator_table_allowed(
        PreservingImmediateClass::CapabilityBootstrap,
        "meta",
        META_APPLICATION_SEQUENCE.as_bytes()
    ));
}

// req: OUT-001, OUT-002, TXN-042, REC-004, PERF-019
#[test]
fn preserving_lane_expected_and_actual_sets_are_independent_exact_and_maximal() {
    fn mutation(
        table: &str,
        key: impl Into<Box<[u8]>>,
        kind: FreshLocatorMutationKind,
    ) -> FreshLocatorMutationPermit {
        FreshLocatorMutationPermit {
            table: table.into(),
            key: key.into(),
            kind,
        }
    }

    let one = mutation(
        "event_consumers",
        b"consumer".as_slice(),
        FreshLocatorMutationKind::Insert,
    );
    let same = mutation(
        "event_consumers",
        b"consumer".as_slice(),
        FreshLocatorMutationKind::Insert,
    );
    assert!(
        matching_fresh_locator_mutations(PreservingImmediateClass::Consumer, &[one], &[same],)
            .is_some()
    );

    for (expected, actual) in [
        (
            vec![mutation(
                "event_consumers",
                b"missing-actual".as_slice(),
                FreshLocatorMutationKind::Insert,
            )],
            vec![],
        ),
        (
            vec![],
            vec![mutation(
                "event_consumers",
                b"unregistered-actual".as_slice(),
                FreshLocatorMutationKind::Insert,
            )],
        ),
        (
            vec![mutation(
                "event_consumers",
                b"expected".as_slice(),
                FreshLocatorMutationKind::Insert,
            )],
            vec![mutation(
                "event_consumers",
                b"actual".as_slice(),
                FreshLocatorMutationKind::Insert,
            )],
        ),
        (
            vec![mutation(
                "event_consumers",
                b"same".as_slice(),
                FreshLocatorMutationKind::Insert,
            )],
            vec![
                mutation(
                    "event_consumers",
                    b"same".as_slice(),
                    FreshLocatorMutationKind::Insert,
                ),
                mutation(
                    "event_consumers",
                    b"extra".as_slice(),
                    FreshLocatorMutationKind::Insert,
                ),
            ],
        ),
        (
            vec![
                mutation(
                    "event_consumers",
                    b"duplicate".as_slice(),
                    FreshLocatorMutationKind::Insert,
                ),
                mutation(
                    "event_consumers",
                    b"duplicate".as_slice(),
                    FreshLocatorMutationKind::Insert,
                ),
            ],
            vec![mutation(
                "event_consumers",
                b"duplicate".as_slice(),
                FreshLocatorMutationKind::Insert,
            )],
        ),
        (
            vec![mutation(
                "event_consumers",
                b"duplicate".as_slice(),
                FreshLocatorMutationKind::Insert,
            )],
            vec![
                mutation(
                    "event_consumers",
                    b"duplicate".as_slice(),
                    FreshLocatorMutationKind::Insert,
                ),
                mutation(
                    "event_consumers",
                    b"duplicate".as_slice(),
                    FreshLocatorMutationKind::Insert,
                ),
            ],
        ),
        (
            vec![mutation(
                "commits",
                b"excluded".as_slice(),
                FreshLocatorMutationKind::Insert,
            )],
            vec![mutation(
                "commits",
                b"excluded".as_slice(),
                FreshLocatorMutationKind::Insert,
            )],
        ),
    ] {
        assert!(
            matching_fresh_locator_mutations(
                PreservingImmediateClass::Consumer,
                &expected,
                &actual,
            )
            .is_none()
        );
    }

    let mut maximum = Vec::with_capacity(MAX_FRESH_LOCATOR_PRESERVING_MUTATIONS);
    maximum.push(mutation(
        "event_consumers",
        b"consumer".as_slice(),
        FreshLocatorMutationKind::Delete,
    ));
    maximum.push(mutation(
        "event_consumers",
        b"consumer".as_slice(),
        FreshLocatorMutationKind::Insert,
    ));
    for ordinal in 0..riffdb_storage_api::MAX_CONSUMER_DELIVERY_RECORDS {
        let key = u64::try_from(ordinal)
            .expect("bounded ordinal")
            .to_be_bytes();
        maximum.push(mutation(
            "event_consumer_deliveries",
            key,
            FreshLocatorMutationKind::Delete,
        ));
        maximum.push(mutation(
            "event_consumer_deliveries",
            key,
            FreshLocatorMutationKind::Insert,
        ));
    }
    assert_eq!(maximum.len(), 8_194);
    assert_eq!(MAX_FRESH_LOCATOR_PRESERVING_MUTATIONS, 8_194);
    assert_eq!(
        matching_fresh_locator_mutations(PreservingImmediateClass::Consumer, &maximum, &maximum,)
            .expect("legal maximum replacement")
            .0,
        8_194
    );
}

// req: OUT-001, OUT-002, TXN-042
#[test]
fn preserving_expectations_close_before_the_first_actual_mutation() {
    let path = TestDatabasePath::new("fresh-locator-expected-before-actual");
    let mut store = RedbStore::open(&path.0).expect("open store");
    store
        .initialize_database(database_id(0x39))
        .expect("initialize store");
    let ports = RedbDormantPorts {
        shared: store.shared,
    }
    .into_operational_after_catalog_validation()
    .expect("activate ports");
    let access = ports.begin_write().expect("begin write");
    access
        .expect_fresh_locator_byte_insert(crate::layout::EVENT_CONSUMERS, b"expected")
        .expect("first expectation");
    access
        .close_fresh_locator_mutation_expectations()
        .expect("close expectations");
    access
        .record_actual_fresh_locator_byte_insert(crate::layout::EVENT_CONSUMERS, b"expected")
        .expect("first actual mutation");
    assert_eq!(
        access
            .expect_fresh_locator_byte_insert(crate::layout::EVENT_CONSUMERS, b"late")
            .expect_err("late expectation must fail closed")
            .kind(),
        StorageErrorKind::InvariantViolation
    );
    access.abort().expect("abort test write");
}

// req: OUT-001, OUT-002, TXN-042
#[test]
fn ordinary_command_bookkeeping_does_not_inherit_the_preserving_mutation_ceiling() {
    let path = TestDatabasePath::new("fresh-locator-command-does-not-use-preserving-bound");
    let mut store = RedbStore::open(&path.0).expect("open store");
    store
        .initialize_database(database_id(0x3a))
        .expect("initialize store");
    let ports = RedbDormantPorts {
        shared: store.shared,
    }
    .into_operational_after_catalog_validation()
    .expect("activate ports");
    let access = ports.begin_write().expect("begin ordinary command write");

    let legal_command_mutations = riffdb_storage_api::MAX_COMPOSITE_OVERLAY_TRANSITIONS
        .checked_add(3)
        .expect("bounded command mutation accounting");
    for ordinal in 0..legal_command_mutations {
        access
            .record_actual_fresh_locator_byte_insert(
                crate::layout::ENTITIES,
                &u64::try_from(ordinal)
                    .expect("bounded ordinal")
                    .to_be_bytes(),
            )
            .expect("ordinary command mutation is not preserving bookkeeping");
    }
    assert!(
        access.fresh_locator_actual_mutations.borrow().is_empty(),
        "ordinary direct/queued commands must retain no preserving mutation inventory"
    );
    access.abort().expect("abort bookkeeping proof");
}

// req: OUT-001, OUT-002, TXN-042
#[test]
fn preserving_bookkeeping_refuses_omitted_or_late_expectations_when_armed() {
    for (name, register_late) in [("omitted", false), ("late", true)] {
        let path = TestDatabasePath::new(&format!("fresh-locator-preserving-{name}"));
        let mut store = RedbStore::open(&path.0).expect("open store");
        store
            .initialize_database(database_id(if register_late { 0x3c } else { 0x3b }))
            .expect("initialize store");
        drop(store);
        let store = RedbStore::open(&path.0).expect("reopen fresh process");
        let ports = RedbDormantPorts {
            shared: store.shared,
        }
        .into_operational_after_catalog_validation()
        .expect("activate ports");
        let access = ports.begin_write().expect("begin preserving write");
        let stamp = access
            .fresh_locator_coverage_stamp()
            .expect("capture empty authority stamp");
        assert!(
            access
                .shared
                .fresh_locator_coverage
                .lock()
                .expect("coverage lock")
                .try_arm(
                    crate::fresh_locator_coverage::EmptyAuthorityProof::new(
                        true, true, true, true, true, true, true, true,
                    ),
                    stamp,
                )
        );
        access
            .record_actual_fresh_locator_byte_insert(crate::layout::IDEMPOTENCY_PENDING, b"key")
            .expect("pre-expectation mutation is not self-authorizing");
        if register_late {
            access
                .expect_fresh_locator_byte_insert(crate::layout::IDEMPOTENCY_PENDING, b"key")
                .expect("late expectation is recorded but has no matching actual inventory");
            access
                .close_fresh_locator_mutation_expectations()
                .expect("close late expectation");
        }
        let Err(error) = access.fresh_locator_preserving_permit(RedbTestOperation::Admission)
        else {
            panic!("omitted or late permit must fail closed");
        };
        assert_eq!(error.kind(), StorageErrorKind::InvariantViolation);
        access.abort().expect("abort preserving proof");
    }
}

fn empty_armed_ports(
    label: &str,
    seed: u8,
    controller: Option<RedbTestController>,
) -> (RedbOperationalPorts, TestDatabasePath) {
    let path = TestDatabasePath::new(label);
    let mut store = RedbStore::open(&path.0).expect("open store");
    store
        .initialize_database(database_id(seed))
        .expect("initialize store");
    drop(store);
    let store = match controller {
        Some(controller) => RedbStore::open_with_test_controller(&path.0, controller)
            .expect("reopen controlled fresh process"),
        None => RedbStore::open(&path.0).expect("reopen fresh process"),
    };
    let ports = RedbDormantPorts {
        shared: store.shared,
    }
    .into_operational_after_catalog_validation()
    .expect("activate ports");
    let access = ports.begin_write().expect("begin arm write");
    assert!(
        access
            .arm_fresh_locator_coverage_for_exact_empty_test()
            .expect("arm exact empty proof")
    );
    access.abort().expect("abort arm transaction");
    (ports, path)
}

// req: OUT-001, OUT-002, TXN-042, REC-004
#[test]
fn durable_root_capture_retries_the_engine_visible_before_identity_window() {
    let path = TestDatabasePath::new("durable-root-publication-seqlock");
    let mut store = RedbStore::open(&path.0).expect("open store");
    store
        .initialize_database(database_id(0x40))
        .expect("initialize store");
    drop(store);
    let (controller, schedule) = RedbTestController::pause_root_publication_after_engine_commit();
    let store = RedbStore::open_with_test_controller(&path.0, controller)
        .expect("reopen controlled process");
    let shared = Arc::clone(&store.shared);
    let predecessor = shared.current_read_root().expect("predecessor root");
    let predecessor_identity = predecessor.identity();
    drop(predecessor);

    let writer_shared = Arc::clone(&shared);
    let writer = std::thread::spawn(move || {
        let transaction = writer_shared.database.begin_write().expect("begin write");
        transaction
            .open_table(crate::layout::APPLICATION_INSTALLATION_CAMPAIGNS)
            .expect("installation table")
            .insert(b"visible-root".as_slice(), b"row".as_slice())
            .expect("insert marker");
        writer_shared
            .commit_durable(transaction)
            .expect("commit exact successor");
    });
    schedule.wait_until_commit_is_visible();

    let reader_shared = Arc::clone(&shared);
    let reader = std::thread::spawn(move || {
        let root = reader_shared
            .current_read_root()
            .expect("capture stable successor root");
        let present = root
            .open_table(crate::layout::APPLICATION_INSTALLATION_CAMPAIGNS)
            .expect("installation table")
            .get(b"visible-root".as_slice())
            .expect("read marker")
            .is_some();
        (root.identity(), present)
    });
    schedule.release_after_reader_retries();
    writer.join().expect("writer joins");
    let (successor_identity, present) = reader.join().expect("reader joins");
    assert!(
        present,
        "captured successor must include the visible commit"
    );
    assert_ne!(successor_identity, predecessor_identity);
    assert_eq!(
        successor_identity,
        shared.durable_root_publication.load(Ordering::Acquire)
    );
}

// req: OUT-001, OUT-002, TXN-042, REC-004
#[test]
fn postcommit_successor_stamp_failure_disables_fences_and_invalidates() {
    let (ports, _path) = empty_armed_ports(
        "fresh-locator-postcommit-stamp",
        0x3d,
        Some(RedbTestController::corrupt_fresh_locator_successor_stamp_once()),
    );
    let access = ports.begin_write().expect("begin preserving commit");
    access
        .expect_fresh_locator_byte_insert(crate::layout::EVENT_CONSUMERS, b"consumer")
        .expect("expected consumer mutation");
    access
        .close_fresh_locator_mutation_expectations()
        .expect("close exact permit");
    access
        .record_actual_fresh_locator_byte_insert(crate::layout::EVENT_CONSUMERS, b"consumer")
        .expect("record actual consumer mutation");
    access
        .transaction()
        .expect("transaction")
        .open_table(crate::layout::EVENT_CONSUMERS)
        .expect("consumer table")
        .insert(b"consumer".as_slice(), b"row".as_slice())
        .expect("stage consumer row");
    let before = ports.shared.durable_commit_epoch.load(Ordering::Acquire);
    let Err(error) = access.commit_for(RedbTestOperation::EventConsumerTransition) else {
        panic!("postcommit successor decoding must not escape the fail-closed handler");
    };
    assert_eq!(
        ports.shared.durable_commit_epoch.load(Ordering::Acquire),
        before + 1,
        "failure must be observed after the authoritative commit: {error:?}"
    );
    assert!(ports.shared.write_fenced.load(Ordering::Acquire));
    assert!(matches!(
        *ports.shared.transient_indexes.read().expect("indexes"),
        TransientIndexState::Invalid
    ));
}

// req: OUT-001, OUT-002, TXN-042, REC-004
#[test]
fn rebase_successor_read_and_coverage_lock_poison_fail_closed() {
    let (ports, _path) = empty_armed_ports("fresh-locator-rebase-stamp", 0x3e, None);
    let witness = ports
        .shared
        .begin_fresh_locator_rebase()
        .expect("begin rebase")
        .expect("armed rebase witness");
    let transaction = ports.shared.database.begin_write().expect("raw test write");
    transaction
        .open_table(crate::layout::META)
        .expect("meta table")
        .insert(META_APPLICATION_SEQUENCE, b"malformed".as_slice())
        .expect("corrupt successor allocator");
    ports
        .shared
        .commit_durable(transaction)
        .expect("publish malformed successor for handler proof");
    assert!(
        ports
            .shared
            .finish_fresh_locator_rebase(Some(witness))
            .is_err()
    );
    assert!(ports.shared.write_fenced.load(Ordering::Acquire));

    let (ports, _path) = empty_armed_ports("fresh-locator-rebase-lock-poison", 0x3f, None);
    let witness = ports
        .shared
        .begin_fresh_locator_rebase()
        .expect("begin rebase")
        .expect("armed rebase witness");
    let shared = Arc::clone(&ports.shared);
    let poison = std::thread::spawn(move || {
        let _guard = shared
            .fresh_locator_coverage
            .lock()
            .expect("coverage lock before poison");
        panic!("deterministic coverage-lock poison");
    });
    assert!(poison.join().is_err());
    assert!(
        ports
            .shared
            .abort_fresh_locator_rebase(Some(witness))
            .is_err()
    );
    assert!(ports.shared.write_fenced.load(Ordering::Acquire));
}

fn armed_shared_with_empty_journal_runtime(
    label: &str,
    seed: u8,
) -> (Arc<SharedRedb>, TestDatabasePath) {
    let (ports, path) = empty_armed_ports(label, seed, None);
    {
        let mut frontier = ports
            .shared
            .durable_read_frontier
            .write()
            .expect("frontier lock");
        if frontier.is_none() {
            *frontier = Some(
                ports
                    .shared
                    .capture_checkpoint_root()
                    .expect("capture exact journal predecessor"),
            );
        }
    }
    drop(
        ports
            .shared
            .journal_runtime()
            .expect("initialize empty journal runtime"),
    );
    (Arc::clone(&ports.shared), path)
}

fn install_completed_async_checkpoint(shared: &Arc<SharedRedb>) {
    let covered_view = shared
        .capture_or_initialize_composite_view()
        .expect("capture covered view");
    let batch = {
        let runtime = shared.journal_runtime.lock().expect("journal runtime lock");
        let runtime = runtime.as_ref().expect("journal runtime");
        JournalCheckpointBatch {
            database_id: runtime.database_id,
            checkpoint_sequence: runtime.published_sequence,
            checkpoint_administration_sequence: runtime.published_administration_sequence,
            checkpoint_hash: runtime.published_hash,
            last_sequence: runtime.last_sequence,
            last_administration_sequence: runtime.last_administration_sequence,
            last_hash: runtime.last_hash,
            transition_count: runtime.suffix_transitions,
            command_count: runtime.suffix_commands,
            audit_count: runtime.suffix_audits,
            encoded_bytes: runtime.suffix_bytes,
            frames: runtime.suffix_frames.clone(),
        }
    };
    *shared
        .journal_checkpoint
        .lock()
        .expect("checkpoint state lock") = Some(AsyncJournalCheckpoint {
        batch,
        covered_view,
        completion: None,
        result: Some(Ok(())),
    });
}

// req: OUT-001, OUT-002, TXN-042, REC-004
#[test]
fn sync_and_async_rebase_begin_poison_restore_owned_checkpoint_state_and_fence() {
    let (sync, _path) =
        armed_shared_with_empty_journal_runtime("fresh-locator-sync-begin-poison", 0x41);
    let poison_shared = Arc::clone(&sync);
    assert!(
        std::thread::spawn(move || {
            let _guard = poison_shared
                .fresh_locator_coverage
                .lock()
                .expect("coverage lock before poison");
            panic!("poison sync begin-rebase lock");
        })
        .join()
        .is_err()
    );
    assert_eq!(
        sync.checkpoint_published_journal_suffix_for_barrier()
            .expect_err("sync begin poison must fail closed")
            .kind(),
        StorageErrorKind::CommitStatusUnknown
    );
    assert!(sync.write_fenced.load(Ordering::Acquire));
    assert!(
        sync.journal_runtime
            .lock()
            .expect("restored journal runtime")
            .is_some(),
        "the prepublication sync runtime must be restored"
    );

    let (asynchronous, _path) =
        armed_shared_with_empty_journal_runtime("fresh-locator-async-begin-poison", 0x42);
    install_completed_async_checkpoint(&asynchronous);
    let poison_shared = Arc::clone(&asynchronous);
    assert!(
        std::thread::spawn(move || {
            let _guard = poison_shared
                .fresh_locator_coverage
                .lock()
                .expect("coverage lock before poison");
            panic!("poison async begin-rebase lock");
        })
        .join()
        .is_err()
    );
    assert_eq!(
        asynchronous
            .finish_async_checkpoint_if_quiescent()
            .expect_err("async begin poison must fail closed")
            .kind(),
        StorageErrorKind::CommitStatusUnknown
    );
    assert!(asynchronous.write_fenced.load(Ordering::Acquire));
    let checkpoint = asynchronous
        .journal_checkpoint
        .lock()
        .expect("checkpoint state remains owned");
    assert!(
        checkpoint
            .as_ref()
            .is_some_and(|checkpoint| checkpoint.result.as_ref().is_some_and(Result::is_ok)),
        "the completed async result must be restored before returning"
    );
}

// req: OUT-001, OUT-002, TXN-042, REC-004
#[test]
fn completed_async_checkpoint_lock_poison_is_uncertain_and_fenced() {
    for poison_checkpoint in [true, false] {
        let (shared, _path) = armed_shared_with_empty_journal_runtime(
            if poison_checkpoint {
                "fresh-locator-async-state-poison"
            } else {
                "fresh-locator-async-runtime-poison"
            },
            if poison_checkpoint { 0x43 } else { 0x44 },
        );
        install_completed_async_checkpoint(&shared);
        let poison_shared = Arc::clone(&shared);
        assert!(
            std::thread::spawn(move || {
                if poison_checkpoint {
                    let _guard = poison_shared
                        .journal_checkpoint
                        .lock()
                        .expect("checkpoint lock before poison");
                    panic!("poison completed checkpoint state");
                }
                let _guard = poison_shared
                    .journal_runtime
                    .lock()
                    .expect("runtime lock before poison");
                panic!("poison completed checkpoint runtime");
            })
            .join()
            .is_err()
        );
        assert_eq!(
            shared
                .poll_async_checkpoint_locked(false)
                .expect_err("completed checkpoint poison must be uncertain")
                .kind(),
            StorageErrorKind::CommitStatusUnknown
        );
        assert!(shared.write_fenced.load(Ordering::Acquire));
    }
}

// req: OUT-001, OUT-002, TXN-042, REC-004
#[test]
fn journal_reset_then_frontier_lock_failure_is_uncertain_and_fenced() {
    let (shared, _path) =
        armed_shared_with_empty_journal_runtime("fresh-locator-after-reset-poison", 0x45);
    let runtime = shared
        .take_published_journal_suffix_locked(true)
        .expect("take empty published suffix")
        .expect("journal runtime");
    let poison_shared = Arc::clone(&shared);
    assert!(
        std::thread::spawn(move || {
            let _guard = poison_shared
                .durable_read_frontier
                .write()
                .expect("frontier lock before poison");
            panic!("poison frontier before post-reset install");
        })
        .join()
        .is_err()
    );
    assert_eq!(
        shared
            .finish_journal_checkpoint(runtime)
            .expect_err("post-reset frontier poison is uncertain")
            .kind(),
        StorageErrorKind::CommitStatusUnknown
    );
    assert!(shared.write_fenced.load(Ordering::Acquire));
    assert!(
        crate::journal::scan_journal_with_media(
            shared.journal_media.as_ref(),
            &crate::journal::journal_path(&shared.path),
            database_id(0x45),
            |_| Ok(()),
        )
        .expect("scan reset journal")
        .is_some(),
        "the canonical reset completed before the frontier lock failure"
    );
}
