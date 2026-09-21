//! Actual offline incarnation stamp over an isolated, already-active V3 fixture.
// req: REP-003, REC-001, STO-012
use super::*;
use crate::changelog_v3_activation::{HISTORY, SOURCE_HOLDS};
use redb::ReadableTableMetadata;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionV3, ChangelogAttributionV3,
    ChangelogHistoryStateV3, ChangelogLineageV3, LeadershipEpochV1, ReplicationFollowerStateV3,
    proto_codec::*,
};
use riffdb_types::DualFrontier;
use std::collections::BTreeMap;

fn fixture(path: &Path) {
    let mut store = crate::RedbStore::open(path).unwrap();
    let id = DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0xd9; 10]).unwrap();
    store.initialize_legacy_fixture(id).unwrap();
    crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(id, 1, LeadershipEpochV1::initial()).unwrap(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    drop(store);
    crate::RedbOfflineRetention::bind(path)
        .add_hold("restore-fixture", 0, "retained")
        .unwrap();
    let database = Database::open(path).unwrap();
    let history = read_history(&database);
    let write = database.begin_write().unwrap();
    {
        let mut meta = write.open_table(META).unwrap();
        let follower = ReplicationFollowerStateV3::attached(
            history.lineage(),
            history.tail(),
            Some(history.tail()),
        )
        .unwrap();
        meta.insert(
            N::ReplicationFollowerState.metadata_key().unwrap(),
            encode_replication_follower_state_v3(follower)
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
    }
    write.commit().unwrap();
    let write = database.begin_write().unwrap();
    {
        use riffdb_storage_api::{
            ReplicationSourceHoldIdV1 as Id, ReplicationSourceHoldKindV1 as Kind,
            ReplicationSourceHoldV1 as Hold,
        };
        let mut holds = write.open_table(SOURCE_HOLDS).unwrap();
        for kind in [
            Kind::FollowerAcknowledgement,
            Kind::ArchiveAcknowledgement,
            Kind::Bootstrap,
        ] {
            let hold = Hold::new(
                Id::new([0xb9; 16]).unwrap(),
                kind,
                history.lineage(),
                history.tail(),
            );
            holds
                .insert(
                    hold.storage_key().as_slice(),
                    encode_replication_source_hold_v1(hold).unwrap().as_bytes(),
                )
                .unwrap();
        }
        let dirty = crate::clean_close::CleanCloseLifecycle::dirty(id, 1, 1)
            .unwrap()
            .encode()
            .unwrap();
        write
            .open_table(META)
            .unwrap()
            .insert(
                N::CleanCloseLifecycle.metadata_key().unwrap(),
                dirty.as_slice(),
            )
            .unwrap();
    }
    write.commit().unwrap();
}

fn read_history(database: &Database) -> ChangelogHistoryStateV3 {
    crate::changelog_v3_roots::validate_retained_history(&database.begin_read().unwrap())
        .unwrap()
        .unwrap()
}

fn metadata(database: &Database) -> BTreeMap<String, Vec<u8>> {
    database
        .begin_read()
        .unwrap()
        .open_table(META)
        .unwrap()
        .iter()
        .unwrap()
        .map(|row| {
            let (key, value) = row.unwrap();
            (key.value().to_owned(), value.value().to_vec())
        })
        .collect()
}

#[test]
fn actual_restore_incarnation_stamp_reanchors_v3_and_detaches_local_progress_atomically() {
    let scope = crate::test_path::ScopedDirectory::new("v3-restore-stamp");
    let path = scope.join("db.redb");
    fixture(&path);
    let database = Database::open(&path).unwrap();
    let before = metadata(&database);
    let old = read_history(&database);
    assert_eq!(old.tail().sequence().get(), 2);
    drop(database);
    stamp_history_incarnation(&path, 2).unwrap();
    let database = Database::open(&path).unwrap();
    let history = read_history(&database);
    assert_eq!(history.lineage().history_incarnation(), 2);
    assert_eq!(history.lineage().database_id(), old.lineage().database_id());
    assert_eq!(
        history.lineage().leadership_epoch(),
        old.lineage().leadership_epoch()
    );
    assert_eq!(history.anchor(), history.tail());
    assert_eq!(history.minimum_resume(), history.tail());
    assert_eq!(history.tail().sequence().get(), 1);
    assert_eq!(history.tail().frontier(), old.tail().frontier());
    let after = metadata(&database);
    assert!(!after.contains_key(N::CleanCloseLifecycle.metadata_key().unwrap()));
    for namespace in N::ALL.into_iter().filter(|n| {
        !n.requires_v3_activation() && *n != N::HistoryIncarnation && *n != N::CleanCloseLifecycle
    }) {
        if let Some(key) = namespace.metadata_key() {
            assert_eq!(
                after.get(key),
                before.get(key),
                "unchanged namespace {namespace:?}"
            );
        }
    }
    let read = database.begin_read().unwrap();
    let table = read.open_table(HISTORY).unwrap();
    assert_eq!(table.len().unwrap(), 1);
    let receipt = AuthoritativeTransactionV3::decode(
        table
            .get(1u64.to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value(),
    )
    .unwrap();
    assert_eq!(receipt.attribution(), ChangelogAttributionV3::RestoreAnchor);
    assert!(receipt.mutations().is_empty());
    assert_eq!(receipt.binding().predecessor, None);
    assert_eq!(receipt.binding().prior_history_hash, [0; 32]);
    assert!(read.open_table(SOURCE_HOLDS).unwrap().is_empty().unwrap());
    assert_eq!(
        *decode_replication_follower_state_v3(
            &after[N::ReplicationFollowerState.metadata_key().unwrap()]
        )
        .unwrap()
        .value(),
        ReplicationFollowerStateV3::detached()
    );
    drop(table);
    drop(read);
    drop(database);
    stamp_history_incarnation(&path, 2).unwrap();
    assert_eq!(metadata(&Database::open(&path).unwrap()), after);
    drop(crate::RedbStore::open(&path).expect("reanchored V3 reopens without repair"));
}

#[derive(Debug, Eq, PartialEq)]
struct Image {
    metadata: BTreeMap<String, Vec<u8>>,
    history: BTreeMap<Vec<u8>, Vec<u8>>,
    holds: BTreeMap<Vec<u8>, Vec<u8>>,
    authority: BTreeMap<(u16, Vec<u8>), Vec<u8>>,
}

fn image(database: &Database) -> Image {
    let read = database.begin_read().unwrap();
    let mut authority = BTreeMap::new();
    for namespace in N::ALL {
        if namespace.class()
            != riffdb_storage_api::ReplicationAuthorityClassV1::ReplicatedAuthoritative
            || matches!(namespace, N::HistoryIncarnation | N::RetentionWatermark)
        {
            continue;
        }
        if let Some(key) = namespace.metadata_key() {
            if let Some(value) = read.open_table(META).unwrap().get(key).unwrap() {
                authority.insert(
                    (namespace.tag(), key.as_bytes().to_vec()),
                    value.value().to_vec(),
                );
            }
        } else {
            let table: redb::ReadOnlyTable<&[u8], &[u8]> = read
                .open_table(redb::TableDefinition::new(namespace.table()))
                .unwrap();
            for row in table.iter().unwrap() {
                let (key, value) = row.unwrap();
                authority.insert(
                    (namespace.tag(), key.value().to_vec()),
                    value.value().to_vec(),
                );
            }
        }
    }
    let rows = |definition: redb::TableDefinition<&[u8], &[u8]>| {
        read.open_table(definition)
            .unwrap()
            .iter()
            .unwrap()
            .map(|row| {
                let (key, value) = row.unwrap();
                (key.value().to_vec(), value.value().to_vec())
            })
            .collect()
    };
    Image {
        metadata: metadata(database),
        history: rows(HISTORY),
        holds: rows(SOURCE_HOLDS),
        authority,
    }
}

#[test]
fn actual_restore_refuses_corrupt_v3_before_mutation_including_equal_incarnation_retry() {
    for arm in 0..5 {
        let scope = crate::test_path::ScopedDirectory::new("v3-restore-corruption");
        let path = scope.join("db.redb");
        fixture(&path);
        let database = Database::open(&path).unwrap();
        let write = database.begin_write().unwrap();
        match arm {
            0 => {
                write
                    .open_table(HISTORY)
                    .unwrap()
                    .remove(1u64.to_be_bytes().as_slice())
                    .unwrap();
            }
            1 => {
                write
                    .open_table(SOURCE_HOLDS)
                    .unwrap()
                    .insert(b"unknown".as_slice(), b"unknown".as_slice())
                    .unwrap();
            }
            2 => {
                write
                    .open_table(META)
                    .unwrap()
                    .remove(N::LeadershipEpoch.metadata_key().unwrap())
                    .unwrap();
            }
            3 => {
                write
                    .open_table(META)
                    .unwrap()
                    .insert(
                        N::ReplicationFollowerState.metadata_key().unwrap(),
                        b"unknown".as_slice(),
                    )
                    .unwrap();
            }
            _ => {
                write
                    .open_table(META)
                    .unwrap()
                    .insert(
                        N::CleanCloseLifecycle.metadata_key().unwrap(),
                        b"unknown".as_slice(),
                    )
                    .unwrap();
            }
        }
        write.commit().unwrap();
        let before = image(&database);
        drop(database);
        for incarnation in [1, 2] {
            assert_eq!(
                stamp_history_incarnation(&path, incarnation)
                    .unwrap_err()
                    .kind(),
                StorageErrorKind::CorruptData,
                "arm {arm}"
            );
            assert_eq!(image(&Database::open(&path).unwrap()), before);
        }
    }
}

#[test]
fn actual_restore_refuses_backwards_incarnation_without_changing_the_new_anchor() {
    let scope = crate::test_path::ScopedDirectory::new("v3-restore-backwards");
    let path = scope.join("db.redb");
    fixture(&path);
    stamp_history_incarnation(&path, 2).unwrap();
    let before = image(&Database::open(&path).unwrap());
    assert_eq!(
        stamp_history_incarnation(&path, 1).unwrap_err().kind(),
        StorageErrorKind::InvariantViolation
    );
    assert_eq!(image(&Database::open(&path).unwrap()), before);
}

#[test]
fn restore_stamp_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_WP772_RESTORE_DATABASE") else {
        return;
    };
    stamp_history_incarnation(path, 2).unwrap();
    panic!("requested restore crash edge did not fire");
}

#[test]
fn actual_restore_process_crashes_keep_whole_lineages_and_idempotent_retries() {
    for edge in [
        "preflight",
        "incarnation",
        "local-reset",
        "anchor",
        "committed",
    ] {
        let scope = crate::test_path::ScopedDirectory::new("v3-restore-crash");
        let path = scope.join("db.redb");
        fixture(&path);
        let before = image(&Database::open(&path).unwrap());
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "backup::v3_tests::restore_stamp_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_WP772_RESTORE_DATABASE", &path)
            .env("RIFFDB_WP772_RESTORE_CRASH", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "edge {edge}");
        for _ in 0..2 {
            let database = Database::open(&path).unwrap();
            let recovered = read_history(&database);
            if edge == "committed" {
                assert_eq!(recovered.lineage().history_incarnation(), 2);
                assert_eq!(recovered.tail().sequence().get(), 1);
                let after = image(&database);
                assert_eq!(after.authority, before.authority);
                assert!(after.holds.is_empty());
                assert_eq!(after.history.len(), 1);
                assert_eq!(
                    *decode_replication_follower_state_v3(
                        &after.metadata[N::ReplicationFollowerState.metadata_key().unwrap()]
                    )
                    .unwrap()
                    .value(),
                    ReplicationFollowerStateV3::detached()
                );
            } else {
                assert_eq!(recovered.lineage().history_incarnation(), 1);
                assert_eq!(image(&database), before);
            }
        }
        stamp_history_incarnation(&path, 2).unwrap();
        let after = image(&Database::open(&path).unwrap());
        assert_eq!(after.authority, before.authority);
        for _ in 0..2 {
            stamp_history_incarnation(&path, 2).unwrap();
            assert_eq!(image(&Database::open(&path).unwrap()), after);
            drop(crate::RedbStore::open(&path).unwrap());
        }
    }
}

#[test]
fn actual_v3_restore_rebinds_unpruned_watermark_without_inventing_a_chain_root() {
    let scope = crate::test_path::ScopedDirectory::new("v3-restore-watermark");
    let path = scope.join("db.redb");
    fixture(&path);
    let database = Database::open(&path).unwrap();
    let old = riffdb_storage_api::StoredRetentionWatermarkV1::new(0, 1, None).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(
            META_RETENTION_WATERMARK,
            encode_retention_watermark_v1(&old).unwrap().as_bytes(),
        )
        .unwrap();
    write.commit().unwrap();
    let before = image(&database);
    drop(database);
    stamp_history_incarnation(&path, 2).unwrap();
    let database = Database::open(&path).unwrap();
    let after = image(&database);
    assert_eq!(after.authority, before.authority);
    let rebound = decode_retention_watermark_v1(&after.metadata[META_RETENTION_WATERMARK])
        .unwrap()
        .into_parts()
        .0;
    assert_eq!(rebound.history_incarnation(), 2);
    assert_eq!(rebound.watermark_sequence(), old.watermark_sequence());
    assert_eq!(
        rebound.chain_root_registry_digest(),
        old.chain_root_registry_digest()
    );
    assert_eq!(read_history(&database).tail().sequence().get(), 1);
}

#[test]
fn actual_watermark_stamp_receipts_the_exact_transition_and_retry_is_read_only() {
    let scope = crate::test_path::ScopedDirectory::new("v3-watermark-receipt");
    let path = scope.join("db.redb");
    fixture(&path);
    let database = Database::open(&path).unwrap();
    let old_history = read_history(&database);
    let before = image(&database);
    assert!(!before.metadata.contains_key(META_RETENTION_WATERMARK));
    drop(database);

    stamp_retention_watermark(&path, 0).unwrap();
    let database = Database::open(&path).unwrap();
    let history = read_history(&database);
    assert_eq!(
        history.tail().sequence().get(),
        old_history.tail().sequence().get() + 1
    );
    assert_eq!(history.lineage(), old_history.lineage());
    assert_eq!(history.tail().frontier(), old_history.tail().frontier());
    let after = image(&database);
    assert_eq!(after.authority, before.authority);
    assert_eq!(after.holds, before.holds);
    for (key, value) in &before.history {
        assert_eq!(after.history.get(key), Some(value));
    }
    let receipt = AuthoritativeTransactionV3::decode(
        &after.history[history.tail().sequence().get().to_be_bytes().as_slice()],
    )
    .unwrap();
    assert_eq!(
        receipt.attribution(),
        ChangelogAttributionV3::RetentionPrune
    );
    assert_eq!(
        receipt.binding().predecessor,
        Some(old_history.tail().sequence())
    );
    assert_eq!(
        receipt.mutations(),
        &[riffdb_storage_api::AuthoritativeMutationV3::put(
            N::RetentionWatermark,
            META_RETENTION_WATERMARK.as_bytes(),
            None,
            &after.metadata[META_RETENTION_WATERMARK],
        )
        .unwrap(),]
    );
    drop(database);
    for _ in 0..2 {
        stamp_retention_watermark(&path, 0).unwrap();
        assert_eq!(image(&Database::open(&path).unwrap()), after);
    }
}

#[test]
fn actual_watermark_stamp_refuses_corrupt_v3_even_on_equal_watermark_retry() {
    let scope = crate::test_path::ScopedDirectory::new("v3-watermark-corruption");
    let path = scope.join("db.redb");
    fixture(&path);
    stamp_retention_watermark(&path, 0).unwrap();
    let database = Database::open(&path).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(HISTORY)
        .unwrap()
        .remove(1u64.to_be_bytes().as_slice())
        .unwrap();
    write.commit().unwrap();
    let before = image(&database);
    drop(database);
    assert_eq!(
        stamp_retention_watermark(&path, 0).unwrap_err().kind(),
        StorageErrorKind::CorruptData
    );
    assert_eq!(image(&Database::open(&path).unwrap()), before);
}

#[test]
fn actual_watermark_stamp_refuses_missing_root_binding_and_unrooted_nonzero_stamp() {
    for arm in 0..4 {
        let scope = crate::test_path::ScopedDirectory::new("v3-watermark-refusal");
        let path = scope.join("db.redb");
        fixture(&path);
        if arm != 0 {
            stamp_retention_watermark(&path, 0).unwrap();
        }
        let database = Database::open(&path).unwrap();
        let write = database.begin_write().unwrap();
        match arm {
            0 => {}
            1 => {
                write
                    .open_table(META)
                    .unwrap()
                    .remove(N::LeadershipEpoch.metadata_key().unwrap())
                    .unwrap();
            }
            2 => {
                write
                    .open_table(SOURCE_HOLDS)
                    .unwrap()
                    .insert(b"unknown".as_slice(), b"unknown".as_slice())
                    .unwrap();
            }
            _ => {
                let stale =
                    riffdb_storage_api::StoredRetentionWatermarkV1::new(0, 2, None).unwrap();
                write
                    .open_table(META)
                    .unwrap()
                    .insert(
                        META_RETENTION_WATERMARK,
                        encode_retention_watermark_v1(&stale).unwrap().as_bytes(),
                    )
                    .unwrap();
            }
        }
        write.commit().unwrap();
        let before = image(&database);
        drop(database);
        let target = u64::from(arm == 0);
        assert_eq!(
            stamp_retention_watermark(&path, target).unwrap_err().kind(),
            StorageErrorKind::CorruptData
        );
        assert_eq!(image(&Database::open(&path).unwrap()), before);
    }
}

#[test]
fn watermark_stamp_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_WP772_WATERMARK_DATABASE") else {
        return;
    };
    stamp_retention_watermark(path, 0).unwrap();
    panic!("requested watermark crash edge did not fire");
}

#[test]
fn actual_watermark_process_crashes_preserve_whole_receipts_and_idempotent_retries() {
    for edge in [
        "watermark-preflight",
        "watermark-mutation",
        "watermark-receipt",
        "watermark-committed",
    ] {
        let scope = crate::test_path::ScopedDirectory::new("v3-watermark-crash");
        let path = scope.join("db.redb");
        fixture(&path);
        let database = Database::open(&path).unwrap();
        let old_history = read_history(&database);
        let before = image(&database);
        drop(database);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "backup::v3_tests::watermark_stamp_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_WP772_WATERMARK_DATABASE", &path)
            .env("RIFFDB_WP772_RESTORE_CRASH", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "edge {edge}");
        for _ in 0..2 {
            let database = Database::open(&path).unwrap();
            let recovered = read_history(&database);
            let after = image(&database);
            assert_eq!(after.authority, before.authority);
            assert_eq!(after.holds, before.holds);
            if edge == "watermark-committed" {
                assert_eq!(
                    recovered.tail().sequence().get(),
                    old_history.tail().sequence().get() + 1
                );
                assert!(after.metadata.contains_key(META_RETENTION_WATERMARK));
                assert_eq!(after.history.len(), before.history.len() + 1);
            } else {
                assert_eq!(after, before);
            }
        }
        stamp_retention_watermark(&path, 0).unwrap();
        let database = Database::open(&path).unwrap();
        let final_history = read_history(&database);
        assert_eq!(
            final_history.tail().sequence().get(),
            old_history.tail().sequence().get() + 1
        );
        let after = image(&database);
        let receipt = AuthoritativeTransactionV3::decode(
            &after.history[final_history
                .tail()
                .sequence()
                .get()
                .to_be_bytes()
                .as_slice()],
        )
        .unwrap();
        assert_eq!(
            receipt.attribution(),
            ChangelogAttributionV3::RetentionPrune
        );
        assert_eq!(receipt.mutations().len(), 1);
        assert!(receipt.mutations()[0].matches_prior(None));
        assert_eq!(
            receipt.mutations()[0].value(),
            Some(after.metadata[META_RETENTION_WATERMARK].as_slice())
        );
        for (key, value) in &before.history {
            assert_eq!(after.history.get(key), Some(value));
        }
        drop(database);
        for _ in 0..2 {
            stamp_retention_watermark(&path, 0).unwrap();
            assert_eq!(image(&Database::open(&path).unwrap()), after);
            // This restore fixture deliberately retains attached follower
            // metadata. ADR-0186 amendment 2 forbids interpreting it as a
            // source-mode database; refusal must also preserve the crash image.
            assert_eq!(
                crate::RedbStore::open(&path).err().unwrap().kind(),
                StorageErrorKind::InvariantViolation
            );
            assert_eq!(image(&Database::open(&path).unwrap()), after);
        }
    }
}
