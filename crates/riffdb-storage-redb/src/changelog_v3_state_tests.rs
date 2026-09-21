//! Inventory transport proof over bounded opaque fixture rows. This does not
//! substitute for operation-owner semantic validation or bootstrap activation.
// req: REP-003, REC-001, PERF-007, STO-012

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use redb::{ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeStateCatalogV1, AuthoritativeStateStepV3,
    ChangelogHistoryStateV3, ChangelogLineageV3, LeadershipEpochV1, PublishedDurableSnapshot,
    ReplicationAuthorityClassV1 as Class,
};
use riffdb_types::{DatabaseId, DualFrontier};

use super::RedbPublishedSnapshot;
use crate::{checkpoint_root::CheckpointRoot, layout::META, store::RedbReadAccess};

type Rows = BTreeMap<(N, Vec<u8>), Vec<u8>>;

fn inventory_fixture() -> (
    crate::test_path::ScopedDirectory,
    crate::RedbStore,
    ChangelogHistoryStateV3,
    Rows,
) {
    let scope = crate::test_path::ScopedDirectory::new("v3-authority-inventory");
    let mut store = crate::RedbStore::open(scope.join("db.redb")).unwrap();
    let database =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x75; 10]).unwrap();
    store.initialize_legacy_fixture(database).unwrap();
    // Populate every pre-activation domain before the isolated fixture anchor.
    // Retain actual core metadata; arbitrary payloads prove transport custody,
    // not catalog semantics or a serving database's readiness.
    let transaction = store.shared.database.begin_write().unwrap();
    for namespace in N::ALL {
        if namespace.requires_v3_activation() {
            continue;
        }
        if let Some(key) = namespace.metadata_key() {
            if namespace.class() == Class::ReplicatedAuthoritative {
                let mut table = transaction.open_table(META).unwrap();
                let absent = table.get(key).unwrap().is_none();
                if absent {
                    table.insert(key, b"opaque-metadata".as_slice()).unwrap();
                }
            }
        } else {
            let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(namespace.table());
            let mut table = transaction.open_table(definition).unwrap();
            table
                .insert(b"a".as_slice(), b"original-a".as_slice())
                .unwrap();
            table
                .insert(b"z".as_slice(), b"original-z".as_slice())
                .unwrap();
        }
    }
    transaction.commit().unwrap();
    let history = crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(database, 1, LeadershipEpochV1::initial()).unwrap(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let pin = store.shared.database.begin_read().unwrap();
    let mut expected = Rows::new();
    for namespace in N::ALL {
        if namespace.class() != Class::ReplicatedAuthoritative {
            continue;
        }
        if let Some(key) = namespace.metadata_key() {
            let table = pin.open_table(META).unwrap();
            expected.insert(
                (namespace, key.as_bytes().to_vec()),
                table.get(key).unwrap().unwrap().value().to_vec(),
            );
        } else {
            let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(namespace.table());
            for row in pin.open_table(definition).unwrap().iter().unwrap() {
                let (key, value) = row.unwrap();
                expected.insert((namespace, key.value().to_vec()), value.value().to_vec());
            }
        }
    }
    drop(pin);
    (scope, store, history, expected)
}

fn published(store: &crate::RedbStore) -> RedbPublishedSnapshot {
    RedbPublishedSnapshot::new(RedbReadAccess::Durable(Arc::new(CheckpointRoot::new(
        store.shared.database.begin_read().unwrap(),
        1,
    ))))
}

#[test]
fn replication_inventory_classifies_every_storage_namespace() {
    let (_scope, store, history, expected) = inventory_fixture();
    let pin = store.shared.database.begin_read().unwrap();
    let tables: BTreeSet<_> = pin
        .list_tables()
        .unwrap()
        .map(|t| t.name().to_owned())
        .collect();
    assert_eq!(
        tables,
        N::ALL.into_iter().map(|n| n.table().to_owned()).collect()
    );
    let mut classified = BTreeSet::new();
    for table in &tables {
        if table == "meta" {
            for row in pin.open_table(META).unwrap().iter().unwrap() {
                let (key, _) = row.unwrap();
                let namespace = AuthoritativeStateCatalogV1
                    .lookup(table, key.value().as_bytes())
                    .unwrap();
                assert!(classified.insert(namespace));
            }
        } else {
            let matches: Vec<_> = N::ALL.into_iter().filter(|n| n.table() == table).collect();
            assert_eq!(matches.len(), 1);
            assert!(classified.insert(matches[0]));
        }
    }
    let authority: BTreeSet<_> = N::ALL
        .into_iter()
        .filter(|n| n.class() == Class::ReplicatedAuthoritative)
        .collect();
    assert!(authority.is_subset(&classified));
    assert_eq!(
        AuthoritativeStateCatalogV1.canonical_fixture(),
        include_str!("../../../fixtures/replication/authoritative-state-catalog-v1.txt")
    );

    let snapshot = published(&store);
    let mut cursor = snapshot.authoritative_state_v3().unwrap();
    assert_eq!(cursor.history(), history);
    let mut rows = Rows::new();
    let mut ended = BTreeSet::new();
    let namespaces: Vec<_> = authority.iter().copied().collect();
    let mut last_row = None;
    let mut steps = 0;
    while let Some(step) = cursor.next_item().unwrap() {
        steps += 1;
        assert!(
            steps <= 256,
            "the fixture's population and exact ends are bounded"
        );
        match step {
            AuthoritativeStateStepV3::Row(row) => {
                assert_eq!(Some(&row.namespace()), namespaces.get(ended.len()));
                let key = (row.namespace(), row.key().to_vec());
                assert!(!ended.contains(&row.namespace()));
                assert!(last_row.as_ref().is_none_or(|prior| prior < &key));
                assert!(rows.insert(key.clone(), row.value().to_vec()).is_none());
                last_row = Some(key);
            }
            AuthoritativeStateStepV3::EndNamespace(namespace) => {
                assert_eq!(Some(&namespace), namespaces.get(ended.len()));
                assert!(ended.insert(namespace));
            }
        }
    }
    assert_eq!(ended, authority);
    assert_eq!(rows, expected);
    assert!(cursor.next_item().unwrap().is_none());
    assert_eq!(cursor.history(), history);
}

#[test]
fn authoritative_inventory_refuses_unknown_or_missing_physical_domains_before_rows() {
    for fault in 0..3 {
        let (_scope, store, _, _) = inventory_fixture();
        let transaction = store.shared.database.begin_write().unwrap();
        match fault {
            0 => {
                let extra: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new("unknown-authority");
                transaction.open_table(extra).unwrap();
            }
            1 => {
                transaction
                    .open_table(META)
                    .unwrap()
                    .insert("unknown-metadata", b"private-value".as_slice())
                    .unwrap();
            }
            2 => {
                transaction
                    .delete_table(crate::layout::OUTBOX_STATUS)
                    .unwrap();
            }
            _ => unreachable!(),
        }
        transaction.commit().unwrap();
        let result = published(&store).authoritative_state_v3();
        assert!(
            matches!(result, Err(riffdb_storage_api::ChangelogCursorErrorV3::Storage(error))
            if error.kind() == riffdb_storage_api::StorageErrorKind::CorruptData)
        );
    }
}

#[test]
fn authoritative_inventory_row_failure_is_fused_and_redacted() {
    for oversized in [false, true] {
        let (_scope, store, _, _) = inventory_fixture();
        let transaction = store.shared.database.begin_write().unwrap();
        {
            let mut table = transaction
                .open_table(crate::layout::CONTRACT_BUNDLES)
                .unwrap();
            if oversized {
                table
                    .insert(
                        b"a".as_slice(),
                        vec![0x73; riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES].as_slice(),
                    )
                    .unwrap();
            } else {
                table
                    .insert(b"".as_slice(), b"private-value".as_slice())
                    .unwrap();
            }
        }
        transaction.commit().unwrap();
        let snapshot = published(&store);
        let mut cursor = snapshot.authoritative_state_v3().unwrap();
        let failure = cursor.next_item().unwrap_err();
        let expected = if oversized {
            riffdb_storage_api::StorageErrorKind::LimitExceeded
        } else {
            riffdb_storage_api::StorageErrorKind::CorruptData
        };
        assert!(
            matches!(&failure, riffdb_storage_api::ChangelogCursorErrorV3::Storage(error) if error.kind() == expected)
        );
        assert_eq!(cursor.next_item().unwrap_err(), failure);
        assert!(!format!("{failure:?} {failure}").contains("private-value"));
    }
}

#[test]
fn authoritative_inventory_never_falls_back_to_an_inactive_partial_catalog() {
    let scope = crate::test_path::ScopedDirectory::new("v3-inactive-state-cursor");
    let mut store = crate::RedbStore::open(scope.join("db.redb")).unwrap();
    let database =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x76; 10]).unwrap();
    store.initialize_legacy_fixture(database).unwrap();
    for _ in 0..2 {
        assert!(matches!(published(&store).authoritative_state_v3(),
            Err(riffdb_storage_api::ChangelogCursorErrorV3::Storage(error))
                if error.kind() == riffdb_storage_api::StorageErrorKind::IncompatibleFormat));
    }
    assert!(
        crate::changelog_v3_roots::read_checkpoint_roots(
            &store.shared.database.begin_read().unwrap()
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn authoritative_inventory_old_pin_survives_receipted_overwrite_and_delete() {
    let (_scope, store, history, expected) = inventory_fixture();
    let old = published(&store);
    let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
        &store.shared.database,
        crate::store::RedbCommitProfile::Hardened,
        riffdb_storage_api::ChangelogAttributionV3::OutboxTransition,
    )
    .unwrap();
    {
        let mut table = write.open_table(crate::layout::OUTBOX_STATUS).unwrap();
        table
            .insert(b"a".as_slice(), b"replacement".as_slice())
            .unwrap();
        table.remove(b"z".as_slice()).unwrap();
    }
    write.finish().unwrap().commit_for_test().unwrap();
    let mut cursor = old.authoritative_state_v3().unwrap();
    assert_eq!(cursor.history(), history);
    let mut observed = Rows::new();
    while let Some(step) = cursor.next_item().unwrap() {
        if let AuthoritativeStateStepV3::Row(row) = step {
            observed.insert((row.namespace(), row.key().to_vec()), row.value().to_vec());
        }
    }
    assert_eq!(observed, expected);
    let current = published(&store);
    let mut tail = current
        .changelog_receipts_v3(history.lineage(), history.tail())
        .unwrap();
    let receipt = tail.next_receipt().unwrap().unwrap();
    assert_eq!(receipt.mutations().len(), 2);
    assert!(tail.next_receipt().unwrap().is_none());
    for mutation in receipt.mutations() {
        let key = (mutation.namespace(), mutation.key().to_vec());
        assert!(mutation.matches_prior(observed.get(&key).map(Vec::as_slice)));
        if let Some(value) = mutation.value() {
            observed.insert(key, value.to_vec());
        } else {
            assert!(observed.remove(&key).is_some());
        }
    }
    let mut cursor = current.authoritative_state_v3().unwrap();
    let mut current_rows = Rows::new();
    while let Some(step) = cursor.next_item().unwrap() {
        if let AuthoritativeStateStepV3::Row(row) = step {
            current_rows.insert((row.namespace(), row.key().to_vec()), row.value().to_vec());
        }
    }
    assert_eq!(observed, current_rows);
}
