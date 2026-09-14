//! Actual offline incarnation stamp over an isolated, already-active V3 fixture.
// req: REP-003, REC-001, STO-012
use super::*;
use crate::changelog_v3_activation::{HISTORY, SOURCE_HOLDS};
use redb::ReadableTableMetadata;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionV3, ChangelogAttributionV3,
    ChangelogHistoryStateV3, ChangelogLineageV3, DatabaseInitializationPort, LeadershipEpochV1,
    ReplicationFollowerStateV3, proto_codec::*,
};
use riffdb_types::DualFrontier;
use std::collections::BTreeMap;

fn fixture(path: &Path) {
    let mut store = crate::RedbStore::open(path).unwrap();
    let id = DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0xd9; 10]).unwrap();
    store.initialize_database(id).unwrap();
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
