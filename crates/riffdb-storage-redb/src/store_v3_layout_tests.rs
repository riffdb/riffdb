//! Real dormant storage open over isolated V3 activation; not full readiness.
// req: REP-003, REC-001, STO-012
use super::*;
use riffdb_storage_api::{ChangelogLineageV3, LeadershipEpochV1};

fn initialized(path: &Path) -> (RedbStore, DatabaseId) {
    let mut store = RedbStore::open(path).unwrap();
    let id = DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x74; 10]).unwrap();
    store.initialize_legacy_fixture(id).unwrap();
    crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(id, 1, LeadershipEpochV1::initial()).unwrap(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    (store, id)
}

#[test]
fn dormant_store_reopens_complete_v3_without_legacy_migration_or_new_history() {
    let scope = crate::test_path::ScopedDirectory::new("v3-real-open");
    let path = scope.join("db.redb");
    let (store, id) = initialized(&path);
    drop(store);
    for _ in 0..2 {
        let mut reopened = RedbStore::open(&path)
            .expect("complete V3 must reopen through the actual dormant owner");
        assert_eq!(
            reopened.probe_database_identity().unwrap(),
            DatabaseIdentityProbe::Existing(id)
        );
        let epoch = reopened.shared.durable_commit_epoch();
        assert_eq!(
            epoch, 0,
            "no legacy migration may rewrite an active V3 database"
        );
        reopened.initialize_database(id).unwrap();
        assert_eq!(reopened.shared.durable_commit_epoch(), epoch);
        let read = reopened.shared.database.begin_read().unwrap();
        let history = crate::changelog_v3_roots::validate_retained_history(&read)
            .unwrap()
            .unwrap();
        assert_eq!(history.tail().sequence().get(), 1);
        assert_eq!(history.tail().frontier(), DualFrontier::INITIAL);
    }
}

#[test]
fn partial_or_downgraded_v3_layout_refuses_before_legacy_repair_or_journal_creation() {
    use riffdb_storage_api::AuthoritativeNamespaceV1 as N;
    for arm in 0..9 {
        let scope = crate::test_path::ScopedDirectory::new("v3-layout-refusal");
        let path = scope.join("db.redb");
        let (store, _) = initialized(&path);
        let transaction = store.shared.database.begin_write().unwrap();
        match arm {
            0 => {
                transaction
                    .delete_table(crate::changelog_v3_activation::HISTORY)
                    .unwrap();
            }
            1 => {
                transaction
                    .delete_table(crate::changelog_v3_activation::SOURCE_HOLDS)
                    .unwrap();
            }
            2 => {
                transaction.delete_table(crate::layout::ENTITIES).unwrap();
            }
            3 => {
                transaction
                    .delete_table(crate::layout::IDEMPOTENCY_LOCATORS)
                    .unwrap();
            }
            4 => {
                transaction
                    .open_table(META)
                    .unwrap()
                    .remove(N::LeadershipEpoch.metadata_key().unwrap())
                    .unwrap();
            }
            5 => {
                transaction
                    .open_table(META)
                    .unwrap()
                    .insert(
                        META_RECORD_REGISTRY,
                        riffdb_storage_api::proto_codec::encode_record_registry_v2(
                            crate::changelog_v3_activation::PRE_V3_REGISTRY,
                        )
                        .unwrap()
                        .as_bytes(),
                    )
                    .unwrap();
            }
            6 => {
                transaction
                    .open_table(META)
                    .unwrap()
                    .insert(
                        META_FORMAT_VERSION,
                        riffdb_storage_api::proto_codec::encode_storage_format_version_v1(
                            StorageFormatVersion::V1,
                        )
                        .unwrap()
                        .as_bytes(),
                    )
                    .unwrap();
            }
            7 => {
                let definition: redb::TableDefinition<&[u8], &[u8]> =
                    redb::TableDefinition::new("unknown_v3_table");
                transaction.open_table(definition).unwrap();
            }
            _ => {
                transaction
                    .open_table(META)
                    .unwrap()
                    .insert(
                        "unknown_v3_meta",
                        riffdb_storage_api::proto_codec::encode_database_identity_v1(
                            DatabaseId::from_unix_milliseconds_and_random(
                                1_700_000_000_000,
                                [0x74; 10],
                            )
                            .unwrap(),
                        )
                        .unwrap()
                        .as_bytes(),
                    )
                    .unwrap();
            }
        }
        transaction.commit().unwrap();
        let before = physical_fixture_rows(&store.shared.database);
        assert!(!crate::journal::journal_path(&path).exists());
        drop(store);
        for _ in 0..2 {
            assert!(
                RedbStore::open(&path).is_err(),
                "accepted invalid arm {arm}"
            );
            let database = Database::open(&path).unwrap();
            assert_eq!(
                physical_fixture_rows(&database),
                before,
                "rewrote invalid arm {arm}"
            );
            assert!(!crate::journal::journal_path(&path).exists());
        }
    }
}

type FixtureRows = std::collections::BTreeMap<String, Vec<(Vec<u8>, Vec<u8>)>>;

#[test]
fn current_registry_initialization_never_advertises_missing_v3_roots() {
    let scope = crate::test_path::ScopedDirectory::new("v3-initialization-registry");
    let path = scope.join("db.redb");
    let mut store = RedbStore::open(&path).unwrap();
    let id = DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x75; 10]).unwrap();
    store.initialize_database(id).unwrap();
    let read = store.shared.database.begin_read().unwrap();
    let roots = crate::changelog_v3_roots::read_checkpoint_roots(&read).expect(
        "fresh initialization must publish either exact inactive registry or complete V3 roots",
    );
    let meta = read.open_table(META).unwrap();
    let registry = meta.get(META_RECORD_REGISTRY).unwrap().unwrap();
    let registry = *riffdb_storage_api::proto_codec::decode_record_registry_v2(registry.value())
        .unwrap()
        .value();
    if let Some(history) = roots {
        assert_eq!(
            registry,
            riffdb_storage_api::proto_codec::current_record_registry_digest()
        );
        assert_eq!(history.lineage().database_id(), id);
        assert_eq!(history.tail().frontier(), DualFrontier::INITIAL);
        assert_eq!(history.tail().sequence().get(), 1);
    } else {
        assert_eq!(registry, crate::changelog_v3_activation::PRE_V3_REGISTRY);
    }
}

#[test]
fn current_registry_root_erasure_is_not_legacy_inactivity() {
    use riffdb_storage_api::AuthoritativeNamespaceV1 as N;
    let scope = crate::test_path::ScopedDirectory::new("v3-erased-roots");
    let path = scope.join("db.redb");
    let (store, _) = initialized(&path);
    let write = store.shared.database.begin_write().unwrap();
    {
        let mut meta = write.open_table(META).unwrap();
        for namespace in N::ALL.into_iter().filter(|n| n.requires_v3_activation()) {
            if let Some(key) = namespace.metadata_key() {
                meta.remove(key).unwrap();
            }
        }
    }
    assert!(
        write
            .delete_table(crate::changelog_v3_activation::HISTORY)
            .unwrap()
    );
    assert!(
        write
            .delete_table(crate::changelog_v3_activation::SOURCE_HOLDS)
            .unwrap()
    );
    write.commit().unwrap();
    let before = physical_fixture_rows(&store.shared.database);
    assert!(!crate::journal::journal_path(&path).exists());
    drop(store);
    for _ in 0..2 {
        assert!(
            RedbStore::open(&path).is_err(),
            "current registry with erased roots must refuse, not reopen as inactive legacy"
        );
        let database = Database::open(&path).unwrap();
        assert_eq!(physical_fixture_rows(&database), before);
        assert!(!crate::journal::journal_path(&path).exists());
    }
}

fn physical_fixture_rows(database: &Database) -> FixtureRows {
    let read = database.begin_read().unwrap();
    let mut output = std::collections::BTreeMap::new();
    for handle in read.list_tables().unwrap() {
        let name = handle.name().to_owned();
        let rows = if name == META.name() {
            read.open_table(META)
                .unwrap()
                .iter()
                .unwrap()
                .map(|row| {
                    let (key, value) = row.unwrap();
                    (key.value().as_bytes().to_vec(), value.value().to_vec())
                })
                .collect()
        } else {
            let definition: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new(&name);
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
        output.insert(name, rows);
    }
    output
}
