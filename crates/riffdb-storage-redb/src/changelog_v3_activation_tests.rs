//! Isolated installation-transaction tests, not complete startup/readiness proof.
// req: REP-003, REC-001, STO-012

use super::*;
use redb::{Database, ReadableDatabase, ReadableTableMetadata};
use riffdb_storage_api::LeadershipEpochV1;
use riffdb_types::DatabaseId;
use std::{collections::BTreeMap, path::Path};

fn lineage() -> ChangelogLineageV3 {
    ChangelogLineageV3::new(
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10]).unwrap(),
        1,
        LeadershipEpochV1::initial(),
    )
    .unwrap()
}

fn fixture(path: &Path, registry: SchemaHash) -> Database {
    let database = Database::create(path).unwrap();
    let transaction = database.begin_write().unwrap();
    create_all_tables(&transaction).unwrap();
    {
        let mut meta = transaction.open_table(META).unwrap();
        for (key, bytes) in [
            (
                META_DATABASE_ID,
                encode_database_identity_v1(lineage().database_id()).unwrap(),
            ),
            (
                META_HISTORY_INCARNATION,
                encode_history_incarnation_v1(1).unwrap(),
            ),
            (
                META_APPLICATION_SEQUENCE,
                encode_application_sequence_allocator_v1(ApplicationSequenceAllocator::initial())
                    .unwrap(),
            ),
            (
                META_ADMINISTRATION_SEQUENCE,
                encode_administration_sequence_allocator_v1(
                    AdministrationSequenceAllocator::initial(),
                )
                .unwrap(),
            ),
            (
                META_RECORD_REGISTRY,
                encode_record_registry_v2(registry).unwrap(),
            ),
        ] {
            meta.insert(key, bytes.as_bytes()).unwrap();
        }
    }
    transaction.commit().unwrap();
    database
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

fn assert_complete(
    database: &Database,
    before: &BTreeMap<String, Vec<u8>>,
) -> ChangelogHistoryStateV3 {
    let actual = metadata(database);
    let get = |namespace| actual.get(key(namespace).unwrap()).unwrap().as_slice();
    assert_eq!(
        *decode_authoritative_state_catalog_v1(get(N::AuthoritativeStateCatalog))
            .unwrap()
            .value(),
        AuthoritativeStateCatalogV1
    );
    assert_eq!(
        *decode_leadership_epoch_v1(get(N::LeadershipEpoch))
            .unwrap()
            .value(),
        lineage().leadership_epoch()
    );
    assert!(
        decode_replication_follower_state_v3(get(N::ReplicationFollowerState))
            .unwrap()
            .value()
            .attached_state()
            .is_none()
    );
    let history = *decode_changelog_history_state_v3(get(N::ChangelogHistoryState))
        .unwrap()
        .value();
    assert_eq!(history.lineage(), lineage());
    assert_eq!(history.anchor().sequence().get(), 1);
    assert_eq!(history.anchor(), history.tail());
    assert_eq!(history.anchor(), history.minimum_resume());
    history
        .validate_allocator(
            *decode_changelog_transaction_allocator_v3(get(N::NextChangelogTransaction))
                .unwrap()
                .value(),
        )
        .unwrap();
    for namespace in [
        META_DATABASE_ID,
        META_HISTORY_INCARNATION,
        META_APPLICATION_SEQUENCE,
        META_ADMINISTRATION_SEQUENCE,
    ] {
        assert_eq!(actual[namespace], before[namespace]);
    }
    let read = database.begin_read().unwrap();
    assert!(read.open_table(SOURCE_HOLDS).unwrap().is_empty().unwrap());
    let table = read.open_table(HISTORY).unwrap();
    assert_eq!(table.len().unwrap(), 1);
    let bytes = table.get(1_u64.to_be_bytes().as_slice()).unwrap().unwrap();
    let receipt = AuthoritativeTransactionV3::decode(bytes.value()).unwrap();
    history.validate_terminal_receipt(&receipt).unwrap();
    assert_eq!(receipt.attribution(), ChangelogAttributionV3::V3Activation);
    assert_eq!(receipt.binding().predecessor, None);
    assert_eq!(receipt.binding().prior_history_hash, [0; 32]);
    assert_eq!(
        receipt.binding().predecessor_frontier,
        DualFrontier::INITIAL
    );
    assert_eq!(receipt.binding().covered_frontier, DualFrontier::INITIAL);
    let changed = before[META_RECORD_REGISTRY] != actual[META_RECORD_REGISTRY];
    assert_eq!(receipt.mutations().len(), usize::from(changed));
    if changed {
        let mutation = &receipt.mutations()[0];
        assert_eq!(mutation.namespace(), N::RecordRegistry);
        assert!(mutation.matches_prior(Some(&before[META_RECORD_REGISTRY])));
        assert_eq!(
            mutation.value(),
            Some(actual[META_RECORD_REGISTRY].as_slice())
        );
    }
    assert_eq!(
        *decode_record_registry_v2(&actual[META_RECORD_REGISTRY])
            .unwrap()
            .value(),
        current_record_registry_digest()
    );
    history
}

#[test]
fn activation_installs_all_roots_and_exact_registry_receipt_in_one_transaction() {
    for registry in [PRE_V3_REGISTRY, current_record_registry_digest()] {
        let root = crate::test_path::ScopedDirectory::new("v3-activation");
        let path = root.join("database.redb");
        let database = fixture(&path, registry);
        let before = metadata(&database);
        let history = activate_validated(
            database.begin_write().unwrap(),
            lineage(),
            DualFrontier::INITIAL,
        )
        .unwrap();
        assert_eq!(history, assert_complete(&database, &before));
        let complete = metadata(&database);
        assert!(
            activate_validated(
                database.begin_write().unwrap(),
                lineage(),
                DualFrontier::INITIAL
            )
            .is_err()
        );
        assert_eq!(metadata(&database), complete);
        drop(database);
        assert_eq!(
            assert_complete(&Database::open(&path).unwrap(), &before),
            history
        );
    }
}

#[test]
fn retained_v3_root_validation_is_pinned_and_rejects_complete_root_erasure() {
    let root = crate::test_path::ScopedDirectory::new("v3-root-pin");
    let database = fixture(&root.join("database.redb"), PRE_V3_REGISTRY);
    let pinned_inactive = database.begin_read().unwrap();
    assert!(
        crate::changelog_v3_roots::read_checkpoint_roots(&pinned_inactive)
            .unwrap()
            .is_none()
    );
    let history = activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    assert!(
        crate::changelog_v3_roots::read_checkpoint_roots(&pinned_inactive)
            .unwrap()
            .is_none()
    );
    let pinned_active = database.begin_read().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots(&pinned_active).unwrap(),
        Some(history)
    );
    let transaction = database.begin_write().unwrap();
    for namespace in N::ALL
        .into_iter()
        .filter(|n| n.requires_v3_activation() && n.metadata_key().is_some())
    {
        transaction
            .open_table(META)
            .unwrap()
            .remove(key(namespace).unwrap())
            .unwrap();
    }
    transaction.delete_table(HISTORY).unwrap();
    transaction.delete_table(SOURCE_HOLDS).unwrap();
    transaction.commit().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots(&pinned_active).unwrap(),
        Some(history)
    );
    // A current registry with every V3 root erased is corrupt, not old inactive.
    assert!(
        crate::changelog_v3_roots::read_checkpoint_roots(&database.begin_read().unwrap()).is_err()
    );
}

#[test]
fn retained_v3_roots_refuse_missing_malformed_or_cross_bound_state_read_only() {
    for arm in 0..18 {
        let root = crate::test_path::ScopedDirectory::new("v3-roots-refusal");
        let database = fixture(&root.join("database.redb"), PRE_V3_REGISTRY);
        let history = activate_validated(
            database.begin_write().unwrap(),
            lineage(),
            DualFrontier::INITIAL,
        )
        .unwrap();
        let transaction = database.begin_write().unwrap();
        {
            let mut meta = transaction.open_table(META).unwrap();
            let namespaces = [
                N::AuthoritativeStateCatalog,
                N::LeadershipEpoch,
                N::ChangelogHistoryState,
                N::NextChangelogTransaction,
                N::ReplicationFollowerState,
            ];
            match arm {
                0..=4 => {
                    meta.remove(key(namespaces[arm]).unwrap()).unwrap();
                }
                5 => {
                    meta.insert(
                        key(N::AuthoritativeStateCatalog).unwrap(),
                        b"bad".as_slice(),
                    )
                    .unwrap();
                }
                6 => {
                    meta.insert(
                        key(N::LeadershipEpoch).unwrap(),
                        encode_leadership_epoch_v1(LeadershipEpochV1::new(2).unwrap())
                            .unwrap()
                            .as_bytes(),
                    )
                    .unwrap();
                }
                7 => {
                    meta.insert(
                        key(N::NextChangelogTransaction).unwrap(),
                        encode_changelog_transaction_allocator_v3(
                            ChangelogTransactionAllocator::initial(),
                        )
                        .unwrap()
                        .as_bytes(),
                    )
                    .unwrap();
                }
                8 => {
                    meta.insert(
                        META_HISTORY_INCARNATION,
                        encode_history_incarnation_v1(2).unwrap().as_bytes(),
                    )
                    .unwrap();
                }
                9 => {
                    meta.insert(
                        META_APPLICATION_SEQUENCE,
                        encode_application_sequence_allocator_v1(
                            ApplicationSequenceAllocator::Next(CommitSequence::new(2).unwrap()),
                        )
                        .unwrap()
                        .as_bytes(),
                    )
                    .unwrap();
                }
                10 => {
                    meta.insert(
                        META_ADMINISTRATION_SEQUENCE,
                        encode_administration_sequence_allocator_v1(
                            AdministrationSequenceAllocator::Next(
                                AdministrationSequence::new(2).unwrap(),
                            ),
                        )
                        .unwrap()
                        .as_bytes(),
                    )
                    .unwrap();
                }
                11 => {
                    meta.insert(
                        key(N::ChangelogHistoryState).unwrap(),
                        [0xff; 513].as_slice(),
                    )
                    .unwrap();
                }
                12 => {
                    meta.insert(
                        META_RECORD_REGISTRY,
                        encode_record_registry_v2(PRE_V3_REGISTRY)
                            .unwrap()
                            .as_bytes(),
                    )
                    .unwrap();
                }
                13 => {
                    let foreign = ChangelogLineageV3::new(
                        lineage().database_id(),
                        2,
                        LeadershipEpochV1::initial(),
                    )
                    .unwrap();
                    let follower =
                        ReplicationFollowerStateV3::attached(foreign, history.tail(), None)
                            .unwrap();
                    meta.insert(
                        key(N::ReplicationFollowerState).unwrap(),
                        encode_replication_follower_state_v3(follower)
                            .unwrap()
                            .as_bytes(),
                    )
                    .unwrap();
                }
                _ => (),
            }
        }
        match arm {
            14 => {
                transaction.delete_table(SOURCE_HOLDS).unwrap();
            }
            15 => {
                transaction.delete_table(HISTORY).unwrap();
            }
            16 => {
                transaction
                    .open_table(HISTORY)
                    .unwrap()
                    .remove(1_u64.to_be_bytes().as_slice())
                    .unwrap();
            }
            17 => {
                let receipt = AuthoritativeTransactionV3::new(
                    AuthoritativeTransactionBindingV3 {
                        database_id: lineage().database_id(),
                        history_incarnation: 1,
                        predecessor: None,
                        sequence: history.tail().sequence(),
                        predecessor_frontier: DualFrontier::INITIAL,
                        covered_frontier: DualFrontier::INITIAL,
                        prior_history_hash: [0; 32],
                    },
                    ChangelogAttributionV3::CleanClose,
                    vec![],
                )
                .unwrap()
                .encode()
                .unwrap();
                transaction
                    .open_table(HISTORY)
                    .unwrap()
                    .insert(1_u64.to_be_bytes().as_slice(), receipt.as_slice())
                    .unwrap();
            }
            _ => (),
        }
        transaction.commit().unwrap();
        let before = metadata(&database);
        assert!(
            crate::changelog_v3_roots::read_checkpoint_roots(&database.begin_read().unwrap())
                .is_err(),
            "accepted arm {arm}"
        );
        assert_eq!(metadata(&database), before);
    }
}

#[test]
fn partial_or_foreign_activation_refuses_without_overwriting_any_existing_bytes() {
    // Each partial metadata root and either empty control table is corruption,
    // not an initialization hint. Failed attempts leave the exact bytes intact.
    for arm in 0..12 {
        let root = crate::test_path::ScopedDirectory::new("v3-activation-refusal");
        let database = fixture(&root.join("database.redb"), PRE_V3_REGISTRY);
        let transaction = database.begin_write().unwrap();
        let mut expected_frontier = DualFrontier::INITIAL;
        match arm {
            0..=4 => {
                let namespaces = [
                    N::AuthoritativeStateCatalog,
                    N::LeadershipEpoch,
                    N::ChangelogHistoryState,
                    N::NextChangelogTransaction,
                    N::ReplicationFollowerState,
                ];
                transaction
                    .open_table(META)
                    .unwrap()
                    .insert(key(namespaces[arm]).unwrap(), b"untrusted".as_slice())
                    .unwrap();
            }
            5 => {
                transaction.open_table(HISTORY).unwrap();
            }
            6 => {
                transaction.open_table(SOURCE_HOLDS).unwrap();
            }
            7 => {
                transaction
                    .open_table(META)
                    .unwrap()
                    .insert(
                        META_RECORD_REGISTRY,
                        encode_record_registry_v2(SchemaHash::from_bytes([0xff; 32]))
                            .unwrap()
                            .as_bytes(),
                    )
                    .unwrap();
            }
            8 => {
                transaction
                    .open_table(META)
                    .unwrap()
                    .remove(META_DATABASE_ID)
                    .unwrap();
            }
            9 => {
                transaction
                    .open_table(META)
                    .unwrap()
                    .insert(
                        META_HISTORY_INCARNATION,
                        encode_history_incarnation_v1(2).unwrap().as_bytes(),
                    )
                    .unwrap();
            }
            10 => {
                expected_frontier = DualFrontier::new(CommitSequence::new(1), None);
            }
            11 => {
                transaction
                    .open_table(META)
                    .unwrap()
                    .insert(META_RECORD_REGISTRY, [0xff; 513].as_slice())
                    .unwrap();
            }
            _ => unreachable!(),
        }
        transaction.commit().unwrap();
        let before = metadata(&database);
        let tables = database
            .begin_read()
            .unwrap()
            .list_tables()
            .unwrap()
            .map(|table| table.name().to_owned())
            .collect::<Vec<_>>();
        assert!(
            activate_validated(
                database.begin_write().unwrap(),
                lineage(),
                expected_frontier
            )
            .is_err()
        );
        assert_eq!(metadata(&database), before);
        assert_eq!(
            database
                .begin_read()
                .unwrap()
                .list_tables()
                .unwrap()
                .map(|table| table.name().to_owned())
                .collect::<Vec<_>>(),
            tables
        );
    }
}

#[test]
fn activation_process_exit_leaves_old_inactive_or_complete_receipted_state() {
    const CHILD: &str = "RIFFDB_WP772_ACTIVATION_DATABASE";
    if let Some(path) = std::env::var_os(CHILD) {
        let database = Database::open(path).unwrap();
        activate_validated(
            database.begin_write().unwrap(),
            lineage(),
            DualFrontier::INITIAL,
        )
        .unwrap();
        panic!("requested process boundary did not fire");
    }
    for edge in ["preflight", "roots", "receipt", "committed"] {
        let root = crate::test_path::ScopedDirectory::new("v3-activation-crash");
        let path = root.join("database.redb");
        let database = fixture(&path, PRE_V3_REGISTRY);
        let before = metadata(&database);
        drop(database);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "changelog_v3_activation::tests::activation_process_exit_leaves_old_inactive_or_complete_receipted_state"])
            .env(CHILD, &path).env("RIFFDB_WP772_ACTIVATION_CRASH", edge).status().unwrap();
        assert_eq!(status.code(), Some(91));
        let database = Database::open(&path).unwrap();
        if edge == "committed" {
            assert_complete(&database, &before);
        } else {
            assert_eq!(metadata(&database), before);
            let read = database.begin_read().unwrap();
            assert!(read.open_table(HISTORY).is_err());
            assert!(read.open_table(SOURCE_HOLDS).is_err());
            drop(read);
            // Repeated recovery has one effect; it never invents earlier history.
            activate_validated(
                database.begin_write().unwrap(),
                lineage(),
                DualFrontier::INITIAL,
            )
            .unwrap();
            assert_complete(&database, &before);
        }
        drop(database);
        assert_complete(&Database::open(&path).unwrap(), &before);
    }
}
