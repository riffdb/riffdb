//! Isolated installation-transaction tests, not complete startup/readiness proof.
// req: REP-003, REC-001, STO-012

use super::*;
use crate::store::RedbCommitProfile;
use redb::{Database, ReadableDatabase, ReadableTableMetadata};
use riffdb_storage_api::LeadershipEpochV1;
use riffdb_types::DatabaseId;
use std::{collections::BTreeMap, path::Path};

#[path = "changelog_v3_catalog_binding_tests.rs"]
mod catalog_binding;

#[path = "changelog_v3_journal_tests.rs"]
mod journal_materialization;

#[path = "changelog_v3_capture_tests.rs"]
mod direct_capture;

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
    let root = crate::test_path::ScopedDirectory::new("v3-activation");
    let path = root.join("database.redb");
    let database = fixture(&path, PRE_V3_REGISTRY);
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

#[test]
fn activation_refuses_a_current_registry_even_when_all_roots_are_absent() {
    let root = crate::test_path::ScopedDirectory::new("v3-erased-activation");
    let database = fixture(
        &root.join("database.redb"),
        current_record_registry_digest(),
    );
    let before = metadata(&database);
    for _ in 0..2 {
        assert!(
            activate_validated(
                database.begin_write().unwrap(),
                lineage(),
                DualFrontier::INITIAL,
            )
            .is_err()
        );
        assert_eq!(metadata(&database), before);
        let read = database.begin_read().unwrap();
        assert!(read.open_table(HISTORY).is_err());
        assert!(read.open_table(SOURCE_HOLDS).is_err());
    }
}

#[test]
fn write_transaction_root_validation_uses_current_bytes_without_creating_missing_tables() {
    let root = crate::test_path::ScopedDirectory::new("v3-write-root");
    let database = fixture(&root.join("database.redb"), PRE_V3_REGISTRY);
    let before = metadata(&database);
    let transaction = database.begin_write().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots_for_write(&transaction).unwrap(),
        None
    );
    assert!(
        !transaction
            .list_tables()
            .unwrap()
            .any(|table| table.name() == HISTORY.name())
    );
    transaction.commit().unwrap();
    assert_eq!(metadata(&database), before);
    let history = activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let complete = metadata(&database);
    let transaction = database.begin_write().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots_for_write(&transaction).unwrap(),
        Some(history)
    );
    {
        let mut meta = transaction.open_table(META).unwrap();
        meta.insert(
            key(N::LeadershipEpoch).unwrap(),
            encode_leadership_epoch_v1(LeadershipEpochV1::new(2).unwrap())
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
    }
    assert!(crate::changelog_v3_roots::read_checkpoint_roots_for_write(&transaction).is_err());
    transaction.abort().unwrap();
    assert_eq!(metadata(&database), complete);
    let transaction = database.begin_write().unwrap();
    transaction.delete_table(HISTORY).unwrap();
    transaction.commit().unwrap();
    let transaction = database.begin_write().unwrap();
    assert!(crate::changelog_v3_roots::read_checkpoint_roots_for_write(&transaction).is_err());
    assert!(
        !transaction
            .list_tables()
            .unwrap()
            .any(|table| table.name() == HISTORY.name())
    );
    transaction.commit().unwrap();
    assert_eq!(metadata(&database), complete);
    assert!(
        !database
            .begin_read()
            .unwrap()
            .list_tables()
            .unwrap()
            .any(|table| table.name() == HISTORY.name())
    );
}

#[test]
fn immediate_receipt_publishes_exact_mutations_allocator_and_tail_atomically() {
    use crate::changelog_v3_write::PreparedImmediateReceipt;
    let root = crate::test_path::ScopedDirectory::new("v3-immediate-receipt");
    let path = root.join("database.redb");
    let database = fixture(&path, PRE_V3_REGISTRY);
    let history = activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let receipt = AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            database_id: lineage().database_id(),
            history_incarnation: 1,
            predecessor: Some(history.tail().sequence()),
            sequence: history.expected_allocator().allocate_one().unwrap().0,
            predecessor_frontier: DualFrontier::INITIAL,
            covered_frontier: DualFrontier::INITIAL,
            prior_history_hash: history.tail().history_hash(),
        },
        ChangelogAttributionV3::ValidatedPrefixCheckpoint,
        vec![
            AuthoritativeMutationV3::put(
                N::ValidatedPrefixEntityHeads,
                b"physical-key",
                None,
                b"original-physical-bytes",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let pin = database.begin_read().unwrap();
    let prepared =
        PreparedImmediateReceipt::apply(&database, RedbCommitProfile::Hardened, &receipt).unwrap();
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots(&pin).unwrap(),
        Some(history)
    );
    prepared.commit_for_test().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots(&pin).unwrap(),
        Some(history)
    );
    drop(pin);
    let expected = history.advance(&receipt).unwrap();
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots(&database.begin_read().unwrap()).unwrap(),
        Some(expected)
    );
    let read = database.begin_read().unwrap();
    assert_eq!(
        read.open_table(VALIDATED_PREFIX_ENTITY_HEADS)
            .unwrap()
            .get(b"physical-key".as_slice())
            .unwrap()
            .unwrap()
            .value(),
        b"original-physical-bytes"
    );
    assert_eq!(
        read.open_table(HISTORY)
            .unwrap()
            .get(receipt.binding().sequence.get().to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value(),
        receipt.encode().unwrap()
    );
    drop(read);
    assert!(
        PreparedImmediateReceipt::apply(&database, RedbCommitProfile::Hardened, &receipt).is_err(),
        "duplicate predecessor refuses, never overwrites"
    );
    drop(database);
    let reopened = Database::open(path).unwrap();
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots(&reopened.begin_read().unwrap()).unwrap(),
        Some(expected)
    );
}

fn direct_receipt(
    history: ChangelogHistoryStateV3,
    mutations: Vec<AuthoritativeMutationV3>,
) -> AuthoritativeTransactionV3 {
    AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            database_id: history.lineage().database_id(),
            history_incarnation: history.lineage().history_incarnation(),
            predecessor: Some(history.tail().sequence()),
            sequence: history.expected_allocator().allocate_one().unwrap().0,
            predecessor_frontier: history.tail().frontier(),
            covered_frontier: history.tail().frontier(),
            prior_history_hash: history.tail().history_hash(),
        },
        ChangelogAttributionV3::ValidatedPrefixCheckpoint,
        mutations,
    )
    .unwrap()
}

#[test]
fn checkpoint_receipt_plan_binds_one_pinned_prefix_and_exact_metadata_before_image() {
    use riffdb_storage_api::{
        EntityChainFingerprint, EntityTransitionFingerprint, StoredValidatedPrefixCheckpointV1,
        StoredValidatedPrefixCheckpointV2, ValidatedPrefixEntityTransitionCounts,
        ValidatedPrefixRetainedSnapshot, ValidatedPrefixSequenceCounts,
    };
    let root = crate::test_path::ScopedDirectory::new("v3-checkpoint-plan");
    let database = fixture(&root.join("database.redb"), PRE_V3_REGISTRY);
    let history = activate_validated(
        database.begin_write().unwrap(),
        lineage(),
        DualFrontier::INITIAL,
    )
    .unwrap();
    let checkpoint_with_parent = |parent| {
        let base = StoredValidatedPrefixCheckpointV1::new(
            lineage().database_id(),
            1,
            current_record_registry_digest(),
            0,
            0,
            ValidatedPrefixSequenceCounts {
                commits_count: 0,
                events_count: 0,
                event_routes_count: 0,
                outbox_count: 0,
                outbox_status_count: 0,
                idempotency_count: 0,
                audit_count: 0,
                audit_by_request_count: 0,
            },
            EntityChainFingerprint::from_sorted_pairs(std::iter::empty()),
            ValidatedPrefixRetainedSnapshot {
                next_application_sequence: 1,
                application_sequence_exhausted: false,
                next_administration_sequence: 1,
                administration_sequence_exhausted: false,
            },
            parent,
            0,
        )
        .unwrap();
        let checkpoint = StoredValidatedPrefixCheckpointV2::new(
            base,
            ValidatedPrefixEntityTransitionCounts {
                live_entity_count: 0,
                deleted_entity_count: 0,
                entity_transition_count: 0,
            },
            EntityTransitionFingerprint::from_sorted_heads(std::iter::empty()).unwrap(),
        );
        checkpoint.unwrap()
    };
    let checkpoint = checkpoint_with_parent(None);
    let pin = database.begin_read().unwrap();
    let receipt = crate::validated_prefix::plan_checkpoint_receipt(&pin, &checkpoint)
        .unwrap()
        .unwrap();
    assert_eq!(
        receipt.attribution(),
        ChangelogAttributionV3::ValidatedPrefixCheckpoint
    );
    assert_eq!(receipt.mutations().len(), 1);
    assert_eq!(
        receipt.mutations()[0].namespace(),
        N::ValidatedPrefixCheckpoint
    );
    assert!(receipt.mutations()[0].matches_prior(None));
    assert_eq!(
        receipt.mutations()[0].value(),
        Some(
            encode_validated_prefix_checkpoint_v2(&checkpoint)
                .unwrap()
                .as_bytes()
        )
    );
    crate::changelog_v3_write::PreparedImmediateReceipt::apply(
        &database,
        RedbCommitProfile::Hardened,
        &receipt,
    )
    .unwrap()
    .commit_for_test()
    .unwrap();
    assert_eq!(
        crate::validated_prefix::plan_checkpoint_receipt(&pin, &checkpoint)
            .unwrap()
            .unwrap(),
        receipt
    );
    assert!(
        crate::changelog_v3_write::PreparedImmediateReceipt::apply(
            &database,
            RedbCommitProfile::Hardened,
            &receipt
        )
        .is_err()
    );
    assert!(
        crate::validated_prefix::plan_checkpoint_receipt(
            &database.begin_read().unwrap(),
            &checkpoint
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots(&database.begin_read().unwrap()).unwrap(),
        Some(history.advance(&receipt).unwrap())
    );
    let incorrect_parent = checkpoint_with_parent(Some(
        riffdb_storage_api::ValidatedPrefixCheckpointHash::from_bytes([0xff; 32]),
    ));
    assert!(
        crate::validated_prefix::plan_checkpoint_receipt(
            &database.begin_read().unwrap(),
            &incorrect_parent
        )
        .is_err()
    );
    let successor = checkpoint_with_parent(Some(checkpoint.base().checkpoint_hash()));
    let next_receipt = crate::validated_prefix::plan_checkpoint_receipt(
        &database.begin_read().unwrap(),
        &successor,
    )
    .unwrap()
    .unwrap();
    assert_eq!(next_receipt.mutations().len(), 1);
    assert!(
        next_receipt.mutations()[0].matches_prior(Some(
            encode_validated_prefix_checkpoint_v2(&checkpoint)
                .unwrap()
                .as_bytes()
        ))
    );
    crate::changelog_v3_write::PreparedImmediateReceipt::apply(
        &database,
        RedbCommitProfile::Hardened,
        &next_receipt,
    )
    .unwrap()
    .commit_for_test()
    .unwrap();
    assert!(
        crate::validated_prefix::plan_checkpoint_receipt(
            &database.begin_read().unwrap(),
            &successor
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn immediate_receipt_history_retains_original_put_after_later_delete() {
    use crate::changelog_v3_write::PreparedImmediateReceipt;
    for profile in [RedbCommitProfile::Standard, RedbCommitProfile::Hardened] {
        let root = crate::test_path::ScopedDirectory::new("v3-direct-history");
        let path = root.join("database.redb");
        let database = fixture(&path, PRE_V3_REGISTRY);
        let history = activate_validated(
            database.begin_write().unwrap(),
            lineage(),
            DualFrontier::INITIAL,
        )
        .unwrap();
        let put = direct_receipt(
            history,
            vec![
                AuthoritativeMutationV3::put(
                    N::ValidatedPrefixEntityHeads,
                    b"key",
                    None,
                    b"historic-full-value",
                )
                .unwrap(),
            ],
        );
        let frozen = put.encode().unwrap();
        PreparedImmediateReceipt::apply(&database, profile, &put)
            .unwrap()
            .commit_for_test()
            .unwrap();
        let prior = history.advance(&put).unwrap();
        let pin = database.begin_read().unwrap();
        let delete = direct_receipt(
            prior,
            vec![
                AuthoritativeMutationV3::delete(
                    N::ValidatedPrefixEntityHeads,
                    b"key",
                    // Frozen raw SHA-256 of the literal predecessor value.
                    [
                        0x9d, 0xe8, 0x03, 0xde, 0x38, 0xc7, 0x92, 0x93, 0x62, 0x0a, 0x30, 0x78,
                        0x5d, 0xc6, 0x3c, 0x5f, 0x05, 0x90, 0xb8, 0xda, 0x7e, 0x29, 0x10, 0xfb,
                        0x47, 0x0c, 0x57, 0x8c, 0x1c, 0x96, 0x09, 0xec,
                    ],
                )
                .unwrap(),
            ],
        );
        PreparedImmediateReceipt::apply(&database, profile, &delete)
            .unwrap()
            .commit_for_test()
            .unwrap();
        assert_eq!(
            pin.open_table(VALIDATED_PREFIX_ENTITY_HEADS)
                .unwrap()
                .get(b"key".as_slice())
                .unwrap()
                .unwrap()
                .value(),
            b"historic-full-value"
        );
        assert_eq!(
            crate::changelog_v3_roots::read_checkpoint_roots(&pin).unwrap(),
            Some(prior)
        );
        drop(pin);
        drop(database);
        let database = Database::open(path).unwrap();
        let read = database.begin_read().unwrap();
        assert!(
            read.open_table(VALIDATED_PREFIX_ENTITY_HEADS)
                .unwrap()
                .get(b"key".as_slice())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            crate::changelog_v3_roots::read_checkpoint_roots(&read).unwrap(),
            Some(prior.advance(&delete).unwrap())
        );
        let receipts = read.open_table(HISTORY).unwrap();
        assert_eq!(
            receipts
                .get(put.binding().sequence.get().to_be_bytes().as_slice())
                .unwrap()
                .unwrap()
                .value(),
            frozen
        );
        let retained = AuthoritativeTransactionV3::decode(&frozen).unwrap();
        assert_eq!(
            retained.mutations()[0].value(),
            Some(b"historic-full-value".as_slice())
        );
        assert_eq!(
            receipts
                .get(delete.binding().sequence.get().to_be_bytes().as_slice())
                .unwrap()
                .unwrap()
                .value(),
            delete.encode().unwrap()
        );
    }
}

#[test]
fn immediate_receipt_refuses_wrong_preconditions_missing_tables_and_false_frontiers_without_commit()
{
    use crate::changelog_v3_write::PreparedImmediateReceipt;
    for arm in 0..3 {
        let root = crate::test_path::ScopedDirectory::new("v3-direct-refusal");
        let database = fixture(&root.join("database.redb"), PRE_V3_REGISTRY);
        let history = activate_validated(
            database.begin_write().unwrap(),
            lineage(),
            DualFrontier::INITIAL,
        )
        .unwrap();
        let mut receipt = direct_receipt(
            history,
            vec![
                AuthoritativeMutationV3::put(N::ValidatedPrefixEntityHeads, b"a", None, b"first")
                    .unwrap(),
                AuthoritativeMutationV3::put(N::ValidatedPrefixEntityHeads, b"b", None, b"second")
                    .unwrap(),
            ],
        );
        match arm {
            0 => {
                let transaction = database.begin_write().unwrap();
                transaction
                    .open_table(VALIDATED_PREFIX_ENTITY_HEADS)
                    .unwrap()
                    .insert(b"b".as_slice(), b"occupied".as_slice())
                    .unwrap();
                transaction.commit().unwrap();
            }
            1 => {
                let transaction = database.begin_write().unwrap();
                transaction
                    .delete_table(VALIDATED_PREFIX_ENTITY_HEADS)
                    .unwrap();
                transaction.commit().unwrap();
            }
            2 => {
                let mut binding = receipt.binding();
                binding.covered_frontier =
                    DualFrontier::new(riffdb_types::CommitSequence::new(1), None);
                receipt = AuthoritativeTransactionV3::new(
                    binding,
                    receipt.attribution(),
                    receipt.mutations().to_vec(),
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let before = metadata(&database);
        assert!(
            PreparedImmediateReceipt::apply(&database, RedbCommitProfile::Hardened, &receipt)
                .is_err()
        );
        assert_eq!(metadata(&database), before);
        let read = database.begin_read().unwrap();
        assert_eq!(read.open_table(HISTORY).unwrap().len().unwrap(), 1);
        if arm == 1 {
            assert!(
                !read
                    .list_tables()
                    .unwrap()
                    .any(|table| table.name() == VALIDATED_PREFIX_ENTITY_HEADS.name())
            );
        } else {
            let heads = read.open_table(VALIDATED_PREFIX_ENTITY_HEADS).unwrap();
            assert!(heads.get(b"a".as_slice()).unwrap().is_none());
            assert_eq!(heads.len().unwrap(), u64::from(arm == 0));
        }
    }
}

#[test]
fn immediate_receipt_process_child() {
    let Ok(path) = std::env::var("RIFFDB_V3_DIRECT_PATH") else {
        return;
    };
    let database = Database::open(path).unwrap();
    let history = crate::changelog_v3_roots::read_checkpoint_roots(&database.begin_read().unwrap())
        .unwrap()
        .unwrap();
    let receipt = direct_receipt(
        history,
        vec![
            AuthoritativeMutationV3::delete(
                N::ValidatedPrefixEntityHeads,
                b"delete-me",
                // Frozen raw SHA-256 of the literal predecessor value.
                [
                    0xbd, 0x14, 0xb1, 0x96, 0xf4, 0x4f, 0x22, 0x15, 0x90, 0x83, 0x6f, 0xc6, 0xd9,
                    0x18, 0x11, 0xb0, 0x9d, 0xad, 0xd0, 0x1e, 0x6f, 0xf3, 0xad, 0xec, 0x62, 0x74,
                    0x68, 0xfc, 0x7b, 0xdd, 0x59, 0x3b,
                ],
            )
            .unwrap(),
            AuthoritativeMutationV3::replace(
                N::ValidatedPrefixEntityHeads,
                b"overwrite-me",
                b"before-overwrite",
                b"after-overwrite",
            )
            .unwrap(),
        ],
    );
    crate::changelog_v3_write::PreparedImmediateReceipt::apply(
        &database,
        RedbCommitProfile::Hardened,
        &receipt,
    )
    .unwrap()
    .commit_for_test()
    .unwrap();
    panic!("child must stop at its requested crash edge");
}

#[test]
fn immediate_receipt_process_edges_recover_old_or_complete_rows_and_exact_history() {
    for edge in ["mutations", "receipt", "roots", "committed"] {
        let root = crate::test_path::ScopedDirectory::new("v3-direct-crash");
        let path = root.join("database.redb");
        let database = fixture(&path, PRE_V3_REGISTRY);
        let transaction = database.begin_write().unwrap();
        {
            let mut heads = transaction
                .open_table(VALIDATED_PREFIX_ENTITY_HEADS)
                .unwrap();
            heads
                .insert(b"delete-me".as_slice(), b"before-delete".as_slice())
                .unwrap();
            heads
                .insert(b"overwrite-me".as_slice(), b"before-overwrite".as_slice())
                .unwrap();
        }
        transaction.commit().unwrap();
        let history = activate_validated(
            database.begin_write().unwrap(),
            lineage(),
            DualFrontier::INITIAL,
        )
        .unwrap();
        let before = metadata(&database);
        drop(database);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "changelog_v3_activation::tests::immediate_receipt_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_V3_DIRECT_PATH", &path)
            .env("RIFFDB_V3_DIRECT_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "edge {edge}");
        let database = Database::open(&path).unwrap();
        let read = database.begin_read().unwrap();
        let recovered = crate::changelog_v3_roots::read_checkpoint_roots(&read)
            .unwrap()
            .unwrap();
        let heads = read.open_table(VALIDATED_PREFIX_ENTITY_HEADS).unwrap();
        let receipts = read.open_table(HISTORY).unwrap();
        if edge == "committed" {
            assert_eq!(recovered.tail().sequence().get(), 2);
            assert!(heads.get(b"delete-me".as_slice()).unwrap().is_none());
            assert_eq!(
                heads
                    .get(b"overwrite-me".as_slice())
                    .unwrap()
                    .unwrap()
                    .value(),
                b"after-overwrite"
            );
            assert_eq!(receipts.len().unwrap(), 2);
            let row = receipts
                .get(2_u64.to_be_bytes().as_slice())
                .unwrap()
                .unwrap();
            let receipt = AuthoritativeTransactionV3::decode(row.value()).unwrap();
            assert_eq!(recovered, history.advance(&receipt).unwrap());
            assert_eq!(receipt.mutations().len(), 2);
            assert!(receipt.mutations()[0].matches_prior(Some(b"before-delete")));
            assert!(receipt.mutations()[1].matches_prior(Some(b"before-overwrite")));
            assert_eq!(receipt.encode().unwrap(), row.value());
        } else {
            assert_eq!(recovered, history);
            assert_eq!(metadata(&database), before);
            assert_eq!(
                heads.get(b"delete-me".as_slice()).unwrap().unwrap().value(),
                b"before-delete"
            );
            assert_eq!(
                heads
                    .get(b"overwrite-me".as_slice())
                    .unwrap()
                    .unwrap()
                    .value(),
                b"before-overwrite"
            );
            assert_eq!(receipts.len().unwrap(), 1);
        }
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
