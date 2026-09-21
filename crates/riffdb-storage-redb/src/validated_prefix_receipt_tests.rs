//! Actual checkpoint writer with isolated activation; no full startup claim.
// req: REP-003, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionV3, ChangelogAttributionV3,
    ChangelogLineageV3, LeadershipEpochV1,
};

fn initialized(path: &std::path::Path) -> crate::store::RedbStore {
    initialized_with_controller(path, None)
}

fn initialized_with_controller(
    path: &std::path::Path,
    controller: Option<crate::hooks::RedbTestController>,
) -> crate::store::RedbStore {
    let mut store = match controller {
        Some(controller) => crate::store::RedbStore::open_with_test_controller(path, controller),
        None => crate::store::RedbStore::open(path),
    }
    .unwrap();
    let id =
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x73; 10])
            .unwrap();
    store.initialize_legacy_fixture(id).unwrap();
    crate::changelog_v3_activation::activate_validated(
        store.shared.database.begin_write().unwrap(),
        ChangelogLineageV3::new(id, 1, LeadershipEpochV1::initial()).unwrap(),
        riffdb_types::DualFrontier::INITIAL,
    )
    .unwrap();
    store
}

#[test]
fn actual_prefix_checkpoint_materializes_its_exact_receipt_with_one_commit() {
    let scope = crate::test_path::ScopedDirectory::new("v3-prefix-writer");
    let store = initialized(&scope.join("db.redb"));
    let before = store.shared.database.begin_read().unwrap();
    let retained = crate::startup::read_retained_metadata_pub(&before).unwrap();
    let mut walked = 0;
    let checkpoint = build_checkpoint_from_snapshot(
        &before,
        &retained,
        CheckpointCountSource::DurableLengths {
            execution_failed_rows: 0,
        },
        &mut walked,
    )
    .unwrap();
    let expected = plan_checkpoint_receipt(&before, &checkpoint)
        .unwrap()
        .unwrap()
        .encode()
        .unwrap();
    let epoch = store.shared.durable_commit_epoch();
    write_validated_prefix_checkpoint(
        &store.shared,
        &retained,
        CheckpointPurpose::StartupValidation,
    )
    .unwrap();
    assert_eq!(store.shared.durable_commit_epoch(), epoch + 1);
    let read = store.shared.database.begin_read().unwrap();
    let rows = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let row = rows.get(2u64.to_be_bytes().as_slice()).unwrap();
    assert!(
        row.is_some(),
        "the existing checkpoint commit must materialize its complete V3 receipt"
    );
    let row = row.unwrap();
    assert_eq!(row.value(), expected);
    let receipt = AuthoritativeTransactionV3::decode(row.value()).unwrap();
    assert_eq!(
        receipt.attribution(),
        ChangelogAttributionV3::ValidatedPrefixCheckpoint
    );
    assert_eq!(receipt.mutations().len(), 1);
    assert_eq!(
        receipt.mutations()[0].namespace(),
        N::ValidatedPrefixCheckpoint
    );
    let meta = read.open_table(META).unwrap();
    let proof = meta.get(META_VALIDATED_PREFIX_CHECKPOINT).unwrap().unwrap();
    assert_eq!(receipt.mutations()[0].value(), Some(proof.value()));
    assert!(receipt.mutations()[0].matches_prior(None));
    assert!(
        before
            .open_table(META)
            .unwrap()
            .get(META_VALIDATED_PREFIX_CHECKPOINT)
            .unwrap()
            .is_none()
    );
    write_validated_prefix_checkpoint(&store.shared, &retained, CheckpointPurpose::TestFixture)
        .unwrap();
    assert_eq!(store.shared.durable_commit_epoch(), epoch + 1);
    write_validated_prefix_checkpoint(
        &store.shared,
        &retained,
        CheckpointPurpose::StartupValidation,
    )
    .unwrap();
    assert_eq!(store.shared.durable_commit_epoch(), epoch + 2);
    let latest = store.shared.database.begin_read().unwrap();
    let history = crate::changelog_v3_roots::validate_retained_history(&latest)
        .unwrap()
        .unwrap();
    assert_eq!(history.tail().sequence().get(), 3);
    let latest_rows = latest
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    assert_eq!(
        latest_rows
            .get(2u64.to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value(),
        expected
    );
    let successor = AuthoritativeTransactionV3::decode(
        latest_rows
            .get(3u64.to_be_bytes().as_slice())
            .unwrap()
            .unwrap()
            .value(),
    )
    .unwrap();
    assert_eq!(successor.mutations().len(), 1);
    assert!(successor.mutations()[0].matches_prior(Some(proof.value())));
    assert_ne!(successor.mutations()[0].value(), Some(proof.value()));
}

#[test]
fn invalid_optional_checkpoint_replacement_keeps_exact_preimage_and_single_commit() {
    let scope = crate::test_path::ScopedDirectory::new("v3-prefix-invalid-proof");
    let store = initialized(&scope.join("db.redb"));
    let read = store.shared.database.begin_read().unwrap();
    let retained = crate::startup::read_retained_metadata_pub(&read).unwrap();
    drop(read);
    let damage = |bytes: &[u8]| {
        let write = store.shared.database.begin_write().unwrap();
        write
            .open_table(META)
            .unwrap()
            .insert(META_VALIDATED_PREFIX_CHECKPOINT, bytes)
            .unwrap();
        write.commit().unwrap();
    };
    damage(b"invalid-optional-proof");
    let pin = store.shared.database.begin_read().unwrap();
    let mut walked = 0;
    let checkpoint = build_checkpoint_from_snapshot(
        &pin,
        &retained,
        CheckpointCountSource::DurableLengths {
            execution_failed_rows: 0,
        },
        &mut walked,
    )
    .unwrap();
    assert_eq!(checkpoint.base().previous_checkpoint_hash(), None);
    let receipt = plan_checkpoint_receipt(&pin, &checkpoint).unwrap().unwrap();
    assert_eq!(receipt.mutations().len(), 1);
    assert!(receipt.mutations()[0].matches_prior(Some(b"invalid-optional-proof")));
    assert!(!receipt.mutations()[0].matches_prior(None));
    let epoch = store.shared.durable_commit_epoch();
    damage(b"different-invalid-proof");
    assert!(
        crate::changelog_v3_write::PreparedImmediateReceipt::apply(
            &store.shared.database,
            crate::RedbCommitProfile::Hardened,
            &receipt,
        )
        .is_err(),
        "a changed invalid preimage must not be overwritten by a stale plan"
    );
    assert_eq!(store.shared.durable_commit_epoch(), epoch);
    write_validated_prefix_checkpoint(
        &store.shared,
        &retained,
        CheckpointPurpose::StartupValidation,
    )
    .unwrap();
    assert_eq!(store.shared.durable_commit_epoch(), epoch + 1);
    let latest = store.shared.database.begin_read().unwrap();
    let history = crate::changelog_v3_roots::validate_retained_history(&latest)
        .unwrap()
        .unwrap();
    assert_eq!(history.tail().sequence().get(), 2);
    let rows = latest
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let bytes = rows.get(2u64.to_be_bytes().as_slice()).unwrap().unwrap();
    let actual = AuthoritativeTransactionV3::decode(bytes.value()).unwrap();
    assert_eq!(
        actual.attribution(),
        ChangelogAttributionV3::ValidatedPrefixCheckpoint
    );
    assert_eq!(
        actual.binding().predecessor_frontier,
        actual.binding().covered_frontier
    );
    assert!(actual.mutations()[0].matches_prior(Some(b"different-invalid-proof")));
    let meta = latest.open_table(META).unwrap();
    let proof = meta.get(META_VALIDATED_PREFIX_CHECKPOINT).unwrap().unwrap();
    assert_eq!(actual.mutations()[0].value(), Some(proof.value()));
    assert_eq!(
        decode_validated_prefix_checkpoint_v2(proof.value())
            .unwrap()
            .value(),
        &checkpoint
    );
    write_validated_prefix_checkpoint(&store.shared, &retained, CheckpointPurpose::TestFixture)
        .unwrap();
    assert_eq!(
        store.shared.durable_commit_epoch(),
        epoch + 1,
        "exact retry is read-only"
    );
}

#[test]
fn prefix_checkpoint_receipt_preserves_existing_precommit_and_unknown_hooks() {
    for committed in [false, true] {
        let scope = crate::test_path::ScopedDirectory::new("v3-prefix-fault");
        let controller = if committed {
            crate::hooks::RedbTestController::return_unknown_after_commit(
                RedbTestOperation::ValidatedPrefixCheckpoint,
            )
        } else {
            crate::hooks::RedbTestController::return_before_commit(
                RedbTestOperation::ValidatedPrefixCheckpoint,
            )
        };
        let store = initialized_with_controller(&scope.join("db.redb"), Some(controller));
        let read = store.shared.database.begin_read().unwrap();
        let retained = crate::startup::read_retained_metadata_pub(&read).unwrap();
        let epoch = store.shared.durable_commit_epoch();
        let error = write_validated_prefix_checkpoint(
            &store.shared,
            &retained,
            CheckpointPurpose::StartupValidation,
        )
        .unwrap_err();
        assert_eq!(
            error.kind(),
            if committed {
                StorageErrorKind::CommitStatusUnknown
            } else {
                StorageErrorKind::Unavailable
            }
        );
        assert_eq!(
            store.shared.durable_commit_epoch(),
            epoch + u64::from(committed)
        );
        let latest = store.shared.database.begin_read().unwrap();
        let history = crate::changelog_v3_roots::validate_retained_history(&latest)
            .unwrap()
            .unwrap();
        assert_eq!(
            history.tail().sequence().get(),
            if committed { 2 } else { 1 }
        );
        assert_eq!(
            latest
                .open_table(META)
                .unwrap()
                .get(META_VALIDATED_PREFIX_CHECKPOINT)
                .unwrap()
                .is_some(),
            committed
        );
    }
}

#[test]
fn prefix_checkpoint_refuses_malformed_v3_history_even_on_exact_fixture_noop() {
    let scope = crate::test_path::ScopedDirectory::new("v3-prefix-bad-history");
    let store = initialized(&scope.join("db.redb"));
    let read = store.shared.database.begin_read().unwrap();
    let retained = crate::startup::read_retained_metadata_pub(&read).unwrap();
    write_validated_prefix_checkpoint(
        &store.shared,
        &retained,
        CheckpointPurpose::StartupValidation,
    )
    .unwrap();
    let pinned = store.shared.database.begin_read().unwrap();
    let proof = pinned
        .open_table(META)
        .unwrap()
        .get(META_VALIDATED_PREFIX_CHECKPOINT)
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    let transaction = store.shared.database.begin_write().unwrap();
    transaction
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap()
        .remove(2u64.to_be_bytes().as_slice())
        .unwrap();
    transaction.commit().unwrap();
    let epoch = store.shared.durable_commit_epoch();
    for purpose in [
        CheckpointPurpose::StartupValidation,
        CheckpointPurpose::TestFixture,
    ] {
        assert_eq!(
            write_validated_prefix_checkpoint(&store.shared, &retained, purpose)
                .unwrap_err()
                .kind(),
            StorageErrorKind::CorruptData
        );
        assert_eq!(store.shared.durable_commit_epoch(), epoch);
        let latest = store.shared.database.begin_read().unwrap();
        assert_eq!(
            latest
                .open_table(META)
                .unwrap()
                .get(META_VALIDATED_PREFIX_CHECKPOINT)
                .unwrap()
                .unwrap()
                .value(),
            proof
        );
    }
}

#[test]
fn prefix_checkpoint_receipt_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_V3_PREFIX_PATH") else {
        return;
    };
    let store = initialized(std::path::Path::new(&path));
    if std::env::var("RIFFDB_V3_PREFIX_INVALID").as_deref() == Ok("true") {
        let write = store.shared.database.begin_write().unwrap();
        write
            .open_table(META)
            .unwrap()
            .insert(
                META_VALIDATED_PREFIX_CHECKPOINT,
                b"invalid-optional-proof".as_slice(),
            )
            .unwrap();
        write.commit().unwrap();
    }
    let read = store.shared.database.begin_read().unwrap();
    let retained = crate::startup::read_retained_metadata_pub(&read).unwrap();
    drop(read);
    write_validated_prefix_checkpoint(
        &store.shared,
        &retained,
        CheckpointPurpose::StartupValidation,
    )
    .unwrap();
    panic!("the requested checkpoint crash edge was not reached");
}

#[test]
fn actual_prefix_checkpoint_crashes_preserve_original_or_complete_receipted_proof() {
    checkpoint_crashes(false);
}

#[test]
fn invalid_optional_checkpoint_crashes_preserve_exact_preimage_or_atomic_replacement() {
    checkpoint_crashes(true);
}

fn checkpoint_crashes(invalid: bool) {
    for edge in ["mutations", "receipt", "roots", "committed"] {
        let scope = crate::test_path::ScopedDirectory::new("v3-prefix-crash");
        let path = scope.join("db.redb");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "validated_prefix::receipt_tests::prefix_checkpoint_receipt_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_V3_PREFIX_PATH", &path)
            .env("RIFFDB_V3_PREFIX_INVALID", invalid.to_string())
            .env("RIFFDB_V3_DIRECT_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "edge {edge}");
        let mut previous = None;
        for _ in 0..2 {
            let database = redb::Database::open(&path).unwrap();
            let read = database.begin_read().unwrap();
            let history = crate::changelog_v3_roots::validate_retained_history(&read)
                .unwrap()
                .unwrap();
            let committed = edge == "committed";
            assert_eq!(
                history.tail().sequence().get(),
                if committed { 2 } else { 1 }
            );
            let meta = read.open_table(META).unwrap();
            let proof = meta.get(META_VALIDATED_PREFIX_CHECKPOINT).unwrap();
            assert_eq!(proof.is_some(), committed || invalid);
            if !committed && invalid {
                assert_eq!(proof.as_ref().unwrap().value(), b"invalid-optional-proof");
            }
            let rows = read
                .open_table(crate::changelog_v3_activation::HISTORY)
                .unwrap();
            let observed = rows
                .get(2u64.to_be_bytes().as_slice())
                .unwrap()
                .map(|row| row.value().to_vec());
            assert_eq!(observed.is_some(), committed);
            if let Some(bytes) = &observed {
                let receipt = AuthoritativeTransactionV3::decode(bytes).unwrap();
                assert_eq!(
                    receipt.attribution(),
                    ChangelogAttributionV3::ValidatedPrefixCheckpoint
                );
                assert_eq!(receipt.mutations().len(), 1);
                assert!(
                    receipt.mutations()[0]
                        .matches_prior(invalid.then_some(b"invalid-optional-proof".as_slice()))
                );
                assert_eq!(
                    receipt.mutations()[0].value(),
                    Some(proof.as_ref().unwrap().value())
                );
            }
            if let Some(previous) = &previous {
                assert_eq!(&observed, previous);
            }
            previous = Some(observed);
        }
        let store = crate::RedbStore::open(&path).unwrap();
        let read = store.shared.database.begin_read().unwrap();
        let retained = crate::startup::read_retained_metadata_pub(&read).unwrap();
        drop(read);
        for _ in 0..2 {
            write_validated_prefix_checkpoint(
                &store.shared,
                &retained,
                CheckpointPurpose::TestFixture,
            )
            .unwrap();
            let read = store.shared.database.begin_read().unwrap();
            let history = crate::changelog_v3_roots::validate_retained_history(&read)
                .unwrap()
                .unwrap();
            assert_eq!(
                history.tail().sequence().get(),
                2,
                "retry allocates exactly once"
            );
            let bytes = read
                .open_table(crate::changelog_v3_activation::HISTORY)
                .unwrap()
                .get(2u64.to_be_bytes().as_slice())
                .unwrap()
                .unwrap()
                .value()
                .to_vec();
            if let Some(Some(prior)) = &previous {
                assert_eq!(&bytes, prior, "retry preserves the original receipt bytes");
            }
            previous = Some(Some(bytes));
        }
    }
}
