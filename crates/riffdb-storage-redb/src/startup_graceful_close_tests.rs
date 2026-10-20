// req: STO-023, REC-001, REC-002, REC-004, PERF-013
#[cfg(feature = "test-fixtures")]
#[test]
fn graceful_close_drains_locator_builder_source_before_final_clean() {
    use crate::hooks::{RedbCheckpointTestPoint, RedbCheckpointTestSchedule};
    struct Release {
        schedule: Option<RedbCheckpointTestSchedule>,
        close: Option<
            std::thread::JoinHandle<(
                crate::RedbOperationalPorts,
                crate::GracefulCheckpointCloseReceiptV1,
            )>,
        >,
    }
    impl Drop for Release {
        fn drop(&mut self) {
            drop(self.schedule.take());
            if let Some(close) = self.close.take() {
                let _ = close.join();
            }
        }
    }
    let path = TestDatabasePath::new("close-builder-source-green");
    let (store, _) = checkpointable_history_store(&path, database_id(0xe8), 3);
    let ports = open_cleanly(store);
    assert_eq!(
        ports.complete_graceful_close().lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    drop(ports);
    let (controller, schedule) = crate::RedbTestController::checkpoint_schedule();
    schedule.arm(RedbCheckpointTestPoint::BeforeLocatorJobSourceRelease);
    schedule.arm(RedbCheckpointTestPoint::LocatorCloseDrainWaiting);
    schedule.arm(RedbCheckpointTestPoint::BeforeFinalCleanCommit);
    let ports = open_cleanly(
        RedbStore::open_with_test_controller(&path.0, controller).expect("bounded reopen"),
    );
    assert!(ports.clean_close_fast_startup());
    schedule.wait(RedbCheckpointTestPoint::BeforeLocatorJobSourceRelease);
    let capability = {
        let access = ports.begin_composite_read().expect("original source");
        access.locator_capability().clone()
    };
    assert!(capability.builder_active_for_close_test());
    let shared = Arc::downgrade(&ports.shared);
    let domain = ports.shared.database.locator_build_domain().clone();
    let gate = ports.shared.locator_close_gate_for_test();
    let before = checkpoint_storage_bytes(&ports);
    let mut release = Release {
        schedule: Some(schedule),
        close: None,
    };
    release.close = Some(std::thread::spawn(move || {
        let receipt = ports.complete_graceful_close();
        (ports, receipt)
    }));
    let schedule = release.schedule.as_ref().expect("release guard");
    schedule.wait(RedbCheckpointTestPoint::LocatorCloseDrainWaiting);
    assert!(
        !gate.is_held(),
        "locator drain must not own the publication mutation lease"
    );
    // Actual acknowledgment can lock its predicate only after wait atomically releases it.
    schedule.release(RedbCheckpointTestPoint::LocatorCloseDrainWaiting);
    schedule.release(RedbCheckpointTestPoint::BeforeLocatorJobSourceRelease);
    schedule.wait(RedbCheckpointTestPoint::BeforeFinalCleanCommit);
    assert!(!capability.builder_active_for_close_test());
    assert_eq!(domain.registrations_for_test(), 0);
    assert!(domain.is_cancelled());
    drop(release.schedule.take());
    let (ports, receipt) = release
        .close
        .take()
        .expect("close thread")
        .join()
        .expect("close returned");
    assert_eq!(
        receipt.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    assert_eq!(checkpoint_storage_bytes(&ports), before);
    drop(ports);
    assert_eq!(shared.strong_count(), 0);
    // The immediate exclusive reopen below checks actual final engine custody release.
    let reopened =
        open_cleanly(RedbStore::open(&path.0).expect("single immediate exclusive reopen"));
    assert_eq!(
        reopened.complete_graceful_close().lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
}

// req: STO-032, REC-001, TXN-042
#[test]
fn graceful_close_preserves_complete_protected_source_and_refuses_pending_reschedule() {
    let directory = crate::test_path::ScopedDirectory::new("close-protected-commands");
    engine_compatibility::restore_fixture_bytes(
        &directory.join(""),
        include_str!("../../../fixtures/storage/redb-4.2.0-native-commands-and-backup.sparsehex"),
        &[
            "backup/database.redb",
            "backup/format.riffdb",
            "backup/journal.riffextent",
            "backup/manifest.riffdb",
            "db.redb",
            "db.redb.riffdb-format-v1",
            "expected-outcomes.hex",
        ],
    );
    let source_path = directory.join("db.redb");
    crate::RedbDurableFormatUpgrade::bind(&source_path, directory.join("backup"))
        .run()
        .unwrap();
    let expected_text = std::fs::read_to_string(directory.join("expected-outcomes.hex")).unwrap();
    assert!(expected_text.len() <= 64 * 1024);
    let lines: Vec<_> = expected_text.lines().take(3).collect();
    assert_eq!(lines.len(), 2, "two actual frozen command outcomes");
    let expected = lines
        .into_iter()
        .map(|line| {
            assert!(line.len() <= 32 * 1024 && line.len() % 2 == 0);
            let bytes: Vec<_> = line
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            codec::decode_stored_outcome_v1(&bytes)
                .unwrap()
                .into_parts()
                .0
        })
        .collect::<Vec<_>>();
    assert_eq!(expected[0].commit_sequence(), CommitSequence::first());
    let absent = expected[1].commit_sequence().checked_next().unwrap();
    let store = RedbStore::open(&source_path).unwrap();
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural_end = finish_structural(&mut session);
    let (catalog, historical_end) = validate_catalog_history(&mut session).unwrap().into_parts();
    assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) =
        session.finish(structural_end, historical_end).unwrap()
    else {
        panic!("genuine command fixture must validate completely");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant.into_operational_after_catalog_validation().unwrap();
    // Use the supported production admission path if this actual source starts Pending.
    // This is not a fixture completion helper or a fabricated directory.
    if let Some(waiter) = ports.initial_native_dependency().unwrap() {
        waiter.wait().unwrap();
    }
    let protected = ports
        .begin_composite_read()
        .expect("captured protected source");
    let capability = protected.locator_capability().clone();
    {
        let complete = capability
            .require_complete()
            .expect("genuine command construction");
        for outcome in &expected {
            assert!(complete.has_command_at(outcome.commit_sequence()).unwrap());
        }
        assert!(!complete.has_command_at(absent).unwrap());
    }
    let pending = crate::checkpoint_root::CheckpointRoot::new(
        ports.shared.database.begin_read().unwrap(),
        0x1615,
    )
    .unwrap()
    .retain_engine_and_history(ports.shared.database.clone(), None);
    let pending = crate::store::RedbReadAccess::Current(
        crate::checkpoint_root::CheckpointRootOwner::new(pending),
    );
    assert!(pending.locator_capability().is_pending());
    assert_eq!(
        ports.complete_graceful_close().lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    assert_eq!(
        crate::locator_builder::start_locator_builder(pending.clone(), None)
            .unwrap_err()
            .kind(),
        riffdb_storage_api::StorageErrorKind::Unavailable,
    );
    assert!(pending.locator_capability().is_pending());
    assert!(!pending.locator_capability().builder_active_for_close_test());
    drop(ports);
    assert!(
        RedbStore::open(&source_path).is_err(),
        "protected pins still retain the actual native writer lock"
    );
    {
        let complete = capability.require_complete().unwrap();
        for outcome in &expected {
            assert!(complete.has_command_at(outcome.commit_sequence()).unwrap());
        }
        assert!(!complete.has_command_at(absent).unwrap());
    }
    drop(protected);
    drop(pending);
    drop(capability);
    // The actual reopen below proves final native lock release after all protected pins retire.
    let reopened =
        open_cleanly(RedbStore::open(&source_path).expect("reopen after final protected pin"));
    assert_eq!(
        reopened.complete_graceful_close().lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
}

// req: STO-023, REC-001, REC-002, REC-004, PERF-007, PERF-013, PERF-019
#[test]
fn graceful_close_retains_exact_current_checkpoint_byte_for_byte() {
    let path = TestDatabasePath::new("graceful-exact-current-retention");
    let (store, _) = checkpointable_history_store(&path, database_id(0xb1), 4);
    let ports = open_cleanly(store);
    let before = checkpoint_storage_bytes(&ports);
    let epoch = ports.shared.durable_commit_epoch();

    let receipt = ports.complete_graceful_close();

    assert_eq!(
        receipt.disposition(),
        crate::GracefulCheckpointDispositionV1::RetainedExactCurrent
    );
    assert_eq!(
        receipt.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    assert_eq!(checkpoint_storage_bytes(&ports), before);
    assert_eq!(
        ports.shared.durable_commit_epoch(),
        epoch + 1,
        "CLEAN must be the sole database transaction after classification"
    );
}

// req: STO-023, REC-001, REC-002, REC-004, PERF-007, PERF-013, PERF-019
#[test]
fn graceful_close_leaves_unusable_checkpoint_state_unchanged() {
    let absent_path = TestDatabasePath::new("graceful-checkpoint-absent");
    let (absent_store, _) = checkpointable_history_store(&absent_path, database_id(0xb2), 3);
    let absent_ports = open_cleanly(absent_store);
    let write = absent_ports
        .shared
        .database
        .begin_write()
        .expect("remove checkpoint");
    write
        .open_table(META)
        .expect("meta")
        .remove(META_VALIDATED_PREFIX_CHECKPOINT)
        .expect("remove checkpoint metadata");
    write
        .open_table(crate::layout::VALIDATED_PREFIX_ENTITY_HEADS)
        .expect("checkpoint heads")
        .retain(|_, _| false)
        .expect("remove companion proof rows");
    write.commit().expect("commit absent fixture");
    let absent_before = checkpoint_storage_bytes(&absent_ports);
    let absent = absent_ports.complete_graceful_close();
    assert_eq!(
        absent.disposition(),
        crate::GracefulCheckpointDispositionV1::LeftAbsent
    );
    assert_eq!(
        absent.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    assert_eq!(checkpoint_storage_bytes(&absent_ports), absent_before);
    drop(absent_ports);
    assert_dirty_reopen_reaches_exact_ends(&absent_path);

    let invalid_path = TestDatabasePath::new("graceful-checkpoint-ineligible");
    let (invalid_store, _) = checkpointable_history_store(&invalid_path, database_id(0xb3), 3);
    let invalid_ports = open_cleanly(invalid_store);
    let write = invalid_ports
        .shared
        .database
        .begin_write()
        .expect("damage checkpoint");
    write
        .open_table(META)
        .expect("meta")
        .insert(
            META_VALIDATED_PREFIX_CHECKPOINT,
            b"bounded-invalid-checkpoint".as_slice(),
        )
        .expect("write malformed checkpoint");
    write.commit().expect("commit malformed fixture");
    let invalid_before = checkpoint_storage_bytes(&invalid_ports);
    let invalid = invalid_ports.complete_graceful_close();
    assert_eq!(
        invalid.disposition(),
        crate::GracefulCheckpointDispositionV1::LeftIneligible
    );
    assert_eq!(
        invalid.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    assert_eq!(checkpoint_storage_bytes(&invalid_ports), invalid_before);
    drop(invalid_ports);
    assert_dirty_reopen_reaches_exact_ends(&invalid_path);

    let unsupported_path = TestDatabasePath::new("graceful-checkpoint-unsupported");
    let (unsupported_store, _) =
        checkpointable_history_store(&unsupported_path, database_id(0xb7), 3);
    let unsupported_ports = open_cleanly(unsupported_store);
    let write = unsupported_ports
        .shared
        .database
        .begin_write()
        .expect("write unsupported checkpoint identity");
    {
        let mut meta = write.open_table(META).expect("meta");
        let unsupported = codec::encode_application_sequence_allocator_v1(
            ApplicationSequenceAllocator::next(CommitSequence::new(2).expect("second sequence")),
        )
        .expect("encode canonical but unsupported record identity");
        meta.insert(META_VALIDATED_PREFIX_CHECKPOINT, unsupported.as_bytes())
            .expect("replace with unsupported identity");
    }
    write.commit().expect("commit unsupported fixture");
    assert_ineligible_checkpoint_unchanged(&unsupported_path, unsupported_ports);

    let partial_path = TestDatabasePath::new("graceful-checkpoint-partial");
    let (partial_store, _) = checkpointable_history_store(&partial_path, database_id(0xb8), 3);
    let partial_ports = open_cleanly(partial_store);
    let write = partial_ports
        .shared
        .database
        .begin_write()
        .expect("write partial checkpoint");
    {
        let mut heads = write
            .open_table(crate::layout::VALIDATED_PREFIX_ENTITY_HEADS)
            .expect("checkpoint heads");
        let key = heads
            .iter()
            .expect("iterate checkpoint heads")
            .next()
            .expect("populated checkpoint")
            .expect("checkpoint row")
            .0
            .value()
            .to_vec();
        heads.remove(key.as_slice()).expect("remove companion row");
    }
    write.commit().expect("commit partial fixture");
    assert_ineligible_checkpoint_unchanged(&partial_path, partial_ports);

    let contradictory_path = TestDatabasePath::new("graceful-checkpoint-contradictory");
    let (contradictory_store, _) =
        checkpointable_history_store(&contradictory_path, database_id(0xb9), 3);
    let contradictory_ports = open_cleanly(contradictory_store);
    let write = contradictory_ports
        .shared
        .database
        .begin_write()
        .expect("write contradictory checkpoint");
    write
        .open_table(crate::layout::VALIDATED_PREFIX_ENTITY_HEADS)
        .expect("checkpoint heads")
        .insert(
            b"contradictory-extra-head".as_slice(),
            b"closed-fixture".as_slice(),
        )
        .expect("insert contradictory companion row");
    write.commit().expect("commit contradictory fixture");
    assert_ineligible_checkpoint_unchanged(&contradictory_path, contradictory_ports);

    let exhausted_path = TestDatabasePath::new("graceful-checkpoint-exhausted");
    let (exhausted_store, _) = checkpointable_history_store(&exhausted_path, database_id(0xba), 3);
    let exhausted_ports = open_cleanly(exhausted_store);
    let write = exhausted_ports
        .shared
        .database
        .begin_write()
        .expect("write exhausted retained checkpoint witness");
    {
        let mut meta = write.open_table(META).expect("meta");
        let encoded = meta
            .get(META_VALIDATED_PREFIX_CHECKPOINT)
            .expect("checkpoint lookup")
            .expect("checkpoint present")
            .value()
            .to_vec();
        let checkpoint =
            riffdb_storage_api::proto_codec::decode_validated_prefix_checkpoint_v2(&encoded)
                .expect("decode checkpoint")
                .into_parts()
                .0;
        let base = checkpoint.base();
        let retained = base.retained();
        let exhausted_base = riffdb_storage_api::StoredValidatedPrefixCheckpointV1::new(
            base.database_id(),
            base.history_incarnation(),
            base.registry_digest(),
            base.checkpoint_commit_sequence(),
            base.audit_sequence_bound(),
            base.counts(),
            base.entity_chain_fingerprint(),
            riffdb_storage_api::ValidatedPrefixRetainedSnapshot {
                next_application_sequence: 0,
                application_sequence_exhausted: true,
                next_administration_sequence: retained.next_administration_sequence,
                administration_sequence_exhausted: retained.administration_sequence_exhausted,
            },
            base.previous_checkpoint_hash(),
            base.retention_watermark_sequence(),
        )
        .expect("rehash exhausted retained snapshot");
        let exhausted = riffdb_storage_api::StoredValidatedPrefixCheckpointV2::new(
            exhausted_base,
            checkpoint.entity_counts(),
            checkpoint.entity_transition_fingerprint(),
        )
        .expect("rehash exhausted V2 checkpoint");
        let encoded =
            riffdb_storage_api::proto_codec::encode_validated_prefix_checkpoint_v2(&exhausted)
                .expect("encode exhausted retained checkpoint");
        meta.insert(META_VALIDATED_PREFIX_CHECKPOINT, encoded.as_bytes())
            .expect("replace checkpoint retained snapshot");
    }
    write.commit().expect("commit exhausted fixture");
    assert_ineligible_checkpoint_unchanged(&exhausted_path, exhausted_ports);

    let stale_path = TestDatabasePath::new("graceful-checkpoint-stale-witness");
    let (stale_store, _) = checkpointable_history_store(&stale_path, database_id(0xb4), 3);
    let stale_ports = open_cleanly(stale_store);
    assert_eq!(
        stale_ports.complete_graceful_close().lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    drop(stale_ports);
    let stale_ports = open_cleanly(RedbStore::open(&stale_path.0).expect("bounded reopen"));
    assert!(stale_ports.clean_close_fast_startup());
    let stale_before = checkpoint_storage_bytes(&stale_ports);
    let stale = stale_ports.complete_graceful_close();
    assert_eq!(
        stale.disposition(),
        crate::GracefulCheckpointDispositionV1::LeftStale
    );
    assert_eq!(
        stale.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    assert_eq!(checkpoint_storage_bytes(&stale_ports), stale_before);
    drop(stale_ports);
    assert_dirty_reopen_reaches_exact_ends(&stale_path);
}

// req: STO-023, REC-001, REC-002, REC-004, PERF-007, PERF-013, PERF-019
#[test]
fn graceful_checkpoint_close_crash_matrix_preserves_dirty_fallback() {
    for (ordinal, controller, expected) in [
        (
            0_u8,
            crate::RedbTestController::return_before_commit(
                crate::RedbTestOperation::GracefulCloseBarrier,
            ),
            crate::GracefulCheckpointDispositionV1::BarrierFailed,
        ),
        (
            1,
            crate::RedbTestController::return_before_commit(
                crate::RedbTestOperation::GracefulCloseBarrierSuffix,
            ),
            crate::GracefulCheckpointDispositionV1::BarrierFailed,
        ),
        (
            2,
            crate::RedbTestController::return_unknown_after_commit(
                crate::RedbTestOperation::GracefulCloseBarrier,
            ),
            crate::GracefulCheckpointDispositionV1::BarrierFailed,
        ),
        (
            3,
            crate::RedbTestController::return_before_commit(
                crate::RedbTestOperation::GracefulCheckpointClassification,
            ),
            crate::GracefulCheckpointDispositionV1::ClassificationFailed,
        ),
        (
            4,
            crate::RedbTestController::return_unknown_after_commit(
                crate::RedbTestOperation::GracefulCheckpointClassification,
            ),
            crate::GracefulCheckpointDispositionV1::ClassificationFailed,
        ),
    ] {
        let path = TestDatabasePath::new(&format!("graceful-stage-failure-{ordinal}"));
        let (store, _) = checkpointable_history_store(&path, database_id(0xc0 + ordinal), 4);
        let ports = open_cleanly(store);
        ports.write_clean_close_lifecycle().expect("seed CLEAN");
        drop(ports);
        let ports = open_cleanly(
            RedbStore::open_with_test_controller(&path.0, controller)
                .expect("open with graceful-stage failure"),
        );
        finish_locator_builder_before_uncertified_drop(&ports);
        let checkpoint_before = checkpoint_storage_bytes(&ports);
        let authority_before = allocator_bytes(&ports);
        let receipt = ports.complete_graceful_close();
        assert_eq!(receipt.disposition(), expected);
        assert_eq!(
            receipt.lifecycle(),
            crate::GracefulLifecycleOutcomeV1::CleanNotAttempted
        );
        assert_eq!(checkpoint_storage_bytes(&ports), checkpoint_before);
        assert_eq!(allocator_bytes(&ports), authority_before);
        drop(ports);

        let reopened = RedbStore::open(&path.0).expect("dirty stage-failure reopen");
        let mut session = reopened
            .begin_structural_evidence(inputs())
            .expect("dirty stage-failure session");
        assert!(!session.clean_close_fast_path());
        let structural_end = finish_structural(&mut session);
        let historical_end = drain_historical(&mut session);
        assert!(matches!(
            session
                .finish(structural_end, historical_end)
                .expect("stage-failure exact ends"),
            StructuralOpenOutcome::Clean(_)
        ));
    }

    let before_path = TestDatabasePath::new("graceful-clean-before-commit");
    let (store, _) = checkpointable_history_store(&before_path, database_id(0xb5), 4);
    let ports = open_cleanly(store);
    ports.write_clean_close_lifecycle().expect("seed CLEAN");
    drop(ports);
    let controller = crate::RedbTestController::return_before_commit(
        crate::RedbTestOperation::CleanCloseLifecycle,
    );
    let ports = open_cleanly(
        RedbStore::open_with_test_controller(&before_path.0, controller)
            .expect("open with before-commit failure"),
    );
    finish_locator_builder_before_uncertified_drop(&ports);
    let checkpoint_before = checkpoint_storage_bytes(&ports);
    let authority_before = allocator_bytes(&ports);
    let receipt = ports.complete_graceful_close();
    assert_eq!(
        receipt.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanFailed
    );
    assert_eq!(checkpoint_storage_bytes(&ports), checkpoint_before);
    assert_eq!(allocator_bytes(&ports), authority_before);
    drop(ports);
    let reopened = RedbStore::open(&before_path.0).expect("dirty fallback reopen");
    let mut session = reopened
        .begin_structural_evidence(inputs())
        .expect("dirty fallback session");
    assert!(!session.clean_close_fast_path());
    // The failed graceful close left no clean certificate, so this reopen is
    // the ADR-0182 bounded dirty path: it admits only because the active
    // validated-prefix checkpoint verified at load, and it still reaches
    // the exact ends below.
    assert!(session.bounded_dirty_startup_path());
    let structural_end = finish_structural(&mut session);
    let historical_end = drain_historical(&mut session);
    assert!(matches!(
        session
            .finish(structural_end, historical_end)
            .expect("dirty fallback exact ends"),
        StructuralOpenOutcome::Clean(_)
    ));

    let after_path = TestDatabasePath::new("graceful-clean-after-commit");
    let (store, _) = checkpointable_history_store(&after_path, database_id(0xb6), 4);
    let ports = open_cleanly(store);
    ports.write_clean_close_lifecycle().expect("seed CLEAN");
    drop(ports);
    let controller = crate::RedbTestController::return_unknown_after_commit(
        crate::RedbTestOperation::CleanCloseLifecycle,
    );
    let ports = open_cleanly(
        RedbStore::open_with_test_controller(&after_path.0, controller)
            .expect("open with after-commit uncertainty"),
    );
    finish_locator_builder_before_uncertified_drop(&ports);
    let checkpoint_before = checkpoint_storage_bytes(&ports);
    let authority_before = allocator_bytes(&ports);
    let receipt = ports.complete_graceful_close();
    assert_eq!(
        receipt.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanUnknown
    );
    assert_eq!(checkpoint_storage_bytes(&ports), checkpoint_before);
    assert_eq!(allocator_bytes(&ports), authority_before);
    drop(ports);
    let reopened = RedbStore::open(&after_path.0).expect("atomic CLEAN reopen");
    let mut session = reopened
        .begin_structural_evidence(inputs())
        .expect("bounded successor session");
    assert!(session.clean_close_fast_path());
    let structural_end = finish_structural(&mut session);
    let historical_end = drain_historical(&mut session);
    assert!(matches!(
        session
            .finish(structural_end, historical_end)
            .expect("clean successor exact ends"),
        StructuralOpenOutcome::Clean(_)
    ));
}

// req: STO-023, REC-001, REC-002, REC-004, PERF-007, PERF-013, PERF-019
#[test]
fn graceful_checkpoint_receipt_is_closed_and_redacted() {
    use crate::validated_prefix::{
        AttemptedGracefulLifecycleOutcome as Attempted,
        SuccessfulGracefulCheckpointDisposition as Successful,
    };
    use crate::{
        GracefulCheckpointCloseReceiptV1 as Receipt,
        GracefulCheckpointDispositionV1 as Disposition, GracefulLifecycleOutcomeV1 as Outcome,
    };

    for disposition in [
        Successful::RetainedExactCurrent,
        Successful::LeftAbsent,
        Successful::LeftStale,
        Successful::LeftIneligible,
    ] {
        for lifecycle in [Attempted::Committed, Attempted::Failed, Attempted::Unknown] {
            let production = Receipt::completed(disposition, lifecycle, [0; 3]);
            assert!(production.disposition().is_successful());
            assert_ne!(production.lifecycle(), Outcome::CleanNotAttempted);
        }
    }
    for production in [
        Receipt::barrier_failed([0; 3]),
        Receipt::classification_failed([0; 3]),
    ] {
        assert!(!production.disposition().is_successful());
        assert_eq!(production.lifecycle(), Outcome::CleanNotAttempted);
    }

    let valid = Receipt::new(
        Disposition::LeftIneligible,
        Outcome::CleanUnknown,
        [1, u64::MAX, 3],
    )
    .expect("successful disposition may pair with a CLEAN attempt");
    assert_eq!(
        valid.format_v1_line(),
        "riffdb-graceful-checkpoint-close-v1\tleft_ineligible\tclean_unknown\t1,18446744073709551615,3"
    );
    assert!(Receipt::new(Disposition::BarrierFailed, Outcome::CleanCommitted, [0; 3]).is_err());
    assert!(
        Receipt::new(
            Disposition::ClassificationFailed,
            Outcome::CleanFailed,
            [0; 3]
        )
        .is_err()
    );
    assert!(
        Receipt::new(
            Disposition::RetainedExactCurrent,
            Outcome::CleanNotAttempted,
            [0; 3]
        )
        .is_err()
    );
    let line = valid.format_v1_line();
    for forbidden in [
        "/",
        "database_id",
        "frontier",
        "hash",
        "key",
        "value",
        "row_count",
    ] {
        assert!(
            !line.contains(forbidden),
            "receipt leaked forbidden class {forbidden}"
        );
    }
}

type CheckpointStorageBytes = (Option<Vec<u8>>, Vec<(Vec<u8>, Vec<u8>)>);

fn checkpoint_storage_bytes(ports: &crate::RedbOperationalPorts) -> CheckpointStorageBytes {
    let read = ports.shared.database.begin_read().expect("checkpoint read");
    let checkpoint = read
        .open_table(META)
        .expect("meta")
        .get(META_VALIDATED_PREFIX_CHECKPOINT)
        .expect("checkpoint lookup")
        .map(|value| value.value().to_vec());
    let heads = read
        .open_table(crate::layout::VALIDATED_PREFIX_ENTITY_HEADS)
        .expect("checkpoint heads")
        .iter()
        .expect("iterate checkpoint heads")
        .map(|row| {
            let (key, value) = row.expect("checkpoint head row");
            (key.value().to_vec(), value.value().to_vec())
        })
        .collect();
    (checkpoint, heads)
}

fn assert_ineligible_checkpoint_unchanged(
    path: &TestDatabasePath,
    ports: crate::RedbOperationalPorts,
) {
    let before = checkpoint_storage_bytes(&ports);
    let receipt = ports.complete_graceful_close();
    assert_eq!(
        receipt.disposition(),
        crate::GracefulCheckpointDispositionV1::LeftIneligible
    );
    assert_eq!(
        receipt.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    assert_eq!(checkpoint_storage_bytes(&ports), before);
    drop(ports);
    assert_dirty_reopen_reaches_exact_ends(path);
}

fn assert_dirty_reopen_reaches_exact_ends(path: &TestDatabasePath) {
    let activated = open_cleanly(RedbStore::open(&path.0).expect("consume clean lifecycle"));
    finish_locator_builder_before_uncertified_drop(&activated);
    drop(activated);

    let reopened = RedbStore::open(&path.0).expect("dirty unusable-checkpoint reopen");
    let mut session = reopened
        .begin_structural_evidence(inputs())
        .expect("begin dirty exact fallback");
    assert!(!session.clean_close_fast_path());
    let structural_end = finish_structural(&mut session);
    let historical_end = drain_historical(&mut session);
    assert!(matches!(
        session
            .finish(structural_end, historical_end)
            .expect("unusable-checkpoint exact ends"),
        StructuralOpenOutcome::Clean(_)
    ));
}

/// These fixtures deliberately omit graceful certification to exercise DIRTY
/// fallback. Finish only their original nondurable builder before raw Drop;
/// this writes no CLEAN record and changes no checkpoint or allocator bytes.
fn finish_locator_builder_before_uncertified_drop(ports: &crate::RedbOperationalPorts) {
    let original = ports
        .begin_composite_read()
        .expect("capture original startup source");
    if original.locator_capability().is_pending() {
        crate::locator_builder::start_locator_builder(
            original.clone(),
            Some(std::sync::Arc::clone(&ports.shared)),
        )
        .expect("request original admitted construction");
    }
    original
        .locator_capability()
        .wait_for_builder_for_test()
        .expect("release the original builder's source before deliberate uncertified Drop");
}

fn allocator_bytes(ports: &crate::RedbOperationalPorts) -> (Vec<u8>, Vec<u8>) {
    let read = ports.shared.database.begin_read().expect("allocator read");
    let meta = read.open_table(META).expect("meta");
    let application = meta
        .get(crate::layout::META_APPLICATION_SEQUENCE)
        .expect("application allocator lookup")
        .expect("application allocator")
        .value()
        .to_vec();
    let administration = meta
        .get(crate::layout::META_ADMINISTRATION_SEQUENCE)
        .expect("administration allocator lookup")
        .expect("administration allocator")
        .value()
        .to_vec();
    (application, administration)
}

// A genuine supported native-command database supplies the exact full source.
// Hook arming happens only AFTER its supported initial construction has finished.
#[cfg(feature = "test-fixtures")]
fn genuine_close_schedule_source(
    controller: Option<crate::RedbTestController>,
) -> (
    crate::test_path::ScopedDirectory,
    std::path::PathBuf,
    crate::RedbOperationalPorts,
) {
    let directory = crate::test_path::ScopedDirectory::new("close-genuine-command-schedule");
    engine_compatibility::restore_fixture_bytes(
        &directory.join(""),
        include_str!("../../../fixtures/storage/redb-4.2.0-native-commands-and-backup.sparsehex"),
        &[
            "backup/database.redb",
            "backup/format.riffdb",
            "backup/journal.riffextent",
            "backup/manifest.riffdb",
            "db.redb",
            "db.redb.riffdb-format-v1",
            "expected-outcomes.hex",
        ],
    );
    let path = directory.join("db.redb");
    crate::RedbDurableFormatUpgrade::bind(&path, directory.join("backup"))
        .run()
        .unwrap();
    let store = match controller {
        Some(controller) => RedbStore::open_with_test_controller(&path, controller).unwrap(),
        None => RedbStore::open(&path).unwrap(),
    };
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural_end = finish_structural(&mut session);
    let (catalog, historical_end) = validate_catalog_history(&mut session).unwrap().into_parts();
    assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) =
        session.finish(structural_end, historical_end).unwrap()
    else {
        panic!("genuine command fixture must validate completely");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant.into_operational_after_catalog_validation().unwrap();
    if let Some(waiter) = ports.initial_native_dependency().unwrap() {
        waiter.wait().unwrap();
    }
    {
        let expected_text =
            std::fs::read_to_string(directory.join("expected-outcomes.hex")).unwrap();
        assert!(expected_text.len() <= 64 * 1024);
        let lines: Vec<_> = expected_text.lines().take(3).collect();
        assert_eq!(lines.len(), 2);
        let expected = lines
            .into_iter()
            .map(|line| {
                assert!(line.len() <= 32 * 1024 && line.len() % 2 == 0);
                let bytes: Vec<_> = line
                    .as_bytes()
                    .chunks_exact(2)
                    .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                    .collect();
                codec::decode_stored_outcome_v1(&bytes)
                    .unwrap()
                    .into_parts()
                    .0
            })
            .collect::<Vec<_>>();
        let current = ports.begin_composite_read().unwrap();
        let complete = current.locator_capability().require_complete().unwrap();
        for outcome in &expected {
            assert!(complete.has_command_at(outcome.commit_sequence()).unwrap());
        }
        let absent = expected[1].commit_sequence().checked_next().unwrap();
        assert!(!complete.has_command_at(absent).unwrap());
    }
    (directory, path, ports)
}

#[cfg(feature = "test-fixtures")]
fn independent_pending_close_capture(
    ports: &crate::RedbOperationalPorts,
    identity: u64,
) -> crate::store::RedbReadAccess {
    let root = crate::checkpoint_root::CheckpointRoot::new(
        ports.shared.database.begin_read().unwrap(),
        identity,
    )
    .unwrap()
    .retain_engine_and_history(ports.shared.database.clone(), None);
    let access = crate::store::RedbReadAccess::Current(
        crate::checkpoint_root::CheckpointRootOwner::new(root),
    );
    assert!(access.locator_capability().is_pending());
    access
}

// req: STO-032, REC-001, TXN-042
#[cfg(feature = "test-fixtures")]
#[test]
fn graceful_close_drains_builder_waiting_for_publication_mutation_lease() {
    use crate::hooks::{RedbCheckpointTestPoint, RedbCheckpointTestSchedule};
    struct Release {
        schedule: Option<RedbCheckpointTestSchedule>,
        lease: Option<crate::gate::ExclusiveLease>,
        close: Option<
            std::thread::JoinHandle<(
                crate::RedbOperationalPorts,
                crate::GracefulCheckpointCloseReceiptV1,
            )>,
        >,
    }
    impl Drop for Release {
        fn drop(&mut self) {
            drop(self.schedule.take());
            drop(self.lease.take());
            if let Some(close) = self.close.take() {
                let _ = close.join();
            }
        }
    }
    let (controller, schedule) = crate::RedbTestController::checkpoint_schedule();
    let (_directory, path, ports) = genuine_close_schedule_source(Some(controller));
    let mut release = Release {
        schedule: Some(schedule),
        lease: None,
        close: None,
    };
    let pending = independent_pending_close_capture(&ports, 0x1633);
    let capability = pending.locator_capability().clone();
    let domain = ports.shared.database.locator_build_domain().clone();
    let gate = ports.shared.locator_close_gate_for_test();
    let before = checkpoint_storage_bytes(&ports);
    {
        let schedule = release.schedule.as_ref().unwrap();
        schedule.arm(RedbCheckpointTestPoint::BeforeLocatorPublicationLease);
        schedule.arm(RedbCheckpointTestPoint::LocatorCloseDrainWaiting);
        schedule.arm(RedbCheckpointTestPoint::BeforeFinalCleanCommit);
    }
    crate::locator_builder::start_locator_builder(pending.clone(), Some(Arc::clone(&ports.shared)))
        .unwrap();
    eprintln!("locator close schedule phase=await_validated_publication");
    release
        .schedule
        .as_ref()
        .unwrap()
        .wait(RedbCheckpointTestPoint::BeforeLocatorPublicationLease);
    release.lease = Some(gate.acquire().unwrap());
    release
        .schedule
        .as_ref()
        .unwrap()
        .release(RedbCheckpointTestPoint::BeforeLocatorPublicationLease);
    gate.wait_for_queued(1);
    assert_eq!(domain.registrations_for_test(), 1);
    eprintln!("locator close schedule phase=worker_has_real_queued_mutation_ticket");
    release.close = Some(std::thread::spawn(move || {
        let receipt = ports.complete_graceful_close();
        (ports, receipt)
    }));
    let schedule = release.schedule.as_ref().unwrap();
    schedule.wait(RedbCheckpointTestPoint::LocatorCloseDrainWaiting);
    assert!(capability.builder_active_for_close_test());
    // The paused close owns the registration predicate mutex. Observe only
    // the lock-free active bit here, then release its real atomic wait.
    schedule.release(RedbCheckpointTestPoint::LocatorCloseDrainWaiting);
    drop(release.lease.take());
    schedule.wait(RedbCheckpointTestPoint::BeforeFinalCleanCommit);
    assert!(!capability.builder_active_for_close_test());
    assert_eq!(domain.registrations_for_test(), 0);
    assert!(
        capability.is_pending(),
        "close cancellation must not fabricate completion"
    );
    drop(release.schedule.take());
    let (ports, receipt) = release.close.take().unwrap().join().unwrap();
    assert_eq!(
        receipt.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted,
        "{receipt:?}"
    );
    assert_eq!(checkpoint_storage_bytes(&ports), before);
    drop(pending);
    drop(ports);
    // The actual reopen below proves final native lock release after all protected pins retire.
    let reopened =
        open_cleanly(RedbStore::open(&path).expect("single reopen after publication drain"));
    // The original close already proved source drain, exact CLEAN and final
    // engine release. This new cold open has its own admitted construction;
    // finish the supported native dependency before testing its separate close.
    if let Some(waiter) = reopened.initial_native_dependency().unwrap() {
        waiter.wait().unwrap();
    }
    let reopened_receipt = reopened.complete_graceful_close();
    assert_eq!(
        reopened_receipt.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted,
        "{reopened_receipt:?}"
    );
}

// req: STO-032, REC-001, TXN-042
#[cfg(feature = "test-fixtures")]
#[test]
fn graceful_close_extracts_global_queued_work_without_waiting_for_other_engine() {
    use crate::hooks::{RedbCheckpointTestPoint, RedbCheckpointTestSchedule};
    struct Release(Option<RedbCheckpointTestSchedule>);
    impl Drop for Release {
        fn drop(&mut self) {
            drop(self.0.take());
        }
    }
    let (_target_directory, target_path, target) = genuine_close_schedule_source(None);
    let (controller, schedule) = crate::RedbTestController::checkpoint_schedule();
    let (_other_directory, _other_path, other) = genuine_close_schedule_source(Some(controller));
    let mut release = Release(Some(schedule));
    release
        .0
        .as_ref()
        .unwrap()
        .arm(RedbCheckpointTestPoint::BeforeLocatorJobSourceRelease);
    let other_pending = independent_pending_close_capture(&other, 0x1634);
    let other_capability = other_pending.locator_capability().clone();
    let other_domain = other.shared.database.locator_build_domain().clone();
    crate::locator_builder::start_locator_builder(
        other_pending.clone(),
        Some(Arc::clone(&other.shared)),
    )
    .unwrap();
    release
        .0
        .as_ref()
        .unwrap()
        .wait(RedbCheckpointTestPoint::BeforeLocatorJobSourceRelease);
    let pending = independent_pending_close_capture(&target, 0x1635);
    let capability = pending.locator_capability().clone();
    let domain = target.shared.database.locator_build_domain().clone();
    let before = checkpoint_storage_bytes(&target);
    crate::locator_builder::start_locator_builder(
        pending.clone(),
        Some(Arc::clone(&target.shared)),
    )
    .unwrap();
    assert!(crate::locator_builder::global_queue_contains_source_for_test(&pending));
    assert!(capability.builder_active_for_close_test());
    assert_eq!(domain.registrations_for_test(), 1);
    let receipt = target.complete_graceful_close();
    assert_eq!(
        receipt.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted,
        "{receipt:?}"
    );
    assert_eq!(checkpoint_storage_bytes(&target), before);
    assert!(!crate::locator_builder::global_queue_contains_source_for_test(&pending));
    assert_eq!(domain.registrations_for_test(), 0);
    assert!(!capability.builder_active_for_close_test());
    assert!(capability.is_pending());
    assert!(other_capability.builder_active_for_close_test());
    assert_eq!(other_domain.registrations_for_test(), 1);
    assert!(!other_domain.is_cancelled());
    // The target completed while the other database's actual worker stayed paused.
    drop(release.0.take());
    let other_receipt = other.complete_graceful_close();
    assert_eq!(
        other_receipt.lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted,
        "{other_receipt:?}"
    );
    drop(other_pending);
    drop(other);
    drop(target);
    assert!(
        RedbStore::open(&target_path).is_err(),
        "protected pending source still retains the actual native writer lock"
    );
    drop(pending);
    drop(capability);
    // The actual reopen below proves final native lock release after all protected pins retire.
    let reopened = open_cleanly(
        RedbStore::open(&target_path).expect("single reopen after queued job extraction"),
    );
    if let Some(waiter) = reopened.initial_native_dependency().unwrap() {
        waiter.wait().unwrap();
    }
    assert_eq!(
        reopened.complete_graceful_close().lifecycle(),
        crate::GracefulLifecycleOutcomeV1::CleanCommitted
    );
}
