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
                ApplicationSequenceAllocator::next(
                    CommitSequence::new(2).expect("second sequence"),
                ),
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
        let (exhausted_store, _) =
            checkpointable_history_store(&exhausted_path, database_id(0xba), 3);
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
        let checkpoint = riffdb_storage_api::proto_codec::decode_validated_prefix_checkpoint_v2(
            &encoded,
        )
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
        let encoded = riffdb_storage_api::proto_codec::encode_validated_prefix_checkpoint_v2(
            &exhausted,
        )
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
        assert!(session.checkpoint_verified());
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
