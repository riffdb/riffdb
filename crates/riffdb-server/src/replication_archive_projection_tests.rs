//! Exact interior stops rebuild derived rows at the retained control frontier.
// req: REP-007, PRJ-001, REC-001
use super::*;
use command_support::{
    RecordingApplicationCommitNotifications, start_group_coordinator_with_notifications,
};
use riffdb_storage_redb::{RedbMaintenanceStorage, RedbOfflineBackup, RedbVerifiedArchiveBackup};
use std::{num::NonZeroU16, time::Duration};

struct Rebuild;
impl riffdb_storage_redb::RedbArchiveProjectionRebuild for Rebuild {
    fn rebuild(
        &self,
        session: riffdb_storage_redb::RedbFollowerRecoveryCatalogSession,
        cancellation: Arc<AtomicBool>,
    ) -> Result<riffdb_storage_redb::RedbFollowerProjectionRecovery, StorageError> {
        rebuild_follower_projection_session(session, cancellation)
    }
}

fn run(
    database: &BudgetDatabase,
    preparation: riffdb_commit::CommandExecutionPreparation,
    seed: u8,
) {
    let coordinator = start_coordinator(
        database.open(),
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(seed)),
    );
    let executor = coordinator.command_executor();
    let result = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .unwrap()
            .submit(preparation)
            .unwrap()
            .completion()
            .await
            .unwrap()
    });
    assert!(matches!(
        result,
        riffdb_commit::CommandExecutionResult::Committed(_)
    ));
    drop(executor);
    coordinator.shutdown().unwrap();
}

fn apply_through(database: &BudgetDatabase, schema: &CheckedProjectionSchema, through: u64) {
    let mut ports = database.open();
    let resolved = riffdb_catalog::ActiveCatalogSnapshot::read(&ports)
        .unwrap()
        .unwrap()
        .resolve_projection(schema.identity())
        .unwrap();
    let commits = ports
        .scan_commits(CommitScanRequest::initial(
            StorageScanLimit::new(8).unwrap(),
        ))
        .unwrap();
    let after = match control(&ports)
        .frontier_for(ProjectionGeneration::first())
        .unwrap()
    {
        FrontierPosition::BeforeFirst => 0,
        FrontierPosition::AppliedThrough(sequence) => sequence.get(),
    };
    for item in commits.records() {
        if item.value().commit_sequence().get() <= after {
            continue;
        }
        if item.value().commit_sequence().get() > through {
            break;
        }
        let apply = evaluate_and_prepare_projection_commit(
            &resolved,
            schema.clone(),
            ProjectionGeneration::first(),
            item.value(),
            &ports,
        )
        .unwrap();
        ports.apply_projection(&apply).unwrap();
    }
}

fn control(ports: &riffdb_storage_redb::RedbOperationalPorts) -> StoredProjectionControlV1 {
    let page = ports
        .scan_projection_controls(
            None,
            ProjectionRecoveryPageLimit::new(NonZeroU16::new(8).unwrap()).unwrap(),
        )
        .unwrap();
    assert_eq!(page.controls().len(), 1);
    page.controls()[0].value().clone()
}

fn assert_projection(result: &ProjectionQueryResult, sequence: u64, amounts: &[i128]) {
    let ProjectionQueryResult::Ready {
        frontier,
        rows,
        next,
        ..
    } = result
    else {
        panic!("published projection must be ready")
    };
    assert_eq!(
        *frontier,
        FrontierPosition::AppliedThrough(CommitSequence::new(sequence).unwrap())
    );
    assert!(next.is_none());
    let actual: Vec<_> = rows
        .iter()
        .map(|row| {
            let [(_, CanonicalValue::Decimal(amount))] = row.value().measures().fields() else {
                panic!("one decimal sum")
            };
            amount.coefficient()
        })
        .collect();
    assert_eq!(actual, amounts);
}

#[test]
fn archive_interior_stop_rebuilds_projection_without_later_rows_or_control_advances() {
    let database = BudgetDatabase::create_with_setup("archive-projection-interior", bootstrap);
    for seed in [0x61, 0x62] {
        let ports = database.open();
        let command = database.prepare_budget_for(&ports, [seed; 16], seed);
        drop(ports);
        run(&database, command, seed);
    }
    let ports = database.open();
    let command = database.prepare_allocation_for(&ports, [0x61; 16], 1000, 0x63);
    drop(ports);
    run(&database, command, 0x63);
    let schema = CheckedProjectionSchema::new(
        database
            .bundle()
            .bundle()
            .bound_projection_group_schema(
                database.bundle().bundle().projections()[0].projection_id(),
            )
            .unwrap(),
    );
    let mut ports = database.open();
    let ProjectionControlResult::Updated(control) = ports
        .transition_projection_control(ProjectionControlOperation::CreateInitial {
            schema: schema.clone(),
        })
        .unwrap()
    else {
        panic!("create")
    };
    let ProjectionControlResult::Updated(_) = ports
        .transition_projection_control(ProjectionControlOperation::StartInitialScan {
            expected: control,
        })
        .unwrap()
    else {
        panic!("scan")
    };
    drop(ports);
    apply_through(&database, &schema, 3);
    let mut ports = database.open();
    let current = self::control(&ports);
    let ProjectionControlResult::Updated(control) = ports
        .transition_projection_control(ProjectionControlOperation::PublishCandidate {
            expected: current,
        })
        .unwrap()
    else {
        panic!("publish")
    };
    let query = ProjectionQueryRequest::new(
        ProjectionQuerySelector::new(schema.clone(), vec![]).unwrap(),
        NonZeroU16::new(8).unwrap(),
        None,
    )
    .unwrap();
    let expected_projection = ports.query_projection(&query).unwrap();
    assert_projection(&expected_projection, 3, &[1000]);
    let expected_control = control.clone();
    let excluded_target = ports
        .read_commit(CommitSequence::new(2).unwrap())
        .unwrap()
        .unwrap()
        .entity_references()[0]
        .target()
        .clone();
    let excluded_baseline = ports.read_entity(&excluded_target).unwrap().unwrap();
    drop(ports);
    let root = tempfile::tempdir().unwrap();
    let backups = root.path().join("backups");
    std::fs::create_dir(&backups).unwrap();
    let backup = backups.join("baseline");
    RedbOfflineBackup::bind(database.path(), &backup)
        .create_offline_backup(
            &BackupBuildMetadataV1::new("0.1.0", "0123456789abcdef", "rustc-1.97.0", 1, vec![])
                .unwrap(),
        )
        .unwrap();
    let binding = RedbVerifiedArchiveBackup::open(&backup).unwrap();
    let history = binding.history();
    let archive = root.path().join("archive");
    let repository = binding
        .open_archive(&archive, ArchiveEncryptionPostureV1::Unencrypted)
        .unwrap();
    drop(binding);

    // Hold storage admission while a separate command enters the coordinator.
    // Both allocations are then queued before release, with disjoint conflicts.
    let ports = database.open();
    let blocker_command = database.prepare_budget_for(&ports, [0x64; 16], 0x64);
    let first = database.prepare_allocation_for(&ports, [0x61; 16], 500, 0x65);
    let second = database.prepare_allocation_for(&ports, [0x62; 16], 700, 0x66);
    let blocker = ports.begin_empty_batch().unwrap();
    let (clock, entered) = FixedAdmissionClock::observed(command_timestamp());
    let coordinator = start_group_coordinator_with_notifications(
        ports,
        Arc::new(clock),
        Arc::new(IncrementingProvenanceSource::new(0x71)),
        Arc::new(RecordingApplicationCommitNotifications::default()),
    );
    let executor = coordinator.command_executor();
    runtime().block_on(async {
        let guard = executor
            .reserve_capacity()
            .await
            .unwrap()
            .submit(blocker_command)
            .unwrap();
        if let Err(error) = entered.recv_timeout(Duration::from_secs(30)) {
            blocker.rollback();
            panic!("coordinator did not reach storage admission: {error}");
        }
        let first = executor
            .reserve_capacity()
            .await
            .unwrap()
            .submit(first)
            .unwrap();
        let second = executor
            .reserve_capacity()
            .await
            .unwrap()
            .submit(second)
            .unwrap();
        blocker.rollback();
        for completion in [guard, first, second] {
            assert!(matches!(
                completion.completion().await.unwrap(),
                riffdb_commit::CommandExecutionResult::Committed(_)
            ));
        }
    });
    drop(executor);
    coordinator.shutdown().unwrap();
    apply_through(&database, &schema, 6);
    let ports = database.open();
    assert_projection(&ports.query_projection(&query).unwrap(), 6, &[1500, 700]);
    let included_commit = ports
        .read_commit(CommitSequence::new(5).unwrap())
        .unwrap()
        .unwrap();
    let included_target = included_commit.entity_references()[0].target().clone();
    let included_entity = ports.read_entity(&included_target).unwrap().unwrap();
    let pin = ports.published_changelog_snapshot_v3().unwrap();
    let mut cursor = ChangelogFrameCursorV3::open(
        pin.as_ref(),
        ReplicationHandshakeV3::new(
            history.lineage(),
            history.tail(),
            ChangelogFrameV3::IDENTITY,
            history.lineage().catalog_digest(),
            MAX_CHANGELOG_FRAME_BYTES as u64,
            MAX_STAGED_COMMANDS as u64,
        )
        .unwrap(),
    )
    .unwrap();
    let mut consumer = ArchiveConsumerV1::new(repository, history.lineage(), history.tail());
    consumer.begin_stream().unwrap();
    let mut grouped = false;
    while let Some(frame) = cursor.next_coalesced_frame().unwrap() {
        let decoded = ChangelogFrameV3::decode(frame.as_bytes()).unwrap();
        grouped |= decoded.receipts().iter().any(|r| {
            r.binding().predecessor_frontier.application() == CommitSequence::new(4)
                && r.binding().covered_frontier.application() == CommitSequence::new(6)
        });
        consumer.append(frame.into_bytes()).unwrap();
    }
    assert!(
        grouped,
        "the requested stop must be inside one physical receipt"
    );
    drop(consumer);
    drop(cursor);
    drop(pin);
    drop(ports);

    let target = root.path().join("restored.redb");
    let (mut maintenance, _) = RedbMaintenanceStorage::open(&target, &backups).unwrap();
    let operation =
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1000, [0x77; 10]).unwrap();
    let choice = ArchiveRestoreStopV1::AtApplicationSequence(CommitSequence::new(5).unwrap());
    let rejected_id =
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1000, [0x76; 10]).unwrap();
    let repository = RedbVerifiedArchiveBackup::open(&backup)
        .unwrap()
        .open_archive(&archive, ArchiveEncryptionPostureV1::Unencrypted)
        .unwrap();
    let incomplete = maintenance
        .stage_recovery_restore_candidate(
            rejected_id,
            &BackupNameV1::new("baseline").unwrap(),
            inputs(),
        )
        .unwrap()
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();
    let incomplete_path = incomplete.staged_database_file().to_path_buf();
    assert_eq!(
        incomplete
            .prepare_restore(choice, inputs(), Arc::new(AtomicBool::new(false)))
            .err()
            .unwrap()
            .kind(),
        StorageErrorKind::CorruptData
    );
    assert!(!incomplete_path.exists());
    assert!(!target.exists());

    let repository = RedbVerifiedArchiveBackup::open(&backup)
        .unwrap()
        .open_archive(&archive, ArchiveEncryptionPostureV1::Unencrypted)
        .unwrap();
    let stage = maintenance
        .stage_recovery_restore_candidate(
            operation,
            &BackupNameV1::new("baseline").unwrap(),
            inputs(),
        )
        .unwrap()
        .begin_archive_replay(repository, &AtomicBool::new(false))
        .unwrap();

    let prepared = stage
        .prepare_restore_with_projection_rebuild(
            choice,
            inputs(),
            Arc::new(AtomicBool::new(false)),
            &Rebuild,
        )
        .unwrap();
    assert_eq!(
        prepared.restored_frontier().application(),
        CommitSequence::new(5)
    );
    assert_eq!(
        prepared.selection().terminal_frontier().application(),
        CommitSequence::new(6)
    );
    let snapshot = prepared.authorization_snapshot().unwrap();
    assert_eq!(
        snapshot
            .read_commit(CommitSequence::new(5).unwrap())
            .unwrap(),
        Some(included_commit)
    );
    assert_eq!(
        snapshot.read_entity(&included_target).unwrap(),
        Some(included_entity)
    );
    assert_eq!(
        snapshot.read_entity(&excluded_target).unwrap(),
        Some(excluded_baseline)
    );
    assert!(
        snapshot
            .read_commit(CommitSequence::new(5).unwrap())
            .unwrap()
            .is_some()
    );
    assert!(
        snapshot
            .read_commit(CommitSequence::new(6).unwrap())
            .unwrap()
            .is_none()
    );
    drop(snapshot);
    let name = BackupNameV1::new("baseline").unwrap();
    let archive_name = ArchiveNameV1::new("daily").unwrap();
    let confirmation = OfflineMaintenanceReplacementConfirmation::NotProvided;
    let mut receipt = OfflineMaintenanceReceiptV3::accepted_archive_restore(
        operation,
        name.clone(),
        archive_name.clone(),
        choice,
        archive_restore_input_hash(&name, &archive_name, choice, confirmation),
        confirmation,
        OfflineMaintenanceAdmissionV1::new(
            ActorId::new("operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1000, [0x78; 10]).unwrap(),
            None,
        ),
        None,
    )
    .unwrap();
    let database_id = prepared.selection().lineage().database_id();
    receipt
        .record_selection(prepared.selection().clone())
        .unwrap();
    receipt
        .record_validated_restore(database_id, prepared.restored_frontier())
        .unwrap();
    maintenance
        .create_or_read_archive_receipt(&receipt)
        .unwrap();
    receipt
        .advance(OfflineMaintenanceReceiptTransitionV1::phase(
            OfflineMaintenanceReceiptPhaseV1::Offline,
        ))
        .unwrap();
    receipt.record_published_incarnation(2).unwrap();
    maintenance.replace_archive_receipt(&receipt).unwrap();
    let sealed = prepared.seal_after_authorization(database_id).unwrap();
    maintenance.publish_sealed_archive_restore(sealed).unwrap();
    let restored =
        command_support::open_operational(riffdb_storage_redb::RedbStore::open(&target).unwrap());
    assert_eq!(self::control(&restored), expected_control);
    assert_eq!(
        restored.query_projection(&query).unwrap(),
        expected_projection
    );
}
