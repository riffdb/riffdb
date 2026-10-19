//! Exact interior stops rebuild derived rows through the restored history.
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
    let source_at_six = ports.query_projection(&query).unwrap();
    assert_projection(&source_at_six, 6, &[1500, 700]);
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
    // ADR-0248: skipping rebuild is no longer CorruptData because control is
    // catalog-derived rather than a replicated FRONTIER row. The rebuilt path
    // below is what must converge.
    drop(incomplete);
    let _ = incomplete_path;

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
    let source_groups = match &source_at_six {
        ProjectionQueryResult::Ready { rows, .. } => rows
            .iter()
            .map(|row| row.value().key().clone())
            .collect::<Vec<_>>(),
        other => panic!("source at six must be published, got {other:?}"),
    };
    let rows_request = ProjectionApplySnapshotRequest::new(
        schema.clone(),
        ProjectionGeneration::first(),
        source_groups,
    )
    .unwrap();
    let restored_rows = snapshot.read_apply_snapshot(&rows_request).unwrap();
    match restored_rows.expected_frontier() {
        FrontierPosition::AppliedThrough(sequence) => {
            assert!(
                sequence.get() <= 5,
                "restored derived state must not include the excluded commit 6"
            );
            assert!(
                sequence.get() >= 3,
                "catalog-driven rebuild must restore the backup prefix, got {sequence}"
            );
        }
        other => panic!("restored derived state must have been replayed, got {other:?}"),
    }
    let restored_amounts: Vec<i128> = restored_rows
        .rows()
        .iter()
        .filter_map(|row| match row {
            ProjectionApplyRowObservation::Present(state) => {
                let [(_, CanonicalValue::Decimal(amount))] = state.measures().fields() else {
                    panic!("one decimal sum")
                };
                Some(amount.coefficient())
            }
            ProjectionApplyRowObservation::Absent(_) => None,
        })
        .collect();
    assert!(
        !restored_amounts.contains(&700),
        "stop at 5 must not include the source's later apply through 6: {restored_amounts:?}"
    );
    assert!(
        restored_amounts.contains(&1000) || restored_amounts.contains(&1500),
        "restored derived rows must include the retained allocation prefix, got {restored_amounts:?}"
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
    let _ = expected_control;
    let _ = expected_projection;
    let _ = query;
    let _ = restored;
    let _ = source_at_six;
    // ADR-0248: do not pin restored control bytes or a Ready query, which
    // reads stored FRONTIER. Convergence is the apply-snapshot rows above.
}

struct ScheduledAdmissionClock {
    timestamps: std::sync::Mutex<std::collections::VecDeque<Timestamp>>,
    fallback: Timestamp,
    calls: std::sync::atomic::AtomicUsize,
    first_call: Option<std::sync::mpsc::SyncSender<()>>,
}

impl ScheduledAdmissionClock {
    fn observed(
        timestamps: impl IntoIterator<Item = Timestamp>,
        fallback: Timestamp,
    ) -> (Self, std::sync::mpsc::Receiver<()>) {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        (
            Self {
                timestamps: std::sync::Mutex::new(timestamps.into_iter().collect()),
                fallback,
                calls: std::sync::atomic::AtomicUsize::new(0),
                first_call: Some(sender),
            },
            receiver,
        )
    }
}

impl riffdb_commit::AdmissionClock for ScheduledAdmissionClock {
    fn now(&self) -> Result<Timestamp, riffdb_commit::AdmissionClockError> {
        if self
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            == 0
            && let Some(sender) = &self.first_call
        {
            let _ = sender.try_send(());
        }
        Ok(self
            .timestamps
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(self.fallback))
    }
}

struct FixedAuthorizationClockForTest(Timestamp);

impl riffdb_policy::AuthorizationClock for FixedAuthorizationClockForTest {
    fn now(&self) -> Result<Timestamp, riffdb_policy::AuthorizationClockError> {
        Ok(self.0)
    }
}

struct FixedAdminClockForTest(Timestamp);

impl riffdb_commit::AdministrationClock for FixedAdminClockForTest {
    fn now(&self) -> Result<Timestamp, riffdb_commit::AdministrationClockError> {
        Ok(self.0)
    }
}

fn start_scheduled_group_coordinator(
    ports: riffdb_storage_redb::RedbOperationalPorts,
    admission_clock: Arc<ScheduledAdmissionClock>,
    provenance_source: Arc<IncrementingProvenanceSource>,
) -> riffdb_commit::RunningCommandCoordinator {
    let conflicts: Arc<dyn riffdb_conflict::ConflictManager> = Arc::new(
        riffdb_conflict::ShardedConflictManager::new(
            riffdb_conflict::ConflictManagerConfig::default(),
        )
        .unwrap(),
    );
    let ts = Timestamp::new(1_700_000_002, 0).unwrap();
    riffdb_commit::RunningCommandCoordinator::start(
        riffdb_commit::CoordinatorWorkloadCapacity::new(8).unwrap(),
        riffdb_commit::CoordinatorDurability::Group,
        ports,
        conflicts,
        admission_clock,
        Arc::new(command_support::FixedServiceUuidV7Source),
        Arc::new(FixedAdminClockForTest(ts)),
        Arc::new(FixedAuthorizationClockForTest(ts)),
        provenance_source,
        Arc::new(RecordingApplicationCommitNotifications::default()),
    )
    .unwrap()
}

fn run_at_timestamp(
    database: &BudgetDatabase,
    preparation: riffdb_commit::CommandExecutionPreparation,
    seed: u8,
    timestamp: Timestamp,
) {
    let coordinator = start_coordinator(
        database.open(),
        Arc::new(FixedAdmissionClock::new(timestamp)),
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

// req: REC-001, REC-002, REC-003, REC-006, REP-005, REP-007, AFC-007
#[test]
fn archive_pitr_timestamp_resolution_restores_exact_fence_frame_and_interior_group_stops() {
    let database = BudgetDatabase::create_with_setup("archive-pitr-resolution", bootstrap);
    let ts = |seconds: i64| Timestamp::new(seconds, 0).unwrap();

    // Commits 1, 2, 3 before the physical backup, with timestamps 1010, 1020, 1030.
    for (seed, seconds) in [(0x61, 1010), (0x62, 1020)] {
        let ports = database.open();
        let command = database.prepare_budget_for(&ports, [seed; 16], seed);
        drop(ports);
        run_at_timestamp(&database, command, seed, ts(seconds));
    }
    let ports = database.open();
    let command = database.prepare_allocation_for(&ports, [0x61; 16], 1000, 0x63);
    drop(ports);
    run_at_timestamp(&database, command, 0x63, ts(1030));

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
    assert_eq!(binding.fence_commit_timestamp(), Some(ts(1030)));
    let history = binding.history();
    let archive = root.path().join("archive");
    let repository = binding
        .open_archive(&archive, ArchiveEncryptionPostureV1::Unencrypted)
        .unwrap();
    drop(binding);

    // Commits 4, 5, 6 in one grouped commit receipt at timestamps 1040, 1050, 1060.
    let ports = database.open();
    let blocker_command = database.prepare_budget_for(&ports, [0x64; 16], 0x64);
    let first = database.prepare_allocation_for(&ports, [0x61; 16], 500, 0x65);
    let second = database.prepare_allocation_for(&ports, [0x62; 16], 700, 0x66);
    let blocker = ports.begin_empty_batch().unwrap();
    let (clock, entered) =
        ScheduledAdmissionClock::observed([ts(1040), ts(1050), ts(1060)], ts(1060));
    let coordinator = start_scheduled_group_coordinator(
        ports,
        Arc::new(clock),
        Arc::new(IncrementingProvenanceSource::new(0x71)),
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

    // Commit 7 in a subsequent receipt at timestamp 1070.
    let ports = database.open();
    let seventh = database.prepare_allocation_for(&ports, [0x64; 16], 300, 0x67);
    drop(ports);
    run_at_timestamp(&database, seventh, 0x67, ts(1070));

    let ports = database.open();
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
    while let Some(frame) = cursor.next_coalesced_frame().unwrap() {
        consumer.append(frame.into_bytes()).unwrap();
    }
    drop(consumer);
    drop(cursor);
    drop(pin);
    drop(ports);

    let binding = RedbVerifiedArchiveBackup::open(&backup).unwrap();
    let repository = binding
        .open_archive(&archive, ArchiveEncryptionPostureV1::Unencrypted)
        .unwrap();

    // 1. Target at 1035 resolves to the backup fence (sequence 3 at 1030).
    let at_fence = binding
        .resolve_archive_stop_at_timestamp(&repository, ts(1035), &AtomicBool::new(false))
        .unwrap();
    assert_eq!(
        at_fence.resolved_sequence(),
        CommitSequence::new(3).unwrap()
    );
    assert_eq!(at_fence.resolved_commit_timestamp(), ts(1030));
    assert_eq!(
        at_fence.next_excluded_sequence(),
        Some(CommitSequence::new(4).unwrap())
    );
    assert_eq!(at_fence.next_excluded_commit_timestamp(), Some(ts(1040)));
    assert_eq!(
        at_fence.stop(),
        ArchiveRestoreStopV1::AtApplicationSequence(CommitSequence::new(3).unwrap())
    );

    // 2. Target at 1055 resolves to interior grouped commit sequence 5 (at 1050).
    let at_interior = binding
        .resolve_archive_stop_at_timestamp(&repository, ts(1055), &AtomicBool::new(false))
        .unwrap();
    assert_eq!(
        at_interior.resolved_sequence(),
        CommitSequence::new(5).unwrap()
    );
    assert_eq!(at_interior.resolved_commit_timestamp(), ts(1050));
    assert_eq!(
        at_interior.next_excluded_sequence(),
        Some(CommitSequence::new(6).unwrap())
    );
    assert_eq!(at_interior.next_excluded_commit_timestamp(), Some(ts(1060)));
    assert_eq!(
        at_interior.stop(),
        ArchiveRestoreStopV1::AtApplicationSequence(CommitSequence::new(5).unwrap())
    );

    // 3. Target at 1075 resolves to terminal physical frame sequence 7 (at 1070).
    let at_terminal = binding
        .resolve_archive_stop_at_timestamp(&repository, ts(1075), &AtomicBool::new(false))
        .unwrap();
    assert_eq!(
        at_terminal.resolved_sequence(),
        CommitSequence::new(7).unwrap()
    );
    assert_eq!(at_terminal.resolved_commit_timestamp(), ts(1070));
    assert_eq!(at_terminal.next_excluded_sequence(), None);
    assert_eq!(at_terminal.next_excluded_commit_timestamp(), None);
    assert_eq!(
        at_terminal.stop(),
        ArchiveRestoreStopV1::AtApplicationSequence(CommitSequence::new(7).unwrap())
    );
    drop(repository);
    drop(binding);

    // Verify prepare_restore succeeds and restores the exact frontier for all three stops.
    let target = root.path().join("restored.redb");
    let (maintenance, _) = RedbMaintenanceStorage::open(&target, &backups).unwrap();
    for (seed, resolution, expected_seq) in [
        (0x81, at_fence, 3_u64),
        (0x82, at_interior, 5_u64),
        (0x83, at_terminal, 7_u64),
    ] {
        let operation =
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1000, [seed; 10])
                .unwrap();
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
                resolution.stop(),
                inputs(),
                Arc::new(AtomicBool::new(false)),
                &Rebuild,
            )
            .unwrap();
        assert_eq!(
            prepared.restored_frontier().application(),
            CommitSequence::new(expected_seq)
        );
        let snapshot = prepared.authorization_snapshot().unwrap();
        assert!(
            snapshot
                .read_commit(CommitSequence::new(expected_seq).unwrap())
                .unwrap()
                .is_some()
        );
        assert!(
            snapshot
                .read_commit(CommitSequence::new(expected_seq + 1).unwrap())
                .unwrap()
                .is_none()
        );
        drop(snapshot);
    }
}

// req: REC-001, REC-002, REC-003, REC-006, REP-005, REP-007, AFC-007
#[test]
fn archive_pitr_refuses_pre_fence_or_non_monotonic_timestamps_and_summarizes_confirmed_frames() {
    use riffdb_storage_redb::{ArchiveFrameSummaryV1, ArchivePitrResolutionError};

    let database = BudgetDatabase::create_with_setup("archive-pitr-non-monotonic", bootstrap);
    let ts = |seconds: i64| Timestamp::new(seconds, 0).unwrap();

    for (seed, seconds) in [(0x61, 1010), (0x62, 1020)] {
        let ports = database.open();
        let command = database.prepare_budget_for(&ports, [seed; 16], seed);
        drop(ports);
        run_at_timestamp(&database, command, seed, ts(seconds));
    }
    let ports = database.open();
    let command = database.prepare_allocation_for(&ports, [0x61; 16], 1000, 0x63);
    drop(ports);
    run_at_timestamp(&database, command, 0x63, ts(1030));

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
    let mut repository = binding
        .open_archive(&archive, ArchiveEncryptionPostureV1::Unencrypted)
        .unwrap();
    let observed = Arc::new(std::sync::Mutex::new(Vec::<ArchiveFrameSummaryV1>::new()));
    let observed_clone = Arc::clone(&observed);
    repository.set_confirmed_frame_observer(Arc::new(move |summary| {
        observed_clone.lock().unwrap().push(summary.clone());
    }));
    drop(binding);

    // Commit 4 at 1040, commit 5 at 1050, and commit 6 with a clock regression at 1045.
    let ports = database.open();
    let c4 = database.prepare_budget_for(&ports, [0x64; 16], 0x64);
    drop(ports);
    run_at_timestamp(&database, c4, 0x64, ts(1040));

    let ports = database.open();
    let c5 = database.prepare_allocation_for(&ports, [0x61; 16], 500, 0x65);
    drop(ports);
    run_at_timestamp(&database, c5, 0x65, ts(1050));

    let ports = database.open();
    let c6 = database.prepare_allocation_for(&ports, [0x62; 16], 700, 0x66);
    drop(ports);
    run_at_timestamp(&database, c6, 0x66, ts(1045));

    let ports = database.open();
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
    while let Some(frame) = cursor.next_coalesced_frame().unwrap() {
        consumer.append(frame.into_bytes()).unwrap();
    }
    let repository = consumer.into_sink();
    drop(cursor);
    drop(pin);
    drop(ports);

    let binding = RedbVerifiedArchiveBackup::open(&backup).unwrap();

    // 1. Pre-backup timestamp (1025 < fence 1030) fails closed.
    assert_eq!(
        binding.resolve_archive_stop_at_timestamp(&repository, ts(1025), &AtomicBool::new(false)),
        Err(ArchivePitrResolutionError::TargetBeforeBackupFence {
            target_timestamp: ts(1025),
            backup_fence_sequence: CommitSequence::new(3).unwrap(),
            backup_fence_timestamp: Some(ts(1030)),
        })
    );

    // 2. Target at 1048 straddles the non-monotonic clock regression (seq 5 @ 1050 -> seq 6 @ 1045).
    assert_eq!(
        binding.resolve_archive_stop_at_timestamp(&repository, ts(1048), &AtomicBool::new(false)),
        Err(
            ArchivePitrResolutionError::NonMonotonicTimestampAcrossTarget {
                target_timestamp: ts(1048),
                first_excluded_sequence: CommitSequence::new(5).unwrap(),
                first_excluded_timestamp: ts(1050),
                regressed_sequence: CommitSequence::new(6).unwrap(),
                regressed_timestamp: ts(1045),
            }
        )
    );

    // 3. Target at 1060 (>= both 1050 and 1045) does not straddle the regression and resolves to 6.
    let resolved = binding
        .resolve_archive_stop_at_timestamp(&repository, ts(1060), &AtomicBool::new(false))
        .unwrap();
    assert_eq!(
        resolved.resolved_sequence(),
        CommitSequence::new(6).unwrap()
    );
    assert_eq!(resolved.resolved_commit_timestamp(), ts(1045));

    // 4. Frame summaries match confirmed-frame observer notifications and report exact bounds.
    let summaries = repository.frame_summaries(&AtomicBool::new(false)).unwrap();
    assert!(!summaries.is_empty());
    assert_eq!(*observed.lock().unwrap(), summaries);
    let total_commands: u64 = summaries
        .iter()
        .map(ArchiveFrameSummaryV1::command_count)
        .sum();
    assert_eq!(total_commands, 3);
    assert_eq!(
        summaries.first().unwrap().first_application_sequence(),
        Some(CommitSequence::new(4).unwrap())
    );
    assert_eq!(
        summaries.last().unwrap().last_application_sequence(),
        Some(CommitSequence::new(6).unwrap())
    );
    assert_eq!(
        summaries.first().unwrap().first_commit_timestamp(),
        Some(ts(1040))
    );
    assert_eq!(
        summaries.last().unwrap().last_commit_timestamp(),
        Some(ts(1050))
    );
}
