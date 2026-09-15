//! Real command commits and source controls, applied before local replay.
// req: REP-002, REP-003, PRJ-001, REC-001, PERF-007
use super::tests::{bootstrap, inputs};
use super::*;
use riffdb_storage_api::*;
use riffdb_types::*;
#[path = "../../../tests/command_semantics/support.rs"]
mod command_support;
use command_support::{
    BudgetDatabase, FixedAdmissionClock, IncrementingProvenanceSource, command_timestamp, runtime,
    start_coordinator,
};

fn execute(database: &BudgetDatabase, allocate: bool) {
    let ports = database.open();
    let preparation = if allocate {
        database.prepare_allocation(&ports, 2500, 0x35)
    } else {
        database.prepare(&ports, 12500, 0x34)
    };
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(if allocate {
            0x55
        } else {
            0x45
        })),
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

#[test]
fn follower_reopen_replays_lagging_projection_before_unchanged_startup_validation() {
    exercise_recovery(None, ReplayMode::Recovery);
}

#[test]
fn follower_projection_recovery_crashes_resume_exactly_before_startup_activation() {
    for edge in ["replay-committed", "rebuilt"] {
        exercise_recovery(Some(edge), ReplayMode::Recovery);
    }
}

#[test]
fn follower_projection_recovery_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_FOLLOWER_PROJECTION_RECOVERY_DATABASE") else {
        return;
    };
    crate::startup::open_redb_follower_startup(std::path::Path::new(&path), inputs()).unwrap();
    panic!("requested recovery crash did not fire");
}

#[test]
fn follower_tail_replays_changed_projections_before_acknowledgement() {
    exercise_recovery(None, ReplayMode::Direct);
}

#[test]
fn continuous_receiver_acknowledges_only_rebuilt_projection_prefix() {
    exercise_recovery(None, ReplayMode::Receiver);
}

#[test]
fn follower_tail_rebuilds_new_candidate_without_replaying_published_prefix() {
    exercise_recovery(None, ReplayMode::Rebuild);
}

#[test]
fn follower_read_snapshots_pin_exact_prefix_and_refuse_unreplayed_projection_frontiers() {
    exercise_recovery(None, ReplayMode::Snapshots);
}

#[derive(Clone, Copy)]
enum ReplayMode {
    Recovery,
    Direct,
    Receiver,
    Rebuild,
    Snapshots,
}

fn exercise_recovery(crash: Option<&str>, mode: ReplayMode) {
    let database = BudgetDatabase::create_with_setup("follower-projection-recovery", bootstrap);
    let mut ports = database.open();
    let plan = database.bundle().bundle().projections().first().unwrap();
    let schema = CheckedProjectionSchema::new(
        database
            .bundle()
            .bundle()
            .bound_projection_group_schema(plan.projection_id())
            .unwrap(),
    );
    let ProjectionControlResult::Updated(control) = ports
        .transition_projection_control(ProjectionControlOperation::CreateInitial {
            schema: schema.clone(),
        })
        .unwrap()
    else {
        panic!("create");
    };
    let ProjectionControlResult::Updated(mut control) = ports
        .transition_projection_control(ProjectionControlOperation::StartInitialScan {
            expected: control,
        })
        .unwrap()
    else {
        panic!("scan");
    };
    let scope = tempfile::tempdir().unwrap();
    let held = ports
        .prepare_replication_bootstrap_v3(
            &scope.path().join("source"),
            ReplicationSourceHoldIdV1::new([0x74; 16]).unwrap(),
        )
        .unwrap();
    let manifest = held.manifest();
    let mut stage =
        riffdb_storage_redb::RedbBootstrapStage::create(&scope.path().join("transfer"), manifest)
            .unwrap();
    for n in 1..=manifest.page_count() {
        stage
            .append(&held.read_page(n).unwrap().encode().unwrap())
            .unwrap();
    }
    let mut materializer = riffdb_storage_redb::RedbBootstrapMaterializer::create(
        &scope.path().join("candidate"),
        stage.into_materialization_input().unwrap(),
    )
    .unwrap();
    while materializer.copy_next_page().unwrap().is_some() {}
    let mut worker = BootstrapProjectionRebuild::new(
        materializer.finish().unwrap(),
        inputs(),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    while !worker.advance().unwrap() {}
    let target = scope.path().join("follower.redb");
    let mut applier = worker
        .finish()
        .unwrap()
        .publish(&target)
        .unwrap()
        .activate(inputs())
        .unwrap();
    let initial_snapshot = if matches!(mode, ReplayMode::Snapshots) {
        Some(applier.capture_read_snapshot().unwrap().1)
    } else {
        None
    };
    let mut lagged_snapshots = Vec::new();
    drop(ports);
    execute(&database, false);
    execute(&database, true);
    let mut ports = database.open();
    let resolved = riffdb_catalog::ActiveCatalogSnapshot::read(&ports)
        .unwrap()
        .unwrap()
        .resolve_projection(schema.identity())
        .unwrap();
    let commits = ports
        .scan_commits(CommitScanRequest::initial(
            StorageScanLimit::new(2).unwrap(),
        ))
        .unwrap();
    assert_eq!(commits.records().len(), 2);
    let mut groups = Vec::new();
    for item in commits.records() {
        let apply = evaluate_and_prepare_projection_commit(
            &resolved,
            schema.clone(),
            ProjectionGeneration::first(),
            item.value(),
            &ports,
        )
        .unwrap();
        groups.extend(apply.row_updates().iter().map(|row| row.key().clone()));
        let ProjectionApplyResult::Applied { control: next, .. } =
            ports.apply_projection(&apply).unwrap()
        else {
            panic!("apply");
        };
        control = next;
    }
    assert_eq!(groups.len(), 1, "only allocation contributes a group");
    let rows_request =
        ProjectionApplySnapshotRequest::new(schema.clone(), ProjectionGeneration::first(), groups)
            .unwrap();
    let source_rows = ports.read_apply_snapshot(&rows_request).unwrap();
    let [ProjectionApplyRowObservation::Present(row)] = source_rows.rows() else {
        panic!("allocation row");
    };
    assert!(
        matches!(row.measures().fields(), [(_, CanonicalValue::Decimal(amount))] if amount.coefficient() == 2500)
    );
    let candidate_request = if matches!(mode, ReplayMode::Rebuild) {
        let ProjectionControlResult::Updated(published) = ports
            .transition_projection_control(ProjectionControlOperation::PublishCandidate {
                expected: control.clone(),
            })
            .unwrap()
        else {
            panic!("publish");
        };
        let ProjectionControlResult::Updated(rebuilding) = ports
            .transition_projection_control(ProjectionControlOperation::AllocateRebuild {
                expected: published,
            })
            .unwrap()
        else {
            panic!("rebuild");
        };
        let generation = rebuilding.candidate().unwrap().generation();
        let mut candidate_groups = Vec::new();
        for item in commits.records() {
            let apply = evaluate_and_prepare_projection_commit(
                &resolved,
                schema.clone(),
                generation,
                item.value(),
                &ports,
            )
            .unwrap();
            candidate_groups.extend(apply.row_updates().iter().map(|row| row.key().clone()));
            assert!(matches!(
                ports.apply_projection(&apply).unwrap(),
                ProjectionApplyResult::Applied { .. }
            ));
        }
        Some(
            ProjectionApplySnapshotRequest::new(schema.clone(), generation, candidate_groups)
                .unwrap(),
        )
    } else {
        None
    };
    let published = ports.published_changelog_snapshot_v3().unwrap();
    let history = published.authoritative_state_v3().unwrap().history();
    if matches!(mode, ReplayMode::Receiver) {
        drop(applier);
        let peer = SnapshotPeer {
            snapshot: published,
        };
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                let jobs = BootstrapReceiverJobs::new();
                let mut receiver = jobs
                    .reopen_follower(
                        target.clone(),
                        inputs(),
                        history.lineage(),
                        manifest.fence().hold_id(),
                        Some(manifest),
                    )
                    .await
                    .unwrap();
                let (_evidence, mut readers, notifier) = receiver.prepare_service().await.unwrap();
                let initial = readers.latest().unwrap();
                assert_eq!(initial.history(), manifest.fence().history());
                assert!(initial.catalog().is_some());
                let mut last = None;
                let mut notified_frontiers = 0;
                loop {
                    let before = readers
                        .latest()
                        .unwrap()
                        .snapshot()
                        .read_apply_snapshot(&rows_request)
                        .unwrap()
                        .expected_frontier();
                    let registration = notifier.register(schema.identity().clone()).unwrap();
                    let Some(position) = receiver.advance(&peer).await.unwrap() else {
                        break;
                    };
                    let view = readers.changed().await.unwrap();
                    if view
                        .snapshot()
                        .read_apply_snapshot(&rows_request)
                        .unwrap()
                        .expected_frontier()
                        != before
                    {
                        assert_eq!(
                            registration
                                .wait(std::time::Instant::now() + std::time::Duration::from_secs(1))
                                .unwrap(),
                            riffdb_projection::ProjectionWake::Notified
                        );
                        notified_frontiers += 1;
                    }
                    assert_eq!(view.history().tail(), position);
                    assert!(
                        view.snapshot()
                            .read_projection_status(schema.identity())
                            .is_ok()
                    );
                    last = Some(position);
                }
                assert_eq!(last, Some(history.tail()));
                assert_eq!(notified_frontiers, 2);
                assert_eq!(
                    readers
                        .latest()
                        .unwrap()
                        .snapshot()
                        .read_apply_snapshot(&rows_request)
                        .unwrap(),
                    source_rows
                );
                assert_eq!(
                    initial
                        .snapshot()
                        .read_apply_snapshot(&rows_request)
                        .unwrap()
                        .expected_frontier(),
                    FrontierPosition::BeforeFirst
                );
                drop(initial);
                let closing = notifier.register(schema.identity().clone()).unwrap();
                receiver.close().await.unwrap();
                assert_eq!(
                    closing
                        .wait(std::time::Instant::now() + std::time::Duration::from_secs(1))
                        .unwrap(),
                    riffdb_projection::ProjectionWake::Notified
                );
                assert_eq!(
                    readers.latest().unwrap_err().kind(),
                    StorageErrorKind::Unavailable
                );
            });
        // Inspect before production reopen: recovery must not conceal a missing
        // replay in the continuous receiver's acknowledgement path.
        assert!(!structural_findings(&target));
        let checked = crate::startup::open_redb_follower_startup(&target, inputs()).unwrap();
        assert_eq!(checked.applier.durable_history().unwrap(), history);
        assert_eq!(
            checked.applier.read_apply_snapshot(&rows_request).unwrap(),
            source_rows
        );
        return;
    }
    let mut cursor = ChangelogFrameCursorV3::open(
        published.as_ref(),
        ReplicationHandshakeV3::new(
            history.lineage(),
            manifest.fence().history().tail(),
            ChangelogFrameV3::IDENTITY,
            AuthoritativeStateCatalogV1.digest(),
            MAX_CHANGELOG_FRAME_BYTES as u64,
            MAX_STAGED_COMMANDS as u64,
        )
        .unwrap(),
    )
    .unwrap();
    let mut tail = super::projection_tail::FollowerProjectionTail::default();
    while let Some(frame) = cursor.next_frame().unwrap() {
        let decoded = ChangelogFrameV3::decode(frame.as_bytes()).unwrap();
        if matches!(
            mode,
            ReplayMode::Direct | ReplayMode::Rebuild | ReplayMode::Snapshots
        ) {
            assert_control_deletion_refused(&tail, &decoded);
        }
        let changes = tail.plan(&decoded).unwrap();
        applier.apply_frame(frame.as_bytes()).unwrap();
        if matches!(mode, ReplayMode::Snapshots) {
            let (_, snapshot) = applier.capture_read_snapshot().unwrap();
            if snapshot.read_projection_status(schema.identity()).is_err() {
                lagged_snapshots.push(snapshot);
            }
        }
        if matches!(
            mode,
            ReplayMode::Direct | ReplayMode::Rebuild | ReplayMode::Snapshots
        ) {
            tail.replay(&mut applier, changes, &AtomicBool::new(false))
                .unwrap();
            applier.acknowledge_durable_position().unwrap();
        }
    }
    if matches!(
        mode,
        ReplayMode::Direct | ReplayMode::Rebuild | ReplayMode::Snapshots
    ) {
        assert_eq!(
            applier.read_apply_snapshot(&rows_request).unwrap(),
            source_rows
        );
        assert_eq!(applier.durable_history().unwrap(), history);
        assert_eq!(tail.catalog_loads, 1);
        assert_eq!(
            tail.replay_steps,
            if candidate_request.is_some() { 4 } else { 2 }
        );
        if let Some(initial) = initial_snapshot {
            assert_eq!(
                initial
                    .read_apply_snapshot(&rows_request)
                    .unwrap()
                    .expected_frontier(),
                FrontierPosition::BeforeFirst
            );
            assert!(
                initial
                    .read_apply_snapshot(&rows_request)
                    .unwrap()
                    .rows()
                    .iter()
                    .all(|row| matches!(row, ProjectionApplyRowObservation::Absent(_)))
            );
            assert_eq!(lagged_snapshots.len(), 2);
            for snapshot in lagged_snapshots {
                assert_eq!(
                    snapshot
                        .read_projection_status(schema.identity())
                        .unwrap_err()
                        .kind(),
                    StorageErrorKind::Unavailable
                );
            }
            let (bound, snapshot) = applier.capture_read_snapshot().unwrap();
            assert_eq!(bound, history);
            assert_eq!(
                snapshot.read_apply_snapshot(&rows_request).unwrap(),
                source_rows
            );
            assert!(snapshot.read_projection_status(schema.identity()).is_ok());
            assert!(
                riffdb_catalog::ActiveCatalogSnapshot::read(&snapshot)
                    .unwrap()
                    .is_some()
            );
        }
        if let Some(request) = candidate_request {
            assert_eq!(
                applier.read_apply_snapshot(&request).unwrap(),
                ports.read_apply_snapshot(&request).unwrap()
            );
        }
        return;
    }
    assert_eq!(applier.durable_history().unwrap(), history);
    let empty_request =
        ProjectionApplySnapshotRequest::new(schema.clone(), ProjectionGeneration::first(), vec![])
            .unwrap();
    assert_eq!(
        applier
            .read_apply_snapshot(&empty_request)
            .unwrap()
            .expected_frontier(),
        FrontierPosition::BeforeFirst,
        "frame apply does not invent local projection markers"
    );
    drop(applier);

    // The existing validator still rejects the lag before recovery.
    assert!(
        structural_findings(&target),
        "unchanged startup must detect the missing marker prefix"
    );
    if let Some(edge) = crash {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "replication_bootstrap::projection_recovery_tests::follower_projection_recovery_process_child", "--nocapture"])
            .env("RIFFDB_FOLLOWER_PROJECTION_RECOVERY_DATABASE", &target)
            .env("RIFFDB_FOLLOWER_PROJECTION_RECOVERY_EDGE", edge)
            .status().unwrap();
        assert_eq!(status.code(), Some(93), "{edge}");
    }
    let checked = crate::startup::open_redb_follower_startup(&target, inputs()).unwrap();
    assert_eq!(checked.applier.durable_history().unwrap(), history);
    assert_eq!(
        checked.applier.read_apply_snapshot(&rows_request).unwrap(),
        source_rows
    );
    assert_eq!(
        checked
            .applier
            .read_apply_snapshot(&empty_request)
            .unwrap()
            .expected_frontier(),
        FrontierPosition::AppliedThrough(CommitSequence::new(2).unwrap())
    );
    assert!(matches!(
        validate_projection_generation(
            &checked.applier,
            &resolved,
            schema,
            &control,
            FrontierPosition::AppliedThrough(CommitSequence::new(2).unwrap()),
            ProjectionGeneration::first(),
            one_page().unwrap()
        )
        .unwrap(),
        ProjectionGenerationValidationOutcome::Clean(_)
    ));
}

fn structural_findings(target: &std::path::Path) -> bool {
    let mut session = riffdb_storage_redb::RedbFollowerStore::open(target)
        .unwrap()
        .begin_structural_evidence_cancellable(inputs(), Arc::new(AtomicBool::new(false)))
        .unwrap();
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let mut found = false;
    while let StructuralEvidencePage::Page { findings, next, .. } = session
        .read_structural_evidence(cursor, EvidencePageLimit::new(64).unwrap())
        .unwrap()
    {
        found |= !findings.is_empty();
        cursor = next;
    }
    found
}

// An immutable real source cursor keeps this receiver test independent of
// authorization/hold handling, which has separate production-source tests.
struct SnapshotPeer {
    snapshot: Arc<dyn PublishedDurableSnapshot>,
}
struct SnapshotStream(ChangelogFrameCursorV3);
impl riffdb_service::ReplicationItemSource for SnapshotStream {
    fn next_item(
        &mut self,
    ) -> riffdb_service::ReplicationFuture<'_, Option<riffdb_service::ReplicationItem>> {
        Box::pin(async move {
            self.0
                .next_frame()
                .map(|frame| {
                    frame.map(|frame| riffdb_service::ReplicationItem::Frame(frame.into_bytes()))
                })
                .map_err(riffdb_service::ReplicationFailure::Source)
        })
    }
}
impl riffdb_service::ReplicationSourcePort for SnapshotPeer {
    fn open(
        &self,
        request: riffdb_service::ReplicationRequest,
    ) -> riffdb_service::ReplicationFuture<'_, Box<dyn riffdb_service::ReplicationItemSource>> {
        Box::pin(async move {
            let lineage = ChangelogLineageV3::new(
                request.database_id,
                request.history_incarnation,
                LeadershipEpochV1::new(request.leadership_epoch).unwrap(),
            )
            .unwrap();
            let after = ChangelogHistoryPointV3::new(
                ChangelogTransactionSequence::new(request.after_sequence).unwrap(),
                request.after_hash,
                request.after_frontier,
            );
            let handshake = ReplicationHandshakeV3::new(
                lineage,
                after,
                &request.readable_format,
                request.catalog_digest,
                request.maximum_frame_bytes,
                request.maximum_transitions,
            )
            .map_err(riffdb_service::ReplicationFailure::Source)?;
            let cursor = ChangelogFrameCursorV3::open(self.snapshot.as_ref(), handshake)
                .map_err(riffdb_service::ReplicationFailure::Source)?;
            Ok(Box::new(SnapshotStream(cursor)) as Box<dyn riffdb_service::ReplicationItemSource>)
        })
    }
}

fn assert_control_deletion_refused(
    tail: &super::projection_tail::FollowerProjectionTail,
    frame: &ChangelogFrameV3,
) {
    let receipt = &frame.receipts()[0];
    if !receipt
        .mutations()
        .iter()
        .any(|item| item.namespace() == AuthoritativeNamespaceV1::ProjectionFrontier)
    {
        return;
    }
    let mutations = receipt
        .mutations()
        .iter()
        .map(|item| {
            if item.namespace() == AuthoritativeNamespaceV1::ProjectionFrontier {
                AuthoritativeMutationV3::delete_matching(
                    item.namespace(),
                    item.key(),
                    item.value().unwrap(),
                )
                .unwrap()
            } else {
                item.clone()
            }
        })
        .collect();
    let altered =
        AuthoritativeTransactionV3::new(receipt.binding(), receipt.attribution(), mutations)
            .unwrap();
    let altered = ChangelogFrameV3::new(frame.binding(), vec![altered]).unwrap();
    assert!(
        tail.plan(&altered).is_err(),
        "control deletion must not leave orphaned generation authority"
    );
}
