//! ADR-0182 section 6 online retention (WP-761) over the shared fixtures.
// req: STO-030, STO-031, STO-032, REC-001, REC-002
use super::*;
use riffdb_storage_redb::{OnlineRetentionHold, OnlineRetentionStep};
use std::num::NonZeroU64;

/// Commands per committed segment in these fixtures.
const GROUP: u64 = 8;
/// Whole segments committed by `prepare_segmented_history`.
const GROUPS: u64 = 4;

const ONLINE_RETENTION_CHILD_MODE: &str = "RIFFDB_ONLINE_RETENTION_CHILD_MODE";
const ONLINE_RETENTION_CHILD_PATH: &str = "RIFFDB_ONLINE_RETENTION_CHILD_PATH";

fn sequences(count: u64) -> NonZeroU64 {
    NonZeroU64::new(count).expect("nonzero sequence count")
}

/// Commits `GROUPS` whole segments of `GROUP` commands each, and marks every
/// event delivered when `deliver` is set.
fn prepare_segmented_history(path: &Path, deliver: bool) -> Vec<CommandFixture> {
    prepare_command_database(path);
    let ports = open_operational(RedbStore::open(path).expect("open segmented source"));
    let mut fixtures = Vec::new();
    for group in 0..GROUPS {
        let batch = (1..=GROUP)
            .map(|ordinal| command_fixture_at(group * GROUP + ordinal))
            .collect::<Vec<_>>();
        commit_command_group(&ports, &batch);
        fixtures.extend(batch);
    }
    drop(ports);
    if deliver {
        let database = Database::open(path).expect("open delivery fixture");
        for sequence in 1..=GROUP * GROUPS {
            retention_deliver_outbox_status_at(&database, sequence);
        }
    }
    fixtures
}

fn assert_found(ports: &RedbOperationalPorts, fixture: &CommandFixture) {
    assert!(
        matches!(
            ports
                .lookup_admission(fixture.candidates.clone())
                .expect("lookup admission"),
            AdmissionLookupResultV1::Found(_)
        ),
        "a pruned or retained command's idempotency answer survives"
    );
}

fn assert_pruned(ports: &RedbOperationalPorts, sequence: u64) {
    assert_eq!(
        ports
            .read_commit(CommitSequence::new(sequence).expect("sequence"))
            .expect_err("below-watermark commit read is typed pruned")
            .kind(),
        riffdb_storage_api::StorageErrorKind::HistoryPruned
    );
}

/// ADR-0182 section 6 end to end on one live store: bounded whole-segment
/// steps through the ordered writer advance the watermark toward the
/// retained-sequence target, append a contiguous tombstone chain, delete the
/// validated-prefix checkpoint, trim the warm transient indexes without a
/// rebuild, keep every retained view, and leave a database that reopens
/// clean and keeps accepting commands.
#[test]
fn online_retention_prunes_whole_segments_through_the_ordered_writer() {
    let path = TestDatabasePath::new("online-retention-steps");
    let fixtures = prepare_segmented_history(&path.0, true);
    // A complete pass writes the validated-prefix checkpoint the first step
    // must delete.
    strip_checkpoint_meta(&path.0);
    complete_startup_pass(RedbStore::open(&path.0).expect("checkpoint pass"));
    assert!(retention_meta_present(
        &path.0,
        "validated_prefix_checkpoint/v1"
    ));

    let ports = open_operational(RedbStore::open(&path.0).expect("open live store"));
    // Warm the transient indexes so the step's delta trims live state.
    use riffdb_storage_api::OutboxRepository;
    assert!(
        !ports
            .has_undelivered_outbox()
            .expect("warm the transient indexes")
    );
    let rebuilds = ports.transient_index_rebuilds();
    let retain = sequences(2 * GROUP);

    // A budget smaller than one whole segment removes that segment alone
    // rather than splitting it or wedging the lane.
    assert_eq!(
        ports
            .online_retention_step(retain, sequences(GROUP - 1))
            .expect("undersized step"),
        OnlineRetentionStep::Advanced {
            through: GROUP,
            sequences: GROUP,
        }
    );
    // A target inside the next segment is a row boundary, not a split.
    assert_eq!(
        ports
            .online_retention_step(
                sequences(GROUP * GROUPS - GROUP - GROUP / 2),
                sequences(256)
            )
            .expect("target inside a segment"),
        OnlineRetentionStep::Held(OnlineRetentionHold::RowBoundary)
    );
    assert_eq!(
        ports
            .online_retention_step(retain, sequences(GROUP))
            .expect("online step"),
        OnlineRetentionStep::Advanced {
            through: 2 * GROUP,
            sequences: GROUP,
        }
    );
    assert_eq!(
        ports
            .online_retention_step(retain, sequences(256))
            .expect("step at the target"),
        OnlineRetentionStep::Held(OnlineRetentionHold::TargetReached)
    );

    assert_pruned(&ports, 1);
    assert_pruned(&ports, 2 * GROUP);
    assert_eq!(
        ports
            .read_commit(CommitSequence::new(2 * GROUP + 1).expect("retained"))
            .expect("read retained commit"),
        Some(
            fixtures[usize::try_from(2 * GROUP).expect("index")]
                .records
                .commit()
                .clone()
        )
    );
    // Pruned commands answer from the materialized views; retained ones from
    // the trimmed index. Neither rebuilt the indexes.
    assert_found(&ports, &fixtures[0]);
    assert_found(
        &ports,
        &fixtures[usize::try_from(2 * GROUP - 1).expect("index")],
    );
    assert_found(
        &ports,
        &fixtures[usize::try_from(2 * GROUP).expect("index")],
    );
    assert_eq!(
        ports.transient_index_rebuilds(),
        rebuilds,
        "the step trimmed the warm indexes instead of invalidating them"
    );
    // The writer keeps serving: a new command commits above the watermark.
    commit_command_fixture(&ports, &command_fixture_at(GROUP * GROUPS + 1));
    drop(ports);

    assert!(
        !retention_meta_present(&path.0, "validated_prefix_checkpoint/v1"),
        "the first step deleted the validated-prefix checkpoint"
    );
    let tombstones = retention_tombstones(&path.0);
    assert_eq!(
        tombstones
            .iter()
            .map(|tombstone| (tombstone.first_sequence(), tombstone.last_sequence()))
            .collect::<Vec<_>>(),
        vec![(1, GROUP), (GROUP + 1, 2 * GROUP)],
        "one contiguous tombstone per step"
    );
    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("reopen pruned"));
    assert!(findings.is_empty(), "online-pruned reopen: {findings:?}");
}

/// ADR-0182 section 6 fencing on the online lane: an undelivered event and an
/// operator hold each stop advancement with their closed binding, and a
/// fenced step deletes nothing.
#[test]
fn online_retention_refuses_to_cross_a_fence() {
    // Undelivered outbox: nothing in the first segment was delivered.
    let path = TestDatabasePath::new("online-retention-outbox-fence");
    prepare_segmented_history(&path.0, false);
    let ports = open_operational(RedbStore::open(&path.0).expect("open undelivered"));
    assert_eq!(
        ports
            .online_retention_step(sequences(1), sequences(256))
            .expect("fenced step"),
        OnlineRetentionStep::Held(OnlineRetentionHold::Fenced(
            riffdb_storage_api::RetentionFenceBinding::UndeliveredOutboxLowWater
        ))
    );
    drop(ports);
    assert!(retention_tombstones(&path.0).is_empty());

    // Operator hold below the first segment's end.
    let path = TestDatabasePath::new("online-retention-hold-fence");
    prepare_segmented_history(&path.0, true);
    riffdb_storage_redb::RedbOfflineRetention::bind(&path.0)
        .add_hold("online-cap", GROUP / 2, "fence the online lane")
        .expect("install hold");
    let ports = open_operational(RedbStore::open(&path.0).expect("open held"));
    assert_eq!(
        ports
            .online_retention_step(sequences(1), sequences(256))
            .expect("held step"),
        OnlineRetentionStep::Held(OnlineRetentionHold::Fenced(
            riffdb_storage_api::RetentionFenceBinding::OperatorHold
        ))
    );
    drop(ports);
    assert!(retention_tombstones(&path.0).is_empty());
}

/// An unreadable fencing source refuses the online step with a typed error
/// and deletes nothing; the offline status refuses the same source.
#[test]
fn online_retention_refuses_an_unreadable_fencing_source() {
    let path = TestDatabasePath::new("online-retention-unreadable-fence");
    prepare_segmented_history(&path.0, true);
    riffdb_storage_redb::plant_unreadable_projection_frontier_fixture(&path.0)
        .expect("plant unreadable projection frontier");
    let ports = open_operational(RedbStore::open(&path.0).expect("open unreadable"));
    let error = ports
        .online_retention_step(sequences(1), sequences(256))
        .expect_err("an unreadable fence refuses");
    assert_eq!(
        error.kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation
    );
    assert!(
        ports
            .read_commit(CommitSequence::new(1).expect("sequence"))
            .expect("first commit retained")
            .is_some()
    );
    drop(ports);
    assert!(retention_tombstones(&path.0).is_empty());
    assert!(
        riffdb_storage_redb::RedbOfflineRetention::bind(&path.0)
            .status()
            .is_err()
    );
}

/// Plants an open staged contract migration whose frozen application
/// frontier is `frontier`, exactly as `retention_staged_migration_frontier_fences_prune`
/// does for the offline operation.
fn plant_staged_migration(path: &Path, frontier: u64) {
    use riffdb_storage_api::{
        ContractMigrationArtifactsV1, ContractMigrationJournalStepV1, MigrationScanCursor,
        StoredContractMigrationJournalV1,
    };
    use riffdb_types::{
        ContractBundleHash, ContractMigrationInputHash, ContractMigrationOperationId,
        MigrationBundleHash,
    };
    let operation_id =
        ContractMigrationOperationId::from_unix_milliseconds_and_random(2, [0x23; 10])
            .expect("operation id");
    let journal = StoredContractMigrationJournalV1::new(
        database_id(),
        operation_id,
        ContractMigrationInputHash::from_bytes([0x55; 32]),
        ContractMigrationArtifactsV1::new(
            ContractBundleHash::from_bytes([0x66; 32]),
            ContractBundleHash::from_bytes([0x77; 32]),
            MigrationBundleHash::from_bytes([0x88; 32]),
        ),
        ContractMigrationJournalStepV1::Transforming,
        MigrationScanCursor::start(),
        0,
        0,
        1,
        Some(CommitSequence::new(frontier).expect("frozen frontier")),
        Vec::new(),
        None,
    )
    .expect("staged journal with frozen frontier");
    let database = Database::open(path).expect("open raw");
    let write = database.begin_write().expect("write");
    {
        let mut table = write
            .open_table(TableDefinition::<&[u8], &[u8]>::new(
                "contract_migration_journal",
            ))
            .expect("journal table");
        let encoded =
            riffdb_storage_api::proto_codec::encode_contract_migration_journal_v1(&journal)
                .expect("encode journal");
        table
            .insert(operation_id.into_bytes().as_slice(), encoded.as_bytes())
            .expect("insert journal");
    }
    write.commit().expect("commit journal");
}

/// The online lane honors an open staged migration's frozen application
/// frontier with its closed binding and deletes nothing.
#[test]
fn online_retention_honors_a_staged_migration_frozen_frontier() {
    let path = TestDatabasePath::new("online-retention-staged-fence");
    prepare_segmented_history(&path.0, true);
    plant_staged_migration(&path.0, GROUP / 2);
    let ports = open_operational(RedbStore::open(&path.0).expect("open staged"));
    assert_eq!(
        ports
            .online_retention_step(sequences(1), sequences(256))
            .expect("staged step"),
        OnlineRetentionStep::Held(OnlineRetentionHold::Fenced(
            riffdb_storage_api::RetentionFenceBinding::StagedMigrationFrozenFrontier
        ))
    );
    drop(ports);
    assert!(retention_tombstones(&path.0).is_empty());
}

#[test]
fn online_retention_crash_child() {
    let Ok(mode) = std::env::var(ONLINE_RETENTION_CHILD_MODE) else {
        return;
    };
    let path = PathBuf::from(std::env::var_os(ONLINE_RETENTION_CHILD_PATH).expect("child path"));
    let controller = match mode.as_str() {
        "before-step-commit" => {
            RedbTestController::abort_before_commit(RedbTestOperation::OnlineRetentionStep)
        }
        "after-step-commit" => {
            RedbTestController::abort_after_commit(RedbTestOperation::OnlineRetentionStep)
        }
        _ => panic!("unknown online retention crash mode"),
    };
    let ports = open_operational(
        RedbStore::open_with_test_controller(&path, controller).expect("open armed store"),
    );
    let _ = ports.online_retention_step(sequences(2 * GROUP), sequences(GROUP));
    panic!("the armed online retention step did not abort the process");
}

// req: STO-032, REC-001, REC-002
/// OBL-0182-5 proof (process arms; the riffdb-sim seeded schedules are in
/// `riffdb-sim`). A process killed immediately before or after one online
/// step's atomic transaction (its row deletes, tombstone append, watermark
/// advance, and first-step checkpoint deletion) reopens with a valid
/// watermark, a contiguous tombstone chain that ends at it, and allocator
/// agreement; repeated restart creates no command, event, outcome,
/// provenance, audit, projection, outbox, or allocator advance; and the lane
/// resumes to its target from wherever the crash left it.
#[test]
fn online_retention_crash_windows_recover_to_a_valid_watermark() {
    for (mode, expected_watermark) in [("before-step-commit", 0), ("after-step-commit", GROUP)] {
        let path = TestDatabasePath::new("online-retention-crash");
        let fixtures = prepare_segmented_history(&path.0, true);
        strip_checkpoint_meta(&path.0);
        complete_startup_pass(RedbStore::open(&path.0).expect("checkpoint pass"));
        let status = Command::new(std::env::current_exe().expect("current test executable"))
            .args(["--exact", "online_retention::online_retention_crash_child"])
            .env(ONLINE_RETENTION_CHILD_MODE, mode)
            .env(ONLINE_RETENTION_CHILD_PATH, &path.0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("run online retention child");
        assert_child_aborted(status, mode);

        // The step is one atomic transaction: after the kill it is either
        // wholly absent or wholly present.
        let tombstones = retention_tombstones(&path.0);
        let watermark = tombstones
            .last()
            .map_or(0, |tombstone| tombstone.last_sequence());
        assert_eq!(watermark, expected_watermark, "{mode}");
        assert_eq!(
            retention_meta_present(&path.0, "validated_prefix_checkpoint/v1"),
            expected_watermark == 0,
            "{mode}: the checkpoint deletion is atomic with the first step"
        );
        let findings =
            collect_structural_findings(RedbStore::open(&path.0).expect("reopen after kill"));
        assert!(findings.is_empty(), "{mode}: {findings:?}");

        // Repeated restart advances nothing.
        let before =
            riffdb_storage_redb::benchmark_support::recovery_authority_snapshot_v1(&path.0)
                .expect("authority after first recovery");
        drop(open_operational(
            RedbStore::open(&path.0).expect("second restart"),
        ));
        let after = riffdb_storage_redb::benchmark_support::recovery_authority_snapshot_v1(&path.0)
            .expect("authority after second restart");
        assert!(
            before.same_authority(&after),
            "{mode}: a repeated restart advanced authority"
        );

        // The lane resumes from the recovered watermark to its target.
        let ports = open_operational(RedbStore::open(&path.0).expect("resume lane"));
        let mut through = expected_watermark;
        loop {
            match ports
                .online_retention_step(sequences(2 * GROUP), sequences(GROUP))
                .expect("resumed step")
            {
                OnlineRetentionStep::Advanced { through: next, .. } => {
                    assert_eq!(next, through + GROUP, "{mode}: contiguous resume");
                    through = next;
                }
                OnlineRetentionStep::Held(OnlineRetentionHold::TargetReached) => break,
                other => panic!("{mode}: unexpected resume outcome {other:?}"),
            }
        }
        assert_eq!(through, 2 * GROUP, "{mode}");
        assert_found(&ports, &fixtures[0]);
        assert_pruned(&ports, 2 * GROUP);
    }
}

// req: STO-032
/// ADR-0085 Amendment 2 keeps a pruned command's idempotency, provenance,
/// and audit views. The prune materializes them, and must also remove the
/// ADR-0165 locator rows that still named the deleted segment: lookups read
/// the locator first, so before this an idempotent retry of any pruned
/// command failed closed as corruption. Offline and online share the path.
#[test]
fn a_pruned_command_still_answers_its_idempotent_retry() {
    let path = TestDatabasePath::new("retention-pruned-idempotent-retry");
    let fixtures = prepare_segmented_history(&path.0, true);
    let retention = riffdb_storage_redb::RedbOfflineRetention::bind(&path.0);
    retention
        .add_hold("retry-cap", GROUP, "prune one segment")
        .expect("install hold");
    retention.prune_to(GROUP).expect("offline prune");
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen pruned"));
    assert_found(&ports, &fixtures[0]);
    assert_found(
        &ports,
        &fixtures[usize::try_from(GROUP - 1).expect("index")],
    );
    assert_pruned(&ports, 1);
    drop(ports);
    assert!(
        collect_structural_findings(RedbStore::open(&path.0).expect("validate")).is_empty(),
        "removing the pruned locators keeps the database structurally valid"
    );
}

/// Asserts a commit scan starting at or below `watermark` is typed pruned and
/// one starting above it reads the retained history.
fn assert_scans_classify_pruned_history(
    reader: &impl riffdb_storage_api::AuthoritativeScanReader,
    watermark: u64,
) {
    use riffdb_storage_api::{CommitScanRequest, StorageScanLimit};
    let limit = StorageScanLimit::new(4).expect("scan limit");
    for request in [
        CommitScanRequest::initial(limit),
        CommitScanRequest::initial_after(CommitSequence::new(watermark - 1).expect("after"), limit),
    ] {
        assert_eq!(
            reader
                .scan_commits(request)
                .expect_err("a scan into pruned history is refused")
                .kind(),
            riffdb_storage_api::StorageErrorKind::HistoryPruned
        );
    }
    let page = reader
        .scan_commits(CommitScanRequest::initial_after(
            CommitSequence::new(watermark).expect("after"),
            limit,
        ))
        .expect("a scan from the watermark reads retained history");
    assert_eq!(
        page.records()[0].value().commit_sequence(),
        CommitSequence::new(watermark + 1).expect("first retained")
    );
}

// req: STO-032
/// A commit scan that starts in pruned history is typed `HistoryPruned`, as
/// a point read is, whether the prefix was pruned online or offline, and
/// through the live reader or an owned snapshot.
#[test]
fn commit_scans_into_pruned_history_are_typed_pruned() {
    use riffdb_storage_api::OwnedSnapshotReader;
    let path = TestDatabasePath::new("online-retention-scan-pruned");
    prepare_segmented_history(&path.0, true);
    let ports = open_operational(RedbStore::open(&path.0).expect("open"));
    ports
        .online_retention_step(sequences(2 * GROUP), sequences(256))
        .expect("online step");
    assert_scans_classify_pruned_history(&ports, 2 * GROUP);
    let snapshot = ports.open_owned_snapshot().expect("owned snapshot");
    assert_scans_classify_pruned_history(&snapshot, 2 * GROUP);
    drop(snapshot);
    drop(ports);

    riffdb_storage_redb::RedbOfflineRetention::bind(&path.0)
        .prune_to(3 * GROUP)
        .expect("offline prune");
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen"));
    assert_scans_classify_pruned_history(&ports, 3 * GROUP);
}
