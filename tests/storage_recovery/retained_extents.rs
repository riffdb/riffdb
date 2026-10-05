//! ADR-0263: an installed asynchronous checkpoint extent is sealed and
//! retained with every frame it held, not recycled as the next spare.
// req: REC-005

use super::*;
use riffdb_storage_api::{
    BackupBuildMetadataV1, OfflineBackupPersistencePort, OfflineRestoreOverwritePolicyV1,
    OfflineRestorePersistencePort, OfflineRestoreResultV1, StorageErrorKind,
};
use riffdb_storage_redb::RedbCheckpointTestPoint as Point;
use riffdb_storage_redb::{
    HistoryStubCensusFixture, history_stub_census_fixture, hydrate_history_stubs_fixture,
    overlay_commits_fixture, sealed_journal_extents_fixture, unseal_journal_extent_fixture,
};

fn seal(
    ports: &RedbOperationalPorts,
    ordinal: u64,
) -> <RedbDurabilityEpoch as DeferredCommandEpoch>::Fence {
    let epoch = ports
        .begin_deferred_command_epoch()
        .expect("begin command epoch");
    DeferredCommandEpoch::seal(apply_unpublished_command_fixture(
        epoch,
        &command_fixture_at(ordinal),
    ))
    .expect("seal complete command graph")
}

fn assert_committed(ports: &RedbOperationalPorts, ordinal: u64) {
    let fixture = command_fixture_at(ordinal);
    assert_eq!(
        ports
            .read_commit(fixture.records.commit().commit_sequence())
            .expect("commit read"),
        Some(fixture.records.commit().clone()),
    );
}

pub(super) fn prepare_two_sealed_commands(path: &Path) -> PathBuf {
    prepare_command_database(path);
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports = open_operational(RedbStore::open_with_test_controller(path, controller).unwrap());
    assert!(command_audit_phases(&ports).is_empty());
    seal(&ports, 1).wait().unwrap();
    seal(&ports, 2).wait().unwrap();
    schedule.arm(Point::Materialized);
    schedule.request_checkpoint();
    drop(ports.begin_deferred_command_epoch().unwrap());
    schedule.wait(Point::Materialized);
    schedule.release(Point::Materialized);
    drop(ports.begin_deferred_command_epoch().unwrap());
    drop(ports);
    drop(schedule);
    let extents = sealed_journal_extents_fixture(path).unwrap();
    assert_eq!(extents.len(), 1);
    let mut name = path.as_os_str().to_os_string();
    name.push(format!(".riffjournal.sealed.{:020}", extents[0].0));
    let extent_path = PathBuf::from(name);
    {
        let database = Database::open(path).unwrap();
        for sequence in 1..=2 {
            retention_deliver_outbox_status_at(&database, sequence);
        }
    }
    extent_path
}

// req: STO-030, STO-031, STO-032
#[test]
fn sealed_extent_retirement_waits_for_receipts_and_pinned_readers() {
    use riffdb_storage_api::OwnedSnapshotReader;
    use riffdb_storage_redb::{OnlineRetentionStep, source_history_reclamation_fixture};
    use std::num::NonZeroU64;

    let path = TestDatabasePath::new("sealed-retirement-references");
    let extent_path = prepare_two_sealed_commands(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).unwrap());
    commit_command_group(&ports, &[command_fixture_at(3)]);
    let lane = ports.online_retention_lane();
    let retain = NonZeroU64::new(1).unwrap();
    let budget = NonZeroU64::new(256).unwrap();
    assert_eq!(
        lane.step(retain, budget).unwrap(),
        OnlineRetentionStep::Advanced {
            through: 2,
            sequences: 2
        }
    );
    assert!(
        extent_path.exists(),
        "application pruning cannot delete live receipt bytes"
    );
    let reader = ports.open_owned_snapshot().unwrap();
    let mut source = source_history_reclamation_fixture(&ports);
    assert!(
        !source.step().unwrap(),
        "a first observation is not a later checkpoint"
    );
    commit_command_group(&ports, &[command_fixture_at(4)]);
    assert!(
        source.step().unwrap(),
        "only the existing source protocol removes receipts"
    );
    for _ in 0..3 {
        lane.step(retain, budget).unwrap();
    }
    assert!(
        extent_path.exists(),
        "a pre-reclamation snapshot still owns its receipt view"
    );
    drop(reader);
    for _ in 0..3 {
        lane.step(retain, budget).unwrap();
    }
    assert!(
        !extent_path.exists(),
        "durably unreferenced extent leaves its authoritative name"
    );
    let mut recycled = path.0.as_os_str().to_os_string();
    recycled.push(".riffjournal.recycled");
    assert!(
        PathBuf::from(recycled).exists(),
        "retired blocks become a bounded reusable pool"
    );
    assert!(matches!(
        ports
            .lookup_admission(command_fixture_at(1).candidates)
            .unwrap(),
        AdmissionLookupResultV1::Found(_)
    ));
    drop(source);
    drop(lane);
    drop(ports);
    let reopened = open_operational(RedbStore::open(&path.0).unwrap());
    assert_committed(&reopened, 3);
    assert_committed(&reopened, 4);
    assert!(matches!(
        reopened
            .lookup_admission(command_fixture_at(1).candidates)
            .unwrap(),
        AdmissionLookupResultV1::Found(_)
    ));
}

// req: STO-030, STO-031, STO-032, REC-001, REC-002, REC-005
#[test]
fn production_retention_lane_releases_receipts_then_retires_extent_after_reader_release() {
    use riffdb_storage_api::OwnedSnapshotReader;
    use std::num::NonZeroU64;

    let path = TestDatabasePath::new("composed-receipt-retirement");
    let extent = prepare_two_sealed_commands(&path.0);
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports =
        open_operational(RedbStore::open_with_test_controller(&path.0, controller).unwrap());
    seal(&ports, 3).wait().unwrap();
    complete_checkpoint(&ports, &schedule);
    let lane = ports.online_retention_lane();
    let retain = NonZeroU64::new(1).unwrap();
    let budget = NonZeroU64::new(256).unwrap();
    let reader = ports.open_owned_snapshot().unwrap();
    for _ in 0..4 {
        lane.step(retain, budget).unwrap();
    }
    seal(&ports, 4).wait().unwrap();
    complete_checkpoint(&ports, &schedule);
    for _ in 0..4 {
        lane.step(retain, budget).unwrap();
    }
    seal(&ports, 5).wait().unwrap();
    complete_checkpoint(&ports, &schedule);
    let mut removed = 0;
    let mut reader_holds = 0;
    for _ in 0..8 {
        let (_, phases) = lane.step_timed(retain, budget, false).unwrap();
        removed += phases.reclaimed_receipts;
        reader_holds += phases.extent_reader_holds;
        assert!(phases.reclaimed_receipts <= 256);
    }
    assert!(removed > 0, "the production owner releases source receipts");
    assert!(
        reader_holds > 0,
        "the captured reader still prevents file retirement"
    );
    assert!(extent.exists());
    drop(reader);
    let mut retired = 0;
    let mut remaining_reader_holds = 0;
    let mut remaining_reference_holds = 0;
    for _ in 0..8 {
        let (_, phases) = lane.step_timed(retain, budget, false).unwrap();
        retired += phases.deleted_extents + phases.recycled_extents;
        remaining_reader_holds += phases.extent_reader_holds;
        remaining_reference_holds += phases.extent_reference_holds;
    }
    assert_eq!(
        retired, 1,
        "remaining reader holds={remaining_reader_holds}, reference holds={remaining_reference_holds}"
    );
    assert!(!extent.exists());
    assert!(matches!(
        ports
            .lookup_admission(command_fixture_at(1).candidates)
            .unwrap(),
        AdmissionLookupResultV1::Found(_)
    ));
    assert_committed(&ports, 5);
    lane.refresh_validated_prefix(
        startup_inputs(),
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )
    .unwrap();
    drop(lane);
    drop(ports);
    drop(schedule);

    // No clean-close ceremony: after pruning, the rebuilt optional evidence
    // must support bounded dirty readiness and an actual next durable write.
    let store = RedbStore::open(&path.0).unwrap();
    let mut session = store.begin_structural_evidence(startup_inputs()).unwrap();
    assert!(
        session.bounded_dirty_startup_path(),
        "refresh must avoid population validation before readiness"
    );
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let structural = loop {
        match session
            .read_structural_evidence(cursor, EvidencePageLimit::new(64).unwrap())
            .unwrap()
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty());
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (_, historical) = riffdb_catalog::validate_catalog_history(&mut session)
        .unwrap()
        .into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("refreshed source must open cleanly");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant.into_operational_after_catalog_validation().unwrap();
    commit_command_group(&ports, &[command_fixture_at(6)]);
    assert_committed(&ports, 6);
    assert!(matches!(
        ports
            .lookup_admission(command_fixture_at(1).candidates)
            .unwrap(),
        AdmissionLookupResultV1::Found(_)
    ));
}

fn complete_checkpoint(
    ports: &RedbOperationalPorts,
    schedule: &riffdb_storage_redb::RedbCheckpointTestSchedule,
) {
    schedule.arm(Point::Materialized);
    schedule.request_checkpoint();
    drop(ports.begin_deferred_command_epoch().unwrap());
    schedule.wait(Point::Materialized);
    schedule.release(Point::Materialized);
    drop(ports.begin_deferred_command_epoch().unwrap());
    riffdb_storage_redb::join_extent_sealing_fixture(ports);
}

// req: STO-030, STO-031, STO-032, END-008
#[test]
fn production_receipt_reclamation_progresses_with_interleaved_durable_writes() {
    use std::num::NonZeroU64;

    let path = TestDatabasePath::new("receipt-reclamation-writer-progress");
    prepare_command_database(&path.0);
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports =
        open_operational(RedbStore::open_with_test_controller(&path.0, controller).unwrap());
    // More than one validation page prevents a small idle fixture from hiding
    // the exact-root admission problem. Every command uses the existing full
    // command/provenance/outcome/event fixture; no history row is injected.
    for ordinal in 1..=600 {
        seal(&ports, ordinal).wait().unwrap();
    }
    complete_checkpoint(&ports, &schedule);
    let lane = ports.online_retention_lane();
    // Keep application history throughout this diagnostic so logical pruning
    // cannot explain an absent commit or supply the required source progress.
    let retain = NonZeroU64::new(u64::MAX).unwrap();
    let budget = NonZeroU64::new(256).unwrap();
    let mut removed_during_writes = 0;
    let mut pending_pages = 0;
    for ordinal in 601..=624 {
        let (_, phases) = lane.step_timed(retain, budget, false).unwrap();
        assert!(phases.reclaimed_receipts <= 256);
        removed_during_writes += phases.reclaimed_receipts;
        pending_pages += u64::from(phases.receipt_validation_pending);
        // Explicit checkpoint hooks, rather than sleeps, publish another
        // independently durable root before the next validation page.
        seal(&ports, ordinal).wait().unwrap();
        complete_checkpoint(&ports, &schedule);
    }
    assert!(pending_pages > 0, "exercise the paged validation owner");
    // Give the same owner an idle exact-end pass, then publish the later
    // independent checkpoint required by the source-reclamation protocol.
    for _ in 0..24 {
        let (_, phases) = lane.step_timed(retain, budget, false).unwrap();
        assert!(phases.reclaimed_receipts <= 256);
    }
    seal(&ports, 625).wait().unwrap();
    complete_checkpoint(&ports, &schedule);
    let mut removed_after_writes = 0;
    for _ in 0..24 {
        let (_, phases) = lane.step_timed(retain, budget, false).unwrap();
        assert!(phases.reclaimed_receipts <= 256);
        removed_after_writes += phases.reclaimed_receipts;
    }
    assert!(
        removed_after_writes > 0,
        "the same fixture must permit receipt reclamation at an idle checkpoint"
    );
    assert!(
        removed_during_writes > 0,
        "source reclamation starved during 24 durable writer turns; \
         removed after writes stopped={removed_after_writes}"
    );
}

// req: STO-030, STO-031, STO-032, END-008
#[test]
fn production_receipt_continuation_survives_command_audit_materialization() {
    use riffdb_storage_redb::OnlineRetentionStep;
    use std::num::NonZeroU64;

    let path = TestDatabasePath::new("receipt-continuation-command-audit");
    prepare_command_database(&path.0);
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports =
        open_operational(RedbStore::open_with_test_controller(&path.0, controller).unwrap());
    for ordinal in 1..=600 {
        seal(&ports, ordinal).wait().unwrap();
    }
    complete_checkpoint(&ports, &schedule);
    drop(ports);
    drop(schedule);
    {
        let database = Database::open(&path.0).unwrap();
        for ordinal in 1..=600 {
            retention_deliver_outbox_status_at(&database, ordinal);
        }
    }
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports =
        open_operational(RedbStore::open_with_test_controller(&path.0, controller).unwrap());
    let lane = ports.online_retention_lane();
    let mut removed = 0;
    let mut pruned = 0;
    let mut invalidations = 0;
    // Each logical prune materializes two exact command-audit records from
    // their locators. Those representations grant no source-control authority.
    // A writer/checkpoint follows every page, with no idle validation window.
    for ordinal in 601..=624 {
        let (step, phases) = lane
            .step_timed(
                NonZeroU64::new(1).unwrap(),
                NonZeroU64::new(1).unwrap(),
                true,
            )
            .unwrap();
        if matches!(step, OnlineRetentionStep::Advanced { .. }) {
            pruned += 1;
        }
        removed += phases.reclaimed_receipts;
        invalidations += phases.source_validation_invalidations;
        assert!(phases.reclaimed_receipts <= 256);
        seal(&ports, ordinal).wait().unwrap();
        complete_checkpoint(&ports, &schedule);
    }
    assert_eq!(pruned, 24, "exercise actual logical pruning every turn");
    assert!(
        removed > 0,
        "command-audit materialization starved source reclamation; invalidations={invalidations}"
    );
    assert!(matches!(
        ports
            .lookup_admission(command_fixture_at(1).candidates)
            .unwrap(),
        AdmissionLookupResultV1::Found(_)
    ));
    assert_committed(&ports, 624);
}

// req: STO-030, STO-031, STO-032
#[test]
fn logical_pruning_crosses_a_large_sealed_extent_in_bounded_steps() {
    use riffdb_storage_redb::OnlineRetentionStep;
    use std::num::NonZeroU64;
    let path = TestDatabasePath::new("sealed-large-bounded-prune");
    prepare_command_database(&path.0);
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports =
        open_operational(RedbStore::open_with_test_controller(&path.0, controller).unwrap());
    assert!(command_audit_phases(&ports).is_empty());
    for ordinal in 1..=257 {
        seal(&ports, ordinal).wait().unwrap();
    }
    schedule.arm(Point::Materialized);
    schedule.request_checkpoint();
    drop(ports.begin_deferred_command_epoch().unwrap());
    schedule.wait(Point::Materialized);
    schedule.release(Point::Materialized);
    drop(ports.begin_deferred_command_epoch().unwrap());
    drop(ports);
    drop(schedule);
    assert_eq!(
        sealed_journal_extents_fixture(&path.0).unwrap()[0].3,
        Some(257)
    );
    {
        let database = Database::open(&path.0).unwrap();
        for sequence in 1..=257 {
            retention_deliver_outbox_status_at(&database, sequence);
        }
    }
    let ports = open_operational(RedbStore::open(&path.0).unwrap());
    commit_command_group(&ports, &[command_fixture_at(258)]);
    let lane = ports.online_retention_lane();
    for (through, sequences) in [(127, 127), (254, 127), (257, 3)] {
        assert_eq!(
            lane.step(NonZeroU64::new(1).unwrap(), NonZeroU64::new(127).unwrap())
                .unwrap(),
            OnlineRetentionStep::Advanced { through, sequences }
        );
        assert!(sequences <= 256);
        assert!(matches!(
            ports
                .lookup_admission(command_fixture_at(through).candidates)
                .unwrap(),
            AdmissionLookupResultV1::Found(_)
        ));
    }
    assert_committed(&ports, 258);
    drop(lane);
    drop(ports);
    let reopened = open_operational(RedbStore::open(&path.0).unwrap());
    assert_committed(&reopened, 258);
    assert_eq!(
        reopened
            .read_commit(CommitSequence::new(257).unwrap())
            .unwrap_err()
            .kind(),
        StorageErrorKind::HistoryPruned
    );
    assert!(matches!(
        reopened
            .lookup_admission(command_fixture_at(1).candidates)
            .unwrap(),
        AdmissionLookupResultV1::Found(_)
    ));
}

// req: STO-030, STO-031
#[test]
fn logical_pruning_waits_for_an_unsealed_command_extent() {
    use riffdb_storage_redb::{OnlineRetentionHold, OnlineRetentionStep};
    use std::num::NonZeroU64;
    let path = TestDatabasePath::new("unsealed-prune-hold");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).unwrap());
    assert!(command_audit_phases(&ports).is_empty());
    seal(&ports, 1).wait().unwrap();
    seal(&ports, 2).wait().unwrap();
    assert_eq!(
        ports
            .online_retention_step(NonZeroU64::new(1).unwrap(), NonZeroU64::new(256).unwrap())
            .unwrap(),
        OnlineRetentionStep::Held(OnlineRetentionHold::RowBoundary)
    );
    assert_committed(&ports, 1);
    assert_committed(&ports, 2);
}

#[test]
fn an_installed_checkpoint_extent_is_sealed_with_a_footer_over_its_frames() {
    let path = TestDatabasePath::new("retained-extent-seal");
    prepare_command_database(&path.0);
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports = open_operational(
        RedbStore::open_with_test_controller(&path.0, controller).expect("controlled open"),
    );
    assert!(command_audit_phases(&ports).is_empty()); // warm before the mutation lease
    seal(&ports, 1).wait().expect("publish first group");
    seal(&ports, 2).wait().expect("publish second group");

    schedule.arm(Point::Materialized);
    schedule.request_checkpoint();
    drop(
        ports
            .begin_deferred_command_epoch()
            .expect("start the checkpoint"),
    );
    schedule.wait(Point::Materialized);
    schedule.release(Point::Materialized);
    drop(
        ports
            .begin_deferred_command_epoch()
            .expect("install the checkpoint at quiescence"),
    );
    let checkpoint_path = path.0.with_extension("redb.riffjournal.checkpoint");
    drop(ports);
    drop(schedule);
    // The sealer retires the installed extent off the writer's path; closing
    // the store waits for it.
    assert!(
        !checkpoint_path.exists(),
        "the installed extent left the checkpoint name"
    );

    let sealed = sealed_journal_extents_fixture(&path.0).expect("verified sealed extents");
    assert_eq!(
        sealed.len(),
        1,
        "the installed extent is retained: {sealed:?}"
    );
    let (_, frames, first, last) = sealed[0];
    assert!(frames >= 2, "every checkpointed frame stays: {sealed:?}");
    assert_eq!(
        last,
        Some(2),
        "the sealed extent ends at the checkpointed group"
    );
    assert!(first.is_some_and(|first| first <= 1));
    assert!(
        !path.0.with_extension("redb.riffjournal.sealing").exists(),
        "sealing finished before the store dropped"
    );

    for _ in 0..2 {
        let reopened = open_operational(RedbStore::open(&path.0).expect("restart"));
        assert_committed(&reopened, 1);
        assert_committed(&reopened, 2);
        drop(reopened);
        assert_eq!(
            sealed_journal_extents_fixture(&path.0).expect("sealed extents after restart"),
            sealed,
            "restarts leave retained extents untouched"
        );
    }
}

#[test]
fn a_backup_carries_sealed_extents_so_a_restored_copy_resolves_its_history() {
    let path = TestDatabasePath::new("retained-extent-backup");
    prepare_command_database(&path.0);
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports = open_operational(
        RedbStore::open_with_test_controller(&path.0, controller).expect("controlled open"),
    );
    assert!(command_audit_phases(&ports).is_empty());
    seal(&ports, 1).wait().expect("publish first group");
    seal(&ports, 2).wait().expect("publish second group");
    schedule.arm(Point::Materialized);
    schedule.request_checkpoint();
    drop(
        ports
            .begin_deferred_command_epoch()
            .expect("start the checkpoint"),
    );
    schedule.wait(Point::Materialized);
    schedule.release(Point::Materialized);
    drop(
        ports
            .begin_deferred_command_epoch()
            .expect("install the checkpoint"),
    );
    drop(ports);
    drop(schedule);
    let sealed = sealed_journal_extents_fixture(&path.0).expect("sealed extents");
    assert_eq!(sealed.len(), 1);

    let root = ScratchScope::new("retained-extent-backup");
    let backup_dir = root.path().join("backup");
    let build = BackupBuildMetadataV1::new("0.1.0", "0123456789abcdef", "rustc-1.97.0", 1, vec![])
        .expect("build metadata");
    let manifest = riffdb_storage_redb::RedbOfflineBackup::bind(&path.0, &backup_dir)
        .create_offline_backup(&build)
        .expect("backup");
    assert_eq!(manifest.checksums().len(), 4, "one sealed extent artifact");

    // The source's extents are gone: only the backup's copy can resolve.
    for entry in std::fs::read_dir(path.0.parent().expect("parent")).expect("list") {
        let entry = entry.expect("entry");
        if entry
            .file_name()
            .to_string_lossy()
            .contains(".riffjournal.sealed.")
        {
            std::fs::remove_file(entry.path()).expect("remove source extent");
        }
    }
    // An archive reads the backup's history through its own artifacts.
    let archived = riffdb_storage_redb::RedbVerifiedArchiveBackup::open(&backup_dir)
        .expect("archive binds a backup whose history is stubbed");
    assert_eq!(archived.manifest(), &manifest);
    drop(archived);
    let restore_dir = root.path().join("restored");
    std::fs::create_dir_all(&restore_dir).expect("restore dir");
    let result = riffdb_storage_redb::RedbOfflineRestore::bind(&backup_dir, &restore_dir)
        .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
        .expect("restore");
    assert!(matches!(result, OfflineRestoreResultV1::Restored { .. }));
    let restored = restore_dir.join("database.redb");
    assert_eq!(
        sealed_journal_extents_fixture(&restored).expect("restored extents"),
        sealed
    );
    let reopened = open_operational(RedbStore::open(&restored).expect("open restored"));
    assert_committed(&reopened, 1);
    assert_committed(&reopened, 2);
    drop(reopened);
    let findings = collect_structural_findings(RedbStore::open(&restored).expect("validate"));
    assert!(
        findings.is_empty(),
        "restored history validates: {findings:?}"
    );
}

/// Rows the given census holds beyond `baseline`: `(stubs, full values)` for
/// COMMITS and HISTORY.
fn census_growth(
    baseline: HistoryStubCensusFixture,
    census: HistoryStubCensusFixture,
) -> ((usize, usize), (usize, usize)) {
    let grow = |before: (usize, usize), after: (usize, usize)| {
        (
            after
                .0
                .checked_sub(before.0)
                .expect("stubs never disappear"),
            after
                .1
                .checked_sub(before.1)
                .expect("full rows never disappear"),
        )
    };
    (
        grow(baseline.commits, census.commits),
        grow(baseline.history, census.history),
    )
}

// req: REC-005
#[test]
fn a_graceful_close_drains_journaled_history_as_history_stubs() {
    let path = TestDatabasePath::new("retained-extent-drain");
    prepare_command_database(&path.0);
    let baseline = history_stub_census_fixture(&path.0).expect("baseline census");
    let ports = open_operational(RedbStore::open(&path.0).expect("open"));
    seal(&ports, 1).wait().expect("publish first group");
    seal(&ports, 2).wait().expect("publish second group");
    // The graceful close barrier drains the published suffix synchronously.
    assert_eq!(
        ports.complete_graceful_close().lifecycle(),
        riffdb_storage_redb::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    drop(ports);

    let census = history_stub_census_fixture(&path.0).expect("census after drain");
    let ((segment_stubs, full_segments), (receipt_stubs, full_receipts)) =
        census_growth(baseline, census);
    assert_eq!(segment_stubs, 2, "each drained segment is a stub");
    assert_eq!(full_segments, 0, "no drained segment bytes reach redb");
    assert_eq!(receipt_stubs, 2, "each drained receipt is a stub");
    // Opening and closing the store are direct ceremonies, which keep their
    // receipt bytes (ADR-0263 Amendment 1); no journaled receipt does.
    assert_eq!(full_receipts, 2, "one open and one close ceremony");
    assert_eq!(
        census.journaled_full_receipts,
        baseline.journaled_full_receipts
    );

    let reopened = open_operational(RedbStore::open(&path.0).expect("reopen"));
    assert_committed(&reopened, 1);
    assert_committed(&reopened, 2);
    drop(reopened);
    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("validate"));
    assert!(
        findings.is_empty(),
        "drained history validates: {findings:?}"
    );
}

// req: REC-005
#[test]
fn crash_recovery_replays_journaled_history_as_history_stubs() {
    let path = TestDatabasePath::new("retained-extent-recovery");
    prepare_command_database(&path.0);
    run_crashing_child("after-command-epoch-tail", &path.0);
    // The crashed epoch is durable only in the journal.
    let baseline = history_stub_census_fixture(&path.0).expect("census after crash");
    assert_eq!(baseline.commits, (0, 0));

    let ports = open_operational(RedbStore::open(&path.0).expect("recover"));
    assert_postcommit_command_state(&ports, &command_fixture());
    drop(ports);
    let census = history_stub_census_fixture(&path.0).expect("census after recovery");
    let ((segment_stubs, full_segments), (receipt_stubs, full_receipts)) =
        census_growth(baseline, census);
    assert_eq!(segment_stubs, 1, "the replayed segment is a stub");
    assert_eq!(full_segments, 0, "no replayed segment bytes reach redb");
    assert_eq!(receipt_stubs, 1, "the replayed receipt is a stub");
    // The recovering open is a direct ceremony, which keeps its receipt bytes
    // (ADR-0263 Amendment 1); no journaled receipt does.
    assert_eq!(full_receipts, 1, "one open ceremony");
    assert_eq!(
        census.journaled_full_receipts,
        baseline.journaled_full_receipts
    );

    let ports = open_operational(RedbStore::open(&path.0).expect("repeat recovery"));
    assert_postcommit_command_state(&ports, &command_fixture());
    drop(ports);
    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("validate"));
    assert!(
        findings.is_empty(),
        "recovered history validates: {findings:?}"
    );
}

/// Runs one asynchronous checkpoint to installation on `ports`.
fn install_checkpoint(
    ports: &RedbOperationalPorts,
    schedule: &riffdb_storage_redb::RedbCheckpointTestSchedule,
) {
    schedule.arm(Point::Materialized);
    schedule.request_checkpoint();
    drop(
        ports
            .begin_deferred_command_epoch()
            .expect("start the checkpoint"),
    );
    schedule.wait(Point::Materialized);
    schedule.release(Point::Materialized);
    drop(
        ports
            .begin_deferred_command_epoch()
            .expect("install the checkpoint"),
    );
}

// req: REC-005
#[test]
fn a_crash_between_rotation_renames_recovers_to_fresh_extent_generations() {
    let path = TestDatabasePath::new("retained-extent-rotation-crash");
    prepare_command_database(&path.0);
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports = open_operational(
        RedbStore::open_with_test_controller(&path.0, controller).expect("controlled open"),
    );
    assert!(command_audit_phases(&ports).is_empty());
    seal(&ports, 1).wait().expect("publish first group");
    install_checkpoint(&ports, &schedule);
    seal(&ports, 2).wait().expect("publish second group");
    drop(ports);
    drop(schedule);

    // Rotation renames the active extent to the checkpoint name, syncs, then
    // renames the spare over the active name. Stop between the two.
    let active = path.0.with_extension("redb.riffjournal");
    std::fs::rename(
        &active,
        path.0.with_extension("redb.riffjournal.checkpoint"),
    )
    .expect("leave only the checkpoint name");

    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports = open_operational(
        RedbStore::open_with_test_controller(&path.0, controller).expect("recover"),
    );
    let _ = command_audit_phases(&ports); // warm before the mutation lease
    assert_committed(&ports, 1);
    assert_committed(&ports, 2);
    seal(&ports, 3).wait().expect("publish third group");
    install_checkpoint(&ports, &schedule);
    seal(&ports, 4).wait().expect("publish fourth group");
    install_checkpoint(&ports, &schedule);
    drop(ports);
    drop(schedule);

    let sealed = sealed_journal_extents_fixture(&path.0).expect("sealed extents");
    let generations = sealed.iter().map(|extent| extent.0).collect::<Vec<_>>();
    assert!(
        sealed.len() >= 3 && generations.windows(2).all(|pair| pair[0] < pair[1]),
        "every retired extent seals under its own generation: {sealed:?}"
    );
    let reopened = open_operational(RedbStore::open(&path.0).expect("reopen"));
    for ordinal in 1..=4 {
        assert_committed(&reopened, ordinal);
    }
    drop(reopened);
    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("validate"));
    assert!(findings.is_empty(), "history validates: {findings:?}");
}

// req: REC-005
#[test]
fn hydration_rewrites_every_history_stub_as_its_exact_value() {
    let path = TestDatabasePath::new("retained-extent-hydrate");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("open"));
    seal(&ports, 1).wait().expect("publish first group");
    seal(&ports, 2).wait().expect("publish second group");
    assert_eq!(
        ports.complete_graceful_close().lifecycle(),
        riffdb_storage_redb::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    drop(ports);
    let stubbed = history_stub_census_fixture(&path.0).expect("stubbed census");
    assert!(
        stubbed.commits.0 > 0 && stubbed.history.0 > 0,
        "{stubbed:?}"
    );

    for _ in 0..2 {
        hydrate_history_stubs_fixture(&path.0).expect("hydrate");
        let hydrated = history_stub_census_fixture(&path.0).expect("hydrated census");
        assert_eq!(
            (hydrated.commits, hydrated.history),
            (
                (0, stubbed.commits.0 + stubbed.commits.1),
                (0, stubbed.history.0 + stubbed.history.1)
            ),
            "every stub became a full row and no row was added or lost"
        );
    }
    // The hydrated values are exactly what the stubs resolved to.
    let reopened = open_operational(RedbStore::open(&path.0).expect("reopen"));
    assert_committed(&reopened, 1);
    assert_committed(&reopened, 2);
    drop(reopened);
    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("validate"));
    assert!(
        findings.is_empty(),
        "hydrated history validates: {findings:?}"
    );
}

// req: REC-005
#[test]
fn the_uncheckpointed_overlay_holds_history_stubs_not_segments() {
    let path = TestDatabasePath::new("retained-extent-overlay");
    prepare_command_database(&path.0);
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports = open_operational(
        RedbStore::open_with_test_controller(&path.0, controller).expect("controlled open"),
    );
    assert!(command_audit_phases(&ports).is_empty());
    let (_, empty_charge) = overlay_commits_fixture(&ports).expect("empty overlay");
    seal(&ports, 1).wait().expect("publish first group");
    seal(&ports, 2).wait().expect("publish second group");

    let (commits, charge) = overlay_commits_fixture(&ports).expect("overlay census");
    assert_eq!(
        commits,
        (2, 0),
        "each published segment is a stub in the overlay"
    );
    assert!(charge > empty_charge);
    // Readers resolve the stubs from the uncheckpointed frames, exactly.
    assert_committed(&ports, 1);
    assert_committed(&ports, 2);
    drop(ports);
    drop(schedule);
    let reopened = open_operational(RedbStore::open(&path.0).expect("reopen"));
    assert_committed(&reopened, 1);
    assert_committed(&reopened, 2);
}

/// Every reader of the command history named by ADR-0263 Decision 4 returns
/// the exact records of command `ordinal`: commit, provenance, terminal
/// outcome, admission, and durable events.
fn assert_history_readers(ports: &RedbOperationalPorts, ordinal: u64) {
    let fixture = command_fixture_at(ordinal);
    assert_committed(ports, ordinal);
    assert_eq!(
        ports
            .read_provenance(fixture.records.provenance().provenance_id())
            .expect("provenance"),
        Some(fixture.records.provenance().clone())
    );
    assert_eq!(
        ports
            .read_stored_outcome(fixture.pending.identity())
            .expect("outcome"),
        Some(fixture.records.stored_outcome().clone())
    );
    let AdmissionLookupResultV1::Found(admission) = ports
        .lookup_admission(fixture.candidates.clone())
        .expect("admission")
    else {
        panic!("a committed identity is terminal");
    };
    assert_eq!(
        *admission,
        riffdb_storage_api::StoredAdmissionStateV1::StoredOutcome(
            fixture.records.stored_outcome().clone()
        )
    );
    for event in fixture.records.events() {
        assert_eq!(
            ports.read_durable_event(event.event_id()).expect("event"),
            Some(event.clone())
        );
    }
}

/// Publishes commands 1 and 2, installs an asynchronous checkpoint over them,
/// publishes command 3 after it, and closes the store: 1 and 2 are history in
/// a sealed extent, 3 an uncheckpointed frame in the active extent.
fn sealed_and_live_history(label: &str) -> TestDatabasePath {
    let path = TestDatabasePath::new(label);
    prepare_command_database(&path.0);
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports = open_operational(
        RedbStore::open_with_test_controller(&path.0, controller).expect("controlled open"),
    );
    assert!(command_audit_phases(&ports).is_empty());
    seal(&ports, 1).wait().expect("publish first group");
    seal(&ports, 2).wait().expect("publish second group");
    install_checkpoint(&ports, &schedule);
    seal(&ports, 3).wait().expect("publish third group");
    drop(ports);
    drop(schedule);
    path
}

// req: REC-005
#[test]
fn checkpoint_materializes_no_commits_or_history_rows() {
    let path = TestDatabasePath::new("retained-extent-no-history-rows");
    prepare_command_database(&path.0);
    let baseline = history_stub_census_fixture(&path.0).expect("baseline census");
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports = open_operational(
        RedbStore::open_with_test_controller(&path.0, controller).expect("controlled open"),
    );
    assert!(command_audit_phases(&ports).is_empty());
    for ordinal in 1..=3 {
        seal(&ports, ordinal).wait().expect("publish group");
    }
    install_checkpoint(&ports, &schedule);
    drop(ports);
    drop(schedule);

    let census = history_stub_census_fixture(&path.0).expect("census");
    let ((segment_stubs, full_segments), (receipt_stubs, _)) = census_growth(baseline, census);
    assert_eq!(
        segment_stubs, 3,
        "each checkpointed segment is a history stub"
    );
    assert_eq!(full_segments, 0, "no segment row reaches redb");
    assert!(
        receipt_stubs >= 3,
        "each journaled receipt is a history stub"
    );
    assert_eq!(
        census.journaled_full_receipts, baseline.journaled_full_receipts,
        "no journaled receipt row reaches redb"
    );
}

// req: REC-005
#[test]
fn history_readers_resolve_through_the_history_index() {
    let path = sealed_and_live_history("retained-extent-readers");
    assert_eq!(
        sealed_journal_extents_fixture(&path.0)
            .expect("sealed")
            .len(),
        1
    );
    // Reopened, 1 and 2 resolve from the sealed extent, 3 from the live one.
    for _ in 0..2 {
        let ports = open_operational(RedbStore::open(&path.0).expect("reopen"));
        for ordinal in 1..=3 {
            assert_history_readers(&ports, ordinal);
        }
        // A new command's stub resolves from the uncheckpointed frame in memory.
        drop(ports);
    }
    let ports = open_operational(RedbStore::open(&path.0).expect("reopen"));
    let _ = command_audit_phases(&ports);
    seal(&ports, 4).wait().expect("publish fourth group");
    for ordinal in 1..=4 {
        assert_history_readers(&ports, ordinal);
    }
    drop(ports);
    let findings = collect_structural_findings(RedbStore::open(&path.0).expect("validate"));
    assert!(
        findings.is_empty(),
        "every history row resolves: {findings:?}"
    );
}

// req: REC-005
#[test]
fn sealed_extent_crash_windows_recover_to_a_consistent_history_index() {
    // A crash can leave the installed extent at the checkpoint name (before
    // the sealer retired it) or at the sealing name without its footer
    // (before the seal wrote it). A crash between rotation's renames is
    // covered by a_crash_between_rotation_renames_recovers_to_fresh_extent_generations.
    for window in ["checkpoint", "sealing"] {
        let path = sealed_and_live_history(&format!("retained-extent-window-{window}"));
        let sealed = sealed_journal_extents_fixture(&path.0).expect("sealed");
        assert_eq!(sealed.len(), 1);
        unseal_journal_extent_fixture(&path.0, sealed[0].0, window).expect("recreate window");
        assert!(
            sealed_journal_extents_fixture(&path.0)
                .expect("unsealed")
                .is_empty()
        );

        for _ in 0..2 {
            let ports = open_operational(
                RedbStore::open(&path.0)
                    .unwrap_or_else(|error| panic!("recover {window}: {error:?}")),
            );
            for ordinal in 1..=3 {
                assert_history_readers(&ports, ordinal);
            }
            drop(ports);
            assert_eq!(
                sealed_journal_extents_fixture(&path.0).expect("resealed"),
                sealed,
                "recovery reseals the same extent under the same generation ({window})"
            );
        }
        let findings = collect_structural_findings(RedbStore::open(&path.0).expect("validate"));
        assert!(findings.is_empty(), "{window}: {findings:?}");
    }
}

// req: REC-005
#[test]
fn missing_or_corrupt_sealed_extent_is_a_typed_refusal() {
    for damage in ["missing", "corrupt"] {
        let path = sealed_and_live_history(&format!("retained-extent-damage-{damage}"));
        let sealed = sealed_journal_extents_fixture(&path.0).expect("sealed");
        let file = {
            let mut name = path.0.as_os_str().to_os_string();
            name.push(format!(".riffjournal.sealed.{:020}", sealed[0].0));
            std::path::PathBuf::from(name)
        };
        if damage == "missing" {
            std::fs::remove_file(&file).expect("remove sealed extent");
        } else {
            // Flip one byte inside the first frame's payload.
            let mut bytes = std::fs::read(&file).expect("read sealed extent");
            bytes[8192 + 256] ^= 0x5a;
            std::fs::write(&file, bytes).expect("write damaged extent");
        }
        // A fresh process: stub resolution may use any open copy of the same
        // database whose frame hashes match, and parallel tests open copies
        // of this fixture database.
        let status = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .arg("--exact")
            .arg("retained_extents::damaged_sealed_extent_child")
            .arg("--nocapture")
            .env(DAMAGED_DATABASE, &path.0)
            .status()
            .expect("run child");
        assert!(status.success(), "{damage}: {status:?}");
    }
}

const DAMAGED_DATABASE: &str = "RIFFDB_RETAINED_EXTENT_DAMAGED_DATABASE";

/// The damaged history is never presented as absent or as other bytes: the
/// open refuses, or every read of it does, with a typed error.
#[test]
fn damaged_sealed_extent_child() {
    let Some(path) = std::env::var_os(DAMAGED_DATABASE) else {
        return;
    };
    match RedbStore::open(std::path::Path::new(&path)) {
        Err(error) => assert_eq!(error.kind(), StorageErrorKind::CorruptData),
        Ok(store) => {
            let ports = open_operational(store);
            let fixture = command_fixture_at(1);
            assert_eq!(
                ports
                    .read_commit(fixture.records.commit().commit_sequence())
                    .map_err(|error| error.kind()),
                Err(StorageErrorKind::CorruptData)
            );
            assert_history_readers(&ports, 3);
        }
    }
}

// req: STO-030
#[test]
fn interference_seed_delivery_resolves_stopped_retained_history() {
    let path = TestDatabasePath::new("interference-seed-history");
    prepare_two_sealed_commands(&path.0);
    // No operational store owns a resolver in this process at this boundary.
    assert_eq!(
        riffdb_storage_redb::benchmark_support::mark_all_outbox_events_delivered_v1(&path.0)
            .unwrap(),
        2
    );
    assert!(
        retention_tombstones(&path.0).is_empty(),
        "benchmark setup cannot prune authority"
    );
    let ports = open_operational(RedbStore::open(&path.0).unwrap());
    assert_committed(&ports, 1);
    assert_committed(&ports, 2);
}

// req: STO-030, STO-031, STO-032, REC-001, REC-002, REC-005
#[test]
#[ignore = "explicit qualification-sized storage recovery measurement"]
fn population_65536_first_successful_write_after_online_pruning_meets_60_seconds() {
    use riffdb_storage_redb::OnlineRetentionStep;
    const POPULATION: u64 = 65_536;
    let path = TestDatabasePath::new("population-first-write-after-pruning");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).unwrap());
    for first in (1..=POPULATION).step_by(64) {
        let batch = (first..=first + 63)
            .map(command_fixture_at)
            .collect::<Vec<_>>();
        commit_command_group(&ports, &batch);
    }
    drop(ports);
    assert_eq!(
        riffdb_storage_redb::benchmark_support::mark_all_outbox_events_delivered_v1(&path.0)
            .unwrap(),
        POPULATION
    );
    let ports = open_operational(RedbStore::open(&path.0).unwrap());
    let lane = ports.online_retention_lane();
    let mut removed = 0;
    for _ in 0..=POPULATION / 256 {
        match lane
            .step(NonZeroU64::new(16).unwrap(), NonZeroU64::new(256).unwrap())
            .unwrap()
        {
            OnlineRetentionStep::Advanced { sequences, .. } => {
                assert!(sequences <= 256);
                removed += sequences;
            }
            OnlineRetentionStep::Held(_) => break,
        }
    }
    // Atomic 64-command receipt groups retain the entire last group when
    // the requested 16-sequence floor falls inside it.
    assert_eq!(removed, POPULATION - 64);
    drop(lane);
    drop(ports);
    // No prefix refresh or graceful close: include the deferred validation
    // cost in the actual next durable write, rather than stopping at readiness.
    let started = std::time::Instant::now();
    let reopened = open_operational(RedbStore::open(&path.0).unwrap());
    let readiness_us = started.elapsed().as_micros();
    commit_command_group(&reopened, &[command_fixture_at(POPULATION + 1)]);
    let first_write_us = started.elapsed().as_micros();
    assert_committed(&reopened, POPULATION + 1);
    assert!(matches!(
        reopened
            .lookup_admission(command_fixture_at(1).candidates)
            .unwrap(),
        AdmissionLookupResultV1::Found(_)
    ));
    println!(
        "riffdb-storage-recovery-measurement-v1 population={POPULATION} pruned={removed} readiness_us={readiness_us} first_successful_write_us={first_write_us}"
    );
    assert!(
        first_write_us <= 60_000_000,
        "first successful write exceeded the frozen recovery target"
    );
}

// req: STO-030, STO-031, STO-032, REC-001, REC-005
#[test]
fn live_prefix_refresh_preserves_the_next_native_journal_allocation() {
    use std::num::NonZeroU64;
    let path = TestDatabasePath::new("live-prefix-source-allocation");
    prepare_two_sealed_commands(&path.0);
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports =
        open_operational(RedbStore::open_with_test_controller(&path.0, controller).unwrap());
    seal(&ports, 3).wait().unwrap();
    complete_checkpoint(&ports, &schedule);
    let lane = ports.online_retention_lane();
    let retain = NonZeroU64::new(1).unwrap();
    let budget = NonZeroU64::new(256).unwrap();
    for _ in 0..4 {
        lane.step(retain, budget).unwrap();
    }
    seal(&ports, 4).wait().unwrap();
    complete_checkpoint(&ports, &schedule);
    for _ in 0..4 {
        lane.step(retain, budget).unwrap();
    }
    lane.refresh_validated_prefix(
        startup_inputs(),
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )
    .unwrap();
    // No close/reopen between publishing the complete prefix proof and this
    // ordinary command. Restarting here would conceal the stale live allocator.
    seal(&ports, 5).wait().unwrap();
    assert_committed(&ports, 5);
    complete_checkpoint(&ports, &schedule);
    drop(lane);
    drop(ports);
    drop(schedule);
    let reopened = open_operational(RedbStore::open(&path.0).unwrap());
    assert_committed(&reopened, 5);
    assert!(matches!(
        reopened
            .lookup_admission(command_fixture_at(1).candidates)
            .unwrap(),
        AdmissionLookupResultV1::Found(_)
    ));
}
