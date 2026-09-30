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
        // The damaged history is never presented as absent or as other bytes:
        // the open refuses, or every read of it does, with a typed error.
        match RedbStore::open(&path.0) {
            Err(error) => assert_eq!(error.kind(), StorageErrorKind::CorruptData, "{damage}"),
            Ok(store) => {
                let ports = open_operational(store);
                let fixture = command_fixture_at(1);
                assert_eq!(
                    ports
                        .read_commit(fixture.records.commit().commit_sequence())
                        .map_err(|error| error.kind()),
                    Err(StorageErrorKind::CorruptData),
                    "{damage}"
                );
                assert_history_readers(&ports, 3);
            }
        }
    }
}
