//! ADR-0263: an installed asynchronous checkpoint extent is sealed and
//! retained with every frame it held, not recycled as the next spare.
// req: REC-005

use super::*;
use riffdb_storage_redb::RedbCheckpointTestPoint as Point;
use riffdb_storage_redb::sealed_journal_extents_fixture;

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
    assert!(
        !checkpoint_path.exists(),
        "the installed extent left the checkpoint name"
    );
    drop(ports);
    drop(schedule);

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
