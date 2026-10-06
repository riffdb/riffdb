//! Real journal, materializer and ordered-publication schedules for WP-810.
// req: STO-001, STO-002, REC-001, REC-002, END-007, END-008

use super::*;
use riffdb_storage_redb::RedbCheckpointTestPoint as Point;
use std::sync::mpsc;

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

fn assert_visible(ports: &RedbOperationalPorts, ordinal: u64, present: bool) {
    let fixture = command_fixture_at(ordinal);
    assert_eq!(
        ports
            .read_entity(&fixture.target)
            .expect("immediate entity read"),
        present.then(|| fixture.records.entities()[0].post_image().clone()),
    );
    assert_eq!(
        ports
            .read_stored_outcome(fixture.pending.identity())
            .expect("outcome read"),
        present.then(|| fixture.records.stored_outcome().clone()),
    );
    assert_eq!(
        ports
            .read_commit(fixture.records.commit().commit_sequence())
            .expect("commit read"),
        present.then(|| fixture.records.commit().clone()),
    );
    assert_eq!(
        ports
            .read_provenance(fixture.records.provenance().provenance_id())
            .expect("provenance read"),
        present.then(|| fixture.records.provenance().clone()),
    );
    let event = &fixture.records.events()[0];
    assert_eq!(
        ports
            .read_durable_event(event.event_id())
            .expect("event read"),
        present.then(|| event.clone()),
    );
}

#[test]
fn checkpoint_materialization_allows_pipeline_but_installation_requires_publication() {
    let path = TestDatabasePath::new("checkpoint-pipeline-schedule");
    prepare_command_database(&path.0);
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports = open_operational(
        RedbStore::open_with_test_controller(&path.0, controller).expect("controlled open"),
    );
    assert!(command_audit_phases(&ports).is_empty()); // warm before the mutation lease
    seal(&ports, 1).wait().expect("publish checkpoint source");
    assert_eq!(
        ports
            .shared_ports()
            .pending_outbox_delivery_count()
            .unwrap(),
        1
    );

    schedule.arm(Point::BeforeMaterialization);
    schedule.arm(Point::Materialized);
    schedule.request_checkpoint();
    let second_epoch = ports
        .begin_deferred_command_epoch()
        .expect("start real checkpoint");
    schedule.wait(Point::BeforeMaterialization);
    schedule.arm(Point::BeforeJournalSync);
    schedule.arm(Point::BeforePublication);
    let mut second = DeferredCommandEpoch::seal(apply_unpublished_command_fixture(
        second_epoch,
        &command_fixture_at(2),
    ))
    .expect("seal while materializer is paused");
    let materializing_requires_drain = second.requires_pipeline_drain();
    schedule.wait(Point::BeforeJournalSync);
    assert!(
        second
            .try_wait()
            .expect("nonblocking durability poll")
            .is_none()
    );
    assert_visible(&ports, 2, false);
    assert_eq!(
        ports
            .shared_ports()
            .pending_outbox_delivery_count()
            .unwrap(),
        1
    );

    // A second real group can be staged on the private frontier while both the
    // checkpoint and its predecessor's durability are independently paused.
    let third = seal(&ports, 3);
    let pipelined_requires_drain = third.requires_pipeline_drain();
    assert_visible(&ports, 3, false);
    let (completed, completion) = mpsc::sync_channel(1);
    let publisher = std::thread::spawn(move || {
        completed
            .send(second.wait())
            .expect("report checked completion");
    });
    assert!(matches!(
        completion.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    schedule.release(Point::BeforeJournalSync);
    schedule.wait(Point::BeforePublication);
    // This pause is reached only after the actual journal receipt succeeds.
    // Durability alone cannot release a result, entity, event or outcome.
    assert_visible(&ports, 2, false);
    assert_visible(&ports, 3, false);
    assert_eq!(
        ports
            .shared_ports()
            .pending_outbox_delivery_count()
            .unwrap(),
        1,
        "a synced but unpublished event cannot inflate delivery backlog"
    );
    assert!(matches!(
        completion.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    schedule.release(Point::BeforePublication);
    completion
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("ordered completion")
        .expect("durable publication");
    publisher.join().expect("publisher joins");
    assert_visible(&ports, 2, true);
    assert_visible(&ports, 3, false);
    assert_eq!(
        ports
            .shared_ports()
            .pending_outbox_delivery_count()
            .unwrap(),
        2
    );

    schedule.release(Point::BeforeMaterialization);
    schedule.wait(Point::Materialized);
    let fourth = seal(&ports, 4);
    let installation_requires_drain = fourth.requires_pipeline_drain();
    let checkpoint_path = path.0.with_extension("redb.riffjournal.checkpoint");
    assert!(
        checkpoint_path.exists(),
        "unpublished groups prevent extent reclamation"
    );
    schedule.release(Point::Materialized);
    third.wait().expect("publish predecessor");
    fourth.wait().expect("publish installation barrier");
    drop(
        ports
            .begin_deferred_command_epoch()
            .expect("install exact checkpoint at quiescence"),
    );
    for ordinal in 1..=4 {
        assert_visible(&ports, ordinal, true);
    }
    drop(ports);
    drop(schedule);
    // Installation hands the covered extent to the sealer, which retires it
    // off the writer's path; closing the store waits for it.
    assert!(
        !checkpoint_path.exists(),
        "the covered extent is reclaimed only after installation"
    );
    for _ in 0..2 {
        let reopened = open_operational(RedbStore::open(&path.0).expect("idempotent restart"));
        for ordinal in 1..=4 {
            assert_visible(&reopened, ordinal, true);
        }
        assert_visible(&reopened, 5, false);
        assert_eq!(command_audit_phases(&reopened).len(), 8);
    }

    assert!(
        !materializing_requires_drain,
        "a pending checkpoint must not serialize journal groups"
    );
    assert!(
        !pipelined_requires_drain,
        "materialization preserves bounded pipelining"
    );
    assert!(
        installation_requires_drain,
        "a ready checkpoint still requests the ordered barrier"
    );
}

#[test]
fn unpublished_capacity_wait_publishes_the_prefix_without_losing_original_tickets() {
    let path = TestDatabasePath::new("checkpoint-pipeline-capacity");
    prepare_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("open capacity fixture"));
    assert!(command_audit_phases(&ports).is_empty());
    // The production unpublished ceiling is 256 transitions. Storage must
    // apply bounded backpressure before admitting the 257th, even if the
    // completion owner has not yet collected its already-submitted tickets.
    let mut pending = Vec::with_capacity(256);
    for ordinal in 1..=256 {
        pending.push(seal(&ports, ordinal));
    }
    assert_visible(&ports, 256, false);
    let successor = seal(&ports, 257);
    assert_visible(&ports, 256, true);
    assert_visible(&ports, 257, false);
    for (offset, fence) in pending.into_iter().enumerate() {
        let committed = fence.wait().expect("original ticket remains valid");
        assert_eq!(
            committed[0].batch().outcomes()[0].commit_sequence().get(),
            u64::try_from(offset).expect("bounded offset") + 1,
        );
    }
    successor
        .wait()
        .expect("publish successor after its prefix");
    assert_visible(&ports, 257, true);
    drop(ports);
    for _ in 0..2 {
        let reopened = open_operational(RedbStore::open(&path.0).expect("capacity restart"));
        for ordinal in 1..=257 {
            assert_visible(&reopened, ordinal, true);
        }
        assert_visible(&reopened, 258, false);
    }
}

#[test]
fn failed_checkpoint_refuses_new_apply_while_a_durable_predecessor_awaits_publication() {
    let path = TestDatabasePath::new("checkpoint-pipeline-failure");
    prepare_command_database(&path.0);
    let (controller, schedule) =
        RedbTestController::return_before_commit(RedbTestOperation::JournalCheckpoint)
            .with_checkpoint_schedule();
    let ports = open_operational(
        RedbStore::open_with_test_controller(&path.0, controller).expect("controlled open"),
    );
    assert!(command_audit_phases(&ports).is_empty());
    seal(&ports, 1).wait().expect("publish checkpoint source");
    schedule.arm(Point::BeforeMaterialization);
    schedule.arm(Point::Materialized);
    schedule.request_checkpoint();
    let second = seal(&ports, 2);
    schedule.wait(Point::BeforeMaterialization);
    schedule.release(Point::BeforeMaterialization);
    schedule.wait(Point::Materialized);
    // The failed checkpoint must fence new authority immediately, even though
    // there is still a submitted predecessor for the completion owner to finish.
    let refused = ports.begin_deferred_command_epoch();
    schedule.release(Point::Materialized);
    assert!(
        matches!(refused, Err(ref error) if error.kind() == riffdb_storage_api::StorageErrorKind::Unavailable)
    );
    assert!(ports.begin_deferred_command_epoch().is_err());
    second
        .wait()
        .expect("known durable predecessor can still publish");
    assert_visible(&ports, 2, true);
    drop(ports);
    drop(schedule);
    for _ in 0..2 {
        let reopened =
            open_operational(RedbStore::open(&path.0).expect("failed-checkpoint recovery"));
        assert_visible(&reopened, 1, true);
        assert_visible(&reopened, 2, true);
        assert_visible(&reopened, 3, false);
    }
}

const CRASH_EDGE: &str = "RIFFDB_WP810_CRASH_EDGE";
const CRASH_PATH: &str = "RIFFDB_WP810_CRASH_PATH";

#[test]
fn checkpoint_pipeline_crash_child() {
    let Ok(edge) = std::env::var(CRASH_EDGE) else {
        return;
    };
    let path = PathBuf::from(std::env::var_os(CRASH_PATH).expect("child database path"));
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports = open_operational(
        RedbStore::open_with_test_controller(&path, controller).expect("child controlled open"),
    );
    assert!(command_audit_phases(&ports).is_empty());
    seal(&ports, 1)
        .wait()
        .expect("acknowledge checkpoint source");
    schedule.arm(Point::BeforeMaterialization);
    schedule.request_checkpoint();
    let point = match edge.as_str() {
        "before-sync" => Point::BeforeJournalSync,
        "durable-unpublished" => Point::BeforePublication,
        "checkpoint-staged" => Point::BeforeCheckpointCommit,
        "checkpoint-durable" => Point::Materialized,
        "installed-before-reclaim" => Point::BeforeReclaim,
        "reclaimed" => Point::Reclaimed,
        _ => panic!("unknown fixed crash edge"),
    };
    schedule.arm(point);
    let second = seal(&ports, 2);
    schedule.wait(Point::BeforeMaterialization);
    match edge.as_str() {
        "before-sync" => schedule.wait(point),
        "durable-unpublished" => {
            std::thread::spawn(move || second.wait().expect("publication"));
            schedule.wait(point);
        }
        "checkpoint-staged" | "checkpoint-durable" => {
            second
                .wait()
                .expect("acknowledge newer suffix before checkpoint commit");
            assert_visible(&ports, 2, true);
            schedule.release(Point::BeforeMaterialization);
            schedule.wait(point);
        }
        "installed-before-reclaim" | "reclaimed" => {
            second
                .wait()
                .expect("acknowledge newer suffix before installation");
            assert_visible(&ports, 2, true);
            schedule.arm(Point::Materialized);
            schedule.release(Point::BeforeMaterialization);
            schedule.wait(Point::Materialized);
            schedule.release(Point::Materialized);
            std::thread::spawn(move || {
                drop(
                    ports
                        .begin_deferred_command_epoch()
                        .expect("install completed checkpoint"),
                );
            });
            schedule.wait(point);
        }
        _ => unreachable!("closed crash schedule"),
    }
    // Abrupt process exit, without Rust destructors or graceful storage close.
    // The held worker is inside real I/O/publication, not a fabricated receipt.
    std::process::exit(73);
}

#[test]
fn checkpoint_pipeline_process_crashes_recover_exactly_and_idempotently() {
    for edge in [
        "before-sync",
        "durable-unpublished",
        "checkpoint-staged",
        "checkpoint-durable",
        "installed-before-reclaim",
        "reclaimed",
    ] {
        let path = TestDatabasePath::new("checkpoint-pipeline-crash");
        prepare_command_database(&path.0);
        let output = Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "checkpoint_pipeline::checkpoint_pipeline_crash_child",
                "--nocapture",
            ])
            .env(CRASH_EDGE, edge)
            .env(CRASH_PATH, &path.0)
            .output()
            .expect("run actual process crash");
        assert_eq!(
            output.status.code(),
            Some(73),
            "{edge}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut recovered_second = None;
        for _ in 0..2 {
            let reopened = open_operational(
                RedbStore::open(&path.0)
                    .unwrap_or_else(|error| panic!("crash recovery at {edge}: {error:?}")),
            );
            assert_visible(&reopened, 1, true);
            let present = reopened
                .read_commit(CommitSequence::new(2).expect("sequence"))
                .expect("recovered successor")
                .is_some();
            if edge != "before-sync" {
                assert!(
                    present,
                    "the successor's real journal fence completed at {edge}"
                );
            }
            if let Some(previous) = recovered_second {
                assert_eq!(
                    present, previous,
                    "restart cannot invent or discard authority"
                );
            }
            recovered_second = Some(present);
            assert_visible(&reopened, 2, present);
            assert_visible(&reopened, 3, false);
            assert_eq!(
                command_audit_phases(&reopened).len(),
                if present { 4 } else { 2 }
            );
        }
    }
}
