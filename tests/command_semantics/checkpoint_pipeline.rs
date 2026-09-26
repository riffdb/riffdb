//! Exercise the production coordinator, not only the storage drain predicate.
// req: STO-001, TXN-042

use super::*;
use riffdb_storage_redb::RedbCheckpointTestPoint as Point;
use std::future::Future;

#[test]
fn writer_pipelines_during_checkpoint_without_releasing_results_or_effects_early() {
    let database = UniqueUserDatabase::create("checkpoint-writer-schedule");
    let (controller, schedule) = RedbTestController::checkpoint_schedule();
    let ports = database.open_with_controller(controller);
    let mut commands = (0_u8..4)
        .map(|offset| {
            database.prepare_audited_organization_for(
                &ports,
                [0xb0 + offset; 16],
                &format!("checkpoint-{offset}"),
                0xb0 + offset,
                0xc0 + offset,
            )
        })
        .collect::<Vec<_>>()
        .into_iter();
    let notifications = Arc::new(RecordingApplicationCommitNotifications::default());
    let coordinator = start_group_coordinator_with_notifications(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(0xd0)),
        notifications.clone(),
    );
    let executor = coordinator.command_executor();
    // Drop pauses before the coordinator on assertion failure so its shutdown
    // can drain the actual workers rather than deadlocking in the fixture.
    let pauses = schedule;
    let rt = runtime();
    let first = rt
        .block_on(executor.reserve_capacity())
        .expect("first capacity")
        .submit(commands.next().expect("first command"))
        .expect("first submit");
    assert!(matches!(
        rt.block_on(first.completion()).expect("first response"),
        CommandExecutionResult::Committed(_)
    ));

    pauses.arm(Point::BeforeMaterialization);
    pauses.arm(Point::BeforeJournalSync);
    pauses.arm(Point::BeforePublication);
    pauses.arm(Point::Materialized);
    pauses.arm(Point::AfterCommandSeal);
    pauses.request_checkpoint();
    let second = rt
        .block_on(executor.reserve_capacity())
        .expect("second capacity")
        .submit(commands.next().expect("second command"))
        .expect("second submit");
    pauses.wait(Point::BeforeMaterialization);
    pauses.wait(Point::AfterCommandSeal);
    pauses.arm(Point::BeforeCommandApply);
    pauses.release(Point::AfterCommandSeal);
    pauses.wait(Point::BeforeJournalSync);
    let mut second = Box::pin(second.completion());
    let mut context = Context::from_waker(Waker::noop());
    assert!(second.as_mut().poll(&mut context).is_pending());
    assert_eq!(notifications.sequences(), vec![CommitSequence::first()]);
    // The predecessor's real journal sync is paused. The writer must reach
    // the next apply instead of waiting in POST_DRAIN_ALL for its receipt.
    let third = rt
        .block_on(executor.reserve_capacity())
        .expect("third capacity")
        .submit(commands.next().expect("third command"))
        .expect("third submit");
    pauses.wait(Point::BeforeCommandApply);
    pauses.arm(Point::AfterCommandSeal);
    pauses.release(Point::BeforeCommandApply);
    pauses.wait(Point::AfterCommandSeal);
    let mut third = Box::pin(third.completion());
    assert!(third.as_mut().poll(&mut context).is_pending());
    assert!(second.as_mut().poll(&mut context).is_pending());
    assert_eq!(notifications.sequences(), vec![CommitSequence::first()]);
    pauses.release(Point::AfterCommandSeal);
    pauses.release(Point::BeforeJournalSync);
    pauses.wait(Point::BeforePublication);
    assert!(second.as_mut().poll(&mut context).is_pending());
    assert!(third.as_mut().poll(&mut context).is_pending());
    assert_eq!(notifications.sequences(), vec![CommitSequence::first()]);
    pauses.release(Point::BeforePublication);
    assert!(matches!(
        rt.block_on(second).expect("second response"),
        CommandExecutionResult::Committed(_)
    ));
    assert!(matches!(
        rt.block_on(third).expect("third response"),
        CommandExecutionResult::Committed(_)
    ));
    pauses.release(Point::BeforeMaterialization);
    pauses.wait(Point::Materialized);
    pauses.release(Point::Materialized);
    let fourth = rt
        .block_on(executor.reserve_capacity())
        .expect("fourth capacity")
        .submit(commands.next().expect("fourth command"))
        .expect("fourth submit");
    assert!(matches!(
        rt.block_on(fourth.completion())
            .expect("post-installation response"),
        CommandExecutionResult::Committed(_)
    ));
    assert_eq!(
        notifications.sequences(),
        (1..=4)
            .map(|seq| CommitSequence::new(seq).expect("sequence"))
            .collect::<Vec<_>>()
    );
    drop(executor);
    coordinator.shutdown().expect("ordered shutdown");
}
