#![forbid(unsafe_code)]

//! Real-redb command serialization and idempotency evidence.

mod support;

use std::sync::Arc;

use riffdb_commit::{
    ApplicationCommitNotificationSink, CommandExecutionAdmissionError, CommandExecutionResult,
    CommittedOutcomeDisposition, CoordinatorLifecycleState,
};
use riffdb_storage_api::AuthoritativePointReader;
use riffdb_types::{CommitSequence, ExecutionFailureCode};

use support::{
    BudgetDatabase, CountingProvenanceSource, FailingApplicationCommitNotifications,
    FixedAdmissionClock, IncrementingProvenanceSource, PanickingApplicationCommitNotifications,
    RecordingApplicationCommitNotifications, UniqueUserDatabase, command_timestamp, runtime,
    start_coordinator_with_notifications,
};

#[test]
fn equal_key_commands_commit_once_and_replay_exactly() {
    let database = BudgetDatabase::create("concurrency");
    let ports = database.open();
    let first_preparation = database.prepare(&ports, 12_500, 0x11);
    let replay_preparation = database.prepare(&ports, 12_500, 0x12);
    let changed_preparation = database.prepare(&ports, 12_600, 0x13);
    let admission_clock = Arc::new(FixedAdmissionClock::new(command_timestamp()));
    let provenance_source = Arc::new(CountingProvenanceSource::new(0x21));
    let notifications = Arc::new(RecordingApplicationCommitNotifications::default());
    let coordinator = start_coordinator_with_notifications(
        ports,
        Arc::clone(&admission_clock),
        Arc::clone(&provenance_source),
        notifications.clone(),
    );
    let executor = coordinator.command_executor();

    let (left, right, changed) = runtime().block_on(async {
        let first_permit = executor
            .reserve_capacity()
            .await
            .expect("reserve first equal-key command");
        let replay_permit = executor
            .reserve_capacity()
            .await
            .expect("reserve concurrent equal-key command");
        let first_receipt = first_permit
            .submit(first_preparation)
            .expect("submit first equal-key command");
        let replay_receipt = replay_permit
            .submit(replay_preparation)
            .expect("submit concurrent equal-key command");

        let left = first_receipt
            .completion()
            .await
            .expect("first equal-key completion");
        let right = replay_receipt
            .completion()
            .await
            .expect("second equal-key completion");

        let changed = executor
            .reserve_capacity()
            .await
            .expect("reserve changed-input command")
            .submit(changed_preparation)
            .expect("submit changed-input command")
            .completion()
            .await
            .expect("changed-input completion");
        (left, right, changed)
    });

    let CommandExecutionResult::Committed(left) = left else {
        panic!("first equal-key submission must return a committed outcome");
    };
    let CommandExecutionResult::Committed(right) = right else {
        panic!("second equal-key submission must return a committed outcome");
    };
    assert_ne!(left.disposition(), right.disposition());
    assert!(
        [left.disposition(), right.disposition()]
            .contains(&CommittedOutcomeDisposition::FirstCommit)
    );
    assert!(
        [left.disposition(), right.disposition()].contains(&CommittedOutcomeDisposition::Replay)
    );
    assert_eq!(left.stored_outcome(), right.stored_outcome());
    assert_eq!(
        left.stored_outcome().commit_sequence(),
        riffdb_types::CommitSequence::first()
    );
    assert_eq!(
        left.stored_outcome().provenance_id(),
        right.stored_outcome().provenance_id()
    );
    assert_eq!(changed, CommandExecutionResult::InputMismatch);
    assert_eq!(notifications.sequences(), vec![CommitSequence::first()]);
    assert_eq!(admission_clock.calls(), 1);
    assert_eq!(provenance_source.calls(), 1);

    let durable = left.into_stored_outcome();
    drop(executor);
    coordinator.shutdown().expect("drain command coordinator");

    let ports = database.open();
    database.assert_one_budget_commit(&ports, &durable, 12_500);
}

#[test]
fn notification_failure_or_panic_preserves_commit_and_stops_admission() {
    assert_defective_notification_sink(
        "notification-error",
        0x31,
        Arc::new(FailingApplicationCommitNotifications),
    );
    assert_defective_notification_sink(
        "notification-panic",
        0x32,
        Arc::new(PanickingApplicationCommitNotifications),
    );
}

#[test]
fn equal_scoped_unique_values_commit_once_and_loser_replays_without_sequence() {
    let database = UniqueUserDatabase::create("same-email");
    let ports = database.open();
    let organization = database.prepare_organization(&ports);
    let first_user = [0x41; 16];
    let second_user = [0x42; 16];
    let first = database.prepare(
        &ports,
        first_user,
        "same@example.test",
        "create-first",
        0x61,
        0x51,
    );
    let second = database.prepare(
        &ports,
        second_user,
        "same@example.test",
        "create-second",
        0x62,
        0x52,
    );
    let second_replay = database.prepare(
        &ports,
        second_user,
        "same@example.test",
        "create-second",
        0x62,
        0x53,
    );
    let admission_clock = Arc::new(FixedAdmissionClock::new(command_timestamp()));
    let provenance_source = Arc::new(IncrementingProvenanceSource::new(0x71));
    let notifications = Arc::new(RecordingApplicationCommitNotifications::default());
    let coordinator = start_coordinator_with_notifications(
        ports,
        admission_clock,
        provenance_source,
        notifications.clone(),
    );
    let executor = coordinator.command_executor();

    let (winner, loser, replay) = runtime().block_on(async {
        let organization = executor
            .reserve_capacity()
            .await
            .expect("reserve organization")
            .submit(organization)
            .expect("submit organization")
            .completion()
            .await
            .expect("complete organization");
        assert!(matches!(organization, CommandExecutionResult::Committed(_)));
        let winner_permit = executor
            .reserve_capacity()
            .await
            .expect("reserve unique winner");
        let loser_permit = executor
            .reserve_capacity()
            .await
            .expect("reserve concurrent unique loser");
        let winner_receipt = winner_permit.submit(first).expect("submit unique winner");
        let loser_receipt = loser_permit
            .submit(second)
            .expect("submit concurrent unique loser");
        let winner = winner_receipt
            .completion()
            .await
            .expect("complete unique winner");
        let loser = loser_receipt
            .completion()
            .await
            .expect("complete unique loser");
        let replay = executor
            .reserve_capacity()
            .await
            .expect("reserve unique loser replay")
            .submit(second_replay)
            .expect("submit unique loser replay")
            .completion()
            .await
            .expect("complete unique loser replay");
        (winner, loser, replay)
    });

    assert!(matches!(winner, CommandExecutionResult::Committed(_)));
    assert_eq!(
        loser,
        CommandExecutionResult::ExecutionFailed(ExecutionFailureCode::UniqueConflict)
    );
    assert_eq!(replay, loser);
    assert_eq!(
        notifications.sequences(),
        vec![
            CommitSequence::first(),
            CommitSequence::new(2).expect("second sequence")
        ]
    );

    drop(executor);
    coordinator.shutdown().expect("drain unique coordinator");
    let ports = database.open();
    database.assert_user_exists(&ports, first_user, true);
    database.assert_user_exists(&ports, second_user, false);
    assert!(
        ports
            .read_commit(CommitSequence::new(3).expect("third sequence"))
            .expect("read second sequence")
            .is_none(),
        "unique collision and replay must not allocate a commit sequence"
    );
}

#[test]
fn changing_to_an_owned_unique_value_preserves_the_original_entity_and_replays() {
    let database = UniqueUserDatabase::create("change-email");
    let ports = database.open();
    let first_user = [0x43; 16];
    let second_user = [0x44; 16];
    let preparations = [
        database.prepare_organization(&ports),
        database.prepare(
            &ports,
            first_user,
            "first@example.test",
            "create-first-change-test",
            0x63,
            0x54,
        ),
        database.prepare(
            &ports,
            second_user,
            "second@example.test",
            "create-second-change-test",
            0x64,
            0x55,
        ),
    ];
    let change = database.prepare_email_change(
        &ports,
        second_user,
        "first@example.test",
        "change-second-to-first",
        0x65,
        0x56,
    );
    let replay = database.prepare_email_change(
        &ports,
        second_user,
        "first@example.test",
        "change-second-to-first",
        0x65,
        0x57,
    );
    let coordinator = start_coordinator_with_notifications(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(0x75)),
        Arc::new(RecordingApplicationCommitNotifications::default()),
    );
    let executor = coordinator.command_executor();
    let (failure, replayed) = runtime().block_on(async {
        for preparation in preparations {
            let result = executor
                .reserve_capacity()
                .await
                .expect("reserve uniqueness setup")
                .submit(preparation)
                .expect("submit uniqueness setup")
                .completion()
                .await
                .expect("complete uniqueness setup");
            assert!(matches!(result, CommandExecutionResult::Committed(_)));
        }
        let failure = executor
            .reserve_capacity()
            .await
            .expect("reserve conflicting change")
            .submit(change)
            .expect("submit conflicting change")
            .completion()
            .await
            .expect("complete conflicting change");
        let replayed = executor
            .reserve_capacity()
            .await
            .expect("reserve conflicting change replay")
            .submit(replay)
            .expect("submit conflicting change replay")
            .completion()
            .await
            .expect("complete conflicting change replay");
        (failure, replayed)
    });
    assert_eq!(
        failure,
        CommandExecutionResult::ExecutionFailed(ExecutionFailureCode::UniqueConflict)
    );
    assert_eq!(replayed, failure);

    drop(executor);
    coordinator.shutdown().expect("drain change coordinator");
    let ports = database.open();
    database.assert_user_email(&ports, first_user, "first@example.test");
    database.assert_user_email(&ports, second_user, "second@example.test");
    assert!(
        ports
            .read_commit(CommitSequence::new(4).expect("fourth sequence"))
            .expect("read fourth sequence")
            .is_none(),
        "failed change and replay must not allocate a sequence"
    );
}

fn assert_defective_notification_sink(
    label: &str,
    seed: u8,
    notifications: Arc<dyn ApplicationCommitNotificationSink>,
) {
    let database = BudgetDatabase::create(label);
    let ports = database.open();
    let preparation = database.prepare(&ports, 12_500, seed);
    let admission_clock = Arc::new(FixedAdmissionClock::new(command_timestamp()));
    let provenance_source = Arc::new(CountingProvenanceSource::new(seed.wrapping_add(0x20)));
    let coordinator = start_coordinator_with_notifications(
        ports,
        admission_clock,
        provenance_source,
        notifications,
    );
    let executor = coordinator.command_executor();

    let result = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve command before sink defect")
            .submit(preparation)
            .expect("submit command before sink defect")
            .completion()
            .await
            .expect("known durable result survives sink defect")
    });
    let CommandExecutionResult::Committed(committed) = result else {
        panic!("sink defect must not rewrite a known committed outcome");
    };
    assert_eq!(
        committed.disposition(),
        CommittedOutcomeDisposition::FirstCommit
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Stopped
    );
    assert!(matches!(
        runtime().block_on(executor.reserve_capacity()),
        Err(CommandExecutionAdmissionError::Stopped)
    ));

    let durable = committed.into_stored_outcome();
    drop(executor);
    coordinator
        .shutdown()
        .expect("join coordinator stopped by sink defect");
    let ports = database.open();
    database.assert_one_budget_commit(&ports, &durable, 12_500);
}
