#![forbid(unsafe_code)]

//! Real-redb command serialization and idempotency evidence.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use riffdb_commit::{
    ApplicationCommitNotificationSink, CommandExecutionAdmissionError, CommandExecutionResult,
    CommitCallTerminal, CommitTelemetry, CommitTelemetryEvent, CommittedOutcomeDisposition,
    CoordinatorLifecycleState,
};
use riffdb_storage_api::AuthoritativePointReader;
use riffdb_storage_api::{ApplicationCommandTransactionPort, EmptyCommandBatch};
use riffdb_storage_redb::{RedbTestController, RedbTestOperation, RedbTestPhase};
use riffdb_types::{CommitSequence, ExecutionFailureCode};

use support::{
    BudgetDatabase, CountingProvenanceSource, FailingApplicationCommitNotifications,
    FixedAdmissionClock, IncrementingProvenanceSource, PanickingApplicationCommitNotifications,
    RecordingApplicationCommitNotifications, UniqueUserDatabase, command_timestamp, runtime,
    start_coordinator_with_notifications, start_group_coordinator_with_commit_telemetry,
    start_group_coordinator_with_notifications,
};

#[test]
fn audited_standard_group_uses_one_unpublished_root_and_one_immediate_tail() {
    let database = UniqueUserDatabase::create("audited-deferred-group");
    let controller = RedbTestController::observe_index_migration();
    let ports = database.open_with_controller(controller.clone());
    let organization =
        database.prepare_organization_for(&ports, [0x83; 16], "audit-blocker", 0x83, 0x93);
    let first =
        database.prepare_audited_organization_for(&ports, [0x84; 16], "audit-first", 0x84, 0x94);
    let second =
        database.prepare_audited_organization_for(&ports, [0x85; 16], "audit-second", 0x85, 0x95);
    let blocker = ports.begin_empty_batch().expect("hold writer admission");
    let admission_clock = Arc::new(FixedAdmissionClock::new(command_timestamp()));
    let provenance_source = Arc::new(IncrementingProvenanceSource::new(0xa4));
    let notifications = Arc::new(RecordingApplicationCommitNotifications::default());
    let coordinator = start_group_coordinator_with_notifications(
        ports,
        Arc::clone(&admission_clock),
        provenance_source,
        notifications.clone(),
    );
    let executor = coordinator.command_executor();

    let (first, second) = runtime().block_on(async {
        let organization = executor
            .reserve_capacity()
            .await
            .expect("reserve organization")
            .submit(organization)
            .expect("submit organization");
        for _ in 0..10_000 {
            if admission_clock.calls() == 1 {
                break;
            }
            std::thread::yield_now();
        }
        assert_eq!(admission_clock.calls(), 1);

        let first = executor
            .reserve_capacity()
            .await
            .expect("reserve first audited command")
            .submit(first)
            .expect("submit first audited command");
        let second = executor
            .reserve_capacity()
            .await
            .expect("reserve second audited command")
            .submit(second)
            .expect("submit second audited command");
        blocker.rollback();

        assert!(matches!(
            organization
                .completion()
                .await
                .expect("organization completion"),
            CommandExecutionResult::Committed(_)
        ));
        (
            first.completion().await.expect("first audited completion"),
            second
                .completion()
                .await
                .expect("second audited completion"),
        )
    });

    let CommandExecutionResult::Committed(first) = first else {
        panic!("first audited command commits");
    };
    let CommandExecutionResult::Committed(second) = second else {
        panic!("second audited command commits");
    };
    assert_ne!(
        first.stored_outcome().identity(),
        second.stored_outcome().identity()
    );
    assert_eq!(
        notifications.sequences(),
        vec![
            CommitSequence::first(),
            CommitSequence::new(2).expect("second sequence"),
            CommitSequence::new(3).expect("third sequence"),
        ]
    );

    drop(executor);
    coordinator.shutdown().expect("drain grouped coordinator");
    let transitions = controller
        .events()
        .into_iter()
        .filter(|event| event.phase() == RedbTestPhase::BeforeEngineCommit)
        .filter(|event| event.operation() != RedbTestOperation::ValidatedPrefixCheckpoint)
        .map(|event| event.operation())
        .collect::<Vec<_>>();
    assert_eq!(
        transitions,
        vec![
            RedbTestOperation::CommandBatch,
            RedbTestOperation::DeferredCommandBatch,
            RedbTestOperation::CommandEpochTail,
        ],
        "the singleton stays direct and the audited non-singleton uses one private root plus tail"
    );
}

/// Counts CommitCallCompleted samples that feed durable-flush histograms.
struct FlushCountingTelemetry {
    commits: AtomicUsize,
}
impl CommitTelemetry for FlushCountingTelemetry {
    fn record(&self, event: CommitTelemetryEvent) {
        if matches!(
            event,
            CommitTelemetryEvent::CommitCallCompleted {
                terminal: CommitCallTerminal::Committed,
                ..
            }
        ) {
            self.commits.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[test]
fn hardened_audited_group_stays_direct_and_keeps_independent_results() {
    let database = UniqueUserDatabase::create("hardened-grouped-disjoint");
    let controller = RedbTestController::observe_index_migration();
    let ports = database.open_with_controller(controller.clone());
    let organization =
        database.prepare_organization_for(&ports, [0x80; 16], "group-blocker", 0x80, 0x90);
    let first =
        database.prepare_audited_organization_for(&ports, [0x81; 16], "group-first", 0x81, 0x91);
    let second =
        database.prepare_audited_organization_for(&ports, [0x82; 16], "group-second", 0x82, 0x92);
    let blocker = ports.begin_empty_batch().expect("hold writer admission");
    let admission_clock = Arc::new(FixedAdmissionClock::new(command_timestamp()));
    let provenance_source = Arc::new(IncrementingProvenanceSource::new(0xa1));
    let notifications = Arc::new(RecordingApplicationCommitNotifications::default());
    let coordinator = start_coordinator_with_notifications(
        ports,
        Arc::clone(&admission_clock),
        provenance_source,
        notifications.clone(),
    );
    let executor = coordinator.command_executor();

    let (first, second) = runtime().block_on(async {
        let organization = executor
            .reserve_capacity()
            .await
            .expect("reserve organization")
            .submit(organization)
            .expect("submit organization");
        for _ in 0..10_000 {
            if admission_clock.calls() == 1 {
                break;
            }
            std::thread::yield_now();
        }
        assert_eq!(
            admission_clock.calls(),
            1,
            "organization reached the explicit admission-clock hook"
        );

        let first = executor
            .reserve_capacity()
            .await
            .expect("reserve first grouped command")
            .submit(first)
            .expect("submit first grouped command");
        let second = executor
            .reserve_capacity()
            .await
            .expect("reserve second grouped command")
            .submit(second)
            .expect("submit second grouped command");
        blocker.rollback();

        assert!(matches!(
            organization
                .completion()
                .await
                .expect("organization completion"),
            CommandExecutionResult::Committed(_)
        ));
        (
            first.completion().await.expect("first grouped completion"),
            second
                .completion()
                .await
                .expect("second grouped completion"),
        )
    });

    let CommandExecutionResult::Committed(first) = first else {
        panic!("first disjoint command commits");
    };
    let CommandExecutionResult::Committed(second) = second else {
        panic!("second disjoint command commits");
    };
    assert_ne!(
        first.stored_outcome().identity(),
        second.stored_outcome().identity()
    );
    assert_ne!(
        first.stored_outcome().provenance_id(),
        second.stored_outcome().provenance_id()
    );
    assert_eq!(
        first.stored_outcome().durability_mode(),
        riffdb_storage_api::DurabilityMode::Sync
    );
    assert_eq!(
        second.stored_outcome().durability_mode(),
        riffdb_storage_api::DurabilityMode::Sync
    );
    assert_eq!(
        notifications.sequences(),
        vec![
            CommitSequence::first(),
            CommitSequence::new(2).expect("second sequence"),
            CommitSequence::new(3).expect("third sequence"),
        ]
    );

    drop(executor);
    coordinator.shutdown().expect("drain grouped coordinator");
    let transitions = controller
        .events()
        .into_iter()
        .filter(|event| event.phase() == RedbTestPhase::BeforeEngineCommit)
        // Startup may write a validated-prefix checkpoint on open; ignore it.
        .filter(|event| event.operation() != RedbTestOperation::ValidatedPrefixCheckpoint)
        .map(|event| event.operation())
        .collect::<Vec<_>>();
    assert_eq!(
        transitions,
        vec![
            RedbTestOperation::CommandBatch,
            RedbTestOperation::CommandBatch,
        ],
        "hardened commands persist their fused terminal transitions directly"
    );
}

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
    // Both concurrently observed vacancies receive a proposed logical time;
    // the grouped atomic admission retains the FIFO winner and rebinds the
    // equal-input follower to that exact durable Pending record.
    assert_eq!(admission_clock.calls(), 2);
    assert_eq!(provenance_source.calls(), 1);

    let durable = left.into_stored_outcome();
    drop(executor);
    coordinator.shutdown().expect("drain command coordinator");

    let ports = database.open();
    database.assert_one_budget_commit(&ports, &durable, 12_500);
}

#[test]
fn durable_flush_duration_is_recorded_under_group_durability() {
    // End-to-end (M8): one Group commit through a real store emits
    // CommitCallCompleted (source of the durable-flush histogram).
    let database = BudgetDatabase::create("group-flush-histogram");
    let ports = database.open();
    let preparation = database.prepare(&ports, 9_900, 0x56);
    let telemetry = Arc::new(FlushCountingTelemetry {
        commits: AtomicUsize::new(0),
    });
    let coordinator = start_group_coordinator_with_commit_telemetry(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(0xf1)),
        Arc::new(RecordingApplicationCommitNotifications::default()),
        Arc::clone(&telemetry) as Arc<dyn CommitTelemetry>,
    );
    let executor = coordinator.command_executor();
    let result = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve")
            .submit(preparation)
            .expect("submit")
            .completion()
            .await
            .expect("complete")
    });
    assert!(matches!(result, CommandExecutionResult::Committed(_)));
    coordinator.shutdown().expect("shutdown");
    assert!(
        telemetry.commits.load(Ordering::Relaxed) > 0,
        "Group commit must emit CommitCallCompleted (durable-flush source event)"
    );
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
    let CommandExecutionResult::ExecutionFailed(loser) = loser else {
        panic!("unique loser must be a deterministic failure");
    };
    let CommandExecutionResult::ExecutionFailed(replay) = replay else {
        panic!("unique loser replay must remain a deterministic failure");
    };
    assert_eq!(loser.code(), ExecutionFailureCode::UniqueConflict);
    assert_eq!(
        loser.disposition(),
        riffdb_commit::CommittedOutcomeDisposition::FirstCommit
    );
    assert_eq!(replay.code(), loser.code());
    assert_eq!(
        replay.disposition(),
        riffdb_commit::CommittedOutcomeDisposition::Replay
    );
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
    let CommandExecutionResult::ExecutionFailed(failure) = failure else {
        panic!("conflicting change must be a deterministic failure");
    };
    let CommandExecutionResult::ExecutionFailed(replayed) = replayed else {
        panic!("conflicting change replay must remain a deterministic failure");
    };
    assert_eq!(failure.code(), ExecutionFailureCode::UniqueConflict);
    assert_eq!(
        failure.disposition(),
        riffdb_commit::CommittedOutcomeDisposition::FirstCommit
    );
    assert_eq!(replayed.code(), failure.code());
    assert_eq!(
        replayed.disposition(),
        riffdb_commit::CommittedOutcomeDisposition::Replay
    );

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
