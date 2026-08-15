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
    FixedAdmissionClock, FrameworkProfileDatabase, IncrementingProvenanceSource,
    PanickingApplicationCommitNotifications, RecordingApplicationCommitNotifications,
    UniqueUserDatabase, command_timestamp, runtime, start_coordinator_with_notifications,
    start_group_coordinator_with_commit_telemetry, start_group_coordinator_with_notifications,
};

#[test]
fn audited_standard_singleton_uses_one_unpublished_root_and_one_journal_tail() {
    let database = UniqueUserDatabase::create("audited-deferred-singleton");
    let controller = RedbTestController::observe_index_migration();
    let ports = database.open_with_controller(controller.clone());
    let command =
        database.prepare_audited_organization_for(&ports, [0x86; 16], "audit-only", 0x86, 0x96);
    let admission_clock = Arc::new(FixedAdmissionClock::new(command_timestamp()));
    let provenance_source = Arc::new(IncrementingProvenanceSource::new(0xa6));
    let notifications = Arc::new(RecordingApplicationCommitNotifications::default());
    let coordinator = start_group_coordinator_with_notifications(
        ports,
        admission_clock,
        provenance_source,
        notifications.clone(),
    );
    let executor = coordinator.command_executor();

    let result = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve audited singleton")
            .submit(command)
            .expect("submit audited singleton")
            .completion()
            .await
            .expect("audited singleton completion")
    });
    assert!(matches!(result, CommandExecutionResult::Committed(_)));
    assert_eq!(notifications.sequences(), vec![CommitSequence::first()]);

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
            RedbTestOperation::DeferredCommandBatch,
            RedbTestOperation::CommandEpochTail,
        ],
        "a Standard audited singleton never opens the direct redb command path"
    );
}

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
            RedbTestOperation::DeferredCommandBatch,
            RedbTestOperation::CommandEpochTail,
            RedbTestOperation::DeferredCommandBatch,
            RedbTestOperation::CommandEpochTail,
        ],
        "each audited group uses one private root plus one durable tail"
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
fn create_before_delete_refuses_then_delete_releases_unique_value_for_reinsert() {
    let database = UniqueUserDatabase::create("create-delete-reinsert");
    let ports = database.open();
    let organization = database.prepare_organization(&ports);
    let first_user = [0x45; 16];
    let second_user = [0x46; 16];
    let third_user = [0x47; 16];
    let first = database.prepare_audited(
        &ports,
        first_user,
        "owner@example.test",
        "create-first-owner",
        0x66,
        0x58,
    );
    // A fresh identity (new user, caller key, and request) re-inserting the
    // committed unique value sequentially: not a replay, a new admission.
    let sequential_reinsert = database.prepare(
        &ports,
        second_user,
        "owner@example.test",
        "create-second-owner",
        0x67,
        0x59,
    );
    let delete_first =
        database.prepare_delete(&ports, first_user, "delete-first-owner", 0x68, 0x5a);
    let reinsert_after_release = database.prepare_audited(
        &ports,
        third_user,
        "owner@example.test",
        "create-third-owner",
        0x69,
        0x5b,
    );
    let notifications = Arc::new(RecordingApplicationCommitNotifications::default());
    let coordinator = start_coordinator_with_notifications(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(0x79)),
        notifications.clone(),
    );
    let executor = coordinator.command_executor();

    let (refused, deleted, reinserted) = runtime().block_on(async {
        for preparation in [organization, first] {
            let seeded = executor
                .reserve_capacity()
                .await
                .expect("reserve unique seed")
                .submit(preparation)
                .expect("submit unique seed")
                .completion()
                .await
                .expect("complete unique seed");
            assert!(matches!(seeded, CommandExecutionResult::Committed(_)));
        }
        let refused = executor
            .reserve_capacity()
            .await
            .expect("reserve sequential reinsert")
            .submit(sequential_reinsert)
            .expect("submit sequential reinsert")
            .completion()
            .await
            .expect("complete sequential reinsert");
        let deleted = executor
            .reserve_capacity()
            .await
            .expect("reserve owner delete")
            .submit(delete_first)
            .expect("submit owner delete")
            .completion()
            .await
            .expect("complete owner delete");
        let reinserted = executor
            .reserve_capacity()
            .await
            .expect("reserve post-release reinsert")
            .submit(reinsert_after_release)
            .expect("submit post-release reinsert")
            .completion()
            .await
            .expect("complete post-release reinsert");
        (refused, deleted, reinserted)
    });

    // Refusal side: the sequential re-insert of an owned unique value is the
    // declared typed conflict, not a duplicate row and not a replay.
    let CommandExecutionResult::ExecutionFailed(refused) = refused else {
        panic!("sequential reinsert of an owned unique value must fail deterministically");
    };
    assert_eq!(refused.code(), ExecutionFailureCode::UniqueConflict);
    assert_eq!(
        refused.disposition(),
        riffdb_commit::CommittedOutcomeDisposition::FirstCommit
    );
    // Success side: deleting the owner removes the exact old unique entry and
    // frees it for a new admission in the next transaction.
    assert!(matches!(deleted, CommandExecutionResult::Committed(_)));
    assert!(matches!(reinserted, CommandExecutionResult::Committed(_)));
    assert_eq!(
        notifications.sequences(),
        vec![
            CommitSequence::first(),
            CommitSequence::new(2).expect("first-owner sequence"),
            CommitSequence::new(3).expect("delete sequence"),
            CommitSequence::new(4).expect("reinsert sequence"),
        ],
        "the refused reinsert must not allocate a commit sequence"
    );

    drop(executor);
    coordinator
        .shutdown()
        .expect("drain create-delete-reinsert coordinator");
    let ports = database.open();
    database.assert_user_exists(&ports, first_user, false);
    database.assert_user_exists(&ports, second_user, false);
    database.assert_user_email(&ports, third_user, "owner@example.test");
}

#[test]
fn delete_before_create_releases_unique_value_in_submission_order() {
    let database = UniqueUserDatabase::create("delete-create-order");
    let ports = database.open();
    let first_user = [0x48; 16];
    let second_user = [0x49; 16];
    let organization = database.prepare_organization(&ports);
    let first = database.prepare_audited(
        &ports,
        first_user,
        "ordered@example.test",
        "ordered-first-owner",
        0x6a,
        0x5c,
    );
    let delete_first =
        database.prepare_delete(&ports, first_user, "ordered-delete-owner", 0x6b, 0x5d);
    let create_second = database.prepare_audited(
        &ports,
        second_user,
        "ordered@example.test",
        "ordered-second-owner",
        0x6c,
        0x5e,
    );
    let notifications = Arc::new(RecordingApplicationCommitNotifications::default());
    let coordinator = start_coordinator_with_notifications(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(0x7a)),
        notifications.clone(),
    );
    let executor = coordinator.command_executor();

    let (deleted, created) = runtime().block_on(async {
        for preparation in [organization, first] {
            let seeded = executor
                .reserve_capacity()
                .await
                .expect("reserve ordered unique seed")
                .submit(preparation)
                .expect("submit ordered unique seed")
                .completion()
                .await
                .expect("complete ordered unique seed");
            assert!(matches!(seeded, CommandExecutionResult::Committed(_)));
        }
        let deleted = executor
            .reserve_capacity()
            .await
            .expect("reserve ordered delete")
            .submit(delete_first)
            .expect("submit ordered delete");
        let created = executor
            .reserve_capacity()
            .await
            .expect("reserve ordered create")
            .submit(create_second)
            .expect("submit ordered create");
        (
            deleted
                .completion()
                .await
                .expect("ordered delete completion"),
            created
                .completion()
                .await
                .expect("ordered create completion"),
        )
    });

    assert!(matches!(deleted, CommandExecutionResult::Committed(_)));
    assert!(matches!(created, CommandExecutionResult::Committed(_)));
    assert_eq!(
        notifications.sequences(),
        vec![
            CommitSequence::first(),
            CommitSequence::new(2).expect("second sequence"),
            CommitSequence::new(3).expect("third sequence"),
            CommitSequence::new(4).expect("fourth sequence"),
        ],
        "the same aggregate conflict preserves delete-before-create order"
    );

    drop(executor);
    coordinator.shutdown().expect("drain ordered coordinator");
    let ports = database.open();
    database.assert_user_exists(&ports, first_user, false);
    database.assert_user_email(&ports, second_user, "ordered@example.test");
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

fn race_session_transitions(
    label: &str,
    submit_refresh_first: bool,
) -> (
    FrameworkProfileDatabase,
    CommandExecutionResult,
    CommandExecutionResult,
    CommandExecutionResult,
    Vec<CommitSequence>,
) {
    let database = FrameworkProfileDatabase::create(label);
    let ports = database.open();
    let organization = [0x21; 16];
    let user = [0x22; 16];
    let account = [0x23; 16];
    let session = [0x24; 16];
    let signup = database.prepare_signup(
        &ports,
        organization,
        user,
        account,
        session,
        "digest-initial",
        0xa1,
        0x6a,
        0x5c,
    );
    let refresh = database.prepare_refresh(
        &ports,
        organization,
        user,
        session,
        1,
        "digest-successor",
        0xa2,
        0x6b,
        0x5d,
    );
    let revoke = database.prepare_revoke(&ports, organization, user, session, 1, 0xa3, 0x6c, 0x5e);
    // The loser's declared refusal must be a durable terminal outcome: the
    // same caller identity replayed later returns the stored outcome.
    let loser_replay = if submit_refresh_first {
        database.prepare_revoke(&ports, organization, user, session, 1, 0xa3, 0x6c, 0x5f)
    } else {
        database.prepare_refresh(
            &ports,
            organization,
            user,
            session,
            1,
            "digest-successor",
            0xa2,
            0x6b,
            0x5f,
        )
    };
    let notifications = Arc::new(RecordingApplicationCommitNotifications::default());
    let coordinator = start_coordinator_with_notifications(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(0x7b)),
        notifications.clone(),
    );
    let executor = coordinator.command_executor();

    let (winner, loser, replay) = runtime().block_on(async {
        let seeded = executor
            .reserve_capacity()
            .await
            .expect("reserve signup graph")
            .submit(signup)
            .expect("submit signup graph")
            .completion()
            .await
            .expect("complete signup graph");
        assert!(matches!(seeded, CommandExecutionResult::Committed(_)));

        // Deterministic schedule: both admissions are reserved before either
        // submission, then submitted in a fixed order; the coordinator's
        // conflict machinery decides the loser, not test timing.
        let first_permit = executor
            .reserve_capacity()
            .await
            .expect("reserve first transition");
        let second_permit = executor
            .reserve_capacity()
            .await
            .expect("reserve concurrent transition");
        let (winner_receipt, loser_receipt) = if submit_refresh_first {
            (
                first_permit.submit(refresh).expect("submit refresh"),
                second_permit.submit(revoke).expect("submit revoke"),
            )
        } else {
            (
                first_permit.submit(revoke).expect("submit revoke"),
                second_permit.submit(refresh).expect("submit refresh"),
            )
        };
        let winner = winner_receipt
            .completion()
            .await
            .expect("complete winning transition");
        let loser = loser_receipt
            .completion()
            .await
            .expect("complete losing transition");
        let replay = executor
            .reserve_capacity()
            .await
            .expect("reserve loser replay")
            .submit(loser_replay)
            .expect("submit loser replay")
            .completion()
            .await
            .expect("complete loser replay");
        (winner, loser, replay)
    });

    drop(executor);
    coordinator
        .shutdown()
        .expect("drain session race coordinator");
    (database, winner, loser, replay, notifications.sequences())
}

fn assert_transition_outcome(
    result: &CommandExecutionResult,
    expected: riffdb_types::OutcomeId,
    expected_disposition: CommittedOutcomeDisposition,
    role: &str,
) {
    let CommandExecutionResult::Committed(outcome) = result else {
        panic!("{role} transition must resolve to a declared terminal outcome");
    };
    assert_eq!(
        outcome.stored_outcome().declared_outcome().outcome_id(),
        expected,
        "{role} outcome identity"
    );
    assert_eq!(
        outcome.disposition(),
        expected_disposition,
        "{role} disposition"
    );
}

// WP-598 escalation, discharged by WP-606: these two schedules are the
// acceptance evidence for the commit-path version audit. The V9 grammar/IR
// pair is admitted by the audited derivation whitelist
// (crates/riffdb-commit/src/command_index.rs, WP-606 audit notes at
// `index_derivation_version_supported`), and the profile harness commits
// through the audited capsule path consistently. Both schedules pass with
// their WP-598 assertions unchanged.
#[test]
fn framework_refresh_wins_the_race_and_revocation_observes_its_declared_stale_outcome() {
    let (database, winner, loser, replay, sequences) =
        race_session_transitions("refresh-first", true);

    // Winner side: the refresh committed its declared success outcome.
    assert_transition_outcome(
        &winner,
        database.outcome_id("RefreshSession", "SessionRefreshed"),
        CommittedOutcomeDisposition::FirstCommit,
        "winning refresh",
    );
    // Loser side: the revocation observed its declared stale refusal, and
    // that refusal is durable — the identical caller identity replays it.
    assert_transition_outcome(
        &loser,
        database.outcome_id("RevokeSession", "RevokeSessionStale"),
        CommittedOutcomeDisposition::FirstCommit,
        "losing revocation",
    );
    assert_transition_outcome(
        &replay,
        database.outcome_id("RevokeSession", "RevokeSessionStale"),
        CommittedOutcomeDisposition::Replay,
        "replayed losing revocation",
    );

    // Exactly one transition survived revision 1: the session advanced once,
    // kept the refreshed digest, and remained Active.
    let ports = database.open();
    database.assert_session(
        &ports,
        [0x21; 16],
        [0x22; 16],
        [0x24; 16],
        &database.session_state_value("Active"),
        "digest-successor",
        2,
    );
    assert_eq!(
        sequences.first(),
        Some(&CommitSequence::first()),
        "the signup graph owns the first commit sequence"
    );
}

#[test]
fn framework_revocation_wins_the_race_and_refresh_observes_its_declared_stale_outcome() {
    let (database, winner, loser, replay, sequences) =
        race_session_transitions("revoke-first", false);

    assert_transition_outcome(
        &winner,
        database.outcome_id("RevokeSession", "SessionRevoked"),
        CommittedOutcomeDisposition::FirstCommit,
        "winning revocation",
    );
    assert_transition_outcome(
        &loser,
        database.outcome_id("RefreshSession", "RefreshSessionStale"),
        CommittedOutcomeDisposition::FirstCommit,
        "losing refresh",
    );
    assert_transition_outcome(
        &replay,
        database.outcome_id("RefreshSession", "RefreshSessionStale"),
        CommittedOutcomeDisposition::Replay,
        "replayed losing refresh",
    );

    // The mirrored order: revocation survived, the refresh digest never
    // landed, and the session is Revoked at exactly revision 2.
    let ports = database.open();
    database.assert_session(
        &ports,
        [0x21; 16],
        [0x22; 16],
        [0x24; 16],
        &database.session_state_value("Revoked"),
        "digest-initial",
        2,
    );
    assert_eq!(
        sequences.first(),
        Some(&CommitSequence::first()),
        "the signup graph owns the first commit sequence"
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

// Retained live pin from WP-598: a declared refusal with zero mutations
// bypasses index derivation entirely and commits terminally on the V9
// bundle. Under WP-606's audited whitelist mutating commands also commit
// (the race schedules above); this pin keeps the zero-mutation shape
// independently anchored.
#[test]
fn framework_profile_declared_refusal_commits_terminally_on_the_v9_bundle() {
    let database = FrameworkProfileDatabase::create("v9-declared-refusal");
    let ports = database.open();
    let refresh = database.prepare_refresh(
        &ports,
        [0x21; 16],
        [0x22; 16],
        [0x24; 16],
        1,
        "digest-successor",
        0xa2,
        0x6b,
        0x5d,
    );
    let coordinator = start_coordinator_with_notifications(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(0x7b)),
        Arc::new(RecordingApplicationCommitNotifications::default()),
    );
    let executor = coordinator.command_executor();
    let result = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve refresh")
            .submit(refresh)
            .expect("submit refresh")
            .completion()
            .await
            .expect("complete refresh")
    });
    let CommandExecutionResult::Committed(outcome) = &result else {
        panic!("refresh of missing session must produce a declared outcome");
    };
    assert_eq!(
        outcome.stored_outcome().declared_outcome().outcome_id(),
        database.outcome_id("RefreshSession", "RefreshSessionMissing")
    );
    assert_eq!(
        outcome.disposition(),
        CommittedOutcomeDisposition::FirstCommit,
        "the declared refusal must persist as a first terminal outcome"
    );
    drop(executor);
    coordinator
        .shutdown()
        .expect("drain declared-refusal coordinator");
}
