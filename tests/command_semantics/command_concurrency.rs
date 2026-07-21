#![forbid(unsafe_code)]

//! Real-redb command serialization and idempotency evidence.

mod support;

use std::sync::Arc;

use riffdb_commit::{CommandExecutionResult, CommittedOutcomeDisposition};

use support::{
    BudgetDatabase, CountingProvenanceSource, FixedAdmissionClock, command_timestamp, runtime,
    start_coordinator,
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
    let coordinator = start_coordinator(
        ports,
        Arc::clone(&admission_clock),
        Arc::clone(&provenance_source),
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
    assert_eq!(admission_clock.calls(), 1);
    assert_eq!(provenance_source.calls(), 1);

    let durable = left.into_stored_outcome();
    drop(executor);
    coordinator.shutdown().expect("drain command coordinator");

    let ports = database.open();
    database.assert_one_budget_commit(&ports, &durable, 12_500);
}
