#![forbid(unsafe_code)]

//! Real-redb uncertain-response and reopen evidence.

mod support;

use std::sync::Arc;

use riffdb_commit::{
    CommandExecutionAdmissionError, CommandExecutionResult, CommittedOutcomeDisposition,
    CoordinatorLifecycleState,
};
use riffdb_storage_redb::{RedbTestController, RedbTestOperation, RedbTestPhase};

use support::{
    BudgetDatabase, CountingProvenanceSource, FixedAdmissionClock, UniqueUserDatabase,
    command_timestamp, runtime, start_coordinator,
};

#[test]
fn committed_unknown_response_replays_exactly_after_reopen() {
    let database = BudgetDatabase::create("lost-response");
    let controller =
        RedbTestController::return_unknown_after_commit(RedbTestOperation::CommandBatch);
    let ports = database.open_with_controller(controller.clone());
    let preparation = database.prepare(&ports, 12_500, 0x31);
    let admission_clock = Arc::new(FixedAdmissionClock::new(command_timestamp()));
    let provenance_source = Arc::new(CountingProvenanceSource::new(0x41));
    let coordinator = start_coordinator(
        ports,
        Arc::clone(&admission_clock),
        Arc::clone(&provenance_source),
    );
    let executor = coordinator.command_executor();

    let same_call = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve command with lost response")
            .submit(preparation)
            .expect("submit command with lost response")
            .completion()
            .await
            .expect("same-key lookup resolves committed command")
    });
    let CommandExecutionResult::Committed(same_call) = same_call else {
        panic!("same-call lookup must resolve the committed outcome");
    };
    assert_eq!(
        same_call.disposition(),
        CommittedOutcomeDisposition::FirstCommit
    );
    assert_eq!(
        executor.lifecycle_state(),
        CoordinatorLifecycleState::Fenced
    );
    let admission = runtime().block_on(executor.reserve_capacity());
    assert!(matches!(
        admission,
        Err(CommandExecutionAdmissionError::Fenced)
    ));
    assert_eq!(admission_clock.calls(), 1);
    assert_eq!(provenance_source.calls(), 1);
    assert!(controller.events().iter().any(|event| {
        event.operation() == RedbTestOperation::CommandBatch
            && event.phase() == RedbTestPhase::AfterEngineCommit
    }));

    let durable = same_call.into_stored_outcome();
    drop(executor);
    coordinator.shutdown().expect("join fenced coordinator");

    let ports = database.open();
    let replay_preparation = database.prepare(&ports, 12_500, 0x32);
    let replay_clock = Arc::new(FixedAdmissionClock::new(command_timestamp()));
    let replay_source = Arc::new(CountingProvenanceSource::new(0x42));
    let coordinator =
        start_coordinator(ports, Arc::clone(&replay_clock), Arc::clone(&replay_source));
    let executor = coordinator.command_executor();
    let replay = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve retry after healthy reopen")
            .submit(replay_preparation)
            .expect("submit retry after healthy reopen")
            .completion()
            .await
            .expect("replay committed command after healthy reopen")
    });
    let CommandExecutionResult::Committed(replay) = replay else {
        panic!("healthy reopen must replay the durable outcome");
    };
    assert_eq!(replay.disposition(), CommittedOutcomeDisposition::Replay);
    assert_eq!(replay.stored_outcome(), &durable);
    assert_eq!(replay_clock.calls(), 0);
    assert_eq!(replay_source.calls(), 0);

    drop(executor);
    coordinator
        .shutdown()
        .expect("drain reopened command coordinator");
    let ports = database.open();
    database.assert_one_budget_commit(&ports, &durable, 12_500);
}

#[test]
fn committed_unique_entity_and_index_reopen_as_one_reciprocal_state() {
    let database = UniqueUserDatabase::create("unique-lost-response");
    let ports = database.open();
    let organization = database.prepare_organization(&ports);
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(CountingProvenanceSource::new(0x45)),
    );
    let executor = coordinator.command_executor();
    let organization = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve organization")
            .submit(organization)
            .expect("submit organization")
            .completion()
            .await
            .expect("complete organization")
    });
    assert!(matches!(organization, CommandExecutionResult::Committed(_)));
    drop(executor);
    coordinator.shutdown().expect("drain organization setup");

    let controller =
        RedbTestController::return_unknown_after_commit(RedbTestOperation::CommandBatch);
    let ports = database.open_with_controller(controller);
    let user_id = [0x46; 16];
    let create = database.prepare(
        &ports,
        user_id,
        "atomic@example.test",
        "atomic-unique-create",
        0x66,
        0x58,
    );
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(CountingProvenanceSource::new(0x47)),
    );
    let executor = coordinator.command_executor();
    let committed = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve unique create")
            .submit(create)
            .expect("submit unique create")
            .completion()
            .await
            .expect("same-call recovery resolves unique create")
    });
    assert!(matches!(committed, CommandExecutionResult::Committed(_)));
    drop(executor);
    coordinator
        .shutdown()
        .expect("join fenced unique coordinator");

    let ports = database.open();
    database.assert_user_exists(&ports, user_id, true);
    let replay = database.prepare(
        &ports,
        user_id,
        "atomic@example.test",
        "atomic-unique-create",
        0x66,
        0x59,
    );
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(CountingProvenanceSource::new(0x48)),
    );
    let executor = coordinator.command_executor();
    let replayed = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve unique replay")
            .submit(replay)
            .expect("submit unique replay")
            .completion()
            .await
            .expect("complete unique replay")
    });
    let CommandExecutionResult::Committed(replayed) = replayed else {
        panic!("unique create must replay after reciprocal startup validation");
    };
    assert_eq!(replayed.disposition(), CommittedOutcomeDisposition::Replay);
    drop(executor);
    coordinator.shutdown().expect("drain unique replay");
}
