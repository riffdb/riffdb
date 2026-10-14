// req: BLK-024, BLK-025, BLK-027, BLK-039, BLK-041, BLK-045, TXN-030, TXN-031
#![forbid(unsafe_code)]

//! WP-561 and WP-684 process-crash recovery, independent model, and remote
//! loopback evidence for atomic collection commands and correlated index work.

#[path = "bulk_correlated_index_support.rs"]
mod bulk_correlated_index_support;
#[path = "command_semantics/support.rs"]
mod support;

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use riffdb_commit::{
    CommandExecutionPreparation, CommandExecutionResult, CommittedOutcome,
    CommittedOutcomeDisposition,
};
use riffdb_storage_api::AuthoritativePointReader;
use riffdb_storage_redb::{RedbTestController, RedbTestOperation};
use riffdb_types::{CommitSequence, EntityVersion};

use support::{
    BulkRowsDatabase, CountingProvenanceSource, FixedAdmissionClock, IncrementingProvenanceSource,
    command_timestamp, runtime, start_coordinator,
};

const CHILD_MODE: &str = "RIFFDB_BULK_RECOVERY_CHILD_MODE";
const CHILD_DATABASE: &str = "RIFFDB_BULK_RECOVERY_DATABASE";
const ROW_IDS: [[u8; 16]; 4] = [[0x61; 16], [0x62; 16], [0x63; 16], [0x64; 16]];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Put,
    InitializedPut,
    DecisionApply,
    DecisionNoEffect,
    Delete,
    Consume,
}

impl Operation {
    const fn label(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::InitializedPut => "initialized-put",
            Self::DecisionApply => "decision-apply",
            Self::DecisionNoEffect => "decision-no-effect",
            Self::Delete => "delete",
            Self::Consume => "consume",
        }
    }

    const fn input_seed(self) -> u8 {
        match self {
            Self::Put => 0xa1,
            Self::InitializedPut => 0xa4,
            Self::DecisionApply => 0xa5,
            Self::DecisionNoEffect => 0xa6,
            Self::Delete => 0xa2,
            Self::Consume => 0xa3,
        }
    }

    const fn digest_seed(self) -> u8 {
        match self {
            Self::Put => 0xb1,
            Self::InitializedPut => 0xb4,
            Self::DecisionApply => 0xb5,
            Self::DecisionNoEffect => 0xb6,
            Self::Delete => 0xb2,
            Self::Consume => 0xb3,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CrashPhase {
    BeforeCommit,
    AfterCommit,
}

impl CrashPhase {
    const fn label(self) -> &'static str {
        match self {
            Self::BeforeCommit => "before",
            Self::AfterCommit => "after",
        }
    }

    fn controller(self) -> RedbTestController {
        match self {
            Self::BeforeCommit => {
                RedbTestController::abort_before_commit(RedbTestOperation::CommandBatch)
            }
            Self::AfterCommit => {
                RedbTestController::abort_after_commit(RedbTestOperation::CommandBatch)
            }
        }
    }

    const fn expected_disposition(self) -> CommittedOutcomeDisposition {
        match self {
            Self::BeforeCommit => CommittedOutcomeDisposition::FirstCommit,
            Self::AfterCommit => CommittedOutcomeDisposition::Replay,
        }
    }
}

#[test]
// req: BLK-069
fn collection_create_and_delete_are_complete_or_absent_across_process_crash() {
    for operation in [
        Operation::Delete,
        Operation::Put,
        Operation::InitializedPut,
        Operation::DecisionApply,
        Operation::DecisionNoEffect,
        Operation::Consume,
    ] {
        for phase in [CrashPhase::BeforeCommit, CrashPhase::AfterCommit] {
            run_crash_case(operation, phase);
        }
    }
}

#[test]
fn healthy_collection_delete_reopens_with_a_reciprocal_tombstone_graph() {
    let database = BulkRowsDatabase::create("healthy-delete-reopen");
    let put = execute_healthy(&database, Operation::Put, 0x91, 0xe1, 0xf1, 0x81);
    let deleted = execute_healthy(&database, Operation::Delete, 0x92, 0xe2, 0xf2, 0x82);

    let ports = database.open();
    database.assert_rows_present(&ports, &ROW_IDS, false);
    database.assert_commit_graph(&ports, put.stored_outcome(), ROW_IDS.len());
    database.assert_commit_graph(&ports, deleted.stored_outcome(), ROW_IDS.len());
}

#[test]
fn healthy_collection_delete_can_be_recreated_through_the_same_entity_chain() {
    let database = BulkRowsDatabase::create("healthy-delete-recreate");
    let put = execute_healthy(&database, Operation::Put, 0x93, 0xe3, 0xf3, 0x83);
    let deleted = execute_healthy(&database, Operation::Delete, 0x94, 0xe4, 0xf4, 0x84);
    let recreated = execute_healthy(&database, Operation::Put, 0x95, 0xe5, 0xf5, 0x85);

    let ports = database.open();
    database.assert_rows_present(&ports, &ROW_IDS, true);
    database.assert_commit_graph(&ports, put.stored_outcome(), ROW_IDS.len());
    database.assert_commit_graph(&ports, deleted.stored_outcome(), ROW_IDS.len());
    database.assert_commit_graph(&ports, recreated.stored_outcome(), ROW_IDS.len());
}

#[test]
fn initialized_collection_creates_then_revision_checked_replaces_on_redb() {
    let database = BulkRowsDatabase::create("initialized-create-replace");
    let created = execute_healthy(&database, Operation::InitializedPut, 0x96, 0xe6, 0xf6, 0x86);
    let ports = database.open();
    database.assert_row_versions(&ports, &ROW_IDS, EntityVersion::first());
    drop(ports);

    let replaced = execute_healthy(&database, Operation::InitializedPut, 0x97, 0xe7, 0xf7, 0x87);
    assert_eq!(
        created.stored_outcome().commit_sequence(),
        CommitSequence::first()
    );
    assert_eq!(
        replaced.stored_outcome().commit_sequence(),
        CommitSequence::new(2).expect("second sequence")
    );
    let ports = database.open();
    database.assert_row_versions(
        &ports,
        &ROW_IDS,
        EntityVersion::new(2).expect("second version"),
    );
    database.assert_commit_graph(&ports, replaced.stored_outcome(), ROW_IDS.len());
}

#[test]
fn cascade_maximum_plus_one_commits_only_the_declared_zero_mutation_outcome() {
    let database = BulkRowsDatabase::create("cascade-overflow");
    let _put = execute_healthy(&database, Operation::Put, 0x96, 0xe6, 0xf6, 0x86);
    let ports = database.open();
    let extra_child = [0x72; 16];
    let preparation =
        database.prepare_extra_child(&ports, ROW_IDS[0], extra_child, 0x97, 0xe7, 0x87);
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(CountingProvenanceSource::new(0xf7)),
    );
    let executor = coordinator.command_executor();
    let extra = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve extra child")
            .submit(preparation)
            .expect("submit extra child")
            .completion()
            .await
            .expect("commit extra child")
    });
    assert!(matches!(extra, CommandExecutionResult::Committed(_)));
    drop(executor);
    coordinator
        .shutdown()
        .expect("drain extra-child coordinator");

    let overflow = execute_healthy(&database, Operation::Delete, 0x98, 0xe8, 0xf8, 0x88);
    assert_eq!(
        overflow.stored_outcome().declared_outcome().outcome_id(),
        database.outcome_id("DeleteRows", "CascadeLimitExceeded")
    );
    let ports = database.open();
    database.assert_rows_present(&ports, &ROW_IDS, true);
    database.assert_child_present(&ports, ROW_IDS[0], extra_child, true);
    let commit = ports
        .read_commit(overflow.stored_outcome().commit_sequence())
        .expect("read overflow commit")
        .expect("overflow commit exists");
    assert!(commit.entity_references().is_empty());
}

#[test]
fn child_create_and_cascade_serialize_in_both_submission_orders() {
    for create_first in [false, true] {
        let label = if create_first {
            "child-create-before-cascade"
        } else {
            "cascade-before-child-create"
        };
        let database = BulkRowsDatabase::create(label);
        let row_id = ROW_IDS[0];
        let extra_child = [0x73; 16];
        let ports = database.open();
        let put = database.prepare_put(&ports, &[row_id], 0x99, 0xe9, 0x89);
        let delete = database.prepare_delete(&ports, &[row_id], 0x9a, 0xea, 0x8a);
        let create = database.prepare_extra_child(&ports, row_id, extra_child, 0x9b, 0xeb, 0x8b);
        let coordinator = start_coordinator(
            ports,
            Arc::new(FixedAdmissionClock::new(command_timestamp())),
            Arc::new(IncrementingProvenanceSource::new(0xf9)),
        );
        let executor = coordinator.command_executor();

        let (first, second) = runtime().block_on(async {
            let seeded = executor
                .reserve_capacity()
                .await
                .expect("reserve cascade race seed")
                .submit(put)
                .expect("submit cascade race seed")
                .completion()
                .await
                .expect("complete cascade race seed");
            assert!(matches!(seeded, CommandExecutionResult::Committed(_)));

            let (first, second) = if create_first {
                (create, delete)
            } else {
                (delete, create)
            };
            let first = executor
                .reserve_capacity()
                .await
                .expect("reserve first racing command")
                .submit(first)
                .expect("submit first racing command");
            let second = executor
                .reserve_capacity()
                .await
                .expect("reserve second racing command")
                .submit(second)
                .expect("submit second racing command");
            let first = first.completion().await;
            let second = second.completion().await;
            (
                first.unwrap_or_else(|error| {
                    panic!("first racing command failed while second was {second:?}: {error:?}")
                }),
                second.expect("complete second racing command"),
            )
        });

        let CommandExecutionResult::Committed(first) = first else {
            panic!("first racing command must select a declared outcome");
        };
        let CommandExecutionResult::Committed(second) = second else {
            panic!("second racing command must select a declared outcome");
        };
        let (create, delete) = if create_first {
            (first, second)
        } else {
            (second, first)
        };
        assert_eq!(
            create.stored_outcome().declared_outcome().outcome_id(),
            database.outcome_id(
                "PutExtraChild",
                if create_first {
                    "ExtraChildCreated"
                } else {
                    "Missing"
                },
            )
        );
        assert_eq!(
            delete.stored_outcome().declared_outcome().outcome_id(),
            database.outcome_id(
                "DeleteRows",
                if create_first {
                    "CascadeLimitExceeded"
                } else {
                    "Deleted"
                },
            )
        );

        drop(executor);
        coordinator
            .shutdown()
            .expect("drain cascade race coordinator");
        let ports = database.open();
        database.assert_rows_present(&ports, &[row_id], create_first);
        database.assert_child_present(&ports, row_id, extra_child, create_first);
    }
}

#[test]
fn bulk_command_recovery_child() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    let path = std::env::var_os(CHILD_DATABASE).expect("child database path");
    if let Some(rest) = mode.strip_prefix("correlated-") {
        run_correlated_crash_child(Path::new(&path), rest);
        return;
    }
    let (operation, phase) = parse_mode(&mode);
    let database = BulkRowsDatabase::attach(Path::new(&path));
    let ports = database.open_with_controller(phase.controller());
    let preparation = preparation(&database, &ports, operation, 0xc1);
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(CountingProvenanceSource::new(0xd1)),
    );
    let executor = coordinator.command_executor();
    let unexpected = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve crashing collection command")
            .submit(preparation)
            .expect("submit crashing collection command")
            .completion()
            .await
    });
    panic!("armed command-batch crash failpoint did not abort the child: {unexpected:?}");
}

fn run_crash_case(operation: Operation, phase: CrashPhase) {
    let label = format!("{}-{}", operation.label(), phase.label());
    let database = BulkRowsDatabase::create(&label);
    if matches!(
        operation,
        Operation::Delete | Operation::Consume | Operation::DecisionNoEffect
    ) {
        let seed_operation = if operation == Operation::DecisionNoEffect {
            Operation::DecisionApply
        } else {
            Operation::Put
        };
        let outcome = execute_healthy(&database, seed_operation, 0x91, 0xe1, 0xf1, 0x81);
        // A retained checkpoint routes the reopen to the ADR-0182 bounded
        // dirty path, which never writes one; strip it so this open validates
        // completely and checkpoints the seed.
        riffdb_storage_redb::strip_validated_prefix_checkpoint_fixture(database.path())
            .expect("strip the pre-seed checkpoint");
        let ports = database.open();
        if operation == Operation::DecisionNoEffect {
            assert_decision_rows_present(&database, &ports, true);
        } else {
            database.assert_rows_present(&ports, &ROW_IDS, true);
        }
        database.assert_commit_graph(&ports, outcome.stored_outcome(), ROW_IDS.len());
        assert!(
            ports
                .write_validated_prefix_checkpoint()
                .expect("checkpoint collection seed before arming delete crash"),
            "clean collection seed must checkpoint before the delete crash arm"
        );
        drop(ports);
    }

    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("bulk_command_recovery_child")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(
            CHILD_MODE,
            format!("{}-{}", operation.label(), phase.label()),
        )
        .env(CHILD_DATABASE, database.path())
        .status()
        .expect("run crashing bulk-command child");
    assert!(!status.success(), "crash child must terminate abnormally");

    let ports = database.open();
    let durable_visibility = phase == CrashPhase::AfterCommit;
    let expected_present = match operation {
        Operation::Put | Operation::InitializedPut => durable_visibility,
        Operation::DecisionApply => durable_visibility,
        Operation::DecisionNoEffect => true,
        Operation::Delete => !durable_visibility,
        Operation::Consume => true,
    };
    if operation == Operation::Consume {
        database.assert_child_present(&ports, ROW_IDS[0], ROW_IDS[0], !durable_visibility);
    } else if matches!(
        operation,
        Operation::DecisionApply | Operation::DecisionNoEffect
    ) {
        assert_decision_rows_present(&database, &ports, expected_present);
    } else {
        database.assert_rows_present(&ports, &ROW_IDS, expected_present);
    }

    let replay_preparation = preparation(&database, &ports, operation, 0xc2);
    let provenance = Arc::new(CountingProvenanceSource::new(0xd2));
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::clone(&provenance),
    );
    let executor = coordinator.command_executor();
    let replay = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve collection retry")
            .submit(replay_preparation)
            .expect("submit collection retry")
            .completion()
            .await
            .expect("complete collection retry")
    });
    let CommandExecutionResult::Committed(replay) = replay else {
        panic!("collection retry must return its committed declared outcome");
    };
    assert_eq!(replay.disposition(), phase.expected_disposition());
    assert_eq!(
        provenance.calls(),
        usize::from(phase == CrashPhase::BeforeCommit),
        "replay must not allocate fresh provenance"
    );
    drop(executor);
    coordinator
        .shutdown()
        .expect("drain recovered collection coordinator");

    let ports = database.open();
    if operation == Operation::Consume {
        database.assert_child_present(&ports, ROW_IDS[0], ROW_IDS[0], false);
        database.assert_commit_graph(&ports, replay.stored_outcome(), 0);
    } else if matches!(
        operation,
        Operation::DecisionApply | Operation::DecisionNoEffect
    ) {
        assert_decision_rows_present(&database, &ports, true);
        database.assert_commit_graph(
            &ports,
            replay.stored_outcome(),
            usize::from(operation == Operation::DecisionApply) * ROW_IDS.len(),
        );
    } else {
        database.assert_rows_present(
            &ports,
            &ROW_IDS,
            matches!(operation, Operation::Put | Operation::InitializedPut),
        );
        database.assert_commit_graph(&ports, replay.stored_outcome(), ROW_IDS.len());
    }
    let expected_sequence = match operation {
        Operation::Put | Operation::InitializedPut | Operation::DecisionApply => {
            CommitSequence::first()
        }
        Operation::DecisionNoEffect => CommitSequence::new(2).expect("second sequence"),
        Operation::Delete => CommitSequence::new(2).expect("second sequence"),
        Operation::Consume => CommitSequence::new(2).expect("second sequence"),
    };
    assert_eq!(replay.stored_outcome().commit_sequence(), expected_sequence);
}

#[allow(clippy::too_many_arguments)]
fn execute_healthy(
    database: &BulkRowsDatabase,
    operation: Operation,
    input_seed: u8,
    digest_seed: u8,
    admission_seed: u8,
    provenance_seed: u8,
) -> CommittedOutcome {
    let ports = database.open();
    let preparation = match operation {
        Operation::Put => {
            database.prepare_put(&ports, &ROW_IDS, input_seed, digest_seed, admission_seed)
        }
        Operation::InitializedPut => database.prepare_initialized_put(
            &ports,
            &ROW_IDS,
            input_seed,
            digest_seed,
            admission_seed,
        ),
        Operation::DecisionApply | Operation::DecisionNoEffect => {
            database.prepare_decision_put(&ports, &ROW_IDS, input_seed, digest_seed, admission_seed)
        }
        Operation::Delete => {
            database.prepare_delete(&ports, &ROW_IDS, input_seed, digest_seed, admission_seed)
        }
        Operation::Consume => database.prepare_consume_child(
            &ports,
            ROW_IDS[0],
            ROW_IDS[0],
            input_seed,
            digest_seed,
            admission_seed,
        ),
    };
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(CountingProvenanceSource::new(provenance_seed)),
    );
    let executor = coordinator.command_executor();
    let result = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve healthy collection command")
            .submit(preparation)
            .expect("submit healthy collection command")
            .completion()
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "complete healthy {:?} collection command: {error:?}",
                    operation
                )
            })
    });
    drop(executor);
    coordinator
        .shutdown()
        .expect("drain healthy collection coordinator");
    let CommandExecutionResult::Committed(outcome) = result else {
        panic!("healthy collection command must commit");
    };
    outcome
}

fn preparation(
    database: &BulkRowsDatabase,
    ports: &riffdb_storage_redb::RedbOperationalPorts,
    operation: Operation,
    admission_seed: u8,
) -> CommandExecutionPreparation {
    match operation {
        Operation::Put => database.prepare_put(
            ports,
            &ROW_IDS,
            operation.input_seed(),
            operation.digest_seed(),
            admission_seed,
        ),
        Operation::InitializedPut => database.prepare_initialized_put(
            ports,
            &ROW_IDS,
            operation.input_seed(),
            operation.digest_seed(),
            admission_seed,
        ),
        Operation::DecisionApply | Operation::DecisionNoEffect => database.prepare_decision_put(
            ports,
            &ROW_IDS,
            operation.input_seed(),
            operation.digest_seed(),
            admission_seed,
        ),
        Operation::Delete => database.prepare_delete(
            ports,
            &ROW_IDS,
            operation.input_seed(),
            operation.digest_seed(),
            admission_seed,
        ),
        Operation::Consume => database.prepare_consume_child(
            ports,
            ROW_IDS[0],
            ROW_IDS[0],
            operation.input_seed(),
            operation.digest_seed(),
            admission_seed,
        ),
    }
}

fn parse_mode(mode: &str) -> (Operation, CrashPhase) {
    match mode {
        "put-before" => (Operation::Put, CrashPhase::BeforeCommit),
        "put-after" => (Operation::Put, CrashPhase::AfterCommit),
        "initialized-put-before" => (Operation::InitializedPut, CrashPhase::BeforeCommit),
        "initialized-put-after" => (Operation::InitializedPut, CrashPhase::AfterCommit),
        "decision-apply-before" => (Operation::DecisionApply, CrashPhase::BeforeCommit),
        "decision-apply-after" => (Operation::DecisionApply, CrashPhase::AfterCommit),
        "decision-no-effect-before" => (Operation::DecisionNoEffect, CrashPhase::BeforeCommit),
        "decision-no-effect-after" => (Operation::DecisionNoEffect, CrashPhase::AfterCommit),
        "delete-before" => (Operation::Delete, CrashPhase::BeforeCommit),
        "delete-after" => (Operation::Delete, CrashPhase::AfterCommit),
        "consume-before" => (Operation::Consume, CrashPhase::BeforeCommit),
        "consume-after" => (Operation::Consume, CrashPhase::AfterCommit),
        _ => panic!("unknown bulk recovery child mode"),
    }
}

fn assert_decision_rows_present(
    database: &BulkRowsDatabase,
    ports: &riffdb_storage_redb::RedbOperationalPorts,
    expected: bool,
) {
    for row in ROW_IDS {
        assert_eq!(
            ports
                .read_entity(&database.row_target(row))
                .expect("read decision recovery row")
                .is_some(),
            expected
        );
    }
}

#[test]
// req: BLK-024, BLK-025, BLK-027, TXN-030, TXN-031
fn correlated_index_work_matches_independent_authoritative_model() {
    use bulk_correlated_index_support::IndexedMutationBulkDatabase;
    use riffdb_storage_api::AuthoritativeNamespaceV1;
    use riffdb_testkit::model::{
        CorrelatedIndexWorkModel, CorrelatedIndexedMutationRow, CorrelatedModelDivergence,
        verify_correlated_index_work_against_redb, verify_work_accounting_boundary_witnesses,
    };

    verify_work_accounting_boundary_witnesses()
        .expect("exact and +1 work-accounting boundary witnesses hold");

    for (case_index, count) in [1usize, 9, 19, 100].into_iter().enumerate() {
        let seed_offset = u8::try_from(case_index * 8).expect("case offset");
        let partition_id = format!("tenant-model-{count}");
        let rows =
            CorrelatedIndexedMutationRow::deterministic_batch(&partition_id, count, true, false, 1);
        let database = IndexedMutationBulkDatabase::create(&format!("model-{count}"));

        let mut model = CorrelatedIndexWorkModel::new(&partition_id);
        let seed_metrics = model
            .apply_seed_initial_rows(&rows)
            .expect("model applies SeedInitialIndexedStateRows");
        assert_eq!(seed_metrics.binding_targets(), count);
        assert_eq!(seed_metrics.delta_count(), count * 18);
        assert_eq!(seed_metrics.affected_epoch_target_count(), 18);

        let ports = database.open();
        let seed_prep = database.prepare_seed(
            &ports,
            &rows,
            0x10 + seed_offset,
            0x20 + seed_offset,
            0x30 + seed_offset,
        );
        let seed_outcome = database.execute_committed(seed_prep, ports, 0x40 + seed_offset);
        assert_eq!(
            seed_outcome.stored_outcome().commit_sequence(),
            CommitSequence::first()
        );
        let ports = database.open();
        database.assert_commit_graph(&ports, seed_outcome.stored_outcome(), count, 0);
        drop(ports);

        let seed_session = database.open_inspection(model.namespace().clone());
        let seed_agreement = verify_correlated_index_work_against_redb(&model, &seed_session)
            .expect("seeded redb state matches independent correlated-index model");
        assert_eq!(seed_agreement.total_targets(), count * 19 + 20);
        drop(seed_session);

        let apply_metrics = model
            .apply_indexed_mutations(&rows)
            .expect("model applies ApplyIndexedMutations");
        assert_eq!(apply_metrics.binding_targets(), count * 2);
        assert_eq!(apply_metrics.delta_count(), count * 38);
        assert_eq!(apply_metrics.affected_epoch_target_count(), 20);
        assert_eq!(apply_metrics.validation_positions(), count * 2 + 20);
        assert_eq!(apply_metrics.work_units(), count * 40 + 40);

        let ports = database.open();
        let apply_prep = database.prepare_apply(
            &ports,
            &rows,
            0x11 + seed_offset,
            0x21 + seed_offset,
            0x31 + seed_offset,
        );
        let apply_outcome = database.execute_committed(apply_prep, ports, 0x41 + seed_offset);
        assert_eq!(
            apply_outcome.stored_outcome().commit_sequence(),
            CommitSequence::new(2).expect("second sequence")
        );
        let ports = database.open();
        database.assert_commit_graph(&ports, apply_outcome.stored_outcome(), count * 2, 0);
        drop(ports);

        let mut apply_session = database.open_inspection(model.namespace().clone());
        let apply_agreement = verify_correlated_index_work_against_redb(&model, &apply_session)
            .expect("post-apply redb state matches independent correlated-index model");
        assert_eq!(apply_agreement.total_targets(), count * 40 + 20);
        if count == 100 {
            assert_eq!(apply_agreement.page_count(), 16);
            assert_eq!(apply_agreement.total_targets(), 4_020);
        }

        // Verify that an undeclared redb-only target injected into the stopped
        // inspection session is deterministically rejected across pagination.
        apply_session.inject_undeclared_target(
            AuthoritativeNamespaceV1::SecondaryIndexes,
            vec![0xde, 0xad, 0xbe, 0xef],
        );
        assert_eq!(
            verify_correlated_index_work_against_redb(&model, &apply_session),
            Err(CorrelatedModelDivergence::RedbOnlyUndeclaredTarget {
                page_ordinal: 0,
                namespace: AuthoritativeNamespaceV1::SecondaryIndexes,
                raw_key: vec![0xde, 0xad, 0xbe, 0xef],
            })
        );
    }
}

#[test]
// req: BLK-024, BLK-025, BLK-027, TXN-030, TXN-031
fn correlated_index_work_redb_crash_reopen_is_complete_or_absent() {
    for count in [19usize, 100] {
        for phase in [CrashPhase::BeforeCommit, CrashPhase::AfterCommit] {
            run_correlated_crash_case(count, phase);
        }
    }
}

#[test]
// req: BLK-024, BLK-025, BLK-027, TXN-030, TXN-031
fn correlated_index_work_remote_redb_loopback_matches_model() {
    use bulk_correlated_index_support::run_remote_correlated_index_loopback_case;

    for count in [1usize, 9, 19, 100] {
        let agreement = run_remote_correlated_index_loopback_case(count);
        assert_eq!(agreement.total_targets(), count * 40 + 20);
        if count == 100 {
            assert_eq!(agreement.page_count(), 16);
            assert_eq!(agreement.total_targets(), 4_020);
        }
    }
}

fn run_correlated_crash_case(count: usize, phase: CrashPhase) {
    use bulk_correlated_index_support::IndexedMutationBulkDatabase;
    use riffdb_testkit::model::{
        CorrelatedIndexWorkModel, CorrelatedIndexedMutationRow,
        verify_correlated_index_work_against_redb,
    };

    let label = format!("correlated-{count}-{}", phase.label());
    let partition_id = format!("tenant-crash-{count}-{}", phase.label());
    let rows =
        CorrelatedIndexedMutationRow::deterministic_batch(&partition_id, count, true, false, 1);
    let database = IndexedMutationBulkDatabase::create(&label);

    let mut seed_model = CorrelatedIndexWorkModel::new(&partition_id);
    seed_model
        .apply_seed_initial_rows(&rows)
        .expect("seed model applies SeedInitialIndexedStateRows");
    let mut full_model = seed_model.clone();
    full_model
        .apply_indexed_mutations(&rows)
        .expect("full model applies ApplyIndexedMutations");

    let ports = database.open();
    let seed_prep = database.prepare_seed(&ports, &rows, 0x41, 0x51, 0x61);
    let seed_outcome = database.execute_committed(seed_prep, ports, 0x71);
    assert_eq!(
        seed_outcome.stored_outcome().commit_sequence(),
        CommitSequence::first()
    );
    riffdb_storage_redb::strip_validated_prefix_checkpoint_fixture(database.path())
        .expect("strip pre-seed checkpoint before correlated crash");
    let ports = database.open();
    database.assert_commit_graph(&ports, seed_outcome.stored_outcome(), count, 0);
    assert!(
        ports
            .write_validated_prefix_checkpoint()
            .expect("checkpoint correlated seed before crash"),
        "correlated seed must checkpoint before crash arm"
    );
    drop(ports);

    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("bulk_command_recovery_child")
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(CHILD_MODE, format!("correlated-{count}-{}", phase.label()))
        .env(CHILD_DATABASE, database.path())
        .status()
        .expect("run crashing correlated-index child");
    assert!(
        !status.success(),
        "correlated-index crash child must terminate abnormally"
    );

    // Inspect stopped redb immediately after process crash and verify complete
    // absence (BeforeCommit -> seed_model) or complete presence (AfterCommit -> full_model).
    let expected_post_crash_model = match phase {
        CrashPhase::BeforeCommit => &seed_model,
        CrashPhase::AfterCommit => &full_model,
    };
    let post_crash_session =
        database.open_inspection(expected_post_crash_model.namespace().clone());
    assert!(
        post_crash_session.structural_findings().is_empty(),
        "stopped redb after correlated-index crash must have zero structural findings"
    );
    let post_crash_agreement =
        verify_correlated_index_work_against_redb(expected_post_crash_model, &post_crash_session)
            .expect("stopped redb after crash must be complete or absent with zero partial index/entity residue");
    match phase {
        CrashPhase::BeforeCommit => {
            assert_eq!(post_crash_agreement.total_targets(), count * 19 + 20);
        }
        CrashPhase::AfterCommit => {
            assert_eq!(post_crash_agreement.total_targets(), count * 40 + 20);
        }
    }
    drop(post_crash_session);

    // Reopen operational ports and replay the exact same ApplyIndexedMutations command.
    let ports = database.open();
    let replay_prep = database.prepare_apply(&ports, &rows, 0x42, 0x52, 0xc2);
    let provenance = Arc::new(CountingProvenanceSource::new(0xd2));
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::clone(&provenance),
    );
    let executor = coordinator.command_executor();
    let replay = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve correlated retry")
            .submit(replay_prep)
            .expect("submit correlated retry")
            .completion()
            .await
            .expect("complete correlated retry")
    });
    let CommandExecutionResult::Committed(replay) = replay else {
        panic!("correlated retry must commit");
    };
    assert_eq!(replay.disposition(), phase.expected_disposition());
    assert_eq!(
        provenance.calls(),
        usize::from(phase == CrashPhase::BeforeCommit),
        "correlated replay after commit must not allocate fresh provenance"
    );
    assert_eq!(
        replay.stored_outcome().commit_sequence(),
        CommitSequence::new(2).expect("second sequence")
    );
    drop(executor);
    coordinator
        .shutdown()
        .expect("drain recovered correlated coordinator");

    let ports = database.open();
    database.assert_commit_graph(&ports, replay.stored_outcome(), count * 2, 0);
    drop(ports);

    let post_replay_session = database.open_inspection(full_model.namespace().clone());
    let post_replay_agreement =
        verify_correlated_index_work_against_redb(&full_model, &post_replay_session)
            .expect("stopped redb after correlated retry must match full model");
    assert_eq!(post_replay_agreement.total_targets(), count * 40 + 20);
}

fn run_correlated_crash_child(path: &Path, rest: &str) {
    use bulk_correlated_index_support::IndexedMutationBulkDatabase;
    use riffdb_testkit::model::CorrelatedIndexedMutationRow;

    let (count_str, phase_str) = rest
        .rsplit_once('-')
        .expect("correlated child mode format <count>-<phase>");
    let count: usize = count_str.parse().expect("correlated child count");
    let phase = match phase_str {
        "before" => CrashPhase::BeforeCommit,
        "after" => CrashPhase::AfterCommit,
        _ => panic!("unknown correlated crash phase"),
    };
    let partition_id = format!("tenant-crash-{count}-{}", phase.label());
    let rows =
        CorrelatedIndexedMutationRow::deterministic_batch(&partition_id, count, true, false, 1);
    let database = IndexedMutationBulkDatabase::attach(path);
    let ports = database.open_with_controller(phase.controller());
    let preparation = database.prepare_apply(&ports, &rows, 0x42, 0x52, 0xc1);
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(CountingProvenanceSource::new(0xd1)),
    );
    let executor = coordinator.command_executor();
    let unexpected = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("reserve crashing correlated command")
            .submit(preparation)
            .expect("submit crashing correlated command")
            .completion()
            .await
    });
    panic!(
        "armed correlated command-batch crash failpoint did not abort the child: {unexpected:?}"
    );
}
