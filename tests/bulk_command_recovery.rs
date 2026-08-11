#![forbid(unsafe_code)]

//! WP-561 process-crash recovery evidence for atomic collection commands.

#[path = "command_semantics/support.rs"]
mod support;

use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use riffdb_commit::{
    CommandExecutionPreparation, CommandExecutionResult, CommittedOutcome,
    CommittedOutcomeDisposition,
};
use riffdb_storage_redb::{RedbTestController, RedbTestOperation};
use riffdb_types::CommitSequence;

use support::{
    BulkRowsDatabase, CountingProvenanceSource, FixedAdmissionClock, command_timestamp, runtime,
    start_coordinator,
};

const CHILD_MODE: &str = "RIFFDB_BULK_RECOVERY_CHILD_MODE";
const CHILD_DATABASE: &str = "RIFFDB_BULK_RECOVERY_DATABASE";
const ROW_IDS: [[u8; 16]; 4] = [[0x61; 16], [0x62; 16], [0x63; 16], [0x64; 16]];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Put,
    Delete,
}

impl Operation {
    const fn label(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::Delete => "delete",
        }
    }

    const fn input_seed(self) -> u8 {
        match self {
            Self::Put => 0xa1,
            Self::Delete => 0xa2,
        }
    }

    const fn digest_seed(self) -> u8 {
        match self {
            Self::Put => 0xb1,
            Self::Delete => 0xb2,
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
fn collection_create_and_delete_are_complete_or_absent_across_process_crash() {
    for operation in [Operation::Delete, Operation::Put] {
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
fn bulk_command_recovery_child() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    let path = std::env::var_os(CHILD_DATABASE).expect("child database path");
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
    if operation == Operation::Delete {
        let outcome = execute_healthy(&database, Operation::Put, 0x91, 0xe1, 0xf1, 0x81);
        let ports = database.open();
        database.assert_rows_present(&ports, &ROW_IDS, true);
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
        Operation::Put => durable_visibility,
        Operation::Delete => !durable_visibility,
    };
    database.assert_rows_present(&ports, &ROW_IDS, expected_present);

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
    database.assert_rows_present(&ports, &ROW_IDS, operation == Operation::Put);
    database.assert_commit_graph(&ports, replay.stored_outcome(), ROW_IDS.len());
    let expected_sequence = match operation {
        Operation::Put => CommitSequence::first(),
        Operation::Delete => CommitSequence::new(2).expect("second sequence"),
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
        Operation::Delete => {
            database.prepare_delete(&ports, &ROW_IDS, input_seed, digest_seed, admission_seed)
        }
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
            .expect("complete healthy collection command")
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
        Operation::Delete => database.prepare_delete(
            ports,
            &ROW_IDS,
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
        "delete-before" => (Operation::Delete, CrashPhase::BeforeCommit),
        "delete-after" => (Operation::Delete, CrashPhase::AfterCommit),
        _ => panic!("unknown bulk recovery child mode"),
    }
}
