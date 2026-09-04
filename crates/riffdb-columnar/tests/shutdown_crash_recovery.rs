//! Process-level proof for worker-only columnar abandonment and replay.

mod common;

use std::path::PathBuf;
use std::process::{Command, ExitStatus};

use riffdb_columnar::{ColumnarEngine, OpenOptions, WorkerApplyOutcome};
use riffdb_types::{CommitSequence, FrontierPosition};

use common::{
    HistorySource, Oracle, assert_corpus_equivalence, compile_bundle, open_engine,
    push_ticket_create, register_ticket_board, temp_dir, uuid,
};

const CHILD_ROLE: &str = "RIFFDB_WP773_CRASH_CHILD";
const CHILD_DIRECTORY: &str = "RIFFDB_WP773_CRASH_DIRECTORY";
const CRASH_EXIT: i32 = 73;

fn source_through(count: u64) -> (HistorySource, Oracle, [u8; 16]) {
    let bundle = compile_bundle();
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0x73);
    for sequence in 1..=count {
        push_ticket_create(
            &mut source,
            &mut oracle,
            &bundle,
            sequence,
            org,
            sequence,
            sequence,
            "crash-replay",
            sequence as i64,
        );
    }
    (source, oracle, org)
}

fn run_child(directory: PathBuf) -> ! {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let (prefix, _, _) = source_through(10);
    let mut seeded = ColumnarEngine::open(definition.clone(), OpenOptions::new(directory.clone()))
        .expect("open child seed");
    seeded.apply_available(&prefix).expect("apply child prefix");
    seeded.checkpoint().expect("checkpoint child prefix");
    drop(seeded);

    let (complete, _, _) = source_through(150);
    let mut abandoned = ColumnarEngine::open(definition, OpenOptions::new(directory))
        .expect("reopen child projection");
    assert_eq!(
        abandoned
            .apply_available_for_worker(&complete, || true)
            .expect("abandon child page"),
        WorkerApplyOutcome::AbandonedUnpublished
    );
    assert_eq!(
        abandoned.durable_frontier().position(),
        FrontierPosition::AppliedThrough(CommitSequence::new(10).expect("durable prefix"))
    );

    // `exit` intentionally skips Rust destructors, modeling process loss after
    // the worker has abandoned its unpublished in-memory delta.
    std::process::exit(CRASH_EXIT);
}

fn expected_crash(status: ExitStatus) -> bool {
    status.code() == Some(CRASH_EXIT)
}

// req: PRJ-001, PRJ-002, PRJ-003, PRJ-004
#[test]
fn abandoned_columnar_shutdown_replays_from_the_durable_frontier() {
    if std::env::var_os(CHILD_ROLE).is_some() {
        let directory = std::env::var_os(CHILD_DIRECTORY)
            .map(PathBuf::from)
            .expect("child directory");
        run_child(directory);
    }

    let directory = temp_dir("worker-process-crash-replay");
    let status = Command::new(std::env::current_exe().expect("current test binary"))
        .arg("--exact")
        .arg("abandoned_columnar_shutdown_replays_from_the_durable_frontier")
        .arg("--nocapture")
        .env(CHILD_ROLE, "1")
        .env(CHILD_DIRECTORY, &directory)
        .current_dir(&directory)
        .status()
        .expect("run crash child");
    assert!(expected_crash(status), "unexpected child status: {status}");

    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let (source, oracle, org) = source_through(150);
    let mut replayed = ColumnarEngine::open(definition.clone(), OpenOptions::new(directory))
        .expect("reopen after child loss");
    assert_eq!(
        replayed.processed_frontier().position(),
        FrontierPosition::AppliedThrough(CommitSequence::new(10).expect("replay origin"))
    );
    assert_eq!(
        replayed.durable_frontier().position(),
        FrontierPosition::AppliedThrough(CommitSequence::new(10).expect("durable origin"))
    );
    replayed
        .apply_available(&source)
        .expect("replay abandoned suffix");

    let mut uninterrupted = open_engine(definition, "process-crash-oracle");
    uninterrupted
        .apply_available(&source)
        .expect("uninterrupted oracle");
    assert_eq!(
        replayed.published_frontier_position(),
        uninterrupted.published_frontier_position()
    );
    assert_corpus_equivalence(&replayed, &oracle, &bundle, &[org], "process replay");
    assert_corpus_equivalence(
        &uninterrupted,
        &oracle,
        &bundle,
        &[org],
        "process uninterrupted",
    );
}
