//! Characterisation of checkpoint segment-rewrite amplification.
//!
//! `CheckpointDir::checkpoint` rewrites in full every organization holding at
//! least one dirty row: it merges that organization's prior segments with the
//! delta via `materialize_org_rows`, re-encodes every cell of every merged row,
//! and writes and fsyncs one new segment file. The cost of one checkpoint is
//! therefore proportional to the organization's resident row count, not to the
//! number of rows that changed.
//!
//! These tests pin that shape so a later change to the materialization strategy
//! has a baseline to compare against. They assert the *current* behaviour; they
//! are not a statement that the current behaviour is desirable.

mod common;

use riffdb_columnar::{ColumnarAmplification, ColumnarEngine, RegisteredDefinition};

use common::*;

/// Applies `commits` single-ticket creates into one organization, checkpointing
/// every `cadence` commits exactly as the server's columnar worker does.
fn drive_one_org(
    definition: RegisteredDefinition,
    label: &str,
    commits: u64,
    cadence: u64,
) -> (ColumnarAmplification, u64) {
    let bundle = compile_bundle();
    let mut engine = ColumnarEngine::open(
        definition,
        riffdb_columnar::OpenOptions::new(temp_dir(label)).with_history_incarnation(1),
    )
    .expect("open engine");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0x10);

    let mut since_checkpoint = 0u64;
    for sequence in 1..=commits {
        push_ticket_create(
            &mut source,
            &mut oracle,
            &bundle,
            sequence,
            org,
            sequence,
            1,
            "title",
            5,
        );
        engine.apply_available(&source).expect("apply");
        since_checkpoint += 1;
        if since_checkpoint >= cadence {
            engine.checkpoint().expect("checkpoint");
            since_checkpoint = 0;
        }
    }
    (engine.amplification(), engine.resident_segment_rows())
}

#[test]
fn checkpoint_rewrites_the_whole_organization_not_the_dirty_rows() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let cadence = 64;
    let commits = 512;
    let (amplification, resident) =
        drive_one_org(definition, "amplification-whole-org", commits, cadence);

    // Every commit created exactly one row, so the dirty count is the commit
    // count and the resident count is the distinct row count.
    assert_eq!(amplification.rows_dirty, commits);
    assert_eq!(resident, commits);
    assert_eq!(amplification.checkpoints, commits / cadence);
    assert_eq!(amplification.segments_written, commits / cadence);

    // Checkpoint k rewrites k * cadence rows, so the total is the triangular
    // number of checkpoints times the cadence: 64 * (1+2+..+8) = 2304.
    let checkpoints = commits / cadence;
    let expected_rewritten = cadence * checkpoints * (checkpoints + 1) / 2;
    assert_eq!(amplification.rows_rewritten, expected_rewritten);
    assert!(
        amplification.rows_rewritten > amplification.rows_dirty * 4,
        "expected material amplification, got {} rewritten for {} dirty",
        amplification.rows_rewritten,
        amplification.rows_dirty
    );
}

#[test]
fn rewrite_amplification_grows_linearly_with_dataset_size() {
    let bundle = compile_bundle();
    let cadence = 64;

    let small = drive_one_org(
        register_ticket_board(&bundle),
        "amplification-small",
        512,
        cadence,
    )
    .0;
    let large = drive_one_org(
        register_ticket_board(&bundle),
        "amplification-large",
        1024,
        cadence,
    )
    .0;

    let small_ratio = small.rows_rewritten as f64 / small.rows_dirty as f64;
    let large_ratio = large.rows_rewritten as f64 / large.rows_dirty as f64;

    // Total rewritten work is ~N^2 / (2 * cadence), so the per-row
    // amplification ratio is ~N / (2 * cadence): doubling the dataset doubles
    // the amplification rather than leaving it constant.
    assert!(
        large_ratio > small_ratio * 1.8,
        "amplification ratio must grow with size: {small_ratio} then {large_ratio}"
    );

    // Encoded bytes follow the same shape, so derived write volume is
    // superlinear in the dataset for a linear command stream.
    assert!(
        large.segment_bytes_written > small.segment_bytes_written * 3,
        "segment bytes must grow superlinearly: {} then {}",
        small.segment_bytes_written,
        large.segment_bytes_written
    );
}

#[test]
fn an_idle_checkpoint_writes_no_segment() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut engine = ColumnarEngine::open(
        definition,
        riffdb_columnar::OpenOptions::new(temp_dir("amplification-idle"))
            .with_history_incarnation(1),
    )
    .expect("open engine");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0x10);
    push_ticket_create(&mut source, &mut oracle, &bundle, 1, org, 1, 1, "one", 5);
    engine.apply_available(&source).expect("apply");
    engine.checkpoint().expect("first checkpoint");
    let after_first = engine.amplification();
    assert_eq!(after_first.segments_written, 1);

    // The columnar worker forces a checkpoint every CHECKPOINT_POLL_CADENCE
    // polls whether or not anything changed. With an empty delta that must not
    // rewrite a segment.
    for _ in 0..8 {
        engine.apply_available(&source).expect("apply");
        engine.checkpoint().expect("idle checkpoint");
    }
    let after_idle = engine.amplification();
    assert_eq!(
        after_idle, after_first,
        "an idle checkpoint must not rewrite segments"
    );
}

#[test]
fn skipping_an_idle_checkpoint_preserves_the_durable_state() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let directory = temp_dir("amplification-idle-durable");
    let org = uuid(0x10);
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();

    let durable_after_write;
    {
        let mut engine = ColumnarEngine::open(
            definition.clone(),
            riffdb_columnar::OpenOptions::new(directory.clone()).with_history_incarnation(1),
        )
        .expect("open engine");
        for sequence in 1..=4 {
            push_ticket_create(
                &mut source,
                &mut oracle,
                &bundle,
                sequence,
                org,
                sequence,
                1,
                "title",
                5,
            );
        }
        engine.apply_available(&source).expect("apply");
        engine.checkpoint().expect("durable checkpoint");
        durable_after_write = engine.durable_frontier().position();

        // Forced idle checkpoints must leave the durable frontier where the
        // last real checkpoint put it.
        for _ in 0..5 {
            engine.apply_available(&source).expect("apply");
            engine.checkpoint().expect("idle checkpoint");
        }
        assert_eq!(engine.durable_frontier().position(), durable_after_write);
    }

    // Reopening reads the manifest the real checkpoint wrote: skipping the
    // idle rewrites lost nothing.
    let reopened = ColumnarEngine::open(
        definition,
        riffdb_columnar::OpenOptions::new(directory).with_history_incarnation(1),
    )
    .expect("reopen engine");
    assert_eq!(reopened.durable_frontier().position(), durable_after_write);
    assert_eq!(reopened.resident_segment_rows(), 4);
}
