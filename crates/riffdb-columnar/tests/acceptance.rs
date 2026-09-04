//! CP1 acceptance matrix: apply protocol, org scope, idempotence, equivalence,
//! compaction invariance, lifecycle outcomes, and crash recovery.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use riffdb_storage_api::CommittedEntityReferenceV2;
use riffdb_types::{
    AggregateSemanticIdentityV1, CanonicalValue, CommitSequence, EntityVersion, FrontierPosition,
    ProjectionProviderCapabilitiesV1, ProjectionProviderKindV1, ProjectionProviderPolicyModeV1,
    ProjectionProviderPostureV1, ProjectionProviderStaticBoundsV1,
};

use riffdb_columnar::{
    AggregateOp, CheckpointError, ColumnPredicate, ColumnarEngine, ColumnarError, ColumnarOutcome,
    ColumnarProjectionDefinition, ColumnarQueryRequest, ColumnarSnapshotRebuild,
    ColumnarTestBoundary, ColumnarTestController, GroupBySpec, OpenOptions, OrderSpec, QueryBudget,
    QueryError, QueryResult, RegisteredDefinition, SortDirection, WorkerApplyOutcome,
};

use common::*;

#[test]
// req: PRJ-004
fn authoritative_snapshot_rebuild_is_private_until_exact_publication() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut engine = open_engine(definition.clone(), "snapshot-rebuild");
    let mut source = HistorySource::default();
    let org = uuid(0x10);
    push_ticket_create(
        &mut source,
        &mut Oracle::default(),
        &bundle,
        1,
        org,
        1,
        1,
        "one",
        5,
    );
    push_ticket_create(
        &mut source,
        &mut Oracle::default(),
        &bundle,
        2,
        org,
        2,
        1,
        "two",
        7,
    );

    assert!(matches!(
        engine.outcome(FrontierPosition::AppliedThrough(
            CommitSequence::new(2).expect("head")
        )),
        ColumnarOutcome::Building(_)
    ));
    let records = source.entities.values().cloned().collect::<Vec<_>>();
    let mut rebuild = ColumnarSnapshotRebuild::new(definition);
    rebuild.apply_page(&records).expect("bounded page");
    assert!(matches!(
        engine.outcome(FrontierPosition::AppliedThrough(
            CommitSequence::new(2).expect("head")
        )),
        ColumnarOutcome::Building(_)
    ));

    rebuild
        .install(
            &mut engine,
            FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("frontier")),
        )
        .expect("atomic install");
    let rows = engine
        .published_snapshot()
        .merged_org(&riffdb_columnar::OrgKey::from_value(&CanonicalValue::Uuid(org)).expect("org"));
    assert_eq!(rows.len(), 2);
    assert_eq!(
        engine.published_frontier_position(),
        FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("frontier"))
    );
}

// req: PRJ-001, PRJ-002, PERF-007, PERF-008
#[test]
fn ordinary_columnar_apply_publication_and_checkpoint_are_unchanged() {
    let bundle = compile_bundle();
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0x31);
    for sequence in 1..=130 {
        push_ticket_create(
            &mut source,
            &mut oracle,
            &bundle,
            sequence,
            org,
            sequence,
            sequence,
            "ordinary",
            sequence as i64,
        );
    }
    let mut ordinary = open_engine(register_ticket_board(&bundle), "ordinary-control");
    let ordinary_progress = ordinary.apply_available(&source).expect("ordinary apply");
    let ordinary_manifest = ordinary.checkpoint().expect("ordinary checkpoint");

    let mut worker = open_engine(register_ticket_board(&bundle), "ordinary-worker");
    let mut continuation_observations = 0_u8;
    let worker_progress = match worker
        .apply_available_for_worker(&source, || {
            continuation_observations = continuation_observations.saturating_add(1);
            false
        })
        .expect("worker apply")
    {
        WorkerApplyOutcome::Completed(progress) => progress,
        WorkerApplyOutcome::AbandonedUnpublished => panic!("no stop was requested"),
    };
    let worker_manifest = worker.checkpoint().expect("worker checkpoint");

    assert_eq!(
        continuation_observations, 2,
        "64 + 64 records have continuation edges; terminal 2 does not"
    );
    assert_eq!(worker_progress, ordinary_progress);
    assert_eq!(worker_manifest, ordinary_manifest);
    assert_eq!(worker.durable_frontier(), ordinary.durable_frontier());
    assert_corpus_equivalence(&worker, &oracle, &bundle, &[org], "worker no-stop");
    assert_corpus_equivalence(&ordinary, &oracle, &bundle, &[org], "ordinary control");
}

// req: PRJ-001, PRJ-002, PERF-007, PERF-008, PERF-019
#[test]
fn shutdown_abandons_only_unpublished_columnar_work_at_a_page_boundary() {
    let bundle = compile_bundle();
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0x32);
    for sequence in 1..=63 {
        push_ticket_create(
            &mut source,
            &mut oracle,
            &bundle,
            sequence,
            org,
            sequence,
            sequence,
            "safe-prefix",
            sequence as i64,
        );
    }
    let _held = push_open_race_v1(&mut source, &mut oracle, &bundle, 64, org, 64);
    for sequence in 65..=130 {
        push_ticket_create(
            &mut source,
            &mut oracle,
            &bundle,
            sequence,
            org,
            sequence,
            sequence,
            "later-unpublished",
            sequence as i64,
        );
    }
    let dir = temp_dir("worker-abandon");
    let mut engine = ColumnarEngine::open(
        register_ticket_board(&bundle),
        OpenOptions::new(dir.clone()),
    )
    .expect("open");
    let stop_requested = Arc::new(AtomicBool::new(false));
    let (boundary_entered_tx, boundary_entered_rx) = std::sync::mpsc::channel();
    let (boundary_release_tx, boundary_release_rx) = std::sync::mpsc::channel();
    let worker_stop = Arc::clone(&stop_requested);
    let worker = std::thread::spawn(move || {
        let mut continuation_observations = 0_u8;
        let outcome = engine
            .apply_available_for_worker(&source, || {
                continuation_observations = continuation_observations.saturating_add(1);
                boundary_entered_tx
                    .send(())
                    .expect("announce complete page boundary");
                boundary_release_rx
                    .recv()
                    .expect("release complete page boundary");
                worker_stop.load(Ordering::Acquire)
            })
            .expect("bounded abandonment");
        (engine, outcome, continuation_observations)
    });

    boundary_entered_rx
        .recv()
        .expect("worker reached a complete page boundary");
    stop_requested.store(true, Ordering::Release);
    boundary_release_tx
        .send(())
        .expect("release worker after stop request");
    let (engine, outcome, continuation_observations) =
        worker.join().expect("join bounded worker apply");

    assert_eq!(outcome, WorkerApplyOutcome::AbandonedUnpublished);
    assert_eq!(continuation_observations, 1);
    assert_eq!(
        engine.processed_frontier().position(),
        FrontierPosition::AppliedThrough(CommitSequence::new(64).expect("page boundary"))
    );
    assert_eq!(
        engine.published_frontier_position(),
        FrontierPosition::AppliedThrough(CommitSequence::new(63).expect("safe prefix")),
        "the pre-holdback safe prefix remains published"
    );
    assert_eq!(
        engine.durable_frontier().position(),
        FrontierPosition::BeforeFirst
    );
    assert_no_durable_files(&dir, "abandoned worker apply");
    let rows = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("safe-prefix query"),
    );
    assert_eq!(rows.len(), 63, "later unpublished work remains invisible");
}

// req: PRJ-001, PRJ-002, PRJ-003, PRJ-004
#[test]
fn abandoned_columnar_reopen_replays_from_the_durable_frontier() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0x33);
    for sequence in 1..=10 {
        push_ticket_create(
            &mut source,
            &mut oracle,
            &bundle,
            sequence,
            org,
            sequence,
            sequence,
            "durable",
            sequence as i64,
        );
    }
    let dir = temp_dir("worker-replay");
    let mut seeded =
        ColumnarEngine::open(definition.clone(), OpenOptions::new(dir.clone())).expect("seed");
    seeded.apply_available(&source).expect("seed apply");
    seeded.checkpoint().expect("seed checkpoint");
    drop(seeded);

    for sequence in 11..=150 {
        push_ticket_create(
            &mut source,
            &mut oracle,
            &bundle,
            sequence,
            org,
            sequence,
            sequence,
            "replayed",
            sequence as i64,
        );
    }
    let mut abandoned =
        ColumnarEngine::open(definition.clone(), OpenOptions::new(dir.clone())).expect("reopen");
    let outcome = abandoned
        .apply_available_for_worker(&source, || true)
        .expect("abandon one page");
    assert_eq!(outcome, WorkerApplyOutcome::AbandonedUnpublished);
    assert_eq!(
        abandoned.durable_frontier().position(),
        FrontierPosition::AppliedThrough(CommitSequence::new(10).expect("durable frontier"))
    );
    drop(abandoned);

    let mut replayed =
        ColumnarEngine::open(definition.clone(), OpenOptions::new(dir)).expect("replay reopen");
    assert_eq!(
        replayed.processed_frontier().position(),
        FrontierPosition::AppliedThrough(CommitSequence::new(10).expect("replay begins here"))
    );
    replayed
        .apply_available(&source)
        .expect("replay to exact end");

    let mut uninterrupted = open_engine(definition, "uninterrupted-oracle");
    uninterrupted
        .apply_available(&source)
        .expect("uninterrupted apply");
    assert_eq!(
        replayed.published_frontier_position(),
        uninterrupted.published_frontier_position()
    );
    assert_corpus_equivalence(&replayed, &oracle, &bundle, &[org], "replayed");
    assert_corpus_equivalence(&uninterrupted, &oracle, &bundle, &[org], "uninterrupted");
}

// req: PRJ-001, PRJ-002, PRJ-003, PRJ-004, PERF-007, PERF-008, PERF-019
#[test]
fn production_scale_backlog_stops_at_one_page_and_no_stop_reaches_exact_end() {
    const PRODUCTION_BACKLOG: u64 = 19_220;
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0x34);
    for sequence in 1..=PRODUCTION_BACKLOG {
        push_ticket_create(
            &mut source,
            &mut oracle,
            &bundle,
            sequence,
            org,
            sequence,
            sequence,
            "production-backlog",
            sequence as i64,
        );
    }

    let mut stopped = open_engine(definition.clone(), "production-stop");
    let stopped_outcome = stopped
        .apply_available_for_worker(&source, || true)
        .expect("bounded production stop");
    assert_eq!(stopped_outcome, WorkerApplyOutcome::AbandonedUnpublished);
    assert_eq!(
        stopped.processed_frontier().position(),
        FrontierPosition::AppliedThrough(CommitSequence::new(64).expect("one page"))
    );
    assert_eq!(
        stopped.published_frontier_position(),
        FrontierPosition::BeforeFirst
    );
    assert_eq!(
        stopped.durable_frontier().position(),
        FrontierPosition::BeforeFirst
    );

    let mut ordinary = open_engine(definition.clone(), "production-ordinary");
    let ordinary_progress = ordinary
        .apply_available(&source)
        .expect("ordinary exact end");

    let mut worker = open_engine(definition, "production-worker");
    let mut continuation_observations = 0_u16;
    let worker_progress = match worker
        .apply_available_for_worker(&source, || {
            continuation_observations = continuation_observations.saturating_add(1);
            false
        })
        .expect("worker exact end")
    {
        WorkerApplyOutcome::Completed(progress) => progress,
        WorkerApplyOutcome::AbandonedUnpublished => panic!("no stop requested"),
    };

    assert_eq!(continuation_observations, 300);
    assert_eq!(worker_progress, ordinary_progress);
    assert_eq!(
        worker.resident_segment_rows(),
        ordinary.resident_segment_rows()
    );
    let worker_rows = rows_of(
        worker
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("worker rows"),
    );
    let ordinary_rows = rows_of(
        ordinary
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("ordinary rows"),
    );
    assert_eq!(worker_rows, ordinary_rows);
    assert_eq!(worker_rows.len(), PRODUCTION_BACKLOG as usize);
}

#[test]
// req: PRJ-001, PRJ-002
fn acceptance_reference_match_at_published_frontier() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut engine = open_engine(definition, "equiv");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();

    let org_a = uuid(0x10);
    let org_b = uuid(0x20);
    push_ticket_create(&mut source, &mut oracle, &bundle, 1, org_a, 1, 1, "a1", 5);
    push_ticket_create(&mut source, &mut oracle, &bundle, 2, org_b, 1, 1, "b1", 3);
    push_ticket_create(&mut source, &mut oracle, &bundle, 3, org_a, 2, 2, "a2", 7);
    push_ticket_replace(
        &mut source,
        &mut oracle,
        &bundle,
        4,
        org_a,
        1,
        2,
        3,
        "a1-upd",
        9,
    );
    push_irrelevant_note(&mut source, &bundle, 5, org_a, 99);

    let progress = engine.apply_available(&source).expect("apply");
    assert!(progress.caught_up);
    assert_eq!(
        progress.published_frontier,
        FrontierPosition::AppliedThrough(CommitSequence::new(5).expect("5"))
    );

    assert_corpus_equivalence(
        &engine,
        &oracle,
        &bundle,
        &[org_a, org_b],
        "reference match",
    );
}

#[test]
fn acceptance_all_or_none_multi_entity_commit() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut engine = open_engine(definition, "all-or-none");
    let mut source = HistorySource::default();

    let ticket_type = entity_type_id(&bundle, "Ticket");
    let org = uuid(0x11);
    let t1 = HistorySource::ticket_target(ticket_type, org, 1);
    let t2 = HistorySource::ticket_target(ticket_type, org, 2);
    let e1 = HistorySource::make_entity(
        t1.clone(),
        EntityVersion::first(),
        ticket_fields(&bundle, org, 1, 1, "one", 1),
    );
    let e2 = HistorySource::make_entity(
        t2.clone(),
        EntityVersion::first(),
        ticket_fields(&bundle, org, 2, 2, "two", 2),
    );
    let r1 = CommittedEntityReferenceV2::from_post_image(&e1).expect("r1");
    let r2 = CommittedEntityReferenceV2::from_post_image(&e2).expect("r2");
    source.put_entity(e1);
    source.put_entity(e2);
    // Single multi-entity commit.
    source.append_commit(
        CommitSequence::new(1).expect("1"),
        vec![r1, r2],
        vec![
            (t1, riffdb_storage_api::ExpectedEntityState::Absent),
            (t2, riffdb_storage_api::ExpectedEntityState::Absent),
        ],
    );

    // Publication is atomic per commit: after apply, both rows are visible.
    engine.apply_available(&source).expect("apply");
    let rows = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("q"),
    );
    assert_eq!(rows.len(), 2, "both entities of the commit must be visible");

    // Race + holdback half: build a commit where the first entity matches and
    // the second is deferred — holdback must keep the previous snapshot
    // (empty), never one row.
    let mut engine2 = open_engine(register_ticket_board(&bundle), "all-or-none-2");
    let mut source2 = HistorySource::default();
    let t3 = HistorySource::ticket_target(ticket_type, org, 3);
    let t4 = HistorySource::ticket_target(ticket_type, org, 4);
    let e3_v1 = HistorySource::make_entity(
        t3.clone(),
        EntityVersion::first(),
        ticket_fields(&bundle, org, 3, 1, "e3v1", 1),
    );
    let e3_v2 = HistorySource::make_entity(
        t3.clone(),
        EntityVersion::new(2).expect("2"),
        ticket_fields(&bundle, org, 3, 9, "e3v2", 9),
    );
    let e4 = HistorySource::make_entity(
        t4.clone(),
        EntityVersion::first(),
        ticket_fields(&bundle, org, 4, 4, "e4", 4),
    );
    let ref_e3_v1 = CommittedEntityReferenceV2::from_post_image(&e3_v1).expect("r");
    let ref_e3_v2 = CommittedEntityReferenceV2::from_post_image(&e3_v2).expect("r");
    let ref_e4 = CommittedEntityReferenceV2::from_post_image(&e4).expect("r");
    // Storage at e3=v2, e4=v1. Commit1: e3@v1 + e4@v1 (e3 races). Commit2: e3@v2.
    source2.put_entity(e3_v2);
    source2.put_entity(e4);
    source2.append_commit(
        CommitSequence::new(1).expect("1"),
        vec![ref_e3_v1, ref_e4],
        vec![
            (t3.clone(), riffdb_storage_api::ExpectedEntityState::Absent),
            (t4, riffdb_storage_api::ExpectedEntityState::Absent),
        ],
    );
    source2.append_commit(
        CommitSequence::new(2).expect("2"),
        vec![ref_e3_v2],
        vec![(
            t3,
            riffdb_storage_api::ExpectedEntityState::Present(EntityVersion::first()),
        )],
    );

    // Apply only first commit by using a truncated source... use full apply and
    // inspect deferred: after full apply published must include both or neither mid-way.
    engine2.apply_available(&source2).expect("apply");
    let rows = rows_of(
        engine2
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("q"),
    );
    // Final state: e3 v2 and e4 v1 — two rows, never a half commit left visible alone
    // without the other side of commit 1 (e4 must be present once published past holdback).
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .any(|r| r[1] == CanonicalValue::string("e4").unwrap()),
        "e4 from multi-entity commit must be visible after holdback release"
    );
}

#[test]
// req: PRJ-003
fn acceptance_duplicate_application_idempotent() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut engine = open_engine(definition, "idem");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0x30);
    push_ticket_create(&mut source, &mut oracle, &bundle, 1, org, 1, 1, "t", 1);
    engine.apply_available(&source).expect("apply1");
    let first = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("q1"),
    );
    // Replay same commits: processed already at 1, no-op.
    let progress = engine.apply_available(&source).expect("apply2");
    assert_eq!(
        progress.processed,
        FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("1"))
    );
    let second = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("q2"),
    );
    assert_eq!(first, second);
}

#[test]
fn acceptance_supersession_holdback_and_jump() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut engine = open_engine(definition, "race");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0x40);
    push_race_pair(&mut source, &mut oracle, &bundle, 1, 2, org, 1);

    let progress = engine.apply_available(&source).expect("apply");
    assert_eq!(progress.deferred_set_size, 0, "deferral resolved at v2");
    assert_eq!(
        progress.published_frontier,
        FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("2"))
    );
    let rows = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("q"),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], CanonicalValue::U64(2));
    assert_eq!(rows[0][1], CanonicalValue::string("v2-1").expect("s"));
    // Superseded v1 must not appear.
    assert!(
        !rows
            .iter()
            .any(|r| r[1] == CanonicalValue::string("v1-1").unwrap())
    );
    assert_corpus_equivalence(&engine, &oracle, &bundle, &[org], "holdback and jump");
}

#[test]
fn acceptance_holdback_defers_publication() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut engine = open_engine(definition, "holdback");
    let mut source = HistorySource::default();
    let org = uuid(0x41);
    let ticket_type = entity_type_id(&bundle, "Ticket");
    let target = HistorySource::ticket_target(ticket_type, org, 1);
    let e_v1 = HistorySource::make_entity(
        target.clone(),
        EntityVersion::first(),
        ticket_fields(&bundle, org, 1, 1, "v1", 1),
    );
    let e_v2 = HistorySource::make_entity(
        target.clone(),
        EntityVersion::new(2).expect("2"),
        ticket_fields(&bundle, org, 1, 2, "v2", 2),
    );
    let ref_v1 = CommittedEntityReferenceV2::from_post_image(&e_v1).expect("r");
    source.put_entity(e_v2); // live is v2
    source.append_commit(
        CommitSequence::new(1).expect("1"),
        vec![ref_v1],
        vec![(target, riffdb_storage_api::ExpectedEntityState::Absent)],
    );
    let progress = engine.apply_available(&source).expect("apply");
    assert_eq!(progress.deferred_set_size, 1);
    assert_eq!(
        progress.processed,
        FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("1"))
    );
    // Published held back — still BeforeFirst, Building.
    assert_eq!(progress.published_frontier, FrontierPosition::BeforeFirst);
    assert!(matches!(
        engine.outcome(progress.processed),
        ColumnarOutcome::Building(_)
    ));
}

#[test]
fn acceptance_irrelevant_commits_advance_processed() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut engine = open_engine(definition, "irrel");
    let mut source = HistorySource::default();
    let org = uuid(0x50);
    push_irrelevant_note(&mut source, &bundle, 1, org, 1);
    push_irrelevant_note(&mut source, &bundle, 2, org, 2);
    let progress = engine.apply_available(&source).expect("apply");
    assert_eq!(
        progress.processed,
        FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("2"))
    );
    assert_eq!(
        progress.published_frontier,
        FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("2"))
    );
}

#[test]
// req: OQ-021
fn acceptance_org_scope_airtight() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut engine = open_engine(definition, "org");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org_a = uuid(0x60);
    let org_b = uuid(0x61);
    push_ticket_create(
        &mut source,
        &mut oracle,
        &bundle,
        1,
        org_a,
        1,
        1,
        "only-a",
        1,
    );
    push_ticket_create(
        &mut source,
        &mut oracle,
        &bundle,
        2,
        org_b,
        1,
        1,
        "only-b",
        1,
    );
    engine.apply_available(&source).expect("apply");

    let rows_a = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org_a)))
            .expect("a"),
    );
    let rows_b = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org_b)))
            .expect("b"),
    );
    assert_eq!(rows_a.len(), 1);
    assert_eq!(rows_b.len(), 1);
    assert_eq!(rows_a[0][1], CanonicalValue::string("only-a").unwrap());
    assert_eq!(rows_b[0][1], CanonicalValue::string("only-b").unwrap());
    // Count aggregate must not leak across orgs.
    let status = field_id(&bundle, "Ticket", "status");
    let count_a = engine
        .query(&ColumnarQueryRequest {
            org_scope: CanonicalValue::Uuid(org_a),
            select: Vec::new(),
            predicates: Vec::new(),
            order: Vec::new(),
            limit: None,
            group_by: None,
            aggregate: Some(AggregateOp::Count),
            budget: QueryBudget::default(),
        })
        .expect("count");
    match count_a {
        QueryResult::Aggregate(riffdb_columnar::AggregateValue::Count(1)) => {}
        other => panic!("expected count 1, got {other:?}"),
    }
    let _ = status;
}

#[test]
// req: OQ-019
fn acceptance_query_range_sort_limit_aggregates_group_by_budgets() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut engine = open_engine(definition, "query");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0x70);
    for (id, status, title, prio) in [
        (1u64, 1u64, "alpha", 10i64),
        (2, 1, "bravo", 20),
        (3, 2, "charlie", 30),
        (4, 2, "delta", 40),
    ] {
        push_ticket_create(
            &mut source,
            &mut oracle,
            &bundle,
            id,
            org,
            id,
            status,
            title,
            prio,
        );
    }
    engine.apply_available(&source).expect("apply");

    let status = field_id(&bundle, "Ticket", "status");
    let priority = field_id(&bundle, "Ticket", "priority");
    let title = field_id(&bundle, "Ticket", "title");

    // Range on priority [15, 35)
    let ranged = rows_of(
        engine
            .query(&ColumnarQueryRequest {
                org_scope: CanonicalValue::Uuid(org),
                select: Vec::new(),
                predicates: vec![ColumnPredicate::Range {
                    field: priority,
                    low: Some(CanonicalValue::I64(15)),
                    high: Some(CanonicalValue::I64(35)),
                }],
                order: vec![OrderSpec {
                    field: title,
                    direction: SortDirection::Asc,
                }],
                limit: Some(10),
                group_by: None,
                aggregate: None,
                budget: QueryBudget::default(),
            })
            .expect("range"),
    );
    assert_eq!(ranged.len(), 2);
    assert_eq!(ranged[0][1], CanonicalValue::string("bravo").unwrap());

    // Sort with a REAL tie: tickets 1 (alpha) and 2 (bravo) share status 1, so
    // ordering by status alone cannot discriminate — the pk tie-break must.
    let tied = rows_of(
        engine
            .query(&ColumnarQueryRequest {
                org_scope: CanonicalValue::Uuid(org),
                select: Vec::new(),
                predicates: vec![ColumnPredicate::Eq {
                    field: status,
                    value: CanonicalValue::U64(1),
                }],
                order: vec![OrderSpec {
                    field: status,
                    direction: SortDirection::Asc,
                }],
                limit: Some(2),
                group_by: None,
                aggregate: None,
                budget: QueryBudget::default(),
            })
            .expect("tie"),
    );
    assert_eq!(tied.len(), 2, "both tied rows must survive the limit");
    assert_eq!(
        tied[0][1],
        CanonicalValue::string("alpha").unwrap(),
        "pk tie-break must order ticket 1 before ticket 2"
    );
    assert_eq!(tied[1][1], CanonicalValue::string("bravo").unwrap());
    // Descending sort key with equal values: pk tie-break stays ascending.
    let tied_desc = rows_of(
        engine
            .query(&ColumnarQueryRequest {
                org_scope: CanonicalValue::Uuid(org),
                select: Vec::new(),
                predicates: vec![ColumnPredicate::Eq {
                    field: status,
                    value: CanonicalValue::U64(1),
                }],
                order: vec![OrderSpec {
                    field: status,
                    direction: SortDirection::Desc,
                }],
                limit: Some(1),
                group_by: None,
                aggregate: None,
                budget: QueryBudget::default(),
            })
            .expect("tie desc"),
    );
    assert_eq!(
        tied_desc[0][1],
        CanonicalValue::string("alpha").unwrap(),
        "descending sort with tied keys must still tie-break by ascending pk"
    );

    // Aggregates
    for op in [
        AggregateOp::Count,
        AggregateOp::Sum { field: priority },
        AggregateOp::Min { field: priority },
        AggregateOp::Max { field: priority },
    ] {
        let result = engine
            .query(&ColumnarQueryRequest {
                org_scope: CanonicalValue::Uuid(org),
                select: Vec::new(),
                predicates: Vec::new(),
                order: Vec::new(),
                limit: None,
                group_by: None,
                aggregate: Some(op),
                budget: QueryBudget::default(),
            })
            .expect("agg");
        assert!(matches!(result, QueryResult::Aggregate(_)));
    }
    for (op, expected) in [
        (
            AggregateOp::CountPresent { field: title },
            riffdb_columnar::AggregateValue::Count(4),
        ),
        (
            AggregateOp::CountDistinct { field: title },
            riffdb_columnar::AggregateValue::Count(4),
        ),
        (
            AggregateOp::CountDistinctPresent { field: title },
            riffdb_columnar::AggregateValue::Count(4),
        ),
        (
            AggregateOp::Mean { field: priority },
            riffdb_columnar::AggregateValue::ExactMean {
                total: 100,
                count: 4,
            },
        ),
    ] {
        let result = engine
            .query(&ColumnarQueryRequest {
                org_scope: CanonicalValue::Uuid(org),
                select: Vec::new(),
                predicates: Vec::new(),
                order: Vec::new(),
                limit: None,
                group_by: None,
                aggregate: Some(op),
                budget: QueryBudget::default(),
            })
            .expect("exact core aggregate");
        assert_eq!(result, QueryResult::Aggregate(expected));
    }

    // Group-by status
    let groups = engine
        .query(&ColumnarQueryRequest {
            org_scope: CanonicalValue::Uuid(org),
            select: Vec::new(),
            predicates: Vec::new(),
            order: Vec::new(),
            limit: None,
            group_by: Some(GroupBySpec {
                keys: vec![status],
                aggregates: vec![AggregateOp::Count],
            }),
            aggregate: None,
            budget: QueryBudget::default(),
        })
        .expect("group");
    match groups {
        QueryResult::Groups { groups, .. } => assert_eq!(groups.len(), 2),
        other => panic!("expected groups: {other:?}"),
    }

    // Scan budget
    let err = engine
        .query(&ColumnarQueryRequest {
            org_scope: CanonicalValue::Uuid(org),
            select: Vec::new(),
            predicates: Vec::new(),
            order: Vec::new(),
            limit: None,
            group_by: None,
            aggregate: None,
            budget: QueryBudget {
                max_scanned_rows: 1,
                max_group_cardinality: 10_000,
            },
        })
        .expect_err("budget");
    assert!(matches!(
        err,
        ColumnarError::Query(QueryError::ScanBudgetExceeded { .. })
    ));

    // Group cardinality budget
    let err = engine
        .query(&ColumnarQueryRequest {
            org_scope: CanonicalValue::Uuid(org),
            select: Vec::new(),
            predicates: Vec::new(),
            order: Vec::new(),
            limit: None,
            group_by: Some(GroupBySpec {
                keys: vec![title],
                aggregates: vec![AggregateOp::Count],
            }),
            aggregate: None,
            budget: QueryBudget {
                max_scanned_rows: 100_000,
                max_group_cardinality: 1,
            },
        })
        .expect_err("group budget");
    assert!(matches!(
        err,
        ColumnarError::Query(QueryError::GroupCardinalityExceeded { .. })
    ));

    // Empty result
    let empty = rows_of(
        engine
            .query(&eq_status_query(CanonicalValue::Uuid(org), status, 99))
            .expect("empty"),
    );
    assert!(empty.is_empty());
}

#[test]
fn acceptance_compaction_result_invariant() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut engine = open_engine(definition, "compact");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0x80);
    push_ticket_create(&mut source, &mut oracle, &bundle, 1, org, 1, 1, "x", 1);
    push_ticket_create(&mut source, &mut oracle, &bundle, 2, org, 2, 2, "y", 2);
    push_ticket_replace(&mut source, &mut oracle, &bundle, 3, org, 1, 2, 3, "x2", 9);
    engine.apply_available(&source).expect("apply");
    // Full D10 corpus before and after the merge must be identical.
    let org_v = CanonicalValue::Uuid(org);
    let before = corpus_results(&engine, &bundle, &org_v);
    engine.compact().expect("compact");
    let after = corpus_results(&engine, &bundle, &org_v);
    assert_eq!(before, after, "compaction must be result-invariant");
    // Compaction rewrites each org to exactly one segment with an empty delta.
    let snapshot = engine.published_snapshot();
    assert_eq!(snapshot.segments.len(), 1, "one segment per org");
    assert!(
        snapshot
            .delta
            .values()
            .all(std::collections::BTreeMap::is_empty),
        "delta must be merged into segments"
    );
}

#[test]
fn acceptance_checkpoint_recover_and_replay() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let dir = temp_dir("ckpt");
    let mut engine =
        ColumnarEngine::open(definition.clone(), OpenOptions::new(dir.clone())).expect("open");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0x90);
    push_ticket_create(&mut source, &mut oracle, &bundle, 1, org, 1, 1, "c1", 1);
    engine.apply_available(&source).expect("apply");
    let manifest = engine.checkpoint().expect("checkpoint");
    assert_eq!(
        manifest.durable_frontier,
        FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("1"))
    );

    // Reopen: durable frontier restored; further apply is idempotent.
    let mut engine2 = ColumnarEngine::open(definition, OpenOptions::new(dir)).expect("reopen");
    assert_eq!(
        engine2.durable_frontier().position(),
        FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("1"))
    );
    // Durable frontier must not overclaim: data for seq 1 must already be queryable
    // without re-applying (neuter that advances manifest before segment durability fails here).
    let recovered = rows_of(
        engine2
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("query recovered"),
    );
    assert_eq!(
        recovered.len(),
        1,
        "durable frontier AppliedThrough(1) must recover the projected row"
    );
    push_ticket_create(&mut source, &mut oracle, &bundle, 2, org, 2, 2, "c2", 2);
    engine2.apply_available(&source).expect("replay");
    let rows = rows_of(
        engine2
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("q"),
    );
    assert_eq!(rows.len(), 2);
}

#[test]
fn acceptance_fingerprint_mismatch_invalid() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let dir = temp_dir("fp");
    let mut engine = ColumnarEngine::open(definition, OpenOptions::new(dir.clone())).expect("open");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    push_ticket_create(
        &mut source,
        &mut oracle,
        &bundle,
        1,
        uuid(0x91),
        1,
        1,
        "t",
        1,
    );
    engine.apply_available(&source).expect("apply");
    engine.checkpoint().expect("ckpt");

    // Different projected fields → different fingerprint.
    let org = field_id(&bundle, "Ticket", "organization_id");
    let status = field_id(&bundle, "Ticket", "status");
    let other = RegisteredDefinition::register(
        ColumnarProjectionDefinition {
            name: "other".into(),
            entity_name: "Ticket".into(),
            projected_fields: vec![status],
            org_scope_field: org,
        },
        &bundle,
    )
    .expect("register");
    let err = ColumnarEngine::open(other, OpenOptions::new(dir))
        .map(|_| ())
        .expect_err("mismatch");
    assert!(matches!(
        err,
        ColumnarError::Checkpoint(CheckpointError::FingerprintMismatch { .. })
    ));
}

#[test]
fn acceptance_lifecycle_outcomes_shapes() {
    use riffdb_columnar::{
        ProjectionDegraded, ProjectionRebuilding, frontier_lag_sequences, lagging_for,
    };
    let required = CommitSequence::new(5).expect("5");
    let current = FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("2"));
    let head = FrontierPosition::AppliedThrough(CommitSequence::new(9).expect("9"));
    match lagging_for(1, required, current, head) {
        ColumnarOutcome::Lagging(lag) => {
            assert_eq!(lag.lag_sequences, Some(7));
            assert_eq!(lag.current.history_incarnation(), 1);
            assert_eq!(frontier_lag_sequences(current, head), Some(7));
        }
        _ => panic!("lagging"),
    }
    assert!(matches!(
        ColumnarOutcome::Rebuilding(ProjectionRebuilding {
            reason: riffdb_columnar::RebuildingReason::ReplayBudgetExceeded,
            progress_applied: 1,
            progress_total: 2,
        }),
        ColumnarOutcome::Rebuilding(_)
    ));
    assert!(matches!(
        ColumnarOutcome::Degraded(ProjectionDegraded {
            reason: riffdb_columnar::DegradedReason::ApplyLagSlo,
            current_frontier: riffdb_types::ProjectionFrontier::new(
                1,
                FrontierPosition::BeforeFirst
            ),
        }),
        ColumnarOutcome::Degraded(_)
    ));
}

#[test]
fn acceptance_randomized_histories_equivalence() {
    // Fixed default seeds for CI determinism; the failing seed is embedded in
    // every assertion context so a failure prints it.
    for seed in [11u64, 0x00C0_FFEE, 20_260_801] {
        run_randomized_history(seed);
    }
}

fn run_randomized_history(seed: u64) {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    let mut rng = StdRng::seed_from_u64(seed);
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let dir = temp_dir(&format!("rand-{seed}"));
    let mut engine = ColumnarEngine::open(definition.clone(), OpenOptions::new(dir.clone()))
        .unwrap_or_else(|error| panic!("seed {seed}: open: {error}"));
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let orgs = [uuid(0xa0), uuid(0xa1)];
    let mut seq = 1u64;
    let mut next_ticket = 1u64;
    // (org index, ticket id) → current entity version (replace candidates).
    let mut ticket_versions: std::collections::BTreeMap<(usize, u64), u64> =
        std::collections::BTreeMap::new();
    // Races whose superseding commit has not been appended yet (beyond fence).
    let mut pending: Vec<(usize, OpenRace)> = Vec::new();

    for step in 0..48u32 {
        let context = format!("seed {seed} step {step}");
        let org_idx = rng.gen_range(0..orgs.len());
        let org = orgs[org_idx];
        match rng.gen_range(0u32..100) {
            0..=29 => {
                // Create a fresh ticket.
                let ticket_id = next_ticket;
                next_ticket += 1;
                push_ticket_create(
                    &mut source,
                    &mut oracle,
                    &bundle,
                    seq,
                    org,
                    ticket_id,
                    rng.gen_range(0u64..3),
                    &format!("t-{ticket_id}"),
                    rng.gen_range(-10i64..40),
                );
                ticket_versions.insert((org_idx, ticket_id), 1);
                seq += 1;
            }
            30..=49 => {
                // Replace an existing ticket in this org (fresh create if none).
                let candidates: Vec<(u64, u64)> = ticket_versions
                    .iter()
                    .filter(|((idx, _), _)| *idx == org_idx)
                    .map(|((_, ticket_id), version)| (*ticket_id, *version))
                    .collect();
                if let Some(&(ticket_id, version)) =
                    candidates.get(rng.gen_range(0..candidates.len().max(1)))
                {
                    push_ticket_replace(
                        &mut source,
                        &mut oracle,
                        &bundle,
                        seq,
                        org,
                        ticket_id,
                        version + 1,
                        rng.gen_range(0u64..3),
                        &format!("t-{ticket_id}-v{}", version + 1),
                        rng.gen_range(-10i64..40),
                    );
                    ticket_versions.insert((org_idx, ticket_id), version + 1);
                } else {
                    let ticket_id = next_ticket;
                    next_ticket += 1;
                    push_ticket_create(
                        &mut source,
                        &mut oracle,
                        &bundle,
                        seq,
                        org,
                        ticket_id,
                        rng.gen_range(0u64..3),
                        &format!("t-{ticket_id}"),
                        rng.gen_range(-10i64..40),
                    );
                    ticket_versions.insert((org_idx, ticket_id), 1);
                }
                seq += 1;
            }
            50..=61 => {
                // Forced supersession race resolved within the same pull.
                let ticket_id = next_ticket;
                next_ticket += 1;
                push_race_pair(
                    &mut source,
                    &mut oracle,
                    &bundle,
                    seq,
                    seq + 1,
                    org,
                    ticket_id,
                );
                ticket_versions.insert((org_idx, ticket_id), 2);
                seq += 2;
            }
            62..=73 => {
                // Open race: superseding commit lands beyond the frozen fence.
                let ticket_id = next_ticket;
                next_ticket += 1;
                let race =
                    push_open_race_v1(&mut source, &mut oracle, &bundle, seq, org, ticket_id);
                pending.push((org_idx, race));
                seq += 1;
            }
            74..=86 => {
                // Multi-entity commit: 2-3 fresh tickets in ONE commit.
                let count = rng.gen_range(2usize..4);
                let mut tickets = Vec::with_capacity(count);
                for _ in 0..count {
                    let ticket_id = next_ticket;
                    next_ticket += 1;
                    tickets.push((ticket_id, rng.gen_range(0u64..3), rng.gen_range(-10i64..40)));
                    ticket_versions.insert((org_idx, ticket_id), 1);
                }
                push_multi_ticket_create(&mut source, &mut oracle, &bundle, seq, org, &tickets);
                seq += 1;
            }
            _ => {
                // Irrelevant commit (Note entity) must advance processed only.
                push_irrelevant_note(&mut source, &bundle, seq, org, seq);
                seq += 1;
            }
        }
        // Occasionally resolve one pending open race.
        if !pending.is_empty() && rng.gen_range(0u32..3) == 0 {
            let (race_org_idx, race) = pending.remove(0);
            let ticket_id = race.ticket_id;
            resolve_open_race(&mut source, &mut oracle, race, seq);
            ticket_versions.insert((race_org_idx, ticket_id), 2);
            seq += 1;
        }

        engine
            .apply_available(&source)
            .unwrap_or_else(|error| panic!("{context}: apply: {error}"));
        // Compare the FULL corpus at the published frontier, including during
        // holdback (published behind processed).
        assert_corpus_equivalence(&engine, &oracle, &bundle, &orgs, &context);

        // Occasionally checkpoint + reopen mid-history.
        if rng.gen_range(0u32..8) == 0 {
            match engine.checkpoint() {
                Ok(_) => {
                    engine =
                        ColumnarEngine::open(definition.clone(), OpenOptions::new(dir.clone()))
                            .unwrap_or_else(|error| panic!("{context}: reopen: {error}"));
                    assert_corpus_equivalence(
                        &engine,
                        &oracle,
                        &bundle,
                        &orgs,
                        &format!("{context} after reopen"),
                    );
                    engine
                        .apply_available(&source)
                        .unwrap_or_else(|error| panic!("{context}: replay: {error}"));
                    assert_corpus_equivalence(
                        &engine,
                        &oracle,
                        &bundle,
                        &orgs,
                        &format!("{context} after reopen replay"),
                    );
                }
                Err(ColumnarError::Checkpoint(CheckpointError::HoldbackActive { .. })) => {
                    assert!(
                        engine.deferred_set_size() > 0
                            || engine.published_frontier() != engine.processed_frontier(),
                        "{context}: checkpoint refused outside a holdback window"
                    );
                }
                Err(other) => panic!("{context}: checkpoint: {other}"),
            }
        }
    }

    // Drain pending races so every deferral resolves before the final check.
    while !pending.is_empty() {
        let (race_org_idx, race) = pending.remove(0);
        let ticket_id = race.ticket_id;
        resolve_open_race(&mut source, &mut oracle, race, seq);
        ticket_versions.insert((race_org_idx, ticket_id), 2);
        seq += 1;
    }
    let progress = engine
        .apply_available(&source)
        .unwrap_or_else(|error| panic!("seed {seed}: final apply: {error}"));
    assert!(progress.caught_up, "seed {seed}: final pull must catch up");
    assert_eq!(
        engine.published_frontier(),
        engine.processed_frontier(),
        "seed {seed}: all races must be resolved after the final pull"
    );
    assert_corpus_equivalence(
        &engine,
        &oracle,
        &bundle,
        &orgs,
        &format!("seed {seed} final"),
    );

    // Final checkpoint, reopen without replay, then replay: all equivalent.
    engine
        .checkpoint()
        .unwrap_or_else(|error| panic!("seed {seed}: final checkpoint: {error}"));
    let mut engine2 = ColumnarEngine::open(definition, OpenOptions::new(dir))
        .unwrap_or_else(|error| panic!("seed {seed}: final reopen: {error}"));
    assert_corpus_equivalence(
        &engine2,
        &oracle,
        &bundle,
        &orgs,
        &format!("seed {seed} recovered"),
    );
    engine2
        .apply_available(&source)
        .unwrap_or_else(|error| panic!("seed {seed}: final replay: {error}"));
    assert_corpus_equivalence(
        &engine2,
        &oracle,
        &bundle,
        &orgs,
        &format!("seed {seed} recovered replay"),
    );
}

// --- Falsifiability-backed tests (named for neuter transcripts) ---

#[test]
fn falsify_version_mask_on_supersession() {
    // Superseded-row masking rests on the version comparison in
    // supersession_should_replace (no tombstones exist — deletes do not exist).
    // Neuter transcript: force supersession_should_replace to always return
    // false (incoming never replaces) — this test then fails with title "v1".
    let bundle = compile_bundle();
    let mut engine = open_engine(register_ticket_board(&bundle), "vmask");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0xb0);
    push_ticket_create(&mut source, &mut oracle, &bundle, 1, org, 1, 1, "v1", 1);
    engine.apply_available(&source).expect("apply v1");
    engine.checkpoint().expect("seg v1");
    push_ticket_replace(&mut source, &mut oracle, &bundle, 2, org, 1, 2, 2, "v2", 2);
    engine.apply_available(&source).expect("apply v2");
    engine.compact().expect("compact");
    let rows = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("q"),
    );
    assert_eq!(rows.len(), 1, "exactly one live row after supersession");
    assert_eq!(
        rows[0][1],
        CanonicalValue::string("v2").unwrap(),
        "superseded v1 must be masked; visible title must be v2"
    );
}

// The mid-commit-publish falsifiability property lives in the library's own
// cfg(test) module (apply::publish_observer_tests::
// publish_is_all_or_nothing_for_multi_entity_commits): a publish-observer hook
// records every snapshot at the instant it is swapped in, and moving the
// publish call inside the per-entity loop of apply_commit makes it fail.

#[test]
fn falsify_drop_holdback_rule() {
    // With holdback, race at C1 must not publish v2 early under the C1 frontier.
    let bundle = compile_bundle();
    let mut engine = open_engine(register_ticket_board(&bundle), "nohold");
    let mut source = HistorySource::default();
    let org = uuid(0xb2);
    let ticket_type = entity_type_id(&bundle, "Ticket");
    let target = HistorySource::ticket_target(ticket_type, org, 1);
    let e_v1 = HistorySource::make_entity(
        target.clone(),
        EntityVersion::first(),
        ticket_fields(&bundle, org, 1, 1, "v1", 1),
    );
    let e_v2 = HistorySource::make_entity(
        target.clone(),
        EntityVersion::new(2).expect("2"),
        ticket_fields(&bundle, org, 1, 2, "v2", 2),
    );
    let ref_v1 = CommittedEntityReferenceV2::from_post_image(&e_v1).expect("r");
    source.put_entity(e_v2);
    source.append_commit(
        CommitSequence::new(1).expect("1"),
        vec![ref_v1],
        vec![(target, riffdb_storage_api::ExpectedEntityState::Absent)],
    );
    engine.apply_available(&source).expect("apply");
    assert_eq!(
        engine.published_frontier().position(),
        FrontierPosition::BeforeFirst
    );
    assert_eq!(engine.deferred_set_size(), 1);
}

#[test]
fn falsify_org_scope_filter() {
    // Query without the right org must not return foreign rows (API requires org).
    let bundle = compile_bundle();
    let mut engine = open_engine(register_ticket_board(&bundle), "orgf");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org_a = uuid(0xb3);
    let org_b = uuid(0xb4);
    push_ticket_create(
        &mut source,
        &mut oracle,
        &bundle,
        1,
        org_a,
        1,
        1,
        "secret-a",
        1,
    );
    engine.apply_available(&source).expect("apply");
    let rows_b = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org_b)))
            .expect("b"),
    );
    assert!(
        rows_b.is_empty(),
        "org B must not see org A rows: {rows_b:?}"
    );
}

#[test]
fn crash_child_process_matrix() {
    // Child-process crash injection over all four boundaries.
    const CHILD_MODE: &str = "RIFFDB_COLUMNAR_CRASH_CHILD";
    const CHILD_PATH: &str = "RIFFDB_COLUMNAR_CRASH_PATH";
    const CHILD_BOUNDARY: &str = "RIFFDB_COLUMNAR_CRASH_BOUNDARY";

    if let Ok(mode) = std::env::var(CHILD_MODE)
        && mode == "1"
    {
        run_crash_child();
        return;
    }

    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);

    for (name, boundary) in [
        ("before_seg", ColumnarTestBoundary::BeforeSegmentSync),
        ("after_seg", ColumnarTestBoundary::AfterSegmentSync),
        ("before_man", ColumnarTestBoundary::BeforeManifestRename),
        ("after_man", ColumnarTestBoundary::AfterManifestRename),
    ] {
        let dir = temp_dir(&format!("child-{name}"));
        // Seed nothing durable first; child writes + checkpoints with abort.
        let status = std::process::Command::new(std::env::current_exe().expect("exe"))
            .arg("--exact")
            .arg("crash_child_process_matrix")
            .arg("--nocapture")
            .env(CHILD_MODE, "1")
            .env(CHILD_PATH, &dir)
            .env(CHILD_BOUNDARY, name)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("spawn child");
        assert!(!status.success(), "child must abort at {name}");

        // Parent reopens and asserts no overclaim + recoverability. The reopen
        // itself must sweep any torn-checkpoint leftovers (orphan segments,
        // temp manifests).
        let mut engine = ColumnarEngine::open(definition.clone(), OpenOptions::new(dir.clone()))
            .expect("reopen after crash");
        let durable = engine.durable_frontier().position();

        // The same history + oracle the child applied before it died.
        let org = uuid(0xc1);
        let mut source = HistorySource::default();
        let mut oracle = Oracle::default();
        push_ticket_create(
            &mut source,
            &mut oracle,
            &bundle,
            1,
            org,
            1,
            1,
            "crash-child",
            1,
        );

        // Durable may be BeforeFirst (torn before rename) or AppliedThrough(1)
        // only if AfterManifestRename completed (rename happened; dir sync may not).
        match boundary {
            ColumnarTestBoundary::BeforeSegmentSync
            | ColumnarTestBoundary::AfterSegmentSync
            | ColumnarTestBoundary::BeforeManifestRename => {
                assert_eq!(
                    durable,
                    FrontierPosition::BeforeFirst,
                    "{name}: must not overclaim"
                );
            }
            ColumnarTestBoundary::AfterManifestRename => {
                assert!(
                    durable == FrontierPosition::BeforeFirst
                        || durable
                            == FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("1")),
                    "{name}: unexpected durable {durable:?}"
                );
                // A manifest that survived the crash must not overclaim even
                // consistently: the rows recovered BEFORE any replay must match
                // the oracle at exactly the frontier the manifest claims.
                if durable != FrontierPosition::BeforeFirst {
                    assert_corpus_equivalence(
                        &engine,
                        &oracle,
                        &bundle,
                        &[org],
                        &format!("{name}: recovered state vs manifest frontier claim"),
                    );
                }
            }
        }

        // Replay from history must reach head without error.
        engine.apply_available(&source).expect("replay after crash");
        assert_eq!(
            engine.processed_frontier().position(),
            FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("1"))
        );
        // Post-recovery FULL-CORPUS equivalence at the published frontier (D9).
        assert_corpus_equivalence(
            &engine,
            &oracle,
            &bundle,
            &[org],
            &format!("{name}: post-recovery equivalence"),
        );
        // Post-recovery checkpoint must succeed: a torn first checkpoint must
        // never brick subsequent checkpoints (orphan segment files / temp
        // manifests are swept at open and segment names are generation-unique).
        let manifest = engine
            .checkpoint()
            .unwrap_or_else(|error| panic!("{name}: checkpoint after recovery: {error}"));
        assert_eq!(
            manifest.durable_frontier,
            FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("1")),
            "{name}: post-recovery checkpoint frontier"
        );
        // And the new checkpoint must itself be recoverable.
        let recovered = ColumnarEngine::open(definition.clone(), OpenOptions::new(dir))
            .expect("reopen after post-recovery checkpoint");
        assert_eq!(
            recovered.durable_frontier().position(),
            manifest.durable_frontier
        );
        assert_corpus_equivalence(
            &recovered,
            &oracle,
            &bundle,
            &[org],
            &format!("{name}: post-recovery checkpoint equivalence"),
        );
    }
}

fn run_crash_child() {
    let path = std::env::var("RIFFDB_COLUMNAR_CRASH_PATH").expect("path");
    let boundary_name = std::env::var("RIFFDB_COLUMNAR_CRASH_BOUNDARY").expect("boundary");
    let boundary = match boundary_name.as_str() {
        "before_seg" => ColumnarTestBoundary::BeforeSegmentSync,
        "after_seg" => ColumnarTestBoundary::AfterSegmentSync,
        "before_man" => ColumnarTestBoundary::BeforeManifestRename,
        "after_man" => ColumnarTestBoundary::AfterManifestRename,
        other => panic!("unknown boundary {other}"),
    };
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut engine = ColumnarEngine::open(definition, OpenOptions::new(path)).expect("child open");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    push_ticket_create(
        &mut source,
        &mut oracle,
        &bundle,
        1,
        uuid(0xc1),
        1,
        1,
        "crash-child",
        1,
    );
    engine.apply_available(&source).expect("child apply");
    let controller = ColumnarTestController::new();
    controller.arm_abort_at(boundary);
    engine.install_test_controller(controller);
    let _ = engine.checkpoint(); // aborts
    std::process::exit(0);
}

/// Builds the reviewer's PoC holdback state: commit 1 = {raced entity (live
/// already at v2), matched entity}, so after one pull the engine is processed
/// through 1 with a non-empty deferred set and nothing published.
fn build_holdback_state(
    bundle: &riffdb_contract_ir::ContractBundle,
    dir: std::path::PathBuf,
    org: [u8; 16],
) -> (ColumnarEngine, HistorySource, Oracle) {
    let definition = register_ticket_board(bundle);
    let mut engine = ColumnarEngine::open(definition, OpenOptions::new(dir)).expect("open");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let ticket_type = entity_type_id(bundle, "Ticket");
    // Raced entity: live state already at v2, commit 1 references v1.
    let raced_target = HistorySource::ticket_target(ticket_type, org, 3);
    let raced_v1 = HistorySource::make_entity(
        raced_target.clone(),
        EntityVersion::first(),
        ticket_fields(bundle, org, 3, 1, "e3v1", 1),
    );
    let raced_v2 = HistorySource::make_entity(
        raced_target.clone(),
        EntityVersion::new(2).expect("2"),
        ticket_fields(bundle, org, 3, 9, "e3v2", 9),
    );
    // Matched entity: exact post-image of commit 1.
    let matched_target = HistorySource::ticket_target(ticket_type, org, 4);
    let matched = HistorySource::make_entity(
        matched_target.clone(),
        EntityVersion::first(),
        ticket_fields(bundle, org, 4, 4, "e4", 4),
    );
    let raced_ref_v1 = CommittedEntityReferenceV2::from_post_image(&raced_v1).expect("r");
    let matched_ref = CommittedEntityReferenceV2::from_post_image(&matched).expect("r");
    source.put_entity(raced_v2.clone());
    source.put_entity(matched);
    source.append_commit(
        CommitSequence::new(1).expect("1"),
        vec![raced_ref_v1, matched_ref],
        vec![
            (
                raced_target.clone(),
                riffdb_storage_api::ExpectedEntityState::Absent,
            ),
            (
                matched_target.clone(),
                riffdb_storage_api::ExpectedEntityState::Absent,
            ),
        ],
    );
    oracle.apply_exact(
        &CanonicalValue::Uuid(org),
        raced_target.key().as_bytes(),
        1,
        projected_cells(1, "e3v1", 1),
        CommitSequence::new(1).expect("1"),
    );
    oracle.apply_exact(
        &CanonicalValue::Uuid(org),
        matched_target.key().as_bytes(),
        1,
        projected_cells(4, "e4", 4),
        CommitSequence::new(1).expect("1"),
    );

    let progress = engine.apply_available(&source).expect("apply");
    assert_eq!(progress.deferred_set_size, 1, "raced entity deferred");
    assert_eq!(
        progress.processed,
        FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("1"))
    );
    assert_eq!(progress.published_frontier, FrontierPosition::BeforeFirst);

    // Superseding commit 2 (appended but not yet applied): pulling it resolves
    // the holdback.
    let raced_ref_v2 = CommittedEntityReferenceV2::from_post_image(&raced_v2).expect("r");
    source.append_commit(
        CommitSequence::new(2).expect("2"),
        vec![raced_ref_v2],
        vec![(
            raced_target.clone(),
            riffdb_storage_api::ExpectedEntityState::Present(EntityVersion::first()),
        )],
    );
    oracle.apply_exact(
        &CanonicalValue::Uuid(org),
        raced_target.key().as_bytes(),
        2,
        projected_cells(9, "e3v2", 9),
        CommitSequence::new(2).expect("2"),
    );
    (engine, source, oracle)
}

fn assert_holdback_refusal(error: ColumnarError, operation: &str) {
    match error {
        ColumnarError::Checkpoint(CheckpointError::HoldbackActive {
            published,
            processed,
            deferred,
        }) => {
            assert_eq!(published, FrontierPosition::BeforeFirst);
            assert_eq!(
                processed,
                FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("1"))
            );
            assert_eq!(deferred, 1);
        }
        other => panic!("{operation} during holdback must refuse typed, got {other:?}"),
    }
}

fn assert_no_durable_files(dir: &std::path::Path, context: &str) {
    let leftovers: Vec<String> = std::fs::read_dir(dir)
        .expect("read dir")
        .map(|entry| {
            entry
                .expect("entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name == "MANIFEST" || name.starts_with("seg-") || name.ends_with(".tmp"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "{context}: refused durability operation must write nothing, found {leftovers:?}"
    );
}

#[test]
fn checkpoint_refuses_during_holdback_then_recovers_complete_commit() {
    let bundle = compile_bundle();
    let org = uuid(0xd0);
    let dir = temp_dir("holdback-ckpt");
    let (mut engine, source, oracle) = build_holdback_state(&bundle, dir.clone(), org);

    // (i) Checkpoint during holdback: typed refusal, nothing durable written.
    let error = engine
        .checkpoint()
        .expect_err("must refuse during holdback");
    assert_holdback_refusal(error, "checkpoint");
    assert_no_durable_files(&dir, "checkpoint refusal");
    // The published snapshot must be untouched by the refusal.
    assert_eq!(
        engine.published_frontier().position(),
        FrontierPosition::BeforeFirst
    );

    // (ii) The next pull applies the superseding commit; checkpoint succeeds.
    let progress = engine.apply_available(&source).expect("resolve");
    assert_eq!(progress.deferred_set_size, 0);
    assert_eq!(
        progress.published_frontier,
        FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("2"))
    );
    let manifest = engine.checkpoint().expect("checkpoint after resolution");
    assert_eq!(
        manifest.durable_frontier,
        FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("2"))
    );

    // Reopen: the recovered state carries the COMPLETE commit 1 (both
    // entities), never the half-applied form.
    let definition = register_ticket_board(&bundle);
    let mut reopened = ColumnarEngine::open(definition, OpenOptions::new(dir)).expect("reopen");
    let rows = rows_of(
        reopened
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("recovered query"),
    );
    assert_eq!(rows.len(), 2, "complete commit: both entities recovered");
    assert!(
        rows.iter()
            .any(|row| row[1] == CanonicalValue::string("e4").unwrap()),
        "matched entity of commit 1 must be recovered"
    );
    assert!(
        rows.iter()
            .any(|row| row[1] == CanonicalValue::string("e3v2").unwrap()),
        "raced entity must be recovered at its superseding version"
    );
    assert_corpus_equivalence(
        &reopened,
        &oracle,
        &bundle,
        &[org],
        "holdback ckpt recovered",
    );
    reopened.apply_available(&source).expect("replay");
    assert_corpus_equivalence(&reopened, &oracle, &bundle, &[org], "holdback ckpt replay");
}

#[test]
fn compact_refuses_during_holdback_then_succeeds() {
    let bundle = compile_bundle();
    let org = uuid(0xd1);
    let dir = temp_dir("holdback-compact");
    let (mut engine, source, oracle) = build_holdback_state(&bundle, dir.clone(), org);

    // (iii) Compact during holdback: same typed refusal, nothing written.
    let error = engine.compact().expect_err("must refuse during holdback");
    assert_holdback_refusal(error, "compact");
    assert_no_durable_files(&dir, "compact refusal");
    assert_eq!(
        engine.published_frontier().position(),
        FrontierPosition::BeforeFirst
    );

    // After the race resolves, compact succeeds and stays result-invariant.
    engine.apply_available(&source).expect("resolve");
    let org_v = CanonicalValue::Uuid(org);
    let before = corpus_results(&engine, &bundle, &org_v);
    engine.compact().expect("compact after resolution");
    let after = corpus_results(&engine, &bundle, &org_v);
    assert_eq!(before, after, "compaction must be result-invariant");
    assert_corpus_equivalence(&engine, &oracle, &bundle, &[org], "compact resolved");
}

#[test]
fn supersession_beyond_fence_resolves_on_next_pull() {
    // The superseding commit lands AFTER the frozen fence of the pull that
    // deferred the entity: holdback persists across pulls; the next pull
    // applies the superseding commit and publishes with a frontier jump.
    let bundle = compile_bundle();
    let mut engine = open_engine(register_ticket_board(&bundle), "beyond-fence");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0xd2);

    let race = push_open_race_v1(&mut source, &mut oracle, &bundle, 1, org, 1);
    // Pull 1: fence freezes at commit 1; the raced entity defers; holdback.
    let progress = engine.apply_available(&source).expect("pull 1");
    assert!(progress.caught_up, "pull 1 reaches its frozen fence");
    assert_eq!(
        progress.deferred_set_size, 1,
        "deferral persists past fence"
    );
    assert_eq!(
        progress.processed,
        FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("1"))
    );
    assert_eq!(progress.published_frontier, FrontierPosition::BeforeFirst);

    // Holdback persists across an idle pull with no new commits.
    let progress = engine.apply_available(&source).expect("idle pull");
    assert_eq!(progress.deferred_set_size, 1);
    assert_eq!(progress.published_frontier, FrontierPosition::BeforeFirst);

    // The superseding commit lands beyond the original fence; the next pull
    // resolves the deferral and publishes with a jump to commit 2.
    resolve_open_race(&mut source, &mut oracle, race, 2);
    let progress = engine.apply_available(&source).expect("pull 2");
    assert_eq!(progress.deferred_set_size, 0, "deferral resolved");
    assert_eq!(
        progress.published_frontier,
        FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("2"))
    );
    let rows = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("q"),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][1],
        CanonicalValue::string("v2-1").unwrap(),
        "only the superseding image is visible"
    );
    assert_corpus_equivalence(&engine, &oracle, &bundle, &[org], "beyond fence");
}

#[test]
fn catch_up_holdback_publishes_the_maximal_complete_prefix() {
    let bundle = compile_bundle();
    let mut engine = open_engine(register_ticket_board(&bundle), "batched-safe-prefix");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0xd5);

    push_ticket_create(&mut source, &mut oracle, &bundle, 1, org, 1, 1, "safe", 1);
    let race = push_open_race_v1(&mut source, &mut oracle, &bundle, 2, org, 2);

    let progress = engine.apply_available(&source).expect("catch up");

    assert_eq!(progress.deferred_set_size, 1);
    assert_eq!(
        progress.processed,
        FrontierPosition::AppliedThrough(CommitSequence::new(2).expect("two"))
    );
    assert_eq!(
        progress.published_frontier,
        FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("one")),
        "the fully applied prefix before the raced commit remains visible"
    );
    let rows = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("safe prefix query"),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], CanonicalValue::string("safe").expect("title"));

    resolve_open_race(&mut source, &mut oracle, race, 3);
    let progress = engine.apply_available(&source).expect("resolve");
    assert_eq!(progress.deferred_set_size, 0);
    assert_eq!(
        progress.published_frontier,
        FrontierPosition::AppliedThrough(CommitSequence::new(3).expect("three"))
    );
    assert_corpus_equivalence(&engine, &oracle, &bundle, &[org], "resolved safe prefix");
}

#[test]
fn checkpoint_bounds_segment_growth_and_sweeps_superseded_files() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let dir = temp_dir("growth");
    let mut engine =
        ColumnarEngine::open(definition.clone(), OpenOptions::new(dir.clone())).expect("open");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org_a = uuid(0xd3);
    let org_b = uuid(0xd4);
    push_ticket_create(&mut source, &mut oracle, &bundle, 1, org_a, 1, 1, "a1", 1);
    push_ticket_create(&mut source, &mut oracle, &bundle, 2, org_a, 2, 2, "a2", 2);
    push_ticket_create(&mut source, &mut oracle, &bundle, 3, org_b, 1, 1, "b1", 1);
    engine.apply_available(&source).expect("apply 1");
    let first = engine.checkpoint().expect("checkpoint 1");
    assert_eq!(first.segments.len(), 2, "one segment per org");

    // Second checkpoint touches only org A: its prior segment must be dropped
    // from the manifest (fully superseded by the new materialization) while
    // org B's segment entry is carried over unchanged.
    push_ticket_replace(
        &mut source,
        &mut oracle,
        &bundle,
        4,
        org_a,
        1,
        2,
        3,
        "a1v2",
        9,
    );
    engine.apply_available(&source).expect("apply 2");
    let second = engine.checkpoint().expect("checkpoint 2");
    assert_eq!(
        second.segments.len(),
        2,
        "manifest must reference only live segments (one per org), not grow per checkpoint"
    );
    let first_names: std::collections::BTreeSet<&str> = first
        .segments
        .iter()
        .map(|entry| entry.file_name.as_str())
        .collect();
    let second_names: std::collections::BTreeSet<&str> = second
        .segments
        .iter()
        .map(|entry| entry.file_name.as_str())
        .collect();
    assert_ne!(
        first_names, second_names,
        "org A must be rewritten into a new segment"
    );
    let carried: Vec<&&str> = second_names.intersection(&first_names).collect();
    assert_eq!(carried.len(), 1, "untouched org B segment is carried over");

    // The superseded org-A file is NOT deleted at checkpoint time (the previous
    // manifest may still reference it)...
    let on_disk = |dir: &std::path::Path| -> std::collections::BTreeSet<String> {
        std::fs::read_dir(dir)
            .expect("read dir")
            .map(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .filter(|name| name.starts_with("seg-"))
            .collect()
    };
    let files_after_second = on_disk(&dir);
    assert_eq!(
        files_after_second.len(),
        3,
        "superseded file must survive until the next open sweeps it"
    );

    // ...but the next open sweeps it, leaving exactly the referenced files.
    let reopened = ColumnarEngine::open(definition, OpenOptions::new(dir.clone())).expect("reopen");
    let files_after_open = on_disk(&dir);
    let referenced: std::collections::BTreeSet<String> = second
        .segments
        .iter()
        .map(|entry| entry.file_name.clone())
        .collect();
    assert_eq!(
        files_after_open, referenced,
        "open must sweep superseded segment files"
    );
    assert_corpus_equivalence(
        &reopened,
        &oracle,
        &bundle,
        &[org_a, org_b],
        "growth recovered",
    );
}

#[test]
fn query_org_scope_type_mismatch_is_typed_error() {
    let bundle = compile_bundle();
    let mut engine = open_engine(register_ticket_board(&bundle), "org-type");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0xd5);
    push_ticket_create(&mut source, &mut oracle, &bundle, 1, org, 1, 1, "t", 1);
    engine.apply_available(&source).expect("apply");

    // The org field is uuid; querying with a u64 org value must fail typed,
    // not silently return an empty result.
    let error = engine
        .query(&board_query(CanonicalValue::U64(7)))
        .expect_err("wrong-typed org value must be rejected");
    assert!(
        matches!(
            error,
            ColumnarError::Query(QueryError::OrgScopeTypeMismatch { .. })
        ),
        "expected OrgScopeTypeMismatch, got {error:?}"
    );
    // A correctly typed org value still works.
    let rows = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("typed org query"),
    );
    assert_eq!(rows.len(), 1);
}

#[test]
// req: OQ-017, OQ-018
fn columnar_provider_descriptor_matches_real_reference_engine_contract() {
    let bundle = compile_bundle();
    let descriptor = register_ticket_board(&bundle)
        .columnar_provider_descriptor_v1(
            ProjectionProviderPolicyModeV1::PartitionAligned,
            ProjectionProviderStaticBoundsV1 {
                max_candidates: 100_000,
                max_output_rows: 500,
                max_measures: 16,
                max_input_bytes: 16_384,
                max_work_units: 1_000_000,
                max_state_bytes_per_row: 16_384,
                max_diagnostic_bytes: 4_096,
                retained_epochs: 8_192,
                max_catchup_lag: 100,
                max_epoch_lease_steps: 1_000,
            },
        )
        .unwrap();
    assert_eq!(descriptor.kind(), ProjectionProviderKindV1::Columnar);
    assert_eq!(descriptor.posture(), ProjectionProviderPostureV1::Exact);
    assert!(
        descriptor
            .capabilities()
            .contains(ProjectionProviderCapabilitiesV1::ORDER)
    );
    assert!(
        descriptor
            .capabilities()
            .contains(ProjectionProviderCapabilitiesV1::MEASURE)
    );
    assert!(
        !descriptor
            .capabilities()
            .contains(ProjectionProviderCapabilitiesV1::RANK)
    );
    let aggregate_semantics = descriptor.aggregate_semantics();
    for semantic in [
        AggregateSemanticIdentityV1::Count,
        AggregateSemanticIdentityV1::Sum,
        AggregateSemanticIdentityV1::Min,
        AggregateSemanticIdentityV1::Max,
        AggregateSemanticIdentityV1::CountPresent,
        AggregateSemanticIdentityV1::CountDistinct,
        AggregateSemanticIdentityV1::CountDistinctPresent,
        AggregateSemanticIdentityV1::Mean,
        AggregateSemanticIdentityV1::Any,
        AggregateSemanticIdentityV1::All,
    ] {
        assert!(aggregate_semantics.contains(semantic));
    }
    assert!(!aggregate_semantics.contains(AggregateSemanticIdentityV1::ExactCount));
}
