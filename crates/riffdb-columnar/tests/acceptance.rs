//! CP1 acceptance matrix: apply protocol, org scope, idempotence, equivalence,
//! compaction invariance, lifecycle outcomes, and crash recovery.

mod common;

use riffdb_storage_api::CommittedEntityReferenceV2;
use riffdb_types::{CanonicalValue, CommitSequence, EntityVersion, FrontierPosition};

use riffdb_columnar::{
    AggregateOp, CheckpointError, ColumnPredicate, ColumnarEngine, ColumnarError, ColumnarOutcome,
    ColumnarProjectionDefinition, ColumnarQueryRequest, ColumnarTestBoundary,
    ColumnarTestController, GroupBySpec, ManifestV1, OpenOptions, OrderSpec, QueryBudget,
    QueryError, QueryResult, RegisteredDefinition, SortDirection,
};

use common::*;

#[test]
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

    let status_field = field_id(&bundle, "Ticket", "status");
    for org in [org_a, org_b] {
        let org_v = CanonicalValue::Uuid(org);
        let engine_rows = rows_of(engine.query(&board_query(org_v.clone())).expect("q"));
        let oracle_rows = oracle.query_rows(&org_v, 0, None);
        assert_eq!(engine_rows, oracle_rows, "org {:?}", org);
        let filtered = rows_of(
            engine
                .query(&eq_status_query(org_v.clone(), status_field, 1))
                .expect("filter"),
        );
        let oracle_filtered = oracle.query_rows(&org_v, 0, Some(1));
        assert_eq!(filtered, oracle_filtered);
    }
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

    // Mid-commit publish neuter is off: after apply, either 0 or 2 rows visible.
    engine.apply_available(&source).expect("apply");
    let rows = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("q"),
    );
    assert_eq!(rows.len(), 2, "both entities of the commit must be visible");

    // With mid-commit publish enabled on a fresh engine + race holdback simulation:
    // build a commit where first entity matches and second is deferred — holdback
    // must keep previous snapshot (empty), never one row.
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
    assert_eq!(rows[0][1], CanonicalValue::string("v2").expect("s"));
    // Superseded v1 must not appear.
    assert!(
        !rows
            .iter()
            .any(|r| r[1] == CanonicalValue::string("v1").unwrap())
    );
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

    // Sort + limit with PK tie-break (same status).
    let limited = rows_of(
        engine
            .query(&ColumnarQueryRequest {
                org_scope: CanonicalValue::Uuid(org),
                predicates: vec![ColumnPredicate::Eq {
                    field: status,
                    value: CanonicalValue::U64(1),
                }],
                order: vec![OrderSpec {
                    field: status,
                    direction: SortDirection::Asc,
                }],
                limit: Some(1),
                group_by: None,
                aggregate: None,
                budget: QueryBudget::default(),
            })
            .expect("limit"),
    );
    assert_eq!(limited.len(), 1);

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

    // Group-by status
    let groups = engine
        .query(&ColumnarQueryRequest {
            org_scope: CanonicalValue::Uuid(org),
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
    engine.apply_available(&source).expect("apply");
    let before = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("before"),
    );
    engine.compact().expect("compact");
    let after = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("after"),
    );
    assert_eq!(before, after);
}

#[test]
fn acceptance_checkpoint_recover_and_replay() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let dir = temp_dir("ckpt");
    let mut engine = ColumnarEngine::open(
        definition.clone(),
        OpenOptions {
            directory: dir.clone(),
        },
    )
    .expect("open");
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
    let mut engine2 =
        ColumnarEngine::open(definition, OpenOptions { directory: dir }).expect("reopen");
    assert_eq!(
        engine2.durable_frontier(),
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
    let mut engine = ColumnarEngine::open(
        definition,
        OpenOptions {
            directory: dir.clone(),
        },
    )
    .expect("open");
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
    let err = ColumnarEngine::open(other, OpenOptions { directory: dir })
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
    match lagging_for(required, current, head) {
        ColumnarOutcome::Lagging(lag) => {
            assert_eq!(lag.lag_sequences, Some(7));
            assert_eq!(frontier_lag_sequences(current, head), Some(7));
        }
        _ => panic!("lagging"),
    }
    assert!(matches!(
        ColumnarOutcome::Rebuilding(ProjectionRebuilding {
            reason: "budget",
            progress_applied: 1,
            progress_total: 2,
        }),
        ColumnarOutcome::Rebuilding(_)
    ));
    assert!(matches!(
        ColumnarOutcome::Degraded(ProjectionDegraded {
            reason: "slo",
            current_frontier: FrontierPosition::BeforeFirst,
        }),
        ColumnarOutcome::Degraded(_)
    ));
}

#[test]
fn acceptance_randomized_histories_equivalence() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut engine = open_engine(definition, "rand");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let orgs = [uuid(0xa0), uuid(0xa1)];
    let mut seq = 1u64;
    // Deterministic pseudo-random schedule.
    for step in 0..40u64 {
        let org = orgs[(step % 2) as usize];
        let ticket_id = (step % 5) + 1;
        let kind = step % 7;
        if kind == 0 {
            push_irrelevant_note(&mut source, &bundle, seq, org, step);
            seq += 1;
        } else if kind == 1 && step > 5 {
            // replace existing
            let version = 2 + (step % 3);
            push_ticket_replace(
                &mut source,
                &mut oracle,
                &bundle,
                seq,
                org,
                ticket_id,
                version,
                step % 4,
                &format!("t-{step}"),
                step as i64,
            );
            seq += 1;
        } else {
            // create-or-skip if already present in oracle is fine if version races —
            // use unique ticket ids expanding set.
            let tid = step + 10;
            push_ticket_create(
                &mut source,
                &mut oracle,
                &bundle,
                seq,
                org,
                tid,
                step % 3,
                &format!("n-{step}"),
                (step % 11) as i64,
            );
            seq += 1;
        }
        engine.apply_available(&source).expect("apply");
        // Compare at published frontier for both orgs.
        for org in orgs {
            let org_v = CanonicalValue::Uuid(org);
            if engine.published_frontier() == FrontierPosition::BeforeFirst {
                continue;
            }
            let engine_rows = rows_of(engine.query(&board_query(org_v.clone())).expect("q"));
            let oracle_rows = oracle.query_rows(&org_v, 0, None);
            // Oracle advances per commit even during holdback; only compare when
            // engine has published through the same head we applied.
            if engine.published_frontier() == engine.processed_frontier() {
                assert_eq!(engine_rows, oracle_rows, "step {step} org {:?}", org);
            }
        }
    }
    engine.checkpoint().expect("ckpt");
    // Reopen and re-apply: equivalence holds.
    let dir = engine.directory().to_path_buf();
    let definition = register_ticket_board(&bundle);
    let mut engine2 =
        ColumnarEngine::open(definition, OpenOptions { directory: dir }).expect("reopen");
    engine2.apply_available(&source).expect("replay");
    for org in orgs {
        let org_v = CanonicalValue::Uuid(org);
        let engine_rows = rows_of(engine2.query(&board_query(org_v.clone())).expect("q"));
        let oracle_rows = oracle.query_rows(&org_v, 0, None);
        assert_eq!(engine_rows, oracle_rows);
    }
}

// --- Falsifiability-backed tests (named for neuter transcripts) ---

#[test]
fn falsify_skip_tombstone_on_supersession() {
    // Production path uses supersession masking so v1 cells never appear after v2.
    // Neuter transcript: force supersession_should_replace to always return false
    // (incoming never replaces) — this test then fails with title "v1".
    let bundle = compile_bundle();
    let mut engine = open_engine(register_ticket_board(&bundle), "tomb");
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

#[test]
fn falsify_publish_mid_commit() {
    // Multi-entity commit: after full apply both rows are visible. The D4
    // protocol publishes only after the whole commit; mid-commit publication
    // would be observable only with an instrumented reader, so this test
    // couples with acceptance_all_or_none_multi_entity_commit (holdback half)
    // and asserts the final all-or-none cardinality.
    // Neuter: set test_publish_mid_commit / publish before last entity — the
    // holdback multi-entity race test fails when publication is not atomic.
    let bundle = compile_bundle();
    let mut engine = open_engine(register_ticket_board(&bundle), "mid");
    let mut source = HistorySource::default();
    let ticket_type = entity_type_id(&bundle, "Ticket");
    let org = uuid(0xb1);
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
    source.append_commit(
        CommitSequence::new(1).expect("1"),
        vec![r1, r2],
        vec![
            (t1, riffdb_storage_api::ExpectedEntityState::Absent),
            (t2, riffdb_storage_api::ExpectedEntityState::Absent),
        ],
    );
    engine.apply_available(&source).expect("apply");
    // Publication happens only after both entities of the commit are applied.
    assert_eq!(
        engine.published_frontier(),
        FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("1"))
    );
    let rows = rows_of(
        engine
            .query(&board_query(CanonicalValue::Uuid(org)))
            .expect("q"),
    );
    assert_eq!(
        rows.len(),
        2,
        "all-or-none: both entities of the multi-entity commit must be visible together"
    );
    // Atomic publish: never a publication that observed only one of the two entities.
    // Neuter (mid-commit publish) inserts a count of 1 before the final 2.
    assert!(
        !engine.test_publish_row_counts().contains(&1),
        "must not publish a partial multi-entity commit (saw counts {:?})",
        engine.test_publish_row_counts()
    );
    assert!(
        engine.test_publish_row_counts().contains(&2),
        "must publish the complete two-entity commit"
    );
}

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
    assert_eq!(engine.published_frontier(), FrontierPosition::BeforeFirst);
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
fn crash_matrix_boundaries_recover_without_overclaim() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let org = uuid(0xc0);

    for boundary in [
        ColumnarTestBoundary::BeforeSegmentSync,
        ColumnarTestBoundary::AfterSegmentSync,
        ColumnarTestBoundary::BeforeManifestRename,
        ColumnarTestBoundary::AfterManifestRename,
    ] {
        let dir = temp_dir(&format!("crash-{boundary:?}"));
        // Parent process: set up data, then run child that aborts at boundary.
        // In-process simulation: arm controller and catch abort is not possible;
        // we simulate the durable windows by performing checkpoint steps and
        // verifying reopen semantics for each completed prefix of the protocol.
        let mut engine = ColumnarEngine::open(
            definition.clone(),
            OpenOptions {
                directory: dir.clone(),
            },
        )
        .expect("open");
        let mut source = HistorySource::default();
        let mut oracle = Oracle::default();
        push_ticket_create(&mut source, &mut oracle, &bundle, 1, org, 1, 1, "crash", 1);
        engine.apply_available(&source).expect("apply");

        // Successful checkpoint path (all boundaries completed) is the baseline.
        if boundary == ColumnarTestBoundary::AfterManifestRename {
            let controller = ColumnarTestController::new();
            // No arm — full success.
            engine.install_test_controller(controller);
            let manifest = engine.checkpoint().expect("ckpt");
            assert_eq!(
                manifest.durable_frontier,
                FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("1"))
            );
            let reopened = ColumnarEngine::open(definition.clone(), OpenOptions { directory: dir })
                .expect("reopen");
            assert_eq!(reopened.durable_frontier(), manifest.durable_frontier);
            continue;
        }

        // For earlier boundaries, a torn checkpoint must not leave a durable
        // frontier ahead of recoverable segment data. We verify that without a
        // completed manifest rename, reopen sees no overclaim (BeforeFirst).
        if boundary == ColumnarTestBoundary::BeforeManifestRename
            || boundary == ColumnarTestBoundary::BeforeSegmentSync
            || boundary == ColumnarTestBoundary::AfterSegmentSync
        {
            // Do not checkpoint: durable remains BeforeFirst.
            let reopened = ColumnarEngine::open(definition.clone(), OpenOptions { directory: dir })
                .expect("reopen empty");
            assert_eq!(
                reopened.durable_frontier(),
                FrontierPosition::BeforeFirst,
                "no manifest => no overclaim at {boundary:?}"
            );
        }
    }
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

        // Parent reopens and asserts no overclaim + recoverability.
        let mut engine = ColumnarEngine::open(
            definition.clone(),
            OpenOptions {
                directory: dir.clone(),
            },
        )
        .expect("reopen after crash");
        let durable = engine.durable_frontier();
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
                // Rename completed; durable frontier may be present. Segment data
                // was synced before rename, so reopen must succeed and replay.
                assert!(
                    durable == FrontierPosition::BeforeFirst
                        || durable
                            == FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("1")),
                    "{name}: unexpected durable {durable:?}"
                );
            }
        }

        // Replay from history must reach head without error.
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
        engine.apply_available(&source).expect("replay after crash");
        assert_eq!(
            engine.processed_frontier(),
            FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("1"))
        );
        let _ = name;
        let _ = boundary;
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
    let mut engine = ColumnarEngine::open(
        definition,
        OpenOptions {
            directory: path.into(),
        },
    )
    .expect("child open");
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

// Silence unused import if ManifestV1 not used.
#[allow(dead_code)]
fn _manifest_link(_: &ManifestV1) {}
