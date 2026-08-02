//! CP2a deliverable coverage: select, PK rows/order, query_snapshot, storage
//! shape, org-key hook, and incarnation-bound frontiers.

mod common;

use riffdb_storage_api::{StorageError, StorageErrorKind};
use riffdb_types::{
    CanonicalValue, CommitSequence, CommitToken, FrontierPosition, ProjectionFrontier,
    encode_canonical_value,
};

use riffdb_columnar::{
    ColumnPredicate, ColumnarEngine, ColumnarError, ColumnarQueryRequest, DegradedReason,
    OpenOptions, OrderSpec, QueryBudget, QueryError, QueryResult, RebuildingReason, SortDirection,
    StorageFailure, encode_org_scope_key, query_snapshot,
};

use common::*;

fn seed_board_three_tickets() -> (riffdb_contract_ir::ContractBundle, ColumnarEngine, [u8; 16]) {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut engine = open_engine(definition, "cp2a-seed");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0xc2);
    // Ticket ids deliberately out of insertion order relative to status/title.
    push_ticket_create(&mut source, &mut oracle, &bundle, 1, org, 30, 2, "c", 3);
    push_ticket_create(&mut source, &mut oracle, &bundle, 2, org, 10, 1, "a", 1);
    push_ticket_create(&mut source, &mut oracle, &bundle, 3, org, 20, 1, "b", 2);
    engine.apply_available(&source).expect("apply");
    (bundle, engine, org)
}

/// A1: empty select returns all projected fields; non-empty narrows exactly.
#[test]
fn a1_select_empty_is_all_projected_and_narrowing_cannot_widen() {
    let (bundle, engine, org) = seed_board_three_tickets();
    let status = field_id(&bundle, "Ticket", "status");
    let title = field_id(&bundle, "Ticket", "title");
    let priority = field_id(&bundle, "Ticket", "priority");

    let all = match engine
        .query(&board_query(CanonicalValue::Uuid(org)))
        .expect("all")
    {
        QueryResult::Rows(rows) => rows,
        other => panic!("rows: {other:?}"),
    };
    assert_eq!(all.fields, vec![status, title, priority]);
    assert_eq!(all.rows.len(), 3);
    assert_eq!(all.rows[0].cells.len(), 3);

    let narrowed = match engine
        .query(&ColumnarQueryRequest {
            org_scope: CanonicalValue::Uuid(org),
            select: vec![title],
            predicates: Vec::new(),
            order: Vec::new(),
            limit: None,
            group_by: None,
            aggregate: None,
            budget: QueryBudget::default(),
        })
        .expect("narrow")
    {
        QueryResult::Rows(rows) => rows,
        other => panic!("rows: {other:?}"),
    };
    assert_eq!(narrowed.fields, vec![title]);
    assert_eq!(narrowed.rows.len(), 3);
    for row in &narrowed.rows {
        assert_eq!(row.cells.len(), 1, "must not widen to unselected cells");
    }
    // Falsifiable: neutering project_selected_cells to return all cells fails this.
    let titles: Vec<_> = narrowed
        .rows
        .iter()
        .map(|row| row.cells[0].clone())
        .collect();
    assert!(
        titles
            .iter()
            .all(|cell| matches!(cell, CanonicalValue::String(_)))
    );

    let err = engine
        .query(&ColumnarQueryRequest {
            org_scope: CanonicalValue::Uuid(org),
            select: vec![field_id(&bundle, "Ticket", "ticket_id")],
            predicates: Vec::new(),
            order: Vec::new(),
            limit: None,
            group_by: None,
            aggregate: None,
            budget: QueryBudget::default(),
        })
        .expect_err("ticket_id is PK not projected");
    assert!(matches!(
        err,
        ColumnarError::Query(QueryError::UnprojectedSelectField { .. })
    ));
}

/// A2: every row exposes decoded primary-key values, field-id addressable.
#[test]
fn a2_primary_key_values_on_rows_field_id_addressable() {
    let (bundle, engine, org) = seed_board_three_tickets();
    let ticket_id = field_id(&bundle, "Ticket", "ticket_id");
    let org_field = field_id(&bundle, "Ticket", "organization_id");

    let rows = match engine
        .query(&board_query(CanonicalValue::Uuid(org)))
        .expect("q")
    {
        QueryResult::Rows(rows) => rows,
        other => panic!("{other:?}"),
    };
    assert_eq!(rows.primary_key_fields, vec![org_field, ticket_id]);
    // Default order is PrimaryKeyBytes order ⇒ ticket_id ascending within org.
    let ids: Vec<u64> = rows
        .rows
        .iter()
        .map(|row| {
            match row
                .primary_key_value(&rows.primary_key_fields, ticket_id)
                .expect("ticket_id")
            {
                CanonicalValue::U64(id) => *id,
                other => panic!("ticket_id cell {other:?}"),
            }
        })
        .collect();
    assert_eq!(
        ids,
        vec![10, 20, 30],
        "board-shaped rows carry ticket_id in PK order"
    );
    for row in &rows.rows {
        assert_eq!(
            row.primary_key_value(&rows.primary_key_fields, org_field),
            Some(&CanonicalValue::Uuid(org))
        );
    }
}

/// A3: PK OrderSpec works for order-preserving components; proves ticket_id
/// ASC matches PrimaryKeyBytes order (uuid+u64 key; uuid fixed within org).
#[test]
fn a3_pk_order_spec_matches_primary_key_bytes_order() {
    let (bundle, engine, org) = seed_board_three_tickets();
    let ticket_id = field_id(&bundle, "Ticket", "ticket_id");

    let ordered = match engine
        .query(&ColumnarQueryRequest {
            org_scope: CanonicalValue::Uuid(org),
            select: vec![field_id(&bundle, "Ticket", "title")],
            predicates: Vec::new(),
            order: vec![OrderSpec {
                field: ticket_id,
                direction: SortDirection::Asc,
            }],
            limit: None,
            group_by: None,
            aggregate: None,
            budget: QueryBudget::default(),
        })
        .expect("order by pk")
    {
        QueryResult::Rows(rows) => rows,
        other => panic!("{other:?}"),
    };
    let ids: Vec<u64> = ordered
        .rows
        .iter()
        .map(|row| match row.primary_key[1] {
            CanonicalValue::U64(id) => id,
            _ => panic!("pk"),
        })
        .collect();
    assert_eq!(ids, vec![10, 20, 30]);

    let desc = match engine
        .query(&ColumnarQueryRequest {
            org_scope: CanonicalValue::Uuid(org),
            select: Vec::new(),
            predicates: Vec::new(),
            order: vec![OrderSpec {
                field: ticket_id,
                direction: SortDirection::Desc,
            }],
            limit: None,
            group_by: None,
            aggregate: None,
            budget: QueryBudget::default(),
        })
        .expect("desc")
    {
        QueryResult::Rows(rows) => rows,
        other => panic!("{other:?}"),
    };
    let ids_desc: Vec<u64> = desc
        .rows
        .iter()
        .map(|row| match row.primary_key[1] {
            CanonicalValue::U64(id) => id,
            _ => panic!("pk"),
        })
        .collect();
    assert_eq!(ids_desc, vec![30, 20, 10]);
}

/// A4: query_snapshot serves the same results as engine.query without the engine.
#[test]
fn a4_query_snapshot_matches_engine_query() {
    let (bundle, engine, org) = seed_board_three_tickets();
    let request = ColumnarQueryRequest {
        org_scope: CanonicalValue::Uuid(org),
        select: vec![field_id(&bundle, "Ticket", "status")],
        predicates: vec![ColumnPredicate::Eq {
            field: field_id(&bundle, "Ticket", "status"),
            value: CanonicalValue::U64(1),
        }],
        order: Vec::new(),
        limit: Some(10),
        group_by: None,
        aggregate: None,
        budget: QueryBudget::default(),
    };
    let via_engine = engine.query(&request).expect("engine");
    let via_snapshot = query_snapshot(engine.definition(), &engine.published_snapshot(), &request)
        .expect("snapshot");
    assert_eq!(via_engine, via_snapshot);
}

/// A5: closed reason enums are exhaustive (tags unique, match arms closed).
#[test]
fn a5_closed_reason_enums_exhaustiveness() {
    assert_eq!(RebuildingReason::all().len(), 3);
    assert_eq!(DegradedReason::all().len(), 3);
    let rebuild_tags: std::collections::BTreeSet<_> =
        RebuildingReason::all().iter().map(|r| r.tag()).collect();
    assert_eq!(rebuild_tags.len(), 3);
    let degraded_tags: std::collections::BTreeSet<_> =
        DegradedReason::all().iter().map(|r| r.tag()).collect();
    assert_eq!(degraded_tags.len(), 3);
}

/// A6: Storage failures are structured kinds, never raw to_string dumps.
#[test]
fn a6_storage_failure_is_structured_not_stringified() {
    let storage = StorageError::new(StorageErrorKind::CorruptData, None);
    let failure = StorageFailure::from_storage_error(&storage);
    assert_eq!(failure.kind(), StorageErrorKind::CorruptData);
    let err = ColumnarError::from(storage);
    match err {
        ColumnarError::Storage(failure) => {
            assert_eq!(failure.kind(), StorageErrorKind::CorruptData);
            let display = failure.to_string();
            assert!(display.contains("corrupt"));
            // Must not look like Debug dump of the storage error.
            assert!(!display.contains("StorageError"));
        }
        other => panic!("expected Storage, got {other:?}"),
    }
}

/// A7: org-key encoding is publicly comparable to encode_canonical_value.
#[test]
fn a7_org_key_encoding_is_canonical_value_bytes() {
    let org = CanonicalValue::Uuid(uuid(0xab));
    let key = encode_org_scope_key(&org).expect("encode");
    let direct = encode_canonical_value(&org).expect("canonical");
    assert_eq!(key.as_bytes(), direct.as_slice());
    let via_from_value = riffdb_columnar::OrgKey::from_value(&org).expect("from_value");
    assert_eq!(via_from_value, key);
}

/// A8: engine frontiers carry history incarnation; stale token cannot satisfy.
#[test]
fn a8_frontier_binds_incarnation_stale_token_rejected() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let mut engine = ColumnarEngine::open(
        definition,
        OpenOptions::new(temp_dir("a8")).with_history_incarnation(7),
    )
    .expect("open");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let org = uuid(0xa8);
    push_ticket_create(&mut source, &mut oracle, &bundle, 1, org, 1, 1, "t", 1);
    engine.apply_available(&source).expect("apply");

    let frontier = engine.published_frontier();
    assert_eq!(frontier.history_incarnation(), 7);
    assert_eq!(
        frontier.position(),
        FrontierPosition::AppliedThrough(CommitSequence::new(1).expect("1"))
    );

    let same_inc = CommitToken::new(7, CommitSequence::new(1).expect("1"));
    let stale_inc = CommitToken::new(6, CommitSequence::new(1).expect("1"));
    assert!(frontier.satisfies(&same_inc));
    // Falsifiable: dropping incarnation from satisfies() would make this pass.
    assert!(
        !frontier.satisfies(&stale_inc),
        "stale-token-satisfies must fail across incarnations"
    );

    // Opaque bytes round-trip preserves incarnation.
    let decoded =
        ProjectionFrontier::from_bytes(frontier.as_bytes().to_vec()).expect("decode frontier");
    assert_eq!(decoded.history_incarnation(), 7);
    assert!(!decoded.satisfies(&stale_inc));
}
