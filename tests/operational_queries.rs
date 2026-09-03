//! WP-564 closed operational-index execution acceptance.

use std::collections::BTreeMap;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::compile_operational_query_family;
use riffdb_query_executor::{
    BoundPredicate, QueryBackendFault, QueryContinuation, QueryExecutionError, QueryOwnedSnapshot,
    QueryParameters, QueryReadView, QueryResultValue, QueryRow, QueryScanPage,
    bound_index_range_schedule_v1, execute_operational_page_in_snapshot,
};
use riffdb_query_ir::{AccessDirection, QueryAccessKind, QueryAccessStep, SymbolicCatalog};
use riffdb_riffql_syntax::parse_query;
use riffdb_types::{CanonicalRecord, CanonicalValue, Timestamp};

const CONTRACT: &str = r#"
contract OperationalQueries version 1 {
  entity Document {
    key (organization_id: uuid, document_id: uuid)
    field deleted_at: optional<timestamp>
    field title: string<64>
    field relation: string<64>
    field user: string<256>
    field sequence: u64
    index by_deleted (organization_id, deleted_at, document_id) presence(deleted_at)
    index by_title (organization_id, title, document_id) text_key(title, binary_utf8_v1)
    index by_relation_user (organization_id, relation, user, document_id)
      text_key(relation, binary_utf8_v1)
      text_key(user, binary_utf8_v1)
    index by_sequence (organization_id, sequence, document_id)
  }
  aggregate Documents {
    root Document
    partition_by organization_id
    conflict_key (organization_id, document_id)
  }
}
"#;

const NULL_QUERY: &str = r#"
query NullDocuments($organization_id: Document.organization_id) {
  many documents from Document
    where organization_id == $organization_id && deleted_at is null
    order by document_id asc
    take 10
  return Found { documents: documents { document_id deleted_at } }
  outcomes Found
}
"#;

const NOT_NULL_QUERY: &str = r#"
query NotNullDocuments($organization_id: Document.organization_id) {
  many documents from Document
    where organization_id == $organization_id && deleted_at is not null
    order by deleted_at asc nulls first, document_id asc
    take 10
  return Found { documents: documents { document_id deleted_at } }
  outcomes Found
}
"#;

const EXISTS_QUERY: &str = r#"
query ExistingDocuments($organization_id: Document.organization_id) {
  many documents from Document
    where organization_id == $organization_id && exists deleted_at
    order by deleted_at asc nulls first, document_id asc
    take 10
  return Found { documents: documents { document_id deleted_at } }
  outcomes Found
}
"#;

const PREFIX_QUERY: &str = r#"
query PrefixDocuments(
  $organization_id: Document.organization_id,
  $prefix: Document.title,
) {
  many documents from Document
    where organization_id == $organization_id && title prefix $prefix
    order by title asc, document_id asc
    take 10
  return Found { documents: documents { document_id title } }
  outcomes Found
}
"#;

const BINARY_ORDER_QUERY: &str = r#"
query DocumentsByBinaryTitle(
  $organization_id: Document.organization_id,
  $after: Cursor?,
) {
  many documents from Document
    where organization_id == $organization_id
    order by title asc, document_id asc
    take 1 after $after
  return Found { documents: documents { document_id title } }
  outcomes Found
}
"#;

const RUNTIME_LIMIT_ORDER_QUERY: &str = r#"
query DocumentsByRuntimePage(
  $organization_id: Document.organization_id,
  $limit: Limit<5> = 2,
  $after: Cursor?,
) {
  many documents from Document
    where organization_id == $organization_id
    order by sequence asc, document_id asc
    take $limit after $after
  return Found { documents: documents { document_id sequence } }
  outcomes Found
}
"#;

const BINARY_COMPONENT_ORDER_QUERY: &str = r#"
query DocumentsByRelationAndUser(
  $organization_id: Document.organization_id,
  $after: Cursor?,
) {
  many documents from Document
    where organization_id == $organization_id
    order by relation asc, user asc, document_id asc
    take 1 after $after
  return Found { documents: documents { document_id relation user } }
  outcomes Found
}
"#;

const BINARY_COMPONENT_EQUALITY_QUERY: &str = r#"
query DocumentsByExactRelation(
  $organization_id: Document.organization_id,
  $relation: Document.relation,
  $after: Cursor?,
) {
  many documents from Document
    where organization_id == $organization_id && relation == $relation
    order by user asc, document_id asc
    take 1 after $after
  return Found { documents: documents { document_id relation user } }
  outcomes Found
}
"#;

const BINARY_COMPONENT_MEMBERSHIP_QUERY: &str = r#"
query DocumentsByRelations(
  $organization_id: Document.organization_id,
  $relations: Set<Document.relation>,
  $after: Cursor?,
) {
  many documents from Document
    where organization_id == $organization_id && relation in $relations
    order by relation asc, user asc, document_id asc
    take 1 after $after
  return Found { documents: documents { document_id relation user } }
  outcomes Found
}
"#;

const BINARY_TEXT_RANGE_QUERY: &str = r#"
query DocumentsByTitleRange(
  $organization_id: Document.organization_id,
  $text_lower: Document.title,
  $text_upper: Document.title,
  $after: Cursor?,
) {
  many documents from Document
    where organization_id == $organization_id
      && title > $text_lower
      && title < $text_upper
    order by title asc, document_id asc
    take 1 after $after
  return Found { documents: documents { document_id title } }
  outcomes Found
}
"#;

const BINARY_TEXT_COMPLEMENT_QUERY: &str = r#"
query DocumentsExceptTitle(
  $organization_id: Document.organization_id,
  $text_excluded: Document.title,
  $after: Cursor?,
) {
  many documents from Document
    where organization_id == $organization_id && title != $text_excluded
    order by title asc, document_id asc
    take 1 after $after
  return Found { documents: documents { document_id title } }
  outcomes Found
}
"#;

const CANONICAL_RANGE_QUERY: &str = r#"
query DocumentsBySequenceRange(
  $organization_id: Document.organization_id,
  $lower: Document.sequence,
  $upper: Document.sequence,
  $after: Cursor?,
) {
  many documents from Document
    where organization_id == $organization_id && sequence >= $lower && sequence < $upper
    order by sequence asc, document_id asc
    take 1 after $after
  return Found { documents: documents { document_id sequence } }
  outcomes Found
}
"#;

const CANONICAL_COMPLEMENT_QUERY: &str = r#"
query DocumentsExceptSequence(
  $organization_id: Document.organization_id,
  $excluded: Document.sequence,
  $after: Cursor?,
) {
  many documents from Document
    where organization_id == $organization_id && sequence != $excluded
    order by sequence asc, document_id asc
    take 1 after $after
  return Found { documents: documents { document_id sequence } }
  outcomes Found
}
"#;

#[derive(Clone, Copy)]
enum Presence {
    Missing,
    Null,
    Value(Timestamp),
}

#[derive(Clone)]
struct Document {
    ordinal: u8,
    title: &'static str,
    relation: &'static str,
    user: &'static str,
    deleted_at: Presence,
}

struct IndexedView {
    rows: Vec<(Vec<u8>, QueryRow)>,
}

impl QueryReadView for IndexedView {
    type Error = ();

    fn fault(&self, _error: &Self::Error) -> QueryBackendFault {
        QueryBackendFault::Unavailable
    }

    fn application_head(&self) -> u64 {
        7
    }

    fn point(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<QueryRow>, Self::Error> {
        Ok(None)
    }

    fn dependent_point_batch(
        &mut self,
        _step: &QueryAccessStep,
        predicates: &[Vec<BoundPredicate>],
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
        Ok(vec![None; predicates.len()])
    }

    fn scan(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        _after_inclusive: bool,
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, Self::Error> {
        let schedule =
            bound_index_range_schedule_v1(step, predicates).expect("sealed range schedule");
        let direction = match step.access() {
            QueryAccessKind::Index { direction, .. } => *direction,
            _ => panic!("operational acceptance scan requires an index"),
        };
        let mut candidates = self
            .rows
            .iter()
            .filter(|(key, _)| {
                schedule.ranges().iter().any(|range| {
                    key.as_slice() >= range.start_inclusive()
                        && key.as_slice() < range.end_exclusive()
                })
            })
            .filter(|(key, _)| {
                after.is_none_or(|after| match direction {
                    AccessDirection::Forward => key.as_slice() > after,
                    AccessDirection::Reverse => key.as_slice() < after,
                })
            })
            .collect::<Vec<_>>();
        if direction == AccessDirection::Reverse {
            candidates.reverse();
        }
        let limit = usize::try_from(limit).expect("bounded limit");
        let has_more = candidates.len() > limit;
        let selected = candidates.into_iter().take(limit).collect::<Vec<_>>();
        let continuation = has_more.then(|| {
            selected
                .last()
                .expect("continued page is nonempty")
                .0
                .clone()
        });
        let rows = selected
            .into_iter()
            .map(|(_, row)| row.clone())
            .collect::<Vec<_>>();
        Ok(match continuation {
            Some(continuation) => QueryScanPage::continued(
                rows,
                1,
                u64::try_from(limit + 1).expect("bounded scan work"),
                continuation,
            )
            .expect("bounded continuation"),
            None => QueryScanPage::exact_end(rows, 1),
        })
    }

    fn nearest(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _k: u32,
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<riffdb_query_executor::QueryNearestPage, Self::Error> {
        Ok(riffdb_query_executor::QueryNearestPage {
            rows: Vec::new(),
            scanned_rows: 0,
        })
    }
}

#[derive(Default)]
struct QueryInputs<'a> {
    prefix: Option<&'a str>,
    relation: Option<&'a str>,
    relations: Option<&'a [&'a str]>,
    range: Option<(u64, u64)>,
    excluded: Option<u64>,
    text_range: Option<(&'a str, &'a str)>,
    text_excluded: Option<&'a str>,
    limit: Option<u64>,
    prior: Option<&'a QueryContinuation>,
}

fn execute_page(
    source: &str,
    inputs: QueryInputs<'_>,
) -> Result<QueryOwnedSnapshot, QueryExecutionError> {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family =
        compile_operational_query_family(&parse_query(source).expect("query syntax"), &catalog)
            .expect("operational plan");
    let program = family.select(&[]).expect("sole member").program();
    let step = &program.steps()[0];
    let index_name = match step.access() {
        riffdb_query_ir::QueryAccessKind::Index { index, .. } => index,
        _ => panic!("operational acceptance requires an index"),
    };
    let entity = &bundle.schema().entities()[0];
    let index = entity
        .indexes()
        .iter()
        .find(|candidate| candidate.name() == index_name)
        .expect("selected index");
    let field_id = |name: &str| {
        entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .expect("field")
            .id()
    };
    let organization = CanonicalValue::Uuid([1; 16]);
    let documents = [
        Document {
            ordinal: 2,
            title: "a",
            relation: "rel6",
            user: "x",
            deleted_at: Presence::Missing,
        },
        Document {
            ordinal: 3,
            title: "ab",
            relation: "rel-3",
            user: "x",
            deleted_at: Presence::Null,
        },
        Document {
            ordinal: 4,
            title: "abacus",
            relation: "viewer",
            user: "doc6",
            deleted_at: Presence::Value(Timestamp::new(10, 0).expect("timestamp")),
        },
        Document {
            ordinal: 5,
            title: "ac",
            relation: "viewer",
            user: "doc-3",
            deleted_at: Presence::Missing,
        },
        Document {
            ordinal: 6,
            title: "doc6",
            relation: "owner",
            user: "z",
            deleted_at: Presence::Missing,
        },
        Document {
            ordinal: 7,
            title: "doc-3",
            relation: "editor",
            user: "a",
            deleted_at: Presence::Missing,
        },
    ];
    let mut indexed = Vec::new();
    for document in documents {
        let document_id = CanonicalValue::Uuid([document.ordinal; 16]);
        let mut stored_fields = vec![
            (field_id("organization_id"), organization.clone()),
            (field_id("document_id"), document_id.clone()),
            (
                field_id("title"),
                CanonicalValue::string(document.title).expect("title"),
            ),
            (
                field_id("relation"),
                CanonicalValue::string(document.relation).expect("relation"),
            ),
            (
                field_id("user"),
                CanonicalValue::string(document.user).expect("user"),
            ),
            (
                field_id("sequence"),
                CanonicalValue::U64(u64::from(document.ordinal)),
            ),
        ];
        match document.deleted_at {
            Presence::Missing => {}
            Presence::Null => stored_fields.push((field_id("deleted_at"), CanonicalValue::Null)),
            Presence::Value(value) => {
                stored_fields.push((field_id("deleted_at"), CanonicalValue::Timestamp(value)))
            }
        }
        stored_fields.sort_unstable_by_key(|(field, _)| *field);
        let record = CanonicalRecord::new(stored_fields).expect("stored record");
        let values = riffdb_contract_ir::encode_operational_index_values_v1(index, &record)
            .expect("index values");
        let entity_key = entity
            .primary_key()
            .encode_entity(&[organization.clone(), document_id.clone()])
            .expect("entity key");
        let key = index
            .key_schema()
            .encode_index(&values, entity_key)
            .expect("index key");
        let mut row_fields = BTreeMap::from([
            ("organization_id".to_owned(), organization.clone()),
            ("document_id".to_owned(), document_id),
            (
                "title".to_owned(),
                CanonicalValue::string(document.title).expect("title"),
            ),
            (
                "relation".to_owned(),
                CanonicalValue::string(document.relation).expect("relation"),
            ),
            (
                "user".to_owned(),
                CanonicalValue::string(document.user).expect("user"),
            ),
            (
                "sequence".to_owned(),
                CanonicalValue::U64(u64::from(document.ordinal)),
            ),
        ]);
        match document.deleted_at {
            Presence::Missing => {}
            Presence::Null => {
                row_fields.insert("deleted_at".to_owned(), CanonicalValue::Null);
            }
            Presence::Value(value) => {
                row_fields.insert("deleted_at".to_owned(), CanonicalValue::Timestamp(value));
            }
        }
        indexed.push((
            key.as_bytes().to_vec(),
            QueryRow::checked("Document".to_owned(), row_fields).expect("query row"),
        ));
    }
    indexed.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    let mut parameter_values = BTreeMap::from([("organization_id".to_owned(), organization)]);
    if let Some(prefix) = inputs.prefix {
        parameter_values.insert(
            "prefix".to_owned(),
            CanonicalValue::string(prefix).expect("prefix"),
        );
    }
    if let Some(relation) = inputs.relation {
        parameter_values.insert(
            "relation".to_owned(),
            CanonicalValue::string(relation).expect("relation"),
        );
    }
    if let Some(relations) = inputs.relations {
        parameter_values.insert(
            "relations".to_owned(),
            CanonicalValue::list(
                relations
                    .iter()
                    .map(|relation| CanonicalValue::string(*relation).expect("relation member"))
                    .collect(),
            )
            .expect("bounded relation set"),
        );
    }
    if let Some((lower, upper)) = inputs.range {
        parameter_values.insert("lower".to_owned(), CanonicalValue::U64(lower));
        parameter_values.insert("upper".to_owned(), CanonicalValue::U64(upper));
    }
    if let Some(excluded) = inputs.excluded {
        parameter_values.insert("excluded".to_owned(), CanonicalValue::U64(excluded));
    }
    if let Some((lower, upper)) = inputs.text_range {
        parameter_values.insert(
            "text_lower".to_owned(),
            CanonicalValue::string(lower).expect("text lower"),
        );
        parameter_values.insert(
            "text_upper".to_owned(),
            CanonicalValue::string(upper).expect("text upper"),
        );
    }
    if let Some(excluded) = inputs.text_excluded {
        parameter_values.insert(
            "text_excluded".to_owned(),
            CanonicalValue::string(excluded).expect("excluded text"),
        );
    }
    if let Some(limit) = inputs.limit {
        parameter_values.insert("limit".to_owned(), CanonicalValue::U64(limit));
    }
    let parameters = QueryParameters::checked(parameter_values).expect("parameters");
    let mut view = IndexedView { rows: indexed };
    execute_operational_page_in_snapshot(
        program,
        family.aggregates(),
        &parameters,
        inputs.prior,
        &mut view,
    )
}

fn execute(source: &str, prefix: Option<&str>) -> Result<Vec<QueryRow>, QueryExecutionError> {
    let snapshot = execute_page(
        source,
        QueryInputs {
            prefix,
            ..QueryInputs::default()
        },
    )?;
    match snapshot.fields().get("documents") {
        Some(QueryResultValue::Many(rows)) => Ok(rows.clone()),
        other => panic!("expected documents result, got {other:?}"),
    }
}

fn titles(rows: &[QueryRow]) -> Vec<&str> {
    rows.iter()
        .map(|row| match row.field("title") {
            Some(CanonicalValue::String(value)) => value.as_str(),
            other => panic!("title missing: {other:?}"),
        })
        .collect()
}

fn sequences(rows: &[QueryRow]) -> Vec<u64> {
    rows.iter()
        .map(|row| match row.field("sequence") {
            Some(CanonicalValue::U64(value)) => *value,
            other => panic!("sequence missing: {other:?}"),
        })
        .collect()
}

fn continuation(snapshot: &QueryOwnedSnapshot) -> Option<QueryContinuation> {
    snapshot.continuation().map(|lower| {
        QueryContinuation::checked(
            snapshot
                .continuation_binding()
                .expect("continuation binding")
                .to_owned(),
            lower.to_vec(),
            snapshot.index_epochs().clone(),
        )
        .expect("checked continuation")
    })
}

fn execute_all_cursor_pages(source: &str, relation: Option<&str>) -> Vec<QueryRow> {
    let mut rows = Vec::new();
    let mut prior = None;
    loop {
        let snapshot = execute_page(
            source,
            QueryInputs {
                relation,
                prior: prior.as_ref(),
                ..QueryInputs::default()
            },
        )
        .expect("binary-order page");
        match snapshot.fields().get("documents") {
            Some(QueryResultValue::Many(page)) => rows.extend(page.iter().cloned()),
            other => panic!("expected documents page, got {other:?}"),
        }
        let Some(lower) = snapshot.continuation() else {
            break;
        };
        prior = Some(
            QueryContinuation::checked(
                snapshot
                    .continuation_binding()
                    .expect("continuation binding")
                    .to_owned(),
                lower.to_vec(),
                snapshot.index_epochs().clone(),
            )
            .expect("checked continuation"),
        );
    }
    rows
}

fn execute_all_membership_cursor_pages(source: &str, relations: &[&str]) -> Vec<QueryRow> {
    let mut rows = Vec::new();
    let mut prior = None;
    loop {
        let snapshot = execute_page(
            source,
            QueryInputs {
                relations: Some(relations),
                prior: prior.as_ref(),
                ..QueryInputs::default()
            },
        )
        .expect("binary-membership page");
        match snapshot.fields().get("documents") {
            Some(QueryResultValue::Many(page)) => rows.extend(page.iter().cloned()),
            other => panic!("expected documents page, got {other:?}"),
        }
        let Some(lower) = snapshot.continuation() else {
            break;
        };
        prior = Some(
            QueryContinuation::checked(
                snapshot
                    .continuation_binding()
                    .expect("continuation binding")
                    .to_owned(),
                lower.to_vec(),
                snapshot.index_epochs().clone(),
            )
            .expect("checked continuation"),
        );
    }
    rows
}

fn execute_all_interval_cursor_pages(
    source: &str,
    range: Option<(u64, u64)>,
    excluded: Option<u64>,
) -> Vec<QueryRow> {
    let mut rows = Vec::new();
    let mut prior = None;
    loop {
        let snapshot = execute_page(
            source,
            QueryInputs {
                range,
                excluded,
                prior: prior.as_ref(),
                ..QueryInputs::default()
            },
        )
        .expect("canonical interval page");
        match snapshot.fields().get("documents") {
            Some(QueryResultValue::Many(page)) => rows.extend(page.iter().cloned()),
            other => panic!("expected documents page, got {other:?}"),
        }
        let Some(lower) = snapshot.continuation() else {
            break;
        };
        prior = Some(
            QueryContinuation::checked(
                snapshot
                    .continuation_binding()
                    .expect("continuation binding")
                    .to_owned(),
                lower.to_vec(),
                snapshot.index_epochs().clone(),
            )
            .expect("checked continuation"),
        );
    }
    rows
}

fn execute_all_text_interval_cursor_pages(
    source: &str,
    range: Option<(&str, &str)>,
    excluded: Option<&str>,
) -> Vec<QueryRow> {
    let mut rows = Vec::new();
    let mut prior = None;
    loop {
        let snapshot = execute_page(
            source,
            QueryInputs {
                text_range: range,
                text_excluded: excluded,
                prior: prior.as_ref(),
                ..QueryInputs::default()
            },
        )
        .expect("binary text interval page");
        match snapshot.fields().get("documents") {
            Some(QueryResultValue::Many(page)) => rows.extend(page.iter().cloned()),
            other => panic!("expected documents page, got {other:?}"),
        }
        let Some(lower) = snapshot.continuation() else {
            break;
        };
        prior = Some(
            QueryContinuation::checked(
                snapshot
                    .continuation_binding()
                    .expect("continuation binding")
                    .to_owned(),
                lower.to_vec(),
                snapshot.index_epochs().clone(),
            )
            .expect("checked continuation"),
        );
    }
    rows
}

fn ordinals(rows: &[QueryRow]) -> Vec<u8> {
    rows.iter()
        .map(|row| match row.field("document_id") {
            Some(CanonicalValue::Uuid(value)) => value[0],
            other => panic!("document ID missing: {other:?}"),
        })
        .collect()
}

#[test]
fn null_existence_and_binary_prefix_execute_without_scan_fallback() {
    assert_eq!(
        ordinals(&execute(NULL_QUERY, None).expect("null query")),
        [3]
    );
    assert_eq!(
        ordinals(&execute(NOT_NULL_QUERY, None).expect("not-null query")),
        [4]
    );
    assert_eq!(
        ordinals(&execute(EXISTS_QUERY, None).expect("exists query")),
        [3, 4]
    );
    assert_eq!(
        ordinals(&execute(PREFIX_QUERY, Some("ab")).expect("prefix query")),
        [3, 4]
    );
}

#[test]
fn binary_text_key_order_is_bytewise_and_cursor_exact_in_both_directions() {
    let ascending = execute_all_cursor_pages(BINARY_ORDER_QUERY, None);
    assert_eq!(
        titles(&ascending),
        ["a", "ab", "abacus", "ac", "doc-3", "doc6"]
    );
    assert_eq!(ordinals(&ascending), [2, 3, 4, 5, 7, 6]);

    let descending_source = BINARY_ORDER_QUERY
        .replace("title asc", "title desc")
        .replace("document_id asc", "document_id desc");
    let descending = execute_all_cursor_pages(&descending_source, None);
    assert_eq!(
        titles(&descending),
        ["doc6", "doc-3", "ac", "abacus", "ab", "a"]
    );
    assert_eq!(ordinals(&descending), [6, 7, 5, 4, 3, 2]);
}

#[test]
fn runtime_page_cardinality_changes_preserve_exact_forward_and_reverse_continuation() {
    for (source, expected) in [
        (RUNTIME_LIMIT_ORDER_QUERY.to_owned(), vec![2, 3, 4, 5, 6, 7]),
        (
            RUNTIME_LIMIT_ORDER_QUERY.replace(
                "order by sequence asc, document_id asc",
                "order by sequence desc, document_id desc",
            ),
            vec![7, 6, 5, 4, 3, 2],
        ),
    ] {
        let first = execute_page(
            &source,
            QueryInputs {
                limit: Some(1),
                ..QueryInputs::default()
            },
        )
        .expect("one-row first page");
        let first_cursor = continuation(&first).expect("first page continues");

        let second = execute_page(
            &source,
            QueryInputs {
                limit: Some(4),
                prior: Some(&first_cursor),
                ..QueryInputs::default()
            },
        )
        .expect("four-row resumed page");
        let second_cursor = continuation(&second).expect("second page continues");

        let third = execute_page(
            &source,
            QueryInputs {
                limit: Some(1),
                prior: Some(&second_cursor),
                ..QueryInputs::default()
            },
        )
        .expect("one-row final page");
        assert!(continuation(&third).is_none());

        let rows = [&first, &second, &third]
            .into_iter()
            .flat_map(|snapshot| match snapshot.fields().get("documents") {
                Some(QueryResultValue::Many(rows)) => rows.iter(),
                other => panic!("expected documents page, got {other:?}"),
            })
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(sequences(&rows), expected);
    }
}

#[test]
fn one_binary_text_index_executes_ordered_and_exact_equality_cursor_shapes() {
    let wider = execute_all_cursor_pages(BINARY_COMPONENT_ORDER_QUERY, None);
    assert_eq!(ordinals(&wider), [7, 6, 3, 2, 5, 4]);

    let narrower = execute_all_cursor_pages(BINARY_COMPONENT_EQUALITY_QUERY, Some("viewer"));
    assert_eq!(ordinals(&narrower), [5, 4]);
}

#[test]
fn binary_text_membership_uses_bytewise_prefix_order_and_one_global_cursor() {
    let ascending = execute_all_membership_cursor_pages(
        BINARY_COMPONENT_MEMBERSHIP_QUERY,
        &["rel-3", "rel6", "rel-3"],
    );
    assert_eq!(ordinals(&ascending), [3, 2]);

    let descending_source = BINARY_COMPONENT_MEMBERSHIP_QUERY
        .replace("relation asc", "relation desc")
        .replace("user asc", "user desc")
        .replace("document_id asc", "document_id desc");
    let descending = execute_all_membership_cursor_pages(&descending_source, &["rel6", "rel-3"]);
    assert_eq!(ordinals(&descending), [2, 3]);

    assert!(execute_all_membership_cursor_pages(BINARY_COMPONENT_MEMBERSHIP_QUERY, &[]).is_empty());
}

// req: OQ-036
#[test]
fn canonical_intervals_and_complements_share_exact_forward_reverse_cursors() {
    let ascending = execute_all_interval_cursor_pages(CANONICAL_RANGE_QUERY, Some((3, 7)), None);
    assert_eq!(ordinals(&ascending), [3, 4, 5, 6]);

    let descending_source = CANONICAL_RANGE_QUERY
        .replace("sequence asc", "sequence desc")
        .replace("document_id asc", "document_id desc");
    let descending = execute_all_interval_cursor_pages(&descending_source, Some((3, 7)), None);
    assert_eq!(ordinals(&descending), [6, 5, 4, 3]);

    let complement = execute_all_interval_cursor_pages(CANONICAL_COMPLEMENT_QUERY, None, Some(4));
    assert_eq!(ordinals(&complement), [2, 3, 5, 6, 7]);

    let reverse_complement_source = CANONICAL_COMPLEMENT_QUERY
        .replace("sequence asc", "sequence desc")
        .replace("document_id asc", "document_id desc");
    let reverse_complement =
        execute_all_interval_cursor_pages(&reverse_complement_source, None, Some(4));
    assert_eq!(ordinals(&reverse_complement), [7, 6, 5, 3, 2]);

    assert!(
        execute_all_interval_cursor_pages(CANONICAL_RANGE_QUERY, Some((7, 3)), None).is_empty()
    );
}

#[test]
fn binary_text_intervals_and_complements_share_bytewise_forward_reverse_cursors() {
    let ascending =
        execute_all_text_interval_cursor_pages(BINARY_TEXT_RANGE_QUERY, Some(("ab", "doc6")), None);
    assert_eq!(titles(&ascending), ["abacus", "ac", "doc-3"]);

    let descending_source = BINARY_TEXT_RANGE_QUERY
        .replace("title asc", "title desc")
        .replace("document_id asc", "document_id desc");
    let descending =
        execute_all_text_interval_cursor_pages(&descending_source, Some(("ab", "doc6")), None);
    assert_eq!(titles(&descending), ["doc-3", "ac", "abacus"]);

    let complement =
        execute_all_text_interval_cursor_pages(BINARY_TEXT_COMPLEMENT_QUERY, None, Some("ac"));
    assert_eq!(titles(&complement), ["a", "ab", "abacus", "doc-3", "doc6"]);

    assert!(
        execute_all_text_interval_cursor_pages(
            BINARY_TEXT_RANGE_QUERY,
            Some(("doc6", "ab")),
            None,
        )
        .is_empty()
    );
}
