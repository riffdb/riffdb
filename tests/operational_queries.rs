//! WP-564 closed operational-index execution acceptance.

use std::collections::BTreeMap;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::compile_operational_query_family;
use riffdb_query_executor::{
    BoundPredicate, QueryBackendFault, QueryContinuation, QueryExecutionError, QueryOwnedSnapshot,
    QueryParameters, QueryReadView, QueryResultValue, QueryRow, QueryScanPage,
    bound_index_prefix_bytes_v1, execute_operational_page_in_snapshot,
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
    index by_deleted (organization_id, deleted_at, document_id) presence(deleted_at)
    index by_title (organization_id, title, document_id) text_key(title, binary_utf8_v1)
    index by_relation_user (organization_id, relation, user, document_id)
      text_key(relation, binary_utf8_v1)
      text_key(user, binary_utf8_v1)
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
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, Self::Error> {
        let prefixes = bound_index_prefix_bytes_v1(step, predicates).expect("sealed prefixes");
        let direction = match step.access() {
            QueryAccessKind::Index { direction, .. } => *direction,
            _ => panic!("operational acceptance scan requires an index"),
        };
        let mut candidates = self
            .rows
            .iter()
            .filter(|(key, _)| prefixes.iter().any(|prefix| key.starts_with(prefix)))
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

fn execute_page(
    source: &str,
    prefix: Option<&str>,
    relation: Option<&str>,
    relations: Option<&[&str]>,
    prior: Option<&QueryContinuation>,
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
    if let Some(prefix) = prefix {
        parameter_values.insert(
            "prefix".to_owned(),
            CanonicalValue::string(prefix).expect("prefix"),
        );
    }
    if let Some(relation) = relation {
        parameter_values.insert(
            "relation".to_owned(),
            CanonicalValue::string(relation).expect("relation"),
        );
    }
    if let Some(relations) = relations {
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
    let parameters = QueryParameters::checked(parameter_values).expect("parameters");
    let mut view = IndexedView { rows: indexed };
    execute_operational_page_in_snapshot(
        program,
        family.aggregates(),
        &parameters,
        prior,
        &mut view,
    )
}

fn execute(source: &str, prefix: Option<&str>) -> Result<Vec<QueryRow>, QueryExecutionError> {
    let snapshot = execute_page(source, prefix, None, None, None)?;
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

fn execute_all_cursor_pages(source: &str, relation: Option<&str>) -> Vec<QueryRow> {
    let mut rows = Vec::new();
    let mut prior = None;
    loop {
        let snapshot =
            execute_page(source, None, relation, None, prior.as_ref()).expect("binary-order page");
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
        let snapshot = execute_page(source, None, None, Some(relations), prior.as_ref())
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
