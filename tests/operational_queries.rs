//! WP-564 closed operational-index execution acceptance.

use std::collections::BTreeMap;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::compile_operational_query_family;
use riffdb_query_executor::{
    BoundPredicate, QueryBackendFault, QueryExecutionError, QueryParameters, QueryReadView,
    QueryResultValue, QueryRow, QueryScanPage, bound_index_prefix_bytes_v1,
    execute_operational_page_in_snapshot,
};
use riffdb_query_ir::{QueryAccessStep, SymbolicCatalog};
use riffdb_riffql_syntax::parse_query;
use riffdb_types::{CanonicalRecord, CanonicalValue, Timestamp};

const CONTRACT: &str = r#"
contract OperationalQueries version 1 {
  entity Document {
    key (organization_id: uuid, document_id: uuid)
    field deleted_at: optional<timestamp>
    field title: string<64>
    index by_deleted (organization_id, deleted_at, document_id) presence(deleted_at)
    index by_title (organization_id, title, document_id) text_key(title, binary_utf8_v1)
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
    order by deleted_at asc, document_id asc
    take 10
  return Found { documents: documents { document_id deleted_at } }
  outcomes Found
}
"#;

const EXISTS_QUERY: &str = r#"
query ExistingDocuments($organization_id: Document.organization_id) {
  many documents from Document
    where organization_id == $organization_id && exists deleted_at
    order by deleted_at asc, document_id asc
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
    ) -> Result<Option<QueryRow>, Self::Error> {
        Ok(None)
    }

    fn dependent_point_batch(
        &mut self,
        _step: &QueryAccessStep,
        predicates: &[Vec<BoundPredicate>],
    ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
        Ok(vec![None; predicates.len()])
    }

    fn scan(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
    ) -> Result<QueryScanPage, Self::Error> {
        assert!(after.is_none(), "acceptance page fits in one bounded read");
        let prefixes = bound_index_prefix_bytes_v1(step, predicates).expect("sealed prefixes");
        let rows = self
            .rows
            .iter()
            .filter(|(key, _)| prefixes.iter().any(|prefix| key.starts_with(prefix)))
            .take(usize::try_from(limit).expect("bounded limit"))
            .map(|(_, row)| row.clone())
            .collect();
        Ok(QueryScanPage::exact_end(rows, 1))
    }

    fn nearest(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _k: u32,
    ) -> Result<Vec<QueryRow>, Self::Error> {
        Ok(Vec::new())
    }
}

fn execute(source: &str, prefix: Option<&str>) -> Result<Vec<QueryRow>, QueryExecutionError> {
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
            deleted_at: Presence::Missing,
        },
        Document {
            ordinal: 3,
            title: "ab",
            deleted_at: Presence::Null,
        },
        Document {
            ordinal: 4,
            title: "abacus",
            deleted_at: Presence::Value(Timestamp::new(10, 0).expect("timestamp")),
        },
        Document {
            ordinal: 5,
            title: "ac",
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
    let parameters = QueryParameters::checked(parameter_values).expect("parameters");
    let mut view = IndexedView { rows: indexed };
    let snapshot = execute_operational_page_in_snapshot(
        program,
        family.aggregates(),
        &parameters,
        None,
        &mut view,
    )?;
    match snapshot.fields().get("documents") {
        Some(QueryResultValue::Many(rows)) => Ok(rows.clone()),
        other => panic!("expected documents result, got {other:?}"),
    }
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
