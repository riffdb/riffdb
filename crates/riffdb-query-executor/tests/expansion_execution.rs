//! One-snapshot bounded expansion execution proofs.

use std::collections::BTreeMap;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::compile_query;
use riffdb_query_executor::{
    BoundPredicate, QueryBackendFault, QueryExecutionError, QueryNearestPage, QueryParameters,
    QueryReadView, QueryResultValue, QueryRow, QueryScanPage, execute_in_snapshot,
};
use riffdb_query_ir::{QueryAccessStep, SymbolicCatalog};
use riffdb_riffql_syntax::parse_query;
use riffdb_types::CanonicalValue;

const CONTRACT: &str = r#"
contract ExpansionExecution version 1 {
  entity Parent {
    key (scope: u64, parent_id: u64)
    index by_scope (scope, parent_id)
  }
  entity Child {
    key (scope: u64, parent_id: u64, child_id: u64)
    field created: u64
    index by_parent (scope, parent_id, created, child_id)
    reference parent (scope, parent_id) -> Parent(scope, parent_id)
  }
  aggregate Parents { root Parent partition_by scope conflict_key (scope, parent_id) }
  aggregate Children { root Child partition_by scope conflict_key (scope, parent_id) }
}
"#;

const QUERY: &str = r#"
query ParentsWithChildren($scope: Parent.scope) {
  many parents from Parent
    where scope == $scope
    order by parent_id asc
    take 2

  many children from Child
    for each parent in parents
    where scope == $scope && parent_id == parent.parent_id
    order by created asc, child_id asc
    take 2 per parent

  return Found {
    parents: parents {
      parent_id
      children: children { child_id created }
    }
  }
  outcomes Found
}
"#;

#[derive(Default)]
struct ExpansionView {
    overflow_parent: Option<u64>,
    mutation_on_parent: Option<u64>,
    calls: Vec<(String, Option<u64>, u64)>,
}

impl QueryReadView for ExpansionView {
    type Error = ();

    fn fault(&self, _error: &Self::Error) -> QueryBackendFault {
        QueryBackendFault::Integrity
    }

    fn application_head(&self) -> u64 {
        17
    }

    fn point(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<QueryRow>, Self::Error> {
        Err(())
    }

    fn dependent_point_batch(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[Vec<BoundPredicate>],
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
        Err(())
    }

    fn scan(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        _after: Option<&[u8]>,
        _after_inclusive: bool,
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, Self::Error> {
        let parent = predicates.iter().find_map(|predicate| {
            (predicate.field() == "parent_id").then(|| match predicate.value() {
                CanonicalValue::U64(value) => Some(*value),
                _ => None,
            })?
        });
        self.calls.push((step.binding().to_owned(), parent, limit));
        if step.binding() == "parents" {
            return Ok(QueryScanPage::exact_end(
                vec![parent_row(1), parent_row(2)],
                9,
            ));
        }
        let parent = parent.ok_or(())?;
        let rows = vec![
            child_row(parent, parent * 10 + 1),
            child_row(parent, parent * 10 + 2),
        ];
        if self.overflow_parent == Some(parent) {
            QueryScanPage::continued(rows, 9, 3, vec![parent as u8]).ok_or(())
        } else {
            Ok(QueryScanPage::exact_end(
                rows,
                if self.mutation_on_parent == Some(parent) {
                    10
                } else {
                    9
                },
            ))
        }
    }

    fn nearest(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _k: u32,
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryNearestPage, Self::Error> {
        Err(())
    }
}

fn row(entity: &str, fields: impl IntoIterator<Item = (&'static str, CanonicalValue)>) -> QueryRow {
    QueryRow::checked(
        entity.to_owned(),
        fields
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    )
    .expect("bounded row")
}

fn parent_row(parent_id: u64) -> QueryRow {
    row(
        "Parent",
        [
            ("scope", CanonicalValue::U64(7)),
            ("parent_id", CanonicalValue::U64(parent_id)),
        ],
    )
}

fn child_row(parent_id: u64, child_id: u64) -> QueryRow {
    row(
        "Child",
        [
            ("scope", CanonicalValue::U64(7)),
            ("parent_id", CanonicalValue::U64(parent_id)),
            ("child_id", CanonicalValue::U64(child_id)),
            ("created", CanonicalValue::U64(child_id)),
        ],
    )
}

fn program() -> riffdb_query_ir::QueryAccessProgramV1 {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    compile_query(&parse_query(QUERY).expect("query"), &catalog).expect("program")
}

fn parameters() -> QueryParameters {
    QueryParameters::checked(BTreeMap::from([(
        "scope".to_owned(),
        CanonicalValue::U64(7),
    )]))
    .expect("parameters")
}

// req: OQ-114
#[test]
fn expansion_completes_every_driver_before_release_or_refuses_whole() {
    let program = program();
    let expansion = program
        .steps()
        .iter()
        .find(|step| step.binding() == "children")
        .expect("expansion step");
    assert_eq!(expansion.selected_fields(), &["child_id", "created"]);
    assert_eq!(expansion.result_names(), &["children"]);
    let mut view = ExpansionView::default();
    let snapshot = execute_in_snapshot(&program, &parameters(), &mut view).expect("expansion");
    let QueryResultValue::Many(parents) = &snapshot.fields()["parents"] else {
        panic!("parents must retain many cardinality");
    };
    assert_eq!(parents.len(), 2);
    assert_eq!(parents[0].nested_rows("children").expect("nested").len(), 2);
    assert_eq!(
        parents[1].nested_rows("children").expect("nested")[0].field("child_id"),
        Some(&CanonicalValue::U64(21)),
    );
    assert_eq!(
        view.calls,
        vec![
            ("parents".to_owned(), None, 2),
            ("children".to_owned(), Some(1), 2),
            ("children".to_owned(), Some(2), 2),
        ],
    );

    let mut overflow = ExpansionView {
        overflow_parent: Some(2),
        ..ExpansionView::default()
    };
    assert_eq!(
        execute_in_snapshot(&program, &parameters(), &mut overflow),
        Err(QueryExecutionError::BoundExceeded),
        "a late driver overflow releases no partial snapshot",
    );

    let mut concurrent_mutation = ExpansionView {
        mutation_on_parent: Some(2),
        ..ExpansionView::default()
    };
    assert_eq!(
        execute_in_snapshot(&program, &parameters(), &mut concurrent_mutation),
        Err(QueryExecutionError::BackendIntegrity),
        "mixed index epochs cannot release a cross-snapshot expansion",
    );
}
