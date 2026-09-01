//! Complete candidate intersection, mixed root ordering, and cursor coverage.

use std::collections::BTreeMap;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::compile_query;
use riffdb_query_executor::{
    BoundPredicate, LongPatternCandidateBatch, QueryBackendFault, QueryContinuation,
    QueryExecutionError, QueryNearestPage, QueryParameters, QueryReadView, QueryResultValue,
    QueryRow, QueryScanPage, execute_page_in_snapshot, execute_provider_page_in_snapshot,
};
use riffdb_query_ir::{QueryAccessKind, QueryAccessStep, SymbolicCatalog};
use riffdb_riffql_syntax::parse_query;
use riffdb_types::{
    ApplicationRoleHash, CanonicalValue, CommitSequence, MAX_BYTES_VALUE_BYTES,
    ProjectionGeneration,
};

const CONTRACT: &str = r#"
contract CandidateExecution version 1 {
  entity Experiment {
    key (scope: string<32>, experiment_id: u64)
    field last_update_time: i64
    index by_updated (scope, last_update_time, experiment_id)
  }
  entity ExperimentTag {
    key (scope: string<32>, experiment_id: u64, tag_key: string<250>)
    field value_digest: bytes<32>
    index by_tag_digest (scope, tag_key, value_digest, experiment_id)
    reference experiment (scope, experiment_id) -> Experiment(scope, experiment_id)
  }
  aggregate Experiments { root Experiment partition_by scope conflict_key (scope, experiment_id) }
  aggregate ExperimentTags { root ExperimentTag partition_by scope conflict_key (scope, experiment_id) }
}
"#;

const QUERY: &str = r#"query MatchTags(
    $scope: Experiment.scope,
    $key: ExperimentTag.tag_key,
    $digest: ExperimentTag.value_digest,
    $limit: Limit<10> = 1,
    $after: Cursor?
) {
    candidates matching: Experiment.experiment_id
        from intersect {
            ExperimentTag.experiment_id using by_tag_digest where scope == $scope && tag_key == $key && value_digest == $digest,
            ExperimentTag.experiment_id using by_tag_digest where scope == $scope && tag_key == $key && value_digest == $digest,
        }
        within 65535
        else IntegrityFailure
    many experiments from Experiment
        where scope == $scope && experiment_id in matching
        order by last_update_time desc, experiment_id asc
        take $limit after $after
        else IntegrityFailure
    return Found { experiments: experiments { experiment_id, last_update_time } }
    outcomes Found | IntegrityFailure
}"#;

#[derive(Default)]
struct CandidateView {
    continue_first_source: bool,
    include_null_root: bool,
    one_per_partition: bool,
    root_reads: usize,
}

impl QueryReadView for CandidateView {
    type Error = ();

    fn fault(&self, _error: &Self::Error) -> QueryBackendFault {
        QueryBackendFault::Integrity
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
        Err(())
    }

    fn dependent_point_batch(
        &mut self,
        _step: &QueryAccessStep,
        predicates: &[Vec<BoundPredicate>],
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
        self.root_reads += predicates.len();
        Ok(predicates
            .iter()
            .map(|predicates| {
                let scope = predicates
                    .iter()
                    .find(|predicate| predicate.field() == "scope")
                    .and_then(|predicate| match predicate.value() {
                        CanonicalValue::String(value) => Some(value.as_str()),
                        _ => None,
                    })?;
                let id = predicates
                    .iter()
                    .find(|predicate| predicate.field() == "experiment_id")
                    .and_then(|predicate| match predicate.value() {
                        CanonicalValue::U64(value) => Some(*value),
                        _ => None,
                    })?;
                Some(row(
                    "Experiment",
                    [
                        ("scope", text(scope)),
                        ("experiment_id", CanonicalValue::U64(id)),
                        (
                            "last_update_time",
                            if id == 3 {
                                CanonicalValue::Null
                            } else {
                                CanonicalValue::I64(if id == 1 { 10 } else { 20 })
                            },
                        ),
                        ("name", text(if id == 1 { "Zeta" } else { "Alpha" })),
                        ("creation_time", CanonicalValue::I64(id as i64)),
                    ],
                ))
            })
            .collect())
    }

    fn scan(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        _limit: u64,
        _after: Option<&[u8]>,
        _after_inclusive: bool,
        _policy: Option<&riffdb_policy::AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, Self::Error> {
        let scope = predicates
            .iter()
            .find(|predicate| predicate.field() == "scope")
            .and_then(|predicate| match predicate.value() {
                CanonicalValue::String(value) => Some(value.as_str()),
                _ => None,
            })
            .ok_or(())?;
        let ids: &[u64] = if self.one_per_partition {
            &[1]
        } else if step.binding().ends_with(":0") || self.include_null_root {
            &[1, 2, 3]
        } else {
            &[1, 2]
        };
        let rows = ids
            .iter()
            .map(|id| {
                row(
                    "ExperimentTag",
                    [
                        ("scope", text(scope)),
                        ("experiment_id", CanonicalValue::U64(*id)),
                        ("tag_key", text("kind")),
                        ("value_digest", bytes(&[9; 32])),
                    ],
                )
            })
            .collect();
        if self.continue_first_source && step.binding().ends_with(":0") {
            return QueryScanPage::continued(rows, 11, ids.len() as u64, vec![1]).ok_or(());
        }
        Ok(QueryScanPage::exact_end(rows, 11))
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

#[test]
fn partition_set_candidates_remain_scoped_before_global_root_order() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let query = QUERY
        .replace(
            "$scope: Experiment.scope",
            "$scopes: Set<Experiment.scope, 2>",
        )
        .replace("scope == $scope", "scope in $scopes")
        .replace("within 65535", "within 10")
        .replace(
            "experiments { experiment_id, last_update_time }",
            "experiments { scope, experiment_id, last_update_time }",
        );
    let program = compile_query(&parse_query(&query).expect("query"), &catalog).expect("plan");
    let parameters = QueryParameters::checked(BTreeMap::from([
        (
            "scopes".to_owned(),
            CanonicalValue::list(vec![text("org-b"), text("org-a")]).expect("set"),
        ),
        ("key".to_owned(), text("kind")),
        ("digest".to_owned(), bytes(&[9; 32])),
        ("limit".to_owned(), CanonicalValue::U64(1)),
    ]))
    .expect("parameters");

    let first = execute_page_in_snapshot(
        &program,
        &parameters,
        None,
        &mut CandidateView {
            one_per_partition: true,
            ..CandidateView::default()
        },
    )
    .expect("first scoped candidate page");
    assert_eq!(page_scopes(&first), vec!["org-a"]);
    let prior = QueryContinuation::checked(
        first.continuation_binding().expect("binding").to_owned(),
        first.continuation().expect("continuation").to_vec(),
        first.index_epochs().clone(),
    )
    .expect("continuation");
    let second = execute_page_in_snapshot(
        &program,
        &parameters,
        Some(&prior),
        &mut CandidateView {
            one_per_partition: true,
            ..CandidateView::default()
        },
    )
    .expect("second scoped candidate page");
    assert_eq!(page_scopes(&second), vec!["org-b"]);
    assert!(second.continuation().is_none());
}

#[test]
fn every_candidate_completes_before_mixed_root_order_and_cursor_selection() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let program = compile_query(&parse_query(QUERY).expect("query"), &catalog).expect("plan");
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("scope".to_owned(), text("org")),
        ("key".to_owned(), text("kind")),
        ("digest".to_owned(), bytes(&[9; 32])),
        ("limit".to_owned(), CanonicalValue::U64(1)),
    ]))
    .expect("parameters");

    let first =
        execute_page_in_snapshot(&program, &parameters, None, &mut CandidateView::default())
            .expect("first candidate page");
    assert_eq!(page_ids(&first), vec![2]);
    let prior = QueryContinuation::checked(
        first.continuation_binding().expect("binding").to_owned(),
        first.continuation().expect("continuation").to_vec(),
        first.index_epochs().clone(),
    )
    .expect("checked continuation");
    let second = execute_page_in_snapshot(
        &program,
        &parameters,
        Some(&prior),
        &mut CandidateView::default(),
    )
    .expect("second candidate page");
    assert_eq!(page_ids(&second), vec![1]);
    assert!(second.continuation().is_none());
}

#[test]
fn incomplete_candidate_source_refuses_before_any_root_hydration() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let program = compile_query(&parse_query(QUERY).expect("query"), &catalog).expect("plan");
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("scope".to_owned(), text("org")),
        ("key".to_owned(), text("kind")),
        ("digest".to_owned(), bytes(&[9; 32])),
        ("limit".to_owned(), CanonicalValue::U64(1)),
    ]))
    .expect("parameters");
    let mut view = CandidateView {
        continue_first_source: true,
        include_null_root: false,
        one_per_partition: false,
        root_reads: 0,
    };
    assert_eq!(
        execute_page_in_snapshot(&program, &parameters, None, &mut view),
        Err(QueryExecutionError::BoundExceeded)
    );
    assert_eq!(view.root_reads, 0, "no partial candidate may hydrate roots");
}

#[test]
fn explicit_null_placement_is_independent_from_descending_direction() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let query = QUERY.replace(
        "order by last_update_time desc, experiment_id asc",
        "order by last_update_time desc nulls last, experiment_id asc",
    );
    let program = compile_query(&parse_query(&query).expect("query"), &catalog).expect("plan");
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("scope".to_owned(), text("org")),
        ("key".to_owned(), text("kind")),
        ("digest".to_owned(), bytes(&[9; 32])),
        ("limit".to_owned(), CanonicalValue::U64(10)),
    ]))
    .expect("parameters");
    let mut view = CandidateView {
        continue_first_source: false,
        include_null_root: true,
        one_per_partition: false,
        root_reads: 0,
    };
    let page = execute_page_in_snapshot(&program, &parameters, None, &mut view).expect("page");
    assert_eq!(page_ids(&page), vec![2, 1, 3]);
    assert!(page.continuation().is_none());
}

#[test]
fn provider_candidates_share_one_exact_head_before_root_order_and_page() {
    let contract =
        include_str!("../../../fixtures/riffql/bounded-filtered-result-v1/contract.riff");
    let query = include_str!(
        "../../../fixtures/riffql/bounded-filtered-result-v1/queries/combined_tag_pattern.riffq"
    )
    .replace("Limit<50000>", "Limit<10>")
    .replace("= 1000", "= 1");
    let bundle = compile_contract_source(contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let program = compile_query(&parse_query(&query).expect("query"), &catalog).expect("plan");
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("scope".to_owned(), text("org")),
        ("tag_key".to_owned(), text("kind")),
        ("tag_digest".to_owned(), bytes(&[9; 32])),
        ("name_pattern".to_owned(), text("%a%")),
        ("limit".to_owned(), CanonicalValue::U64(1)),
    ]))
    .expect("parameters");
    let provider_step = program
        .steps()
        .iter()
        .find(|step| matches!(step.access(), QueryAccessKind::LongPatternCandidate { .. }))
        .expect("provider step");
    let QueryAccessKind::LongPatternCandidate { pattern, .. } = provider_step.access() else {
        panic!("provider")
    };
    let epoch = CommitSequence::new(7).expect("epoch");
    let generation = ProjectionGeneration::first();
    let batch = LongPatternCandidateBatch::checked(
        provider_step.binding().to_owned(),
        vec![
            row(
                "Experiment",
                [
                    ("scope", text("org")),
                    ("experiment_id", CanonicalValue::U64(1)),
                    ("name", text("Zeta")),
                ],
            ),
            row(
                "Experiment",
                [
                    ("scope", text("org")),
                    ("experiment_id", CanonicalValue::U64(2)),
                    ("name", text("Alpha")),
                ],
            ),
        ],
        pattern.descriptor().digest(),
        pattern.descriptor().state_identity().schema_hash(),
        1,
        generation,
        epoch,
        epoch,
        2,
        9,
    )
    .expect("batch");
    let policy_shape = ApplicationRoleHash::from_bytes([3; 32]);
    let proof = riffdb_projection::negotiate_result_set_epoch_v1(
        riffdb_projection::ResultSetEpochContextV1::new(program.identity().hash(), policy_shape),
        &[batch.observation().expect("observation")],
        riffdb_projection::ResultSetEpochRequirementV1::Exact(epoch),
    )
    .expect("proof");
    let mut view = CandidateView::default();
    let page = execute_provider_page_in_snapshot(
        &program,
        &parameters,
        None,
        &mut view,
        policy_shape,
        &proof,
        &[batch],
    )
    .expect("provider page");
    assert_eq!(page_ids(&page), vec![2]);
    assert_eq!(
        view.root_reads, 2,
        "all intersected roots hydrate before paging"
    );

    let stale = CommitSequence::new(6).expect("stale");
    let stale_proof = riffdb_projection::negotiate_result_set_epoch_v1(
        riffdb_projection::ResultSetEpochContextV1::new(program.identity().hash(), policy_shape),
        &[riffdb_projection::ProviderEpochObservationV1::new(
            pattern.descriptor().digest(),
            pattern.descriptor().state_identity().schema_hash(),
            1,
            generation,
            stale,
            stale,
            riffdb_projection::ProviderLifecycleV1::Ready,
        )
        .expect("stale observation")],
        riffdb_projection::ResultSetEpochRequirementV1::Exact(stale),
    )
    .expect("stale proof");
    assert_eq!(
        execute_provider_page_in_snapshot(
            &program,
            &parameters,
            None,
            &mut CandidateView::default(),
            policy_shape,
            &stale_proof,
            &[],
        ),
        Err(QueryExecutionError::BackendUnavailable)
    );
}

#[test]
fn partition_provider_batches_merge_before_scoped_candidate_hydration() {
    let contract =
        include_str!("../../../fixtures/riffql/bounded-filtered-result-v1/contract.riff");
    let query = include_str!(
        "../../../fixtures/riffql/bounded-filtered-result-v1/queries/combined_tag_pattern.riffq"
    )
    .replace(
        "$scope: Experiment.scope",
        "$scopes: Set<Experiment.scope, 2>",
    )
    .replace("scope == $scope", "scope in $scopes")
    .replace("Limit<50000>", "Limit<10>")
    .replace("= 1000", "= 10")
    .replace(
        "experiments { experiment_id, name, last_update_time }",
        "experiments { scope, experiment_id, name, last_update_time }",
    );
    let bundle = compile_contract_source(contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let program = compile_query(&parse_query(&query).expect("query"), &catalog).expect("plan");
    let parameters = QueryParameters::checked(BTreeMap::from([
        (
            "scopes".to_owned(),
            CanonicalValue::list(vec![text("org-b"), text("org-a")]).expect("set"),
        ),
        ("tag_key".to_owned(), text("kind")),
        ("tag_digest".to_owned(), bytes(&[9; 32])),
        ("name_pattern".to_owned(), text("%a%")),
        ("limit".to_owned(), CanonicalValue::U64(10)),
    ]))
    .expect("parameters");
    let provider_step = program
        .steps()
        .iter()
        .find(|step| matches!(step.access(), QueryAccessKind::LongPatternCandidate { .. }))
        .expect("provider step");
    let QueryAccessKind::LongPatternCandidate { pattern, .. } = provider_step.access() else {
        panic!("provider")
    };
    let epoch = CommitSequence::new(7).expect("epoch");
    let generation = ProjectionGeneration::first();
    let batches = ["org-a", "org-b"]
        .into_iter()
        .map(|scope| {
            LongPatternCandidateBatch::checked(
                provider_step.binding().to_owned(),
                vec![row(
                    "Experiment",
                    [
                        ("scope", text(scope)),
                        ("experiment_id", CanonicalValue::U64(1)),
                        ("name", text("Alpha")),
                    ],
                )],
                pattern.descriptor().digest(),
                pattern.descriptor().state_identity().schema_hash(),
                1,
                generation,
                epoch,
                epoch,
                1,
                9,
            )
            .expect("partition batch")
        })
        .collect::<Vec<_>>();
    let merged = LongPatternCandidateBatch::merge_partition_batches(batches, 2, 2, 18)
        .expect("merged provider batch");
    let policy_shape = ApplicationRoleHash::from_bytes([4; 32]);
    let proof = riffdb_projection::negotiate_result_set_epoch_v1(
        riffdb_projection::ResultSetEpochContextV1::new(program.identity().hash(), policy_shape),
        &[merged.observation().expect("observation")],
        riffdb_projection::ResultSetEpochRequirementV1::Exact(epoch),
    )
    .expect("proof");
    let page = execute_provider_page_in_snapshot(
        &program,
        &parameters,
        None,
        &mut CandidateView {
            one_per_partition: true,
            ..CandidateView::default()
        },
        policy_shape,
        &proof,
        &[merged],
    )
    .expect("partition provider page");
    assert_eq!(page_scopes(&page), vec!["org-a", "org-b"]);
}

fn page_ids(snapshot: &riffdb_query_executor::QueryOwnedSnapshot) -> Vec<u64> {
    let QueryResultValue::Many(rows) = &snapshot.fields()["experiments"] else {
        panic!("many result")
    };
    rows.iter()
        .map(|row| match row.field("experiment_id") {
            Some(CanonicalValue::U64(value)) => *value,
            _ => panic!("experiment id"),
        })
        .collect()
}

fn page_scopes(snapshot: &riffdb_query_executor::QueryOwnedSnapshot) -> Vec<&str> {
    let QueryResultValue::Many(rows) = &snapshot.fields()["experiments"] else {
        panic!("many result")
    };
    rows.iter()
        .map(|row| match row.field("scope") {
            Some(CanonicalValue::String(value)) => value.as_str(),
            _ => panic!("scope"),
        })
        .collect()
}

fn row<const N: usize>(entity: &str, fields: [(&str, CanonicalValue); N]) -> QueryRow {
    QueryRow::checked(
        entity.to_owned(),
        fields
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    )
    .expect("row")
}

fn text(value: &str) -> CanonicalValue {
    CanonicalValue::string(value.to_owned()).expect("text")
}

fn bytes(value: &[u8]) -> CanonicalValue {
    assert!(value.len() <= MAX_BYTES_VALUE_BYTES);
    CanonicalValue::bytes(value.to_vec()).expect("bytes")
}
