use super::*;
use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::{compile_operational_query_family, compile_query};
use riffdb_query_ir::{QueryAccessStep, SymbolicCatalog};
use riffdb_riffql_syntax::parse_query;
use riffdb_types::CanonicalRecord;

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
const OPEN_TICKETS: &str = r#"
query OpenTickets(
    $organization_id: Organization.organization_id,
    $project_id: Project.project_id,
) {
    many tickets from Ticket
        where organization_id == $organization_id
            && project_id == $project_id
            && status == TicketStatus.Open
        order by ticket_id asc
        take 5
    return Found { tickets: tickets { ticket_id status } }
    outcomes Found
}
"#;

const AGGREGATE_CONTRACT: &str = r#"
contract OperationalAggregate version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field status: string<32>
    field story_points: i64
    field label: optional<string<32>>
    field enabled: bool
    index by_status (organization_id, status, ticket_id)
  }
  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }
}
"#;

const GROUPED_SUMMARY: &str = r#"
query GroupedSummary($organization_id: Ticket.organization_id) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by status asc, ticket_id asc
        take 5
    aggregate summary from tickets {
        group by status
        count() as ticket_count
        sum(story_points) as total_points
    }
    return Found { summary: summary { status ticket_count total_points } }
    outcomes Found
}
"#;

const PARAMETER_BOUNDED_GROUPED_SUMMARY: &str = r#"
query ParameterBoundedGroupedSummary(
    $organization_id: Ticket.organization_id,
    $limit: Limit<499> = 5,
) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by status asc, ticket_id asc
        take $limit
    aggregate summary from tickets {
        group by status
        count() as ticket_count
    }
    return Found { summary: summary { status ticket_count } }
    outcomes Found
}
"#;

const EMPTY_SUMMARY: &str = r#"
query EmptySummary($organization_id: Ticket.organization_id) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by status asc, ticket_id asc
        take 5
    aggregate summary from tickets {
        count() as ticket_count
        sum(story_points) as total_points
        min(story_points) as minimum_points
    }
    return Found { summary: summary { ticket_count total_points minimum_points } }
    outcomes Found
}
"#;

const EXACT_CORE_SUMMARY: &str = r#"
query ExactCoreSummary($organization_id: Ticket.organization_id) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by status asc, ticket_id asc
        take 5
    aggregate summary from tickets {
        count_present(label) as present_labels
        count_distinct(label) as distinct_labels
        count_distinct_present(label) as distinct_present_labels
        mean(story_points) as mean_points
        any(enabled) as any_enabled
        all(enabled) as all_enabled
    }
    return Found { summary: summary {
        present_labels distinct_labels distinct_present_labels
        mean_points any_enabled all_enabled
    } }
    outcomes Found
}
"#;

const DISTINCT_BUDGET_SUMMARY: &str = r#"
query DistinctBudgetSummary($organization_id: Ticket.organization_id) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by status asc, ticket_id asc
        take 300
    aggregate summary from tickets { count_distinct(label) as distinct_labels }
    return Found { summary: summary { distinct_labels } }
    outcomes Found
}
"#;

const DECIMAL_AGGREGATE_CONTRACT: &str = r#"
contract DecimalAggregate version 1 {
  entity Entry {
    key (organization_id: uuid, entry_id: uuid)
    field amount: decimal<38,0>
    index by_entry (organization_id, entry_id)
  }
  aggregate Entries {
    root Entry
    partition_by organization_id
    conflict_key (organization_id, entry_id)
  }
}
"#;

const OVERFLOWING_SUMMARY: &str = r#"
query OverflowingSummary($organization_id: Entry.organization_id) {
    many entries from Entry
        where organization_id == $organization_id
        order by entry_id asc
        take 5
    aggregate summary from entries { sum(amount) as total }
    return Found { summary: summary { total } }
    outcomes Found
}
"#;

const OPERATIONAL_INDEX_CONTRACT: &str = r#"
contract OperationalIndexQueries version 1 {
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

const UNICODE_FOLD_OPERATIONAL_INDEX_CONTRACT: &str = r#"
contract UnicodeFoldOperationalIndexQueries version 1 {
  entity Document {
    key (organization_id: uuid, document_id: uuid)
    field title: string<8>
    index by_title (organization_id, title, document_id) text_key(title, unicode_fold_v1)
  }
  aggregate Documents {
    root Document
    partition_by organization_id
    conflict_key (organization_id, document_id)
  }
}
"#;

const UNICODE_FOLD_EXACT_DOCUMENTS: &str = r#"
query ExactFoldedDocuments(
    $organization_id: Document.organization_id,
    $title: Document.title,
) {
    many documents from Document
        where organization_id == $organization_id && title == $title
        order by document_id asc
        take 10
    return Found { documents: documents { document_id title } }
    outcomes Found
}
"#;

const NULL_DOCUMENTS: &str = r#"
query NullDocuments($organization_id: Document.organization_id) {
    many documents from Document
        where organization_id == $organization_id && deleted_at is null
        order by document_id asc
        take 10
    return Found { documents: documents { document_id deleted_at } }
    outcomes Found
}
"#;

const EXISTING_DOCUMENTS: &str = r#"
query ExistingDocuments($organization_id: Document.organization_id) {
    many documents from Document
        where organization_id == $organization_id && exists deleted_at
        order by deleted_at asc nulls first, document_id asc
        take 10
    return Found { documents: documents { document_id deleted_at } }
    outcomes Found
}
"#;

const PREFIX_DOCUMENTS: &str = r#"
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

const BINARY_TEXT_INTERVAL_DOCUMENTS: &str = r#"
query BinaryTextIntervalDocuments(
    $organization_id: Document.organization_id,
    $lower: Document.title,
    $upper: Document.title,
) {
    many documents from Document
        where organization_id == $organization_id
          && title >= $lower
          && title < $upper
        order by title asc, document_id asc
        take 10
    return Found { documents: documents { document_id title } }
    outcomes Found
}
"#;

const PARTITION_SET_CONTRACT: &str = r#"
contract PartitionSetExecution version 1 {
  enum LifecycleStage { Active, Deleted }

  entity Run {
    key (experiment_id: u64, run_id: string<32>)
    field start_time: i64
    field lifecycle_stage: LifecycleStage
    index by_start (experiment_id, start_time, run_id)
    index by_lifecycle_start (experiment_id, lifecycle_stage, start_time, run_id)
  }
  aggregate Runs {
    root Run
    partition_by experiment_id
    conflict_key (experiment_id, run_id)
  }
}
"#;

const FILTERED_PARTITION_SET_QUERY: &str = r#"
query SearchRunsByLifecycle(
    $experiment_ids: Set<Run.experiment_id, 4>,
    $lifecycle_stage: Run.lifecycle_stage,
    $limit: Limit<3> = 1,
    $after: Cursor?,
) {
    many runs from Run
        where experiment_id in $experiment_ids
          && lifecycle_stage == $lifecycle_stage
        order by start_time desc, run_id asc
        take $limit after $after
    return Found { runs: runs { experiment_id run_id start_time lifecycle_stage } }
    outcomes Found
}
"#;

const PARTITION_SET_QUERY: &str = r#"
query SearchRuns(
    $experiment_ids: Set<Run.experiment_id, 4>,
    $limit: Limit<3> = 1,
    $after: Cursor?,
) {
    many runs from Run
        where experiment_id in $experiment_ids
        order by start_time desc, run_id asc
        take $limit after $after
    return Found { runs: runs { experiment_id run_id start_time } }
    outcomes Found
}
"#;

const OPTIONAL_MINIMUM_SUMMARY: &str = r#"
query OptionalMinimumSummary($organization_id: Ticket.organization_id) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by status asc, ticket_id asc
        take 5
    aggregate summary from tickets { min(story_points) as minimum_points }
    return Found { summary: summary { minimum_points } }
    outcomes Found
}
"#;

struct FakeView {
    rows: BTreeMap<String, Vec<QueryRow>>,
}

impl QueryReadView for FakeView {
    type Error = ();

    fn fault(&self, _error: &Self::Error) -> QueryBackendFault {
        QueryBackendFault::Unavailable
    }

    fn application_head(&self) -> u64 {
        1
    }

    fn point(
        &mut self,
        step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<QueryRow>, Self::Error> {
        Ok(self
            .rows
            .get(step.binding())
            .and_then(|rows| rows.first())
            .cloned())
    }

    fn dependent_point_batch(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[Vec<BoundPredicate>],
        _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
        Ok(self
            .rows
            .get(step.binding())
            .into_iter()
            .flatten()
            .cloned()
            .map(Some)
            .take(predicates.len())
            .collect())
    }

    fn scan(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        after_inclusive: bool,
        _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, Self::Error> {
        let route = predicates.iter().find_map(|predicate| {
            (predicate.operator() == QueryPredicateOperator::Equal
                && predicate.field() == "experiment_id")
                .then(|| match predicate.value() {
                    CanonicalValue::U64(value) => Some(*value),
                    _ => None,
                })
                .flatten()
        });
        let route_key = route.map(|route| format!("{}#{route}", step.binding()));
        let mut rows = route_key
            .as_ref()
            .and_then(|key| self.rows.get(key))
            .or_else(|| self.rows.get(step.binding()))
            .cloned()
            .unwrap_or_default();
        rows.retain(|row| predicates_match(row, predicates) == Ok(true));
        let offset = match after {
            Some([1, start]) if !after_inclusive => usize::from(*start).min(rows.len()),
            Some(_) if !after_inclusive && limit == 1 && !rows.is_empty() => 1,
            _ => 0,
        };
        rows.drain(..offset);
        let limit = usize::try_from(limit).expect("test limit");
        if rows.len() > limit {
            rows.truncate(limit);
            let next_offset = u8::try_from(offset + limit).expect("test offset");
            QueryScanPage::continued(
                rows,
                route.unwrap_or(1),
                limit as u64 + 1,
                vec![1, next_offset],
            )
            .ok_or(())
        } else {
            Ok(QueryScanPage::exact_end(rows, route.unwrap_or(1)))
        }
    }

    fn scan_covered(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<CoveredResultBatch>, Self::Error> {
        let Some(layout) = step.covered_result_layout() else {
            return Ok(None);
        };
        let page = self.scan(step, predicates, limit, after, false, policy)?;
        let rows = page
            .rows
            .into_iter()
            .map(|row| {
                layout
                    .fields()
                    .iter()
                    .map(|field| {
                        row.field(field.name())
                            .cloned()
                            .unwrap_or(CanonicalValue::Null)
                    })
                    .collect()
            })
            .collect();
        Ok(CoveredResultBatch::checked(
            layout.clone(),
            rows,
            page.epoch,
            page.scanned_rows,
            0,
            page.continuation,
        ))
    }

    fn nearest(
        &mut self,
        step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _k: u32,
        _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryNearestPage, Self::Error> {
        let rows = self.rows.get(step.binding()).cloned().unwrap_or_default();
        let scanned_rows = rows.len() as u64;
        Ok(QueryNearestPage { rows, scanned_rows })
    }
}

fn row(entity: &str, fields: &[(&str, CanonicalValue)]) -> QueryRow {
    QueryRow::checked(
        entity.to_owned(),
        fields
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.clone()))
            .collect(),
    )
    .expect("row")
}

fn compiled_operational_step(
    source: &str,
) -> (riffdb_contract_ir::ContractBundle, QueryAccessStep) {
    let bundle = compile_contract_source(OPERATIONAL_INDEX_CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family = compile_operational_query_family(
        &parse_query(source).expect("operational query"),
        &catalog,
    )
    .expect("operational family");
    (
        bundle,
        family.select(&[]).expect("sole member").program().steps()[0].clone(),
    )
}

struct PagedView {
    rows: Vec<QueryRow>,
    limits: Vec<u64>,
}
impl QueryReadView for PagedView {
    type Error = ();
    fn fault(&self, _: &()) -> QueryBackendFault {
        QueryBackendFault::Unavailable
    }
    fn application_head(&self) -> u64 {
        1
    }
    fn point(
        &mut self,
        _: &QueryAccessStep,
        _: &[BoundPredicate],
        _: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<QueryRow>, ()> {
        Ok(None)
    }
    fn dependent_point_batch(
        &mut self,
        _: &QueryAccessStep,
        _: &[Vec<BoundPredicate>],
        _: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Vec<Option<QueryRow>>, ()> {
        Err(())
    }
    fn nearest(
        &mut self,
        _: &QueryAccessStep,
        _: &[BoundPredicate],
        _: u32,
        _: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryNearestPage, ()> {
        Err(())
    }
    fn scan(
        &mut self,
        _: &QueryAccessStep,
        _: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        _: bool,
        _: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, ()> {
        self.limits.push(limit);
        let start = after
            .map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()) as usize)
            .unwrap_or(0);
        let end = (start + limit as usize).min(self.rows.len());
        let rows = self.rows[start..end].to_vec();
        if end < self.rows.len() {
            QueryScanPage::continued(rows, 1, limit + 1, (end as u64).to_be_bytes().to_vec())
                .ok_or(())
        } else {
            Ok(QueryScanPage::exact_end(rows, 1))
        }
    }
}

// req: OQ-019
#[test]
fn review_partition_buffering_preserves_order_and_maximum_point_budget() {
    let bundle = compile_contract_source(PARTITION_SET_CONTRACT).unwrap();
    let catalog = SymbolicCatalog::from_bundle(&bundle).unwrap();
    let source = PARTITION_SET_QUERY
        .replace("Limit<3>", "Limit<128>")
        .replace("start_time desc", "start_time asc");
    let family =
        compile_operational_query_family(&parse_query(&source).unwrap(), &catalog).unwrap();
    let program = family.select(&[]).unwrap().program();
    let step = &program.steps()[0];
    let QueryAccessKind::PartitionSetIndex {
        fields,
        order,
        key_fields,
        scan_ceiling,
        ..
    } = step.access()
    else {
        panic!("partition step")
    };
    let rows = (0..129)
        .map(|id| {
            row(
                "Run",
                &[
                    ("experiment_id", CanonicalValue::U64(1)),
                    (
                        "run_id",
                        CanonicalValue::string(format!("{id:04}")).unwrap(),
                    ),
                    ("start_time", CanonicalValue::I64(id)),
                ],
            )
        })
        .collect::<Vec<_>>();
    let routes = [CanonicalValue::U64(1)];
    let predicates = [BoundPredicate {
        field: "experiment_id".to_owned(),
        operator: QueryPredicateOperator::In,
        value: CanonicalValue::list(routes.to_vec()).unwrap(),
    }];
    for limit in [1, 2, 63, 64, 127, 128] {
        let mut view = PagedView {
            rows: rows.clone(),
            limits: Vec::new(),
        };
        let mut fuel = QueryExecutionFuel::from_cost(program.cost());
        // Exactly the compiler's row allowance plus one route head.
        fuel.point_reads = 129;
        let (actual, _, more) = execute_partition_set_uniform_merge(
            &mut view,
            &mut fuel,
            step,
            None,
            &predicates,
            &fields[0],
            Some("experiment_ids"),
            &routes,
            fields,
            order,
            key_fields,
            *scan_ceiling,
            limit,
            None,
            None,
        )
        .unwrap();
        assert_eq!(actual, rows[..limit as usize]);
        assert!(more);
        assert!(view.limits.iter().all(|limit| *limit <= 64));
        assert!(view.limits.len() <= 4, "{:?}", view.limits);
        assert!(129 - fuel.point_reads > limit);
        assert!(129 - fuel.point_reads <= 129);
        if limit == 128 {
            assert_eq!(fuel.point_reads, 0);
        }
    }
}

// req: OQ-019
#[test]
fn review_mixed_partition_heap_matches_independent_sort_and_refuses_bad_comparisons() {
    let bundle = compile_contract_source(PARTITION_SET_CONTRACT).unwrap();
    let catalog = SymbolicCatalog::from_bundle(&bundle).unwrap();
    let family =
        compile_operational_query_family(&parse_query(PARTITION_SET_QUERY).unwrap(), &catalog)
            .unwrap();
    let program = family.select(&[]).unwrap().program();
    let QueryAccessKind::PartitionSetIndex {
        fields,
        order,
        key_fields,
        ..
    } = program.steps()[0].access()
    else {
        panic!("partition step")
    };
    let make = |route, id: i64| {
        row(
            "Run",
            &[
                ("experiment_id", CanonicalValue::U64(route)),
                (
                    "run_id",
                    CanonicalValue::string(format!("{id:04}")).unwrap(),
                ),
                ("start_time", CanonicalValue::I64(id % 3)),
            ],
        )
    };
    let mut runs = (1..=4)
        .map(|route| (0..33).map(|id| make(route, id)).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let mut expected = runs.iter().flatten().cloned().collect::<Vec<_>>();
    expected.sort_by_key(|row| {
        let CanonicalValue::I64(time) = row.field("start_time").unwrap() else {
            panic!("time")
        };
        let CanonicalValue::U64(route) = row.field("experiment_id").unwrap() else {
            panic!("route")
        };
        (
            std::cmp::Reverse(*time),
            riffdb_types::encode_canonical_value(row.field("run_id").unwrap()).unwrap(),
            *route,
        )
    });
    for run in &mut runs {
        sort_partition_rows(run, &fields[0], order, key_fields).unwrap();
    }
    runs.push(Vec::new());
    for limit in [0, 1, 3, 64, 131, 132, 133] {
        let (actual, discarded) =
            merge_partition_runs(runs.clone(), limit, &fields[0], order, key_fields).unwrap();
        assert_eq!(actual, expected[..limit.min(expected.len())]);
        assert_eq!(discarded, limit < expected.len());
    }
    let invalid = row(
        "Run",
        &[
            ("run_id", CanonicalValue::string("0001").unwrap()),
            ("start_time", CanonicalValue::I64(1)),
        ],
    );
    assert!(
        merge_partition_runs(
            vec![vec![make(1, 1)], vec![invalid.clone()]],
            2,
            &fields[0],
            order,
            key_fields
        )
        .is_err()
    );
    assert!(
        sort_partition_rows(&mut [make(1, 1), invalid], &fields[0], order, key_fields).is_err()
    );
}

fn matching_range(schedule: &BoundIndexRangeScheduleV1, key: &[u8]) -> bool {
    schedule
        .ranges()
        .iter()
        .any(|range| key >= range.start_inclusive() && key < range.end_exclusive())
}

#[test]
fn bounded_partition_streams_merge_globally_and_resume_with_one_cursor() {
    let bundle = compile_contract_source(PARTITION_SET_CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family = compile_operational_query_family(
        &parse_query(PARTITION_SET_QUERY).expect("query"),
        &catalog,
    )
    .expect("partition-set plan");
    let program = family.select(&[]).expect("member").program();
    let run = |experiment_id, run_id: &str, start_time| {
        row(
            "Run",
            &[
                ("experiment_id", CanonicalValue::U64(experiment_id)),
                ("run_id", CanonicalValue::string(run_id).expect("run id")),
                ("start_time", CanonicalValue::I64(start_time)),
            ],
        )
    };
    let rows = BTreeMap::from([
        ("runs#1".to_owned(), vec![run(1, "c", 30), run(1, "a", 10)]),
        ("runs#2".to_owned(), vec![run(2, "d", 40), run(2, "b", 20)]),
    ]);
    let parameters = |limit| {
        QueryParameters::checked(BTreeMap::from([
            (
                "experiment_ids".to_owned(),
                CanonicalValue::list(vec![CanonicalValue::U64(2), CanonicalValue::U64(1)])
                    .expect("routes"),
            ),
            ("limit".to_owned(), CanonicalValue::U64(limit)),
        ]))
        .expect("parameters")
    };

    let mut first_view = FakeView { rows: rows.clone() };
    let first =
        execute_operational_page_in_snapshot(program, &[], &parameters(1), None, &mut first_view)
            .expect("first page");
    let QueryResultValue::Many(first_rows) = &first.fields()["runs"] else {
        panic!("runs");
    };
    assert_eq!(
        first_rows[0].field("run_id"),
        Some(&CanonicalValue::string("d").unwrap())
    );
    let cursor = QueryContinuation::checked(
        first.continuation_binding().expect("binding").to_owned(),
        first.continuation().expect("continuation").to_vec(),
        first.index_epochs().clone(),
    )
    .expect("cursor");

    let mut second_view = FakeView { rows };
    let second = execute_operational_page_in_snapshot(
        program,
        &[],
        &parameters(3),
        Some(&cursor),
        &mut second_view,
    )
    .expect("second page");
    let QueryResultValue::Many(second_rows) = &second.fields()["runs"] else {
        panic!("runs");
    };
    let ids = second_rows
        .iter()
        .map(|row| match row.field("run_id") {
            Some(CanonicalValue::String(value)) => value.as_str(),
            _ => panic!("run id"),
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, ["c", "b", "a"]);
    assert!(second.continuation().is_none());

    let mut empty_view = FakeView {
        rows: BTreeMap::new(),
    };
    let empty_parameters = QueryParameters::checked(BTreeMap::from([
        (
            "experiment_ids".to_owned(),
            CanonicalValue::list(Vec::new()).expect("empty routes"),
        ),
        ("limit".to_owned(), CanonicalValue::U64(3)),
    ]))
    .expect("parameters");
    let empty = execute_operational_page_in_snapshot(
        program,
        &[],
        &empty_parameters,
        None,
        &mut empty_view,
    )
    .expect("empty route set");
    let QueryResultValue::Many(empty_rows) = &empty.fields()["runs"] else {
        panic!("runs");
    };
    assert!(empty_rows.is_empty());
}

#[test]
fn partition_set_exact_prefix_filters_before_global_order_and_resumes() {
    let bundle = compile_contract_source(PARTITION_SET_CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let lifecycle = catalog
        .enumeration("LifecycleStage")
        .expect("lifecycle enum");
    let active = CanonicalValue::Enum {
        type_id: lifecycle.internal_id(),
        variant_id: lifecycle.variant("Active").expect("Active"),
    };
    let deleted = CanonicalValue::Enum {
        type_id: lifecycle.internal_id(),
        variant_id: lifecycle.variant("Deleted").expect("Deleted"),
    };
    let family = compile_operational_query_family(
        &parse_query(FILTERED_PARTITION_SET_QUERY).expect("query"),
        &catalog,
    )
    .expect("partition-set exact-prefix plan");
    let program = family.select(&[]).expect("member").program();
    assert_eq!(
        program.ir_version(),
        riffdb_query_ir::QUERY_IR_VERSION_PARTITION_SET_EXACT_PREFIX_V1
    );
    let run = |experiment_id, run_id: &str, start_time, lifecycle_stage| {
        row(
            "Run",
            &[
                ("experiment_id", CanonicalValue::U64(experiment_id)),
                ("run_id", CanonicalValue::string(run_id).expect("run id")),
                ("start_time", CanonicalValue::I64(start_time)),
                ("lifecycle_stage", lifecycle_stage),
            ],
        )
    };
    let rows = BTreeMap::from([
        (
            "runs#1".to_owned(),
            vec![
                run(1, "c", 30, active.clone()),
                run(1, "x", 25, deleted.clone()),
                run(1, "a", 10, active.clone()),
            ],
        ),
        (
            "runs#2".to_owned(),
            vec![
                run(2, "d", 40, active.clone()),
                run(2, "y", 35, deleted),
                run(2, "b", 20, active.clone()),
            ],
        ),
    ]);
    let parameters = |limit| {
        QueryParameters::checked(BTreeMap::from([
            (
                "experiment_ids".to_owned(),
                CanonicalValue::list(vec![CanonicalValue::U64(2), CanonicalValue::U64(1)])
                    .expect("routes"),
            ),
            ("lifecycle_stage".to_owned(), active.clone()),
            ("limit".to_owned(), CanonicalValue::U64(limit)),
        ]))
        .expect("parameters")
    };

    let mut first_view = FakeView { rows: rows.clone() };
    let first =
        execute_operational_page_in_snapshot(program, &[], &parameters(1), None, &mut first_view)
            .expect("first filtered page");
    let QueryResultValue::Many(first_rows) = &first.fields()["runs"] else {
        panic!("runs")
    };
    assert_eq!(
        first_rows[0].field("run_id"),
        Some(&CanonicalValue::string("d").unwrap())
    );
    let cursor = QueryContinuation::checked(
        first.continuation_binding().expect("binding").to_owned(),
        first.continuation().expect("continuation").to_vec(),
        first.index_epochs().clone(),
    )
    .expect("cursor");

    let mut second_view = FakeView { rows };
    let second = execute_operational_page_in_snapshot(
        program,
        &[],
        &parameters(3),
        Some(&cursor),
        &mut second_view,
    )
    .expect("continued filtered page");
    let QueryResultValue::Many(second_rows) = &second.fields()["runs"] else {
        panic!("runs")
    };
    let ids = second_rows
        .iter()
        .map(|row| match row.field("run_id") {
            Some(CanonicalValue::String(value)) => value.as_str(),
            _ => panic!("run id"),
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, ["c", "b", "a"]);
    assert!(second.continuation().is_none());
}

#[test]
fn presence_prefixes_and_row_filters_distinguish_missing_null_and_value() {
    let organization = CanonicalValue::Uuid([1; 16]);
    let parameters = QueryParameters::checked(BTreeMap::from([(
        "organization_id".to_owned(),
        organization.clone(),
    )]))
    .expect("parameters");
    let (bundle, null_step) = compiled_operational_step(NULL_DOCUMENTS);
    let null_predicates = bind_predicates(&null_step, &parameters, &BTreeMap::new(), &[])
        .expect("bound null predicates");
    let null_ranges =
        bound_index_range_schedule_v1(&null_step, &null_predicates).expect("null ranges");
    let entity = &bundle.schema().entities()[0];
    let index = entity
        .indexes()
        .iter()
        .find(|index| index.name() == "by_deleted")
        .expect("presence index");
    let deleted_field = entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "deleted_at")
        .expect("deleted field")
        .id();
    let field_id = |name: &str| {
        entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .expect("field")
            .id()
    };
    for (ordinal, (presence, expected_null)) in [
        None,
        Some(CanonicalValue::Null),
        Some(CanonicalValue::Timestamp(
            riffdb_types::Timestamp::new(1, 0).expect("timestamp"),
        )),
    ]
    .into_iter()
    .zip([false, true, false])
    .enumerate()
    {
        let document_id = CanonicalValue::Uuid([ordinal as u8 + 2; 16]);
        let mut fields = vec![
            (field_id("organization_id"), organization.clone()),
            (field_id("document_id"), document_id.clone()),
        ];
        if let Some(value) = presence {
            fields.push((deleted_field, value));
        }
        let record = CanonicalRecord::new(fields).expect("presence record");
        let values = riffdb_contract_ir::encode_operational_index_values_v1(index, &record)
            .expect("physical presence values");
        let key = index
            .key_schema()
            .encode_index(
                &values,
                entity
                    .primary_key()
                    .encode_entity(&[organization.clone(), document_id])
                    .expect("entity key"),
            )
            .expect("index key");
        assert_eq!(matching_range(&null_ranges, key.as_bytes()), expected_null);
    }

    let (_, exists_step) = compiled_operational_step(EXISTING_DOCUMENTS);
    let exists_predicates = bind_predicates(&exists_step, &parameters, &BTreeMap::new(), &[])
        .expect("bound exists predicates");
    let missing_row = row("Document", &[("organization_id", organization.clone())]);
    let null_row = row(
        "Document",
        &[
            ("organization_id", organization.clone()),
            ("deleted_at", CanonicalValue::Null),
        ],
    );
    let value_row = row(
        "Document",
        &[
            ("organization_id", organization),
            (
                "deleted_at",
                CanonicalValue::Timestamp(riffdb_types::Timestamp::new(1, 0).expect("timestamp")),
            ),
        ],
    );
    assert!(!predicates_match(&missing_row, &exists_predicates).expect("missing filter"));
    assert!(predicates_match(&null_row, &exists_predicates).expect("null filter"));
    assert!(predicates_match(&value_row, &exists_predicates).expect("value filter"));
}

#[test]
fn binary_text_prefix_selects_only_the_exact_ordered_byte_range() {
    let organization = CanonicalValue::Uuid([7; 16]);
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), organization.clone()),
        (
            "prefix".to_owned(),
            CanonicalValue::string("ab").expect("prefix"),
        ),
    ]))
    .expect("parameters");
    let (bundle, step) = compiled_operational_step(PREFIX_DOCUMENTS);
    let predicates =
        bind_predicates(&step, &parameters, &BTreeMap::new(), &[]).expect("bound predicates");
    let ranges = bound_index_range_schedule_v1(&step, &predicates).expect("prefix range");
    assert_eq!(ranges.ranges().len(), 1);
    let entity = &bundle.schema().entities()[0];
    let index = entity
        .indexes()
        .iter()
        .find(|index| index.name() == "by_title")
        .expect("text index");
    let title_field = entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "title")
        .expect("title field")
        .id();
    for (title, expected) in [("a", false), ("ab", true), ("abacus", true), ("ac", false)] {
        let document_id = CanonicalValue::Uuid([title.len() as u8; 16]);
        let record = CanonicalRecord::new(vec![
            (
                entity
                    .record()
                    .fields()
                    .iter()
                    .find(|field| field.name() == "organization_id")
                    .expect("organization field")
                    .id(),
                organization.clone(),
            ),
            (
                entity
                    .record()
                    .fields()
                    .iter()
                    .find(|field| field.name() == "document_id")
                    .expect("document field")
                    .id(),
                document_id.clone(),
            ),
            (title_field, CanonicalValue::string(title).expect("title")),
        ])
        .expect("record");
        let values = riffdb_contract_ir::encode_operational_index_values_v1(index, &record)
            .expect("physical text values");
        let entity_key = entity
            .primary_key()
            .encode_entity(&[organization.clone(), document_id])
            .expect("entity key");
        let key = index
            .key_schema()
            .encode_index(&values, entity_key)
            .expect("index key");
        assert_eq!(matching_range(&ranges, key.as_bytes()), expected, "{title}");
    }
}

// req: OQ-057, OQ-058, OQ-059, OQ-060
#[test]
fn binary_text_interval_boundaries_match_the_authoritative_utf8_byte_oracle() {
    let organization = CanonicalValue::Uuid([7; 16]);
    let bundle = compile_contract_source(OPERATIONAL_INDEX_CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family = compile_operational_query_family(
        &parse_query(BINARY_TEXT_INTERVAL_DOCUMENTS).expect("interval query"),
        &catalog,
    )
    .expect("binary-text interval family");
    let step = family.select(&[]).expect("sole member").program().steps()[0].clone();
    let entity = &bundle.schema().entities()[0];
    let index = entity
        .indexes()
        .iter()
        .find(|index| index.name() == "by_title")
        .expect("text index");
    let field_id = |name: &str| {
        entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .expect("field")
            .id()
    };
    let values = vec![
        String::new(),
        "\0".to_owned(),
        "a".to_owned(),
        "a\0b".to_owned(),
        "ab".to_owned(),
        "doc-3".to_owned(),
        "doc6".to_owned(),
        "é".to_owned(),
        "東京".to_owned(),
        "z".repeat(64),
    ];

    for lower in &values {
        for upper in &values {
            let parameters = QueryParameters::checked(BTreeMap::from([
                ("organization_id".to_owned(), organization.clone()),
                (
                    "lower".to_owned(),
                    CanonicalValue::string(lower).expect("bounded lower"),
                ),
                (
                    "upper".to_owned(),
                    CanonicalValue::string(upper).expect("bounded upper"),
                ),
            ]))
            .expect("parameters");
            let predicates = bind_predicates(&step, &parameters, &BTreeMap::new(), &[])
                .expect("bound predicates");
            let schedule =
                bound_index_range_schedule_v1(&step, &predicates).expect("range schedule");

            for (ordinal, candidate) in values.iter().enumerate() {
                let document_id =
                    CanonicalValue::Uuid([u8::try_from(ordinal).expect("ordinal"); 16]);
                let record = CanonicalRecord::new(vec![
                    (field_id("organization_id"), organization.clone()),
                    (field_id("document_id"), document_id.clone()),
                    (
                        field_id("title"),
                        CanonicalValue::string(candidate).expect("bounded candidate"),
                    ),
                ])
                .expect("record");
                let physical =
                    riffdb_contract_ir::encode_operational_index_values_v1(index, &record)
                        .expect("physical index values");
                let entity_key = entity
                    .primary_key()
                    .encode_entity(&[organization.clone(), document_id])
                    .expect("entity key");
                let key = index
                    .key_schema()
                    .encode_index(&physical, entity_key)
                    .expect("index key");
                let expected = candidate.as_bytes() >= lower.as_bytes()
                    && candidate.as_bytes() < upper.as_bytes();
                assert_eq!(
                    matching_range(&schedule, key.as_bytes()),
                    expected,
                    "candidate={candidate:?}, lower={lower:?}, upper={upper:?}"
                );
            }
        }
    }
}

// req: OQ-032, OQ-033, OQ-034, OQ-037
#[test]
fn unicode_fold_exact_predicate_uses_the_declared_physical_profile() {
    let bundle = compile_contract_source(UNICODE_FOLD_OPERATIONAL_INDEX_CONTRACT)
        .expect("unicode-fold contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family = compile_operational_query_family(
        &parse_query(UNICODE_FOLD_EXACT_DOCUMENTS).expect("query"),
        &catalog,
    )
    .expect("unicode-fold exact plan");
    let step = family.select(&[]).expect("member").program().steps()[0].clone();

    let organization = CanonicalValue::Uuid([7; 16]);
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), organization.clone()),
        (
            "title".to_owned(),
            CanonicalValue::string("STRASSE").expect("folded needle"),
        ),
    ]))
    .expect("parameters");
    let predicates =
        bind_predicates(&step, &parameters, &BTreeMap::new(), &[]).expect("bound predicates");
    let ranges =
        bound_index_range_schedule_v1(&step, &predicates).expect("unicode-fold exact range");

    let entity = &bundle.schema().entities()[0];
    let index = entity
        .indexes()
        .iter()
        .find(|index| index.name() == "by_title")
        .expect("folded index");
    let field_id = |name: &str| {
        entity
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .expect("field")
            .id()
    };
    let document_id = CanonicalValue::Uuid([9; 16]);
    let record = CanonicalRecord::new(vec![
        (field_id("organization_id"), organization.clone()),
        (field_id("document_id"), document_id.clone()),
        (
            field_id("title"),
            CanonicalValue::string("Straße").expect("stored title"),
        ),
    ])
    .expect("stored record");
    let values = riffdb_contract_ir::encode_operational_index_values_v1(index, &record)
        .expect("folded stored values");
    let entity_key = entity
        .primary_key()
        .encode_entity(&[organization, document_id])
        .expect("entity key");
    let key = index
        .key_schema()
        .encode_index(&values, entity_key)
        .expect("index key");

    assert!(matching_range(&ranges, key.as_bytes()));
}

// req: OQ-032, OQ-033, OQ-034, OQ-037, OQ-038, OQ-043
#[test]
fn operational_descriptor_reconstruction_preserves_canonical_plan_bytes_and_hash() {
    let bundle = compile_contract_source(UNICODE_FOLD_OPERATIONAL_INDEX_CONTRACT)
        .expect("unicode-fold contract");
    let first_catalog = SymbolicCatalog::from_bundle(&bundle).expect("first catalog");
    let second_catalog = SymbolicCatalog::from_bundle(&bundle).expect("second catalog");
    let document = parse_query(UNICODE_FOLD_EXACT_DOCUMENTS).expect("query");
    let first = compile_operational_query_family(&document, &first_catalog)
        .expect("first reconstructed family");
    let second = compile_operational_query_family(&document, &second_catalog)
        .expect("second reconstructed family");
    let first = first.select(&[]).expect("first member").program();
    let second = second.select(&[]).expect("second member").program();

    assert!(
        first
            .steps()
            .iter()
            .all(|step| step.internal_operational_index_descriptor().is_some())
    );
    assert!(
        second
            .steps()
            .iter()
            .all(|step| step.internal_operational_index_descriptor().is_some())
    );
    assert_eq!(first.canonical_bytes(), second.canonical_bytes());
    assert_eq!(first.identity(), second.identity());

    let rebuilt = QueryAccessProgramV1::checked(
        first.contract().clone(),
        first.surface().clone(),
        first.name().map(str::to_owned),
        first.partition_parameter().to_owned(),
        first.steps().to_vec(),
        first.authorization().to_vec(),
        first.cost(),
    )
    .expect("reattached private descriptors remain valid");
    assert_eq!(first.canonical_bytes(), rebuilt.canonical_bytes());
    assert_eq!(first.identity(), rebuilt.identity());
}

/// Falsifiability: leaf many-query projection must not clone selected values.
/// Transcript: force project_clone → observed 6 → restore move path → 0.
#[test]
fn leaf_many_projection_does_not_clone_selected_values() {
    enable_pipeline_clone_counting();
    let contract = CONTRACT.replace(" cover (title, reporter_id, assignee_id)", "");
    let bundle = compile_contract_source(&contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let program =
        compile_query(&parse_query(OPEN_TICKETS).expect("query"), &catalog).expect("program");
    let status = catalog.enumeration("TicketStatus").expect("status");
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        ("project_id".to_owned(), CanonicalValue::Uuid([2; 16])),
    ]))
    .expect("parameters");
    let tickets: Vec<QueryRow> = (0u8..3)
        .map(|n| {
            row(
                "Ticket",
                &[
                    ("organization_id", CanonicalValue::Uuid([1; 16])),
                    ("project_id", CanonicalValue::Uuid([2; 16])),
                    ("ticket_id", CanonicalValue::Uuid([n; 16])),
                    (
                        "status",
                        CanonicalValue::Enum {
                            type_id: status.internal_id(),
                            variant_id: status.variant("Open").expect("Open"),
                        },
                    ),
                ],
            )
        })
        .collect();
    let mut view = FakeView {
        rows: BTreeMap::from([("tickets".to_owned(), tickets)]),
    };
    let snapshot = execute_in_snapshot(&program, &parameters, &mut view).expect("execute");
    let clones = disable_pipeline_clone_counting();
    match snapshot.fields().get("tickets") {
        Some(QueryResultValue::Many(rows)) => assert_eq!(rows.len(), 3),
        other => panic!("expected 3 tickets, got {other:?}"),
    }
    assert_eq!(
        clones, 0,
        "per-row result-path clone count must be 0 for a leaf 3-row query (observed {clones})"
    );
}

// req: DEP-001
#[test]
fn operational_aggregates_are_exact_grouped_and_canonically_ordered() {
    let bundle = compile_contract_source(AGGREGATE_CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family =
        compile_operational_query_family(&parse_query(GROUPED_SUMMARY).expect("query"), &catalog)
            .expect("family");
    let program = family.select(&[]).expect("sole member").program();
    let parameters = QueryParameters::checked(BTreeMap::from([(
        "organization_id".to_owned(),
        CanonicalValue::Uuid([1; 16]),
    )]))
    .expect("parameters");
    let tickets = vec![
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([2; 16])),
                ("status", CanonicalValue::string("Open").expect("status")),
                ("story_points", CanonicalValue::I64(3)),
            ],
        ),
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([3; 16])),
                ("status", CanonicalValue::string("Closed").expect("status")),
                ("story_points", CanonicalValue::I64(5)),
            ],
        ),
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([4; 16])),
                ("status", CanonicalValue::string("Open").expect("status")),
                ("story_points", CanonicalValue::I64(7)),
            ],
        ),
    ];
    let mut view = FakeView {
        rows: BTreeMap::from([("tickets".to_owned(), tickets)]),
    };
    let snapshot = execute_operational_page_in_snapshot(
        program,
        family.aggregates(),
        &parameters,
        None,
        &mut view,
    )
    .expect("execute exact aggregate");
    let Some(QueryResultValue::AggregateMany(groups)) = snapshot.fields().get("summary") else {
        panic!("grouped aggregate result missing: {:?}", snapshot.fields());
    };
    capture_snapshot("operational-grouped-aggregate", &snapshot);
    assert_eq!(groups.len(), 2);
    assert_eq!(
        groups[0].fields().get("status"),
        Some(&QueryAggregateCell::Canonical(
            CanonicalValue::string("Open").expect("status")
        ))
    );
    assert_eq!(
        groups[0].fields().get("ticket_count"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::U64(2)))
    );
    assert_eq!(
        groups[0].fields().get("total_points"),
        Some(&QueryAggregateCell::ExactDecimal {
            coefficient: 10,
            scale: 0,
        })
    );
}

// req: DEP-001
#[test]
fn exact_aggregate_core_freezes_novalue_empty_and_mean_state() {
    let bundle = compile_contract_source(AGGREGATE_CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family = compile_operational_query_family(
        &parse_query(EXACT_CORE_SUMMARY).expect("query"),
        &catalog,
    )
    .expect("family");
    let parameters = QueryParameters::checked(BTreeMap::from([(
        "organization_id".to_owned(),
        CanonicalValue::Uuid([1; 16]),
    )]))
    .expect("parameters");
    let tickets = vec![
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([2; 16])),
                ("status", CanonicalValue::string("Open").expect("status")),
                ("story_points", CanonicalValue::I64(3)),
                ("label", CanonicalValue::Null),
                ("enabled", CanonicalValue::Bool(false)),
            ],
        ),
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([3; 16])),
                ("status", CanonicalValue::string("Open").expect("status")),
                ("story_points", CanonicalValue::I64(5)),
                ("label", CanonicalValue::string("alpha").expect("label")),
                ("enabled", CanonicalValue::Bool(true)),
            ],
        ),
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([4; 16])),
                ("status", CanonicalValue::string("Open").expect("status")),
                ("story_points", CanonicalValue::I64(7)),
                ("label", CanonicalValue::string("alpha").expect("label")),
                ("enabled", CanonicalValue::Bool(true)),
            ],
        ),
    ];
    let execute = |rows| {
        let mut view = FakeView {
            rows: BTreeMap::from([("tickets".to_owned(), rows)]),
        };
        execute_operational_page_in_snapshot(
            family.select(&[]).expect("sole member").program(),
            family.aggregates(),
            &parameters,
            None,
            &mut view,
        )
        .expect("execute")
    };

    let snapshot = execute(tickets);
    capture_snapshot("operational-exact-aggregate", &snapshot);
    let Some(QueryResultValue::AggregateOne(summary)) = snapshot.fields().get("summary") else {
        panic!("summary missing")
    };
    for (name, expected) in [
        ("present_labels", 2),
        ("distinct_labels", 2),
        ("distinct_present_labels", 1),
    ] {
        assert_eq!(
            summary.fields().get(name),
            Some(&QueryAggregateCell::Canonical(CanonicalValue::U64(
                expected
            )))
        );
    }
    assert_eq!(
        summary.fields().get("mean_points"),
        Some(&QueryAggregateCell::ExactMean {
            coefficient: 15,
            scale: 0,
            count: 3,
        })
    );
    assert_eq!(
        summary.fields().get("any_enabled"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::Bool(true)))
    );
    assert_eq!(
        summary.fields().get("all_enabled"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::Bool(false)))
    );

    let empty = execute(Vec::new());
    capture_snapshot("operational-empty-aggregate", &empty);
    let Some(QueryResultValue::AggregateOne(summary)) = empty.fields().get("summary") else {
        panic!("empty summary missing")
    };
    assert_eq!(
        summary.fields().get("mean_points"),
        Some(&QueryAggregateCell::ExactMean {
            coefficient: 0,
            scale: 0,
            count: 0,
        })
    );
    assert_eq!(
        summary.fields().get("any_enabled"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::Bool(false)))
    );
    assert_eq!(
        summary.fields().get("all_enabled"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::Bool(true)))
    );
}

#[test]
fn distinct_budget_exhaustion_withholds_the_complete_aggregate() {
    let bundle = compile_contract_source(AGGREGATE_CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family = compile_operational_query_family(
        &parse_query(DISTINCT_BUDGET_SUMMARY).expect("query"),
        &catalog,
    )
    .expect("family");
    let parameters = QueryParameters::checked(BTreeMap::from([(
        "organization_id".to_owned(),
        CanonicalValue::Uuid([1; 16]),
    )]))
    .expect("parameters");
    let tickets = (0_u16..=256)
        .map(|ordinal| {
            let mut ticket_id = [0_u8; 16];
            ticket_id[14..].copy_from_slice(&ordinal.to_be_bytes());
            row(
                "Ticket",
                &[
                    ("organization_id", CanonicalValue::Uuid([1; 16])),
                    ("ticket_id", CanonicalValue::Uuid(ticket_id)),
                    ("status", CanonicalValue::string("Open").expect("status")),
                    ("story_points", CanonicalValue::I64(1)),
                    (
                        "label",
                        CanonicalValue::string(format!("label-{ordinal:03}")).expect("label"),
                    ),
                    ("enabled", CanonicalValue::Bool(true)),
                ],
            )
        })
        .collect();
    let mut view = FakeView {
        rows: BTreeMap::from([("tickets".to_owned(), tickets)]),
    };
    assert_eq!(
        execute_operational_page_in_snapshot(
            family.select(&[]).expect("sole member").program(),
            family.aggregates(),
            &parameters,
            None,
            &mut view,
        ),
        Err(QueryExecutionError::BoundExceeded)
    );
}

#[test]
fn aggregate_execution_requires_the_programs_exact_sealed_descriptors() {
    let bundle = compile_contract_source(AGGREGATE_CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family =
        compile_operational_query_family(&parse_query(GROUPED_SUMMARY).expect("query"), &catalog)
            .expect("family");
    let parameters = QueryParameters::checked(BTreeMap::from([(
        "organization_id".to_owned(),
        CanonicalValue::Uuid([1; 16]),
    )]))
    .expect("parameters");
    let mut view = FakeView {
        rows: BTreeMap::from([("tickets".to_owned(), Vec::new())]),
    };

    assert_eq!(
        execute_operational_page_in_snapshot(
            family.select(&[]).expect("sole member").program(),
            &[],
            &parameters,
            None,
            &mut view,
        ),
        Err(QueryExecutionError::InvalidProgram),
        "an executor caller cannot omit or substitute compiler-sealed aggregate work"
    );
}

#[test]
fn runtime_limit_clamps_group_cardinality_before_backend_access() {
    let bundle = compile_contract_source(AGGREGATE_CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family = compile_operational_query_family(
        &parse_query(PARAMETER_BOUNDED_GROUPED_SUMMARY).expect("query"),
        &catalog,
    )
    .expect("family");
    // The only typed limit is now Limit<MAX>, so a parameter-bounded
    // group cardinality carries its declared maximum.
    assert_eq!(
        family.aggregates()[0].maximum_groups(),
        &PageBound::BoundedParameter {
            name: "limit".to_owned(),
            maximum: 499
        }
    );
    let parameters = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        ("limit".to_owned(), CanonicalValue::U64(501)),
    ]))
    .expect("parameters");
    let mut view = FakeView {
        rows: BTreeMap::from([("tickets".to_owned(), Vec::new())]),
    };

    assert_eq!(
        execute_operational_page_in_snapshot(
            family.select(&[]).expect("sole member").program(),
            family.aggregates(),
            &parameters,
            None,
            &mut view,
        ),
        Err(QueryExecutionError::InvalidParameter {
            parameter: "limit".to_owned(),
        })
    );
}

#[test]
fn whole_set_empty_aggregate_preserves_wp492_identities() {
    let bundle = compile_contract_source(AGGREGATE_CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family =
        compile_operational_query_family(&parse_query(EMPTY_SUMMARY).expect("query"), &catalog)
            .expect("family");
    let program = family.select(&[]).expect("sole member").program();
    let parameters = QueryParameters::checked(BTreeMap::from([(
        "organization_id".to_owned(),
        CanonicalValue::Uuid([1; 16]),
    )]))
    .expect("parameters");
    let mut view = FakeView {
        rows: BTreeMap::from([("tickets".to_owned(), Vec::new())]),
    };
    let snapshot = execute_operational_page_in_snapshot(
        program,
        family.aggregates(),
        &parameters,
        None,
        &mut view,
    )
    .expect("execute empty aggregate");
    let Some(QueryResultValue::AggregateOne(summary)) = snapshot.fields().get("summary") else {
        panic!(
            "whole-set aggregate result missing: {:?}",
            snapshot.fields()
        );
    };
    assert_eq!(
        summary.fields().get("ticket_count"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::U64(0)))
    );
    assert_eq!(
        summary.fields().get("total_points"),
        Some(&QueryAggregateCell::ExactDecimal {
            coefficient: 0,
            scale: 0,
        })
    );
    assert_eq!(
        summary.fields().get("minimum_points"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::Null))
    );
}

#[test]
fn optional_minimum_distinguishes_empty_input_from_present_null() {
    let contract = AGGREGATE_CONTRACT.replace(
        "    field story_points: i64",
        "    field story_points: optional<i64>",
    );
    let bundle = compile_contract_source(&contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family = compile_operational_query_family(
        &parse_query(OPTIONAL_MINIMUM_SUMMARY).expect("query"),
        &catalog,
    )
    .expect("family");
    let program = family.select(&[]).expect("sole member").program();
    let parameters = QueryParameters::checked(BTreeMap::from([(
        "organization_id".to_owned(),
        CanonicalValue::Uuid([1; 16]),
    )]))
    .expect("parameters");

    let mut empty_view = FakeView {
        rows: BTreeMap::from([("tickets".to_owned(), Vec::new())]),
    };
    let empty = execute_operational_page_in_snapshot(
        program,
        family.aggregates(),
        &parameters,
        None,
        &mut empty_view,
    )
    .expect("empty aggregate");
    let Some(QueryResultValue::AggregateOne(empty)) = empty.fields().get("summary") else {
        panic!("empty summary")
    };
    assert_eq!(empty.fields().get("minimum_points"), None);

    let mut null_view = FakeView {
        rows: BTreeMap::from([(
            "tickets".to_owned(),
            vec![row(
                "Ticket",
                &[
                    ("organization_id", CanonicalValue::Uuid([1; 16])),
                    ("ticket_id", CanonicalValue::Uuid([2; 16])),
                    ("status", CanonicalValue::string("Open").expect("status")),
                    ("story_points", CanonicalValue::Null),
                ],
            )],
        )]),
    };
    let present_null = execute_operational_page_in_snapshot(
        program,
        family.aggregates(),
        &parameters,
        None,
        &mut null_view,
    )
    .expect("present null aggregate");
    let Some(QueryResultValue::AggregateOne(present_null)) = present_null.fields().get("summary")
    else {
        panic!("present-null summary")
    };
    assert_eq!(
        present_null.fields().get("minimum_points"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::Null))
    );
}

// req: DEP-001
#[test]
fn aggregate_overflow_withholds_the_complete_result() {
    let bundle = compile_contract_source(DECIMAL_AGGREGATE_CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family = compile_operational_query_family(
        &parse_query(OVERFLOWING_SUMMARY).expect("query"),
        &catalog,
    )
    .expect("family");
    let program = family.select(&[]).expect("sole member").program();
    let parameters = QueryParameters::checked(BTreeMap::from([(
        "organization_id".to_owned(),
        CanonicalValue::Uuid([1; 16]),
    )]))
    .expect("parameters");
    let spec = riffdb_types::DecimalSpec::new(38, 0).expect("decimal spec");
    let amount =
        riffdb_types::Decimal::new(spec, 10_i128.pow(38) - 1).expect("maximum contract decimal");
    let entries = [2_u8, 3_u8]
        .into_iter()
        .map(|id| {
            row(
                "Entry",
                &[
                    ("organization_id", CanonicalValue::Uuid([1; 16])),
                    ("entry_id", CanonicalValue::Uuid([id; 16])),
                    ("amount", CanonicalValue::Decimal(amount)),
                ],
            )
        })
        .collect();
    let mut view = FakeView {
        rows: BTreeMap::from([("entries".to_owned(), entries)]),
    };
    let result = execute_operational_page_in_snapshot(
        program,
        family.aggregates(),
        &parameters,
        None,
        &mut view,
    );
    capture_error(
        "operational-overflow-refusal",
        result.as_ref().expect_err("overflow refusal"),
    );
    assert_eq!(
        result,
        Err(QueryExecutionError::AggregateOverflow),
        "overflow must release no partial count/group/result"
    );
}

#[cfg(feature = "test-fixtures")]
fn capture_snapshot(name: &str, snapshot: &QueryOwnedSnapshot) {
    let Some(root) = std::env::var_os("RIFFDB_WP754_FIXTURE_OUTPUT") else {
        return;
    };
    let path = std::path::PathBuf::from(root).join(format!("{name}.bin"));
    std::fs::create_dir_all(path.parent().expect("fixture parent")).expect("fixture directory");
    std::fs::write(path, encode_query_snapshot_fixture_v1(name, snapshot)).expect("write fixture");
}

#[cfg(not(feature = "test-fixtures"))]
fn capture_snapshot(_name: &str, _snapshot: &QueryOwnedSnapshot) {}

#[cfg(feature = "test-fixtures")]
fn capture_error(name: &str, error: &QueryExecutionError) {
    let Some(root) = std::env::var_os("RIFFDB_WP754_FIXTURE_OUTPUT") else {
        return;
    };
    let path = std::path::PathBuf::from(root).join(format!("{name}.bin"));
    std::fs::create_dir_all(path.parent().expect("fixture parent")).expect("fixture directory");
    std::fs::write(path, encode_query_error_fixture_v1(name, error)).expect("write fixture");
}

#[cfg(not(feature = "test-fixtures"))]
fn capture_error(_name: &str, _error: &QueryExecutionError) {}

#[test]
fn unique_same_partition_index_satisfies_one_and_maybe_bindings() {
    let contract = r#"
contract Directory version 1 {
  entity User {
    key (organization_id: uuid, user_id: uuid)
    field email: string<120>
    field handle: string<64>
    unique by_email (organization_id, email)
  }
  aggregate Users {
    root User
    partition_by organization_id
    conflict_key (organization_id, user_id)
  }
}
"#;
    let bundle = compile_contract_source(contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let query_source = r#"
query LookupByEmail(
    $organization_id: User.organization_id,
    $email: User.email,
    $secondary_email: User.email
) {
    one primary_user from User
        where organization_id == $organization_id && email == $email
        else UserMissing
    maybe secondary_user from User
        where organization_id == $organization_id && email == $secondary_email
    return Found {
        primary_user: primary_user { user_id email handle }
        secondary_user: secondary_user { user_id handle }
    }
    outcomes Found | UserMissing
}
"#;
    let program = compile_query(&parse_query(query_source).expect("parse"), &catalog)
        .expect("unique same-partition index satisfies one and maybe");
    assert!(matches!(
        program.steps()[0].access(),
        riffdb_query_ir::QueryAccessKind::Index { index, .. } if index == "by_email"
    ));

    let parameters = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        (
            "email".to_owned(),
            CanonicalValue::string("alice@example.com").expect("email"),
        ),
        (
            "secondary_email".to_owned(),
            CanonicalValue::string("nobody@example.com").expect("email"),
        ),
    ]))
    .expect("parameters");

    let alice = row(
        "User",
        &[
            ("organization_id", CanonicalValue::Uuid([1; 16])),
            ("user_id", CanonicalValue::Uuid([9; 16])),
            (
                "email",
                CanonicalValue::string("alice@example.com").expect("email"),
            ),
            ("handle", CanonicalValue::string("alice").expect("handle")),
        ],
    );
    let mut view = FakeView {
        rows: BTreeMap::from([
            ("primary_user".to_owned(), vec![alice.clone()]),
            ("secondary_user".to_owned(), Vec::new()),
        ]),
    };
    let found = execute_page_in_snapshot(&program, &parameters, None, &mut view).expect("found");
    assert_eq!(found.outcome(), "Found");
    assert!(matches!(
        found.fields().get("primary_user"),
        Some(QueryResultValue::One(row))
            if row.field("handle") == Some(&CanonicalValue::string("alice").unwrap())
    ));
    assert_eq!(
        found.fields().get("secondary_user"),
        Some(&QueryResultValue::Maybe(None))
    );

    let mut missing_view = FakeView {
        rows: BTreeMap::from([
            ("primary_user".to_owned(), Vec::new()),
            ("secondary_user".to_owned(), Vec::new()),
        ]),
    };
    let missing =
        execute_page_in_snapshot(&program, &parameters, None, &mut missing_view).expect("missing");
    assert_eq!(missing.outcome(), "UserMissing");

    let mut corrupt_view = FakeView {
        rows: BTreeMap::from([
            ("primary_user".to_owned(), vec![alice.clone(), alice]),
            ("secondary_user".to_owned(), Vec::new()),
        ]),
    };
    assert_eq!(
        execute_page_in_snapshot(&program, &parameters, None, &mut corrupt_view),
        Err(QueryExecutionError::UnexpectedCardinality {
            binding: "primary_user".to_owned(),
        }),
        "singular unique-index execution fails closed if storage returns continuation"
    );
}

#[test]
fn distinct_dependent_key_batch_hydrates_foreign_keys_across_unordered_and_nullable_drivers() {
    let contract = r#"
contract Helpdesk version 1 {
  entity User {
    key (organization_id: uuid, user_id: uuid)
    field display_name: string<80>
  }
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field updated_at: timestamp
    field assignee_id: optional<uuid>
    field title: string<120>
    index by_updated (organization_id, updated_at, ticket_id)
  }
  aggregate Users {
    root User
    partition_by organization_id
    conflict_key (organization_id, user_id)
  }
  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }
}
"#;
    let bundle = compile_contract_source(contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let query_source = r#"
query RecentTicketsWithAssignees($organization_id: Ticket.organization_id) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by updated_at desc, ticket_id desc
        take 25

    many assignees from User
        where organization_id == $organization_id
          && user_id in distinct(tickets.assignee_id)
        take 25

    return Found {
        tickets: tickets { ticket_id assignee_id title }
        assignees: assignees { user_id display_name }
    }

    outcomes Found
}
"#;
    let parsed = parse_query(query_source).expect("parse");
    let formatted = riffdb_riffql_syntax::format_query(&parsed);
    assert!(
        formatted.contains("user_id in distinct(tickets.assignee_id)"),
        "formatter preserves distinct(binding.field): {formatted}"
    );
    let reparsed = parse_query(&formatted).expect("reparse formatted query");
    let program = compile_query(&reparsed, &catalog).expect("compile distinct hydration");
    assert!(program.steps()[1].distinct_dependent_keys());

    let parameters = QueryParameters::checked(BTreeMap::from([(
        "organization_id".to_owned(),
        CanonicalValue::Uuid([1; 16]),
    )]))
    .expect("parameters");

    let ticket_rows = vec![
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([10; 16])),
                (
                    "updated_at",
                    CanonicalValue::Timestamp(riffdb_types::Timestamp::new(3000, 0).unwrap()),
                ),
                ("assignee_id", CanonicalValue::Uuid([5; 16])),
                ("title", CanonicalValue::string("First").unwrap()),
            ],
        ),
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([9; 16])),
                (
                    "updated_at",
                    CanonicalValue::Timestamp(riffdb_types::Timestamp::new(2000, 0).unwrap()),
                ),
                ("assignee_id", CanonicalValue::Null),
                ("title", CanonicalValue::string("Unassigned").unwrap()),
            ],
        ),
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([8; 16])),
                (
                    "updated_at",
                    CanonicalValue::Timestamp(riffdb_types::Timestamp::new(1000, 0).unwrap()),
                ),
                ("assignee_id", CanonicalValue::Uuid([2; 16])),
                ("title", CanonicalValue::string("Second").unwrap()),
            ],
        ),
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([7; 16])),
                (
                    "updated_at",
                    CanonicalValue::Timestamp(riffdb_types::Timestamp::new(500, 0).unwrap()),
                ),
                ("assignee_id", CanonicalValue::Uuid([5; 16])),
                (
                    "title",
                    CanonicalValue::string("Duplicate assignee").unwrap(),
                ),
            ],
        ),
    ];

    struct RecordingPointBatchView {
        tickets: Vec<QueryRow>,
        users: BTreeMap<[u8; 16], QueryRow>,
        requested_user_ids: Vec<[u8; 16]>,
    }

    impl QueryReadView for RecordingPointBatchView {
        type Error = ();

        fn fault(&self, _error: &Self::Error) -> QueryBackendFault {
            QueryBackendFault::Unavailable
        }

        fn application_head(&self) -> u64 {
            1
        }

        fn point(
            &mut self,
            _step: &QueryAccessStep,
            _predicates: &[BoundPredicate],
            _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
        ) -> Result<Option<QueryRow>, Self::Error> {
            Ok(None)
        }

        fn dependent_point_batch(
            &mut self,
            step: &QueryAccessStep,
            predicates: &[Vec<BoundPredicate>],
            _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
        ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
            assert_eq!(step.binding(), "assignees");
            let mut out = Vec::with_capacity(predicates.len());
            for key in predicates {
                let Some(CanonicalValue::Uuid(user_id)) = key
                    .iter()
                    .find_map(|pred| (pred.field() == "user_id").then_some(pred.value()))
                else {
                    panic!("missing user_id in key: {key:?}");
                };
                self.requested_user_ids.push(*user_id);
                out.push(self.users.get(user_id).cloned());
            }
            Ok(out)
        }

        fn scan(
            &mut self,
            step: &QueryAccessStep,
            _predicates: &[BoundPredicate],
            _limit: u64,
            _after: Option<&[u8]>,
            _after_inclusive: bool,
            _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
        ) -> Result<QueryScanPage, Self::Error> {
            assert_eq!(step.binding(), "tickets");
            Ok(QueryScanPage::exact_end(self.tickets.clone(), 1))
        }

        fn nearest(
            &mut self,
            _step: &QueryAccessStep,
            _predicates: &[BoundPredicate],
            _k: u32,
            _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
        ) -> Result<QueryNearestPage, Self::Error> {
            Ok(QueryNearestPage {
                rows: Vec::new(),
                scanned_rows: 0,
            })
        }
    }

    let mut view = RecordingPointBatchView {
        tickets: ticket_rows,
        users: BTreeMap::from([
            (
                [2; 16],
                row(
                    "User",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("user_id", CanonicalValue::Uuid([2; 16])),
                        ("display_name", CanonicalValue::string("Bob").unwrap()),
                    ],
                ),
            ),
            (
                [5; 16],
                row(
                    "User",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("user_id", CanonicalValue::Uuid([5; 16])),
                        ("display_name", CanonicalValue::string("Eva").unwrap()),
                    ],
                ),
            ),
        ]),
        requested_user_ids: Vec::new(),
    };

    let result = execute_page_in_snapshot(&program, &parameters, None, &mut view).expect("execute");
    assert_eq!(
        view.requested_user_ids,
        vec![[2; 16], [5; 16]],
        "nulls are skipped and duplicate foreign keys are sorted and deduplicated"
    );
    let Some(QueryResultValue::Many(assignees)) = result.fields().get("assignees") else {
        panic!("assignees missing")
    };
    assert_eq!(assignees.len(), 2);
}

#[test]
fn order_family_composes_with_operational_null_and_prefix_predicates() {
    let contract = r#"
contract Catalog version 1 {
  enum ItemOrder { BySlugAsc, BySlugDesc }
  entity Item {
    key (organization_id: uuid, item_id: uuid)
    field slug: string<64>
    field archived_at: optional<timestamp>
    field updated_at: timestamp
    index by_slug (organization_id, archived_at, slug, item_id) presence(archived_at) text_key(slug, binary_utf8_v1)
  }
  aggregate Items {
    root Item
    partition_by organization_id
    conflict_key (organization_id, item_id)
  }
}
"#;
    let bundle = compile_contract_source(contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let query_source = r#"
query ActiveItemsByPrefix(
    $organization_id: Item.organization_id,
    $slug_prefix: Item.slug,
    $order: ItemOrder
) {
    many items from Item
        where organization_id == $organization_id
          && archived_at is null
          && slug prefix $slug_prefix
        order by $order {
            BySlugAsc: slug asc, item_id asc;
            BySlugDesc: slug desc, item_id desc;
        }
        take 25

    return Found {
        items: items { item_id slug updated_at }
    }
    outcomes Found
}
"#;
    let family = riffdb_query_compiler::compile_order_query_family(
        &parse_query(query_source).expect("parse"),
        &catalog,
    )
    .expect("order family composes with operational predicates");
    assert_eq!(family.members().len(), 2);
}

#[test]
fn finite_domain_group_by_and_explicit_group_take_bound_large_aggregate_rollups() {
    let contract = r#"
contract Rollup version 1 {
  enum TicketStatus { Open, InProgress, Closed }
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field status: TicketStatus
    field urgent: bool
    field category: string<64>
    field story_points: i64
    index by_org (organization_id, ticket_id)
  }
  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }
}
"#;
    let bundle = compile_contract_source(contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");

    // 1. Finite enum + bool domain (3 * 2 = 6 groups) over 4,000 input rows
    // caps maximum_groups to 6 rather than 4,000.
    let finite_domain_query = r#"
query StatusBreakdown($organization_id: Ticket.organization_id) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by ticket_id asc
        take 4000

    aggregate by_status from tickets {
        group by status, urgent
        count() as ticket_count
        sum(story_points) as total_points
    }

    return Found {
        by_status: by_status {
            status
            urgent
            ticket_count
            total_points
        }
    }
    outcomes Found
}
"#;
    let finite_family = compile_operational_query_family(
        &parse_query(finite_domain_query).expect("parse"),
        &catalog,
    )
    .expect("finite enum/bool domain caps maximum_groups to 6");
    assert_eq!(
        finite_family.aggregates()[0].maximum_groups(),
        &PageBound::Literal(6)
    );

    // 2. Explicit `group by category take 2` bounds a high-cardinality string
    // grouping over 5,000 input rows and fails closed at runtime if exceeded.
    let explicit_cap_query = r#"
query CategoryBreakdown($organization_id: Ticket.organization_id) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by ticket_id asc
        take 5000

    aggregate by_category from tickets {
        group by category take 2
        count() as ticket_count
    }

    return Found {
        by_category: by_category {
            category
            ticket_count
        }
    }
    outcomes Found
}
"#;
    let parsed = parse_query(explicit_cap_query).expect("parse");
    let formatted = riffdb_riffql_syntax::format_query(&parsed);
    assert!(
        formatted.contains("group by category take 2"),
        "formatter preserves group by ... take N: {formatted}"
    );
    let explicit_family =
        compile_operational_query_family(&parse_query(&formatted).expect("reparse"), &catalog)
            .expect("explicit group take caps maximum_groups");
    assert_eq!(
        explicit_family.aggregates()[0].maximum_groups(),
        &PageBound::Literal(2)
    );

    let parameters = QueryParameters::checked(BTreeMap::from([(
        "organization_id".to_owned(),
        CanonicalValue::Uuid([1; 16]),
    )]))
    .expect("parameters");
    let three_categories = ["alpha", "beta", "gamma"]
        .into_iter()
        .enumerate()
        .map(|(idx, category)| {
            row(
                "Ticket",
                &[
                    ("organization_id", CanonicalValue::Uuid([1; 16])),
                    ("ticket_id", CanonicalValue::Uuid([idx as u8 + 1; 16])),
                    ("status", CanonicalValue::string("Open").unwrap()),
                    ("urgent", CanonicalValue::Bool(false)),
                    ("category", CanonicalValue::string(category).unwrap()),
                    ("story_points", CanonicalValue::I64(1)),
                ],
            )
        })
        .collect();
    let mut view = FakeView {
        rows: BTreeMap::from([("tickets".to_owned(), three_categories)]),
    };
    assert_eq!(
        execute_operational_page_in_snapshot(
            explicit_family.select(&[]).expect("sole member").program(),
            explicit_family.aggregates(),
            &parameters,
            None,
            &mut view,
        ),
        Err(QueryExecutionError::BoundExceeded),
        "exceeding explicit group by take N fails closed rather than truncating"
    );
}

// req: RQL-002, RQL-003, RQL-004, QRY-001, QRY-002, OQ-001, OQ-002, OQ-003
#[test]
fn order_family_with_presence_and_candidate_operational_predicates_execute() {
    let contract = r#"
contract Support version 1 {
  enum TicketOrder { PriorityDesc, PriorityAsc }
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field status: string<32>
    field priority: i64
    field title: string<120>
    index by_org_priority (organization_id, priority, ticket_id)
    index by_org_status_priority (organization_id, status, priority, ticket_id)
  }
  entity TicketSignal {
    key (organization_id: uuid, signal_id: uuid)
    field ticket_id: uuid
    field score: i64
    field resolved_at: optional<timestamp>
    index by_score (organization_id, resolved_at, score, ticket_id, signal_id) presence(resolved_at)
    reference ticket (organization_id, ticket_id) -> Ticket(organization_id, ticket_id)
  }
  aggregate Tickets { root Ticket partition_by organization_id conflict_key (organization_id, ticket_id) }
  aggregate TicketSignals { root TicketSignal partition_by organization_id conflict_key (organization_id, signal_id) }
}
"#;
    let bundle = compile_contract_source(contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let query = r#"
query SignaledInbox(
    $organization_id: Ticket.organization_id,
    $min_score: TicketSignal.score,
    $status: Ticket.status?,
    $order: TicketOrder
) {
    candidates hot_ids: Ticket.ticket_id
        from TicketSignal.ticket_id using by_score where organization_id == $organization_id && resolved_at is null && score >= $min_score
        within 50 else IntegrityFailure

    many tickets from Ticket
        where organization_id == $organization_id
          && when $status { status == $status }
          && ticket_id in hot_ids
        order by $order {
            PriorityDesc: priority desc, ticket_id desc;
            PriorityAsc: priority asc, ticket_id asc;
        }
        take 20

    return Found {
        tickets: tickets { ticket_id status priority title }
    }
    outcomes Found | IntegrityFailure
}
"#;
    let family = riffdb_query_compiler::compile_order_query_family(
        &parse_query(query).expect("parse"),
        &catalog,
    )
    .expect("compile order family with presence and candidate operational predicates");
    assert_eq!(family.members().len(), 4);

    let mut view = FakeView {
        rows: BTreeMap::from([
            (
                "@candidate:hot_ids:0".to_owned(),
                vec![
                    row(
                        "TicketSignal",
                        &[
                            ("organization_id", CanonicalValue::Uuid([1; 16])),
                            ("signal_id", CanonicalValue::Uuid([91; 16])),
                            ("ticket_id", CanonicalValue::Uuid([10; 16])),
                            ("score", CanonicalValue::I64(90)),
                            ("resolved_at", CanonicalValue::Null),
                        ],
                    ),
                    row(
                        "TicketSignal",
                        &[
                            ("organization_id", CanonicalValue::Uuid([1; 16])),
                            ("signal_id", CanonicalValue::Uuid([92; 16])),
                            ("ticket_id", CanonicalValue::Uuid([20; 16])),
                            ("score", CanonicalValue::I64(85)),
                            ("resolved_at", CanonicalValue::Null),
                        ],
                    ),
                ],
            ),
            (
                "tickets".to_owned(),
                vec![
                    row(
                        "Ticket",
                        &[
                            ("organization_id", CanonicalValue::Uuid([1; 16])),
                            ("ticket_id", CanonicalValue::Uuid([10; 16])),
                            ("status", CanonicalValue::string("Open").unwrap()),
                            ("priority", CanonicalValue::I64(10)),
                            ("title", CanonicalValue::string("First").unwrap()),
                        ],
                    ),
                    row(
                        "Ticket",
                        &[
                            ("organization_id", CanonicalValue::Uuid([1; 16])),
                            ("ticket_id", CanonicalValue::Uuid([20; 16])),
                            ("status", CanonicalValue::string("Closed").unwrap()),
                            ("priority", CanonicalValue::I64(20)),
                            ("title", CanonicalValue::string("Second").unwrap()),
                        ],
                    ),
                ],
            ),
        ]),
    };

    let open_params = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        ("min_score".to_owned(), CanonicalValue::I64(80)),
        ("status".to_owned(), CanonicalValue::string("Open").unwrap()),
        (
            "order".to_owned(),
            CanonicalValue::string("PriorityDesc").unwrap(),
        ),
    ]))
    .expect("params");
    let open_member = family
        .select_name_with_presence(&[true], "PriorityDesc")
        .expect("select open member");
    let open_result = execute_operational_page_in_snapshot(
        open_member.program(),
        family.aggregates(),
        &open_params,
        None,
        &mut view,
    )
    .expect("execute open member");
    let Some(QueryResultValue::Many(open_rows)) = open_result.fields().get("tickets") else {
        panic!("tickets missing");
    };
    assert_eq!(open_rows.len(), 1);
    assert_eq!(
        open_rows[0].field("ticket_id"),
        Some(&CanonicalValue::Uuid([10; 16]))
    );
}

// req: RQL-002, RQL-003, QRY-001, QRY-002
#[test]
fn non_partition_bounded_set_with_order_executes_k_way_merge() {
    let contract = r#"
contract Support version 1 {
  enum TicketStatus { Open, InProgress, Blocked, Closed }
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field status: TicketStatus
    field updated_at: i64
    field title: string<120>
    index by_org_updated (organization_id, updated_at, ticket_id)
    index by_org_status_updated (organization_id, status, updated_at, ticket_id)
  }
  aggregate Tickets { root Ticket partition_by organization_id conflict_key (organization_id, ticket_id) }
}
"#;
    let bundle = compile_contract_source(contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let query = r#"
query FilteredBoard(
    $organization_id: Ticket.organization_id,
    $statuses: Set<Ticket.status, 4>?,
    $after: Cursor?
) {
    many tickets from Ticket
        where organization_id == $organization_id
          && when $statuses { status in $statuses }
        order by updated_at desc, ticket_id desc
        take 2 after $after

    return Found {
        tickets: tickets { ticket_id status updated_at title }
    }
    outcomes Found
}
"#;
    let family = compile_operational_query_family(&parse_query(query).expect("parse"), &catalog)
        .expect("compile non-partition bounded set with order");
    let present_member = family.select(&[true]).expect("present member");
    assert!(matches!(
        present_member.program().steps()[0].access(),
        riffdb_query_ir::QueryAccessKind::PartitionSetIndex { index, .. } if index == "by_org_status_updated"
    ));

    let status_enum = catalog.enumeration("TicketStatus").expect("TicketStatus");
    let status_val = |name: &str| CanonicalValue::Enum {
        type_id: status_enum.internal_id(),
        variant_id: status_enum.variant(name).expect("variant"),
    };

    let all_tickets = vec![
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([4; 16])),
                ("status", status_val("Closed")),
                ("updated_at", CanonicalValue::I64(400)),
                ("title", CanonicalValue::string("Closed 400").unwrap()),
            ],
        ),
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([2; 16])),
                ("status", status_val("Blocked")),
                ("updated_at", CanonicalValue::I64(300)),
                ("title", CanonicalValue::string("Blocked 300").unwrap()),
            ],
        ),
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([3; 16])),
                ("status", status_val("Open")),
                ("updated_at", CanonicalValue::I64(200)),
                ("title", CanonicalValue::string("Open 200").unwrap()),
            ],
        ),
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([1; 16])),
                ("status", status_val("Open")),
                ("updated_at", CanonicalValue::I64(100)),
                ("title", CanonicalValue::string("Open 100").unwrap()),
            ],
        ),
    ];

    let params = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        (
            "statuses".to_owned(),
            CanonicalValue::list(vec![status_val("Open"), status_val("Blocked")]).unwrap(),
        ),
    ]))
    .expect("params");

    let mut view = FakeView {
        rows: BTreeMap::from([("tickets".to_owned(), all_tickets.clone())]),
    };
    let first_page = execute_operational_page_in_snapshot(
        present_member.program(),
        family.aggregates(),
        &params,
        None,
        &mut view,
    )
    .expect("execute first page");
    let Some(QueryResultValue::Many(first_rows)) = first_page.fields().get("tickets") else {
        panic!("tickets missing");
    };
    assert_eq!(first_rows.len(), 2);
    assert_eq!(
        first_rows[0].field("updated_at"),
        Some(&CanonicalValue::I64(300))
    );
    assert_eq!(
        first_rows[1].field("updated_at"),
        Some(&CanonicalValue::I64(200))
    );
    let next_cursor = QueryContinuation::checked(
        first_page
            .continuation_binding()
            .expect("binding")
            .to_owned(),
        first_page.continuation().expect("next cursor").to_vec(),
        first_page.index_epochs().clone(),
    )
    .expect("checked continuation");

    let mut second_view = FakeView {
        rows: BTreeMap::from([("tickets".to_owned(), all_tickets)]),
    };
    let second_page = execute_operational_page_in_snapshot(
        present_member.program(),
        family.aggregates(),
        &params,
        Some(&next_cursor),
        &mut second_view,
    )
    .expect("execute second page");
    let Some(QueryResultValue::Many(second_rows)) = second_page.fields().get("tickets") else {
        panic!("tickets missing");
    };
    assert_eq!(second_rows.len(), 1);
    assert_eq!(
        second_rows[0].field("updated_at"),
        Some(&CanonicalValue::I64(100))
    );
    assert!(second_page.continuation().is_none());
}

// req: RQL-002, RQL-003, QRY-001, QRY-002
#[test]
fn single_partition_mixed_direction_order_executes_bounded_merge() {
    let contract = r#"
contract Support version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field priority: i64
    field created_at: i64
    field title: string<120>
    index by_priority_created (organization_id, priority, created_at, ticket_id)
  }
  aggregate Tickets { root Ticket partition_by organization_id conflict_key (organization_id, ticket_id) }
}
"#;
    let bundle = compile_contract_source(contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let query = r#"
query MixedOrderQueue(
    $organization_id: Ticket.organization_id,
    $after: Cursor?
) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by priority desc, created_at asc, ticket_id asc
        take 2 after $after

    return Found {
        tickets: tickets { ticket_id priority created_at title }
    }
    outcomes Found
}
"#;
    let program = compile_query(&parse_query(query).expect("parse"), &catalog)
        .expect("compile single-partition mixed direction order");
    assert!(matches!(
        program.steps()[0].access(),
        riffdb_query_ir::QueryAccessKind::PartitionSetIndex { index, .. } if index == "by_priority_created"
    ));

    let rows = vec![
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([1; 16])),
                ("priority", CanonicalValue::I64(10)),
                ("created_at", CanonicalValue::I64(500)),
                ("title", CanonicalValue::string("P10 Later").unwrap()),
            ],
        ),
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([2; 16])),
                ("priority", CanonicalValue::I64(10)),
                ("created_at", CanonicalValue::I64(100)),
                ("title", CanonicalValue::string("P10 Earlier").unwrap()),
            ],
        ),
        row(
            "Ticket",
            &[
                ("organization_id", CanonicalValue::Uuid([1; 16])),
                ("ticket_id", CanonicalValue::Uuid([3; 16])),
                ("priority", CanonicalValue::I64(5)),
                ("created_at", CanonicalValue::I64(50)),
                ("title", CanonicalValue::string("P5 Earliest").unwrap()),
            ],
        ),
    ];

    let params = QueryParameters::checked(BTreeMap::from([(
        "organization_id".to_owned(),
        CanonicalValue::Uuid([1; 16]),
    )]))
    .expect("params");

    let mut view = FakeView {
        rows: BTreeMap::from([("tickets".to_owned(), rows.clone())]),
    };
    let first_page =
        execute_page_in_snapshot(&program, &params, None, &mut view).expect("first page");
    let Some(QueryResultValue::Many(first_rows)) = first_page.fields().get("tickets") else {
        panic!("tickets missing");
    };
    assert_eq!(first_rows.len(), 2);
    // Priority 10 comes first (desc), and within priority 10 created_at 100 comes before 500 (asc)
    assert_eq!(
        first_rows[0].field("ticket_id"),
        Some(&CanonicalValue::Uuid([2; 16]))
    );
    assert_eq!(
        first_rows[1].field("ticket_id"),
        Some(&CanonicalValue::Uuid([1; 16]))
    );
    let cursor = QueryContinuation::checked(
        first_page
            .continuation_binding()
            .expect("binding")
            .to_owned(),
        first_page.continuation().expect("cursor").to_vec(),
        first_page.index_epochs().clone(),
    )
    .expect("checked continuation");

    let mut second_view = FakeView {
        rows: BTreeMap::from([("tickets".to_owned(), rows)]),
    };
    let second_page = execute_page_in_snapshot(&program, &params, Some(&cursor), &mut second_view)
        .expect("second page");
    let Some(QueryResultValue::Many(second_rows)) = second_page.fields().get("tickets") else {
        panic!("tickets missing");
    };
    assert_eq!(second_rows.len(), 1);
    assert_eq!(
        second_rows[0].field("ticket_id"),
        Some(&CanonicalValue::Uuid([3; 16]))
    );
    assert!(second_page.continuation().is_none());
}

// req: RQL-003, RQL-004, QRY-001, QRY-007
#[test]
fn covered_index_executes_across_multi_binding_singular_and_aggregate_steps() {
    let contract = r#"
contract Support version 1 {
  enum TicketStatus { Open, Closed }
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field slug: string<64>
    field status: TicketStatus
    field priority: i64
    field title: string<120>
    index by_org_ticket (organization_id, ticket_id) cover (slug, status, priority, title)
    index by_org_status_priority (organization_id, status, priority, ticket_id) cover (title)
  }
  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }
}
"#;
    let bundle = compile_contract_source(contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let query = r#"
query CoveredDashboard(
    $organization_id: Ticket.organization_id,
    $pinned_id: Ticket.ticket_id,
    $status: Ticket.status
) {
    maybe pinned from Ticket
        where organization_id == $organization_id
          && ticket_id == $pinned_id

    many tickets from Ticket
        where organization_id == $organization_id
          && status == $status
        order by priority desc, ticket_id desc
        take 10

    aggregate summary from tickets {
        group by status
        count() as ticket_count
        sum(priority) as total_priority
    }

    return Found {
        pinned: pinned { ticket_id slug status priority title }
        tickets: tickets { ticket_id status priority title }
        summary: summary { status ticket_count total_priority }
    }
    outcomes Found
}
"#;
    let parsed = parse_query(query).expect("parse");
    let family = compile_operational_query_family(&parsed, &catalog)
        .expect("multi-binding + singular + aggregate covered query compiles");
    let program = family.select(&[]).expect("sole member").program();
    assert_eq!(program.steps().len(), 2);
    assert!(program.steps()[0].covered_result_layout().is_some());
    assert!(program.steps()[1].covered_result_layout().is_some());
    assert_eq!(program.cost().point_reads(), 0);

    let open = CanonicalValue::Enum {
        type_id: riffdb_types::EnumTypeId::new(1).expect("enum id"),
        variant_id: riffdb_types::EnumVariantId::new(1).expect("variant id"),
    };
    let rows = BTreeMap::from([
        (
            "pinned".to_owned(),
            vec![row(
                "Ticket",
                &[
                    ("organization_id", CanonicalValue::Uuid([1; 16])),
                    ("ticket_id", CanonicalValue::Uuid([9; 16])),
                    ("slug", CanonicalValue::string("pinned-1").expect("slug")),
                    ("status", open.clone()),
                    ("priority", CanonicalValue::I64(10)),
                    ("title", CanonicalValue::string("Pinned").expect("title")),
                ],
            )],
        ),
        (
            "tickets".to_owned(),
            vec![
                row(
                    "Ticket",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("ticket_id", CanonicalValue::Uuid([1; 16])),
                        ("status", open.clone()),
                        ("priority", CanonicalValue::I64(5)),
                        ("title", CanonicalValue::string("First").expect("title")),
                    ],
                ),
                row(
                    "Ticket",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("ticket_id", CanonicalValue::Uuid([2; 16])),
                        ("status", open.clone()),
                        ("priority", CanonicalValue::I64(3)),
                        ("title", CanonicalValue::string("Second").expect("title")),
                    ],
                ),
            ],
        ),
    ]);
    let params = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        ("pinned_id".to_owned(), CanonicalValue::Uuid([9; 16])),
        ("status".to_owned(), open),
    ]))
    .expect("params");
    let mut view = FakeView { rows };
    let snapshot = execute_operational_page_in_snapshot(
        program,
        family.aggregates(),
        &params,
        None,
        &mut view,
    )
    .expect("covered multi-binding + aggregate execution succeeds");
    let Some(QueryResultValue::Maybe(Some(pinned_row))) = snapshot.fields().get("pinned") else {
        panic!("pinned missing");
    };
    assert_eq!(
        pinned_row.field("title"),
        Some(&CanonicalValue::string("Pinned").expect("title"))
    );
    let Some(QueryResultValue::Many(ticket_rows)) = snapshot.fields().get("tickets") else {
        panic!("tickets missing");
    };
    assert_eq!(ticket_rows.len(), 2);
    let Some(QueryResultValue::AggregateMany(summary_rows)) = snapshot.fields().get("summary")
    else {
        panic!("summary missing");
    };
    assert_eq!(summary_rows.len(), 1);
    assert_eq!(
        summary_rows[0].fields().get("ticket_count"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::U64(2)))
    );
}

// req: RQL-002, RQL-003, RQL-004, QRY-001, QRY-002, QRY-007
#[test]
fn real_world_complex_queries_compile_and_execute() {
    let contract = r#"
contract SupportWorkspace version 1 {
  enum TicketStatus { Open, InProgress, Closed }
  enum TicketOrder { PriorityDesc, PriorityAsc }
  entity User {
    key (organization_id: uuid, user_id: uuid)
    field display_name: string<80>
  }
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field status: TicketStatus
    field priority: i64
    field assignee_id: optional<uuid>
    field title: string<120>
    index by_org_ticket (organization_id, ticket_id) cover (status, priority, assignee_id, title)
    index by_org_priority (organization_id, priority, ticket_id) cover (status, assignee_id, title)
    index by_org_status_priority (organization_id, status, priority, ticket_id) cover (assignee_id, title)
  }
  entity TicketSignal {
    key (organization_id: uuid, signal_id: uuid)
    field ticket_id: uuid
    field score: i64
    field label: string<64>
    field resolved_at: optional<timestamp>
    index by_score (organization_id, resolved_at, score, ticket_id, signal_id) presence(resolved_at)
    index by_label (organization_id, label, ticket_id, signal_id) text_key(label, binary_utf8_v1)
    reference ticket (organization_id, ticket_id) -> Ticket(organization_id, ticket_id)
  }
  aggregate Users { root User partition_by organization_id conflict_key (organization_id, user_id) }
  aggregate Tickets { root Ticket partition_by organization_id conflict_key (organization_id, ticket_id) }
  aggregate TicketSignals { root TicketSignal partition_by organization_id conflict_key (organization_id, signal_id) }
}
"#;
    let bundle = compile_contract_source(contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");

    // 1. Combined `when $status` + `order by $order` + operational `candidates` + `in distinct(...)` + `aggregate`
    let inbox_query = r#"
query TriageWorkspace(
    $organization_id: Ticket.organization_id,
    $min_score: TicketSignal.score,
    $label_prefix: TicketSignal.label,
    $status: Ticket.status?,
    $order: TicketOrder
) {
    candidates hot_ids: Ticket.ticket_id
        from intersect {
            TicketSignal.ticket_id using by_score where organization_id == $organization_id && resolved_at is null && score >= $min_score,
            TicketSignal.ticket_id using by_label where organization_id == $organization_id && label prefix $label_prefix,
        }
        within 50 else IntegrityFailure

    many tickets from Ticket
        where organization_id == $organization_id
          && when $status { status == $status }
          && ticket_id in hot_ids
        order by $order {
            PriorityDesc: priority desc, ticket_id desc;
            PriorityAsc: priority asc, ticket_id asc;
        }
        take 25

    many assignees from User
        where organization_id == $organization_id
          && user_id in distinct(tickets.assignee_id)
        take 25

    aggregate status_counts from tickets {
        group by status
        count() as ticket_count
        sum(priority) as total_priority
    }

    return Found {
        tickets: tickets { ticket_id status priority assignee_id title }
        assignees: assignees { user_id display_name }
        status_counts: status_counts { status ticket_count total_priority }
    }
    outcomes Found | IntegrityFailure
}
"#;
    let family = riffdb_query_compiler::compile_order_query_family(
        &parse_query(inbox_query).expect("parse inbox"),
        &catalog,
    )
    .expect("compile triage workspace order family");
    assert_eq!(family.members().len(), 4);

    let open = CanonicalValue::Enum {
        type_id: riffdb_types::EnumTypeId::new(1).expect("enum id"),
        variant_id: riffdb_types::EnumVariantId::new(1).expect("variant id"),
    };
    let in_progress = CanonicalValue::Enum {
        type_id: riffdb_types::EnumTypeId::new(1).expect("enum id"),
        variant_id: riffdb_types::EnumVariantId::new(2).expect("variant id"),
    };
    let mut view = FakeView {
        rows: BTreeMap::from([
            (
                "@candidate:hot_ids:0".to_owned(),
                vec![
                    row(
                        "TicketSignal",
                        &[
                            ("organization_id", CanonicalValue::Uuid([1; 16])),
                            ("signal_id", CanonicalValue::Uuid([81; 16])),
                            ("ticket_id", CanonicalValue::Uuid([10; 16])),
                            ("score", CanonicalValue::I64(95)),
                            ("resolved_at", CanonicalValue::Null),
                        ],
                    ),
                    row(
                        "TicketSignal",
                        &[
                            ("organization_id", CanonicalValue::Uuid([1; 16])),
                            ("signal_id", CanonicalValue::Uuid([82; 16])),
                            ("ticket_id", CanonicalValue::Uuid([20; 16])),
                            ("score", CanonicalValue::I64(85)),
                            ("resolved_at", CanonicalValue::Null),
                        ],
                    ),
                ],
            ),
            (
                "@candidate:hot_ids:1".to_owned(),
                vec![row(
                    "TicketSignal",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("signal_id", CanonicalValue::Uuid([83; 16])),
                        ("ticket_id", CanonicalValue::Uuid([10; 16])),
                        (
                            "label",
                            CanonicalValue::string("sev:critical").expect("label"),
                        ),
                    ],
                )],
            ),
            (
                "tickets".to_owned(),
                vec![row(
                    "Ticket",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("ticket_id", CanonicalValue::Uuid([10; 16])),
                        ("status", open.clone()),
                        ("priority", CanonicalValue::I64(10)),
                        ("assignee_id", CanonicalValue::Uuid([7; 16])),
                        ("title", CanonicalValue::string("Outage").expect("title")),
                    ],
                )],
            ),
            (
                "assignees".to_owned(),
                vec![row(
                    "User",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("user_id", CanonicalValue::Uuid([7; 16])),
                        (
                            "display_name",
                            CanonicalValue::string("On-Call").expect("name"),
                        ),
                    ],
                )],
            ),
        ]),
    };
    let params = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        ("min_score".to_owned(), CanonicalValue::I64(80)),
        (
            "label_prefix".to_owned(),
            CanonicalValue::string("sev:").expect("prefix"),
        ),
        ("status".to_owned(), open.clone()),
        (
            "order".to_owned(),
            CanonicalValue::string("PriorityDesc").expect("order"),
        ),
    ]))
    .expect("params");
    let member = family
        .select_name_with_presence(&[true], "PriorityDesc")
        .expect("member");
    let snapshot = execute_operational_page_in_snapshot(
        member.program(),
        family.aggregates(),
        &params,
        None,
        &mut view,
    )
    .expect("execute triage workspace");
    let Some(QueryResultValue::Many(tickets)) = snapshot.fields().get("tickets") else {
        panic!("tickets missing");
    };
    assert_eq!(tickets.len(), 1);
    assert_eq!(
        tickets[0].field("ticket_id"),
        Some(&CanonicalValue::Uuid([10; 16]))
    );
    let Some(QueryResultValue::Many(assignees)) = snapshot.fields().get("assignees") else {
        panic!("assignees missing");
    };
    assert_eq!(assignees.len(), 1);
    let Some(QueryResultValue::AggregateMany(counts)) = snapshot.fields().get("status_counts")
    else {
        panic!("status_counts missing");
    };
    assert_eq!(counts.len(), 1);

    // 2. Non-partition `status in $statuses` + mixed `priority desc, ticket_id asc` k-way merge
    let multi_status_query = r#"
query ActiveQueue(
    $organization_id: Ticket.organization_id,
    $statuses: Set<Ticket.status, 4>
) {
    many tickets from Ticket
        where organization_id == $organization_id
          && status in $statuses
        order by priority desc, ticket_id asc
        take 5

    return Found {
        tickets: tickets { ticket_id status priority title }
    }
    outcomes Found
}
"#;
    let queue_program = compile_query(
        &parse_query(multi_status_query).expect("parse queue"),
        &catalog,
    )
    .expect("compile multi-status mixed-order queue");
    let mut queue_view = FakeView {
        rows: BTreeMap::from([(
            "tickets".to_owned(),
            vec![
                row(
                    "Ticket",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("ticket_id", CanonicalValue::Uuid([2; 16])),
                        ("status", open.clone()),
                        ("priority", CanonicalValue::I64(10)),
                        ("title", CanonicalValue::string("B").expect("title")),
                    ],
                ),
                row(
                    "Ticket",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("ticket_id", CanonicalValue::Uuid([1; 16])),
                        ("status", in_progress.clone()),
                        ("priority", CanonicalValue::I64(10)),
                        ("title", CanonicalValue::string("A").expect("title")),
                    ],
                ),
            ],
        )]),
    };
    let queue_params = QueryParameters::checked(BTreeMap::from([
        ("organization_id".to_owned(), CanonicalValue::Uuid([1; 16])),
        (
            "statuses".to_owned(),
            CanonicalValue::list(vec![open, in_progress]).expect("statuses"),
        ),
    ]))
    .expect("queue params");
    let queue_snapshot =
        execute_page_in_snapshot(&queue_program, &queue_params, None, &mut queue_view)
            .expect("execute queue");
    let Some(QueryResultValue::Many(queue_rows)) = queue_snapshot.fields().get("tickets") else {
        panic!("tickets missing");
    };
    assert_eq!(queue_rows.len(), 2);
    assert_eq!(
        queue_rows[0].field("ticket_id"),
        Some(&CanonicalValue::Uuid([1; 16]))
    );
    assert_eq!(
        queue_rows[1].field("ticket_id"),
        Some(&CanonicalValue::Uuid([2; 16]))
    );
}

// req: RQL-002, RQL-003, QRY-001, QRY-002
#[test]
fn optional_money_and_filtered_aggregate_measures_execute() {
    let contract = r#"
contract BillingAggregates version 1 {
  enum InvoiceStatus { Open, Paid, Void }
  entity Invoice {
    key (organization_id: uuid, invoice_id: uuid)
    field status: InvoiceStatus
    field amount: money<USD>
    field discount: optional<money<USD>>
    field seats: optional<i64>
    field auto_pay: optional<bool>
    index by_org_invoice (organization_id, invoice_id)
  }
  aggregate Invoices { root Invoice partition_by organization_id conflict_key (organization_id, invoice_id) }
}
"#;
    let query = r#"
query BillingSummary($organization_id: Invoice.organization_id) {
    many invoices from Invoice
        where organization_id == $organization_id
        order by invoice_id asc
        take 20
    aggregate summary from invoices {
        count() as total_invoices
        count() where status == InvoiceStatus.Paid as paid_invoices
        sum(amount) as gross_amount
        sum(amount) where status == InvoiceStatus.Paid as paid_amount
        sum(discount) as total_discount
        mean(seats) as avg_seats
        any(auto_pay) as any_autopay
        all(auto_pay) as all_autopay
    }
    return Found {
        summary: summary {
            total_invoices paid_invoices gross_amount paid_amount
            total_discount avg_seats any_autopay all_autopay
        }
    }
    outcomes Found
}
"#;
    let bundle = compile_contract_source(contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family = compile_operational_query_family(&parse_query(query).expect("query"), &catalog)
        .expect("family");
    let program = family.select(&[]).expect("member").program();
    let usd = riffdb_types::CurrencyCode::new("USD").expect("usd");
    let spec = riffdb_types::DecimalSpec::new(38, 2).expect("spec");
    let money = |cents: i128| {
        CanonicalValue::Money(riffdb_types::Money::new(
            usd,
            riffdb_types::Decimal::new(spec, cents).expect("decimal"),
        ))
    };
    let open = CanonicalValue::Enum {
        type_id: riffdb_types::EnumTypeId::new(1).expect("type"),
        variant_id: riffdb_types::EnumVariantId::new(1).expect("variant"),
    };
    let paid = CanonicalValue::Enum {
        type_id: riffdb_types::EnumTypeId::new(1).expect("type"),
        variant_id: riffdb_types::EnumVariantId::new(2).expect("variant"),
    };
    let mut view = FakeView {
        rows: BTreeMap::from([(
            "invoices".to_owned(),
            vec![
                row(
                    "Invoice",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("invoice_id", CanonicalValue::Uuid([1; 16])),
                        ("status", paid.clone()),
                        ("amount", money(15_000)),
                        ("discount", money(1_500)),
                        ("seats", CanonicalValue::I64(10)),
                        ("auto_pay", CanonicalValue::Bool(true)),
                    ],
                ),
                row(
                    "Invoice",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("invoice_id", CanonicalValue::Uuid([2; 16])),
                        ("status", open),
                        ("amount", money(5_000)),
                        ("discount", CanonicalValue::Null),
                        ("seats", CanonicalValue::Null),
                        ("auto_pay", CanonicalValue::Null),
                    ],
                ),
                row(
                    "Invoice",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("invoice_id", CanonicalValue::Uuid([3; 16])),
                        ("status", paid),
                        ("amount", money(25_000)),
                        ("discount", money(2_500)),
                        ("seats", CanonicalValue::I64(20)),
                        ("auto_pay", CanonicalValue::Bool(true)),
                    ],
                ),
            ],
        )]),
    };
    let params = QueryParameters::checked(BTreeMap::from([(
        "organization_id".to_owned(),
        CanonicalValue::Uuid([1; 16]),
    )]))
    .expect("params");
    let snapshot = execute_operational_page_in_snapshot(
        program,
        family.aggregates(),
        &params,
        None,
        &mut view,
    )
    .expect("execute billing summary");
    let Some(QueryResultValue::AggregateOne(summary)) = snapshot.fields().get("summary") else {
        panic!("summary missing");
    };
    assert_eq!(
        summary.fields().get("total_invoices"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::U64(3)))
    );
    assert_eq!(
        summary.fields().get("paid_invoices"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::U64(2)))
    );
    assert_eq!(
        summary.fields().get("gross_amount"),
        Some(&QueryAggregateCell::ExactDecimal {
            coefficient: 45_000,
            scale: 2,
        })
    );
    assert_eq!(
        summary.fields().get("paid_amount"),
        Some(&QueryAggregateCell::ExactDecimal {
            coefficient: 40_000,
            scale: 2,
        })
    );
    assert_eq!(
        summary.fields().get("total_discount"),
        Some(&QueryAggregateCell::ExactDecimal {
            coefficient: 4_000,
            scale: 2,
        })
    );
    assert_eq!(
        summary.fields().get("avg_seats"),
        Some(&QueryAggregateCell::ExactMean {
            coefficient: 30,
            scale: 0,
            count: 2,
        })
    );
    assert_eq!(
        summary.fields().get("any_autopay"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::Bool(true)))
    );
    assert_eq!(
        summary.fields().get("all_autopay"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::Bool(true)))
    );
}

// req: RQL-004, RQL-008, QRY-001, QRY-007
#[test]
fn grouped_aggregate_temporal_buckets_having_and_order_by_execute() {
    let contract = r#"
contract UsageTimeSeries version 1 {
  entity UsageEvent {
    key (organization_id: uuid, event_id: uuid)
    field occurred_at: timestamp
    field tokens: i64
    index by_org_event (organization_id, event_id)
  }
  aggregate UsageEvents { root UsageEvent partition_by organization_id conflict_key (organization_id, event_id) }
}
"#;
    let query = r#"
query DailyUsageLeaderboard($organization_id: UsageEvent.organization_id) {
    many events from UsageEvent
        where organization_id == $organization_id
        order by event_id asc
        take 25
    aggregate daily from events {
        group by day(occurred_at) as day_bucket
        count() as event_count
        sum(tokens) as total_tokens
        having event_count >= 2
        order by total_tokens desc, day_bucket asc
        take 2
    }
    return Found {
        daily: daily { day_bucket event_count total_tokens }
    }
    outcomes Found
}
"#;
    let bundle = compile_contract_source(contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family = compile_operational_query_family(&parse_query(query).expect("query"), &catalog)
        .expect("family");
    let program = family.select(&[]).expect("member").program();
    let day1 = 1_700_006_400_i64; // exact UTC midnight: 19676 * 86400
    let day2 = day1 + 86_400;
    let day3 = day2 + 86_400;
    let day4 = day3 + 86_400;
    let ts = |secs: i64| CanonicalValue::Timestamp(Timestamp::new(secs, 500).expect("ts"));
    let mut view = FakeView {
        rows: BTreeMap::from([(
            "events".to_owned(),
            vec![
                row(
                    "UsageEvent",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("event_id", CanonicalValue::Uuid([1; 16])),
                        ("occurred_at", ts(day1 + 120)),
                        ("tokens", CanonicalValue::I64(100)),
                    ],
                ),
                row(
                    "UsageEvent",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("event_id", CanonicalValue::Uuid([2; 16])),
                        ("occurred_at", ts(day1 + 3600)),
                        ("tokens", CanonicalValue::I64(150)),
                    ],
                ),
                row(
                    "UsageEvent",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("event_id", CanonicalValue::Uuid([3; 16])),
                        ("occurred_at", ts(day2 + 60)),
                        ("tokens", CanonicalValue::I64(900)),
                    ],
                ),
                row(
                    "UsageEvent",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("event_id", CanonicalValue::Uuid([4; 16])),
                        ("occurred_at", ts(day3 + 10)),
                        ("tokens", CanonicalValue::I64(400)),
                    ],
                ),
                row(
                    "UsageEvent",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("event_id", CanonicalValue::Uuid([5; 16])),
                        ("occurred_at", ts(day3 + 20)),
                        ("tokens", CanonicalValue::I64(500)),
                    ],
                ),
                row(
                    "UsageEvent",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("event_id", CanonicalValue::Uuid([6; 16])),
                        ("occurred_at", ts(day4 + 10)),
                        ("tokens", CanonicalValue::I64(50)),
                    ],
                ),
                row(
                    "UsageEvent",
                    &[
                        ("organization_id", CanonicalValue::Uuid([1; 16])),
                        ("event_id", CanonicalValue::Uuid([7; 16])),
                        ("occurred_at", ts(day4 + 20)),
                        ("tokens", CanonicalValue::I64(60)),
                    ],
                ),
            ],
        )]),
    };
    let params = QueryParameters::checked(BTreeMap::from([(
        "organization_id".to_owned(),
        CanonicalValue::Uuid([1; 16]),
    )]))
    .expect("params");
    let snapshot = execute_operational_page_in_snapshot(
        program,
        family.aggregates(),
        &params,
        None,
        &mut view,
    )
    .expect("execute daily usage leaderboard");
    let Some(QueryResultValue::AggregateMany(rows)) = snapshot.fields().get("daily") else {
        panic!("daily missing");
    };
    // 4 distinct days existed (> take 2), day2 filtered out by having event_count >= 2,
    // remaining 3 days (day3=900, day1=250, day4=110) sorted desc by total_tokens and truncated to top 2!
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].fields().get("day_bucket"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::Timestamp(
            Timestamp::new(day3, 0).expect("day3")
        )))
    );
    assert_eq!(
        rows[0].fields().get("total_tokens"),
        Some(&QueryAggregateCell::ExactDecimal {
            coefficient: 900,
            scale: 0,
        })
    );
    assert_eq!(
        rows[1].fields().get("day_bucket"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::Timestamp(
            Timestamp::new(day1, 0).expect("day1")
        )))
    );
    assert_eq!(
        rows[1].fields().get("total_tokens"),
        Some(&QueryAggregateCell::ExactDecimal {
            coefficient: 250,
            scale: 0,
        })
    );
}

// req: RQL-002, RQL-003, RQL-004, QRY-002
#[test]
fn saas_quantiles_collections_and_per_parent_rollups_execute() {
    let contract = r#"
contract ProjectSlaRollups version 1 {
  enum TaskStatus { Open, Done }
  entity Project {
    key (organization_id: uuid, project_id: uuid)
    field name: string<64>
    index by_org_project (organization_id, project_id)
  }
  entity Task {
    key (organization_id: uuid, task_id: uuid)
    field project_id: uuid
    field status: TaskStatus
    field duration_ms: i64
    field tag: string<32>
    index by_project_task (organization_id, project_id, task_id)
    reference project (organization_id, project_id) -> Project(organization_id, project_id)
  }
  aggregate Projects { root Project partition_by organization_id conflict_key (organization_id, project_id) }
  aggregate Tasks { root Task partition_by organization_id conflict_key (organization_id, task_id) }
}
"#;
    let query = r#"
query ProjectRollup($organization_id: Project.organization_id) {
    many projects from Project
        where organization_id == $organization_id
        order by project_id asc
        take 5
    many tasks from Task for each project in projects
        where organization_id == $organization_id
          && project_id == project.project_id
        order by task_id asc
        take 10 per project
    aggregate task_stats from tasks per projects {
        count() as total_tasks
        count() where status == TaskStatus.Done as done_tasks
        median(duration_ms) as p50_ms
        p95(duration_ms) as p95_ms
        first(tag) as first_tag
        last(tag) as last_tag
        stddev(duration_ms) as stddev_ms
        collect_distinct(tag) as distinct_tags
    }
    return Found {
        projects: projects {
            project_id
            name
            task_stats {
                total_tasks done_tasks p50_ms p95_ms
                first_tag last_tag stddev_ms distinct_tags
            }
        }
    }
    outcomes Found
}
"#;
    let bundle = compile_contract_source(contract).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let family = compile_operational_query_family(&parse_query(query).expect("query"), &catalog)
        .expect("family");
    let program = family.select(&[]).expect("member").program();
    let task_status = catalog.enumeration("TaskStatus").expect("enum");
    let open = CanonicalValue::Enum {
        type_id: task_status.internal_id(),
        variant_id: task_status.variant("Open").expect("Open"),
    };
    let done = CanonicalValue::Enum {
        type_id: task_status.internal_id(),
        variant_id: task_status.variant("Done").expect("Done"),
    };
    let mut view = FakeView {
        rows: BTreeMap::from([
            (
                "projects".to_owned(),
                vec![
                    row(
                        "Project",
                        &[
                            ("organization_id", CanonicalValue::Uuid([1; 16])),
                            ("project_id", CanonicalValue::Uuid([10; 16])),
                            ("name", CanonicalValue::string("Alpha").expect("name")),
                        ],
                    ),
                    row(
                        "Project",
                        &[
                            ("organization_id", CanonicalValue::Uuid([1; 16])),
                            ("project_id", CanonicalValue::Uuid([20; 16])),
                            ("name", CanonicalValue::string("EmptyBeta").expect("name")),
                        ],
                    ),
                ],
            ),
            (
                "tasks".to_owned(),
                vec![
                    row(
                        "Task",
                        &[
                            ("organization_id", CanonicalValue::Uuid([1; 16])),
                            ("task_id", CanonicalValue::Uuid([1; 16])),
                            ("project_id", CanonicalValue::Uuid([10; 16])),
                            ("status", done.clone()),
                            ("duration_ms", CanonicalValue::I64(10)),
                            ("tag", CanonicalValue::string("api").expect("tag")),
                        ],
                    ),
                    row(
                        "Task",
                        &[
                            ("organization_id", CanonicalValue::Uuid([1; 16])),
                            ("task_id", CanonicalValue::Uuid([2; 16])),
                            ("project_id", CanonicalValue::Uuid([10; 16])),
                            ("status", open),
                            ("duration_ms", CanonicalValue::I64(20)),
                            ("tag", CanonicalValue::string("db").expect("tag")),
                        ],
                    ),
                    row(
                        "Task",
                        &[
                            ("organization_id", CanonicalValue::Uuid([1; 16])),
                            ("task_id", CanonicalValue::Uuid([3; 16])),
                            ("project_id", CanonicalValue::Uuid([10; 16])),
                            ("status", done),
                            ("duration_ms", CanonicalValue::I64(30)),
                            ("tag", CanonicalValue::string("api").expect("tag")),
                        ],
                    ),
                ],
            ),
        ]),
    };
    let params = QueryParameters::checked(BTreeMap::from([(
        "organization_id".to_owned(),
        CanonicalValue::Uuid([1; 16]),
    )]))
    .expect("params");
    let snapshot = execute_operational_page_in_snapshot(
        program,
        family.aggregates(),
        &params,
        None,
        &mut view,
    )
    .expect("execute project rollup");
    let Some(QueryResultValue::Many(projects)) = snapshot.fields().get("projects") else {
        panic!("projects missing");
    };
    assert_eq!(projects.len(), 2);
    let alpha_stats = projects[0]
        .nested_aggregate_rows("task_stats")
        .expect("alpha task_stats");
    assert_eq!(alpha_stats.len(), 1);
    assert_eq!(
        alpha_stats[0].fields().get("total_tasks"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::U64(3)))
    );
    assert_eq!(
        alpha_stats[0].fields().get("done_tasks"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::U64(2)))
    );
    assert_eq!(
        alpha_stats[0].fields().get("p50_ms"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::I64(20)))
    );
    assert_eq!(
        alpha_stats[0].fields().get("p95_ms"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::I64(30)))
    );
    assert_eq!(
        alpha_stats[0].fields().get("first_tag"),
        Some(&QueryAggregateCell::Canonical(
            CanonicalValue::string("api").expect("api")
        ))
    );
    assert_eq!(
        alpha_stats[0].fields().get("last_tag"),
        Some(&QueryAggregateCell::Canonical(
            CanonicalValue::string("api").expect("api")
        ))
    );
    // stddev([10, 20, 30]) = floor(sqrt(200/3)) = 8
    assert_eq!(
        alpha_stats[0].fields().get("stddev_ms"),
        Some(&QueryAggregateCell::ExactDecimal {
            coefficient: 8,
            scale: 0,
        })
    );
    assert_eq!(
        alpha_stats[0].fields().get("distinct_tags"),
        Some(&QueryAggregateCell::Canonical(
            CanonicalValue::list(vec![
                CanonicalValue::string("api").expect("api"),
                CanonicalValue::string("db").expect("db"),
            ])
            .expect("list")
        ))
    );

    let beta_stats = projects[1]
        .nested_aggregate_rows("task_stats")
        .expect("beta task_stats");
    assert_eq!(beta_stats.len(), 1);
    assert_eq!(
        beta_stats[0].fields().get("total_tasks"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::U64(0)))
    );
    assert_eq!(
        beta_stats[0].fields().get("p50_ms"),
        Some(&QueryAggregateCell::Canonical(CanonicalValue::Null))
    );
}

// req: RQL-002, RQL-003, RQL-004, RQL-008, QRY-001, QRY-002, QRY-007
#[test]
fn saas_complete_aggregations_compile_and_execute_end_to_end() {
    optional_money_and_filtered_aggregate_measures_execute();
    grouped_aggregate_temporal_buckets_having_and_order_by_execute();
    saas_quantiles_collections_and_per_parent_rollups_execute();
}
