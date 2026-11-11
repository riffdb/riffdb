//! Regressions for the v0.2.2 release review of composable EAV search and
//! runtime byte-budgeted pagination (ADR-0174, ADR-0347).
//!
//! The view here honours physical index order, the requested page limit, and
//! continuation, unlike the original proof mock, so a truncated helper page is
//! observable exactly as a storage adapter would produce it.

use super::*;
use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_compiler::{compile_order_query_family, compile_query};
use riffdb_query_ir::{OrderQueryFamilyV1, QueryAccessStep, SymbolicCatalog};
use riffdb_riffql_syntax::parse_query;

const CONTRACT: &str = r#"
contract MlflowRuns version 1 {
  enum RunOrder {
    MetricAsc,
    MetricDesc,
    StartTimeDesc,
    StartTimeAsc,
  }
  entity Run {
    key (experiment_id: string<64>, run_id: string<64>)
    field status: string<32>
    field start_time: i64
    field packed_tags: bytes<900000>
    field packed_params: bytes<700000>
    field packed_latest_metrics: bytes<400000>
    index by_start_time (experiment_id, start_time, run_id)
  }
  entity RunDatum {
    key (experiment_id: string<64>, run_id: string<64>, entry_id: string<160>)
    field kind: string<16>
    field item_key: string<120>
    field sort_key: i64
    index by_item_sort (experiment_id, kind, item_key, sort_key, run_id, entry_id)
    reference run (experiment_id, run_id) -> Run(experiment_id, run_id)
  }
  aggregate RunUnit {
    root Run
    partition_by experiment_id
    conflict_key (experiment_id, run_id)
  }
  aggregate RunDatumUnit {
    root RunDatum
    partition_by experiment_id
    conflict_key (experiment_id, run_id, entry_id)
  }
}
"#;

/// The ADR-0347 proof query (`composable_eav_predicates_and_joined_key_order_by_nulls_last_execute`).
const QUERY: &str = r#"
query SearchRunsComposable(
    $experiment_id: Run.experiment_id,
    $c1_kind: RunDatum.kind,
    $c1_item_key: RunDatum.item_key,
    $c1_min_sort_key: RunDatum.sort_key?,
    $c1_max_sort_key: RunDatum.sort_key?,
    $c2_kind: RunDatum.kind?,
    $c2_item_key: RunDatum.item_key?,
    $c2_min_sort_key: RunDatum.sort_key?,
    $c2_max_sort_key: RunDatum.sort_key?,
    $c3_kind: RunDatum.kind?,
    $c3_item_key: RunDatum.item_key?,
    $c3_min_sort_key: RunDatum.sort_key?,
    $c3_max_sort_key: RunDatum.sort_key?,
    $c4_kind: RunDatum.kind?,
    $c4_item_key: RunDatum.item_key?,
    $c4_min_sort_key: RunDatum.sort_key?,
    $c4_max_sort_key: RunDatum.sort_key?,
    $filter_status: Run.status?,
    $min_start_time: Run.start_time?,
    $max_start_time: Run.start_time?,
    $order_metric_key: RunDatum.item_key?,
    $order: RunOrder,
    $limit: Limit<100>,
    $after: Cursor?
) {
    candidates matching_runs: Run.run_id from intersect {
        RunDatum.run_id using by_item_sort
            where experiment_id == $experiment_id
              && kind == $c1_kind
              && item_key == $c1_item_key
              && when $c1_min_sort_key { sort_key >= $c1_min_sort_key }
              && when $c1_max_sort_key { sort_key <= $c1_max_sort_key },
        RunDatum.run_id using by_item_sort
            where experiment_id == $experiment_id
              && when $c2_kind { kind == $c2_kind }
              && when $c2_item_key { item_key == $c2_item_key }
              && when $c2_min_sort_key { sort_key >= $c2_min_sort_key }
              && when $c2_max_sort_key { sort_key <= $c2_max_sort_key },
        RunDatum.run_id using by_item_sort
            where experiment_id == $experiment_id
              && when $c3_kind { kind == $c3_kind }
              && when $c3_item_key { item_key == $c3_item_key }
              && when $c3_min_sort_key { sort_key >= $c3_min_sort_key }
              && when $c3_max_sort_key { sort_key <= $c3_max_sort_key },
        RunDatum.run_id using by_item_sort
            where experiment_id == $experiment_id
              && when $c4_kind { kind == $c4_kind }
              && when $c4_item_key { item_key == $c4_item_key }
              && when $c4_min_sort_key { sort_key >= $c4_min_sort_key }
              && when $c4_max_sort_key { sort_key <= $c4_max_sort_key },
    } within 200 else SearchRunsWindowExceeded

    many metric_ranks from RunDatum
        where experiment_id == $experiment_id
          && kind == "metric"
          && when $order_metric_key { item_key == $order_metric_key }
        order by sort_key asc, run_id asc, entry_id asc
        take 200

    many runs from Run
        where experiment_id == $experiment_id
          && run_id in matching_runs
          && when $filter_status { status == $filter_status }
          && when $min_start_time { start_time >= $min_start_time }
          && when $max_start_time { start_time <= $max_start_time }
        order by $order {
            MetricAsc: metric_ranks.sort_key asc nulls last, start_time desc, run_id asc;
            MetricDesc: metric_ranks.sort_key desc nulls last, start_time desc, run_id asc;
            StartTimeDesc: start_time desc, run_id asc;
            StartTimeAsc: start_time asc, run_id asc;
        }
        take $limit after $after else SearchRunsWindowExceeded

    return Found {
        runs: runs {
            experiment_id
            run_id
            status
            start_time
            packed_tags
            packed_params
            packed_latest_metrics
        }
    }
    outcomes Found | SearchRunsWindowExceeded
}
"#;

const C4_KIND_GUARD: &str = "&& when $c4_kind { kind == $c4_kind }";
const ROOT_MIN_START_GUARD: &str = "&& when $min_start_time { start_time >= $min_start_time }";

fn replace_once(source: &str, from: &str, to: &str) -> String {
    assert_eq!(source.matches(from).count(), 1, "fixture edit `{from}`");
    source.replacen(from, to, 1)
}

fn family(source: &str) -> OrderQueryFamilyV1 {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    compile_order_query_family(&parse_query(source).expect("parse"), &catalog).expect("family")
}

fn member<'a>(
    family: &'a OrderQueryFamilyV1,
    variant: &str,
    presence_mask: u16,
) -> &'a QueryAccessProgramV1 {
    family
        .members()
        .iter()
        .find(|m| m.variant_name() == variant && m.presence_mask() == presence_mask)
        .expect("member")
        .program()
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

fn text(value: &str) -> CanonicalValue {
    CanonicalValue::string(value).expect("string")
}

fn datum(run_id: &str, kind: &str, item_key: &str, sort_key: i64) -> QueryRow {
    row(
        "RunDatum",
        &[
            ("experiment_id", text("exp-1")),
            ("run_id", text(run_id)),
            ("entry_id", text(&format!("{kind}:{item_key}"))),
            ("kind", text(kind)),
            ("item_key", text(item_key)),
            ("sort_key", CanonicalValue::I64(sort_key)),
        ],
    )
}

fn run(run_id: &str, status: &str, start_time: i64) -> QueryRow {
    row(
        "Run",
        &[
            ("experiment_id", text("exp-1")),
            ("run_id", text(run_id)),
            ("status", text(status)),
            ("start_time", CanonicalValue::I64(start_time)),
            (
                "packed_tags",
                CanonicalValue::bytes(vec![1]).expect("bytes"),
            ),
            (
                "packed_params",
                CanonicalValue::bytes(vec![2]).expect("bytes"),
            ),
            (
                "packed_latest_metrics",
                CanonicalValue::bytes(vec![3]).expect("bytes"),
            ),
        ],
    )
}

/// The proof fixture plus run-6, which matches c1 and c3 but has no `param`
/// rows, so dropping a whole c2/c4 source becomes observable.
fn view() -> EavView {
    let data = vec![
        datum("run-1", "param", "optimizer", 10),
        datum("run-1", "tag", "stage", 20),
        datum("run-1", "metric", "val_loss", 120),
        datum("run-1", "metric", "latency", 50),
        datum("run-2", "param", "optimizer", 10),
        datum("run-2", "tag", "stage", 20),
        datum("run-2", "metric", "val_loss", 80),
        datum("run-3", "param", "optimizer", 10),
        datum("run-3", "tag", "stage", 20),
        datum("run-3", "metric", "val_loss", 140),
        datum("run-3", "metric", "latency", 10),
        // run-4 is excluded by c1 yet holds the lowest latency, so it ranks
        // first in the helper's physical order.
        datum("run-4", "param", "optimizer", 10),
        datum("run-4", "tag", "stage", 20),
        datum("run-4", "metric", "val_loss", 200),
        datum("run-4", "metric", "latency", 5),
        datum("run-5", "param", "optimizer", 99),
        datum("run-5", "tag", "stage", 20),
        datum("run-5", "metric", "val_loss", 50),
        datum("run-6", "tag", "stage", 20),
        datum("run-6", "metric", "val_loss", 60),
        datum("run-6", "metric", "latency", 30),
    ];
    let runs = [
        run("run-1", "FINISHED", 1000),
        run("run-2", "FINISHED", 2000),
        run("run-3", "FINISHED", 3000),
        run("run-4", "FINISHED", 4000),
        run("run-5", "FINISHED", 5000),
        run("run-6", "FINISHED", 6000),
    ]
    .into_iter()
    .map(|r| match r.field("run_id") {
        Some(CanonicalValue::String(id)) => (id.as_str().to_owned(), r),
        _ => panic!("run_id"),
    })
    .collect();
    EavView {
        data,
        runs,
        scans: 0,
    }
}

fn parameters(overrides: &[(&str, Option<CanonicalValue>)]) -> QueryParameters {
    let mut values = BTreeMap::from([
        ("experiment_id".to_owned(), text("exp-1")),
        ("c1_kind".to_owned(), text("metric")),
        ("c1_item_key".to_owned(), text("val_loss")),
        ("c1_min_sort_key".to_owned(), CanonicalValue::Null),
        ("c1_max_sort_key".to_owned(), CanonicalValue::I64(150)),
        ("c2_kind".to_owned(), text("param")),
        ("c2_item_key".to_owned(), text("optimizer")),
        ("c2_min_sort_key".to_owned(), CanonicalValue::I64(10)),
        ("c2_max_sort_key".to_owned(), CanonicalValue::I64(10)),
        ("c3_kind".to_owned(), text("tag")),
        ("c3_item_key".to_owned(), text("stage")),
        ("c3_min_sort_key".to_owned(), CanonicalValue::I64(20)),
        ("c3_max_sort_key".to_owned(), CanonicalValue::I64(20)),
        ("c4_kind".to_owned(), CanonicalValue::Null),
        ("c4_item_key".to_owned(), CanonicalValue::Null),
        ("c4_min_sort_key".to_owned(), CanonicalValue::Null),
        ("c4_max_sort_key".to_owned(), CanonicalValue::Null),
        ("filter_status".to_owned(), text("FINISHED")),
        ("min_start_time".to_owned(), CanonicalValue::Null),
        ("max_start_time".to_owned(), CanonicalValue::Null),
        ("order_metric_key".to_owned(), text("latency")),
        ("limit".to_owned(), CanonicalValue::U64(100)),
    ]);
    for (name, value) in overrides {
        match value {
            Some(value) => values.insert((*name).to_owned(), value.clone()),
            None => values.remove(*name),
        };
    }
    QueryParameters::checked(values).expect("parameters")
}

fn run_ids(snapshot: &QueryOwnedSnapshot) -> Vec<String> {
    let Some(QueryResultValue::Many(rows)) = snapshot.fields().get("runs") else {
        panic!("runs");
    };
    rows.iter()
        .map(|r| match r.field("run_id") {
            Some(CanonicalValue::String(s)) => s.as_str().to_owned(),
            _ => panic!("run_id"),
        })
        .collect()
}

fn ids(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

struct EavView {
    data: Vec<QueryRow>,
    runs: BTreeMap<String, QueryRow>,
    scans: usize,
}

fn compare_values(left: Option<&CanonicalValue>, right: Option<&CanonicalValue>) -> Ordering {
    match (left, right) {
        (Some(CanonicalValue::String(l)), Some(CanonicalValue::String(r))) => {
            l.as_str().cmp(r.as_str())
        }
        (Some(CanonicalValue::I64(l)), Some(CanonicalValue::I64(r))) => l.cmp(r),
        _ => Ordering::Equal,
    }
}

impl EavView {
    /// Matching rows in physical index order, one page of `limit` rows, and a
    /// continuation when more rows remain.
    fn page(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
    ) -> (Vec<QueryRow>, u64, Option<Vec<u8>>) {
        self.scans += 1;
        let QueryAccessKind::Index {
            fields, direction, ..
        } = step.access()
        else {
            panic!("index step");
        };
        let mut matched = self
            .data
            .iter()
            .filter(|r| crate::predicates_match(r, predicates) == Ok(true))
            .cloned()
            .collect::<Vec<_>>();
        matched.sort_by(|l, r| {
            fields
                .iter()
                .map(|f| compare_values(l.field(f), r.field(f)))
                .find(|ordering| ordering.is_ne())
                .unwrap_or(Ordering::Equal)
        });
        if *direction == AccessDirection::Reverse {
            matched.reverse();
        }
        let start = after.map_or(0, |cursor| usize::from(cursor[0]));
        let limit = usize::try_from(limit).expect("limit");
        let remaining = matched.into_iter().skip(start).collect::<Vec<_>>();
        let more = remaining.len() > limit;
        let page = remaining.into_iter().take(limit).collect::<Vec<_>>();
        let scanned = (page.len() + usize::from(more)) as u64;
        let next = more.then(|| vec![u8::try_from(start + page.len()).expect("cursor")]);
        (page, scanned, next)
    }
}

impl QueryReadView for EavView {
    type Error = ();
    fn fault(&self, _error: &Self::Error) -> QueryBackendFault {
        QueryBackendFault::Unavailable
    }
    fn application_head(&self) -> u64 {
        19
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
        _step: &QueryAccessStep,
        predicates: &[Vec<BoundPredicate>],
        _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
        Ok(predicates
            .iter()
            .map(|preds| {
                preds
                    .iter()
                    .find(|p| p.field() == "run_id")
                    .and_then(|p| match p.value() {
                        CanonicalValue::String(s) => self.runs.get(s.as_str()).cloned(),
                        _ => None,
                    })
            })
            .collect())
    }
    fn scan(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        _after_inclusive: bool,
        _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryScanPage, Self::Error> {
        let (rows, scanned, next) = self.page(step, predicates, limit, after);
        Ok(match next {
            Some(next) => QueryScanPage::continued(rows, 5, scanned, next).ok_or(())?,
            None => QueryScanPage::exact_end(rows, 5),
        })
    }
    fn scan_covered(
        &mut self,
        step: &QueryAccessStep,
        predicates: &[BoundPredicate],
        limit: u64,
        after: Option<&[u8]>,
        _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<Option<CoveredResultBatch>, Self::Error> {
        let layout = step.covered_result_layout().ok_or(())?.clone();
        let (rows, scanned, next) = self.page(step, predicates, limit, after);
        let values = rows
            .iter()
            .map(|r| {
                layout
                    .fields()
                    .iter()
                    .map(|f| r.field(f.name()).cloned().ok_or(()))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(
            CoveredResultBatch::checked(layout, values, 5, scanned, 0, next).ok_or(())?,
        ))
    }
    fn nearest(
        &mut self,
        _step: &QueryAccessStep,
        _predicates: &[BoundPredicate],
        _k: u32,
        _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
    ) -> Result<QueryNearestPage, Self::Error> {
        Err(())
    }
}

// req: RQL-002, RQL-003
#[test]
fn joined_order_refuses_a_truncated_helper_population() {
    // `take 2` keeps run-4 (5) and run-3 (10) and omits run-1 (50): run-1's
    // latency is indistinguishable from a missing metric, so either order would
    // be released wrongly. The query must fail closed instead.
    let source = replace_once(
        QUERY,
        "order by sort_key asc, run_id asc, entry_id asc\n        take 200",
        "order by sort_key asc, run_id asc, entry_id asc\n        take 2",
    );
    let family = family(&source);
    for variant in ["MetricAsc", "MetricDesc"] {
        let result = execute_page_in_snapshot(
            member(&family, variant, 0),
            &parameters(&[]),
            None,
            &mut view(),
        );
        assert_eq!(
            result.map(|snapshot| run_ids(&snapshot)),
            Err(QueryExecutionError::BoundExceeded),
            "{variant}: a paged joined-order helper is never a complete sort-key population"
        );
    }
    // The same helper limit is harmless when the order does not join it, and
    // the non-output helper never contaminates the root response continuation.
    let snapshot = execute_page_in_snapshot(
        member(&family, "StartTimeDesc", 0),
        &parameters(&[]),
        None,
        &mut view(),
    )
    .expect("StartTimeDesc");
    assert_eq!(run_ids(&snapshot), ids(&["run-3", "run-2", "run-1"]));
    assert!(snapshot.continuation().is_none());
    assert!(snapshot.continuation_binding().is_none());
}

// req: RQL-002, RQL-003
#[test]
fn joined_order_with_complete_helper_pages_only_on_the_root_cursor() {
    let family = family(QUERY);
    for (variant, expected) in [
        ("MetricAsc", ["run-3", "run-1", "run-2"]),
        ("MetricDesc", ["run-1", "run-3", "run-2"]),
    ] {
        let program = member(&family, variant, 0);
        let whole = execute_page_in_snapshot(program, &parameters(&[]), None, &mut view())
            .expect("whole page");
        assert_eq!(run_ids(&whole), ids(&expected), "{variant}");
        assert!(whole.continuation().is_none(), "{variant}: complete page");

        let one = parameters(&[("limit", Some(CanonicalValue::U64(1)))]);
        let mut seen = Vec::new();
        let mut cursor = None;
        loop {
            let page = execute_page_in_snapshot(program, &one, cursor.as_ref(), &mut view())
                .expect("page");
            seen.extend(run_ids(&page));
            let Some(lower) = page.continuation() else {
                break;
            };
            assert_eq!(
                page.continuation_binding(),
                Some("runs"),
                "{variant}: only the root order owns the response continuation"
            );
            cursor = Some(
                QueryContinuation::checked(
                    "runs".to_owned(),
                    lower.to_vec(),
                    page.index_epochs().clone(),
                )
                .expect("cursor"),
            );
            assert!(
                seen.len() <= expected.len(),
                "{variant}: pagination terminates"
            );
        }
        assert_eq!(
            seen,
            ids(&expected),
            "{variant}: root cursor resumes in order"
        );
    }
}

// req: RQL-002, RQL-003
#[test]
fn absent_optional_guards_remove_only_their_own_candidate_predicates() {
    let family = family(QUERY);
    let asc = member(&family, "MetricAsc", 0);
    let null = || Some(CanonicalValue::Null);
    // Every c2 guard absent: c2 is the intersection identity (run-6 survives).
    let all_absent = [
        ("c2_kind", null()),
        ("c2_item_key", null()),
        ("c2_min_sort_key", null()),
        ("c2_max_sort_key", null()),
    ];
    let mut v = view();
    let snapshot =
        execute_page_in_snapshot(asc, &parameters(&all_absent), None, &mut v).expect("c2 omitted");
    assert_eq!(
        run_ids(&snapshot),
        ids(&["run-3", "run-6", "run-1", "run-5", "run-2"])
    );
    assert_eq!(
        v.scans, 3,
        "c1, c3 and metric_ranks; omitted c2 and c4 skip"
    );

    // kind present, every other c2 guard absent: `kind == "param"` stays
    // active and excludes run-6, which has no param rows.
    let kind_only = [
        ("c2_item_key", null()),
        ("c2_min_sort_key", null()),
        ("c2_max_sort_key", null()),
    ];
    let snapshot = execute_page_in_snapshot(asc, &parameters(&kind_only), None, &mut view())
        .expect("kind only");
    assert_eq!(
        run_ids(&snapshot),
        ids(&["run-3", "run-1", "run-5", "run-2"])
    );

    // kind and item_key present, ranges absent: any optimizer value matches.
    let no_ranges = [("c2_min_sort_key", null()), ("c2_max_sort_key", null())];
    let snapshot = execute_page_in_snapshot(asc, &parameters(&no_ranges), None, &mut view())
        .expect("no ranges");
    assert_eq!(
        run_ids(&snapshot),
        ids(&["run-3", "run-1", "run-5", "run-2"])
    );

    // Only the lower bound absent: the upper bound still excludes run-5 (99).
    let upper_only = [("c2_min_sort_key", null())];
    let snapshot = execute_page_in_snapshot(asc, &parameters(&upper_only), None, &mut view())
        .expect("upper only");
    assert_eq!(run_ids(&snapshot), ids(&["run-3", "run-1", "run-2"]));

    // An absent guard ahead of an active one on the index leaves no exact
    // index prefix. Refuse it, naming the absent guard, rather than widen.
    for (absent, expected) in [
        (vec![("c2_kind", null())], "c2_kind"),
        (
            vec![("c2_kind", null()), ("c2_min_sort_key", null())],
            "c2_kind",
        ),
        (vec![("c2_item_key", null())], "c2_item_key"),
        (
            vec![("c2_item_key", null()), ("c2_min_sort_key", null())],
            "c2_item_key",
        ),
    ] {
        let result = execute_page_in_snapshot(asc, &parameters(&absent), None, &mut view());
        assert_eq!(
            result.map(|snapshot| run_ids(&snapshot)),
            Err(QueryExecutionError::InvalidParameter {
                parameter: expected.to_owned()
            }),
            "{absent:?}"
        );
    }
}

// req: RQL-002, RQL-003
#[test]
fn absent_optional_guards_retain_unconditional_source_predicates() {
    // c4 = `kind == "param"` plus absent optional guards. The unconditional
    // predicate stays active, so run-6 (no param rows) is excluded even though
    // c2 is omitted entirely.
    let source = replace_once(QUERY, C4_KIND_GUARD, "&& kind == \"param\"");
    let source = replace_once(&source, "    $c4_kind: RunDatum.kind?,\n", "");
    let family = family(&source);
    let params = parameters(&[
        ("c4_kind", None),
        ("c2_kind", Some(CanonicalValue::Null)),
        ("c2_item_key", Some(CanonicalValue::Null)),
        ("c2_min_sort_key", Some(CanonicalValue::Null)),
        ("c2_max_sort_key", Some(CanonicalValue::Null)),
    ]);
    let mut v = view();
    let snapshot = execute_page_in_snapshot(member(&family, "MetricAsc", 0), &params, None, &mut v)
        .expect("MetricAsc");
    assert_eq!(
        run_ids(&snapshot),
        ids(&["run-3", "run-1", "run-5", "run-2"])
    );
    assert_eq!(v.scans, 4, "c1, c3, c4 and metric_ranks");
}

// req: RQL-002, RQL-003
#[test]
fn compound_presence_guard_on_root_hydration_drops_every_conjunct_when_absent() {
    let source = replace_once(
        QUERY,
        ROOT_MIN_START_GUARD,
        "&& when $min_start_time { start_time >= $min_start_time && status == \"FINISHED\" }",
    );
    let family = family(&source);
    let mut v = view();
    v.runs
        .insert("run-2".to_owned(), run("run-2", "FAILED", 2000));
    let absent = parameters(&[("filter_status", Some(CanonicalValue::Null))]);
    let snapshot = execute_page_in_snapshot(member(&family, "MetricAsc", 0), &absent, None, &mut v)
        .expect("guard absent");
    assert_eq!(
        run_ids(&snapshot),
        ids(&["run-3", "run-1", "run-2"]),
        "the whole guarded predicate is absent, so FAILED run-2 stays eligible"
    );
    assert_eq!(
        family.presence_parameters(),
        ["min_start_time".to_owned()],
        "a compound guard cannot be disabled predicate-by-predicate at runtime"
    );
    let present = parameters(&[
        ("filter_status", Some(CanonicalValue::Null)),
        ("min_start_time", Some(CanonicalValue::I64(0))),
    ]);
    let mut v = view();
    v.runs
        .insert("run-2".to_owned(), run("run-2", "FAILED", 2000));
    let snapshot =
        execute_page_in_snapshot(member(&family, "MetricAsc", 1), &present, None, &mut v)
            .expect("guard present");
    assert_eq!(run_ids(&snapshot), ids(&["run-3", "run-1"]));
}

// req: RQL-002, RQL-003
#[test]
fn compound_presence_guard_on_candidate_source_drops_every_conjunct_when_absent() {
    // c4 = `when $c4_kind { kind == $c4_kind && item_key == "optimizer" }`.
    let source = replace_once(
        QUERY,
        C4_KIND_GUARD,
        "&& when $c4_kind { kind == $c4_kind && item_key == \"optimizer\" }",
    );
    let source = replace_once(
        &source,
        "              && when $c4_item_key { item_key == $c4_item_key }\n              && when $c4_min_sort_key { sort_key >= $c4_min_sort_key }\n              && when $c4_max_sort_key { sort_key <= $c4_max_sort_key },\n",
        ",\n",
    );
    let source = replace_once(
        &source,
        "    $c4_item_key: RunDatum.item_key?,\n    $c4_min_sort_key: RunDatum.sort_key?,\n    $c4_max_sort_key: RunDatum.sort_key?,\n",
        "",
    );
    let family = family(&source);
    assert_eq!(family.presence_parameters(), ["c4_kind".to_owned()]);
    let base = [
        ("c4_item_key", None),
        ("c4_min_sort_key", None),
        ("c4_max_sort_key", None),
        ("c2_kind", Some(CanonicalValue::Null)),
        ("c2_item_key", Some(CanonicalValue::Null)),
        ("c2_min_sort_key", Some(CanonicalValue::Null)),
        ("c2_max_sort_key", Some(CanonicalValue::Null)),
    ];
    let snapshot = execute_page_in_snapshot(
        member(&family, "MetricAsc", 0),
        &parameters(&base),
        None,
        &mut view(),
    )
    .expect("guard absent");
    assert_eq!(
        run_ids(&snapshot),
        ids(&["run-3", "run-6", "run-1", "run-5", "run-2"]),
        "absent c4 guard removes the literal conjunct too"
    );
    let mut present = base.to_vec();
    present.push(("c4_kind", Some(text("param"))));
    let snapshot = execute_page_in_snapshot(
        member(&family, "MetricAsc", 1),
        &parameters(&present),
        None,
        &mut view(),
    )
    .expect("guard present");
    assert_eq!(
        run_ids(&snapshot),
        ids(&["run-3", "run-1", "run-5", "run-2"])
    );
}

const WIDE_SPAN_CONTRACT: &str = r#"
contract MlflowTraces version 1 {
  entity TraceSpan {
    key (trace_id: string<64>, span_id: string<64>)
    field start_time_ns: i64
    field packed_span_json: string<1048576>
    index by_start (trace_id, start_time_ns, span_id)
  }
  aggregate TraceSpanUnit {
    root TraceSpan
    partition_by trace_id
    conflict_key (trace_id, span_id)
  }
}
"#;

// req: QRY-001, QRY-002, QRY-007
#[test]
fn runtime_byte_relief_reserves_sibling_result_bytes_and_paginates_without_fuel_exhaustion() {
    let bundle = compile_contract_source(WIDE_SPAN_CONTRACT).expect("contract");
    let catalog = SymbolicCatalog::from_bundle(&bundle).expect("catalog");
    let query = |extra_binding: &str, extra_result: &str| {
        format!(
            r#"
query ListTraceSpans(
    $trace_id: TraceSpan.trace_id,
    $limit: Limit<100>,
    $after: Cursor?
) {{
    {extra_binding}
    many spans from TraceSpan
        where trace_id == $trace_id
        order by start_time_ns asc, span_id asc
        take $limit after $after
    return Found {{
        spans: spans {{ trace_id span_id start_time_ns packed_span_json }}
        {extra_result}
    }}
    outcomes Found | Missing
}}
"#
        )
    };
    let with_sibling = query(
        r#"one extra from TraceSpan
        where trace_id == $trace_id && span_id == "sp-01"
        else Missing"#,
        "extra: extra { packed_span_json }",
    );
    let program = compile_query(&parse_query(&with_sibling).expect("parse"), &catalog)
        .expect("a paginated wide Many beside a bounded sibling reserves sibling bytes");

    let big_payload = "x".repeat(950_000);
    let make_span = |idx: u32, payload: &str| {
        row(
            "TraceSpan",
            &[
                ("trace_id", text("tr-1")),
                ("span_id", text(&format!("sp-{idx:02}"))),
                ("start_time_ns", CanonicalValue::I64(i64::from(idx) * 10)),
                ("packed_span_json", text(payload)),
            ],
        )
    };
    let all_rows = (1..=6)
        .map(|idx| make_span(idx, &big_payload))
        .collect::<Vec<_>>();

    struct SiblingByteBudgetView {
        rows: Vec<QueryRow>,
    }

    impl QueryReadView for SiblingByteBudgetView {
        type Error = ();
        fn fault(&self, _error: &Self::Error) -> QueryBackendFault {
            QueryBackendFault::Unavailable
        }
        fn application_head(&self) -> u64 {
            11
        }
        fn point(
            &mut self,
            _step: &QueryAccessStep,
            _predicates: &[BoundPredicate],
            _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
        ) -> Result<Option<QueryRow>, Self::Error> {
            Ok(self.rows.first().cloned())
        }
        fn dependent_point_batch(
            &mut self,
            _step: &QueryAccessStep,
            _predicates: &[Vec<BoundPredicate>],
            _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
        ) -> Result<Vec<Option<QueryRow>>, Self::Error> {
            Ok(Vec::new())
        }
        fn scan(
            &mut self,
            step: &QueryAccessStep,
            _predicates: &[BoundPredicate],
            limit: u64,
            after: Option<&[u8]>,
            _after_inclusive: bool,
            _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
        ) -> Result<QueryScanPage, Self::Error> {
            let start_idx = after.map_or(0, |cursor| usize::from(cursor[0]));
            let page_limit = usize::try_from(limit).expect("limit");
            let byte_budget = crate::step_page_byte_budget_v1(step);
            let mut effective_limit = page_limit;
            let mut page_bytes = 0usize;
            let mut page_rows = Vec::new();
            let mut last_idx = start_idx;
            for (idx, r) in self.rows.iter().enumerate().skip(start_idx) {
                if let Some(budget) = byte_budget {
                    let rb = crate::estimated_query_row_result_bytes_v1(step, r);
                    if !page_rows.is_empty()
                        && page_rows.len() < effective_limit
                        && page_bytes.saturating_add(rb) > budget
                    {
                        effective_limit = page_rows.len();
                    }
                    page_bytes = page_bytes.saturating_add(rb);
                }
                page_rows.push(r.clone());
                if page_rows.len() <= effective_limit {
                    last_idx = idx + 1;
                }
                if page_rows.len() > effective_limit {
                    break;
                }
            }
            let has_more = page_rows.len() > effective_limit;
            if has_more {
                page_rows.truncate(effective_limit);
            }
            let scanned = (page_rows.len() + usize::from(has_more)) as u64;
            match has_more {
                true => Ok(QueryScanPage::continued(
                    page_rows,
                    3,
                    scanned,
                    vec![u8::try_from(last_idx).expect("idx")],
                )
                .expect("page")),
                false => Ok(QueryScanPage::exact_end(page_rows, 3)),
            }
        }
        fn nearest(
            &mut self,
            _step: &QueryAccessStep,
            _predicates: &[BoundPredicate],
            _k: u32,
            _policy: Option<&AuthorizedQueryRowPolicyContextV1>,
        ) -> Result<QueryNearestPage, Self::Error> {
            Err(())
        }
    }

    let params = QueryParameters::checked(BTreeMap::from([
        ("trace_id".to_owned(), text("tr-1")),
        ("limit".to_owned(), CanonicalValue::U64(100)),
    ]))
    .expect("params");
    let mut view = SiblingByteBudgetView { rows: all_rows };
    let page1 = execute_page_in_snapshot(&program, &params, None, &mut view)
        .expect("reserving extra's worst-case bytes prevents FuelExhausted");
    let Some(QueryResultValue::Many(rows1)) = page1.fields().get("spans") else {
        panic!("spans");
    };
    assert_eq!(
        rows1.len(),
        3,
        "reserving ~1 MiB for `extra` leaves ~3.14 MiB for `spans`, fitting 3 x 950 KB rows"
    );
    assert!(page1.continuation().is_some());

    let cursor = crate::QueryContinuation::checked(
        page1.continuation_binding().expect("binding").to_owned(),
        page1.continuation().expect("continuation").to_vec(),
        page1.index_epochs().clone(),
    )
    .expect("cursor");
    let page2 = execute_page_in_snapshot(&program, &params, Some(&cursor), &mut view)
        .expect("page 2 completes remaining 3 rows");
    let Some(QueryResultValue::Many(rows2)) = page2.fields().get("spans") else {
        panic!("spans");
    };
    assert_eq!(rows2.len(), 3);
    assert!(page2.continuation().is_none());

    // When the unpaged sibling itself plus 1 row of `spans` exceeds 4 MiB, compilation fails.
    let oversized_sibling = query(
        r#"many extras from TraceSpan
        where trace_id == $trace_id
        order by start_time_ns asc, span_id asc
        take 4"#,
        "extras: extras { packed_span_json }",
    );
    let refused = compile_query(&parse_query(&oversized_sibling).expect("parse"), &catalog)
        .expect_err("4 x 1 MiB sibling plus 1 x 1 MiB paginated row exceeds 4 MiB");
    assert!(
        refused.as_slice().iter().any(|diagnostic| {
            diagnostic.bound().map(|bound| bound.resource())
                == Some(riffdb_query_compiler::PlannerBoundResource::EncodedResultBytes)
        }),
        "{refused:?}"
    );
}
