//! JSON report assembly.

use serde_json::{Value, json};

use crate::{
    SEED_GENERATION, SampleSummary, Scale, ScenarioId, ScenarioResult, board_marginal_from_results,
    board_packed_marginal_from_results, board_projected_marginal_from_results,
};

/// RiffDB board queries use static-compiled `take N` (BoardPage50/200/450).
///
/// take-500 (static or runtime Limit) trips `MAX_QUERY_SCANNED_ROWS=500` because
/// the executor probes one extra row for continuation (scan 501 → RDB-INTERNAL-0001;
/// incidents include 019fbf5b-1a64-7877-94c3-47d7a0763539). Largest board page is 450.
pub const BOARD_PAGE_RIFFQ_50: &str =
    include_str!("../../../../queries/ticketdesk/board_page_50.riffq");
/// Static 200-row board page.
pub const BOARD_PAGE_RIFFQ_200: &str =
    include_str!("../../../../queries/ticketdesk/board_page_200.riffq");
/// Static 450-row board page (within scan ceiling + continuation probe).
pub const BOARD_PAGE_RIFFQ_450: &str =
    include_str!("../../../../queries/ticketdesk/board_page_450.riffq");

/// Incident id for the take-500 / scan-ceiling execute failure (engine out of scope).
pub const BOARD_LIMIT_ENGINE_INCIDENT: &str = "019fbf5b-1a64-7877-94c3-47d7a0763539";

/// Executor bound that forces board pages ≤ 450 (`take N` + one continuation probe).
pub const MAX_QUERY_SCANNED_ROWS: u32 = 500;

/// Canonical PostgreSQL SQL executed for board pages (and cited in the report).
///
/// Six result columns only — `organization_id` is taken from the bind parameter
/// `$1`, matching the RiffDB adapter (which never re-encodes org from the row).
/// PG keeps a parameterized `LIMIT $4` (works). RiffDB uses static-compiled
/// BoardPage50/200/450 (take 500 would exceed the scan ceiling). The Ticket
/// entity has eight fields; `created_at` / `updated_at` are omitted on both
/// sides by design for this result-size curve.
pub const BOARD_PAGE_SQL: &str = "SELECT ticket_id::text, project_id::text,\n\
            reporter_id::text, assignee_id::text, status, title\n\
     FROM ticket\n\
     WHERE organization_id = $1::text::uuid\n\
       AND project_id = $2::text::uuid\n\
       AND status = $3\n\
     ORDER BY ticket_id ASC\n\
     LIMIT $4";

/// One backend's complete measured layer.
#[derive(Clone, Debug)]
pub struct BackendReport {
    /// Stable backend id.
    pub backend_id: &'static str,
    /// Human description / access path.
    pub description: String,
    /// Guarantee notes.
    pub guarantee_notes: Vec<String>,
    /// Seed wall-clock nanoseconds.
    pub seed_ns: u64,
    /// Approximate rows loaded.
    pub seed_rows: u64,
    /// Scenario results.
    pub scenarios: Vec<ScenarioResult>,
    /// Exact successful RiffDB completion commits by group size 1 through 256.
    pub write_completion_groups: Option<Vec<u64>>,
}

/// Builds the top-level diagnostics report.
#[must_use]
pub fn build_report(
    scale: Scale,
    warmups: usize,
    samples: usize,
    backends: &[BackendReport],
) -> Value {
    let backend_values: Vec<Value> = backends.iter().map(backend_json).collect();
    let comparisons = build_comparisons(backends);
    json!({
        "schema": "riffdb.app-baseline/v1",
        "report_id": "ticketdesk-app-baseline",
        "domain": "TicketDesk",
        "tables": [
            "organization",
            "app_user",
            "project",
            "project_member",
            "ticket",
            "comment",
            "label",
            "ticket_label"
        ],
        "scale": {
            "profile": scale.name(),
            "organizations": scale.organizations,
            "users_per_org": scale.users_per_org,
            "projects_per_org": scale.projects_per_org,
            "members_per_project": scale.members_per_project,
            "tickets_per_project": scale.tickets_per_project,
            "comments_per_ticket": scale.comments_per_ticket,
            "labels_per_org": scale.labels_per_org,
            "labels_per_ticket": scale.labels_per_ticket,
            "board_dense_open": scale.board_dense_open,
            "approximate_row_count": scale.approximate_row_count(),
        },
        "configuration": {
            "warmup_iterations": warmups,
            "measured_samples": samples,
            "timing_source": "std::time::Instant",
            "distribution_method": "nearest-rank p50/p95/p99",
            "list_limit": 50,
            "board_page_sizes": [50, 200, 450],
            "board_scenarios_in_smoke": "skipped (board_dense_open=0)",
            "seed_generation": SEED_GENERATION,
            "baseline_note": "seed_generation 2 supersedes pre-B1 full baselines (ticket count and probe keys changed)",
            "board_limit_mode": "static_compiled",
            "board_limit_engine_incident": BOARD_LIMIT_ENGINE_INCIDENT,
            "board_limit_note": "RiffDB BoardPage50/200/450 use static take N ≤ 450 so take+continuation-probe stays within MAX_QUERY_SCANNED_ROWS=500. take 500 fails (RDB-INTERNAL-0001). PostgreSQL keeps LIMIT $4.",
            "board_marginal_formula": "(p50_450 − p50_50) / 400",
            "board_projected_scenarios": [
                "board_page_projected_50",
                "board_page_projected_200",
                "board_page_projected_450"
            ],
            "board_projected_marginal_formula": "(p50_projected_450 − p50_projected_50) / 400",
            "board_projected_note": "RiffDB-only ExecuteProjectedQuery over config-registered board projection; PG has no projected path (compiled-vs-PG fields unchanged).",
            "board_packed_scenarios": [
                "board_page_packed_50",
                "board_page_packed_200",
                "board_page_packed_450"
            ],
            "board_packed_marginal_formula": "(p50_packed_450 − p50_packed_50) / 400",
            "board_packed_note": "RiffDB-only ExecuteProjectedQuery with response_encoding=PACKED (column-major canonical cells); additive to projected-row scenarios.",
        },
        "board_page_query": {
            "riffql_sources": [
                "queries/ticketdesk/board_page_50.riffq",
                "queries/ticketdesk/board_page_200.riffq",
                "queries/ticketdesk/board_page_450.riffq"
            ],
            "riffql": {
                "50": BOARD_PAGE_RIFFQ_50,
                "200": BOARD_PAGE_RIFFQ_200,
                "450": BOARD_PAGE_RIFFQ_450,
            },
            "sql": BOARD_PAGE_SQL,
            "max_query_scanned_rows": MAX_QUERY_SCANNED_ROWS,
            "max_query_scanned_rows_note": "Engine page contract: take N + one continuation probe must be ≤ 500. Larger boards paginate via cursors (future scenario).",
            "limit_asymmetry": "RiffDB static-compiled take 50/200/450 (take 500 trips scan ceiling); PG parameterized LIMIT $4 (works)",
            "engine_incident": BOARD_LIMIT_ENGINE_INCIDENT,
            "predicate": "organization_id + project_id + status",
            "order_by": "ticket_id ASC",
            "field_set": "6-field wide row (ticket_id, project_id, title, status, reporter_id, assignee_id); Ticket entity has 8 fields — created_at/updated_at omitted on both backends by design",
            "columns": [
                "ticket_id",
                "project_id",
                "title",
                "status",
                "reporter_id",
                "assignee_id"
            ],
            "organization_id": "bound as $1 / query parameter; not re-selected or re-encoded from the row",
        },
        "backends": backend_values,
        "comparisons": comparisons,
        "limitations": [
            "Application-equivalent baseline, not SQL plan equivalence.",
            "PostgreSQL uses SQL joins/filters; RiffDB uses symbolic commands and named RiffQL queries over public gRPC.",
            "Not the frozen WP-200 budget-comparison publication suite.",
            "Results are architectural feedback for optimization prioritization, not marketing claims.",
            "Write scenarios execute one new durable write per measured sample on both backends (no idempotent replays).",
            "PostgreSQL runs behind a Docker userland port proxy; RiffDB listens directly on loopback.",
            "Board scenarios skip under --smoke (board_dense_open=0); full profile densifies org-0/project-0 open tickets.",
            "seed_generation 2 (board-density layout) supersedes pre-B1 full baselines; do not compare ticket counts or probe keys across generations.",
            "RiffDB board pages use static-compiled take 50/200/450 (BoardPage50/200/450). take 500 + continuation probe exceeds MAX_QUERY_SCANNED_ROWS=500 (RDB-INTERNAL-0001; incident 019fbf5b-1a64-7877-94c3-47d7a0763539). Larger boards need cursor pagination. PostgreSQL uses parameterized LIMIT $4.",
            "RiffDB board_page_projected_* uses ExecuteProjectedQuery (generated tonic wire path) after Causal catch-up to the seed head and a run-aborting three-way compiled-vs-projected-row-vs-projected-packed row-content equivalence gate.",
            "RiffDB board_page_packed_* uses the same projection with response_encoding=PACKED (column-major canonical cell buffers); measured only when the dense cell fits the page size.",
        ],
    })
}

pub(crate) fn backend_json(backend: &BackendReport) -> Value {
    let scenarios: Vec<Value> = backend
        .scenarios
        .iter()
        .map(|scenario| {
            let summary = scenario.samples.summary();
            json!({
                "scenario": scenario.scenario.as_str(),
                "last_row_count": scenario.last_row_count,
                "timing": summary_json(&summary),
            })
        })
        .collect();
    let mut value = json!({
        "backend_id": backend.backend_id,
        "description": backend.description,
        "guarantee_notes": backend.guarantee_notes,
        "seed_ns": backend.seed_ns,
        "seed_rows": backend.seed_rows,
        "write_completion_groups_by_size": backend.write_completion_groups,
        "scenarios": scenarios,
    });
    if let Some(marginal) = board_marginal_from_results(&backend.scenarios) {
        // Scalar placeholder; attach_rep_summaries overwrites with a rep-stability
        // summary when multiple reps are present.
        value["board_marginal_ns_per_row"] = json!(marginal);
    }
    if let Some(marginal) = board_projected_marginal_from_results(&backend.scenarios) {
        value["board_projected_marginal_ns_per_row"] = json!(marginal);
    }
    if let Some(marginal) = board_packed_marginal_from_results(&backend.scenarios) {
        value["board_packed_marginal_ns_per_row"] = json!(marginal);
    }
    // Per-N projected p50s (additive; absent when projected scenarios were skipped).
    let mut projected_p50s = serde_json::Map::new();
    for scenario in [
        ScenarioId::BoardPageProjected50,
        ScenarioId::BoardPageProjected200,
        ScenarioId::BoardPageProjected450,
    ] {
        if let Some(row) = backend
            .scenarios
            .iter()
            .find(|candidate| candidate.scenario == scenario)
        {
            projected_p50s.insert(
                scenario.as_str().to_owned(),
                json!(row.samples.summary().p50_ns),
            );
        }
    }
    if !projected_p50s.is_empty() {
        value["board_projected_p50_ns"] = Value::Object(projected_p50s);
    }
    let mut packed_p50s = serde_json::Map::new();
    for scenario in [
        ScenarioId::BoardPagePacked50,
        ScenarioId::BoardPagePacked200,
        ScenarioId::BoardPagePacked450,
    ] {
        if let Some(row) = backend
            .scenarios
            .iter()
            .find(|candidate| candidate.scenario == scenario)
        {
            packed_p50s.insert(
                scenario.as_str().to_owned(),
                json!(row.samples.summary().p50_ns),
            );
        }
    }
    if !packed_p50s.is_empty() {
        value["board_packed_p50_ns"] = Value::Object(packed_p50s);
    }
    value
}

/// Asserts board scenario last_row_counts match across backends.
///
/// Called after both backends have measured so a silent cardinality skew cannot
/// masquerade as a fair p50 ratio.
pub fn assert_board_last_row_counts_equal(backends: &[BackendReport]) -> Result<(), String> {
    let Some(postgres) = backends.iter().find(|b| b.backend_id == "postgres_sql") else {
        return Ok(());
    };
    let Some(riffdb) = backends
        .iter()
        .find(|b| b.backend_id == "riffdb_public_grpc")
    else {
        return Ok(());
    };
    for scenario in ScenarioId::all() {
        if scenario.board_page_limit().is_none() {
            continue;
        }
        let Some(pg) = postgres
            .scenarios
            .iter()
            .find(|row| row.scenario == scenario)
        else {
            continue;
        };
        let Some(rd) = riffdb.scenarios.iter().find(|row| row.scenario == scenario) else {
            continue;
        };
        if pg.last_row_count != rd.last_row_count {
            return Err(format!(
                "measurement-integrity: board scenario {} last_row_count mismatch postgres={} riffdb={}",
                scenario.as_str(),
                pg.last_row_count,
                rd.last_row_count
            ));
        }
        if let Some(limit) = scenario.board_page_limit()
            && pg.last_row_count != limit as usize
        {
            return Err(format!(
                "measurement-integrity: board scenario {} expected {} rows, got {}",
                scenario.as_str(),
                limit,
                pg.last_row_count
            ));
        }
    }
    Ok(())
}

/// Order-sensitive board page ticket_id sequence comparison (live cross-check).
pub fn assert_board_ticket_sequences_equal(
    postgres_ids: &[[u8; 16]],
    riffdb_ids: &[[u8; 16]],
    limit: u32,
) -> Result<(), String> {
    if postgres_ids.len() != limit as usize {
        return Err(format!(
            "measurement-integrity: postgres board_page({limit}) returned {} rows",
            postgres_ids.len()
        ));
    }
    if riffdb_ids.len() != limit as usize {
        return Err(format!(
            "measurement-integrity: riffdb board_page({limit}) returned {} rows",
            riffdb_ids.len()
        ));
    }
    if postgres_ids != riffdb_ids {
        // Locate first divergence for a useful abort message without dumping
        // full UUID lists into stderr.
        let first = postgres_ids
            .iter()
            .zip(riffdb_ids.iter())
            .position(|(pg, rd)| pg != rd)
            .unwrap_or(0);
        return Err(format!(
            "measurement-integrity: board_page({limit}) ticket_id sequence mismatch \
             at index {first} (order-sensitive PG vs RiffDB cross-check)"
        ));
    }
    Ok(())
}

fn summary_json(summary: &SampleSummary) -> Value {
    json!({
        "sample_count": summary.sample_count,
        "samples_ns": summary.samples_ns,
        "min_ns": summary.min_ns,
        "p50_ns": summary.p50_ns,
        "p95_ns": summary.p95_ns,
        "p99_ns": summary.p99_ns,
        "max_ns": summary.max_ns,
        "mean_ns": summary.mean_ns,
    })
}

fn build_comparisons(backends: &[BackendReport]) -> Value {
    let postgres = backends
        .iter()
        .find(|backend| backend.backend_id == "postgres_sql");
    let riffdb = backends
        .iter()
        .find(|backend| backend.backend_id == "riffdb_public_grpc");
    let (Some(postgres), Some(riffdb)) = (postgres, riffdb) else {
        return json!({
            "available": false,
            "reason": "both postgres_sql and riffdb_public_grpc backends required for ratios",
        });
    };

    let mut scenarios = Vec::new();
    for scenario in ScenarioId::all() {
        let Some(pg) = postgres
            .scenarios
            .iter()
            .find(|row| row.scenario == scenario)
        else {
            continue;
        };
        let Some(rd) = riffdb.scenarios.iter().find(|row| row.scenario == scenario) else {
            continue;
        };
        let pg_p50 = pg.samples.summary().p50_ns;
        let rd_p50 = rd.samples.summary().p50_ns;
        let mut row = json!({
            "scenario": scenario.as_str(),
            "postgres_p50_ns": pg_p50,
            "riffdb_p50_ns": rd_p50,
            "postgres_last_row_count": pg.last_row_count,
            "riffdb_last_row_count": rd.last_row_count,
            "last_row_count_equal": pg.last_row_count == rd.last_row_count,
            "ratio_riffdb_over_postgres": if pg_p50 == 0 {
                0.0
            } else {
                rd_p50 as f64 / pg_p50 as f64
            },
        });
        if scenario.board_page_limit().is_some() && pg.last_row_count != rd.last_row_count {
            // Surface the integrity failure in the report payload even when the
            // hard abort in main is the primary guard.
            row["measurement_integrity"] = json!("last_row_count_mismatch");
        }
        scenarios.push(row);
    }
    scenarios.sort_by(|left, right| {
        right["ratio_riffdb_over_postgres"]
            .as_f64()
            .partial_cmp(&left["ratio_riffdb_over_postgres"].as_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let pg_seed = postgres.seed_ns;
    let rd_seed = riffdb.seed_ns;
    let mut comparisons = json!({
        "available": true,
        "seed": {
            "postgres_ns": pg_seed,
            "riffdb_ns": rd_seed,
            "ratio_riffdb_over_postgres": if pg_seed == 0 {
                0.0
            } else {
                rd_seed as f64 / pg_seed as f64
            },
        },
        "scenarios": scenarios,
    });
    if let (Some(pg_m), Some(rd_m)) = (
        board_marginal_from_results(&postgres.scenarios),
        board_marginal_from_results(&riffdb.scenarios),
    ) {
        // Single-rep scalar; multi-rep stability is attached later.
        comparisons["board_marginal_ns_per_row"] = json!({
            "postgres": pg_m,
            "riffdb": rd_m,
        });
    }
    // Projected marginal is RiffDB-only; PG compiled number remains the 777 ns/row baseline.
    if let Some(rd_projected) = board_projected_marginal_from_results(&riffdb.scenarios) {
        let mut projected = json!({
            "riffdb": rd_projected,
        });
        if let Some(rd_compiled) = board_marginal_from_results(&riffdb.scenarios) {
            projected["riffdb_compiled"] = json!(rd_compiled);
        }
        if let Some(pg_m) = board_marginal_from_results(&postgres.scenarios) {
            projected["postgres_compiled"] = json!(pg_m);
        }
        comparisons["board_projected_marginal_ns_per_row"] = projected;
    }
    // Packed marginal is RiffDB-only (additive).
    if let Some(rd_packed) = board_packed_marginal_from_results(&riffdb.scenarios) {
        let mut packed = json!({
            "riffdb": rd_packed,
        });
        if let Some(rd_projected) = board_projected_marginal_from_results(&riffdb.scenarios) {
            packed["riffdb_projected_row"] = json!(rd_projected);
        }
        if let Some(rd_compiled) = board_marginal_from_results(&riffdb.scenarios) {
            packed["riffdb_compiled"] = json!(rd_compiled);
        }
        if let Some(pg_m) = board_marginal_from_results(&postgres.scenarios) {
            packed["postgres_compiled"] = json!(pg_m);
        }
        comparisons["board_packed_marginal_ns_per_row"] = packed;
    }
    comparisons
}

#[cfg(test)]
mod tests {
    use super::{BOARD_PAGE_SQL, assert_board_ticket_sequences_equal, backend_json, build_report};
    use crate::{
        BackendReport, SampleSet, Scale, ScenarioId, ScenarioResult, board_marginal_ns_per_row,
        board_projected_marginal_from_results,
    };

    #[test]
    fn board_page_sql_omits_organization_id_column() {
        assert!(
            !BOARD_PAGE_SQL.contains("organization_id::text"),
            "board SQL must not pay to encode organization_id; fill from $1"
        );
        assert!(BOARD_PAGE_SQL.contains("ticket_id::text"));
        assert!(BOARD_PAGE_SQL.contains("ORDER BY ticket_id ASC"));
        assert!(BOARD_PAGE_SQL.contains("LIMIT $4"));
    }

    #[test]
    fn board_ticket_sequence_compare_detects_mismatch() {
        let a = [[1_u8; 16], [2_u8; 16]];
        let b = [[1_u8; 16], [3_u8; 16]];
        assert!(assert_board_ticket_sequences_equal(&a, &a, 2).is_ok());
        let err = assert_board_ticket_sequences_equal(&a, &b, 2).expect_err("mismatch");
        assert!(err.contains("measurement-integrity"));
        assert!(err.contains("index 1"));
    }

    /// Falsifiability (c): report shape requires (p50_450 − p50_50) / 400, not swapped.
    #[test]
    fn report_board_projected_marginal_uses_450_minus_50_over_400() {
        let mut s50 = SampleSet::default();
        let mut s450 = SampleSet::default();
        for _ in 0..3 {
            s50.record(std::time::Duration::from_nanos(5_000));
            s450.record(std::time::Duration::from_nanos(45_000));
        }
        let results = vec![
            ScenarioResult {
                scenario: ScenarioId::BoardPageProjected50,
                samples: s50.clone(),
                last_row_count: 50,
            },
            ScenarioResult {
                scenario: ScenarioId::BoardPageProjected450,
                samples: s450.clone(),
                last_row_count: 450,
            },
        ];
        assert_eq!(board_projected_marginal_from_results(&results), Some(100));
        // Swapped scenario wiring would compute None or the wrong formula.
        let swapped = vec![
            ScenarioResult {
                scenario: ScenarioId::BoardPageProjected50,
                samples: s450,
                last_row_count: 50,
            },
            ScenarioResult {
                scenario: ScenarioId::BoardPageProjected450,
                samples: s50,
                last_row_count: 450,
            },
        ];
        assert_eq!(board_projected_marginal_from_results(&swapped), None);
        assert_eq!(board_marginal_ns_per_row(45_000, 5_000), None);

        let backend = BackendReport {
            backend_id: "riffdb_public_grpc",
            description: "test".to_owned(),
            guarantee_notes: Vec::new(),
            seed_ns: 1,
            seed_rows: 1,
            scenarios: results,
            write_completion_groups: None,
        };
        let value = backend_json(&backend);
        assert_eq!(value["board_projected_marginal_ns_per_row"], 100);
        assert!(value["board_projected_p50_ns"]["board_page_projected_50"].is_number());
        assert!(value["board_projected_p50_ns"]["board_page_projected_450"].is_number());

        // Existing fields remain present when compiled board is also measured.
        let report = build_report(Scale::smoke(), 0, 1, &[]);
        assert_eq!(report["schema"], "riffdb.app-baseline/v1");
        assert!(
            report["configuration"]["board_marginal_formula"]
                .as_str()
                .unwrap()
                .contains("400")
        );
    }
}
