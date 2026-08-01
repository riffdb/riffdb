//! JSON report assembly.

use serde_json::{Value, json};

use crate::{
    SEED_GENERATION, SampleSummary, Scale, ScenarioId, ScenarioResult, board_marginal_from_results,
};

/// RiffDB board queries use static-compiled `take N` (BoardPage50/200/500).
///
/// Runtime `take $limit` compiles but fails at execute with RDB-INTERNAL-0001
/// (incident 019fbf5b-1a64-7877-94c3-47d7a0763539); the harness does not use it.
pub const BOARD_PAGE_RIFFQ_50: &str =
    include_str!("../../../../queries/ticketdesk/board_page_50.riffq");
/// Static 200-row board page.
pub const BOARD_PAGE_RIFFQ_200: &str =
    include_str!("../../../../queries/ticketdesk/board_page_200.riffq");
/// Static 500-row board page.
pub const BOARD_PAGE_RIFFQ_500: &str =
    include_str!("../../../../queries/ticketdesk/board_page_500.riffq");

/// Incident id for the parameterized-take execute failure (engine out of scope).
pub const BOARD_LIMIT_ENGINE_INCIDENT: &str = "019fbf5b-1a64-7877-94c3-47d7a0763539";

/// Canonical PostgreSQL SQL executed for board pages (and cited in the report).
///
/// Six result columns only — `organization_id` is taken from the bind parameter
/// `$1`, matching the RiffDB adapter (which never re-encodes org from the row).
/// PG keeps a parameterized `LIMIT $4` (works). RiffDB uses static-compiled
/// BoardPage50/200/500 by design constraint pending the engine fix above.
/// The Ticket entity has eight fields; `created_at` / `updated_at` are omitted
/// on both sides by design for this result-size curve.
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
    /// Exact successful RiffDB completion commits by group size 1 through 64.
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
            "board_page_sizes": [50, 200, 500],
            "board_scenarios_in_smoke": "skipped (board_dense_open=0)",
            "seed_generation": SEED_GENERATION,
            "baseline_note": "seed_generation 2 supersedes pre-B1 full baselines (ticket count and probe keys changed)",
            "board_limit_mode": "static_compiled",
            "board_limit_engine_incident": BOARD_LIMIT_ENGINE_INCIDENT,
            "board_limit_note": "RiffDB BoardPage50/200/500 use static take N; runtime take $limit fails at execute (RDB-INTERNAL-0001). PostgreSQL keeps LIMIT $4.",
        },
        "board_page_query": {
            "riffql_sources": [
                "queries/ticketdesk/board_page_50.riffq",
                "queries/ticketdesk/board_page_200.riffq",
                "queries/ticketdesk/board_page_500.riffq"
            ],
            "riffql": {
                "50": BOARD_PAGE_RIFFQ_50,
                "200": BOARD_PAGE_RIFFQ_200,
                "500": BOARD_PAGE_RIFFQ_500,
            },
            "sql": BOARD_PAGE_SQL,
            "limit_asymmetry": "RiffDB static-compiled take 50/200/500 (engine parameterized-take broken); PG parameterized LIMIT $4 (works)",
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
            "RiffDB board pages use static-compiled take 50/200/500 (BoardPage50/200/500); runtime take $limit fails at execute (RDB-INTERNAL-0001 incident 019fbf5b-1a64-7877-94c3-47d7a0763539). PostgreSQL uses parameterized LIMIT $4.",
        ],
    })
}

fn backend_json(backend: &BackendReport) -> Value {
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
    comparisons
}

#[cfg(test)]
mod tests {
    use super::{BOARD_PAGE_SQL, assert_board_ticket_sequences_equal};

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
}
