//! JSON report assembly.

use serde_json::{Value, json};

use crate::{SampleSummary, Scale, ScenarioResult};

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
            "approximate_row_count": scale.approximate_row_count(),
        },
        "configuration": {
            "warmup_iterations": warmups,
            "measured_samples": samples,
            "timing_source": "std::time::Instant",
            "distribution_method": "nearest-rank p50/p95/p99",
            "list_limit": 50,
        },
        "backends": backend_values,
        "comparisons": comparisons,
        "limitations": [
            "Application-equivalent baseline, not SQL plan equivalence.",
            "PostgreSQL uses SQL joins/filters; RiffDB uses symbolic commands and named RiffQL queries over public gRPC.",
            "Not the frozen WP-200 budget-comparison publication suite.",
            "Results are architectural feedback for optimization prioritization, not marketing claims.",
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
    json!({
        "backend_id": backend.backend_id,
        "description": backend.description,
        "guarantee_notes": backend.guarantee_notes,
        "seed_ns": backend.seed_ns,
        "seed_rows": backend.seed_rows,
        "scenarios": scenarios,
    })
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
    let postgres = backends.iter().find(|backend| backend.backend_id == "postgres_sql");
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
        let Some(rd) = riffdb
            .scenarios
            .iter()
            .find(|row| row.scenario == scenario)
        else {
            continue;
        };
        let pg_p50 = pg.samples.summary().p50_ns;
        let rd_p50 = rd.samples.summary().p50_ns;
        scenarios.push(json!({
            "scenario": scenario.as_str(),
            "postgres_p50_ns": pg_p50,
            "riffdb_p50_ns": rd_p50,
            "ratio_riffdb_over_postgres": if pg_p50 == 0 {
                0.0
            } else {
                rd_p50 as f64 / pg_p50 as f64
            },
        }));
    }
    scenarios.sort_by(|left, right| {
        right["ratio_riffdb_over_postgres"]
            .as_f64()
            .partial_cmp(&left["ratio_riffdb_over_postgres"].as_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let pg_seed = postgres.seed_ns;
    let rd_seed = riffdb.seed_ns;
    json!({
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
    })
}

use crate::ScenarioId;
