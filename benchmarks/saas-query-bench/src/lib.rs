//! Head-to-head SaaS complex-query and aggregation benchmark for RiffDB vs PostgreSQL.
//!
//! Exercises the five core SaaS analytical and operational query shapes introduced in
//! ADR-0307 and ADR-0309 over a live `riffdbd` gRPC server and a live PostgreSQL instance
//! (`minimal` and `safe-app` profiles), verifying exact result parity before measuring
//! unary latency (`p50`, `p95`, `p99`, `mean`, `min`, `max`) and concurrent throughput.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::future::Future;
use std::io::{self, BufRead, BufReader, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use postgres::{Client, NoTls, Statement};
use riffdb_auth::bootstrap_secret::{
    BootstrapCredential as RetainedBootstrapCredential, SystemEntropy,
    generate_bootstrap_credential, load_bootstrap_credential_file,
};
use riffdb_client_rust::{
    ApplicationCommand, ApplicationContract, ApplicationValue, AttemptBudget, BearerCredential,
    BootstrapCallMetadata, BootstrapCredential as TransportBootstrapCredential, CallMetadata,
    NamedQuery, NamedQueryResult, RiffDbClient, StableApplicationClient, app_v1,
    generate_capability_id, generate_request_id, v1,
};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::ContractBundle;
use riffdb_query_module::{
    ApplicationManifest, CompiledApplicationRole, NamedQuerySource, QueryModule,
    QueryModuleCandidate, QueryModuleName, QueryModuleVersion, compile_application_role,
};
use riffdb_types::{
    CapabilityGrantV1, CapabilityPermissionKindV1, CapabilityPermissionV1, PartitionScopeV1,
    TenantScope,
};
use serde::{Deserialize, Serialize};
use tonic::transport::Endpoint;

/// Schema version for JSON reports emitted by `riffdb-saas-query-bench`.
pub const SAAS_QUERY_BENCH_SCHEMA_VERSION: &str = "riffdb.saas-query-bench/v1";

const CONTRACT_LINEAGE: &str = "SaasAnalytics";
const CONTRACT_VERSION: u64 = 1;
const MODULE_NAME: &str = "saas_analytics";
const MODULE_VERSION: u64 = 1;
const APPLICATION_ROLE_NAME: &str = "SaasBenchApplication";
const AUDIENCE: &str = "riffdb-grpc-loopback";
const ENVIRONMENT: &str = "app-baseline";
const BOOTSTRAP_UNIX_MILLISECONDS: u64 = 1_700_000_000_000;
const CAPABILITY_LIFETIME_SECONDS: u32 = 3_600;

const CAPABILITY_KEY_DOCUMENT: &[u8] =
    b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEY_DOCUMENT: &[u8] =
    b"riffdb-idempotency-digest-keys-v1\n9:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";

/// Contract source for the SaaS analytics benchmark application.
pub const SAAS_CONTRACT_SOURCE: &str = r#"
contract SaasAnalytics version 1 {
  entity Organization {
    key (org_id: string<32>)
    field name: string<64>
    field plan_tier: string<32>
    field created_at: timestamp
  }

  entity AppUser {
    key (org_id: string<32>, user_id: string<32>)
    field email: string<128>
    field display_name: string<64>
    field department: string<32>
    field active: bool
    index by_org_user (org_id, user_id)
  }

  entity Invoice {
    key (org_id: string<32>, invoice_id: string<32>)
    field customer_tier: string<32>
    field status: string<32>
    field amount: money<USD>
    field discount: optional<money<USD>>
    field seats: optional<i64>
    field is_enterprise: bool
    field auto_pay: optional<bool>
    field issued_at: timestamp
    index by_org_issued_covering (org_id, issued_at, invoice_id) cover (amount, discount, seats, status, is_enterprise, auto_pay, customer_tier)
  }

  entity Project {
    key (org_id: string<32>, project_id: string<32>)
    field name: string<64>
    field tier: string<32>
    field status: string<32>
    field owner_id: string<32>
    field updated_at: timestamp
    index by_org_updated (org_id, updated_at, project_id) cover (name, tier, status, owner_id)
  }

  entity Task {
    key (org_id: string<32>, task_id: string<32>)
    field project_id: string<32>
    field assignee_id: string<32>
    field severity: string<32>
    field status: string<32>
    field environment: string<32>
    field title: string<128>
    field priority_rank: i64
    field story_points: i64
    field duration_ms: i64
    field cycle_hours: i64
    field is_critical_blocker: bool
    field completed_at: timestamp
    index by_org_completed (org_id, completed_at, task_id) cover (project_id, assignee_id, severity, status, environment, title, story_points, duration_ms, cycle_hours, is_critical_blocker)
    index by_org_project_completed (org_id, project_id, completed_at, task_id) cover (assignee_id, severity, status, story_points, cycle_hours, is_critical_blocker)
    index by_org_priority (org_id, priority_rank, task_id) cover (project_id, assignee_id, severity, status, title)
  }

  entity TaskSignal {
    key (org_id: string<32>, signal_kind: string<32>, task_id: string<32>)
    field recorded_at: timestamp
    index by_org_signal (org_id, signal_kind, task_id)
    reference task (org_id, task_id) -> Task(org_id, task_id)
  }

  aggregate Organizations {
    root Organization
    partition_by org_id
    conflict_key (org_id)
  }

  aggregate Users {
    root AppUser
    partition_by org_id
    conflict_key (org_id, user_id)
  }

  aggregate Invoices {
    root Invoice
    partition_by org_id
    conflict_key (org_id, invoice_id)
  }

  aggregate Projects {
    root Project
    partition_by org_id
    conflict_key (org_id, project_id)
  }

  aggregate Tasks {
    root Task
    partition_by org_id
    conflict_key (org_id, task_id)
  }

  aggregate TaskSignals {
    root TaskSignal
    partition_by org_id
    conflict_key (org_id, signal_kind, task_id)
  }

  command SeedOrganization {
    input idempotency_key: string<128>
    input org_id: string<32>
    input name: string<64>
    input plan_tier: string<32>
    input created_at: timestamp
    idempotency_key idempotency_key
    create Organization(org_id) as org
      else OrgExists { org_id: org_id }
    set org.name = name
    set org.plan_tier = plan_tier
    set org.created_at = created_at
    return Created { org_id: org_id }
  }

  command SeedAppUser {
    input idempotency_key: string<128>
    input org_id: string<32>
    input user_id: string<32>
    input email: string<128>
    input display_name: string<64>
    input department: string<32>
    input active: bool
    idempotency_key idempotency_key
    create AppUser(org_id, user_id) as u
      else UserExists { user_id: user_id }
    set u.email = email
    set u.display_name = display_name
    set u.department = department
    set u.active = active
    return Created { user_id: user_id }
  }

  command SeedInvoice {
    input idempotency_key: string<128>
    input org_id: string<32>
    input invoice_id: string<32>
    input customer_tier: string<32>
    input status: string<32>
    input amount: money<USD>
    input discount: optional<money<USD>>
    input seats: optional<i64>
    input is_enterprise: bool
    input auto_pay: optional<bool>
    input issued_at: timestamp
    idempotency_key idempotency_key
    create Invoice(org_id, invoice_id) as inv
      else InvoiceExists { invoice_id: invoice_id }
    set inv.customer_tier = customer_tier
    set inv.status = status
    set inv.amount = amount
    set inv.discount = discount
    set inv.seats = seats
    set inv.is_enterprise = is_enterprise
    set inv.auto_pay = auto_pay
    set inv.issued_at = issued_at
    return Created { invoice_id: invoice_id }
  }

  command SeedProject {
    input idempotency_key: string<128>
    input org_id: string<32>
    input project_id: string<32>
    input name: string<64>
    input tier: string<32>
    input status: string<32>
    input owner_id: string<32>
    input updated_at: timestamp
    idempotency_key idempotency_key
    create Project(org_id, project_id) as proj
      else ProjectExists { project_id: project_id }
    set proj.name = name
    set proj.tier = tier
    set proj.status = status
    set proj.owner_id = owner_id
    set proj.updated_at = updated_at
    return Created { project_id: project_id }
  }

  command SeedTask {
    input idempotency_key: string<128>
    input org_id: string<32>
    input task_id: string<32>
    input project_id: string<32>
    input assignee_id: string<32>
    input severity: string<32>
    input status: string<32>
    input environment: string<32>
    input title: string<128>
    input priority_rank: i64
    input story_points: i64
    input duration_ms: i64
    input cycle_hours: i64
    input is_critical_blocker: bool
    input completed_at: timestamp
    idempotency_key idempotency_key
    create Task(org_id, task_id) as t
      else TaskExists { task_id: task_id }
    set t.project_id = project_id
    set t.assignee_id = assignee_id
    set t.severity = severity
    set t.status = status
    set t.environment = environment
    set t.title = title
    set t.priority_rank = priority_rank
    set t.story_points = story_points
    set t.duration_ms = duration_ms
    set t.cycle_hours = cycle_hours
    set t.is_critical_blocker = is_critical_blocker
    set t.completed_at = completed_at
    return Created { task_id: task_id }
  }

  command SeedTaskSignal {
    input idempotency_key: string<128>
    input org_id: string<32>
    input signal_kind: string<32>
    input task_id: string<32>
    input recorded_at: timestamp
    idempotency_key idempotency_key
    read Task(org_id, task_id) as task
      else TaskMissing { task_id: task_id }
    create TaskSignal(org_id, signal_kind, task_id) as sig
      else SignalExists { task_id: task_id }
    set sig.recorded_at = recorded_at
    return Created { task_id: task_id }
  }
}
"#;

const QUERY_BILLING_KPI_SUMMARY: &str = r#"
query BillingKpiSummary(
    $org_id: Invoice.org_id,
    $window_start: Invoice.issued_at,
    $window_end: Invoice.issued_at
) {
    many invoices from Invoice
        where org_id == $org_id
          && issued_at >= $window_start
          && issued_at <= $window_end
        order by issued_at desc, invoice_id desc
        take 240

    aggregate summary from invoices {
        count() as invoice_count
        count() where status == "paid" as paid_count
        count() where status == "overdue" as overdue_count
        sum(amount) as gross_revenue
        sum(amount) where status == "paid" as paid_revenue
        sum(discount) where status == "paid" as discount_total
        sum(seats) where status == "paid" as paid_seats_total
        count() where status == "paid" && seats != null as paid_seats_count
        any(is_enterprise) where status == "overdue" as has_overdue_enterprise
        all(auto_pay) where status == "paid" as all_paid_autopay
    }

    return Found {
        summary: summary {
            invoice_count
            paid_count
            overdue_count
            gross_revenue
            paid_revenue
            discount_total
            paid_seats_total
            paid_seats_count
            has_overdue_enterprise
            all_paid_autopay
        }
    }
    outcomes Found
}
"#;

const QUERY_DAILY_REVENUE_COHORT_TOP_N: &str = r#"
query DailyRevenueCohortTopN(
    $org_id: Invoice.org_id,
    $window_start: Invoice.issued_at,
    $window_end: Invoice.issued_at,
    $min_paid_revenue: Invoice.amount,
    $min_paid_invoices: u64
) {
    many invoices from Invoice
        where org_id == $org_id
          && issued_at >= $window_start
          && issued_at <= $window_end
        order by issued_at desc, invoice_id desc
        take 240

    aggregate cohorts from invoices {
        group by day(issued_at) as issued_day, customer_tier
        count() as invoice_count
        count() where status == "paid" as paid_invoices
        sum(amount) where status == "paid" as paid_revenue
        sum(discount) where status == "paid" as discount_total
        sum(seats) where status == "paid" as seats_total
        count() where status == "paid" && seats != null as seats_count
        having paid_revenue >= $min_paid_revenue && paid_invoices >= $min_paid_invoices
        order by paid_revenue desc, issued_day desc, customer_tier asc
        take 10
    }

    return Found {
        cohorts: cohorts {
            issued_day
            customer_tier
            invoice_count
            paid_invoices
            paid_revenue
            discount_total
            seats_total
            seats_count
        }
    }
    outcomes Found
}
"#;

const QUERY_SLA_QUANTILES_AND_TAGS: &str = r#"
query SlaQuantilesAndTags(
    $org_id: Task.org_id,
    $window_start: Task.completed_at,
    $window_end: Task.completed_at,
    $breach_threshold_ms: Task.duration_ms
) {
    many tasks from Task
        where org_id == $org_id
          && completed_at >= $window_start
          && completed_at <= $window_end
        order by completed_at desc, task_id desc
        take 240

    aggregate sla_by_severity from tasks {
        group by severity
        count() as sample_count
        count() where duration_ms >= $breach_threshold_ms as breach_count
        median(duration_ms) as p50_ms
        p90(duration_ms) as p90_ms
        p95(duration_ms) as p95_ms
        p99(duration_ms) as p99_ms
        stddev(duration_ms) as stddev_ms
        first(title) as latest_title
        last(title) as oldest_title
        collect_distinct(environment) as environments
        order by p99_ms desc, severity asc
        take 8
    }

    return Found {
        sla_by_severity: sla_by_severity {
            severity
            sample_count
            breach_count
            p50_ms
            p90_ms
            p95_ms
            p99_ms
            stddev_ms
            latest_title
            oldest_title
            environments
        }
    }
    outcomes Found
}
"#;

const QUERY_PROJECT_PORTFOLIO_ROLLUP: &str = r#"
query ProjectPortfolioRollup(
    $org_id: Project.org_id,
    $since: Project.updated_at,
    $task_since: Task.completed_at
) {
    many projects from Project
        where org_id == $org_id
          && updated_at >= $since
        order by updated_at desc, project_id desc
        take 12

    many tasks from Task
        for each project in projects
        where org_id == $org_id
          && project_id == project.project_id
          && completed_at >= $task_since
        order by completed_at desc, task_id desc
        take 32 per project

    many org_tasks from Task
        where org_id == $org_id
          && completed_at >= $task_since
        order by completed_at desc, task_id desc
        take 240

    aggregate task_stats from org_tasks {
        group by project_id
        count() as total_tasks
        count() where status == "done" as completed_tasks
        count() where status == "blocked" as blocked_tasks
        sum(story_points) as total_points
        sum(story_points) where status == "done" as completed_points
        p95(cycle_hours) as p95_cycle_hours
        any(is_critical_blocker) as has_critical_blocker
        collect_distinct(assignee_id) as assignees
        order by project_id asc
        take 12
    }

    return Found {
        projects: projects {
            project_id
            name
            tier
            status
            tasks: tasks {
                task_id
            }
        }
        task_stats: task_stats {
            project_id
            total_tasks
            completed_tasks
            blocked_tasks
            total_points
            completed_points
            p95_cycle_hours
            has_critical_blocker
            assignees
        }
    }
    outcomes Found
}
"#;

const QUERY_CANDIDATE_SIGNAL_HYDRATION: &str = r#"
query CandidateSignalIntersectionHydration(
    $org_id: Task.org_id
) {
    candidates matched: Task.task_id
        from intersect {
            TaskSignal.task_id using by_org_signal where org_id == $org_id && signal_kind == "sla-breach",
            TaskSignal.task_id using by_org_signal where org_id == $org_id && signal_kind == "customer-escalation",
        }
        within 64
        else IntegrityFailure

    many tasks from Task
        where org_id == $org_id
          && task_id in matched
        order by priority_rank asc, task_id asc
        take 16

    many assignees from AppUser
        where org_id == $org_id
          && user_id in distinct(tasks.assignee_id)
        order by user_id asc
        take 16

    return Found {
        tasks: tasks {
            task_id
            project_id
            assignee_id
            severity
            status
            priority_rank
            title
        }
        assignees: assignees {
            user_id
            email
            display_name
            department
        }
    }
    outcomes Found | IntegrityFailure
}
"#;

const SAAS_QUERY_SOURCES: &[(&str, &str)] = &[
    ("BillingKpiSummary", QUERY_BILLING_KPI_SUMMARY),
    (
        "CandidateSignalIntersectionHydration",
        QUERY_CANDIDATE_SIGNAL_HYDRATION,
    ),
    ("DailyRevenueCohortTopN", QUERY_DAILY_REVENUE_COHORT_TOP_N),
    ("ProjectPortfolioRollup", QUERY_PROJECT_PORTFOLIO_ROLLUP),
    ("SlaQuantilesAndTags", QUERY_SLA_QUANTILES_AND_TAGS),
];

/// Closed list of SaaS complex-query workloads evaluated by the benchmark.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SaasWorkloadKind {
    /// Conditional KPI & `money<USD>` / optional field billing summary.
    BillingKpiConditionalSummary,
    /// UTC `day(issued_at)` + `customer_tier` cohort rollup with `having` and `order by ... take N`.
    DailyRevenueCohortTopN,
    /// Exact order statistics (`median`, `p90`, `p95`, `p99`), `stddev`, `first`/`last`, and `collect_distinct`.
    SlaQuantilesDispersionAndTags,
    /// Per-parent child expansion (`for each`) + nested `aggregate ... per` rollup in one snapshot.
    ProjectExpansionAndNestedRollup,
    /// Multi-index `candidates ... from intersect` + ordered page + `in distinct(...)` FK hydration.
    CandidateIntersectionAndDistinctHydration,
}

impl SaasWorkloadKind {
    /// All five SaaS complex-query workloads.
    pub const ALL: [Self; 5] = [
        Self::BillingKpiConditionalSummary,
        Self::DailyRevenueCohortTopN,
        Self::SlaQuantilesDispersionAndTags,
        Self::ProjectExpansionAndNestedRollup,
        Self::CandidateIntersectionAndDistinctHydration,
    ];

    /// Stable identifier used in reports and permissions.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BillingKpiConditionalSummary => "billing_kpi_conditional_summary",
            Self::DailyRevenueCohortTopN => "daily_revenue_cohort_top_n",
            Self::SlaQuantilesDispersionAndTags => "sla_quantiles_dispersion_and_tags",
            Self::ProjectExpansionAndNestedRollup => "project_expansion_and_nested_rollup",
            Self::CandidateIntersectionAndDistinctHydration => {
                "candidate_intersection_and_distinct_hydration"
            }
        }
    }

    /// RiffQL query name in `saas_analytics`.
    #[must_use]
    pub const fn query_name(self) -> &'static str {
        match self {
            Self::BillingKpiConditionalSummary => "BillingKpiSummary",
            Self::DailyRevenueCohortTopN => "DailyRevenueCohortTopN",
            Self::SlaQuantilesDispersionAndTags => "SlaQuantilesAndTags",
            Self::ProjectExpansionAndNestedRollup => "ProjectPortfolioRollup",
            Self::CandidateIntersectionAndDistinctHydration => {
                "CandidateSignalIntersectionHydration"
            }
        }
    }
}

/// PostgreSQL comparator profile (`minimal` or `safe-app`).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PostgresProfile {
    /// Direct prepared SQL execution without application permission checks or repeatable-read wrapper.
    Minimal,
    /// Prepared SQL execution with per-operation permission check and `REPEATABLE READ READ ONLY` snapshot wrapper where required.
    SafeApp,
}

impl PostgresProfile {
    /// Stable profile name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Minimal => "postgres-minimal",
            Self::SafeApp => "postgres-safe-app",
        }
    }
}

/// Scale preset for seeding and timing.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeedScale {
    /// Fast smoke scale for CI and quick verification.
    Smoke,
    /// Full benchmark scale for latency and concurrent throughput comparisons.
    Full,
}

impl SeedScale {
    /// Stable preset name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Full => "full",
        }
    }

    /// Number of organizations seeded.
    #[must_use]
    pub const fn org_count(self) -> usize {
        match self {
            Self::Smoke => 2,
            Self::Full => 4,
        }
    }

    /// Users per organization.
    #[must_use]
    pub const fn users_per_org(self) -> usize {
        match self {
            Self::Smoke => 16,
            Self::Full => 24,
        }
    }

    /// Invoices per organization (fits within the 240-row bounded scan window so RiffDB and PostgreSQL scan the exact same complete set).
    #[must_use]
    pub const fn invoices_per_org(self) -> usize {
        match self {
            Self::Smoke => 120,
            Self::Full => 240,
        }
    }

    /// Projects per organization (fits within the 12-project portfolio page).
    #[must_use]
    pub const fn projects_per_org(self) -> usize {
        12
    }

    /// Tasks per project (`12 * 12 = 144` or `12 * 20 = 240` tasks per org, fitting both the 32-task per-project bound and 240-task org SLA window).
    #[must_use]
    pub const fn tasks_per_project(self) -> usize {
        match self {
            Self::Smoke => 12,
            Self::Full => 20,
        }
    }

    /// Unary measurement iterations per query.
    #[must_use]
    pub const fn unary_iterations(self) -> usize {
        match self {
            Self::Smoke => 40,
            Self::Full => 200,
        }
    }

    /// Warmup iterations before timing.
    #[must_use]
    pub const fn warmup_iterations(self) -> usize {
        match self {
            Self::Smoke => 8,
            Self::Full => 24,
        }
    }

    /// Concurrency levels tested for throughput and tail latency.
    #[must_use]
    pub const fn concurrency_levels(self) -> &'static [usize] {
        match self {
            Self::Smoke => &[1, 4],
            Self::Full => &[1, 8, 32],
        }
    }

    /// Operations per worker at each concurrency level.
    #[must_use]
    pub const fn ops_per_worker(self) -> usize {
        match self {
            Self::Smoke => 25,
            Self::Full => 100,
        }
    }
}

/// Compiled RiffDB contract, query module, manifest, and application role for the SaaS benchmark.
pub struct CompiledSaasArtifacts {
    /// Compiled contract bundle.
    pub contract: ContractBundle,
    /// Compiled RiffQL query module.
    pub query_module: QueryModule,
    /// Parsed application manifest.
    pub manifest: ApplicationManifest,
    /// Compiled application role for `SaasBenchApplication`.
    pub role: CompiledApplicationRole,
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(char::from(HEX[usize::from(b >> 4)]));
        out.push(char::from(HEX[usize::from(b & 0x0f)]));
    }
    out
}

/// Compiles the SaaS benchmark contract, query module, manifest, and application role.
pub fn compile_saas_artifacts() -> Result<CompiledSaasArtifacts, String> {
    let contract = compile_contract_source(SAAS_CONTRACT_SOURCE)
        .map_err(|diagnostics| format!("contract compile failed: {diagnostics:?}"))?;
    let queries = SAAS_QUERY_SOURCES
        .iter()
        .map(|(name, source)| NamedQuerySource::new(*name, *source).map_err(|e| format!("{e}")))
        .collect::<Result<Vec<_>, _>>()?;
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new(MODULE_NAME).map_err(|e| format!("{e}"))?,
        QueryModuleVersion::new(MODULE_VERSION)
            .ok_or_else(|| "invalid module version".to_owned())?,
        queries,
    )
    .map_err(|e| format!("{e}"))?;
    let query_module =
        QueryModule::compile(candidate, &contract).map_err(|e| format!("query compile: {e:?}"))?;

    let contract_hash_hex = hex_encode(contract.bundle_hash().as_bytes());
    let module_hash_hex = hex_encode(query_module.identity().as_bytes());

    let query_entries: Vec<serde_json::Value> = SAAS_QUERY_SOURCES
        .iter()
        .map(|(name, _)| {
            serde_json::json!({
                "name": name,
                "source": format!("queries/saas_analytics/{name}.riffq")
            })
        })
        .collect();
    let query_names: Vec<&str> = SAAS_QUERY_SOURCES.iter().map(|(name, _)| *name).collect();

    let manifest_json = serde_json::json!({
        "application": "saas-analytics",
        "contract": {
            "bundle_hash": contract_hash_hex,
            "lineage": CONTRACT_LINEAGE,
            "source": "contracts/saas_analytics.riff",
            "version": CONTRACT_VERSION
        },
        "generation": {
            "mcp": "fixtures/query-modules/saas_analytics.mcp.json",
            "rust": "fixtures/query-modules/saas_analytics.rs",
            "typescript": "clients/typescript/saas_analytics/client.ts"
        },
        "query_modules": [
            {
                "module_hash": module_hash_hex,
                "name": MODULE_NAME,
                "queries": query_entries,
                "version": MODULE_VERSION
            }
        ],
        "roles": [
            {
                "commands": [
                    "SeedAppUser",
                    "SeedInvoice",
                    "SeedOrganization",
                    "SeedProject",
                    "SeedTask",
                    "SeedTaskSignal"
                ],
                "environment": "development",
                "name": APPLICATION_ROLE_NAME,
                "queries": query_names,
                "tenant_scope": "global"
            }
        ],
        "schema": "riffdb.application-manifest/v1",
        "seed_inputs": ["examples/ticketdesk/seed/dev.jsonl"]
    });
    let manifest = ApplicationManifest::parse(&manifest_json.to_string())
        .map_err(|e| format!("manifest parse failed: {e:?}"))?;
    let role = compile_application_role(
        &manifest,
        APPLICATION_ROLE_NAME,
        None,
        &contract,
        std::slice::from_ref(&query_module),
    )
    .map_err(|e| format!("compile_application_role failed: {e:?}"))?;

    Ok(CompiledSaasArtifacts {
        contract,
        query_module,
        manifest,
        role,
    })
}

/// Base UTC epoch millisecond anchor (`2026-01-01T00:00:00Z`) for deterministic benchmark data.
pub const BASE_EPOCH_MS: i64 = 1_767_225_600_000;
/// Milliseconds per UTC day.
pub const DAY_MS: i64 = 86_400_000;

/// Deterministic organization seed row.
#[derive(Clone, Debug)]
pub struct SeedOrg {
    /// Organization ID.
    pub org_id: String,
    /// Organization display name.
    pub name: String,
    /// Plan tier.
    pub plan_tier: String,
    /// Creation timestamp (UTC ms).
    pub created_at_ms: i64,
}

/// Deterministic user seed row.
#[derive(Clone, Debug)]
pub struct SeedUser {
    /// Organization ID.
    pub org_id: String,
    /// User ID.
    pub user_id: String,
    /// Email address.
    pub email: String,
    /// Display name.
    pub display_name: String,
    /// Department name.
    pub department: String,
    /// Active flag.
    pub active: bool,
}

/// Deterministic invoice seed row.
#[derive(Clone, Debug)]
pub struct SeedInvoice {
    /// Organization ID.
    pub org_id: String,
    /// Invoice ID.
    pub invoice_id: String,
    /// Customer tier (`enterprise`, `growth`, `starter`).
    pub customer_tier: String,
    /// Status (`paid`, `overdue`, `open`).
    pub status: String,
    /// Amount in USD cents (`money<USD>`).
    pub amount_cents: i128,
    /// Optional discount in USD cents (`optional<money<USD>>`).
    pub discount_cents: Option<i128>,
    /// Optional billed seat count (`optional<i64>`).
    pub seats: Option<i64>,
    /// Whether this invoice belongs to an enterprise account.
    pub is_enterprise: bool,
    /// Optional auto-pay flag (`optional<bool>`).
    pub auto_pay: Option<bool>,
    /// Issued timestamp (UTC ms).
    pub issued_at_ms: i64,
}

/// Deterministic project seed row.
#[derive(Clone, Debug)]
pub struct SeedProject {
    /// Organization ID.
    pub org_id: String,
    /// Project ID.
    pub project_id: String,
    /// Project name.
    pub name: String,
    /// Tier (`mission-critical`, `standard`, `internal`).
    pub tier: String,
    /// Status (`active`, `at-risk`).
    pub status: String,
    /// Owner user ID.
    pub owner_id: String,
    /// Updated timestamp (UTC ms).
    pub updated_at_ms: i64,
}

/// Deterministic task seed row.
#[derive(Clone, Debug)]
pub struct SeedTask {
    /// Organization ID.
    pub org_id: String,
    /// Task ID.
    pub task_id: String,
    /// Parent project ID.
    pub project_id: String,
    /// Assignee user ID.
    pub assignee_id: String,
    /// Severity (`sev1`, `sev2`, `sev3`, `sev4`).
    pub severity: String,
    /// Status (`done`, `in_progress`, `blocked`).
    pub status: String,
    /// Deployment environment (`prod-us`, `prod-eu`, `prod-ap`, `staging`).
    pub environment: String,
    /// Task title.
    pub title: String,
    /// Priority rank.
    pub priority_rank: i64,
    /// Story points.
    pub story_points: i64,
    /// Resolution duration in milliseconds.
    pub duration_ms: i64,
    /// Cycle time in hours.
    pub cycle_hours: i64,
    /// Critical blocker flag.
    pub is_critical_blocker: bool,
    /// Completed timestamp (UTC ms).
    pub completed_at_ms: i64,
}

/// Deterministic task signal seed row.
#[derive(Clone, Debug)]
pub struct SeedTaskSignal {
    /// Organization ID.
    pub org_id: String,
    /// Signal kind (`sla-breach`, `customer-escalation`).
    pub signal_kind: String,
    /// Task ID.
    pub task_id: String,
    /// Recorded timestamp (UTC ms).
    pub recorded_at_ms: i64,
}

/// Complete deterministic dataset for both RiffDB and PostgreSQL.
#[derive(Clone, Debug)]
pub struct SeedDataset {
    /// Scale preset.
    pub scale: SeedScale,
    /// Organizations.
    pub orgs: Vec<SeedOrg>,
    /// Users.
    pub users: Vec<SeedUser>,
    /// Invoices.
    pub invoices: Vec<SeedInvoice>,
    /// Projects.
    pub projects: Vec<SeedProject>,
    /// Tasks.
    pub tasks: Vec<SeedTask>,
    /// Task signals.
    pub signals: Vec<SeedTaskSignal>,
    /// Query window start (UTC ms).
    pub window_start_ms: i64,
    /// Query window end (UTC ms).
    pub window_end_ms: i64,
}

impl SeedDataset {
    /// Generates the deterministic dataset for the requested scale.
    #[must_use]
    pub fn generate(scale: SeedScale) -> Self {
        let mut orgs = Vec::new();
        let mut users = Vec::new();
        let mut invoices = Vec::new();
        let mut projects = Vec::new();
        let mut tasks = Vec::new();
        let mut signals = Vec::new();

        let tiers = ["enterprise", "growth", "starter"];
        let departments = ["engineering", "platform", "security", "operations"];
        let severities = ["sev1", "sev2", "sev3", "sev4"];
        let environments = ["prod-us", "prod-eu", "prod-ap", "staging"];

        for org_idx in 0..scale.org_count() {
            let org_num = org_idx + 1;
            let org_id = format!("org-{org_num}");
            orgs.push(SeedOrg {
                org_id: org_id.clone(),
                name: format!("Organization {org_num}"),
                plan_tier: tiers[org_idx % tiers.len()].to_owned(),
                created_at_ms: BASE_EPOCH_MS,
            });

            for user_idx in 0..scale.users_per_org() {
                let user_num = user_idx + 1;
                let user_id = format!("user-{user_num:02}");
                users.push(SeedUser {
                    org_id: org_id.clone(),
                    user_id: user_id.clone(),
                    email: format!("user-{user_num:02}@org-{org_num}.example.com"),
                    display_name: format!("Engineer {user_num:02} ({org_id})"),
                    department: departments[user_idx % departments.len()].to_owned(),
                    active: true,
                });
            }

            // Invoices across 15 UTC days (`day 0` .. `day 14`).
            for inv_idx in 0..scale.invoices_per_org() {
                let inv_num = inv_idx + 1;
                let invoice_id = format!("inv-{inv_num:04}");
                let day_offset = (inv_idx % 15) as i64;
                let intra_day_ms = ((inv_idx * 1_733) % 80_000) as i64 * 1_000;
                let issued_at_ms = BASE_EPOCH_MS + day_offset * DAY_MS + intra_day_ms;

                let customer_tier = tiers[(inv_idx + org_idx) % tiers.len()].to_owned();
                let is_enterprise = customer_tier == "enterprise";
                let status = match inv_idx % 5 {
                    0..=2 => "paid",
                    3 => "overdue",
                    _ => "open",
                }
                .to_owned();

                let base_cents = match customer_tier.as_str() {
                    "enterprise" => 250_000_i128,
                    "growth" => 85_000_i128,
                    _ => 24_000_i128,
                };
                let amount_cents =
                    base_cents + ((inv_idx as i128 * 3_750) + (org_idx as i128 * 11_200));

                let discount_cents = if inv_idx % 3 == 0 {
                    None
                } else {
                    Some((amount_cents / 20) + ((inv_idx as i128 % 7) * 125))
                };

                let seats = if inv_idx % 4 == 0 {
                    None
                } else {
                    Some(10 + ((inv_idx as i64 * 7 + org_idx as i64 * 3) % 190))
                };

                // In odd-numbered organizations (`org-1`, `org-3`), all non-null paid invoices have
                // `auto_pay = Some(true)` so `all(auto_pay) where status == "paid"` evaluates to `true`.
                // In even-numbered organizations (`org-2`, `org-4`), some paid invoices have `Some(false)`
                // so `all(auto_pay) where status == "paid"` evaluates to `false`.
                let auto_pay = if inv_idx % 6 == 0 {
                    None
                } else if status == "paid" {
                    if org_num % 2 == 1 {
                        Some(true)
                    } else {
                        Some(inv_idx % 5 != 1)
                    }
                } else {
                    Some(inv_idx % 2 == 0)
                };

                invoices.push(SeedInvoice {
                    org_id: org_id.clone(),
                    invoice_id,
                    customer_tier,
                    status,
                    amount_cents,
                    discount_cents,
                    seats,
                    is_enterprise,
                    auto_pay,
                    issued_at_ms,
                });
            }

            // Projects and child tasks.
            let mut task_seq = 0usize;
            for proj_idx in 0..scale.projects_per_org() {
                let proj_num = proj_idx + 1;
                let project_id = format!("proj-{proj_num:02}");
                let owner_num = (proj_idx % scale.users_per_org()) + 1;
                let updated_at_ms =
                    BASE_EPOCH_MS + (proj_idx as i64 + 1) * DAY_MS + (org_idx as i64 * 60_000);

                projects.push(SeedProject {
                    org_id: org_id.clone(),
                    project_id: project_id.clone(),
                    name: format!("Project {proj_num:02} ({org_id})"),
                    tier: match proj_idx % 3 {
                        0 => "mission-critical",
                        1 => "standard",
                        _ => "internal",
                    }
                    .to_owned(),
                    status: if proj_idx % 4 == 0 {
                        "at-risk".to_owned()
                    } else {
                        "active".to_owned()
                    },
                    owner_id: format!("user-{owner_num:02}"),
                    updated_at_ms,
                });

                for task_in_proj in 0..scale.tasks_per_project() {
                    task_seq += 1;
                    let task_id = format!("task-{task_seq:04}");
                    let assignee_num = ((task_seq + org_idx) % scale.users_per_org()) + 1;
                    let assignee_id = format!("user-{assignee_num:02}");
                    let severity = severities[(task_seq + proj_idx) % severities.len()].to_owned();
                    let status = match task_in_proj % 4 {
                        0 | 1 => "done",
                        2 => "in_progress",
                        _ => "blocked",
                    }
                    .to_owned();
                    let environment =
                        environments[(task_seq + org_idx) % environments.len()].to_owned();
                    let priority_rank = ((task_seq * 17 + proj_idx * 3) % 100) as i64 + 1;
                    let story_points = ((task_in_proj % 5) as i64 * 2) + 1;
                    let severity_weight = match severity.as_str() {
                        "sev1" => 4_800_000_i64,
                        "sev2" => 2_700_000_i64,
                        "sev3" => 1_400_000_i64,
                        _ => 650_000_i64,
                    };
                    let duration_ms = severity_weight
                        + ((task_seq as i64 * 193_700 + org_idx as i64 * 41_000) % 3_200_000);
                    let cycle_hours = 4 + ((task_seq as i64 * 9 + proj_idx as i64 * 5) % 96);
                    let is_critical_blocker = status == "blocked" && (task_in_proj % 7 == 3);
                    let completed_at_ms = BASE_EPOCH_MS
                        + ((task_seq % 15) as i64) * DAY_MS
                        + (task_seq as i64 * 12_000);

                    tasks.push(SeedTask {
                        org_id: org_id.clone(),
                        task_id: task_id.clone(),
                        project_id: project_id.clone(),
                        assignee_id,
                        severity,
                        status,
                        environment,
                        title: format!("Task {task_seq:04} in {project_id}"),
                        priority_rank,
                        story_points,
                        duration_ms,
                        cycle_hours,
                        is_critical_blocker,
                        completed_at_ms,
                    });

                    // Emit intersecting and non-intersecting signals on a subset of tasks.
                    if task_seq <= 36 {
                        if task_seq.is_multiple_of(2) {
                            signals.push(SeedTaskSignal {
                                org_id: org_id.clone(),
                                signal_kind: "sla-breach".to_owned(),
                                task_id: task_id.clone(),
                                recorded_at_ms: completed_at_ms,
                            });
                        }
                        if task_seq.is_multiple_of(3) {
                            signals.push(SeedTaskSignal {
                                org_id: org_id.clone(),
                                signal_kind: "customer-escalation".to_owned(),
                                task_id,
                                recorded_at_ms: completed_at_ms + 1_000,
                            });
                        }
                    }
                }
            }
        }

        Self {
            scale,
            orgs,
            users,
            invoices,
            projects,
            tasks,
            signals,
            window_start_ms: BASE_EPOCH_MS,
            window_end_ms: BASE_EPOCH_MS + 20 * DAY_MS,
        }
    }
}

/// Canonical domain representation of the 5 query outputs used to assert exact parity
/// between RiffDB and PostgreSQL (`minimal` and `safe-app`).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CanonicalQueryOutput {
    /// Output of `BillingKpiSummary`.
    BillingKpiSummary {
        /// Total invoices in window.
        invoice_count: u64,
        /// Paid invoices in window.
        paid_count: u64,
        /// Overdue invoices in window.
        overdue_count: u64,
        /// Gross invoice revenue in USD cents.
        gross_revenue_cents: i128,
        /// Paid invoice revenue in USD cents.
        paid_revenue_cents: i128,
        /// Paid discount total in USD cents.
        discount_total_cents: Option<i128>,
        /// Mean paid seats represented as `(total_seats, non_null_count)`.
        avg_paid_seats: Option<(i128, u64)>,
        /// Whether any overdue invoice belongs to an enterprise customer.
        has_overdue_enterprise: bool,
        /// Whether all paid invoices with non-null `auto_pay` have `auto_pay == true`.
        all_paid_autopay: bool,
    },
    /// Output of `DailyRevenueCohortTopN`.
    DailyRevenueCohorts(Vec<CanonicalRevenueCohortRow>),
    /// Output of `SlaQuantilesAndTags`.
    SlaBySeverity(Vec<CanonicalSlaSeverityRow>),
    /// Output of `ProjectPortfolioRollup`.
    ProjectPortfolio(Vec<CanonicalProjectPortfolioRow>),
    /// Output of `CandidateSignalIntersectionHydration`.
    CandidateSignalHydration {
        /// Ordered hydrated task rows.
        tasks: Vec<CanonicalHydratedTaskRow>,
        /// Ordered hydrated assignee rows.
        assignees: Vec<CanonicalHydratedAssigneeRow>,
    },
}

/// One grouped cohort row from `DailyRevenueCohortTopN`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalRevenueCohortRow {
    /// UTC day bucket start in epoch milliseconds.
    pub issued_day_ms: i64,
    /// Customer tier.
    pub customer_tier: String,
    /// Total invoices in cohort.
    pub invoice_count: u64,
    /// Paid invoices in cohort.
    pub paid_invoices: u64,
    /// Paid revenue in USD cents.
    pub paid_revenue_cents: i128,
    /// Discount total on paid invoices in USD cents.
    pub discount_total_cents: Option<i128>,
    /// Mean seats on paid invoices as `(total_seats, non_null_count)`.
    pub avg_seats: Option<(i128, u64)>,
}

/// One grouped severity row from `SlaQuantilesAndTags`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalSlaSeverityRow {
    /// Severity level.
    pub severity: String,
    /// Total tasks in severity group.
    pub sample_count: u64,
    /// Tasks with `duration_ms >= 3_600_000`.
    pub breach_count: u64,
    /// Exact nearest-rank p50 (`median`) duration in ms.
    pub p50_ms: i64,
    /// Exact nearest-rank p90 duration in ms.
    pub p90_ms: i64,
    /// Exact nearest-rank p95 duration in ms.
    pub p95_ms: i64,
    /// Exact nearest-rank p99 duration in ms.
    pub p99_ms: i64,
    /// Exact population standard deviation scaled by `100` (scale=2 fixed-point integer).
    pub stddev_ms_scaled_100: i128,
    /// First title in scan order (`completed_at desc, task_id asc`).
    pub latest_title: String,
    /// Last title in scan order (`completed_at desc, task_id asc`).
    pub oldest_title: String,
    /// Canonically sorted distinct environments.
    pub environments: Vec<String>,
}

/// One expanded project + nested task rollup row from `ProjectPortfolioRollup`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalProjectPortfolioRow {
    /// Project ID.
    pub project_id: String,
    /// Project name.
    pub name: String,
    /// Project tier.
    pub tier: String,
    /// Project status.
    pub status: String,
    /// Expanded child task IDs in scan order.
    pub child_task_ids: Vec<String>,
    /// Total child tasks.
    pub total_tasks: u64,
    /// Completed child tasks (`status == "done"`).
    pub completed_tasks: u64,
    /// Blocked child tasks (`status == "blocked"`).
    pub blocked_tasks: u64,
    /// Total story points across child tasks.
    pub total_points: i128,
    /// Completed story points (`status == "done"`).
    pub completed_points: i128,
    /// Exact nearest-rank p95 cycle hours across child tasks.
    pub p95_cycle_hours: i64,
    /// Whether any child task has `is_critical_blocker == true`.
    pub has_critical_blocker: bool,
    /// Canonically sorted distinct assignee IDs across child tasks.
    pub assignees: Vec<String>,
}

/// One hydrated task row from `CandidateSignalIntersectionHydration`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalHydratedTaskRow {
    /// Task ID.
    pub task_id: String,
    /// Project ID.
    pub project_id: String,
    /// Assignee ID.
    pub assignee_id: String,
    /// Severity.
    pub severity: String,
    /// Status.
    pub status: String,
    /// Priority rank.
    pub priority_rank: i64,
    /// Title.
    pub title: String,
}

/// One hydrated assignee row from `CandidateSignalIntersectionHydration`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalHydratedAssigneeRow {
    /// User ID.
    pub user_id: String,
    /// Email.
    pub email: String,
    /// Display name.
    pub display_name: String,
    /// Department.
    pub department: String,
}

fn usd_value(cents: i128) -> ApplicationValue {
    ApplicationValue::Money {
        currency: "USD".to_owned(),
        amount: Box::new(ApplicationValue::Decimal {
            coefficient_twos_complement: encode_twos_complement_i128(cents),
            scale: 2,
            precision: Some(38),
        }),
    }
}

fn timestamp_value(epoch_millis: i64) -> ApplicationValue {
    let seconds = epoch_millis.div_euclid(1_000);
    let nanos = (epoch_millis.rem_euclid(1_000) * 1_000_000) as u32;
    ApplicationValue::Timestamp { seconds, nanos }
}

fn encode_twos_complement_i128(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let mut start = 0usize;
    if value >= 0 {
        while start < 15 && bytes[start] == 0 && (bytes[start + 1] & 0x80) == 0 {
            start += 1;
        }
    } else {
        while start < 15 && bytes[start] == 0xFF && (bytes[start + 1] & 0x80) != 0 {
            start += 1;
        }
    }
    bytes[start..].to_vec()
}

fn decode_twos_complement_i128(bytes: &[u8]) -> Result<i128, String> {
    if bytes.is_empty() || bytes.len() > 16 {
        return Err(format!(
            "invalid two's-complement byte length {}",
            bytes.len()
        ));
    }
    let fill = if (bytes[0] & 0x80) != 0 { 0xFF } else { 0x00 };
    let mut buf = [fill; 16];
    let offset = 16 - bytes.len();
    buf[offset..].copy_from_slice(bytes);
    Ok(i128::from_be_bytes(buf))
}

fn fresh_request_id_bytes() -> Result<Vec<u8>, String> {
    Ok(generate_request_id()
        .map_err(|e| format!("generate_request_id: {e}"))?
        .into_bytes()
        .to_vec())
}

fn write_protected_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn bootstrap_request(
    credential: &RetainedBootstrapCredential,
) -> Result<v1::CreateCapabilityRequest, String> {
    use v1::capability_permission::Permission;
    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Bootstrap as i32,
        capability_id: credential.capability_id().into_bytes().to_vec(),
        principal_id: "saas-bench-bootstrap".to_owned(),
        actor_kind: v1::ActorKind::Human as i32,
        requested_lifetime_seconds: CAPABILITY_LIFETIME_SECONDS,
        audiences: vec![AUDIENCE.to_owned()],
        grant: Some(v1::CapabilityGrant {
            tenant_scope: Some(v1::TenantScope {
                scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
            }),
            partition_scope: Some(v1::PartitionScope {
                scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
            }),
            permissions: vec![
                v1::CapabilityPermission {
                    permission: Some(Permission::DeployContract(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::ReadHealth(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::AdministerCapabilities(v1::Unit {})),
                },
            ],
            field_visibility: Vec::new(),
            max_scan_rows: 1,
            approval_required: Vec::new(),
            row_policy: None,
            export: None,
            reimport: None,
            vector_inspection: None,
        }),
    })
}

fn role_capability_request(
    grant: &CapabilityGrantV1,
) -> Result<v1::CreateCapabilityRequest, String> {
    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Normal as i32,
        capability_id: generate_capability_id()
            .map_err(|e| format!("{e}"))?
            .into_bytes()
            .to_vec(),
        principal_id: "saas-bench-application".to_owned(),
        actor_kind: v1::ActorKind::Service as i32,
        requested_lifetime_seconds: CAPABILITY_LIFETIME_SECONDS,
        audiences: vec![AUDIENCE.to_owned()],
        grant: Some(application_role_grant_to_proto(grant)?),
    })
}

fn application_role_grant_to_proto(
    grant: &CapabilityGrantV1,
) -> Result<v1::CapabilityGrant, String> {
    let tenant_scope = match grant.tenant_scope() {
        TenantScope::Global => v1::TenantScope {
            scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
        },
        TenantScope::Tenant(tenant) => v1::TenantScope {
            scope: Some(v1::tenant_scope::Scope::TenantId(
                tenant.as_str().to_owned(),
            )),
        },
    };
    let partition_scope = match grant.partition_scope() {
        PartitionScopeV1::All => v1::PartitionScope {
            scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
        },
        PartitionScopeV1::Explicit(_) => {
            return Err("application role lowered to explicit partition scope".to_owned());
        }
    };
    let permissions = grant
        .permissions()
        .as_slice()
        .iter()
        .map(application_role_permission_to_proto)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(v1::CapabilityGrant {
        tenant_scope: Some(tenant_scope),
        partition_scope: Some(partition_scope),
        permissions,
        field_visibility: grant
            .field_visibility()
            .iter()
            .map(|visibility| v1::EntityFieldVisibility {
                contract_lineage: visibility.lineage().as_str().to_owned(),
                entity_type_id: visibility.entity_type().get(),
                field_ids: visibility
                    .fields()
                    .iter()
                    .map(|field| field.get())
                    .collect(),
                secret_field_ids: Vec::new(),
            })
            .collect(),
        max_scan_rows: u32::from(grant.max_scan_rows().get()),
        approval_required: Vec::new(),
        row_policy: None,
        export: None,
        reimport: None,
        vector_inspection: None,
    })
}

fn application_role_permission_to_proto(
    permission: &CapabilityPermissionV1,
) -> Result<v1::CapabilityPermission, String> {
    use v1::capability_permission::Permission;
    let permission = match permission {
        CapabilityPermissionV1::InvokeCommand(lineage, command) => {
            Permission::InvokeCommand(v1::LineageScopedStableId {
                contract_lineage: lineage.as_str().to_owned(),
                stable_id: command.get(),
            })
        }
        CapabilityPermissionV1::ExecuteNamedQuery(lineage, module_hash, query_name) => {
            Permission::ExecuteNamedQuery(v1::NamedQueryPermission {
                contract_lineage: lineage.as_str().to_owned(),
                query_module_hash: module_hash.as_bytes().to_vec(),
                query_name: query_name.as_str().to_owned(),
            })
        }
        CapabilityPermissionV1::ApplicationRoleIdentity(role_hash) => {
            Permission::ApplicationRoleIdentity(role_hash.as_bytes().to_vec())
        }
        CapabilityPermissionV1::Unparameterized(kind) => match kind {
            CapabilityPermissionKindV1::ReadContract => Permission::ReadContract(v1::Unit {}),
            CapabilityPermissionKindV1::ExecuteAdHocQuery => {
                Permission::ExecuteAdHocQuery(v1::Unit {})
            }
            other => {
                return Err(format!(
                    "unsupported unparameterized capability permission: {other:?}"
                ));
            }
        },
        _ => {
            return Err("application role compiler emitted non-application authority".to_owned());
        }
    };
    Ok(v1::CapabilityPermission {
        permission: Some(permission),
    })
}

/// Guard owning a spawned `riffdbd` child process for the duration of a benchmark run.
pub struct SpawnedRiffdbd {
    child: Child,
    /// Public gRPC endpoint (`http://127.0.0.1:<port>`).
    pub endpoint: String,
    /// Retained bootstrap credential used to deploy the contract and query module.
    pub bootstrap_credential: RetainedBootstrapCredential,
}

impl SpawnedRiffdbd {
    /// Prepares a fresh directory with capability/idempotency keys and bootstrap credential,
    /// spawns `riffdbd`, and waits for the `riffdbd-ready-v1` readiness line.
    pub fn spawn(riffdbd_bin: &Path, work_dir: &Path) -> Result<Self, String> {
        let database_path = work_dir.join("riffdb.redb");
        let backup_root = work_dir.join("backups");
        let capability_keys_path = work_dir.join("capability.keys");
        let idempotency_keys_path = work_dir.join("idempotency.keys");
        let bootstrap_path = work_dir.join("bootstrap.credential");

        fs::create_dir_all(&backup_root).map_err(|e| format!("create backup_root: {e}"))?;
        write_protected_file(&capability_keys_path, CAPABILITY_KEY_DOCUMENT)
            .map_err(|e| format!("write capability.keys: {e}"))?;
        write_protected_file(&idempotency_keys_path, IDEMPOTENCY_KEY_DOCUMENT)
            .map_err(|e| format!("write idempotency.keys: {e}"))?;

        let generated = generate_bootstrap_credential(BOOTSTRAP_UNIX_MILLISECONDS, &SystemEntropy)
            .map_err(|e| format!("generate_bootstrap_credential: {e}"))?;
        write_protected_file(&bootstrap_path, generated.render_document().expose_secret())
            .map_err(|e| format!("write bootstrap.credential: {e}"))?;
        drop(generated);
        let bootstrap_credential = load_bootstrap_credential_file(&bootstrap_path)
            .map_err(|e| format!("load_bootstrap_credential_file: {e}"))?;

        let mut child = Command::new(riffdbd_bin)
            .arg("--database")
            .arg(&database_path)
            .arg("--backup-root")
            .arg(&backup_root)
            .arg("--listen")
            .arg("127.0.0.1:0")
            .arg("--environment")
            .arg(ENVIRONMENT)
            .arg("--audience")
            .arg(AUDIENCE)
            .arg("--capability-keys")
            .arg(&capability_keys_path)
            .arg("--idempotency-keys")
            .arg(&idempotency_keys_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| format!("spawn riffdbd: {e}"))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "missing riffdbd stdout".to_owned())?;
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                match line {
                    Ok(text) => {
                        if let Some(addr) = text.strip_prefix("riffdbd-ready-v1\t") {
                            let _ = tx.send(Ok(addr.trim().to_owned()));
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = tx.send(Err(format!("read riffdbd stdout: {err}")));
                        break;
                    }
                }
            }
        });

        let address = rx
            .recv_timeout(Duration::from_secs(60))
            .map_err(|e| format!("timed out waiting for riffdbd readiness: {e}"))??;

        Ok(Self {
            child,
            endpoint: format!("http://{address}"),
            bootstrap_credential,
        })
    }
}

impl Drop for SpawnedRiffdbd {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Live `riffdbd` benchmark runner over the public gRPC application surface.
#[derive(Clone)]
pub struct RiffdbSaasRunner {
    endpoint: Endpoint,
    transport: StableApplicationClient,
    metadata: CallMetadata,
    attempts: AttemptBudget,
    contract_bundle_hash: [u8; 32],
    query_module_hash: [u8; 32],
    window_start_ms: i64,
    window_end_ms: i64,
}

impl RiffdbSaasRunner {
    /// Bootstraps a live `riffdbd` instance, deploys the `SaasAnalytics` contract and query module,
    /// issues the least-privilege application role token, and seeds the deterministic dataset.
    pub async fn setup_and_seed(
        endpoint_url: &str,
        credential: &RetainedBootstrapCredential,
        dataset: &SeedDataset,
    ) -> Result<(Self, Duration), String> {
        let artifacts = compile_saas_artifacts()?;
        let endpoint = Endpoint::from_shared(endpoint_url.to_owned())
            .map_err(|e| format!("invalid endpoint {endpoint_url}: {e}"))?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .tcp_nodelay(true);

        let mut admin_client = RiffDbClient::connect(endpoint.clone())
            .await
            .map_err(|e| format!("RiffDbClient connect failed: {e}"))?;

        let bootstrap_token = std::str::from_utf8(credential.token().expose_secret())
            .map_err(|e| format!("bootstrap token utf8: {e}"))?;
        let bootstrap_metadata = BootstrapCallMetadata::new(
            TransportBootstrapCredential::new(bootstrap_token).map_err(|e| format!("{e}"))?,
        );
        let admin_auth = CallMetadata::authenticated(
            BearerCredential::new(bootstrap_token).map_err(|e| format!("{e}"))?,
        );

        let created = admin_client
            .create_bootstrap_capability(bootstrap_request(credential)?, &bootstrap_metadata)
            .await
            .map_err(|e| format!("create_bootstrap_capability: {e}"))?;
        let Some(v1::create_capability_response::Result::Bootstrap(boot_res)) = created.result
        else {
            return Err("bootstrap capability response was not Bootstrap".to_owned());
        };
        let Some(v1::bootstrap_create_capability_result::Result::Created(_)) = boot_res.result
        else {
            return Err("bootstrap capability was not newly created".to_owned());
        };

        let deployed_contract = admin_client
            .deploy_contract(
                v1::DeployContractRequest {
                    request_id: fresh_request_id_bytes()?,
                    source: SAAS_CONTRACT_SOURCE.to_owned(),
                    expected_active_version: None,
                    expected_active_bundle_hash: Vec::new(),
                    expected_candidate_bundle_hash: Vec::new(),
                },
                &admin_auth,
            )
            .await
            .map_err(|e| format!("deploy_contract: {e}"))?;
        let Some(v1::deploy_contract_response::Result::Activated(activated_contract)) =
            deployed_contract.result
        else {
            return Err(format!(
                "deploy_contract did not activate: {deployed_contract:?}"
            ));
        };
        let contract_bundle_hash: [u8; 32] =
            activated_contract
                .bundle_hash
                .as_slice()
                .try_into()
                .map_err(|_| "invalid bundle_hash length".to_owned())?;

        let deployed_module = admin_client
            .deploy_query_module(
                app_v1::DeployQueryModuleRequest {
                    contract: Some(app_v1::ContractSelector {
                        lineage: CONTRACT_LINEAGE.to_owned(),
                        version: CONTRACT_VERSION,
                        bundle_hash: Vec::new(),
                    }),
                    module_name: MODULE_NAME.to_owned(),
                    module_version: MODULE_VERSION,
                    queries: SAAS_QUERY_SOURCES
                        .iter()
                        .map(|(name, source)| app_v1::NamedQuerySource {
                            name: (*name).to_owned(),
                            source: (*source).to_owned(),
                        })
                        .collect(),
                    request_id: fresh_request_id_bytes()?,
                    expected_active: Some(
                        app_v1::deploy_query_module_request::ExpectedActive::AnyActive(true),
                    ),
                },
                &admin_auth,
            )
            .await
            .map_err(|e| format!("deploy_query_module: {e}"))?;
        let Some(active_module) = deployed_module.module else {
            return Err(format!(
                "deploy_query_module did not activate: {deployed_module:?}"
            ));
        };
        let query_module_hash: [u8; 32] = active_module
            .module_hash
            .as_slice()
            .try_into()
            .map_err(|_| "invalid module_hash length".to_owned())?;

        let role_cap_resp = admin_client
            .create_capability(
                role_capability_request(artifacts.role.internal_grant())?,
                &admin_auth,
            )
            .await
            .map_err(|e| format!("create_capability: {e}"))?;
        let Some(v1::create_capability_response::Result::Normal(normal_res)) = role_cap_resp.result
        else {
            return Err("role capability response was not Normal".to_owned());
        };
        let Some(v1::normal_create_capability_result::Result::Created(created_cap)) =
            normal_res.result
        else {
            return Err("role capability was not Created".to_owned());
        };

        let transport = StableApplicationClient::connect(endpoint.clone())
            .await
            .map_err(|e| format!("StableApplicationClient connect: {e}"))?;
        let metadata = CallMetadata::authenticated(
            BearerCredential::new(&created_cap.token).map_err(|e| format!("{e}"))?,
        );
        let attempts = AttemptBudget::new(3).ok_or_else(|| "invalid AttemptBudget".to_owned())?;

        let runner = Self {
            endpoint,
            transport,
            metadata,
            attempts,
            contract_bundle_hash,
            query_module_hash,
            window_start_ms: dataset.window_start_ms,
            window_end_ms: dataset.window_end_ms,
        };

        let seed_start = Instant::now();
        runner.seed_all(dataset).await?;
        let seed_elapsed = seed_start.elapsed();

        Ok((runner, seed_elapsed))
    }

    /// Opens an independent HTTP/2 connection for one concurrent worker, matching PostgreSQL's
    /// one-connection-per-worker topology.
    pub async fn fresh_worker_session(&self) -> Result<Self, String> {
        let transport = StableApplicationClient::connect(self.endpoint.clone())
            .await
            .map_err(|e| format!("fresh_worker_session connect: {e}"))?;
        Ok(Self {
            endpoint: self.endpoint.clone(),
            transport,
            metadata: self.metadata.clone(),
            attempts: self.attempts,
            contract_bundle_hash: self.contract_bundle_hash,
            query_module_hash: self.query_module_hash,
            window_start_ms: self.window_start_ms,
            window_end_ms: self.window_end_ms,
        })
    }

    async fn seed_all(&self, dataset: &SeedDataset) -> Result<(), String> {
        let mut commands = Vec::with_capacity(
            dataset.orgs.len()
                + dataset.users.len()
                + dataset.invoices.len()
                + dataset.projects.len()
                + dataset.tasks.len()
                + dataset.signals.len(),
        );

        for org in &dataset.orgs {
            commands.push(
                ApplicationCommand::new(
                    "SeedOrganization",
                    Some(CONTRACT_VERSION),
                    BTreeMap::from([
                        (
                            "idempotency_key".to_owned(),
                            ApplicationValue::String(format!("seed-org-{}", org.org_id)),
                        ),
                        (
                            "org_id".to_owned(),
                            ApplicationValue::String(org.org_id.clone()),
                        ),
                        (
                            "name".to_owned(),
                            ApplicationValue::String(org.name.clone()),
                        ),
                        (
                            "plan_tier".to_owned(),
                            ApplicationValue::String(org.plan_tier.clone()),
                        ),
                        ("created_at".to_owned(), timestamp_value(org.created_at_ms)),
                    ]),
                )
                .map_err(|e| format!("{e}"))?,
            );
        }

        for user in &dataset.users {
            commands.push(
                ApplicationCommand::new(
                    "SeedAppUser",
                    Some(CONTRACT_VERSION),
                    BTreeMap::from([
                        (
                            "idempotency_key".to_owned(),
                            ApplicationValue::String(format!(
                                "seed-user-{}-{}",
                                user.org_id, user.user_id
                            )),
                        ),
                        (
                            "org_id".to_owned(),
                            ApplicationValue::String(user.org_id.clone()),
                        ),
                        (
                            "user_id".to_owned(),
                            ApplicationValue::String(user.user_id.clone()),
                        ),
                        (
                            "email".to_owned(),
                            ApplicationValue::String(user.email.clone()),
                        ),
                        (
                            "display_name".to_owned(),
                            ApplicationValue::String(user.display_name.clone()),
                        ),
                        (
                            "department".to_owned(),
                            ApplicationValue::String(user.department.clone()),
                        ),
                        ("active".to_owned(), ApplicationValue::Bool(user.active)),
                    ]),
                )
                .map_err(|e| format!("{e}"))?,
            );
        }

        for inv in &dataset.invoices {
            commands.push(
                ApplicationCommand::new(
                    "SeedInvoice",
                    Some(CONTRACT_VERSION),
                    BTreeMap::from([
                        (
                            "idempotency_key".to_owned(),
                            ApplicationValue::String(format!(
                                "seed-inv-{}-{}",
                                inv.org_id, inv.invoice_id
                            )),
                        ),
                        (
                            "org_id".to_owned(),
                            ApplicationValue::String(inv.org_id.clone()),
                        ),
                        (
                            "invoice_id".to_owned(),
                            ApplicationValue::String(inv.invoice_id.clone()),
                        ),
                        (
                            "customer_tier".to_owned(),
                            ApplicationValue::String(inv.customer_tier.clone()),
                        ),
                        (
                            "status".to_owned(),
                            ApplicationValue::String(inv.status.clone()),
                        ),
                        ("amount".to_owned(), usd_value(inv.amount_cents)),
                        (
                            "discount".to_owned(),
                            match inv.discount_cents {
                                Some(cents) => usd_value(cents),
                                None => ApplicationValue::Null,
                            },
                        ),
                        (
                            "seats".to_owned(),
                            match inv.seats {
                                Some(s) => ApplicationValue::I64(s),
                                None => ApplicationValue::Null,
                            },
                        ),
                        (
                            "is_enterprise".to_owned(),
                            ApplicationValue::Bool(inv.is_enterprise),
                        ),
                        (
                            "auto_pay".to_owned(),
                            match inv.auto_pay {
                                Some(flag) => ApplicationValue::Bool(flag),
                                None => ApplicationValue::Null,
                            },
                        ),
                        ("issued_at".to_owned(), timestamp_value(inv.issued_at_ms)),
                    ]),
                )
                .map_err(|e| format!("{e}"))?,
            );
        }

        for proj in &dataset.projects {
            commands.push(
                ApplicationCommand::new(
                    "SeedProject",
                    Some(CONTRACT_VERSION),
                    BTreeMap::from([
                        (
                            "idempotency_key".to_owned(),
                            ApplicationValue::String(format!(
                                "seed-proj-{}-{}",
                                proj.org_id, proj.project_id
                            )),
                        ),
                        (
                            "org_id".to_owned(),
                            ApplicationValue::String(proj.org_id.clone()),
                        ),
                        (
                            "project_id".to_owned(),
                            ApplicationValue::String(proj.project_id.clone()),
                        ),
                        (
                            "name".to_owned(),
                            ApplicationValue::String(proj.name.clone()),
                        ),
                        (
                            "tier".to_owned(),
                            ApplicationValue::String(proj.tier.clone()),
                        ),
                        (
                            "status".to_owned(),
                            ApplicationValue::String(proj.status.clone()),
                        ),
                        (
                            "owner_id".to_owned(),
                            ApplicationValue::String(proj.owner_id.clone()),
                        ),
                        ("updated_at".to_owned(), timestamp_value(proj.updated_at_ms)),
                    ]),
                )
                .map_err(|e| format!("{e}"))?,
            );
        }

        for task in &dataset.tasks {
            commands.push(
                ApplicationCommand::new(
                    "SeedTask",
                    Some(CONTRACT_VERSION),
                    BTreeMap::from([
                        (
                            "idempotency_key".to_owned(),
                            ApplicationValue::String(format!(
                                "seed-task-{}-{}",
                                task.org_id, task.task_id
                            )),
                        ),
                        (
                            "org_id".to_owned(),
                            ApplicationValue::String(task.org_id.clone()),
                        ),
                        (
                            "task_id".to_owned(),
                            ApplicationValue::String(task.task_id.clone()),
                        ),
                        (
                            "project_id".to_owned(),
                            ApplicationValue::String(task.project_id.clone()),
                        ),
                        (
                            "assignee_id".to_owned(),
                            ApplicationValue::String(task.assignee_id.clone()),
                        ),
                        (
                            "severity".to_owned(),
                            ApplicationValue::String(task.severity.clone()),
                        ),
                        (
                            "status".to_owned(),
                            ApplicationValue::String(task.status.clone()),
                        ),
                        (
                            "environment".to_owned(),
                            ApplicationValue::String(task.environment.clone()),
                        ),
                        (
                            "title".to_owned(),
                            ApplicationValue::String(task.title.clone()),
                        ),
                        (
                            "priority_rank".to_owned(),
                            ApplicationValue::I64(task.priority_rank),
                        ),
                        (
                            "story_points".to_owned(),
                            ApplicationValue::I64(task.story_points),
                        ),
                        (
                            "duration_ms".to_owned(),
                            ApplicationValue::I64(task.duration_ms),
                        ),
                        (
                            "cycle_hours".to_owned(),
                            ApplicationValue::I64(task.cycle_hours),
                        ),
                        (
                            "is_critical_blocker".to_owned(),
                            ApplicationValue::Bool(task.is_critical_blocker),
                        ),
                        (
                            "completed_at".to_owned(),
                            timestamp_value(task.completed_at_ms),
                        ),
                    ]),
                )
                .map_err(|e| format!("{e}"))?,
            );
        }

        for sig in &dataset.signals {
            commands.push(
                ApplicationCommand::new(
                    "SeedTaskSignal",
                    Some(CONTRACT_VERSION),
                    BTreeMap::from([
                        (
                            "idempotency_key".to_owned(),
                            ApplicationValue::String(format!(
                                "seed-sig-{}-{}-{}",
                                sig.org_id, sig.signal_kind, sig.task_id
                            )),
                        ),
                        (
                            "org_id".to_owned(),
                            ApplicationValue::String(sig.org_id.clone()),
                        ),
                        (
                            "signal_kind".to_owned(),
                            ApplicationValue::String(sig.signal_kind.clone()),
                        ),
                        (
                            "task_id".to_owned(),
                            ApplicationValue::String(sig.task_id.clone()),
                        ),
                        (
                            "recorded_at".to_owned(),
                            timestamp_value(sig.recorded_at_ms),
                        ),
                    ]),
                )
                .map_err(|e| format!("{e}"))?,
            );
        }

        let semaphore = Arc::new(tokio::sync::Semaphore::new(64));
        let mut handles = Vec::with_capacity(commands.len());
        for cmd in commands {
            let sem = Arc::clone(&semaphore);
            let mut transport = self.transport.clone();
            let metadata = self.metadata.clone();
            let attempts = self.attempts;
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.map_err(|e| format!("{e}"))?;
                transport
                    .execute_command(cmd, attempts, &metadata)
                    .await
                    .map_err(|e| format!("seed execute_command: {e}"))?;
                Ok::<(), String>(())
            }));
        }
        for handle in handles {
            handle.await.map_err(|e| format!("seed join: {e}"))??;
        }
        Ok(())
    }

    /// Executes one SaaS workload against `riffdbd` and decodes its canonical output.
    pub async fn execute_workload(
        &self,
        workload: SaasWorkloadKind,
        org_id: &str,
    ) -> Result<CanonicalQueryOutput, String> {
        let parameters = match workload {
            SaasWorkloadKind::BillingKpiConditionalSummary => BTreeMap::from([
                (
                    "org_id".to_owned(),
                    ApplicationValue::String(org_id.to_owned()),
                ),
                (
                    "window_start".to_owned(),
                    timestamp_value(self.window_start_ms),
                ),
                ("window_end".to_owned(), timestamp_value(self.window_end_ms)),
            ]),
            SaasWorkloadKind::SlaQuantilesDispersionAndTags => BTreeMap::from([
                (
                    "org_id".to_owned(),
                    ApplicationValue::String(org_id.to_owned()),
                ),
                (
                    "window_start".to_owned(),
                    timestamp_value(self.window_start_ms),
                ),
                ("window_end".to_owned(), timestamp_value(self.window_end_ms)),
                (
                    "breach_threshold_ms".to_owned(),
                    ApplicationValue::I64(3_600_000),
                ),
            ]),
            SaasWorkloadKind::DailyRevenueCohortTopN => BTreeMap::from([
                (
                    "org_id".to_owned(),
                    ApplicationValue::String(org_id.to_owned()),
                ),
                (
                    "window_start".to_owned(),
                    timestamp_value(self.window_start_ms),
                ),
                ("window_end".to_owned(), timestamp_value(self.window_end_ms)),
                ("min_paid_revenue".to_owned(), usd_value(50_000)),
                ("min_paid_invoices".to_owned(), ApplicationValue::U64(2)),
            ]),
            SaasWorkloadKind::ProjectExpansionAndNestedRollup => BTreeMap::from([
                (
                    "org_id".to_owned(),
                    ApplicationValue::String(org_id.to_owned()),
                ),
                ("since".to_owned(), timestamp_value(self.window_start_ms)),
                (
                    "task_since".to_owned(),
                    timestamp_value(self.window_start_ms),
                ),
            ]),
            SaasWorkloadKind::CandidateIntersectionAndDistinctHydration => BTreeMap::from([(
                "org_id".to_owned(),
                ApplicationValue::String(org_id.to_owned()),
            )]),
        };

        let query = NamedQuery::new(
            ApplicationContract::Exact {
                lineage: CONTRACT_LINEAGE.to_owned(),
                version: CONTRACT_VERSION,
                bundle_hash: Some(self.contract_bundle_hash),
            },
            workload.query_name().to_owned(),
            Some(self.query_module_hash),
            parameters,
            None,
        )
        .map_err(|e| format!("NamedQuery::new {}: {e}", workload.query_name()))?;

        let mut transport = self.transport.clone();
        let response = transport
            .execute_named_query(query, &self.metadata)
            .await
            .map_err(|e| format!("RiffDB query {} failed: {e}", workload.query_name()))?;

        decode_riffdb_result(workload, &response)
    }
}

fn decode_riffdb_result(
    workload: SaasWorkloadKind,
    result: &NamedQueryResult,
) -> Result<CanonicalQueryOutput, String> {
    if result.outcome != "Found" {
        return Err(format!(
            "unexpected outcome {} for {}",
            result.outcome,
            workload.query_name()
        ));
    }
    match workload {
        SaasWorkloadKind::BillingKpiConditionalSummary => {
            let summary_rec = result
                .fields
                .get("summary")
                .and_then(|field| field.records.first())
                .ok_or_else(|| "missing summary record".to_owned())?;
            let summary = &summary_rec.fields;
            Ok(CanonicalQueryOutput::BillingKpiSummary {
                invoice_count: extract_u64(summary, "invoice_count")?,
                paid_count: extract_u64(summary, "paid_count")?,
                overdue_count: extract_u64(summary, "overdue_count")?,
                gross_revenue_cents: extract_money_or_decimal_i128(summary, "gross_revenue")?
                    .unwrap_or(0),
                paid_revenue_cents: extract_money_or_decimal_i128(summary, "paid_revenue")?
                    .unwrap_or(0),
                discount_total_cents: extract_money_or_decimal_i128(summary, "discount_total")?,
                avg_paid_seats: extract_total_and_count(
                    summary,
                    "paid_seats_total",
                    "paid_seats_count",
                )?,
                has_overdue_enterprise: extract_bool(summary, "has_overdue_enterprise")?,
                all_paid_autopay: extract_bool(summary, "all_paid_autopay")?,
            })
        }
        SaasWorkloadKind::DailyRevenueCohortTopN => {
            let records = &result
                .fields
                .get("cohorts")
                .ok_or_else(|| "missing cohorts field".to_owned())?
                .records;
            let mut rows = Vec::with_capacity(records.len());
            for rec in records {
                let f = &rec.fields;
                rows.push(CanonicalRevenueCohortRow {
                    issued_day_ms: extract_timestamp_ms(f, "issued_day")?,
                    customer_tier: extract_string(f, "customer_tier")?,
                    invoice_count: extract_u64(f, "invoice_count")?,
                    paid_invoices: extract_u64(f, "paid_invoices")?,
                    paid_revenue_cents: extract_money_or_decimal_i128(f, "paid_revenue")?
                        .unwrap_or(0),
                    discount_total_cents: extract_money_or_decimal_i128(f, "discount_total")?,
                    avg_seats: extract_total_and_count(f, "seats_total", "seats_count")?,
                });
            }
            Ok(CanonicalQueryOutput::DailyRevenueCohorts(rows))
        }
        SaasWorkloadKind::SlaQuantilesDispersionAndTags => {
            let records = &result
                .fields
                .get("sla_by_severity")
                .ok_or_else(|| "missing sla_by_severity field".to_owned())?
                .records;
            let mut rows = Vec::with_capacity(records.len());
            for rec in records {
                let f = &rec.fields;
                rows.push(CanonicalSlaSeverityRow {
                    severity: extract_string(f, "severity")?,
                    sample_count: extract_u64(f, "sample_count")?,
                    breach_count: extract_u64(f, "breach_count")?,
                    p50_ms: extract_i64(f, "p50_ms")?,
                    p90_ms: extract_i64(f, "p90_ms")?,
                    p95_ms: extract_i64(f, "p95_ms")?,
                    p99_ms: extract_i64(f, "p99_ms")?,
                    stddev_ms_scaled_100: extract_money_or_decimal_i128(f, "stddev_ms")?
                        .unwrap_or(0),
                    latest_title: extract_string(f, "latest_title")?,
                    oldest_title: extract_string(f, "oldest_title")?,
                    environments: {
                        let mut envs = extract_string_list(f, "environments")?;
                        envs.sort();
                        envs
                    },
                });
            }
            Ok(CanonicalQueryOutput::SlaBySeverity(rows))
        }
        SaasWorkloadKind::ProjectExpansionAndNestedRollup => {
            let records = &result
                .fields
                .get("projects")
                .ok_or_else(|| "missing projects field".to_owned())?
                .records;
            let stats_records = &result
                .fields
                .get("task_stats")
                .ok_or_else(|| "missing task_stats field".to_owned())?
                .records;
            let mut stats_by_project = BTreeMap::new();
            for srec in stats_records {
                let sf = &srec.fields;
                let pid = extract_string(sf, "project_id")?;
                stats_by_project.insert(pid, sf);
            }
            let mut rows = Vec::with_capacity(records.len());
            for rec in records {
                let f = &rec.fields;
                let project_id = extract_string(f, "project_id")?;
                let child_tasks = match f.get("tasks") {
                    Some(ApplicationValue::List(items)) => items,
                    other => return Err(format!("expected List for project.tasks, got {other:?}")),
                };
                let mut child_task_ids = Vec::with_capacity(child_tasks.len());
                for item in child_tasks {
                    match item {
                        ApplicationValue::Record(task_fields) => {
                            child_task_ids.push(extract_string(task_fields, "task_id")?);
                        }
                        other => {
                            return Err(format!("expected Record in child tasks, got {other:?}"));
                        }
                    }
                }
                let stats = stats_by_project
                    .get(&project_id)
                    .copied()
                    .ok_or_else(|| format!("missing task_stats for project {project_id}"))?;
                rows.push(CanonicalProjectPortfolioRow {
                    project_id,
                    name: extract_string(f, "name")?,
                    tier: extract_string(f, "tier")?,
                    status: extract_string(f, "status")?,
                    child_task_ids,
                    total_tasks: extract_u64(stats, "total_tasks")?,
                    completed_tasks: extract_u64(stats, "completed_tasks")?,
                    blocked_tasks: extract_u64(stats, "blocked_tasks")?,
                    total_points: extract_money_or_decimal_i128(stats, "total_points")?
                        .unwrap_or(0),
                    completed_points: extract_money_or_decimal_i128(stats, "completed_points")?
                        .unwrap_or(0),
                    p95_cycle_hours: extract_i64(stats, "p95_cycle_hours")?,
                    has_critical_blocker: extract_bool(stats, "has_critical_blocker")?,
                    assignees: {
                        let mut list = extract_string_list(stats, "assignees")?;
                        list.sort();
                        list
                    },
                });
            }
            Ok(CanonicalQueryOutput::ProjectPortfolio(rows))
        }
        SaasWorkloadKind::CandidateIntersectionAndDistinctHydration => {
            let task_records = &result
                .fields
                .get("tasks")
                .ok_or_else(|| "missing tasks field".to_owned())?
                .records;
            let assignee_records = &result
                .fields
                .get("assignees")
                .ok_or_else(|| "missing assignees field".to_owned())?
                .records;
            let mut tasks = Vec::with_capacity(task_records.len());
            for rec in task_records {
                let f = &rec.fields;
                tasks.push(CanonicalHydratedTaskRow {
                    task_id: extract_string(f, "task_id")?,
                    project_id: extract_string(f, "project_id")?,
                    assignee_id: extract_string(f, "assignee_id")?,
                    severity: extract_string(f, "severity")?,
                    status: extract_string(f, "status")?,
                    priority_rank: extract_i64(f, "priority_rank")?,
                    title: extract_string(f, "title")?,
                });
            }
            let mut assignees = Vec::with_capacity(assignee_records.len());
            for rec in assignee_records {
                let f = &rec.fields;
                assignees.push(CanonicalHydratedAssigneeRow {
                    user_id: extract_string(f, "user_id")?,
                    email: extract_string(f, "email")?,
                    display_name: extract_string(f, "display_name")?,
                    department: extract_string(f, "department")?,
                });
            }
            Ok(CanonicalQueryOutput::CandidateSignalHydration { tasks, assignees })
        }
    }
}

fn extract_u64(map: &BTreeMap<String, ApplicationValue>, key: &str) -> Result<u64, String> {
    match map.get(key) {
        Some(ApplicationValue::U64(v)) => Ok(*v),
        other => Err(format!("field {key}: expected U64, got {other:?}")),
    }
}

fn extract_i64(map: &BTreeMap<String, ApplicationValue>, key: &str) -> Result<i64, String> {
    match map.get(key) {
        Some(ApplicationValue::I64(v)) => Ok(*v),
        other => Err(format!("field {key}: expected I64, got {other:?}")),
    }
}

fn extract_bool(map: &BTreeMap<String, ApplicationValue>, key: &str) -> Result<bool, String> {
    match map.get(key) {
        Some(ApplicationValue::Bool(v)) => Ok(*v),
        other => Err(format!("field {key}: expected Bool, got {other:?}")),
    }
}

fn extract_string(map: &BTreeMap<String, ApplicationValue>, key: &str) -> Result<String, String> {
    match map.get(key) {
        Some(ApplicationValue::String(v)) => Ok(v.clone()),
        other => Err(format!("field {key}: expected String, got {other:?}")),
    }
}

fn extract_timestamp_ms(
    map: &BTreeMap<String, ApplicationValue>,
    key: &str,
) -> Result<i64, String> {
    match map.get(key) {
        Some(ApplicationValue::Timestamp { seconds, nanos }) => {
            Ok(seconds * 1_000 + i64::from(*nanos / 1_000_000))
        }
        other => Err(format!("field {key}: expected Timestamp, got {other:?}")),
    }
}

fn extract_money_or_decimal_i128(
    map: &BTreeMap<String, ApplicationValue>,
    key: &str,
) -> Result<Option<i128>, String> {
    match map.get(key) {
        Some(ApplicationValue::Null) => Ok(None),
        Some(ApplicationValue::Decimal {
            coefficient_twos_complement,
            ..
        }) => Ok(Some(decode_twos_complement_i128(
            coefficient_twos_complement,
        )?)),
        Some(ApplicationValue::Money { amount, .. }) => match amount.as_ref() {
            ApplicationValue::Decimal {
                coefficient_twos_complement,
                ..
            } => Ok(Some(decode_twos_complement_i128(
                coefficient_twos_complement,
            )?)),
            other => Err(format!(
                "field {key}: expected Decimal inside Money, got {other:?}"
            )),
        },
        Some(ApplicationValue::I64(v)) => Ok(Some(i128::from(*v))),
        other => Err(format!(
            "field {key}: expected Money, Decimal, I64, or Null, got {other:?}"
        )),
    }
}

fn extract_total_and_count(
    map: &BTreeMap<String, ApplicationValue>,
    total_key: &str,
    count_key: &str,
) -> Result<Option<(i128, u64)>, String> {
    let count = extract_u64(map, count_key)?;
    match extract_money_or_decimal_i128(map, total_key)? {
        Some(total) if count > 0 => Ok(Some((total, count))),
        _ => Ok(None),
    }
}

fn extract_string_list(
    map: &BTreeMap<String, ApplicationValue>,
    key: &str,
) -> Result<Vec<String>, String> {
    match map.get(key) {
        Some(ApplicationValue::List(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    ApplicationValue::String(s) => out.push(s.clone()),
                    other => {
                        return Err(format!("field {key}: expected String item, got {other:?}"));
                    }
                }
            }
            Ok(out)
        }
        other => Err(format!("field {key}: expected List, got {other:?}")),
    }
}

/// PostgreSQL schema DDL for the SaaS analytics benchmark.
pub const POSTGRES_SCHEMA_SQL: &str = r#"
DROP TABLE IF EXISTS saas_task_signal CASCADE;
DROP TABLE IF EXISTS saas_task CASCADE;
DROP TABLE IF EXISTS saas_project CASCADE;
DROP TABLE IF EXISTS saas_invoice CASCADE;
DROP TABLE IF EXISTS saas_user CASCADE;
DROP TABLE IF EXISTS saas_organization CASCADE;
DROP TABLE IF EXISTS saas_app_permission CASCADE;

CREATE TABLE saas_app_permission (
    principal TEXT NOT NULL,
    operation TEXT NOT NULL,
    PRIMARY KEY (principal, operation)
);

CREATE TABLE saas_organization (
    org_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    plan_tier TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL
);

CREATE TABLE saas_user (
    org_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    email TEXT NOT NULL,
    display_name TEXT NOT NULL,
    department TEXT NOT NULL,
    active BOOLEAN NOT NULL,
    PRIMARY KEY (org_id, user_id)
);

CREATE TABLE saas_invoice (
    org_id TEXT NOT NULL,
    invoice_id TEXT NOT NULL,
    customer_tier TEXT NOT NULL,
    status TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    discount_cents BIGINT NULL,
    seats BIGINT NULL,
    is_enterprise BOOLEAN NOT NULL,
    auto_pay BOOLEAN NULL,
    issued_at_ms BIGINT NOT NULL,
    PRIMARY KEY (org_id, invoice_id)
);

CREATE INDEX saas_invoice_by_org_issued_covering
    ON saas_invoice (org_id, issued_at_ms DESC, invoice_id DESC)
    INCLUDE (amount_cents, discount_cents, seats, status, is_enterprise, auto_pay, customer_tier);

CREATE TABLE saas_project (
    org_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    tier TEXT NOT NULL,
    status TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (org_id, project_id)
);

CREATE INDEX saas_project_by_org_updated
    ON saas_project (org_id, updated_at_ms DESC, project_id DESC)
    INCLUDE (name, tier, status, owner_id);

CREATE TABLE saas_task (
    org_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    assignee_id TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL,
    environment TEXT NOT NULL,
    title TEXT NOT NULL,
    priority_rank BIGINT NOT NULL,
    story_points BIGINT NOT NULL,
    duration_ms BIGINT NOT NULL,
    cycle_hours BIGINT NOT NULL,
    is_critical_blocker BOOLEAN NOT NULL,
    completed_at_ms BIGINT NOT NULL,
    PRIMARY KEY (org_id, task_id)
);

CREATE INDEX saas_task_by_org_completed
    ON saas_task (org_id, completed_at_ms DESC, task_id DESC)
    INCLUDE (project_id, assignee_id, severity, status, environment, title, story_points, duration_ms, cycle_hours, is_critical_blocker);

CREATE INDEX saas_task_by_org_project_completed
    ON saas_task (org_id, project_id, completed_at_ms DESC, task_id DESC)
    INCLUDE (assignee_id, severity, status, story_points, cycle_hours, is_critical_blocker);

CREATE INDEX saas_task_by_org_priority
    ON saas_task (org_id, priority_rank ASC, task_id ASC)
    INCLUDE (project_id, assignee_id, severity, status, title);

CREATE TABLE saas_task_signal (
    org_id TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    task_id TEXT NOT NULL,
    recorded_at_ms BIGINT NOT NULL,
    PRIMARY KEY (org_id, signal_kind, task_id)
);
"#;

const PG_BILLING_SUMMARY_SQL: &str = r#"
SELECT
    COUNT(*)::BIGINT AS invoice_count,
    COUNT(*) FILTER (WHERE status = 'paid')::BIGINT AS paid_count,
    COUNT(*) FILTER (WHERE status = 'overdue')::BIGINT AS overdue_count,
    COALESCE(SUM(amount_cents), 0)::BIGINT AS gross_revenue_cents,
    COALESCE(SUM(amount_cents) FILTER (WHERE status = 'paid'), 0)::BIGINT AS paid_revenue_cents,
    COALESCE(SUM(discount_cents) FILTER (WHERE status = 'paid'), 0)::BIGINT AS discount_total_cents,
    (SUM(seats) FILTER (WHERE status = 'paid' AND seats IS NOT NULL))::BIGINT AS paid_seats_total,
    (COUNT(seats) FILTER (WHERE status = 'paid'))::BIGINT AS paid_seats_count,
    COALESCE(BOOL_OR(is_enterprise) FILTER (WHERE status = 'overdue'), FALSE) AS has_overdue_enterprise,
    COALESCE(BOOL_AND(auto_pay) FILTER (WHERE status = 'paid'), TRUE) AS all_paid_autopay
FROM (
    SELECT amount_cents, discount_cents, seats, status, is_enterprise, auto_pay
    FROM saas_invoice
    WHERE org_id = $1
      AND issued_at_ms >= $2
      AND issued_at_ms <= $3
    ORDER BY issued_at_ms DESC, invoice_id DESC
    LIMIT 240
) scanned
"#;

const PG_DAILY_COHORT_TOP_N_SQL: &str = r#"
SELECT
    ((issued_at_ms / 86400000) * 86400000)::BIGINT AS issued_day_ms,
    customer_tier,
    COUNT(*)::BIGINT AS invoice_count,
    COUNT(*) FILTER (WHERE status = 'paid')::BIGINT AS paid_invoices,
    COALESCE(SUM(amount_cents) FILTER (WHERE status = 'paid'), 0)::BIGINT AS paid_revenue_cents,
    COALESCE(SUM(discount_cents) FILTER (WHERE status = 'paid'), 0)::BIGINT AS discount_total_cents,
    (SUM(seats) FILTER (WHERE status = 'paid' AND seats IS NOT NULL))::BIGINT AS seats_total,
    (COUNT(seats) FILTER (WHERE status = 'paid'))::BIGINT AS seats_count
FROM (
    SELECT issued_at_ms, customer_tier, status, amount_cents, discount_cents, seats
    FROM saas_invoice
    WHERE org_id = $1
      AND issued_at_ms >= $2
      AND issued_at_ms <= $3
    ORDER BY issued_at_ms DESC, invoice_id DESC
    LIMIT 240
) scanned
GROUP BY 1, 2
HAVING COALESCE(SUM(amount_cents) FILTER (WHERE status = 'paid'), 0)::BIGINT >= $4::BIGINT
   AND (COUNT(*) FILTER (WHERE status = 'paid'))::BIGINT >= $5::BIGINT
ORDER BY paid_revenue_cents DESC, issued_day_ms DESC, customer_tier ASC
LIMIT 10
"#;

const PG_SLA_QUANTILES_SQL: &str = r#"
SELECT
    severity,
    COUNT(*)::BIGINT AS sample_count,
    COUNT(*) FILTER (WHERE duration_ms >= 3600000)::BIGINT AS breach_count,
    (PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY duration_ms ASC))::BIGINT AS p50_ms,
    (PERCENTILE_DISC(0.90) WITHIN GROUP (ORDER BY duration_ms ASC))::BIGINT AS p90_ms,
    (PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY duration_ms ASC))::BIGINT AS p95_ms,
    (PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY duration_ms ASC))::BIGINT AS p99_ms,
    (ARRAY_AGG(duration_ms ORDER BY completed_at_ms DESC, task_id DESC)) AS durations_ms,
    (ARRAY_AGG(title ORDER BY completed_at_ms DESC, task_id DESC))[1] AS latest_title,
    (ARRAY_AGG(title ORDER BY completed_at_ms ASC, task_id ASC))[1] AS oldest_title,
    ARRAY_AGG(DISTINCT environment ORDER BY environment ASC) AS environments
FROM (
    SELECT severity, duration_ms, title, environment, completed_at_ms, task_id
    FROM saas_task
    WHERE org_id = $1
      AND completed_at_ms >= $2
      AND completed_at_ms <= $3
    ORDER BY completed_at_ms DESC, task_id DESC
    LIMIT 240
) scanned
GROUP BY severity
ORDER BY p99_ms DESC, severity ASC
LIMIT 8
"#;

const PG_PROJECT_PORTFOLIO_SQL: &str = r#"
SELECT
    p.project_id,
    p.name,
    p.tier,
    p.status,
    COALESCE(t.child_task_ids, ARRAY[]::TEXT[]) AS child_task_ids,
    COALESCE(t.total_tasks, 0)::BIGINT AS total_tasks,
    COALESCE(t.completed_tasks, 0)::BIGINT AS completed_tasks,
    COALESCE(t.blocked_tasks, 0)::BIGINT AS blocked_tasks,
    COALESCE(t.total_points, 0)::BIGINT AS total_points,
    COALESCE(t.completed_points, 0)::BIGINT AS completed_points,
    COALESCE(t.p95_cycle_hours, 0)::BIGINT AS p95_cycle_hours,
    COALESCE(t.has_critical_blocker, FALSE) AS has_critical_blocker,
    COALESCE(t.assignees, ARRAY[]::TEXT[]) AS assignees
FROM (
    SELECT org_id, project_id, name, tier, status, updated_at_ms
    FROM saas_project
    WHERE org_id = $1
      AND updated_at_ms >= $2
    ORDER BY updated_at_ms DESC, project_id DESC
    LIMIT 12
) p
LEFT JOIN LATERAL (
    SELECT
        ARRAY_AGG(sub.task_id ORDER BY sub.completed_at_ms DESC, sub.task_id DESC) AS child_task_ids,
        COUNT(*)::BIGINT AS total_tasks,
        COUNT(*) FILTER (WHERE sub.status = 'done')::BIGINT AS completed_tasks,
        COUNT(*) FILTER (WHERE sub.status = 'blocked')::BIGINT AS blocked_tasks,
        COALESCE(SUM(sub.story_points), 0)::BIGINT AS total_points,
        COALESCE(SUM(sub.story_points) FILTER (WHERE sub.status = 'done'), 0)::BIGINT AS completed_points,
        (PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY sub.cycle_hours ASC))::BIGINT AS p95_cycle_hours,
        COALESCE(BOOL_OR(sub.is_critical_blocker), FALSE) AS has_critical_blocker,
        ARRAY_AGG(DISTINCT sub.assignee_id ORDER BY sub.assignee_id ASC) AS assignees
    FROM (
        SELECT task_id, assignee_id, status, story_points, cycle_hours, is_critical_blocker, completed_at_ms
        FROM saas_task
        WHERE org_id = $1
          AND project_id = p.project_id
          AND completed_at_ms >= $3
        ORDER BY completed_at_ms DESC, task_id DESC
        LIMIT 32
    ) sub
) t ON TRUE
ORDER BY p.updated_at_ms DESC, p.project_id DESC
"#;

const PG_CANDIDATE_TASKS_SQL: &str = r#"
WITH matched AS (
    SELECT task_id FROM (
        SELECT task_id
        FROM saas_task_signal
        WHERE org_id = $1 AND signal_kind = 'sla-breach'
        ORDER BY task_id ASC
        LIMIT 128
    ) s1
    INTERSECT
    SELECT task_id FROM (
        SELECT task_id
        FROM saas_task_signal
        WHERE org_id = $1 AND signal_kind = 'customer-escalation'
        ORDER BY task_id ASC
        LIMIT 128
    ) s2
    ORDER BY task_id ASC
    LIMIT 64
)
SELECT t.task_id, t.project_id, t.assignee_id, t.severity, t.status, t.priority_rank, t.title
FROM saas_task t
WHERE t.org_id = $1
  AND t.task_id IN (SELECT task_id FROM matched)
ORDER BY t.priority_rank ASC, t.task_id ASC
LIMIT 16
"#;

const PG_HYDRATE_ASSIGNEES_SQL: &str = r#"
SELECT user_id, email, display_name, department
FROM saas_user
WHERE org_id = $1
  AND user_id = ANY($2)
ORDER BY user_id ASC
LIMIT 16
"#;

/// Seeds the PostgreSQL schema and dataset, then runs `VACUUM (ANALYZE)` and `CHECKPOINT`
/// so statistics and covering index visibility maps are warm before timing starts.
pub fn seed_postgres(postgres_url: &str, dataset: &SeedDataset) -> Result<Duration, String> {
    let mut client = Client::connect(postgres_url, NoTls)
        .map_err(|e| format!("postgres connect failed: {e}"))?;
    client
        .batch_execute(POSTGRES_SCHEMA_SQL)
        .map_err(|e| format!("postgres schema DDL failed: {e}"))?;

    let start = Instant::now();
    {
        let mut tx = client
            .transaction()
            .map_err(|e| format!("postgres begin tx: {e}"))?;
        let ins_perm = tx
            .prepare("INSERT INTO saas_app_permission (principal, operation) VALUES ($1, $2)")
            .map_err(|e| format!("{e}"))?;
        for workload in SaasWorkloadKind::ALL {
            tx.execute(&ins_perm, &[&"saas-bench-app", &workload.as_str()])
                .map_err(|e| format!("{e}"))?;
        }

        let ins_org = tx
            .prepare(
                "INSERT INTO saas_organization (org_id, name, plan_tier, created_at_ms) VALUES ($1, $2, $3, $4)",
            )
            .map_err(|e| format!("{e}"))?;
        for org in &dataset.orgs {
            tx.execute(
                &ins_org,
                &[&org.org_id, &org.name, &org.plan_tier, &org.created_at_ms],
            )
            .map_err(|e| format!("{e}"))?;
        }

        let ins_user = tx
            .prepare(
                "INSERT INTO saas_user (org_id, user_id, email, display_name, department, active) VALUES ($1, $2, $3, $4, $5, $6)",
            )
            .map_err(|e| format!("{e}"))?;
        for user in &dataset.users {
            tx.execute(
                &ins_user,
                &[
                    &user.org_id,
                    &user.user_id,
                    &user.email,
                    &user.display_name,
                    &user.department,
                    &user.active,
                ],
            )
            .map_err(|e| format!("{e}"))?;
        }

        let ins_inv = tx
            .prepare(
                "INSERT INTO saas_invoice (org_id, invoice_id, customer_tier, status, amount_cents, discount_cents, seats, is_enterprise, auto_pay, issued_at_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            )
            .map_err(|e| format!("{e}"))?;
        for inv in &dataset.invoices {
            let amount_i64 = i64::try_from(inv.amount_cents).map_err(|e| format!("{e}"))?;
            let discount_i64 = inv
                .discount_cents
                .map(i64::try_from)
                .transpose()
                .map_err(|e| format!("{e}"))?;
            tx.execute(
                &ins_inv,
                &[
                    &inv.org_id,
                    &inv.invoice_id,
                    &inv.customer_tier,
                    &inv.status,
                    &amount_i64,
                    &discount_i64,
                    &inv.seats,
                    &inv.is_enterprise,
                    &inv.auto_pay,
                    &inv.issued_at_ms,
                ],
            )
            .map_err(|e| format!("{e}"))?;
        }

        let ins_proj = tx
            .prepare(
                "INSERT INTO saas_project (org_id, project_id, name, tier, status, owner_id, updated_at_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .map_err(|e| format!("{e}"))?;
        for proj in &dataset.projects {
            tx.execute(
                &ins_proj,
                &[
                    &proj.org_id,
                    &proj.project_id,
                    &proj.name,
                    &proj.tier,
                    &proj.status,
                    &proj.owner_id,
                    &proj.updated_at_ms,
                ],
            )
            .map_err(|e| format!("{e}"))?;
        }

        let ins_task = tx
            .prepare(
                "INSERT INTO saas_task (org_id, task_id, project_id, assignee_id, severity, status, environment, title, priority_rank, story_points, duration_ms, cycle_hours, is_critical_blocker, completed_at_ms) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
            )
            .map_err(|e| format!("{e}"))?;
        for task in &dataset.tasks {
            tx.execute(
                &ins_task,
                &[
                    &task.org_id,
                    &task.task_id,
                    &task.project_id,
                    &task.assignee_id,
                    &task.severity,
                    &task.status,
                    &task.environment,
                    &task.title,
                    &task.priority_rank,
                    &task.story_points,
                    &task.duration_ms,
                    &task.cycle_hours,
                    &task.is_critical_blocker,
                    &task.completed_at_ms,
                ],
            )
            .map_err(|e| format!("{e}"))?;
        }

        let ins_sig = tx
            .prepare(
                "INSERT INTO saas_task_signal (org_id, signal_kind, task_id, recorded_at_ms) VALUES ($1, $2, $3, $4)",
            )
            .map_err(|e| format!("{e}"))?;
        for sig in &dataset.signals {
            tx.execute(
                &ins_sig,
                &[
                    &sig.org_id,
                    &sig.signal_kind,
                    &sig.task_id,
                    &sig.recorded_at_ms,
                ],
            )
            .map_err(|e| format!("{e}"))?;
        }

        tx.commit()
            .map_err(|e| format!("postgres commit tx: {e}"))?;
    }
    let elapsed = start.elapsed();

    client
        .batch_execute("VACUUM (ANALYZE)")
        .map_err(|e| format!("postgres VACUUM ANALYZE failed: {e}"))?;
    client
        .batch_execute("CHECKPOINT")
        .map_err(|e| format!("postgres CHECKPOINT failed: {e}"))?;

    Ok(elapsed)
}

/// One synchronous PostgreSQL connection with pre-prepared statements for all SaaS workloads.
pub struct PostgresSaasWorker {
    client: Client,
    profile: PostgresProfile,
    window_start_ms: i64,
    window_end_ms: i64,
    stmt_permission: Statement,
    stmt_billing_summary: Statement,
    stmt_daily_cohorts: Statement,
    stmt_sla_quantiles: Statement,
    stmt_project_portfolio: Statement,
    stmt_candidate_tasks: Statement,
    stmt_hydrate_assignees: Statement,
}

impl PostgresSaasWorker {
    /// Connects to PostgreSQL and prepares all statements for `profile`.
    pub fn connect(
        postgres_url: &str,
        profile: PostgresProfile,
        window_start_ms: i64,
        window_end_ms: i64,
    ) -> Result<Self, String> {
        let mut client = Client::connect(postgres_url, NoTls)
            .map_err(|e| format!("postgres worker connect: {e}"))?;
        let stmt_permission = client
            .prepare("SELECT 1 FROM saas_app_permission WHERE principal = $1 AND operation = $2")
            .map_err(|e| format!("{e}"))?;
        let stmt_billing_summary = client
            .prepare(PG_BILLING_SUMMARY_SQL)
            .map_err(|e| format!("prepare billing summary: {e}"))?;
        let stmt_daily_cohorts = client
            .prepare(PG_DAILY_COHORT_TOP_N_SQL)
            .map_err(|e| format!("prepare daily cohorts: {e}"))?;
        let stmt_sla_quantiles = client
            .prepare(PG_SLA_QUANTILES_SQL)
            .map_err(|e| format!("prepare sla quantiles: {e}"))?;
        let stmt_project_portfolio = client
            .prepare(PG_PROJECT_PORTFOLIO_SQL)
            .map_err(|e| format!("prepare project portfolio: {e}"))?;
        let stmt_candidate_tasks = client
            .prepare(PG_CANDIDATE_TASKS_SQL)
            .map_err(|e| format!("prepare candidate tasks: {e}"))?;
        let stmt_hydrate_assignees = client
            .prepare(PG_HYDRATE_ASSIGNEES_SQL)
            .map_err(|e| format!("prepare hydrate assignees: {e}"))?;

        Ok(Self {
            client,
            profile,
            window_start_ms,
            window_end_ms,
            stmt_permission,
            stmt_billing_summary,
            stmt_daily_cohorts,
            stmt_sla_quantiles,
            stmt_project_portfolio,
            stmt_candidate_tasks,
            stmt_hydrate_assignees,
        })
    }

    fn enforce_permission(&mut self, workload: SaasWorkloadKind) -> Result<(), String> {
        if self.profile == PostgresProfile::SafeApp {
            let rows = self
                .client
                .query(
                    &self.stmt_permission,
                    &[&"saas-bench-app", &workload.as_str()],
                )
                .map_err(|e| format!("permission query failed: {e}"))?;
            if rows.is_empty() {
                return Err(format!("permission denied for {}", workload.as_str()));
            }
        }
        Ok(())
    }

    /// Executes one SaaS workload against PostgreSQL and decodes its canonical output.
    pub fn execute_workload(
        &mut self,
        workload: SaasWorkloadKind,
        org_id: &str,
    ) -> Result<CanonicalQueryOutput, String> {
        self.enforce_permission(workload)?;

        match workload {
            SaasWorkloadKind::BillingKpiConditionalSummary => {
                let row = self
                    .client
                    .query_one(
                        &self.stmt_billing_summary,
                        &[&org_id, &self.window_start_ms, &self.window_end_ms],
                    )
                    .map_err(|e| format!("pg billing summary failed: {e}"))?;
                let invoice_count: i64 = row.get("invoice_count");
                let paid_count: i64 = row.get("paid_count");
                let overdue_count: i64 = row.get("overdue_count");
                let gross_revenue_cents: i64 = row.get("gross_revenue_cents");
                let paid_revenue_cents: i64 = row.get("paid_revenue_cents");
                let discount_total_cents: Option<i64> = row.get("discount_total_cents");
                let paid_seats_total: Option<i64> = row.get("paid_seats_total");
                let paid_seats_count: i64 = row.get("paid_seats_count");
                let has_overdue_enterprise: bool = row.get("has_overdue_enterprise");
                let all_paid_autopay: bool = row.get("all_paid_autopay");

                let avg_paid_seats = match paid_seats_total {
                    Some(total) if paid_seats_count > 0 => {
                        Some((i128::from(total), paid_seats_count as u64))
                    }
                    _ => None,
                };

                Ok(CanonicalQueryOutput::BillingKpiSummary {
                    invoice_count: invoice_count as u64,
                    paid_count: paid_count as u64,
                    overdue_count: overdue_count as u64,
                    gross_revenue_cents: i128::from(gross_revenue_cents),
                    paid_revenue_cents: i128::from(paid_revenue_cents),
                    discount_total_cents: discount_total_cents.map(i128::from),
                    avg_paid_seats,
                    has_overdue_enterprise,
                    all_paid_autopay,
                })
            }
            SaasWorkloadKind::DailyRevenueCohortTopN => {
                let min_paid_revenue_cents = 50_000_i64;
                let min_paid_invoices = 2_i64;
                let rows = self
                    .client
                    .query(
                        &self.stmt_daily_cohorts,
                        &[
                            &org_id,
                            &self.window_start_ms,
                            &self.window_end_ms,
                            &min_paid_revenue_cents,
                            &min_paid_invoices,
                        ],
                    )
                    .map_err(|e| format!("pg daily cohorts failed: {e}"))?;

                let mut out = Vec::with_capacity(rows.len());
                for row in rows {
                    let issued_day_ms: i64 = row.get("issued_day_ms");
                    let customer_tier: String = row.get("customer_tier");
                    let invoice_count: i64 = row.get("invoice_count");
                    let paid_invoices: i64 = row.get("paid_invoices");
                    let paid_revenue_cents: i64 = row.get("paid_revenue_cents");
                    let discount_total_cents: Option<i64> = row.get("discount_total_cents");
                    let seats_total: Option<i64> = row.get("seats_total");
                    let seats_count: i64 = row.get("seats_count");

                    let avg_seats = match seats_total {
                        Some(total) if seats_count > 0 => {
                            Some((i128::from(total), seats_count as u64))
                        }
                        _ => None,
                    };

                    out.push(CanonicalRevenueCohortRow {
                        issued_day_ms,
                        customer_tier,
                        invoice_count: invoice_count as u64,
                        paid_invoices: paid_invoices as u64,
                        paid_revenue_cents: i128::from(paid_revenue_cents),
                        discount_total_cents: discount_total_cents.map(i128::from),
                        avg_seats,
                    });
                }
                Ok(CanonicalQueryOutput::DailyRevenueCohorts(out))
            }
            SaasWorkloadKind::SlaQuantilesDispersionAndTags => {
                let rows = self
                    .client
                    .query(
                        &self.stmt_sla_quantiles,
                        &[&org_id, &self.window_start_ms, &self.window_end_ms],
                    )
                    .map_err(|e| format!("pg sla quantiles failed: {e}"))?;
                let mut out = Vec::with_capacity(rows.len());
                for row in rows {
                    let severity: String = row.get("severity");
                    let sample_count: i64 = row.get("sample_count");
                    let breach_count: i64 = row.get("breach_count");
                    let p50_ms: i64 = row.get("p50_ms");
                    let p90_ms: i64 = row.get("p90_ms");
                    let p95_ms: i64 = row.get("p95_ms");
                    let p99_ms: i64 = row.get("p99_ms");
                    let durations_ms: Vec<i64> = row.get("durations_ms");
                    let latest_title: String = row.get("latest_title");
                    let oldest_title: String = row.get("oldest_title");
                    let environments: Vec<String> = row.get("environments");

                    let stddev_ms_scaled_100 = exact_stddev_pop_scaled_100(&durations_ms)?;

                    out.push(CanonicalSlaSeverityRow {
                        severity,
                        sample_count: sample_count as u64,
                        breach_count: breach_count as u64,
                        p50_ms,
                        p90_ms,
                        p95_ms,
                        p99_ms,
                        stddev_ms_scaled_100,
                        latest_title,
                        oldest_title,
                        environments,
                    });
                }
                Ok(CanonicalQueryOutput::SlaBySeverity(out))
            }
            SaasWorkloadKind::ProjectExpansionAndNestedRollup => {
                if self.profile == PostgresProfile::SafeApp {
                    self.client
                        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                        .map_err(|e| format!("{e}"))?;
                }
                let rows_res = self.client.query(
                    &self.stmt_project_portfolio,
                    &[&org_id, &self.window_start_ms, &self.window_start_ms],
                );
                if self.profile == PostgresProfile::SafeApp {
                    let _ = self.client.batch_execute("COMMIT");
                }
                let rows = rows_res.map_err(|e| format!("pg project portfolio failed: {e}"))?;
                let mut out = Vec::with_capacity(rows.len());
                for row in rows {
                    let total_points: i64 = row.get("total_points");
                    let completed_points: i64 = row.get("completed_points");
                    let total_tasks: i64 = row.get("total_tasks");
                    let completed_tasks: i64 = row.get("completed_tasks");
                    let blocked_tasks: i64 = row.get("blocked_tasks");
                    out.push(CanonicalProjectPortfolioRow {
                        project_id: row.get("project_id"),
                        name: row.get("name"),
                        tier: row.get("tier"),
                        status: row.get("status"),
                        child_task_ids: row.get("child_task_ids"),
                        total_tasks: total_tasks as u64,
                        completed_tasks: completed_tasks as u64,
                        blocked_tasks: blocked_tasks as u64,
                        total_points: i128::from(total_points),
                        completed_points: i128::from(completed_points),
                        p95_cycle_hours: row.get("p95_cycle_hours"),
                        has_critical_blocker: row.get("has_critical_blocker"),
                        assignees: row.get("assignees"),
                    });
                }
                Ok(CanonicalQueryOutput::ProjectPortfolio(out))
            }
            SaasWorkloadKind::CandidateIntersectionAndDistinctHydration => {
                if self.profile == PostgresProfile::SafeApp {
                    self.client
                        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                        .map_err(|e| format!("{e}"))?;
                }
                let step = (|| -> Result<CanonicalQueryOutput, String> {
                    let task_rows = self
                        .client
                        .query(&self.stmt_candidate_tasks, &[&org_id])
                        .map_err(|e| format!("pg candidate tasks failed: {e}"))?;
                    let mut tasks = Vec::with_capacity(task_rows.len());
                    let mut distinct_assignee_ids = BTreeSet::new();
                    for row in task_rows {
                        let assignee_id: String = row.get("assignee_id");
                        distinct_assignee_ids.insert(assignee_id.clone());
                        tasks.push(CanonicalHydratedTaskRow {
                            task_id: row.get("task_id"),
                            project_id: row.get("project_id"),
                            assignee_id,
                            severity: row.get("severity"),
                            status: row.get("status"),
                            priority_rank: row.get("priority_rank"),
                            title: row.get("title"),
                        });
                    }
                    let assignee_vec: Vec<String> = distinct_assignee_ids.into_iter().collect();
                    let assignee_rows = self
                        .client
                        .query(&self.stmt_hydrate_assignees, &[&org_id, &assignee_vec])
                        .map_err(|e| format!("pg hydrate assignees failed: {e}"))?;
                    let mut assignees = Vec::with_capacity(assignee_rows.len());
                    for row in assignee_rows {
                        assignees.push(CanonicalHydratedAssigneeRow {
                            user_id: row.get("user_id"),
                            email: row.get("email"),
                            display_name: row.get("display_name"),
                            department: row.get("department"),
                        });
                    }
                    Ok(CanonicalQueryOutput::CandidateSignalHydration { tasks, assignees })
                })();
                if self.profile == PostgresProfile::SafeApp {
                    let _ = self.client.batch_execute("COMMIT");
                }
                step
            }
        }
    }
}

fn exact_stddev_pop_scaled_100(values: &[i64]) -> Result<i128, String> {
    let count = values.len() as u128;
    if count <= 1 {
        return Ok(0);
    }
    let mut sum = 0_i128;
    let mut sum_sq = 0_u128;
    for &v in values {
        let c = i128::from(v);
        sum = sum.checked_add(c).ok_or("stddev sum overflow")?;
        let abs = c.unsigned_abs();
        let sq = abs.checked_mul(abs).ok_or("stddev sq overflow")?;
        sum_sq = sum_sq.checked_add(sq).ok_or("stddev sum_sq overflow")?;
    }
    let n_sum_sq = count
        .checked_mul(sum_sq)
        .ok_or("stddev n_sum_sq overflow")?;
    let sum_abs = sum.unsigned_abs();
    let sum_sq_total = sum_abs
        .checked_mul(sum_abs)
        .ok_or("stddev sum_sq_total overflow")?;
    let numerator = n_sum_sq.saturating_sub(sum_sq_total);
    let std_coeff = numerator.isqrt() / count;
    i128::try_from(std_coeff).map_err(|e| format!("{e}"))
}

/// Verifies that `riffdbd`, `postgres-minimal`, and `postgres-safe-app` return identical
/// results for every workload and every seeded organization before timing starts.
pub fn verify_exact_parity(
    rt: &tokio::runtime::Runtime,
    riffdb: &RiffdbSaasRunner,
    postgres_url: &str,
    dataset: &SeedDataset,
) -> Result<usize, String> {
    let mut pg_min = PostgresSaasWorker::connect(
        postgres_url,
        PostgresProfile::Minimal,
        dataset.window_start_ms,
        dataset.window_end_ms,
    )?;
    let mut pg_safe = PostgresSaasWorker::connect(
        postgres_url,
        PostgresProfile::SafeApp,
        dataset.window_start_ms,
        dataset.window_end_ms,
    )?;

    let mut verified_pairs = 0usize;
    for org in &dataset.orgs {
        for workload in SaasWorkloadKind::ALL {
            let riff_out = rt.block_on(riffdb.execute_workload(workload, &org.org_id))?;
            let pg_min_out = pg_min.execute_workload(workload, &org.org_id)?;
            let pg_safe_out = pg_safe.execute_workload(workload, &org.org_id)?;

            if riff_out != pg_min_out {
                return Err(format!(
                    "parity mismatch on workload {} for {} between RiffDB and postgres-minimal:\n  riffdb:  {riff_out:?}\n  pg_min:  {pg_min_out:?}",
                    workload.as_str(),
                    org.org_id
                ));
            }
            if riff_out != pg_safe_out {
                return Err(format!(
                    "parity mismatch on workload {} for {} between RiffDB and postgres-safe-app:\n  riffdb:  {riff_out:?}\n  pg_safe: {pg_safe_out:?}",
                    workload.as_str(),
                    org.org_id
                ));
            }
            verified_pairs += 1;
        }
    }

    Ok(verified_pairs)
}

/// Latency distribution and throughput summary for one measured cell.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LatencyDistribution {
    /// Total completed operations.
    pub operations: usize,
    /// Concurrency level (`1` for unary).
    pub concurrency: usize,
    /// Total wall-clock duration in microseconds.
    pub wall_time_us: u64,
    /// Throughput in queries per second (`operations / wall_time`).
    pub qps: u64,
    /// Minimum latency in microseconds.
    pub min_us: u64,
    /// p50 (median) latency in microseconds.
    pub p50_us: u64,
    /// p95 latency in microseconds.
    pub p95_us: u64,
    /// p99 latency in microseconds (required by ADR-0249).
    pub p99_us: u64,
    /// Maximum latency in microseconds.
    pub max_us: u64,
    /// Arithmetic mean latency in microseconds.
    pub mean_us: u64,
}

impl LatencyDistribution {
    /// Computes exact nearest-rank percentiles and throughput from a vector of sample durations in microseconds.
    #[must_use]
    pub fn from_samples(
        mut samples_us: Vec<u64>,
        concurrency: usize,
        wall_elapsed: Duration,
    ) -> Self {
        samples_us.sort_unstable();
        let operations = samples_us.len();
        let wall_time_us = u64::try_from(wall_elapsed.as_micros())
            .unwrap_or(u64::MAX)
            .max(1);
        let qps = ((operations as u128) * 1_000_000 / u128::from(wall_time_us)) as u64;
        if operations == 0 {
            return Self {
                operations: 0,
                concurrency,
                wall_time_us,
                qps: 0,
                min_us: 0,
                p50_us: 0,
                p95_us: 0,
                p99_us: 0,
                max_us: 0,
                mean_us: 0,
            };
        }
        let min_us = samples_us[0];
        let max_us = samples_us[operations - 1];
        let sum_us: u128 = samples_us.iter().copied().map(u128::from).sum();
        let mean_us = (sum_us / (operations as u128)) as u64;
        let p50_us = nearest_rank(&samples_us, 50);
        let p95_us = nearest_rank(&samples_us, 95);
        let p99_us = nearest_rank(&samples_us, 99);

        Self {
            operations,
            concurrency,
            wall_time_us,
            qps,
            min_us,
            p50_us,
            p95_us,
            p99_us,
            max_us,
            mean_us,
        }
    }
}

fn nearest_rank(sorted: &[u64], percentile: usize) -> u64 {
    let n = sorted.len();
    let rank = (percentile * n).div_ceil(100).max(1);
    sorted[rank - 1]
}

/// Unary comparison cell for one workload across `riffdbd`, `postgres-minimal`, and `postgres-safe-app`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnaryWorkloadComparison {
    /// Workload identifier.
    pub workload: SaasWorkloadKind,
    /// RiffDB query name.
    pub query_name: String,
    /// RiffDB unary latency stats (`c = 1`).
    pub riffdb: LatencyDistribution,
    /// PostgreSQL `minimal` unary latency stats (`c = 1`).
    pub postgres_minimal: LatencyDistribution,
    /// PostgreSQL `safe-app` unary latency stats (`c = 1`).
    pub postgres_safe_app: LatencyDistribution,
}

/// Concurrent comparison cell at a specific concurrency level.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConcurrentComparisonCell {
    /// Workload name (`saas_analytics_mix` or a single workload identifier).
    pub workload_label: String,
    /// Concurrency level (`c = 1, 8, 32`).
    pub concurrency: usize,
    /// RiffDB concurrent throughput & tail latency.
    pub riffdb: LatencyDistribution,
    /// PostgreSQL `minimal` concurrent throughput & tail latency.
    pub postgres_minimal: LatencyDistribution,
    /// PostgreSQL `safe-app` concurrent throughput & tail latency.
    pub postgres_safe_app: LatencyDistribution,
}

/// Full benchmark report comparing RiffDB and PostgreSQL across all SaaS complex queries.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SaasQueryBenchReport {
    /// Schema version (`riffdb.saas-query-bench/v1`).
    pub schema_version: String,
    /// Scale preset (`smoke` or `full`).
    pub scale: SeedScale,
    /// Number of exact parity checks verified before timing.
    pub verified_parity_checks: usize,
    /// RiffDB seed time in milliseconds.
    pub riffdb_seed_ms: u64,
    /// PostgreSQL seed time in milliseconds.
    pub postgres_seed_ms: u64,
    /// Unary latency comparison across all 5 SaaS complex-query workloads.
    pub unary_workloads: Vec<UnaryWorkloadComparison>,
    /// Concurrent throughput and tail latency across the 5-workload SaaS mix and individual workloads.
    pub concurrent_cells: Vec<ConcurrentComparisonCell>,
}

/// Spawns `riffdbd`, deploys the SaaS contract and query module, seeds both RiffDB and PostgreSQL,
/// verifies exact result parity across all 5 workloads, and measures unary latency and concurrent throughput.
pub fn run_saas_query_benchmark(
    riffdbd_bin: &Path,
    work_dir: &Path,
    postgres_url: &str,
    scale: SeedScale,
) -> Result<SaasQueryBenchReport, String> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime build: {e}"))?;

    let spawned = SpawnedRiffdbd::spawn(riffdbd_bin, work_dir)?;
    let dataset = SeedDataset::generate(scale);
    let (riffdb_runner, riffdb_seed_dur) = rt.block_on(RiffdbSaasRunner::setup_and_seed(
        &spawned.endpoint,
        &spawned.bootstrap_credential,
        &dataset,
    ))?;
    let postgres_seed_dur = seed_postgres(postgres_url, &dataset)?;

    let verified_parity_checks = verify_exact_parity(&rt, &riffdb_runner, postgres_url, &dataset)?;

    let org_ids: Vec<String> = dataset.orgs.iter().map(|o| o.org_id.clone()).collect();

    // 1. Unary latency measurements (`c = 1`) for all 5 SaaS workloads.
    let mut unary_workloads = Vec::with_capacity(SaasWorkloadKind::ALL.len());
    for workload in SaasWorkloadKind::ALL {
        let riffdb_stats = rt.block_on(measure_riffdb_unary(
            &riffdb_runner,
            workload,
            &org_ids,
            scale.warmup_iterations(),
            scale.unary_iterations(),
        ))?;

        let pg_min_stats = measure_postgres_unary(
            postgres_url,
            PostgresProfile::Minimal,
            workload,
            &org_ids,
            dataset.window_start_ms,
            dataset.window_end_ms,
            scale.warmup_iterations(),
            scale.unary_iterations(),
        )?;

        let pg_safe_stats = measure_postgres_unary(
            postgres_url,
            PostgresProfile::SafeApp,
            workload,
            &org_ids,
            dataset.window_start_ms,
            dataset.window_end_ms,
            scale.warmup_iterations(),
            scale.unary_iterations(),
        )?;

        unary_workloads.push(UnaryWorkloadComparison {
            workload,
            query_name: workload.query_name().to_owned(),
            riffdb: riffdb_stats,
            postgres_minimal: pg_min_stats,
            postgres_safe_app: pg_safe_stats,
        });
    }

    // 2. Concurrent throughput and tail latency across the 5-workload SaaS mix at each concurrency level,
    // plus per-workload concurrent cells at peak concurrency.
    let mut concurrent_cells = Vec::new();
    for &concurrency in scale.concurrency_levels() {
        let mix = &SaasWorkloadKind::ALL;
        let riff_mix = rt.block_on(measure_riffdb_concurrent(
            &riffdb_runner,
            mix,
            &org_ids,
            concurrency,
            scale.ops_per_worker(),
        ))?;
        let pg_min_mix = measure_postgres_concurrent(
            postgres_url,
            PostgresProfile::Minimal,
            mix,
            &org_ids,
            dataset.window_start_ms,
            dataset.window_end_ms,
            concurrency,
            scale.ops_per_worker(),
        )?;
        let pg_safe_mix = measure_postgres_concurrent(
            postgres_url,
            PostgresProfile::SafeApp,
            mix,
            &org_ids,
            dataset.window_start_ms,
            dataset.window_end_ms,
            concurrency,
            scale.ops_per_worker(),
        )?;

        concurrent_cells.push(ConcurrentComparisonCell {
            workload_label: "saas_analytics_mix".to_owned(),
            concurrency,
            riffdb: riff_mix,
            postgres_minimal: pg_min_mix,
            postgres_safe_app: pg_safe_mix,
        });
    }

    let peak_concurrency = match scale {
        SeedScale::Smoke => 4,
        SeedScale::Full => 8,
    };
    for workload in SaasWorkloadKind::ALL {
        let single = [workload];
        let riff_cell = rt.block_on(measure_riffdb_concurrent(
            &riffdb_runner,
            &single,
            &org_ids,
            peak_concurrency,
            scale.ops_per_worker(),
        ))?;
        let pg_min_cell = measure_postgres_concurrent(
            postgres_url,
            PostgresProfile::Minimal,
            &single,
            &org_ids,
            dataset.window_start_ms,
            dataset.window_end_ms,
            peak_concurrency,
            scale.ops_per_worker(),
        )?;
        let pg_safe_cell = measure_postgres_concurrent(
            postgres_url,
            PostgresProfile::SafeApp,
            &single,
            &org_ids,
            dataset.window_start_ms,
            dataset.window_end_ms,
            peak_concurrency,
            scale.ops_per_worker(),
        )?;

        concurrent_cells.push(ConcurrentComparisonCell {
            workload_label: workload.as_str().to_owned(),
            concurrency: peak_concurrency,
            riffdb: riff_cell,
            postgres_minimal: pg_min_cell,
            postgres_safe_app: pg_safe_cell,
        });
    }

    Ok(SaasQueryBenchReport {
        schema_version: SAAS_QUERY_BENCH_SCHEMA_VERSION.to_owned(),
        scale,
        verified_parity_checks,
        riffdb_seed_ms: u64::try_from(riffdb_seed_dur.as_millis()).unwrap_or(u64::MAX),
        postgres_seed_ms: u64::try_from(postgres_seed_dur.as_millis()).unwrap_or(u64::MAX),
        unary_workloads,
        concurrent_cells,
    })
}

async fn measure_riffdb_unary(
    runner: &RiffdbSaasRunner,
    workload: SaasWorkloadKind,
    org_ids: &[String],
    warmup: usize,
    iterations: usize,
) -> Result<LatencyDistribution, String> {
    for i in 0..warmup {
        let org = &org_ids[i % org_ids.len()];
        let _ = runner.execute_workload(workload, org).await?;
    }
    let mut samples = Vec::with_capacity(iterations);
    let wall_start = Instant::now();
    for i in 0..iterations {
        let org = &org_ids[i % org_ids.len()];
        let t0 = Instant::now();
        let _ = runner.execute_workload(workload, org).await?;
        samples.push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
    }
    Ok(LatencyDistribution::from_samples(
        samples,
        1,
        wall_start.elapsed(),
    ))
}

#[allow(clippy::too_many_arguments)]
fn measure_postgres_unary(
    postgres_url: &str,
    profile: PostgresProfile,
    workload: SaasWorkloadKind,
    org_ids: &[String],
    window_start_ms: i64,
    window_end_ms: i64,
    warmup: usize,
    iterations: usize,
) -> Result<LatencyDistribution, String> {
    let mut worker =
        PostgresSaasWorker::connect(postgres_url, profile, window_start_ms, window_end_ms)?;
    for i in 0..warmup {
        let org = &org_ids[i % org_ids.len()];
        let _ = worker.execute_workload(workload, org)?;
    }
    let mut samples = Vec::with_capacity(iterations);
    let wall_start = Instant::now();
    for i in 0..iterations {
        let org = &org_ids[i % org_ids.len()];
        let t0 = Instant::now();
        let _ = worker.execute_workload(workload, org)?;
        samples.push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
    }
    Ok(LatencyDistribution::from_samples(
        samples,
        1,
        wall_start.elapsed(),
    ))
}

async fn measure_riffdb_concurrent(
    runner: &RiffdbSaasRunner,
    workloads: &[SaasWorkloadKind],
    org_ids: &[String],
    concurrency: usize,
    ops_per_worker: usize,
) -> Result<LatencyDistribution, String> {
    let barrier = Arc::new(tokio::sync::Barrier::new(concurrency));
    let workloads_vec = workloads.to_vec();
    let org_ids_vec = org_ids.to_vec();

    let mut handles = Vec::with_capacity(concurrency);
    for worker_idx in 0..concurrency {
        let worker_session = runner.fresh_worker_session().await?;
        let barrier_clone = Arc::clone(&barrier);
        let wl = workloads_vec.clone();
        let orgs = org_ids_vec.clone();
        handles.push(tokio::spawn(async move {
            for w in 0..2 {
                let workload = wl[(worker_idx + w) % wl.len()];
                let org = &orgs[(worker_idx + w) % orgs.len()];
                worker_session.execute_workload(workload, org).await?;
            }
            barrier_clone.wait().await;
            let wall_t0 = Instant::now();
            let mut local_samples = Vec::with_capacity(ops_per_worker);
            for op in 0..ops_per_worker {
                let workload = wl[(worker_idx + op) % wl.len()];
                let org = &orgs[(worker_idx + op) % orgs.len()];
                let t0 = Instant::now();
                worker_session.execute_workload(workload, org).await?;
                local_samples.push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
            }
            Ok::<(Vec<u64>, Duration), String>((local_samples, wall_t0.elapsed()))
        }));
    }

    let mut all_samples = Vec::with_capacity(concurrency * ops_per_worker);
    let mut max_worker_wall = Duration::ZERO;
    for handle in handles {
        let (local, worker_wall) = handle
            .await
            .map_err(|e| format!("tokio worker join failed: {e}"))??;
        all_samples.extend(local);
        if worker_wall > max_worker_wall {
            max_worker_wall = worker_wall;
        }
    }
    Ok(LatencyDistribution::from_samples(
        all_samples,
        concurrency,
        max_worker_wall,
    ))
}

#[allow(clippy::too_many_arguments)]
fn measure_postgres_concurrent(
    postgres_url: &str,
    profile: PostgresProfile,
    workloads: &[SaasWorkloadKind],
    org_ids: &[String],
    window_start_ms: i64,
    window_end_ms: i64,
    concurrency: usize,
    ops_per_worker: usize,
) -> Result<LatencyDistribution, String> {
    let barrier = Arc::new(Barrier::new(concurrency));
    let workloads_vec = workloads.to_vec();
    let org_ids_vec = org_ids.to_vec();
    let pg_url = postgres_url.to_owned();

    let mut handles = Vec::with_capacity(concurrency);
    for worker_idx in 0..concurrency {
        let barrier_clone = Arc::clone(&barrier);
        let wl = workloads_vec.clone();
        let orgs = org_ids_vec.clone();
        let url = pg_url.clone();
        handles.push(thread::spawn(move || {
            let mut worker =
                PostgresSaasWorker::connect(&url, profile, window_start_ms, window_end_ms)?;
            for w in 0..2 {
                let workload = wl[(worker_idx + w) % wl.len()];
                let org = &orgs[(worker_idx + w) % orgs.len()];
                worker.execute_workload(workload, org)?;
            }
            barrier_clone.wait();
            let wall_t0 = Instant::now();
            let mut local_samples = Vec::with_capacity(ops_per_worker);
            for op in 0..ops_per_worker {
                let workload = wl[(worker_idx + op) % wl.len()];
                let org = &orgs[(worker_idx + op) % orgs.len()];
                let t0 = Instant::now();
                worker.execute_workload(workload, org)?;
                local_samples.push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
            }
            Ok::<(Vec<u64>, Duration), String>((local_samples, wall_t0.elapsed()))
        }));
    }

    let mut all_samples = Vec::with_capacity(concurrency * ops_per_worker);
    let mut max_worker_wall = Duration::ZERO;
    for handle in handles {
        let (local, worker_wall) = handle
            .join()
            .map_err(|_| "postgres worker thread panicked".to_owned())??;
        all_samples.extend(local);
        if worker_wall > max_worker_wall {
            max_worker_wall = worker_wall;
        }
    }
    Ok(LatencyDistribution::from_samples(
        all_samples,
        concurrency,
        max_worker_wall,
    ))
}

/// Formats a human-readable Markdown summary table from a `SaasQueryBenchReport`.
#[must_use]
pub fn format_markdown_summary(report: &SaasQueryBenchReport) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# RiffDB vs PostgreSQL — SaaS Complex Query & Aggregation Benchmark ({})",
        report.scale.as_str()
    );
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "- **Schema:** `{}`\n- **Exact Result Parity Checks Passed:** `{}` (`riffdbd` == `postgres-minimal` == `postgres-safe-app`)\n- **Seed Time:** RiffDB `{} ms` | PostgreSQL `{} ms`",
        report.schema_version,
        report.verified_parity_checks,
        report.riffdb_seed_ms,
        report.postgres_seed_ms
    );
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "## 1. Unary Query Latency (`c = 1`, microseconds — lower is better)"
    );
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "| Workload | Engine | p50 (µs) | p95 (µs) | p99 (µs) | Mean (µs) | QPS |"
    );
    let _ = writeln!(out, "|---|---|---:|---:|---:|---:|---:|");
    for item in &report.unary_workloads {
        let name = item.workload.as_str();
        for (engine, stats) in [
            ("RiffDB (gRPC)", &item.riffdb),
            ("PostgreSQL (`minimal`)", &item.postgres_minimal),
            ("PostgreSQL (`safe-app`)", &item.postgres_safe_app),
        ] {
            let _ = writeln!(
                out,
                "| `{name}` | {engine} | {} | {} | {} | {} | {} |",
                stats.p50_us, stats.p95_us, stats.p99_us, stats.mean_us, stats.qps
            );
        }
    }

    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "## 2. Concurrent Throughput & Tail Latency (higher QPS / lower µs is better)"
    );
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "| Workload | Concurrency | Engine | QPS | p50 (µs) | p95 (µs) | p99 (µs) | Mean (µs) |"
    );
    let _ = writeln!(out, "|---|---:|---|---:|---:|---:|---:|---:|");
    for cell in &report.concurrent_cells {
        for (engine, stats) in [
            ("RiffDB (gRPC)", &cell.riffdb),
            ("PostgreSQL (`minimal`)", &cell.postgres_minimal),
            ("PostgreSQL (`safe-app`)", &cell.postgres_safe_app),
        ] {
            let _ = writeln!(
                out,
                "| `{}` | `{}` | {engine} | {} | {} | {} | {} | {} |",
                cell.workload_label,
                cell.concurrency,
                stats.qps,
                stats.p50_us,
                stats.p95_us,
                stats.p99_us,
                stats.mean_us
            );
        }
    }

    out
}

/// Runs a future on a dedicated multi-threaded Tokio runtime.
pub fn block_on_saas_bench<F, T>(future: F) -> Result<T, String>
where
    F: Future<Output = Result<T, String>>,
{
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime build: {e}"))?;
    rt.block_on(Pin::from(Box::new(future)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saas_contract_and_query_module_compile_cleanly() {
        let artifacts = compile_saas_artifacts().expect("SaaS contract and queries must compile");
        assert_eq!(artifacts.contract.schema().entities().len(), 6);
        assert_eq!(artifacts.query_module.queries().len(), 5);
    }

    #[test]
    fn deterministic_dataset_scales_fit_query_bounds() {
        for scale in [SeedScale::Smoke, SeedScale::Full] {
            let ds = SeedDataset::generate(scale);
            assert_eq!(ds.orgs.len(), scale.org_count());
            assert!(scale.invoices_per_org() <= 240);
            assert!(scale.projects_per_org() <= 12);
            assert!(scale.tasks_per_project() <= 32);
            assert!(scale.projects_per_org() * scale.tasks_per_project() <= 240);
        }
    }
}
