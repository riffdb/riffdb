//! Concurrent closed-loop mixed-workload driver.
//!
//! This answers questions the single-client parity suite cannot:
//! tails under concurrency, hot-key contention, outcome mix, and
//! duration-based steady-state throughput. It reuses the existing
//! [`AppBackend`] operations and seed probes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{TrySendError, sync_channel};
use std::thread;
use std::time::{Duration, Instant};

use crate::{
    AppBackend, CloseTicketWithCommentSeed, CommentRow, CommentSeed, LatencyHistogram,
    LoadErrorClass, OpenTicketWithLabelsSeed, ScenarioProbes, SeedDataset, SwapMemberRolesSeed,
    TicketRow, TicketStatus, time_call, uuid_from_ordinal,
};

const NS_LOAD_WRITE: u8 = 0x7e;

/// Named load profiles (weights and pacing, not free-form knobs).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkloadProfile {
    /// Read-only diagnostic mix used to isolate application-port and RiffQL capacity.
    ReadOnly,
    /// Write-only diagnostic mix used to expose the physical durable-command ceiling.
    WriteOnly,
    /// Isolated compiler-proved child appends without root-mutation compatibility cuts.
    AppendOnly,
    /// Mostly reads with a small write mix (~85/12/3).
    ///
    /// Omits `SwapMemberRoles` so the default mix does not serialize all clients
    /// onto one membership pair.
    Interactive,
    /// Bursty multi-call agent: batches of work, think-time, some multi-entity
    /// writes, and a few percent intentional idempotent replays.
    ///
    /// Omits `SwapMemberRoles` for the same isolation reason as `Interactive`.
    Agent,
    /// Explicit contention profile that hammers `SwapMemberRoles` on the shared
    /// probe membership pair (not isolated under concurrency by design).
    MembershipContention,
}

impl WorkloadProfile {
    /// Stable report id.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::WriteOnly => "write_only",
            Self::AppendOnly => "append_only",
            Self::Interactive => "interactive",
            Self::Agent => "agent",
            Self::MembershipContention => "membership_contention",
        }
    }

    /// Parses a profile name.
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "read_only" => Some(Self::ReadOnly),
            "write_only" => Some(Self::WriteOnly),
            "append_only" => Some(Self::AppendOnly),
            "interactive" => Some(Self::Interactive),
            "agent" => Some(Self::Agent),
            "membership_contention" => Some(Self::MembershipContention),
            _ => None,
        }
    }

    /// Weighted operation table (weights need not sum to 100).
    #[must_use]
    pub fn weights(self) -> &'static [(LoadOp, u32)] {
        match self {
            Self::ReadOnly => &[
                (LoadOp::PointGetTicket, 20),
                (LoadOp::PointGetUser, 15),
                (LoadOp::ListTicketsByProjectStatus, 15),
                (LoadOp::ListOpenTicketsForAssignee, 15),
                (LoadOp::ListCommentsForTicket, 10),
                (LoadOp::ListProjectMembers, 10),
                (LoadOp::TicketDetailPage, 15),
            ],
            Self::WriteOnly => Self::saturating_weights(),
            Self::AppendOnly => &[(LoadOp::CreateComment, 1)],
            // Default mixes intentionally omit SwapMemberRoles: that op serializes
            // on a shared membership pair and is not "isolated" under concurrency.
            // Use WorkloadProfile::MembershipContention for that stress path.
            Self::Interactive => &[
                (LoadOp::PointGetTicket, 25),
                (LoadOp::PointGetUser, 15),
                (LoadOp::ListTicketsByProjectStatus, 12),
                (LoadOp::ListOpenTicketsForAssignee, 10),
                (LoadOp::ListCommentsForTicket, 10),
                (LoadOp::ListProjectMembers, 8),
                (LoadOp::TicketDetailPage, 5),
                (LoadOp::CreateComment, 12),
                (LoadOp::CloseTicketWithComment, 2),
                (LoadOp::OpenTicketWithLabels, 1),
            ],
            Self::Agent => &[
                (LoadOp::PointGetTicket, 15),
                (LoadOp::TicketDetailPage, 20),
                (LoadOp::ListTicketsByProjectStatus, 10),
                (LoadOp::ListCommentsForTicket, 10),
                (LoadOp::CreateComment, 20),
                (LoadOp::CloseTicketWithComment, 10),
                (LoadOp::OpenTicketWithLabels, 10),
                (LoadOp::ListProjectMembers, 5),
            ],
            Self::MembershipContention => &[
                (LoadOp::PointGetTicket, 20),
                (LoadOp::ListProjectMembers, 20),
                (LoadOp::SwapMemberRoles, 40),
                (LoadOp::CreateComment, 15),
                (LoadOp::TicketDetailPage, 5),
            ],
        }
    }

    /// Write-heavy mix for saturation evidence (holds coordinator depth).
    #[must_use]
    pub fn saturating_weights() -> &'static [(LoadOp, u32)] {
        &[
            (LoadOp::CreateComment, 70),
            (LoadOp::CloseTicketWithComment, 20),
            (LoadOp::OpenTicketWithLabels, 10),
        ]
    }

    /// Burst length for agent pacing (interactive is continuous).
    #[must_use]
    pub const fn burst_ops(self) -> u32 {
        match self {
            Self::ReadOnly
            | Self::WriteOnly
            | Self::AppendOnly
            | Self::Interactive
            | Self::MembershipContention => 1,
            Self::Agent => 8,
        }
    }

    /// Think-time after each agent burst.
    #[must_use]
    pub const fn think_time(self) -> Duration {
        match self {
            Self::ReadOnly
            | Self::WriteOnly
            | Self::AppendOnly
            | Self::Interactive
            | Self::MembershipContention => Duration::ZERO,
            Self::Agent => Duration::from_millis(8),
        }
    }

    /// Fraction of create-comment ops that intentionally reuse the prior
    /// sample's idempotency key (replay under load), in basis points.
    #[must_use]
    pub const fn replay_basis_points(self) -> u32 {
        match self {
            Self::ReadOnly
            | Self::WriteOnly
            | Self::AppendOnly
            | Self::Interactive
            | Self::MembershipContention => 0,
            Self::Agent => 500, // 5%
        }
    }
}

/// Hard cap for RiffDB load clients (matches seed concurrency bound).
pub const RIFFDB_MAX_LOAD_CLIENTS: usize = 128;

/// Fixed client counts for `--load-concurrency-sweep`.
///
/// Same workload mix and measure window at each point; only concurrency changes.
/// Order is low → high so the curve is monotonic in client count.
pub const LOAD_CONCURRENCY_SWEEP_CLIENTS: &[usize] = &[1, 8, 32, 128];
/// Max clients under saturate (knee-sweep friendly; not auto-forced).
pub const RIFFDB_SATURATE_LOAD_CLIENTS: usize = 512;
/// Default long-lived concurrent workers per logical load client when
/// `--load-saturate` is set.
///
/// Effective continuous concurrency = `clients × saturate_fanout` (each fan-out
/// slot is an independent closed-loop worker, not a wave barrier). The actor
/// drains the mpsc channel into an unbounded `pending` deque and frees queue
/// permits at group selection, so only sustained concurrency above channel
/// depth can keep slots full long enough for the 150 ms admission cap to fire.
/// Default long-lived workers per logical client under `--load-saturate`.
///
/// Paired with [`SATURATE_COORDINATOR_WORKLOAD_CAPACITY`]: 8 clients × 128 = 1024
/// continuous jobs against a capacity-1 channel. Channel slots free at drain
/// (before execute finishes), so wait ≈ `queue_position × group_turn`; need
/// workers ≫ 150ms / ~0.25ms ≈ 600 to push oldest waiters over the cap.
pub const SATURATE_DEFAULT_FANOUT: usize = 128;
/// Coordinator queue depth for the saturate app-baseline riffdbd child.
///
/// Production default is 128; at that depth the actor drains into unbounded
/// `pending` faster than closed-loop load can hold the channel full for 150 ms.
/// The saturate profile intentionally uses capacity 1 so live evidence is
/// reachable without shortening the ADR-0071 wait.
pub const SATURATE_COORDINATOR_WORKLOAD_CAPACITY: u16 = 1;

/// One operation drawn from the existing scenario catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum LoadOp {
    /// PK ticket get.
    PointGetTicket,
    /// PK user get.
    PointGetUser,
    /// Filtered ticket list.
    ListTicketsByProjectStatus,
    /// Open tickets for assignee.
    ListOpenTicketsForAssignee,
    /// Comments for ticket.
    ListCommentsForTicket,
    /// Project members.
    ListProjectMembers,
    /// Ticket detail page.
    TicketDetailPage,
    /// Single-entity write.
    CreateComment,
    /// Multi-entity write.
    CloseTicketWithComment,
    /// Multi-entity role swap.
    SwapMemberRoles,
    /// Multi-entity open + labels.
    OpenTicketWithLabels,
}

impl LoadOp {
    /// Stable report id.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PointGetTicket => "point_get_ticket",
            Self::PointGetUser => "point_get_user",
            Self::ListTicketsByProjectStatus => "list_tickets_by_project_status",
            Self::ListOpenTicketsForAssignee => "list_open_tickets_for_assignee",
            Self::ListCommentsForTicket => "list_comments_for_ticket",
            Self::ListProjectMembers => "list_project_members",
            Self::TicketDetailPage => "ticket_detail_page",
            Self::CreateComment => "create_comment",
            Self::CloseTicketWithComment => "close_ticket_with_comment",
            Self::SwapMemberRoles => "swap_member_roles",
            Self::OpenTicketWithLabels => "open_ticket_with_labels",
        }
    }

    /// All ops in report order.
    #[must_use]
    pub const fn all() -> [Self; 11] {
        [
            Self::PointGetTicket,
            Self::PointGetUser,
            Self::ListTicketsByProjectStatus,
            Self::ListOpenTicketsForAssignee,
            Self::ListCommentsForTicket,
            Self::ListProjectMembers,
            Self::TicketDetailPage,
            Self::CreateComment,
            Self::CloseTicketWithComment,
            Self::SwapMemberRoles,
            Self::OpenTicketWithLabels,
        ]
    }

    /// Whether this operation invokes one public application command.
    #[must_use]
    pub const fn is_command(self) -> bool {
        matches!(
            self,
            Self::CreateComment
                | Self::CloseTicketWithComment
                | Self::SwapMemberRoles
                | Self::OpenTicketWithLabels
        )
    }
}

/// Closed classification of one operation attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpOutcome {
    /// Completed with an application-success result.
    Success,
    /// Backend reported a conflict-like durable rejection.
    Conflict,
    /// A supposedly replay-safe identity was reused with unequal input.
    IdempotencyMismatch,
    /// Backend reported temporary storage or transport unavailability.
    Unavailable,
    /// Typed capacity rejection (`RDB-CAPACITY-0101`); certain-not-executed.
    Overloaded,
    /// Observed history predates a restore (`RDB-HISTORY-0101`).
    HistoryIncarnationMismatch,
    /// Intentional idempotent replay path completed.
    Replayed,
    /// Other failure (timeout, transport, unexpected).
    Error,
}

/// Configuration for one closed-loop load run.
#[derive(Clone, Debug)]
pub struct LoadConfig {
    /// Named profile.
    pub profile: WorkloadProfile,
    /// Concurrent sessions.
    pub clients: usize,
    /// Steady-state measurement window.
    pub duration: Duration,
    /// Warmup before measurement (discarded).
    pub warmup: Duration,
    /// Zipf `s` exponent over open tickets (0 = uniform).
    pub zipf_s: f64,
    /// Deterministic RNG seed.
    pub rng_seed: u64,
    /// Direct ticket reads and ticket writes at the same hot row.
    pub contended: bool,
    /// Drive concurrent fan-out to evidence typed overload under depth.
    pub saturate: bool,
    /// p99 ceiling for **successful** ops under `--load-saturate` (default 250 ms).
    ///
    /// Admission alone may consume up to 150 ms before accept; the ceiling leaves
    /// room for one group-turn of accepted work plus histogram bucket rounding.
    pub saturate_p99_ceiling: Duration,
    /// Long-lived concurrent workers per logical load client under saturate.
    ///
    /// Effective continuous concurrency = `clients × saturate_fanout`. Each
    /// fan-out slot owns a session and loops independently (not a join-wave).
    pub saturate_fanout: usize,
    /// Starting sample ordinal for write identity (idempotency keys, comment ids).
    ///
    /// Concurrency sweeps seed once and re-run at higher client counts; without a
    /// rising base, later points reuse earlier keys and trip RDB-COMMAND-0101 /
    /// SQLSTATE 23505.
    pub sample_id_base: u64,
    /// Offered Poisson arrival rate. `None` selects the closed-loop driver.
    pub open_loop_rate_per_sec: Option<u64>,
    /// Bounded client-side arrival queue for open-loop work.
    pub open_loop_queue_depth: usize,
    /// Number of organization partitions receiving load (1..=64).
    pub tenant_count: usize,
    /// Percentage of operations routed to tenant zero; zero means uniform.
    pub hot_tenant_percent: u8,
}

/// Backend-specific execution shape that materially affects load results.
#[derive(Clone, Copy, Debug)]
pub struct LoadExecutionShape {
    /// Stable connection topology label.
    pub transport_topology: &'static str,
    /// Maximum transport submissions made for one logical command.
    pub command_attempt_budget: u32,
}

impl LoadConfig {
    /// Smoke defaults: short window, modest concurrency.
    #[must_use]
    pub fn smoke(profile: WorkloadProfile) -> Self {
        Self {
            profile,
            clients: 8,
            duration: Duration::from_secs(5),
            warmup: Duration::from_secs(1),
            zipf_s: 1.0,
            rng_seed: 0x000A_11CE_BEEF,
            contended: false,
            saturate: false,
            saturate_p99_ceiling: Duration::from_millis(250),
            saturate_fanout: 1,
            sample_id_base: 0,
            open_loop_rate_per_sec: None,
            open_loop_queue_depth: 256,
            tenant_count: 1,
            hot_tenant_percent: 0,
        }
    }

    /// Fuller defaults for iterative optimization / evidentiary windows.
    ///
    /// Measure 90 s with 15 s warmup so published numbers are not noise from a
    /// short burst. Smoke profiles stay short via [`Self::smoke`].
    #[must_use]
    pub fn standard(profile: WorkloadProfile, clients: usize) -> Self {
        Self {
            profile,
            clients: clients.clamp(1, 128),
            duration: Duration::from_secs(90),
            warmup: Duration::from_secs(15),
            zipf_s: 1.0,
            rng_seed: 0x000A_11CE_BEEF,
            contended: false,
            saturate: false,
            saturate_p99_ceiling: Duration::from_millis(250),
            saturate_fanout: 1,
            sample_id_base: 0,
            open_loop_rate_per_sec: None,
            open_loop_queue_depth: 4_096,
            tenant_count: 1,
            hot_tenant_percent: 0,
        }
    }

    /// Caps `clients` for a backend family (Postgres connection headroom vs RiffDB).
    #[must_use]
    pub fn with_client_cap(mut self, max_clients: usize) -> Self {
        self.clients = self.clients.clamp(1, max_clients.max(1));
        self
    }
}

/// Per-operation aggregates for one load run.
#[derive(Clone, Debug, Default)]
pub struct OpStats {
    /// Latency histogram (all outcomes).
    pub latency: LatencyHistogram,
    /// Latency histogram for successful ops only (saturate p99 gate).
    pub success_latency: LatencyHistogram,
    /// Outcome counts.
    pub success: u64,
    /// Conflict-like failures.
    pub conflict: u64,
    /// Unequal-input idempotency reuse (a harness/application correctness bug).
    pub idempotency_mismatch: u64,
    /// Temporary storage or transport unavailability.
    pub unavailable: u64,
    /// Typed capacity rejections.
    pub overloaded: u64,
    /// History incarnation fence rejections.
    pub history_incarnation_mismatch: u64,
    /// Intentional replays.
    pub replayed: u64,
    /// Other errors.
    pub error: u64,
    /// First error text observed (bounded, redacted-safe public messages only).
    pub first_error: Option<String>,
}

impl OpStats {
    fn record(&mut self, elapsed: Duration, outcome: OpOutcome, error_text: Option<String>) {
        self.latency.record(elapsed);
        if matches!(outcome, OpOutcome::Success) {
            self.success_latency.record(elapsed);
        }
        match outcome {
            OpOutcome::Success => self.success = self.success.saturating_add(1),
            OpOutcome::Conflict => self.conflict = self.conflict.saturating_add(1),
            OpOutcome::IdempotencyMismatch => {
                self.idempotency_mismatch = self.idempotency_mismatch.saturating_add(1);
            }
            OpOutcome::Unavailable => self.unavailable = self.unavailable.saturating_add(1),
            OpOutcome::Overloaded => self.overloaded = self.overloaded.saturating_add(1),
            OpOutcome::HistoryIncarnationMismatch => {
                self.history_incarnation_mismatch =
                    self.history_incarnation_mismatch.saturating_add(1);
            }
            OpOutcome::Replayed => self.replayed = self.replayed.saturating_add(1),
            OpOutcome::Error => self.error = self.error.saturating_add(1),
        }
        if self.first_error.is_none()
            && let Some(text) = error_text
        {
            self.first_error = Some(text.chars().take(240).collect());
        }
    }

    fn merge(&mut self, other: &Self) {
        self.latency.merge(&other.latency);
        self.success_latency.merge(&other.success_latency);
        self.success = self.success.saturating_add(other.success);
        self.conflict = self.conflict.saturating_add(other.conflict);
        self.idempotency_mismatch = self
            .idempotency_mismatch
            .saturating_add(other.idempotency_mismatch);
        self.unavailable = self.unavailable.saturating_add(other.unavailable);
        self.overloaded = self.overloaded.saturating_add(other.overloaded);
        self.history_incarnation_mismatch = self
            .history_incarnation_mismatch
            .saturating_add(other.history_incarnation_mismatch);
        self.replayed = self.replayed.saturating_add(other.replayed);
        self.error = self.error.saturating_add(other.error);
        if self.first_error.is_none() {
            self.first_error = other.first_error.clone();
        }
    }

    fn total_operations(&self) -> u64 {
        self.success
            .saturating_add(self.conflict)
            .saturating_add(self.idempotency_mismatch)
            .saturating_add(self.unavailable)
            .saturating_add(self.overloaded)
            .saturating_add(self.history_incarnation_mismatch)
            .saturating_add(self.replayed)
            .saturating_add(self.error)
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "latency": self.latency.summary_json(),
            "success_latency": self.success_latency.summary_json(),
            "outcomes": {
                "success": self.success,
                "conflict": self.conflict,
                "idempotency_mismatch": self.idempotency_mismatch,
                "unavailable": self.unavailable,
                "overloaded": self.overloaded,
                "history_incarnation_mismatch": self.history_incarnation_mismatch,
                "replayed": self.replayed,
                "error": self.error,
                "logical_operations": self.total_operations(),
            },
            "first_error": self.first_error,
        })
    }
}

/// One completed closed-loop load measurement.
#[derive(Clone, Debug)]
pub struct LoadReport {
    /// Backend label.
    pub backend_id: &'static str,
    /// Config used.
    pub config: LoadConfig,
    /// Backend transport and retry shape.
    pub execution_shape: LoadExecutionShape,
    /// Exact measure window: max(worker measure end) − min(worker measure start).
    ///
    /// Throughput uses this span so join stragglers and coordinator sleep skew
    /// cannot inflate or deflate ops/s.
    pub measured_elapsed: Duration,
    /// Per-worker recorded measure intervals (first measure sample start → last sample end).
    pub worker_measure_intervals: Vec<Duration>,
    /// Seed duration preceding the load (if known).
    pub seed_ns: u64,
    /// Per-op stats.
    pub by_op: Vec<(LoadOp, OpStats)>,
    /// Aggregate across ops.
    pub aggregate: OpStats,
    /// Optional server write-group histogram (RiffDB only).
    pub write_completion_groups: Option<Vec<u64>>,
    /// Open-loop offered/admitted/rejected counts, absent for closed-loop runs.
    pub open_loop: Option<OpenLoopReport>,
    /// Per-worker completed measured operations (bounded by client ceiling).
    pub worker_completed_operations: Vec<u64>,
    /// Longest observed gap between completions per worker.
    pub worker_longest_pause: Vec<Duration>,
    /// Per-tenant measured completion latency and operation counts.
    pub tenant_stats: Vec<TenantLoadStats>,
}

/// Bounded per-tenant fairness evidence without tenant identifiers.
#[derive(Clone, Debug, Default)]
pub struct TenantLoadStats {
    /// Completed measured operations.
    pub completed_operations: u64,
    /// End-to-end latency.
    pub latency: LatencyHistogram,
}

/// Offered-demand evidence for one open-loop run.
#[derive(Clone, Debug)]
pub struct OpenLoopReport {
    /// Requested mean Poisson arrival rate.
    pub offered_rate_per_sec: u64,
    /// Jobs scheduled during the measured window.
    pub offered: u64,
    /// Jobs admitted to the bounded client queue.
    pub admitted: u64,
    /// Jobs rejected because the bounded client queue was full.
    pub client_queue_rejected: u64,
    /// Queue delay before a worker began the public operation.
    pub queue_delay: LatencyHistogram,
    /// Fixed interval during which arrivals were offered.
    pub offered_window: Duration,
}

impl LoadReport {
    /// Successful application mutations in the measured window.
    #[must_use]
    pub fn successful_mutations(&self) -> u64 {
        self.by_op
            .iter()
            .filter(|(operation, _)| operation.is_command())
            .map(|(_, stats)| stats.success)
            .fold(0_u64, u64::saturating_add)
    }

    /// Public command invocations in the measured window, including closed failures and replays.
    #[must_use]
    pub fn command_attempts(&self) -> u64 {
        self.by_op
            .iter()
            .filter(|(operation, _)| operation.is_command())
            .map(|(_, stats)| stats.total_operations())
            .fold(0_u64, u64::saturating_add)
    }

    /// JSON report under `riffdb.app-baseline-load/v1`.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        let total_ops = self.aggregate.total_operations();
        let elapsed_ns = u64::try_from(self.measured_elapsed.as_nanos()).unwrap_or(u64::MAX);
        let throughput = total_ops
            .saturating_mul(1_000_000_000)
            .checked_div(elapsed_ns)
            .unwrap_or(0);
        let successful_mutations = self.successful_mutations();
        let command_attempts = self.command_attempts();
        let mut ops = serde_json::Map::new();
        for (op, stats) in &self.by_op {
            if stats.total_operations() > 0 {
                ops.insert(op.as_str().to_owned(), stats.json());
            }
        }
        let worker_measure_ns: Vec<u64> = self
            .worker_measure_intervals
            .iter()
            .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
            .collect();
        let worker_longest_pause_ns: Vec<u64> = self
            .worker_longest_pause
            .iter()
            .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
            .collect();
        let worker_min = self
            .worker_completed_operations
            .iter()
            .copied()
            .min()
            .unwrap_or(0);
        let worker_max = self
            .worker_completed_operations
            .iter()
            .copied()
            .max()
            .unwrap_or(0);
        let tenant_stats = self
            .tenant_stats
            .iter()
            .enumerate()
            .map(|(tenant_index, stats)| {
                serde_json::json!({
                    "tenant_index": tenant_index,
                    "completed_operations": stats.completed_operations,
                    "latency": stats.latency.summary_json(),
                })
            })
            .collect::<Vec<_>>();
        let open_loop = self.open_loop.as_ref().map(|arrival| {
            let offered_window_ns = u64::try_from(arrival.offered_window.as_nanos())
                .unwrap_or(u64::MAX)
                .max(1);
            serde_json::json!({
                "offered_rate_per_sec": arrival.offered_rate_per_sec,
                "offered": arrival.offered,
                "admitted": arrival.admitted,
                "completed": total_ops,
                "client_queue_rejected": arrival.client_queue_rejected,
                "offered_window_ns": offered_window_ns,
                "service_window_ns": elapsed_ns,
                "drain_elapsed_ns": elapsed_ns.saturating_sub(offered_window_ns),
                "offered_ops_s": arrival.offered.saturating_mul(1_000_000_000).checked_div(offered_window_ns).unwrap_or(0),
                "admitted_ops_s": arrival.admitted.saturating_mul(1_000_000_000).checked_div(offered_window_ns).unwrap_or(0),
                "completed_ops_s": throughput,
                "queue_delay": arrival.queue_delay.summary_json(),
            })
        });
        serde_json::json!({
            "schema": "riffdb.app-baseline-load/v1",
            "backend_id": self.backend_id,
            "profile": self.config.profile.as_str(),
            "clients": self.config.clients,
            "duration_ms": self.config.duration.as_millis() as u64,
            "warmup_ms": self.config.warmup.as_millis() as u64,
            "zipf_s": self.config.zipf_s,
            "rng_seed": self.config.rng_seed,
            "transport_topology": self.execution_shape.transport_topology,
            "command_attempt_budget": self.execution_shape.command_attempt_budget,
            "retry_observability": if self.execution_shape.command_attempt_budget == 1 {
                "automatic_retries_disabled"
            } else {
                "logical_latency_includes_client_retries"
            },
            "contention_mode": if self.config.contended {
                "shared_hot_ticket"
            } else {
                "isolated_read_ticket"
            },
            "seed_ns": self.seed_ns,
            "measured_elapsed_ns": elapsed_ns,
            "worker_measure_intervals_ns": worker_measure_ns,
            "worker_fairness": {
                "completed_operations": self.worker_completed_operations,
                "min_completed": worker_min,
                "max_completed": worker_max,
                "max_to_min_ratio": if worker_min == 0 { serde_json::Value::Null } else { serde_json::json!(worker_max as f64 / worker_min as f64) },
                "longest_pause_ns": worker_longest_pause_ns,
                "max_pause_ns": worker_longest_pause_ns.iter().copied().max().unwrap_or(0),
            },
            "throughput_ops_s": throughput,
            "command_attempts": command_attempts,
            "command_attempts_s": command_attempts.saturating_mul(1_000_000_000).checked_div(elapsed_ns).unwrap_or(0),
            "successful_mutations": successful_mutations,
            "successful_mutations_s": successful_mutations.saturating_mul(1_000_000_000).checked_div(elapsed_ns).unwrap_or(0),
            "tenant_scope": if self.config.tenant_count == 1 { "single_organization" } else { "multiple_organizations" },
            "tenant_count": self.config.tenant_count,
            "hot_tenant_percent": self.config.hot_tenant_percent,
            "tenant_fairness": tenant_stats,
            "aggregate": self.aggregate.json(),
            "operations": ops,
            "write_completion_groups_by_size": self.write_completion_groups,
            "mode": if self.open_loop.is_some() { "open_loop_poisson" } else { "closed_loop" },
            "open_loop": open_loop,
            "notes": [
                "Closed-loop concurrent sessions; transport_topology states whether those handles own independent connections.",
                "Throughput denominator is max(worker_measure_end)−min(worker_measure_start); stop is a shared AtomicBool so the window is exact, not coordinator-sleep approximate.",
                "worker_measure_intervals_ns is each worker's first-to-last measured sample span.",
                "tenant_count and hot_tenant_percent define bounded partition routing; tenant_fairness uses indices rather than application identifiers.",
                "Zipfian ticket selection (CDF partition_point) skews CreateComment and CloseTicketWithComment onto hot write-pool tickets; read probes use a separate stable ticket excluded from that pool.",
                "write_tickets is a seed-time Open snapshot; CloseTicketWithComment may flip rows to Closed mid-run. Harmless while commands have no open-status require; a future require would need live open filtering.",
                "interactive/agent omit SwapMemberRoles; use membership_contention for intentional shared-membership serialization.",
                "Outcomes classify via RiffDB RDB-* codes or PostgreSQL SQLSTATE, never prose matching.",
                "Agent profile injects a small fraction of intentional idempotent comment replays (PG ON CONFLICT DO NOTHING; RiffDB same-key equal-input replay).",
                "unavailable is a correctness signal, kept separate from expected application conflicts; RDB-STORAGE-0101 is never hidden or retried by the load driver.",
                "Open-loop runs use a bounded client queue and count offered, admitted, completed, and client-queue-rejected work separately.",
                "Open-loop offered/admitted rates use the fixed arrival window; completed throughput uses the service window through bounded queue drain, avoiding backlog-as-throughput inflation."
            ],
        })
    }
}

/// Per-worker load result: op stats plus absolute measure bounds.
struct WorkerLoadResult {
    by_op: Vec<(LoadOp, OpStats)>,
    /// Instant the worker started its first measured sample (if any).
    measure_start: Option<Instant>,
    /// Instant the worker finished its last measured sample (if any).
    measure_end: Option<Instant>,
    completed_operations: u64,
    longest_pause: Duration,
    tenant_stats: Vec<TenantLoadStats>,
}

/// Runs a closed-loop concurrent load against one backend family.
///
/// `factory` must return an independent session handle (cloned RiffDB client
/// or a fresh Postgres connection). The driver also calls [`AppBackend::prewarm`]
/// on each handle so timed work never pays first-prepare. The dataset is shared
/// read-only.
///
/// Measurement: coordinator sets `measuring` after warmup and `stop` after
/// `duration`. Each worker records absolute start/end Instants for measured
/// samples. Throughput uses **max(end) − min(start)** across workers.
pub fn run_closed_loop_load<B, Factory>(
    backend_id: &'static str,
    config: LoadConfig,
    execution_shape: LoadExecutionShape,
    dataset: &SeedDataset,
    seed_ns: u64,
    factory: Factory,
) -> Result<LoadReport, String>
where
    B: AppBackend + Send + 'static,
    B::Error: Send + 'static,
    Factory: Fn() -> Result<B, String> + Send + Sync + 'static,
{
    run_closed_loop_load_with_abort(
        backend_id,
        config,
        execution_shape,
        dataset,
        seed_ns,
        factory,
        None,
    )
}

/// Like [`run_closed_loop_load`] with an optional mid-load abort hook.
///
/// The coordinator sleeps in ≤250 ms slices and polls `abort`. When the hook
/// returns `Some(reason)`, workers are stopped and the function returns
/// `Err("backend died mid-load: {reason}")` so a dead peer cannot burn the
/// full measurement window.
pub fn run_closed_loop_load_with_abort<B, Factory>(
    backend_id: &'static str,
    config: LoadConfig,
    execution_shape: LoadExecutionShape,
    dataset: &SeedDataset,
    seed_ns: u64,
    factory: Factory,
    abort: Option<std::sync::Arc<dyn Fn() -> Option<String> + Send + Sync>>,
) -> Result<LoadReport, String>
where
    B: AppBackend + Send + 'static,
    B::Error: Send + 'static,
    Factory: Fn() -> Result<B, String> + Send + Sync + 'static,
{
    let client_ceiling = if config.saturate {
        RIFFDB_SATURATE_LOAD_CLIENTS
    } else {
        RIFFDB_MAX_LOAD_CLIENTS
    };
    if !(1..=client_ceiling).contains(&config.clients) {
        return Err(format!("load clients must be 1..={client_ceiling}"));
    }
    if config.duration.is_zero() {
        return Err("load duration must be positive".to_owned());
    }

    if !(1..=64).contains(&config.tenant_count) {
        return Err("load tenant count must be 1..=64".to_owned());
    }
    if config.hot_tenant_percent > 100 {
        return Err("hot tenant percent must be <= 100".to_owned());
    }
    let probes = dataset.tenant_probes(config.tenant_count);
    if probes.len() != config.tenant_count {
        return Err(format!(
            "load requested {} tenants but seed contains {}",
            config.tenant_count,
            probes.len()
        ));
    }
    // Read probes use a stable ticket. Write hot-keys exclude it so concurrent
    // comments/closes do not thrash the measured read path or exhaust its scan budget.
    //
    // Single-tenant v1: every write-pool ticket is drawn from the read-probe
    // organization only (see LoadReport tenant_scope).
    //
    // Status snapshot: membership is "Open at seed time". CloseTicketWithComment
    // flips tickets to Closed during the run, so the Zipf pool is not a live
    // open-set. That is intentional and harmless today — neither CreateComment
    // nor CloseTicketWithComment requires open status. If a future contract
    // adds that require, filter live opens (or pin a never-closed subset) or
    // mid-run closes will surface as mystery conflicts.
    let write_tickets = probes
        .iter()
        .map(|tenant| {
            let mut tickets = dataset
                .tickets
                .iter()
                .filter(|ticket| {
                    ticket.organization_id == tenant.organization_id
                        && ticket.status == TicketStatus::Open
                        && if config.contended {
                            ticket.ticket_id == tenant.ticket_id
                        } else {
                            ticket.ticket_id != tenant.ticket_id
                        }
                })
                .cloned()
                .collect::<Vec<_>>();
            if tickets.is_empty() && !config.contended {
                tickets = dataset
                    .tickets
                    .iter()
                    .filter(|ticket| {
                        ticket.organization_id == tenant.organization_id
                            && ticket.status == TicketStatus::Open
                    })
                    .cloned()
                    .collect();
            }
            tickets
        })
        .collect::<Vec<_>>();
    if write_tickets.iter().any(Vec::is_empty) {
        return Err("load driver requires at least one open ticket per tenant".to_owned());
    }
    let zipf = Arc::new(
        write_tickets
            .iter()
            .map(|tickets| Zipf::new(tickets.len(), config.zipf_s))
            .collect::<Vec<_>>(),
    );
    let weights = if config.saturate {
        WorkloadProfile::saturating_weights()
    } else {
        config.profile.weights()
    };
    let weight_sum: u32 = weights.iter().map(|(_, w)| *w).sum();
    if weight_sum == 0 {
        return Err("workload profile has zero weight".to_owned());
    }

    // sample_id_base lets multi-point sweeps avoid reusing write identities.
    let sample_counter = Arc::new(AtomicU64::new(config.sample_id_base.saturating_add(1)));
    // Shared control plane: measuring opens the record window; stop is the
    // single stop boundary every worker observes (no per-worker deadline skew).
    let measuring = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let factory = Arc::new(factory);
    let write_tickets = Arc::new(write_tickets);
    let probes = Arc::new(probes);
    let config = Arc::new(config);

    // Saturate expands each logical client into independent long-lived workers
    // (continuous concurrency). Wave-join fan-out cannot keep the channel full
    // for the 150 ms admission window because permits free at group drain.
    let fanout = if config.saturate {
        config.saturate_fanout.max(1)
    } else {
        1
    };
    let worker_ceiling = if config.saturate {
        RIFFDB_SATURATE_LOAD_CLIENTS.saturating_mul(SATURATE_DEFAULT_FANOUT)
    } else {
        RIFFDB_MAX_LOAD_CLIENTS
    };
    let worker_count = config
        .clients
        .saturating_mul(fanout)
        .clamp(1, worker_ceiling);

    let ready = Arc::new(std::sync::Barrier::new(worker_count + 1));
    let go = Arc::new(std::sync::Barrier::new(worker_count + 1));
    let mut workers = Vec::with_capacity(worker_count);

    for worker_id in 0..worker_count {
        let ready = Arc::clone(&ready);
        let go = Arc::clone(&go);
        let factory = Arc::clone(&factory);
        let write_tickets = Arc::clone(&write_tickets);
        let probes = Arc::clone(&probes);
        let config = Arc::clone(&config);
        let zipf = Arc::clone(&zipf);
        let sample_counter = Arc::clone(&sample_counter);
        let measuring = Arc::clone(&measuring);
        let stop = Arc::clone(&stop);
        let weights = weights.to_vec();
        workers.push(thread::spawn(
            move || -> Result<WorkerLoadResult, String> {
                let mut backend = factory()?;
                // Full statement/path prewarm even if the factory forgot — rare-weight
                // ops must not pay Parse/Describe inside the measured window.
                backend.prewarm().map_err(|error| error.to_string())?;
                let mut rng = XorShift64::new(
                    config.rng_seed ^ ((worker_id as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)),
                );
                // Extra touch so transport/auth is warm before the barrier.
                let warm_tenant = worker_id % probes.len();
                let _ = backend
                    .point_get_ticket(
                        probes[warm_tenant].organization_id,
                        probes[warm_tenant].ticket_id,
                    )
                    .map_err(|error| error.to_string())?;
                ready.wait();
                go.wait();

                let mut by_op: Vec<(LoadOp, OpStats)> = LoadOp::all()
                    .into_iter()
                    .map(|op| (op, OpStats::default()))
                    .collect();
                let mut last_comments = vec![None; probes.len()];
                let mut tenant_stats = vec![TenantLoadStats::default(); probes.len()];
                let mut burst_left = config.profile.burst_ops();
                let mut measure_start: Option<Instant> = None;
                let mut measure_end: Option<Instant> = None;
                let mut completed_operations = 0_u64;
                let mut last_completion: Option<Instant> = None;
                let mut longest_pause = Duration::ZERO;

                while !stop.load(Ordering::Acquire) {
                    let record = measuring.load(Ordering::Acquire);
                    let (op, replay) = draw_op(&weights, weight_sum, &mut rng, config.profile);
                    let tenant_index =
                        select_tenant_index(probes.len(), config.hot_tenant_percent, &mut rng);
                    let probe = &probes[tenant_index];
                    let write_ticket = select_write_ticket(
                        &write_tickets[tenant_index],
                        &zipf[tenant_index],
                        &mut rng,
                    );
                    // Re-check stop before starting work so we do not launch a new
                    // sample after the shared boundary (in-flight samples still finish).
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    let started = Instant::now();
                    let sample = sample_counter.fetch_add(1, Ordering::Relaxed);
                    // Under saturate every submission is a distinct in-flight
                    // command that contributes to coordinator depth (no replay).
                    let replay = if config.saturate { false } else { replay };
                    let (outcome, elapsed, error_text) = execute_op(
                        &mut backend,
                        probe,
                        op,
                        sample,
                        write_ticket,
                        replay,
                        &mut last_comments[tenant_index],
                    );
                    let ended = Instant::now();
                    if record {
                        if measure_start.is_none() {
                            measure_start = Some(started);
                        }
                        measure_end = Some(ended);
                        if let Some(previous) = last_completion {
                            longest_pause =
                                longest_pause.max(ended.saturating_duration_since(previous));
                        }
                        last_completion = Some(ended);
                        completed_operations = completed_operations.saturating_add(1);
                        tenant_stats[tenant_index].completed_operations = tenant_stats
                            [tenant_index]
                            .completed_operations
                            .saturating_add(1);
                        tenant_stats[tenant_index].latency.record(elapsed);
                        if let Some((_, stats)) =
                            by_op.iter_mut().find(|(candidate, _)| *candidate == op)
                        {
                            stats.record(elapsed, outcome, error_text);
                        }
                    }
                    if !config.saturate && config.profile.burst_ops() > 1 {
                        burst_left = burst_left.saturating_sub(1);
                        if burst_left == 0 {
                            thread::sleep(config.profile.think_time());
                            burst_left = config.profile.burst_ops();
                        }
                    }
                }

                Ok(WorkerLoadResult {
                    by_op,
                    measure_start,
                    measure_end,
                    completed_operations,
                    longest_pause,
                    tenant_stats,
                })
            },
        ));
    }

    ready.wait();
    go.wait();
    // Shared warmup: all workers already issuing discarded traffic.
    // On abort: store the reason, always signal stop, join every worker, then
    // return the error — never drop JoinHandles while workers still loop.
    let mut abort_reason: Option<String> = None;
    if !config.warmup.is_zero()
        && let Err(reason) = sleep_with_abort(config.warmup, abort.as_ref())
    {
        abort_reason = Some(reason);
    }
    if abort_reason.is_none() {
        measuring.store(true, Ordering::Release);
        if let Err(reason) = sleep_with_abort(config.duration, abort.as_ref()) {
            abort_reason = Some(reason);
        }
    }
    stop.store(true, Ordering::Release);

    let mut merged: Vec<(LoadOp, OpStats)> = LoadOp::all()
        .into_iter()
        .map(|op| (op, OpStats::default()))
        .collect();
    let mut worker_measure_intervals = Vec::with_capacity(worker_count);
    let mut worker_completed_operations = Vec::with_capacity(worker_count);
    let mut worker_longest_pause = Vec::with_capacity(worker_count);
    let mut tenant_stats = vec![TenantLoadStats::default(); config.tenant_count];
    let mut global_start: Option<Instant> = None;
    let mut global_end: Option<Instant> = None;
    let mut join_error: Option<String> = None;
    for worker in workers {
        match worker.join() {
            Ok(Ok(worker_result)) => {
                let interval = match (worker_result.measure_start, worker_result.measure_end) {
                    (Some(start), Some(end)) => {
                        global_start = Some(match global_start {
                            Some(existing) => existing.min(start),
                            None => start,
                        });
                        global_end = Some(match global_end {
                            Some(existing) => existing.max(end),
                            None => end,
                        });
                        end.saturating_duration_since(start)
                    }
                    _ => Duration::ZERO,
                };
                worker_measure_intervals.push(interval);
                worker_completed_operations.push(worker_result.completed_operations);
                worker_longest_pause.push(worker_result.longest_pause);
                for (destination, source) in tenant_stats
                    .iter_mut()
                    .zip(worker_result.tenant_stats.iter())
                {
                    destination.completed_operations = destination
                        .completed_operations
                        .saturating_add(source.completed_operations);
                    destination.latency.merge(&source.latency);
                }
                for (op, stats) in worker_result.by_op {
                    if let Some((_, dst)) =
                        merged.iter_mut().find(|(candidate, _)| *candidate == op)
                    {
                        dst.merge(&stats);
                    }
                }
            }
            Ok(Err(error)) => {
                if join_error.is_none() {
                    join_error = Some(error);
                }
            }
            Err(_) => {
                if join_error.is_none() {
                    join_error = Some("load worker panicked".to_owned());
                }
            }
        }
    }
    if let Some(reason) = abort_reason {
        return Err(reason);
    }
    if let Some(error) = join_error {
        return Err(error);
    }
    // Exact shared window covering every measured sample across all workers.
    let measured_elapsed = match (global_start, global_end) {
        (Some(start), Some(end)) => end
            .saturating_duration_since(start)
            .max(Duration::from_millis(1)),
        _ => config.duration.max(Duration::from_millis(1)),
    };
    let mut aggregate = OpStats::default();
    for (_, stats) in &merged {
        aggregate.merge(stats);
    }

    Ok(LoadReport {
        backend_id,
        config: (*config).clone(),
        execution_shape,
        measured_elapsed,
        worker_measure_intervals,
        seed_ns,
        by_op: merged,
        aggregate,
        write_completion_groups: None,
        open_loop: None,
        worker_completed_operations,
        worker_longest_pause,
        tenant_stats,
    })
}

#[derive(Clone, Copy, Debug)]
struct OpenLoopJob {
    scheduled_at: Instant,
    record: bool,
    op: LoadOp,
    replay: bool,
    sample: u64,
    tenant_index: usize,
    ticket_index: usize,
}

/// Runs a bounded open-loop Poisson arrival process.
///
/// The producer offers work independently of completion latency. A full
/// client queue is counted as a client-side rejection rather than blocking and
/// silently turning the workload back into a closed loop. Operation latency is
/// scheduled-arrival to completion; queue delay is reported separately.
pub fn run_open_loop_load<B, Factory>(
    backend_id: &'static str,
    config: LoadConfig,
    execution_shape: LoadExecutionShape,
    dataset: &SeedDataset,
    seed_ns: u64,
    factory: Factory,
) -> Result<LoadReport, String>
where
    B: AppBackend + Send + 'static,
    B::Error: Send + 'static,
    Factory: Fn() -> Result<B, String> + Send + Sync + 'static,
{
    let offered_rate = config
        .open_loop_rate_per_sec
        .ok_or_else(|| "open-loop load requires a positive offered rate".to_owned())?;
    if offered_rate == 0 || offered_rate > 1_000_000 {
        return Err("open-loop offered rate must be 1..=1000000 ops/s".to_owned());
    }
    if !(1..=65_536).contains(&config.open_loop_queue_depth) {
        return Err("open-loop queue depth must be 1..=65536".to_owned());
    }
    if !(1..=RIFFDB_MAX_LOAD_CLIENTS).contains(&config.clients) {
        return Err(format!(
            "open-loop clients must be 1..={RIFFDB_MAX_LOAD_CLIENTS}"
        ));
    }
    if config.duration.is_zero() {
        return Err("load duration must be positive".to_owned());
    }

    if !(1..=64).contains(&config.tenant_count) || config.hot_tenant_percent > 100 {
        return Err("open-loop tenant configuration is invalid".to_owned());
    }
    let probes = Arc::new(dataset.tenant_probes(config.tenant_count));
    if probes.len() != config.tenant_count {
        return Err(format!(
            "open-loop requested {} tenants but seed contains {}",
            config.tenant_count,
            probes.len()
        ));
    }
    let write_tickets = Arc::new(
        probes
            .iter()
            .map(|probe| {
                dataset
                    .tickets
                    .iter()
                    .filter(|ticket| {
                        ticket.organization_id == probe.organization_id
                            && ticket.status == TicketStatus::Open
                            && (config.contended || ticket.ticket_id != probe.ticket_id)
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
    );
    if write_tickets.iter().any(Vec::is_empty) {
        return Err("open-loop driver requires at least one open ticket per tenant".to_owned());
    }
    let zipf = write_tickets
        .iter()
        .map(|tickets| Zipf::new(tickets.len(), config.zipf_s))
        .collect::<Vec<_>>();
    let weights = config.profile.weights();
    let weight_sum: u32 = weights.iter().map(|(_, weight)| *weight).sum();
    if weight_sum == 0 {
        return Err("workload profile has zero weight".to_owned());
    }

    let (sender, receiver) = sync_channel::<OpenLoopJob>(config.open_loop_queue_depth);
    let receiver = Arc::new(std::sync::Mutex::new(receiver));
    let factory = Arc::new(factory);
    let ready = Arc::new(std::sync::Barrier::new(config.clients + 1));
    let go = Arc::new(std::sync::Barrier::new(config.clients + 1));
    let mut workers = Vec::with_capacity(config.clients);
    for worker_id in 0..config.clients {
        let receiver = Arc::clone(&receiver);
        let factory = Arc::clone(&factory);
        let ready = Arc::clone(&ready);
        let go = Arc::clone(&go);
        let probes = Arc::clone(&probes);
        let write_tickets = Arc::clone(&write_tickets);
        workers.push(thread::spawn(
            move || -> Result<(WorkerLoadResult, LatencyHistogram), String> {
                let mut backend = factory()?;
                backend.prewarm().map_err(|error| error.to_string())?;
                let warm_tenant = worker_id % probes.len();
                let _ = backend
                    .point_get_ticket(
                        probes[warm_tenant].organization_id,
                        probes[warm_tenant].ticket_id,
                    )
                    .map_err(|error| error.to_string())?;
                ready.wait();
                go.wait();
                let mut by_op: Vec<(LoadOp, OpStats)> = LoadOp::all()
                    .into_iter()
                    .map(|op| (op, OpStats::default()))
                    .collect();
                let mut queue_delay = LatencyHistogram::default();
                let mut last_comments = vec![None; probes.len()];
                let mut tenant_stats = vec![TenantLoadStats::default(); probes.len()];
                let mut measure_start = None;
                let mut measure_end = None;
                let mut completed_operations = 0_u64;
                let mut last_completion: Option<Instant> = None;
                let mut longest_pause = Duration::ZERO;
                loop {
                    let job = {
                        let locked = receiver
                            .lock()
                            .map_err(|_| "open-loop receiver lock poisoned".to_owned())?;
                        locked.recv()
                    };
                    let Ok(job) = job else {
                        break;
                    };
                    let service_start = Instant::now();
                    let queue_elapsed = service_start.saturating_duration_since(job.scheduled_at);
                    let probe = &probes[job.tenant_index];
                    let tenant_tickets = &write_tickets[job.tenant_index];
                    let ticket = &tenant_tickets[job.ticket_index.min(tenant_tickets.len() - 1)];
                    let (outcome, _, error_text) = execute_op(
                        &mut backend,
                        probe,
                        job.op,
                        job.sample,
                        ticket,
                        job.replay,
                        &mut last_comments[job.tenant_index],
                    );
                    let completed_at = Instant::now();
                    if job.record {
                        let end_to_end = completed_at.saturating_duration_since(job.scheduled_at);
                        queue_delay.record(queue_elapsed);
                        measure_start =
                            Some(measure_start.map_or(job.scheduled_at, |start: Instant| {
                                start.min(job.scheduled_at)
                            }));
                        measure_end = Some(
                            measure_end.map_or(completed_at, |end: Instant| end.max(completed_at)),
                        );
                        if let Some(previous) = last_completion {
                            longest_pause =
                                longest_pause.max(completed_at.saturating_duration_since(previous));
                        }
                        last_completion = Some(completed_at);
                        completed_operations = completed_operations.saturating_add(1);
                        tenant_stats[job.tenant_index].completed_operations = tenant_stats
                            [job.tenant_index]
                            .completed_operations
                            .saturating_add(1);
                        tenant_stats[job.tenant_index].latency.record(end_to_end);
                        if let Some((_, stats)) =
                            by_op.iter_mut().find(|(candidate, _)| *candidate == job.op)
                        {
                            stats.record(end_to_end, outcome, error_text);
                        }
                    }
                }
                Ok((
                    WorkerLoadResult {
                        by_op,
                        measure_start,
                        measure_end,
                        completed_operations,
                        longest_pause,
                        tenant_stats,
                    },
                    queue_delay,
                ))
            },
        ));
    }

    ready.wait();
    go.wait();
    let started = Instant::now();
    let measure_at = started + config.warmup;
    let stop_at = measure_at + config.duration;
    let mut next_arrival = started;
    let mut rng = XorShift64::new(config.rng_seed);
    let mut sample = config.sample_id_base.saturating_add(1);
    let mut offered = 0_u64;
    let mut admitted = 0_u64;
    let mut client_queue_rejected = 0_u64;
    while next_arrival < stop_at {
        let now = Instant::now();
        if next_arrival > now {
            thread::sleep(next_arrival.saturating_duration_since(now));
        }
        // Preserve the independent offered-arrival timestamp even when the
        // producer wakes late. Using `Instant::now()` here would erase producer
        // scheduling lag and reintroduce coordinated omission at saturation.
        let scheduled_at = next_arrival;
        let record = scheduled_at >= measure_at;
        let (op, replay) = draw_op(weights, weight_sum, &mut rng, config.profile);
        let tenant_index = select_tenant_index(probes.len(), config.hot_tenant_percent, &mut rng);
        let ticket_index = zipf[tenant_index].sample(&mut rng);
        let job = OpenLoopJob {
            scheduled_at,
            record,
            op,
            replay,
            sample,
            tenant_index,
            ticket_index,
        };
        sample = sample.saturating_add(1);
        if record {
            offered = offered.saturating_add(1);
        }
        match sender.try_send(job) {
            Ok(()) => {
                if record {
                    admitted = admitted.saturating_add(1);
                }
            }
            Err(TrySendError::Full(_)) => {
                if record {
                    client_queue_rejected = client_queue_rejected.saturating_add(1);
                }
            }
            Err(TrySendError::Disconnected(_)) => {
                return Err("open-loop workers disconnected".to_owned());
            }
        }
        let u = rng.gen_f64().clamp(f64::MIN_POSITIVE, 1.0 - f64::EPSILON);
        let inter_arrival_seconds = -u.ln() / offered_rate as f64;
        next_arrival += Duration::from_secs_f64(inter_arrival_seconds);
    }
    drop(sender);

    let mut merged: Vec<(LoadOp, OpStats)> = LoadOp::all()
        .into_iter()
        .map(|op| (op, OpStats::default()))
        .collect();
    let mut queue_delay = LatencyHistogram::default();
    let mut worker_measure_intervals = Vec::with_capacity(config.clients);
    let mut worker_completed_operations = Vec::with_capacity(config.clients);
    let mut worker_longest_pause = Vec::with_capacity(config.clients);
    let mut tenant_stats = vec![TenantLoadStats::default(); config.tenant_count];
    let mut global_start: Option<Instant> = None;
    let mut global_end: Option<Instant> = None;
    for worker in workers {
        let (result, worker_queue_delay) = worker
            .join()
            .map_err(|_| "open-loop worker panicked".to_owned())??;
        queue_delay.merge(&worker_queue_delay);
        let interval = match (result.measure_start, result.measure_end) {
            (Some(start), Some(end)) => {
                global_start = Some(global_start.map_or(start, |existing| existing.min(start)));
                global_end = Some(global_end.map_or(end, |existing| existing.max(end)));
                end.saturating_duration_since(start)
            }
            _ => Duration::ZERO,
        };
        worker_measure_intervals.push(interval);
        worker_completed_operations.push(result.completed_operations);
        worker_longest_pause.push(result.longest_pause);
        for (destination, source) in tenant_stats.iter_mut().zip(result.tenant_stats.iter()) {
            destination.completed_operations = destination
                .completed_operations
                .saturating_add(source.completed_operations);
            destination.latency.merge(&source.latency);
        }
        for (op, stats) in result.by_op {
            if let Some((_, destination)) =
                merged.iter_mut().find(|(candidate, _)| *candidate == op)
            {
                destination.merge(&stats);
            }
        }
    }
    let mut aggregate = OpStats::default();
    for (_, stats) in &merged {
        aggregate.merge(stats);
    }
    if aggregate.total_operations() != admitted {
        return Err(format!(
            "open-loop accounting mismatch: admitted={admitted}, completed={}",
            aggregate.total_operations()
        ));
    }

    let service_elapsed = match (global_start, global_end) {
        (Some(start), Some(end)) => end
            .saturating_duration_since(start)
            .max(Duration::from_millis(1)),
        _ => config.duration.max(Duration::from_millis(1)),
    };
    Ok(LoadReport {
        backend_id,
        config,
        execution_shape,
        measured_elapsed: service_elapsed,
        worker_measure_intervals,
        seed_ns,
        by_op: merged,
        aggregate,
        write_completion_groups: None,
        open_loop: Some(OpenLoopReport {
            offered_rate_per_sec: offered_rate,
            offered,
            admitted,
            client_queue_rejected,
            queue_delay,
            offered_window: stop_at.saturating_duration_since(measure_at),
        }),
        worker_completed_operations,
        worker_longest_pause,
        tenant_stats,
    })
}

fn draw_op(
    weights: &[(LoadOp, u32)],
    weight_sum: u32,
    rng: &mut XorShift64,
    profile: WorkloadProfile,
) -> (LoadOp, bool) {
    let mut pick = rng.gen_range(weight_sum);
    let mut selected = weights[0].0;
    for (op, weight) in weights {
        if pick < *weight {
            selected = *op;
            break;
        }
        pick -= *weight;
    }
    let replay = selected == LoadOp::CreateComment
        && profile.replay_basis_points() > 0
        && rng.gen_range(10_000) < profile.replay_basis_points();
    (selected, replay)
}

fn select_write_ticket<'a>(
    write_tickets: &'a [TicketRow],
    zipf: &Zipf,
    rng: &mut XorShift64,
) -> &'a TicketRow {
    let index = zipf.sample(rng);
    &write_tickets[index.min(write_tickets.len() - 1)]
}

fn select_tenant_index(tenant_count: usize, hot_tenant_percent: u8, rng: &mut XorShift64) -> usize {
    if tenant_count <= 1 {
        return 0;
    }
    if hot_tenant_percent > 0 && rng.gen_range(100) < u32::from(hot_tenant_percent) {
        return 0;
    }
    let start = usize::from(hot_tenant_percent > 0);
    start
        + usize::try_from(rng.gen_range(u32::try_from(tenant_count - start).unwrap_or(u32::MAX)))
            .unwrap_or(0)
}

fn execute_op<B: AppBackend>(
    backend: &mut B,
    probes: &ScenarioProbes,
    op: LoadOp,
    sample: u64,
    ticket: &TicketRow,
    replay: bool,
    last_comment: &mut Option<CommentSeed>,
) -> (OpOutcome, Duration, Option<String>) {
    let sample_usize = usize::try_from(sample).unwrap_or(usize::MAX);
    match op {
        LoadOp::PointGetTicket => {
            let (result, elapsed) =
                time_call(|| backend.point_get_ticket(probes.organization_id, probes.ticket_id));
            finish_read::<B>(result.map(|row| row.is_some()), elapsed)
        }
        LoadOp::PointGetUser => {
            let (result, elapsed) =
                time_call(|| backend.point_get_user(probes.organization_id, probes.user_id));
            finish_read::<B>(result.map(|row| row.is_some()), elapsed)
        }
        LoadOp::ListTicketsByProjectStatus => {
            let (result, elapsed) = time_call(|| {
                backend.list_tickets_by_project_status(
                    probes.organization_id,
                    probes.project_id,
                    TicketStatus::Open,
                    50,
                )
            });
            finish_unit::<B>(result.map(|_| ()), elapsed)
        }
        LoadOp::ListOpenTicketsForAssignee => {
            let (result, elapsed) = time_call(|| {
                backend.list_open_tickets_for_assignee(
                    probes.organization_id,
                    probes.assignee_id,
                    50,
                )
            });
            finish_unit::<B>(result.map(|_| ()), elapsed)
        }
        LoadOp::ListCommentsForTicket => {
            let (result, elapsed) = time_call(|| {
                backend.list_comments_for_ticket(probes.organization_id, probes.ticket_id, 50)
            });
            finish_unit::<B>(result.map(|_| ()), elapsed)
        }
        LoadOp::ListProjectMembers => {
            let (result, elapsed) = time_call(|| {
                backend.list_project_members(probes.organization_id, probes.project_id, 50)
            });
            finish_unit::<B>(result.map(|_| ()), elapsed)
        }
        LoadOp::TicketDetailPage => {
            let (result, elapsed) = time_call(|| {
                backend.ticket_detail_page(probes.organization_id, probes.ticket_id, 50)
            });
            finish_read::<B>(result.map(|page| page.is_some()), elapsed)
        }
        LoadOp::CreateComment => {
            let (input, is_replay) =
                select_comment_input(probes, ticket, sample, replay, last_comment.as_ref());
            let (result, elapsed) = if is_replay {
                time_call(|| backend.replay_comment(&input))
            } else {
                time_call(|| backend.create_comment(&input))
            };
            if is_replay {
                match result {
                    Ok(()) => (OpOutcome::Replayed, elapsed, None),
                    Err(error) => classify_typed::<B>(error, elapsed),
                }
            } else {
                if result.is_ok() {
                    *last_comment = Some(input);
                }
                finish_unit::<B>(result, elapsed)
            }
        }
        LoadOp::CloseTicketWithComment => {
            // Zipf-selected open ticket (same pool as CreateComment). Re-close is
            // valid — the command has no open-status requirement — so multi-ticket
            // targeting avoids serializing every client onto one row. Read probes
            // still use probes.ticket_id, which is excluded from the write pool.
            let input = CloseTicketWithCommentSeed {
                organization_id: ticket.organization_id,
                ticket_id: ticket.ticket_id,
                author_id: probes.write_author_id,
                comment_id: uuid_from_ordinal(NS_LOAD_WRITE, 2_000_000_000 + sample),
                body: format!("load close note {sample}"),
                idempotency_key: format!("load-close-{sample}"),
            };
            let (result, elapsed) = time_call(|| backend.close_ticket_with_comment(&input));
            finish_unit::<B>(result, elapsed)
        }
        LoadOp::SwapMemberRoles => {
            let input = probes.swap_member_roles(sample_usize);
            let input = SwapMemberRolesSeed {
                idempotency_key: format!("load-swap-{sample}"),
                ..input
            };
            let (result, elapsed) = time_call(|| backend.swap_member_roles(&input));
            finish_unit::<B>(result, elapsed)
        }
        LoadOp::OpenTicketWithLabels => {
            let input = OpenTicketWithLabelsSeed {
                organization_id: probes.organization_id,
                ticket_id: uuid_from_ordinal(NS_LOAD_WRITE, 3_000_000_000 + sample),
                project_id: probes.write_project_id,
                reporter_id: probes.write_author_id,
                assignee_id: probes.write_assignee_id,
                title: format!("load open ticket {sample}"),
                label_a: probes.write_label_a,
                label_b: probes.write_label_b,
                idempotency_key: format!("load-open-{sample}"),
            };
            let (result, elapsed) = time_call(|| backend.open_ticket_with_labels(&input));
            finish_unit::<B>(result, elapsed)
        }
    }
}

fn comment_for(probes: &ScenarioProbes, ticket: &TicketRow, sample: u64) -> CommentSeed {
    CommentSeed {
        row: CommentRow {
            organization_id: ticket.organization_id,
            comment_id: uuid_from_ordinal(NS_LOAD_WRITE, 1_000_000_000 + sample),
            ticket_id: ticket.ticket_id,
            author_id: probes.write_author_id,
            body: format!("load comment {sample}"),
        },
        idempotency_key: format!("load-comment-{sample}"),
    }
}

fn select_comment_input(
    probes: &ScenarioProbes,
    ticket: &TicketRow,
    sample: u64,
    replay_requested: bool,
    previous: Option<&CommentSeed>,
) -> (CommentSeed, bool) {
    match (replay_requested, previous) {
        (true, Some(previous)) => (previous.clone(), true),
        _ => (comment_for(probes, ticket, sample), false),
    }
}

fn finish_unit<B: AppBackend>(
    result: Result<(), B::Error>,
    elapsed: Duration,
) -> (OpOutcome, Duration, Option<String>) {
    match result {
        Ok(()) => (OpOutcome::Success, elapsed, None),
        Err(error) => classify_typed::<B>(error, elapsed),
    }
}

fn finish_read<B: AppBackend>(
    result: Result<bool, B::Error>,
    elapsed: Duration,
) -> (OpOutcome, Duration, Option<String>) {
    match result {
        Ok(true) => (OpOutcome::Success, elapsed, None),
        Ok(false) => (OpOutcome::Error, elapsed, Some("EMPTY_READ".to_owned())),
        Err(error) => classify_typed::<B>(error, elapsed),
    }
}

/// Maps a backend error using typed codes only (`RDB-*` or SQLSTATE).
///
/// Never classifies from free-form prose. `first_error` stores the machine code
/// when known, otherwise a stable fallback token.
fn classify_typed<B: AppBackend>(
    error: B::Error,
    elapsed: Duration,
) -> (OpOutcome, Duration, Option<String>) {
    let outcome = match B::load_error_class(&error) {
        LoadErrorClass::Conflict => OpOutcome::Conflict,
        LoadErrorClass::IdempotencyMismatch => OpOutcome::IdempotencyMismatch,
        LoadErrorClass::Unavailable => OpOutcome::Unavailable,
        LoadErrorClass::Overloaded => OpOutcome::Overloaded,
        LoadErrorClass::HistoryIncarnationMismatch => OpOutcome::HistoryIncarnationMismatch,
        LoadErrorClass::Other => OpOutcome::Error,
    };
    let code = B::load_error_code(&error)
        .map(str::to_owned)
        .unwrap_or_else(|| "UNCLASSIFIED".to_owned());
    (outcome, elapsed, Some(code))
}

/// Deterministic xorshift64* RNG (no extra dependency).
#[derive(Clone, Debug)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self {
            state: seed | 1, // avoid zero state
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn gen_range(&mut self, max_exclusive: u32) -> u32 {
        if max_exclusive == 0 {
            return 0;
        }
        (self.next_u64() % u64::from(max_exclusive)) as u32
    }

    fn gen_f64(&mut self) -> f64 {
        // [0, 1)
        (self.next_u64() as f64) / ((u64::MAX as f64) + 1.0)
    }
}

/// Precomputed Zipf sampler over `1..=n` ranks.
#[derive(Clone, Debug)]
struct Zipf {
    /// Cumulative probability mass, length n.
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(n: usize, s: f64) -> Self {
        assert!(n > 0);
        if s <= 0.0 {
            let step = 1.0 / n as f64;
            let cdf = (1..=n).map(|i| step * i as f64).collect();
            return Self { cdf };
        }
        let mut weights = Vec::with_capacity(n);
        let mut total = 0.0;
        for rank in 1..=n {
            let w = 1.0 / (rank as f64).powf(s);
            weights.push(w);
            total += w;
        }
        let mut cdf = Vec::with_capacity(n);
        let mut run = 0.0;
        for w in weights {
            run += w / total;
            cdf.push(run);
        }
        if let Some(last) = cdf.last_mut() {
            *last = 1.0;
        }
        Self { cdf }
    }

    fn sample(&self, rng: &mut XorShift64) -> usize {
        let u = rng.gen_f64();
        // First CDF edge >= u (equivalent to linear position with u <= *edge).
        let index = self.cdf.partition_point(|edge| *edge < u);
        index.min(self.cdf.len() - 1)
    }
}

/// Prints a compact human summary of a load report.
pub fn print_load_summary(report: &LoadReport) {
    let elapsed_s = report.measured_elapsed.as_secs_f64().max(0.001);
    let total = report.aggregate.total_operations();
    let thr = total as f64 / elapsed_s;
    let tenant_scope = if report.config.tenant_count == 1 {
        "single_organization"
    } else {
        "multiple_organizations"
    };
    println!(
        "\n== load {} profile={} clients={} window={:.1}s tenant={} count={} hot={}pct ==",
        report.backend_id,
        report.config.profile.as_str(),
        report.config.clients,
        elapsed_s,
        tenant_scope,
        report.config.tenant_count,
        report.config.hot_tenant_percent,
    );
    println!(
        "throughput={thr:.0} ops/s  logical_ops={total}  success={}  conflict={}  idempotency_mismatch={}  unavailable={}  overloaded={}  replayed={}  error={}",
        report.aggregate.success,
        report.aggregate.conflict,
        report.aggregate.idempotency_mismatch,
        report.aggregate.unavailable,
        report.aggregate.overloaded,
        report.aggregate.replayed,
        report.aggregate.error
    );
    println!(
        "latency p50={:.3}ms p95={:.3}ms p99={:.3}ms max={:.3}ms",
        report.aggregate.latency.percentile_ns(50) as f64 / 1e6,
        report.aggregate.latency.percentile_ns(95) as f64 / 1e6,
        report.aggregate.latency.percentile_ns(99) as f64 / 1e6,
        report.aggregate.latency.summary_json()["max_ns"]
            .as_u64()
            .unwrap_or(0) as f64
            / 1e6
    );
    for (op, stats) in &report.by_op {
        if stats.total_operations() == 0 {
            continue;
        }
        print!(
            "  {}: n={} p50={:.3}ms p99={:.3}ms ok={} conflict={} idempotency_mismatch={} unavailable={} replay={} err={}",
            op.as_str(),
            stats.total_operations(),
            stats.latency.percentile_ns(50) as f64 / 1e6,
            stats.latency.percentile_ns(99) as f64 / 1e6,
            stats.success,
            stats.conflict,
            stats.idempotency_mismatch,
            stats.unavailable,
            stats.replayed,
            stats.error
        );
        if let Some(first) = &stats.first_error {
            print!(" first_error={first}");
        }
        println!();
    }
}

/// One point on a concurrency-sweep curve (JSON-friendly).
#[must_use]
pub fn concurrency_curve_point(report: &LoadReport) -> serde_json::Value {
    let elapsed_ns = u64::try_from(report.measured_elapsed.as_nanos()).unwrap_or(u64::MAX);
    let logical_ops = report.aggregate.total_operations();
    let throughput = logical_ops
        .saturating_mul(1_000_000_000)
        .checked_div(elapsed_ns.max(1))
        .unwrap_or(0);
    let write_p50 = |op: LoadOp| -> Option<u64> {
        report
            .by_op
            .iter()
            .find(|(candidate, stats)| *candidate == op && stats.total_operations() > 0)
            .map(|(_, stats)| stats.latency.percentile_ns(50))
    };
    serde_json::json!({
        "backend_id": report.backend_id,
        "profile": report.config.profile.as_str(),
        "clients": report.config.clients,
        "measured_elapsed_ns": elapsed_ns,
        "logical_ops": logical_ops,
        "throughput_ops_s": throughput,
        "aggregate_p50_ns": report.aggregate.latency.percentile_ns(50),
        "aggregate_p95_ns": report.aggregate.latency.percentile_ns(95),
        "aggregate_p99_ns": report.aggregate.latency.percentile_ns(99),
        "create_comment_p50_ns": write_p50(LoadOp::CreateComment),
        "close_ticket_with_comment_p50_ns": write_p50(LoadOp::CloseTicketWithComment),
        "open_ticket_with_labels_p50_ns": write_p50(LoadOp::OpenTicketWithLabels),
        "outcomes": {
            "success": report.aggregate.success,
            "conflict": report.aggregate.conflict,
            "unavailable": report.aggregate.unavailable,
            "error": report.aggregate.error,
        },
    })
}

/// Coordinator sleep sliced at 250 ms so a dead peer aborts mid-window.
fn sleep_with_abort(
    total: Duration,
    abort: Option<&std::sync::Arc<dyn Fn() -> Option<String> + Send + Sync>>,
) -> Result<(), String> {
    const SLICE: Duration = Duration::from_millis(250);
    let deadline = Instant::now() + total;
    loop {
        if let Some(hook) = abort
            && let Some(reason) = hook()
        {
            return Err(format!("backend died mid-load: {reason}"));
        }
        let now = Instant::now();
        if now >= deadline {
            return Ok(());
        }
        let remaining = deadline.saturating_duration_since(now);
        thread::sleep(remaining.min(SLICE));
    }
}

/// Modal group size and share of commands in groups larger than one.
#[must_use]
pub fn write_group_summary(groups: &[u64]) -> (Option<usize>, f64) {
    let mut total = 0_u64;
    let mut multi = 0_u64;
    let mut modal_size = None;
    let mut modal_count = 0_u64;
    for (index, count) in groups.iter().enumerate() {
        let size = index + 1;
        total = total.saturating_add(*count);
        if size > 1 {
            multi = multi.saturating_add(*count);
        }
        if *count > modal_count {
            modal_count = *count;
            modal_size = Some(size);
        }
    }
    let grouped_fraction = if total == 0 {
        0.0
    } else {
        multi as f64 / total as f64
    };
    (modal_size, grouped_fraction)
}

/// Prints a concurrency-sweep table (throughput and p50 vs client count).
pub fn print_concurrency_sweep_summary(points: &[serde_json::Value]) {
    if points.is_empty() {
        return;
    }
    println!("\n== concurrency sweep curve ==");
    let has_groups = points.iter().any(|p| {
        p.get("write_completion_groups_by_size")
            .and_then(|v| v.as_array())
            .is_some()
    });
    if has_groups {
        println!(
            "{:<22} {:>8} {:>12} {:>10} {:>10} {:>12} {:>8} {:>10}",
            "backend",
            "clients",
            "ops/s",
            "p50_ms",
            "p99_ms",
            "write_p50_ms",
            "modal_g",
            "grp_frac"
        );
    } else {
        println!(
            "{:<22} {:>8} {:>12} {:>10} {:>10} {:>12}",
            "backend", "clients", "ops/s", "p50_ms", "p99_ms", "write_p50_ms"
        );
    }
    for point in points {
        let backend = point["backend_id"].as_str().unwrap_or("?");
        let clients = point["clients"].as_u64().unwrap_or(0);
        let thr = point["throughput_ops_s"].as_u64().unwrap_or(0);
        let p50 = point["aggregate_p50_ns"].as_u64().unwrap_or(0) as f64 / 1e6;
        let p99 = point["aggregate_p99_ns"].as_u64().unwrap_or(0) as f64 / 1e6;
        let write_p50 = point["create_comment_p50_ns"].as_u64().unwrap_or(0) as f64 / 1e6;
        if has_groups {
            let (modal, frac) = point
                .get("write_completion_groups_by_size")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    let groups: Vec<u64> = arr.iter().filter_map(|x| x.as_u64()).collect();
                    write_group_summary(&groups)
                })
                .unwrap_or((None, 0.0));
            let modal_s = modal
                .map(|m| m.to_string())
                .unwrap_or_else(|| "-".to_owned());
            println!(
                "{backend:<22} {clients:>8} {thr:>12} {p50:>10.3} {p99:>10.3} {write_p50:>12.3} {modal_s:>8} {frac:>10.3}"
            );
        } else {
            println!(
                "{backend:<22} {clients:>8} {thr:>12} {p50:>10.3} {p99:>10.3} {write_p50:>12.3}"
            );
        }
    }
    // Relative thr vs each backend's 1-client point when present.
    let backend_ids = points
        .iter()
        .filter_map(|point| point["backend_id"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for backend in backend_ids {
        let series: Vec<_> = points
            .iter()
            .filter(|p| p["backend_id"].as_str() == Some(backend))
            .collect();
        if series.len() < 2 {
            continue;
        }
        let base = series[0]["throughput_ops_s"].as_u64().unwrap_or(0).max(1) as f64;
        let mut parts = Vec::new();
        for point in &series {
            let clients = point["clients"].as_u64().unwrap_or(0);
            let thr = point["throughput_ops_s"].as_u64().unwrap_or(0) as f64;
            parts.push(format!("{clients}→{:.2}×", thr / base));
        }
        println!("{backend} thr vs c=1: {}", parts.join("  "));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrency_sweep_points_are_monotonic() {
        let points = LOAD_CONCURRENCY_SWEEP_CLIENTS;
        assert_eq!(points, &[1, 8, 32, 128]);
        for window in points.windows(2) {
            assert!(window[0] < window[1]);
        }
    }

    #[test]
    fn zipf_prefers_low_ranks_when_skewed() {
        let zipf = Zipf::new(100, 1.2);
        let mut rng = XorShift64::new(7);
        let mut hits = [0_u32; 100];
        for _ in 0..10_000 {
            hits[zipf.sample(&mut rng)] += 1;
        }
        assert!(hits[0] > hits[50]);
        assert!(hits[0] > hits[99]);
    }

    #[test]
    fn interactive_weights_are_mostly_reads() {
        let weights = WorkloadProfile::Interactive.weights();
        let reads: u32 = weights
            .iter()
            .filter(|(op, _)| {
                !matches!(
                    op,
                    LoadOp::CreateComment
                        | LoadOp::CloseTicketWithComment
                        | LoadOp::SwapMemberRoles
                        | LoadOp::OpenTicketWithLabels
                )
            })
            .map(|(_, w)| *w)
            .sum();
        let total: u32 = weights.iter().map(|(_, w)| *w).sum();
        assert!(reads * 100 / total >= 70);
    }

    #[test]
    fn diagnostic_profiles_isolate_reads_and_writes() {
        let is_write = |op: LoadOp| {
            matches!(
                op,
                LoadOp::CreateComment
                    | LoadOp::CloseTicketWithComment
                    | LoadOp::SwapMemberRoles
                    | LoadOp::OpenTicketWithLabels
            )
        };
        assert!(
            WorkloadProfile::ReadOnly
                .weights()
                .iter()
                .all(|(op, _)| !is_write(*op))
        );
        assert!(
            WorkloadProfile::WriteOnly
                .weights()
                .iter()
                .all(|(op, _)| is_write(*op))
        );
        assert_eq!(
            WorkloadProfile::parse("read_only"),
            Some(WorkloadProfile::ReadOnly)
        );
        assert_eq!(
            WorkloadProfile::parse("write_only"),
            Some(WorkloadProfile::WriteOnly)
        );
        assert_eq!(
            WorkloadProfile::AppendOnly.weights(),
            &[(LoadOp::CreateComment, 1)]
        );
        assert_eq!(
            WorkloadProfile::parse("append_only"),
            Some(WorkloadProfile::AppendOnly)
        );
    }

    #[test]
    fn default_profiles_omit_swap_member_roles() {
        for profile in [WorkloadProfile::Interactive, WorkloadProfile::Agent] {
            assert!(
                !profile
                    .weights()
                    .iter()
                    .any(|(op, _)| *op == LoadOp::SwapMemberRoles),
                "{:?} must not include SwapMemberRoles",
                profile
            );
        }
        assert!(
            WorkloadProfile::MembershipContention
                .weights()
                .iter()
                .any(|(op, _)| *op == LoadOp::SwapMemberRoles)
        );
    }

    #[test]
    fn zipf_partition_point_covers_full_range() {
        let zipf = Zipf::new(10, 0.0);
        let mut rng = XorShift64::new(1);
        let mut seen = [false; 10];
        for _ in 0..1_000 {
            seen[zipf.sample(&mut rng)] = true;
        }
        assert!(seen.iter().all(|hit| *hit));
    }

    #[test]
    fn replay_without_prior_success_is_a_fresh_create() {
        let dataset = SeedDataset::generate(crate::Scale::smoke());
        let probes = dataset.probes();
        let ticket = dataset
            .tickets
            .iter()
            .find(|ticket| ticket.ticket_id == probes.ticket_id)
            .expect("probe ticket");
        let (input, replayed) = select_comment_input(&probes, ticket, 42, true, None);
        assert!(!replayed);
        assert_eq!(input.idempotency_key, "load-comment-42");

        let (replay, replayed) = select_comment_input(&probes, ticket, 43, true, Some(&input));
        assert!(replayed);
        assert_eq!(replay, input);
    }

    #[test]
    fn abort_hook_interrupts_sliced_sleep_within_two_seconds() {
        let started = Instant::now();
        let abort: std::sync::Arc<dyn Fn() -> Option<String> + Send + Sync> =
            std::sync::Arc::new({
                let start = Instant::now();
                move || {
                    if start.elapsed() >= Duration::from_millis(100) {
                        Some("unit-test-kill".to_owned())
                    } else {
                        None
                    }
                }
            });
        let result = sleep_with_abort(Duration::from_secs(30), Some(&abort));
        assert!(result.is_err(), "expected abort error, got {result:?}");
        let err = result.unwrap_err();
        assert!(
            err.contains("backend died mid-load"),
            "unexpected message: {err}"
        );
        assert!(
            started.elapsed() <= Duration::from_secs(2),
            "abort took too long: {:?}",
            started.elapsed()
        );
    }

    /// Stub backend that succeeds instantly so workers spin until `stop`.
    struct CountingBackend {
        ops: Arc<AtomicU64>,
    }

    impl AppBackend for CountingBackend {
        type Error = String;

        fn reset(&mut self) -> Result<(), Self::Error> {
            Ok(())
        }
        fn seed(&mut self, _: &SeedDataset) -> Result<(), Self::Error> {
            Ok(())
        }
        fn load_error_class(_: &Self::Error) -> LoadErrorClass {
            LoadErrorClass::Other
        }
        fn load_error_code(_: &Self::Error) -> Option<&str> {
            None
        }
        fn point_get_ticket(
            &mut self,
            _: crate::UuidBytes,
            _: crate::UuidBytes,
        ) -> Result<Option<crate::TicketRow>, Self::Error> {
            self.ops.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }
        fn point_get_user(
            &mut self,
            _: crate::UuidBytes,
            _: crate::UuidBytes,
        ) -> Result<Option<crate::UserRow>, Self::Error> {
            self.ops.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }
        fn list_tickets_by_project_status(
            &mut self,
            _: crate::UuidBytes,
            _: crate::UuidBytes,
            _: crate::TicketStatus,
            _: u32,
        ) -> Result<Vec<crate::TicketRow>, Self::Error> {
            self.ops.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }
        fn list_open_tickets_for_assignee(
            &mut self,
            _: crate::UuidBytes,
            _: crate::UuidBytes,
            _: u32,
        ) -> Result<Vec<crate::TicketRow>, Self::Error> {
            self.ops.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }
        fn list_comments_for_ticket(
            &mut self,
            _: crate::UuidBytes,
            _: crate::UuidBytes,
            _: u32,
        ) -> Result<Vec<crate::CommentRow>, Self::Error> {
            self.ops.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }
        fn list_project_members(
            &mut self,
            _: crate::UuidBytes,
            _: crate::UuidBytes,
            _: u32,
        ) -> Result<Vec<crate::ProjectMemberRow>, Self::Error> {
            self.ops.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }
        fn ticket_detail_page(
            &mut self,
            _: crate::UuidBytes,
            _: crate::UuidBytes,
            _: u32,
        ) -> Result<Option<crate::TicketDetailPage>, Self::Error> {
            self.ops.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }
        fn board_page(
            &mut self,
            _: crate::UuidBytes,
            _: crate::UuidBytes,
            _: crate::TicketStatus,
            _: u32,
        ) -> Result<Vec<crate::TicketRow>, Self::Error> {
            self.ops.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }
        fn board_page_projected(
            &mut self,
            _: crate::UuidBytes,
            _: crate::UuidBytes,
            _: crate::TicketStatus,
            _: u32,
        ) -> Result<Vec<crate::TicketRow>, Self::Error> {
            self.ops.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }
        fn create_comment(&mut self, _: &crate::CommentSeed) -> Result<(), Self::Error> {
            self.ops.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn replay_comment(&mut self, _: &crate::CommentSeed) -> Result<(), Self::Error> {
            self.ops.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn close_ticket_with_comment(
            &mut self,
            _: &crate::CloseTicketWithCommentSeed,
        ) -> Result<(), Self::Error> {
            self.ops.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn swap_member_roles(&mut self, _: &crate::SwapMemberRolesSeed) -> Result<(), Self::Error> {
            self.ops.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn open_ticket_with_labels(
            &mut self,
            _: &crate::OpenTicketWithLabelsSeed,
        ) -> Result<(), Self::Error> {
            self.ops.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn abort_joins_workers_before_returning() {
        let ops = Arc::new(AtomicU64::new(0));
        let started = Instant::now();
        let abort: std::sync::Arc<dyn Fn() -> Option<String> + Send + Sync> =
            std::sync::Arc::new({
                let start = Instant::now();
                move || {
                    if start.elapsed() >= Duration::from_millis(150) {
                        Some("unit-test-abort".to_owned())
                    } else {
                        None
                    }
                }
            });
        let dataset = SeedDataset::generate(crate::Scale::smoke());
        let mut config = LoadConfig::smoke(WorkloadProfile::Interactive);
        config.clients = 4;
        config.duration = Duration::from_secs(30);
        config.warmup = Duration::ZERO;
        let ops_factory = Arc::clone(&ops);
        let result = run_closed_loop_load_with_abort(
            "stub",
            config,
            LoadExecutionShape {
                transport_topology: "test",
                command_attempt_budget: 1,
            },
            &dataset,
            0,
            move || {
                Ok(CountingBackend {
                    ops: Arc::clone(&ops_factory),
                })
            },
            Some(abort),
        );
        assert!(result.is_err(), "expected abort, got {result:?}");
        assert!(
            started.elapsed() <= Duration::from_secs(2),
            "abort+join took {:?}",
            started.elapsed()
        );
        let after_return = ops.load(Ordering::Relaxed);
        thread::sleep(Duration::from_millis(80));
        let later = ops.load(Ordering::Relaxed);
        assert_eq!(
            after_return, later,
            "workers kept issuing ops after abort returned (detach bug)"
        );
        assert!(after_return > 0, "workers should have done some work");
    }

    #[test]
    fn standard_load_window_is_evidentiary() {
        let config = LoadConfig::standard(WorkloadProfile::Interactive, 8);
        assert_eq!(config.duration, Duration::from_secs(90));
        assert_eq!(config.warmup, Duration::from_secs(15));
    }

    #[test]
    fn open_loop_counts_offered_admitted_completed_and_queue_delay() {
        let ops = Arc::new(AtomicU64::new(0));
        let dataset = SeedDataset::generate(crate::Scale::smoke());
        let mut config = LoadConfig::smoke(WorkloadProfile::Interactive);
        config.clients = 2;
        config.duration = Duration::from_millis(120);
        config.warmup = Duration::from_millis(10);
        config.open_loop_rate_per_sec = Some(200);
        config.open_loop_queue_depth = 16;
        let factory_ops = Arc::clone(&ops);
        let report = run_open_loop_load(
            "stub",
            config,
            LoadExecutionShape {
                transport_topology: "test",
                command_attempt_budget: 1,
            },
            &dataset,
            0,
            move || {
                Ok(CountingBackend {
                    ops: Arc::clone(&factory_ops),
                })
            },
        )
        .expect("open loop");
        let arrival = report.open_loop.expect("arrival report");
        assert!(arrival.offered > 0);
        assert_eq!(
            arrival.offered,
            arrival
                .admitted
                .saturating_add(arrival.client_queue_rejected)
        );
        assert_eq!(arrival.admitted, report.aggregate.total_operations());
        assert_eq!(arrival.queue_delay.total(), arrival.admitted);
    }

    #[test]
    fn multi_tenant_load_accounts_every_completed_operation() {
        let ops = Arc::new(AtomicU64::new(0));
        let mut scale = crate::Scale::smoke();
        scale.organizations = 4;
        let dataset = SeedDataset::generate(scale);
        let mut config = LoadConfig::smoke(WorkloadProfile::Interactive);
        config.clients = 4;
        config.duration = Duration::from_millis(100);
        config.warmup = Duration::ZERO;
        config.tenant_count = 4;
        config.hot_tenant_percent = 70;
        let factory_ops = Arc::clone(&ops);
        let report = run_closed_loop_load(
            "stub",
            config,
            LoadExecutionShape {
                transport_topology: "test",
                command_attempt_budget: 1,
            },
            &dataset,
            0,
            move || {
                Ok(CountingBackend {
                    ops: Arc::clone(&factory_ops),
                })
            },
        )
        .expect("multi-tenant load");
        assert_eq!(report.tenant_stats.len(), 4);
        assert_eq!(
            report
                .tenant_stats
                .iter()
                .map(|tenant| tenant.completed_operations)
                .sum::<u64>(),
            report.aggregate.total_operations()
        );
        assert!(report.tenant_stats[0].completed_operations > 0);
    }

    #[test]
    fn tenant_selector_is_bounded_for_uniform_and_hot_modes() {
        let mut rng = XorShift64::new(7);
        for hot_percent in [0, 70, 100] {
            for _ in 0..1_000 {
                assert!(select_tenant_index(8, hot_percent, &mut rng) < 8);
            }
        }
        for _ in 0..100 {
            assert_eq!(select_tenant_index(8, 100, &mut rng), 0);
        }
        let mut rng = XorShift64::new(99);
        let hot = (0..10_000)
            .filter(|_| select_tenant_index(8, 70, &mut rng) == 0)
            .count();
        assert!((6_700..=7_300).contains(&hot), "hot tenant draws={hot}");
    }

    #[test]
    fn write_group_summary_reports_modal_and_grouped_fraction() {
        let mut groups = [0_u64; 64];
        groups[0] = 10; // size 1
        groups[3] = 30; // size 4 modal
        groups[7] = 10; // size 8
        let (modal, frac) = write_group_summary(&groups);
        assert_eq!(modal, Some(4));
        assert!((frac - 0.8).abs() < 1e-9);
    }
}
