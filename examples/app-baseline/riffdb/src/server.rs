//! Process harness that starts a real `riffdbd` for the app baseline.

use std::collections::VecDeque;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::SocketAddr;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use riffdb_auth::bootstrap_secret::{
    BootstrapCredential as RetainedBootstrapCredential, SystemEntropy,
    generate_bootstrap_credential, load_bootstrap_credential_file,
};
use riffdb_client_rust::{
    BearerCredential, BootstrapCallMetadata, BootstrapCredential as TransportBootstrapCredential,
    CallMetadata, RiffDbClient, app_v1, generate_capability_id, generate_request_id, v1,
};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    ApplicationManifest, CompiledApplicationRole, NamedQuerySource, QueryModule,
    QueryModuleCandidate, QueryModuleName, QueryModuleVersion, compile_application_role,
};
use riffdb_types::{
    CapabilityGrantV1, CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1,
    PartitionScopeV1, TenantScope,
};
use tokio::time::timeout;
use tonic::transport::Endpoint;

use crate::projected::TicketStatusEnumIds;
use crate::{RiffDbError, RiffDbPublicBackend};

/// TOML body registering the board columnar projection (ADR-0086 config form).
const BOARD_PROJECTIONS_TOML: &str = r#"
[[projections]]
name = "board"
entity = "Ticket"
projected_fields = ["project_id", "reporter_id", "assignee_id", "status", "title"]
org_scope_field = "organization_id"
"#;

const AUDIENCE: &str = "riffdb-grpc-loopback";
const ENVIRONMENT: &str = "app-baseline";
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const EXCLUSIVE_DIAGNOSTIC_PREFIX: &str = "riffdb-private-wp670-v3\t";
const WRITE_GROUP_PREFIX: &str = "riffdb-write-completion-groups-v1\t";
const WRITE_GROUP_BUCKETS: usize = riffdb_storage_redb::benchmark_support::MAX_GROUP_COMMANDS;
const DISPATCH_REASON_PREFIX: &str = "riffdb-dispatch-reasons-v1\t";
const READ_STAGE_PREFIX: &str = "riffdb-read-stages-v1\t";
const WRITE_SERVICE_STAGE_PREFIX: &str = "riffdb-write-service-stages-v1\t";
const COMMAND_STAGE_PREFIX: &str = "riffdb-command-stages-v1\t";
const WRITER_EVIDENCE_PREFIX: &str = "riffdb-writer-evidence-v1\t";
const WRITER_FRAME_CENSUS_PREFIX: &str = "riffdb-writer-frame-census-v1\t";
const WRITER_FLUSH_CENSUS_PREFIX: &str = "riffdb-writer-flush-census-v1\t";
const WRITER_JOURNAL_STAGES_PREFIX: &str = "riffdb-writer-journal-stages-v1\t";
const WRITER_PUBLICATION_STAGES_PREFIX: &str = "riffdb-writer-publication-stages-v1\t";
const COMPLETION_LANE_PREFIX: &str = "riffdb-completion-lane-v1\t";
const QUERY_EXECUTE_WINDOWS_PREFIX: &str = "riffdb-query-execute-windows-v1\t";
const SHUTDOWN_STAGES_PREFIX: &str = "riffdb-shutdown-stages-v1\t";
const PROCESS_START_TIMEOUT: Duration = Duration::from_secs(90);
const PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(15);
/// Keep the last N stderr lines for crash diagnosis (panic / OOM messages).
const STDERR_RING_LINES: usize = 200;
/// Soft byte cap for the retained stderr ring (in addition to the line cap).
const STDERR_RING_BYTES: usize = 64 * 1024;
const STDERR_DETAIL_CHARS: usize = 4_000;
const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const BOOTSTRAP_UNIX_MILLISECONDS: u64 = 1_700_000_000_000;
const CAPABILITY_LIFETIME_SECONDS: u32 = 3_600;
const CONTRACT_LINEAGE: &str = "TicketDesk";
const CONTRACT_VERSION: u64 = 1;
const APPLICATION_ROLE_NAME: &str = "TicketDeskApplication";
const TICKETDESK_CONTRACT: &str = include_str!("../../contracts/ticketdesk.riff");
const TICKETDESK_MANIFEST: &str =
    include_str!("../../../../fixtures/application-manifests/ticketdesk-v1.json");
const MODULE_NAME: &str = "ticketdesk";
const MODULE_VERSION: u64 = 1;
const QUERY_SOURCES: &[(&str, &str)] = &[
    (
        "BoardPage200",
        include_str!("../../../../queries/ticketdesk/board_page_200.riffq"),
    ),
    (
        "BoardPage450",
        include_str!("../../../../queries/ticketdesk/board_page_450.riffq"),
    ),
    (
        "BoardPage50",
        include_str!("../../../../queries/ticketdesk/board_page_50.riffq"),
    ),
    (
        "GetTicket",
        include_str!("../../../../queries/ticketdesk/get_ticket.riffq"),
    ),
    (
        "GetUser",
        include_str!("../../../../queries/ticketdesk/get_user.riffq"),
    ),
    (
        "ListComments",
        include_str!("../../../../queries/ticketdesk/list_comments.riffq"),
    ),
    (
        "ListTickets",
        include_str!("../../../../queries/ticketdesk/list_tickets.riffq"),
    ),
    (
        "ListTicketsByAssignee",
        include_str!("../../../../queries/ticketdesk/list_tickets_by_assignee.riffq"),
    ),
    (
        "ProjectMembers",
        include_str!("../../../../queries/ticketdesk/project_members.riffq"),
    ),
    (
        "ProjectSummary",
        include_str!("../../../../queries/ticketdesk/project_summary.riffq"),
    ),
    (
        "TicketPage",
        include_str!("../../../../queries/ticketdesk/ticket_page.riffq"),
    ),
    (
        "TicketPagePaged",
        include_str!("../../../../queries/ticketdesk/ticket_page_paged.riffq"),
    ),
    (
        "TicketQueue",
        include_str!("../../../../queries/ticketdesk/ticket_queue.riffq"),
    ),
];
const CAPABILITY_KEY_DOCUMENT: &[u8] =
    b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEY_DOCUMENT: &[u8] =
    b"riffdb-idempotency-digest-keys-v1\n9:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";

/// Env override for the on-disk app-baseline database root (not `/tmp`).
///
/// Prefer [`riffdb_bench_root::BENCH_DB_ROOT_ENV`] (`RIFFDB_BENCH_DB_ROOT`); this
/// legacy name remains as a secondary fallback for one transition release.
pub const DATABASE_ROOT_ENV: &str = "RIFFDB_APP_BASELINE_DB_ROOT";
/// Default database root under the repo (real disk; gitignored via `/target/`).
pub const DEFAULT_DATABASE_ROOT: &str = "target/perf-db/app-baseline";
/// Minimum free bytes required before spawning `riffdbd` in smoke mode (256 MiB).
pub const MIN_FREE_BYTES_SMOKE: u64 = 256 * 1024 * 1024;
/// Minimum free bytes required for evidentiary `--full` runs (8 GiB).
pub const MIN_FREE_BYTES_FULL: u64 = 8 * 1024 * 1024 * 1024;
/// Backward-compatible alias for smoke floor.
pub const MIN_FREE_BYTES: u64 = MIN_FREE_BYTES_SMOKE;

/// Free-space floor for a harness mode.
#[must_use]
pub const fn min_free_bytes_for_full(full: bool) -> u64 {
    if full {
        MIN_FREE_BYTES_FULL
    } else {
        MIN_FREE_BYTES_SMOKE
    }
}

/// Optional process overrides for a baseline `riffdbd` session.
#[derive(Clone, Debug, Default)]
pub struct ServerStartOptions {
    /// When set, exported as `RIFFDB_P1_COORDINATOR_WORKLOAD_CAPACITY` for the child.
    ///
    /// Used by `--load-saturate` so the live profile can reach typed overload
    /// under continuous concurrency without shortening the 150 ms admission wait.
    pub coordinator_workload_capacity: Option<u16>,
    /// Root directory for per-run database/session dirs (real disk by default).
    ///
    /// Resolution order: this field → `RIFFDB_BENCH_DB_ROOT` →
    /// `RIFFDB_APP_BASELINE_DB_ROOT` → [`DEFAULT_DATABASE_ROOT`].
    /// Does **not** use `/tmp` (often a small ramdisk) unless `allow_tmpfs`.
    pub database_root: Option<PathBuf>,
    /// Permit resolving onto tmpfs/ramfs (tests only; never for published numbers).
    pub allow_tmpfs: bool,
    /// Free-space floor before spawn (smoke 256 MiB / full 8 GiB).
    ///
    /// `0` means use [`MIN_FREE_BYTES_SMOKE`].
    pub min_free_bytes: u64,
    /// Enable fixed-cardinality query-execute attribution in the child only.
    pub query_execute_diagnostics: bool,
    /// Enable ADR-0141's feature-gated private direct lane.
    pub exclusive_diagnostic: bool,
    /// Carry the private lane over the repository's verified-TLS fixture.
    ///
    /// This remains diagnostic-only and is not a public transport selector.
    pub exclusive_diagnostic_tls: bool,
}

/// Owns one live `riffdbd` process and a ready public client backend.
pub struct RiffDbServerSession {
    _temporary: riffdb_bench_root::BenchDir,
    process: ServerProcess,
    riffdbd_bin: PathBuf,
    coordinator_workload_capacity: Option<u16>,
    query_execute_diagnostics: bool,
    exclusive_diagnostic: bool,
    exclusive_diagnostic_tls: bool,
    exclusive_diagnostic_address: Option<SocketAddr>,
    table_inventory_before_measurement:
        Option<Vec<riffdb_storage_redb::benchmark_support::AuthoritativeTableInventoryV1>>,
    /// Resolved real-disk root used for this session.
    pub bench_root: riffdb_bench_root::BenchRoot,
    /// Public application backend.
    pub backend: RiffDbPublicBackend,
}

/// Redaction-safe fixed-cardinality server telemetry emitted at clean shutdown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiffDbShutdownEvidence {
    /// Complete graph-local shutdown wall time from the server receipt.
    pub graph_shutdown_elapsed_us: Option<u64>,
    /// Service jobs, exact text, columnar, projection, notifications,
    /// coordinator, blocking ports, and checkpoint elapsed microseconds.
    pub shutdown_stages_us: Option<[u64; 8]>,
    /// Complete harness-observed clean-shutdown wall time.
    pub harness_shutdown_elapsed_us: u64,
    /// Successful completion groups indexed by group size minus one.
    pub write_completion_groups: [u64; WRITE_GROUP_BUCKETS],
    /// Dispatch counts in full, barrier, queue-drained, receiver-closed order.
    pub dispatch_reasons: [u64; 4],
    /// Per-stage public read pipeline summaries.
    pub read_stages: Vec<RiffDbReadStageEvidence>,
    /// Per-stage mutating-command service summaries.
    pub write_service_stages: Vec<RiffDbReadStageEvidence>,
    /// Per-stage coordinator command pipeline summaries.
    pub command_stages: Vec<RiffDbReadStageEvidence>,
    /// Commit, queue, grouping, and writer-utilization evidence.
    pub writer: RiffDbWriterEvidence,
    /// Frames, commands, selected/raw-equivalent frame bytes, and selected/raw segment bytes.
    pub writer_frame_census: Option<[u64; 6]>,
    /// Physical flushes, frames, commands, bytes, maximum grouping, and I/O time.
    pub writer_flush_census: Option<[u64; 7]>,
    /// Journal queue, encode, positional-write, and sync stage totals.
    pub writer_journal_stages: Option<[u64; 10]>,
    /// Ordered publication residence, wait, readiness, and work totals.
    pub writer_publication_stages: Option<[u64; 9]>,
    /// Ordered completion phase counts/times and bounded depth maxima.
    pub completion_lane: Option<RiffDbCompletionLaneEvidence>,
    /// Optional fixed-cardinality query-execute ordinal windows.
    pub query_execute: Option<RiffDbQueryExecuteEvidence>,
    /// Command-table inventory after seed and before the measured process.
    pub table_inventory_before_measurement:
        Option<Vec<riffdb_storage_redb::benchmark_support::AuthoritativeTableInventoryV1>>,
    /// Command-table inventory after the measured process stopped cleanly.
    pub table_inventory_after_measurement:
        Vec<riffdb_storage_redb::benchmark_support::AuthoritativeTableInventoryV1>,
}

/// Closed completion-lane evidence in submitted, published, drained, shutdown order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RiffDbCompletionLaneEvidence {
    /// Observation counts by canonical phase.
    pub phase_counts: [u64; 4],
    /// Summed microseconds by canonical phase.
    pub phase_elapsed_us: [u64; 4],
    /// Maximum number of submitted transitions awaiting publication.
    pub max_depth: u64,
    /// Maximum software reorder-buffer occupancy; FIFO completion keeps this zero.
    pub max_reorder_occupancy: u64,
}

/// One bounded process-generation exact-query execute census.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiffDbQueryExecuteEvidence {
    /// Operations merged into each ordinal window before the terminal bucket.
    pub window_width: u64,
    /// Closed stage names in `stage_ns` order.
    pub stage_names: Vec<String>,
    /// Complete observed operation count.
    pub total_count: u64,
    /// Fixed bounded windows; trailing empty windows are retained.
    pub windows: Vec<RiffDbQueryExecuteWindowEvidence>,
}

/// One query-execute ordinal window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiffDbQueryExecuteWindowEvidence {
    /// Observations merged into this window.
    pub count: u64,
    /// Nanosecond sums in the closed stage order.
    pub stage_ns: Vec<u64>,
    /// Sum of captured composite-overlay transitions.
    pub overlay_transitions_sum: u64,
    /// Maximum captured composite-overlay transitions.
    pub overlay_transitions_max: u64,
    /// Sum of captured composite-overlay charged bytes.
    pub overlay_bytes_sum: u64,
    /// Maximum captured composite-overlay charged bytes.
    pub overlay_bytes_max: u64,
    /// Sum of physical authority-row bytes decoded for frontier capture.
    pub authority_tail_bytes_sum: u64,
    /// Maximum physical authority-row bytes decoded for frontier capture.
    pub authority_tail_bytes_max: u64,
    /// Sum of logical commands decoded from the authority tail.
    pub authority_tail_commands_sum: u64,
    /// Maximum logical commands decoded from one authority tail.
    pub authority_tail_commands_max: u64,
}

/// Closed process-generation writer evidence emitted by `riffdbd`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiffDbWriterEvidence {
    /// Cumulative writer busy microseconds.
    pub busy_us: u64,
    /// Cumulative writer idle microseconds.
    pub idle_us: u64,
    /// Commands selected into intake groups.
    pub dispatch_selected: u64,
    /// Messages deferred at intake group edges.
    pub dispatch_deferred: u64,
    /// Commands passed through exact compatibility partitioning.
    pub compatibility_selected: u64,
    /// Compatible durable groups produced by partitioning.
    pub compatibility_groups: u64,
    /// Boundaries caused by declared conflict-key overlap.
    pub compatibility_conflict_key_splits: u64,
    /// Boundaries caused by exact entity read/write overlap.
    pub compatibility_exact_access_splits: u64,
    /// Compatibility groups selected for compiler-proved shared conflict ownership.
    pub compatibility_commutative_shared_groups: u64,
    /// Latest queue-delay EWMA after a writer unit.
    pub queue_delay_estimate_us: Option<u64>,
    /// Commit-call duration histogram.
    pub commit_duration: RiffDbReadStageEvidence,
    /// Durable-flush duration histogram.
    pub flush_duration: RiffDbReadStageEvidence,
    /// Commands per commit histogram.
    pub batch_size: RiffDbReadStageEvidence,
    /// Accepted command queue-duration histogram.
    pub storage_queue_duration: RiffDbReadStageEvidence,
    /// Bounded command-group formation duration.
    pub group_residence_duration: Option<RiffDbReadStageEvidence>,
    /// Final authoritative apply duration.
    pub final_apply_duration: Option<RiffDbReadStageEvidence>,
    /// Final apply through deferred-journal receipt creation duration.
    pub journal_submit_duration: RiffDbReadStageEvidence,
    /// Bounded preparation-pool depth observations, when emitted by the server.
    pub preparation_pool_depth: Option<RiffDbReadStageEvidence>,
    /// Bounded reorder-buffer occupancy observations, when emitted by the server.
    pub reorder_buffer_occupancy: Option<RiffDbReadStageEvidence>,
    /// Complete unpublished prepared epochs rolled back.
    pub prepared_epoch_rollbacks: u64,
    /// Prepared epochs rolled back for a proof mismatch.
    pub prepared_epoch_proof_mismatches: u64,
    /// Private-frontier equivalence checks completed.
    pub frontier_equivalence_checks: u64,
    /// Private-frontier equivalence checks that failed.
    pub frontier_equivalence_failures: u64,
}

/// One fixed read-pipeline stage summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiffDbReadStageEvidence {
    /// Closed stage label.
    pub name: String,
    /// Number of observations.
    pub count: u64,
    /// Saturating elapsed microsecond sum.
    pub sum_us: u64,
    /// Cumulative fixed histogram buckets.
    pub buckets: Vec<u64>,
}

impl RiffDbServerSession {
    /// Spawns `riffdbd`, bootstraps, deploys TicketDesk + query module, issues a runner capability.
    pub async fn start(riffdbd_bin: &Path) -> Result<Self, RiffDbError> {
        Self::start_with_options(riffdbd_bin, ServerStartOptions::default()).await
    }

    /// Like [`start`](Self::start) with optional process overrides (saturation capacity).
    ///
    /// Two-phase bootstrap: (1) deploy TicketDesk on a process without columnar
    /// projections (ColumnarRuntime::open requires an active catalog), then
    /// clean shutdown; (2) restart the same database with `--projections-root`
    /// and the board projection document so the projected board path is live.
    pub async fn start_with_options(
        riffdbd_bin: &Path,
        options: ServerStartOptions,
    ) -> Result<Self, RiffDbError> {
        let bench_root = resolve_bench_root(&options).map_err(|error| RiffDbError::Server {
            detail: error.to_string(),
        })?;
        let _ = riffdb_bench_root::sweep_stale(&bench_root);
        let temporary =
            riffdb_bench_root::BenchDir::create(&bench_root, "app-baseline").map_err(|error| {
                RiffDbError::Server {
                    detail: error.to_string(),
                }
            })?;
        let database_path = temporary.path().join("riffdb.redb");
        let backup_root = temporary.path().join("backups");
        let projections_root = temporary.path().join("projections");
        let projections_config_path = temporary.path().join("projections.toml");
        let capability_keys_path = temporary.path().join("capability.keys");
        let idempotency_keys_path = temporary.path().join("idempotency.keys");
        let bootstrap_path = temporary.path().join("bootstrap.credential");

        fs::create_dir_all(&projections_root).map_err(|_| RiffDbError::Io)?;
        fs::create_dir_all(&backup_root).map_err(|_| RiffDbError::Io)?;
        write_protected_file(&capability_keys_path, CAPABILITY_KEY_DOCUMENT)
            .map_err(|_| RiffDbError::Io)?;
        write_protected_file(&idempotency_keys_path, IDEMPOTENCY_KEY_DOCUMENT)
            .map_err(|_| RiffDbError::Io)?;
        write_protected_file(
            &projections_config_path,
            BOARD_PROJECTIONS_TOML.trim_start().as_bytes(),
        )
        .map_err(|_| RiffDbError::Io)?;
        let generated = generate_bootstrap_credential(BOOTSTRAP_UNIX_MILLISECONDS, &SystemEntropy)
            .map_err(|_| RiffDbError::Bootstrap)?;
        write_protected_file(&bootstrap_path, generated.render_document().expose_secret())
            .map_err(|_| RiffDbError::Io)?;
        drop(generated);
        let retained =
            load_bootstrap_credential_file(&bootstrap_path).map_err(|_| RiffDbError::Bootstrap)?;

        // Phase 1: deploy contract + module + issue capability (no projections).
        let mut phase1 = ServerProcess::spawn(
            riffdbd_bin,
            &database_path,
            &backup_root,
            &capability_keys_path,
            &idempotency_keys_path,
            options.coordinator_workload_capacity,
            options.query_execute_diagnostics,
            false,
            false,
            None,
            None,
        )
        .map_err(|error| RiffDbError::Server {
            detail: error.to_string(),
        })?;
        let address = phase1.wait_for_ready_address().map_err(|error| {
            let detail = phase1.diagnostic_detail(&error.to_string());
            RiffDbError::Server { detail }
        })?;
        let endpoint = format!("http://{address}");
        let mut client = connect(&endpoint).await?;
        // Phase 1 only activates the contract so ColumnarRuntime can resolve the
        // Ticket entity on the next process open. Query module + runner capability
        // are issued after the projection-enabled restart (avoids MODULE_UNAVAILABLE
        // from a pre-restart module identity that is not re-bound on reopen).
        let (status_ids, contract_bundle_hash) =
            bootstrap_and_deploy_contract(&mut client, &retained).await?;
        // History incarnation for a fresh DB is 1; Causal tokens bind it.
        let history_incarnation = 1_u64;
        phase1
            .shutdown_cleanly()
            .map_err(|error| RiffDbError::Server {
                detail: error.to_string(),
            })?;

        // Phase 2: same database with board projection registration.
        let process = ServerProcess::spawn(
            riffdbd_bin,
            &database_path,
            &backup_root,
            &capability_keys_path,
            &idempotency_keys_path,
            options.coordinator_workload_capacity,
            options.query_execute_diagnostics,
            options.exclusive_diagnostic,
            options.exclusive_diagnostic_tls,
            Some(projections_root.as_path()),
            Some(projections_config_path.as_path()),
        )
        .map_err(|error| RiffDbError::Server {
            detail: error.to_string(),
        })?;
        let address = process.wait_for_ready_address().map_err(|error| {
            let detail = process.diagnostic_detail(&error.to_string());
            RiffDbError::Server { detail }
        })?;
        let exclusive_diagnostic_address = if options.exclusive_diagnostic {
            Some(process.wait_for_exclusive_diagnostic_address().map_err(|error| {
                let detail = process.diagnostic_detail(&error.to_string());
                RiffDbError::Server { detail }
            })?)
        } else {
            None
        };
        let endpoint = format!("http://{address}");
        let mut client = connect(&endpoint).await?;
        let (token, module_hash) = deploy_module_and_issue_runner(&mut client, &retained).await?;
        let mut backend = RiffDbPublicBackend::connect(
            &endpoint,
            &token,
            status_ids,
            contract_bundle_hash,
            module_hash,
            history_incarnation,
        )
        .await?;
        if let Some(address) = exclusive_diagnostic_address {
            backend
                .enable_exclusive_diagnostic(address, diagnostic_trust_root(options.exclusive_diagnostic_tls))
                .await?;
        }
        Ok(Self {
            _temporary: temporary,
            process,
            riffdbd_bin: riffdbd_bin.to_path_buf(),
            coordinator_workload_capacity: options.coordinator_workload_capacity,
            query_execute_diagnostics: options.query_execute_diagnostics,
            exclusive_diagnostic: options.exclusive_diagnostic,
            exclusive_diagnostic_tls: options.exclusive_diagnostic_tls,
            exclusive_diagnostic_address,
            table_inventory_before_measurement: None,
            bench_root,
            backend,
        })
    }

    /// Restarts the daemon after setup so measured telemetry starts at zero.
    ///
    /// The database, contract, query module, capability, and seed remain durable;
    /// only process-generation counters and channels are replaced. This is the
    /// benchmark boundary that prevents setup and warmup work from contaminating
    /// per-point server evidence.
    pub async fn restart_for_measurement(
        mut self,
    ) -> Result<(Self, RiffDbShutdownEvidence), RiffDbError> {
        let setup_evidence =
            self.process
                .shutdown_cleanly()
                .map_err(|error| RiffDbError::Server {
                    detail: error.to_string(),
                })?;
        let database_path = self._temporary.path().join("riffdb.redb");
        let table_inventory_before_measurement =
            riffdb_storage_redb::benchmark_support::authoritative_table_inventory_after_reopen_v1(
                &database_path,
            )
            .map_err(|error| RiffDbError::Server {
                detail: format!("read pre-measurement table inventory: {error}"),
            })?;
        let backup_root = self._temporary.path().join("backups");
        let projections_root = self._temporary.path().join("projections");
        let projections_config_path = self._temporary.path().join("projections.toml");
        let capability_keys_path = self._temporary.path().join("capability.keys");
        let idempotency_keys_path = self._temporary.path().join("idempotency.keys");
        let process = ServerProcess::spawn(
            &self.riffdbd_bin,
            &database_path,
            &backup_root,
            &capability_keys_path,
            &idempotency_keys_path,
            self.coordinator_workload_capacity,
            self.query_execute_diagnostics,
            self.exclusive_diagnostic,
            self.exclusive_diagnostic_tls,
            Some(projections_root.as_path()),
            Some(projections_config_path.as_path()),
        )
        .map_err(|error| RiffDbError::Server {
            detail: error.to_string(),
        })?;
        let address = process.wait_for_ready_address().map_err(|error| {
            let detail = process.diagnostic_detail(&error.to_string());
            RiffDbError::Server { detail }
        })?;
        let exclusive_diagnostic_address = if self.exclusive_diagnostic {
            Some(process.wait_for_exclusive_diagnostic_address().map_err(|error| {
                let detail = process.diagnostic_detail(&error.to_string());
                RiffDbError::Server { detail }
            })?)
        } else {
            None
        };
        let endpoint = format!("http://{address}");
        let mut backend = self.backend.reconnect_endpoint(&endpoint).await?;
        if let Some(address) = exclusive_diagnostic_address {
            backend
                .enable_exclusive_diagnostic(
                    address,
                    diagnostic_trust_root(self.exclusive_diagnostic_tls),
                )
                .await?;
        }
        self.process = process;
        self.exclusive_diagnostic_address = exclusive_diagnostic_address;
        self.backend = backend;
        self.table_inventory_before_measurement = Some(table_inventory_before_measurement);
        Ok((self, setup_evidence))
    }

    /// Returns the private diagnostic endpoint for this process generation.
    #[must_use]
    pub const fn exclusive_diagnostic_address(&self) -> Option<SocketAddr> {
        self.exclusive_diagnostic_address
    }

    /// Stops the server cleanly.
    pub fn shutdown(mut self) -> Result<[u64; WRITE_GROUP_BUCKETS], RiffDbError> {
        self.process
            .shutdown_cleanly()
            .map(|evidence| evidence.write_completion_groups)
            .map_err(|error| RiffDbError::Server {
                detail: error.to_string(),
            })
    }

    /// Stops cleanly and returns bounded stage/scheduler evidence.
    pub fn shutdown_with_evidence(mut self) -> Result<RiffDbShutdownEvidence, RiffDbError> {
        let database_path = self._temporary.path().join("riffdb.redb");
        let mut evidence =
            self.process
                .shutdown_cleanly()
                .map_err(|error| RiffDbError::Server {
                    detail: error.to_string(),
                })?;
        evidence.table_inventory_before_measurement =
            self.table_inventory_before_measurement.take();
        evidence.table_inventory_after_measurement =
            riffdb_storage_redb::benchmark_support::authoritative_table_inventory_after_reopen_v1(
                &database_path,
            )
            .map_err(|error| RiffDbError::Server {
                detail: format!("reopen and read post-measurement table inventory: {error}"),
            })?;
        Ok(evidence)
    }

    /// Last stderr lines captured from `riffdbd` (for crash diagnosis).
    #[must_use]
    pub fn stderr_tail(&self) -> String {
        self.process.stderr_tail()
    }

    /// Non-blocking liveness check: `Ok` while the child is still running.
    pub fn server_alive(&mut self) -> Result<(), String> {
        match self.process.poll_exit() {
            None => Ok(()),
            Some(Ok(status)) => {
                let mut detail = format!("riffdbd exited mid-load: {status}");
                #[cfg(unix)]
                {
                    use std::os::unix::process::ExitStatusExt;
                    if let Some(signal) = status.signal() {
                        detail.push_str(&format!(" signal={signal}"));
                    }
                }
                let tail = self.process.stderr_tail();
                if !tail.is_empty() {
                    detail.push_str("; stderr_tail=\n");
                    detail.push_str(&tail);
                }
                detail.push_str(&format!(
                    "; database_root={} free_bytes_at_start={}",
                    self.bench_root.path().display(),
                    self.bench_root.free_bytes_at_start()
                ));
                Err(detail)
            }
            Some(Err(error)) => Err(format!(
                "riffdbd wait error mid-load: {error}; database_root={} free_bytes_at_start={}",
                self.bench_root.path().display(),
                self.bench_root.free_bytes_at_start()
            )),
        }
    }

    /// OS process id of the `riffdbd` child (for integration kill tests).
    #[must_use]
    pub fn child_pid(&self) -> u32 {
        self.process.child_id
    }

    /// Private per-run directory containing the database, projection, and
    /// auxiliary durable files. Benchmark attribution may inspect aggregate
    /// byte counts, but must never publish paths or file contents as labels.
    #[must_use]
    pub fn session_directory(&self) -> &Path {
        self._temporary.path()
    }
}

fn diagnostic_fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../crates/riffdb-server/tests/fixtures")
        .join(name)
}

fn diagnostic_trust_root(enabled: bool) -> Option<PathBuf> {
    enabled.then(|| diagnostic_fixture("test-ca.pem"))
}

/// Phase 1: bootstrap capability + deploy TicketDesk contract only.
async fn bootstrap_and_deploy_contract(
    client: &mut RiffDbClient,
    credential: &RetainedBootstrapCredential,
) -> Result<(TicketStatusEnumIds, [u8; 32]), RiffDbError> {
    let token = std::str::from_utf8(credential.token().expose_secret())
        .map_err(|_| RiffDbError::Bootstrap)?;
    let bootstrap_metadata = BootstrapCallMetadata::new(
        TransportBootstrapCredential::new(token).map_err(|_| RiffDbError::Bootstrap)?,
    );
    let authenticated = CallMetadata::authenticated(
        BearerCredential::new(token).map_err(|_| RiffDbError::Bootstrap)?,
    );

    let created = bounded_rpc(
        "bootstrap_create_capability",
        client.create_bootstrap_capability(bootstrap_request(credential)?, &bootstrap_metadata),
    )
    .await?;
    let Some(v1::create_capability_response::Result::Bootstrap(result)) = created.result else {
        return Err(RiffDbError::Rpc(
            "bootstrap response was not Bootstrap/Created".into(),
        ));
    };
    let Some(v1::bootstrap_create_capability_result::Result::Created(_)) = result.result else {
        return Err(RiffDbError::Rpc(
            "bootstrap capability was not newly created".into(),
        ));
    };

    let deployed = bounded_rpc(
        "deploy_contract",
        client.deploy_contract(
            v1::DeployContractRequest {
                request_id: fresh_request_id_bytes()?,
                source: TICKETDESK_CONTRACT.to_owned(),
                expected_active_version: None,
                expected_active_bundle_hash: Vec::new(),
                expected_candidate_bundle_hash: Vec::new(),
            },
            &authenticated,
        ),
    )
    .await?;
    let Some(v1::deploy_contract_response::Result::Activated(contract)) = deployed.result else {
        return Err(RiffDbError::Rpc(format!(
            "deploy_contract did not activate: {deployed:?}"
        )));
    };
    if contract.contract_lineage != CONTRACT_LINEAGE
        || contract.contract_version != CONTRACT_VERSION
    {
        return Err(RiffDbError::Rpc(format!(
            "unexpected contract identity {}/{}",
            contract.contract_lineage, contract.contract_version
        )));
    }
    let bundle_hash = contract
        .bundle_hash
        .as_slice()
        .try_into()
        .map_err(|_| RiffDbError::Deploy)?;
    Ok((ticket_status_enum_ids()?, bundle_hash))
}

/// Phase 2: deploy query module and issue the hybrid runner capability.
async fn deploy_module_and_issue_runner(
    client: &mut RiffDbClient,
    credential: &RetainedBootstrapCredential,
) -> Result<(String, [u8; 32]), RiffDbError> {
    let token = std::str::from_utf8(credential.token().expose_secret())
        .map_err(|_| RiffDbError::Bootstrap)?;
    let authenticated = CallMetadata::authenticated(
        BearerCredential::new(token).map_err(|_| RiffDbError::Bootstrap)?,
    );

    let module = bounded_rpc(
        "deploy_query_module",
        client.deploy_query_module(
            app_v1::DeployQueryModuleRequest {
                contract: Some(app_v1::ContractSelector {
                    lineage: CONTRACT_LINEAGE.to_owned(),
                    version: CONTRACT_VERSION,
                    bundle_hash: Vec::new(),
                }),
                module_name: MODULE_NAME.to_owned(),
                module_version: MODULE_VERSION,
                queries: QUERY_SOURCES
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
            &authenticated,
        ),
    )
    .await
    .map_err(|error| RiffDbError::Rpc(format!("deploy_query_module: {error}")))?;
    let Some(module) = module.module else {
        return Err(RiffDbError::Rpc(format!(
            "deploy_query_module did not activate: {module:?}"
        )));
    };

    // Compile the exact TicketDeskApplication role from the same sources that
    // were deployed. Field visibility, command IDs, and scan ceilings remain
    // compiler-private consequences of named operations (WP-310).
    let role = compile_ticketdesk_application_role()?;
    let expected_module_hash = role
        .module_hashes()
        .first()
        .map(|hash| hash.as_bytes())
        .ok_or(RiffDbError::Deploy)?;
    if module.module_hash.as_slice() != expected_module_hash.as_slice() {
        return Err(RiffDbError::Rpc(
            "deployed query-module hash does not match compiled role module identity".into(),
        ));
    }
    let module_hash: [u8; 32] = module
        .module_hash
        .as_slice()
        .try_into()
        .map_err(|_| RiffDbError::Deploy)?;

    // Hybrid grant: application role atoms + ExecuteAdHocQuery so the same
    // runner token covers compiled named board queries and ExecuteProjectedQuery
    // (policy maps projected reads to Kind::ExecuteAdHocQuery).
    let grant = runner_grant_with_projected_read(role.internal_grant())?;

    let response = bounded_rpc(
        "create_runner_capability",
        client.create_capability(role_capability_request(&grant)?, &authenticated),
    )
    .await?;
    let Some(v1::create_capability_response::Result::Normal(result)) = response.result else {
        return Err(RiffDbError::Rpc(
            "runner capability response was not Normal".into(),
        ));
    };
    let Some(v1::normal_create_capability_result::Result::Created(created)) = result.result else {
        return Err(RiffDbError::Rpc("runner capability was not created".into()));
    };
    Ok((created.token, module_hash))
}

/// Application-role grant plus ExecuteAdHocQuery for the projected board path.
fn runner_grant_with_projected_read(
    base: &CapabilityGrantV1,
) -> Result<CapabilityGrantV1, RiffDbError> {
    let mut permissions = base.permissions().as_slice().to_vec();
    permissions.push(
        CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ExecuteAdHocQuery)
            .map_err(|_| RiffDbError::Bootstrap)?,
    );
    let permissions =
        CapabilityPermissionsV1::new(permissions).map_err(|_| RiffDbError::Bootstrap)?;
    CapabilityGrantV1::new(
        base.tenant_scope().clone(),
        base.partition_scope().clone(),
        permissions,
        base.field_visibility().to_vec(),
        base.max_scan_rows(),
        Vec::new(),
    )
    .map_err(|_| RiffDbError::Bootstrap)
}

/// Resolves TicketStatus type/variant ids from the harness contract source.
fn ticket_status_enum_ids() -> Result<TicketStatusEnumIds, RiffDbError> {
    let bundle = compile_contract_source(TICKETDESK_CONTRACT).map_err(|_| RiffDbError::Deploy)?;
    let ticket_status = bundle
        .schema()
        .enums()
        .iter()
        .find(|candidate| candidate.name() == "TicketStatus")
        .ok_or(RiffDbError::Deploy)?;
    let variant = |name: &str| -> Result<u32, RiffDbError> {
        ticket_status
            .variants()
            .iter()
            .find(|candidate| candidate.name() == name)
            .map(|candidate| candidate.id().get())
            .ok_or(RiffDbError::Deploy)
    };
    Ok(TicketStatusEnumIds {
        type_id: ticket_status.id().get(),
        open: variant("Open")?,
        closed: variant("Closed")?,
        in_progress: variant("InProgress")?,
    })
}

fn bootstrap_request(
    credential: &RetainedBootstrapCredential,
) -> Result<v1::CreateCapabilityRequest, RiffDbError> {
    use v1::capability_permission::Permission;
    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Bootstrap as i32,
        capability_id: credential.capability_id().into_bytes().to_vec(),
        principal_id: "app-baseline-bootstrap".to_owned(),
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

/// Compiles the checked-in TicketDesk application role from the same contract
/// and query sources the harness deploys to `riffdbd`.
fn compile_ticketdesk_application_role() -> Result<CompiledApplicationRole, RiffDbError> {
    let manifest = ApplicationManifest::decode_canonical(TICKETDESK_MANIFEST.as_bytes())
        .map_err(|_| RiffDbError::Deploy)?;
    let contract = compile_contract_source(TICKETDESK_CONTRACT).map_err(|_| RiffDbError::Deploy)?;
    let queries = QUERY_SOURCES
        .iter()
        .map(|(name, source)| {
            NamedQuerySource::new(*name, *source).map_err(|_| RiffDbError::Deploy)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new(MODULE_NAME).map_err(|_| RiffDbError::Deploy)?,
        QueryModuleVersion::new(MODULE_VERSION).ok_or(RiffDbError::Deploy)?,
        queries,
    )
    .map_err(|_| RiffDbError::Deploy)?;
    let module = QueryModule::compile(candidate, &contract).map_err(|_| RiffDbError::Deploy)?;
    compile_application_role(&manifest, APPLICATION_ROLE_NAME, None, &contract, &[module])
        .map_err(|_| RiffDbError::Deploy)
}

/// Lowers a compiler-private role grant into the public create-capability request.
fn role_capability_request(
    grant: &CapabilityGrantV1,
) -> Result<v1::CreateCapabilityRequest, RiffDbError> {
    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Normal as i32,
        capability_id: generate_capability_id()
            .map_err(|_| RiffDbError::Bootstrap)?
            .into_bytes()
            .to_vec(),
        principal_id: "ticketdesk-application".to_owned(),
        actor_kind: v1::ActorKind::Service as i32,
        requested_lifetime_seconds: CAPABILITY_LIFETIME_SECONDS,
        audiences: vec![AUDIENCE.to_owned()],
        grant: Some(application_role_grant_to_proto(grant)?),
    })
}

fn application_role_grant_to_proto(
    grant: &CapabilityGrantV1,
) -> Result<v1::CapabilityGrant, RiffDbError> {
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
            return Err(RiffDbError::Rpc(
                "application role lowered to explicit partition scope".into(),
            ));
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
) -> Result<v1::CapabilityPermission, RiffDbError> {
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
            _ => {
                return Err(RiffDbError::Rpc(format!(
                    "unsupported unparameterized capability permission: {kind:?}"
                )));
            }
        },
        _ => {
            return Err(RiffDbError::Rpc(
                "application role compiler emitted non-application authority".into(),
            ));
        }
    };
    Ok(v1::CapabilityPermission {
        permission: Some(permission),
    })
}

async fn connect(endpoint: &str) -> Result<RiffDbClient, RiffDbError> {
    let endpoint = Endpoint::from_shared(endpoint.to_owned())
        .map_err(|_| RiffDbError::Connection)?
        .connect_timeout(RPC_TIMEOUT)
        .timeout(RPC_TIMEOUT);
    bounded_rpc("connect", RiffDbClient::connect(endpoint)).await
}

async fn bounded_rpc<T, E>(
    step: &str,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, RiffDbError>
where
    E: std::fmt::Display,
{
    match timeout(RPC_TIMEOUT, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(RiffDbError::Rpc(format!("{step}: {error}"))),
        Err(_) => Err(RiffDbError::Rpc(format!("{step}: timeout"))),
    }
}

fn fresh_request_id_bytes() -> Result<Vec<u8>, RiffDbError> {
    Ok(generate_request_id()
        .map_err(|_| RiffDbError::Bootstrap)?
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

/// Resolves the on-disk root for app-baseline `riffdbd` session directories.
///
/// Order: explicit `override_root` → `RIFFDB_BENCH_DB_ROOT` →
/// `RIFFDB_APP_BASELINE_DB_ROOT` → [`DEFAULT_DATABASE_ROOT`].
/// Never falls back to `std::env::temp_dir()`.
#[must_use]
pub fn resolve_database_root(override_root: Option<&Path>) -> PathBuf {
    if let Some(path) = override_root {
        return path.to_path_buf();
    }
    if let Some(from_env) = std::env::var_os(riffdb_bench_root::BENCH_DB_ROOT_ENV)
        && !from_env.is_empty()
    {
        return PathBuf::from(from_env);
    }
    if let Some(from_env) = std::env::var_os(DATABASE_ROOT_ENV)
        && !from_env.is_empty()
    {
        return PathBuf::from(from_env);
    }
    PathBuf::from(DEFAULT_DATABASE_ROOT)
}

/// Resolves a classified [`riffdb_bench_root::BenchRoot`] for session dirs.
#[allow(clippy::result_large_err)]
pub fn resolve_bench_root(
    options: &ServerStartOptions,
) -> Result<riffdb_bench_root::BenchRoot, riffdb_bench_root::BenchRootError> {
    let default_root = if let Some(legacy) = std::env::var_os(DATABASE_ROOT_ENV) {
        if !legacy.is_empty() && options.database_root.is_none() {
            // Legacy env only fills default when unified env/cli absent; BenchRoot
            // still prefers RIFFDB_BENCH_DB_ROOT over this default.
            PathBuf::from(legacy)
        } else {
            PathBuf::from(DEFAULT_DATABASE_ROOT)
        }
    } else {
        PathBuf::from(DEFAULT_DATABASE_ROOT)
    };
    let min_free_bytes = if options.min_free_bytes == 0 {
        MIN_FREE_BYTES_SMOKE
    } else {
        options.min_free_bytes
    };
    riffdb_bench_root::BenchRoot::resolve(riffdb_bench_root::BenchRootOptions {
        harness: "app-baseline",
        cli_override: options.database_root.clone(),
        default_root,
        allow_tmpfs: options.allow_tmpfs,
        min_free_bytes,
    })
}

/// Removes orphaned session dirs left by killed/hung harness runs.
///
/// Prefers [`riffdb_bench_root::sweep_stale_path`] when the root contains
/// `perf-db`; otherwise best-effort removes legacy `riffdb-app-baseline-*`
/// children only (no broad recursive wipe).
pub fn sweep_stale_session_dirs(root: &Path) {
    if riffdb_bench_root::path_has_perf_db_component(root) {
        let _ = riffdb_bench_root::sweep_stale_path(root);
        return;
    }
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !(name.starts_with("riffdb-app-baseline-") || name.starts_with("riffdb-bench-")) {
            continue;
        }
        if path.is_dir() {
            let _ = fs::remove_dir_all(&path);
        }
    }
}

enum ReaperCommand {
    Kill,
}

struct ServerProcess {
    child_id: u32,
    stdin: Option<std::process::ChildStdin>,
    ready: Receiver<io::Result<String>>,
    exclusive_ready: Receiver<io::Result<String>>,
    shutdown_evidence: Receiver<io::Result<RiffDbShutdownEvidence>>,
    reaper_commands: SyncSender<ReaperCommand>,
    exited: Receiver<io::Result<ExitStatus>>,
    reaper: Option<JoinHandle<()>>,
    stdout: Option<JoinHandle<usize>>,
    stderr: Option<JoinHandle<usize>>,
    stderr_ring: Arc<Mutex<VecDeque<String>>>,
    exit_observed: bool,
    /// Cached exit once observed; subsequent [`poll_exit`] returns this.
    cached_exit: Option<Result<ExitStatus, String>>,
}

impl ServerProcess {
    #[allow(clippy::too_many_arguments)]
    fn spawn(
        binary: &Path,
        database_path: &Path,
        backup_root: &Path,
        capability_keys_path: &Path,
        idempotency_keys_path: &Path,
        coordinator_workload_capacity: Option<u16>,
        query_execute_diagnostics: bool,
        exclusive_diagnostic: bool,
        exclusive_diagnostic_tls: bool,
        projections_root: Option<&Path>,
        projections_config: Option<&Path>,
    ) -> io::Result<Self> {
        let mut command = Command::new(binary);
        command
            .arg("--database")
            .arg(database_path)
            .arg("--backup-root")
            .arg(backup_root)
            .arg("--listen")
            .arg("127.0.0.1:0")
            .arg("--environment")
            .arg(ENVIRONMENT)
            .arg("--audience")
            .arg(AUDIENCE)
            .arg("--capability-keys")
            .arg(capability_keys_path)
            .arg("--idempotency-keys")
            .arg(idempotency_keys_path)
            // Surface panics / allocator errors in the harness tail.
            .env("RUST_BACKTRACE", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(root) = projections_root {
            command.arg("--projections-root").arg(root);
        }
        if let Some(config) = projections_config {
            command.arg("--config").arg(config);
        }
        if let Some(capacity) = coordinator_workload_capacity {
            command.env(
                "RIFFDB_P1_COORDINATOR_WORKLOAD_CAPACITY",
                capacity.to_string(),
            );
        }
        if query_execute_diagnostics {
            command.env("RIFFDB_QUERY_EXECUTE_DIAGNOSTICS", "1");
        }
        if exclusive_diagnostic {
            command.env("RIFFDB_DIRECT_STREAM_DIAGNOSTIC", "1");
            if exclusive_diagnostic_tls {
                command
                    .env(
                        "RIFFDB_DIRECT_STREAM_DIAGNOSTIC_TLS_CERT",
                        diagnostic_fixture("localhost-cert.pem"),
                    )
                    .env(
                        "RIFFDB_DIRECT_STREAM_DIAGNOSTIC_TLS_KEY",
                        diagnostic_fixture("localhost-key.pem"),
                    );
            }
        }
        let mut child = command.spawn()?;
        let child_id = child.id();
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("stdout"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("stderr"))?;
        let (ready_sender, ready) = mpsc::sync_channel(1);
        let (exclusive_ready_sender, exclusive_ready) = mpsc::sync_channel(1);
        let (shutdown_sender, shutdown_evidence) = mpsc::sync_channel(1);
        let stdout = thread::spawn(move || {
            read_server_stdout(
                stdout,
                ready_sender,
                exclusive_ready_sender,
                shutdown_sender,
            )
        });
        let stderr_ring = Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_RING_LINES)));
        let stderr_ring_worker = Arc::clone(&stderr_ring);
        let stderr = thread::spawn(move || drain_server_stderr(stderr, stderr_ring_worker));
        let (reaper_commands, commands) = mpsc::sync_channel(1);
        let (exit_sender, exited) = mpsc::sync_channel(1);
        let reaper = thread::spawn(move || reap_child(child, commands, exit_sender));
        Ok(Self {
            child_id,
            stdin: Some(stdin),
            ready,
            exclusive_ready,
            shutdown_evidence,
            reaper_commands,
            exited,
            reaper: Some(reaper),
            stdout: Some(stdout),
            stderr: Some(stderr),
            stderr_ring,
            exit_observed: false,
            cached_exit: None,
        })
    }

    fn wait_for_ready_address(&self) -> io::Result<SocketAddr> {
        let line = match self.ready.recv_timeout(PROCESS_START_TIMEOUT) {
            Ok(result) => result?,
            Err(RecvTimeoutError::Timeout) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    self.diagnostic_detail("ready timeout"),
                ));
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(io::Error::other(
                    self.diagnostic_detail("ready disconnected"),
                ));
            }
        };
        let address = line
            .strip_prefix(READY_PREFIX)
            .ok_or_else(|| io::Error::other("bad ready line"))?;
        address
            .parse()
            .map_err(|_| io::Error::other("bad ready address"))
    }

    fn wait_for_exclusive_diagnostic_address(&self) -> io::Result<SocketAddr> {
        let line = match self.exclusive_ready.recv_timeout(PROCESS_START_TIMEOUT) {
            Ok(result) => result?,
            Err(RecvTimeoutError::Timeout) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    self.diagnostic_detail("private diagnostic ready timeout"),
                ));
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(io::Error::other(
                    self.diagnostic_detail("private diagnostic ready disconnected"),
                ));
            }
        };
        line.strip_prefix(EXCLUSIVE_DIAGNOSTIC_PREFIX)
            .ok_or_else(|| io::Error::other("bad private diagnostic ready line"))?
            .parse()
            .map_err(|_| io::Error::other("bad private diagnostic ready address"))
    }

    fn stderr_tail(&self) -> String {
        self.stderr_ring
            .lock()
            .map(|ring| ring.iter().cloned().collect::<Vec<_>>().join(""))
            .unwrap_or_default()
    }

    /// Non-blocking poll of the child exit channel (caches observed status).
    ///
    /// Once an exit is observed, subsequent calls return the **same** cached
    /// status so [`RiffDbServerSession::server_alive`] stays truthful.
    fn poll_exit(&mut self) -> Option<io::Result<ExitStatus>> {
        if let Some(cached) = &self.cached_exit {
            return Some(match cached {
                Ok(status) => Ok(*status),
                Err(message) => Err(io::Error::other(message.clone())),
            });
        }
        match self.exited.try_recv() {
            Ok(status) => {
                self.exit_observed = true;
                let cached = status
                    .as_ref()
                    .map(|s| *s)
                    .map_err(|error| error.to_string());
                self.cached_exit = Some(cached);
                Some(status)
            }
            Err(mpsc::TryRecvError::Empty) => None,
            Err(mpsc::TryRecvError::Disconnected) => {
                self.exit_observed = true;
                let message = "riffdbd reaper disconnected".to_owned();
                self.cached_exit = Some(Err(message.clone()));
                Some(Err(io::Error::other(message)))
            }
        }
    }

    fn diagnostic_detail(&self, headline: &str) -> String {
        let tail = self.stderr_tail();
        let mut detail = headline.to_owned();
        if !tail.is_empty() {
            let clipped: String = tail.chars().rev().take(STDERR_DETAIL_CHARS).collect();
            let clipped: String = clipped.chars().rev().collect();
            detail.push_str("; stderr_tail=\n");
            detail.push_str(&clipped);
        }
        detail
    }

    fn shutdown_cleanly(&mut self) -> io::Result<RiffDbShutdownEvidence> {
        let started = Instant::now();
        if let Some(mut stdin) = self.stdin.take() {
            let _ = stdin.write_all(b"shutdown\n");
            let _ = stdin.flush();
        }
        let evidence = match self.exited.recv_timeout(PROCESS_STOP_TIMEOUT) {
            Ok(status) => {
                self.exit_observed = true;
                let status = status?;
                self.cached_exit = Some(Ok(status));
                if status.success() {
                    self.shutdown_evidence
                        .recv_timeout(PROCESS_STOP_TIMEOUT)
                        .map_err(|_| {
                            io::Error::other(
                                self.diagnostic_detail(
                                    "write-group report missing after clean exit",
                                ),
                            )
                        })?
                } else {
                    let code = status
                        .code()
                        .map(|c| format!("exit_code={c}"))
                        .unwrap_or_else(|| format!("exit_status={status}"));
                    // Unix signal when available.
                    #[cfg(unix)]
                    let code = {
                        use std::os::unix::process::ExitStatusExt;
                        match status.signal() {
                            Some(sig) => format!("{code} signal={sig}"),
                            None => code,
                        }
                    };
                    Err(io::Error::other(self.diagnostic_detail(&code)))
                }
            }
            Err(_) => {
                let _ = self.reaper_commands.send(ReaperCommand::Kill);
                // Give the reaper a moment to deposit an exit status if kill works.
                let after_kill = self.exited.recv_timeout(Duration::from_secs(2));
                let headline = match after_kill {
                    Ok(Ok(status)) => {
                        self.exit_observed = true;
                        format!("shutdown timeout; killed child status={status}")
                    }
                    Ok(Err(error)) => format!("shutdown timeout; kill wait error={error}"),
                    Err(_) => "shutdown timeout; child unresponsive after kill".to_owned(),
                };
                Err(io::Error::other(self.diagnostic_detail(&headline)))
            }
        };
        evidence.map(|mut evidence| {
            evidence.harness_shutdown_elapsed_us =
                u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
            evidence
        })
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if !self.exit_observed {
            let _ = self.reaper_commands.send(ReaperCommand::Kill);
        }
        if let Some(handle) = self.reaper.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.stdout.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.stderr.take() {
            let _ = handle.join();
        }
    }
}

fn read_server_stdout(
    stream: impl Read,
    ready_sender: SyncSender<io::Result<String>>,
    exclusive_ready_sender: SyncSender<io::Result<String>>,
    shutdown_sender: SyncSender<io::Result<RiffDbShutdownEvidence>>,
) -> usize {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let ready = reader
        .read_line(&mut line)
        .map(|_| line.trim_end().to_owned());
    let _ = ready_sender.send(ready);
    let mut total = line.len();
    let mut write_completion_groups = None;
    let mut dispatch_reasons = None;
    let mut read_stages = None;
    let mut write_service_stages = None;
    let mut command_stages = None;
    let mut writer = None;
    let mut writer_frame_census = None;
    let mut writer_flush_census = None;
    let mut writer_journal_stages = None;
    let mut writer_publication_stages = None;
    let mut completion_lane = None;
    let mut query_execute = None;
    let mut shutdown_stages_us = None;
    loop {
        line.clear();
        let Ok(read) = reader.read_line(&mut line) else {
            break;
        };
        if read == 0 {
            break;
        }
        eprint!("{line}");
        total = total.saturating_add(read);
        if line.trim_end().starts_with(EXCLUSIVE_DIAGNOSTIC_PREFIX) {
            let _ = exclusive_ready_sender.send(Ok(line.trim_end().to_owned()));
        } else if let Some(encoded) = line.trim_end().strip_prefix(WRITE_GROUP_PREFIX) {
            write_completion_groups = Some(parse_fixed_counts(
                encoded,
                "write-group",
                WRITE_GROUP_BUCKETS,
            ));
        } else if let Some(encoded) = line.trim_end().strip_prefix(DISPATCH_REASON_PREFIX) {
            dispatch_reasons = Some(parse_fixed_counts(encoded, "dispatch-reason", 4));
        } else if let Some(encoded) = line.trim_end().strip_prefix(READ_STAGE_PREFIX) {
            read_stages = Some(parse_read_stages(encoded));
        } else if let Some(encoded) = line.trim_end().strip_prefix(WRITE_SERVICE_STAGE_PREFIX) {
            write_service_stages = Some(parse_read_stages(encoded));
        } else if let Some(encoded) = line.trim_end().strip_prefix(COMMAND_STAGE_PREFIX) {
            command_stages = Some(parse_read_stages(encoded));
        } else if let Some(encoded) = line.trim_end().strip_prefix(WRITER_EVIDENCE_PREFIX) {
            writer = Some(parse_writer_evidence(encoded));
        } else if let Some(encoded) = line.trim_end().strip_prefix(WRITER_FRAME_CENSUS_PREFIX) {
            writer_frame_census = Some(parse_fixed_counts(encoded, "writer-frame-census", 6));
        } else if let Some(encoded) = line.trim_end().strip_prefix(WRITER_FLUSH_CENSUS_PREFIX) {
            writer_flush_census = Some(parse_fixed_counts(encoded, "writer-flush-census", 7));
        } else if let Some(encoded) = line.trim_end().strip_prefix(WRITER_JOURNAL_STAGES_PREFIX) {
            writer_journal_stages = Some(parse_fixed_counts(encoded, "writer-journal-stages", 10));
        } else if let Some(encoded) = line
            .trim_end()
            .strip_prefix(WRITER_PUBLICATION_STAGES_PREFIX)
        {
            writer_publication_stages = Some(parse_fixed_counts(
                encoded,
                "writer-publication-stages",
                9,
            ));
        } else if let Some(encoded) = line.trim_end().strip_prefix(COMPLETION_LANE_PREFIX) {
            completion_lane = Some(parse_completion_lane(encoded));
        } else if let Some(encoded) = line.trim_end().strip_prefix(QUERY_EXECUTE_WINDOWS_PREFIX) {
            query_execute = Some(parse_query_execute_windows(encoded));
        } else if let Some(encoded) = line.trim_end().strip_prefix(SHUTDOWN_STAGES_PREFIX) {
            shutdown_stages_us = Some(parse_shutdown_stages(encoded));
        }
    }
    let evidence = match (
        write_completion_groups,
        dispatch_reasons,
        read_stages,
        write_service_stages,
        command_stages,
        writer,
    ) {
        (
            Some(Ok(write_completion_groups)),
            Some(Ok(dispatch_reasons)),
            Some(Ok(read_stages)),
            Some(Ok(write_service_stages)),
            Some(Ok(command_stages)),
            Some(Ok(writer)),
        ) => writer_frame_census
            .transpose()
            .and_then(|writer_frame_census| {
                writer_flush_census
                    .transpose()
                    .and_then(|writer_flush_census| {
                        writer_journal_stages.transpose().and_then(|writer_journal_stages| {
                            writer_publication_stages.transpose().and_then(
                                |writer_publication_stages| {
                                    completion_lane.transpose().and_then(|completion_lane| {
                                        query_execute.transpose().and_then(|query_execute| {
                                            optional_shutdown_stages(shutdown_stages_us).map(|(graph_shutdown_elapsed_us, shutdown_stages_us)| RiffDbShutdownEvidence {
                                                graph_shutdown_elapsed_us,
                                                shutdown_stages_us,
                                                harness_shutdown_elapsed_us: 0,
                                                write_completion_groups,
                                                dispatch_reasons,
                                                read_stages,
                                                write_service_stages,
                                                command_stages,
                                                writer,
                                                writer_frame_census,
                                                writer_flush_census,
                                                writer_journal_stages,
                                                writer_publication_stages,
                                                completion_lane,
                                                query_execute,
                                                table_inventory_before_measurement: None,
                                                table_inventory_after_measurement: Vec::new(),
                                            })
                                        })
                                    })
                                },
                            )
                        })
                    })
            }),
        (Some(Err(error)), _, _, _, _, _)
        | (_, Some(Err(error)), _, _, _, _)
        | (_, _, Some(Err(error)), _, _, _)
        | (_, _, _, Some(Err(error)), _, _)
        | (_, _, _, _, Some(Err(error)), _)
        | (_, _, _, _, _, Some(Err(error))) => Err(error),
        _ => Err(io::Error::other("incomplete riffdb shutdown telemetry")),
    };
    let _ = shutdown_sender.send(evidence);
    total
}

fn parse_completion_lane(encoded: &str) -> io::Result<RiffDbCompletionLaneEvidence> {
    let mut counts = None;
    let mut elapsed = None;
    let mut max_depth = None;
    let mut max_reorder = None;
    for field in encoded.split(';') {
        let (name, value) = field
            .split_once('=')
            .ok_or_else(|| io::Error::other("invalid completion-lane field"))?;
        match name {
            "counts" => counts = Some(parse_fixed_counts(value, "completion-lane-counts", 4)?),
            "elapsed_us" => {
                elapsed = Some(parse_fixed_counts(value, "completion-lane-elapsed", 4)?)
            }
            "max_depth" => {
                max_depth = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| io::Error::other("invalid completion-lane max depth"))?,
                )
            }
            "max_reorder" => {
                max_reorder = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| io::Error::other("invalid completion-lane max reorder"))?,
                )
            }
            _ => return Err(io::Error::other("unknown completion-lane field")),
        }
    }
    Ok(RiffDbCompletionLaneEvidence {
        phase_counts: counts.ok_or_else(|| io::Error::other("missing completion-lane counts"))?,
        phase_elapsed_us: elapsed
            .ok_or_else(|| io::Error::other("missing completion-lane elapsed"))?,
        max_depth: max_depth
            .ok_or_else(|| io::Error::other("missing completion-lane max depth"))?,
        max_reorder_occupancy: max_reorder
            .ok_or_else(|| io::Error::other("missing completion-lane max reorder"))?,
    })
}

fn parse_query_execute_windows(encoded: &str) -> io::Result<RiffDbQueryExecuteEvidence> {
    let mut fields = encoded.splitn(5, '\t');
    let parse_scalar = |value: Option<&str>, label: &'static str| {
        value
            .ok_or_else(|| io::Error::other(format!("missing {label}")))?
            .parse::<u64>()
            .map_err(|_| io::Error::other(format!("invalid {label}")))
    };
    let window_width = parse_scalar(fields.next(), "query window width")?;
    let window_count = usize::try_from(parse_scalar(fields.next(), "query window count")?)
        .map_err(|_| io::Error::other("query window count overflow"))?;
    let stage_names = fields
        .next()
        .ok_or_else(|| io::Error::other("missing query stage names"))?
        .split(',')
        .map(|name| {
            (!name.is_empty() && name.len() <= 64)
                .then(|| name.to_owned())
                .ok_or_else(|| io::Error::other("invalid query stage name"))
        })
        .collect::<io::Result<Vec<_>>>()?;
    let total_count = parse_scalar(fields.next(), "query total count")?;
    if window_width == 0 || window_count == 0 || window_count > 64 || stage_names.len() != 14 {
        return Err(io::Error::other("invalid query execute shape"));
    }
    let encoded_windows = fields
        .next()
        .ok_or_else(|| io::Error::other("missing query windows"))?;
    let windows = encoded_windows
        .split(';')
        .map(|window| {
            let values = window
                .split(',')
                .map(|value| {
                    value
                        .parse::<u64>()
                        .map_err(|_| io::Error::other("invalid query window value"))
                })
                .collect::<io::Result<Vec<_>>>()?;
            if values.len() != stage_names.len() + 9 {
                return Err(io::Error::other("invalid query window cardinality"));
            }
            Ok(RiffDbQueryExecuteWindowEvidence {
                count: values[0],
                stage_ns: values[1..1 + stage_names.len()].to_vec(),
                overlay_transitions_sum: values[1 + stage_names.len()],
                overlay_transitions_max: values[2 + stage_names.len()],
                overlay_bytes_sum: values[3 + stage_names.len()],
                overlay_bytes_max: values[4 + stage_names.len()],
                authority_tail_bytes_sum: values[5 + stage_names.len()],
                authority_tail_bytes_max: values[6 + stage_names.len()],
                authority_tail_commands_sum: values[7 + stage_names.len()],
                authority_tail_commands_max: values[8 + stage_names.len()],
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    if windows.len() != window_count {
        return Err(io::Error::other("invalid query window count"));
    }
    Ok(RiffDbQueryExecuteEvidence {
        window_width,
        stage_names,
        total_count,
        windows,
    })
}

fn parse_fixed_counts<const N: usize>(
    encoded: &str,
    label: &'static str,
    expected: usize,
) -> io::Result<[u64; N]> {
    let values = encoded
        .split(',')
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| io::Error::other(format!("invalid {label} count")))
        })
        .collect::<io::Result<Vec<_>>>()?;
    if values.len() != expected || expected != N {
        return Err(io::Error::other(format!("invalid {label} count length")));
    }
    values.try_into().map_err(|_| io::Error::other(label))
}

fn parse_shutdown_stages(encoded: &str) -> io::Result<(u64, [u64; 8])> {
    let (graph_elapsed, stages) = encoded
        .split_once('\t')
        .ok_or_else(|| io::Error::other("missing shutdown graph wall"))?;
    let graph_elapsed = graph_elapsed
        .parse::<u64>()
        .map_err(|_| io::Error::other("invalid shutdown graph wall"))?;
    let stages = parse_fixed_counts(stages, "shutdown-stages", 8)?;
    Ok((graph_elapsed, stages))
}

fn optional_shutdown_stages(
    evidence: Option<io::Result<(u64, [u64; 8])>>,
) -> io::Result<(Option<u64>, Option<[u64; 8]>)> {
    match evidence {
        Some(Ok((graph_elapsed_us, stages_us))) => {
            Ok((Some(graph_elapsed_us), Some(stages_us)))
        }
        Some(Err(error)) => Err(error),
        None => Ok((None, None)),
    }
}

fn parse_read_stages(encoded: &str) -> io::Result<Vec<RiffDbReadStageEvidence>> {
    let mut stages = Vec::new();
    for part in encoded.split(';') {
        let mut fields = part.splitn(4, ':');
        let name = fields
            .next()
            .filter(|name| !name.is_empty() && name.len() <= 64)
            .ok_or_else(|| io::Error::other("invalid read-stage name"))?
            .to_owned();
        let parse = |value: Option<&str>| {
            value
                .ok_or_else(|| io::Error::other("missing read-stage counter"))?
                .parse::<u64>()
                .map_err(|_| io::Error::other("invalid read-stage counter"))
        };
        let count = parse(fields.next())?;
        let sum_us = parse(fields.next())?;
        let buckets = fields
            .next()
            .ok_or_else(|| io::Error::other("missing read-stage buckets"))?
            .split(',')
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| io::Error::other("invalid read-stage bucket"))
            })
            .collect::<io::Result<Vec<_>>>()?;
        if buckets.len() != 16 || stages.len() >= 32 {
            return Err(io::Error::other("invalid read-stage cardinality"));
        }
        stages.push(RiffDbReadStageEvidence {
            name,
            count,
            sum_us,
            buckets,
        });
    }
    if stages.is_empty() {
        return Err(io::Error::other("empty read-stage evidence"));
    }
    Ok(stages)
}

fn parse_writer_evidence(encoded: &str) -> io::Result<RiffDbWriterEvidence> {
    let (scalars, histograms) = encoded
        .split_once('\t')
        .ok_or_else(|| io::Error::other("missing writer evidence histograms"))?;
    let mut values = std::collections::BTreeMap::new();
    for pair in scalars.split(';') {
        let (name, value) = pair
            .split_once('=')
            .ok_or_else(|| io::Error::other("invalid writer evidence scalar"))?;
        if values.insert(name, value).is_some() {
            return Err(io::Error::other("duplicate writer evidence scalar"));
        }
    }
    let required = |name: &str| -> io::Result<u64> {
        values
            .get(name)
            .ok_or_else(|| io::Error::other("missing writer evidence scalar"))?
            .parse::<u64>()
            .map_err(|_| io::Error::other("invalid writer evidence scalar"))
    };
    let queue_delay_estimate_us = match values.get("queue_delay_estimate_us").copied() {
        Some("none") => None,
        Some(value) => Some(
            value
                .parse::<u64>()
                .map_err(|_| io::Error::other("invalid writer queue estimate"))?,
        ),
        None => return Err(io::Error::other("missing writer queue estimate")),
    };
    let parsed = parse_read_stages(histograms)?;
    if !matches!(parsed.len(), 5 | 6 | 8 | 9) {
        return Err(io::Error::other("invalid writer histogram cardinality"));
    }
    let find = |name: &str| -> io::Result<RiffDbReadStageEvidence> {
        parsed
            .iter()
            .find(|entry| entry.name == name)
            .cloned()
            .ok_or_else(|| io::Error::other("missing writer histogram"))
    };
    Ok(RiffDbWriterEvidence {
        busy_us: required("busy_us")?,
        idle_us: required("idle_us")?,
        dispatch_selected: required("dispatch_selected")?,
        dispatch_deferred: required("dispatch_deferred")?,
        compatibility_selected: required("compatibility_selected")?,
        compatibility_groups: required("compatibility_groups")?,
        compatibility_conflict_key_splits: required("compatibility_conflict_key_splits")?,
        compatibility_exact_access_splits: required("compatibility_exact_access_splits")?,
        compatibility_commutative_shared_groups: required(
            "compatibility_commutative_shared_groups",
        )?,
        queue_delay_estimate_us,
        commit_duration: find("commit_us")?,
        flush_duration: find("flush_us")?,
        batch_size: find("batch_size")?,
        storage_queue_duration: find("storage_queue_us")?,
        group_residence_duration: parsed
            .iter()
            .find(|entry| entry.name == "group_residence_us")
            .cloned(),
        final_apply_duration: parsed
            .iter()
            .find(|entry| entry.name == "final_apply_us")
            .cloned(),
        journal_submit_duration: find("journal_submit_us")?,
        preparation_pool_depth: parsed
            .iter()
            .find(|entry| entry.name == "preparation_pool_depth")
            .cloned(),
        reorder_buffer_occupancy: parsed
            .iter()
            .find(|entry| entry.name == "reorder_buffer_occupancy")
            .cloned(),
        prepared_epoch_rollbacks: values
            .get("prepared_epoch_rollbacks")
            .map_or(Ok(0), |value| {
                value
                    .parse::<u64>()
                    .map_err(|_| io::Error::other("invalid epoch rollback count"))
            })?,
        prepared_epoch_proof_mismatches: values
            .get("prepared_epoch_proof_mismatches")
            .map_or(Ok(0), |value| {
                value
                    .parse::<u64>()
                    .map_err(|_| io::Error::other("invalid proof mismatch count"))
            })?,
        frontier_equivalence_checks: values
            .get("frontier_equivalence_checks")
            .map_or(Ok(0), |value| {
                value
                    .parse::<u64>()
                    .map_err(|_| io::Error::other("invalid frontier check count"))
            })?,
        frontier_equivalence_failures: values
            .get("frontier_equivalence_failures")
            .map_or(Ok(0), |value| {
                value
                    .parse::<u64>()
                    .map_err(|_| io::Error::other("invalid frontier failure count"))
            })?,
    })
}

fn drain_server_stderr(stream: impl Read, ring: Arc<Mutex<VecDeque<String>>>) -> usize {
    let mut reader = BufReader::new(stream);
    let mut total = 0_usize;
    let mut ring_bytes = 0_usize;
    let mut line = String::new();
    loop {
        line.clear();
        let Ok(read) = reader.read_line(&mut line) else {
            break;
        };
        if read == 0 {
            break;
        }
        // Mirror to parent stderr so panics are visible during a hung load.
        eprint!("[riffdbd-stderr] {line}");
        if let Ok(mut guard) = ring.lock() {
            push_stderr_line(&mut guard, &mut ring_bytes, line.clone());
        }
        total = total.saturating_add(read);
    }
    total
}

fn push_stderr_line(ring: &mut VecDeque<String>, ring_bytes: &mut usize, line: String) {
    *ring_bytes = ring_bytes.saturating_add(line.len());
    ring.push_back(line);
    while ring.len() > STDERR_RING_LINES || (*ring_bytes > STDERR_RING_BYTES && ring.len() > 1) {
        if let Some(front) = ring.pop_front() {
            *ring_bytes = ring_bytes.saturating_sub(front.len());
        } else {
            break;
        }
    }
}

fn reap_child(
    mut child: Child,
    commands: Receiver<ReaperCommand>,
    exit_sender: SyncSender<io::Result<ExitStatus>>,
) {
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let _ = exit_sender.send(Ok(status));
                return;
            }
            Ok(None) => match commands.recv_timeout(Duration::from_millis(10)) {
                Ok(ReaperCommand::Kill) => {
                    let _ = child.kill();
                    let _ = exit_sender.send(child.wait());
                    return;
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return;
                }
            },
            Err(error) => {
                let _ = exit_sender.send(Err(error));
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_application_role_and_query_module_are_exact() {
        let role =
            compile_ticketdesk_application_role().expect("embedded TicketDesk role must compile");
        let grant = application_role_grant_to_proto(role.internal_grant())
            .expect("compiled role grant must lower to the public wire shape");
        assert!(grant.permissions.iter().any(|permission| matches!(
            permission.permission,
            Some(v1::capability_permission::Permission::ReadContract(_))
        )));
        assert!(
            grant
                .field_visibility
                .iter()
                .all(|visibility| visibility.secret_field_ids.is_empty())
        );
    }

    #[test]
    fn database_root_prefers_explicit_override() {
        let override_root = PathBuf::from("/data/riffdb-baseline");
        assert_eq!(
            resolve_database_root(Some(override_root.as_path())),
            override_root
        );
    }

    #[test]
    fn default_database_root_constant_is_not_tmp() {
        let root = PathBuf::from(DEFAULT_DATABASE_ROOT);
        assert!(
            !root.starts_with("/tmp"),
            "default root must not be under /tmp: {}",
            root.display()
        );
        assert_eq!(root.as_os_str(), "target/perf-db/app-baseline");
        // Explicit override always wins over env/default.
        let forced = PathBuf::from("target/perf-db/app-baseline/forced-db");
        assert_eq!(resolve_database_root(Some(forced.as_path())), forced);
    }

    #[test]
    fn sweep_removes_only_session_prefix_dirs() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("perf-db")
            .join(format!("riffdb-baseline-sweep-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let stale = root.join("riffdb-app-baseline-999999-1");
        let keep = root.join("keep-me");
        fs::create_dir_all(&stale).expect("stale");
        fs::create_dir_all(&keep).expect("keep");
        sweep_stale_session_dirs(&root);
        assert!(!stale.exists());
        assert!(keep.exists());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn min_free_bytes_scales_with_mode() {
        assert_eq!(min_free_bytes_for_full(false), MIN_FREE_BYTES_SMOKE);
        assert_eq!(min_free_bytes_for_full(true), MIN_FREE_BYTES_FULL);
        const {
            assert!(MIN_FREE_BYTES_FULL > MIN_FREE_BYTES_SMOKE);
        }
    }

    #[test]
    fn stderr_ring_keeps_last_200_within_byte_cap() {
        let mut ring = VecDeque::new();
        let mut ring_bytes = 0_usize;
        for i in 0..10_000 {
            // Short lines so the line cap (200) binds before the byte cap.
            push_stderr_line(&mut ring, &mut ring_bytes, format!("line-{i}\n"));
        }
        assert_eq!(ring.len(), STDERR_RING_LINES);
        assert!(ring.front().unwrap().starts_with("line-9800"));
        assert!(ring.back().unwrap().starts_with("line-9999"));
        assert!(ring_bytes <= STDERR_RING_BYTES + 64);

        // Byte-cap eviction: huge lines shrink the ring below the line cap.
        let mut ring = VecDeque::new();
        let mut ring_bytes = 0_usize;
        let huge = "x".repeat(8_000) + "\n";
        for _ in 0..20 {
            push_stderr_line(&mut ring, &mut ring_bytes, huge.clone());
        }
        assert!(ring.len() < STDERR_RING_LINES);
        assert!(ring_bytes <= STDERR_RING_BYTES + huge.len());
    }

    #[test]
    fn shutdown_evidence_parsers_reject_shape_drift() {
        let groups = (0_u64..u64::try_from(WRITE_GROUP_BUCKETS).expect("bucket count"))
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let parsed =
            parse_fixed_counts::<WRITE_GROUP_BUCKETS>(&groups, "write-group", WRITE_GROUP_BUCKETS)
                .expect("groups");
        assert_eq!(parsed[WRITE_GROUP_BUCKETS - 1], 255);
        assert!(parse_fixed_counts::<4>("1,2,3", "dispatch", 4).is_err());
        assert_eq!(
            parse_shutdown_stages("9\t1,2,3,4,5,6,7,8")
                .expect("shutdown stages"),
            (9, [1, 2, 3, 4, 5, 6, 7, 8])
        );
        assert!(parse_shutdown_stages("9\t1,2,3,4,5,6,7").is_err());
        assert!(parse_shutdown_stages("1,2,3,4,5,6,7,8").is_err());
        assert_eq!(
            optional_shutdown_stages(None).expect("old server omits additive evidence"),
            (None, None)
        );
        assert_eq!(
            optional_shutdown_stages(Some(Ok((9, [1, 2, 3, 4, 5, 6, 7, 8]))))
                .expect("current shutdown evidence"),
            (Some(9), Some([1, 2, 3, 4, 5, 6, 7, 8]))
        );

        let buckets = (0_u64..16)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let stages =
            parse_read_stages(&format!("authorize:2:9:{buckets}")).expect("read stage evidence");
        assert_eq!(stages[0].name, "authorize");
        assert_eq!(stages[0].count, 2);
        assert_eq!(stages[0].buckets.len(), 16);
        assert!(parse_read_stages("authorize:2:9:1,2").is_err());

        let completion = parse_completion_lane(
            "counts=3,2,1,0;elapsed_us=30,20,10,0;max_depth=7;max_reorder=0",
        )
        .expect("completion lane evidence");
        assert_eq!(completion.phase_counts, [3, 2, 1, 0]);
        assert_eq!(completion.phase_elapsed_us, [30, 20, 10, 0]);
        assert_eq!(completion.max_depth, 7);
        assert_eq!(completion.max_reorder_occupancy, 0);
        assert!(parse_completion_lane(
            "counts=3,2,1;elapsed_us=30,20,10,0;max_depth=7;max_reorder=0"
        )
        .is_err());
        assert!(parse_completion_lane(
            "counts=3,2,1,0;elapsed_us=30,20,10,0;max_depth=7;unknown=0"
        )
        .is_err());

        let writer_histograms = [
            "commit_us",
            "flush_us",
            "batch_size",
            "storage_queue_us",
            "final_apply_us",
            "journal_submit_us",
        ]
            .map(|name| format!("{name}:2:9:{buckets}"))
            .join(";");
        let writer = parse_writer_evidence(&format!(
            "busy_us=11;idle_us=12;dispatch_selected=13;dispatch_deferred=14;compatibility_selected=15;compatibility_groups=16;compatibility_conflict_key_splits=17;compatibility_exact_access_splits=18;compatibility_commutative_shared_groups=19;queue_delay_estimate_us=20\t{writer_histograms}"
        ))
        .expect("writer evidence");
        assert_eq!(writer.busy_us, 11);
        assert_eq!(writer.idle_us, 12);
        assert_eq!(writer.dispatch_selected, 13);
        assert_eq!(writer.dispatch_deferred, 14);
        assert_eq!(writer.compatibility_selected, 15);
        assert_eq!(writer.compatibility_groups, 16);
        assert_eq!(writer.compatibility_conflict_key_splits, 17);
        assert_eq!(writer.compatibility_exact_access_splits, 18);
        assert_eq!(writer.compatibility_commutative_shared_groups, 19);
        assert_eq!(writer.queue_delay_estimate_us, Some(20));
        assert_eq!(writer.commit_duration.name, "commit_us");
        assert_eq!(writer.flush_duration.name, "flush_us");
        assert_eq!(writer.batch_size.name, "batch_size");
        assert_eq!(writer.storage_queue_duration.name, "storage_queue_us");
        assert_eq!(writer.group_residence_duration, None);
        assert_eq!(
            writer.final_apply_duration.as_ref().map(|stage| stage.name.as_str()),
            Some("final_apply_us")
        );
        assert_eq!(writer.journal_submit_duration.name, "journal_submit_us");
        assert_eq!(writer.preparation_pool_depth, None);
        assert_eq!(writer.reorder_buffer_occupancy, None);
        assert_eq!(writer.prepared_epoch_rollbacks, 0);
        assert_eq!(writer.prepared_epoch_proof_mismatches, 0);
        assert_eq!(writer.frontier_equivalence_checks, 0);
        assert_eq!(writer.frontier_equivalence_failures, 0);

        let current_writer_histograms = [
            "commit_us",
            "flush_us",
            "batch_size",
            "storage_queue_us",
            "group_residence_us",
            "final_apply_us",
            "journal_submit_us",
            "preparation_pool_depth",
            "reorder_buffer_occupancy",
        ]
        .map(|name| format!("{name}:2:9:{buckets}"))
        .join(";");
        let current_writer = parse_writer_evidence(&format!(
            "busy_us=11;idle_us=12;dispatch_selected=13;dispatch_deferred=14;compatibility_selected=15;compatibility_groups=16;compatibility_conflict_key_splits=17;compatibility_exact_access_splits=18;compatibility_commutative_shared_groups=19;queue_delay_estimate_us=20\t{current_writer_histograms}"
        ))
        .expect("current writer evidence");
        assert_eq!(
            current_writer
                .group_residence_duration
                .as_ref()
                .map(|stage| stage.name.as_str()),
            Some("group_residence_us")
        );
        assert_eq!(
            current_writer
                .preparation_pool_depth
                .as_ref()
                .map(|stage| stage.name.as_str()),
            Some("preparation_pool_depth")
        );
        assert_eq!(
            current_writer
                .reorder_buffer_occupancy
                .as_ref()
                .map(|stage| stage.name.as_str()),
            Some("reorder_buffer_occupancy")
        );
        assert!(parse_writer_evidence("busy_us=1\tcommit_us:1:1:1,2").is_err());

        let query_stages = riffdb_storage_redb::QUERY_EXECUTE_STAGE_LABELS_V1.join(",");
        let stage_sums = (1_u64..=14)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let query = parse_query_execute_windows(&format!(
            "256\t1\t{query_stages}\t1\t1,{stage_sums},2,3,4,5,6,7,8,9"
        ))
        .expect("query execute evidence");
        assert_eq!(query.total_count, 1);
        assert_eq!(query.windows[0].authority_tail_bytes_sum, 6);
        assert_eq!(query.windows[0].authority_tail_bytes_max, 7);
        assert_eq!(query.windows[0].authority_tail_commands_sum, 8);
        assert_eq!(query.windows[0].authority_tail_commands_max, 9);
        assert!(parse_query_execute_windows(&format!(
            "256\t1\t{query_stages}\t1\t1,{stage_sums},2,3,4,5"
        ))
        .is_err());
    }
}
