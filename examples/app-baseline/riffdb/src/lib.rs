//! Public symbolic TicketDesk adapter against a live `riffdbd`.
//!
//! All application reads use named RiffQL queries. All mutations use symbolic
//! generated commands. Seed uses bounded client-side concurrency over one
//! reusable HTTP/2 channel (same model as `riffdb command batch`).

#![forbid(unsafe_code)]

mod projected;
mod server;

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use riffdb_app_baseline_core::{
    AppBackend, CloseTicketWithCommentSeed, CommentRow, CommentSeed, LabelRow,
    OpenTicketWithLabelsSeed, OrganizationRow, ProjectMemberRow, ProjectRow, SeedDataset,
    SwapMemberRolesSeed, TicketDetailPage, TicketRow, TicketStatus, UserRow, UuidBytes,
    format_uuid,
};
use riffdb_client_rust::ApplicationRecord;
use riffdb_client_rust::{
    ApplicationClientError, ApplicationContract, ApplicationSessionIdentity, ApplicationUuid,
    ApplicationValue, AttemptBudget, BearerCredential, CallMetadata, GeneratedBatchError,
    GeneratedBatchOptions, GeneratedBatchResult, NamedQuery, NamedQueryResult,
    StableApplicationClient,
};
use riffdb_ticketdesk::{
    AddProjectMemberInput, AttachLabelInput, BoardPage50Params, BoardPage50Result,
    BoardPage200Params, BoardPage200Result, BoardPage450Params, BoardPage450Result,
    CloseTicketWithCommentInput, CreateCommentInput, CreateLabelInput, CreateOrganizationInput,
    CreateProjectInput, CreateTicketInput, CreateUserInput, GetTicketParams, GetTicketResult,
    OpenTicketWithLabelsInput, SwapMemberRolesInput, TicketDeskClient, TicketPageParams,
};
use tonic::transport::Endpoint;

pub use projected::{
    BOARD_PROJECTION_NAME, BOARD_ROW_FIELDS, BOARD_SELECT, TicketStatusEnumIds,
    assert_board_rows_equivalent, assert_board_rows_three_way_equivalent, board_rows_digest,
    build_board_projected_request, build_board_projected_request_with_encoding,
    catchup_projected_board, commit_token_bytes, execute_projected_board,
    execute_projected_board_packed, freshness_available, freshness_causal, request_shape,
};
pub use server::{
    DATABASE_ROOT_ENV, DEFAULT_DATABASE_ROOT, MIN_FREE_BYTES, MIN_FREE_BYTES_FULL,
    MIN_FREE_BYTES_SMOKE, RiffDbCompletionLaneEvidence, RiffDbQueryExecuteEvidence,
    RiffDbQueryExecuteWindowEvidence, RiffDbReadStageEvidence, RiffDbServerSession,
    RiffDbShutdownEvidence, RiffDbWriterEvidence, ServerStartOptions, min_free_bytes_for_full,
    resolve_bench_root, resolve_database_root, sweep_stale_session_dirs,
};

/// Default in-flight seed commands (bounded client concurrency, not a bulk RPC).
///
/// The general generated client permits 384 items, but that is a protocol
/// ceiling rather than a safe default for one shared benchmark daemon. At 384,
/// a full 19,220-command seed can produce a burst of request-scoped audit
/// deadline failures large enough to trip the deliberately fail-closed audit
/// readiness threshold. The benchmark therefore owns the smaller operational
/// ceiling it documents. Operators can lower it, but cannot turn an evidence
/// run into an audit-saturation probe through the environment.
const DEFAULT_SEED_CONCURRENCY: usize = 128;
const MAX_SEED_CONCURRENCY: usize = 128;
const TICKETDESK_APPLICATION_LOCK_HASH: [u8; 32] = [
    0xf4, 0x8d, 0x43, 0xb9, 0x5f, 0x60, 0xd2, 0xc9, 0xb9, 0x4d, 0x12, 0x99, 0x80, 0x03, 0x6e, 0x00,
    0x94, 0x01, 0xbe, 0x04, 0xbc, 0x58, 0x66, 0x9b, 0xfe, 0x89, 0x30, 0x88, 0x8e, 0x93, 0xc8, 0x5d,
];

/// Public symbolic application backend.
#[derive(Clone)]
pub struct RiffDbPublicBackend {
    endpoint: Endpoint,
    projected_channel: tokio::sync::OnceCell<tonic::transport::Channel>,
    transport: StableApplicationClient,
    metadata: CallMetadata,
    /// Raw bearer token for the generated ApplicationQueryService client path.
    bearer_token: String,
    command_attempts: AttemptBudget,
    runtime: tokio::runtime::Handle,
    /// TicketStatus enum ids for projected predicates/decoding.
    status_ids: TicketStatusEnumIds,
    /// Exact deployed contract bundle hash used by generated-style named reads.
    contract_bundle_hash: [u8; 32],
    /// Deployed query-module hash (must match NamedQuery requests; generated
    /// TicketDesk client embeds a stale hash that would yield RDB-MODULE-0101).
    query_module_hash: [u8; 32],
    /// Max commit sequence observed during seed (for Causal catch-up).
    last_seed_commit_sequence: Option<u64>,
    /// History incarnation for Causal tokens (fresh DBs use 1).
    history_incarnation: u64,
    /// Whether the projected catch-up + equivalence gates have passed.
    projected_gates_ready: bool,
}

/// One diagnostic-only timing tuple for a synchronous call into the real
/// asynchronous generated client.
///
/// The tuple is produced by one call, so the bridge and asynchronous durations
/// are paired rather than inferred by subtracting unrelated percentiles. It is
/// not part of the frozen PERF-018 report or any application API.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PairedClientTiming {
    /// Caller-observed duration around `Handle::block_on`.
    pub outer: Duration,
    /// Delay from entering `Handle::block_on` until the wrapper future begins.
    pub runtime_entry: Duration,
    /// Time awaiting the generated application-client future.
    pub asynchronous_call: Duration,
    /// Time after the wrapper future completes until `Handle::block_on` returns.
    pub runtime_exit: Duration,
}

impl PairedClientTiming {
    /// Complete synchronous bridge time outside the generated async future.
    #[must_use]
    pub fn bridge_only(self) -> Duration {
        self.runtime_entry.saturating_add(self.runtime_exit)
    }
}

/// One generated result accompanied by its diagnostic-only paired timing.
#[derive(Debug)]
pub struct PairedClientResult<T> {
    /// Decoded generated operation result.
    pub value: T,
    /// Paired client timing for the same request.
    pub timing: PairedClientTiming,
}

impl RiffDbPublicBackend {
    /// One lazily-connected shared channel for the projected wire path, so
    /// timed projected samples ride warm HTTP/2 exactly like compiled ones
    /// ride the client's persistent transport (review must-fix: symmetric
    /// transport).
    async fn projected_channel(&self) -> Result<tonic::transport::Channel, RiffDbError> {
        self.projected_channel
            .get_or_try_init(|| async {
                self.endpoint
                    .connect()
                    .await
                    .map_err(|_| RiffDbError::Connection)
            })
            .await
            .cloned()
    }

    /// Connects to an already bootstrapped TicketDesk-ready endpoint.
    pub async fn connect(
        endpoint: &str,
        bearer_token: &str,
        status_ids: TicketStatusEnumIds,
        contract_bundle_hash: [u8; 32],
        query_module_hash: [u8; 32],
        history_incarnation: u64,
    ) -> Result<Self, RiffDbError> {
        let endpoint = Endpoint::from_shared(endpoint.to_owned())
            .map_err(|_| RiffDbError::Connection)?
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(60));
        let transport = StableApplicationClient::connect(endpoint.clone())
            .await
            .map_err(|_| RiffDbError::Connection)?;
        let metadata = CallMetadata::authenticated(
            BearerCredential::new(bearer_token).map_err(|_| RiffDbError::Connection)?,
        );
        // The harness always connects from inside its persistent runtime; a
        // missing runtime is a recoverable configuration error, not a panic.
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| RiffDbError::Runtime)?;
        Ok(Self {
            endpoint,
            projected_channel: tokio::sync::OnceCell::new(),
            transport,
            metadata,
            bearer_token: bearer_token.to_owned(),
            command_attempts: AttemptBudget::new(3).expect("positive command attempt budget"),
            runtime,
            status_ids,
            contract_bundle_hash,
            query_module_hash,
            last_seed_commit_sequence: None,
            history_incarnation,
            projected_gates_ready: false,
        })
    }

    /// Loopback gRPC URI for an independent generated application client.
    #[must_use]
    pub fn public_grpc_uri(&self) -> String {
        self.endpoint.uri().to_string()
    }

    /// Runner bearer token for an independent generated application client.
    ///
    /// Callers must not log, print, or persist this value except into a
    /// mode-0600 credential file owned by the Python comparison harness.
    #[must_use]
    pub fn runner_bearer_token(&self) -> &str {
        &self.bearer_token
    }

    /// Opens an independent HTTP/2 connection with the same bounded metadata.
    ///
    /// Load comparisons use this to match PostgreSQL's one-connection-per-session
    /// topology. Credentials remain encapsulated in redacting metadata.
    pub fn fresh_session(&self) -> Result<Self, RiffDbError> {
        let endpoint = self.endpoint.clone();
        let metadata = self.metadata.clone();
        let runtime = self.runtime.clone();
        let transport = runtime
            .block_on(StableApplicationClient::connect(endpoint.clone()))
            .map_err(|_| RiffDbError::Connection)?;
        Ok(Self {
            projected_channel: tokio::sync::OnceCell::new(),
            endpoint,
            transport,
            metadata,
            bearer_token: self.bearer_token.clone(),
            command_attempts: self.command_attempts,
            runtime,
            status_ids: self.status_ids,
            contract_bundle_hash: self.contract_bundle_hash,
            query_module_hash: self.query_module_hash,
            last_seed_commit_sequence: self.last_seed_commit_sequence,
            history_incarnation: self.history_incarnation,
            projected_gates_ready: self.projected_gates_ready,
        })
    }

    /// Opens ADR-0127's explicitly non-evidentiary bounded application
    /// session on an independent HTTP/2 connection.
    pub fn fresh_bounded_session(&self) -> Result<Self, RiffDbError> {
        let mut backend = self.fresh_session()?;
        let identity = ApplicationSessionIdentity::new(
            "TicketDesk".to_owned(),
            1,
            self.contract_bundle_hash,
            vec![self.query_module_hash],
            TICKETDESK_APPLICATION_LOCK_HASH,
            128,
        )
        .map_err(|_| RiffDbError::Connection)?;
        backend
            .runtime
            .clone()
            .block_on(
                backend
                    .transport
                    .open_bounded_session(identity, &backend.metadata),
            )
            .map_err(map_app)?;
        Ok(backend)
    }

    /// Ends an explicitly selected bounded session before the harness asks
    /// its colocated daemon to perform a graceful shutdown.
    pub fn close_bounded_session(&mut self) {
        self.transport.close_bounded_session();
    }

    /// Opens an independent HTTP/2 connection without crossing the
    /// synchronous benchmark bridge.
    ///
    /// This exists only for WP-632's non-evidentiary asynchronous shadow. The
    /// frozen PERF-018 driver continues to use [`Self::fresh_session`].
    pub async fn fresh_session_async(&self) -> Result<Self, RiffDbError> {
        let transport = StableApplicationClient::connect(self.endpoint.clone())
            .await
            .map_err(|_| RiffDbError::Connection)?;
        Ok(Self {
            projected_channel: tokio::sync::OnceCell::new(),
            endpoint: self.endpoint.clone(),
            transport,
            metadata: self.metadata.clone(),
            bearer_token: self.bearer_token.clone(),
            command_attempts: self.command_attempts,
            runtime: self.runtime.clone(),
            status_ids: self.status_ids,
            contract_bundle_hash: self.contract_bundle_hash,
            query_module_hash: self.query_module_hash,
            last_seed_commit_sequence: self.last_seed_commit_sequence,
            history_incarnation: self.history_incarnation,
            projected_gates_ready: self.projected_gates_ready,
        })
    }

    /// Reconnects this logical application session to a restarted local daemon.
    async fn reconnect_endpoint(&self, endpoint: &str) -> Result<Self, RiffDbError> {
        let mut reconnected = Self::connect(
            endpoint,
            &self.bearer_token,
            self.status_ids,
            self.contract_bundle_hash,
            self.query_module_hash,
            self.history_incarnation,
        )
        .await?;
        reconnected.command_attempts = self.command_attempts;
        reconnected.last_seed_commit_sequence = self.last_seed_commit_sequence;
        reconnected.projected_gates_ready = self.projected_gates_ready;
        Ok(reconnected)
    }

    async fn execute_named(
        &self,
        name: &str,
        parameters: BTreeMap<String, ApplicationValue>,
    ) -> Result<NamedQueryResult, RiffDbError> {
        let query = NamedQuery::new(
            ApplicationContract::Exact {
                lineage: "TicketDesk".to_owned(),
                version: 1,
                bundle_hash: Some(self.contract_bundle_hash),
            },
            name.to_owned(),
            Some(self.query_module_hash),
            parameters,
            None,
        )
        .map_err(map_app)?;
        // Clone the stable client (shared channel) so &self call sites can run
        // named queries inside block_on without exclusive self borrows.
        let mut transport = self.transport.clone();
        transport
            .execute_named_query(query, &self.metadata)
            .await
            .map_err(map_app)
    }

    /// Selects the explicit command transport-submission budget.
    #[must_use]
    pub fn with_command_attempt_budget(mut self, maximum_submissions: u32) -> Self {
        if let Some(budget) = AttemptBudget::new(maximum_submissions) {
            self.command_attempts = budget;
        }
        self
    }

    fn ticketdesk(&self) -> TicketDeskClient {
        TicketDeskClient::new(
            self.transport.clone(),
            self.metadata.clone(),
            self.command_attempts,
        )
    }

    /// Drives a backend future on the persistent harness runtime.
    ///
    /// Every `AppBackend` method is called from the synchronous benchmark
    /// thread, never from async context, so `Handle::block_on` is safe here
    /// and no per-call runtime is ever constructed.
    fn block_on<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, RiffDbError>>,
    ) -> Result<T, RiffDbError> {
        self.runtime.clone().block_on(future)
    }

    fn block_on_paired<T>(
        &self,
        future: impl Future<Output = Result<T, RiffDbError>>,
    ) -> Result<PairedClientResult<T>, RiffDbError> {
        let outer_started = Instant::now();
        let (runtime_entry, asynchronous_call, result) = self.runtime.clone().block_on(async {
            let runtime_entry = outer_started.elapsed();
            let async_started = Instant::now();
            let result = future.await;
            (runtime_entry, async_started.elapsed(), result)
        });
        let outer = outer_started.elapsed();
        let runtime_exit = outer.saturating_sub(runtime_entry.saturating_add(asynchronous_call));
        Ok(PairedClientResult {
            value: result?,
            timing: PairedClientTiming {
                outer,
                runtime_entry,
                asynchronous_call,
                runtime_exit,
            },
        })
    }

    /// Executes generated `GetTicket` directly on the asynchronous application
    /// client used by real Rust applications.
    ///
    /// WP-632 uses this as a non-evidentiary shadow of the frozen synchronous
    /// comparison. It invokes the same server operation and does not introduce
    /// a transport or semantic fallback.
    pub async fn get_ticket_generated_async(
        &self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
    ) -> Result<Option<TicketRow>, RiffDbError> {
        let result = self
            .ticketdesk()
            .get_ticket(GetTicketParams {
                organization_id: uuid_text(organization_id),
                ticket_id: uuid_text(ticket_id),
            })
            .await
            .map_err(map_app)?;
        match result {
            GetTicketResult::Found(found) => Ok(Some(TicketRow {
                organization_id,
                ticket_id: parse_uuid_text(&found.ticket.ticket_id)?,
                project_id: parse_uuid_text(&found.ticket.project_id)?,
                reporter_id: parse_uuid_text(&found.ticket.reporter_id)?,
                assignee_id: parse_uuid_text(&found.ticket.assignee_id)?,
                status: parse_status_name(&found.ticket.status)?,
                title: found.ticket.title,
            })),
            GetTicketResult::NotFound(_) => Ok(None),
        }
    }

    /// Executes the same generated `GetTicket` through the frozen driver's
    /// synchronous runtime bridge and returns paired diagnostic timing.
    pub fn get_ticket_generated_paired(
        &self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
    ) -> Result<PairedClientResult<Option<TicketRow>>, RiffDbError> {
        self.block_on_paired(self.get_ticket_generated_async(organization_id, ticket_id))
    }

    /// Exercises one generated command followed by an explicit generated
    /// read-after-commit query on the currently selected transport.
    ///
    /// This is process-test support for ADR-0127. It does not add a public
    /// database transaction or infer freshness from stream order.
    pub fn create_comment_then_fenced_ticket_page(
        &self,
        comment: &CommentSeed,
    ) -> Result<(u64, u64), RiffDbError> {
        self.block_on(async {
            let mut client = self.ticketdesk();
            let committed = client
                .create_comment(CreateCommentInput {
                    body: comment.row.body.clone(),
                    author_id: uuid_text(comment.row.author_id),
                    ticket_id: uuid_text(comment.row.ticket_id),
                    comment_id: uuid_text(comment.row.comment_id),
                    idempotency_key: comment.idempotency_key.clone(),
                    organization_id: uuid_text(comment.row.organization_id),
                })
                .await
                .map_err(map_app)?;
            let commit_sequence = committed.commit_sequence.ok_or_else(|| {
                RiffDbError::Rpc("generated command omitted its durable commit sequence".into())
            })?;
            let page = client
                .ticket_page_after_commit(
                    TicketPageParams {
                        organization_id: uuid_text(comment.row.organization_id),
                        ticket_id: uuid_text(comment.row.ticket_id),
                    },
                    commit_sequence,
                )
                .await
                .map_err(map_app)?;
            if page.application_head < commit_sequence {
                return Err(RiffDbError::Rpc(
                    "generated read returned below its explicit commit fence".into(),
                ));
            }
            Ok((commit_sequence, page.application_head))
        })
    }
}

fn seed_concurrency() -> usize {
    std::env::var("RIFFDB_SEED_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_SEED_CONCURRENCY)
        .clamp(1, MAX_SEED_CONCURRENCY)
}

impl RiffDbPublicBackend {
    /// Catch-up + cross-path equivalence gates (must pass before projected timing).
    ///
    /// Causal to the last seed commit proves the projection reached the head;
    /// then each board page size is compared three-way: compiled vs projected-row
    /// vs projected-packed (never path-to-self). Divergence aborts with digests.
    pub fn prepare_projected_board_gates(
        &mut self,
        dataset: &SeedDataset,
    ) -> Result<(), RiffDbError> {
        let dense = dataset.board_dense_open_count();
        let limits: Vec<u32> = [50_u32, 200, 450]
            .into_iter()
            .filter(|&limit| dense >= limit as usize)
            .collect();
        if limits.is_empty() {
            self.projected_gates_ready = true;
            return Ok(());
        }
        let probes = dataset.probes();
        let sequence = self.last_seed_commit_sequence.ok_or_else(|| {
            RiffDbError::Rpc("projected catch-up requires a seed commit sequence".into())
        })?;
        let token = commit_token_bytes(self.history_incarnation, sequence)?;
        self.block_on(async {
            let channel = self.projected_channel().await?;
            catchup_projected_board(
                &channel,
                &self.bearer_token,
                probes.board_organization_id,
                probes.board_project_id,
                probes.open_status,
                self.status_ids,
                token,
            )
            .await
        })?;
        for limit in limits {
            let compiled = self.board_page(
                probes.board_organization_id,
                probes.board_project_id,
                probes.open_status,
                limit,
            )?;
            // Equivalence uses the projected wire path directly (gates not yet open
            // for timed samples). Never compare a path to itself.
            let (projected_row, projected_packed) = self.block_on(async {
                let channel = self.projected_channel().await?;
                let row = execute_projected_board(
                    &channel,
                    &self.bearer_token,
                    probes.board_organization_id,
                    probes.board_project_id,
                    probes.open_status,
                    limit,
                    self.status_ids,
                    freshness_available(),
                )
                .await?;
                let packed = execute_projected_board_packed(
                    &channel,
                    &self.bearer_token,
                    probes.board_organization_id,
                    probes.board_project_id,
                    probes.open_status,
                    limit,
                    self.status_ids,
                    freshness_available(),
                )
                .await?;
                Ok((row, packed))
            })?;
            assert_board_rows_three_way_equivalent(
                &compiled,
                &projected_row,
                &projected_packed,
                limit,
            )
            .map_err(RiffDbError::Rpc)?;
        }
        self.projected_gates_ready = true;
        eprintln!(
            "projected-board gates ok: catch-up Ready at commit_sequence={sequence}; \
             compiled vs projected-row vs projected-packed row content identical for \
             limits that fit the dense cell"
        );
        Ok(())
    }
}

impl AppBackend for RiffDbPublicBackend {
    type Error = RiffDbError;

    fn reset(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn prewarm(&mut self) -> Result<(), Self::Error> {
        // gRPC channel + auth are established at connect; touch one cheap read
        // so named-query path and HTTP/2 stream are warm before timed windows.
        let _ = self.point_get_ticket([0; 16], [0; 16]);
        Ok(())
    }

    fn load_error_class(error: &Self::Error) -> riffdb_app_baseline_core::LoadErrorClass {
        use riffdb_app_baseline_core::LoadErrorClass;
        match error {
            RiffDbError::Application { code, .. } => match code.as_str() {
                // Unequal same-key reuse is a harness/application bug, not contention.
                "RDB-COMMAND-0101" => LoadErrorClass::IdempotencyMismatch,
                // Declared command business failure (includes uniqueness-style outcomes
                // surfaced as execution failures on some paths).
                "RDB-COMMAND-0102" => LoadErrorClass::Conflict,
                // Temporary storage unavailability.
                "RDB-STORAGE-0101" => LoadErrorClass::Unavailable,
                // Result exceeds service limit under concurrent load.
                "RDB-RESOURCE-0101" => LoadErrorClass::Unavailable,
                // Typed capacity rejection — certain-not-executed, retryable.
                "RDB-CAPACITY-0101" => LoadErrorClass::Overloaded,
                // History incarnation fence after restore.
                "RDB-HISTORY-0101" => LoadErrorClass::HistoryIncarnationMismatch,
                _ => LoadErrorClass::Other,
            },
            RiffDbError::Connection | RiffDbError::Runtime | RiffDbError::Server { .. } => {
                LoadErrorClass::Unavailable
            }
            _ => LoadErrorClass::Other,
        }
    }

    fn load_error_code(error: &Self::Error) -> Option<&str> {
        match error {
            RiffDbError::Application { code, .. } => Some(code.as_str()),
            _ => None,
        }
    }

    fn seed(&mut self, dataset: &SeedDataset) -> Result<(), Self::Error> {
        self.projected_gates_ready = false;
        let max_commit = self.block_on(async {
            let concurrency = seed_concurrency();
            let total = dataset.organizations.len()
                + dataset.users.len()
                + dataset.projects.len()
                + dataset.members.len()
                + dataset.labels.len()
                + dataset.tickets.len()
                + dataset.comments.len()
                + dataset.ticket_labels.len();
            let progress = Arc::new(SeedProgress::new(total));
            let max_commit = Arc::new(AtomicUsize::new(0));
            eprintln!("riffdb-seed-start\ttotal={total}\tconcurrency={concurrency}");

            // Phases respect foreign-key order. Each phase uses bounded public
            // transport batches; every item remains an ordinary independent
            // generated command with its own durable lifecycle.
            let options = GeneratedBatchOptions::new(concurrency).map_err(map_generated_batch)?;
            macro_rules! run_seed_batches {
                ($phase:literal, $inputs:expr, $method:ident) => {{
                    let inputs = $inputs;
                    for chunk in inputs.chunks(4_096) {
                        finish_generated_batch(
                            &progress,
                            &max_commit,
                            $phase,
                            self.ticketdesk().$method(chunk.to_vec(), options).await,
                        )?;
                    }
                }};
            }
            let organizations = dataset
                .organizations
                .iter()
                .map(|org| CreateOrganizationInput {
                    name: org.name.clone(),
                    organization_id: uuid_text(org.organization_id),
                    idempotency_key: format!("seed-org-{}", encode_short(org.organization_id)),
                })
                .collect::<Vec<_>>();
            run_seed_batches!("organization", organizations, create_organization_batch);

            let users = dataset
                .users
                .iter()
                .map(|user| CreateUserInput {
                    email: user.email.clone(),
                    display_name: user.display_name.clone(),
                    user_id: uuid_text(user.user_id),
                    idempotency_key: format!("seed-user-{}", encode_short(user.user_id)),
                    organization_id: uuid_text(user.organization_id),
                })
                .collect::<Vec<_>>();
            run_seed_batches!("user", users, create_user_batch);

            let projects = dataset
                .projects
                .iter()
                .map(|project| CreateProjectInput {
                    name: project.name.clone(),
                    project_id: uuid_text(project.project_id),
                    idempotency_key: format!("seed-project-{}", encode_short(project.project_id)),
                    organization_id: uuid_text(project.organization_id),
                })
                .collect::<Vec<_>>();
            run_seed_batches!("project", projects, create_project_batch);

            let members = spread_conflict_domains(&dataset.members, |member| {
                (member.organization_id, member.project_id)
            })
            .into_iter()
            .map(|member| AddProjectMemberInput {
                role: member.role.clone(),
                user_id: uuid_text(member.user_id),
                project_id: uuid_text(member.project_id),
                idempotency_key: format!(
                    "seed-member-{}-{}",
                    encode_short(member.project_id),
                    encode_short(member.user_id)
                ),
                organization_id: uuid_text(member.organization_id),
            })
            .collect::<Vec<_>>();
            run_seed_batches!("member", members, add_project_member_batch);

            let labels = dataset
                .labels
                .iter()
                .map(|label| CreateLabelInput {
                    name: label.name.clone(),
                    label_id: uuid_text(label.label_id),
                    idempotency_key: format!("seed-label-{}", encode_short(label.label_id)),
                    organization_id: uuid_text(label.organization_id),
                })
                .collect::<Vec<_>>();
            run_seed_batches!("label", labels, create_label_batch);

            let tickets = dataset
                .tickets
                .iter()
                .map(|ticket| CreateTicketInput {
                    title: ticket.title.clone(),
                    status: status_name(ticket.status).to_owned(),
                    ticket_id: uuid_text(ticket.ticket_id),
                    project_id: uuid_text(ticket.project_id),
                    assignee_id: uuid_text(ticket.assignee_id),
                    reporter_id: uuid_text(ticket.reporter_id),
                    idempotency_key: format!("seed-ticket-{}", encode_short(ticket.ticket_id)),
                    organization_id: uuid_text(ticket.organization_id),
                })
                .collect::<Vec<_>>();
            run_seed_batches!("ticket", tickets, create_ticket_batch);

            let comments = spread_conflict_domains(&dataset.comments, |comment| {
                (comment.organization_id, comment.ticket_id)
            })
            .into_iter()
            .map(|comment| CreateCommentInput {
                body: comment.body.clone(),
                author_id: uuid_text(comment.author_id),
                ticket_id: uuid_text(comment.ticket_id),
                comment_id: uuid_text(comment.comment_id),
                idempotency_key: format!("seed-comment-{}", encode_short(comment.comment_id)),
                organization_id: uuid_text(comment.organization_id),
            })
            .collect::<Vec<_>>();
            run_seed_batches!("comment", comments, create_comment_batch);

            let links = spread_conflict_domains(&dataset.ticket_labels, |link| {
                (link.organization_id, link.ticket_id)
            })
            .into_iter()
            .map(|link| AttachLabelInput {
                label_id: uuid_text(link.label_id),
                ticket_id: uuid_text(link.ticket_id),
                idempotency_key: format!(
                    "seed-link-{}-{}",
                    encode_short(link.ticket_id),
                    encode_short(link.label_id)
                ),
                organization_id: uuid_text(link.organization_id),
            })
            .collect::<Vec<_>>();
            run_seed_batches!("ticket_label", links, attach_label_batch);

            progress.finish();
            Ok::<u64, RiffDbError>(max_commit.load(Ordering::Relaxed) as u64)
        })?;
        self.last_seed_commit_sequence = (max_commit > 0).then_some(max_commit);
        Ok(())
    }

    fn point_get_ticket(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
    ) -> Result<Option<TicketRow>, Self::Error> {
        self.block_on(async {
            let mut parameters = BTreeMap::new();
            parameters.insert(
                "organization_id".to_owned(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes(organization_id)),
            );
            parameters.insert(
                "ticket_id".to_owned(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes(ticket_id)),
            );
            let result = self.execute_named("GetTicket", parameters).await?;
            if result.outcome != "Found" {
                return Ok(None);
            }
            let ticket = result
                .fields
                .get("ticket")
                .and_then(|field| field.records.first())
                .ok_or(RiffDbError::Decode)?;
            Ok(Some(decode_ticket_record(ticket, organization_id)?))
        })
    }

    fn point_get_user(
        &mut self,
        organization_id: UuidBytes,
        user_id: UuidBytes,
    ) -> Result<Option<UserRow>, Self::Error> {
        self.block_on(async {
            let mut parameters = BTreeMap::new();
            parameters.insert(
                "organization_id".to_owned(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes(organization_id)),
            );
            parameters.insert(
                "user_id".to_owned(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes(user_id)),
            );
            let result = self.execute_named("GetUser", parameters).await?;
            if result.outcome != "Found" {
                return Ok(None);
            }
            let user = result
                .fields
                .get("user")
                .and_then(|field| field.records.first())
                .ok_or(RiffDbError::Decode)?;
            Ok(Some(UserRow {
                organization_id,
                user_id: value_uuid(user.fields.get("user_id"))?,
                email: value_string(user.fields.get("email"))?,
                display_name: value_string(user.fields.get("display_name"))?,
            }))
        })
    }

    fn list_tickets_by_project_status(
        &mut self,
        organization_id: UuidBytes,
        project_id: UuidBytes,
        status: TicketStatus,
        limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error> {
        self.block_on(async {
            let mut parameters = BTreeMap::new();
            parameters.insert(
                "organization_id".to_owned(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes(organization_id)),
            );
            parameters.insert(
                "project_id".to_owned(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes(project_id)),
            );
            parameters.insert(
                "statuses".to_owned(),
                ApplicationValue::List(vec![ApplicationValue::Enum(
                    status_name(status).to_owned(),
                )]),
            );
            parameters.insert("limit".to_owned(), ApplicationValue::U64(u64::from(limit)));
            let result = self.execute_named("ListTickets", parameters).await?;
            if result.outcome != "Found" {
                return Ok(Vec::new());
            }
            decode_ticket_list_field(&result, "tickets", organization_id)
        })
    }

    fn board_page_projected(
        &mut self,
        organization_id: UuidBytes,
        project_id: UuidBytes,
        status: TicketStatus,
        limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error> {
        if !self.projected_gates_ready {
            return Err(RiffDbError::Rpc(
                "projected board measured before catch-up/equivalence gates".into(),
            ));
        }
        self.block_on(async {
            execute_projected_board(
                &self.projected_channel().await?,
                &self.bearer_token,
                organization_id,
                project_id,
                status,
                limit,
                self.status_ids,
                freshness_available(),
            )
            .await
        })
    }

    fn board_page_packed(
        &mut self,
        organization_id: UuidBytes,
        project_id: UuidBytes,
        status: TicketStatus,
        limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error> {
        if !self.projected_gates_ready {
            return Err(RiffDbError::Rpc(
                "projected board measured before catch-up/equivalence gates".into(),
            ));
        }
        self.block_on(async {
            execute_projected_board_packed(
                &self.projected_channel().await?,
                &self.bearer_token,
                organization_id,
                project_id,
                status,
                limit,
                self.status_ids,
                freshness_available(),
            )
            .await
        })
    }

    fn board_page(
        &mut self,
        organization_id: UuidBytes,
        project_id: UuidBytes,
        status: TicketStatus,
        limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error> {
        // Static BoardPage50/200/450 only. take 500 (static or runtime Limit)
        // trips MAX_QUERY_SCANNED_ROWS=500 via continuation probe (scan 501 →
        // RDB-INTERNAL-0001; incident 019fbf5b-1a64-7877-94c3-47d7a0763539).
        self.block_on(async {
            let organization = uuid_text(organization_id);
            let project = uuid_text(project_id);
            let status = status_name(status).to_owned();
            let mut client = self.ticketdesk();
            match limit {
                50 => match client
                    .board_page50(BoardPage50Params {
                        organization_id: organization,
                        project_id: project,
                        status,
                    })
                    .await
                    .map_err(map_app)?
                {
                    BoardPage50Result::Found(found) => found
                        .tickets
                        .into_iter()
                        .map(|row| generated_board_row(
                            organization_id,
                            row.ticket_id,
                            row.project_id,
                            row.reporter_id,
                            row.assignee_id,
                            row.status,
                            row.title,
                        ))
                        .collect(),
                },
                200 => match client
                    .board_page200(BoardPage200Params {
                        organization_id: organization,
                        project_id: project,
                        status,
                    })
                    .await
                    .map_err(map_app)?
                {
                    BoardPage200Result::Found(found) => found
                        .tickets
                        .into_iter()
                        .map(|row| generated_board_row(
                            organization_id,
                            row.ticket_id,
                            row.project_id,
                            row.reporter_id,
                            row.assignee_id,
                            row.status,
                            row.title,
                        ))
                        .collect(),
                },
                450 => match client
                    .board_page450(BoardPage450Params {
                        organization_id: organization,
                        project_id: project,
                        status,
                    })
                    .await
                    .map_err(map_app)?
                {
                    BoardPage450Result::Found(found) => found
                        .tickets
                        .into_iter()
                        .map(|row| generated_board_row(
                            organization_id,
                            row.ticket_id,
                            row.project_id,
                            row.reporter_id,
                            row.assignee_id,
                            row.status,
                            row.title,
                        ))
                        .collect(),
                },
                other => Err(RiffDbError::Application {
                    code: "RDB-INTERNAL-0001".to_owned(),
                    detail: format!(
                        "board_page limit {other} has no static BoardPage query \
                         (only 50/200/450; take 500 trips MAX_QUERY_SCANNED_ROWS via continuation probe)"
                    ),
                }),
            }
        })
    }

    fn list_open_tickets_for_assignee(
        &mut self,
        organization_id: UuidBytes,
        assignee_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error> {
        self.block_on(async {
            let mut parameters = BTreeMap::new();
            parameters.insert(
                "organization_id".to_owned(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes(organization_id)),
            );
            parameters.insert(
                "assignee_id".to_owned(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes(assignee_id)),
            );
            parameters.insert(
                "statuses".to_owned(),
                ApplicationValue::List(vec![ApplicationValue::Enum("Open".to_owned())]),
            );
            parameters.insert("limit".to_owned(), ApplicationValue::U64(u64::from(limit)));
            let result = self
                .execute_named("ListTicketsByAssignee", parameters)
                .await?;
            if result.outcome != "Found" {
                return Ok(Vec::new());
            }
            decode_ticket_list_field(&result, "tickets", organization_id)
        })
    }

    fn list_comments_for_ticket(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<CommentRow>, Self::Error> {
        self.block_on(async {
            let mut parameters = BTreeMap::new();
            parameters.insert(
                "organization_id".to_owned(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes(organization_id)),
            );
            parameters.insert(
                "ticket_id".to_owned(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes(ticket_id)),
            );
            parameters.insert("limit".to_owned(), ApplicationValue::U64(u64::from(limit)));
            let result = self.execute_named("ListComments", parameters).await?;
            if result.outcome != "Found" {
                return Ok(Vec::new());
            }
            let comments = result.fields.get("comments").ok_or(RiffDbError::Decode)?;
            comments
                .records
                .iter()
                .map(|record| {
                    Ok(CommentRow {
                        organization_id,
                        comment_id: value_uuid(record.fields.get("comment_id"))?,
                        ticket_id,
                        author_id: value_uuid(record.fields.get("author_id"))?,
                        body: value_string(record.fields.get("body"))?,
                    })
                })
                .collect()
        })
    }

    fn list_project_members(
        &mut self,
        organization_id: UuidBytes,
        project_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<ProjectMemberRow>, Self::Error> {
        self.block_on(async {
            let mut parameters = BTreeMap::new();
            parameters.insert(
                "organization_id".to_owned(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes(organization_id)),
            );
            parameters.insert(
                "project_id".to_owned(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes(project_id)),
            );
            let result = self.execute_named("ProjectMembers", parameters).await?;
            if result.outcome != "Found" {
                return Ok(Vec::new());
            }
            let members = result.fields.get("members").ok_or(RiffDbError::Decode)?;
            members
                .records
                .iter()
                .take(limit as usize)
                .map(|record| {
                    Ok(ProjectMemberRow {
                        organization_id,
                        project_id,
                        user_id: value_uuid(record.fields.get("user_id"))?,
                        role: value_string(record.fields.get("role"))?,
                    })
                })
                .collect()
        })
    }

    fn ticket_detail_page(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
        comment_limit: u32,
    ) -> Result<Option<TicketDetailPage>, Self::Error> {
        self.block_on(async {
            let mut parameters = BTreeMap::new();
            parameters.insert(
                "organization_id".to_owned(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes(organization_id)),
            );
            parameters.insert(
                "ticket_id".to_owned(),
                ApplicationValue::Uuid(ApplicationUuid::from_bytes(ticket_id)),
            );
            let result = self.execute_named("TicketPage", parameters).await?;
            match result.outcome.as_str() {
                "NotFound" => Ok(None),
                "IntegrityFailure" => Err(RiffDbError::Rpc("TicketPage integrity failure".into())),
                "Found" => {
                    let ticket_rec = result
                        .fields
                        .get("ticket")
                        .and_then(|field| field.records.first())
                        .ok_or(RiffDbError::Decode)?;
                    let project_rec = result
                        .fields
                        .get("project")
                        .and_then(|field| field.records.first())
                        .ok_or(RiffDbError::Decode)?;
                    let org_rec = result
                        .fields
                        .get("organization")
                        .and_then(|field| field.records.first())
                        .ok_or(RiffDbError::Decode)?;
                    let reporter_rec = result
                        .fields
                        .get("reporter")
                        .and_then(|field| field.records.first())
                        .ok_or(RiffDbError::Decode)?;
                    let project_id = value_uuid(project_rec.fields.get("project_id"))?;
                    let assignee = result
                        .fields
                        .get("assignee")
                        .and_then(|field| field.records.first())
                        .map(|record| {
                            Ok(UserRow {
                                organization_id,
                                user_id: value_uuid(record.fields.get("user_id"))?,
                                email: String::new(),
                                display_name: value_string(record.fields.get("display_name"))?,
                            })
                        })
                        .transpose()?;
                    let assignee_id = assignee
                        .as_ref()
                        .map(|user| user.user_id)
                        .unwrap_or([0; 16]);
                    let comments = result
                        .fields
                        .get("comments")
                        .map(|field| {
                            field
                                .records
                                .iter()
                                .take(comment_limit as usize)
                                .map(|record| {
                                    Ok(CommentRow {
                                        organization_id,
                                        comment_id: value_uuid(record.fields.get("comment_id"))?,
                                        ticket_id,
                                        author_id: value_uuid(record.fields.get("author_id"))?,
                                        body: value_string(record.fields.get("body"))?,
                                    })
                                })
                                .collect::<Result<Vec<_>, RiffDbError>>()
                        })
                        .transpose()?
                        .unwrap_or_default();
                    let labels = result
                        .fields
                        .get("labels")
                        .map(|field| {
                            field
                                .records
                                .iter()
                                .map(|record| {
                                    Ok(LabelRow {
                                        organization_id,
                                        label_id: value_uuid(record.fields.get("label_id"))?,
                                        name: value_string(record.fields.get("name"))?,
                                    })
                                })
                                .collect::<Result<Vec<_>, RiffDbError>>()
                        })
                        .transpose()?
                        .unwrap_or_default();
                    Ok(Some(TicketDetailPage {
                        ticket: TicketRow {
                            organization_id,
                            ticket_id: value_uuid(ticket_rec.fields.get("ticket_id"))?,
                            project_id,
                            reporter_id: value_uuid(reporter_rec.fields.get("user_id"))?,
                            assignee_id,
                            status: value_status(ticket_rec.fields.get("status"))?,
                            title: value_string(ticket_rec.fields.get("title"))?,
                        },
                        project: ProjectRow {
                            organization_id,
                            project_id,
                            name: value_string(project_rec.fields.get("name"))?,
                        },
                        organization: OrganizationRow {
                            organization_id: value_uuid(org_rec.fields.get("organization_id"))?,
                            name: value_string(org_rec.fields.get("name"))?,
                        },
                        assignee,
                        comments,
                        labels,
                    }))
                }
                other => Err(RiffDbError::Rpc(format!(
                    "TicketPage unexpected outcome {other}"
                ))),
            }
        })
    }

    fn create_comment(&mut self, comment: &CommentSeed) -> Result<(), Self::Error> {
        self.block_on(async {
            self.ticketdesk()
                .create_comment(CreateCommentInput {
                    body: comment.row.body.clone(),
                    author_id: uuid_text(comment.row.author_id),
                    ticket_id: uuid_text(comment.row.ticket_id),
                    comment_id: uuid_text(comment.row.comment_id),
                    idempotency_key: comment.idempotency_key.clone(),
                    organization_id: uuid_text(comment.row.organization_id),
                })
                .await
                .map_err(map_app)?;
            Ok(())
        })
    }

    fn replay_comment(&mut self, comment: &CommentSeed) -> Result<(), Self::Error> {
        // RiffDB detects the repeated idempotency key and equal typed input,
        // returning the stored command outcome without applying a second row.
        self.create_comment(comment)
    }

    fn close_ticket_with_comment(
        &mut self,
        input: &CloseTicketWithCommentSeed,
    ) -> Result<(), Self::Error> {
        self.block_on(async {
            self.ticketdesk()
                .close_ticket_with_comment(CloseTicketWithCommentInput {
                    body: input.body.clone(),
                    author_id: uuid_text(input.author_id),
                    ticket_id: uuid_text(input.ticket_id),
                    comment_id: uuid_text(input.comment_id),
                    idempotency_key: input.idempotency_key.clone(),
                    organization_id: uuid_text(input.organization_id),
                })
                .await
                .map_err(map_app)?;
            Ok(())
        })
    }

    fn swap_member_roles(&mut self, input: &SwapMemberRolesSeed) -> Result<(), Self::Error> {
        self.block_on(async {
            self.ticketdesk()
                .swap_member_roles(SwapMemberRolesInput {
                    role_a: input.role_a.clone(),
                    role_b: input.role_b.clone(),
                    user_a: uuid_text(input.user_a),
                    user_b: uuid_text(input.user_b),
                    project_id: uuid_text(input.project_id),
                    idempotency_key: input.idempotency_key.clone(),
                    organization_id: uuid_text(input.organization_id),
                })
                .await
                .map_err(map_app)?;
            Ok(())
        })
    }

    fn open_ticket_with_labels(
        &mut self,
        input: &OpenTicketWithLabelsSeed,
    ) -> Result<(), Self::Error> {
        self.block_on(async {
            self.ticketdesk()
                .open_ticket_with_labels(OpenTicketWithLabelsInput {
                    title: input.title.clone(),
                    label_a: uuid_text(input.label_a),
                    label_b: uuid_text(input.label_b),
                    ticket_id: uuid_text(input.ticket_id),
                    project_id: uuid_text(input.project_id),
                    assignee_id: uuid_text(input.assignee_id),
                    reporter_id: uuid_text(input.reporter_id),
                    idempotency_key: input.idempotency_key.clone(),
                    organization_id: uuid_text(input.organization_id),
                })
                .await
                .map_err(map_app)?;
            Ok(())
        })
    }
}

fn spread_conflict_domains<T, K: Ord>(items: &[T], key: impl Fn(&T) -> K) -> Vec<&T> {
    let mut lanes = BTreeMap::<K, VecDeque<&T>>::new();
    for item in items {
        lanes.entry(key(item)).or_default().push_back(item);
    }
    let mut spread = Vec::with_capacity(items.len());
    loop {
        let mut added = false;
        for lane in lanes.values_mut() {
            if let Some(item) = lane.pop_front() {
                spread.push(item);
                added = true;
            }
        }
        if !added {
            break;
        }
    }
    spread
}

fn finish_generated_batch<T>(
    progress: &SeedProgress,
    max_commit: &AtomicUsize,
    phase: &'static str,
    result: Result<GeneratedBatchResult<T>, GeneratedBatchError>,
) -> Result<(), RiffDbError> {
    let result = result.map_err(map_generated_batch)?;
    for item in result.items {
        let typed = item
            .result
            .map_err(ApplicationClientError::from)
            .map_err(map_app)?;
        if let Some(sequence) = typed.commit_sequence {
            let sequence = usize::try_from(sequence).unwrap_or(usize::MAX);
            max_commit.fetch_max(sequence, Ordering::Relaxed);
        }
        progress.tick(phase);
    }
    Ok(())
}

fn map_generated_batch(error: GeneratedBatchError) -> RiffDbError {
    RiffDbError::Rpc(error.to_string())
}

struct SeedProgress {
    total: usize,
    completed: AtomicUsize,
    started: std::time::Instant,
    last_report_ms: AtomicUsize,
}

impl SeedProgress {
    fn new(total: usize) -> Self {
        Self {
            total,
            completed: AtomicUsize::new(0),
            started: std::time::Instant::now(),
            last_report_ms: AtomicUsize::new(0),
        }
    }

    fn tick(&self, phase: &str) {
        let completed = self.completed.fetch_add(1, Ordering::Relaxed) + 1;
        let overall_ms = self.started.elapsed().as_millis() as usize;
        let last = self.last_report_ms.load(Ordering::Relaxed);
        if (completed == self.total || overall_ms.saturating_sub(last) >= 2_000)
            && (self
                .last_report_ms
                .compare_exchange(last, overall_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
                || completed == self.total)
        {
            let overall_ops = if overall_ms == 0 {
                0.0
            } else {
                (completed as f64) * 1000.0 / (overall_ms as f64)
            };
            eprintln!(
                "riffdb-seed-progress\tphase={phase}\tcompleted={completed}/{}\toverall_ms={overall_ms}\trate_ops_s={overall_ops:.1}",
                self.total
            );
        }
    }

    fn finish(&self) {
        let completed = self.completed.load(Ordering::Relaxed);
        let overall_ms = self.started.elapsed().as_millis();
        let overall_ops = if overall_ms == 0 {
            0.0
        } else {
            (completed as f64) * 1000.0 / (overall_ms as f64)
        };
        eprintln!(
            "riffdb-seed-progress\tphase=done\tcompleted={completed}/{}\toverall_ms={overall_ms}\trate_ops_s={overall_ops:.1}",
            self.total
        );
    }
}

fn uuid_text(bytes: UuidBytes) -> String {
    format_uuid(bytes)
}

fn parse_uuid_text(text: &str) -> Result<UuidBytes, RiffDbError> {
    let bytes = text.as_bytes();
    if bytes.len() != 36
        || [8, 13, 18, 23]
            .into_iter()
            .any(|index| bytes[index] != b'-')
    {
        return Err(RiffDbError::Decode);
    }
    let mut out = [0_u8; 16];
    let mut source = 0_usize;
    for byte in &mut out {
        if matches!(source, 8 | 13 | 18 | 23) {
            source = source.saturating_add(1);
        }
        let high = hex_nibble(bytes[source]).ok_or(RiffDbError::Decode)?;
        let low = hex_nibble(bytes[source + 1]).ok_or(RiffDbError::Decode)?;
        *byte = (high << 4) | low;
        source = source.saturating_add(2);
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn parse_status_name(name: &str) -> Result<TicketStatus, RiffDbError> {
    match name {
        "Open" => Ok(TicketStatus::Open),
        "Closed" => Ok(TicketStatus::Closed),
        "InProgress" => Ok(TicketStatus::InProgress),
        _ => Err(RiffDbError::Decode),
    }
}

fn generated_board_row(
    organization_id: UuidBytes,
    ticket_id: String,
    project_id: String,
    reporter_id: String,
    assignee_id: String,
    status: String,
    title: String,
) -> Result<TicketRow, RiffDbError> {
    Ok(TicketRow {
        organization_id,
        ticket_id: parse_uuid_text(&ticket_id)?,
        project_id: parse_uuid_text(&project_id)?,
        reporter_id: parse_uuid_text(&reporter_id)?,
        assignee_id: parse_uuid_text(&assignee_id)?,
        status: parse_status_name(&status)?,
        title,
    })
}

fn status_name(status: TicketStatus) -> &'static str {
    match status {
        TicketStatus::Open => "Open",
        TicketStatus::Closed => "Closed",
        TicketStatus::InProgress => "InProgress",
    }
}

fn decode_ticket_list_field(
    result: &NamedQueryResult,
    field: &str,
    organization_id: UuidBytes,
) -> Result<Vec<TicketRow>, RiffDbError> {
    let tickets = result.fields.get(field).ok_or(RiffDbError::Decode)?;
    tickets
        .records
        .iter()
        .map(|record| decode_ticket_record(record, organization_id))
        .collect()
}

fn decode_ticket_record(
    record: &ApplicationRecord,
    organization_id: UuidBytes,
) -> Result<TicketRow, RiffDbError> {
    Ok(TicketRow {
        organization_id,
        ticket_id: value_uuid(record.fields.get("ticket_id"))?,
        project_id: value_uuid(record.fields.get("project_id"))?,
        reporter_id: value_uuid(record.fields.get("reporter_id"))?,
        assignee_id: value_uuid(record.fields.get("assignee_id"))?,
        status: value_status(record.fields.get("status"))?,
        title: value_string(record.fields.get("title"))?,
    })
}

fn value_uuid(value: Option<&ApplicationValue>) -> Result<UuidBytes, RiffDbError> {
    match value {
        Some(ApplicationValue::Uuid(uuid)) => Ok(*uuid.as_bytes()),
        Some(ApplicationValue::String(text)) => {
            if text.len() != 36 {
                return Err(RiffDbError::Decode);
            }
            let mut out = [0_u8; 16];
            let hex =
                |i: usize| u8::from_str_radix(&text[i..i + 2], 16).map_err(|_| RiffDbError::Decode);
            let positions = [0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34];
            for (index, start) in positions.into_iter().enumerate() {
                out[index] = hex(start)?;
            }
            Ok(out)
        }
        _ => Err(RiffDbError::Decode),
    }
}

fn value_string(value: Option<&ApplicationValue>) -> Result<String, RiffDbError> {
    match value {
        Some(ApplicationValue::String(text)) => Ok(text.clone()),
        _ => Err(RiffDbError::Decode),
    }
}

fn value_status(value: Option<&ApplicationValue>) -> Result<TicketStatus, RiffDbError> {
    match value {
        Some(ApplicationValue::Enum(name)) | Some(ApplicationValue::String(name)) => {
            match name.as_str() {
                "Open" => Ok(TicketStatus::Open),
                "Closed" => Ok(TicketStatus::Closed),
                "InProgress" => Ok(TicketStatus::InProgress),
                _ => Err(RiffDbError::Decode),
            }
        }
        Some(ApplicationValue::EnumIdentity { name, .. }) => match name.as_str() {
            "Open" => Ok(TicketStatus::Open),
            "Closed" => Ok(TicketStatus::Closed),
            "InProgress" => Ok(TicketStatus::InProgress),
            _ => Err(RiffDbError::Decode),
        },
        _ => Err(RiffDbError::Decode),
    }
}

fn encode_short(bytes: UuidBytes) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}",
        bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

fn map_app(error: ApplicationClientError) -> RiffDbError {
    // Prefer the versioned application-error object (WP-315): code, category,
    // recovery action, operation identity, and optional safe context only.
    if let Some(semantic) = error.semantic_error() {
        let mut detail = format!(
            "application {} category={} recovery={} operation={}",
            semantic.code().as_str(),
            semantic.category().as_str(),
            semantic.recovery_action().as_str(),
            semantic.operation().as_str(),
        );
        if let Some(symbol) = semantic.context().operation_symbol() {
            detail.push_str(" symbol=");
            detail.push_str(symbol);
        }
        detail.push_str(": ");
        detail.push_str(semantic.safe_message());
        if let Some(incident) = semantic.incident_id() {
            detail.push_str(" incident=");
            detail.push_str(&incident.to_string());
        }
        return RiffDbError::Application {
            code: semantic.code().as_str().to_owned(),
            detail,
        };
    }
    RiffDbError::Rpc(format!("{error:?}"))
}

/// Public RiffDB adapter errors.
#[derive(Clone, Debug)]
pub enum RiffDbError {
    /// Bad endpoint/credential.
    Connection,
    /// Bootstrap failed.
    Bootstrap,
    /// Contract or query-module deploy failed.
    Deploy,
    /// Server process failed (includes exit status / stderr tail when known).
    Server {
        /// Bounded diagnostic from the harness (exit status, last stderr lines).
        detail: String,
    },
    /// Filesystem error.
    Io,
    /// Application semantic error with stable `RDB-*` code.
    Application {
        /// Stable public application error code (e.g. `RDB-STORAGE-0101`).
        code: String,
        /// Bounded public detail (not used for load classification).
        detail: String,
    },
    /// Non-application RPC/transport failure.
    Rpc(String),
    /// Runtime missing.
    Runtime,
    /// Decode failure.
    Decode,
}

impl fmt::Display for RiffDbError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection => formatter.write_str("riffdb connection failed"),
            Self::Bootstrap => formatter.write_str("riffdb bootstrap failed"),
            Self::Deploy => formatter.write_str("riffdb contract/query module deploy failed"),
            Self::Server { detail } => write!(formatter, "riffdbd process failed: {detail}"),
            Self::Io => formatter.write_str("riffdb harness io failed"),
            Self::Application { code, detail } => {
                write!(formatter, "riffdb application {code}: {detail}")
            }
            Self::Rpc(detail) => write!(formatter, "riffdb rpc failed: {detail}"),
            Self::Runtime => formatter.write_str("riffdb async runtime unavailable"),
            Self::Decode => formatter.write_str("riffdb response decode failed"),
        }
    }
}

impl Error for RiffDbError {}

#[cfg(test)]
mod tests {
    use riffdb_app_baseline_core::{AppBackend, LoadErrorClass};

    use super::{RiffDbError, RiffDbPublicBackend, parse_uuid_text};

    #[test]
    fn generated_uuid_text_parser_preserves_binary_identity() {
        let expected = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
            0xcd, 0xef,
        ];
        assert_eq!(
            parse_uuid_text("01234567-89ab-cdef-0123-456789abcdef")
                .expect("lowercase UUID text must decode"),
            expected
        );
        assert_eq!(
            parse_uuid_text("01234567-89AB-CDEF-0123-456789ABCDEF")
                .expect("uppercase UUID text must decode"),
            expected
        );
        assert!(parse_uuid_text("0123456789ab-cdef-0123-456789abcdef").is_err());
    }

    fn application_error(code: &str) -> RiffDbError {
        RiffDbError::Application {
            code: code.to_owned(),
            detail: "bounded public detail".to_owned(),
        }
    }

    #[test]
    fn load_classification_uses_stable_application_code() {
        assert_eq!(
            RiffDbPublicBackend::load_error_class(&application_error("RDB-COMMAND-0101")),
            LoadErrorClass::IdempotencyMismatch
        );
        assert_eq!(
            RiffDbPublicBackend::load_error_class(&application_error("RDB-STORAGE-0101")),
            LoadErrorClass::Unavailable
        );
        assert_eq!(
            RiffDbPublicBackend::load_error_class(&application_error("RDB-CAPACITY-0101")),
            LoadErrorClass::Overloaded
        );
        assert_eq!(
            RiffDbPublicBackend::load_error_class(&application_error("RDB-HISTORY-0101")),
            LoadErrorClass::HistoryIncarnationMismatch
        );
        assert_eq!(
            RiffDbPublicBackend::load_error_class(&application_error("RDB-QUERY-0101")),
            LoadErrorClass::Other
        );
        assert_eq!(
            RiffDbPublicBackend::load_error_code(&application_error("RDB-STORAGE-0101")),
            Some("RDB-STORAGE-0101")
        );
    }
}
