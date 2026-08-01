//! Public symbolic TicketDesk adapter against a live `riffdbd`.
//!
//! All application reads use named RiffQL queries. All mutations use symbolic
//! generated commands. Seed uses bounded client-side concurrency over one
//! reusable HTTP/2 channel (same model as `riffdb command batch`).

#![forbid(unsafe_code)]

mod server;

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use riffdb_app_baseline_core::{
    AppBackend, CloseTicketWithCommentSeed, CommentRow, CommentSeed, LabelRow,
    OpenTicketWithLabelsSeed, OrganizationRow, ProjectMemberRow, ProjectRow, SeedDataset,
    SwapMemberRolesSeed, TicketDetailPage, TicketRow, TicketStatus, UserRow, UuidBytes,
    format_uuid,
};
use riffdb_client_rust::{
    ApplicationClientError, AttemptBudget, BearerCredential, CallMetadata, GeneratedBatchError,
    GeneratedBatchOptions, GeneratedBatchResult, StableApplicationClient,
};
use riffdb_ticketdesk::{
    AddProjectMemberInput, AttachLabelInput, BoardPage50Params, BoardPage50Result,
    BoardPage200Params, BoardPage200Result, BoardPage500Params, BoardPage500Result,
    CloseTicketWithCommentInput, CreateCommentInput, CreateLabelInput, CreateOrganizationInput,
    CreateProjectInput, CreateTicketInput, CreateUserInput, GetTicketParams, GetTicketResult,
    GetUserParams, GetUserResult, ListCommentsParams, ListCommentsResult,
    ListTicketsByAssigneeParams, ListTicketsByAssigneeResult, ListTicketsParams, ListTicketsResult,
    OpenTicketWithLabelsInput, ProjectMembersParams, ProjectMembersResult, SwapMemberRolesInput,
    TicketDeskClient, TicketPageParams, TicketPageResult,
};
use tonic::transport::Endpoint;

pub use server::{
    DATABASE_ROOT_ENV, DEFAULT_DATABASE_ROOT, MIN_FREE_BYTES, MIN_FREE_BYTES_FULL,
    MIN_FREE_BYTES_SMOKE, RiffDbServerSession, ServerStartOptions, min_free_bytes_for_full,
    resolve_bench_root, resolve_database_root, sweep_stale_session_dirs,
};

/// Default in-flight seed commands (bounded client concurrency, not a bulk RPC).
const DEFAULT_SEED_CONCURRENCY: usize = 128;
const MAX_SEED_CONCURRENCY: usize = 128;

/// Public symbolic application backend.
#[derive(Clone)]
pub struct RiffDbPublicBackend {
    endpoint: Endpoint,
    transport: StableApplicationClient,
    metadata: CallMetadata,
    command_attempts: AttemptBudget,
    runtime: tokio::runtime::Handle,
}

impl RiffDbPublicBackend {
    /// Connects to an already bootstrapped TicketDesk-ready endpoint.
    pub async fn connect(endpoint: &str, bearer_token: &str) -> Result<Self, RiffDbError> {
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
            transport,
            metadata,
            command_attempts: AttemptBudget::new(3).expect("positive command attempt budget"),
            runtime,
        })
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
            endpoint,
            transport,
            metadata,
            command_attempts: self.command_attempts,
            runtime,
        })
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
}

fn seed_concurrency() -> usize {
    std::env::var("RIFFDB_SEED_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_SEED_CONCURRENCY)
        .clamp(1, MAX_SEED_CONCURRENCY)
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
        self.block_on(async {
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
            Ok(())
        })
    }

    fn point_get_ticket(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
    ) -> Result<Option<TicketRow>, Self::Error> {
        self.block_on(async {
            match self
                .ticketdesk()
                .get_ticket(GetTicketParams {
                    organization_id: uuid_text(organization_id),
                    ticket_id: uuid_text(ticket_id),
                })
                .await
                .map_err(map_app)?
            {
                GetTicketResult::Found(found) => Ok(Some(TicketRow {
                    organization_id,
                    ticket_id: parse_uuid(&found.ticket.ticket_id)?,
                    project_id: parse_uuid(&found.ticket.project_id)?,
                    reporter_id: parse_uuid(&found.ticket.reporter_id)?,
                    assignee_id: parse_uuid(&found.ticket.assignee_id)?,
                    status: parse_status(&found.ticket.status)?,
                    title: found.ticket.title,
                })),
                GetTicketResult::NotFound(_) => Ok(None),
            }
        })
    }

    fn point_get_user(
        &mut self,
        organization_id: UuidBytes,
        user_id: UuidBytes,
    ) -> Result<Option<UserRow>, Self::Error> {
        self.block_on(async {
            match self
                .ticketdesk()
                .get_user(GetUserParams {
                    organization_id: uuid_text(organization_id),
                    user_id: uuid_text(user_id),
                })
                .await
                .map_err(map_app)?
            {
                GetUserResult::Found(found) => Ok(Some(UserRow {
                    organization_id,
                    user_id: parse_uuid(&found.user.user_id)?,
                    email: found.user.email,
                    display_name: found.user.display_name,
                })),
                GetUserResult::NotFound(_) => Ok(None),
            }
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
            let ListTicketsResult::Found(found) = self
                .ticketdesk()
                .list_tickets(ListTicketsParams {
                    organization_id: uuid_text(organization_id),
                    project_id: uuid_text(project_id),
                    statuses: vec![status_name(status).to_owned()],
                    after: None,
                    limit: u64::from(limit),
                })
                .await
                .map_err(map_app)?;
            found
                .tickets
                .into_iter()
                .map(|ticket| {
                    Ok(TicketRow {
                        organization_id,
                        ticket_id: parse_uuid(&ticket.ticket_id)?,
                        project_id: parse_uuid(&ticket.project_id)?,
                        reporter_id: parse_uuid(&ticket.reporter_id)?,
                        assignee_id: parse_uuid(&ticket.assignee_id)?,
                        status: parse_status(&ticket.status)?,
                        title: ticket.title,
                    })
                })
                .collect()
        })
    }

    fn board_page(
        &mut self,
        organization_id: UuidBytes,
        project_id: UuidBytes,
        status: TicketStatus,
        limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error> {
        // Static-compiled limits only: runtime `take $limit` hits RDB-INTERNAL-0001
        // (incident 019fbf5b-1a64-7877-94c3-47d7a0763539). Map harness page sizes
        // onto BoardPage50/200/500 named queries.
        self.block_on(async {
            let org = uuid_text(organization_id);
            let project = uuid_text(project_id);
            let status_s = status_name(status).to_owned();
            // Each static query has a distinct generated row type; decode to
            // TicketRow inside each arm so the match unifies.
            match limit {
                50 => {
                    let BoardPage50Result::Found(found) = self
                        .ticketdesk()
                        .board_page50(BoardPage50Params {
                            organization_id: org,
                            project_id: project,
                            status: status_s,
                        })
                        .await
                        .map_err(map_app)?;
                    found
                        .tickets
                        .into_iter()
                        .map(|ticket| {
                            Ok(TicketRow {
                                organization_id,
                                ticket_id: parse_uuid(&ticket.ticket_id)?,
                                project_id: parse_uuid(&ticket.project_id)?,
                                reporter_id: parse_uuid(&ticket.reporter_id)?,
                                assignee_id: parse_uuid(&ticket.assignee_id)?,
                                status: parse_status(&ticket.status)?,
                                title: ticket.title,
                            })
                        })
                        .collect()
                }
                200 => {
                    let BoardPage200Result::Found(found) = self
                        .ticketdesk()
                        .board_page200(BoardPage200Params {
                            organization_id: org,
                            project_id: project,
                            status: status_s,
                        })
                        .await
                        .map_err(map_app)?;
                    found
                        .tickets
                        .into_iter()
                        .map(|ticket| {
                            Ok(TicketRow {
                                organization_id,
                                ticket_id: parse_uuid(&ticket.ticket_id)?,
                                project_id: parse_uuid(&ticket.project_id)?,
                                reporter_id: parse_uuid(&ticket.reporter_id)?,
                                assignee_id: parse_uuid(&ticket.assignee_id)?,
                                status: parse_status(&ticket.status)?,
                                title: ticket.title,
                            })
                        })
                        .collect()
                }
                500 => {
                    let BoardPage500Result::Found(found) = self
                        .ticketdesk()
                        .board_page500(BoardPage500Params {
                            organization_id: org,
                            project_id: project,
                            status: status_s,
                        })
                        .await
                        .map_err(map_app)?;
                    found
                        .tickets
                        .into_iter()
                        .map(|ticket| {
                            Ok(TicketRow {
                                organization_id,
                                ticket_id: parse_uuid(&ticket.ticket_id)?,
                                project_id: parse_uuid(&ticket.project_id)?,
                                reporter_id: parse_uuid(&ticket.reporter_id)?,
                                assignee_id: parse_uuid(&ticket.assignee_id)?,
                                status: parse_status(&ticket.status)?,
                                title: ticket.title,
                            })
                        })
                        .collect()
                }
                other => Err(RiffDbError::Application {
                    code: "RDB-INTERNAL-0001".to_owned(),
                    detail: format!(
                        "board_page limit {other} has no static BoardPage query \
                         (only 50/200/500; parameterized take is broken at execute)"
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
            let ListTicketsByAssigneeResult::Found(found) = self
                .ticketdesk()
                .list_tickets_by_assignee(ListTicketsByAssigneeParams {
                    organization_id: uuid_text(organization_id),
                    assignee_id: uuid_text(assignee_id),
                    statuses: vec![status_name(TicketStatus::Open).to_owned()],
                    after: None,
                    limit: u64::from(limit),
                })
                .await
                .map_err(map_app)?;
            found
                .tickets
                .into_iter()
                .map(|ticket| {
                    Ok(TicketRow {
                        organization_id,
                        ticket_id: parse_uuid(&ticket.ticket_id)?,
                        project_id: parse_uuid(&ticket.project_id)?,
                        reporter_id: parse_uuid(&ticket.reporter_id)?,
                        assignee_id: parse_uuid(&ticket.assignee_id)?,
                        status: parse_status(&ticket.status)?,
                        title: ticket.title,
                    })
                })
                .collect()
        })
    }

    fn list_comments_for_ticket(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<CommentRow>, Self::Error> {
        self.block_on(async {
            let ListCommentsResult::Found(found) = self
                .ticketdesk()
                .list_comments(ListCommentsParams {
                    organization_id: uuid_text(organization_id),
                    ticket_id: uuid_text(ticket_id),
                    after: None,
                    limit: u64::from(limit),
                })
                .await
                .map_err(map_app)?;
            found
                .comments
                .into_iter()
                .map(|comment| {
                    Ok(CommentRow {
                        organization_id,
                        comment_id: parse_uuid(&comment.comment_id)?,
                        ticket_id,
                        author_id: parse_uuid(&comment.author_id)?,
                        body: comment.body,
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
            let ProjectMembersResult::Found(found) = self
                .ticketdesk()
                .project_members(ProjectMembersParams {
                    organization_id: uuid_text(organization_id),
                    project_id: uuid_text(project_id),
                    after: None,
                })
                .await
                .map_err(map_app)?;
            found
                .members
                .into_iter()
                .take(limit as usize)
                .map(|member| {
                    Ok(ProjectMemberRow {
                        organization_id,
                        project_id,
                        user_id: parse_uuid(&member.user_id)?,
                        role: member.role,
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
            match self
                .ticketdesk()
                .ticket_page(TicketPageParams {
                    organization_id: uuid_text(organization_id),
                    ticket_id: uuid_text(ticket_id),
                    comments_after: None,
                })
                .await
                .map_err(map_app)?
            {
                TicketPageResult::NotFound(_) => Ok(None),
                TicketPageResult::IntegrityFailure(_) => {
                    Err(RiffDbError::Rpc("TicketPage integrity failure".into()))
                }
                TicketPageResult::Found(page) => {
                    let project_id = parse_uuid(&page.project.project_id)?;
                    let assignee_id = page
                        .assignee
                        .as_ref()
                        .map(|assignee| parse_uuid(&assignee.user_id))
                        .transpose()?
                        .unwrap_or([0; 16]);
                    let assignee = page
                        .assignee
                        .map(|assignee| {
                            Ok(UserRow {
                                organization_id,
                                user_id: parse_uuid(&assignee.user_id)?,
                                email: String::new(),
                                display_name: assignee.display_name,
                            })
                        })
                        .transpose()?;
                    let comments = page
                        .comments
                        .into_iter()
                        .take(comment_limit as usize)
                        .map(|comment| {
                            Ok(CommentRow {
                                organization_id,
                                comment_id: parse_uuid(&comment.comment_id)?,
                                ticket_id,
                                author_id: parse_uuid(&comment.author_id)?,
                                body: comment.body,
                            })
                        })
                        .collect::<Result<Vec<_>, RiffDbError>>()?;
                    let labels = page
                        .labels
                        .into_iter()
                        .map(|label| {
                            Ok(LabelRow {
                                organization_id,
                                label_id: parse_uuid(&label.label_id)?,
                                name: label.name,
                            })
                        })
                        .collect::<Result<Vec<_>, RiffDbError>>()?;
                    Ok(Some(TicketDetailPage {
                        ticket: TicketRow {
                            organization_id,
                            ticket_id: parse_uuid(&page.ticket.ticket_id)?,
                            project_id,
                            reporter_id: parse_uuid(&page.reporter.user_id)?,
                            assignee_id,
                            status: parse_status(&page.ticket.status)?,
                            title: page.ticket.title,
                        },
                        project: ProjectRow {
                            organization_id,
                            project_id,
                            name: page.project.name,
                        },
                        organization: OrganizationRow {
                            organization_id: parse_uuid(&page.organization.organization_id)?,
                            name: page.organization.name,
                        },
                        assignee,
                        comments,
                        labels,
                    }))
                }
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
    phase: &'static str,
    result: Result<GeneratedBatchResult<T>, GeneratedBatchError>,
) -> Result<(), RiffDbError> {
    let result = result.map_err(map_generated_batch)?;
    for item in result.items {
        item.result
            .map_err(ApplicationClientError::from)
            .map_err(map_app)?;
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

fn parse_uuid(text: &str) -> Result<UuidBytes, RiffDbError> {
    if text.len() != 36 {
        return Err(RiffDbError::Decode);
    }
    let mut out = [0_u8; 16];
    let hex = |i: usize| u8::from_str_radix(&text[i..i + 2], 16).map_err(|_| RiffDbError::Decode);
    let positions = [0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34];
    for (index, start) in positions.into_iter().enumerate() {
        out[index] = hex(start)?;
    }
    Ok(out)
}

fn status_name(status: TicketStatus) -> &'static str {
    match status {
        TicketStatus::Open => "Open",
        TicketStatus::Closed => "Closed",
        TicketStatus::InProgress => "InProgress",
    }
}

fn parse_status(name: &str) -> Result<TicketStatus, RiffDbError> {
    match name {
        "Open" => Ok(TicketStatus::Open),
        "Closed" => Ok(TicketStatus::Closed),
        "InProgress" => Ok(TicketStatus::InProgress),
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

    use super::{RiffDbError, RiffDbPublicBackend};

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
