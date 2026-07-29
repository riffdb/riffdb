//! Public symbolic TicketDesk adapter against a live `riffdbd`.
//!
//! All application reads use named RiffQL queries. All mutations use symbolic
//! command invocation. There is no GetEntity/ScanIndex/field-id path here.

#![forbid(unsafe_code)]

mod server;

use std::error::Error;
use std::fmt;

use riffdb_app_baseline_core::{
    AppBackend, CommentRow, CommentSeed, LabelRow, OrganizationRow, ProjectMemberRow, ProjectRow,
    SeedDataset, TicketDetailPage, TicketRow, TicketStatus, UserRow, UuidBytes, format_uuid,
};
use riffdb_client_rust::{BearerCredential, CallMetadata, StableApplicationClient};
use riffdb_ticketdesk::{
    AddProjectMemberInput, AttachLabelInput, CreateCommentInput, CreateLabelInput,
    CreateOrganizationInput, CreateProjectInput, CreateTicketInput, CreateUserInput, GetTicketParams,
    GetTicketResult, GetUserParams, GetUserResult, ListCommentsParams, ListCommentsResult,
    ListTicketsByAssigneeParams, ListTicketsByAssigneeResult, ListTicketsParams, ListTicketsResult,
    ProjectMembersParams, ProjectMembersResult, TicketDeskClient, TicketPageParams, TicketPageResult,
};
use tonic::transport::Endpoint;

pub use server::RiffDbServerSession;

/// Public symbolic application backend.
pub struct RiffDbPublicBackend {
    client: TicketDeskClient,
}

impl RiffDbPublicBackend {
    /// Connects to an already bootstrapped TicketDesk-ready endpoint.
    pub async fn connect(endpoint: &str, bearer_token: &str) -> Result<Self, RiffDbError> {
        let endpoint = Endpoint::from_shared(endpoint.to_owned())
            .map_err(|_| RiffDbError::Connection)?
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(60));
        let client = StableApplicationClient::connect(endpoint)
            .await
            .map_err(|_| RiffDbError::Connection)?;
        let metadata = CallMetadata::authenticated(
            BearerCredential::new(bearer_token).map_err(|_| RiffDbError::Connection)?,
        );
        Ok(Self {
            client: TicketDeskClient::new(client, metadata),
        })
    }
}

fn block_on_runtime<T>(
    future: impl std::future::Future<Output = Result<T, RiffDbError>>,
) -> Result<T, RiffDbError> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| RiffDbError::Runtime)?;
            runtime.block_on(future)
        }
    }
}

impl AppBackend for RiffDbPublicBackend {
    type Error = RiffDbError;

    fn reset(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn seed(&mut self, dataset: &SeedDataset) -> Result<(), Self::Error> {
        block_on_runtime(async {
            for org in &dataset.organizations {
                self.client
                    .create_organization(CreateOrganizationInput {
                        name: org.name.clone(),
                        organization_id: uuid_text(org.organization_id),
                        idempotency_key: format!(
                            "seed-org-{}",
                            encode_short(org.organization_id)
                        ),
                    })
                    .await
                    .map_err(map_app)?;
            }
            for user in &dataset.users {
                self.client
                    .create_user(CreateUserInput {
                        email: user.email.clone(),
                        display_name: user.display_name.clone(),
                        user_id: uuid_text(user.user_id),
                        idempotency_key: format!("seed-user-{}", encode_short(user.user_id)),
                        organization_id: uuid_text(user.organization_id),
                    })
                    .await
                    .map_err(map_app)?;
            }
            for project in &dataset.projects {
                self.client
                    .create_project(CreateProjectInput {
                        name: project.name.clone(),
                        project_id: uuid_text(project.project_id),
                        idempotency_key: format!(
                            "seed-project-{}",
                            encode_short(project.project_id)
                        ),
                        organization_id: uuid_text(project.organization_id),
                    })
                    .await
                    .map_err(map_app)?;
            }
            for member in &dataset.members {
                self.client
                    .add_project_member(AddProjectMemberInput {
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
                    .await
                    .map_err(map_app)?;
            }
            for label in &dataset.labels {
                self.client
                    .create_label(CreateLabelInput {
                        name: label.name.clone(),
                        label_id: uuid_text(label.label_id),
                        idempotency_key: format!("seed-label-{}", encode_short(label.label_id)),
                        organization_id: uuid_text(label.organization_id),
                    })
                    .await
                    .map_err(map_app)?;
            }
            for ticket in &dataset.tickets {
                self.client
                    .create_ticket(CreateTicketInput {
                        title: ticket.title.clone(),
                        status: status_name(ticket.status).to_owned(),
                        ticket_id: uuid_text(ticket.ticket_id),
                        project_id: uuid_text(ticket.project_id),
                        assignee_id: uuid_text(ticket.assignee_id),
                        reporter_id: uuid_text(ticket.reporter_id),
                        idempotency_key: format!(
                            "seed-ticket-{}",
                            encode_short(ticket.ticket_id)
                        ),
                        organization_id: uuid_text(ticket.organization_id),
                    })
                    .await
                    .map_err(map_app)?;
            }
            for comment in &dataset.comments {
                self.client
                    .create_comment(CreateCommentInput {
                        body: comment.body.clone(),
                        author_id: uuid_text(comment.author_id),
                        ticket_id: uuid_text(comment.ticket_id),
                        comment_id: uuid_text(comment.comment_id),
                        idempotency_key: format!(
                            "seed-comment-{}",
                            encode_short(comment.comment_id)
                        ),
                        organization_id: uuid_text(comment.organization_id),
                    })
                    .await
                    .map_err(map_app)?;
            }
            for link in &dataset.ticket_labels {
                self.client
                    .attach_label(AttachLabelInput {
                        label_id: uuid_text(link.label_id),
                        ticket_id: uuid_text(link.ticket_id),
                        idempotency_key: format!(
                            "seed-link-{}-{}",
                            encode_short(link.ticket_id),
                            encode_short(link.label_id)
                        ),
                        organization_id: uuid_text(link.organization_id),
                    })
                    .await
                    .map_err(map_app)?;
            }
            Ok(())
        })
    }

    fn point_get_ticket(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
    ) -> Result<Option<TicketRow>, Self::Error> {
        block_on_runtime(async {
            match self
                .client
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
        block_on_runtime(async {
            match self
                .client
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
        block_on_runtime(async {
            let ListTicketsResult::Found(found) = self
                .client
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

    fn list_open_tickets_for_assignee(
        &mut self,
        organization_id: UuidBytes,
        assignee_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error> {
        block_on_runtime(async {
            let ListTicketsByAssigneeResult::Found(found) = self
                .client
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
        block_on_runtime(async {
            let ListCommentsResult::Found(found) = self
                .client
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
        block_on_runtime(async {
            let ProjectMembersResult::Found(found) = self
                .client
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
        block_on_runtime(async {
            match self
                .client
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
        block_on_runtime(async {
            self.client
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
}

fn uuid_text(bytes: UuidBytes) -> String {
    format_uuid(bytes)
}

fn parse_uuid(text: &str) -> Result<UuidBytes, RiffDbError> {
    if text.len() != 36 {
        return Err(RiffDbError::Decode);
    }
    let mut out = [0_u8; 16];
    let hex = |i: usize| {
        u8::from_str_radix(&text[i..i + 2], 16).map_err(|_| RiffDbError::Decode)
    };
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

fn map_app(error: riffdb_client_rust::ApplicationClientError) -> RiffDbError {
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
    /// Server process failed.
    Server,
    /// Filesystem error.
    Io,
    /// RPC failed.
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
            Self::Server => formatter.write_str("riffdbd process failed"),
            Self::Io => formatter.write_str("riffdb harness io failed"),
            Self::Rpc(detail) => write!(formatter, "riffdb rpc failed: {detail}"),
            Self::Runtime => formatter.write_str("riffdb async runtime unavailable"),
            Self::Decode => formatter.write_str("riffdb response decode failed"),
        }
    }
}

impl Error for RiffDbError {}
