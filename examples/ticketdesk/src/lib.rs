#![forbid(unsafe_code)]

//! TicketDesk acceptance client using only symbolic application operations.

mod decode;
mod generated;

use std::collections::BTreeMap;

use riffdb_client_rust::{
    ApplicationClientError, ApplicationCommand, ApplicationCommandResult, ApplicationContract,
    ApplicationValue, AttemptBudget, CallMetadata, NamedQuery, RiffDbClient,
};

pub use generated::{
    AddProjectMemberInput, AttachLabelInput, CreateCommentInput, CreateLabelInput,
    CreateOrganizationInput, CreateProjectInput, CreateTicketInput, CreateUserInput,
    GetTicketParams, GetTicketResult, GetUserParams, GetUserResult, ListCommentsParams,
    ListCommentsResult, ListTicketsByAssigneeParams, ListTicketsByAssigneeResult,
    ListTicketsParams, ListTicketsResult, ProjectMembersParams, ProjectMembersResult,
    ProjectSummaryParams, ProjectSummaryResult, TicketPageParams, TicketPageResult,
};

/// One application client pinned to the checked TicketDesk query module.
pub struct TicketDeskClient {
    client: RiffDbClient,
    metadata: CallMetadata,
}

impl TicketDeskClient {
    /// Wraps a connected public client and authenticated metadata.
    #[must_use]
    pub const fn new(client: RiffDbClient, metadata: CallMetadata) -> Self {
        Self { client, metadata }
    }

    /// Point-gets one ticket with one symbolic request.
    pub async fn get_ticket(
        &mut self,
        input: GetTicketParams,
    ) -> Result<GetTicketResult, ApplicationClientError> {
        let result = self
            .query(
                "GetTicket",
                parameters([
                    ("organization_id", uuid(input.organization_id)),
                    ("ticket_id", uuid(input.ticket_id)),
                ]),
            )
            .await?;
        decode::get_ticket_result(result)
    }

    /// Point-gets one user with one symbolic request.
    pub async fn get_user(
        &mut self,
        input: GetUserParams,
    ) -> Result<GetUserResult, ApplicationClientError> {
        let result = self
            .query(
                "GetUser",
                parameters([
                    ("organization_id", uuid(input.organization_id)),
                    ("user_id", uuid(input.user_id)),
                ]),
            )
            .await?;
        decode::get_user_result(result)
    }

    /// Lists comments for one ticket with one symbolic request.
    pub async fn list_comments(
        &mut self,
        input: ListCommentsParams,
    ) -> Result<ListCommentsResult, ApplicationClientError> {
        let result = self
            .query(
                "ListComments",
                parameters([
                    ("organization_id", uuid(input.organization_id)),
                    ("ticket_id", uuid(input.ticket_id)),
                    ("after", optional_text(input.after)),
                    ("limit", ApplicationValue::U64(input.limit)),
                ]),
            )
            .await?;
        decode::list_comments_result(result)
    }

    /// Lists one bounded project/status page with one symbolic request.
    pub async fn list_tickets(
        &mut self,
        input: ListTicketsParams,
    ) -> Result<ListTicketsResult, ApplicationClientError> {
        let result = self
            .query(
                "ListTickets",
                parameters([
                    ("organization_id", uuid(input.organization_id)),
                    ("project_id", uuid(input.project_id)),
                    (
                        "statuses",
                        ApplicationValue::List(
                            input.statuses.into_iter().map(enum_variant).collect(),
                        ),
                    ),
                    ("after", optional_text(input.after)),
                    ("limit", ApplicationValue::U64(input.limit)),
                ]),
            )
            .await?;
        decode::list_tickets_result(result)
    }

    /// Lists tickets for one assignee/status page with one symbolic request.
    pub async fn list_tickets_by_assignee(
        &mut self,
        input: ListTicketsByAssigneeParams,
    ) -> Result<ListTicketsByAssigneeResult, ApplicationClientError> {
        let result = self
            .query(
                "ListTicketsByAssignee",
                parameters([
                    ("organization_id", uuid(input.organization_id)),
                    ("assignee_id", uuid(input.assignee_id)),
                    (
                        "statuses",
                        ApplicationValue::List(
                            input.statuses.into_iter().map(enum_variant).collect(),
                        ),
                    ),
                    ("after", optional_text(input.after)),
                    ("limit", ApplicationValue::U64(input.limit)),
                ]),
            )
            .await?;
        decode::list_tickets_by_assignee_result(result)
    }

    /// Returns a project-members page with one symbolic request.
    pub async fn project_members(
        &mut self,
        input: ProjectMembersParams,
    ) -> Result<ProjectMembersResult, ApplicationClientError> {
        let result = self
            .query(
                "ProjectMembers",
                parameters([
                    ("organization_id", uuid(input.organization_id)),
                    ("project_id", uuid(input.project_id)),
                    ("after", optional_text(input.after)),
                ]),
            )
            .await?;
        decode::project_members_result(result)
    }

    /// Returns the project summary with one symbolic request.
    pub async fn project_summary(
        &mut self,
        input: ProjectSummaryParams,
    ) -> Result<ProjectSummaryResult, ApplicationClientError> {
        let result = self
            .query(
                "ProjectSummary",
                parameters([
                    ("organization_id", uuid(input.organization_id)),
                    ("project_id", uuid(input.project_id)),
                    ("status", enum_variant(input.status)),
                ]),
            )
            .await?;
        decode::project_summary_result(result)
    }

    /// Returns the complete detail page with one symbolic request.
    pub async fn ticket_page(
        &mut self,
        input: TicketPageParams,
    ) -> Result<TicketPageResult, ApplicationClientError> {
        let result = self
            .query(
                "TicketPage",
                parameters([
                    ("organization_id", uuid(input.organization_id)),
                    ("ticket_id", uuid(input.ticket_id)),
                    ("comments_after", optional_text(input.comments_after)),
                ]),
            )
            .await?;
        decode::ticket_page_result(result)
    }

    /// Creates an organization with one symbolic command invocation.
    pub async fn create_organization(
        &mut self,
        input: CreateOrganizationInput,
    ) -> Result<ApplicationCommandResult, ApplicationClientError> {
        self.command(
            "CreateOrganization",
            parameters([
                ("idempotency_key", text(input.idempotency_key)),
                ("organization_id", uuid(input.organization_id)),
                ("name", text(input.name)),
            ]),
        )
        .await
    }

    /// Creates a user with one symbolic command invocation.
    pub async fn create_user(
        &mut self,
        input: CreateUserInput,
    ) -> Result<ApplicationCommandResult, ApplicationClientError> {
        self.command(
            "CreateUser",
            parameters([
                ("idempotency_key", text(input.idempotency_key)),
                ("organization_id", uuid(input.organization_id)),
                ("user_id", uuid(input.user_id)),
                ("email", text(input.email)),
                ("display_name", text(input.display_name)),
            ]),
        )
        .await
    }

    /// Creates a project with one symbolic command invocation.
    pub async fn create_project(
        &mut self,
        input: CreateProjectInput,
    ) -> Result<ApplicationCommandResult, ApplicationClientError> {
        self.command(
            "CreateProject",
            parameters([
                ("idempotency_key", text(input.idempotency_key)),
                ("organization_id", uuid(input.organization_id)),
                ("project_id", uuid(input.project_id)),
                ("name", text(input.name)),
            ]),
        )
        .await
    }

    /// Adds a project member with one symbolic command invocation.
    pub async fn add_project_member(
        &mut self,
        input: AddProjectMemberInput,
    ) -> Result<ApplicationCommandResult, ApplicationClientError> {
        self.command(
            "AddProjectMember",
            parameters([
                ("idempotency_key", text(input.idempotency_key)),
                ("organization_id", uuid(input.organization_id)),
                ("project_id", uuid(input.project_id)),
                ("user_id", uuid(input.user_id)),
                ("role", text(input.role)),
            ]),
        )
        .await
    }

    /// Creates a ticket with one symbolic command invocation.
    pub async fn create_ticket(
        &mut self,
        input: CreateTicketInput,
    ) -> Result<ApplicationCommandResult, ApplicationClientError> {
        self.command(
            "CreateTicket",
            parameters([
                ("idempotency_key", text(input.idempotency_key)),
                ("organization_id", uuid(input.organization_id)),
                ("ticket_id", uuid(input.ticket_id)),
                ("project_id", uuid(input.project_id)),
                ("reporter_id", uuid(input.reporter_id)),
                ("assignee_id", uuid(input.assignee_id)),
                ("title", text(input.title)),
                ("status", enum_variant(input.status)),
            ]),
        )
        .await
    }

    /// Creates a comment with one symbolic command invocation.
    pub async fn create_comment(
        &mut self,
        input: CreateCommentInput,
    ) -> Result<ApplicationCommandResult, ApplicationClientError> {
        self.command(
            "CreateComment",
            parameters([
                ("idempotency_key", text(input.idempotency_key)),
                ("organization_id", uuid(input.organization_id)),
                ("comment_id", uuid(input.comment_id)),
                ("ticket_id", uuid(input.ticket_id)),
                ("author_id", uuid(input.author_id)),
                ("body", text(input.body)),
            ]),
        )
        .await
    }

    /// Creates a label with one symbolic command invocation.
    pub async fn create_label(
        &mut self,
        input: CreateLabelInput,
    ) -> Result<ApplicationCommandResult, ApplicationClientError> {
        self.command(
            "CreateLabel",
            parameters([
                ("idempotency_key", text(input.idempotency_key)),
                ("organization_id", uuid(input.organization_id)),
                ("label_id", uuid(input.label_id)),
                ("name", text(input.name)),
            ]),
        )
        .await
    }

    /// Attaches a label with one symbolic command invocation.
    pub async fn attach_label(
        &mut self,
        input: AttachLabelInput,
    ) -> Result<ApplicationCommandResult, ApplicationClientError> {
        self.command(
            "AttachLabel",
            parameters([
                ("idempotency_key", text(input.idempotency_key)),
                ("organization_id", uuid(input.organization_id)),
                ("ticket_id", uuid(input.ticket_id)),
                ("label_id", uuid(input.label_id)),
            ]),
        )
        .await
    }

    async fn query(
        &mut self,
        name: &str,
        parameters: BTreeMap<String, ApplicationValue>,
    ) -> Result<riffdb_client_rust::NamedQueryResult, ApplicationClientError> {
        let query = NamedQuery::new(
            ApplicationContract::Exact {
                lineage: generated::CONTRACT_LINEAGE.to_owned(),
                version: generated::CONTRACT_VERSION,
                bundle_hash: Some(generated::CONTRACT_BUNDLE_HASH),
            },
            name,
            Some(generated::QUERY_MODULE_HASH),
            parameters,
            None,
        )?;
        self.client
            .execute_named_application_query(query, &self.metadata)
            .await
    }

    async fn command(
        &mut self,
        name: &str,
        input: BTreeMap<String, ApplicationValue>,
    ) -> Result<ApplicationCommandResult, ApplicationClientError> {
        let command = ApplicationCommand::new(name, Some(generated::CONTRACT_VERSION), input)?;
        self.client
            .execute_application_command(
                command,
                AttemptBudget::new(3).expect("nonzero constant"),
                &self.metadata,
            )
            .await
    }
}

fn parameters<const N: usize>(
    values: [(&str, ApplicationValue); N],
) -> BTreeMap<String, ApplicationValue> {
    values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn text(value: String) -> ApplicationValue {
    ApplicationValue::String(value)
}

fn uuid(value: String) -> ApplicationValue {
    ApplicationValue::Uuid(value)
}

fn enum_variant(value: String) -> ApplicationValue {
    ApplicationValue::Enum(value)
}

fn optional_text(value: Option<String>) -> ApplicationValue {
    value.map_or(ApplicationValue::Null, ApplicationValue::String)
}
