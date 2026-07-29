//! Public gRPC TicketDesk adapter against a live `riffdbd`.

#![forbid(unsafe_code)]

mod schema;
mod server;
mod values;

use std::error::Error;
use std::fmt;

use riffdb_app_baseline_core::{
    AppBackend, CommentRow, CommentSeed, LabelRow, OrganizationRow, ProjectMemberRow, ProjectRow,
    SeedDataset, TicketDetailPage, TicketRow, TicketStatus, UserRow, UuidBytes,
};
use riffdb_client_rust::{
    AttemptBudget, BearerCredential, CallMetadata, IdempotentCommand, RiffDbClient,
    generate_request_id, v1,
};
use tonic::transport::Endpoint;

use schema::{
    CONTRACT_LINEAGE, CONTRACT_VERSION, comment_field, entity, index, label_field, member_field,
    org_field, project_field, ticket_field, ticket_label_field, user_field,
};
use values::{
    entity_key, enum_value, field, field_map, record, require_string, require_uuid, string_value,
    uuid_value,
};

pub use server::RiffDbServerSession;

/// Public-gRPC application backend.
pub struct RiffDbPublicBackend {
    client: RiffDbClient,
    metadata: CallMetadata,
}

impl RiffDbPublicBackend {
    /// Connects to an already bootstrapped TicketDesk-ready endpoint.
    pub async fn connect(endpoint: &str, bearer_token: &str) -> Result<Self, RiffDbError> {
        let endpoint = Endpoint::from_shared(endpoint.to_owned())
            .map_err(|_| RiffDbError::Connection)?
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(60));
        let client = RiffDbClient::connect(endpoint)
            .await
            .map_err(|_| RiffDbError::Connection)?;
        let metadata = CallMetadata::authenticated(
            BearerCredential::new(bearer_token).map_err(|_| RiffDbError::Connection)?,
        );
        Ok(Self { client, metadata })
    }



    async fn execute_command(
        &mut self,
        command_name: &str,
        input: v1::Value,
    ) -> Result<(), RiffDbError> {
        let command = IdempotentCommand::new(command_name, Some(CONTRACT_VERSION), input)
            .map_err(|error| RiffDbError::Rpc(format!("bad command shape {command_name}: {error:?}")))?;
        let budget = AttemptBudget::new(1).ok_or(RiffDbError::InvalidSchema)?;
        let response = self
            .client
            .execute_with_retry(&command, budget, &self.metadata)
            .await
            .map_err(|e| {
                RiffDbError::Rpc(format!(
                    "EXECUTE_CMD {command_name} FAILED ({e:?}): {e}"
                ))
            })?;
        // Accept committed/replayed; existence outcomes still complete as committed declared outcomes.
        let status = v1::execute_command_response::CompletionStatus::try_from(response.status)
            .map_err(|e| RiffDbError::Rpc(e.to_string()))?;
        match status {
            v1::execute_command_response::CompletionStatus::Committed
            | v1::execute_command_response::CompletionStatus::Replayed => Ok(()),
            other => Err(RiffDbError::Rpc(format!("unexpected status {other:?}"))),
        }
    }

    async fn get_entity_record(
        &mut self,
        entity_type_id: u32,
        key_components: &[[u8; 16]],
        field_ids: &[u32],
    ) -> Result<Option<v1::ValueRecord>, RiffDbError> {
        let entity_key = entity_key(entity_type_id, key_components)?;
        let response = self
            .client
            .get_entity(
                v1::GetEntityRequest {
                    request_id: request_id_bytes()?,
                    contract: Some(exact_contract()),
                    entity_type_id,
                    entity_key: entity_key.clone(),
                    fields: Some(v1::FieldSelection {
                        field_ids: field_ids.to_vec(),
                    }),
                },
                &self.metadata,
            )
            .await
            .map_err(|e| {
                RiffDbError::Rpc(format!(
                    "GetEntity type={entity_type_id} key_len={} fields={field_ids:?}: {e}",
                    entity_key.len()
                ))
            })?;
        match response.result {
            Some(v1::get_entity_response::Result::NotFound(_)) => Ok(None),
            Some(v1::get_entity_response::Result::Found(entity)) => {
                let fields = entity.fields.ok_or_else(|| {
                    RiffDbError::Rpc("GetEntity Found without fields message".into())
                })?;
                if fields.fields.is_empty() {
                    return Err(RiffDbError::Rpc(format!(
                        "GETENTITY_EMPTY_FIELDS_V2 type={entity_type_id} version={} contract={} requested={field_ids:?} key_prefix={:02x?}",
                        entity.entity_version,
                        entity.written_by_contract_version,
                        &entity_key[..entity_key.len().min(8)]
                    )));
                }
                Ok(Some(fields))
            }
            None => Err(RiffDbError::Decode),
        }
    }


    async fn fetch_tickets_from_index(
        &mut self,
        index_id: u32,
        leading: Vec<v1::Value>,
        organization_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<TicketRow>, RiffDbError> {
        // v1 indexes have empty covering values; request any granted non-key field
        // so ScanIndex is authorized, then resolve rows via GetEntity.
        let rows = self
            .scan_index_keys(index_id, leading, &[ticket_field::TITLE], limit)
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for index_key in rows {
            let ticket_id = last_uuid_component(&index_key)?;
            let record = self
                .get_entity_record(
                    entity::TICKET,
                    &[organization_id, ticket_id],
                    TICKET_NON_KEY_FIELDS,
                )
                .await?
                .ok_or_else(|| {
                    RiffDbError::Rpc(format!(
                        "index pointed at missing ticket {:02x?}",
                        &ticket_id[..4]
                    ))
                })?;
            out.push(decode_ticket(&record, organization_id, ticket_id)?);
        }
        Ok(out)
    }

    async fn fetch_comments_from_index(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<CommentRow>, RiffDbError> {
        let rows = self
            .scan_index_keys(
                index::COMMENT_BY_TICKET,
                vec![uuid_value(organization_id), uuid_value(ticket_id)],
                &[comment_field::BODY],
                limit,
            )
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for index_key in rows {
            let comment_id = last_uuid_component(&index_key)?;
            let record = self
                .get_entity_record(
                    entity::COMMENT,
                    &[organization_id, comment_id],
                    COMMENT_NON_KEY_FIELDS,
                )
                .await?
                .ok_or_else(|| {
                    RiffDbError::Rpc(format!(
                        "index pointed at missing comment {:02x?}",
                        &comment_id[..4]
                    ))
                })?;
            out.push(decode_comment(
                &record,
                organization_id,
                comment_id,
                ticket_id,
            )?);
        }
        Ok(out)
    }

    async fn fetch_members_from_index(
        &mut self,
        organization_id: UuidBytes,
        project_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<ProjectMemberRow>, RiffDbError> {
        let rows = self
            .scan_index_keys(
                index::PROJECT_MEMBER_BY_PROJECT,
                vec![uuid_value(organization_id), uuid_value(project_id)],
                &[member_field::ROLE],
                limit,
            )
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for index_key in rows {
            // ProjectMember PK is (organization_id, project_id, user_id); entity key is 54 bytes.
            let user_id = last_uuid_component(&index_key)?;
            let record = self
                .get_entity_record(
                    entity::PROJECT_MEMBER,
                    &[organization_id, project_id, user_id],
                    MEMBER_NON_KEY_FIELDS,
                )
                .await?
                .ok_or_else(|| {
                    RiffDbError::Rpc(format!(
                        "index pointed at missing member {:02x?}",
                        &user_id[..4]
                    ))
                })?;
            out.push(decode_member(
                &record,
                organization_id,
                project_id,
                user_id,
            )?);
        }
        Ok(out)
    }

    async fn fetch_labels_for_ticket(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<LabelRow>, RiffDbError> {
        let rows = self
            .scan_index_keys(
                index::TICKET_LABEL_BY_TICKET,
                vec![uuid_value(organization_id), uuid_value(ticket_id)],
                // TicketLabel only has created_at as a non-key field.
                &[ticket_label_field::CREATED_AT],
                limit,
            )
            .await?;
        let mut labels = Vec::with_capacity(rows.len());
        for index_key in rows {
            // TicketLabel PK is (organization_id, ticket_id, label_id).
            let label_id = last_uuid_component(&index_key)?;
            if let Some(label_record) = self
                .get_entity_record(
                    entity::LABEL,
                    &[organization_id, label_id],
                    LABEL_NON_KEY_FIELDS,
                )
                .await?
            {
                labels.push(decode_label(&label_record, organization_id, label_id)?);
            }
        }
        Ok(labels)
    }

    /// Scans an index and returns raw index-entry keys only.
    ///
    /// v1 index covering values are empty; callers must GetEntity for payload fields.
    async fn scan_index_keys(
        &mut self,
        index_id: u32,
        leading: Vec<v1::Value>,
        field_ids: &[u32],
        limit: u32,
    ) -> Result<Vec<Vec<u8>>, RiffDbError> {
        let leading_len = leading.len();
        let response = self
            .client
            .scan_index(
                v1::ScanIndexRequest {
                    request_id: request_id_bytes()?,
                    contract: Some(exact_contract()),
                    index_id,
                    leading_components: leading,
                    fields: Some(v1::FieldSelection {
                        field_ids: field_ids.to_vec(),
                    }),
                    page: Some(v1::PageRequest {
                        limit: Some(limit),
                        cursor: None,
                    }),
                },
                &self.metadata,
            )
            .await
            .map_err(|e| {
                RiffDbError::Rpc(format!(
                    "ScanIndex id={index_id} leading={leading_len} limit={limit}: {e}"
                ))
            })?;
        let page = response.page.ok_or_else(|| {
            RiffDbError::Rpc(format!("ScanIndex id={index_id}: missing page"))
        })?;
        Ok(page
            .items
            .into_iter()
            .map(|item| item.index_entry_key)
            .collect())
    }
}

/// Ticket non-key field ids (must match capability field_visibility).
const TICKET_NON_KEY_FIELDS: &[u32] = &[1, 2, 4, 5, 6, 7, 8];
/// Comment non-key field ids.
const COMMENT_NON_KEY_FIELDS: &[u32] = &[1, 2, 3, 5];
/// ProjectMember non-key field ids (user_id is part of the primary key).
const MEMBER_NON_KEY_FIELDS: &[u32] = &[1, 3];
/// Label non-key field ids.
const LABEL_NON_KEY_FIELDS: &[u32] = &[1, 3];
/// AppUser non-key field ids.
const USER_NON_KEY_FIELDS: &[u32] = &[1, 3, 4];
/// Project non-key field ids.
const PROJECT_NON_KEY_FIELDS: &[u32] = &[1, 2];
/// Organization non-key field ids.
const ORG_NON_KEY_FIELDS: &[u32] = &[1, 2];

/// Last UUID component of the length-delimited entity key suffix on an index entry.
///
/// Index entry keys end with `u32_be_len || entity_key_bytes`. Entity keys end with
/// their final primary-key UUID (16 bytes), which is enough for ticket_id, comment_id,
/// user_id, and label_id in this contract.
fn last_uuid_component(index_entry_key: &[u8]) -> Result<UuidBytes, RiffDbError> {
    if index_entry_key.len() < 16 {
        return Err(RiffDbError::Rpc(format!(
            "index entry too short for uuid suffix: {}",
            index_entry_key.len()
        )));
    }
    let mut id = [0_u8; 16];
    id.copy_from_slice(&index_entry_key[index_entry_key.len() - 16..]);
    Ok(id)
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
        // Fresh riffdbd process per session; nothing to drop.
        Ok(())
    }

    fn seed(&mut self, dataset: &SeedDataset) -> Result<(), Self::Error> {
        block_on_runtime(async {
            for org in &dataset.organizations {
                self.execute_command(
                    "CreateOrganization",
                    record(vec![
                        field(1, string_value(&org.name)),
                        field(2, string_value(format!("seed-org-{}", hex::encode_short(org.organization_id)))),
                        field(3, uuid_value(org.organization_id)),
                    ]),
                )
                .await?;
            }
            for user in &dataset.users {
                self.execute_command(
                    "CreateUser",
                    record(vec![
                        field(1, string_value(&user.email)),
                        field(2, uuid_value(user.user_id)),
                        field(3, string_value(&user.display_name)),
                        field(4, string_value(format!("seed-user-{}", hex::encode_short(user.user_id)))),
                        field(5, uuid_value(user.organization_id)),
                    ]),
                )
                .await?;
            }
            for project in &dataset.projects {
                self.execute_command(
                    "CreateProject",
                    record(vec![
                        field(1, string_value(&project.name)),
                        field(2, uuid_value(project.project_id)),
                        field(3, string_value(format!("seed-project-{}", hex::encode_short(project.project_id)))),
                        field(4, uuid_value(project.organization_id)),
                    ]),
                )
                .await?;
            }
            for member in &dataset.members {
                self.execute_command(
                    "AddProjectMember",
                    record(vec![
                        field(1, string_value(&member.role)),
                        field(2, uuid_value(member.user_id)),
                        field(3, uuid_value(member.project_id)),
                        field(
                            4,
                            string_value(format!(
                                "seed-member-{}-{}",
                                hex::encode_short(member.project_id),
                                hex::encode_short(member.user_id)
                            )),
                        ),
                        field(5, uuid_value(member.organization_id)),
                    ]),
                )
                .await?;
            }
            for label in &dataset.labels {
                self.execute_command(
                    "CreateLabel",
                    record(vec![
                        field(1, string_value(&label.name)),
                        field(2, uuid_value(label.label_id)),
                        field(3, string_value(format!("seed-label-{}", hex::encode_short(label.label_id)))),
                        field(4, uuid_value(label.organization_id)),
                    ]),
                )
                .await?;
            }
            for ticket in &dataset.tickets {
                self.execute_command(
                    "CreateTicket",
                    record(vec![
                        field(1, string_value(&ticket.title)),
                        field(
                            2,
                            enum_value(
                                1,
                                ticket.status.riffdb_variant_id(),
                                match ticket.status {
                                    TicketStatus::Open => "Open",
                                    TicketStatus::Closed => "Closed",
                                    TicketStatus::InProgress => "InProgress",
                                },
                            ),
                        ),
                        field(3, uuid_value(ticket.ticket_id)),
                        field(4, uuid_value(ticket.project_id)),
                        field(5, uuid_value(ticket.assignee_id)),
                        field(6, uuid_value(ticket.reporter_id)),
                        field(7, string_value(format!("seed-ticket-{}", hex::encode_short(ticket.ticket_id)))),
                        field(8, uuid_value(ticket.organization_id)),
                    ]),
                )
                .await?;
            }
            for comment in &dataset.comments {
                self.execute_command(
                    "CreateComment",
                    record(vec![
                        field(1, string_value(&comment.body)),
                        field(2, uuid_value(comment.author_id)),
                        field(3, uuid_value(comment.ticket_id)),
                        field(4, uuid_value(comment.comment_id)),
                        field(5, string_value(format!("seed-comment-{}", hex::encode_short(comment.comment_id)))),
                        field(6, uuid_value(comment.organization_id)),
                    ]),
                )
                .await?;
            }
            for link in &dataset.ticket_labels {
                self.execute_command(
                    "AttachLabel",
                    record(vec![
                        field(1, uuid_value(link.label_id)),
                        field(2, uuid_value(link.ticket_id)),
                        field(
                            3,
                            string_value(format!(
                                "seed-link-{}-{}",
                                hex::encode_short(link.ticket_id),
                                hex::encode_short(link.label_id)
                            )),
                        ),
                        field(4, uuid_value(link.organization_id)),
                    ]),
                )
                .await?;
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
            let record = self
                .get_entity_record(
                    entity::TICKET,
                    &[organization_id, ticket_id],
                    TICKET_NON_KEY_FIELDS,
                )
                .await?;
            record
                .map(|record| decode_ticket(&record, organization_id, ticket_id))
                .transpose()
        })
    }

    fn point_get_user(
        &mut self,
        organization_id: UuidBytes,
        user_id: UuidBytes,
    ) -> Result<Option<UserRow>, Self::Error> {
        block_on_runtime(async {
            let record = self
                .get_entity_record(
                    entity::APP_USER,
                    &[organization_id, user_id],
                    USER_NON_KEY_FIELDS,
                )
                .await?;
            record
                .map(|record| decode_user(&record, organization_id, user_id))
                .transpose()
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
            self.fetch_tickets_from_index(
                index::TICKET_BY_PROJECT_STATUS,
                vec![
                    uuid_value(organization_id),
                    uuid_value(project_id),
                    enum_value(
                        1,
                        status.riffdb_variant_id(),
                        match status {
                            TicketStatus::Open => "Open",
                            TicketStatus::Closed => "Closed",
                            TicketStatus::InProgress => "InProgress",
                        },
                    ),
                ],
                organization_id,
                limit,
            )
            .await
        })
    }

    fn list_open_tickets_for_assignee(
        &mut self,
        organization_id: UuidBytes,
        assignee_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<TicketRow>, Self::Error> {
        block_on_runtime(async {
            self.fetch_tickets_from_index(
                index::TICKET_BY_ASSIGNEE_STATUS,
                vec![
                    uuid_value(organization_id),
                    uuid_value(assignee_id),
                    enum_value(1, TicketStatus::Open.riffdb_variant_id(), "Open"),
                ],
                organization_id,
                limit,
            )
            .await
        })
    }

    fn list_comments_for_ticket(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<CommentRow>, Self::Error> {
        block_on_runtime(async {
            self.fetch_comments_from_index(organization_id, ticket_id, limit)
                .await
        })
    }

    fn list_project_members(
        &mut self,
        organization_id: UuidBytes,
        project_id: UuidBytes,
        limit: u32,
    ) -> Result<Vec<ProjectMemberRow>, Self::Error> {
        block_on_runtime(async {
            self.fetch_members_from_index(organization_id, project_id, limit)
                .await
        })
    }

    fn ticket_detail_page(
        &mut self,
        organization_id: UuidBytes,
        ticket_id: UuidBytes,
        comment_limit: u32,
    ) -> Result<Option<TicketDetailPage>, Self::Error> {
        block_on_runtime(async {
            let Some(ticket_record) = self
                .get_entity_record(
                    entity::TICKET,
                    &[organization_id, ticket_id],
                    TICKET_NON_KEY_FIELDS,
                )
                .await?
            else {
                return Ok(None);
            };
            let ticket = decode_ticket(&ticket_record, organization_id, ticket_id)?;
            let project_record = self
                .get_entity_record(
                    entity::PROJECT,
                    &[organization_id, ticket.project_id],
                    PROJECT_NON_KEY_FIELDS,
                )
                .await?
                .ok_or_else(|| {
                    RiffDbError::Rpc(format!(
                        "ticket detail missing project {:02x?}",
                        &ticket.project_id[..4]
                    ))
                })?;
            let project = decode_project(&project_record, organization_id, ticket.project_id)?;
            let org_record = self
                .get_entity_record(
                    entity::ORGANIZATION,
                    &[organization_id],
                    ORG_NON_KEY_FIELDS,
                )
                .await?
                .ok_or_else(|| RiffDbError::Rpc("ticket detail missing organization".into()))?;
            let organization = decode_organization(&org_record, organization_id)?;
            let assignee = self
                .get_entity_record(
                    entity::APP_USER,
                    &[organization_id, ticket.assignee_id],
                    USER_NON_KEY_FIELDS,
                )
                .await?
                .map(|record| decode_user(&record, organization_id, ticket.assignee_id))
                .transpose()?;
            let comments = self
                .fetch_comments_from_index(organization_id, ticket_id, comment_limit)
                .await?;
            let labels = self
                .fetch_labels_for_ticket(organization_id, ticket_id, 50)
                .await?;

            Ok(Some(TicketDetailPage {
                ticket,
                project,
                organization,
                assignee,
                comments,
                labels,
            }))
        })
    }

    fn create_comment(&mut self, comment: &CommentSeed) -> Result<(), Self::Error> {
        block_on_runtime(async {
            self.execute_command(
                "CreateComment",
                record(vec![
                    field(1, string_value(&comment.row.body)),
                    field(2, uuid_value(comment.row.author_id)),
                    field(3, uuid_value(comment.row.ticket_id)),
                    field(4, uuid_value(comment.row.comment_id)),
                    field(5, string_value(&comment.idempotency_key)),
                    field(6, uuid_value(comment.row.organization_id)),
                ]),
            )
            .await
        })
    }
}

// Avoid hex dependency: short stable key fragment for idempotency labels.
mod hex {
    pub(crate) fn encode_short(bytes: [u8; 16]) -> String {
        format!(
            "{:02x}{:02x}{:02x}{:02x}",
            bytes[12], bytes[13], bytes[14], bytes[15]
        )
    }
}

fn exact_contract() -> v1::ContractSelection {
    v1::ContractSelection {
        selection: Some(v1::contract_selection::Selection::Exact(
            v1::ExactContractSelection {
                contract_lineage: CONTRACT_LINEAGE.to_owned(),
                contract_version: CONTRACT_VERSION,
            },
        )),
    }
}

fn request_id_bytes() -> Result<Vec<u8>, RiffDbError> {
    Ok(generate_request_id()
        .map_err(|e| RiffDbError::Rpc(e.to_string()))?
        .into_bytes()
        .to_vec())
}

fn decode_ticket(
    record: &v1::ValueRecord,
    organization_id: UuidBytes,
    ticket_id: UuidBytes,
) -> Result<TicketRow, RiffDbError> {
    let fields = field_map(record).map_err(|_| RiffDbError::Rpc("ticket field_map".into()))?;
    let status_value = fields.get(&ticket_field::STATUS).ok_or_else(|| {
        let names: Vec<_> = record
            .fields
            .iter()
            .map(|field| {
                format!(
                    "id={:?},name={},has_value={}",
                    field.field_id,
                    field.name,
                    field.value.is_some()
                )
            })
            .collect();
        RiffDbError::Rpc(format!("TICKET_DECODE_MISSING_STATUS_V2; fields={names:?}"))
    })?;
    let status = match status_value.kind.as_ref() {
        Some(v1::value::Kind::EnumValue(value)) => match value.variant_id {
            1 => TicketStatus::Open,
            2 => TicketStatus::Closed,
            3 => TicketStatus::InProgress,
            other => {
                return Err(RiffDbError::Rpc(format!(
                    "ticket status variant {other} name={}",
                    value.name
                )));
            }
        },
        Some(v1::value::Kind::StringValue(name)) => match name.as_str() {
            "Open" => TicketStatus::Open,
            "Closed" => TicketStatus::Closed,
            "InProgress" => TicketStatus::InProgress,
            other => return Err(RiffDbError::Rpc(format!("ticket status string {other}"))),
        },
        other => {
            return Err(RiffDbError::Rpc(format!("ticket status kind {other:?}")));
        }
    };
    Ok(TicketRow {
        organization_id,
        ticket_id,
        project_id: require_uuid(
            fields
                .get(&ticket_field::PROJECT_ID)
                .ok_or_else(|| RiffDbError::Rpc("ticket missing project_id".into()))?,
        )?,
        reporter_id: require_uuid(
            fields
                .get(&ticket_field::REPORTER_ID)
                .ok_or_else(|| RiffDbError::Rpc("ticket missing reporter_id".into()))?,
        )?,
        assignee_id: require_uuid(
            fields
                .get(&ticket_field::ASSIGNEE_ID)
                .ok_or_else(|| RiffDbError::Rpc("ticket missing assignee_id".into()))?,
        )?,
        status,
        title: require_string(
            fields
                .get(&ticket_field::TITLE)
                .ok_or_else(|| RiffDbError::Rpc("ticket missing title".into()))?,
        )?,
    })
}

fn decode_user(
    record: &v1::ValueRecord,
    organization_id: UuidBytes,
    user_id: UuidBytes,
) -> Result<UserRow, RiffDbError> {
    let fields = field_map(record).map_err(|_| RiffDbError::Rpc("user field_map".into()))?;
    Ok(UserRow {
        organization_id,
        user_id,
        email: require_string(
            fields
                .get(&user_field::EMAIL)
                .ok_or_else(|| RiffDbError::Rpc("user missing email".into()))?,
        )?,
        display_name: require_string(
            fields
                .get(&user_field::DISPLAY_NAME)
                .ok_or_else(|| RiffDbError::Rpc("user missing display_name".into()))?,
        )?,
    })
}

fn decode_project(
    record: &v1::ValueRecord,
    organization_id: UuidBytes,
    project_id: UuidBytes,
) -> Result<ProjectRow, RiffDbError> {
    let fields = field_map(record).map_err(|_| RiffDbError::Rpc("project field_map".into()))?;
    Ok(ProjectRow {
        organization_id,
        project_id,
        name: require_string(
            fields
                .get(&project_field::NAME)
                .ok_or_else(|| RiffDbError::Rpc("project missing name".into()))?,
        )?,
    })
}

fn decode_organization(
    record: &v1::ValueRecord,
    organization_id: UuidBytes,
) -> Result<OrganizationRow, RiffDbError> {
    let fields = field_map(record).map_err(|_| RiffDbError::Rpc("org field_map".into()))?;
    Ok(OrganizationRow {
        organization_id,
        name: require_string(
            fields
                .get(&org_field::NAME)
                .ok_or_else(|| RiffDbError::Rpc("org missing name".into()))?,
        )?,
    })
}

fn decode_comment(
    record: &v1::ValueRecord,
    organization_id: UuidBytes,
    comment_id: UuidBytes,
    ticket_id: UuidBytes,
) -> Result<CommentRow, RiffDbError> {
    let fields = field_map(record).map_err(|_| RiffDbError::Rpc("comment field_map".into()))?;
    Ok(CommentRow {
        organization_id,
        comment_id,
        ticket_id,
        author_id: require_uuid(
            fields
                .get(&comment_field::AUTHOR_ID)
                .ok_or_else(|| RiffDbError::Rpc("comment missing author_id".into()))?,
        )?,
        body: require_string(
            fields
                .get(&comment_field::BODY)
                .ok_or_else(|| RiffDbError::Rpc("comment missing body".into()))?,
        )?,
    })
}

fn decode_member(
    record: &v1::ValueRecord,
    organization_id: UuidBytes,
    project_id: UuidBytes,
    user_id: UuidBytes,
) -> Result<ProjectMemberRow, RiffDbError> {
    let fields = field_map(record).map_err(|_| RiffDbError::Rpc("member field_map".into()))?;
    Ok(ProjectMemberRow {
        organization_id,
        project_id,
        user_id,
        role: require_string(
            fields
                .get(&member_field::ROLE)
                .ok_or_else(|| RiffDbError::Rpc("member missing role".into()))?,
        )?,
    })
}

fn decode_label(
    record: &v1::ValueRecord,
    organization_id: UuidBytes,
    label_id: UuidBytes,
) -> Result<LabelRow, RiffDbError> {
    let fields = field_map(record).map_err(|_| RiffDbError::Rpc("label field_map".into()))?;
    Ok(LabelRow {
        organization_id,
        label_id,
        name: require_string(
            fields
                .get(&label_field::NAME)
                .ok_or_else(|| RiffDbError::Rpc("label missing name".into()))?,
        )?,
    })
}

/// Public RiffDB adapter errors.
#[derive(Clone, Debug)]
pub enum RiffDbError {
    /// Bad endpoint/credential.
    Connection,
    /// Bootstrap failed.
    Bootstrap,
    /// Contract deploy failed.
    Deploy,
    /// Server process failed.
    Server,
    /// Filesystem error.
    Io,
    /// RPC failed.
    Rpc(String),
    /// Timeout.
    Timeout,
    /// Runtime missing.
    Runtime,
    /// Schema/constant mismatch.
    InvalidSchema,
    /// Decode failure.
    Decode,
    /// Scenario not supported by current contract surface.
    Unsupported(&'static str),
}

impl fmt::Display for RiffDbError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection => formatter.write_str("riffdb connection failed"),
            Self::Bootstrap => formatter.write_str("riffdb bootstrap failed"),
            Self::Deploy => formatter.write_str("riffdb contract deploy failed"),
            Self::Server => formatter.write_str("riffdbd process failed"),
            Self::Io => formatter.write_str("riffdb harness io failed"),
            Self::Rpc(detail) => write!(formatter, "riffdb rpc failed: {detail}"),
            Self::Timeout => formatter.write_str("riffdb rpc timeout"),
            Self::Runtime => formatter.write_str("riffdb async runtime unavailable"),
            Self::InvalidSchema => formatter.write_str("riffdb schema/constants invalid"),
            Self::Decode => formatter.write_str("riffdb response decode failed"),
            Self::Unsupported(reason) => write!(formatter, "unsupported scenario: {reason}"),
        }
    }
}

impl Error for RiffDbError {}
