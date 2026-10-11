//! Private workload assembly for two exact generated application releases.

use super::{Result, application as v1, application_v2 as v2};
use riffdb_client_rust::{
    ApplicationClientError, AttemptBudget, CallMetadata, RiffDbClient, TypedCommandResult,
    generated::GeneratedCommand,
};

/// The public generated-command transport performs exactly one Execute RPC.
/// Recovery belongs to the workload's retained-input loop, so an automatic
/// outcome lookup cannot escape its accounting or in-flight reservation.
pub(super) struct Submission {
    client: RiffDbClient,
    metadata: CallMetadata,
}

impl Submission {
    async fn execute<C: GeneratedCommand>(
        &mut self,
        input: &C,
    ) -> std::result::Result<TypedCommandResult<C::Outcome>, ApplicationClientError> {
        let execution = self
            .client
            .execute_generated(
                input,
                AttemptBudget::new(1).ok_or(ApplicationClientError::InvalidInput)?,
                &self.metadata,
            )
            .await?;
        let (outcome, response) = execution.into_parts();
        let workflow_revisions = input
            .workflow_successor_revisions(&response)
            .map_err(riffdb_client_rust::GeneratedExecutionError::CommandShape)?;
        Ok(TypedCommandResult {
            outcome,
            commit_sequence: (response.commit_sequence != 0).then_some(response.commit_sequence),
            contract_version: response.contract_version,
            plan_hash: response
                .plan_hash
                .try_into()
                .map_err(|_| ApplicationClientError::InvalidResponse)?,
            replayed: response.status
                == riffdb_proto::v1::execute_command_response::CompletionStatus::Replayed as i32,
            outcome_uri: response.outcome_uri,
            workflow_revisions,
        })
    }
}

pub(super) enum CurrentClient {
    V1(v1::TicketDeskLifecycleClient, Submission),
    V2(v2::TicketDeskLifecycleClient, Submission),
}

pub(super) async fn connect() -> Result<CurrentClient> {
    let version = match std::env::var("RIFFDB_LIFECYCLE_CONTRACT_VERSION") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "1".to_owned(),
        Err(std::env::VarError::NotUnicode(_)) => return Err("contract_version"),
    };
    let credential =
        std::env::var_os("RIFFDB_LIFECYCLE_APPLICATION_CREDENTIAL").ok_or("configuration")?;
    let version = match version.as_str() {
        "1" => 1,
        "2" => 2,
        _ => return Err("contract_version"),
    };
    connect_release(version, std::path::Path::new(&credential)).await
}

pub(super) async fn connect_release(
    version: u64,
    credential: &std::path::Path,
) -> Result<CurrentClient> {
    let (client, metadata) = super::connect_base_path(credential).await?;
    let (tls, command_metadata) = super::connection_configuration(credential)?;
    let commands = Submission {
        client: RiffDbClient::connect_verified_tls(&tls)
            .await
            .map_err(|_| "connection")?,
        metadata: command_metadata,
    };
    let attempts = AttemptBudget::new(1).ok_or("configuration")?;
    match version {
        1 => Ok(CurrentClient::V1(
            v1::TicketDeskLifecycleClient::new(client, metadata, attempts),
            commands,
        )),
        2 => Ok(CurrentClient::V2(
            v2::TicketDeskLifecycleClient::new(client, metadata, attempts),
            commands,
        )),
        _ => Err("contract_version"),
    }
}

impl CurrentClient {
    pub(super) async fn get_ticket(
        &mut self,
        input: v1::GetTicketParams,
    ) -> std::result::Result<v1::GetTicketResult, ApplicationClientError> {
        match self {
            Self::V1(client, _) => client.get_ticket(input).await,
            Self::V2(client, _) => {
                let result = client
                    .get_ticket(v2::GetTicketParams {
                        organization_id: input.organization_id,
                        ticket_id: input.ticket_id,
                    })
                    .await?;
                Ok(match result {
                    v2::GetTicketResult::NotFound(_) => {
                        v1::GetTicketResult::NotFound(Box::new(v1::GetTicketNotFound {}))
                    }
                    v2::GetTicketResult::Found(found) => {
                        let row = found.ticket;
                        v1::GetTicketResult::Found(Box::new(v1::GetTicketFound {
                            ticket: v1::GetTicketFoundTicket {
                                ticket_id: row.ticket_id,
                                project_id: row.project_id,
                                title: row.title,
                                status: row.status,
                                revision: row.revision,
                                reporter_id: row.reporter_id,
                                assignee_id: row.assignee_id,
                            },
                        }))
                    }
                })
            }
        }
    }

    pub(super) async fn update_ticket(
        &mut self,
        input: v1::UpdateTicketInput,
    ) -> std::result::Result<TypedCommandResult<v1::UpdateTicketOutcome>, ApplicationClientError>
    {
        match self {
            Self::V1(_, commands) => commands.execute(&input).await,
            Self::V2(_, commands) => {
                let result = commands
                    .execute(&v2::UpdateTicketInput {
                        idempotency_key: input.idempotency_key,
                        organization_id: input.organization_id,
                        ticket_id: input.ticket_id,
                        expected_revision: input.expected_revision,
                        status: input.status,
                    })
                    .await?;
                let outcome = match result.outcome {
                    v2::UpdateTicketOutcome::Stale => v1::UpdateTicketOutcome::Stale,
                    v2::UpdateTicketOutcome::TicketMissing { ticket_id } => {
                        v1::UpdateTicketOutcome::TicketMissing { ticket_id }
                    }
                    v2::UpdateTicketOutcome::Updated { revision, status } => {
                        v1::UpdateTicketOutcome::Updated { revision, status }
                    }
                };
                Ok(TypedCommandResult {
                    outcome,
                    commit_sequence: result.commit_sequence,
                    contract_version: result.contract_version,
                    plan_hash: result.plan_hash,
                    replayed: result.replayed,
                    outcome_uri: result.outcome_uri,
                    workflow_revisions: result.workflow_revisions,
                })
            }
        }
    }
}

pub(super) async fn verify_successor_authorization() -> Result<()> {
    let mut client = connect().await?;
    let CurrentClient::V2(generated, _) = &mut client else {
        return Err("successor_required");
    };
    let denied = generated
        .lifecycle_version(v2::LifecycleVersionInput {
            organization_id: super::uuid(0x8001, 0),
            idempotency_key: "ticketdesk-lifecycle-ungranted-successor".to_owned(),
        })
        .await
        .err()
        .ok_or("successor_authorization")?;
    if denied.semantic_error().map(|error| error.code().as_str()) != Some("RDB-AUTH-0214") {
        return Err("successor_authorization");
    }
    println!(
        "{{\"schema\":\"riffdb.ticketdesk-lifecycle-successor-authorization/v1\",\"contract_version\":2,\"ungranted_command_refused\":true}}"
    );
    Ok(())
}

pub(super) async fn smoke() -> Result<()> {
    let mut client = connect().await?;
    if !matches!(&client, CurrentClient::V2(_, _)) {
        return Err("successor_required");
    }
    let CurrentClient::V2(generated, _) = &mut client else {
        return Err("successor_required");
    };
    let denied = generated
        .lifecycle_version(v2::LifecycleVersionInput {
            organization_id: super::uuid(0x8001, 0),
            idempotency_key: "ticketdesk-lifecycle-ungranted-successor".to_owned(),
        })
        .await
        .err()
        .ok_or("successor_authorization")?;
    if denied.semantic_error().map(|error| error.code().as_str()) != Some("RDB-AUTH-0214") {
        return Err("successor_authorization");
    }
    let input = super::model::update(super::model::POPULATION)?;
    let changed = client
        .update_ticket(input.clone())
        .await
        .map_err(|_| "successor_command")?;
    super::model::validate_update(super::model::POPULATION, &changed.outcome)?;
    if changed.contract_version != 2 || changed.replayed || changed.commit_sequence.is_none() {
        return Err("successor_identity");
    }
    let replay = client
        .update_ticket(input.clone())
        .await
        .map_err(|_| "successor_replay")?;
    if !replay.replayed
        || replay.outcome != changed.outcome
        || replay.commit_sequence != changed.commit_sequence
        || replay.contract_version != 2
    {
        return Err("successor_replay");
    }
    let stale = client
        .update_ticket(v1::UpdateTicketInput {
            idempotency_key: "ticketdesk-lifecycle-successor-stale".to_owned(),
            ..input
        })
        .await
        .map_err(|_| "successor_stale")?;
    if stale.outcome != v1::UpdateTicketOutcome::Stale {
        return Err("successor_stale");
    }
    let v1::GetTicketResult::Found(found) = client
        .get_ticket(v1::GetTicketParams {
            organization_id: super::uuid(0x8001, 0),
            ticket_id: super::uuid(0x8004, 0),
        })
        .await
        .map_err(|_| "successor_read")?
    else {
        return Err("successor_read");
    };
    super::model::validate_final(super::model::POPULATION + 1, 0, &found.ticket)?;
    if !matches!(
        client
            .get_ticket(v1::GetTicketParams {
                organization_id: super::uuid(0x8001, 1),
                ticket_id: super::uuid(0x8004, 0)
            })
            .await
            .map_err(|_| "successor_read")?,
        v1::GetTicketResult::NotFound(_)
    ) {
        return Err("successor_partition");
    }
    println!(
        "{{\"schema\":\"riffdb.ticketdesk-lifecycle-successor-smoke/v1\",\"contract_version\":2,\"caller_fence\":true,\"idempotency\":true,\"state_reconciled\":true,\"partition_isolation\":true}}"
    );
    Ok(())
}

/// A replacement keeps the principal's original idempotency namespace while
/// the revoked bearer loses both reads and same-key command recovery.
pub(super) async fn rotation_smoke() -> Result<()> {
    let old = std::env::var_os("RIFFDB_LIFECYCLE_REVOKED_CREDENTIAL").ok_or("configuration")?;
    let mut revoked = connect_release(1, std::path::Path::new(&old)).await?;
    let input = super::model::update(0)?;
    let parameters = v1::GetTicketParams {
        organization_id: super::uuid(0x8001, 0),
        ticket_id: super::uuid(0x8004, 0),
    };
    for (index, error) in [
        revoked.get_ticket(parameters.clone()).await.err(),
        revoked.update_ticket(input.clone()).await.err(),
    ]
    .into_iter()
    .enumerate()
    {
        // The symbolic query carries CapabilityRevoked. Native Execute's
        // public error bridge maps the authentication refusal to AuthorizationDenied.
        let expected = if index == 0 {
            riffdb_errors::ApplicationErrorCode::CapabilityRevoked
        } else {
            riffdb_errors::ApplicationErrorCode::AuthorizationDenied
        };
        if error
            .as_ref()
            .and_then(ApplicationClientError::semantic_error)
            .map(|error| error.code())
            != Some(expected)
        {
            return Err(if index == 0 {
                "rotation_revoked_read"
            } else {
                "rotation_revoked_command"
            });
        }
    }
    let root = std::env::var_os("RIFFDB_LIFECYCLE_WITNESS_ROOT").ok_or("configuration")?;
    let root = std::path::PathBuf::from(root);
    let quiesced = match std::env::var("RIFFDB_LIFECYCLE_QUIESCED_COMMANDS") {
        Ok(value) => {
            let count = value.parse::<u64>().map_err(|_| "rotation_witness")?;
            if count == 0
                || count > super::model::MAX_COMMANDS
                || count.to_string() != value
                || !root.join("load.quiesce").is_file()
                || !root.join("load.quiesced").is_file()
            {
                return Err("rotation_witness");
            }
            Some(count)
        }
        Err(std::env::VarError::NotPresent) => None,
        Err(_) => return Err("configuration"),
    };
    let mut reader = super::witness::Reader::open(&root.join("updates.witness"))?;
    let (original, revisions) = reader.acknowledged_prefix(quiesced.unwrap_or(1))?;
    if original.ordinal != 0
        || original.version != 1
        || original.incarnation != 1
        || (quiesced.is_none() && reader.next()?.is_some())
    {
        return Err("rotation_witness");
    }
    let mut replacement = connect().await?;
    let replay = replacement
        .update_ticket(input)
        .await
        .map_err(|_| "rotation_replay")?;
    super::model::validate_update(0, &replay.outcome)?;
    if !replay.replayed
        || replay.commit_sequence != Some(original.sequence)
        || replay.contract_version != original.version
    {
        return Err("rotation_identity");
    }
    let v1::GetTicketResult::Found(found) = replacement
        .get_ticket(parameters)
        .await
        .map_err(|_| "rotation_read")?
    else {
        return Err("rotation_read");
    };
    super::model::validate_revision(0, revisions[0], &found.ticket)?;
    println!(
        "{{\"schema\":\"riffdb.ticketdesk-lifecycle-rotation-smoke/v1\",\"revoked_reads_refused\":true,\"revoked_replay_refused\":true,\"same_principal_replay\":true,\"state_reconciled\":true}}"
    );
    Ok(())
}
