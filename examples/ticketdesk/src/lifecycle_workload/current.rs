//! Private workload assembly for two exact generated application releases.

use super::{Result, application as v1, application_v2 as v2};
use riffdb_client_rust::{ApplicationClientError, AttemptBudget, TypedCommandResult};

pub(super) enum CurrentClient {
    V1(v1::TicketDeskLifecycleClient),
    V2(v2::TicketDeskLifecycleClient),
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
    let attempts = AttemptBudget::new(1).ok_or("configuration")?;
    match version {
        1 => Ok(CurrentClient::V1(v1::TicketDeskLifecycleClient::new(
            client, metadata, attempts,
        ))),
        2 => Ok(CurrentClient::V2(v2::TicketDeskLifecycleClient::new(
            client, metadata, attempts,
        ))),
        _ => Err("contract_version"),
    }
}

impl CurrentClient {
    pub(super) async fn get_ticket(
        &mut self,
        input: v1::GetTicketParams,
    ) -> std::result::Result<v1::GetTicketResult, ApplicationClientError> {
        match self {
            Self::V1(client) => client.get_ticket(input).await,
            Self::V2(client) => {
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
            Self::V1(client) => client.update_ticket(input).await,
            Self::V2(client) => {
                let result = client
                    .update_ticket(v2::UpdateTicketInput {
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

pub(super) async fn smoke() -> Result<()> {
    let mut client = connect().await?;
    if !matches!(&client, CurrentClient::V2(_)) {
        return Err("successor_required");
    }
    let CurrentClient::V2(generated) = &mut client else {
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
