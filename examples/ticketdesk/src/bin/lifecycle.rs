#![forbid(unsafe_code)]

//! Public generated-client preparation for the sustainable TicketDesk campaign.

use std::env;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::time::Duration;

use riffdb_client_rust::{
    AttemptBudget, CallMetadata, DatabaseAlias, EventConsumerOptions, StableApplicationClient,
    load_protected_bearer_credential,
};
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
};
use tokio::task::JoinSet;

#[allow(dead_code, missing_docs, unreachable_pub, unused_imports)]
mod application {
    include!("../../lifecycle/generated/rust/client.rs");
}

#[allow(dead_code, missing_docs, unreachable_pub, unused_imports)]
mod application_v2 {
    include!("../../lifecycle-v2/generated/rust/client.rs");
}

use application::{
    CreateOrganizationInput, CreateOrganizationOutcome, CreateProjectInput, CreateProjectOutcome,
    CreateTicketInput, CreateTicketOutcome, CreateUserInput, CreateUserOutcome, GetTicketParams,
    GetTicketResult, TicketDeskLifecycleClient, TicketEventsConsumer, TicketEventsEvent,
    TicketEventsParams, TicketQueueResult, TicketQueueWatchParams, TicketQueueWatchUpdate,
    UpdateTicketInput, UpdateTicketOutcome,
};

#[path = "../lifecycle_workload/current.rs"]
mod current;
#[path = "../lifecycle_workload/current_reactive.rs"]
mod current_reactive;
#[path = "../lifecycle_workload/deployment.rs"]
mod deployment;
#[path = "../lifecycle_workload/model.rs"]
mod model;
#[path = "../lifecycle_workload/observe.rs"]
mod observe;
#[path = "../lifecycle_workload/probe.rs"]
mod probe;
#[path = "../lifecycle_workload/recovery.rs"]
mod recovery;

type Result<T> = std::result::Result<T, &'static str>;
const ORGANIZATIONS: u64 = 64;
const TICKETS_PER_ORGANIZATION: u64 = 1024;
const SEED_WORKERS: u64 = 16;

#[tokio::main]
async fn main() {
    let result = match env::args().nth(1).as_deref() {
        Some("seed") => seed().await,
        Some("smoke") => smoke().await,
        Some("reconcile") => reconcile().await,
        Some("probe") => probe::run().await,
        Some("descriptor") => descriptor(),
        Some("observe") => observe::run().await,
        Some("successor-smoke") => current::smoke().await,
        Some("recovery-smoke") => recovery::smoke().await,
        _ => Err("arguments"),
    };
    if let Err(code) = result {
        eprintln!("ticketdesk-lifecycle-worker: {code}");
        std::process::exit(1);
    }
}

async fn connect_base(
    credential_variable: &str,
) -> Result<(StableApplicationClient, CallMetadata)> {
    let credential_path = env::var_os(credential_variable).ok_or("configuration")?;
    connect_base_path(Path::new(&credential_path)).await
}

async fn connect_base_path(
    credential_path: &Path,
) -> Result<(StableApplicationClient, CallMetadata)> {
    let endpoint = env::var("RIFFDB_LIFECYCLE_ENDPOINT").map_err(|_| "configuration")?;
    let ca = env::var_os("RIFFDB_LIFECYCLE_CA").ok_or("configuration")?;
    let credential = load_protected_bearer_credential(credential_path).map_err(|_| "credential")?;
    let tls = TlsClientConfig::new(
        CanonicalHttpsEndpoint::parse(&endpoint).map_err(|_| "configuration")?,
        ProtectedFilePath::new(PathBuf::from(ca)).map_err(|_| "configuration")?,
        TlsServerIdentity::parse("127.0.0.1").map_err(|_| "configuration")?,
        Duration::from_secs(5),
        Duration::from_secs(10),
        NonZeroU32::new(1).ok_or("configuration")?,
        NonZeroU32::new(32).ok_or("configuration")?,
    )
    .map_err(|_| "configuration")?;
    let client = StableApplicationClient::connect_verified_tls(&tls)
        .await
        .map_err(|_| "connection")?;
    let metadata =
        CallMetadata::authenticated(credential).with_database(DatabaseAlias::default_alias());
    Ok((client, metadata))
}

async fn connect(credential_variable: &str) -> Result<TicketDeskLifecycleClient> {
    let (client, metadata) = connect_base(credential_variable).await?;
    Ok(TicketDeskLifecycleClient::new(
        client,
        metadata,
        AttemptBudget::new(1).ok_or("configuration")?,
    ))
}

fn uuid(kind: u64, ordinal: u64) -> String {
    format!("01900000-0000-7000-{kind:04x}-{ordinal:012x}")
}

fn key(kind: &str, ordinal: u64) -> String {
    format!("ticketdesk-lifecycle-seed-{kind}-{ordinal}")
}

async fn foundation(client: &mut TicketDeskLifecycleClient, organization: u64) -> Result<()> {
    let organization_id = uuid(0x8001, organization);
    let created = client
        .create_organization(CreateOrganizationInput {
            name: format!("Lifecycle organization {organization}"),
            organization_id: organization_id.clone(),
            idempotency_key: key("organization", organization),
        })
        .await
        .map_err(|_| "command")?;
    if !matches!(created.outcome, CreateOrganizationOutcome::Created { .. }) {
        return Err("seed_outcome");
    }
    let user = client
        .create_user(CreateUserInput {
            email: format!("lifecycle-{organization}@example.com"),
            display_name: format!("Lifecycle user {organization}"),
            organization_id: organization_id.clone(),
            user_id: uuid(0x8002, organization),
            idempotency_key: key("user", organization),
        })
        .await
        .map_err(|_| "command")?;
    if !matches!(user.outcome, CreateUserOutcome::Created { .. }) {
        return Err("seed_outcome");
    }
    let project = client
        .create_project(CreateProjectInput {
            name: format!("Lifecycle project {organization}"),
            organization_id,
            project_id: uuid(0x8003, organization),
            idempotency_key: key("project", organization),
        })
        .await
        .map_err(|_| "command")?;
    if !matches!(project.outcome, CreateProjectOutcome::Created { .. }) {
        return Err("seed_outcome");
    }
    Ok(())
}

async fn ticket(client: &mut TicketDeskLifecycleClient, ordinal: u64) -> Result<()> {
    let organization = ordinal / TICKETS_PER_ORGANIZATION;
    let created = client
        .create_ticket(CreateTicketInput {
            organization_id: uuid(0x8001, organization),
            ticket_id: uuid(0x8004, ordinal),
            project_id: uuid(0x8003, organization),
            reporter_id: uuid(0x8002, organization),
            assignee_id: uuid(0x8002, organization),
            title: format!("Lifecycle ticket {ordinal}"),
            status: "Open".to_owned(),
            idempotency_key: key("ticket", ordinal),
        })
        .await
        .map_err(|_| "command")?;
    if !matches!(created.outcome, CreateTicketOutcome::Created { .. }) {
        return Err("seed_outcome");
    }
    Ok(())
}

async fn seed() -> Result<()> {
    let mut workers = JoinSet::new();
    for worker in 0..SEED_WORKERS {
        workers.spawn(async move {
            let mut client = connect("RIFFDB_LIFECYCLE_SEED_CREDENTIAL").await?;
            for organization in (worker..ORGANIZATIONS).step_by(SEED_WORKERS as usize) {
                foundation(&mut client, organization).await?;
                for local in 0..TICKETS_PER_ORGANIZATION {
                    ticket(&mut client, organization * TICKETS_PER_ORGANIZATION + local).await?;
                }
            }
            Ok::<_, &'static str>(())
        });
    }
    while let Some(result) = workers.join_next().await {
        result.map_err(|_| "worker")??;
    }
    println!(
        "{{\"schema\":\"riffdb.ticketdesk-lifecycle-seed/v1\",\"organizations\":64,\"tickets\":65536,\"max_in_flight\":16}}"
    );
    Ok(())
}

async fn smoke() -> Result<()> {
    // Run in an owned preparation database, before creating campaign evidence.
    let mut seeder = connect("RIFFDB_LIFECYCLE_SEED_CREDENTIAL").await?;
    foundation(&mut seeder, 0).await?;
    ticket(&mut seeder, 0).await?;
    let mut client = connect("RIFFDB_LIFECYCLE_APPLICATION_CREDENTIAL").await?;
    let input = model::update(0)?;
    let changed = client
        .update_ticket(input.clone())
        .await
        .map_err(|_| "command")?;
    if changed.outcome
        != (UpdateTicketOutcome::Updated {
            revision: 1,
            status: "InProgress".to_owned(),
        })
    {
        return Err("update_outcome");
    }
    model::validate_update(0, &changed.outcome)?;
    let committed = changed.commit_sequence.ok_or("missing_commit")?;
    let replay = client
        .update_ticket(input.clone())
        .await
        .map_err(|_| "command")?;
    if !replay.replayed
        || replay.commit_sequence != changed.commit_sequence
        || replay.outcome != changed.outcome
    {
        return Err("idempotency");
    }
    let stale = client
        .update_ticket(UpdateTicketInput {
            idempotency_key: "ticketdesk-lifecycle-smoke-stale".to_owned(),
            ..input
        })
        .await
        .map_err(|_| "command")?;
    if stale.outcome != UpdateTicketOutcome::Stale {
        return Err("caller_fence");
    }
    let read = client
        .get_ticket_after_commit(
            GetTicketParams {
                organization_id: uuid(0x8001, 0),
                ticket_id: uuid(0x8004, 0),
            },
            committed,
        )
        .await
        .map_err(|_| "read")?;
    let GetTicketResult::Found(found) = read.value else {
        return Err("state_reconciliation");
    };
    if found.ticket.revision != 1
        || found.ticket.status != "InProgress"
        || found.ticket.title != "Lifecycle ticket 0"
    {
        return Err("state_reconciliation");
    }
    if !matches!(
        client
            .get_ticket(GetTicketParams {
                organization_id: uuid(0x8001, 1),
                ticket_id: uuid(0x8004, 0),
            })
            .await
            .map_err(|_| "read")?,
        GetTicketResult::NotFound(_)
    ) {
        return Err("partition_isolation");
    }
    let consumer = TicketEventsConsumer {
        parameters: TicketEventsParams {
            organization_id: uuid(0x8001, 0),
        },
        consumer_name: "ticketdesk-lifecycle-smoke".to_owned(),
    };
    let mut created_events = 0;
    let mut updated_events = 0;
    for _ in 0..4 {
        let batch = client
            .next_ticket_events(
                consumer.clone(),
                EventConsumerOptions {
                    batch_limit: 32,
                    in_flight_limit: 32,
                    lease_seconds: 60,
                    maximum_wait_nanos: 0,
                },
            )
            .await
            .map_err(|_| "consumer")?;
        if batch.events.is_empty() {
            break;
        }
        for delivery in &batch.events {
            match &delivery.event {
                TicketEventsEvent::TicketCreated(event) => {
                    if event.ticket_id != uuid(0x8004, 0) {
                        return Err("event_reconciliation");
                    }
                    created_events += 1;
                }
                TicketEventsEvent::TicketUpdated(event) => {
                    if event.ticket_id != uuid(0x8004, 0)
                        || event.revision != 1
                        || event.status != "InProgress"
                        || event.increment != 1
                    {
                        return Err("event_reconciliation");
                    }
                    updated_events += 1;
                }
            }
            client
                .ack_ticket_events(&consumer, delivery)
                .await
                .map_err(|_| "consumer")?;
        }
    }
    if (created_events, updated_events) != (1, 1) {
        return Err("event_reconciliation");
    }
    let mut stream = client
        .watch_ticket_queue_watch(
            TicketQueueWatchParams {
                organization_id: uuid(0x8001, 0),
                project_id: uuid(0x8003, 0),
            },
            None,
        )
        .await
        .map_err(|_| "live_query")?;
    let update = tokio::time::timeout(Duration::from_secs(10), stream.message())
        .await
        .map_err(|_| "live_query_timeout")?
        .map_err(|_| "live_query")?
        .ok_or("live_query_ended")?;
    let TicketQueueWatchUpdate::Snapshot(snapshot) = update else {
        return Err("live_query_snapshot");
    };
    let TicketQueueResult::Found(queue) = snapshot.result;
    if queue.tickets.len() != 1
        || queue.tickets[0].ticket_id != uuid(0x8004, 0)
        || queue.tickets[0].status != "InProgress"
    {
        return Err("live_query_reconciliation");
    }
    println!(
        "{{\"schema\":\"riffdb.ticketdesk-lifecycle-smoke/v1\",\"caller_fence\":true,\"idempotency\":true,\"state_reconciled\":true,\"partition_isolation\":true,\"events_reconciled\":true,\"live_query_snapshot\":true}}"
    );
    Ok(())
}

async fn reconcile() -> Result<()> {
    let completed = env::var("RIFFDB_LIFECYCLE_COMPLETED_COMMANDS")
        .map_err(|_| "configuration")?
        .parse::<u64>()
        .map_err(|_| "configuration")?;
    if completed > model::MAX_COMMANDS {
        return Err("operation_bound");
    }
    let mut workers = JoinSet::new();
    for worker in 0..SEED_WORKERS {
        workers.spawn(async move {
            let mut client = current::connect().await?;
            for ticket in (worker..model::POPULATION).step_by(SEED_WORKERS as usize) {
                let query = GetTicketParams {
                    organization_id: uuid(0x8001, ticket / TICKETS_PER_ORGANIZATION),
                    ticket_id: uuid(0x8004, ticket),
                };
                let GetTicketResult::Found(found) =
                    client.get_ticket(query).await.map_err(|_| "read")?
                else {
                    return Err("state_reconciliation");
                };
                model::validate_final(completed, ticket, &found.ticket)?;
            }
            Ok::<_, &'static str>(())
        });
    }
    while let Some(result) = workers.join_next().await {
        result.map_err(|_| "worker")??;
    }
    println!(
        "{{\"schema\":\"riffdb.ticketdesk-lifecycle-state-reconciliation/v1\",\"tickets_checked\":65536,\"updates\":{completed},\"max_in_flight\":16}}"
    );
    Ok(())
}

fn descriptor() -> Result<()> {
    let bundle = riffdb_contract_compiler::compile_contract_source(include_str!(
        "../../lifecycle/riffdb/contract.riff"
    ))
    .map_err(|_| "contract_descriptor")?;
    if bundle.bundle_hash().as_bytes() != &application::CONTRACT_BUNDLE_HASH {
        return Err("contract_descriptor_hash");
    }
    let projection = bundle
        .projections()
        .iter()
        .find(|projection| projection.name() == "TicketUpdates")
        .ok_or("projection_descriptor")?;
    println!(
        "{{\"schema\":\"riffdb.ticketdesk-lifecycle-descriptor/v1\",\"projection_id\":{},\"projection_plan_hash\":\"{}\",\"reactive_v1\":\"{}\",\"reactive_v2\":\"{}\"}}",
        projection.projection_id().get(),
        projection
            .plan_hash()
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        application::TICKET_ACTIVITY_REACTIVE_MODULE_HASH
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        application_v2::TICKET_ACTIVITY_REACTIVE_MODULE_HASH
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    Ok(())
}
