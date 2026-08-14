#![forbid(unsafe_code)]

//! Long-lived public-surface TicketDesk workload for the alpha endurance gate.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use riffdb_client_rust::{
    AttemptBudget, CallMetadata, DatabaseAlias, EventConsumerOptions, StableApplicationClient,
    load_protected_bearer_credential,
};
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
};
use riffdb_ticketdesk::{
    CreateCommentInput, CreateOrganizationInput, CreateProjectInput, CreateTicketInput,
    CreateUserInput, TicketDeskClient, TicketEventsConsumer, TicketEventsEvent, TicketEventsParams,
    TicketPageParams, TicketPageResult, TicketQueueWatchParams, TriageTicketConsumer,
    TriageTicketParams,
};
use tokio::sync::Mutex;

const TENANTS: [&str; 4] = [
    "tenant_alpha",
    "tenant_beta",
    "tenant_gamma",
    "tenant_delta",
];
const EXPECTED_TENANTS_JSON: &str =
    "[\"tenant_alpha\",\"tenant_beta\",\"tenant_gamma\",\"tenant_delta\"]";
const WORKLOADS: [&str; 5] = ["events", "live_queries", "reads", "workflows", "writes"];

type WorkerResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug)]
struct Metrics {
    started_seconds: u64,
    operations: u64,
    transport_attempts: u64,
    modeled_retained_bytes: u64,
    workloads: BTreeMap<&'static str, u64>,
    tenants: BTreeMap<&'static str, u64>,
}

impl Metrics {
    fn new() -> WorkerResult<Self> {
        let started_seconds = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        Ok(Self {
            started_seconds,
            operations: 0,
            transport_attempts: 0,
            modeled_retained_bytes: 0,
            workloads: WORKLOADS.into_iter().map(|name| (name, 0)).collect(),
            tenants: TENANTS.into_iter().map(|name| (name, 0)).collect(),
        })
    }

    fn record(&mut self, workload: &'static str, tenant: &'static str, retained_bytes: u64) {
        self.operations = self.operations.saturating_add(1);
        self.transport_attempts = self.transport_attempts.saturating_add(1);
        self.modeled_retained_bytes = self.modeled_retained_bytes.saturating_add(retained_bytes);
        *self.workloads.get_mut(workload).expect("closed workload") += 1;
        *self.tenants.get_mut(tenant).expect("closed tenant") += 1;
    }

    fn json(&self) -> String {
        format!(
            concat!(
                "{{\"schema\":\"riffdb.alpha-endurance-worker/v1\",",
                "\"language\":\"rust\",\"pid\":{},\"started_unix_seconds\":{},",
                "\"logical_operations\":{},\"transport_attempts\":{},",
                "\"declared_retries\":0,\"error_count\":0,",
                "\"modeled_retained_bytes\":{},",
                "\"workloads\":{{\"events\":{},\"live_queries\":{},\"reads\":{},",
                "\"workflows\":{},\"writes\":{}}},",
                "\"tenants\":{{\"tenant_alpha\":{},\"tenant_beta\":{},",
                "\"tenant_gamma\":{},\"tenant_delta\":{}}}}}\n"
            ),
            std::process::id(),
            self.started_seconds,
            self.operations,
            self.transport_attempts,
            self.modeled_retained_bytes,
            self.workloads["events"],
            self.workloads["live_queries"],
            self.workloads["reads"],
            self.workloads["workflows"],
            self.workloads["writes"],
            self.tenants["tenant_alpha"],
            self.tenants["tenant_beta"],
            self.tenants["tenant_gamma"],
            self.tenants["tenant_delta"],
        )
    }
}

struct Clients {
    seeder: TicketDeskClient,
    application: TicketDeskClient,
    agent: TicketDeskClient,
}

#[tokio::main]
async fn main() -> WorkerResult<()> {
    require_environment()?;
    let artifact_root = absolute_environment_path("RIFFDB_ENDURANCE_ARTIFACT_ROOT")?;
    let environment_root = artifact_root.join("environment-v1");
    let metrics_path = environment_root.join("metrics/rust.json");
    let metrics = Arc::new(Mutex::new(Metrics::new()?));
    publish_metrics(&metrics_path, &metrics).await?;

    let clients = required_u64("RIFFDB_ENDURANCE_CLIENTS")?;
    if clients != 4 {
        return Err("Rust endurance worker requires exactly four clients".into());
    }
    let maximum_rate = required_u64("RIFFDB_ENDURANCE_MAXIMUM_OPERATIONS_PER_SECOND")?;
    if !(1..=1_024).contains(&maximum_rate) {
        return Err("Rust endurance rate exceeds its checked bound".into());
    }
    let seed = required_u64("RIFFDB_ENDURANCE_SEED")?;
    let delay = Duration::from_nanos(
        1_000_000_000_u64
            .saturating_mul(clients)
            .div_ceil(maximum_rate),
    );

    let mut tasks = Vec::new();
    for client_index in 0..clients {
        let metrics = Arc::clone(&metrics);
        let metrics_path = metrics_path.clone();
        let environment_root = environment_root.clone();
        tasks.push(tokio::spawn(async move {
            run_client(
                client_index,
                seed,
                delay,
                &environment_root,
                &metrics_path,
                metrics,
            )
            .await
        }));
    }
    for task in tasks {
        task.await??;
    }
    Ok(())
}

async fn run_client(
    client_index: u64,
    seed: u64,
    delay: Duration,
    environment_root: &Path,
    metrics_path: &Path,
    metrics: Arc<Mutex<Metrics>>,
) -> WorkerResult<()> {
    let tenant = TENANTS[usize::try_from(client_index)?];
    let organization_id = id(10, client_index);
    let user_id = id(seed, 100 + client_index);
    let project_id = id(seed, 200 + client_index);
    let hot_ticket_id = id(seed, 300 + client_index);
    let mut clients = Clients {
        seeder: connect(environment_root, "seeder.credential").await?,
        application: connect(environment_root, "application.credential").await?,
        agent: connect(environment_root, "agent.credential").await?,
    };

    seed_client(
        &mut clients,
        tenant,
        &organization_id,
        &user_id,
        &project_id,
        &hot_ticket_id,
        seed,
        client_index,
        &metrics,
    )
    .await?;
    publish_metrics(metrics_path, &metrics).await?;

    let event_consumer = TicketEventsConsumer {
        parameters: TicketEventsParams {
            organization_id: organization_id.clone(),
        },
        consumer_name: format!("endurance-rust-events-{client_index}"),
    };
    let triage_consumer = TriageTicketConsumer {
        parameters: TriageTicketParams {
            organization_id: organization_id.clone(),
        },
        consumer_name: format!("endurance-rust-triage-{client_index}"),
    };
    let mut counter = 0_u64;
    loop {
        let slot = counter % 100;
        if slot < 35 {
            let page = clients
                .application
                .ticket_page(TicketPageParams {
                    organization_id: organization_id.clone(),
                    ticket_id: hot_ticket_id.clone(),
                })
                .await?;
            if !matches!(page, TicketPageResult::Found(_)) {
                return Err("Rust endurance read lost its hot ticket".into());
            }
            record(&metrics, "reads", tenant, 0).await;
        } else if slot < 60 {
            clients
                .application
                .create_comment(CreateCommentInput {
                    body: format!("rust endurance comment {counter}"),
                    author_id: user_id.clone(),
                    ticket_id: hot_ticket_id.clone(),
                    comment_id: id(seed, 10_000 + client_index * 1_000_000 + counter),
                    idempotency_key: format!("endurance-rust-comment-{client_index}-{counter}"),
                    organization_id: organization_id.clone(),
                })
                .await?;
            record(&metrics, "writes", tenant, 512).await;
        } else if slot < 70 {
            let batch = clients
                .agent
                .next_triage_ticket(triage_consumer.clone(), 0)
                .await?;
            record(&metrics, "workflows", tenant, 0).await;
            if let Some(item) = batch.items.first() {
                let TicketEventsEvent::TicketCreated(event) = &item.event;
                clients
                    .agent
                    .react_comment(
                        &triage_consumer,
                        item,
                        &CreateCommentInput {
                            body: "rust contextual endurance reaction".to_owned(),
                            author_id: event.reporter_id.clone(),
                            ticket_id: event.ticket_id.clone(),
                            comment_id: id(seed, 20_000 + client_index * 1_000_000 + counter),
                            idempotency_key: format!(
                                "endurance-rust-reaction-{client_index}-{counter}"
                            ),
                            organization_id: organization_id.clone(),
                        },
                    )
                    .await?;
                record(&metrics, "workflows", tenant, 512).await;
            }
        } else if slot < 80 {
            let batch = clients
                .agent
                .next_ticket_events(
                    event_consumer.clone(),
                    EventConsumerOptions {
                        batch_limit: 1,
                        in_flight_limit: 4,
                        lease_seconds: 60,
                        maximum_wait_nanos: 0,
                    },
                )
                .await?;
            record(&metrics, "events", tenant, 0).await;
            if let Some(delivery) = batch.events.first() {
                clients
                    .agent
                    .ack_ticket_events(&event_consumer, delivery)
                    .await?;
                record(&metrics, "events", tenant, 0).await;
            }
        } else {
            let mut stream = clients
                .application
                .watch_ticket_queue_watch(
                    TicketQueueWatchParams {
                        organization_id: organization_id.clone(),
                        project_id: project_id.clone(),
                    },
                    None,
                )
                .await?;
            let update = tokio::time::timeout(Duration::from_secs(5), stream.message()).await??;
            if update.is_none() {
                return Err("Rust endurance live query ended before its snapshot".into());
            }
            record(&metrics, "live_queries", tenant, 0).await;
        }

        if slot == 69 {
            let cold_ordinal = (counter / 100) % 4_096;
            clients
                .application
                .create_ticket(CreateTicketInput {
                    title: format!("Rust cold ticket {client_index}-{cold_ordinal}"),
                    status: "Open".to_owned(),
                    ticket_id: id(seed, 30_000 + client_index * 4_096 + cold_ordinal),
                    project_id: project_id.clone(),
                    assignee_id: user_id.clone(),
                    reporter_id: user_id.clone(),
                    idempotency_key: format!("endurance-rust-cold-{client_index}-{cold_ordinal}"),
                    organization_id: organization_id.clone(),
                })
                .await?;
            record(&metrics, "workflows", tenant, 768).await;
        }
        counter = counter.saturating_add(1);
        if counter.is_multiple_of(64) {
            publish_metrics(metrics_path, &metrics).await?;
        }
        tokio::time::sleep(delay).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn seed_client(
    clients: &mut Clients,
    tenant: &'static str,
    organization_id: &str,
    user_id: &str,
    project_id: &str,
    ticket_id: &str,
    seed: u64,
    client_index: u64,
    metrics: &Arc<Mutex<Metrics>>,
) -> WorkerResult<()> {
    clients
        .seeder
        .create_organization(CreateOrganizationInput {
            name: format!("Endurance {tenant}"),
            organization_id: organization_id.to_owned(),
            idempotency_key: format!("endurance-organization-{tenant}"),
        })
        .await?;
    record(metrics, "writes", tenant, 512).await;
    clients
        .seeder
        .create_user(CreateUserInput {
            email: format!("rust-{client_index}@{tenant}.example.test"),
            user_id: user_id.to_owned(),
            display_name: format!("Rust endurance {client_index}"),
            idempotency_key: format!("endurance-rust-user-{client_index}"),
            organization_id: organization_id.to_owned(),
        })
        .await?;
    record(metrics, "writes", tenant, 512).await;
    clients
        .seeder
        .create_project(CreateProjectInput {
            name: format!("Rust endurance {client_index}"),
            project_id: project_id.to_owned(),
            idempotency_key: format!("endurance-rust-project-{client_index}"),
            organization_id: organization_id.to_owned(),
        })
        .await?;
    record(metrics, "writes", tenant, 512).await;
    clients
        .application
        .create_ticket(CreateTicketInput {
            title: format!("Rust hot ticket {client_index}"),
            status: "Open".to_owned(),
            ticket_id: ticket_id.to_owned(),
            project_id: project_id.to_owned(),
            assignee_id: user_id.to_owned(),
            reporter_id: user_id.to_owned(),
            idempotency_key: format!("endurance-rust-hot-{seed}-{client_index}"),
            organization_id: organization_id.to_owned(),
        })
        .await?;
    record(metrics, "writes", tenant, 768).await;
    Ok(())
}

async fn connect(environment_root: &Path, credential_name: &str) -> WorkerResult<TicketDeskClient> {
    let endpoint = required("RIFFDB_ENDURANCE_ENDPOINT")?;
    let tls = TlsClientConfig::new(
        CanonicalHttpsEndpoint::parse(&endpoint)?,
        ProtectedFilePath::new(environment_root.join("ca.pem"))?,
        TlsServerIdentity::parse("127.0.0.1")?,
        Duration::from_secs(5),
        Duration::from_secs(30),
        NonZeroU32::new(4).expect("nonzero pool bound"),
        NonZeroU32::new(64).expect("nonzero stream bound"),
    )?;
    let credential = load_protected_bearer_credential(&environment_root.join(credential_name))?;
    let metadata =
        CallMetadata::authenticated(credential).with_database(DatabaseAlias::default_alias());
    Ok(TicketDeskClient::new(
        StableApplicationClient::connect_verified_tls(&tls).await?,
        metadata,
        AttemptBudget::new(1).expect("positive attempt budget"),
    ))
}

async fn record(
    metrics: &Arc<Mutex<Metrics>>,
    workload: &'static str,
    tenant: &'static str,
    retained_bytes: u64,
) {
    metrics
        .lock()
        .await
        .record(workload, tenant, retained_bytes);
}

async fn publish_metrics(path: &Path, metrics: &Arc<Mutex<Metrics>>) -> WorkerResult<()> {
    let next = path.with_extension("json.next");
    let value = metrics.lock().await.json();
    fs::write(&next, value)?;
    fs::rename(next, path)?;
    Ok(())
}

fn require_environment() -> WorkerResult<()> {
    if required("RIFFDB_ENDURANCE_LANGUAGE")? != "rust"
        || required("RIFFDB_ENDURANCE_TENANTS_JSON")? != EXPECTED_TENANTS_JSON
    {
        return Err("Rust endurance worker environment differs from the closed manifest".into());
    }
    let coverage = required("RIFFDB_ENDURANCE_WORKLOAD_COVERAGE_JSON")?;
    for required_name in [
        "cold_keys",
        "events",
        "hot_keys",
        "live_queries",
        "multiple_tenants",
        "reads",
        "workflows",
        "writes",
    ] {
        if !coverage.contains(&format!("\"{required_name}\"")) {
            return Err("Rust endurance workload coverage is incomplete".into());
        }
    }
    Ok(())
}

fn required(name: &str) -> WorkerResult<String> {
    let value = env::var(name).map_err(|_| format!("{name} is required"))?;
    if value.is_empty() || value.len() > 16_384 || value.contains('\0') {
        return Err(format!("{name} is invalid").into());
    }
    Ok(value)
}

fn required_u64(name: &str) -> WorkerResult<u64> {
    Ok(required(name)?.parse()?)
}

fn absolute_environment_path(name: &str) -> WorkerResult<PathBuf> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() || path.is_symlink() {
        return Err(format!("{name} must be an absolute non-symlink path").into());
    }
    Ok(path)
}

fn id(namespace: u64, value: u64) -> String {
    format!(
        "{namespace:08x}-0000-8000-8000-{value:012x}",
        namespace = namespace & 0xffff_ffff,
        value = value & 0xffff_ffff_ffff,
    )
}
