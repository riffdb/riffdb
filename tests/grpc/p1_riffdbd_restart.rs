#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Real-process WP-130 restart acceptance for the public Rust SDK and gRPC path.

use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{self, BufReader, Read, Write};
use std::net::SocketAddr;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::str;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use riffdb_auth::bootstrap_secret::{
    BootstrapCredential as RetainedBootstrapCredential, SystemEntropy,
    generate_bootstrap_credential, load_bootstrap_credential_file,
};
use riffdb_client_rust::generated::GeneratedCommand;
use riffdb_client_rust::generated::legal_spend::{
    AllocateBudget, AllocateBudgetOutcome, Amount, CONTRACT_LINEAGE, CONTRACT_VERSION,
    CreateBudget, CreateBudgetOutcome,
};
use riffdb_client_rust::{
    AttemptBudget, BearerCredential, BootstrapCallMetadata,
    BootstrapCredential as TransportBootstrapCredential, CallMetadata, ClientError,
    DetailsFreeStatus, RiffDbClient, generate_request_id,
};
use riffdb_proto::decimal_from_proto;
use riffdb_proto::v1;
use riffdb_types::{DecimalSpec, EntityKeyBuilder, EntityTypeId};
use tokio::time::timeout;
use tonic::transport::Endpoint;

const AUDIENCE: &str = "riffdb-grpc-loopback";
const ENVIRONMENT: &str = "p1-restart-test";
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const SHUTDOWN_COMMAND: &[u8] = b"shutdown\n";
const MAX_READY_LINE_BYTES: usize = 256;
const PROCESS_START_TIMEOUT: Duration = Duration::from_secs(15);
const PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(10);
const PROCESS_KILL_TIMEOUT: Duration = Duration::from_secs(5);
const PROCESS_REAPER_POLL: Duration = Duration::from_millis(10);
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const BOOTSTRAP_UNIX_MILLISECONDS: u64 = 1_700_000_000_000;
const CAPABILITY_LIFETIME_SECONDS: u32 = 3_600;
const FISCAL_YEAR: i64 = 2027;
const ORGANIZATION_ID: [u8; 16] = [0x11; 16];
const MATTER_ID: [u8; 16] = [0x22; 16];
const APPROVED_MINOR_UNITS: i128 = 10_000;
const ALLOCATED_MINOR_UNITS: i128 = 2_500;

const CAPABILITY_KEY_DOCUMENT: &[u8] = b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEY_DOCUMENT: &[u8] = b"riffdb-idempotency-digest-keys-v1\n9:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";
const BUDGET_CONTRACT: &str = include_str!("../../contracts/examples/budget.riff");

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_riffdbd_restart_preserves_budget_and_bootstrap_replay() -> TestResult<()> {
    let temporary = TemporaryDirectory::new()?;
    let database_path = temporary.path().join("riffdb.redb");
    let capability_keys_path = temporary.path().join("capability.keys");
    let idempotency_keys_path = temporary.path().join("idempotency.keys");
    let bootstrap_path = temporary.path().join("bootstrap.credential");

    write_protected_file(&capability_keys_path, CAPABILITY_KEY_DOCUMENT)?;
    write_protected_file(&idempotency_keys_path, IDEMPOTENCY_KEY_DOCUMENT)?;
    let generated_bootstrap =
        generate_bootstrap_credential(BOOTSTRAP_UNIX_MILLISECONDS, &SystemEntropy)?;
    write_protected_file(
        &bootstrap_path,
        generated_bootstrap.render_document().expose_secret(),
    )?;
    drop(generated_bootstrap);
    let retained_bootstrap = load_bootstrap_credential_file(&bootstrap_path)?;

    let bearer = bearer_credential(&retained_bootstrap)?;
    let authenticated = CallMetadata::authenticated(bearer);
    let entity_key = budget_entity_key()?;
    let create = CreateBudget {
        idempotency_key: "p1-create-budget".to_owned(),
        organization_id: ORGANIZATION_ID,
        fiscal_year: FISCAL_YEAR,
        approved_amount: amount(APPROVED_MINOR_UNITS)?,
    };
    let allocate = AllocateBudget {
        idempotency_key: "p1-allocate-budget".to_owned(),
        organization_id: ORGANIZATION_ID,
        fiscal_year: FISCAL_YEAR,
        matter_id: MATTER_ID,
        amount: amount(ALLOCATED_MINOR_UNITS)?,
    };

    let mut first_process = ServerProcess::spawn(
        &database_path,
        &capability_keys_path,
        &idempotency_keys_path,
    )?;
    let first_address = first_process.wait_for_ready_address()?;
    let mut first_client = connect(first_address).await?;

    let pre_bootstrap_health = bounded_rpc(
        "pre-bootstrap Health",
        first_client.health(
            v1::HealthRequest { request_id: None },
            &CallMetadata::default(),
        ),
    )
    .await?;
    assert_pre_bootstrap_health(&pre_bootstrap_health)?;

    let bootstrap_created = bounded_rpc(
        "bootstrap capability creation",
        first_client.create_bootstrap_capability(
            bootstrap_request(&retained_bootstrap)?,
            &bootstrap_metadata(&retained_bootstrap)?,
        ),
    )
    .await?;
    let bootstrap_transition = created_bootstrap_transition(bootstrap_created)?;

    assert_principal_less_health_closed(&mut first_client).await?;

    let deployment_health = bounded_rpc(
        "deployment-required Health",
        first_client.health(authenticated_health_request()?, &authenticated),
    )
    .await?;
    assert_authenticated_health(&deployment_health, v1::HealthStatus::NotReady, None, None)?;

    let deployment = bounded_rpc(
        "budget contract deployment",
        first_client.deploy_contract(
            v1::DeployContractRequest {
                request_id: fresh_request_id_bytes()?,
                source: BUDGET_CONTRACT.to_owned(),
                expected_active_version: None,
            },
            &authenticated,
        ),
    )
    .await?;
    assert_activated_budget_contract(deployment)?;

    let ready_health = bounded_rpc(
        "ready Health",
        first_client.health(authenticated_health_request()?, &authenticated),
    )
    .await?;
    assert_authenticated_health(
        &ready_health,
        v1::HealthStatus::Ready,
        Some(CONTRACT_VERSION),
        None,
    )?;

    let created = bounded_rpc(
        "CreateBudget",
        first_client.execute_generated(&create, one_attempt(), &authenticated),
    )
    .await?;
    assert_eq!(
        created.response().status,
        v1::execute_command_response::CompletionStatus::Committed as i32
    );
    assert_eq!(created.response().commit_sequence, 1);
    assert_eq!(created.response().durability_mode, "sync");
    let CreateBudgetOutcome::BudgetCreated { budget } = created.outcome() else {
        return Err(test_failure("CreateBudget did not return BudgetCreated"));
    };
    assert_eq!(budget.organization_id, ORGANIZATION_ID);
    assert_eq!(budget.fiscal_year, FISCAL_YEAR);
    assert_eq!(budget.approved_amount.minor_units(), APPROVED_MINOR_UNITS);
    assert_eq!(budget.allocated_amount.minor_units(), 0);

    let entity_after_create = found_entity(
        bounded_rpc(
            "entity read after CreateBudget",
            first_client.get_entity(entity_request(&entity_key)?, &authenticated),
        )
        .await?,
    )?;
    assert_budget_entity(&entity_after_create, &entity_key, 1, 0)?;

    let allocated = bounded_rpc(
        "AllocateBudget",
        first_client.execute_generated(&allocate, one_attempt(), &authenticated),
    )
    .await?;
    assert_eq!(
        allocated.response().status,
        v1::execute_command_response::CompletionStatus::Committed as i32
    );
    assert_eq!(allocated.response().commit_sequence, 2);
    assert_eq!(allocated.response().durability_mode, "sync");
    let AllocateBudgetOutcome::Allocated { budget, remaining } = allocated.outcome() else {
        return Err(test_failure("AllocateBudget did not return Allocated"));
    };
    assert_eq!(budget.allocated_amount.minor_units(), ALLOCATED_MINOR_UNITS);
    assert_eq!(
        remaining.minor_units(),
        APPROVED_MINOR_UNITS - ALLOCATED_MINOR_UNITS
    );

    let entity_after_allocate = found_entity(
        bounded_rpc(
            "entity read after AllocateBudget",
            first_client.get_entity(entity_request(&entity_key)?, &authenticated),
        )
        .await?,
    )?;
    assert_budget_entity(
        &entity_after_allocate,
        &entity_key,
        2,
        ALLOCATED_MINOR_UNITS,
    )?;
    assert_ne!(entity_after_create, entity_after_allocate);

    let outcome_before_restart = found_outcome(
        bounded_rpc(
            "outcome resolution before restart",
            first_client.get_outcome(
                allocate.outcome_request(generate_request_id()?)?,
                &authenticated,
            ),
        )
        .await?,
    )?;
    assert_eq!(
        outcome_before_restart.status,
        v1::execute_command_response::CompletionStatus::Replayed as i32
    );
    assert_eq!(
        allocate.decode_outcome(&outcome_before_restart)?,
        AllocateBudgetOutcome::Allocated {
            budget: *budget,
            remaining: *remaining,
        }
    );

    drop(first_client);
    first_process.shutdown_cleanly()?;

    let mut second_process = ServerProcess::spawn(
        &database_path,
        &capability_keys_path,
        &idempotency_keys_path,
    )?;
    let second_address = second_process.wait_for_ready_address()?;
    let mut second_client = connect(second_address).await?;

    assert_principal_less_health_closed(&mut second_client).await?;

    let reopened_health = bounded_rpc(
        "Health after restart",
        second_client.health(authenticated_health_request()?, &authenticated),
    )
    .await?;
    assert_authenticated_health(
        &reopened_health,
        v1::HealthStatus::Ready,
        Some(CONTRACT_VERSION),
        Some(2),
    )?;

    let entity_after_restart = found_entity(
        bounded_rpc(
            "entity read after restart",
            second_client.get_entity(entity_request(&entity_key)?, &authenticated),
        )
        .await?,
    )?;
    assert_eq!(entity_after_restart, entity_after_allocate);

    let outcome_after_restart = found_outcome(
        bounded_rpc(
            "outcome resolution after restart",
            second_client.get_outcome(
                allocate.outcome_request(generate_request_id()?)?,
                &authenticated,
            ),
        )
        .await?,
    )?;
    assert_eq!(outcome_after_restart, outcome_before_restart);

    let bootstrap_replayed = bounded_rpc(
        "retained bootstrap replay after restart",
        second_client.create_bootstrap_capability(
            bootstrap_request(&retained_bootstrap)?,
            &bootstrap_metadata(&retained_bootstrap)?,
        ),
    )
    .await?;
    let replayed_transition = replayed_bootstrap_transition(bootstrap_replayed)?;
    assert_eq!(replayed_transition, bootstrap_transition);

    let live_subscription = bounded_rpc(
        "live commit subscription before clean shutdown",
        second_client.subscribe_commits(
            v1::SubscribeCommitsRequest {
                request_id: fresh_request_id_bytes()?,
                after_sequence: Some(2),
                maximum_lifetime_nanos: 300_000_000_000,
            },
            &authenticated,
        ),
    )
    .await?;
    second_process.shutdown_cleanly()?;
    drop(live_subscription);
    drop(second_client);
    Ok(())
}

async fn bounded_rpc<T, E>(
    label: &'static str,
    future: impl Future<Output = Result<T, E>>,
) -> TestResult<T>
where
    E: std::fmt::Display,
{
    match timeout(RPC_TIMEOUT, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(test_failure(format!("{label} failed: {error}"))),
        Err(_) => Err(test_failure(format!("{label} exceeded its deadline"))),
    }
}

async fn assert_principal_less_health_closed(client: &mut RiffDbClient) -> TestResult<()> {
    let metadata = CallMetadata::default();
    let call = client.health(v1::HealthRequest { request_id: None }, &metadata);
    match timeout(RPC_TIMEOUT, call).await {
        Ok(Err(ClientError::DetailsFree(DetailsFreeStatus::Unauthenticated))) => Ok(()),
        Ok(Ok(_)) => Err(test_failure(
            "principal-less Health remained available after bootstrap",
        )),
        Ok(Err(error)) => Err(test_failure(format!(
            "principal-less Health returned the wrong post-bootstrap failure: {error}"
        ))),
        Err(_) => Err(test_failure(
            "principal-less Health rejection exceeded its deadline",
        )),
    }
}

async fn connect(address: SocketAddr) -> TestResult<RiffDbClient> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(RPC_TIMEOUT)
        .timeout(RPC_TIMEOUT);
    bounded_rpc("gRPC connection", RiffDbClient::connect(endpoint)).await
}

fn bootstrap_request(
    credential: &RetainedBootstrapCredential,
) -> TestResult<v1::CreateCapabilityRequest> {
    use v1::capability_permission::Permission;

    let scoped = |stable_id| v1::LineageScopedStableId {
        contract_lineage: CONTRACT_LINEAGE.to_owned(),
        stable_id,
    };
    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Bootstrap as i32,
        capability_id: credential.capability_id().into_bytes().to_vec(),
        principal_id: "p1-maintainer".to_owned(),
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
                    permission: Some(Permission::InvokeCommand(scoped(1))),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::InvokeCommand(scoped(2))),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::ReadEntity(scoped(1))),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::SubscribeCommits(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::ReadHealth(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::AdministerCapabilities(v1::Unit {})),
                },
            ],
            field_visibility: vec![v1::EntityFieldVisibility {
                contract_lineage: CONTRACT_LINEAGE.to_owned(),
                entity_type_id: 1,
                field_ids: vec![1, 2, 3, 4, 5],
            }],
            max_scan_rows: 100,
            approval_required: Vec::new(),
        }),
    })
}

fn bearer_credential(credential: &RetainedBootstrapCredential) -> TestResult<BearerCredential> {
    BearerCredential::new(bootstrap_token_text(credential)?).map_err(Into::into)
}

fn bootstrap_metadata(
    credential: &RetainedBootstrapCredential,
) -> TestResult<BootstrapCallMetadata> {
    let transport = TransportBootstrapCredential::new(bootstrap_token_text(credential)?)?;
    Ok(BootstrapCallMetadata::new(transport))
}

fn bootstrap_token_text(credential: &RetainedBootstrapCredential) -> TestResult<&str> {
    str::from_utf8(credential.token().expose_secret()).map_err(Into::into)
}

fn created_bootstrap_transition(
    response: v1::CreateCapabilityResponse,
) -> TestResult<v1::CapabilityTransition> {
    let Some(v1::create_capability_response::Result::Bootstrap(result)) = response.result else {
        return Err(test_failure(
            "bootstrap response used the wrong result family",
        ));
    };
    let Some(v1::bootstrap_create_capability_result::Result::Created(transition)) = result.result
    else {
        return Err(test_failure(
            "initial bootstrap did not create the capability",
        ));
    };
    if transition.administration_sequence == 0 || transition.identity.is_none() {
        return Err(test_failure("bootstrap transition was incomplete"));
    }
    Ok(transition)
}

fn replayed_bootstrap_transition(
    response: v1::CreateCapabilityResponse,
) -> TestResult<v1::CapabilityTransition> {
    let Some(v1::create_capability_response::Result::Bootstrap(result)) = response.result else {
        return Err(test_failure(
            "bootstrap replay used the wrong result family",
        ));
    };
    let Some(v1::bootstrap_create_capability_result::Result::Replayed(transition)) = result.result
    else {
        return Err(test_failure("retained bootstrap did not replay"));
    };
    Ok(transition)
}

fn assert_pre_bootstrap_health(response: &v1::HealthResponse) -> TestResult<()> {
    let Some(v1::health_response::Result::PreBootstrap(report)) = response.result.as_ref() else {
        return Err(test_failure(
            "initial Health was not restricted pre-bootstrap Health",
        ));
    };
    if !report.liveness || report.readiness {
        return Err(test_failure(
            "pre-bootstrap Health reported an invalid state",
        ));
    }
    if v1::PreBootstrapLifecycle::try_from(report.lifecycle)
        != Ok(v1::PreBootstrapLifecycle::InitializingBootstrap)
    {
        return Err(test_failure(
            "ready pre-bootstrap Health was not in bootstrap admission",
        ));
    }
    Ok(())
}

fn assert_authenticated_health(
    response: &v1::HealthResponse,
    expected_status: v1::HealthStatus,
    expected_contract_version: Option<u64>,
    expected_last_commit_sequence: Option<u64>,
) -> TestResult<()> {
    let Some(v1::health_response::Result::Authenticated(report)) = response.result.as_ref() else {
        return Err(test_failure("Health did not use the authenticated result"));
    };
    if report.status != expected_status as i32
        || report.active_contract_version != expected_contract_version
        || report.last_commit_sequence != expected_last_commit_sequence
    {
        return Err(test_failure(
            "authenticated Health reported an unexpected state",
        ));
    }
    Ok(())
}

fn assert_activated_budget_contract(response: v1::DeployContractResponse) -> TestResult<()> {
    let Some(v1::deploy_contract_response::Result::Activated(contract)) = response.result else {
        return Err(test_failure("budget contract was not newly activated"));
    };
    if contract.contract_lineage != CONTRACT_LINEAGE
        || contract.contract_version != CONTRACT_VERSION
    {
        return Err(test_failure("activated contract identity was unexpected"));
    }
    Ok(())
}

fn authenticated_health_request() -> TestResult<v1::HealthRequest> {
    Ok(v1::HealthRequest {
        request_id: Some(fresh_request_id_bytes()?),
    })
}

fn entity_request(entity_key: &[u8]) -> TestResult<v1::GetEntityRequest> {
    Ok(v1::GetEntityRequest {
        request_id: fresh_request_id_bytes()?,
        contract: Some(v1::ContractSelection {
            selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
        }),
        entity_type_id: 1,
        entity_key: entity_key.to_vec(),
        fields: Some(v1::FieldSelection {
            field_ids: vec![1, 3, 5],
        }),
    })
}

fn found_entity(response: v1::GetEntityResponse) -> TestResult<v1::Entity> {
    match response.result {
        Some(v1::get_entity_response::Result::Found(entity)) => Ok(entity),
        _ => Err(test_failure("budget entity was not found")),
    }
}

fn assert_budget_entity(
    entity: &v1::Entity,
    expected_key: &[u8],
    expected_version: u64,
    expected_allocated_minor_units: i128,
) -> TestResult<()> {
    if entity.entity_key != expected_key
        || entity.entity_version != expected_version
        || entity.written_by_contract_version != CONTRACT_VERSION
    {
        return Err(test_failure(
            "budget entity identity or version was unexpected",
        ));
    }
    let record = entity
        .fields
        .as_ref()
        .ok_or_else(|| test_failure("budget entity omitted its selected fields"))?;
    let field_ids = record
        .fields
        .iter()
        .map(|field| field.field_id)
        .collect::<Vec<_>>();
    if field_ids != [Some(1), Some(3), Some(5)] {
        return Err(test_failure(
            "budget entity fields were not complete and canonical",
        ));
    }
    if decimal_minor_units(record, 3)? != APPROVED_MINOR_UNITS
        || decimal_minor_units(record, 5)? != expected_allocated_minor_units
    {
        return Err(test_failure("budget entity values were unexpected"));
    }
    if !matches!(
        value_field(record, 1)?.kind.as_ref(),
        Some(v1::value::Kind::TimestampValue(_))
    ) {
        return Err(test_failure(
            "budget entity update time was not a timestamp",
        ));
    }
    Ok(())
}

fn value_field(record: &v1::ValueRecord, field_id: u32) -> TestResult<&v1::Value> {
    record
        .fields
        .iter()
        .find(|field| field.field_id == Some(field_id))
        .and_then(|field| field.value.as_ref())
        .ok_or_else(|| test_failure("budget entity field was absent"))
}

fn decimal_minor_units(record: &v1::ValueRecord, field_id: u32) -> TestResult<i128> {
    let Some(v1::value::Kind::DecimalValue(value)) = value_field(record, field_id)?.kind.as_ref()
    else {
        return Err(test_failure("budget entity field was not decimal"));
    };
    let spec = DecimalSpec::new(28, 2)?;
    Ok(decimal_from_proto(value, spec)?.coefficient())
}

fn found_outcome(response: v1::GetOutcomeResponse) -> TestResult<v1::ExecuteCommandResponse> {
    match response.result {
        Some(v1::get_outcome_response::Result::Found(outcome)) => Ok(outcome),
        _ => Err(test_failure("durable command outcome was not found")),
    }
}

fn amount(minor_units: i128) -> TestResult<Amount> {
    Amount::from_minor_units(minor_units)
        .ok_or_else(|| test_failure("budget test amount exceeded the generated type"))
}

fn one_attempt() -> AttemptBudget {
    AttemptBudget::new(1).expect("one is a nonzero submission bound")
}

fn budget_entity_key() -> TestResult<Vec<u8>> {
    let entity_type = EntityTypeId::new(1).expect("Budget has frozen nonzero entity ID 1");
    let mut key = EntityKeyBuilder::new(entity_type);
    key.push_uuid(&ORGANIZATION_ID)?;
    key.push_i64(FISCAL_YEAR)?;
    Ok(key.finish()?.into_bytes())
}

fn fresh_request_id_bytes() -> TestResult<Vec<u8>> {
    Ok(generate_request_id()?.into_bytes().to_vec())
}

fn write_protected_file(path: &Path, document: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(document)?;
    file.sync_all()?;
    drop(file);
    File::open(
        path.parent()
            .ok_or_else(|| io::Error::other("secret path has no parent"))?,
    )?
    .sync_all()
}

fn test_failure(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn new() -> io::Result<Self> {
        static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

        for _ in 0..1_024 {
            let ordinal = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "riffdb-p1-restart-{}-{ordinal}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a unique P1 test directory",
        ))
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

enum ReaperCommand {
    Kill,
}

struct ServerProcess {
    stdin: Option<ChildStdin>,
    ready: Receiver<io::Result<String>>,
    reaper_commands: SyncSender<ReaperCommand>,
    exited: Receiver<io::Result<ExitStatus>>,
    reaper: Option<JoinHandle<()>>,
    stdout: Option<JoinHandle<usize>>,
    stderr: Option<JoinHandle<usize>>,
    exit_observed: bool,
}

impl ServerProcess {
    fn spawn(
        database_path: &Path,
        capability_keys_path: &Path,
        idempotency_keys_path: &Path,
    ) -> io::Result<Self> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_riffdbd"));
        command
            .arg("--database")
            .arg(database_path)
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
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("riffdbd stdin missing"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("riffdbd stdout missing"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("riffdbd stderr missing"))?;

        let (ready_sender, ready) = mpsc::sync_channel(1);
        let stdout = thread::spawn(move || read_ready_then_drain(stdout, ready_sender));
        let stderr = thread::spawn(move || drain_stream(stderr));
        let (reaper_commands, commands) = mpsc::sync_channel(1);
        let (exit_sender, exited) = mpsc::sync_channel(1);
        let reaper = thread::spawn(move || reap_child(child, commands, exit_sender));

        Ok(Self {
            stdin: Some(stdin),
            ready,
            reaper_commands,
            exited,
            reaper: Some(reaper),
            stdout: Some(stdout),
            stderr: Some(stderr),
            exit_observed: false,
        })
    }

    fn wait_for_ready_address(&self) -> TestResult<SocketAddr> {
        let line = match self.ready.recv_timeout(PROCESS_START_TIMEOUT) {
            Ok(result) => result?,
            Err(RecvTimeoutError::Timeout) => {
                return Err(test_failure("riffdbd readiness line timed out"));
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(test_failure("riffdbd readiness reader disconnected"));
            }
        };
        let address = line
            .strip_prefix(READY_PREFIX)
            .ok_or_else(|| test_failure("riffdbd emitted an unknown readiness line"))?
            .parse::<SocketAddr>()?;
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(test_failure(
                "riffdbd readiness address was not bound loopback",
            ));
        }
        Ok(address)
    }

    fn shutdown_cleanly(&mut self) -> TestResult<()> {
        let mut stdin = self
            .stdin
            .take()
            .ok_or_else(|| test_failure("riffdbd stdin was already closed"))?;
        stdin.write_all(SHUTDOWN_COMMAND)?;
        stdin.flush()?;
        drop(stdin);
        self.wait_for_successful_exit(PROCESS_STOP_TIMEOUT)
    }

    fn wait_for_successful_exit(&mut self, deadline: Duration) -> TestResult<()> {
        match self.exited.recv_timeout(deadline) {
            Ok(result) => {
                self.exit_observed = true;
                let (stdout_bytes, stderr_bytes) = self.join_threads()?;
                let status = result?;
                if !status.success() {
                    return Err(test_failure(format!(
                        "riffdbd exited with {status}; drained {stdout_bytes} stdout and {stderr_bytes} stderr bytes"
                    )));
                }
                Ok(())
            }
            Err(RecvTimeoutError::Timeout) => {
                let _ = self.reaper_commands.send(ReaperCommand::Kill);
                if self.exited.recv_timeout(PROCESS_KILL_TIMEOUT).is_ok() {
                    self.exit_observed = true;
                    let _ = self.join_threads();
                }
                Err(test_failure("riffdbd clean shutdown timed out"))
            }
            Err(RecvTimeoutError::Disconnected) => {
                Err(test_failure("riffdbd process reaper disconnected"))
            }
        }
    }

    fn join_threads(&mut self) -> TestResult<(usize, usize)> {
        if let Some(reaper) = self.reaper.take() {
            reaper
                .join()
                .map_err(|_| test_failure("riffdbd process reaper panicked"))?;
        }
        let stdout_bytes = self
            .stdout
            .take()
            .ok_or_else(|| test_failure("riffdbd stdout reader was already joined"))?
            .join()
            .map_err(|_| test_failure("riffdbd stdout reader panicked"))?;
        let stderr_bytes = self
            .stderr
            .take()
            .ok_or_else(|| test_failure("riffdbd stderr reader was already joined"))?
            .join()
            .map_err(|_| test_failure("riffdbd stderr reader panicked"))?;
        Ok((stdout_bytes, stderr_bytes))
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.stdin.take();
        if !self.exit_observed {
            let _ = self.reaper_commands.send(ReaperCommand::Kill);
            if self.exited.recv_timeout(PROCESS_KILL_TIMEOUT).is_ok() {
                self.exit_observed = true;
                let _ = self.join_threads();
            }
        }
    }
}

fn reap_child(
    mut child: Child,
    commands: Receiver<ReaperCommand>,
    exited: SyncSender<io::Result<ExitStatus>>,
) {
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let _ = exited.send(Ok(status));
                return;
            }
            Ok(None) => {}
            Err(error) => {
                let _ = exited.send(Err(error));
                return;
            }
        }
        match commands.recv_timeout(PROCESS_REAPER_POLL) {
            Ok(ReaperCommand::Kill) | Err(RecvTimeoutError::Disconnected) => {
                let _ = child.kill();
                let _ = exited.send(child.wait());
                return;
            }
            Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

fn read_ready_then_drain(stdout: ChildStdout, ready: SyncSender<io::Result<String>>) -> usize {
    let mut reader = BufReader::new(stdout);
    let line = read_bounded_line(&mut reader, MAX_READY_LINE_BYTES);
    let line_bytes = line.as_ref().map_or(0, String::len);
    let _ = ready.send(line);
    line_bytes.saturating_add(drain_reader(&mut reader))
}

fn read_bounded_line(reader: &mut impl Read, maximum: usize) -> io::Result<String> {
    let mut bytes = Vec::with_capacity(maximum.min(64));
    let mut byte = [0_u8; 1];
    loop {
        match reader.read(&mut byte)? {
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "readiness line missing",
                ));
            }
            1 if byte[0] == b'\n' => break,
            1 if bytes.len() < maximum => bytes.push(byte[0]),
            1 => {
                drain_through_newline(reader)?;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "readiness line exceeded its bound",
                ));
            }
            _ => unreachable!("one-byte read returned more than one byte"),
        }
    }
    String::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "readiness line was not UTF-8"))
}

fn drain_through_newline(reader: &mut impl Read) -> io::Result<()> {
    let mut byte = [0_u8; 1];
    loop {
        match reader.read(&mut byte)? {
            0 => return Ok(()),
            1 if byte[0] == b'\n' => return Ok(()),
            1 => {}
            _ => unreachable!("one-byte read returned more than one byte"),
        }
    }
}

fn drain_stream(stderr: ChildStderr) -> usize {
    drain_reader(&mut BufReader::new(stderr))
}

fn drain_reader(reader: &mut impl Read) -> usize {
    let mut total = 0_usize;
    let mut buffer = [0_u8; 1_024];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) | Err(_) => return total,
            Ok(read) => total = total.saturating_add(read),
        }
    }
}
