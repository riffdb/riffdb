#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Real-process WP-130 restart acceptance for the public Rust SDK and gRPC path.

use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{self, BufReader, Read, Write};
use std::net::SocketAddr;
use std::num::NonZeroU16;
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
use riffdb_catalog::{
    CatalogHistoryOutcome, ValidatedContractBundle, ValidatedReactiveModule,
    validate_catalog_history,
};
use riffdb_client_rust::generated::GeneratedCommand;
use riffdb_client_rust::generated::legal_spend::{
    AllocateBudget, AllocateBudgetOutcome, Amount, CONTRACT_LINEAGE, CONTRACT_VERSION,
    CreateBudget, CreateBudgetOutcome,
};
use riffdb_client_rust::{
    AttemptBudget, BearerCredential, BootstrapCallMetadata,
    BootstrapCredential as TransportBootstrapCredential, CallMetadata, ClientError, RiffDbClient,
    generate_capability_id, generate_request_id,
};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_errors::PublicErrorKind;
use riffdb_proto::app::v1 as app_v1;
use riffdb_proto::decimal_from_proto;
use riffdb_proto::v1;
use riffdb_storage_api::{
    AdministrationAuditReader, AdministrationAuditScan, AdministrationAuditScanRequest,
    EvidencePageLimit, OutboxClaimV1, OutboxDestinationIdV1, OutboxPageLimit, OutboxRepository,
    OutboxSucceedV1, OutboxTransitionResultV1, PendingOutboxScanV1,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs, StorageScanLimit, StoredAdministrationAuditRecordV1,
    StructuralEvidenceCursor, StructuralEvidenceOpen, StructuralEvidencePage,
    StructuralEvidenceSession, StructuralOpenOutcome,
};
use riffdb_storage_redb::{
    RedbDormantPorts, RedbOfflineRetention, RedbOperationalPorts, RedbStore,
    downgrade_all_index_rows_to_v1_fixture,
    read_validated_prefix_checkpoint_commit_sequence_fixture,
};
use riffdb_types::{
    AdministrationSequence, AggregateTypeId, DecimalSpec, DigestKeyId, EntityKey, EntityKeyBuilder,
    EntityTypeId, IndexEntryKeyBuilder, IndexId, PartitionKeyBuilder, ReactiveModuleHash,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceOperationV1, Timestamp,
};
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
const INDEXED_BUDGET_CONTRACT: &str = include_str!("fixtures/budget_indexed.riff");
const STREAMABLE_BUDGET_CONTRACT: &str = include_str!("fixtures/budget_streamable.riff");
const BUDGET_STREAM_MODULE: &str = include_str!("fixtures/budget_allocations.riffr");
const BUDGET_STREAM_OPERATION: &str = "BudgetAllocations";
const PRUNED_CONSUMER_NAME: &str = "pruned-history-consumer";
/// Daemon key IDs from the fixed key documents above (`7:` and `9:`).
const CAPABILITY_DIGEST_KEY_ID: u32 = 7;
const IDEMPOTENCY_DIGEST_KEY_ID: u32 = 9;

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
    assert_process_liveness(&pre_bootstrap_health)?;

    let bootstrap_created = bounded_rpc(
        "bootstrap capability creation",
        first_client.create_bootstrap_capability(
            bootstrap_request(&retained_bootstrap)?,
            &bootstrap_metadata(&retained_bootstrap)?,
        ),
    )
    .await?;
    let bootstrap_transition = created_bootstrap_transition(bootstrap_created)?;

    assert_principal_less_liveness(&mut first_client).await?;

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
                expected_active_bundle_hash: Vec::new(),
                expected_candidate_bundle_hash: Vec::new(),
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
    assert_authoritatively_ready_health(&ready_health, Some(CONTRACT_VERSION), None)?;

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
    assert_eq!(created.response().durability_mode, "group");
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
    assert_eq!(allocated.response().durability_mode, "group");
    assert_schema_bound_allocated_outcome(allocated.response())?;
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
    assert_schema_bound_allocated_outcome(&outcome_before_restart)?;
    assert_eq!(
        allocate.decode_outcome(&outcome_before_restart)?,
        AllocateBudgetOutcome::Allocated {
            budget: *budget,
            remaining: *remaining,
        }
    );

    drop(first_client);
    first_process.shutdown_cleanly()?;

    // ADR-0019 A1 write point (2): a real graceful daemon shutdown must leave a
    // durable validated-prefix checkpoint bound at the drained commit frontier
    // (S=2 after the two committed commands), and the next open must verify it
    // (fast path). The startup-finish checkpoint of this run was bound at S=0,
    // so the bound sequence discriminates the shutdown write: skipping or
    // vetoing it fails this assertion, never silently costing every future
    // open its fast path.
    assert_graceful_shutdown_checkpoint(&database_path, 2)?;

    let mut second_process = ServerProcess::spawn(
        &database_path,
        &capability_keys_path,
        &idempotency_keys_path,
    )?;
    let second_address = second_process.wait_for_ready_address()?;
    let mut second_client = connect(second_address).await?;

    assert_principal_less_liveness(&mut second_client).await?;

    let reopened_health = bounded_rpc(
        "Health after restart",
        second_client.health(authenticated_health_request()?, &authenticated),
    )
    .await?;
    assert_authenticated_health(
        &reopened_health,
        v1::HealthStatus::Degraded,
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

                observed_history_incarnation: None,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_riffdbd_migrates_v1_index_before_public_readiness() -> TestResult<()> {
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
    let root_metadata = CallMetadata::authenticated(bearer_credential(&retained_bootstrap)?);

    let entity_key = budget_entity_key()?;
    let create = CreateBudget {
        idempotency_key: "p1-create-indexed-budget".to_owned(),
        organization_id: ORGANIZATION_ID,
        fiscal_year: FISCAL_YEAR,
        approved_amount: amount(APPROVED_MINOR_UNITS)?,
    };

    let mut first_process = ServerProcess::spawn(
        &database_path,
        &capability_keys_path,
        &idempotency_keys_path,
    )?;
    let first_address = first_process.wait_for_ready_address()?;
    let mut first_client = connect(first_address).await?;

    let bootstrap_created = bounded_rpc(
        "indexed fixture bootstrap",
        first_client.create_bootstrap_capability(
            bootstrap_request(&retained_bootstrap)?,
            &bootstrap_metadata(&retained_bootstrap)?,
        ),
    )
    .await?;
    let _ = created_bootstrap_transition(bootstrap_created)?;

    let deployment = bounded_rpc(
        "indexed budget contract deployment",
        first_client.deploy_contract(
            v1::DeployContractRequest {
                request_id: fresh_request_id_bytes()?,
                source: INDEXED_BUDGET_CONTRACT.to_owned(),
                expected_active_version: None,
                expected_active_bundle_hash: Vec::new(),
                expected_candidate_bundle_hash: Vec::new(),
            },
            &root_metadata,
        ),
    )
    .await?;
    assert_activated_budget_contract(deployment)?;

    let created = bounded_rpc(
        "indexed CreateBudget",
        first_client.execute_with_retry(
            &create.idempotent_command()?,
            one_attempt(),
            &root_metadata,
        ),
    )
    .await?;
    assert_eq!(
        created.status,
        v1::execute_command_response::CompletionStatus::Committed as i32
    );
    assert_eq!(created.commit_sequence, 1);
    assert_eq!(created.contract_version, CONTRACT_VERSION);
    assert_eq!(created.durability_mode, "group");

    let restricted_token = normal_capability_token(
        bounded_rpc(
            "explicit partition capability creation",
            first_client
                .create_capability(explicit_partition_capability_request()?, &root_metadata),
        )
        .await?,
    )?;
    let restricted_metadata =
        CallMetadata::authenticated(BearerCredential::new(&restricted_token)?);

    let before_migration = found_entity(
        bounded_rpc(
            "indexed entity read before migration",
            first_client.get_entity(entity_request(&entity_key)?, &restricted_metadata),
        )
        .await?,
    )?;
    assert_budget_entity(&before_migration, &entity_key, 1, 0)?;

    drop(first_client);
    first_process.shutdown_cleanly()?;

    assert_eq!(
        downgrade_all_index_rows_to_v1_fixture(&database_path)?,
        1,
        "the stopped indexed workload must contain exactly one canonical V2 row"
    );

    let mut migrated_process = ServerProcess::spawn(
        &database_path,
        &capability_keys_path,
        &idempotency_keys_path,
    )?;
    let migrated_address = migrated_process.wait_for_ready_address()?;
    let mut migrated_client = connect(migrated_address).await?;

    assert_principal_less_liveness(&mut migrated_client).await?;
    let health = bounded_rpc(
        "Health after V1 index migration",
        migrated_client.health(authenticated_health_request()?, &restricted_metadata),
    )
    .await?;
    assert_authoritatively_ready_health(&health, Some(CONTRACT_VERSION), Some(1))?;

    let scan = bounded_rpc(
        "partition-filtered scan after V1 index migration",
        migrated_client.scan_index(index_scan_request()?, &restricted_metadata),
    )
    .await?;
    assert_migrated_index_scan(&scan, &entity_key)?;

    let after_migration = found_entity(
        bounded_rpc(
            "entity read after V1 index migration",
            migrated_client.get_entity(entity_request(&entity_key)?, &restricted_metadata),
        )
        .await?,
    )?;
    assert_eq!(after_migration, before_migration);

    migrated_process.shutdown_cleanly()?;
    drop(migrated_client);
    Ok(())
}

#[test]
fn streamable_budget_fixture_compiles_and_proves_application_routing() {
    // Pins the fixture pair this file's pruned-history test deploys through
    // the real daemon: the contract compiles with a compiler-proved event
    // partition, and the reactive stream module compiles against it.
    let contract = compile_contract_source(STREAMABLE_BUDGET_CONTRACT)
        .expect("streamable budget contract compiles");
    let checked = ValidatedContractBundle::from_compiler_bundle(contract)
        .expect("streamable budget contract validates");
    let module = ValidatedReactiveModule::compile(BUDGET_STREAM_MODULE, &checked, &[])
        .expect("budget allocations stream module compiles");
    let _ = module.identity();
}

// A successful DeployReactiveModule publication must be able to record its OWN
// terminal audit record through the daemon.
//
// Regression (confirmed production defect): riffdb-storage-api's
// `validate_service_audit_phase_link` omitted `DeployReactiveModule` from its
// Succeeded/ControlPlane arm, so the coordinator's own Succeeded audit append
// failed `InvalidShape`. The module was already durably published, but the failed
// Succeeded finish mapped to `PublicError::outcome_unknown()` and the
// audit-unavailable readiness failure degraded the daemon. Falsifiability:
// removing `DeployReactiveModule` from that arm again turns the SUCCESS
// assertion below into an outcome_unknown failure, and the audit assertions
// below find no Succeeded record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_riffdbd_publishes_a_reactive_module_and_records_its_own_success_audit()
-> TestResult<()> {
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
    let root_metadata = CallMetadata::authenticated(bearer_credential(&retained_bootstrap)?);

    let mut process = ServerProcess::spawn(
        &database_path,
        &capability_keys_path,
        &idempotency_keys_path,
    )?;
    let address = process.wait_for_ready_address()?;
    let mut client = connect(address).await?;

    let bootstrap_created = bounded_rpc(
        "reactive publication bootstrap",
        client.create_bootstrap_capability(
            pruned_history_bootstrap_request(&retained_bootstrap)?,
            &bootstrap_metadata(&retained_bootstrap)?,
        ),
    )
    .await?;
    let _ = created_bootstrap_transition(bootstrap_created)?;

    let deployment = bounded_rpc(
        "streamable budget contract deployment",
        client.deploy_contract(
            v1::DeployContractRequest {
                request_id: fresh_request_id_bytes()?,
                source: STREAMABLE_BUDGET_CONTRACT.to_owned(),
                expected_active_version: None,
                expected_active_bundle_hash: Vec::new(),
                expected_candidate_bundle_hash: Vec::new(),
            },
            &root_metadata,
        ),
    )
    .await?;
    let Some(v1::deploy_contract_response::Result::Activated(active)) = deployment.result else {
        return Err(test_failure(
            "the streamable budget contract was not newly activated",
        ));
    };

    // The one RPC under test. A rejected Succeeded audit record cannot be
    // distinguished from a lost write by the caller, so it surfaces as the typed
    // outcome_unknown public error rather than a transport fault.
    let publication = match timeout(
        RPC_TIMEOUT,
        client.deploy_reactive_module(
            app_v1::DeployReactiveModuleRequest {
                contract: Some(app_v1::ContractSelector {
                    lineage: active.contract_lineage.clone(),
                    version: active.contract_version,
                    bundle_hash: active.bundle_hash.clone(),
                }),
                source: BUDGET_STREAM_MODULE.to_owned(),
                query_module_hashes: Vec::new(),
                request_id: fresh_request_id_bytes()?,
            },
            &root_metadata,
        ),
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            return Err(test_failure(format!(
                "DeployReactiveModule must report SUCCESS over the real daemon, got {error:?} \
                 (outcome_unknown here means the publication's own Succeeded audit record \
                 was rejected by the storage-side phase/link validator)"
            )));
        }
        Err(_) => return Err(test_failure("DeployReactiveModule exceeded its deadline")),
    };
    assert_eq!(
        publication.outcome,
        app_v1::ReactiveModuleDeploymentOutcome::Published as i32,
        "a first publication of the budget stream module must publish"
    );
    assert_eq!(publication.unavailable_query_module_hash, None);
    let descriptor = publication
        .module
        .ok_or_else(|| test_failure("a published module must return its descriptor"))?;
    assert_eq!(descriptor.contract_lineage, CONTRACT_LINEAGE);
    assert_eq!(descriptor.contract_version, CONTRACT_VERSION);
    assert_eq!(
        descriptor.operation_names,
        vec![BUDGET_STREAM_OPERATION.to_owned()]
    );
    let module_hash: [u8; 32] = descriptor
        .module_hash
        .clone()
        .try_into()
        .map_err(|_| test_failure("a module hash is exactly 32 bytes"))?;

    drop(client);
    process.shutdown_cleanly()?;

    // The durable proof: the daemon wrote BOTH lifecycle records for the
    // publication, and the terminal one names the authoritative transition.
    let records = reactive_publication_service_audits(&database_path);
    let started = records
        .iter()
        .filter(|(phase, _)| *phase == ServiceAuditPhaseV1::Started)
        .count();
    assert_eq!(
        started, 1,
        "the publication must record exactly one authenticated Started record"
    );
    let terminals = records
        .iter()
        .filter(|(phase, _)| *phase != ServiceAuditPhaseV1::Started)
        .collect::<Vec<_>>();
    let [(phase, link)] = terminals.as_slice() else {
        return Err(test_failure(format!(
            "the publication must record exactly one terminal audit record, got {terminals:?}"
        )));
    };
    assert_eq!(
        *phase,
        ServiceAuditPhaseV1::Succeeded,
        "a published module must record Succeeded, never a failure or uncertainty"
    );
    let ServiceAuditLinkV1::ControlPlane {
        administration_sequence,
    } = *link
    else {
        return Err(test_failure(format!(
            "a published module's success must name its administration transition, got {link:?}"
        )));
    };
    assert_eq!(
        reactive_administration_module_hash(&database_path, administration_sequence),
        Some(ReactiveModuleHash::from_bytes(module_hash)),
        "the linked administration sequence must name the exact published module"
    );
    Ok(())
}

/// Reads every `DeployReactiveModule` service-audit phase and link from the
/// stopped daemon database, in administration order.
fn reactive_publication_service_audits(
    database_path: &Path,
) -> Vec<(ServiceAuditPhaseV1, ServiceAuditLinkV1)> {
    scan_administration_audit_records(database_path)
        .into_iter()
        .filter_map(|record| match record {
            StoredAdministrationAuditRecordV1::Service(service)
                if service.operation() == ServiceOperationV1::DeployReactiveModule =>
            {
                Some((service.phase(), service.link()))
            }
            _ => None,
        })
        .collect()
}

/// Returns the module published by the reactive administration record at the
/// exact shared administration sequence, if that sequence names one.
fn reactive_administration_module_hash(
    database_path: &Path,
    administration_sequence: AdministrationSequence,
) -> Option<ReactiveModuleHash> {
    scan_administration_audit_records(database_path)
        .into_iter()
        .find_map(|record| match record {
            StoredAdministrationAuditRecordV1::ReactiveModule(published)
                if published.administration_sequence() == administration_sequence =>
            {
                Some(published.module_hash())
            }
            _ => None,
        })
}

// An idempotent REPUBLISH of an already published reactive module is a SUCCESS
// over the real daemon, and it must record one. Before this fix the
// already-published storage result carried no administration sequence, so the
// coordinator produced `transition_sequence: None`, the terminal audit degraded
// to `ControlPlaneTerminalAudit::Failed`, and a successful RPC durably wrote a
// **Failed** audit record -- while the catalog and query-module already-active
// arms recorded success from their original activation. Falsifiability: putting
// the arm back to sequence-less turns the terminal assertions below into a
// Failed/None record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_riffdbd_republishes_a_reactive_module_as_success_linking_the_original()
-> TestResult<()> {
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
    let root_metadata = CallMetadata::authenticated(bearer_credential(&retained_bootstrap)?);

    let mut process = ServerProcess::spawn(
        &database_path,
        &capability_keys_path,
        &idempotency_keys_path,
    )?;
    let address = process.wait_for_ready_address()?;
    let mut client = connect(address).await?;

    let bootstrap_created = bounded_rpc(
        "reactive republication bootstrap",
        client.create_bootstrap_capability(
            pruned_history_bootstrap_request(&retained_bootstrap)?,
            &bootstrap_metadata(&retained_bootstrap)?,
        ),
    )
    .await?;
    let _ = created_bootstrap_transition(bootstrap_created)?;

    let deployment = bounded_rpc(
        "streamable budget contract deployment",
        client.deploy_contract(
            v1::DeployContractRequest {
                request_id: fresh_request_id_bytes()?,
                source: STREAMABLE_BUDGET_CONTRACT.to_owned(),
                expected_active_version: None,
                expected_active_bundle_hash: Vec::new(),
                expected_candidate_bundle_hash: Vec::new(),
            },
            &root_metadata,
        ),
    )
    .await?;
    let Some(v1::deploy_contract_response::Result::Activated(active)) = deployment.result else {
        return Err(test_failure(
            "the streamable budget contract was not newly activated",
        ));
    };
    let contract = app_v1::ContractSelector {
        lineage: active.contract_lineage.clone(),
        version: active.contract_version,
        bundle_hash: active.bundle_hash.clone(),
    };

    let first = bounded_rpc(
        "first reactive publication",
        client.deploy_reactive_module(
            app_v1::DeployReactiveModuleRequest {
                contract: Some(contract.clone()),
                source: BUDGET_STREAM_MODULE.to_owned(),
                query_module_hashes: Vec::new(),
                request_id: fresh_request_id_bytes()?,
            },
            &root_metadata,
        ),
    )
    .await?;
    assert_eq!(
        first.outcome,
        app_v1::ReactiveModuleDeploymentOutcome::Published as i32,
        "the first publication of the budget stream module must publish"
    );
    let first_descriptor = first
        .module
        .ok_or_else(|| test_failure("a published module must return its descriptor"))?;

    // The RPC under test: the identical module, a fresh request identity. No
    // durable module write happens, yet the invocation succeeded.
    let republication = match timeout(
        RPC_TIMEOUT,
        client.deploy_reactive_module(
            app_v1::DeployReactiveModuleRequest {
                contract: Some(contract),
                source: BUDGET_STREAM_MODULE.to_owned(),
                query_module_hashes: Vec::new(),
                request_id: fresh_request_id_bytes()?,
            },
            &root_metadata,
        ),
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            return Err(test_failure(format!(
                "an idempotent republish must report ALREADY_PUBLISHED over the real \
                 daemon, got {error:?}"
            )));
        }
        Err(_) => {
            return Err(test_failure(
                "the reactive republication exceeded its deadline",
            ));
        }
    };
    assert_eq!(
        republication.outcome,
        app_v1::ReactiveModuleDeploymentOutcome::AlreadyPublished as i32,
        "republishing the exact same module must report ALREADY_PUBLISHED"
    );
    assert_eq!(republication.unavailable_query_module_hash, None);
    let republished_descriptor = republication
        .module
        .ok_or_else(|| test_failure("a republished module must return its descriptor"))?;
    assert_eq!(
        republished_descriptor, first_descriptor,
        "an idempotent republish must describe the exact same module"
    );
    assert_eq!(republished_descriptor.contract_lineage, CONTRACT_LINEAGE);
    assert_eq!(republished_descriptor.contract_version, CONTRACT_VERSION);
    assert_eq!(
        republished_descriptor.operation_names,
        vec![BUDGET_STREAM_OPERATION.to_owned()]
    );
    let module_hash: [u8; 32] = republished_descriptor
        .module_hash
        .clone()
        .try_into()
        .map_err(|_| test_failure("a module hash is exactly 32 bytes"))?;
    let module_hash = ReactiveModuleHash::from_bytes(module_hash);

    drop(client);
    process.shutdown_cleanly()?;

    // The durable proof. Exactly ONE publication record exists -- the republish
    // wrote nothing -- and BOTH invocations recorded a Succeeded terminal whose
    // control-plane link names that one publication.
    let publications = scan_administration_audit_records(&database_path)
        .into_iter()
        .filter_map(|record| match record {
            StoredAdministrationAuditRecordV1::ReactiveModule(published) => Some(published),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [publication] = publications.as_slice() else {
        return Err(test_failure(format!(
            "an idempotent republish must write no second publication record, got \
             {publications:?}"
        )));
    };
    assert_eq!(publication.module_hash(), module_hash);
    let original = publication.administration_sequence();

    let records = reactive_publication_service_audits(&database_path);
    let started = records
        .iter()
        .filter(|(phase, _)| *phase == ServiceAuditPhaseV1::Started)
        .count();
    assert_eq!(
        started, 2,
        "each publication attempt must record its own authenticated Started record"
    );
    let terminals = records
        .iter()
        .filter(|(phase, _)| *phase != ServiceAuditPhaseV1::Started)
        .collect::<Vec<_>>();
    let [
        (publish_phase, publish_link),
        (republish_phase, republish_link),
    ] = terminals.as_slice()
    else {
        return Err(test_failure(format!(
            "two attempts must record exactly two terminal records, got {terminals:?}"
        )));
    };
    assert_eq!(
        *publish_phase,
        ServiceAuditPhaseV1::Succeeded,
        "the first publication must record Succeeded"
    );
    assert_eq!(
        *republish_phase,
        ServiceAuditPhaseV1::Succeeded,
        "an idempotent republish is a SUCCESS and must never record a failure"
    );
    let expected_link = ServiceAuditLinkV1::ControlPlane {
        administration_sequence: original,
    };
    assert_eq!(
        *publish_link, expected_link,
        "the publication's success must name its own transition"
    );
    assert_eq!(
        *republish_link, expected_link,
        "the republish's success must name the ORIGINAL publication's transition"
    );
    assert_eq!(
        reactive_administration_module_hash(&database_path, original),
        Some(module_hash),
        "the linked administration sequence must name the exact published module"
    );

    // The linked record is older than the invocation that names it, exactly as a
    // catalog or query-module replay success is. The startup structural pass must
    // still validate the stopped database clean.
    let mut session = RedbStore::open(&database_path)
        .expect("reopen the stopped daemon database")
        .begin_structural_evidence(daemon_startup_inputs())
        .expect("begin structural evidence");
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let mut findings = Vec::new();
    while let StructuralEvidencePage::Page {
        findings: page,
        next,
        ..
    } = session
        .read_structural_evidence(cursor, EvidencePageLimit::new(256).expect("page limit"))
        .expect("structural page")
    {
        findings.extend(page);
        cursor = next;
    }
    assert!(
        findings.is_empty(),
        "a republished module's linked success must validate clean, got {findings:?}"
    );
    Ok(())
}

/// Scans the complete shared administration stream off a stopped database.
fn scan_administration_audit_records(
    database_path: &Path,
) -> Vec<StoredAdministrationAuditRecordV1> {
    let ports = open_operational_offline(database_path);
    let limit = StorageScanLimit::new(64).expect("bounded administration scan limit");
    let mut collected = Vec::new();
    let mut after = None;
    loop {
        match ports
            .scan_administration_audit(AdministrationAuditScanRequest::new(after, limit))
            .expect("scan the shared administration stream")
        {
            AdministrationAuditScan::Page {
                records,
                next_after,
            } => {
                collected.extend(records.into_iter().map(|item| item.into_parts().0));
                after = Some(next_after);
            }
            AdministrationAuditScan::ExactEnd { records } => {
                collected.extend(records.into_iter().map(|item| item.into_parts().0));
                break;
            }
        }
    }
    collected
}

// Covers RT-A/RT-B owed tail: end-to-end typed pruned reads over a REAL pruned
// database through the composed daemon (service + server read adapters + gRPC).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_riffdbd_serves_typed_history_pruned_replay_and_consumer_after_offline_prune()
-> TestResult<()> {
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
    let root_metadata = CallMetadata::authenticated(bearer_credential(&retained_bootstrap)?);

    // ---- Process A: build REAL event history through the daemon. ----
    let mut first_process = ServerProcess::spawn(
        &database_path,
        &capability_keys_path,
        &idempotency_keys_path,
    )?;
    let first_address = first_process.wait_for_ready_address()?;
    let mut first_client = connect(first_address).await?;

    let bootstrap_created = bounded_rpc(
        "pruned-history bootstrap",
        first_client.create_bootstrap_capability(
            pruned_history_bootstrap_request(&retained_bootstrap)?,
            &bootstrap_metadata(&retained_bootstrap)?,
        ),
    )
    .await?;
    let _ = created_bootstrap_transition(bootstrap_created)?;

    let deployment = bounded_rpc(
        "streamable budget contract deployment",
        first_client.deploy_contract(
            v1::DeployContractRequest {
                request_id: fresh_request_id_bytes()?,
                source: STREAMABLE_BUDGET_CONTRACT.to_owned(),
                expected_active_version: None,
                expected_active_bundle_hash: Vec::new(),
                expected_candidate_bundle_hash: Vec::new(),
            },
            &root_metadata,
        ),
    )
    .await?;
    let Some(v1::deploy_contract_response::Result::Activated(active_contract)) = deployment.result
    else {
        return Err(test_failure(
            "the streamable budget contract was not newly activated",
        ));
    };

    // Commit sequence 1 (no event), then 2/3/4 (one BudgetAllocated each).
    // The streamable contract's plans differ from the generated bindings'
    // pinned plan hashes (the event carries a compiler-proved partition), so
    // commands run through the generic transport envelope without generated
    // outcome decoding — the same pattern as the indexed-contract test above.
    let create = CreateBudget {
        idempotency_key: "p1-pruned-create-budget".to_owned(),
        organization_id: ORGANIZATION_ID,
        fiscal_year: FISCAL_YEAR,
        approved_amount: amount(APPROVED_MINOR_UNITS)?,
    };
    let created = bounded_rpc(
        "pruned-history CreateBudget",
        first_client.execute_with_retry(
            &create.idempotent_command()?,
            one_attempt(),
            &root_metadata,
        ),
    )
    .await?;
    assert_eq!(
        created.status,
        v1::execute_command_response::CompletionStatus::Committed as i32
    );
    assert_eq!(created.commit_sequence, 1);
    for (ordinal, expected_sequence) in [(1_u8, 2_u64), (2, 3), (3, 4)] {
        let mut matter_id = MATTER_ID;
        matter_id[0] = ordinal;
        let allocate = AllocateBudget {
            idempotency_key: format!("p1-pruned-allocate-{ordinal}"),
            organization_id: ORGANIZATION_ID,
            fiscal_year: FISCAL_YEAR,
            matter_id,
            amount: amount(ALLOCATED_MINOR_UNITS)?,
        };
        let allocated = bounded_rpc(
            "pruned-history AllocateBudget",
            first_client.execute_with_retry(
                &allocate.idempotent_command()?,
                one_attempt(),
                &root_metadata,
            ),
        )
        .await?;
        assert_eq!(
            allocated.status,
            v1::execute_command_response::CompletionStatus::Committed as i32
        );
        assert_eq!(allocated.commit_sequence, expected_sequence);
    }

    // Baseline replay over live history: exactly the three allocation events.
    let baseline = replayed_event_page(
        bounded_rpc(
            "baseline event replay before prune",
            first_client.replay_events(budget_replay_request(None)?, &root_metadata),
        )
        .await?,
    )?;
    assert_eq!(
        replayed_commit_sequences(&baseline)?,
        vec![2, 3, 4],
        "live history must replay one BudgetAllocated per allocation commit"
    );
    let boundary_event_id = baseline.items[0]
        .event_id
        .ok_or_else(|| test_failure("baseline replay item omitted its event ID"))?;

    // The reactive stream module is published through the daemon's own
    // DeployReactiveModule RPC, over the live process, before shutdown. This
    // path used to be unusable: the storage-side phase/link validators omitted
    // DeployReactiveModule, so the publication's Succeeded audit record was
    // rejected and the RPC reported outcome_unknown for an already durable
    // module. This test previously routed around it by publishing offline
    // through the storage repository.
    let publication = bounded_rpc(
        "pruned-history stream module publication",
        first_client.deploy_reactive_module(
            app_v1::DeployReactiveModuleRequest {
                contract: Some(app_v1::ContractSelector {
                    lineage: active_contract.contract_lineage.clone(),
                    version: active_contract.contract_version,
                    bundle_hash: active_contract.bundle_hash.clone(),
                }),
                source: BUDGET_STREAM_MODULE.to_owned(),
                query_module_hashes: Vec::new(),
                request_id: fresh_request_id_bytes()?,
            },
            &root_metadata,
        ),
    )
    .await?;
    assert_eq!(
        publication.outcome,
        app_v1::ReactiveModuleDeploymentOutcome::Published as i32,
        "the stream module must publish through the live daemon"
    );
    let module_hash = publication
        .module
        .ok_or_else(|| test_failure("a published module must return its descriptor"))?
        .module_hash;

    drop(first_client);
    first_process.shutdown_cleanly()?;

    // ---- Offline: deliver the outbox, obtain fencing, prune sequences 1-2. ----
    // Undelivered outbox intents fence retention (fail toward NOT deleting),
    // so the operator flow drains them through the real claim/succeed
    // transitions before the watermark may cover their commits.
    let delivered = drain_outbox_offline(&database_path);
    assert_eq!(
        delivered, 3,
        "each allocation event must carry exactly one pending outbox intent"
    );
    let retention = RedbOfflineRetention::bind(&database_path);
    let status = retention
        .status()
        .map_err(|error| test_failure(format!("retention status failed: {error:?}")))?;
    assert_eq!(
        status.max_permissible_watermark,
        Some(4),
        "with the outbox drained and no projections, fencing must permit the durable head"
    );
    let status = retention
        .prune_to(2)
        .map_err(|error| test_failure(format!("offline prune failed: {error:?}")))?;
    assert_eq!(status.watermark_sequence, 2);
    assert!(status.tombstone_count >= 1);

    // ---- Process B: the SAME pruned database serves through the daemon. ----
    let mut second_process = ServerProcess::spawn(
        &database_path,
        &capability_keys_path,
        &idempotency_keys_path,
    )?;
    let second_address = second_process.wait_for_ready_address()?;
    let mut second_client = connect(second_address).await?;

    // The consumer capability binds the published module hash; created through
    // the restarted daemon so consumption exercises the full live wire path.
    let consumer_capability = match timeout(
        RPC_TIMEOUT,
        second_client.create_capability(consumer_capability_request(&module_hash)?, &root_metadata),
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            return Err(test_failure(format!(
                "stream consumer capability creation failed: {error:?}"
            )));
        }
        Err(_) => return Err(test_failure("capability creation exceeded its deadline")),
    };
    let consumer_token = normal_capability_token(consumer_capability)?;
    let consumer_metadata = CallMetadata::authenticated(BearerCredential::new(&consumer_token)?);

    // A replay window resolving below the watermark yields the TYPED
    // history_pruned public error (RDB-HISTORY-0102) — never integrity or an
    // internal defect. Falsifiability: re-neutering the server read-adapter
    // mapping (Storage(HistoryPruned) -> Integrity) fails THIS assertion.
    let pruned_replay = bounded_rpc_error(
        "below-watermark event replay",
        second_client.replay_events(budget_replay_request(None)?, &root_metadata),
    )
    .await?;
    assert_history_pruned(pruned_replay, "below-watermark replay")?;

    // A window entirely above the watermark serves normally on the SAME
    // database: exactly the baseline tail, byte-for-byte.
    let retained = replayed_event_page(
        bounded_rpc(
            "above-watermark event replay",
            second_client.replay_events(
                budget_replay_request(Some(boundary_event_id))?,
                &root_metadata,
            ),
        )
        .await?,
    )?;
    assert_eq!(
        retained.items,
        baseline.items[1..],
        "retained events above the watermark must serve unchanged after the prune"
    );

    // F4: a consumer registered AFTER the prune resolves its first window from
    // the stream start — below the watermark. That is a correct-request client
    // outcome (typed history_pruned), never an internal defect.
    let pruned_consume = bounded_rpc_error(
        "below-watermark consumer window",
        second_client
            .consume_event_stream(consume_stream_request(&module_hash)?, &consumer_metadata),
    )
    .await?;
    assert_history_pruned(pruned_consume, "below-watermark consumer window")?;

    // The typed outcomes are client errors: the daemon stays healthy and keeps
    // serving retained history afterwards.
    let after_errors = replayed_event_page(
        bounded_rpc(
            "post-error event replay",
            second_client.replay_events(
                budget_replay_request(Some(boundary_event_id))?,
                &root_metadata,
            ),
        )
        .await?,
    )?;
    assert_eq!(after_errors.items, baseline.items[1..]);

    second_process.shutdown_cleanly()?;
    drop(second_client);
    Ok(())
}

/// Awaits an RPC that MUST fail, returning its typed client error.
async fn bounded_rpc_error<T>(
    label: &'static str,
    future: impl Future<Output = Result<T, ClientError>>,
) -> TestResult<ClientError> {
    match timeout(RPC_TIMEOUT, future).await {
        Ok(Err(error)) => Ok(error),
        Ok(Ok(_)) => Err(test_failure(format!("{label} unexpectedly succeeded"))),
        Err(_) => Err(test_failure(format!("{label} exceeded its deadline"))),
    }
}

fn assert_history_pruned(error: ClientError, label: &str) -> TestResult<()> {
    match error {
        ClientError::Public(public) if public.kind() == PublicErrorKind::HistoryPruned => Ok(()),
        other => Err(test_failure(format!(
            "{label} must surface the typed history_pruned public error \
             (RDB-HISTORY-0102), got {other:?}"
        ))),
    }
}

fn pruned_history_bootstrap_request(
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
        principal_id: "p1-pruned-maintainer".to_owned(),
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
                // ReplayEvents/TailEvents are operator reads gated on ReadCommit.
                v1::CapabilityPermission {
                    permission: Some(Permission::ReadCommit(v1::Unit {})),
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
            row_policy: None,
        }),
    })
}

fn consumer_capability_request(module_hash: &[u8]) -> TestResult<v1::CreateCapabilityRequest> {
    use v1::capability_permission::Permission;

    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Normal as i32,
        capability_id: generate_capability_id()?.into_bytes().to_vec(),
        principal_id: "p1-pruned-consumer".to_owned(),
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
            // Canonical permission order: ReadHealth (15) precedes
            // ConsumeEventStream (27).
            permissions: vec![
                v1::CapabilityPermission {
                    permission: Some(Permission::ReadHealth(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::ConsumeEventStream(
                        v1::ReactiveOperationPermission {
                            contract_lineage: CONTRACT_LINEAGE.to_owned(),
                            reactive_module_hash: module_hash.to_vec(),
                            operation_name: BUDGET_STREAM_OPERATION.to_owned(),
                        },
                    )),
                },
            ],
            field_visibility: vec![v1::EntityFieldVisibility {
                contract_lineage: CONTRACT_LINEAGE.to_owned(),
                entity_type_id: 1,
                field_ids: vec![1, 2, 3, 4, 5],
            }],
            max_scan_rows: 100,
            approval_required: Vec::new(),
            row_policy: None,
        }),
    })
}

fn organization_uuid_value() -> v1::Value {
    v1::Value {
        kind: Some(v1::value::Kind::UuidValue(ORGANIZATION_ID.to_vec())),
    }
}

fn budget_replay_request(after: Option<v1::EventId>) -> TestResult<v1::ReplayEventsRequest> {
    Ok(v1::ReplayEventsRequest {
        request_id: fresh_request_id_bytes()?,
        selection: Some(v1::EventSelection {
            event_name: "BudgetAllocated".to_owned(),
            partition: vec![v1::EventPartitionComponent {
                name: "organization_id".to_owned(),
                value: Some(organization_uuid_value()),
            }],
            selected_fields: vec!["matter_id".to_owned(), "amount".to_owned()],
        }),
        after_event_id: after,
        page: Some(v1::PageRequest {
            limit: Some(10),
            cursor: None,
        }),
        observed_history_incarnation: 0,
    })
}

fn consume_stream_request(module_hash: &[u8]) -> TestResult<v1::ConsumeEventStreamRequest> {
    Ok(v1::ConsumeEventStreamRequest {
        request_id: fresh_request_id_bytes()?,
        selection: Some(v1::EventConsumerSelection {
            reactive_module_hash: module_hash.to_vec(),
            operation_name: BUDGET_STREAM_OPERATION.to_owned(),
            parameters: vec![v1::EventConsumerParameter {
                name: "organization_id".to_owned(),
                value: Some(organization_uuid_value()),
            }],
            consumer_name: PRUNED_CONSUMER_NAME.to_owned(),
        }),
        batch_limit: 4,
        in_flight_limit: 4,
        lease_seconds: 30,
        maximum_wait_nanos: 0,
    })
}

fn replayed_event_page(response: v1::ReplayEventsResponse) -> TestResult<v1::EventPage> {
    response
        .page
        .ok_or_else(|| test_failure("event replay omitted its page"))
}

fn replayed_commit_sequences(page: &v1::EventPage) -> TestResult<Vec<u64>> {
    page.items
        .iter()
        .map(|item| {
            item.event_id
                .map(|event_id| event_id.commit_sequence)
                .ok_or_else(|| test_failure("replayed event omitted its event ID"))
        })
        .collect()
}

fn assert_graceful_shutdown_checkpoint(
    database_path: &Path,
    expected_sequence: u64,
) -> TestResult<()> {
    // (1) A durable checkpoint exists and binds S at the drained frontier.
    let bound = read_validated_prefix_checkpoint_commit_sequence_fixture(database_path)
        .map_err(|error| test_failure(format!("checkpoint probe failed: {error:?}")))?;
    if bound != Some(expected_sequence) {
        return Err(test_failure(format!(
            "graceful shutdown must leave a durable validated-prefix checkpoint bound to \
             S={expected_sequence}; found {bound:?}"
        )));
    }
    // (2) The next open verifies it, taking the fast path.
    let store = RedbStore::open(database_path)
        .map_err(|error| test_failure(format!("checkpoint reopen failed: {error:?}")))?;
    let session = store
        .begin_structural_evidence(daemon_startup_inputs())
        .map_err(|error| test_failure(format!("evidence session failed: {error:?}")))?;
    if !session.checkpoint_verified() {
        return Err(test_failure(
            "the shutdown checkpoint must verify on the next open (fast path)",
        ));
    }
    drop(session);
    Ok(())
}

fn daemon_startup_inputs() -> StartupValidationInputs {
    let capability_key =
        DigestKeyId::new(CAPABILITY_DIGEST_KEY_ID).expect("daemon capability digest key ID");
    let idempotency_key =
        DigestKeyId::new(IDEMPOTENCY_DIGEST_KEY_ID).expect("daemon idempotency digest key ID");
    StartupValidationInputs::new(
        Timestamp::new(1_700_000_100, 0).expect("offline probe timestamp"),
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(capability_key)])
            .expect("capability digest inventory"),
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(idempotency_key)])
            .expect("idempotency digest inventory"),
    )
}

/// Full startup pass over a stopped daemon database, activating the real
/// operational ports (the same open the daemon performs).
fn open_operational_offline(database_path: &Path) -> RedbOperationalPorts {
    let store = RedbStore::open(database_path).expect("open stopped daemon database");
    let mut session = store
        .begin_structural_evidence(daemon_startup_inputs())
        .expect("begin structural evidence over the stopped daemon database");
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("bounded evidence page limit");
    let mut cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(cursor, limit)
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(
                    findings.is_empty(),
                    "stopped daemon database must validate clean: {findings:?}"
                );
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (history, historical_end) = validate_catalog_history(&mut session)
        .expect("validate catalog history")
        .into_parts();
    let opened = session
        .finish(structural_end, historical_end)
        .expect("finish structural evidence");
    let CatalogHistoryOutcome::Ready(_) = history else {
        panic!("stopped daemon database must not require catalog migration");
    };
    let StructuralOpenOutcome::Clean(opened) = opened else {
        panic!("stopped daemon database must not require index migration");
    };
    let (_, _, _, dormant): (_, _, _, RedbDormantPorts) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .expect("activate operational ports over the stopped daemon database")
}

/// Offline preparation over the stopped daemon database: drains every pending
/// outbox intent and returns the delivered count.
fn drain_outbox_offline(database_path: &Path) -> usize {
    let mut ports = open_operational_offline(database_path);
    let delivered = deliver_all_pending_outbox(&mut ports);
    drop(ports);
    delivered
}

/// Drains every pending outbox intent through the REAL claim/succeed
/// transitions (the operator precondition for pruning: only `Delivered`
/// lifts the retention fence). Returns how many intents were delivered.
fn deliver_all_pending_outbox(ports: &mut RedbOperationalPorts) -> usize {
    let destination =
        OutboxDestinationIdV1::new("p1-test-outbox-drain").expect("bounded destination ID");
    let limit = OutboxPageLimit::new(NonZeroU16::new(16).expect("nonzero page limit"))
        .expect("bounded outbox page limit");
    let started_at = Timestamp::new(1_700_000_200, 0).expect("claim timestamp");
    let lease_deadline = Timestamp::new(1_700_000_260, 0).expect("lease deadline");
    let delivered_at = Timestamp::new(1_700_000_230, 0).expect("delivery timestamp");
    let mut delivered = 0_usize;
    loop {
        let items = match ports
            .scan_pending_outbox(None, limit)
            .expect("scan pending outbox intents")
        {
            PendingOutboxScanV1::Page { items, .. } | PendingOutboxScanV1::ExactEnd { items } => {
                items
            }
        };
        if items.is_empty() {
            break;
        }
        for item in items {
            let item = item.into_parts().0;
            let claim = OutboxClaimV1::new(
                item.event_id(),
                item.status().clone(),
                destination.clone(),
                started_at,
                lease_deadline,
            )
            .expect("well-formed outbox claim");
            let OutboxTransitionResultV1::Applied(delivering) = ports
                .claim_outbox(&claim)
                .expect("claim one pending outbox intent")
            else {
                panic!("pending outbox intent must claim exactly");
            };
            let succeed =
                OutboxSucceedV1::new(delivering, delivered_at).expect("well-formed outbox success");
            let OutboxTransitionResultV1::Applied(_) = ports
                .succeed_outbox(&succeed)
                .expect("mark one outbox intent delivered")
            else {
                panic!("claimed outbox intent must deliver exactly");
            };
            delivered += 1;
        }
    }
    delivered
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

async fn assert_principal_less_liveness(client: &mut RiffDbClient) -> TestResult<()> {
    let metadata = CallMetadata::default();
    let response = bounded_rpc(
        "principal-less process liveness",
        client.health(v1::HealthRequest { request_id: None }, &metadata),
    )
    .await?;
    assert_process_liveness(&response)
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
                    permission: Some(Permission::ScanIndex(scoped(1))),
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
            row_policy: None,
        }),
    })
}

fn explicit_partition_capability_request() -> TestResult<v1::CreateCapabilityRequest> {
    use v1::capability_permission::Permission;

    let scoped = |stable_id| v1::LineageScopedStableId {
        contract_lineage: CONTRACT_LINEAGE.to_owned(),
        stable_id,
    };
    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Normal as i32,
        capability_id: generate_capability_id()?.into_bytes().to_vec(),
        principal_id: "p1-index-reader".to_owned(),
        actor_kind: v1::ActorKind::Human as i32,
        requested_lifetime_seconds: CAPABILITY_LIFETIME_SECONDS / 2,
        audiences: vec![AUDIENCE.to_owned()],
        grant: Some(v1::CapabilityGrant {
            tenant_scope: Some(v1::TenantScope {
                scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
            }),
            partition_scope: Some(v1::PartitionScope {
                scope: Some(v1::partition_scope::Scope::Explicit(
                    v1::ExplicitPartitionScope {
                        partitions: vec![v1::ScopedPartition {
                            contract_lineage: CONTRACT_LINEAGE.to_owned(),
                            partition_key: budget_partition_key()?,
                        }],
                    },
                )),
            }),
            permissions: vec![
                v1::CapabilityPermission {
                    permission: Some(Permission::ReadEntity(scoped(1))),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::ScanIndex(scoped(1))),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::ReadHealth(v1::Unit {})),
                },
            ],
            field_visibility: vec![v1::EntityFieldVisibility {
                contract_lineage: CONTRACT_LINEAGE.to_owned(),
                entity_type_id: 1,
                field_ids: vec![1, 2, 3, 4, 5],
            }],
            max_scan_rows: 10,
            approval_required: Vec::new(),
            row_policy: None,
        }),
    })
}

fn normal_capability_token(response: v1::CreateCapabilityResponse) -> TestResult<String> {
    let Some(v1::create_capability_response::Result::Normal(result)) = response.result else {
        return Err(test_failure(
            "explicit partition capability used the wrong result family",
        ));
    };
    let Some(v1::normal_create_capability_result::Result::Created(created)) = result.result else {
        return Err(test_failure(
            "explicit partition capability was not newly created",
        ));
    };
    if created.transition.is_none() || created.token.is_empty() {
        return Err(test_failure(
            "explicit partition capability response was incomplete",
        ));
    }
    Ok(created.token)
}

fn budget_partition_key() -> TestResult<Vec<u8>> {
    let aggregate = AggregateTypeId::new(1).expect("AnnualBudget has frozen nonzero ID 1");
    let mut key = PartitionKeyBuilder::new(aggregate);
    key.push_uuid(&ORGANIZATION_ID)?;
    Ok(key.finish()?.into_bytes())
}

fn index_scan_request() -> TestResult<v1::ScanIndexRequest> {
    Ok(v1::ScanIndexRequest {
        request_id: fresh_request_id_bytes()?,
        contract: Some(v1::ContractSelection {
            selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
        }),
        index_id: 1,
        leading_components: vec![v1::Value {
            kind: Some(v1::value::Kind::I64Value(FISCAL_YEAR)),
        }],
        fields: Some(v1::FieldSelection {
            field_ids: Vec::new(),
        }),
        page: Some(v1::PageRequest {
            limit: Some(10),
            cursor: None,
        }),
    })
}

fn assert_migrated_index_scan(
    response: &v1::ScanIndexResponse,
    entity_key: &[u8],
) -> TestResult<()> {
    let page = response
        .page
        .as_ref()
        .ok_or_else(|| test_failure("migrated index scan omitted its page"))?;
    if page.items.len() != 1 || page.next_cursor.is_some() {
        return Err(test_failure(
            "migrated index scan did not return one exact-end row",
        ));
    }
    let Some(v1::index_scan_fence::Position::AppliedEpoch(1)) = page
        .observed_fence
        .as_ref()
        .and_then(|fence| fence.position.as_ref())
    else {
        return Err(test_failure(
            "migrated index scan did not retain its authoritative epoch",
        ));
    };

    let entity_key = EntityKey::from_bytes(entity_key.to_vec())?;
    let mut expected_key = IndexEntryKeyBuilder::new(IndexId::new(1).expect("index ID"));
    expected_key.push_i64(FISCAL_YEAR)?;
    let expected_key = expected_key.finish(entity_key)?;
    let row = &page.items[0];
    if row.index_entry_key != expected_key.as_bytes() {
        return Err(test_failure(
            "migrated index scan returned the wrong physical row",
        ));
    }

    let values = row
        .values
        .as_ref()
        .ok_or_else(|| test_failure("migrated index row omitted selected values"))?;
    if !values.fields.is_empty() {
        return Err(test_failure(
            "grammar-v1 migrated index row returned nonempty covered values",
        ));
    }
    Ok(())
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

fn assert_process_liveness(response: &v1::HealthResponse) -> TestResult<()> {
    let Some(v1::health_response::Result::PreBootstrap(report)) = response.result.as_ref() else {
        return Err(test_failure(
            "principal-less Health was not restricted process liveness",
        ));
    };
    if !report.liveness || report.readiness {
        return Err(test_failure(
            "process liveness reported readiness or failed liveness",
        ));
    }
    if v1::PreBootstrapLifecycle::try_from(report.lifecycle)
        != Ok(v1::PreBootstrapLifecycle::Unspecified)
        || !response.database_alias.is_empty()
        || !response.authentication_audience.is_empty()
    {
        return Err(test_failure(
            "process liveness disclosed a database lifecycle or identity",
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
    if response.authentication_audience != AUDIENCE {
        return Err(test_failure(
            "authenticated Health omitted the configured authentication audience",
        ));
    }
    let Some(v1::health_response::Result::Authenticated(report)) = response.result.as_ref() else {
        return Err(test_failure("Health did not use the authenticated result"));
    };
    if report.status != expected_status as i32
        || report.active_contract_version != expected_contract_version
        || report.last_commit_sequence != expected_last_commit_sequence
    {
        return Err(test_failure(format!(
            "authenticated Health reported status={}, contract={:?}, sequence={:?}; expected status={}, contract={expected_contract_version:?}, sequence={expected_last_commit_sequence:?}",
            report.status,
            report.active_contract_version,
            report.last_commit_sequence,
            expected_status as i32,
        )));
    }
    Ok(())
}

fn assert_authoritatively_ready_health(
    response: &v1::HealthResponse,
    expected_contract_version: Option<u64>,
    expected_last_commit_sequence: Option<u64>,
) -> TestResult<()> {
    let Some(v1::health_response::Result::Authenticated(report)) = response.result.as_ref() else {
        return Err(test_failure("Health did not use the authenticated result"));
    };
    if report.active_contract_version != expected_contract_version
        || report.last_commit_sequence != expected_last_commit_sequence
    {
        return Err(test_failure(
            "authoritatively ready Health reported the wrong contract or sequence",
        ));
    }

    let expected_kinds = [
        v1::HealthComponentKind::AuthoritativeStorage,
        v1::HealthComponentKind::Catalog,
        v1::HealthComponentKind::CommitCoordinator,
        v1::HealthComponentKind::Projection,
        v1::HealthComponentKind::Outbox,
    ];
    let actual_kinds = report
        .components
        .iter()
        .map(|component| v1::HealthComponentKind::try_from(component.component))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| test_failure("Health reported an unknown component kind"))?;
    if actual_kinds != expected_kinds {
        return Err(test_failure(
            "Health omitted or reordered the authoritative/derived component shape",
        ));
    }
    for component in &report.components[..3] {
        if v1::HealthComponentStatus::try_from(component.status)
            != Ok(v1::HealthComponentStatus::Healthy)
        {
            return Err(test_failure(
                "authoritatively ready Health reported an unhealthy authoritative component",
            ));
        }
    }

    let mut derived_degraded = false;
    for component in &report.components[3..] {
        match v1::HealthComponentStatus::try_from(component.status) {
            Ok(v1::HealthComponentStatus::Healthy) => {}
            Ok(v1::HealthComponentStatus::Degraded) => derived_degraded = true,
            _ => {
                return Err(test_failure(
                    "authoritatively ready Health reported an unavailable derived component",
                ));
            }
        }
    }
    let expected_status = if derived_degraded {
        v1::HealthStatus::Degraded
    } else {
        v1::HealthStatus::Ready
    };
    if v1::HealthStatus::try_from(report.status) != Ok(expected_status) {
        return Err(test_failure(
            "aggregate Health did not match its authoritative and derived components",
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

fn assert_schema_bound_allocated_outcome(response: &v1::ExecuteCommandResponse) -> TestResult<()> {
    let Some(v1::Value {
        kind: Some(v1::value::Kind::RecordValue(outcome)),
    }) = response.outcome.as_ref()
    else {
        return Err(test_failure("allocated outcome was not a record"));
    };
    let outcome_fields = outcome
        .fields
        .iter()
        .map(|field| (field.field_id, field.name.as_str()))
        .collect::<Vec<_>>();
    if outcome_fields != [(Some(1), "budget"), (Some(2), "remaining")] {
        return Err(test_failure(
            "allocated outcome omitted its exact schema-bound field names",
        ));
    }

    let Some(v1::value::Kind::RecordValue(budget)) = value_field(outcome, 1)?.kind.as_ref() else {
        return Err(test_failure("allocated budget was not a record"));
    };
    let budget_fields = budget
        .fields
        .iter()
        .map(|field| (field.field_id, field.name.as_str()))
        .collect::<Vec<_>>();
    if budget_fields
        != [
            (Some(1), "updated_at"),
            (Some(2), "fiscal_year"),
            (Some(3), "approved_amount"),
            (Some(4), "organization_id"),
            (Some(5), "allocated_amount"),
        ]
    {
        return Err(test_failure(
            "allocated budget omitted its exact schema-bound field names",
        ));
    }
    for (record, field_id) in [(budget, 3), (budget, 5), (outcome, 2)] {
        let Some(v1::value::Kind::DecimalValue(decimal)) =
            value_field(record, field_id)?.kind.as_ref()
        else {
            return Err(test_failure("schema-bound amount was not decimal"));
        };
        if decimal.precision != Some(28) || decimal.scale != 2 {
            return Err(test_failure(
                "schema-bound amount omitted exact public decimal evidence",
            ));
        }
    }
    Ok(())
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
            .arg("--backup-root")
            .arg(
                database_path
                    .parent()
                    .expect("test database has parent")
                    .join("backups"),
            )
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
