#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Real-process WP-155 acceptance for public offline backup and restore.

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
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender};
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
    AttemptBudget, BackupNameV1, BearerCredential, BootstrapCallMetadata,
    BootstrapCredential as TransportBootstrapCredential, CallMetadata, CreateOfflineBackup,
    OfflineMaintenanceOperationId, OfflineMaintenanceReplacementConfirmation, RestoreOfflineBackup,
    RiffDbClient, generate_offline_maintenance_operation_id, generate_request_id, v1,
};
use riffdb_storage_api::{DatabaseIdentityProbe, DatabaseIdentityProbePort};
use riffdb_storage_redb::RedbStore;
use riffdb_types::{EntityKeyBuilder, EntityTypeId};
use tokio::time::timeout;
use tonic::transport::Endpoint;

const AUDIENCE: &str = "riffdb-grpc-loopback";
const ENVIRONMENT: &str = "wp155-maintenance-test";
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const SHUTDOWN_COMMAND: &[u8] = b"shutdown\n";
const MAX_READY_LINE_BYTES: usize = 256;
const PROCESS_START_TIMEOUT: Duration = Duration::from_secs(30);
const MAINTENANCE_TIMEOUT: Duration = Duration::from_secs(90);
const PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(15);
const PROCESS_KILL_TIMEOUT: Duration = Duration::from_secs(5);
const PROCESS_REAPER_POLL: Duration = Duration::from_millis(10);
const RPC_TIMEOUT: Duration = Duration::from_secs(15);
const BOOTSTRAP_UNIX_MILLISECONDS: u64 = 1_700_000_000_000;
const CAPABILITY_LIFETIME_SECONDS: u32 = 3_600;
const FISCAL_YEAR: i64 = 2027;
const ORGANIZATION_ID: [u8; 16] = [0x31; 16];
const MATTER_ID: [u8; 16] = [0x42; 16];
const APPROVED_MINOR_UNITS: i128 = 10_000;
const ALLOCATED_MINOR_UNITS: i128 = 2_500;

const CAPABILITY_KEY_DOCUMENT: &[u8] = b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEY_DOCUMENT: &[u8] = b"riffdb-idempotency-digest-keys-v1\n9:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";
const BUDGET_CONTRACT: &str = include_str!("../../contracts/examples/budget.riff");
const FIXED_MCP_REGISTRY: &str =
    include_str!("../../crates/riffdb-api-mcp/fixtures/fixed-tool-registry-v1.json");

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawns the real riffdbd process and performs destructive offline restore"]
async fn public_backup_restore_preserves_identity_and_rewinds_authoritative_history()
-> TestResult<()> {
    assert_no_mcp_maintenance_surface()?;

    let temporary = TemporaryDirectory::new()?;
    let database_path = temporary.path().join("riffdb.redb");
    let backup_root = temporary.path().join("backups");
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
    let authenticated = CallMetadata::authenticated(bearer_credential(&retained_bootstrap)?);

    let create_budget = CreateBudget {
        idempotency_key: "wp155-create-before-backup".to_owned(),
        organization_id: ORGANIZATION_ID,
        fiscal_year: FISCAL_YEAR,
        approved_amount: amount(APPROVED_MINOR_UNITS)?,
    };
    let allocate_suffix = AllocateBudget {
        idempotency_key: "wp155-reused-after-rewind".to_owned(),
        organization_id: ORGANIZATION_ID,
        fiscal_year: FISCAL_YEAR,
        matter_id: MATTER_ID,
        amount: amount(ALLOCATED_MINOR_UNITS)?,
    };
    let entity_key = budget_entity_key()?;

    let mut seed_process = ServerProcess::spawn(
        &database_path,
        &backup_root,
        &capability_keys_path,
        &idempotency_keys_path,
    )?;
    let seed_address = seed_process.wait_for_ready_address(PROCESS_START_TIMEOUT)?;
    let mut seed_client = connect(seed_address).await?;

    let bootstrap = bounded_rpc(
        "bootstrap capability creation",
        seed_client.create_bootstrap_capability(
            bootstrap_request(&retained_bootstrap)?,
            &bootstrap_metadata(&retained_bootstrap)?,
        ),
    )
    .await?;
    assert_bootstrap_created(bootstrap)?;

    let deployment = bounded_rpc(
        "budget contract deployment",
        seed_client.deploy_contract(
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
    assert_contract_activated(deployment)?;

    let created = bounded_rpc(
        "CreateBudget before backup",
        seed_client.execute_generated(&create_budget, one_attempt(), &authenticated),
    )
    .await?;
    if created.response().status != v1::execute_command_response::CompletionStatus::Committed as i32
        || created.response().commit_sequence != 1
    {
        return Err(test_failure(
            "the backup frontier was not seeded at authoritative commit 1",
        ));
    }
    let CreateBudgetOutcome::BudgetCreated { budget } = created.outcome() else {
        return Err(test_failure(
            "CreateBudget did not create the backup fixture",
        ));
    };
    if budget.allocated_amount.minor_units() != 0 {
        return Err(test_failure(
            "the pre-backup budget unexpectedly contained an allocation",
        ));
    }
    let entity_at_backup = found_entity(
        bounded_rpc(
            "entity read at backup frontier",
            seed_client.get_entity(entity_request(&entity_key)?, &authenticated),
        )
        .await?,
    )?;
    if entity_at_backup.entity_version != 1 {
        return Err(test_failure(
            "the backup fixture entity was not at version 1",
        ));
    }
    drop(seed_client);
    seed_process.shutdown_cleanly()?;

    // This probe is deliberately test-only and runs with riffdbd fully stopped.
    // No maintenance request, SDK, gRPC adapter, CLI, or MCP path receives a
    // storage handle or filesystem path.
    let database_id_before_backup = probe_database_id_offline(&database_path)?;

    let mut process = ServerProcess::spawn(
        &database_path,
        &backup_root,
        &capability_keys_path,
        &idempotency_keys_path,
    )?;
    let address = process.wait_for_ready_address(PROCESS_START_TIMEOUT)?;
    let mut client = connect(address).await?;

    let backup_name = BackupNameV1::new("rewind-point")?;
    let create_operation = CreateOfflineBackup::new(
        generate_offline_maintenance_operation_id()?,
        backup_name.clone(),
    );
    let create_started = bounded_rpc(
        "public CreateOfflineBackup",
        client.create_offline_backup_with_retry(&create_operation, one_attempt(), &authenticated),
    )
    .await?;
    let create_accepted = require_started_operation(
        create_started.disposition,
        create_started.operation.as_ref(),
        v1::OfflineMaintenanceStartDisposition::Accepted,
        create_operation.operation_id(),
        v1::OfflineMaintenanceOperationKind::CreateBackup,
        &backup_name,
        v1::OfflineMaintenancePhase::Draining,
    )?;
    drop(client);

    let ready_after_create = process.wait_for_ready_address(MAINTENANCE_TIMEOUT)?;
    if ready_after_create != address {
        return Err(test_failure(
            "riffdbd rebound maintenance readiness at a different address",
        ));
    }
    let mut client = connect(ready_after_create).await?;
    let create_terminal =
        poll_terminal_operation(&mut client, create_operation.operation_id(), &authenticated)
            .await?;
    assert_same_operation_identity(create_accepted, &create_terminal)?;

    let create_retry_ids = [fresh_request_id_bytes()?, fresh_request_id_bytes()?];
    if create_retry_ids[0] == create_retry_ids[1] {
        return Err(test_failure(
            "two public backup retries reused a transport RequestId",
        ));
    }
    for request_id in create_retry_ids {
        let response = bounded_rpc(
            "terminal CreateOfflineBackup retry",
            client.create_offline_backup(
                create_backup_request(request_id, create_operation.operation_id(), &backup_name),
                &authenticated,
            ),
        )
        .await?;
        let operation = require_started_operation(
            response.disposition,
            response.operation.as_ref(),
            v1::OfflineMaintenanceStartDisposition::Terminal,
            create_operation.operation_id(),
            v1::OfflineMaintenanceOperationKind::CreateBackup,
            &backup_name,
            v1::OfflineMaintenancePhase::Succeeded,
        )?;
        if operation != &create_terminal {
            return Err(test_failure(
                "a repeated backup start did not resolve the immutable terminal receipt",
            ));
        }
    }

    let old_suffix = bounded_rpc(
        "AllocateBudget after backup",
        client.execute_generated(&allocate_suffix, one_attempt(), &authenticated),
    )
    .await?;
    if old_suffix.response().status
        != v1::execute_command_response::CompletionStatus::Committed as i32
        || old_suffix.response().commit_sequence != 2
    {
        return Err(test_failure(
            "the post-backup suffix was not committed at sequence 2",
        ));
    }
    let AllocateBudgetOutcome::Allocated { budget, .. } = old_suffix.outcome() else {
        return Err(test_failure("AllocateBudget did not create the old suffix"));
    };
    if budget.allocated_amount.minor_units() != ALLOCATED_MINOR_UNITS {
        return Err(test_failure(
            "the old suffix did not contain the expected allocation",
        ));
    }
    let entity_in_old_suffix = found_entity(
        bounded_rpc(
            "entity read in old suffix",
            client.get_entity(entity_request(&entity_key)?, &authenticated),
        )
        .await?,
    )?;
    if entity_in_old_suffix.entity_version != 2 || entity_in_old_suffix == entity_at_backup {
        return Err(test_failure(
            "the post-backup write did not create a distinct version 2 entity",
        ));
    }
    let old_outcome = found_outcome(
        bounded_rpc(
            "old suffix outcome lookup",
            client.get_outcome(
                allocate_suffix.outcome_request(generate_request_id()?)?,
                &authenticated,
            ),
        )
        .await?,
    )?;
    if old_outcome.commit_sequence != 2 {
        return Err(test_failure(
            "the old suffix outcome did not identify commit 2",
        ));
    }

    let restore_operation = RestoreOfflineBackup::new(
        generate_offline_maintenance_operation_id()?,
        backup_name.clone(),
        OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
    );
    let restore_started = bounded_rpc(
        "public RestoreOfflineBackup",
        client.restore_offline_backup_with_retry(&restore_operation, one_attempt(), &authenticated),
    )
    .await?;
    let restore_accepted = require_started_operation(
        restore_started.disposition,
        restore_started.operation.as_ref(),
        v1::OfflineMaintenanceStartDisposition::Accepted,
        restore_operation.operation_id(),
        v1::OfflineMaintenanceOperationKind::RestoreBackup,
        &backup_name,
        v1::OfflineMaintenancePhase::Draining,
    )?;
    drop(client);

    let ready_after_restore = process.wait_for_ready_address(MAINTENANCE_TIMEOUT)?;
    if ready_after_restore != address {
        return Err(test_failure(
            "riffdbd rebound restored readiness at a different address",
        ));
    }
    let mut client = connect(ready_after_restore).await?;
    let restore_terminal = poll_terminal_operation(
        &mut client,
        restore_operation.operation_id(),
        &authenticated,
    )
    .await?;
    assert_same_operation_identity(restore_accepted, &restore_terminal)?;
    assert_authenticated_frontier(&mut client, &authenticated, 1).await?;

    let entity_after_restore = found_entity(
        bounded_rpc(
            "entity read after restore",
            client.get_entity(entity_request(&entity_key)?, &authenticated),
        )
        .await?,
    )?;
    if entity_after_restore != entity_at_backup {
        return Err(test_failure(
            "restore did not return the authoritative entity to the backup frontier",
        ));
    }
    let removed_outcome = bounded_rpc(
        "destroyed suffix outcome lookup",
        client.get_outcome(
            allocate_suffix.outcome_request(generate_request_id()?)?,
            &authenticated,
        ),
    )
    .await?;
    if !matches!(
        removed_outcome.result,
        Some(v1::get_outcome_response::Result::NotFound(_))
    ) {
        return Err(test_failure(
            "restore retained an outcome from the destroyed history suffix",
        ));
    }

    let reused_suffix = bounded_rpc(
        "same-key AllocateBudget after rewind",
        client.execute_generated(&allocate_suffix, one_attempt(), &authenticated),
    )
    .await?;
    if reused_suffix.response().status
        != v1::execute_command_response::CompletionStatus::Committed as i32
        || reused_suffix.response().commit_sequence != 2
    {
        return Err(test_failure(
            "the destroyed idempotency row and commit sequence were not reusable after rewind",
        ));
    }
    let AllocateBudgetOutcome::Allocated { budget, .. } = reused_suffix.outcome() else {
        return Err(test_failure(
            "same-key AllocateBudget did not execute after rewind",
        ));
    };
    if budget.allocated_amount.minor_units() != ALLOCATED_MINOR_UNITS {
        return Err(test_failure(
            "the reused suffix contained the wrong authoritative allocation",
        ));
    }
    let entity_in_reused_suffix = found_entity(
        bounded_rpc(
            "entity read in reused suffix",
            client.get_entity(entity_request(&entity_key)?, &authenticated),
        )
        .await?,
    )?;
    if entity_in_reused_suffix.entity_version != 2 {
        return Err(test_failure(
            "the restored allocator did not reuse entity version 2",
        ));
    }

    let restore_retry_ids = [fresh_request_id_bytes()?, fresh_request_id_bytes()?];
    if restore_retry_ids[0] == restore_retry_ids[1] {
        return Err(test_failure(
            "two public restore retries reused a transport RequestId",
        ));
    }
    for request_id in restore_retry_ids {
        let response = bounded_rpc(
            "terminal RestoreOfflineBackup retry",
            client.restore_offline_backup(
                restore_backup_request(request_id, restore_operation.operation_id(), &backup_name),
                &authenticated,
            ),
        )
        .await?;
        let operation = require_started_operation(
            response.disposition,
            response.operation.as_ref(),
            v1::OfflineMaintenanceStartDisposition::Terminal,
            restore_operation.operation_id(),
            v1::OfflineMaintenanceOperationKind::RestoreBackup,
            &backup_name,
            v1::OfflineMaintenancePhase::Succeeded,
        )?;
        if operation != &restore_terminal {
            return Err(test_failure(
                "a repeated restore start did not resolve the immutable terminal receipt",
            ));
        }
    }
    assert_authenticated_frontier(&mut client, &authenticated, 2).await?;
    let entity_after_terminal_retries = found_entity(
        bounded_rpc(
            "entity read after terminal restore retries",
            client.get_entity(entity_request(&entity_key)?, &authenticated),
        )
        .await?,
    )?;
    if entity_after_terminal_retries != entity_in_reused_suffix {
        return Err(test_failure(
            "a terminal same-operation retry repeated the destructive restore",
        ));
    }

    drop(client);
    process.shutdown_cleanly()?;
    let database_id_after_restore = probe_database_id_offline(&database_path)?;
    if database_id_after_restore != database_id_before_backup {
        return Err(test_failure(
            "restore changed the permanent DatabaseId stored in the backup",
        ));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "spawns real riffdbd processes and replaces absent and corrupt database targets"]
async fn staged_only_recovery_denies_bad_credentials_and_restores_empty_or_corrupt_targets()
-> TestResult<()> {
    let temporary = TemporaryDirectory::new()?;
    let database_path = temporary.path().join("riffdb.redb");
    let backup_root = temporary.path().join("backups");
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
    let authenticated = CallMetadata::authenticated(bearer_credential(&retained_bootstrap)?);

    let mut seed_process = ServerProcess::spawn(
        &database_path,
        &backup_root,
        &capability_keys_path,
        &idempotency_keys_path,
    )?;
    let seed_address = seed_process.wait_for_ready_address(PROCESS_START_TIMEOUT)?;
    let mut seed_client = connect(seed_address).await?;
    assert_bootstrap_created(
        bounded_rpc(
            "recovery fixture bootstrap",
            seed_client.create_bootstrap_capability(
                bootstrap_request(&retained_bootstrap)?,
                &bootstrap_metadata(&retained_bootstrap)?,
            ),
        )
        .await?,
    )?;
    assert_contract_activated(
        bounded_rpc(
            "recovery fixture deployment",
            seed_client.deploy_contract(
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
        .await?,
    )?;
    let recovery_seed = CreateBudget {
        idempotency_key: "wp155-recovery-seed".to_owned(),
        organization_id: ORGANIZATION_ID,
        fiscal_year: FISCAL_YEAR,
        approved_amount: amount(APPROVED_MINOR_UNITS)?,
    };
    let seeded = bounded_rpc(
        "recovery fixture command",
        seed_client.execute_generated(&recovery_seed, one_attempt(), &authenticated),
    )
    .await?;
    if seeded.response().status != v1::execute_command_response::CompletionStatus::Committed as i32
        || seeded.response().commit_sequence != 1
    {
        return Err(test_failure(
            "recovery fixture command did not establish frontier 1",
        ));
    }

    let backup_name = BackupNameV1::new("recovery-source")?;
    let create = CreateOfflineBackup::new(
        generate_offline_maintenance_operation_id()?,
        backup_name.clone(),
    );
    let accepted = bounded_rpc(
        "recovery fixture backup",
        seed_client.create_offline_backup_with_retry(&create, one_attempt(), &authenticated),
    )
    .await?;
    require_started_operation(
        accepted.disposition,
        accepted.operation.as_ref(),
        v1::OfflineMaintenanceStartDisposition::Accepted,
        create.operation_id(),
        v1::OfflineMaintenanceOperationKind::CreateBackup,
        &backup_name,
        v1::OfflineMaintenancePhase::Draining,
    )?;
    drop(seed_client);
    let seed_address_after_backup = seed_process.wait_for_ready_address(MAINTENANCE_TIMEOUT)?;
    if seed_address_after_backup != seed_address {
        return Err(test_failure(
            "backup fixture rebound at an unexpected address",
        ));
    }
    let mut seed_client = connect(seed_address_after_backup).await?;
    poll_terminal_operation(&mut seed_client, create.operation_id(), &authenticated).await?;
    drop(seed_client);
    seed_process.shutdown_cleanly()?;
    let expected_database_id = probe_database_id_offline(&database_path)?;

    fs::remove_file(&database_path)?;
    let recovery_address = reserve_loopback_address()?;
    let mut empty_process = ServerProcess::spawn_at(
        &database_path,
        &backup_root,
        &capability_keys_path,
        &idempotency_keys_path,
        recovery_address,
    )?;
    let mut empty_client = connect_eventually(recovery_address).await?;
    let empty_restore = RestoreOfflineBackup::new(
        generate_offline_maintenance_operation_id()?,
        backup_name.clone(),
        OfflineMaintenanceReplacementConfirmation::NotProvided,
    );
    let denied_credential =
        generate_bootstrap_credential(BOOTSTRAP_UNIX_MILLISECONDS + 1, &SystemEntropy)?;
    let denied_metadata = CallMetadata::authenticated(bearer_credential(&denied_credential)?);
    match timeout(
        RPC_TIMEOUT,
        empty_client.restore_offline_backup_with_retry(
            &empty_restore,
            retry_attempts(),
            &denied_metadata,
        ),
    )
    .await
    {
        Ok(Err(_)) => {}
        Ok(Ok(_)) => {
            return Err(test_failure(
                "staged-only recovery accepted a bearer denied by the backup",
            ));
        }
        Err(_) => return Err(test_failure("denied staged authorization timed out")),
    }
    let empty_result = bounded_rpc(
        "empty-target staged recovery retry",
        empty_client.restore_offline_backup_with_retry(
            &empty_restore,
            retry_attempts(),
            &authenticated,
        ),
    )
    .await?;
    require_started_operation(
        empty_result.disposition,
        empty_result.operation.as_ref(),
        v1::OfflineMaintenanceStartDisposition::Terminal,
        empty_restore.operation_id(),
        v1::OfflineMaintenanceOperationKind::RestoreBackup,
        &backup_name,
        v1::OfflineMaintenancePhase::Succeeded,
    )?;
    drop(empty_client);
    if empty_process.wait_for_ready_address(MAINTENANCE_TIMEOUT)? != recovery_address {
        return Err(test_failure(
            "empty-target recovery did not reactivate the configured listener",
        ));
    }
    let mut ready_client = connect(recovery_address).await?;
    assert_authenticated_frontier(&mut ready_client, &authenticated, 1).await?;
    drop(ready_client);
    empty_process.shutdown_cleanly()?;
    if probe_database_id_offline(&database_path)? != expected_database_id {
        return Err(test_failure(
            "empty-target recovery changed the backup DatabaseId",
        ));
    }

    let mut corrupt = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&database_path)?;
    corrupt.write_all(b"not-a-riffdb-database")?;
    corrupt.sync_all()?;
    drop(corrupt);
    let corrupt_address = reserve_loopback_address()?;
    let mut corrupt_process = ServerProcess::spawn_at(
        &database_path,
        &backup_root,
        &capability_keys_path,
        &idempotency_keys_path,
        corrupt_address,
    )?;
    let mut corrupt_client = connect_eventually(corrupt_address).await?;
    let corrupt_restore = RestoreOfflineBackup::new(
        generate_offline_maintenance_operation_id()?,
        backup_name.clone(),
        OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
    );
    let corrupt_result = restore_eventually(
        &mut corrupt_client,
        corrupt_restore.operation_id(),
        &backup_name,
        &authenticated,
    )
    .await?;
    require_started_operation(
        corrupt_result.disposition,
        corrupt_result.operation.as_ref(),
        v1::OfflineMaintenanceStartDisposition::Terminal,
        corrupt_restore.operation_id(),
        v1::OfflineMaintenanceOperationKind::RestoreBackup,
        &backup_name,
        v1::OfflineMaintenancePhase::Succeeded,
    )?;
    drop(corrupt_client);
    if corrupt_process.wait_for_ready_address(MAINTENANCE_TIMEOUT)? != corrupt_address {
        return Err(test_failure(
            "corrupt-target recovery did not reactivate the configured listener",
        ));
    }
    let mut ready_client = connect(corrupt_address).await?;
    assert_authenticated_frontier(&mut ready_client, &authenticated, 1).await?;
    drop(ready_client);
    corrupt_process.shutdown_cleanly()?;
    if probe_database_id_offline(&database_path)? != expected_database_id {
        return Err(test_failure(
            "corrupt-target recovery changed the backup DatabaseId",
        ));
    }
    Ok(())
}

fn assert_no_mcp_maintenance_surface() -> TestResult<()> {
    for forbidden in [
        "CreateOfflineBackup",
        "RestoreOfflineBackup",
        "GetOfflineMaintenanceOperation",
        "riffdb.backup",
        "backup.create",
        "backup.restore",
        "backup.operation",
    ] {
        if FIXED_MCP_REGISTRY.contains(forbidden) {
            return Err(test_failure(format!(
                "offline maintenance leaked into the fixed MCP registry as {forbidden}"
            )));
        }
    }
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

async fn connect(address: SocketAddr) -> TestResult<RiffDbClient> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(RPC_TIMEOUT)
        .timeout(RPC_TIMEOUT);
    bounded_rpc("gRPC connection", RiffDbClient::connect(endpoint)).await
}

async fn connect_eventually(address: SocketAddr) -> TestResult<RiffDbClient> {
    timeout(PROCESS_START_TIMEOUT, async move {
        loop {
            match connect(address).await {
                Ok(client) => return Ok(client),
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .map_err(|_| test_failure("recovery listener did not accept a connection"))?
}

async fn restore_eventually(
    client: &mut RiffDbClient,
    operation_id: OfflineMaintenanceOperationId,
    backup_name: &BackupNameV1,
    metadata: &CallMetadata,
) -> TestResult<v1::RestoreOfflineBackupResponse> {
    timeout(PROCESS_START_TIMEOUT, async {
        loop {
            let request =
                restore_backup_request(fresh_request_id_bytes()?, operation_id, backup_name);
            match client.restore_offline_backup(request, metadata).await {
                Ok(response) => return Ok(response),
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .map_err(|_| test_failure("recovery restore admission did not become available"))?
}

async fn poll_terminal_operation(
    client: &mut RiffDbClient,
    operation_id: OfflineMaintenanceOperationId,
    metadata: &CallMetadata,
) -> TestResult<v1::OfflineMaintenanceOperation> {
    let response = bounded_rpc(
        "GetOfflineMaintenanceOperation after readiness",
        client.get_offline_maintenance_operation(
            v1::GetOfflineMaintenanceOperationRequest {
                request_id: fresh_request_id_bytes()?,
                operation_id: operation_id.into_bytes().to_vec(),
            },
            metadata,
        ),
    )
    .await?;
    let Some(v1::get_offline_maintenance_operation_response::Result::Found(operation)) =
        response.result
    else {
        return Err(test_failure(
            "terminal maintenance receipt was not available after readiness",
        ));
    };
    if operation.phase != v1::OfflineMaintenancePhase::Succeeded as i32
        || operation.failure != v1::OfflineMaintenanceFailureClass::Unspecified as i32
    {
        return Err(test_failure(
            "maintenance readiness was published before a successful terminal receipt",
        ));
    }
    Ok(operation)
}

fn require_started_operation<'a>(
    disposition: i32,
    operation: Option<&'a v1::OfflineMaintenanceOperation>,
    expected_disposition: v1::OfflineMaintenanceStartDisposition,
    expected_id: OfflineMaintenanceOperationId,
    expected_kind: v1::OfflineMaintenanceOperationKind,
    expected_name: &BackupNameV1,
    expected_phase: v1::OfflineMaintenancePhase,
) -> TestResult<&'a v1::OfflineMaintenanceOperation> {
    let operation =
        operation.ok_or_else(|| test_failure("maintenance start omitted its operation"))?;
    if disposition != expected_disposition as i32
        || operation.operation_id != expected_id.into_bytes()
        || operation.kind != expected_kind as i32
        || operation.backup_name != expected_name.as_str()
        || operation.input_hash.len() != 32
        || operation.phase != expected_phase as i32
        || operation.failure != v1::OfflineMaintenanceFailureClass::Unspecified as i32
    {
        return Err(test_failure(format!(
            "maintenance start returned unexpected receipt fields: disposition={disposition}, kind={}, name={}, hash_len={}, phase={}, failure={}",
            operation.kind,
            operation.backup_name,
            operation.input_hash.len(),
            operation.phase,
            operation.failure,
        )));
    }
    Ok(operation)
}

fn assert_same_operation_identity(
    accepted: &v1::OfflineMaintenanceOperation,
    terminal: &v1::OfflineMaintenanceOperation,
) -> TestResult<()> {
    if accepted.operation_id != terminal.operation_id
        || accepted.kind != terminal.kind
        || accepted.backup_name != terminal.backup_name
        || accepted.input_hash != terminal.input_hash
    {
        return Err(test_failure(
            "maintenance receipt changed its immutable semantic identity",
        ));
    }
    Ok(())
}

async fn assert_authenticated_frontier(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    expected_sequence: u64,
) -> TestResult<()> {
    let response = bounded_rpc(
        "authenticated Health frontier",
        client.health(
            v1::HealthRequest {
                request_id: Some(fresh_request_id_bytes()?),
            },
            metadata,
        ),
    )
    .await?;
    let Some(v1::health_response::Result::Authenticated(report)) = response.result else {
        return Err(test_failure(
            "the retained backed-up bearer did not reach authenticated Health",
        ));
    };
    if !matches!(
        v1::HealthStatus::try_from(report.status),
        Ok(v1::HealthStatus::Ready | v1::HealthStatus::Degraded)
    ) || report.active_contract_version != Some(CONTRACT_VERSION)
        || report.last_commit_sequence != Some(expected_sequence)
    {
        return Err(test_failure(format!(
            "authenticated Health did not report restored frontier {expected_sequence}"
        )));
    }
    Ok(())
}

fn create_backup_request(
    request_id: Vec<u8>,
    operation_id: OfflineMaintenanceOperationId,
    backup_name: &BackupNameV1,
) -> v1::CreateOfflineBackupRequest {
    v1::CreateOfflineBackupRequest {
        request_id,
        operation_id: operation_id.into_bytes().to_vec(),
        backup_name: backup_name.as_str().to_owned(),
    }
}

fn restore_backup_request(
    request_id: Vec<u8>,
    operation_id: OfflineMaintenanceOperationId,
    backup_name: &BackupNameV1,
) -> v1::RestoreOfflineBackupRequest {
    v1::RestoreOfflineBackupRequest {
        request_id,
        operation_id: operation_id.into_bytes().to_vec(),
        backup_name: backup_name.as_str().to_owned(),
        replacement_confirmation:
            v1::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget as i32,
    }
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
        principal_id: "wp155-maintainer".to_owned(),
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
                    permission: Some(Permission::ReadHealth(v1::Unit {})),
                },
                v1::CapabilityPermission {
                    permission: Some(Permission::AdministerCapabilities(v1::Unit {})),
                },
            ],
            field_visibility: vec![v1::EntityFieldVisibility {
                contract_lineage: CONTRACT_LINEAGE.to_owned(),
                entity_type_id: 1,
                field_ids: vec![1, 3, 5],
            }],
            max_scan_rows: 100,
            approval_required: Vec::new(),
        }),
    })
}

fn bootstrap_metadata(
    credential: &RetainedBootstrapCredential,
) -> TestResult<BootstrapCallMetadata> {
    let credential = TransportBootstrapCredential::new(bootstrap_token_text(credential)?)?;
    Ok(BootstrapCallMetadata::new(credential))
}

fn bearer_credential(credential: &RetainedBootstrapCredential) -> TestResult<BearerCredential> {
    BearerCredential::new(bootstrap_token_text(credential)?).map_err(Into::into)
}

fn bootstrap_token_text(credential: &RetainedBootstrapCredential) -> TestResult<&str> {
    str::from_utf8(credential.token().expose_secret()).map_err(Into::into)
}

fn assert_bootstrap_created(response: v1::CreateCapabilityResponse) -> TestResult<()> {
    let Some(v1::create_capability_response::Result::Bootstrap(result)) = response.result else {
        return Err(test_failure(
            "bootstrap capability response used the wrong result family",
        ));
    };
    let Some(v1::bootstrap_create_capability_result::Result::Created(transition)) = result.result
    else {
        return Err(test_failure("bootstrap capability was not newly created"));
    };
    if transition.administration_sequence == 0 || transition.identity.is_none() {
        return Err(test_failure(
            "bootstrap capability transition was incomplete",
        ));
    }
    Ok(())
}

fn assert_contract_activated(response: v1::DeployContractResponse) -> TestResult<()> {
    let Some(v1::deploy_contract_response::Result::Activated(contract)) = response.result else {
        return Err(test_failure("budget contract was not newly activated"));
    };
    if contract.contract_lineage != CONTRACT_LINEAGE
        || contract.contract_version != CONTRACT_VERSION
    {
        return Err(test_failure(
            "activated budget contract identity was unexpected",
        ));
    }
    Ok(())
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

fn retry_attempts() -> AttemptBudget {
    AttemptBudget::new(8).expect("eight is a nonzero submission bound")
}

fn reserve_loopback_address() -> io::Result<SocketAddr> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
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

fn probe_database_id_offline(path: &Path) -> TestResult<riffdb_types::DatabaseId> {
    let store = RedbStore::open(path)?;
    let probe = store.probe_database_identity()?;
    drop(store);
    match probe {
        DatabaseIdentityProbe::Existing(database_id) => Ok(database_id),
        DatabaseIdentityProbe::NeedsInitialization => Err(test_failure(
            "stopped RiffDB database omitted its permanent DatabaseId",
        )),
    }
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
                "riffdb-wp155-maintenance-{}-{ordinal}",
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
            "could not allocate a unique WP-155 test directory",
        ))
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        if std::env::var_os("RIFFDB_KEEP_WP155_TEST_DIRECTORY").is_some() {
            return;
        }
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
        backup_root: &Path,
        capability_keys_path: &Path,
        idempotency_keys_path: &Path,
    ) -> io::Result<Self> {
        Self::spawn_at(
            database_path,
            backup_root,
            capability_keys_path,
            idempotency_keys_path,
            "127.0.0.1:0".parse().expect("valid loopback address"),
        )
    }

    fn spawn_at(
        database_path: &Path,
        backup_root: &Path,
        capability_keys_path: &Path,
        idempotency_keys_path: &Path,
        listen_address: SocketAddr,
    ) -> io::Result<Self> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_riffdbd"));
        command
            .arg("--database")
            .arg(database_path)
            .arg("--listen")
            .arg(listen_address.to_string())
            .arg("--environment")
            .arg(ENVIRONMENT)
            .arg("--audience")
            .arg(AUDIENCE)
            .arg("--backup-root")
            .arg(backup_root)
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

        let (ready_sender, ready) = mpsc::channel();
        let stdout = thread::spawn(move || read_readiness_stream(stdout, ready_sender));
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

    fn wait_for_ready_address(&self, deadline: Duration) -> TestResult<SocketAddr> {
        let line = match self.ready.recv_timeout(deadline) {
            Ok(result) => result?,
            Err(RecvTimeoutError::Timeout) => {
                return Err(test_failure("riffdbd readiness line timed out"));
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(test_failure(
                    "riffdbd exited before publishing the expected readiness line",
                ));
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

fn read_readiness_stream(stdout: ChildStdout, ready: Sender<io::Result<String>>) -> usize {
    let mut reader = BufReader::new(stdout);
    let mut total = 0_usize;
    loop {
        match read_optional_bounded_line(&mut reader, MAX_READY_LINE_BYTES) {
            Ok(Some(line)) => {
                total = total.saturating_add(line.len().saturating_add(1));
                if ready.send(Ok(line)).is_err() {
                    return total.saturating_add(drain_reader(&mut reader));
                }
            }
            Ok(None) => return total,
            Err(error) => {
                let _ = ready.send(Err(error));
                return total.saturating_add(drain_reader(&mut reader));
            }
        }
    }
}

fn read_optional_bounded_line(
    reader: &mut impl Read,
    maximum: usize,
) -> io::Result<Option<String>> {
    let mut bytes = Vec::with_capacity(maximum.min(64));
    let mut byte = [0_u8; 1];
    loop {
        match reader.read(&mut byte)? {
            0 if bytes.is_empty() => return Ok(None),
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "partial readiness line",
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
        .map(Some)
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
