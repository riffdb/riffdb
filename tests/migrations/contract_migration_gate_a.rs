#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Populated-database Gate-A migration proof through the public daemon boundary.

use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use riffdb_auth::bootstrap_secret::{
    BootstrapCredential as RetainedBootstrapCredential, SystemEntropy,
    generate_bootstrap_credential,
};
use riffdb_client_rust::{
    ApplyContractMigration, AttemptBudget, BearerCredential, BootstrapCallMetadata,
    BootstrapCredential as TransportBootstrapCredential, CallMetadata, CheckContractMigration,
    DatabaseAlias, IdempotentCommand, RiffDbClient, generate_contract_migration_operation_id,
    generate_request_id, v1,
};
use riffdb_contract_ir::{ContractBundle, MigrationBundleV1};
use riffdb_storage_api::{
    ApplicationSequenceAllocator, EntityTarget, ProjectionLifecycleV1,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs,
};
use riffdb_testkit::inspection::{DurableInspectionRequest, inspect_redb};
use riffdb_testkit::process::{ChildProcessController, ChildProcessSpec};
use riffdb_types::{
    CanonicalValue, DigestKeyId, EntityKeyBuilder, EntityVersion, FieldId, FrontierPosition,
    ProjectionIdentity, Timestamp,
};
use tokio::time::timeout;
use tonic::transport::Endpoint;

const CHILD_MODE: &str = "RIFFDB_WP410_CHILD_MODE";
const CHILD_RIFFDBD: &str = "riffdbd";
const AUDIENCE: &str = "riffdb-grpc-loopback";
const ENVIRONMENT: &str = "wp410-gate-a";
const LINEAGE: &str = "TicketDeskGateA";
const READY_PREFIX: &str = "riffdbd-ready-v1\t";
const SHUTDOWN: &[u8] = b"shutdown\n";
const START_TIMEOUT: Duration = Duration::from_secs(60);
const STOP_TIMEOUT: Duration = Duration::from_secs(20);
const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const CAPABILITY_LIFETIME_SECONDS: u32 = 3_600;
const VALID_TICKET_COUNT: usize = 70;
const ORGANIZATION_ID: [u8; 16] = [0x41; 16];

const CAPABILITY_KEYS: &[u8] = b"riffdb-capability-digest-keys-v1\n7:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEYS: &[u8] = b"riffdb-idempotency-digest-keys-v1\n9:202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f\n";
const PARENT_SOURCE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-a/ticketdesk/parent.riff"
));
const PARENT_BUNDLE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-a/ticketdesk/parent.contract.bundle"
));
const SUCCESSOR_BUNDLE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-a/ticketdesk/successor.contract.bundle"
));
const MIGRATION_BUNDLE: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/migrations/gate-a/ticketdesk/v1-to-v2.migration.bundle"
));

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn main() -> ExitCode {
    if std::env::var(CHILD_MODE).as_deref() == Ok(CHILD_RIFFDBD) {
        return riffdb_server::riffdbd_main();
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("contract_migration_gate_a: runtime construction failed: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run_gate()) {
        Ok(report) => {
            println!(
                "contract_migration_gate_a: passed rows={} check_rows_per_second={} apply_rows_per_second={} downtime_ms={} peak_disk_bytes={}",
                report.rows,
                report.check_rows_per_second,
                report.apply_rows_per_second,
                report.downtime_ms,
                report.peak_disk_bytes,
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("contract_migration_gate_a failed: {error}");
            ExitCode::FAILURE
        }
    }
}

struct GateReport {
    rows: usize,
    check_rows_per_second: u128,
    apply_rows_per_second: u128,
    downtime_ms: u128,
    peak_disk_bytes: u64,
}

async fn run_gate() -> TestResult<GateReport> {
    let parent = ContractBundle::decode(PARENT_BUNDLE)?;
    let successor = ContractBundle::decode(SUCCESSOR_BUNDLE)?;
    let migration = MigrationBundleV1::decode(MIGRATION_BUNDLE)?;
    verify_exact_fixture(&parent, &successor, &migration)?;

    let fixture = ProcessFixture::new()?;
    let credential = generate_bootstrap_credential(1_786_000_000_000, &SystemEntropy)?;
    let bearer = BearerCredential::new(token_text(&credential)?)?;
    let selected = DatabaseAlias::new("selected")?;
    let sibling = DatabaseAlias::new("sibling")?;
    let selected_metadata =
        CallMetadata::authenticated(bearer.clone()).with_database(selected.clone());
    let sibling_metadata = CallMetadata::authenticated(bearer).with_database(sibling.clone());

    let mut process = fixture.spawn()?;
    let address = parse_ready_address(&process.wait_for_readiness(READY_PREFIX, START_TIMEOUT)?)?;
    let mut client = connect(address).await?;
    bootstrap_and_deploy(
        &mut client,
        &credential,
        &selected_metadata,
        selected.clone(),
        &parent,
    )
    .await?;
    bootstrap_and_deploy(
        &mut client,
        &credential,
        &sibling_metadata,
        sibling,
        &parent,
    )
    .await?;
    seed_valid_rows(&mut client, &selected_metadata, &parent).await?;
    seed_invalid_rows(&mut client, &sibling_metadata, &parent).await?;

    let invalid_check = CheckContractMigration::new(
        generate_contract_migration_operation_id()?,
        SUCCESSOR_BUNDLE.to_vec(),
        MIGRATION_BUNDLE.to_vec(),
    )?;
    let invalid = bounded_rpc(
        "invalid predecessor check",
        client.check_contract_migration_with_retry(
            &invalid_check,
            one_attempt(),
            &sibling_metadata,
        ),
    )
    .await?;
    let invalid_operation = invalid
        .operation
        .ok_or_else(|| test_failure("invalid check omitted its operation"))?;
    if invalid_operation.phase != v1::ContractMigrationPhase::FailedClosed as i32
        || invalid_operation.failure != v1::ContractMigrationFailureClass::InvalidPredecessor as i32
    {
        return Err(test_failure(format!(
            "invalid predecessor did not fail closed: phase={} failure={}",
            invalid_operation.phase, invalid_operation.failure
        )));
    }

    drop(client);
    process.shutdown_cleanly(SHUTDOWN, STOP_TIMEOUT)?;
    let targets = ticket_targets(&parent, VALID_TICKET_COUNT)?;
    let before_request = DurableInspectionRequest::new(targets.clone(), Vec::new())?;
    let before = inspect_redb(
        fixture.selected_database(),
        startup_inputs()?,
        &before_request,
    )?;
    if before.commits().len() != VALID_TICKET_COUNT + 1
        || before.events().len() != VALID_TICKET_COUNT
        || before.provenance().len() != VALID_TICKET_COUNT + 1
    {
        return Err(test_failure("retained predecessor history was incomplete"));
    }
    let predecessor_bytes = fs::metadata(fixture.selected_database())?.len();

    let mut process = fixture.spawn()?;
    let address = parse_ready_address(&process.wait_for_readiness(READY_PREFIX, START_TIMEOUT)?)?;
    let mut client = connect(address).await?;
    let check = CheckContractMigration::new(
        generate_contract_migration_operation_id()?,
        SUCCESSOR_BUNDLE.to_vec(),
        MIGRATION_BUNDLE.to_vec(),
    )?;
    let check_started = Instant::now();
    let checked = bounded_rpc(
        "valid predecessor check",
        client.check_contract_migration_with_retry(&check, one_attempt(), &selected_metadata),
    )
    .await?;
    assert_terminal_success(checked.operation.as_ref(), "check")?;
    let check_elapsed = check_started.elapsed();

    let mut wrong_migration = MIGRATION_BUNDLE.to_vec();
    let final_byte = wrong_migration.len() - 1;
    wrong_migration[final_byte] ^= 0x01;
    let mismatched = CheckContractMigration::new(
        check.operation_id(),
        SUCCESSOR_BUNDLE.to_vec(),
        wrong_migration,
    )?;
    if client
        .check_contract_migration_with_retry(&mismatched, one_attempt(), &selected_metadata)
        .await
        .is_ok()
    {
        return Err(test_failure(
            "same operation accepted different exact bytes",
        ));
    }

    let apply_operation = generate_contract_migration_operation_id()?;
    let apply = ApplyContractMigration::new(
        apply_operation,
        SUCCESSOR_BUNDLE.to_vec(),
        MIGRATION_BUNDLE.to_vec(),
        migration.bundle_hash(),
    )?;
    let apply_started = Instant::now();
    let accepted = bounded_rpc(
        "migration apply",
        client.apply_contract_migration_with_retry(&apply, one_attempt(), &selected_metadata),
    )
    .await?;
    if accepted.disposition != v1::ContractMigrationStartDisposition::Accepted as i32 {
        return Err(test_failure("migration apply was not newly accepted"));
    }

    bounded_rpc(
        "sibling health during selected migration",
        client.health(
            v1::HealthRequest {
                request_id: Some(fresh_request_id_bytes()?),
            },
            &sibling_metadata,
        ),
    )
    .await?;
    drop(client);

    let mut recovered = connect_eventually(address).await?;
    let terminal =
        poll_terminal_migration(&mut recovered, address, apply_operation, &selected_metadata)
            .await?;
    assert_terminal_success(Some(&terminal), "apply")?;
    let apply_elapsed = apply_started.elapsed();

    let active = bounded_rpc(
        "active successor",
        recovered.get_active_contract(
            v1::GetActiveContractRequest {
                request_id: fresh_request_id_bytes()?,
            },
            &selected_metadata,
        ),
    )
    .await?;
    let Some(v1::get_active_contract_response::Result::Present(active)) = active.result else {
        return Err(test_failure("successor activation omitted active contract"));
    };
    if active.contract_lineage != LINEAGE || active.contract_version != 2 {
        return Err(test_failure("migration did not activate exact successor"));
    }

    let stale = ApplyContractMigration::new(
        generate_contract_migration_operation_id()?,
        SUCCESSOR_BUNDLE.to_vec(),
        MIGRATION_BUNDLE.to_vec(),
        migration.bundle_hash(),
    )?;
    let stale_result = recovered
        .apply_contract_migration_with_retry(&stale, one_attempt(), &selected_metadata)
        .await?;
    if stale_result.disposition != v1::ContractMigrationStartDisposition::AlreadyApplied as i32 {
        return Err(test_failure(
            "stale parent did not resolve as already applied",
        ));
    }

    drop(recovered);
    process.shutdown_cleanly(SHUTDOWN, STOP_TIMEOUT)?;

    let projection = projection_identity(&successor)?;
    let after_request = DurableInspectionRequest::new(targets, vec![projection])?;
    let after = inspect_redb(
        fixture.selected_database(),
        startup_inputs()?,
        &after_request,
    )?;
    if before.commits() != after.commits()
        || before.provenance() != after.provenance()
        || before.events() != after.events()
    {
        return Err(test_failure(
            "migration rewrote immutable application history",
        ));
    }
    assert_migrated_rows(&after, &successor)?;
    let status = after
        .projections()
        .first()
        .ok_or_else(|| test_failure("rebuilt projection status was absent"))?;
    let expected_frontier = riffdb_types::CommitSequence::new((VALID_TICKET_COUNT + 1) as u64)
        .ok_or_else(|| test_failure("fixture frontier was zero"))?;
    if status.lifecycle() != ProjectionLifecycleV1::Ready
        || status.published().map(|value| value.frontier())
            != Some(FrontierPosition::AppliedThrough(expected_frontier))
        || status.authoritative_head() != FrontierPosition::AppliedThrough(expected_frontier)
    {
        return Err(test_failure(
            "projection did not publish the frozen frontier",
        ));
    }
    let next_sequence = riffdb_types::CommitSequence::new(expected_frontier.get() + 1)
        .ok_or_else(|| test_failure("application sequence fixture overflowed"))?;
    if after.metadata().application_sequence() != ApplicationSequenceAllocator::next(next_sequence)
    {
        return Err(test_failure("migration changed the application sequence"));
    }

    let backup_bytes = directory_file_bytes(fixture.selected_backup_root())?;
    let successor_bytes = fs::metadata(fixture.selected_database())?.len();
    let peak_disk_bytes = predecessor_bytes
        .checked_add(backup_bytes)
        .and_then(|value| value.checked_add(successor_bytes))
        .ok_or_else(|| test_failure("disk baseline overflowed"))?;
    Ok(GateReport {
        rows: VALID_TICKET_COUNT,
        check_rows_per_second: rows_per_second(VALID_TICKET_COUNT, check_elapsed),
        apply_rows_per_second: rows_per_second(VALID_TICKET_COUNT, apply_elapsed),
        downtime_ms: apply_elapsed.as_millis(),
        peak_disk_bytes,
    })
}

fn verify_exact_fixture(
    parent: &ContractBundle,
    successor: &ContractBundle,
    migration: &MigrationBundleV1,
) -> TestResult<()> {
    let compiled_parent = riffdb_contract_compiler::compile_contract_source(PARENT_SOURCE)?;
    if compiled_parent.canonical_bytes() != PARENT_BUNDLE
        || parent.lineage().as_str() != LINEAGE
        || successor.parent().is_none_or(|identity| {
            identity.bundle_hash() != parent.bundle_hash()
                || identity.contract_version() != parent.contract_version()
        })
        || migration.parent_bundle_hash() != parent.bundle_hash()
        || migration.candidate_bundle_hash() != successor.bundle_hash()
    {
        return Err(test_failure(
            "checked Gate-A artifacts are not one exact lock set",
        ));
    }
    let metadata = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/migrations/gate-a/ticketdesk/exact-artifacts.txt"
    ));
    if !metadata.contains(&hex(parent.bundle_hash().as_bytes()))
        || !metadata.contains(&hex(successor.bundle_hash().as_bytes()))
        || !metadata.contains(&hex(migration.bundle_hash().as_bytes()))
    {
        return Err(test_failure("exact artifact manifest drifted"));
    }
    Ok(())
}

async fn bootstrap_and_deploy(
    client: &mut RiffDbClient,
    credential: &RetainedBootstrapCredential,
    authenticated: &CallMetadata,
    database: DatabaseAlias,
    parent: &ContractBundle,
) -> TestResult<()> {
    let request = bootstrap_request(credential, parent)?;
    riffdb_proto::validate_public_message(&request).map_err(|error| {
        test_failure(format!(
            "bootstrap request failed public validation: {error:?}"
        ))
    })?;
    let bootstrap =
        BootstrapCallMetadata::new(TransportBootstrapCredential::new(token_text(credential)?)?)
            .with_database(database);
    let response = bounded_rpc(
        "bootstrap capability",
        client.create_bootstrap_capability(request, &bootstrap),
    )
    .await?;
    if !matches!(
        response.result,
        Some(v1::create_capability_response::Result::Bootstrap(
            v1::BootstrapCreateCapabilityResult {
                result: Some(v1::bootstrap_create_capability_result::Result::Created(_))
            }
        ))
    ) {
        return Err(test_failure("bootstrap capability was not created"));
    }
    let deployed = bounded_rpc(
        "parent deployment",
        client.deploy_contract(
            v1::DeployContractRequest {
                request_id: fresh_request_id_bytes()?,
                source: PARENT_SOURCE.to_owned(),
                expected_active_version: None,
                expected_active_bundle_hash: Vec::new(),
                expected_candidate_bundle_hash: parent.bundle_hash().into_bytes().to_vec(),
            },
            authenticated,
        ),
    )
    .await?;
    if !matches!(
        deployed.result,
        Some(v1::deploy_contract_response::Result::Activated(_))
    ) {
        return Err(test_failure("parent contract was not activated"));
    }
    Ok(())
}

fn bootstrap_request(
    credential: &RetainedBootstrapCredential,
    parent: &ContractBundle,
) -> TestResult<v1::CreateCapabilityRequest> {
    use v1::capability_permission::Permission;
    let scoped = |stable_id| v1::LineageScopedStableId {
        contract_lineage: LINEAGE.to_owned(),
        stable_id,
    };
    let mut permissions = vec![
        permission(Permission::ReadContract(v1::Unit {})),
        permission(Permission::DeployContract(v1::Unit {})),
    ];
    permissions.extend(parent.commands().iter().map(|command| {
        permission(Permission::InvokeCommand(scoped(
            command.command_id().get(),
        )))
    }));
    permissions.push(permission(Permission::ReadHealth(v1::Unit {})));
    permissions.push(permission(Permission::AdministerCapabilities(v1::Unit {})));
    permissions.push(permission(Permission::MigrateContract(LINEAGE.to_owned())));
    Ok(v1::CreateCapabilityRequest {
        request_id: fresh_request_id_bytes()?,
        mode: v1::CapabilityCreateMode::Bootstrap as i32,
        capability_id: credential.capability_id().into_bytes().to_vec(),
        principal_id: "wp410-maintainer".to_owned(),
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
            permissions,
            field_visibility: parent
                .schema()
                .entities()
                .iter()
                .map(|entity| v1::EntityFieldVisibility {
                    contract_lineage: LINEAGE.to_owned(),
                    entity_type_id: entity.id().get(),
                    field_ids: entity
                        .record()
                        .fields()
                        .iter()
                        .map(|field| field.id().get())
                        .collect(),
                })
                .collect(),
            max_scan_rows: 100,
            approval_required: Vec::new(),
        }),
    })
}

async fn seed_valid_rows(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    parent: &ContractBundle,
) -> TestResult<()> {
    execute(
        client,
        metadata,
        command(
            parent,
            "CreateOrganization",
            vec![
                ("idempotency_key", string_value("organization")),
                ("organization_id", uuid_value(ORGANIZATION_ID)),
                ("name", string_value("Migration fixture")),
            ],
        )?,
    )
    .await?;
    for ordinal in 0..VALID_TICKET_COUNT {
        execute(
            client,
            metadata,
            command(
                parent,
                "CreateTicket",
                vec![
                    (
                        "idempotency_key",
                        string_value(&format!("ticket-{ordinal}")),
                    ),
                    ("organization_id", uuid_value(ORGANIZATION_ID)),
                    ("ticket_id", uuid_value(ticket_id(ordinal))),
                    ("title", string_value(&format!("Ticket {ordinal:03}"))),
                    ("amount", i64_value(ordinal as i64)),
                ],
            )?,
        )
        .await?;
    }
    Ok(())
}

async fn seed_invalid_rows(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    parent: &ContractBundle,
) -> TestResult<()> {
    execute(
        client,
        metadata,
        command(
            parent,
            "CreateOrganization",
            vec![
                ("idempotency_key", string_value("invalid-organization")),
                ("organization_id", uuid_value(ORGANIZATION_ID)),
                ("name", string_value("Invalid migration fixture")),
            ],
        )?,
    )
    .await?;
    execute(
        client,
        metadata,
        command(
            parent,
            "CreateTicket",
            vec![
                ("idempotency_key", string_value("negative-ticket")),
                ("organization_id", uuid_value(ORGANIZATION_ID)),
                ("ticket_id", uuid_value(ticket_id(900))),
                ("title", string_value("Invalid ticket")),
                ("amount", i64_value(-1)),
            ],
        )?,
    )
    .await
}

async fn execute(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    command: IdempotentCommand,
) -> TestResult<()> {
    let response = bounded_rpc(
        "retained command",
        client.execute_with_retry(&command, one_attempt(), metadata),
    )
    .await?;
    if response.status != v1::execute_command_response::CompletionStatus::Committed as i32 {
        return Err(test_failure("retained command did not commit"));
    }
    Ok(())
}

fn command(
    parent: &ContractBundle,
    name: &str,
    values: Vec<(&str, v1::Value)>,
) -> TestResult<IdempotentCommand> {
    let plan = parent
        .commands()
        .iter()
        .find(|command| command.name() == name)
        .ok_or_else(|| test_failure("fixture command was absent"))?;
    let mut fields = plan
        .input()
        .record()
        .fields()
        .iter()
        .map(|field| {
            let value = values
                .iter()
                .find_map(|(name, value)| (field.name() == *name).then(|| value.clone()))
                .ok_or_else(|| test_failure(format!("missing input {}", field.name())))?;
            Ok(v1::ValueField {
                field_id: Some(field.id().get()),
                name: String::new(),
                value: Some(value),
            })
        })
        .collect::<TestResult<Vec<_>>>()?;
    fields.sort_unstable_by_key(|field| field.field_id);
    Ok(IdempotentCommand::new(
        name,
        Some(parent.contract_version().get()),
        v1::Value {
            kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord { fields })),
        },
    )?)
}

async fn poll_terminal_migration(
    client: &mut RiffDbClient,
    address: SocketAddr,
    operation_id: riffdb_types::ContractMigrationOperationId,
    metadata: &CallMetadata,
) -> TestResult<v1::ContractMigrationOperation> {
    timeout(START_TIMEOUT, async {
        loop {
            let request = v1::GetContractMigrationOperationRequest {
                request_id: fresh_request_id_bytes()?,
                operation_id: operation_id.into_bytes().to_vec(),
            };
            match client
                .get_contract_migration_operation(request, metadata)
                .await
            {
                Ok(response) => {
                    if let Some(v1::get_contract_migration_operation_response::Result::Found(
                        operation,
                    )) = response.result
                        && matches!(
                            v1::ContractMigrationPhase::try_from(operation.phase),
                            Ok(v1::ContractMigrationPhase::Succeeded
                                | v1::ContractMigrationPhase::FailedClosed
                                | v1::ContractMigrationPhase::FailedRolledBack)
                        )
                    {
                        return Ok(operation);
                    }
                }
                Err(_) => {
                    *client = connect_eventually(address).await?;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| test_failure("migration did not become terminal"))?
}

fn assert_terminal_success(
    operation: Option<&v1::ContractMigrationOperation>,
    label: &str,
) -> TestResult<()> {
    let operation = operation.ok_or_else(|| test_failure(format!("{label} omitted operation")))?;
    if operation.phase != v1::ContractMigrationPhase::Succeeded as i32
        || operation.failure != v1::ContractMigrationFailureClass::Unspecified as i32
    {
        return Err(test_failure(format!(
            "{label} did not succeed: phase={} failure={}",
            operation.phase, operation.failure
        )));
    }
    Ok(())
}

fn assert_migrated_rows(
    inspection: &riffdb_testkit::inspection::DurableInspection,
    successor: &ContractBundle,
) -> TestResult<()> {
    let ticket = successor
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Ticket")
        .ok_or_else(|| test_failure("successor Ticket schema absent"))?;
    let amount = field_id(ticket, "amount")?;
    let priority = field_id(ticket, "priority")?;
    for entity in inspection.entities() {
        let record = entity
            .record()
            .ok_or_else(|| test_failure("migrated ticket disappeared"))?;
        if record.entity_version() != EntityVersion::new(2).expect("version two")
            || record.schema_binding().bundle_hash() != successor.bundle_hash()
        {
            return Err(test_failure(
                "migrated ticket retained predecessor identity",
            ));
        }
        let amount = canonical_i64(record.fields(), amount)?;
        let priority = canonical_i64(record.fields(), priority)?;
        if priority != amount + 1 {
            return Err(test_failure("required-field backfill value was incorrect"));
        }
    }
    Ok(())
}

fn canonical_i64(record: &riffdb_types::CanonicalRecord, field: FieldId) -> TestResult<i64> {
    match record
        .fields()
        .iter()
        .find_map(|(candidate, value)| (*candidate == field).then_some(value))
    {
        Some(CanonicalValue::I64(value)) => Ok(*value),
        _ => Err(test_failure("migrated i64 field was absent")),
    }
}

fn field_id(entity: &riffdb_contract_ir::EntitySchema, name: &str) -> TestResult<FieldId> {
    entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == name)
        .map(riffdb_contract_ir::FieldSchema::id)
        .ok_or_else(|| test_failure(format!("field {name} was absent")))
}

fn ticket_targets(parent: &ContractBundle, count: usize) -> TestResult<Vec<EntityTarget>> {
    let ticket = parent
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Ticket")
        .ok_or_else(|| test_failure("parent Ticket schema absent"))?;
    (0..count)
        .map(|ordinal| {
            let mut key = EntityKeyBuilder::new(ticket.id());
            key.push_uuid(&ORGANIZATION_ID)?;
            key.push_uuid(&ticket_id(ordinal))?;
            Ok(EntityTarget::new(ticket.id(), key.finish()?)?)
        })
        .collect()
}

fn projection_identity(successor: &ContractBundle) -> TestResult<ProjectionIdentity> {
    let projection = successor
        .projections()
        .iter()
        .find(|projection| projection.name() == "TicketTotals")
        .ok_or_else(|| test_failure("successor projection absent"))?;
    Ok(ProjectionIdentity::new(
        successor.lineage().clone(),
        projection.projection_id(),
        projection.plan_hash(),
    ))
}

fn ticket_id(ordinal: usize) -> [u8; 16] {
    let mut value = [0_u8; 16];
    value[..8].copy_from_slice(&(ordinal as u64 + 1).to_be_bytes());
    value[8..].fill(0x54);
    value
}

fn string_value(value: &str) -> v1::Value {
    v1::Value {
        kind: Some(v1::value::Kind::StringValue(value.to_owned())),
    }
}

fn uuid_value(value: [u8; 16]) -> v1::Value {
    v1::Value {
        kind: Some(v1::value::Kind::UuidValue(value.to_vec())),
    }
}

fn i64_value(value: i64) -> v1::Value {
    v1::Value {
        kind: Some(v1::value::Kind::I64Value(value)),
    }
}

fn permission(permission: v1::capability_permission::Permission) -> v1::CapabilityPermission {
    v1::CapabilityPermission {
        permission: Some(permission),
    }
}

fn one_attempt() -> AttemptBudget {
    AttemptBudget::new(1).expect("one is nonzero")
}

fn fresh_request_id_bytes() -> TestResult<Vec<u8>> {
    Ok(generate_request_id()?.into_bytes().to_vec())
}

fn token_text(credential: &RetainedBootstrapCredential) -> TestResult<&str> {
    Ok(str::from_utf8(credential.token().expose_secret())?)
}

fn startup_inputs() -> TestResult<StartupValidationInputs> {
    Ok(StartupValidationInputs::new(
        Timestamp::new(1_786_000_100, 0)?,
        ReadableCapabilityDigestInventory::new(vec![ReadableDigestKey::v1(
            DigestKeyId::new(7).expect("capability key ID"),
        )])?,
        ReadableIdempotencyDigestInventory::new(vec![ReadableDigestKey::v1(
            DigestKeyId::new(9).expect("idempotency key ID"),
        )])?,
    ))
}

async fn connect(address: SocketAddr) -> TestResult<RiffDbClient> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(RPC_TIMEOUT)
        .timeout(RPC_TIMEOUT);
    bounded_rpc("gRPC connection", RiffDbClient::connect(endpoint)).await
}

async fn connect_eventually(address: SocketAddr) -> TestResult<RiffDbClient> {
    timeout(START_TIMEOUT, async move {
        loop {
            match connect(address).await {
                Ok(client) => return Ok(client),
                Err(_) => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .map_err(|_| test_failure("daemon did not reopen selected database"))?
}

async fn bounded_rpc<T, E>(
    label: &'static str,
    future: impl Future<Output = Result<T, E>>,
) -> TestResult<T>
where
    E: std::fmt::Debug + std::fmt::Display,
{
    match timeout(RPC_TIMEOUT, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(test_failure(format!("{label} failed: {error} ({error:?})"))),
        Err(_) => Err(test_failure(format!("{label} exceeded its deadline"))),
    }
}

fn parse_ready_address(line: &str) -> TestResult<SocketAddr> {
    let address = line
        .strip_prefix(READY_PREFIX)
        .ok_or_else(|| test_failure("daemon readiness prefix changed"))?
        .parse::<SocketAddr>()?;
    if !address.ip().is_loopback() || address.port() == 0 {
        return Err(test_failure("daemon readiness address was invalid"));
    }
    Ok(address)
}

fn rows_per_second(rows: usize, elapsed: Duration) -> u128 {
    let nanos = elapsed.as_nanos().max(1);
    (rows as u128).saturating_mul(1_000_000_000) / nanos
}

fn directory_file_bytes(path: &Path) -> TestResult<u64> {
    let mut total = 0_u64;
    let mut pending = vec![path.to_owned()];
    while let Some(current) = pending.pop() {
        for entry in fs::read_dir(current)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() {
                total = total
                    .checked_add(metadata.len())
                    .ok_or_else(|| test_failure("disk baseline overflowed"))?;
            }
        }
    }
    Ok(total)
}

fn hex(value: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

struct ProcessFixture {
    _directory: TemporaryDirectory,
    selected_database: PathBuf,
    selected_backup: PathBuf,
    config: PathBuf,
}

impl ProcessFixture {
    fn new() -> TestResult<Self> {
        let directory = TemporaryDirectory::new()?;
        let selected_database = directory.path().join("selected.redb");
        let selected_backup = directory.path().join("selected-backups");
        let sibling_database = directory.path().join("sibling.redb");
        let sibling_backup = directory.path().join("sibling-backups");
        let capability_keys = directory.path().join("capability.keys");
        let idempotency_keys = directory.path().join("idempotency.keys");
        fs::create_dir(&selected_backup)?;
        fs::create_dir(&sibling_backup)?;
        write_protected_file(&capability_keys, CAPABILITY_KEYS)?;
        write_protected_file(&idempotency_keys, IDEMPOTENCY_KEYS)?;
        let config = directory.path().join("riffdb.toml");
        fs::write(
            &config,
            format!(
                "[server]\n\
                 grpc_listen = \"127.0.0.1:0\"\n\
                 audience = {AUDIENCE:?}\n\
                 capability_keys = {capability_keys:?}\n\
                 idempotency_keys = {idempotency_keys:?}\n\
                 \n\
                 [databases.selected]\n\
                 path = {selected_database:?}\n\
                 backup_root = {selected_backup:?}\n\
                 environment = {ENVIRONMENT:?}\n\
                 \n\
                 [databases.sibling]\n\
                 path = {sibling_database:?}\n\
                 backup_root = {sibling_backup:?}\n\
                 environment = {ENVIRONMENT:?}\n"
            ),
        )?;
        Ok(Self {
            _directory: directory,
            selected_database,
            selected_backup,
            config,
        })
    }

    fn spawn(&self) -> TestResult<ChildProcessController> {
        let specification = ChildProcessSpec::new(std::env::current_exe()?)?
            .env(CHILD_MODE, CHILD_RIFFDBD)?
            .arg("--config")?
            .arg(self.config.as_os_str())?;
        Ok(ChildProcessController::spawn(&specification)?)
    }

    fn selected_database(&self) -> &Path {
        &self.selected_database
    }

    fn selected_backup_root(&self) -> &Path {
        &self.selected_backup
    }
}

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn new() -> io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/test-tmp");
        fs::create_dir_all(&base)?;
        let base = fs::canonicalize(base)?;
        for _ in 0..1_024 {
            let ordinal = NEXT.fetch_add(1, Ordering::Relaxed);
            let path = base.join(format!("wp410-{}-{ordinal}", std::process::id()));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate WP-410 test directory",
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

fn write_protected_file(path: &Path, contents: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(contents)?;
    file.sync_all()?;
    drop(file);
    File::open(
        path.parent()
            .ok_or_else(|| io::Error::other("protected path has no parent"))?,
    )?
    .sync_all()
}

fn test_failure(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::other(message.into()))
}
