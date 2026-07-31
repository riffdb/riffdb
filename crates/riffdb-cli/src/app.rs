use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, ExitCode};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use clap::Parser;
use riffdb_client_rust::{
    ApplicationError, ApplicationErrorContext, ApplicationOperation, AttemptBudget, BackupNameV1,
    BootstrapCapabilityCreateTemplate, CallMetadata, ClientError, CreateOfflineBackup,
    IdempotentCommand, NormalCapabilityCreateTemplate, OfflineMaintenanceOperationId,
    OfflineMaintenanceReplacementConfirmation, RestoreOfflineBackup, RiffDbClient, app_v1,
    generate_capability_id, generate_offline_maintenance_operation_id, generate_request_id, v1,
};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_diagnostics::{AuthoringDiagnostics, AuthoringSourcePath};
use riffdb_query_module::{
    ApplicationManifest, CompiledApplicationRole, NamedQuerySource, QueryModule,
    QueryModuleCandidate, QueryModuleName, QueryModuleVersion, compile_application_role,
};
use riffdb_types::{
    CapabilityGrantV1, CapabilityPermissionV1, PartitionScopeV1, TenantId, TenantScope,
};
use serde::{Deserialize, Serialize};

use crate::batch::{
    BatchError, BatchOptions, BatchReport, MAX_BATCH_CONCURRENCY, MAX_BATCH_SOURCE_BYTES,
    execute as execute_batch, parse_source as parse_batch_source,
};
use crate::cli::{
    ApplicationCommand, ApplicationLanguage, BackupCommand, CapabilityCommand, Cli, CommandCommand,
    CommitCommand, ContractCommand, ContractSelectionArgs, DemoCommand, EntityCommand,
    MigrationCommand, OutputMode, ProjectionCommand, QueryCommand, RevocationReason, RoleActorKind,
    RoleCommand, ServerCommand, TopLevel,
};
use crate::config::{EffectiveConfig, Environment, ProcessEnvironment, resolve};
use crate::credential::{
    CredentialError, bootstrap_material, normal_credential, retain_normal_token,
};
use crate::input::{
    InputError, MAX_INPUT_BYTES, read_file, read_path_or_stdin, utf8, validate_path,
};
use crate::output::{
    CommandIdentity, NormalCreateDisposition, Terminal, authoring_error, client_error, local_error,
    local_error_with, maintenance_uncertain, render_bootstrap, render_commit,
    render_compilation_diagnostics, render_contract_deploy, render_contract_validation,
    render_create_maintenance_start, render_entity, render_execution, render_health,
    render_maintenance_operation, render_normal_create, render_outcome, render_projection,
    render_restore_maintenance_start, render_revoke, success, take_normal_create_disposition,
    uncertain,
};
use crate::runner::{RunnerError, RunnerStream, run_budget};
use crate::value::format_uuid;

const APPLICATION_DEPLOYMENT_STATE_SCHEMA: &str = "riffdb.application-deployment-state/v1";
const APPLICATION_DEPLOYMENT_TEST_INTERRUPT_AFTER: &str = "RIFFDB_APPLICATION_TEST_INTERRUPT_AFTER";
const APPLICATION_DEPLOYMENT_TEST_INTERRUPT_EXIT: i32 = 86;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplicationDeploymentState {
    schema: String,
    database: String,
    lock_hash: String,
    contract_deployed: bool,
    #[serde(default)]
    contract_bundle_hash: String,
    query_modules_deployed: Vec<String>,
    #[serde(default)]
    query_module_identities: Vec<ApplicationDeploymentQueryModuleState>,
    role: Option<ApplicationDeploymentRoleState>,
    seeds_completed: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplicationDeploymentQueryModuleState {
    module_name: String,
    module_version: u64,
    module_hash: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApplicationDeploymentRoleState {
    role_name: String,
    #[serde(default)]
    role_identity: String,
    capability_id: String,
    bound: bool,
    authentication_audience: Option<String>,
}

#[derive(Serialize)]
struct BatchCheckpointContractVersionError<'a> {
    code: &'static str,
    message: &'static str,
    checkpoint_file: &'a str,
    checkpoint_contract_version: Option<u64>,
    requested_contract_version: Option<u64>,
    recovery_action: &'static str,
}

#[derive(Serialize)]
struct ApplicationModuleIdentityError<'a> {
    code: &'static str,
    message: &'static str,
    database: &'a str,
    lock_hash: &'a str,
    module_name: &'a str,
    locked_module_hash: String,
    expected_active_module_hash: Option<String>,
    actual_active_module_hash: Option<String>,
    recovery_action: &'static str,
}

#[derive(Serialize)]
struct ApplicationContractIdentityError<'a> {
    code: &'static str,
    message: &'static str,
    database: &'a str,
    lock_hash: String,
    expected_parent_version: Option<u64>,
    expected_parent_bundle_hash: Option<String>,
    locked_candidate_bundle_hash: String,
    actual_active_version: Option<u64>,
    actual_active_bundle_hash: Option<String>,
    compiled_candidate_bundle_hash: Option<String>,
    recovery_action: &'static str,
}

#[derive(Serialize)]
struct ApplicationLockResult {
    status: &'static str,
}

#[derive(Serialize)]
struct ApplicationRoleIdentityError<'a> {
    code: &'static str,
    message: &'static str,
    database: &'a str,
    lock_hash: &'a str,
    role_name: &'a str,
    retained_role_hash: Option<&'a str>,
    locked_role_hash: Option<&'a str>,
    recovery_action: &'static str,
}

struct PreparedRoleBinding {
    role: CompiledApplicationRole,
    principal: String,
    actor_kind: RoleActorKind,
    lifetime_seconds: String,
    audiences: Vec<String>,
    capability_id: Option<String>,
    credential_output: OsString,
}

fn interrupt_application_deployment_after(environment: &dyn Environment, stage: &str) {
    if environment
        .value(APPLICATION_DEPLOYMENT_TEST_INTERRUPT_AFTER)
        .is_some_and(|value| value.as_encoded_bytes() == stage.as_bytes())
    {
        std::process::exit(APPLICATION_DEPLOYMENT_TEST_INTERRUPT_EXIT);
    }
}
use crate::scaffold::{
    ApplicationCheckStatus, PinnedLockRefresh, ScaffoldLanguage, application_contract_source,
    application_contract_version, check_application, check_application_lock, create_application,
    generate_application, load_locked_application, migrate_application_source_v2,
    plan_application_migrations, preview_application_lock,
    refresh_application_lock_from_pinned_bundle, write_application_lock,
    write_application_lock_with_bundle,
};
use crate::value::{InputValue, RecordInput, ValueError, parse_uuid};

const DEFAULT_PAGE_LIMIT: u32 = 50;

trait RetryOperations {
    async fn execute_retry(
        &mut self,
        command: &IdempotentCommand,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<v1::ExecuteCommandResponse, ClientError>;

    async fn bootstrap_retry(
        &mut self,
        template: &BootstrapCapabilityCreateTemplate,
        attempts: AttemptBudget,
        metadata: &riffdb_client_rust::BootstrapCallMetadata,
    ) -> Result<v1::CreateCapabilityResponse, ClientError>;

    async fn normal_create_retry(
        &mut self,
        template: &NormalCapabilityCreateTemplate,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<v1::CreateCapabilityResponse, ClientError>;
}

impl RetryOperations for RiffDbClient {
    async fn execute_retry(
        &mut self,
        command: &IdempotentCommand,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<v1::ExecuteCommandResponse, ClientError> {
        self.execute_with_retry(command, attempts, metadata).await
    }

    async fn bootstrap_retry(
        &mut self,
        template: &BootstrapCapabilityCreateTemplate,
        attempts: AttemptBudget,
        metadata: &riffdb_client_rust::BootstrapCallMetadata,
    ) -> Result<v1::CreateCapabilityResponse, ClientError> {
        self.create_bootstrap_capability_with_retry(template, attempts, metadata)
            .await
    }

    async fn normal_create_retry(
        &mut self,
        template: &NormalCapabilityCreateTemplate,
        attempts: AttemptBudget,
        metadata: &CallMetadata,
    ) -> Result<v1::CreateCapabilityResponse, ClientError> {
        self.create_capability_with_retry(template, attempts, metadata)
            .await
    }
}

async fn submit_execute_retry(
    client: &mut impl RetryOperations,
    command: &IdempotentCommand,
    attempts: AttemptBudget,
    metadata: &CallMetadata,
) -> Result<v1::ExecuteCommandResponse, ClientError> {
    client.execute_retry(command, attempts, metadata).await
}

async fn submit_bootstrap_retry(
    client: &mut impl RetryOperations,
    template: &BootstrapCapabilityCreateTemplate,
    attempts: AttemptBudget,
    metadata: &riffdb_client_rust::BootstrapCallMetadata,
) -> Result<v1::CreateCapabilityResponse, ClientError> {
    client.bootstrap_retry(template, attempts, metadata).await
}

async fn submit_normal_create_retry(
    client: &mut impl RetryOperations,
    template: &NormalCapabilityCreateTemplate,
    attempts: AttemptBudget,
    metadata: &CallMetadata,
) -> Result<v1::CreateCapabilityResponse, ClientError> {
    client
        .normal_create_retry(template, attempts, metadata)
        .await
}

/// Parses one process invocation and runs exactly one public CLI operation.
pub async fn run() -> ExitCode {
    let cli = Cli::parse();
    if let TopLevel::New {
        application,
        language,
        directory,
    } = &cli.command
    {
        let destination = directory.as_deref().map_or_else(
            || std::path::PathBuf::from(application),
            std::path::PathBuf::from,
        );
        let language = match language {
            ApplicationLanguage::Rust => ScaffoldLanguage::Rust,
            ApplicationLanguage::Typescript => ScaffoldLanguage::Typescript,
            ApplicationLanguage::Python => ScaffoldLanguage::Python,
        };
        return match create_application(application, language, &destination) {
            Ok(()) => {
                println!(
                    "created application `{application}` at {}",
                    destination.display()
                );
                ExitCode::SUCCESS
            }
            Err(error) => {
                emit_scaffold_failure("riffdb new failed", &error, cli.output);
                ExitCode::FAILURE
            }
        };
    }
    if let TopLevel::Migration {
        command: MigrationCommand::Plan { application, lock },
    } = &cli.command
    {
        return match plan_application_migrations(Path::new(application), Some(Path::new(lock))) {
            Ok(plan) => success(CommandIdentity::MigrationPlan, "planned", &plan).emit(
                cli.output.unwrap_or(OutputMode::Human),
                &mut io::stdout().lock(),
                &mut io::stderr().lock(),
            ),
            Err(error) => {
                emit_scaffold_failure("riffdb migration plan failed", &error, cli.output);
                ExitCode::FAILURE
            }
        };
    }
    let networked_successor_lock = matches!(
        &cli.command,
        TopLevel::Application {
            command: ApplicationCommand::Lock { source, write: true, .. }
        } if application_contract_version(Path::new(source)).is_ok_and(|version| version > 1)
    );
    if networked_successor_lock
        && let TopLevel::Application {
            command:
                ApplicationCommand::Lock {
                    source,
                    write: true,
                    lock,
                    ..
                },
        } = &cli.command
    {
        match refresh_application_lock_from_pinned_bundle(Path::new(source), Some(Path::new(lock)))
        {
            Ok(PinnedLockRefresh::Refreshed) => {
                println!(
                    "application sources, pinned contract bundle, lock, and generated bindings are exact"
                );
                return ExitCode::SUCCESS;
            }
            Ok(PinnedLockRefresh::ContractSourceChanged | PinnedLockRefresh::NotPinned) => {}
            Err(error) => {
                emit_scaffold_failure("riffdb application lock refresh failed", &error, cli.output);
                return ExitCode::FAILURE;
            }
        }
    }
    if let TopLevel::Application { command } = &cli.command
        && !matches!(
            command,
            ApplicationCommand::Deploy { .. } | ApplicationCommand::BindDevRole { .. }
        )
        && !networked_successor_lock
    {
        if let ApplicationCommand::Preview { source } = command {
            return match preview_application_lock(Path::new(source)) {
                Ok(lock) => {
                    use std::io::Write as _;

                    match io::stdout().lock().write_all(&lock) {
                        Ok(()) => ExitCode::SUCCESS,
                        Err(_) => ExitCode::FAILURE,
                    }
                }
                Err(error) => {
                    emit_scaffold_failure("riffdb application failed", &error, cli.output);
                    ExitCode::FAILURE
                }
            };
        }
        if let ApplicationCommand::Migrate { source, to, write } = command {
            debug_assert_eq!(to, "v2");
            return match migrate_application_source_v2(Path::new(source), *write) {
                Ok(source) => {
                    if *write {
                        println!(
                            "application source migrated to v2; lock and generated artifacts are unchanged"
                        );
                        ExitCode::SUCCESS
                    } else {
                        use std::io::Write as _;

                        match io::stdout().lock().write_all(&source) {
                            Ok(()) => ExitCode::SUCCESS,
                            Err(_) => ExitCode::FAILURE,
                        }
                    }
                }
                Err(error) => {
                    emit_scaffold_failure(
                        "riffdb application migration failed",
                        &error,
                        cli.output,
                    );
                    ExitCode::FAILURE
                }
            };
        }
        if let ApplicationCommand::Check { source } = command {
            return match check_application(Path::new(source)) {
                Ok(ApplicationCheckStatus::ExactLock) => {
                    println!("application sources, exact lock, and generated bindings are exact");
                    ExitCode::SUCCESS
                }
                Ok(ApplicationCheckStatus::SourceOnly) => {
                    println!(
                        "application sources compile; no lock or generated artifacts were checked"
                    );
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    emit_scaffold_failure("riffdb application failed", &error, cli.output);
                    ExitCode::FAILURE
                }
            };
        }
        let result = match command {
            ApplicationCommand::Migrate { .. } => unreachable!("migration returned above"),
            ApplicationCommand::Check { .. } => unreachable!("check returned above"),
            ApplicationCommand::Preview { .. } => unreachable!("preview returned above"),
            ApplicationCommand::Lock {
                source,
                write,
                check,
                lock,
            } => {
                debug_assert_ne!(write, check);
                if *write {
                    write_application_lock(Path::new(source), Some(Path::new(lock)))
                } else {
                    check_application_lock(Path::new(source), Some(Path::new(lock)))
                }
            }
            ApplicationCommand::Generate {
                manifest,
                locked,
                lock,
            } => generate_application(Path::new(manifest), *locked, Some(Path::new(lock))),
            ApplicationCommand::Deploy { .. } | ApplicationCommand::BindDevRole { .. } => {
                unreachable!("networked application operation was not intercepted")
            }
        };
        return match result {
            Ok(()) => {
                println!("application sources, lock, and generated bindings are exact");
                ExitCode::SUCCESS
            }
            Err(error) => {
                emit_scaffold_failure("riffdb application failed", &error, cli.output);
                ExitCode::FAILURE
            }
        };
    }
    if let TopLevel::Dev {
        role,
        watch,
        run,
        seed,
        seed_dir,
        seed_concurrency,
        acceptance,
    } = &cli.command
    {
        return run_dev(
            role,
            *watch,
            *run,
            *seed,
            seed_dir.as_deref(),
            seed_concurrency,
            *acceptance,
        );
    }
    let identity = command_identity(&cli.command);
    let environment = ProcessEnvironment;
    let config = match resolve(&cli, &environment) {
        Ok(config) => config,
        Err(error) => {
            return local_error(
                identity,
                "configuration_invalid",
                "client configuration is invalid",
            )
            .emit(
                error.output(),
                &mut io::stdout().lock(),
                &mut io::stderr().lock(),
            );
        }
    };
    let mut stdin = io::stdin().lock();
    let terminal = dispatch(cli.command, &config, &environment, &mut stdin).await;
    terminal.emit(
        config.output,
        &mut io::stdout().lock(),
        &mut io::stderr().lock(),
    )
}

fn emit_scaffold_failure(
    prefix: &str,
    error: &crate::scaffold::ScaffoldError,
    mode: Option<OutputMode>,
) {
    if mode == Some(OutputMode::Json)
        && let Some(diagnostics) = error.diagnostics()
        && let Ok(rendered) = diagnostics.render_json()
    {
        eprint!("{rendered}");
        return;
    }
    eprintln!("{prefix}: {error}");
}

fn run_dev(
    role: &str,
    watch: bool,
    run: bool,
    seed: bool,
    seed_dir: Option<&std::ffi::OsStr>,
    seed_concurrency: &str,
    acceptance: bool,
) -> ExitCode {
    let application_root = match std::env::current_dir() {
        Ok(directory) => directory,
        Err(_) => {
            eprintln!("riffdb dev requires an accessible application directory");
            return ExitCode::FAILURE;
        }
    };
    let current_executable = std::env::current_exe().ok();
    let Some(script) = resolve_dev_script(
        &application_root,
        current_executable.as_deref(),
        Path::new(env!("CARGO_MANIFEST_DIR")),
    ) else {
        eprintln!("riffdb dev workflow is unavailable in this installation");
        return ExitCode::FAILURE;
    };
    let mut command = ProcessCommand::new(script);
    command.args(["--role", role, "--seed-concurrency", seed_concurrency]);
    if application_root.join("riffdb.application.json").is_file() {
        command.arg("--application-root").arg(&application_root);
    }
    if watch {
        command.arg("--watch");
    }
    if run {
        command.arg("--run");
    }
    if seed {
        command.arg("--seed");
    }
    if let Some(seed_dir) = seed_dir {
        let seed_dir = Path::new(seed_dir);
        command.arg("--seed-dir").arg(if seed_dir.is_absolute() {
            seed_dir.to_path_buf()
        } else {
            application_root.join(seed_dir)
        });
    }
    if acceptance {
        command.arg("--acceptance");
    }
    match command.status() {
        Ok(status) if status.success() => ExitCode::SUCCESS,
        Ok(status) => ExitCode::from(
            status
                .code()
                .and_then(|code| u8::try_from(code).ok())
                .unwrap_or(1),
        ),
        Err(_) => {
            eprintln!("riffdb dev workflow could not be started");
            ExitCode::FAILURE
        }
    }
}

fn resolve_dev_script(
    application_root: &Path,
    current_executable: Option<&Path>,
    manifest_directory: &Path,
) -> Option<PathBuf> {
    if let Some(installed_script) = current_executable
        .and_then(Path::parent)
        .map(|directory| directory.join("riffdb-dev"))
        .filter(|candidate| candidate.is_file())
    {
        return Some(installed_script);
    }
    let workspace_script = application_root.join("scripts/riffdb-dev");
    if workspace_script.is_file() {
        return Some(workspace_script);
    }
    let source_script = manifest_directory.join("../../scripts/riffdb-dev");
    source_script.is_file().then_some(source_script)
}

async fn dispatch(
    command: TopLevel,
    config: &EffectiveConfig,
    environment: &dyn Environment,
    stdin: &mut dyn Read,
) -> Terminal {
    match command {
        TopLevel::Application { command } => {
            application_command(command, config, environment, stdin).await
        }
        TopLevel::Migration { .. } => local_error(
            CommandIdentity::MigrationPlan,
            "migration_plan_dispatch_invalid",
            "migration plan dispatch is invalid",
        ),
        TopLevel::New { .. } => local_error(
            CommandIdentity::ServerHealth,
            "new_dispatch_invalid",
            "application scaffold dispatch is invalid",
        ),
        TopLevel::Dev { .. } => local_error(
            CommandIdentity::ServerHealth,
            "dev_dispatch_invalid",
            "development workflow dispatch is invalid",
        ),
        TopLevel::Contract { command } => {
            contract_command(command, config, environment, stdin).await
        }
        TopLevel::Command { command } => command_command(command, config, environment, stdin).await,
        TopLevel::Entity { command } => entity_command(command, config, environment).await,
        TopLevel::Commit { command } => commit_command(command, config, environment).await,
        TopLevel::Projection { command } => {
            projection_command(command, config, environment, stdin).await
        }
        TopLevel::Query { command } => query_command(command, config, environment, stdin).await,
        TopLevel::Role { command } => role_command(command, config, environment).await,
        TopLevel::Capability { command } => {
            capability_command(command, config, environment, stdin).await
        }
        TopLevel::Server { command } => server_command(command, config, environment).await,
        TopLevel::Backup { command } => backup_command(command, config, environment).await,
        TopLevel::Demo { command } => demo_command(command, config, environment),
    }
}

async fn application_command(
    command: ApplicationCommand,
    config: &EffectiveConfig,
    environment: &dyn Environment,
    stdin: &mut dyn Read,
) -> Terminal {
    if let ApplicationCommand::Lock {
        source,
        lock,
        write: true,
        check: false,
    } = &command
    {
        return write_successor_application_lock(source, lock, config, environment).await;
    }
    let (
        identity,
        source,
        lock,
        provision_role,
        tenant,
        lifetime_seconds,
        seed,
        seed_concurrency,
        replace_expired_credential,
    ) = match command {
        ApplicationCommand::Deploy {
            source,
            lock,
            provision_role,
            tenant,
            lifetime_seconds,
            seed,
            seed_concurrency,
            replace_expired_credential,
        } => (
            CommandIdentity::ApplicationDeploy,
            source,
            lock,
            provision_role,
            tenant,
            lifetime_seconds,
            seed,
            seed_concurrency,
            replace_expired_credential,
        ),
        ApplicationCommand::BindDevRole {
            source,
            lock,
            role,
            tenant,
            lifetime_seconds,
            replace_expired_credential,
        } => (
            CommandIdentity::ApplicationBindDevRole,
            source,
            lock,
            Some(role),
            tenant,
            lifetime_seconds,
            false,
            "8".to_owned(),
            replace_expired_credential,
        ),
        ApplicationCommand::Check { .. }
        | ApplicationCommand::Migrate { .. }
        | ApplicationCommand::Preview { .. }
        | ApplicationCommand::Lock { .. }
        | ApplicationCommand::Generate { .. } => {
            return local_error(
                CommandIdentity::ApplicationDeploy,
                "application_dispatch_invalid",
                "local application operation reached network dispatch",
            );
        }
    };
    let lifetime_seconds = match parse_nonzero_u32(&lifetime_seconds) {
        Ok(value) => value,
        Err(()) => return invalid_input(identity),
    };
    let seed_concurrency = match seed_concurrency.parse::<usize>() {
        Ok(value) if (1..=MAX_BATCH_CONCURRENCY).contains(&value) => value,
        _ => return invalid_input(identity),
    };
    let locked = match load_locked_application(Path::new(&source), Some(Path::new(&lock))) {
        Ok(locked) => locked,
        Err(error) => {
            return error.diagnostics().map_or_else(
                || {
                    local_error(
                        identity,
                        "application_lock_inexact",
                        "application source, lock, and generated artifacts are not exact; run application check for the exact failing artifact, then review and write the lock",
                    )
                },
                |diagnostics| authoring_error(identity, diagnostics),
            );
        }
    };
    let mut prepared_query_modules = Vec::with_capacity(locked.manifest().query_modules().len());
    for module in locked.manifest().query_modules() {
        let mut queries = Vec::with_capacity(module.queries().len());
        for query in module.queries() {
            let path = locked.root().join(query.source());
            let source = match read_file(&path, MAX_INPUT_BYTES).and_then(utf8) {
                Ok(source) => source,
                Err(error) => return input_terminal(identity, error),
            };
            queries.push(app_v1::NamedQuerySource {
                name: query.name().to_owned(),
                source,
            });
        }
        prepared_query_modules.push(queries);
    }
    let prepared_role = match provision_role.as_deref() {
        Some(role) => {
            let manifest_path = locked.manifest_path().as_os_str().to_owned();
            match compile_role_from_workspace(&manifest_path, role, tenant.as_deref()) {
                Ok(role) => Some(role),
                Err(error) => return error.terminal(identity),
            }
        }
        None => None,
    };
    let deployment_root = match prepare_deployment_root(locked.root(), config.database.as_str()) {
        Ok(root) => root,
        Err(()) => {
            return local_error(
                identity,
                "deployment_state_unsafe",
                "private deployment state path is unsafe",
            );
        }
    };
    let state_path = deployment_root.join("deployment-state.json");
    let requested_lock_hash = hex(locked.lock_identity().as_bytes());
    let mut state =
        match load_deployment_state(&state_path, config.database.as_str(), &requested_lock_hash) {
            Ok(state) => state,
            Err(()) => {
                return local_error(
                    identity,
                    "deployment_state_inexact",
                    "deployment state does not match the selected database and exact lock",
                );
            }
        };
    let lock_changed = state.lock_hash != requested_lock_hash;
    if lock_changed {
        if let Some(retained) = state.role.as_ref()
            && (!replace_expired_credential || provision_role.is_none())
        {
            let locked_role_hash = prepared_role
                .as_ref()
                .map(|role| hex(role.identity().as_bytes()))
                .unwrap_or_default();
            let detail = ApplicationRoleIdentityError {
                code: "deployment_lock_changed_requires_role_replacement",
                message: "the application lock changed; rerun with --provision-role <role> --replace-role-credential",
                database: config.database.as_str(),
                lock_hash: &requested_lock_hash,
                role_name: provision_role
                    .as_deref()
                    .unwrap_or(retained.role_name.as_str()),
                retained_role_hash: (!retained.role_identity.is_empty())
                    .then_some(retained.role_identity.as_str()),
                locked_role_hash: (!locked_role_hash.is_empty())
                    .then_some(locked_role_hash.as_str()),
                recovery_action: "rerun_with_replace_role_credential",
            };
            return local_error_with(identity, &detail, detail.code, detail.message, 2);
        }
        state.lock_hash.clone_from(&requested_lock_hash);
        state.contract_deployed = false;
        state.contract_bundle_hash.clear();
        state.query_modules_deployed.clear();
        state.query_module_identities.clear();
        state.seeds_completed.clear();
    }
    let metadata = match required_metadata(identity, config, environment) {
        Ok(metadata) => metadata,
        Err(terminal) => return terminal,
    };
    let mut client = match connect(config).await {
        Ok(client) => client,
        Err(error) => return client_error(identity, &error),
    };
    let contract_source_path = locked.root().join(locked.manifest().contract().source());
    let contract_source = match read_file(&contract_source_path, MAX_INPUT_BYTES).and_then(utf8) {
        Ok(source) => source,
        Err(error) => return input_terminal(identity, error),
    };
    let deploy_request_id = match request_id() {
        Ok(request_id) => request_id,
        Err(error) => return client_error(identity, &error),
    };
    let locked_parent = locked.contract().parent();
    let contract_deployment = match client
        .deploy_contract(
            v1::DeployContractRequest {
                request_id: deploy_request_id,
                source: contract_source,
                expected_active_version: locked_parent
                    .map(|parent| parent.contract_version().get()),
                expected_active_bundle_hash: locked_parent
                    .map_or_else(Vec::new, |parent| parent.bundle_hash().as_bytes().to_vec()),
                expected_candidate_bundle_hash: locked.contract().bundle_hash().as_bytes().to_vec(),
            },
            &metadata,
        )
        .await
    {
        Ok(response) => response,
        Err(error) => return client_error(identity, &error),
    };
    let locked_contract = locked.manifest().contract();
    if contract_deployment.result.as_ref().is_some_and(|result| {
        let descriptor = match result {
            v1::deploy_contract_response::Result::Activated(descriptor)
            | v1::deploy_contract_response::Result::AlreadyActive(descriptor) => descriptor,
            _ => return false,
        };
        descriptor.contract_lineage != locked_contract.lineage()
            || descriptor.contract_version != locked_contract.version()
            || descriptor.bundle_hash.as_slice() != locked_contract.bundle_hash().as_bytes()
    }) {
        return local_error(
            identity,
            "locked_contract_identity_mismatch",
            "the deployed contract identity does not match the exact application lock",
        );
    }
    match contract_deployment.result {
        Some(
            v1::deploy_contract_response::Result::Activated(_)
            | v1::deploy_contract_response::Result::AlreadyActive(_),
        ) => {}
        Some(v1::deploy_contract_response::Result::InvalidSource(_)) => {
            return local_error(
                identity,
                "locked_contract_invalid",
                "the exact locked contract was rejected as invalid",
            );
        }
        Some(v1::deploy_contract_response::Result::IncompatibleCandidate(_)) => {
            return local_error(
                identity,
                "locked_contract_incompatible",
                "the exact locked contract is incompatible with the active lineage",
            );
        }
        Some(v1::deploy_contract_response::Result::MigrationRequired(_)) => {
            return local_error(
                identity,
                "locked_contract_requires_migration",
                "the exact locked contract requires a migration from the active lineage",
            );
        }
        Some(v1::deploy_contract_response::Result::ExpectedActiveVersionMismatch(_)) => {
            return local_error(
                identity,
                "active_contract_mismatch",
                "the selected database has a different active contract",
            );
        }
        Some(v1::deploy_contract_response::Result::ExpectedApplicationIdentityMismatch(
            mismatch,
        )) => {
            let detail = ApplicationContractIdentityError {
                code: "application_contract_identity_mismatch",
                message: "the active parent, server-compiled candidate, and application lock identities disagree",
                database: config.database.as_str(),
                lock_hash: hex(locked.lock_identity().as_bytes()),
                expected_parent_version: locked_parent
                    .map(|parent| parent.contract_version().get()),
                expected_parent_bundle_hash: locked_parent
                    .map(|parent| hex(parent.bundle_hash().as_bytes())),
                locked_candidate_bundle_hash: hex(locked.contract().bundle_hash().as_bytes()),
                actual_active_version: mismatch
                    .actual_active
                    .as_ref()
                    .map(|descriptor| descriptor.contract_version),
                actual_active_bundle_hash: mismatch
                    .actual_active
                    .as_ref()
                    .map(|descriptor| hex(&descriptor.bundle_hash)),
                compiled_candidate_bundle_hash: mismatch
                    .compiled_candidate
                    .as_ref()
                    .map(|descriptor| hex(&descriptor.bundle_hash)),
                recovery_action: "riffdb_application_lock_write_against_active",
            };
            return local_error_with(identity, &detail, detail.code, detail.message, 2);
        }
        Some(v1::deploy_contract_response::Result::BundleConflict(_)) => {
            return local_error(
                identity,
                "contract_bundle_conflict",
                "the locked contract identity conflicts with retained catalog state",
            );
        }
        None => {
            return local_error(
                identity,
                "contract_deployment_result_absent",
                "contract deployment returned no terminal result",
            );
        }
    }
    interrupt_application_deployment_after(environment, "contract_remote");
    state.contract_deployed = true;
    state.contract_bundle_hash = hex(locked_contract.bundle_hash().as_bytes());
    if persist_deployment_state(&state_path, &state).is_err() {
        return local_error(
            identity,
            "deployment_state_write_failed",
            "durable deployment progress could not be retained",
        );
    }

    for (module, queries) in locked
        .manifest()
        .query_modules()
        .iter()
        .zip(prepared_query_modules)
    {
        let contract_selector = app_v1::ContractSelector {
            lineage: locked_contract.lineage().to_owned(),
            version: locked_contract.version(),
            bundle_hash: locked_contract.bundle_hash().as_bytes().to_vec(),
        };
        let inspection_request_id = match request_id() {
            Ok(request_id) => request_id,
            Err(error) => return client_error(identity, &error),
        };
        let active_module = match client
            .get_query_module(
                app_v1::GetQueryModuleRequest {
                    contract: Some(contract_selector.clone()),
                    module_hash: None,
                    request_id: inspection_request_id,
                },
                &metadata,
            )
            .await
        {
            Ok(response) => response.module,
            Err(error) => return client_error(identity, &error),
        };
        let expected_active_module_hash = active_module
            .as_ref()
            .map(|descriptor| descriptor.module_hash.clone());
        let expected_active = match expected_active_module_hash.as_ref() {
            Some(hash) => {
                app_v1::deploy_query_module_request::ExpectedActive::ModuleHash(hash.clone())
            }
            None => app_v1::deploy_query_module_request::ExpectedActive::AbsentActive(true),
        };
        let request_id = match request_id() {
            Ok(request_id) => request_id,
            Err(error) => return client_error(identity, &error),
        };
        let response = match client
            .deploy_query_module(
                app_v1::DeployQueryModuleRequest {
                    contract: Some(contract_selector),
                    module_name: module.name().to_owned(),
                    module_version: module.version(),
                    queries,
                    expected_active: Some(expected_active),
                    request_id,
                },
                &metadata,
            )
            .await
        {
            Ok(response) => response,
            Err(error) => return client_error(identity, &error),
        };
        if response.module.as_ref().is_some_and(|descriptor| {
            descriptor.module_name != module.name()
                || descriptor.module_version != module.version()
                || descriptor.module_hash.as_slice() != module.module_hash().as_bytes()
                || descriptor.contract_lineage != locked_contract.lineage()
                || descriptor.contract_version != locked_contract.version()
                || descriptor.contract_bundle_hash.as_slice()
                    != locked_contract.bundle_hash().as_bytes()
        }) {
            return local_error(
                identity,
                "locked_query_module_identity_mismatch",
                "the deployed query module identity does not match the exact application lock",
            );
        }
        match app_v1::QueryModuleDeploymentOutcome::try_from(response.outcome).ok() {
            Some(
                app_v1::QueryModuleDeploymentOutcome::Activated
                | app_v1::QueryModuleDeploymentOutcome::AlreadyActive,
            ) => {}
            Some(app_v1::QueryModuleDeploymentOutcome::ExpectedActiveMismatch) => {
                let lock_hash = hex(locked.lock_identity().as_bytes());
                let mismatch = ApplicationModuleIdentityError {
                    code: "active_query_module_mismatch",
                    message: "the active query module changed during locked deployment",
                    database: config.database.as_str(),
                    lock_hash: &lock_hash,
                    module_name: module.name(),
                    locked_module_hash: hex(module.module_hash().as_bytes()),
                    expected_active_module_hash: expected_active_module_hash.as_deref().map(hex),
                    actual_active_module_hash: response
                        .actual_active_module_hash
                        .as_deref()
                        .map(hex),
                    recovery_action: "rerun_application_deploy",
                };
                return local_error_with(
                    identity,
                    &mismatch,
                    "active_query_module_mismatch",
                    "the active query module changed during locked deployment",
                    2,
                );
            }
            Some(app_v1::QueryModuleDeploymentOutcome::VersionConflict) => {
                return local_error(
                    identity,
                    "query_module_version_conflict",
                    "the locked query module version conflicts with retained state",
                );
            }
            Some(app_v1::QueryModuleDeploymentOutcome::ContractUnavailable) => {
                return local_error(
                    identity,
                    "query_module_contract_unavailable",
                    "the locked query module contract is unavailable",
                );
            }
            Some(app_v1::QueryModuleDeploymentOutcome::Unspecified) | None => {
                return local_error(
                    identity,
                    "query_module_deployment_result_invalid",
                    "query module deployment returned no terminal result",
                );
            }
        }
        if response.module.is_none() {
            return local_error(
                identity,
                "query_module_deployment_identity_absent",
                "query module deployment returned no verifiable module identity",
            );
        }
        interrupt_application_deployment_after(environment, "query_module_remote");
        if !state
            .query_modules_deployed
            .iter()
            .any(|name| name == module.name())
        {
            state.query_modules_deployed.push(module.name().to_owned());
            state.query_modules_deployed.sort();
        }
        state
            .query_module_identities
            .retain(|identity| identity.module_name != module.name());
        state
            .query_module_identities
            .push(ApplicationDeploymentQueryModuleState {
                module_name: module.name().to_owned(),
                module_version: module.version(),
                module_hash: hex(module.module_hash().as_bytes()),
            });
        state
            .query_module_identities
            .sort_by(|left, right| left.module_name.cmp(&right.module_name));
        if persist_deployment_state(&state_path, &state).is_err() {
            return local_error(
                identity,
                "deployment_state_write_failed",
                "durable deployment progress could not be retained",
            );
        }
    }

    let mut application_credential = None;
    if let Some(role) = provision_role {
        let credential_path = deployment_root.join("application.credential");
        let Some(compiled_role) = prepared_role else {
            return local_error(
                identity,
                "application_role_preflight_absent",
                "the requested application role was not preflighted",
            );
        };
        let expected_role_identity = hex(compiled_role.identity().as_bytes());
        if let Some(retained) = state.role.as_ref()
            && (retained.role_name != role
                || (!retained.role_identity.is_empty()
                    && retained.role_identity != expected_role_identity))
            && !replace_expired_credential
        {
            let detail = ApplicationRoleIdentityError {
                code: "deployment_role_identity_mismatch",
                message: "the retained application credential does not match the exact locked role; rerun with --replace-role-credential",
                database: config.database.as_str(),
                lock_hash: &requested_lock_hash,
                role_name: &role,
                retained_role_hash: (!retained.role_identity.is_empty())
                    .then_some(retained.role_identity.as_str()),
                locked_role_hash: Some(expected_role_identity.as_str()),
                recovery_action: "rerun_with_replace_role_credential",
            };
            return local_error_with(identity, &detail, detail.code, detail.message, 2);
        }
        if !replace_expired_credential
            && let Some(retained) = state.role.as_mut()
            && retained.role_identity.is_empty()
        {
            retained.role_identity.clone_from(&expected_role_identity);
            if persist_deployment_state(&state_path, &state).is_err() {
                return local_error(
                    identity,
                    "deployment_state_write_failed",
                    "durable deployment progress could not be retained",
                );
            }
        }
        if replace_expired_credential {
            match state.role.as_ref() {
                Some(retained) => {
                    let terminal = role_command(
                        RoleCommand::Revoke {
                            capability_id: retained.capability_id.clone(),
                            reason: RevocationReason::Replaced,
                        },
                        config,
                        environment,
                    )
                    .await;
                    if terminal.failed() {
                        return terminal;
                    }
                    if credential_path.exists() && fs::remove_file(&credential_path).is_err() {
                        return local_error(
                            identity,
                            "credential_replacement_cleanup_failed",
                            "revoked application credential could not be removed",
                        );
                    }
                    state.role = None;
                    if persist_deployment_state(&state_path, &state).is_err() {
                        return local_error(
                            identity,
                            "deployment_state_write_failed",
                            "durable deployment progress could not be retained",
                        );
                    }
                }
                None if !lock_changed => {
                    return local_error(
                        identity,
                        "credential_replacement_identity_absent",
                        "credential replacement requires the retained old capability identity",
                    );
                }
                None => {}
            }
        }
        if credential_path.exists() && state.role.is_none() {
            return local_error(
                identity,
                "credential_identity_unbound",
                "application credential exists without retained role identity",
            );
        }
        if !credential_path.exists() && state.role.as_ref().is_some_and(|retained| retained.bound) {
            return local_error(
                identity,
                "application_credential_missing",
                "retained application credential is missing; use explicit replacement",
            );
        }
        if state.role.as_ref().is_none_or(|retained| !retained.bound) {
            let capability_id = match state.role.as_ref() {
                Some(retained) => retained.capability_id.clone(),
                None => {
                    let generated = match generate_capability_id() {
                        Ok(value) => value.into_bytes(),
                        Err(error) => {
                            return client_error(
                                identity,
                                &ClientError::IdentifierGeneration(error),
                            );
                        }
                    };
                    let Some(capability_id) = format_uuid(&generated) else {
                        return invalid_input(identity);
                    };
                    state.role = Some(ApplicationDeploymentRoleState {
                        role_name: role.clone(),
                        role_identity: expected_role_identity.clone(),
                        capability_id: capability_id.clone(),
                        bound: false,
                        authentication_audience: None,
                    });
                    if persist_deployment_state(&state_path, &state).is_err() {
                        return local_error(
                            identity,
                            "deployment_state_write_failed",
                            "durable deployment progress could not be retained",
                        );
                    }
                    capability_id
                }
            };
            let request_id = match request_id() {
                Ok(request_id) => request_id,
                Err(error) => return client_error(identity, &error),
            };
            let health = match client
                .health(
                    v1::HealthRequest {
                        request_id: Some(request_id),
                    },
                    &metadata,
                )
                .await
            {
                Ok(health) if !health.authentication_audience.is_empty() => health,
                Ok(_) => {
                    return local_error(
                        identity,
                        "audience_unavailable",
                        "server health did not identify its authentication audience",
                    );
                }
                Err(error) => return client_error(identity, &error),
            };
            let authentication_audience = health.authentication_audience;
            let terminal = bind_compiled_role(
                PreparedRoleBinding {
                    role: compiled_role,
                    principal: format!("app:{}", locked.manifest().application_name()),
                    actor_kind: RoleActorKind::Service,
                    lifetime_seconds: lifetime_seconds.to_string(),
                    audiences: vec![authentication_audience.clone()],
                    capability_id: Some(capability_id),
                    credential_output: credential_path.as_os_str().to_owned(),
                },
                identity,
                config,
                environment,
            )
            .await;
            if terminal.failed() {
                return terminal;
            }
            interrupt_application_deployment_after(environment, "role_bound_remote");
            let Some(retained) = state.role.as_mut() else {
                return invalid_input(identity);
            };
            retained.bound = true;
            retained.authentication_audience = Some(authentication_audience);
            if persist_deployment_state(&state_path, &state).is_err() {
                return local_error(
                    identity,
                    "deployment_state_write_failed",
                    "durable deployment progress could not be retained",
                );
            }
        }
        let Some(authentication_audience) = state
            .role
            .as_ref()
            .and_then(|role| role.authentication_audience.as_deref())
        else {
            return local_error(
                identity,
                "application_audience_missing",
                "retained application role is missing its authentication audience",
            );
        };
        if persist_application_configs(
            &deployment_root,
            config,
            &credential_path,
            authentication_audience,
        )
        .is_err()
        {
            return local_error(
                identity,
                "application_config_write_failed",
                "private application client configuration could not be retained",
            );
        }
        application_credential = Some(credential_path);
    }

    if seed {
        let Some(credential_path) = application_credential.as_ref() else {
            return invalid_input(identity);
        };
        let mut application_config = config.clone();
        application_config.credential_file = Some(credential_path.clone());
        for (index, source) in locked.manifest().seed_inputs().iter().enumerate() {
            let path = locked.root().join(source);
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                return invalid_input(identity);
            };
            let Some(command_name) = file_name
                .strip_suffix(".jsonl")
                .and_then(|name| name.split_once('-').map(|(_, command)| command))
                .filter(|name| !name.is_empty())
            else {
                return invalid_input(identity);
            };
            let checkpoint = seed_checkpoint_path(
                &deployment_root,
                index,
                locked.manifest().contract().version(),
            );
            let terminal = command_command(
                CommandCommand::Batch {
                    command_name: command_name.to_owned(),
                    input: path.into_os_string(),
                    expected_version: Some(locked.manifest().contract().version().to_string()),
                    concurrency: seed_concurrency.to_string(),
                    idempotency_field: "idempotency_key".to_owned(),
                    checkpoint: Some(checkpoint.into_os_string()),
                    error_outcomes: Vec::new(),
                    progress: true,
                },
                &application_config,
                environment,
                stdin,
            )
            .await;
            if terminal.failed() {
                return terminal;
            }
            interrupt_application_deployment_after(environment, "seed_remote");
            if !state.seeds_completed.iter().any(|item| item == source) {
                state.seeds_completed.push(source.to_owned());
                state.seeds_completed.sort();
            }
            if persist_deployment_state(&state_path, &state).is_err() {
                return local_error(
                    identity,
                    "deployment_state_write_failed",
                    "durable deployment progress could not be retained",
                );
            }
        }
    }

    success(
        identity,
        "deployed",
        &serde_json::json!({
            "application": locked.manifest().application_name(),
            "contract_lineage": locked.manifest().contract().lineage(),
            "contract_version": locked.manifest().contract().version().to_string(),
            "database": config.database.as_str(),
            "lock_hash": hex(locked.lock_identity().as_bytes()),
            "provisioned": application_credential.is_some(),
            "seeded": seed,
        }),
    )
}

async fn write_successor_application_lock(
    source_path: &OsStr,
    lock_path: &OsStr,
    config: &EffectiveConfig,
    environment: &dyn Environment,
) -> Terminal {
    let identity = CommandIdentity::ApplicationLock;
    let source = match application_contract_source(Path::new(source_path)) {
        Ok(source) => source,
        Err(_) => {
            return local_error(
                identity,
                "application_source_invalid",
                "the symbolic application or contract source is invalid",
            );
        }
    };
    let metadata = match required_metadata(identity, config, environment) {
        Ok(metadata) => metadata,
        Err(terminal) => return terminal,
    };
    let mut client = match connect(config).await {
        Ok(client) => client,
        Err(error) => return client_error(identity, &error),
    };
    let request_id = match request_id() {
        Ok(request_id) => request_id,
        Err(error) => return client_error(identity, &error),
    };
    let response = match client
        .validate_contract(
            v1::ValidateContractRequest {
                request_id,
                source,
                preview_active_successor: true,
            },
            &metadata,
        )
        .await
    {
        Ok(response) => response,
        Err(error) => return client_error(identity, &error),
    };
    let candidate = match response.result.as_ref() {
        Some(v1::validate_contract_response::Result::Candidate(candidate)) => candidate,
        Some(v1::validate_contract_response::Result::Invalid(diagnostics)) => {
            return render_compilation_diagnostics(identity, diagnostics);
        }
        Some(v1::validate_contract_response::Result::Valid(_)) | None => {
            return local_error(
                identity,
                "candidate_preview_invalid",
                "the server did not return the requested parent-aware candidate",
            );
        }
    };
    let contract = match riffdb_contract_ir::ContractBundle::decode(&candidate.canonical_bundle) {
        Ok(contract) => contract,
        Err(_) => {
            return local_error(
                identity,
                "candidate_bundle_invalid",
                "the server returned an invalid canonical candidate bundle",
            );
        }
    };
    if contract.parent().is_none() {
        return local_error(
            identity,
            "active_parent_absent",
            "a successor lock requires an active parent in the selected database; deploy the genesis contract first or select the intended database",
        );
    }
    let descriptor_matches = candidate.candidate.as_ref().is_some_and(|descriptor| {
        descriptor.contract_lineage == contract.lineage().as_str()
            && descriptor.contract_version == contract.contract_version().get()
            && descriptor.bundle_hash.as_slice() == contract.bundle_hash().as_bytes()
    });
    let parent_matches = contract
        .parent()
        .map(|parent| (parent.contract_version().get(), parent.bundle_hash()))
        == candidate.parent_version.zip(
            candidate
                .parent_bundle_hash
                .as_slice()
                .try_into()
                .ok()
                .map(riffdb_types::ContractBundleHash::from_bytes),
        );
    if !descriptor_matches || !parent_matches {
        return local_error(
            identity,
            "candidate_identity_mismatch",
            "the server candidate descriptor, parent identity, and canonical bundle disagree",
        );
    }
    match write_application_lock_with_bundle(
        Path::new(source_path),
        Some(Path::new(lock_path)),
        contract,
    ) {
        Ok(()) => success(
            identity,
            "locked",
            &ApplicationLockResult { status: "locked" },
        ),
        Err(_) => local_error(
            identity,
            "application_lock_inexact",
            "the candidate does not exactly match the symbolic application sources",
        ),
    }
}

fn prepare_deployment_root(root: &Path, database: &str) -> Result<PathBuf, ()> {
    let private = root.join(".riffdb");
    let deployments = private.join("deployments");
    let selected = deployments.join(database);
    for directory in [&private, &deployments, &selected] {
        match fs::symlink_metadata(directory) {
            Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            }
            Ok(_) => return Err(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(directory).map_err(|_| ())?;
            }
            Err(_) => return Err(()),
        }
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).map_err(|_| ())?;
    }
    Ok(selected)
}

fn seed_checkpoint_path(root: &Path, index: usize, contract_version: u64) -> PathBuf {
    root.join(format!(
        "seed-{index:03}-contract-v{contract_version}.checkpoint.json"
    ))
}

fn load_deployment_state(
    path: &Path,
    database: &str,
    lock_hash: &str,
) -> Result<ApplicationDeploymentState, ()> {
    if fs::symlink_metadata(path).is_err() {
        return Ok(ApplicationDeploymentState {
            schema: APPLICATION_DEPLOYMENT_STATE_SCHEMA.to_owned(),
            database: database.to_owned(),
            lock_hash: lock_hash.to_owned(),
            contract_deployed: false,
            contract_bundle_hash: String::new(),
            query_modules_deployed: Vec::new(),
            query_module_identities: Vec::new(),
            role: None,
            seeds_completed: Vec::new(),
        });
    }
    let metadata = fs::symlink_metadata(path).map_err(|_| ())?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(());
    }
    let bytes = read_file(path, MAX_INPUT_BYTES).map_err(|_| ())?;
    let state: ApplicationDeploymentState = serde_json::from_slice(&bytes).map_err(|_| ())?;
    if state.schema != APPLICATION_DEPLOYMENT_STATE_SCHEMA
        || state.database != database
        || state.lock_hash.len() != 64
        || !state
            .lock_hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || state.query_modules_deployed.len() > 4_096
        || state.query_module_identities.len() > 4_096
        || state.seeds_completed.len() > 4_096
        || (!state.contract_bundle_hash.is_empty()
            && (state.contract_bundle_hash.len() != 64
                || !state
                    .contract_bundle_hash
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))))
        || state
            .query_modules_deployed
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || state
            .query_module_identities
            .windows(2)
            .any(|pair| pair[0].module_name >= pair[1].module_name)
        || state.query_module_identities.iter().any(|identity| {
            identity.module_name.is_empty()
                || identity.module_name.len() > 256
                || identity.module_version == 0
                || identity.module_hash.len() != 64
                || !identity
                    .module_hash
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                || !state
                    .query_modules_deployed
                    .iter()
                    .any(|name| name == &identity.module_name)
        })
        || state
            .seeds_completed
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || state.role.as_ref().is_some_and(|role| {
            role.role_name.is_empty()
                || role.role_name.len() > 256
                || (!role.role_identity.is_empty()
                    && (role.role_identity.len() != 64
                        || !role
                            .role_identity
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))))
                || parse_uuid_v7(&role.capability_id).is_none()
                || (role.bound
                    && role
                        .authentication_audience
                        .as_ref()
                        .is_none_or(|audience| {
                            audience.is_empty()
                                || audience.len() > 512
                                || !audience.bytes().all(|byte| byte.is_ascii_graphic())
                        }))
        })
    {
        return Err(());
    }
    Ok(state)
}

fn persist_deployment_state(path: &Path, state: &ApplicationDeploymentState) -> Result<(), ()> {
    let bytes = serde_json::to_vec(state).map_err(|_| ())?;
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(());
    }
    persist_private_file(path, &bytes)
}

fn persist_application_configs(
    root: &Path,
    config: &EffectiveConfig,
    credential_path: &Path,
    authentication_audience: &str,
) -> Result<(), ()> {
    let credential_path = if credential_path.is_absolute() {
        credential_path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|_| ())?
            .join(credential_path)
    };
    let credential = credential_path.to_str().ok_or(())?;
    let endpoint = serde_json::to_string(&config.endpoint).map_err(|_| ())?;
    let database = serde_json::to_string(config.database.as_str()).map_err(|_| ())?;
    let credential = serde_json::to_string(credential).map_err(|_| ())?;
    let authentication_audience = serde_json::to_string(authentication_audience).map_err(|_| ())?;
    let client = format!(
        "[client]\nendpoint = {endpoint}\ndatabase = {database}\noutput = \"json\"\nmax_attempts = {}\ncredential_file = {credential}\n",
        config.max_attempts
    );
    let mcp = format!(
        "[mcp]\nendpoint = {endpoint}\ndatabase = {database}\ncredential_file = {credential}\nexpected_audience = {authentication_audience}\n"
    );
    persist_private_file(&root.join("client.toml"), client.as_bytes())?;
    persist_private_file(&root.join("mcp.toml"), mcp.as_bytes())
}

fn persist_private_file(path: &Path, bytes: &[u8]) -> Result<(), ()> {
    let parent = path.parent().ok_or(())?;
    let parent_metadata = fs::symlink_metadata(parent).map_err(|_| ())?;
    if !parent_metadata.file_type().is_dir() || parent_metadata.file_type().is_symlink() {
        return Err(());
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {}
        Ok(_) => return Err(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Err(()),
    }
    let name = path.file_name().and_then(|name| name.to_str()).ok_or(())?;
    let mut opened = None;
    for suffix in 0..128_u8 {
        let temporary = parent.join(format!(
            ".{name}.riffdb-deploy-{}-{suffix}",
            std::process::id()
        ));
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)
        {
            Ok(file) => {
                opened = Some((temporary, file));
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(()),
        }
    }
    let (temporary, mut file) = opened.ok_or(())?;
    let result = file
        .write_all(bytes)
        .and_then(|()| file.sync_all())
        .and_then(|()| {
            drop(file);
            fs::rename(&temporary, path)
        })
        .and_then(|()| fs::File::open(parent)?.sync_all());
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
        return Err(());
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|_| ())
}

async fn query_command(
    command: QueryCommand,
    config: &EffectiveConfig,
    environment: &dyn Environment,
    stdin: &mut dyn Read,
) -> Terminal {
    let identity = match command {
        QueryCommand::Describe { .. } => CommandIdentity::QueryDescribe,
        QueryCommand::Check { .. } => CommandIdentity::QueryCheck,
        QueryCommand::Explain { .. } => CommandIdentity::QueryExplain,
        QueryCommand::Run { .. } => CommandIdentity::QueryRun,
        QueryCommand::RunNamed { .. } => CommandIdentity::QueryRunNamed,
        QueryCommand::Deploy { .. } => CommandIdentity::QueryDeploy,
        QueryCommand::Module { .. } => CommandIdentity::QueryModule,
        QueryCommand::Repl { .. } => CommandIdentity::QueryRepl,
    };
    let metadata = match required_metadata(identity, config, environment) {
        Ok(metadata) => metadata,
        Err(terminal) => return terminal,
    };
    let mut client = match connect(config).await {
        Ok(client) => client,
        Err(error) => return client_error(identity, &error),
    };
    match command {
        QueryCommand::Describe { contract } => {
            let contract = match symbolic_contract_selection(contract) {
                Ok(contract) => contract,
                Err(()) => return invalid_input(identity),
            };
            let request_id = match request_id() {
                Ok(request_id) => request_id,
                Err(error) => return client_error(identity, &error),
            };
            match client
                .describe_contract(
                    app_v1::DescribeContractRequest {
                        contract,
                        request_id,
                    },
                    &metadata,
                )
                .await
            {
                Ok(response) => success(
                    identity,
                    "described",
                    &serde_json::json!({
                        "contract_lineage": response.contract_lineage,
                        "contract_version": response.contract_version.to_string(),
                        "contract_bundle_hash": hex(&response.contract_bundle_hash),
                        "catalog": response.symbolic_catalog,
                    }),
                ),
                Err(error) => client_error(identity, &error),
            }
        }
        QueryCommand::Check { source, contract } => {
            let source = match read_text(&source, stdin) {
                Ok(source) => source,
                Err(error) => return input_terminal(identity, error),
            };
            let contract = match symbolic_contract_selection(contract) {
                Ok(contract) => contract,
                Err(()) => return invalid_input(identity),
            };
            let request_id = match request_id() {
                Ok(request_id) => request_id,
                Err(error) => return client_error(identity, &error),
            };
            match client
                .check_query(
                    app_v1::CheckQueryRequest {
                        contract,
                        source,
                        request_id,
                    },
                    &metadata,
                )
                .await
            {
                Ok(response) => render_query_check(identity, response),
                Err(error) => client_error(identity, &error),
            }
        }
        QueryCommand::Explain { source, contract } => {
            let source = match read_text(&source, stdin) {
                Ok(source) => source,
                Err(error) => return input_terminal(identity, error),
            };
            let contract = match symbolic_contract_selection(contract) {
                Ok(contract) => contract,
                Err(()) => return invalid_input(identity),
            };
            let request_id = match request_id() {
                Ok(request_id) => request_id,
                Err(error) => return client_error(identity, &error),
            };
            match client
                .explain_query(
                    app_v1::ExplainQueryRequest {
                        contract,
                        query: Some(app_v1::explain_query_request::Query::Source(source)),
                        module_hash: None,
                        request_id,
                    },
                    &metadata,
                )
                .await
            {
                Ok(response) => render_query_explain(identity, response),
                Err(error) => client_error(identity, &error),
            }
        }
        QueryCommand::Run {
            source,
            parameters,
            cursor,
            read_after_commit,
            contract,
        } => {
            let source = match read_text(&source, stdin) {
                Ok(source) => source,
                Err(error) => return input_terminal(identity, error),
            };
            let parameters = match parameters {
                Some(path) => {
                    match read_json::<serde_json::Map<String, serde_json::Value>>(&path, stdin) {
                        Ok(parameters) => parameters,
                        Err(error) => return input_terminal(identity, error),
                    }
                }
                None => serde_json::Map::new(),
            };
            execute_query_cli(
                identity,
                &mut client,
                &metadata,
                contract,
                source,
                parameters,
                cursor,
                read_after_commit,
            )
            .await
        }
        QueryCommand::RunNamed {
            query_name,
            module_hash,
            parameters,
            cursor,
            read_after_commit,
            contract,
        } => {
            let parameters = match query_parameters(parameters.as_ref(), stdin) {
                Ok(parameters) => parameters,
                Err(error) => return input_terminal(identity, error),
            };
            let module_hash = match module_hash.as_deref().map(parse_hash).transpose() {
                Ok(module_hash) => module_hash,
                Err(()) => return invalid_input(identity),
            };
            execute_named_query_cli(
                identity,
                &mut client,
                &metadata,
                contract,
                query_name,
                module_hash,
                parameters,
                cursor,
                read_after_commit,
            )
            .await
        }
        QueryCommand::Deploy {
            directory,
            module_name,
            module_version,
            expected_active,
            contract,
        } => {
            let contract = match symbolic_contract_selection(contract) {
                Ok(contract) => contract,
                Err(()) => return invalid_input(identity),
            };
            let module_version = match module_version.parse::<u64>() {
                Ok(version) if version != 0 => version,
                _ => return invalid_input(identity),
            };
            let queries = match read_query_directory(Path::new(&directory)) {
                Ok(queries) => queries,
                Err(error) => return input_terminal(identity, error),
            };
            let expected_active = match parse_module_expectation(&expected_active) {
                Ok(expectation) => Some(expectation),
                Err(()) => return invalid_input(identity),
            };
            let request_id = match request_id() {
                Ok(request_id) => request_id,
                Err(error) => return client_error(identity, &error),
            };
            match client
                .deploy_query_module(
                    app_v1::DeployQueryModuleRequest {
                        contract,
                        module_name,
                        module_version,
                        queries,
                        expected_active,
                        request_id,
                    },
                    &metadata,
                )
                .await
            {
                Ok(response) => success(
                    identity,
                    "deployed",
                    &serde_json::json!({
                        "outcome": response.outcome,
                        "module": response.module.map(module_descriptor_json),
                        "actual_active_module_hash": response.actual_active_module_hash.map(|hash| hex(&hash)),
                    }),
                ),
                Err(error) => client_error(identity, &error),
            }
        }
        QueryCommand::Module {
            module_hash,
            contract,
        } => {
            let contract = match symbolic_contract_selection(contract) {
                Ok(contract) => contract,
                Err(()) => return invalid_input(identity),
            };
            let module_hash = match module_hash.as_deref().map(parse_hash).transpose() {
                Ok(module_hash) => module_hash,
                Err(()) => return invalid_input(identity),
            };
            let request_id = match request_id() {
                Ok(request_id) => request_id,
                Err(error) => return client_error(identity, &error),
            };
            match client
                .get_query_module(
                    app_v1::GetQueryModuleRequest {
                        contract,
                        module_hash,
                        request_id,
                    },
                    &metadata,
                )
                .await
            {
                Ok(response) => success(
                    identity,
                    "inspected",
                    &serde_json::json!({
                        "module": response.module.map(module_descriptor_json),
                        "queries": response.queries.into_iter().map(|query| serde_json::json!({
                            "name": query.name,
                            "source": query.source,
                        })).collect::<Vec<_>>(),
                    }),
                ),
                Err(error) => client_error(identity, &error),
            }
        }
        QueryCommand::Repl { contract } => {
            let mut source = String::new();
            if stdin.read_to_string(&mut source).is_err() || source.len() > MAX_INPUT_BYTES {
                return invalid_input(identity);
            }
            let documents = source
                .split("\n---\n")
                .filter(|document| !document.trim().is_empty())
                .collect::<Vec<_>>();
            if documents.is_empty() || documents.len() > 64 {
                return invalid_input(identity);
            }
            let mut results = Vec::with_capacity(documents.len());
            for document in documents {
                let request_id = match request_id() {
                    Ok(request_id) => request_id,
                    Err(error) => return client_error(identity, &error),
                };
                let contract = match symbolic_contract_selection_ref(&contract) {
                    Ok(contract) => contract,
                    Err(()) => return invalid_input(identity),
                };
                let response = match client
                    .execute_query(
                        app_v1::ExecuteQueryRequest {
                            contract,
                            query: Some(app_v1::execute_query_request::Query::Source(
                                document.to_owned(),
                            )),
                            module_hash: None,
                            parameters: Vec::new(),
                            cursor: None,
                            minimum_application_head: None,
                            request_id,
                        },
                        &metadata,
                    )
                    .await
                {
                    Ok(response) => response,
                    Err(error) => return client_error(identity, &error),
                };
                match query_execution_json(&response) {
                    Some(result) => results.push(result),
                    None => return invalid_input(identity),
                }
            }
            success(
                identity,
                "completed",
                &serde_json::json!({"results": results}),
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_query_cli(
    identity: CommandIdentity,
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    contract: ContractSelectionArgs,
    source: String,
    parameters: serde_json::Map<String, serde_json::Value>,
    cursor: Option<String>,
    read_after_commit: Option<String>,
) -> Terminal {
    let contract = match symbolic_contract_selection(contract) {
        Ok(contract) => contract,
        Err(()) => return invalid_input(identity),
    };
    let mut parameters = match parameters
        .into_iter()
        .map(|(name, value)| {
            Ok(app_v1::Parameter {
                name,
                value: Some(natural_query_value(value)?),
            })
        })
        .collect::<Result<Vec<_>, ()>>()
    {
        Ok(parameters) => parameters,
        Err(()) => return invalid_input(identity),
    };
    parameters.sort_by(|left, right| left.name.cmp(&right.name));
    let request_id = match request_id() {
        Ok(request_id) => request_id,
        Err(error) => return client_error(identity, &error),
    };
    let minimum_application_head = match read_after_commit
        .as_deref()
        .map(parse_nonzero_u64)
        .transpose()
    {
        Ok(value) => value,
        Err(()) => return invalid_input(identity),
    };
    match client
        .execute_query(
            app_v1::ExecuteQueryRequest {
                contract,
                query: Some(app_v1::execute_query_request::Query::Source(source)),
                module_hash: None,
                parameters,
                cursor,
                minimum_application_head,
                request_id,
            },
            metadata,
        )
        .await
    {
        Ok(response) => query_execution_json(&response).map_or_else(
            || invalid_input(identity),
            |result| success(identity, "completed", &result),
        ),
        Err(error) => client_error(identity, &error),
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_named_query_cli(
    identity: CommandIdentity,
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    contract: ContractSelectionArgs,
    query_name: String,
    module_hash: Option<Vec<u8>>,
    parameters: Vec<app_v1::Parameter>,
    cursor: Option<String>,
    read_after_commit: Option<String>,
) -> Terminal {
    let contract = match symbolic_contract_selection(contract) {
        Ok(contract) => contract,
        Err(()) => return invalid_input(identity),
    };
    let request_id = match request_id() {
        Ok(request_id) => request_id,
        Err(error) => return client_error(identity, &error),
    };
    let minimum_application_head = match read_after_commit
        .as_deref()
        .map(parse_nonzero_u64)
        .transpose()
    {
        Ok(value) => value,
        Err(()) => return invalid_input(identity),
    };
    match client
        .execute_query(
            app_v1::ExecuteQueryRequest {
                contract,
                query: Some(app_v1::execute_query_request::Query::QueryName(query_name)),
                module_hash,
                parameters,
                cursor,
                minimum_application_head,
                request_id,
            },
            metadata,
        )
        .await
    {
        Ok(response) => query_execution_json(&response).map_or_else(
            || invalid_input(identity),
            |result| success(identity, "completed", &result),
        ),
        Err(error) => client_error(identity, &error),
    }
}

fn query_parameters(
    path: Option<&OsString>,
    stdin: &mut dyn Read,
) -> Result<Vec<app_v1::Parameter>, InputError> {
    let parameters = match path {
        Some(path) => read_json::<serde_json::Map<String, serde_json::Value>>(path, stdin)?,
        None => serde_json::Map::new(),
    };
    let mut parameters = parameters
        .into_iter()
        .map(|(name, value)| {
            Ok(app_v1::Parameter {
                name,
                value: Some(natural_query_value(value).map_err(|_| InputError::Invalid)?),
            })
        })
        .collect::<Result<Vec<_>, InputError>>()?;
    parameters.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(parameters)
}

fn read_query_directory(directory: &Path) -> Result<Vec<app_v1::NamedQuerySource>, InputError> {
    validate_path(directory.as_os_str())?;
    let entries = fs::read_dir(directory).map_err(|_| InputError::ReadFailed)?;
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|_| InputError::ReadFailed)?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("riffq") {
            paths.push(path);
        }
        if paths.len() > 4_096 {
            return Err(InputError::TooLarge);
        }
    }
    paths.sort();
    if paths.is_empty() {
        return Err(InputError::Invalid);
    }
    let mut total = 0_usize;
    let mut queries = Vec::with_capacity(paths.len());
    for path in paths {
        let source = utf8(read_file(&path, MAX_INPUT_BYTES)?)?;
        let name = declared_query_name(&source).ok_or(InputError::Invalid)?;
        total = total
            .checked_add(source.len())
            .ok_or(InputError::TooLarge)?;
        if total > 16 * MAX_INPUT_BYTES {
            return Err(InputError::TooLarge);
        }
        queries.push(app_v1::NamedQuerySource { name, source });
    }
    queries.sort_by(|left, right| left.name.cmp(&right.name));
    if queries.windows(2).any(|pair| pair[0].name == pair[1].name) {
        return Err(InputError::Invalid);
    }
    Ok(queries)
}

fn declared_query_name(source: &str) -> Option<String> {
    let source = source.trim_start();
    let remainder = source.strip_prefix("query ")?;
    let length = remainder
        .bytes()
        .take_while(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        .count();
    let name = remainder.get(..length)?;
    (!name.is_empty() && name.len() <= 256).then(|| name.to_owned())
}

fn parse_module_expectation(
    value: &str,
) -> Result<app_v1::deploy_query_module_request::ExpectedActive, ()> {
    match value {
        "any" => Ok(app_v1::deploy_query_module_request::ExpectedActive::AnyActive(true)),
        "absent" => Ok(app_v1::deploy_query_module_request::ExpectedActive::AbsentActive(true)),
        hash => {
            parse_hash(hash).map(app_v1::deploy_query_module_request::ExpectedActive::ModuleHash)
        }
    }
}

fn parse_hash(value: &str) -> Result<Vec<u8>, ()> {
    if value.len() != 64 {
        return Err(());
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0]).ok_or(())?;
            let low = hex_nibble(pair[1]).ok_or(())?;
            Ok((high << 4) | low)
        })
        .collect()
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn module_descriptor_json(descriptor: app_v1::QueryModuleDescriptor) -> serde_json::Value {
    serde_json::json!({
        "module_name": descriptor.module_name,
        "module_version": descriptor.module_version.to_string(),
        "module_hash": hex(&descriptor.module_hash),
        "contract_lineage": descriptor.contract_lineage,
        "contract_version": descriptor.contract_version.to_string(),
        "contract_bundle_hash": hex(&descriptor.contract_bundle_hash),
        "query_names": descriptor.query_names,
    })
}

async fn backup_command(
    command: BackupCommand,
    config: &EffectiveConfig,
    environment: &dyn Environment,
) -> Terminal {
    let identity = match command {
        BackupCommand::Create { .. } => CommandIdentity::BackupCreate,
        BackupCommand::Restore { .. } => CommandIdentity::BackupRestore,
        BackupCommand::Operation { .. } => CommandIdentity::BackupOperation,
    };
    match command {
        BackupCommand::Create { name } => {
            let name = match BackupNameV1::new(name) {
                Ok(name) => name,
                Err(_) => return invalid_input(identity),
            };
            let operation_id = match generate_offline_maintenance_operation_id() {
                Ok(operation_id) => operation_id,
                Err(error) => {
                    return client_error(identity, &ClientError::IdentifierGeneration(error));
                }
            };
            let metadata = match required_metadata(identity, config, environment) {
                Ok(metadata) => metadata,
                Err(terminal) => return terminal,
            };
            let mut client = match connect(config).await {
                Ok(client) => client,
                Err(error) => return client_error(identity, &error),
            };
            let create = CreateOfflineBackup::new(operation_id, name);
            let attempts = AttemptBudget::new(config.max_attempts).expect("configuration bound");
            match client
                .create_offline_backup_with_retry(&create, attempts, &metadata)
                .await
            {
                Ok(response) => render_create_maintenance_start(&response),
                Err(ClientError::OutcomeUnknown(_)) => {
                    maintenance_uncertain(identity, operation_id)
                }
                Err(error) => client_error(identity, &error),
            }
        }
        BackupCommand::Restore {
            name,
            confirm_replace_current_database,
        } => {
            let name = match BackupNameV1::new(name) {
                Ok(name) => name,
                Err(_) => return invalid_input(identity),
            };
            let operation_id = match generate_offline_maintenance_operation_id() {
                Ok(operation_id) => operation_id,
                Err(error) => {
                    return client_error(identity, &ClientError::IdentifierGeneration(error));
                }
            };
            let confirmation = restore_confirmation(confirm_replace_current_database);
            let metadata = match required_metadata(identity, config, environment) {
                Ok(metadata) => metadata,
                Err(terminal) => return terminal,
            };
            let mut client = match connect(config).await {
                Ok(client) => client,
                Err(error) => return client_error(identity, &error),
            };
            let restore = RestoreOfflineBackup::new(operation_id, name, confirmation);
            let attempts = AttemptBudget::new(config.max_attempts).expect("configuration bound");
            match client
                .restore_offline_backup_with_retry(&restore, attempts, &metadata)
                .await
            {
                Ok(response) => render_restore_maintenance_start(&response),
                Err(ClientError::OutcomeUnknown(_)) => {
                    maintenance_uncertain(identity, operation_id)
                }
                Err(error) => client_error(identity, &error),
            }
        }
        BackupCommand::Operation {
            maintenance_operation_id,
        } => {
            let Some(bytes) = parse_uuid_v7(&maintenance_operation_id) else {
                return invalid_input(identity);
            };
            let operation_id = match OfflineMaintenanceOperationId::from_bytes(bytes) {
                Ok(operation_id) => operation_id,
                Err(_) => return invalid_input(identity),
            };
            let metadata = match required_metadata(identity, config, environment) {
                Ok(metadata) => metadata,
                Err(terminal) => return terminal,
            };
            let mut client = match connect(config).await {
                Ok(client) => client,
                Err(error) => return client_error(identity, &error),
            };
            let request_id = match request_id() {
                Ok(request_id) => request_id,
                Err(error) => return client_error(identity, &error),
            };
            let request = v1::GetOfflineMaintenanceOperationRequest {
                request_id,
                operation_id: operation_id.into_bytes().to_vec(),
            };
            match client
                .get_offline_maintenance_operation(request, &metadata)
                .await
            {
                Ok(response) => render_maintenance_operation(&response),
                Err(error) => client_error(identity, &error),
            }
        }
    }
}

async fn contract_command(
    command: ContractCommand,
    config: &EffectiveConfig,
    environment: &dyn Environment,
    stdin: &mut dyn Read,
) -> Terminal {
    match command {
        ContractCommand::Validate { source } => {
            let source = match read_text(&source, stdin) {
                Ok(source) => source,
                Err(error) => return input_terminal(CommandIdentity::ContractValidate, error),
            };
            let metadata =
                match required_metadata(CommandIdentity::ContractValidate, config, environment) {
                    Ok(metadata) => metadata,
                    Err(terminal) => return terminal,
                };
            let mut client = match connect(config).await {
                Ok(client) => client,
                Err(error) => return client_error(CommandIdentity::ContractValidate, &error),
            };
            let request_id = match request_id() {
                Ok(request_id) => request_id,
                Err(error) => return client_error(CommandIdentity::ContractValidate, &error),
            };
            match client
                .validate_contract(
                    v1::ValidateContractRequest {
                        request_id,
                        source,
                        preview_active_successor: false,
                    },
                    &metadata,
                )
                .await
            {
                Ok(response) => render_contract_validation(&response),
                Err(error) => client_error(CommandIdentity::ContractValidate, &error),
            }
        }
        ContractCommand::Deploy {
            source,
            expected_version,
        } => {
            let source = match read_text(&source, stdin) {
                Ok(source) => source,
                Err(error) => return input_terminal(CommandIdentity::ContractDeploy, error),
            };
            let expected_active_version =
                match parse_expected_active_version(expected_version.as_deref()) {
                    Ok(value) => value,
                    Err(()) => return invalid_input(CommandIdentity::ContractDeploy),
                };
            let metadata =
                match required_metadata(CommandIdentity::ContractDeploy, config, environment) {
                    Ok(metadata) => metadata,
                    Err(terminal) => return terminal,
                };
            let mut client = match connect(config).await {
                Ok(client) => client,
                Err(error) => return client_error(CommandIdentity::ContractDeploy, &error),
            };
            let request_id = match request_id() {
                Ok(request_id) => request_id,
                Err(error) => return client_error(CommandIdentity::ContractDeploy, &error),
            };
            let request = v1::DeployContractRequest {
                request_id,
                source,
                expected_active_version,
                expected_active_bundle_hash: Vec::new(),
                expected_candidate_bundle_hash: Vec::new(),
            };
            match client.deploy_contract(request, &metadata).await {
                Ok(response) => render_contract_deploy(&response),
                Err(error) => client_error(CommandIdentity::ContractDeploy, &error),
            }
        }
    }
}

async fn command_command(
    command: CommandCommand,
    config: &EffectiveConfig,
    environment: &dyn Environment,
    stdin: &mut dyn Read,
) -> Terminal {
    match command {
        CommandCommand::Batch {
            command_name,
            input,
            expected_version,
            concurrency,
            idempotency_field,
            checkpoint,
            error_outcomes,
            progress,
        } => {
            let expected_version = match expected_version
                .as_deref()
                .map(parse_nonzero_u64)
                .transpose()
            {
                Ok(value) => value,
                Err(()) => return invalid_input(CommandIdentity::CommandBatch),
            };
            let concurrency = match concurrency.parse::<usize>() {
                Ok(value) if (1..=MAX_BATCH_CONCURRENCY).contains(&value) => value,
                _ => return invalid_input(CommandIdentity::CommandBatch),
            };
            let source = match read_path_or_stdin(&input, stdin, MAX_BATCH_SOURCE_BYTES) {
                Ok(source) => source,
                Err(error) => return input_terminal(CommandIdentity::CommandBatch, error),
            };
            let source = match parse_batch_source(
                &source,
                &command_name,
                expected_version,
                &idempotency_field,
            ) {
                Ok(source) => source,
                Err(error) => return batch_error_terminal(error),
            };
            let error_outcomes = error_outcomes.into_iter().collect::<BTreeSet<_>>();
            let metadata =
                match required_metadata(CommandIdentity::CommandBatch, config, environment) {
                    Ok(metadata) => metadata,
                    Err(terminal) => return terminal,
                };
            let client = match connect(config).await {
                Ok(client) => client,
                Err(error) => return client_error(CommandIdentity::CommandBatch, &error),
            };
            let attempts = AttemptBudget::new(config.max_attempts).expect("configuration bound");
            let report = match execute_batch(
                source,
                BatchOptions {
                    command_name,
                    expected_contract_version: expected_version,
                    concurrency,
                    idempotency_field,
                    error_outcomes,
                    checkpoint_path: checkpoint.map(std::path::PathBuf::from),
                    progress,
                },
                client,
                attempts,
                metadata,
            )
            .await
            {
                Ok(report) => report,
                Err(error) => return batch_error_terminal(error),
            };
            render_batch_report(report)
        }
        CommandCommand::Run {
            command_name,
            input,
            expected_version,
        } => {
            let application_command_name = command_name.clone();
            let input = match read_json::<serde_json::Map<String, serde_json::Value>>(&input, stdin)
                .and_then(|input| natural_command_record(input).map_err(|()| InputError::Invalid))
            {
                Ok(input) => input,
                Err(error) => return input_terminal(CommandIdentity::CommandRun, error),
            };
            let expected_version = match expected_version
                .as_deref()
                .map(parse_nonzero_u64)
                .transpose()
            {
                Ok(value) => value,
                Err(()) => return invalid_input(CommandIdentity::CommandRun),
            };
            let command = match IdempotentCommand::new(command_name, expected_version, input) {
                Ok(command) => command,
                Err(_) => return invalid_input(CommandIdentity::CommandRun),
            };
            let metadata = match required_metadata(CommandIdentity::CommandRun, config, environment)
            {
                Ok(metadata) => metadata,
                Err(terminal) => return terminal,
            };
            let mut client = match connect(config).await {
                Ok(client) => client,
                Err(error) => return client_error(CommandIdentity::CommandRun, &error),
            };
            let attempts = AttemptBudget::new(config.max_attempts).expect("configuration bound");
            match submit_execute_retry(&mut client, &command, attempts, &metadata).await {
                Ok(response) => render_execution(CommandIdentity::CommandRun, &response),
                Err(error) => {
                    let error =
                        contextualize_application_command_error(error, &application_command_name);
                    client_error(CommandIdentity::CommandRun, &error)
                }
            }
        }
        CommandCommand::Execute {
            command_name,
            input,
            expected_version,
        } => {
            let input = match read_json::<RecordInput>(&input, stdin)
                .and_then(|input| input.into_proto().map_err(|_| InputError::Invalid))
            {
                Ok(input) => input,
                Err(error) => return input_terminal(CommandIdentity::CommandExecute, error),
            };
            let expected_version = match expected_version
                .as_deref()
                .map(parse_nonzero_u64)
                .transpose()
            {
                Ok(value) => value,
                Err(()) => return invalid_input(CommandIdentity::CommandExecute),
            };
            let command = match IdempotentCommand::new(command_name, expected_version, input) {
                Ok(command) => command,
                Err(_) => return invalid_input(CommandIdentity::CommandExecute),
            };
            let metadata =
                match required_metadata(CommandIdentity::CommandExecute, config, environment) {
                    Ok(metadata) => metadata,
                    Err(terminal) => return terminal,
                };
            let mut client = match connect(config).await {
                Ok(client) => client,
                Err(error) => return client_error(CommandIdentity::CommandExecute, &error),
            };
            let attempts = AttemptBudget::new(config.max_attempts).expect("configuration bound");
            match submit_execute_retry(&mut client, &command, attempts, &metadata).await {
                Ok(response) => render_execution(CommandIdentity::CommandExecute, &response),
                Err(error) => client_error(CommandIdentity::CommandExecute, &error),
            }
        }
        CommandCommand::Outcome {
            command_name,
            lineage,
            idempotency_key,
            outcome_uri,
        } => {
            let (contract_lineage, command_name, idempotency_key, outcome_uri) =
                match (command_name, lineage, idempotency_key, outcome_uri) {
                    (Some(command), Some(lineage), Some(key), None)
                        if !command.is_empty() && !lineage.is_empty() && !key.is_empty() =>
                    {
                        (lineage, command, key, None)
                    }
                    (None, None, None, Some(locator))
                        if !locator.is_empty() && locator.len() <= 1_745 =>
                    {
                        (String::new(), String::new(), String::new(), Some(locator))
                    }
                    _ => return invalid_input(CommandIdentity::CommandOutcome),
                };
            let metadata =
                match required_metadata(CommandIdentity::CommandOutcome, config, environment) {
                    Ok(metadata) => metadata,
                    Err(terminal) => return terminal,
                };
            let mut client = match connect(config).await {
                Ok(client) => client,
                Err(error) => return client_error(CommandIdentity::CommandOutcome, &error),
            };
            let request_id = match request_id() {
                Ok(request_id) => request_id,
                Err(error) => return client_error(CommandIdentity::CommandOutcome, &error),
            };
            let request = v1::GetOutcomeRequest {
                request_id,
                contract_lineage,
                command_name,
                idempotency_key,
                outcome_uri,
            };
            match client.get_outcome(request, &metadata).await {
                Ok(response) => render_outcome(&response),
                Err(error) => client_error(CommandIdentity::CommandOutcome, &error),
            }
        }
    }
}

async fn entity_command(
    command: EntityCommand,
    config: &EffectiveConfig,
    environment: &dyn Environment,
) -> Terminal {
    let EntityCommand::Get {
        entity_type_id,
        entity_key,
        contract,
        fields,
    } = command;
    let entity_type_id = match parse_nonzero_u32(&entity_type_id) {
        Ok(value) => value,
        Err(()) => return invalid_input(CommandIdentity::EntityGet),
    };
    let entity_key = match canonical_base64(&entity_key) {
        Ok(value) => value,
        Err(()) => return invalid_input(CommandIdentity::EntityGet),
    };
    let contract = match contract_selection(contract) {
        Ok(value) => value,
        Err(()) => return invalid_input(CommandIdentity::EntityGet),
    };
    let fields = match increasing_nonzero_u32(&fields) {
        Ok(value) => value,
        Err(()) => return invalid_input(CommandIdentity::EntityGet),
    };
    let metadata = match required_metadata(CommandIdentity::EntityGet, config, environment) {
        Ok(metadata) => metadata,
        Err(terminal) => return terminal,
    };
    let mut client = match connect(config).await {
        Ok(client) => client,
        Err(error) => return client_error(CommandIdentity::EntityGet, &error),
    };
    let request_id = match request_id() {
        Ok(request_id) => request_id,
        Err(error) => return client_error(CommandIdentity::EntityGet, &error),
    };
    let request = v1::GetEntityRequest {
        request_id,
        contract: Some(contract),
        entity_type_id,
        entity_key,
        fields: Some(v1::FieldSelection { field_ids: fields }),
    };
    match client.get_entity(request, &metadata).await {
        Ok(response) => render_entity(&response),
        Err(error) => client_error(CommandIdentity::EntityGet, &error),
    }
}

async fn commit_command(
    command: CommitCommand,
    config: &EffectiveConfig,
    environment: &dyn Environment,
) -> Terminal {
    let CommitCommand::Show {
        commit_sequence,
        observed_history_incarnation,
    } = command;
    let commit_sequence = match parse_nonzero_u64(&commit_sequence) {
        Ok(value) => value,
        Err(()) => return invalid_input(CommandIdentity::CommitShow),
    };
    let observed_history_incarnation = match observed_history_incarnation {
        None => None,
        Some(value) => match parse_nonzero_u64(&value) {
            Ok(value) => Some(value),
            Err(()) => return invalid_input(CommandIdentity::CommitShow),
        },
    };
    let metadata = match required_metadata(CommandIdentity::CommitShow, config, environment) {
        Ok(metadata) => metadata,
        Err(terminal) => return terminal,
    };
    let mut client = match connect(config).await {
        Ok(client) => client,
        Err(error) => return client_error(CommandIdentity::CommitShow, &error),
    };
    let request_id = match request_id() {
        Ok(request_id) => request_id,
        Err(error) => return client_error(CommandIdentity::CommitShow, &error),
    };
    match client
        .get_commit(
            v1::GetCommitRequest {
                request_id,
                commit_sequence,
                observed_history_incarnation,
            },
            &metadata,
        )
        .await
    {
        Ok(response) => render_commit(&response),
        Err(error) => client_error(CommandIdentity::CommitShow, &error),
    }
}

async fn bind_compiled_role(
    binding: PreparedRoleBinding,
    identity: CommandIdentity,
    config: &EffectiveConfig,
    environment: &dyn Environment,
) -> Terminal {
    let PreparedRoleBinding {
        role,
        principal,
        actor_kind,
        lifetime_seconds,
        audiences,
        capability_id,
        credential_output,
    } = binding;
    let lifetime_seconds = match parse_nonzero_u32(&lifetime_seconds) {
        Ok(value) => value,
        Err(()) => return invalid_input(identity),
    };
    let capability_id = match capability_id {
        Some(value) => match parse_uuid_v7(&value) {
            Some(value) => value,
            None => return invalid_input(identity),
        },
        None => match generate_capability_id() {
            Ok(value) => value.into_bytes(),
            Err(error) => return client_error(identity, &ClientError::IdentifierGeneration(error)),
        },
    };
    if validate_path(&credential_output).is_err()
        || credential_output.as_encoded_bytes() == b"-"
        || principal.is_empty()
        || audiences.is_empty()
    {
        return invalid_input(identity);
    }
    let request = v1::CreateCapabilityRequest {
        request_id: Vec::new(),
        mode: v1::CapabilityCreateMode::Normal as i32,
        capability_id: capability_id.to_vec(),
        principal_id: principal,
        actor_kind: match actor_kind {
            RoleActorKind::Human => v1::ActorKind::Human as i32,
            RoleActorKind::Agent => v1::ActorKind::Agent as i32,
            RoleActorKind::Service => v1::ActorKind::Service as i32,
        },
        requested_lifetime_seconds: lifetime_seconds,
        audiences,
        grant: Some(application_role_grant_to_proto(role.internal_grant())),
    };
    let template = match NormalCapabilityCreateTemplate::new(request) {
        Ok(template) => template,
        Err(_) => return role_invalid(identity),
    };
    let metadata = match required_metadata(identity, config, environment) {
        Ok(metadata) => metadata,
        Err(terminal) => return terminal,
    };
    let mut client = match connect(config).await {
        Ok(client) => client,
        Err(error) => return client_error(identity, &error),
    };
    let attempts = AttemptBudget::new(config.max_attempts).expect("configuration bound");
    let mut response =
        match submit_normal_create_retry(&mut client, &template, attempts, &metadata).await {
            Ok(response) => response,
            Err(ClientError::OutcomeUnknown(_)) => {
                return uncertain(
                    identity,
                    "role_bind_outcome_unknown",
                    "the role-binding outcome remains unknown",
                    "retry_with_same_capability_id",
                    Some(&capability_id),
                );
            }
            Err(error) => return client_error(identity, &error),
        };
    let Some((disposition, token)) = take_normal_create_disposition(&mut response) else {
        return local_error(identity, "output_render_failed", "output rendering failed");
    };
    match (&disposition, token) {
        (NormalCreateDisposition::Created(_), Some(token)) => {
            if retain_normal_token(&credential_output, token).is_err() {
                return local_error(
                    identity,
                    "credential_retention_failed",
                    "credential retention failed",
                );
            }
        }
        (NormalCreateDisposition::Created(_), None) | (_, Some(_)) => {
            return local_error(identity, "output_render_failed", "output rendering failed");
        }
        _ => {}
    }
    render_normal_create(identity, &disposition)
}

async fn role_command(
    command: RoleCommand,
    config: &EffectiveConfig,
    environment: &dyn Environment,
) -> Terminal {
    match command {
        RoleCommand::Check {
            manifest,
            role,
            tenant,
        } => {
            let role = match compile_role_from_workspace(&manifest, &role, tenant.as_deref()) {
                Ok(role) => role,
                Err(error) => return error.terminal(CommandIdentity::RoleCheck),
            };
            success(
                CommandIdentity::RoleCheck,
                "checked",
                &role_description(&role),
            )
        }
        RoleCommand::Describe {
            manifest,
            role,
            tenant,
        } => {
            let role = match compile_role_from_workspace(&manifest, &role, tenant.as_deref()) {
                Ok(role) => role,
                Err(error) => return error.terminal(CommandIdentity::RoleDescribe),
            };
            success(
                CommandIdentity::RoleDescribe,
                "described",
                &role_description(&role),
            )
        }
        RoleCommand::Bind {
            manifest,
            role,
            tenant,
            principal,
            actor_kind,
            lifetime_seconds,
            audiences,
            capability_id,
            credential_output,
        } => {
            let role = match compile_role_from_workspace(&manifest, &role, tenant.as_deref()) {
                Ok(role) => role,
                Err(error) => return error.terminal(CommandIdentity::RoleBind),
            };
            bind_compiled_role(
                PreparedRoleBinding {
                    role,
                    principal,
                    actor_kind,
                    lifetime_seconds,
                    audiences,
                    capability_id,
                    credential_output,
                },
                CommandIdentity::RoleBind,
                config,
                environment,
            )
            .await
        }
        RoleCommand::Revoke {
            capability_id,
            reason,
        } => {
            let capability_id = match parse_uuid_v7(&capability_id) {
                Some(value) => value,
                None => return invalid_input(CommandIdentity::RoleRevoke),
            };
            let metadata = match required_metadata(CommandIdentity::RoleRevoke, config, environment)
            {
                Ok(metadata) => metadata,
                Err(terminal) => return terminal,
            };
            let mut client = match connect(config).await {
                Ok(client) => client,
                Err(error) => return client_error(CommandIdentity::RoleRevoke, &error),
            };
            let request_id = match request_id() {
                Ok(request_id) => request_id,
                Err(error) => return client_error(CommandIdentity::RoleRevoke, &error),
            };
            let request = v1::RevokeCapabilityRequest {
                request_id,
                capability_id: capability_id.to_vec(),
                reason: revocation_reason_to_proto(reason),
            };
            match client.revoke_capability(request, &metadata).await {
                Ok(response) => render_revoke(CommandIdentity::RoleRevoke, &response),
                Err(error) => client_error(CommandIdentity::RoleRevoke, &error),
            }
        }
    }
}

#[derive(Debug)]
enum RoleWorkspaceError {
    Authoring(AuthoringDiagnostics),
    Invalid,
}

impl RoleWorkspaceError {
    fn terminal(self, command: CommandIdentity) -> Terminal {
        match self {
            Self::Authoring(diagnostics) => authoring_error(command, &diagnostics),
            Self::Invalid => role_invalid(command),
        }
    }
}

fn compile_role_from_workspace(
    manifest_path: &OsString,
    role_name: &str,
    tenant: Option<&str>,
) -> Result<CompiledApplicationRole, RoleWorkspaceError> {
    let requested_path = Path::new(manifest_path);
    let requested_bytes =
        read_file(requested_path, MAX_INPUT_BYTES).map_err(|_| RoleWorkspaceError::Invalid)?;
    let requested_value: serde_json::Value =
        serde_json::from_slice(&requested_bytes).map_err(|_| RoleWorkspaceError::Invalid)?;
    let requested_is_source = matches!(
        requested_value
            .get("schema")
            .and_then(serde_json::Value::as_str),
        Some("riffdb.application-source/v1" | "riffdb.application-source/v2")
    );
    let source_locked = requested_is_source
        .then(|| load_locked_application(requested_path, None))
        .transpose()
        .map_err(role_workspace_lock_error)?;
    let manifest_path = source_locked
        .as_ref()
        .map_or(requested_path, |locked| locked.manifest_path());
    let manifest_bytes =
        read_file(manifest_path, MAX_INPUT_BYTES).map_err(|_| RoleWorkspaceError::Invalid)?;
    let manifest_source =
        std::str::from_utf8(&manifest_bytes).map_err(|_| RoleWorkspaceError::Invalid)?;
    let manifest = ApplicationManifest::parse(manifest_source)
        .map_err(|error| role_manifest_diagnostic(manifest_path, error.kind()))?;
    let workspace = find_application_workspace(manifest_path, manifest.contract().source())
        .map_err(|()| RoleWorkspaceError::Invalid)?;
    let discovered_source_path = workspace.join("riffdb.application.json");
    let discovered_lock_path = workspace.join("riffdb.application.lock.json");
    let discovered_locked = if source_locked.is_none()
        && discovered_source_path.is_file()
        && discovered_lock_path.is_file()
    {
        Some(
            load_locked_application(&discovered_source_path, None)
                .map_err(role_workspace_lock_error)?,
        )
    } else {
        None
    };
    let locked = source_locked.as_ref().or(discovered_locked.as_ref());
    let contract = if let Some(locked) = locked {
        if locked.manifest().identity() != manifest.identity() {
            return Err(RoleWorkspaceError::Invalid);
        }
        locked.contract().clone()
    } else {
        let contract_source = read_workspace_text(&workspace, manifest.contract().source())
            .map_err(|()| RoleWorkspaceError::Invalid)?;
        compile_contract_source(&contract_source).map_err(|error| {
            AuthoringSourcePath::new(manifest.contract().source())
                .ok()
                .and_then(|path| AuthoringDiagnostics::from_contract(path, &error).ok())
                .map_or(RoleWorkspaceError::Invalid, RoleWorkspaceError::Authoring)
        })?
    };
    let mut modules = Vec::with_capacity(manifest.query_modules().len());
    for module in manifest.query_modules() {
        let queries = module
            .queries()
            .iter()
            .map(|query| {
                let source = read_workspace_text(&workspace, query.source())
                    .map_err(|()| RoleWorkspaceError::Invalid)?;
                NamedQuerySource::new(query.name(), source).map_err(|_| RoleWorkspaceError::Invalid)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let candidate = QueryModuleCandidate::new(
            QueryModuleName::new(module.name()).map_err(|_| RoleWorkspaceError::Invalid)?,
            QueryModuleVersion::new(module.version()).ok_or(RoleWorkspaceError::Invalid)?,
            queries,
        )
        .map_err(|_| RoleWorkspaceError::Invalid)?;
        modules.push(QueryModule::compile(candidate, &contract).map_err(|error| {
            let query_path = error.query_name().and_then(|name| {
                module
                    .queries()
                    .iter()
                    .find(|query| query.name() == name)
                    .map(|query| query.source())
            });
            query_path
                .and_then(|path| AuthoringSourcePath::new(path).ok())
                .and_then(|path| AuthoringDiagnostics::from_query_module(path, &error).ok())
                .map_or(RoleWorkspaceError::Invalid, RoleWorkspaceError::Authoring)
        })?);
    }
    let tenant = tenant
        .map(|tenant| TenantId::new(tenant.to_owned()).map_err(|_| RoleWorkspaceError::Invalid))
        .transpose()?;
    compile_application_role(&manifest, role_name, tenant, &contract, &modules).map_err(|error| {
        AuthoringSourcePath::new(
            requested_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("riffdb.application.json"),
        )
        .ok()
        .and_then(|path| AuthoringDiagnostics::from_role(path, error.kind(), Some(role_name)).ok())
        .map_or(RoleWorkspaceError::Invalid, RoleWorkspaceError::Authoring)
    })
}

fn role_workspace_lock_error(error: crate::scaffold::ScaffoldError) -> RoleWorkspaceError {
    error
        .diagnostics()
        .cloned()
        .map_or(RoleWorkspaceError::Invalid, RoleWorkspaceError::Authoring)
}

fn role_manifest_diagnostic(
    manifest_path: &Path,
    kind: riffdb_query_module::ManifestErrorKind,
) -> RoleWorkspaceError {
    manifest_path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|path| AuthoringSourcePath::new(path).ok())
        .and_then(|path| AuthoringDiagnostics::from_manifest(path, kind).ok())
        .map_or(RoleWorkspaceError::Invalid, RoleWorkspaceError::Authoring)
}

fn find_application_workspace(
    manifest_path: &Path,
    contract_path: &str,
) -> Result<std::path::PathBuf, ()> {
    let absolute = if manifest_path.is_absolute() {
        manifest_path.to_path_buf()
    } else {
        std::env::current_dir().map_err(|_| ())?.join(manifest_path)
    };
    let mut matches = absolute
        .parent()
        .into_iter()
        .flat_map(Path::ancestors)
        .filter(|ancestor| ancestor.join(contract_path).is_file());
    let workspace = matches.next().ok_or(())?.to_path_buf();
    if matches.next().is_some() {
        return Err(());
    }
    Ok(workspace)
}

fn read_workspace_text(workspace: &Path, path: &str) -> Result<String, ()> {
    let relative = OsString::from(path);
    validate_path(&relative).map_err(|_| ())?;
    let bytes = read_file(&workspace.join(path), MAX_INPUT_BYTES).map_err(|_| ())?;
    String::from_utf8(bytes).map_err(|_| ())
}

fn role_description(role: &CompiledApplicationRole) -> serde_json::Value {
    serde_json::json!({
        "application": role.application_name(),
        "application_manifest_hash": hex(role.manifest_hash().as_bytes()),
        "role": role.role_name(),
        "role_hash": hex(role.identity().as_bytes()),
        "environment": role.environment().as_str(),
        "tenant_scope": match role.tenant_scope() {
            TenantScope::Global => "global",
            TenantScope::Tenant(_) => "tenant",
        },
        "contract": {
            "lineage": role.contract_lineage().as_str(),
            "version": role.contract_version().get().to_string(),
            "bundle_hash": hex(role.contract_hash().as_bytes()),
        },
        "query_module_hashes": role.module_hashes().iter()
            .map(|hash| hex(hash.as_bytes()))
            .collect::<Vec<_>>(),
        "operations": role.operations().iter().map(|operation| serde_json::json!({
            "kind": match operation.kind() {
                riffdb_query_module::ApplicationRoleOperationKind::Query => "query",
                riffdb_query_module::ApplicationRoleOperationKind::Command => "command",
            },
            "name": operation.name(),
        })).collect::<Vec<_>>(),
    })
}

fn application_role_grant_to_proto(grant: &CapabilityGrantV1) -> v1::CapabilityGrant {
    let tenant_scope = match grant.tenant_scope() {
        TenantScope::Global => v1::TenantScope {
            scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
        },
        TenantScope::Tenant(tenant) => v1::TenantScope {
            scope: Some(v1::tenant_scope::Scope::TenantId(
                tenant.as_str().to_owned(),
            )),
        },
    };
    let partition_scope = match grant.partition_scope() {
        PartitionScopeV1::All => v1::PartitionScope {
            scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
        },
        PartitionScopeV1::Explicit(_) => unreachable!("application roles never expose partitions"),
    };
    v1::CapabilityGrant {
        tenant_scope: Some(tenant_scope),
        partition_scope: Some(partition_scope),
        permissions: grant
            .permissions()
            .as_slice()
            .iter()
            .map(application_role_permission_to_proto)
            .collect(),
        field_visibility: grant
            .field_visibility()
            .iter()
            .map(|visibility| v1::EntityFieldVisibility {
                contract_lineage: visibility.lineage().as_str().to_owned(),
                entity_type_id: visibility.entity_type().get(),
                field_ids: visibility
                    .fields()
                    .iter()
                    .map(|field| field.get())
                    .collect(),
            })
            .collect(),
        max_scan_rows: u32::from(grant.max_scan_rows().get()),
        approval_required: Vec::new(),
    }
}

fn application_role_permission_to_proto(
    permission: &CapabilityPermissionV1,
) -> v1::CapabilityPermission {
    use v1::capability_permission::Permission;
    let permission = match permission {
        CapabilityPermissionV1::InvokeCommand(lineage, command) => {
            Permission::InvokeCommand(v1::LineageScopedStableId {
                contract_lineage: lineage.as_str().to_owned(),
                stable_id: command.get(),
            })
        }
        CapabilityPermissionV1::ExecuteNamedQuery(lineage, module_hash, query_name) => {
            Permission::ExecuteNamedQuery(v1::NamedQueryPermission {
                contract_lineage: lineage.as_str().to_owned(),
                query_module_hash: module_hash.as_bytes().to_vec(),
                query_name: query_name.as_str().to_owned(),
            })
        }
        CapabilityPermissionV1::ApplicationRoleIdentity(role_hash) => {
            Permission::ApplicationRoleIdentity(role_hash.as_bytes().to_vec())
        }
        _ => unreachable!("application role compiler emitted kernel authority"),
    };
    v1::CapabilityPermission {
        permission: Some(permission),
    }
}

fn role_invalid(command: CommandIdentity) -> Terminal {
    local_error(
        command,
        "role_invalid",
        "symbolic application role compilation failed",
    )
}

const fn revocation_reason_to_proto(reason: RevocationReason) -> i32 {
    match reason {
        RevocationReason::Requested => v1::RevocationReason::Requested as i32,
        RevocationReason::Replaced => v1::RevocationReason::Replaced as i32,
        RevocationReason::SuspectedCompromise => v1::RevocationReason::SuspectedCompromise as i32,
        RevocationReason::PolicyChange => v1::RevocationReason::PolicyChange as i32,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectionInput {
    leading_components: Vec<InputValue>,
}

async fn projection_command(
    command: ProjectionCommand,
    config: &EffectiveConfig,
    environment: &dyn Environment,
    stdin: &mut dyn Read,
) -> Terminal {
    let ProjectionCommand::Query {
        projection_id,
        input,
        contract,
        after,
        wait_nanos,
        limit,
        cursor,
    } = command;
    let projection_id = match parse_nonzero_u32(&projection_id) {
        Ok(value) => value,
        Err(()) => return invalid_input(CommandIdentity::ProjectionQuery),
    };
    let input = match read_json::<ProjectionInput>(&input, stdin) {
        Ok(input) => input,
        Err(error) => return input_terminal(CommandIdentity::ProjectionQuery, error),
    };
    let leading_components = match input
        .leading_components
        .into_iter()
        .map(InputValue::into_proto)
        .collect::<Result<Vec<_>, ValueError>>()
    {
        Ok(values) => values,
        Err(_) => return invalid_input(CommandIdentity::ProjectionQuery),
    };
    let contract = match contract_selection(contract) {
        Ok(value) => value,
        Err(()) => return invalid_input(CommandIdentity::ProjectionQuery),
    };
    let required_sequence = match after.as_deref().map(parse_nonzero_u64).transpose() {
        Ok(value) => value,
        Err(()) => return invalid_input(CommandIdentity::ProjectionQuery),
    };
    let wait_nanos = match wait_nanos.as_deref().map(parse_u64).transpose() {
        Ok(Some(value)) if value <= 30_000_000_000 && required_sequence.is_some() => value,
        Ok(None) => 0,
        _ => return invalid_input(CommandIdentity::ProjectionQuery),
    };
    let limit = match page_limit(limit.as_deref()) {
        Ok(value) => value,
        Err(()) => return invalid_input(CommandIdentity::ProjectionQuery),
    };
    let cursor = match cursor.as_deref().map(canonical_base64).transpose() {
        Ok(Some(value)) if value.len() == 16 => Some(value),
        Ok(None) => None,
        Err(()) => return invalid_input(CommandIdentity::ProjectionQuery),
        Ok(Some(_)) => return invalid_input(CommandIdentity::ProjectionQuery),
    };
    let metadata = match required_metadata(CommandIdentity::ProjectionQuery, config, environment) {
        Ok(metadata) => metadata,
        Err(terminal) => return terminal,
    };
    let mut client = match connect(config).await {
        Ok(client) => client,
        Err(error) => return client_error(CommandIdentity::ProjectionQuery, &error),
    };
    let request_id = match request_id() {
        Ok(request_id) => request_id,
        Err(error) => return client_error(CommandIdentity::ProjectionQuery, &error),
    };
    let request = v1::QueryProjectionRequest {
        request_id,
        contract: Some(contract),
        projection_id,
        leading_components,
        required_sequence,
        wait_nanos,
        page: Some(v1::PageRequest {
            limit: Some(limit),
            cursor,
        }),
    };
    match client.query_projection(request, &metadata).await {
        Ok(response) => render_projection(&response),
        Err(error) => client_error(CommandIdentity::ProjectionQuery, &error),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilityCreateInput {
    principal_id: String,
    actor_kind: ActorKindInput,
    requested_lifetime_seconds: u32,
    audiences: Vec<String>,
    grant: CapabilityGrantInput,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ActorKindInput {
    Human,
    Agent,
    Service,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilityGrantInput {
    tenant_scope: TenantScopeInput,
    partition_scope: PartitionScopeInput,
    permissions: Vec<CapabilityPermissionInput>,
    field_visibility: Vec<FieldVisibilityInput>,
    max_scan_rows: u32,
    approval_required: Vec<CapabilityPermissionKindInput>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum TenantScopeInput {
    Global {},
    Tenant { tenant_id: String },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum PartitionScopeInput {
    All {},
    Explicit {
        partitions: Vec<ScopedPartitionInput>,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopedPartitionInput {
    contract_lineage: String,
    #[serde(deserialize_with = "deserialize_canonical_base64")]
    partition_key: Vec<u8>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum CapabilityPermissionInput {
    ValidateContract {},
    ReadContract {},
    ExplainCommand {
        contract_lineage: String,
        stable_id: u32,
    },
    DeployContract {},
    InvokeCommand {
        contract_lineage: String,
        stable_id: u32,
    },
    ReadEntity {
        contract_lineage: String,
        stable_id: u32,
    },
    ScanIndex {
        contract_lineage: String,
        stable_id: u32,
    },
    QueryProjection {
        contract_lineage: String,
        stable_id: u32,
    },
    ReadProjectionStatus {
        contract_lineage: String,
        stable_id: u32,
    },
    ReadCommit {},
    ScanCommits {},
    SubscribeCommits {},
    ReadProvenance {},
    InspectOutbox {},
    ReadHealth {},
    ReadStatistics {},
    CreateCapability {},
    RevokeCapability {},
    AdministerCapabilities {},
    CheckAdHocQuery {},
    ExplainAdHocQuery {},
    ExecuteAdHocQuery {},
    ExplainNamedQuery {
        contract_lineage: String,
        query_module_hash: String,
        query_name: String,
    },
    ExecuteNamedQuery {
        contract_lineage: String,
        query_module_hash: String,
        query_name: String,
    },
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CapabilityPermissionKindInput {
    ValidateContract,
    ReadContract,
    ExplainCommand,
    DeployContract,
    InvokeCommand,
    ReadEntity,
    ScanIndex,
    QueryProjection,
    ReadProjectionStatus,
    ReadCommit,
    ScanCommits,
    SubscribeCommits,
    ReadProvenance,
    InspectOutbox,
    ReadHealth,
    ReadStatistics,
    CreateCapability,
    RevokeCapability,
    AdministerCapabilities,
    CheckAdHocQuery,
    ExplainAdHocQuery,
    ExecuteAdHocQuery,
    ExplainNamedQuery,
    ExecuteNamedQuery,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldVisibilityInput {
    contract_lineage: String,
    entity_type_id: u32,
    field_ids: Vec<u32>,
}

async fn capability_command(
    command: CapabilityCommand,
    config: &EffectiveConfig,
    environment: &dyn Environment,
    stdin: &mut dyn Read,
) -> Terminal {
    match command {
        CapabilityCommand::Bootstrap {
            request,
            generate,
            bootstrap_file,
            bootstrap_stdin,
            bearer_output,
        } => {
            if request.as_encoded_bytes() == b"-" {
                return invalid_input(CommandIdentity::CapabilityBootstrap);
            }
            let input = match read_json::<CapabilityCreateInput>(&request, stdin) {
                Ok(input) => input,
                Err(error) => {
                    return input_terminal(CommandIdentity::CapabilityBootstrap, error);
                }
            };
            let material = match bootstrap_material(
                generate.as_ref(),
                bootstrap_file.as_ref(),
                bootstrap_stdin,
                bearer_output.as_ref(),
                stdin,
            ) {
                Ok(material) => material,
                Err(error) => return bootstrap_credential_terminal(error),
            };
            let request = match capability_request(
                input,
                material.credential.capability_id().into_bytes().to_vec(),
                v1::CapabilityCreateMode::Bootstrap,
            ) {
                Ok(request) => request,
                Err(()) => return invalid_input(CommandIdentity::CapabilityBootstrap),
            };
            let template = match BootstrapCapabilityCreateTemplate::new(request) {
                Ok(template) => template,
                Err(_) => return invalid_input(CommandIdentity::CapabilityBootstrap),
            };
            let metadata = match material.metadata(config.database.clone()) {
                Ok(metadata) => metadata,
                Err(error) => return bootstrap_credential_terminal(error),
            };
            let mut client = match connect(config).await {
                Ok(client) => client,
                Err(error) => return client_error(CommandIdentity::CapabilityBootstrap, &error),
            };
            let attempts = AttemptBudget::new(config.max_attempts).expect("configuration bound");
            match submit_bootstrap_retry(&mut client, &template, attempts, &metadata).await {
                Ok(response) => render_bootstrap(&response, material.bearer_retained),
                Err(ClientError::OutcomeUnknown(_)) => uncertain(
                    CommandIdentity::CapabilityBootstrap,
                    "bootstrap_outcome_unknown",
                    "the bootstrap outcome remains unknown",
                    "retry_with_retained_bootstrap_credential",
                    Some(&material.credential.capability_id().into_bytes()),
                ),
                Err(error) => client_error(CommandIdentity::CapabilityBootstrap, &error),
            }
        }
        CapabilityCommand::Create {
            request,
            capability_id,
            credential_output,
        } => {
            let input = match read_json::<CapabilityCreateInput>(&request, stdin) {
                Ok(input) => input,
                Err(error) => return input_terminal(CommandIdentity::CapabilityCreate, error),
            };
            let capability_id = match capability_id {
                Some(value) => match parse_uuid_v7(&value) {
                    Some(value) => value,
                    None => return invalid_input(CommandIdentity::CapabilityCreate),
                },
                None => match generate_capability_id() {
                    Ok(value) => value.into_bytes(),
                    Err(error) => {
                        return client_error(
                            CommandIdentity::CapabilityCreate,
                            &ClientError::IdentifierGeneration(error),
                        );
                    }
                },
            };
            if validate_path(&credential_output).is_err()
                || credential_output.as_encoded_bytes() == b"-"
            {
                return invalid_input(CommandIdentity::CapabilityCreate);
            }
            let request = match capability_request(
                input,
                capability_id.to_vec(),
                v1::CapabilityCreateMode::Normal,
            ) {
                Ok(request) => request,
                Err(()) => return invalid_input(CommandIdentity::CapabilityCreate),
            };
            let template = match NormalCapabilityCreateTemplate::new(request) {
                Ok(template) => template,
                Err(_) => return invalid_input(CommandIdentity::CapabilityCreate),
            };
            let metadata =
                match required_metadata(CommandIdentity::CapabilityCreate, config, environment) {
                    Ok(metadata) => metadata,
                    Err(terminal) => return terminal,
                };
            let mut client = match connect(config).await {
                Ok(client) => client,
                Err(error) => return client_error(CommandIdentity::CapabilityCreate, &error),
            };
            let attempts = AttemptBudget::new(config.max_attempts).expect("configuration bound");
            let mut response =
                match submit_normal_create_retry(&mut client, &template, attempts, &metadata).await
                {
                    Ok(response) => response,
                    Err(ClientError::OutcomeUnknown(_)) => {
                        return uncertain(
                            CommandIdentity::CapabilityCreate,
                            "capability_create_outcome_unknown",
                            "the capability-create outcome remains unknown",
                            "retry_with_same_capability_id",
                            Some(&capability_id),
                        );
                    }
                    Err(error) => return client_error(CommandIdentity::CapabilityCreate, &error),
                };
            let Some((disposition, token)) = take_normal_create_disposition(&mut response) else {
                return local_error(
                    CommandIdentity::CapabilityCreate,
                    "output_render_failed",
                    "output rendering failed",
                );
            };
            match (&disposition, token) {
                (NormalCreateDisposition::Created(_), Some(token)) => {
                    if retain_normal_token(&credential_output, token).is_err() {
                        return local_error(
                            CommandIdentity::CapabilityCreate,
                            "credential_retention_failed",
                            "credential retention failed",
                        );
                    }
                }
                (NormalCreateDisposition::Created(_), None) | (_, Some(_)) => {
                    return local_error(
                        CommandIdentity::CapabilityCreate,
                        "output_render_failed",
                        "output rendering failed",
                    );
                }
                _ => {}
            }
            render_normal_create(CommandIdentity::CapabilityCreate, &disposition)
        }
        CapabilityCommand::Revoke {
            capability_id,
            reason,
        } => {
            let capability_id = match parse_uuid_v7(&capability_id) {
                Some(value) => value,
                None => return invalid_input(CommandIdentity::CapabilityRevoke),
            };
            let metadata =
                match required_metadata(CommandIdentity::CapabilityRevoke, config, environment) {
                    Ok(metadata) => metadata,
                    Err(terminal) => return terminal,
                };
            let mut client = match connect(config).await {
                Ok(client) => client,
                Err(error) => return client_error(CommandIdentity::CapabilityRevoke, &error),
            };
            let request_id = match request_id() {
                Ok(request_id) => request_id,
                Err(error) => return client_error(CommandIdentity::CapabilityRevoke, &error),
            };
            let request = v1::RevokeCapabilityRequest {
                request_id,
                capability_id: capability_id.to_vec(),
                reason: revocation_reason_to_proto(reason),
            };
            match client.revoke_capability(request, &metadata).await {
                Ok(response) => render_revoke(CommandIdentity::CapabilityRevoke, &response),
                Err(error) => client_error(CommandIdentity::CapabilityRevoke, &error),
            }
        }
    }
}

fn capability_request(
    input: CapabilityCreateInput,
    capability_id: Vec<u8>,
    mode: v1::CapabilityCreateMode,
) -> Result<v1::CreateCapabilityRequest, ()> {
    Ok(v1::CreateCapabilityRequest {
        request_id: Vec::new(),
        mode: mode as i32,
        capability_id,
        principal_id: input.principal_id,
        actor_kind: match input.actor_kind {
            ActorKindInput::Human => v1::ActorKind::Human as i32,
            ActorKindInput::Agent => v1::ActorKind::Agent as i32,
            ActorKindInput::Service => v1::ActorKind::Service as i32,
        },
        requested_lifetime_seconds: input.requested_lifetime_seconds,
        audiences: input.audiences,
        grant: Some(capability_grant(input.grant)?),
    })
}

fn capability_grant(input: CapabilityGrantInput) -> Result<v1::CapabilityGrant, ()> {
    let tenant_scope = match input.tenant_scope {
        TenantScopeInput::Global {} => v1::TenantScope {
            scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
        },
        TenantScopeInput::Tenant { tenant_id } => v1::TenantScope {
            scope: Some(v1::tenant_scope::Scope::TenantId(tenant_id)),
        },
    };
    let partition_scope = match input.partition_scope {
        PartitionScopeInput::All {} => v1::PartitionScope {
            scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
        },
        PartitionScopeInput::Explicit { partitions } => v1::PartitionScope {
            scope: Some(v1::partition_scope::Scope::Explicit(
                v1::ExplicitPartitionScope {
                    partitions: partitions
                        .into_iter()
                        .map(|partition| v1::ScopedPartition {
                            contract_lineage: partition.contract_lineage,
                            partition_key: partition.partition_key,
                        })
                        .collect(),
                },
            )),
        },
    };
    let permissions = input
        .permissions
        .into_iter()
        .map(capability_permission)
        .collect::<Result<_, _>>()?;
    let field_visibility = input
        .field_visibility
        .into_iter()
        .map(|visibility| {
            if visibility.entity_type_id == 0 || visibility.field_ids.contains(&0) {
                return Err(());
            }
            Ok(v1::EntityFieldVisibility {
                contract_lineage: visibility.contract_lineage,
                entity_type_id: visibility.entity_type_id,
                field_ids: visibility.field_ids,
            })
        })
        .collect::<Result<_, _>>()?;
    Ok(v1::CapabilityGrant {
        tenant_scope: Some(tenant_scope),
        partition_scope: Some(partition_scope),
        permissions,
        field_visibility,
        max_scan_rows: input.max_scan_rows,
        approval_required: input
            .approval_required
            .into_iter()
            .map(permission_kind)
            .collect(),
    })
}

fn capability_permission(input: CapabilityPermissionInput) -> Result<v1::CapabilityPermission, ()> {
    use v1::capability_permission::Permission;
    let scoped = |contract_lineage: String, stable_id: u32| {
        if contract_lineage.is_empty() || stable_id == 0 {
            Err(())
        } else {
            Ok(v1::LineageScopedStableId {
                contract_lineage,
                stable_id,
            })
        }
    };
    let permission = match input {
        CapabilityPermissionInput::ValidateContract {} => Permission::ValidateContract(v1::Unit {}),
        CapabilityPermissionInput::ReadContract {} => Permission::ReadContract(v1::Unit {}),
        CapabilityPermissionInput::ExplainCommand {
            contract_lineage,
            stable_id,
        } => Permission::ExplainCommand(scoped(contract_lineage, stable_id)?),
        CapabilityPermissionInput::DeployContract {} => Permission::DeployContract(v1::Unit {}),
        CapabilityPermissionInput::InvokeCommand {
            contract_lineage,
            stable_id,
        } => Permission::InvokeCommand(scoped(contract_lineage, stable_id)?),
        CapabilityPermissionInput::ReadEntity {
            contract_lineage,
            stable_id,
        } => Permission::ReadEntity(scoped(contract_lineage, stable_id)?),
        CapabilityPermissionInput::ScanIndex {
            contract_lineage,
            stable_id,
        } => Permission::ScanIndex(scoped(contract_lineage, stable_id)?),
        CapabilityPermissionInput::QueryProjection {
            contract_lineage,
            stable_id,
        } => Permission::QueryProjection(scoped(contract_lineage, stable_id)?),
        CapabilityPermissionInput::ReadProjectionStatus {
            contract_lineage,
            stable_id,
        } => Permission::ReadProjectionStatus(scoped(contract_lineage, stable_id)?),
        CapabilityPermissionInput::ReadCommit {} => Permission::ReadCommit(v1::Unit {}),
        CapabilityPermissionInput::ScanCommits {} => Permission::ScanCommits(v1::Unit {}),
        CapabilityPermissionInput::SubscribeCommits {} => Permission::SubscribeCommits(v1::Unit {}),
        CapabilityPermissionInput::ReadProvenance {} => Permission::ReadProvenance(v1::Unit {}),
        CapabilityPermissionInput::InspectOutbox {} => Permission::InspectOutbox(v1::Unit {}),
        CapabilityPermissionInput::ReadHealth {} => Permission::ReadHealth(v1::Unit {}),
        CapabilityPermissionInput::ReadStatistics {} => Permission::ReadStatistics(v1::Unit {}),
        CapabilityPermissionInput::CreateCapability {} => Permission::CreateCapability(v1::Unit {}),
        CapabilityPermissionInput::RevokeCapability {} => Permission::RevokeCapability(v1::Unit {}),
        CapabilityPermissionInput::AdministerCapabilities {} => {
            Permission::AdministerCapabilities(v1::Unit {})
        }
        CapabilityPermissionInput::CheckAdHocQuery {} => Permission::CheckAdHocQuery(v1::Unit {}),
        CapabilityPermissionInput::ExplainAdHocQuery {} => {
            Permission::ExplainAdHocQuery(v1::Unit {})
        }
        CapabilityPermissionInput::ExecuteAdHocQuery {} => {
            Permission::ExecuteAdHocQuery(v1::Unit {})
        }
        CapabilityPermissionInput::ExplainNamedQuery {
            contract_lineage,
            query_module_hash,
            query_name,
        } => Permission::ExplainNamedQuery(named_query_permission(
            contract_lineage,
            query_module_hash,
            query_name,
        )?),
        CapabilityPermissionInput::ExecuteNamedQuery {
            contract_lineage,
            query_module_hash,
            query_name,
        } => Permission::ExecuteNamedQuery(named_query_permission(
            contract_lineage,
            query_module_hash,
            query_name,
        )?),
    };
    Ok(v1::CapabilityPermission {
        permission: Some(permission),
    })
}

const fn permission_kind(input: CapabilityPermissionKindInput) -> i32 {
    match input {
        CapabilityPermissionKindInput::ValidateContract => {
            v1::CapabilityPermissionKind::ValidateContract as i32
        }
        CapabilityPermissionKindInput::ReadContract => {
            v1::CapabilityPermissionKind::ReadContract as i32
        }
        CapabilityPermissionKindInput::ExplainCommand => {
            v1::CapabilityPermissionKind::ExplainCommand as i32
        }
        CapabilityPermissionKindInput::DeployContract => {
            v1::CapabilityPermissionKind::DeployContract as i32
        }
        CapabilityPermissionKindInput::InvokeCommand => {
            v1::CapabilityPermissionKind::InvokeCommand as i32
        }
        CapabilityPermissionKindInput::ReadEntity => {
            v1::CapabilityPermissionKind::ReadEntity as i32
        }
        CapabilityPermissionKindInput::ScanIndex => v1::CapabilityPermissionKind::ScanIndex as i32,
        CapabilityPermissionKindInput::QueryProjection => {
            v1::CapabilityPermissionKind::QueryProjection as i32
        }
        CapabilityPermissionKindInput::ReadProjectionStatus => {
            v1::CapabilityPermissionKind::ReadProjectionStatus as i32
        }
        CapabilityPermissionKindInput::ReadCommit => {
            v1::CapabilityPermissionKind::ReadCommit as i32
        }
        CapabilityPermissionKindInput::ScanCommits => {
            v1::CapabilityPermissionKind::ScanCommits as i32
        }
        CapabilityPermissionKindInput::SubscribeCommits => {
            v1::CapabilityPermissionKind::SubscribeCommits as i32
        }
        CapabilityPermissionKindInput::ReadProvenance => {
            v1::CapabilityPermissionKind::ReadProvenance as i32
        }
        CapabilityPermissionKindInput::InspectOutbox => {
            v1::CapabilityPermissionKind::InspectOutbox as i32
        }
        CapabilityPermissionKindInput::ReadHealth => {
            v1::CapabilityPermissionKind::ReadHealth as i32
        }
        CapabilityPermissionKindInput::ReadStatistics => {
            v1::CapabilityPermissionKind::ReadStatistics as i32
        }
        CapabilityPermissionKindInput::CreateCapability => {
            v1::CapabilityPermissionKind::CreateCapability as i32
        }
        CapabilityPermissionKindInput::RevokeCapability => {
            v1::CapabilityPermissionKind::RevokeCapability as i32
        }
        CapabilityPermissionKindInput::AdministerCapabilities => {
            v1::CapabilityPermissionKind::AdministerCapabilities as i32
        }
        CapabilityPermissionKindInput::CheckAdHocQuery => {
            v1::CapabilityPermissionKind::CheckAdHocQuery as i32
        }
        CapabilityPermissionKindInput::ExplainAdHocQuery => {
            v1::CapabilityPermissionKind::ExplainAdHocQuery as i32
        }
        CapabilityPermissionKindInput::ExecuteAdHocQuery => {
            v1::CapabilityPermissionKind::ExecuteAdHocQuery as i32
        }
        CapabilityPermissionKindInput::ExplainNamedQuery => {
            v1::CapabilityPermissionKind::ExplainNamedQuery as i32
        }
        CapabilityPermissionKindInput::ExecuteNamedQuery => {
            v1::CapabilityPermissionKind::ExecuteNamedQuery as i32
        }
    }
}

fn named_query_permission(
    contract_lineage: String,
    query_module_hash: String,
    query_name: String,
) -> Result<v1::NamedQueryPermission, ()> {
    if contract_lineage.is_empty()
        || contract_lineage.len() > 256
        || query_name.is_empty()
        || query_name.len() > 256
    {
        return Err(());
    }
    Ok(v1::NamedQueryPermission {
        contract_lineage,
        query_module_hash: parse_hash(&query_module_hash)?,
        query_name,
    })
}

async fn server_command(
    command: ServerCommand,
    config: &EffectiveConfig,
    environment: &dyn Environment,
) -> Terminal {
    let ServerCommand::Health = command;
    let (metadata, authenticated) = match normal_credential(config, environment) {
        Ok(credential) => (credential.metadata, true),
        Err(CredentialError::Required) => (CallMetadata::default(), false),
        Err(error) => return credential_terminal(CommandIdentity::ServerHealth, error),
    };
    let mut client = match connect(config).await {
        Ok(client) => client,
        Err(error) => return client_error(CommandIdentity::ServerHealth, &error),
    };
    let request_id = if authenticated {
        match generate_request_id() {
            Ok(request_id) => Some(request_id.into_bytes().to_vec()),
            Err(error) => {
                return client_error(
                    CommandIdentity::ServerHealth,
                    &ClientError::IdentifierGeneration(error),
                );
            }
        }
    } else {
        None
    };
    match client
        .health(v1::HealthRequest { request_id }, &metadata)
        .await
    {
        Ok(response) => render_health(&response),
        Err(error) => client_error(CommandIdentity::ServerHealth, &error),
    }
}

#[derive(Serialize)]
struct DemoResult<'a> {
    status: &'a str,
    adapter: &'a str,
    case: &'a str,
    workload_version: u32,
}

struct DemoError {
    code: &'static str,
    message: &'static str,
    case: Option<&'static str>,
    stream: Option<&'static str>,
}

impl Serialize for DemoError {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("type", "local")?;
        map.serialize_entry("code", self.code)?;
        map.serialize_entry("message", self.message)?;
        if let Some(case) = self.case {
            map.serialize_entry("case", case)?;
        }
        if let Some(stream) = self.stream {
            map.serialize_entry("stream", stream)?;
        }
        map.end()
    }
}

fn demo_command(
    command: DemoCommand,
    config: &EffectiveConfig,
    environment: &dyn Environment,
) -> Terminal {
    let DemoCommand::Budget { runner, case } = command;
    if validate_path(&runner).is_err() {
        return demo_error(
            "runner_start_failed",
            "the budget runner could not be started",
            None,
            None,
            2,
        );
    }
    let credential = match normal_credential(config, environment) {
        Ok(credential) => credential,
        Err(CredentialError::Required) => {
            return demo_error(
                "demo_requires_credential_file",
                "the budget demo requires a protected credential file",
                None,
                None,
                2,
            );
        }
        Err(error) => return credential_terminal(CommandIdentity::DemoBudget, error),
    };
    let Some(credential_file) = credential.file.as_ref() else {
        return demo_error(
            "demo_requires_credential_file",
            "the budget demo requires a protected credential file",
            None,
            None,
            2,
        );
    };
    match run_budget(&runner, case, &config.endpoint, credential_file) {
        Ok(()) => success(
            CommandIdentity::DemoBudget,
            "passed",
            &DemoResult {
                status: "passed",
                adapter: "riffdb-public-grpc-v1",
                case: case.as_str(),
                workload_version: 1,
            },
        ),
        Err(RunnerError::StartFailed) => demo_error(
            "runner_start_failed",
            "the budget runner could not be started",
            None,
            None,
            2,
        ),
        Err(RunnerError::Timeout) => demo_error(
            "runner_timeout",
            "the budget runner timed out",
            None,
            None,
            2,
        ),
        Err(RunnerError::OutputTooLarge(stream)) => demo_error(
            "runner_output_too_large",
            "the budget runner output exceeded its limit",
            None,
            Some(match stream {
                RunnerStream::Stdout => "stdout",
                RunnerStream::Stderr => "stderr",
            }),
            2,
        ),
        Err(RunnerError::InvocationInvalid) => demo_error(
            "runner_invocation_invalid",
            "the budget runner rejected its invocation",
            None,
            None,
            2,
        ),
        Err(RunnerError::ProtocolInvalid) => demo_error(
            "runner_protocol_invalid",
            "the budget runner returned an invalid protocol result",
            None,
            None,
            2,
        ),
        Err(RunnerError::CheckedFailure) => demo_error(
            "demo_failed",
            "the budget comparison did not pass",
            Some(case.as_str()),
            None,
            1,
        ),
    }
}

fn demo_error(
    code: &'static str,
    message: &'static str,
    case: Option<&'static str>,
    stream: Option<&'static str>,
    exit: u8,
) -> Terminal {
    local_error_with(
        CommandIdentity::DemoBudget,
        &DemoError {
            code,
            message,
            case,
            stream,
        },
        code,
        message,
        exit,
    )
}

async fn connect(config: &EffectiveConfig) -> Result<RiffDbClient, ClientError> {
    let endpoint = config
        .endpoint
        .parse()
        .map_err(|_| ClientError::ConnectionFailure)?;
    RiffDbClient::connect(endpoint).await
}

fn request_id() -> Result<Vec<u8>, ClientError> {
    generate_request_id()
        .map(|request_id| request_id.into_bytes().to_vec())
        .map_err(ClientError::IdentifierGeneration)
}

fn required_metadata(
    command: CommandIdentity,
    config: &EffectiveConfig,
    environment: &dyn Environment,
) -> Result<CallMetadata, Terminal> {
    normal_credential(config, environment)
        .map(|credential| credential.metadata)
        .map_err(|error| credential_terminal(command, error))
}

fn credential_terminal(command: CommandIdentity, error: CredentialError) -> Terminal {
    match error {
        CredentialError::Required => local_error(
            command,
            "credential_required",
            "a capability credential is required",
        ),
        CredentialError::SourcesConflict => local_error(
            command,
            "credential_sources_conflict",
            "multiple capability credential sources were supplied",
        ),
        CredentialError::Invalid => local_error(
            command,
            "credential_invalid",
            "capability credential is invalid",
        ),
        CredentialError::BootstrapGeneration => local_error(
            command,
            "bootstrap_generation_failed",
            "bootstrap credential generation failed",
        ),
        CredentialError::BootstrapInvalid => local_error(
            command,
            "bootstrap_credential_invalid",
            "bootstrap credential is invalid",
        ),
        CredentialError::Retention => local_error(
            command,
            "credential_retention_failed",
            "credential retention failed",
        ),
    }
}

fn bootstrap_credential_terminal(error: CredentialError) -> Terminal {
    credential_terminal(CommandIdentity::CapabilityBootstrap, error)
}

fn input_terminal(command: CommandIdentity, error: InputError) -> Terminal {
    match error {
        InputError::PathInvalid => local_error(command, "path_invalid", "an input path is invalid"),
        InputError::ReadFailed => local_error(
            command,
            "input_read_failed",
            "input could not be read; provide inline JSON, @path, path, or - for stdin",
        ),
        InputError::TooLarge => {
            local_error(command, "input_too_large", "input exceeds the CLI limit")
        }
        InputError::Invalid => invalid_input(command),
    }
}

fn batch_error_terminal(error: BatchError) -> Terminal {
    match error {
        BatchError::Input(error) => input_terminal(CommandIdentity::CommandBatch, error),
        BatchError::CheckpointInvalid => local_error(
            CommandIdentity::CommandBatch,
            "batch_checkpoint_invalid",
            "the batch checkpoint does not match its checksum, command, or source; archive the checkpoint only after reviewing whether the prior batch must be resumed",
        ),
        BatchError::CheckpointContractVersionMismatch {
            checkpoint_file,
            checkpoint_contract_version,
            requested_contract_version,
        } => {
            const CODE: &str = "batch_checkpoint_contract_version_mismatch";
            const MESSAGE: &str = "the batch checkpoint belongs to another contract version and cannot be resumed for this invocation";
            local_error_with(
                CommandIdentity::CommandBatch,
                &BatchCheckpointContractVersionError {
                    code: CODE,
                    message: MESSAGE,
                    checkpoint_file: &checkpoint_file,
                    checkpoint_contract_version,
                    requested_contract_version,
                    recovery_action: "resume with the checkpoint's original contract version, or archive it and start a new batch checkpoint after reviewing prior outcomes",
                },
                CODE,
                MESSAGE,
                2,
            )
        }
        BatchError::CheckpointWriteFailed => local_error(
            CommandIdentity::CommandBatch,
            "batch_checkpoint_write_failed",
            "the batch checkpoint could not be durably replaced",
        ),
        BatchError::IdentifierUnavailable => local_error(
            CommandIdentity::CommandBatch,
            "batch_session_unavailable",
            "a batch session identifier could not be generated",
        ),
    }
}

fn render_batch_report(report: BatchReport) -> Terminal {
    if !report.is_complete() {
        return local_error_with(
            CommandIdentity::CommandBatch,
            &report,
            "batch_incomplete",
            "one or more command outcomes remain pending; resume with the same source and checkpoint",
            3,
        );
    }
    if report.has_rejections() {
        return local_error_with(
            CommandIdentity::CommandBatch,
            &report,
            "batch_items_rejected",
            "one or more commands reached a terminal rejected result",
            2,
        );
    }
    success(CommandIdentity::CommandBatch, "completed", &report)
}

fn invalid_input(command: CommandIdentity) -> Terminal {
    local_error(command, "input_invalid", "input is invalid")
}

fn contextualize_application_command_error(error: ClientError, command_name: &str) -> ClientError {
    let ClientError::Public(public) = error else {
        return error;
    };
    let context = ApplicationErrorContext::empty()
        .with_operation_symbol(command_name.to_owned())
        .unwrap_or_else(|_| ApplicationErrorContext::empty());
    ClientError::Application(Box::new(ApplicationError::from_public_error(
        &public,
        ApplicationOperation::ExecuteCommand,
        context,
    )))
}

fn read_text(path: &OsString, stdin: &mut dyn Read) -> Result<String, InputError> {
    utf8(read_path_or_stdin(path, stdin, MAX_INPUT_BYTES)?)
}

fn read_json<T: for<'de> Deserialize<'de>>(
    source: &OsString,
    stdin: &mut dyn Read,
) -> Result<T, InputError> {
    let encoded = source.as_encoded_bytes();
    let bytes = if encoded.first() == Some(&b'{') || encoded.first() == Some(&b'[') {
        if encoded.len() > MAX_INPUT_BYTES {
            return Err(InputError::TooLarge);
        }
        encoded.to_vec()
    } else if encoded.first() == Some(&b'@') {
        let path = source
            .to_str()
            .and_then(|value| value.strip_prefix('@'))
            .ok_or(InputError::PathInvalid)?;
        read_path_or_stdin(OsStr::new(path), stdin, MAX_INPUT_BYTES)?
    } else {
        read_path_or_stdin(source, stdin, MAX_INPUT_BYTES)?
    };
    serde_json::from_slice(&bytes).map_err(|_| InputError::Invalid)
}

fn parse_u64(value: &str) -> Result<u64, ()> {
    if !canonical_unsigned(value) {
        return Err(());
    }
    value.parse().map_err(|_| ())
}

fn parse_nonzero_u64(value: &str) -> Result<u64, ()> {
    parse_u64(value).and_then(|value| (value != 0).then_some(value).ok_or(()))
}

fn parse_expected_active_version(value: Option<&str>) -> Result<Option<u64>, ()> {
    value
        .map(parse_u64)
        .transpose()
        .map(|value| value.filter(|value| *value != 0))
}

fn parse_nonzero_u32(value: &str) -> Result<u32, ()> {
    if !canonical_unsigned(value) {
        return Err(());
    }
    value
        .parse::<u32>()
        .ok()
        .filter(|value| *value != 0)
        .ok_or(())
}

fn page_limit(value: Option<&str>) -> Result<u32, ()> {
    match value {
        Some(value) => {
            parse_nonzero_u32(value).and_then(|value| (value <= 500).then_some(value).ok_or(()))
        }
        None => Ok(DEFAULT_PAGE_LIMIT),
    }
}

fn canonical_unsigned(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && (value == "0" || !value.starts_with('0'))
}

fn canonical_base64(value: &str) -> Result<Vec<u8>, ()> {
    let bytes = STANDARD.decode(value.as_bytes()).map_err(|_| ())?;
    (STANDARD.encode(&bytes) == value)
        .then_some(bytes)
        .ok_or(())
}

fn deserialize_canonical_base64<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<u8>, D::Error> {
    use serde::de::Error as _;
    let value = String::deserialize(deserializer)?;
    canonical_base64(&value).map_err(|()| D::Error::custom("invalid canonical base64"))
}

fn increasing_nonzero_u32(values: &[String]) -> Result<Vec<u32>, ()> {
    let mut previous = 0;
    values
        .iter()
        .map(|value| {
            let current = parse_nonzero_u32(value)?;
            if current <= previous {
                return Err(());
            }
            previous = current;
            Ok(current)
        })
        .collect()
}

fn contract_selection(args: ContractSelectionArgs) -> Result<v1::ContractSelection, ()> {
    let selection = match (args.contract_lineage, args.contract_version) {
        (None, None) => v1::contract_selection::Selection::Active(v1::Unit {}),
        (Some(lineage), Some(version)) if !lineage.is_empty() => {
            v1::contract_selection::Selection::Exact(v1::ExactContractSelection {
                contract_lineage: lineage,
                contract_version: parse_nonzero_u64(&version)?,
            })
        }
        _ => return Err(()),
    };
    Ok(v1::ContractSelection {
        selection: Some(selection),
    })
}

fn symbolic_contract_selection(
    args: ContractSelectionArgs,
) -> Result<Option<app_v1::ContractSelector>, ()> {
    symbolic_contract_selection_parts(args.contract_lineage, args.contract_version)
}

fn symbolic_contract_selection_ref(
    args: &ContractSelectionArgs,
) -> Result<Option<app_v1::ContractSelector>, ()> {
    symbolic_contract_selection_parts(args.contract_lineage.clone(), args.contract_version.clone())
}

fn symbolic_contract_selection_parts(
    lineage: Option<String>,
    version: Option<String>,
) -> Result<Option<app_v1::ContractSelector>, ()> {
    match (lineage, version) {
        (None, None) => Ok(None),
        (Some(lineage), Some(version)) if !lineage.is_empty() => {
            Ok(Some(app_v1::ContractSelector {
                lineage,
                version: parse_nonzero_u64(&version)?,
                bundle_hash: Vec::new(),
            }))
        }
        _ => Err(()),
    }
}

fn natural_query_value(value: serde_json::Value) -> Result<v1::Value, ()> {
    use v1::value::Kind;
    let kind = match value {
        serde_json::Value::Null => Kind::NullValue(v1::NullValue::NullValue as i32),
        serde_json::Value::Bool(value) => Kind::BoolValue(value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_u64() {
                Kind::U64Value(value)
            } else {
                Kind::I64Value(value.as_i64().ok_or(())?)
            }
        }
        serde_json::Value::String(value) => Kind::StringValue(value),
        serde_json::Value::Array(values) => Kind::ListValue(v1::ValueList {
            values: values
                .into_iter()
                .map(natural_query_value)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        serde_json::Value::Object(mut tagged) if tagged.len() == 1 => {
            if let Some(serde_json::Value::String(value)) = tagged.remove("$uuid") {
                Kind::UuidValue(parse_uuid(&value).ok_or(())?.to_vec())
            } else if let Some(serde_json::Value::String(value)) = tagged.remove("$enum") {
                if value.is_empty() || value.len() > 256 {
                    return Err(());
                }
                Kind::EnumValue(v1::EnumValue {
                    type_id: 0,
                    variant_id: 0,
                    name: value,
                })
            } else if let Some(serde_json::Value::String(value)) = tagged.remove("$i64") {
                Kind::I64Value(value.parse().map_err(|_| ())?)
            } else if let Some(serde_json::Value::String(value)) = tagged.remove("$u64") {
                Kind::U64Value(value.parse().map_err(|_| ())?)
            } else if let Some(value) = tagged.remove("$decimal") {
                Kind::DecimalValue(natural_decimal(value)?)
            } else if let Some(value) = tagged.remove("$money") {
                let mut value = value.as_object().cloned().ok_or(())?;
                if value.len() != 2 {
                    return Err(());
                }
                let currency = value
                    .remove("currency")
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .filter(|value| value.len() == 3 && value.bytes().all(|byte| byte.is_ascii()))
                    .ok_or(())?;
                let amount = natural_decimal(value.remove("amount").ok_or(())?)?;
                Kind::MoneyValue(v1::Money {
                    currency,
                    amount: Some(amount),
                })
            } else if let Some(serde_json::Value::String(value)) = tagged.remove("$bytes") {
                let value = STANDARD.decode(value).map_err(|_| ())?;
                if value.len() > MAX_INPUT_BYTES {
                    return Err(());
                }
                Kind::BytesValue(value)
            } else if let Some(value) = tagged.remove("$date") {
                let days_since_unix_epoch =
                    i32::try_from(value.as_i64().ok_or(())?).map_err(|_| ())?;
                Kind::DateValue(v1::Date {
                    days_since_unix_epoch,
                })
            } else if let Some(value) = tagged.remove("$timestamp") {
                let mut value = value.as_object().cloned().ok_or(())?;
                if value.len() != 2 {
                    return Err(());
                }
                let seconds = value
                    .remove("seconds")
                    .and_then(|value| value.as_str().and_then(|value| value.parse().ok()))
                    .ok_or(())?;
                let nanos = value
                    .remove("nanos")
                    .and_then(|value| value.as_u64())
                    .and_then(|value| u32::try_from(value).ok())
                    .filter(|value| *value <= 999_999_999)
                    .ok_or(())?;
                Kind::TimestampValue(v1::Timestamp { seconds, nanos })
            } else {
                return Err(());
            }
        }
        serde_json::Value::Object(_) => return Err(()),
    };
    Ok(v1::Value { kind: Some(kind) })
}

fn natural_decimal(value: serde_json::Value) -> Result<v1::Decimal, ()> {
    let mut value = value.as_object().cloned().ok_or(())?;
    if !(value.len() == 2 || value.len() == 3) {
        return Err(());
    }
    let coefficient_twos_complement = value
        .remove("coefficient_twos_complement")
        .and_then(|value| value.as_str().map(str::to_owned))
        .and_then(|value| STANDARD.decode(value).ok())
        .filter(|value| !value.is_empty() && value.len() <= 16)
        .ok_or(())?;
    let scale = value
        .remove("scale")
        .and_then(|value| value.as_u64())
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(())?;
    let precision = value
        .remove("precision")
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or(())
        })
        .transpose()?;
    if !value.is_empty() {
        return Err(());
    }
    Ok(v1::Decimal {
        coefficient_twos_complement,
        scale,
        precision,
    })
}

pub(crate) fn natural_command_record(
    input: serde_json::Map<String, serde_json::Value>,
) -> Result<v1::Value, ()> {
    let mut fields = input
        .into_iter()
        .map(|(name, value)| {
            if name.is_empty() {
                return Err(());
            }
            Ok(v1::ValueField {
                field_id: None,
                name,
                value: Some(natural_query_value(value)?),
            })
        })
        .collect::<Result<Vec<_>, ()>>()?;
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(v1::Value {
        kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord { fields })),
    })
}

fn render_query_check(identity: CommandIdentity, response: app_v1::CheckQueryResponse) -> Terminal {
    if response.diagnostics.is_empty() {
        match response
            .identity
            .as_ref()
            .zip(response.schema.as_ref())
            .map(|(query, schema)| checked_query_json(query, schema))
        {
            Some(result) => success(identity, "valid", &result),
            None => invalid_input(identity),
        }
    } else {
        success(
            identity,
            "invalid",
            &serde_json::json!({
                "status": "invalid",
                "diagnostics": diagnostics_json(&response.diagnostics),
            }),
        )
    }
}

fn render_query_explain(
    identity: CommandIdentity,
    response: app_v1::ExplainQueryResponse,
) -> Terminal {
    if response.diagnostics.is_empty() {
        match response
            .identity
            .as_ref()
            .zip(response.schema.as_ref())
            .map(|(query, schema)| {
                let mut result = checked_query_json(query, schema);
                if let Some(object) = result.as_object_mut() {
                    object.insert("plan".to_owned(), serde_json::json!(response.plan_lines));
                }
                result
            }) {
            Some(result) => success(identity, "valid", &result),
            None => invalid_input(identity),
        }
    } else {
        success(
            identity,
            "invalid",
            &serde_json::json!({
                "status": "invalid",
                "diagnostics": diagnostics_json(&response.diagnostics),
            }),
        )
    }
}

fn checked_query_json(
    identity: &app_v1::QueryIdentity,
    schema: &app_v1::QuerySchema,
) -> serde_json::Value {
    serde_json::json!({
        "status": "valid",
        "identity": {
            "contract_lineage": identity.contract_lineage,
            "contract_version": identity.contract_version.to_string(),
            "contract_bundle_hash": hex(&identity.contract_bundle_hash),
            "module_hash": identity.module_hash.as_deref().map(hex),
            "query_name": identity.query_name,
            "plan_hash": hex(&identity.plan_hash),
        },
        "schema": {
            "parameters": schema.parameters,
            "outcomes": schema.outcomes,
            "result_fields": schema.result_fields,
        }
    })
}

fn diagnostics_json(diagnostics: &[app_v1::Diagnostic]) -> Vec<serde_json::Value> {
    diagnostics
        .iter()
        .map(|diagnostic| {
            serde_json::json!({
                "code": diagnostic.code,
                "summary": diagnostic.summary,
                "span": diagnostic.span.as_ref().map(|span| {
                    serde_json::json!({"start": span.start, "end": span.end})
                }),
                "symbols": diagnostic.symbols,
                "suggestion": diagnostic.suggestion,
            })
        })
        .collect()
}

fn query_execution_json(response: &app_v1::ExecuteQueryResponse) -> Option<serde_json::Value> {
    let identity = response.identity.as_ref()?;
    let fields = response
        .fields
        .iter()
        .map(|field| {
            let cardinality = match app_v1::ResultCardinality::try_from(field.cardinality).ok()? {
                app_v1::ResultCardinality::One => "one",
                app_v1::ResultCardinality::Maybe => "maybe",
                app_v1::ResultCardinality::Many => "many",
                app_v1::ResultCardinality::Unspecified => return None,
            };
            let records = field
                .records
                .iter()
                .map(|record| {
                    let fields = record
                        .fields
                        .iter()
                        .map(|field| {
                            Some(serde_json::json!({
                                "name": field.name,
                                "value": serde_json::to_value(crate::value::OutputValue(
                                    field.value.as_ref()?
                                )).ok()?,
                            }))
                        })
                        .collect::<Option<Vec<_>>>()?;
                    Some(serde_json::json!({
                        "entity": record.entity,
                        "fields": fields,
                    }))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(serde_json::json!({
                "name": field.name,
                "cardinality": cardinality,
                "records": records,
            }))
        })
        .collect::<Option<Vec<_>>>()?;
    Some(serde_json::json!({
        "identity": {
            "contract_lineage": identity.contract_lineage,
            "contract_version": identity.contract_version.to_string(),
            "contract_bundle_hash": hex(&identity.contract_bundle_hash),
            "module_hash": identity.module_hash.as_deref().map(hex),
            "query_name": identity.query_name,
            "plan_hash": hex(&identity.plan_hash),
        },
        "outcome": response.outcome,
        "application_head": response.application_head.to_string(),
        "fields": fields,
        "next_cursor": response.next_cursor,
    }))
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn parse_uuid_v7(value: &str) -> Option<[u8; 16]> {
    let bytes = parse_uuid(value)?;
    ((bytes[6] >> 4 == 7) && (bytes[8] & 0xc0 == 0x80)).then_some(bytes)
}

const fn restore_confirmation(confirmed: bool) -> OfflineMaintenanceReplacementConfirmation {
    if confirmed {
        OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget
    } else {
        OfflineMaintenanceReplacementConfirmation::NotProvided
    }
}

const fn command_identity(command: &TopLevel) -> CommandIdentity {
    match command {
        TopLevel::Application {
            command: ApplicationCommand::Lock { .. },
        } => CommandIdentity::ApplicationLock,
        TopLevel::Application {
            command: ApplicationCommand::Deploy { .. },
        } => CommandIdentity::ApplicationDeploy,
        TopLevel::Application {
            command: ApplicationCommand::BindDevRole { .. },
        } => CommandIdentity::ApplicationBindDevRole,
        TopLevel::Application { .. } => CommandIdentity::ServerHealth,
        TopLevel::Migration { .. } => CommandIdentity::MigrationPlan,
        TopLevel::New { .. } => CommandIdentity::ServerHealth,
        TopLevel::Dev { .. } => CommandIdentity::ServerHealth,
        TopLevel::Contract {
            command: ContractCommand::Validate { .. },
        } => CommandIdentity::ContractValidate,
        TopLevel::Contract {
            command: ContractCommand::Deploy { .. },
        } => CommandIdentity::ContractDeploy,
        TopLevel::Command {
            command: CommandCommand::Batch { .. },
        } => CommandIdentity::CommandBatch,
        TopLevel::Command {
            command: CommandCommand::Run { .. },
        } => CommandIdentity::CommandRun,
        TopLevel::Command {
            command: CommandCommand::Execute { .. },
        } => CommandIdentity::CommandExecute,
        TopLevel::Command {
            command: CommandCommand::Outcome { .. },
        } => CommandIdentity::CommandOutcome,
        TopLevel::Entity { .. } => CommandIdentity::EntityGet,
        TopLevel::Commit { .. } => CommandIdentity::CommitShow,
        TopLevel::Projection { .. } => CommandIdentity::ProjectionQuery,
        TopLevel::Query {
            command: QueryCommand::Describe { .. },
        } => CommandIdentity::QueryDescribe,
        TopLevel::Query {
            command: QueryCommand::Check { .. },
        } => CommandIdentity::QueryCheck,
        TopLevel::Query {
            command: QueryCommand::Explain { .. },
        } => CommandIdentity::QueryExplain,
        TopLevel::Query {
            command: QueryCommand::Run { .. },
        } => CommandIdentity::QueryRun,
        TopLevel::Query {
            command: QueryCommand::RunNamed { .. },
        } => CommandIdentity::QueryRunNamed,
        TopLevel::Query {
            command: QueryCommand::Deploy { .. },
        } => CommandIdentity::QueryDeploy,
        TopLevel::Query {
            command: QueryCommand::Module { .. },
        } => CommandIdentity::QueryModule,
        TopLevel::Query {
            command: QueryCommand::Repl { .. },
        } => CommandIdentity::QueryRepl,
        TopLevel::Role {
            command: RoleCommand::Check { .. },
        } => CommandIdentity::RoleCheck,
        TopLevel::Role {
            command: RoleCommand::Describe { .. },
        } => CommandIdentity::RoleDescribe,
        TopLevel::Role {
            command: RoleCommand::Bind { .. },
        } => CommandIdentity::RoleBind,
        TopLevel::Role {
            command: RoleCommand::Revoke { .. },
        } => CommandIdentity::RoleRevoke,
        TopLevel::Capability {
            command: CapabilityCommand::Bootstrap { .. },
        } => CommandIdentity::CapabilityBootstrap,
        TopLevel::Capability {
            command: CapabilityCommand::Create { .. },
        } => CommandIdentity::CapabilityCreate,
        TopLevel::Capability {
            command: CapabilityCommand::Revoke { .. },
        } => CommandIdentity::CapabilityRevoke,
        TopLevel::Server { .. } => CommandIdentity::ServerHealth,
        TopLevel::Backup {
            command: BackupCommand::Create { .. },
        } => CommandIdentity::BackupCreate,
        TopLevel::Backup {
            command: BackupCommand::Restore { .. },
        } => CommandIdentity::BackupRestore,
        TopLevel::Backup {
            command: BackupCommand::Operation { .. },
        } => CommandIdentity::BackupOperation,
        TopLevel::Demo { .. } => CommandIdentity::DemoBudget,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::io::Cursor;
    use std::net::TcpListener;
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn seed_checkpoints_are_contract_version_scoped() {
        let root = Path::new(".riffdb/deployments/ea");

        assert_eq!(
            seed_checkpoint_path(root, 0, 4),
            root.join("seed-000-contract-v4.checkpoint.json")
        );
        assert_ne!(
            seed_checkpoint_path(root, 0, 2),
            seed_checkpoint_path(root, 0, 4)
        );
    }

    #[derive(Default)]
    struct TestEnvironment(BTreeMap<String, OsString>);

    impl Environment for TestEnvironment {
        fn value(&self, name: &str) -> Option<OsString> {
            self.0.get(name).cloned()
        }
    }

    fn test_config() -> EffectiveConfig {
        EffectiveConfig {
            endpoint: "http://127.0.0.1:7443".to_owned(),
            database: riffdb_types::DatabaseAlias::default_alias(),
            output: crate::cli::OutputMode::Json,
            max_attempts: 3,
            credential_file: None,
        }
    }

    #[test]
    fn named_query_json_preserves_the_exact_returned_module_identity() {
        let response = app_v1::ExecuteQueryResponse {
            identity: Some(app_v1::QueryIdentity {
                contract_lineage: "SafeApplication".to_owned(),
                contract_version: 1,
                contract_bundle_hash: vec![0xbb; 32],
                query_name: Some("ItemPage".to_owned()),
                plan_hash: vec![0xdd; 32],
                module_hash: Some(vec![0xcc; 32]),
            }),
            outcome: "Found".to_owned(),
            application_head: 1,
            fields: Vec::new(),
            next_cursor: None,
        };
        let result = query_execution_json(&response).expect("query JSON");
        assert_eq!(
            result["identity"]["module_hash"],
            serde_json::Value::String("cc".repeat(32))
        );
    }

    #[test]
    fn application_json_accepts_inline_at_file_legacy_path_and_stdin_sources() {
        let directory =
            std::env::temp_dir().join(format!("riffdb-cli-natural-json-{}", std::process::id()));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).expect("fixture directory");
        let path = directory.join("input.json");
        fs::write(&path, br#"{"priority":2}"#).expect("fixture input");

        let inline: serde_json::Value = read_json(
            &OsString::from(r#"{"priority":2}"#),
            &mut Cursor::new(Vec::<u8>::new()),
        )
        .expect("inline JSON");
        let at_file: serde_json::Value = read_json(
            &OsString::from(format!("@{}", path.display())),
            &mut Cursor::new(Vec::<u8>::new()),
        )
        .expect("@file JSON");
        let legacy_path: serde_json::Value = read_json(
            &path.as_os_str().to_owned(),
            &mut Cursor::new(Vec::<u8>::new()),
        )
        .expect("legacy path JSON");
        let stdin: serde_json::Value = read_json(
            &OsString::from("-"),
            &mut Cursor::new(br#"{"priority":2}"#.to_vec()),
        )
        .expect("stdin JSON");

        assert_eq!(inline, serde_json::json!({"priority": 2}));
        assert_eq!(at_file, inline);
        assert_eq!(legacy_path, inline);
        assert_eq!(stdin, inline);
        fs::remove_dir_all(directory).expect("fixture cleanup");
    }

    #[test]
    fn deployment_state_is_private_exact_and_database_bound() {
        let directory = std::env::temp_dir().join(format!(
            "riffdb-cli-deployment-state-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).expect("fixture directory");
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
            .expect("private fixture directory");
        let path = directory.join("deployment-state.json");
        let lock_hash = "ab".repeat(32);
        let mut state =
            load_deployment_state(&path, "ea", &lock_hash).expect("new exact deployment state");
        state.contract_deployed = true;
        state.contract_bundle_hash = "cd".repeat(32);
        state.query_modules_deployed.push("EaQueries".to_owned());
        state
            .query_module_identities
            .push(ApplicationDeploymentQueryModuleState {
                module_name: "EaQueries".to_owned(),
                module_version: 1,
                module_hash: "de".repeat(32),
            });
        state.role = Some(ApplicationDeploymentRoleState {
            role_name: "EaApplication".to_owned(),
            role_identity: "ef".repeat(32),
            capability_id: "01900000-0000-7000-8000-000000000001".to_owned(),
            bound: true,
            authentication_audience: Some("riffdb-grpc-loopback".to_owned()),
        });
        persist_deployment_state(&path, &state).expect("durable deployment state");

        assert_eq!(
            fs::metadata(&path)
                .expect("state metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            load_deployment_state(&path, "ea", &lock_hash)
                .expect("matching state")
                .role
                .expect("retained role")
                .role_name,
            "EaApplication"
        );
        assert!(load_deployment_state(&path, "default", &lock_hash).is_err());
        assert_eq!(
            load_deployment_state(&path, "ea", &"cd".repeat(32))
                .expect("a successor lock is reconciled by deployment")
                .lock_hash,
            lock_hash
        );

        let legacy = serde_json::json!({
            "schema": APPLICATION_DEPLOYMENT_STATE_SCHEMA,
            "database": "ea",
            "lock_hash": lock_hash,
            "contract_deployed": true,
            "query_modules_deployed": ["EaQueries"],
            "role": null,
            "seeds_completed": []
        });
        fs::write(
            &path,
            serde_json::to_vec(&legacy).expect("legacy state JSON"),
        )
        .expect("legacy state fixture");
        let legacy = load_deployment_state(&path, "ea", &"ab".repeat(32))
            .expect("legacy state remains resumable");
        assert!(legacy.contract_bundle_hash.is_empty());
        assert!(legacy.query_module_identities.is_empty());
        fs::remove_dir_all(directory).expect("fixture cleanup");
    }

    #[test]
    fn generated_application_configs_retain_an_absolute_credential_path() {
        let directory = std::env::temp_dir().join(format!(
            "riffdb-cli-application-config-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).expect("fixture directory");
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
            .expect("private fixture directory");

        let relative = Path::new(".riffdb/deployments/ea/application.credential");
        persist_application_configs(&directory, &test_config(), relative, "riffdb-grpc-loopback")
            .expect("application configs");
        let expected = std::env::current_dir()
            .expect("current directory")
            .join(relative);
        let client = fs::read_to_string(directory.join("client.toml")).expect("client config");
        assert!(client.contains(&format!(
            "credential_file = {}\n",
            serde_json::to_string(expected.to_str().expect("UTF-8 path")).expect("quoted path")
        )));
        let mcp = fs::read_to_string(directory.join("mcp.toml")).expect("MCP config");
        assert!(mcp.contains("expected_audience = \"riffdb-grpc-loopback\"\n"));

        fs::remove_dir_all(directory).expect("fixture cleanup");
    }

    #[test]
    fn destructive_restore_confirmation_has_one_exact_cli_source() {
        assert_eq!(
            restore_confirmation(false),
            OfflineMaintenanceReplacementConfirmation::NotProvided
        );
        assert_eq!(
            restore_confirmation(true),
            OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget
        );
    }

    #[test]
    fn dev_script_resolution_prefers_installation_then_workspace_then_source() {
        let directory =
            std::env::temp_dir().join(format!("riffdb-cli-dev-resolution-{}", std::process::id()));
        let _ = fs::remove_dir_all(&directory);
        let application = directory.join("application");
        let executable = directory.join("installation/bin/riffdb");
        let installed = directory.join("installation/bin/riffdb-dev");
        let manifest = directory.join("source/crates/riffdb-cli");
        let source = directory.join("source/scripts/riffdb-dev");
        fs::create_dir_all(application.join("scripts")).expect("application scripts");
        fs::create_dir_all(executable.parent().expect("executable parent"))
            .expect("installation bin");
        fs::create_dir_all(&manifest).expect("source manifest");
        fs::create_dir_all(source.parent().expect("source parent")).expect("source scripts");
        fs::write(&source, b"source").expect("source workflow");

        assert_eq!(
            resolve_dev_script(&application, Some(&executable), &manifest),
            Some(manifest.join("../../scripts/riffdb-dev"))
        );
        fs::write(&installed, b"installed").expect("installed workflow");
        assert_eq!(
            resolve_dev_script(&application, Some(&executable), &manifest),
            Some(installed.clone())
        );
        let workspace = application.join("scripts/riffdb-dev");
        fs::write(&workspace, b"workspace").expect("workspace workflow");
        assert_eq!(
            resolve_dev_script(&application, Some(&executable), &manifest),
            Some(installed)
        );
        fs::remove_file(directory.join("installation/bin/riffdb-dev"))
            .expect("remove installed workflow");
        assert_eq!(
            resolve_dev_script(&application, Some(&executable), &manifest),
            Some(workspace)
        );
        fs::remove_dir_all(directory).expect("cleanup");
    }

    fn assert_terminal_omits(terminal: Terminal, needle: &[u8], mode: crate::cli::OutputMode) {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let _ = terminal.emit(mode, &mut stdout, &mut stderr);
        assert!(!contains_bytes(&stdout, needle));
        assert!(!contains_bytes(&stderr, needle));
    }

    fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|candidate| candidate == needle)
    }

    fn assert_local_code(terminal: Terminal, code: &str) {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let _ = terminal.emit(crate::cli::OutputMode::Json, &mut stdout, &mut stderr);
        assert!(stderr.is_empty());
        let expected = format!("\"type\":\"local\",\"code\":\"{code}\"");
        assert!(
            contains_bytes(&stdout, expected.as_bytes()),
            "missing local code {code}: {}",
            String::from_utf8_lossy(&stdout)
        );
    }

    fn assert_listener_unused(listener: &TcpListener) {
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        match listener.accept() {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Ok(_) => panic!("local rejection opened a transport connection"),
            Err(error) => panic!("listener observation failed: {error}"),
        }
    }

    #[test]
    fn symbolic_application_values_cover_every_generated_scalar_without_numeric_ids() {
        let input = serde_json::json!({
            "signed": {"$i64": "-9223372036854775808"},
            "unsigned": {"$u64": "18446744073709551615"},
            "decimal": {"$decimal": {
                "coefficient_twos_complement": "ew==",
                "scale": 2,
                "precision": 3
            }},
            "money": {"$money": {
                "currency": "USD",
                "amount": {
                    "coefficient_twos_complement": "ew==",
                    "scale": 2,
                    "precision": 3
                }
            }},
            "bytes": {"$bytes": "c2FmZQ=="},
            "date": {"$date": 1},
            "timestamp": {"$timestamp": {"seconds": "-1", "nanos": 999999999}}
        });
        let record = natural_command_record(input.as_object().expect("record").clone())
            .expect("all generated scalar tags");
        let Some(v1::value::Kind::RecordValue(record)) = record.kind else {
            panic!("record value");
        };
        let fields = record
            .fields
            .into_iter()
            .map(|field| {
                (
                    field.name,
                    field
                        .value
                        .and_then(|value| value.kind)
                        .expect("value kind"),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert!(matches!(
            fields["signed"],
            v1::value::Kind::I64Value(i64::MIN)
        ));
        assert!(matches!(
            fields["unsigned"],
            v1::value::Kind::U64Value(u64::MAX)
        ));
        assert!(matches!(
            fields["decimal"],
            v1::value::Kind::DecimalValue(_)
        ));
        assert!(matches!(fields["money"], v1::value::Kind::MoneyValue(_)));
        assert!(matches!(fields["bytes"], v1::value::Kind::BytesValue(_)));
        assert!(matches!(fields["date"], v1::value::Kind::DateValue(_)));
        assert!(matches!(
            fields["timestamp"],
            v1::value::Kind::TimestampValue(_)
        ));

        for invalid in [
            serde_json::json!({"$i64": "9223372036854775808"}),
            serde_json::json!({"$u64": "-1"}),
            serde_json::json!({"$decimal": {
                "coefficient_twos_complement": "",
                "scale": 2
            }}),
            serde_json::json!({"$money": {
                "currency": "US",
                "amount": {"coefficient_twos_complement": "AQ==", "scale": 0}
            }}),
            serde_json::json!({"$timestamp": {"seconds": "0", "nanos": 1000000000}}),
        ] {
            assert!(natural_query_value(invalid).is_err());
        }
    }

    #[derive(Default)]
    struct FakeRetryOperations {
        execute_calls: u32,
        bootstrap_calls: u32,
        normal_create_calls: u32,
        command_name: Option<String>,
        expected_contract_version: Option<Option<u64>>,
        input: Option<v1::Value>,
        attempt_budgets: Vec<u32>,
    }

    impl RetryOperations for FakeRetryOperations {
        async fn execute_retry(
            &mut self,
            command: &IdempotentCommand,
            attempts: AttemptBudget,
            _metadata: &CallMetadata,
        ) -> Result<v1::ExecuteCommandResponse, ClientError> {
            self.execute_calls += 1;
            self.command_name = Some(command.command_name().to_owned());
            self.expected_contract_version = Some(command.expected_contract_version());
            self.input = Some(command.input().clone());
            self.attempt_budgets.push(attempts.maximum_submissions());
            Err(ClientError::ConnectionFailure)
        }

        async fn bootstrap_retry(
            &mut self,
            _template: &BootstrapCapabilityCreateTemplate,
            attempts: AttemptBudget,
            _metadata: &riffdb_client_rust::BootstrapCallMetadata,
        ) -> Result<v1::CreateCapabilityResponse, ClientError> {
            self.bootstrap_calls += 1;
            self.attempt_budgets.push(attempts.maximum_submissions());
            Err(ClientError::ConnectionFailure)
        }

        async fn normal_create_retry(
            &mut self,
            _template: &NormalCapabilityCreateTemplate,
            attempts: AttemptBudget,
            _metadata: &CallMetadata,
        ) -> Result<v1::CreateCapabilityResponse, ClientError> {
            self.normal_create_calls += 1;
            self.attempt_budgets.push(attempts.maximum_submissions());
            Err(ClientError::ConnectionFailure)
        }
    }

    fn retry_capability_request(mode: v1::CapabilityCreateMode) -> v1::CreateCapabilityRequest {
        let permissions = if mode == v1::CapabilityCreateMode::Bootstrap {
            vec![v1::CapabilityPermission {
                permission: Some(
                    v1::capability_permission::Permission::AdministerCapabilities(v1::Unit {}),
                ),
            }]
        } else {
            Vec::new()
        };
        v1::CreateCapabilityRequest {
            request_id: Vec::new(),
            mode: mode as i32,
            capability_id: parse_uuid_v7("00000000-0001-7000-8000-000000000000")
                .expect("UUIDv7")
                .to_vec(),
            principal_id: "operator".to_owned(),
            actor_kind: v1::ActorKind::Human as i32,
            requested_lifetime_seconds: 60,
            audiences: vec!["riffdb-cli".to_owned()],
            grant: Some(v1::CapabilityGrant {
                tenant_scope: Some(v1::TenantScope {
                    scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
                }),
                partition_scope: Some(v1::PartitionScope {
                    scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
                }),
                permissions,
                field_visibility: Vec::new(),
                max_scan_rows: 1,
                approval_required: Vec::new(),
            }),
        }
    }

    #[tokio::test]
    async fn retry_boundaries_receive_one_immutable_template_and_exact_budget() {
        let input = v1::Value {
            kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord {
                fields: vec![v1::ValueField {
                    field_id: None,
                    name: "idempotency_key".to_owned(),
                    value: Some(v1::Value {
                        kind: Some(v1::value::Kind::StringValue(
                            "budget-comparison:0001".to_owned(),
                        )),
                    }),
                }],
            })),
        };
        let command =
            IdempotentCommand::new("budget.allocate", Some(7), input.clone()).expect("command");
        let attempts = AttemptBudget::new(3).expect("nonzero");
        let mut operations = FakeRetryOperations::default();

        let execute = submit_execute_retry(
            &mut operations,
            &command,
            attempts,
            &CallMetadata::default(),
        )
        .await;
        assert!(matches!(execute, Err(ClientError::ConnectionFailure)));
        assert_eq!(operations.execute_calls, 1);
        assert_eq!(operations.command_name.as_deref(), Some("budget.allocate"));
        assert_eq!(operations.expected_contract_version, Some(Some(7)));
        assert_eq!(operations.input.as_ref(), Some(&input));

        let bootstrap_template = BootstrapCapabilityCreateTemplate::new(retry_capability_request(
            v1::CapabilityCreateMode::Bootstrap,
        ))
        .expect("bootstrap template");
        let bootstrap_credential = riffdb_client_rust::BootstrapCredential::new(&"A".repeat(43))
            .expect("bootstrap credential");
        let bootstrap = submit_bootstrap_retry(
            &mut operations,
            &bootstrap_template,
            attempts,
            &riffdb_client_rust::BootstrapCallMetadata::new(bootstrap_credential),
        )
        .await;
        assert!(matches!(bootstrap, Err(ClientError::ConnectionFailure)));
        assert_eq!(operations.bootstrap_calls, 1);

        let normal_template = NormalCapabilityCreateTemplate::new(retry_capability_request(
            v1::CapabilityCreateMode::Normal,
        ))
        .expect("normal template");
        let normal = submit_normal_create_retry(
            &mut operations,
            &normal_template,
            attempts,
            &CallMetadata::default(),
        )
        .await;
        assert!(matches!(normal, Err(ClientError::ConnectionFailure)));
        assert_eq!(operations.normal_create_calls, 1);
        assert_eq!(operations.attempt_budgets, vec![3, 3, 3]);
    }

    #[tokio::test]
    async fn local_input_and_credential_rejections_make_no_transport_attempt() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("loopback listener");
        let mut config = test_config();
        config.endpoint = format!(
            "http://{}",
            listener.local_addr().expect("listener address")
        );
        let environment = TestEnvironment::default();

        let oversized_contract = dispatch(
            TopLevel::Contract {
                command: ContractCommand::Validate {
                    source: OsString::from("-"),
                },
            },
            &config,
            &environment,
            &mut Cursor::new(vec![b'x'; MAX_INPUT_BYTES + 1]),
        )
        .await;
        assert_local_code(oversized_contract, "input_too_large");
        assert_listener_unused(&listener);

        let oversized_json = dispatch(
            TopLevel::Command {
                command: CommandCommand::Execute {
                    command_name: "budget.allocate".to_owned(),
                    input: OsString::from("-"),
                    expected_version: None,
                },
            },
            &config,
            &environment,
            &mut Cursor::new(vec![b'x'; MAX_INPUT_BYTES + 1]),
        )
        .await;
        assert_local_code(oversized_json, "input_too_large");
        assert_listener_unused(&listener);

        let invalid_path = dispatch(
            TopLevel::Contract {
                command: ContractCommand::Validate {
                    source: OsString::from("x".repeat(crate::input::MAX_PATH_BYTES + 1)),
                },
            },
            &config,
            &environment,
            &mut Cursor::new(Vec::new()),
        )
        .await;
        assert_local_code(invalid_path, "path_invalid");
        assert_listener_unused(&listener);

        let mut invalid_credential_environment = TestEnvironment::default();
        invalid_credential_environment
            .0
            .insert("RIFFDB_CAPABILITY_TOKEN".to_owned(), "short".into());
        let invalid_credential = dispatch(
            TopLevel::Server {
                command: ServerCommand::Health,
            },
            &config,
            &invalid_credential_environment,
            &mut Cursor::new(Vec::new()),
        )
        .await;
        assert_local_code(invalid_credential, "credential_invalid");
        assert_listener_unused(&listener);

        let directory =
            std::env::temp_dir().join(format!("riffdb-cli-preflight-role-{}", std::process::id()));
        let _ = fs::remove_dir_all(&directory);
        crate::scaffold::create_application(
            "preflight-app",
            crate::scaffold::ScaffoldLanguage::Rust,
            &directory,
        )
        .expect("scaffold preflight application");
        let invalid_role = dispatch(
            TopLevel::Application {
                command: ApplicationCommand::Deploy {
                    source: directory.join("riffdb.application.json").into_os_string(),
                    lock: OsString::from("riffdb.application.lock.json"),
                    provision_role: Some("MissingRole".to_owned()),
                    tenant: None,
                    lifetime_seconds: "28800".to_owned(),
                    seed: false,
                    seed_concurrency: "8".to_owned(),
                    replace_expired_credential: false,
                },
            },
            &config,
            &environment,
            &mut Cursor::new(Vec::new()),
        )
        .await;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let _ = invalid_role.emit(crate::cli::OutputMode::Json, &mut stdout, &mut stderr);
        assert!(stderr.is_empty());
        assert!(
            contains_bytes(&stdout, b"\"code\":\"RDB-AR001\""),
            "unexpected preflight output: {}",
            String::from_utf8_lossy(&stdout)
        );
        assert!(contains_bytes(
            &stdout,
            b"\"file_change\":\"no_files_changed\""
        ));
        assert_listener_unused(&listener);
        fs::remove_dir_all(directory).expect("preflight fixture cleanup");
    }

    #[test]
    fn credential_and_runner_canaries_never_reach_public_or_debug_output() {
        const NORMAL_CANARY: &str = "riffdb_cli_secret_canary!AAAAAAAAAAAAAAAAAA";
        const BOOTSTRAP_CANARY: &[u8] = b"riffdb_bootstrap_secret_canary!";
        const RUNNER_CANARY: &[u8] = b"riffdb_runner_secret_canary!";

        assert_eq!(NORMAL_CANARY.len(), 43);
        let mut environment = TestEnvironment::default();
        environment.0.insert(
            "RIFFDB_CAPABILITY_TOKEN".to_owned(),
            OsString::from(NORMAL_CANARY),
        );
        let normal_error = match normal_credential(&test_config(), &environment) {
            Ok(_) => panic!("invalid token accepted"),
            Err(error) => error,
        };
        assert!(!format!("{normal_error:?}").contains(NORMAL_CANARY));
        for mode in [crate::cli::OutputMode::Json, crate::cli::OutputMode::Human] {
            assert_terminal_omits(
                credential_terminal(CommandIdentity::ServerHealth, normal_error),
                NORMAL_CANARY.as_bytes(),
                mode,
            );
        }

        let mut bootstrap_document =
            vec![b'x'; riffdb_auth::bootstrap_secret::BOOTSTRAP_CREDENTIAL_DOCUMENT_BYTES];
        bootstrap_document[..BOOTSTRAP_CANARY.len()].copy_from_slice(BOOTSTRAP_CANARY);
        let bootstrap_error = match bootstrap_material(
            None,
            None,
            true,
            None,
            &mut Cursor::new(bootstrap_document),
        ) {
            Ok(_) => panic!("invalid bootstrap document accepted"),
            Err(error) => error,
        };
        assert!(
            !format!("{bootstrap_error:?}")
                .as_bytes()
                .windows(BOOTSTRAP_CANARY.len())
                .any(|candidate| candidate == BOOTSTRAP_CANARY)
        );
        for mode in [crate::cli::OutputMode::Json, crate::cli::OutputMode::Human] {
            assert_terminal_omits(
                bootstrap_credential_terminal(bootstrap_error),
                BOOTSTRAP_CANARY,
                mode,
            );
        }

        let directory =
            std::env::temp_dir().join(format!("riffdb-cli-app-canary-{}", std::process::id()));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir(&directory).expect("temporary directory");
        let runner = directory.join("runner");
        fs::write(
            &runner,
            format!(
                "#!/bin/sh\nprintf '%s' '{}'\n",
                std::str::from_utf8(RUNNER_CANARY).expect("ASCII")
            ),
        )
        .expect("runner script");
        let mut permissions = fs::metadata(&runner).expect("metadata").permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&runner, permissions).expect("mode");
        let runner_error = run_budget(
            runner.as_os_str(),
            crate::cli::BudgetCase::Sequential,
            "http://127.0.0.1:7443",
            PathBuf::from("/not-read").as_path(),
        )
        .expect_err("unexpected runner output accepted");
        assert_eq!(runner_error, RunnerError::ProtocolInvalid);
        assert!(
            !format!("{runner_error:?}")
                .as_bytes()
                .windows(RUNNER_CANARY.len())
                .any(|candidate| candidate == RUNNER_CANARY)
        );
        for mode in [crate::cli::OutputMode::Json, crate::cli::OutputMode::Human] {
            assert_terminal_omits(
                demo_error(
                    "runner_protocol_invalid",
                    "the budget runner returned an invalid protocol result",
                    None,
                    None,
                    2,
                ),
                RUNNER_CANARY,
                mode,
            );
        }
        fs::remove_dir_all(directory).expect("cleanup");
    }

    #[test]
    fn canonical_numeric_and_uuid_inputs_are_closed() {
        assert_eq!(
            CommandIdentity::ApplicationLock.as_str(),
            "application.lock"
        );
        assert_eq!(parse_u64("0"), Ok(0));
        assert_eq!(parse_nonzero_u64("1"), Ok(1));
        assert_eq!(parse_expected_active_version(None), Ok(None));
        assert_eq!(parse_expected_active_version(Some("0")), Ok(None));
        assert_eq!(parse_expected_active_version(Some("1")), Ok(Some(1)));
        assert_eq!(page_limit(None), Ok(50));
        assert_eq!(page_limit(Some("1")), Ok(1));
        assert_eq!(page_limit(Some("500")), Ok(500));
        assert_eq!(page_limit(Some("0")), Err(()));
        assert_eq!(page_limit(Some("501")), Err(()));
        for value in ["", "00", "01", "+1", "-1", " 1"] {
            assert_eq!(parse_u64(value), Err(()), "{value}");
        }
        assert!(parse_uuid_v7("01234567-89ab-7def-8123-456789abcdef").is_some());
        assert!(parse_uuid_v7("01234567-89ab-6def-8123-456789abcdef").is_none());
        assert!(parse_uuid_v7("01234567-89AB-7def-8123-456789abcdef").is_none());
    }

    #[test]
    fn field_selection_is_strictly_increasing() {
        assert_eq!(
            increasing_nonzero_u32(&["1".to_owned(), "2".to_owned()]),
            Ok(vec![1, 2])
        );
        assert_eq!(
            increasing_nonzero_u32(&["2".to_owned(), "1".to_owned()]),
            Err(())
        );
        assert_eq!(
            increasing_nonzero_u32(&["1".to_owned(), "1".to_owned()]),
            Err(())
        );
    }

    #[test]
    fn application_query_permissions_are_explicit_and_hash_checked() {
        use v1::capability_permission::Permission;

        let named: CapabilityPermissionInput = serde_json::from_str(
            r#"{
                "type":"execute_named_query",
                "contract_lineage":"TicketDesk",
                "query_module_hash":"3131313131313131313131313131313131313131313131313131313131313131",
                "query_name":"TicketPage"
            }"#,
        )
        .expect("named permission JSON");
        let lowered = capability_permission(named).expect("named permission");
        assert!(matches!(
            lowered.permission,
            Some(Permission::ExecuteNamedQuery(v1::NamedQueryPermission {
                contract_lineage,
                query_module_hash,
                query_name,
            })) if contract_lineage == "TicketDesk"
                && query_module_hash == vec![0x31; 32]
                && query_name == "TicketPage"
        ));

        for invalid in [
            "",
            "31",
            "gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg",
        ] {
            assert!(
                named_query_permission(
                    "TicketDesk".to_owned(),
                    invalid.to_owned(),
                    "TicketPage".to_owned(),
                )
                .is_err()
            );
        }
    }

    #[test]
    fn workspace_role_compilation_hides_all_kernel_requirements() {
        use v1::capability_permission::Permission;

        let mut workspace = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        assert!(workspace.pop());
        assert!(workspace.pop());
        let role = compile_role_from_workspace(
            &workspace
                .join("fixtures/application-manifests/ticketdesk-v1.json")
                .into_os_string(),
            "TicketDeskAgent",
            None,
        )
        .expect("role");
        let grant = application_role_grant_to_proto(role.internal_grant());
        assert!(grant.permissions.iter().all(|permission| {
            matches!(
                permission.permission,
                Some(Permission::InvokeCommand(_))
                    | Some(Permission::ExecuteNamedQuery(_))
                    | Some(Permission::ApplicationRoleIdentity(_))
            )
        }));
        assert!(grant.permissions.iter().any(|permission| {
            matches!(
                permission.permission.as_ref(),
                Some(Permission::ApplicationRoleIdentity(hash))
                    if hash.as_slice() == role.identity().as_bytes()
            )
        }));
    }

    #[test]
    fn workspace_role_failure_preserves_symbolic_authoring_diagnostic() {
        let mut workspace = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        assert!(workspace.pop());
        assert!(workspace.pop());
        let error = compile_role_from_workspace(
            &workspace
                .join("fixtures/application-manifests/ticketdesk-v1.json")
                .into_os_string(),
            "MissingRole",
            None,
        )
        .expect_err("missing role");
        let RoleWorkspaceError::Authoring(diagnostics) = error else {
            panic!("structured diagnostic required");
        };
        let diagnostic = &diagnostics.as_slice()[0];
        assert_eq!(diagnostic.code().as_str(), "RDB-AR001");
        assert_eq!(diagnostic.symbol_path(), &["MissingRole"]);
        assert!(
            !diagnostics
                .render_json()
                .expect("JSON")
                .contains("field_id")
        );
    }

    #[test]
    fn widened_successor_role_compiles_from_the_pinned_parent_aware_bundle() {
        let directory =
            std::env::temp_dir().join(format!("riffdb-cli-successor-role-{}", std::process::id()));
        let _ = fs::remove_dir_all(&directory);
        crate::scaffold::create_application(
            "safe-app",
            crate::scaffold::ScaffoldLanguage::Rust,
            &directory,
        )
        .expect("scaffold genesis application");

        let source_path = directory.join("riffdb.application.json");
        let contract_path = directory.join("riffdb/contract.riff");
        let genesis_source = fs::read_to_string(&contract_path).expect("genesis source");
        let genesis = riffdb_contract_compiler::compile_contract_source(&genesis_source)
            .expect("genesis contract");
        let versioned = genesis_source.replacen("version 1", "version 2", 1);
        let (body, _) = versioned
            .rsplit_once("}\n")
            .expect("contract closing delimiter");
        let successor_source = format!(
            "{body}\n\n  command RenameItem {{\n    input idempotency_key: string<128>\n    input item_id: uuid\n    input title: string<128>\n    idempotency_key idempotency_key\n    mutate Item(item_id) as item\n      else ItemMissing {{ item_id: item_id }}\n    set item.title = title\n    return Renamed {{ item: item }}\n  }}\n}}\n"
        );
        let successor =
            riffdb_contract_compiler::compile_contract_successor(&successor_source, &genesis)
                .expect("additive successor contract");
        fs::write(&contract_path, successor_source).expect("write successor source");

        let mut application: serde_json::Value =
            serde_json::from_slice(&fs::read(&source_path).expect("application source"))
                .expect("application JSON");
        application["contract"]["version"] = serde_json::json!(2);
        application["roles"][0]["commands"] = serde_json::json!(["CreateItem", "RenameItem"]);
        fs::write(
            &source_path,
            serde_json::to_vec(&application).expect("successor application JSON"),
        )
        .expect("write successor application source");
        crate::scaffold::write_application_lock_with_bundle(&source_path, None, successor)
            .expect("write parent-aware successor lock");

        let role =
            compile_role_from_workspace(&source_path.into_os_string(), "SafeAppApplication", None)
                .expect("the widened role compiles from the pinned successor bundle");
        assert_eq!(role.operations().len(), 3);
        assert!(role.operations().iter().any(|operation| {
            operation.kind() == riffdb_query_module::ApplicationRoleOperationKind::Command
                && operation.name() == "RenameItem"
        }));
        let exact_role = compile_role_from_workspace(
            &directory
                .join("generated/riffdb.application.exact.json")
                .into_os_string(),
            "SafeAppApplication",
            None,
        )
        .expect("standalone role paths discover the same successor lock");
        assert_eq!(exact_role.identity(), role.identity());
        fs::remove_dir_all(directory).expect("fixture cleanup");
    }

    #[test]
    fn development_profile_documents_are_canonical_and_disjoint() {
        for source in [
            include_str!("../../../release/config/dev-role-ticketdesk-application.json"),
            include_str!("../../../release/config/dev-role-ticketdesk-agent.json"),
            include_str!("../../../release/config/dev-role-ticketdesk-kernel.json"),
        ] {
            let input: CapabilityCreateInput = serde_json::from_str(source).expect("profile JSON");
            let request = capability_request(
                input,
                parse_uuid_v7("01900000-0000-7000-8000-000000000041")
                    .expect("capability ID")
                    .to_vec(),
                v1::CapabilityCreateMode::Normal,
            )
            .expect("profile request");
            NormalCapabilityCreateTemplate::new(request).expect("canonical profile");
        }
    }
}
