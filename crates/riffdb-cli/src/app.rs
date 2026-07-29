use std::ffi::OsString;
use std::fs;
use std::io::{self, Read};
use std::path::Path;
use std::process::ExitCode;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use clap::Parser;
use riffdb_client_rust::{
    AttemptBudget, BackupNameV1, BootstrapCapabilityCreateTemplate, CallMetadata, ClientError,
    CreateOfflineBackup, IdempotentCommand, NormalCapabilityCreateTemplate,
    OfflineMaintenanceOperationId, OfflineMaintenanceReplacementConfirmation, RestoreOfflineBackup,
    RiffDbClient, app_v1, generate_capability_id, generate_offline_maintenance_operation_id,
    generate_request_id, v1,
};
use serde::{Deserialize, Serialize};

use crate::cli::{
    BackupCommand, CapabilityCommand, Cli, CommandCommand, CommitCommand, ContractCommand,
    ContractSelectionArgs, DemoCommand, EntityCommand, ProjectionCommand, QueryCommand,
    RevocationReason, ServerCommand, TopLevel,
};
use crate::config::{EffectiveConfig, Environment, ProcessEnvironment, resolve};
use crate::credential::{
    CredentialError, bootstrap_material, normal_credential, retain_normal_token,
};
use crate::input::{
    InputError, MAX_INPUT_BYTES, read_file, read_path_or_stdin, utf8, validate_path,
};
use crate::output::{
    CommandIdentity, NormalCreateDisposition, Terminal, client_error, local_error,
    local_error_with, maintenance_uncertain, render_bootstrap, render_commit,
    render_contract_deploy, render_contract_validation, render_create_maintenance_start,
    render_entity, render_execution, render_health, render_maintenance_operation,
    render_normal_create, render_outcome, render_projection, render_restore_maintenance_start,
    render_revoke, success, take_normal_create_disposition, uncertain,
};
use crate::runner::{RunnerError, RunnerStream, run_budget};
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

async fn dispatch(
    command: TopLevel,
    config: &EffectiveConfig,
    environment: &dyn Environment,
    stdin: &mut dyn Read,
) -> Terminal {
    match command {
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
        TopLevel::Capability { command } => {
            capability_command(command, config, environment, stdin).await
        }
        TopLevel::Server { command } => server_command(command, config, environment).await,
        TopLevel::Backup { command } => backup_command(command, config, environment).await,
        TopLevel::Demo { command } => demo_command(command, config, environment),
    }
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
            )
            .await
        }
        QueryCommand::RunNamed {
            query_name,
            module_hash,
            parameters,
            cursor,
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

async fn execute_query_cli(
    identity: CommandIdentity,
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    contract: ContractSelectionArgs,
    source: String,
    parameters: serde_json::Map<String, serde_json::Value>,
    cursor: Option<String>,
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
    match client
        .execute_query(
            app_v1::ExecuteQueryRequest {
                contract,
                query: Some(app_v1::execute_query_request::Query::Source(source)),
                module_hash: None,
                parameters,
                cursor,
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
) -> Terminal {
    let contract = match symbolic_contract_selection(contract) {
        Ok(contract) => contract,
        Err(()) => return invalid_input(identity),
    };
    let request_id = match request_id() {
        Ok(request_id) => request_id,
        Err(error) => return client_error(identity, &error),
    };
    match client
        .execute_query(
            app_v1::ExecuteQueryRequest {
                contract,
                query: Some(app_v1::execute_query_request::Query::QueryName(query_name)),
                module_hash,
                parameters,
                cursor,
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
        let name = path
            .file_stem()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .ok_or(InputError::Invalid)?
            .to_owned();
        let source = utf8(read_file(&path, MAX_INPUT_BYTES)?)?;
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
                    v1::ValidateContractRequest { request_id, source },
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
        CommandCommand::Run {
            command_name,
            input,
            expected_version,
        } => {
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
                Err(error) => client_error(CommandIdentity::CommandRun, &error),
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
    let CommitCommand::Show { commit_sequence } = command;
    let commit_sequence = match parse_nonzero_u64(&commit_sequence) {
        Ok(value) => value,
        Err(()) => return invalid_input(CommandIdentity::CommitShow),
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
            },
            &metadata,
        )
        .await
    {
        Ok(response) => render_commit(&response),
        Err(error) => client_error(CommandIdentity::CommitShow, &error),
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
            let metadata = match material.metadata() {
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
            render_normal_create(&disposition)
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
                reason: match reason {
                    RevocationReason::Requested => v1::RevocationReason::Requested as i32,
                    RevocationReason::Replaced => v1::RevocationReason::Replaced as i32,
                    RevocationReason::SuspectedCompromise => {
                        v1::RevocationReason::SuspectedCompromise as i32
                    }
                    RevocationReason::PolicyChange => v1::RevocationReason::PolicyChange as i32,
                },
            };
            match client.revoke_capability(request, &metadata).await {
                Ok(response) => render_revoke(&response),
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
    }
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
        InputError::ReadFailed => {
            local_error(command, "input_read_failed", "input could not be read")
        }
        InputError::TooLarge => {
            local_error(command, "input_too_large", "input exceeds the CLI limit")
        }
        InputError::Invalid => invalid_input(command),
    }
}

fn invalid_input(command: CommandIdentity) -> Terminal {
    local_error(command, "input_invalid", "input is invalid")
}

fn read_text(path: &OsString, stdin: &mut dyn Read) -> Result<String, InputError> {
    utf8(read_path_or_stdin(path, stdin, MAX_INPUT_BYTES)?)
}

fn read_json<T: for<'de> Deserialize<'de>>(
    path: &OsString,
    stdin: &mut dyn Read,
) -> Result<T, InputError> {
    let bytes = read_path_or_stdin(path, stdin, MAX_INPUT_BYTES)?;
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
        serde_json::Value::Object(_) => return Err(()),
    };
    Ok(v1::Value { kind: Some(kind) })
}

fn natural_command_record(
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
        TopLevel::Contract {
            command: ContractCommand::Validate { .. },
        } => CommandIdentity::ContractValidate,
        TopLevel::Contract {
            command: ContractCommand::Deploy { .. },
        } => CommandIdentity::ContractDeploy,
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
            output: crate::cli::OutputMode::Json,
            max_attempts: 3,
            credential_file: None,
        }
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
}
