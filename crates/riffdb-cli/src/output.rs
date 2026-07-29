use std::io::{self, Write};
use std::process::ExitCode;

use riffdb_client_rust::{
    ClientError, DetailsFreeStatus, OfflineMaintenanceOperationId, PublicError, PublicErrorDetails,
    RecoveryAction, ValidationPathSegment, v1,
};
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::ser::{Error as _, SerializeMap, SerializeSeq};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::cli::OutputMode;
use crate::value::{LowerHex, OutputRecord, OutputValue, PaddedBytes, UuidText, format_uuid};

pub(crate) const MAX_OUTPUT_BYTES: usize = 4_194_304;
const OUTPUT_SCHEMA: &str = "riffdb.cli.output/v1";
#[cfg(test)]
const OUTPUT_TOO_LARGE: &[u8] =
    b"contract.validate: output_too_large: output exceeds the CLI limit\n";
#[cfg(test)]
const OUTPUT_RENDER_FAILED: &[u8] =
    b"contract.validate: output_render_failed: output rendering failed\n";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommandIdentity {
    ContractValidate,
    ContractDeploy,
    CommandExecute,
    CommandRun,
    CommandOutcome,
    EntityGet,
    CommitShow,
    ProjectionQuery,
    QueryDescribe,
    QueryCheck,
    QueryExplain,
    QueryRun,
    QueryRunNamed,
    QueryDeploy,
    QueryModule,
    QueryRepl,
    CapabilityBootstrap,
    CapabilityCreate,
    CapabilityRevoke,
    ServerHealth,
    BackupCreate,
    BackupRestore,
    BackupOperation,
    DemoBudget,
}

impl CommandIdentity {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ContractValidate => "contract.validate",
            Self::ContractDeploy => "contract.deploy",
            Self::CommandExecute => "command.execute",
            Self::CommandRun => "command.run",
            Self::CommandOutcome => "command.outcome",
            Self::EntityGet => "entity.get",
            Self::CommitShow => "commit.show",
            Self::ProjectionQuery => "projection.query",
            Self::QueryDescribe => "query.describe",
            Self::QueryCheck => "query.check",
            Self::QueryExplain => "query.explain",
            Self::QueryRun => "query.run",
            Self::QueryRunNamed => "query.run_named",
            Self::QueryDeploy => "query.deploy",
            Self::QueryModule => "query.module",
            Self::QueryRepl => "query.repl",
            Self::CapabilityBootstrap => "capability.bootstrap",
            Self::CapabilityCreate => "capability.create",
            Self::CapabilityRevoke => "capability.revoke",
            Self::ServerHealth => "server.health",
            Self::BackupCreate => "backup.create",
            Self::BackupRestore => "backup.restore",
            Self::BackupOperation => "backup.operation",
            Self::DemoBudget => "demo.budget",
        }
    }
}

pub(crate) struct Terminal {
    command: CommandIdentity,
    json: Result<Vec<u8>, RenderFailure>,
    human: Result<String, RenderFailure>,
    failed: bool,
    exit: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RenderFailure {
    TooLarge,
    Failed,
}

pub(crate) fn success<T: Serialize>(
    command: CommandIdentity,
    status: &str,
    result: &T,
) -> Terminal {
    let mut terminal = Terminal::new(command, true, result, String::new(), false, 0);
    terminal.human = terminal
        .json
        .as_deref()
        .map_err(|failure| *failure)
        .and_then(|json| render_human_success(command, status, json));
    terminal
}

pub(crate) fn local_error(
    command: CommandIdentity,
    code: &'static str,
    message: &'static str,
) -> Terminal {
    error_with_exit(command, &LocalError { code, message }, code, message, 2)
}

pub(crate) fn local_error_with<T: Serialize>(
    command: CommandIdentity,
    error: &T,
    code: &'static str,
    message: &'static str,
    exit: u8,
) -> Terminal {
    error_with_exit(command, error, code, message, exit)
}

pub(crate) fn uncertain(
    command: CommandIdentity,
    code: &'static str,
    message: &'static str,
    recovery_action: &'static str,
    capability_id: Option<&[u8]>,
) -> Terminal {
    let id = capability_id.and_then(format_uuid);
    let error = UncertainError {
        code,
        message,
        recovery_action,
        capability_id: id.as_deref(),
    };
    error_with_exit(command, &error, code, message, 3)
}

pub(crate) fn maintenance_uncertain(
    command: CommandIdentity,
    operation_id: OfflineMaintenanceOperationId,
) -> Terminal {
    let operation_id = operation_id.to_string();
    error_with_exit(
        command,
        &MaintenanceUncertainError {
            code: "outcome_unknown",
            message: "the maintenance operation outcome remains unknown",
            recovery_action: "poll_maintenance_operation",
            operation_id: &operation_id,
        },
        "outcome_unknown",
        "the maintenance operation outcome remains unknown",
        3,
    )
}

pub(crate) fn client_error(command: CommandIdentity, error: &ClientError) -> Terminal {
    match error {
        ClientError::Public(error) => public_error(command, error),
        ClientError::OutcomeUnknown(_) => uncertain(
            command,
            "outcome_unknown",
            "the command outcome remains unknown",
            "resolve_with_same_idempotency_key",
            None,
        ),
        ClientError::DetailsFree(status) => {
            let (code, message) = match status {
                DetailsFreeStatus::Unauthenticated => {
                    ("authentication_failed", "authentication failed")
                }
                DetailsFreeStatus::Cancelled => ("request_cancelled", "request was cancelled"),
                DetailsFreeStatus::DeadlineExceeded => {
                    ("request_deadline_elapsed", "request deadline elapsed")
                }
                DetailsFreeStatus::ResponseTooLarge => {
                    ("response_too_large", "response exceeds the service limit")
                }
                DetailsFreeStatus::EmergencyInternal => {
                    ("internal_failure", "an internal error occurred")
                }
                DetailsFreeStatus::TransportUnavailable => {
                    ("transport_unavailable", "the gRPC transport is unavailable")
                }
            };
            error_with_exit(command, &ClientErrorDto { code, message }, code, message, 1)
        }
        ClientError::Protocol(_) => error_with_exit(
            command,
            &ClientErrorDto {
                code: "protocol_failure",
                message: "the RiffDB gRPC peer returned an invalid protocol response",
            },
            "protocol_failure",
            "the RiffDB gRPC peer returned an invalid protocol response",
            1,
        ),
        ClientError::IdentifierGeneration(_) => error_with_exit(
            command,
            &ClientErrorDto {
                code: "identifier_generation_failed",
                message: "request identifier generation failed",
            },
            "identifier_generation_failed",
            "request identifier generation failed",
            1,
        ),
        ClientError::ConnectionFailure => error_with_exit(
            command,
            &ClientErrorDto {
                code: "connection_failed",
                message: "the gRPC channel could not connect",
            },
            "connection_failed",
            "the gRPC channel could not connect",
            1,
        ),
    }
}

pub(crate) fn render_contract_validation(response: &v1::ValidateContractResponse) -> Terminal {
    use v1::compilation_diagnostics::Diagnostics;
    use v1::validate_contract_response::Result;
    match response.result.as_ref() {
        Some(Result::Valid(_)) => success(
            CommandIdentity::ContractValidate,
            "valid",
            &StatusResult { status: "valid" },
        ),
        Some(Result::Invalid(diagnostics)) => match diagnostics.diagnostics.as_ref() {
            Some(Diagnostics::Syntax(list)) => success(
                CommandIdentity::ContractValidate,
                "invalid_syntax",
                &SyntaxResult {
                    status: "invalid_syntax",
                    diagnostics: SyntaxDiagnostics(&list.diagnostics),
                },
            ),
            Some(Diagnostics::Semantic(list)) => success(
                CommandIdentity::ContractValidate,
                "invalid_semantic",
                &SemanticResult {
                    status: "invalid_semantic",
                    diagnostics: SemanticDiagnostics(&list.diagnostics),
                },
            ),
            None => rendering_failure(CommandIdentity::ContractValidate),
        },
        None => rendering_failure(CommandIdentity::ContractValidate),
    }
}

pub(crate) fn render_contract_deploy(response: &v1::DeployContractResponse) -> Terminal {
    use v1::deploy_contract_response::Result;
    match response.result.as_ref() {
        Some(Result::Activated(contract)) => success(
            CommandIdentity::ContractDeploy,
            "activated",
            &ContractResult {
                status: "activated",
                contract: ContractDescriptorDto(contract),
            },
        ),
        Some(Result::AlreadyActive(contract)) => success(
            CommandIdentity::ContractDeploy,
            "already_active",
            &ContractResult {
                status: "already_active",
                contract: ContractDescriptorDto(contract),
            },
        ),
        Some(Result::ExpectedActiveVersionMismatch(mismatch)) => success(
            CommandIdentity::ContractDeploy,
            "expected_version_mismatch",
            &ExpectedVersionMismatch {
                status: "expected_version_mismatch",
                actual_active_version: mismatch
                    .actual_active_version
                    .map(|value| value.to_string()),
            },
        ),
        Some(Result::BundleConflict(_)) => success(
            CommandIdentity::ContractDeploy,
            "bundle_conflict",
            &StatusResult {
                status: "bundle_conflict",
            },
        ),
        None => rendering_failure(CommandIdentity::ContractDeploy),
    }
}

pub(crate) fn render_execution(
    command: CommandIdentity,
    response: &v1::ExecuteCommandResponse,
) -> Terminal {
    use v1::execute_command_response::CompletionStatus;
    let status = match CompletionStatus::try_from(response.status) {
        Ok(CompletionStatus::Committed) => "committed",
        Ok(CompletionStatus::Replayed) => "replayed",
        Ok(CompletionStatus::ExecutedReadOnly) => "executed_read_only",
        _ => return rendering_failure(command),
    };
    let outcome = match response.outcome.as_ref() {
        Some(outcome) => outcome,
        None => return rendering_failure(command),
    };
    let committed = status != "executed_read_only";
    success(
        command,
        status,
        &ExecutionDto {
            status,
            commit_sequence: committed.then(|| response.commit_sequence.to_string()),
            contract_version: response.contract_version.to_string(),
            plan_hash: LowerHex(&response.plan_hash),
            outcome_type: &response.outcome_type,
            outcome: OutputValue(outcome),
            provenance_uri: (!response.provenance_uri.is_empty())
                .then_some(response.provenance_uri.as_str()),
            durability_mode: (!response.durability_mode.is_empty())
                .then_some(response.durability_mode.as_str()),
            outcome_uri: response.outcome_uri.as_deref(),
        },
    )
}

pub(crate) fn render_outcome(response: &v1::GetOutcomeResponse) -> Terminal {
    use v1::get_outcome_response::Result;
    match response.result.as_ref() {
        Some(Result::NotFound(_)) => success(
            CommandIdentity::CommandOutcome,
            "not_found",
            &StatusResult {
                status: "not_found",
            },
        ),
        Some(Result::Found(execution)) => {
            let nested = ExecutionDto::from_response(execution);
            match nested {
                Some(outcome) => success(
                    CommandIdentity::CommandOutcome,
                    "found",
                    &FoundExecution {
                        status: "found",
                        outcome,
                    },
                ),
                None => rendering_failure(CommandIdentity::CommandOutcome),
            }
        }
        None => rendering_failure(CommandIdentity::CommandOutcome),
    }
}

pub(crate) fn render_entity(response: &v1::GetEntityResponse) -> Terminal {
    use v1::get_entity_response::Result;
    match response.result.as_ref() {
        Some(Result::NotFound(_)) => success(
            CommandIdentity::EntityGet,
            "not_found",
            &StatusResult {
                status: "not_found",
            },
        ),
        Some(Result::Found(entity)) => match entity.fields.as_ref() {
            Some(fields) => success(
                CommandIdentity::EntityGet,
                "found",
                &EntityFound {
                    status: "found",
                    entity_key: PaddedBytes(&entity.entity_key),
                    entity_version: entity.entity_version.to_string(),
                    written_by_contract_version: entity.written_by_contract_version.to_string(),
                    fields: OutputRecord(fields),
                },
            ),
            None => rendering_failure(CommandIdentity::EntityGet),
        },
        None => rendering_failure(CommandIdentity::EntityGet),
    }
}

pub(crate) fn render_commit(response: &v1::GetCommitResponse) -> Terminal {
    use v1::get_commit_response::Result;
    match response.result.as_ref() {
        Some(Result::NotFound(_)) => success(
            CommandIdentity::CommitShow,
            "not_found",
            &StatusResult {
                status: "not_found",
            },
        ),
        Some(Result::Found(commit)) => success(
            CommandIdentity::CommitShow,
            "found",
            &CommitFound {
                status: "found",
                commit: CommitDto(commit),
            },
        ),
        None => rendering_failure(CommandIdentity::CommitShow),
    }
}

pub(crate) fn render_projection(response: &v1::QueryProjectionResponse) -> Terminal {
    use v1::query_projection_response::Result;
    match response.result.as_ref() {
        Some(Result::Ready(ready)) => {
            let Some(data) = ready.data.as_ref() else {
                return rendering_failure(CommandIdentity::ProjectionQuery);
            };
            let Some(frontier) = ready.frontier.as_ref() else {
                return rendering_failure(CommandIdentity::ProjectionQuery);
            };
            let Some(observed_fence) = data.observed_fence.as_ref() else {
                return rendering_failure(CommandIdentity::ProjectionQuery);
            };
            success(
                CommandIdentity::ProjectionQuery,
                "ready",
                &ProjectionReady {
                    status: "ready",
                    rows: ProjectionRows(&data.items),
                    next_cursor: data.next_cursor.as_ref().map(|cursor| PaddedBytes(cursor)),
                    observed_fence: ProjectionFence(observed_fence),
                    frontier: Frontier(frontier),
                },
            )
        }
        Some(Result::WaitTimedOut(timeout)) => match timeout.current.as_ref() {
            Some(current) => success(
                CommandIdentity::ProjectionQuery,
                "wait_timed_out",
                &ProjectionWait {
                    status: "wait_timed_out",
                    required_sequence: timeout.required_sequence.to_string(),
                    current: Frontier(current),
                },
            ),
            None => rendering_failure(CommandIdentity::ProjectionQuery),
        },
        Some(Result::Degraded(degraded)) => {
            let (Some(current), Some(reason)) =
                (degraded.current.as_ref(), degraded.reason.as_ref())
            else {
                return rendering_failure(CommandIdentity::ProjectionQuery);
            };
            success(
                CommandIdentity::ProjectionQuery,
                "degraded",
                &ProjectionDegraded {
                    status: "degraded",
                    current: Frontier(current),
                    reason: ProjectionReason(reason),
                },
            )
        }
        Some(Result::Invalid(invalid)) => match projection_failure(invalid.reason) {
            Some(reason) => success(
                CommandIdentity::ProjectionQuery,
                "invalid",
                &ProjectionInvalid {
                    status: "invalid",
                    reason,
                },
            ),
            None => rendering_failure(CommandIdentity::ProjectionQuery),
        },
        None => rendering_failure(CommandIdentity::ProjectionQuery),
    }
}

pub(crate) fn render_bootstrap(
    response: &v1::CreateCapabilityResponse,
    bearer_retained: bool,
) -> Terminal {
    use v1::bootstrap_create_capability_result::Result as BootstrapResult;
    use v1::create_capability_response::Result;
    let Some(Result::Bootstrap(result)) = response.result.as_ref() else {
        return rendering_failure(CommandIdentity::CapabilityBootstrap);
    };
    match result.result.as_ref() {
        Some(BootstrapResult::Created(transition)) => success(
            CommandIdentity::CapabilityBootstrap,
            "created",
            &BootstrapCreated {
                status: "created",
                transition: CapabilityTransitionDto(transition),
                bearer_retained,
            },
        ),
        Some(BootstrapResult::Replayed(transition)) => success(
            CommandIdentity::CapabilityBootstrap,
            "replayed",
            &BootstrapCreated {
                status: "replayed",
                transition: CapabilityTransitionDto(transition),
                bearer_retained,
            },
        ),
        Some(BootstrapResult::BootstrapConflict(_)) => success(
            CommandIdentity::CapabilityBootstrap,
            "bootstrap_conflict",
            &StatusResult {
                status: "bootstrap_conflict",
            },
        ),
        None => rendering_failure(CommandIdentity::CapabilityBootstrap),
    }
}

pub(crate) enum NormalCreateDisposition {
    Created(v1::CapabilityTransition),
    AlreadyCreated(v1::CapabilityIdentity),
    Conflict,
}

pub(crate) fn take_normal_create_disposition(
    response: &mut v1::CreateCapabilityResponse,
) -> Option<(NormalCreateDisposition, Option<String>)> {
    use v1::create_capability_response::Result;
    use v1::normal_create_capability_result::Result as NormalResult;
    let Result::Normal(mut result) = response.result.take()? else {
        return None;
    };
    match result.result.take()? {
        NormalResult::Created(created) => Some((
            NormalCreateDisposition::Created(created.transition?),
            Some(created.token),
        )),
        NormalResult::AlreadyCreatedTokenUnavailable(identity) => {
            Some((NormalCreateDisposition::AlreadyCreated(identity), None))
        }
        NormalResult::CapabilityIdConflict(_) => Some((NormalCreateDisposition::Conflict, None)),
    }
}

pub(crate) fn render_normal_create(disposition: &NormalCreateDisposition) -> Terminal {
    match disposition {
        NormalCreateDisposition::Created(transition) => success(
            CommandIdentity::CapabilityCreate,
            "created",
            &NormalCreated {
                status: "created",
                transition: CapabilityTransitionDto(transition),
                credential_retained: true,
            },
        ),
        NormalCreateDisposition::AlreadyCreated(identity) => success(
            CommandIdentity::CapabilityCreate,
            "already_created_token_unavailable",
            &AlreadyCreated {
                status: "already_created_token_unavailable",
                identity: CapabilityIdentityDto(identity),
            },
        ),
        NormalCreateDisposition::Conflict => success(
            CommandIdentity::CapabilityCreate,
            "capability_id_conflict",
            &StatusResult {
                status: "capability_id_conflict",
            },
        ),
    }
}

pub(crate) fn render_revoke(response: &v1::RevokeCapabilityResponse) -> Terminal {
    use v1::revoke_capability_response::Result;
    match response.result.as_ref() {
        Some(Result::Revoked(transition)) => success(
            CommandIdentity::CapabilityRevoke,
            "revoked",
            &RevokeResult {
                status: "revoked",
                transition: CapabilityTransitionDto(transition),
            },
        ),
        Some(Result::AlreadyRevoked(transition)) => success(
            CommandIdentity::CapabilityRevoke,
            "already_revoked",
            &RevokeResult {
                status: "already_revoked",
                transition: CapabilityTransitionDto(transition),
            },
        ),
        Some(Result::CapabilityNotFound(_)) => success(
            CommandIdentity::CapabilityRevoke,
            "capability_not_found",
            &StatusResult {
                status: "capability_not_found",
            },
        ),
        None => rendering_failure(CommandIdentity::CapabilityRevoke),
    }
}

pub(crate) fn render_create_maintenance_start(
    response: &v1::CreateOfflineBackupResponse,
) -> Terminal {
    render_maintenance_start(
        CommandIdentity::BackupCreate,
        response.disposition,
        response.operation.as_ref(),
    )
}

pub(crate) fn render_restore_maintenance_start(
    response: &v1::RestoreOfflineBackupResponse,
) -> Terminal {
    render_maintenance_start(
        CommandIdentity::BackupRestore,
        response.disposition,
        response.operation.as_ref(),
    )
}

fn render_maintenance_start(
    command: CommandIdentity,
    disposition: i32,
    operation: Option<&v1::OfflineMaintenanceOperation>,
) -> Terminal {
    let status = match v1::OfflineMaintenanceStartDisposition::try_from(disposition) {
        Ok(v1::OfflineMaintenanceStartDisposition::Accepted) => "accepted",
        Ok(v1::OfflineMaintenanceStartDisposition::AlreadyAccepted) => "already_accepted",
        Ok(v1::OfflineMaintenanceStartDisposition::Terminal) => "terminal",
        _ => return rendering_failure(command),
    };
    let Some(operation) = operation else {
        return rendering_failure(command);
    };
    let Some(operation) = MaintenanceOperationDto::new(status, operation) else {
        return rendering_failure(command);
    };
    success(command, status, &operation)
}

pub(crate) fn render_maintenance_operation(
    response: &v1::GetOfflineMaintenanceOperationResponse,
) -> Terminal {
    use v1::get_offline_maintenance_operation_response::Result;
    match response.result.as_ref() {
        Some(Result::NotFound(_)) => success(
            CommandIdentity::BackupOperation,
            "not_found",
            &StatusResult {
                status: "not_found",
            },
        ),
        Some(Result::Found(operation)) => {
            let Some(operation) = MaintenanceOperationDto::new("found", operation) else {
                return rendering_failure(CommandIdentity::BackupOperation);
            };
            success(CommandIdentity::BackupOperation, "found", &operation)
        }
        None => rendering_failure(CommandIdentity::BackupOperation),
    }
}

pub(crate) fn render_health(response: &v1::HealthResponse) -> Terminal {
    use v1::health_response::Result;
    match response.result.as_ref() {
        Some(Result::PreBootstrap(health)) => {
            let lifecycle = match v1::PreBootstrapLifecycle::try_from(health.lifecycle) {
                Ok(v1::PreBootstrapLifecycle::InitializingValidation) => "initializing_validation",
                Ok(v1::PreBootstrapLifecycle::InitializingBootstrap) => "initializing_bootstrap",
                _ => return rendering_failure(CommandIdentity::ServerHealth),
            };
            success(
                CommandIdentity::ServerHealth,
                "pre_bootstrap",
                &PreBootstrapHealth {
                    status: "pre_bootstrap",
                    lifecycle,
                    liveness: health.liveness,
                    readiness: health.readiness,
                },
            )
        }
        Some(Result::Authenticated(health)) => {
            let status = match v1::HealthStatus::try_from(health.status) {
                Ok(v1::HealthStatus::Ready) => "ready",
                Ok(v1::HealthStatus::NotReady) => "not_ready",
                Ok(v1::HealthStatus::Degraded) => "degraded",
                _ => return rendering_failure(CommandIdentity::ServerHealth),
            };
            let (Some(started_at), Some(build)) =
                (health.started_at.as_ref(), health.build.as_ref())
            else {
                return rendering_failure(CommandIdentity::ServerHealth);
            };
            success(
                CommandIdentity::ServerHealth,
                status,
                &AuthenticatedHealth {
                    status,
                    active_contract_version: health
                        .active_contract_version
                        .map(|value| value.to_string()),
                    last_commit_sequence: health
                        .last_commit_sequence
                        .map(|value| value.to_string()),
                    components: HealthComponents(&health.components),
                    started_at: TimestampDto(started_at),
                    build: BuildDto(build),
                },
            )
        }
        None => rendering_failure(CommandIdentity::ServerHealth),
    }
}

fn rendering_failure(command: CommandIdentity) -> Terminal {
    Terminal {
        command,
        json: Err(RenderFailure::Failed),
        human: Err(RenderFailure::Failed),
        failed: true,
        exit: 2,
    }
}

fn public_error(command: CommandIdentity, error: &PublicError) -> Terminal {
    let exit = if error.code() == "outcome_unknown" {
        3
    } else {
        1
    };
    error_with_exit(
        command,
        &PublicErrorDto(error),
        error.code(),
        error.safe_message(),
        exit,
    )
}

fn error_with_exit<T: Serialize>(
    command: CommandIdentity,
    error: &T,
    code: &str,
    message: &str,
    exit: u8,
) -> Terminal {
    Terminal::new(
        command,
        false,
        error,
        format!("{}: {code}: {message}", command.as_str()),
        true,
        exit,
    )
}

impl Terminal {
    fn new<T: Serialize>(
        command: CommandIdentity,
        ok: bool,
        payload: &T,
        human: String,
        failed: bool,
        exit: u8,
    ) -> Self {
        let mut buffer = BoundedBuffer::new(MAX_OUTPUT_BYTES - 1);
        let json = match serde_json::to_writer(
            &mut buffer,
            &Envelope {
                command,
                ok,
                payload,
            },
        ) {
            Ok(()) => {
                let mut bytes = buffer.into_bytes();
                bytes.push(b'\n');
                Ok(bytes)
            }
            Err(_) if buffer.overflowed() => Err(RenderFailure::TooLarge),
            Err(_) => Err(RenderFailure::Failed),
        };
        let human = if human.len().saturating_add(1) > MAX_OUTPUT_BYTES {
            Err(RenderFailure::TooLarge)
        } else {
            Ok(human)
        };
        Self {
            command,
            json,
            human,
            failed,
            exit,
        }
    }

    pub(crate) fn emit(
        self,
        mode: OutputMode,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
    ) -> ExitCode {
        let command = self.command;
        match mode {
            OutputMode::Json => match self.json {
                Ok(bytes) => {
                    if stdout.write_all(&bytes).is_ok() && stdout.flush().is_ok() {
                        ExitCode::from(self.exit)
                    } else {
                        write_emergency(stderr, command, RenderFailure::Failed);
                        ExitCode::from(2)
                    }
                }
                Err(RenderFailure::TooLarge) => {
                    write_emergency(stderr, command, RenderFailure::TooLarge);
                    ExitCode::from(2)
                }
                Err(RenderFailure::Failed) => {
                    write_emergency(stderr, command, RenderFailure::Failed);
                    ExitCode::from(2)
                }
            },
            OutputMode::Human => match self.human {
                Ok(human) => {
                    let rendered = if self.failed {
                        writeln!(stderr, "{human}").and_then(|()| stderr.flush())
                    } else {
                        writeln!(stdout, "{human}").and_then(|()| stdout.flush())
                    };
                    if rendered.is_ok() {
                        ExitCode::from(self.exit)
                    } else {
                        write_emergency(&mut io::stderr(), command, RenderFailure::Failed);
                        ExitCode::from(2)
                    }
                }
                Err(RenderFailure::TooLarge) => {
                    write_emergency(stderr, command, RenderFailure::TooLarge);
                    ExitCode::from(2)
                }
                Err(RenderFailure::Failed) => {
                    write_emergency(stderr, command, RenderFailure::Failed);
                    ExitCode::from(2)
                }
            },
        }
    }

    #[cfg(test)]
    fn json_bytes(&self) -> Option<&[u8]> {
        self.json.as_deref().ok()
    }

    #[allow(dead_code)]
    pub(crate) const fn command(&self) -> CommandIdentity {
        self.command
    }
}

fn write_emergency(stderr: &mut dyn Write, command: CommandIdentity, failure: RenderFailure) {
    let (code, message) = match failure {
        RenderFailure::TooLarge => ("output_too_large", "output exceeds the CLI limit"),
        RenderFailure::Failed => ("output_render_failed", "output rendering failed"),
    };
    let _ = writeln!(stderr, "{}: {code}: {message}", command.as_str());
}

struct BoundedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    overflowed: bool,
}

impl BoundedBuffer {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(8 * 1024)),
            limit,
            overflowed: false,
        }
    }

    const fn overflowed(&self) -> bool {
        self.overflowed
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|length| length > self.limit)
        {
            self.overflowed = true;
            return Err(io::Error::other("bounded output exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
enum HumanValue {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<Self>),
    Object(Vec<(String, Self)>),
}

impl<'de> Deserialize<'de> for HumanValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(HumanValueVisitor)
    }
}

struct HumanValueVisitor;

impl<'de> Visitor<'de> for HumanValueVisitor {
    type Value = HumanValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a closed CLI JSON value")
    }

    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(HumanValue::Bool(value))
    }

    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(HumanValue::Number(value.to_string()))
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(HumanValue::Number(value.to_string()))
    }

    fn visit_f64<E: serde::de::Error>(self, _value: f64) -> Result<Self::Value, E> {
        Err(E::custom("floating-point CLI output is forbidden"))
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(HumanValue::String(value.to_owned()))
    }

    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
        Ok(HumanValue::String(value))
    }

    fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(HumanValue::Null)
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(HumanValue::Null)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(1_024));
        while let Some(value) = sequence.next_element()? {
            values.push(value);
        }
        Ok(HumanValue::Array(values))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut values = Vec::with_capacity(map.size_hint().unwrap_or(0).min(1_024));
        while let Some((key, value)) = map.next_entry()? {
            values.push((key, value));
        }
        Ok(HumanValue::Object(values))
    }
}

struct BoundedText {
    value: String,
}

impl BoundedText {
    fn new() -> Self {
        Self {
            value: String::with_capacity(256),
        }
    }

    fn push(&mut self, value: &str) -> Result<(), RenderFailure> {
        if self
            .value
            .len()
            .checked_add(value.len())
            .is_none_or(|length| length + 1 > MAX_OUTPUT_BYTES)
        {
            return Err(RenderFailure::TooLarge);
        }
        self.value.push_str(value);
        Ok(())
    }
}

fn render_human_success(
    command: CommandIdentity,
    status: &str,
    json: &[u8],
) -> Result<String, RenderFailure> {
    let envelope = serde_json::from_slice::<HumanValue>(json).map_err(|_| RenderFailure::Failed)?;
    let HumanValue::Object(fields) = envelope else {
        return Err(RenderFailure::Failed);
    };
    let result = fields
        .iter()
        .find_map(|(key, value)| (key == "result").then_some(value))
        .ok_or(RenderFailure::Failed)?;
    let HumanValue::Object(fields) = result else {
        return Err(RenderFailure::Failed);
    };

    let mut output = BoundedText::new();
    output.push(command.as_str())?;
    output.push(": ")?;
    output.push(status)?;
    for (key, value) in fields {
        if key != "status" {
            render_human_field(&mut output, 1, key, value)?;
        }
    }
    Ok(output.value)
}

fn render_human_field(
    output: &mut BoundedText,
    depth: usize,
    key: &str,
    value: &HumanValue,
) -> Result<(), RenderFailure> {
    output.push("\n")?;
    push_indent(output, depth)?;
    output.push(key)?;
    output.push(":")?;
    match value {
        HumanValue::Array(values) => {
            output.push(" ")?;
            if values.is_empty() {
                output.push("[]")?;
                return Ok(());
            }
            output.push(&values.len().to_string())?;
            for (index, value) in values.iter().enumerate() {
                output.push("\n")?;
                push_indent(output, depth + 1)?;
                output.push("[")?;
                output.push(&index.to_string())?;
                output.push("]:")?;
                render_human_nested(output, depth + 2, value)?;
            }
        }
        HumanValue::Object(fields) => {
            for (key, value) in fields {
                render_human_field(output, depth + 1, key, value)?;
            }
        }
        _ => {
            output.push(" ")?;
            render_human_scalar(output, value)?;
        }
    }
    Ok(())
}

fn render_human_nested(
    output: &mut BoundedText,
    depth: usize,
    value: &HumanValue,
) -> Result<(), RenderFailure> {
    match value {
        HumanValue::Object(fields) => {
            for (key, value) in fields {
                render_human_field(output, depth, key, value)?;
            }
        }
        HumanValue::Array(values) => {
            output.push(" ")?;
            if values.is_empty() {
                output.push("[]")?;
            } else {
                output.push(&values.len().to_string())?;
            }
        }
        _ => {
            output.push(" ")?;
            render_human_scalar(output, value)?;
        }
    }
    Ok(())
}

fn render_human_scalar(output: &mut BoundedText, value: &HumanValue) -> Result<(), RenderFailure> {
    match value {
        HumanValue::Null => output.push("null"),
        HumanValue::Bool(value) => output.push(if *value { "true" } else { "false" }),
        HumanValue::Number(value) => output.push(value),
        HumanValue::String(value) => {
            for character in value.chars() {
                if character.is_control() || character == '\\' {
                    for escaped in character.escape_default() {
                        let mut bytes = [0_u8; 4];
                        output.push(escaped.encode_utf8(&mut bytes))?;
                    }
                } else {
                    let mut bytes = [0_u8; 4];
                    output.push(character.encode_utf8(&mut bytes))?;
                }
            }
            Ok(())
        }
        HumanValue::Array(_) | HumanValue::Object(_) => Err(RenderFailure::Failed),
    }
}

fn push_indent(output: &mut BoundedText, depth: usize) -> Result<(), RenderFailure> {
    for _ in 0..depth {
        output.push("  ")?;
    }
    Ok(())
}

struct Envelope<'a, T> {
    command: CommandIdentity,
    ok: bool,
    payload: &'a T,
}

impl<T: Serialize> Serialize for Envelope<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(4))?;
        map.serialize_entry("schema", OUTPUT_SCHEMA)?;
        map.serialize_entry("command", self.command.as_str())?;
        map.serialize_entry("ok", &self.ok)?;
        map.serialize_entry(if self.ok { "result" } else { "error" }, self.payload)?;
        map.end()
    }
}

struct LocalError {
    code: &'static str,
    message: &'static str,
}

impl LocalError {
    const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }
}

impl<'a> From<(&'a &'static str, &'a &'static str)> for LocalError {
    fn from((code, message): (&'a &'static str, &'a &'static str)) -> Self {
        Self::new(code, message)
    }
}

struct ClientErrorDto {
    code: &'static str,
    message: &'static str,
}

impl ClientErrorDto {
    const fn kind() -> &'static str {
        "client"
    }
}

impl Serialize for LocalError {
    // Replaced below by the hand-written implementation to freeze key order.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("type", "local")?;
        map.serialize_entry("code", self.code)?;
        map.serialize_entry("message", self.message)?;
        map.end()
    }
}

impl Serialize for ClientErrorDto {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("type", Self::kind())?;
        map.serialize_entry("code", self.code)?;
        map.serialize_entry("message", self.message)?;
        map.end()
    }
}

struct UncertainError<'a> {
    code: &'static str,
    message: &'static str,
    recovery_action: &'static str,
    capability_id: Option<&'a str>,
}

struct MaintenanceUncertainError<'a> {
    code: &'static str,
    message: &'static str,
    recovery_action: &'static str,
    operation_id: &'a str,
}

impl Serialize for MaintenanceUncertainError<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("type", "uncertain")?;
        map.serialize_entry("code", self.code)?;
        map.serialize_entry("message", self.message)?;
        map.serialize_entry("recovery_action", self.recovery_action)?;
        map.serialize_entry("maintenance_operation_id", self.operation_id)?;
        map.end()
    }
}

impl Serialize for UncertainError<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("type", "uncertain")?;
        map.serialize_entry("code", self.code)?;
        map.serialize_entry("message", self.message)?;
        map.serialize_entry("recovery_action", self.recovery_action)?;
        if let Some(capability_id) = self.capability_id {
            map.serialize_entry("capability_id", capability_id)?;
        }
        map.end()
    }
}

struct PublicErrorDto<'a>(&'a PublicError);

impl Serialize for PublicErrorDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let error = self.0;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("type", "public")?;
        map.serialize_entry("code", error.code())?;
        map.serialize_entry("message", error.safe_message())?;
        map.serialize_entry("recovery_action", recovery_action(error.recovery_action()))?;
        match error.details() {
            PublicErrorDetails::None => {}
            PublicErrorDetails::Validation(issues) => {
                map.serialize_entry("details", &ValidationDetails(issues.as_slice()))?;
            }
            PublicErrorDetails::ContractMismatch {
                active_contract_version,
            } => {
                map.serialize_entry(
                    "details",
                    &ContractMismatchDetails(active_contract_version.get()),
                )?;
            }
            PublicErrorDetails::CommandExecutionFailed { code } => {
                let code = match code.code() {
                    1 => "arithmetic_fault",
                    2 => "resource_limit",
                    _ => return Err(S::Error::custom("unknown execution failure")),
                };
                map.serialize_entry("details", &ExecutionFailureDetails(code))?;
            }
        }
        if let Some(incident_id) = error.incident_id() {
            map.serialize_entry("incident_id", &incident_id.to_string())?;
        }
        map.end()
    }
}

fn recovery_action(action: RecoveryAction) -> &'static str {
    match action {
        RecoveryAction::CorrectRequest => "correct_request",
        RecoveryAction::Retry => "retry",
        RecoveryAction::ResolveWithSameIdempotencyKey => "resolve_with_same_idempotency_key",
        RecoveryAction::ObtainPermission => "obtain_permission",
        RecoveryAction::RefreshContract => "refresh_contract",
        RecoveryAction::ContactOperator => "contact_operator",
    }
}

#[derive(Serialize)]
struct StatusResult<'a> {
    status: &'a str,
}

#[derive(Serialize)]
struct SyntaxResult<'a> {
    status: &'a str,
    diagnostics: SyntaxDiagnostics<'a>,
}

struct SyntaxDiagnostics<'a>(&'a [v1::SyntaxDiagnostic]);

impl Serialize for SyntaxDiagnostics<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for diagnostic in self.0 {
            sequence.serialize_element(&SyntaxDiagnosticDto(diagnostic))?;
        }
        sequence.end()
    }
}

struct SyntaxDiagnosticDto<'a>(&'a v1::SyntaxDiagnostic);

impl Serialize for SyntaxDiagnosticDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let diagnostic = self.0;
        let span = diagnostic
            .span
            .as_ref()
            .ok_or_else(|| S::Error::custom("missing syntax span"))?;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("code", &diagnostic.code)?;
        map.serialize_entry("summary", &diagnostic.summary)?;
        if let Some(help) = diagnostic.help.as_deref() {
            map.serialize_entry("help", help)?;
        }
        map.serialize_entry("span", &SpanDto(span))?;
        map.serialize_entry("expected", &diagnostic.expected)?;
        map.end()
    }
}

#[derive(Serialize)]
struct SemanticResult<'a> {
    status: &'a str,
    diagnostics: SemanticDiagnostics<'a>,
}

struct SemanticDiagnostics<'a>(&'a [v1::SemanticDiagnostic]);

impl Serialize for SemanticDiagnostics<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for diagnostic in self.0 {
            sequence.serialize_element(&SemanticDiagnosticDto(diagnostic))?;
        }
        sequence.end()
    }
}

struct SemanticDiagnosticDto<'a>(&'a v1::SemanticDiagnostic);

impl Serialize for SemanticDiagnosticDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let diagnostic = self.0;
        let primary_span = diagnostic
            .primary_span
            .as_ref()
            .ok_or_else(|| S::Error::custom("missing semantic span"))?;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("code", &diagnostic.code)?;
        map.serialize_entry("summary", &diagnostic.summary)?;
        if let Some(help) = diagnostic.help.as_deref() {
            map.serialize_entry("help", help)?;
        }
        map.serialize_entry("primary_span", &SpanDto(primary_span))?;
        if let Some(span) = diagnostic.related_span.as_ref() {
            map.serialize_entry("related_span", &SpanDto(span))?;
        }
        map.end()
    }
}

struct SpanDto<'a>(&'a v1::SourceSpan);

impl Serialize for SpanDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("start", &self.0.start)?;
        map.serialize_entry("end", &self.0.end)?;
        map.end()
    }
}

struct ContractDescriptorDto<'a>(&'a v1::ContractDescriptor);

impl Serialize for ContractDescriptorDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let descriptor = self.0;
        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("contract_lineage", &descriptor.contract_lineage)?;
        map.serialize_entry("contract_version", &descriptor.contract_version.to_string())?;
        map.serialize_entry("bundle_hash", &LowerHex(&descriptor.bundle_hash))?;
        map.serialize_entry("source_hash", &LowerHex(&descriptor.source_hash))?;
        map.serialize_entry("plan_root_hash", &LowerHex(&descriptor.plan_root_hash))?;
        map.end()
    }
}

#[derive(Serialize)]
struct ContractResult<'a> {
    status: &'a str,
    contract: ContractDescriptorDto<'a>,
}

#[derive(Serialize)]
struct ExpectedVersionMismatch<'a> {
    status: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    actual_active_version: Option<String>,
}

#[derive(Serialize)]
struct ExecutionDto<'a> {
    status: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit_sequence: Option<String>,
    contract_version: String,
    plan_hash: LowerHex<'a>,
    outcome_type: &'a str,
    outcome: OutputValue<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provenance_uri: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    durability_mode: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    outcome_uri: Option<&'a str>,
}

impl<'a> ExecutionDto<'a> {
    fn from_response(response: &'a v1::ExecuteCommandResponse) -> Option<Self> {
        use v1::execute_command_response::CompletionStatus;
        let status = match CompletionStatus::try_from(response.status).ok()? {
            CompletionStatus::Committed => "committed",
            CompletionStatus::Replayed => "replayed",
            CompletionStatus::ExecutedReadOnly => "executed_read_only",
            CompletionStatus::Unspecified => return None,
        };
        Some(Self {
            status,
            commit_sequence: (status != "executed_read_only")
                .then(|| response.commit_sequence.to_string()),
            contract_version: response.contract_version.to_string(),
            plan_hash: LowerHex(&response.plan_hash),
            outcome_type: &response.outcome_type,
            outcome: OutputValue(response.outcome.as_ref()?),
            provenance_uri: (!response.provenance_uri.is_empty())
                .then_some(response.provenance_uri.as_str()),
            durability_mode: (!response.durability_mode.is_empty())
                .then_some(response.durability_mode.as_str()),
            outcome_uri: response.outcome_uri.as_deref(),
        })
    }
}

#[derive(Serialize)]
struct FoundExecution<'a> {
    status: &'a str,
    outcome: ExecutionDto<'a>,
}

#[derive(Serialize)]
struct EntityFound<'a> {
    status: &'a str,
    entity_key: PaddedBytes<'a>,
    entity_version: String,
    written_by_contract_version: String,
    fields: OutputRecord<'a>,
}

#[derive(Serialize)]
struct CommitFound<'a> {
    status: &'a str,
    commit: CommitDto<'a>,
}

struct CommitDto<'a>(&'a v1::Commit);

impl Serialize for CommitDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let commit = self.0;
        let actor = commit
            .actor
            .as_ref()
            .ok_or_else(|| S::Error::custom("missing actor"))?;
        let logical_time = commit
            .logical_time
            .as_ref()
            .ok_or_else(|| S::Error::custom("missing logical time"))?;
        let outcome = commit
            .outcome
            .as_ref()
            .ok_or_else(|| S::Error::custom("missing outcome"))?;
        let durability = command_durability(commit.durability)
            .ok_or_else(|| S::Error::custom("invalid durability"))?;
        let mut map = serializer.serialize_map(Some(16))?;
        map.serialize_entry("commit_sequence", &commit.commit_sequence.to_string())?;
        map.serialize_entry(
            "admission_request_id",
            &UuidText(&commit.admission_request_id),
        )?;
        map.serialize_entry("contract_lineage", &commit.contract_lineage)?;
        map.serialize_entry("contract_version", &commit.contract_version.to_string())?;
        map.serialize_entry("command_id", &commit.command_id)?;
        map.serialize_entry("plan_hash", &LowerHex(&commit.plan_hash))?;
        map.serialize_entry(
            "canonical_input_hash",
            &LowerHex(&commit.canonical_input_hash),
        )?;
        map.serialize_entry("actor", &ActorDto(actor))?;
        map.serialize_entry("logical_time", &TimestampDto(logical_time))?;
        map.serialize_entry("partition_hash", &LowerHex(&commit.partition_hash))?;
        map.serialize_entry("conflict_hashes", &Hashes(&commit.conflict_hashes))?;
        map.serialize_entry(
            "affected_entities",
            &AffectedEntities(&commit.affected_entities),
        )?;
        map.serialize_entry("events", &DurableEvents(&commit.events))?;
        map.serialize_entry("outcome", &DeclaredOutcomeDto(outcome))?;
        map.serialize_entry("provenance_uri", &commit.provenance_uri)?;
        map.serialize_entry("durability", durability)?;
        map.end()
    }
}

struct ActorDto<'a>(&'a v1::AdmittedActor);

impl Serialize for ActorDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let actor = self.0;
        let kind = actor_kind(actor.actor_kind).ok_or_else(|| S::Error::custom("actor kind"))?;
        let scope = actor
            .tenant_scope
            .as_ref()
            .ok_or_else(|| S::Error::custom("tenant scope"))?;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("principal_id", &actor.principal_id)?;
        map.serialize_entry("actor_kind", kind)?;
        map.serialize_entry("tenant_scope", &TenantScopeDto(scope))?;
        if let Some(session) = actor.agent_session_id.as_ref() {
            map.serialize_entry("agent_session_id", &UuidText(session))?;
        }
        map.end()
    }
}

struct TenantScopeDto<'a>(&'a v1::TenantScope);

impl Serialize for TenantScopeDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use v1::tenant_scope::Scope;
        let mut map = serializer.serialize_map(None)?;
        match self.0.scope.as_ref() {
            Some(Scope::Global(_)) => map.serialize_entry("type", "global")?,
            Some(Scope::TenantId(id)) => {
                map.serialize_entry("type", "tenant")?;
                map.serialize_entry("tenant_id", id)?;
            }
            None => return Err(S::Error::custom("missing tenant scope")),
        }
        map.end()
    }
}

struct TimestampDto<'a>(&'a v1::Timestamp);

impl Serialize for TimestampDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("seconds", &self.0.seconds.to_string())?;
        map.serialize_entry("nanos", &self.0.nanos)?;
        map.end()
    }
}

struct Hashes<'a>(&'a [Vec<u8>]);

impl Serialize for Hashes<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for hash in self.0 {
            sequence.serialize_element(&LowerHex(hash))?;
        }
        sequence.end()
    }
}

struct AffectedEntities<'a>(&'a [v1::AffectedEntity]);

impl Serialize for AffectedEntities<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for entity in self.0 {
            sequence.serialize_element(&AffectedEntityDto(entity))?;
        }
        sequence.end()
    }
}

struct AffectedEntityDto<'a>(&'a v1::AffectedEntity);

impl Serialize for AffectedEntityDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("entity_key", &PaddedBytes(&self.0.entity_key))?;
        map.serialize_entry("entity_version", &self.0.entity_version.to_string())?;
        map.end()
    }
}

struct DurableEvents<'a>(&'a [v1::DurableEvent]);

impl Serialize for DurableEvents<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for event in self.0 {
            sequence.serialize_element(&DurableEventDto(event))?;
        }
        sequence.end()
    }
}

struct DurableEventDto<'a>(&'a v1::DurableEvent);

impl Serialize for DurableEventDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let event = self.0;
        let id = event
            .event_id
            .as_ref()
            .ok_or_else(|| S::Error::custom("missing event ID"))?;
        let payload = event
            .payload
            .as_ref()
            .ok_or_else(|| S::Error::custom("missing event payload"))?;
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("event_id", &EventIdDto(id))?;
        map.serialize_entry("event_type_id", &event.event_type_id)?;
        map.serialize_entry("payload", &OutputRecord(payload))?;
        map.end()
    }
}

struct EventIdDto<'a>(&'a v1::EventId);

impl Serialize for EventIdDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("commit_sequence", &self.0.commit_sequence.to_string())?;
        map.serialize_entry("event_ordinal", &self.0.event_ordinal)?;
        map.end()
    }
}

struct DeclaredOutcomeDto<'a>(&'a v1::DeclaredOutcome);

impl Serialize for DeclaredOutcomeDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let value = self
            .0
            .value
            .as_ref()
            .ok_or_else(|| S::Error::custom("missing declared outcome"))?;
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("outcome_id", &self.0.outcome_id)?;
        map.serialize_entry("outcome_name", &self.0.outcome_name)?;
        map.serialize_entry("value", &OutputRecord(value))?;
        map.end()
    }
}

fn actor_kind(value: i32) -> Option<&'static str> {
    match v1::ActorKind::try_from(value).ok()? {
        v1::ActorKind::Human => Some("human"),
        v1::ActorKind::Agent => Some("agent"),
        v1::ActorKind::Service => Some("service"),
        v1::ActorKind::Unspecified => None,
    }
}

fn command_durability(value: i32) -> Option<&'static str> {
    match v1::CommandDurability::try_from(value).ok()? {
        v1::CommandDurability::Synchronous => Some("sync"),
        v1::CommandDurability::Group => Some("group"),
        v1::CommandDurability::Unspecified => None,
    }
}

#[derive(Serialize)]
struct ProjectionReady<'a> {
    status: &'a str,
    rows: ProjectionRows<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<PaddedBytes<'a>>,
    observed_fence: ProjectionFence<'a>,
    frontier: Frontier<'a>,
}

struct ProjectionRows<'a>(&'a [v1::ProjectionRow]);

impl Serialize for ProjectionRows<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for row in self.0 {
            sequence.serialize_element(&ProjectionRowDto(row))?;
        }
        sequence.end()
    }
}

struct ProjectionRowDto<'a>(&'a v1::ProjectionRow);

impl Serialize for ProjectionRowDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let values = self
            .0
            .values
            .as_ref()
            .ok_or_else(|| S::Error::custom("missing projection row values"))?;
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("group", &ProjectionGroup(&self.0.group))?;
        map.serialize_entry("values", &OutputRecord(values))?;
        map.end()
    }
}

struct ProjectionGroup<'a>(&'a [v1::Value]);

impl Serialize for ProjectionGroup<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for value in self.0 {
            sequence.serialize_element(&OutputValue(value))?;
        }
        sequence.end()
    }
}

struct ProjectionFence<'a>(&'a v1::ProjectionPageFence);

impl Serialize for ProjectionFence<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let identity = self
            .0
            .identity
            .as_ref()
            .ok_or_else(|| S::Error::custom("missing projection identity"))?;
        let frontier = self
            .0
            .frontier
            .as_ref()
            .ok_or_else(|| S::Error::custom("missing projection fence frontier"))?;
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("identity", &ProjectionIdentityDto(identity))?;
        map.serialize_entry("generation", &self.0.generation.to_string())?;
        map.serialize_entry("frontier", &Frontier(frontier))?;
        map.end()
    }
}

struct ProjectionIdentityDto<'a>(&'a v1::ProjectionIdentity);

impl Serialize for ProjectionIdentityDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("contract_lineage", &self.0.contract_lineage)?;
        map.serialize_entry("projection_id", &self.0.projection_id)?;
        map.serialize_entry(
            "projection_plan_hash",
            &LowerHex(&self.0.projection_plan_hash),
        )?;
        map.end()
    }
}

struct Frontier<'a>(&'a v1::FrontierPosition);

impl Serialize for Frontier<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use v1::frontier_position::Position;
        let mut map = serializer.serialize_map(None)?;
        match self.0.position.as_ref() {
            Some(Position::BeforeFirst(_)) => map.serialize_entry("type", "before_first")?,
            Some(Position::AppliedThrough(sequence)) => {
                map.serialize_entry("type", "applied_through")?;
                map.serialize_entry("sequence", &sequence.to_string())?;
            }
            None => return Err(S::Error::custom("missing frontier")),
        }
        map.end()
    }
}

#[derive(Serialize)]
struct ProjectionWait<'a> {
    status: &'a str,
    required_sequence: String,
    current: Frontier<'a>,
}

#[derive(Serialize)]
struct ProjectionDegraded<'a> {
    status: &'a str,
    current: Frontier<'a>,
    reason: ProjectionReason<'a>,
}

struct ProjectionReason<'a>(&'a v1::ProjectionUnavailableReason);

impl Serialize for ProjectionReason<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use v1::projection_unavailable_reason::Reason;
        let mut map = serializer.serialize_map(None)?;
        match self.0.reason.as_ref() {
            Some(Reason::Building(_)) => map.serialize_entry("type", "building")?,
            Some(Reason::Rebuilding(_)) => map.serialize_entry("type", "rebuilding")?,
            Some(Reason::Failure(code)) => {
                map.serialize_entry("type", "failure")?;
                map.serialize_entry(
                    "code",
                    projection_failure(*code)
                        .ok_or_else(|| S::Error::custom("projection failure"))?,
                )?;
            }
            None => return Err(S::Error::custom("projection unavailable reason")),
        }
        map.end()
    }
}

#[derive(Serialize)]
struct ProjectionInvalid<'a> {
    status: &'a str,
    reason: &'a str,
}

fn projection_failure(value: i32) -> Option<&'static str> {
    match v1::ProjectionFailureCode::try_from(value).ok()? {
        v1::ProjectionFailureCode::ArithmeticOverflow => Some("arithmetic_overflow"),
        v1::ProjectionFailureCode::MalformedDurableEvent => Some("malformed_durable_event"),
        v1::ProjectionFailureCode::MissingCommit => Some("missing_commit"),
        v1::ProjectionFailureCode::PlanOrSchemaUnavailable => Some("plan_or_schema_unavailable"),
        v1::ProjectionFailureCode::ProjectionStateIntegrity => Some("projection_state_integrity"),
        v1::ProjectionFailureCode::HardLimitExceeded => Some("hard_limit_exceeded"),
        v1::ProjectionFailureCode::Unspecified => None,
    }
}

#[derive(Serialize)]
struct BootstrapCreated<'a> {
    status: &'a str,
    transition: CapabilityTransitionDto<'a>,
    bearer_retained: bool,
}

#[derive(Serialize)]
struct NormalCreated<'a> {
    status: &'a str,
    transition: CapabilityTransitionDto<'a>,
    credential_retained: bool,
}

#[derive(Serialize)]
struct AlreadyCreated<'a> {
    status: &'a str,
    identity: CapabilityIdentityDto<'a>,
}

#[derive(Serialize)]
struct RevokeResult<'a> {
    status: &'a str,
    transition: CapabilityTransitionDto<'a>,
}

struct CapabilityTransitionDto<'a>(&'a v1::CapabilityTransition);

impl Serialize for CapabilityTransitionDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let identity = self
            .0
            .identity
            .as_ref()
            .ok_or_else(|| S::Error::custom("missing capability identity"))?;
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("identity", &CapabilityIdentityDto(identity))?;
        map.serialize_entry(
            "administration_sequence",
            &self.0.administration_sequence.to_string(),
        )?;
        map.end()
    }
}

struct CapabilityIdentityDto<'a>(&'a v1::CapabilityIdentity);

impl Serialize for CapabilityIdentityDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("capability_id", &UuidText(&self.0.capability_id))?;
        map.serialize_entry("revision", &self.0.revision.to_string())?;
        map.end()
    }
}

#[derive(Serialize)]
struct MaintenanceOperationDto<'a> {
    status: &'a str,
    maintenance_operation_id: String,
    kind: &'static str,
    backup_name: &'a str,
    input_hash: LowerHex<'a>,
    phase: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<&'static str>,
}

impl<'a> MaintenanceOperationDto<'a> {
    fn new(status: &'a str, operation: &'a v1::OfflineMaintenanceOperation) -> Option<Self> {
        let kind = match v1::OfflineMaintenanceOperationKind::try_from(operation.kind).ok()? {
            v1::OfflineMaintenanceOperationKind::CreateBackup => "create_backup",
            v1::OfflineMaintenanceOperationKind::RestoreBackup => "restore_backup",
            v1::OfflineMaintenanceOperationKind::Unspecified => return None,
        };
        let phase = match v1::OfflineMaintenancePhase::try_from(operation.phase).ok()? {
            v1::OfflineMaintenancePhase::Accepted => "accepted",
            v1::OfflineMaintenancePhase::Draining => "draining",
            v1::OfflineMaintenancePhase::Offline => "offline",
            v1::OfflineMaintenancePhase::ArtifactPublished => "artifact_published",
            v1::OfflineMaintenancePhase::Validating => "validating",
            v1::OfflineMaintenancePhase::Succeeded => "succeeded",
            v1::OfflineMaintenancePhase::FailedClosed => "failed_closed",
            v1::OfflineMaintenancePhase::Unspecified => return None,
        };
        let failure = match v1::OfflineMaintenanceFailureClass::try_from(operation.failure).ok()? {
            v1::OfflineMaintenanceFailureClass::Unspecified => None,
            v1::OfflineMaintenanceFailureClass::QuiescenceFailed => Some("quiescence_failed"),
            v1::OfflineMaintenanceFailureClass::ArtifactUnavailable => Some("artifact_unavailable"),
            v1::OfflineMaintenanceFailureClass::ArtifactInvalid => Some("artifact_invalid"),
            v1::OfflineMaintenanceFailureClass::StagedAuthorizationFailed => {
                Some("staged_authorization_failed")
            }
            v1::OfflineMaintenanceFailureClass::StorageUnavailable => Some("storage_unavailable"),
            v1::OfflineMaintenanceFailureClass::ValidationFailed => Some("validation_failed"),
            v1::OfflineMaintenanceFailureClass::ReceiptUnavailable => Some("receipt_unavailable"),
            v1::OfflineMaintenanceFailureClass::InternalFailure => Some("internal_failure"),
        };
        if operation.input_hash.len() != 32 || (phase == "failed_closed") != failure.is_some() {
            return None;
        }
        Some(Self {
            status,
            maintenance_operation_id: format_uuid(&operation.operation_id)?,
            kind,
            backup_name: &operation.backup_name,
            input_hash: LowerHex(&operation.input_hash),
            phase,
            failure,
        })
    }
}

#[derive(Serialize)]
struct PreBootstrapHealth<'a> {
    status: &'a str,
    lifecycle: &'a str,
    liveness: bool,
    readiness: bool,
}

#[derive(Serialize)]
struct AuthenticatedHealth<'a> {
    status: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_contract_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_commit_sequence: Option<String>,
    components: HealthComponents<'a>,
    started_at: TimestampDto<'a>,
    build: BuildDto<'a>,
}

struct HealthComponents<'a>(&'a [v1::HealthComponent]);

impl Serialize for HealthComponents<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for component in self.0 {
            sequence.serialize_element(&HealthComponentDto(component))?;
        }
        sequence.end()
    }
}

struct HealthComponentDto<'a>(&'a v1::HealthComponent);

impl Serialize for HealthComponentDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let component = match v1::HealthComponentKind::try_from(self.0.component) {
            Ok(v1::HealthComponentKind::AuthoritativeStorage) => "authoritative_storage",
            Ok(v1::HealthComponentKind::Catalog) => "catalog",
            Ok(v1::HealthComponentKind::CommitCoordinator) => "commit_coordinator",
            Ok(v1::HealthComponentKind::Projection) => "projection",
            Ok(v1::HealthComponentKind::Outbox) => "outbox",
            _ => return Err(S::Error::custom("health component")),
        };
        let status = match v1::HealthComponentStatus::try_from(self.0.status) {
            Ok(v1::HealthComponentStatus::Healthy) => "healthy",
            Ok(v1::HealthComponentStatus::Degraded) => "degraded",
            Ok(v1::HealthComponentStatus::Unavailable) => "unavailable",
            _ => return Err(S::Error::custom("health component status")),
        };
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("component", component)?;
        map.serialize_entry("status", status)?;
        map.end()
    }
}

struct BuildDto<'a>(&'a v1::BuildInfo);

impl Serialize for BuildDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let build = self.0;
        let mut map = serializer.serialize_map(Some(7))?;
        map.serialize_entry("semantic_version", &build.semantic_version)?;
        map.serialize_entry("git_revision", &build.git_revision)?;
        map.serialize_entry("rust_version", &build.rust_version)?;
        map.serialize_entry("enabled_features", &build.enabled_features)?;
        map.serialize_entry("storage_format_version", &build.storage_format_version)?;
        map.serialize_entry("contract_ir_version", &build.contract_ir_version)?;
        map.serialize_entry("mcp_protocol_baseline", &build.mcp_protocol_baseline)?;
        map.end()
    }
}

struct ValidationDetails<'a>(&'a [riffdb_client_rust::ValidationIssue]);

impl Serialize for ValidationDetails<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("type", "validation")?;
        map.serialize_entry("issues", &ValidationIssues(self.0))?;
        map.end()
    }
}

struct ValidationIssues<'a>(&'a [riffdb_client_rust::ValidationIssue]);

impl Serialize for ValidationIssues<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for issue in self.0 {
            sequence.serialize_element(&ValidationIssueDto(issue))?;
        }
        sequence.end()
    }
}

struct ValidationIssueDto<'a>(&'a riffdb_client_rust::ValidationIssue);

impl Serialize for ValidationIssueDto<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("code", self.0.code().code())?;
        map.serialize_entry("path", &ValidationPath(self.0.path().segments()))?;
        map.end()
    }
}

struct ValidationPath<'a>(&'a [ValidationPathSegment]);

impl Serialize for ValidationPath<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for segment in self.0 {
            sequence.serialize_element(&ValidationPathItem(segment))?;
        }
        sequence.end()
    }
}

struct ValidationPathItem<'a>(&'a ValidationPathSegment);

impl Serialize for ValidationPathItem<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        match self.0 {
            ValidationPathSegment::Field(field_id) => {
                map.serialize_entry("field_id", &field_id.get())?;
            }
            ValidationPathSegment::ListIndex(index) => {
                map.serialize_entry("list_index", index)?;
            }
        }
        map.end()
    }
}

struct ContractMismatchDetails(u64);

impl Serialize for ContractMismatchDetails {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("type", "contract_mismatch")?;
        map.serialize_entry("active_contract_version", &self.0.to_string())?;
        map.end()
    }
}

struct ExecutionFailureDetails(&'static str);

impl Serialize for ExecutionFailureDetails {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("type", "command_execution_failed")?;
        map.serialize_entry("code", self.0)?;
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::path::PathBuf;

    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use riffdb_client_rust::PublicError;
    use serde::Serialize;
    use serde_json::Value as JsonValue;

    use super::*;
    use crate::value::parse_uuid;

    #[derive(Serialize)]
    struct Status<'a> {
        status: &'a str,
    }

    const RESULT_FIXTURES: &[&str] = &[
        "contract.validate.valid.jsonl",
        "contract.validate.invalid_syntax.jsonl",
        "contract.validate.invalid_semantic.jsonl",
        "contract.deploy.activated.jsonl",
        "contract.deploy.already_active.jsonl",
        "contract.deploy.expected_version_mismatch_absent.jsonl",
        "contract.deploy.expected_version_mismatch_present.jsonl",
        "contract.deploy.bundle_conflict.jsonl",
        "command.execute.committed.jsonl",
        "command.execute.replayed.jsonl",
        "command.execute.executed_read_only.jsonl",
        "command.outcome.not_found.jsonl",
        "command.outcome.found.jsonl",
        "entity.get.not_found.jsonl",
        "entity.get.found.jsonl",
        "commit.show.not_found.jsonl",
        "commit.show.found.jsonl",
        "projection.query.ready_no_cursor.jsonl",
        "projection.query.ready_with_cursor.jsonl",
        "projection.query.wait_timed_out.jsonl",
        "projection.query.degraded_building.jsonl",
        "projection.query.degraded_rebuilding.jsonl",
        "projection.query.degraded_failure.jsonl",
        "projection.query.invalid.jsonl",
        "capability.bootstrap.created.jsonl",
        "capability.bootstrap.replayed.jsonl",
        "capability.bootstrap.bootstrap_conflict.jsonl",
        "capability.create.created.jsonl",
        "capability.create.already_created_token_unavailable.jsonl",
        "capability.create.capability_id_conflict.jsonl",
        "capability.revoke.revoked.jsonl",
        "capability.revoke.already_revoked.jsonl",
        "capability.revoke.capability_not_found.jsonl",
        "server.health.initializing_validation.jsonl",
        "server.health.initializing_bootstrap.jsonl",
        "server.health.ready.jsonl",
        "server.health.not_ready.jsonl",
        "server.health.degraded.jsonl",
        "backup.create.accepted.jsonl",
        "backup.restore.terminal.jsonl",
        "backup.operation.not_found.jsonl",
        "backup.operation.found.jsonl",
        "demo.budget.sequential.jsonl",
        "demo.budget.contention.jsonl",
        "demo.budget.same_key_replay.jsonl",
    ];

    #[test]
    fn every_result_branch_matches_its_accepted_golden() {
        for name in RESULT_FIXTURES {
            let (bytes, document) = fixture(name);
            let terminal = result_terminal(name, &document["result"]);
            assert_eq!(
                terminal.json_bytes().expect("result renders"),
                bytes,
                "{name}"
            );
        }
    }

    #[test]
    fn every_error_and_emergency_branch_is_byte_bound_to_the_checkpoint() {
        let root = fixture_root();
        let mut covered = RESULT_FIXTURES
            .iter()
            .map(|name| (*name).to_owned())
            .collect::<BTreeSet<_>>();
        for entry in fs::read_dir(&root).expect("fixture directory") {
            let entry = entry.expect("fixture entry");
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == "manifest.tsv" || covered.contains(&name) {
                continue;
            }
            let bytes = fs::read(entry.path()).expect("fixture bytes");
            if name.ends_with(".stderr") {
                assert!(
                    bytes == OUTPUT_TOO_LARGE || bytes == OUTPUT_RENDER_FAILED,
                    "{name}"
                );
                covered.insert(name);
                continue;
            }
            let document: JsonValue = serde_json::from_slice(&bytes).expect("fixture JSON");
            assert_eq!(document["ok"], false, "{name}");
            let command = command_from_text(document["command"].as_str().expect("command"));
            let error = &document["error"];
            let exit = error_exit(error);
            let terminal = Terminal::new(
                command,
                false,
                &OrderedJson(error),
                format!(
                    "{}: {}: {}",
                    command.as_str(),
                    error["code"].as_str().expect("error code"),
                    error["message"].as_str().expect("safe message")
                ),
                true,
                exit,
            );
            assert_eq!(
                terminal.json_bytes().expect("error renders"),
                bytes,
                "{name}"
            );
            covered.insert(name);
        }
        assert_eq!(covered.len(), 98);
    }

    #[test]
    fn credential_canaries_never_enter_machine_human_or_debug_output() {
        const TOKEN: &str = "SECRET_TOKEN_CANARY_012345678901234567890123";
        const DOCUMENT: &str = "SECRET_BOOTSTRAP_DOCUMENT_CANARY";
        let terminals = [
            local_error(
                CommandIdentity::CapabilityBootstrap,
                "bootstrap_credential_invalid",
                "bootstrap credential is invalid",
            ),
            local_error(
                CommandIdentity::CapabilityCreate,
                "credential_retention_failed",
                "credential retention failed",
            ),
            client_error(
                CommandIdentity::ServerHealth,
                &ClientError::DetailsFree(DetailsFreeStatus::Unauthenticated),
            ),
        ];
        for terminal in terminals {
            let debug = format!(
                "{} {:?} {:?}",
                terminal.command.as_str(),
                terminal.json,
                terminal.human
            );
            let mut json_stdout = Vec::new();
            let mut json_stderr = Vec::new();
            let _ = terminal.emit(OutputMode::Json, &mut json_stdout, &mut json_stderr);
            for output in [debug.as_bytes(), &json_stdout, &json_stderr] {
                assert!(!contains(output, TOKEN.as_bytes()));
                assert!(!contains(output, DOCUMENT.as_bytes()));
            }
        }
    }

    fn result_terminal(name: &str, result: &JsonValue) -> Terminal {
        let status = result["status"].as_str().expect("result status");
        if name.starts_with("contract.validate.") {
            use v1::compilation_diagnostics::Diagnostics;
            use v1::validate_contract_response::Result;
            let result = match status {
                "valid" => Result::Valid(v1::Unit {}),
                "invalid_syntax" => Result::Invalid(v1::CompilationDiagnostics {
                    diagnostics: Some(Diagnostics::Syntax(v1::SyntaxDiagnosticList {
                        diagnostics: result["diagnostics"]
                            .as_array()
                            .expect("syntax diagnostics")
                            .iter()
                            .map(|diagnostic| v1::SyntaxDiagnostic {
                                code: text(diagnostic, "code"),
                                summary: text(diagnostic, "summary"),
                                help: optional_text(diagnostic, "help"),
                                span: Some(span(&diagnostic["span"])),
                                expected: diagnostic["expected"]
                                    .as_array()
                                    .expect("expected tokens")
                                    .iter()
                                    .map(|value| value.as_str().expect("token").to_owned())
                                    .collect(),
                            })
                            .collect(),
                    })),
                }),
                "invalid_semantic" => Result::Invalid(v1::CompilationDiagnostics {
                    diagnostics: Some(Diagnostics::Semantic(v1::SemanticDiagnosticList {
                        diagnostics: result["diagnostics"]
                            .as_array()
                            .expect("semantic diagnostics")
                            .iter()
                            .map(|diagnostic| v1::SemanticDiagnostic {
                                code: text(diagnostic, "code"),
                                summary: text(diagnostic, "summary"),
                                help: optional_text(diagnostic, "help"),
                                primary_span: Some(span(&diagnostic["primary_span"])),
                                related_span: diagnostic.get("related_span").map(span),
                            })
                            .collect(),
                    })),
                }),
                _ => panic!("unexpected validation status {status}"),
            };
            return render_contract_validation(&v1::ValidateContractResponse {
                result: Some(result),
            });
        }
        if name.starts_with("contract.deploy.") {
            use v1::deploy_contract_response::Result;
            let result = match status {
                "activated" => Result::Activated(contract_descriptor(&result["contract"])),
                "already_active" => Result::AlreadyActive(contract_descriptor(&result["contract"])),
                "expected_version_mismatch" => {
                    Result::ExpectedActiveVersionMismatch(v1::ExpectedActiveVersionMismatch {
                        actual_active_version: optional_u64_string(result, "actual_active_version"),
                    })
                }
                "bundle_conflict" => Result::BundleConflict(v1::Unit {}),
                _ => panic!("unexpected deployment status {status}"),
            };
            return render_contract_deploy(&v1::DeployContractResponse {
                result: Some(result),
            });
        }
        if name.starts_with("command.execute.") {
            return render_execution(
                CommandIdentity::CommandExecute,
                &execution_from_json(result),
            );
        }
        if name.starts_with("command.outcome.") {
            let result = match status {
                "not_found" => v1::get_outcome_response::Result::NotFound(v1::Unit {}),
                "found" => {
                    v1::get_outcome_response::Result::Found(execution_from_json(&result["outcome"]))
                }
                _ => panic!("unexpected outcome status {status}"),
            };
            return render_outcome(&v1::GetOutcomeResponse {
                result: Some(result),
            });
        }
        if name.starts_with("entity.get.") {
            let result = match status {
                "not_found" => v1::get_entity_response::Result::NotFound(v1::Unit {}),
                "found" => v1::get_entity_response::Result::Found(v1::Entity {
                    entity_key: padded_bytes(result, "entity_key"),
                    entity_version: u64_string(result, "entity_version"),
                    written_by_contract_version: u64_string(result, "written_by_contract_version"),
                    fields: Some(record(&result["fields"])),
                }),
                _ => panic!("unexpected entity status {status}"),
            };
            return render_entity(&v1::GetEntityResponse {
                result: Some(result),
            });
        }
        if name.starts_with("commit.show.") {
            let result = match status {
                "not_found" => v1::get_commit_response::Result::NotFound(v1::Unit {}),
                "found" => v1::get_commit_response::Result::Found(commit(&result["commit"])),
                _ => panic!("unexpected commit status {status}"),
            };
            return render_commit(&v1::GetCommitResponse {
                result: Some(result),
            });
        }
        if name.starts_with("projection.query.") {
            return render_projection(&projection_response(result));
        }
        if name.starts_with("capability.bootstrap.") {
            use v1::bootstrap_create_capability_result::Result as BootstrapResult;
            let bearer_retained = result["bearer_retained"].as_bool().unwrap_or(false);
            let bootstrap_result = match status {
                "created" => BootstrapResult::Created(transition(&result["transition"])),
                "replayed" => BootstrapResult::Replayed(transition(&result["transition"])),
                "bootstrap_conflict" => BootstrapResult::BootstrapConflict(v1::Unit {}),
                _ => panic!("unexpected bootstrap status {status}"),
            };
            return render_bootstrap(
                &v1::CreateCapabilityResponse {
                    result: Some(v1::create_capability_response::Result::Bootstrap(
                        v1::BootstrapCreateCapabilityResult {
                            result: Some(bootstrap_result),
                        },
                    )),
                },
                bearer_retained,
            );
        }
        if name.starts_with("capability.create.") {
            let disposition = match status {
                "created" => NormalCreateDisposition::Created(transition(&result["transition"])),
                "already_created_token_unavailable" => {
                    NormalCreateDisposition::AlreadyCreated(identity(&result["identity"]))
                }
                "capability_id_conflict" => NormalCreateDisposition::Conflict,
                _ => panic!("unexpected capability-create status {status}"),
            };
            return render_normal_create(&disposition);
        }
        if name.starts_with("capability.revoke.") {
            use v1::revoke_capability_response::Result;
            let result = match status {
                "revoked" => Result::Revoked(transition(&result["transition"])),
                "already_revoked" => Result::AlreadyRevoked(transition(&result["transition"])),
                "capability_not_found" => Result::CapabilityNotFound(v1::Unit {}),
                _ => panic!("unexpected revoke status {status}"),
            };
            return render_revoke(&v1::RevokeCapabilityResponse {
                result: Some(result),
            });
        }
        if name.starts_with("server.health.") {
            return render_health(&health_response(result));
        }
        if name.starts_with("backup.create.") {
            return render_create_maintenance_start(&v1::CreateOfflineBackupResponse {
                disposition: maintenance_disposition(status),
                operation: Some(maintenance_operation(result)),
            });
        }
        if name.starts_with("backup.restore.") {
            return render_restore_maintenance_start(&v1::RestoreOfflineBackupResponse {
                disposition: maintenance_disposition(status),
                operation: Some(maintenance_operation(result)),
            });
        }
        if name.starts_with("backup.operation.") {
            let result = match status {
                "not_found" => {
                    v1::get_offline_maintenance_operation_response::Result::NotFound(v1::Unit {})
                }
                "found" => v1::get_offline_maintenance_operation_response::Result::Found(
                    maintenance_operation(result),
                ),
                _ => panic!("unexpected maintenance operation status {status}"),
            };
            return render_maintenance_operation(&v1::GetOfflineMaintenanceOperationResponse {
                result: Some(result),
            });
        }
        if name.starts_with("demo.budget.") {
            return success(
                CommandIdentity::DemoBudget,
                "passed",
                &DemoResultFixture {
                    status: "passed",
                    adapter: result["adapter"].as_str().expect("adapter"),
                    case: result["case"].as_str().expect("case"),
                    workload_version: result["workload_version"]
                        .as_u64()
                        .expect("workload version") as u32,
                },
            );
        }
        panic!("unregistered result fixture {name}");
    }

    #[derive(Serialize)]
    struct DemoResultFixture<'a> {
        status: &'a str,
        adapter: &'a str,
        case: &'a str,
        workload_version: u32,
    }

    struct OrderedJson<'a>(&'a JsonValue);

    impl Serialize for OrderedJson<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            match self.0 {
                JsonValue::Null => serializer.serialize_unit(),
                JsonValue::Bool(value) => serializer.serialize_bool(*value),
                JsonValue::Number(value) => {
                    if let Some(value) = value.as_u64() {
                        serializer.serialize_u64(value)
                    } else if let Some(value) = value.as_i64() {
                        serializer.serialize_i64(value)
                    } else if let Some(value) = value.as_f64() {
                        serializer.serialize_f64(value)
                    } else {
                        Err(S::Error::custom("invalid JSON number"))
                    }
                }
                JsonValue::String(value) => serializer.serialize_str(value),
                JsonValue::Array(values) => {
                    let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                    for value in values {
                        sequence.serialize_element(&OrderedJson(value))?;
                    }
                    sequence.end()
                }
                JsonValue::Object(values) => {
                    let mut fields = values.iter().collect::<Vec<_>>();
                    fields.sort_by(|(left, _), (right, _)| {
                        error_key_order(left)
                            .cmp(&error_key_order(right))
                            .then_with(|| left.cmp(right))
                    });
                    let mut map = serializer.serialize_map(Some(fields.len()))?;
                    for (key, value) in fields {
                        map.serialize_entry(key, &OrderedJson(value))?;
                    }
                    map.end()
                }
            }
        }
    }

    fn error_key_order(key: &str) -> u8 {
        match key {
            "type" => 0,
            "code" => 1,
            "message" => 2,
            "recovery_action" => 3,
            "details" => 4,
            "incident_id" => 5,
            "capability_id" => 6,
            "case" => 7,
            "stream" => 8,
            "issues" => 9,
            "path" => 10,
            "field_id" => 11,
            "list_index" => 12,
            "active_contract_version" => 13,
            _ => 100,
        }
    }

    fn fixture(name: &str) -> (Vec<u8>, JsonValue) {
        let bytes = fs::read(fixture_root().join(name)).expect("fixture bytes");
        let document = serde_json::from_slice(&bytes).expect("fixture JSON");
        (bytes, document)
    }

    fn fixture_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/output-v1")
    }

    fn command_from_text(command: &str) -> CommandIdentity {
        match command {
            "contract.validate" => CommandIdentity::ContractValidate,
            "contract.deploy" => CommandIdentity::ContractDeploy,
            "command.execute" => CommandIdentity::CommandExecute,
            "command.outcome" => CommandIdentity::CommandOutcome,
            "entity.get" => CommandIdentity::EntityGet,
            "commit.show" => CommandIdentity::CommitShow,
            "projection.query" => CommandIdentity::ProjectionQuery,
            "capability.bootstrap" => CommandIdentity::CapabilityBootstrap,
            "capability.create" => CommandIdentity::CapabilityCreate,
            "capability.revoke" => CommandIdentity::CapabilityRevoke,
            "server.health" => CommandIdentity::ServerHealth,
            "backup.create" => CommandIdentity::BackupCreate,
            "backup.restore" => CommandIdentity::BackupRestore,
            "backup.operation" => CommandIdentity::BackupOperation,
            "demo.budget" => CommandIdentity::DemoBudget,
            _ => panic!("unknown command {command}"),
        }
    }

    fn error_exit(error: &JsonValue) -> u8 {
        let kind = error["type"].as_str().expect("error type");
        let code = error["code"].as_str().expect("error code");
        if kind == "uncertain" || code == "outcome_unknown" {
            3
        } else if kind == "public" || kind == "client" || code == "demo_failed" {
            1
        } else {
            2
        }
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|candidate| candidate == needle)
    }

    fn text(value: &JsonValue, key: &str) -> String {
        value[key].as_str().expect(key).to_owned()
    }

    fn optional_text(value: &JsonValue, key: &str) -> Option<String> {
        value
            .get(key)
            .and_then(JsonValue::as_str)
            .map(str::to_owned)
    }

    fn u32_number(value: &JsonValue, key: &str) -> u32 {
        u32::try_from(value[key].as_u64().expect(key)).expect("u32")
    }

    fn u64_string(value: &JsonValue, key: &str) -> u64 {
        value[key].as_str().expect(key).parse().expect("u64")
    }

    fn optional_u64_string(value: &JsonValue, key: &str) -> Option<u64> {
        value
            .get(key)
            .and_then(JsonValue::as_str)
            .map(|number| number.parse().expect("optional u64"))
    }

    fn padded_bytes(value: &JsonValue, key: &str) -> Vec<u8> {
        STANDARD
            .decode(value[key].as_str().expect(key))
            .expect("base64")
    }

    fn hex_bytes(value: &JsonValue, key: &str) -> Vec<u8> {
        decode_hex(value[key].as_str().expect(key))
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        assert_eq!(value.len() % 2, 0);
        (0..value.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&value[index..index + 2], 16).expect("hex byte"))
            .collect()
    }

    fn uuid_bytes(value: &JsonValue, key: &str) -> Vec<u8> {
        parse_uuid(value[key].as_str().expect(key))
            .expect("UUID")
            .to_vec()
    }

    fn maintenance_disposition(value: &str) -> i32 {
        match value {
            "accepted" => v1::OfflineMaintenanceStartDisposition::Accepted as i32,
            "already_accepted" => v1::OfflineMaintenanceStartDisposition::AlreadyAccepted as i32,
            "terminal" => v1::OfflineMaintenanceStartDisposition::Terminal as i32,
            _ => panic!("unknown maintenance disposition {value}"),
        }
    }

    fn maintenance_operation(value: &JsonValue) -> v1::OfflineMaintenanceOperation {
        let kind = match value["kind"].as_str().expect("maintenance kind") {
            "create_backup" => v1::OfflineMaintenanceOperationKind::CreateBackup,
            "restore_backup" => v1::OfflineMaintenanceOperationKind::RestoreBackup,
            other => panic!("unknown maintenance kind {other}"),
        };
        let phase = match value["phase"].as_str().expect("maintenance phase") {
            "accepted" => v1::OfflineMaintenancePhase::Accepted,
            "draining" => v1::OfflineMaintenancePhase::Draining,
            "offline" => v1::OfflineMaintenancePhase::Offline,
            "artifact_published" => v1::OfflineMaintenancePhase::ArtifactPublished,
            "validating" => v1::OfflineMaintenancePhase::Validating,
            "succeeded" => v1::OfflineMaintenancePhase::Succeeded,
            "failed_closed" => v1::OfflineMaintenancePhase::FailedClosed,
            other => panic!("unknown maintenance phase {other}"),
        };
        let failure = match value.get("failure").and_then(JsonValue::as_str) {
            None => v1::OfflineMaintenanceFailureClass::Unspecified,
            Some("validation_failed") => v1::OfflineMaintenanceFailureClass::ValidationFailed,
            Some(other) => panic!("unknown maintenance failure {other}"),
        };
        v1::OfflineMaintenanceOperation {
            operation_id: uuid_bytes(value, "maintenance_operation_id"),
            kind: kind as i32,
            backup_name: text(value, "backup_name"),
            input_hash: hex_bytes(value, "input_hash"),
            phase: phase as i32,
            failure: failure as i32,
        }
    }

    fn span(value: &JsonValue) -> v1::SourceSpan {
        v1::SourceSpan {
            start: u32_number(value, "start"),
            end: u32_number(value, "end"),
        }
    }

    fn contract_descriptor(value: &JsonValue) -> v1::ContractDescriptor {
        v1::ContractDescriptor {
            contract_lineage: text(value, "contract_lineage"),
            contract_version: u64_string(value, "contract_version"),
            bundle_hash: hex_bytes(value, "bundle_hash"),
            source_hash: hex_bytes(value, "source_hash"),
            plan_root_hash: hex_bytes(value, "plan_root_hash"),
            compatibility: None,
        }
    }

    fn value(value: &JsonValue) -> v1::Value {
        let kind = match value["type"].as_str().expect("value type") {
            "null" => v1::value::Kind::NullValue(v1::NullValue::NullValue as i32),
            "bool" => v1::value::Kind::BoolValue(value["value"].as_bool().expect("bool")),
            "i64" => v1::value::Kind::I64Value(
                value["value"].as_str().expect("i64").parse().expect("i64"),
            ),
            "u64" => v1::value::Kind::U64Value(
                value["value"].as_str().expect("u64").parse().expect("u64"),
            ),
            "decimal" => v1::value::Kind::DecimalValue(decimal(value)),
            "money" => v1::value::Kind::MoneyValue(v1::Money {
                currency: text(value, "currency"),
                amount: Some(decimal(&value["amount"])),
            }),
            "string" => v1::value::Kind::StringValue(text(value, "value")),
            "bytes" => v1::value::Kind::BytesValue(padded_bytes(value, "value")),
            "uuid" => v1::value::Kind::UuidValue(uuid_bytes(value, "value")),
            "date" => v1::value::Kind::DateValue(v1::Date {
                days_since_unix_epoch: i32::try_from(
                    value["days_since_unix_epoch"].as_i64().expect("date"),
                )
                .expect("i32 date"),
            }),
            "timestamp" => v1::value::Kind::TimestampValue(timestamp(value)),
            "enum" => v1::value::Kind::EnumValue(v1::EnumValue {
                type_id: u32_number(value, "type_id"),
                variant_id: u32_number(value, "variant_id"),
                name: optional_text(value, "name").unwrap_or_default(),
            }),
            "list" => v1::value::Kind::ListValue(v1::ValueList {
                values: value["values"]
                    .as_array()
                    .expect("list")
                    .iter()
                    .map(self::value)
                    .collect(),
            }),
            "record" => v1::value::Kind::RecordValue(record(value)),
            unknown => panic!("unknown value type {unknown}"),
        };
        v1::Value { kind: Some(kind) }
    }

    fn decimal(value: &JsonValue) -> v1::Decimal {
        v1::Decimal {
            coefficient_twos_complement: padded_bytes(value, "coefficient_twos_complement"),
            scale: u32_number(value, "scale"),
            precision: None,
        }
    }

    fn timestamp(value: &JsonValue) -> v1::Timestamp {
        v1::Timestamp {
            seconds: value["seconds"]
                .as_str()
                .expect("timestamp seconds")
                .parse()
                .expect("i64 seconds"),
            nanos: u32_number(value, "nanos"),
        }
    }

    fn record(value: &JsonValue) -> v1::ValueRecord {
        v1::ValueRecord {
            fields: value["fields"]
                .as_array()
                .expect("record fields")
                .iter()
                .map(|field| v1::ValueField {
                    field_id: Some(u32_number(field, "field_id")),
                    name: String::new(),
                    value: Some(self::value(&field["value"])),
                })
                .collect(),
        }
    }

    fn execution_from_json(value: &JsonValue) -> v1::ExecuteCommandResponse {
        let status = match value["status"].as_str().expect("execution status") {
            "committed" => v1::execute_command_response::CompletionStatus::Committed,
            "replayed" => v1::execute_command_response::CompletionStatus::Replayed,
            "executed_read_only" => {
                v1::execute_command_response::CompletionStatus::ExecutedReadOnly
            }
            unknown => panic!("unknown execution status {unknown}"),
        };
        v1::ExecuteCommandResponse {
            status: status as i32,
            commit_sequence: optional_u64_string(value, "commit_sequence").unwrap_or(0),
            contract_version: u64_string(value, "contract_version"),
            plan_hash: hex_bytes(value, "plan_hash"),
            outcome_type: text(value, "outcome_type"),
            outcome: Some(self::value(&value["outcome"])),
            provenance_uri: optional_text(value, "provenance_uri").unwrap_or_default(),
            durability_mode: optional_text(value, "durability_mode").unwrap_or_default(),
            outcome_uri: optional_text(value, "outcome_uri"),
        }
    }

    fn commit(value: &JsonValue) -> v1::Commit {
        let actor = &value["actor"];
        let tenant_scope = match actor["tenant_scope"]["type"]
            .as_str()
            .expect("tenant scope")
        {
            "global" => v1::tenant_scope::Scope::Global(v1::Unit {}),
            "tenant" => {
                v1::tenant_scope::Scope::TenantId(text(&actor["tenant_scope"], "tenant_id"))
            }
            unknown => panic!("unknown tenant scope {unknown}"),
        };
        let actor_kind = match actor["actor_kind"].as_str().expect("actor kind") {
            "human" => v1::ActorKind::Human,
            "agent" => v1::ActorKind::Agent,
            "service" => v1::ActorKind::Service,
            unknown => panic!("unknown actor kind {unknown}"),
        };
        let durability = match value["durability"].as_str().expect("durability") {
            "sync" => v1::CommandDurability::Synchronous,
            "group" => v1::CommandDurability::Group,
            unknown => panic!("unknown durability {unknown}"),
        };
        v1::Commit {
            commit_sequence: u64_string(value, "commit_sequence"),
            admission_request_id: uuid_bytes(value, "admission_request_id"),
            contract_lineage: text(value, "contract_lineage"),
            contract_version: u64_string(value, "contract_version"),
            command_id: u32_number(value, "command_id"),
            plan_hash: hex_bytes(value, "plan_hash"),
            canonical_input_hash: hex_bytes(value, "canonical_input_hash"),
            actor: Some(v1::AdmittedActor {
                principal_id: text(actor, "principal_id"),
                actor_kind: actor_kind as i32,
                tenant_scope: Some(v1::TenantScope {
                    scope: Some(tenant_scope),
                }),
                agent_session_id: actor
                    .get("agent_session_id")
                    .map(|_| uuid_bytes(actor, "agent_session_id")),
            }),
            logical_time: Some(timestamp(&value["logical_time"])),
            partition_hash: hex_bytes(value, "partition_hash"),
            conflict_hashes: value["conflict_hashes"]
                .as_array()
                .expect("conflict hashes")
                .iter()
                .map(|hash| decode_hex(hash.as_str().expect("hash")))
                .collect(),
            affected_entities: value["affected_entities"]
                .as_array()
                .expect("affected entities")
                .iter()
                .map(|entity| v1::AffectedEntity {
                    entity_key: padded_bytes(entity, "entity_key"),
                    entity_version: u64_string(entity, "entity_version"),
                })
                .collect(),
            events: value["events"]
                .as_array()
                .expect("events")
                .iter()
                .map(|event| v1::DurableEvent {
                    event_id: Some(v1::EventId {
                        commit_sequence: u64_string(&event["event_id"], "commit_sequence"),
                        event_ordinal: u32_number(&event["event_id"], "event_ordinal"),
                    }),
                    event_type_id: u32_number(event, "event_type_id"),
                    payload: Some(record(&event["payload"])),
                })
                .collect(),
            outcome: Some(v1::DeclaredOutcome {
                outcome_id: u32_number(&value["outcome"], "outcome_id"),
                outcome_name: text(&value["outcome"], "outcome_name"),
                value: Some(record(&value["outcome"]["value"])),
            }),
            provenance_uri: text(value, "provenance_uri"),
            durability: durability as i32,
        }
    }

    fn projection_response(value: &JsonValue) -> v1::QueryProjectionResponse {
        use v1::query_projection_response::Result;
        let result = match value["status"].as_str().expect("projection status") {
            "ready" => Result::Ready(v1::QueryProjectionReady {
                data: Some(v1::ProjectionPage {
                    items: value["rows"]
                        .as_array()
                        .expect("projection rows")
                        .iter()
                        .map(|row| v1::ProjectionRow {
                            group: row["group"]
                                .as_array()
                                .expect("projection group")
                                .iter()
                                .map(self::value)
                                .collect(),
                            values: Some(record(&row["values"])),
                        })
                        .collect(),
                    next_cursor: value
                        .get("next_cursor")
                        .map(|_| padded_bytes(value, "next_cursor")),
                    observed_fence: Some(projection_fence(&value["observed_fence"])),
                }),
                frontier: Some(frontier(&value["frontier"])),
            }),
            "wait_timed_out" => Result::WaitTimedOut(v1::QueryProjectionWaitTimedOut {
                required_sequence: u64_string(value, "required_sequence"),
                current: Some(frontier(&value["current"])),
            }),
            "degraded" => Result::Degraded(v1::QueryProjectionDegraded {
                current: Some(frontier(&value["current"])),
                reason: Some(projection_reason(&value["reason"])),
            }),
            "invalid" => Result::Invalid(v1::QueryProjectionInvalid {
                reason: projection_failure(value["reason"].as_str().expect("projection failure"))
                    as i32,
            }),
            unknown => panic!("unknown projection status {unknown}"),
        };
        v1::QueryProjectionResponse {
            result: Some(result),
        }
    }

    fn projection_fence(value: &JsonValue) -> v1::ProjectionPageFence {
        let identity = &value["identity"];
        v1::ProjectionPageFence {
            identity: Some(v1::ProjectionIdentity {
                contract_lineage: text(identity, "contract_lineage"),
                projection_id: u32_number(identity, "projection_id"),
                projection_plan_hash: hex_bytes(identity, "projection_plan_hash"),
            }),
            generation: u64_string(value, "generation"),
            frontier: Some(frontier(&value["frontier"])),
        }
    }

    fn frontier(value: &JsonValue) -> v1::FrontierPosition {
        let position = match value["type"].as_str().expect("frontier type") {
            "before_first" => v1::frontier_position::Position::BeforeFirst(v1::Unit {}),
            "applied_through" => {
                v1::frontier_position::Position::AppliedThrough(u64_string(value, "sequence"))
            }
            unknown => panic!("unknown frontier {unknown}"),
        };
        v1::FrontierPosition {
            position: Some(position),
        }
    }

    fn projection_reason(value: &JsonValue) -> v1::ProjectionUnavailableReason {
        let reason = match value["type"].as_str().expect("projection reason") {
            "building" => v1::projection_unavailable_reason::Reason::Building(v1::Unit {}),
            "rebuilding" => v1::projection_unavailable_reason::Reason::Rebuilding(v1::Unit {}),
            "failure" => v1::projection_unavailable_reason::Reason::Failure(projection_failure(
                value["code"].as_str().expect("failure code"),
            ) as i32),
            unknown => panic!("unknown projection reason {unknown}"),
        };
        v1::ProjectionUnavailableReason {
            reason: Some(reason),
        }
    }

    fn projection_failure(value: &str) -> v1::ProjectionFailureCode {
        match value {
            "arithmetic_overflow" => v1::ProjectionFailureCode::ArithmeticOverflow,
            "malformed_durable_event" => v1::ProjectionFailureCode::MalformedDurableEvent,
            "missing_commit" => v1::ProjectionFailureCode::MissingCommit,
            "plan_or_schema_unavailable" => v1::ProjectionFailureCode::PlanOrSchemaUnavailable,
            "projection_state_integrity" => v1::ProjectionFailureCode::ProjectionStateIntegrity,
            "hard_limit_exceeded" => v1::ProjectionFailureCode::HardLimitExceeded,
            unknown => panic!("unknown projection failure {unknown}"),
        }
    }

    fn identity(value: &JsonValue) -> v1::CapabilityIdentity {
        v1::CapabilityIdentity {
            capability_id: uuid_bytes(value, "capability_id"),
            revision: u64_string(value, "revision"),
        }
    }

    fn transition(value: &JsonValue) -> v1::CapabilityTransition {
        v1::CapabilityTransition {
            identity: Some(identity(&value["identity"])),
            administration_sequence: u64_string(value, "administration_sequence"),
        }
    }

    fn health_response(value: &JsonValue) -> v1::HealthResponse {
        let result = if value["status"] == "pre_bootstrap" {
            let lifecycle = match value["lifecycle"].as_str().expect("lifecycle") {
                "initializing_validation" => v1::PreBootstrapLifecycle::InitializingValidation,
                "initializing_bootstrap" => v1::PreBootstrapLifecycle::InitializingBootstrap,
                unknown => panic!("unknown lifecycle {unknown}"),
            };
            v1::health_response::Result::PreBootstrap(v1::PreBootstrapHealth {
                lifecycle: lifecycle as i32,
                liveness: value["liveness"].as_bool().expect("liveness"),
                readiness: value["readiness"].as_bool().expect("readiness"),
            })
        } else {
            let status = match value["status"].as_str().expect("health status") {
                "ready" => v1::HealthStatus::Ready,
                "not_ready" => v1::HealthStatus::NotReady,
                "degraded" => v1::HealthStatus::Degraded,
                unknown => panic!("unknown health status {unknown}"),
            };
            v1::health_response::Result::Authenticated(v1::AuthenticatedHealth {
                status: status as i32,
                active_contract_version: optional_u64_string(value, "active_contract_version"),
                last_commit_sequence: optional_u64_string(value, "last_commit_sequence"),
                components: value["components"]
                    .as_array()
                    .expect("health components")
                    .iter()
                    .map(health_component)
                    .collect(),
                started_at: Some(timestamp(&value["started_at"])),
                build: Some(build(&value["build"])),
            })
        };
        v1::HealthResponse {
            result: Some(result),
        }
    }

    fn health_component(value: &JsonValue) -> v1::HealthComponent {
        let component = match value["component"].as_str().expect("component") {
            "authoritative_storage" => v1::HealthComponentKind::AuthoritativeStorage,
            "catalog" => v1::HealthComponentKind::Catalog,
            "commit_coordinator" => v1::HealthComponentKind::CommitCoordinator,
            "projection" => v1::HealthComponentKind::Projection,
            "outbox" => v1::HealthComponentKind::Outbox,
            unknown => panic!("unknown health component {unknown}"),
        };
        let status = match value["status"].as_str().expect("component status") {
            "healthy" => v1::HealthComponentStatus::Healthy,
            "degraded" => v1::HealthComponentStatus::Degraded,
            "unavailable" => v1::HealthComponentStatus::Unavailable,
            unknown => panic!("unknown component status {unknown}"),
        };
        v1::HealthComponent {
            component: component as i32,
            status: status as i32,
        }
    }

    fn build(value: &JsonValue) -> v1::BuildInfo {
        v1::BuildInfo {
            semantic_version: text(value, "semantic_version"),
            git_revision: text(value, "git_revision"),
            rust_version: text(value, "rust_version"),
            enabled_features: value["enabled_features"]
                .as_array()
                .expect("features")
                .iter()
                .map(|feature| feature.as_str().expect("feature").to_owned())
                .collect(),
            storage_format_version: u32_number(value, "storage_format_version"),
            contract_ir_version: u32_number(value, "contract_ir_version"),
            mcp_protocol_baseline: text(value, "mcp_protocol_baseline"),
        }
    }

    #[test]
    fn envelope_order_and_final_lf_are_exact() {
        let terminal = success(
            CommandIdentity::ContractValidate,
            "valid",
            &Status { status: "valid" },
        );
        assert_eq!(
            terminal.json_bytes().expect("JSON"),
            include_bytes!("../fixtures/output-v1/contract.validate.valid.jsonl")
        );
    }

    #[test]
    fn public_errors_use_only_checked_registry_fields() {
        let terminal = client_error(
            CommandIdentity::EntityGet,
            &ClientError::Public(PublicError::authorization_denied()),
        );
        assert_eq!(
            terminal.json_bytes().expect("JSON"),
            include_bytes!("../fixtures/output-v1/entity.get.public_error.jsonl")
        );
    }

    #[test]
    fn simple_terminal_response_branches_match_accepted_goldens() {
        let deploy = v1::DeployContractResponse {
            result: Some(
                v1::deploy_contract_response::Result::ExpectedActiveVersionMismatch(
                    v1::ExpectedActiveVersionMismatch {
                        actual_active_version: None,
                    },
                ),
            ),
        };
        assert_fixture(
            &render_contract_deploy(&deploy),
            include_bytes!(
                "../fixtures/output-v1/contract.deploy.expected_version_mismatch_absent.jsonl"
            ),
        );

        let outcome = v1::GetOutcomeResponse {
            result: Some(v1::get_outcome_response::Result::NotFound(v1::Unit {})),
        };
        assert_fixture(
            &render_outcome(&outcome),
            include_bytes!("../fixtures/output-v1/command.outcome.not_found.jsonl"),
        );

        let entity = v1::GetEntityResponse {
            result: Some(v1::get_entity_response::Result::NotFound(v1::Unit {})),
        };
        assert_fixture(
            &render_entity(&entity),
            include_bytes!("../fixtures/output-v1/entity.get.not_found.jsonl"),
        );

        let commit = v1::GetCommitResponse {
            result: Some(v1::get_commit_response::Result::NotFound(v1::Unit {})),
        };
        assert_fixture(
            &render_commit(&commit),
            include_bytes!("../fixtures/output-v1/commit.show.not_found.jsonl"),
        );

        let projection = v1::QueryProjectionResponse {
            result: Some(v1::query_projection_response::Result::Invalid(
                v1::QueryProjectionInvalid {
                    reason: v1::ProjectionFailureCode::ProjectionStateIntegrity as i32,
                },
            )),
        };
        assert_fixture(
            &render_projection(&projection),
            include_bytes!("../fixtures/output-v1/projection.query.invalid.jsonl"),
        );
    }

    #[test]
    fn capability_and_health_closed_branches_match_accepted_goldens() {
        let bootstrap = v1::CreateCapabilityResponse {
            result: Some(v1::create_capability_response::Result::Bootstrap(
                v1::BootstrapCreateCapabilityResult {
                    result: Some(
                        v1::bootstrap_create_capability_result::Result::BootstrapConflict(
                            v1::Unit {},
                        ),
                    ),
                },
            )),
        };
        assert_fixture(
            &render_bootstrap(&bootstrap, false),
            include_bytes!("../fixtures/output-v1/capability.bootstrap.bootstrap_conflict.jsonl"),
        );

        assert_fixture(
            &render_normal_create(&NormalCreateDisposition::Conflict),
            include_bytes!("../fixtures/output-v1/capability.create.capability_id_conflict.jsonl"),
        );

        let revoke = v1::RevokeCapabilityResponse {
            result: Some(v1::revoke_capability_response::Result::CapabilityNotFound(
                v1::Unit {},
            )),
        };
        assert_fixture(
            &render_revoke(&revoke),
            include_bytes!("../fixtures/output-v1/capability.revoke.capability_not_found.jsonl"),
        );

        let health = v1::HealthResponse {
            result: Some(v1::health_response::Result::PreBootstrap(
                v1::PreBootstrapHealth {
                    lifecycle: v1::PreBootstrapLifecycle::InitializingBootstrap as i32,
                    liveness: true,
                    readiness: false,
                },
            )),
        };
        assert_fixture(
            &render_health(&health),
            include_bytes!("../fixtures/output-v1/server.health.initializing_bootstrap.jsonl"),
        );
    }

    #[test]
    fn json_and_human_staging_accept_exact_limit_and_reject_one_excess_byte() {
        let base = Terminal::new(
            CommandIdentity::ContractValidate,
            true,
            &String::new(),
            String::new(),
            false,
            0,
        );
        let base_len = base.json_bytes().expect("base JSON").len();
        let exact = Terminal::new(
            CommandIdentity::ContractValidate,
            true,
            &"x".repeat(MAX_OUTPUT_BYTES - base_len),
            String::new(),
            false,
            0,
        );
        assert_eq!(
            exact.json_bytes().expect("exact JSON").len(),
            MAX_OUTPUT_BYTES
        );
        let excessive = Terminal::new(
            CommandIdentity::ContractValidate,
            true,
            &"x".repeat(MAX_OUTPUT_BYTES - base_len + 1),
            String::new(),
            false,
            0,
        );
        assert_eq!(excessive.json, Err(RenderFailure::TooLarge));

        let exact = Terminal::new(
            CommandIdentity::ContractValidate,
            true,
            &Status { status: "valid" },
            "x".repeat(MAX_OUTPUT_BYTES - 1),
            false,
            0,
        );
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let _ = exact.emit(OutputMode::Human, &mut stdout, &mut stderr);
        assert_eq!(stdout.len(), MAX_OUTPUT_BYTES);
        assert!(stderr.is_empty());

        let excessive = Terminal::new(
            CommandIdentity::ContractValidate,
            true,
            &Status { status: "valid" },
            "x".repeat(MAX_OUTPUT_BYTES),
            false,
            0,
        );
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let _ = excessive.emit(OutputMode::Human, &mut stdout, &mut stderr);
        assert!(stdout.is_empty());
        assert_eq!(stderr, OUTPUT_TOO_LARGE);
    }

    #[test]
    fn human_success_uses_the_checked_dto_and_escapes_control_text() {
        #[derive(Serialize)]
        struct HumanFixture<'a> {
            status: &'a str,
            commit_sequence: &'a str,
            outcome_type: &'a str,
            labels: Vec<&'a str>,
        }

        let terminal = success(
            CommandIdentity::CommandExecute,
            "committed",
            &HumanFixture {
                status: "committed",
                commit_sequence: "42",
                outcome_type: "Allocated\n\u{1b}",
                labels: vec!["first", "second"],
            },
        );
        let human = terminal.human.as_deref().expect("human");
        assert!(human.starts_with("command.execute: committed\n"));
        assert!(human.contains("  commit_sequence: 42\n"));
        assert!(human.contains("  outcome_type: Allocated\\n\\u{1b}\n"));
        assert!(human.contains("  labels: 2\n    [0]: first\n    [1]: second"));
        assert!(!human.contains('\u{1b}'));
    }

    #[test]
    fn human_empty_collections_match_the_accepted_bracket_golden() {
        #[derive(Serialize)]
        struct NestedFixture<'a> {
            empty: Vec<&'a str>,
        }

        #[derive(Serialize)]
        struct HumanFixture<'a> {
            status: &'a str,
            empty: Vec<&'a str>,
            groups: Vec<Vec<&'a str>>,
            nested: NestedFixture<'a>,
        }

        let terminal = success(
            CommandIdentity::ServerHealth,
            "ready",
            &HumanFixture {
                status: "ready",
                empty: Vec::new(),
                groups: vec![Vec::new()],
                nested: NestedFixture { empty: Vec::new() },
            },
        );

        assert_eq!(
            terminal.human.as_deref().expect("human"),
            concat!(
                "server.health: ready\n",
                "  empty: []\n",
                "  groups: 1\n",
                "    [0]: []\n",
                "  nested:\n",
                "    empty: []"
            )
        );
    }

    #[test]
    fn rendering_failure_never_writes_partial_stdout_in_either_mode() {
        for mode in [OutputMode::Json, OutputMode::Human] {
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let _ =
                rendering_failure(CommandIdentity::EntityGet).emit(mode, &mut stdout, &mut stderr);
            assert!(stdout.is_empty());
            assert_eq!(
                stderr,
                b"entity.get: output_render_failed: output rendering failed\n"
            );
        }
    }

    fn assert_fixture(terminal: &Terminal, fixture: &[u8]) {
        assert_eq!(terminal.json_bytes().expect("JSON"), fixture);
    }
}
