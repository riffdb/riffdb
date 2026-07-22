//! Checked command execution and uncertainty-recovery orchestration.

use std::sync::Arc;

use riffdb_catalog::{CatalogError, CatalogErrorKind, ResolvedExecutablePlan};
use riffdb_commit::{
    CommandExecutionAdmissionError, CommandExecutionErrorKind, CommandExecutionPreparation,
    CommandExecutionResult as CoordinatorCommandResult, CommandIdempotencyConfirmationError,
    CommandIdempotencyInspectionErrorKind, CommandIdempotencyInspectionRequest,
    CommandIdempotencyPlanSelection, CommittedOutcome, CommittedOutcomeDisposition,
    CoordinatorDurability, ReadOnlyExecutionPreparation, ReadOnlyExecutionResult,
};
use riffdb_contract_ir::{
    CommandPlan, ExecutionClass, OutcomeSchema, SchemaIr, ValueType, ValueTypeTag,
};
use riffdb_errors::{
    MAX_VALIDATION_ISSUES, MAX_VALIDATION_PATH_SEGMENTS, PublicError, ValidationCode,
    ValidationIssue, ValidationIssues, ValidationPath, ValidationPathSegment,
};
use riffdb_invariant::{EvaluationError, derive_input_command_facts};
use riffdb_policy::{
    AuthorizedOperation, CommandExecutionClass, OperationRequest, OperationTenantScope,
    OutputClassification, PartitionConstraint,
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, FieldId, IdempotencyKey, OutcomeId, ScopedPartitionV1,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceOperationV1,
    TenantScope,
};

use crate::orchestration::{AuditScope, BegunInvocation};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    AuthoritativeOutcomeRequest, AuthoritativeOutcomeSnapshot, AuthoritativeReadError,
    AuthoritativeReadinessFailure, CatalogExecutablePlanRequest, CommandApplication,
    CommandDurability, DeclaredOutcomeView, ExecuteCommandRequest, ExecuteCommandResult,
    InternalDefect, JournaledCommandResult, JournaledCompletion, OutcomePlanBinding,
    PendingTerminalResponse, PortAdmissionError, PortDriverStopped, ReadOnlyCommandResult,
    RecoveredJournaledCommandResult, RequestContext, ResolveCommandOutcomeRequest,
    ResolveCommandOutcomeResult, RiffDbService, RiffDbServiceInner, ServiceAuditTargetMap,
    ServiceFailure, ServiceFuture, ServiceResult, ensure_response_budget,
};

impl CommandApplication for RiffDbService {
    fn execute_command(
        &self,
        context: RequestContext,
        request: ExecuteCommandRequest,
    ) -> ServiceFuture<'_, ExecuteCommandResult> {
        let service = Arc::clone(&self.inner);
        self.spawn_operation(ServiceOperationV1::ExecuteCommand, async move {
            execute_command(service.as_ref(), context, request).await
        })
    }

    fn resolve_command_outcome(
        &self,
        context: RequestContext,
        request: ResolveCommandOutcomeRequest,
    ) -> ServiceFuture<'_, ResolveCommandOutcomeResult> {
        let service = Arc::clone(&self.inner);
        self.spawn_operation(ServiceOperationV1::ResolveCommandOutcome, async move {
            resolve_command_outcome(service.as_ref(), context, request).await
        })
    }
}

struct ActiveCommand {
    plan: CommandPlan,
    schema: SchemaIr,
    catalog_request: CatalogExecutablePlanRequest,
}

struct CheckedOutcomeCatalog {
    plan: OutcomePlanBinding,
    contract_schema: SchemaIr,
    outcomes: Vec<OutcomeSchema>,
}

impl CheckedOutcomeCatalog {
    fn from_resolved(resolved: &ResolvedExecutablePlan) -> Self {
        let reference = resolved.reference();
        Self {
            plan: OutcomePlanBinding::new(
                reference.contract_lineage().clone(),
                reference.contract_version(),
                reference.command_id(),
                reference.command_plan_hash(),
                resolved.plan().execution_class(),
            ),
            contract_schema: resolved.bundle().bundle().schema().clone(),
            outcomes: resolved.plan().outcomes().to_vec(),
        }
    }
}

async fn execute_command(
    service: &RiffDbServiceInner,
    context: RequestContext,
    request: ExecuteCommandRequest,
) -> ServiceResult<ExecuteCommandResult> {
    let active = prepare_active_command(service, &context, &request).await?;
    let targets = ServiceAuditTargetMap::execute_command(
        active.catalog_request.lineage().clone(),
        active.catalog_request.version(),
        active.plan.command_id(),
    )
    .map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;

    match active.plan.execution_class() {
        ExecutionClass::ReadOnly => {
            execute_read_only(service, &context, &request, active, targets).await
        }
        ExecutionClass::IdempotentMutation => {
            execute_mutation(service, &context, &request, active, targets).await
        }
    }
}

async fn prepare_active_command(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request: &ExecuteCommandRequest,
) -> ServiceResult<ActiveCommand> {
    let snapshot = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .catalog
            .prepare_active_catalog(context.control()),
    )
    .await
    .map_err(map_controlled_wait)?
    .map_err(|error| map_catalog_error(service, ServiceOperationV1::ExecuteCommand, error))?
    .ok_or_else(|| invalid_root(ValidationCode::InvalidValue))?;

    let pointer = snapshot.pointer();
    if request
        .expected_contract_version()
        .is_some_and(|expected| expected != pointer.contract_version())
    {
        return Err(PublicError::contract_mismatch(pointer.contract_version()).into());
    }

    let plan = snapshot
        .bundle()
        .bundle()
        .commands()
        .iter()
        .find(|plan| plan.name() == request.command().as_str())
        .cloned()
        .ok_or_else(|| invalid_root(ValidationCode::InvalidValue))?;
    let catalog_request = CatalogExecutablePlanRequest::new(
        pointer.lineage().clone(),
        pointer.contract_version(),
        pointer.bundle_hash(),
        plan.command_id(),
        plan.plan_hash(),
    );
    let schema = snapshot.bundle().bundle().schema().clone();
    Ok(ActiveCommand {
        plan,
        schema,
        catalog_request,
    })
}

async fn execute_read_only(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request: &ExecuteCommandRequest,
    active: ActiveCommand,
    targets: ServiceAuditTargetsV1,
) -> ServiceResult<ExecuteCommandResult> {
    let initial = load_plan(
        service,
        context,
        ServiceOperationV1::ExecuteCommand,
        active.catalog_request.clone(),
    )
    .await?;
    let normalized = normalize_command_input(
        initial.plan(),
        initial.bundle().bundle().schema(),
        &active.plan,
        request.input(),
    )
    .map_err(|error| input_error(service, ServiceOperationV1::ExecuteCommand, error))?;
    let facts = derive_input_command_facts(initial.plan(), normalized.clone())
        .map_err(|error| evaluation_error(service, ServiceOperationV1::ExecuteCommand, error))?;
    let operation = command_operation(&initial, CommandExecutionClass::ReadOnly, &facts);
    let begun = service
        .begin_invocation(context, operation, targets, AuditScope::StandardRead)
        .await?;

    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service.executors.command.reserve_capacity(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => {
            let failure = map_command_admission(service, error);
            return Err(
                finish_failure(service, context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
        Err(error) => {
            let failure = map_controlled_wait(error);
            return Err(
                finish_failure(service, context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
    };

    let expected_plan = active.catalog_request.clone();
    let resolved = match load_plan(
        service,
        context,
        ServiceOperationV1::ExecuteCommand,
        active.catalog_request.clone(),
    )
    .await
    {
        Ok(resolved) => resolved,
        Err(failure) => {
            return Err(
                finish_failure(service, context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
    };
    let normalized = match normalize_command_input(
        resolved.plan(),
        resolved.bundle().bundle().schema(),
        &active.plan,
        request.input(),
    ) {
        Ok(normalized) => normalized,
        Err(error) => {
            let failure = input_error(service, ServiceOperationV1::ExecuteCommand, error);
            return Err(
                finish_failure(service, context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
    };
    let facts = match derive_input_command_facts(resolved.plan(), normalized.clone()) {
        Ok(facts) => facts,
        Err(error) => {
            let failure = evaluation_error(service, ServiceOperationV1::ExecuteCommand, error);
            return Err(
                finish_failure(service, context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
    };
    let operation = command_operation(&resolved, CommandExecutionClass::ReadOnly, &facts);
    let return_operation = operation.clone();
    let outcome_catalog = CheckedOutcomeCatalog::from_resolved(&resolved);
    let control = match context.control().command_control() {
        Ok(control) => control,
        Err(_) => {
            let failure = service.internal_failure(
                ServiceOperationV1::ExecuteCommand,
                InternalDefect::ProofMismatch,
            );
            return Err(
                finish_failure(service, context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
    };
    let authorization = begun
        .reauthorize_request(service, context, operation)
        .await?;
    let authorization = match authorization.into_command_execution(
        context.claims().clone(),
        service.identity.agent_session_policy(),
    ) {
        Ok(authorization) => authorization,
        Err(_) => {
            let failure = service.internal_failure(
                ServiceOperationV1::ExecuteCommand,
                InternalDefect::ProofMismatch,
            );
            return Err(
                finish_failure(service, context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
    };
    let preparation = match ReadOnlyExecutionPreparation::new(
        service.identity.database_id(),
        service.identity.environment(),
        resolved,
        normalized,
        facts,
        authorization,
        context.request_id(),
        control,
    ) {
        Ok(preparation) => preparation,
        Err(_) => {
            let failure = service.internal_failure(
                ServiceOperationV1::ExecuteCommand,
                InternalDefect::ProofMismatch,
            );
            return Err(
                finish_failure(service, context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
    };
    let receipt = match permit.submit_read_only(preparation) {
        Ok(receipt) => receipt,
        Err(error) => {
            let failure = map_command_admission(service, error);
            return Err(
                finish_failure(service, context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
    };
    match receipt.completion().await {
        Ok(ReadOnlyExecutionResult::Executed(executed)) => {
            let return_authorization = begun
                .reauthorize_request(service, context, return_operation)
                .await?;
            if return_authorization
                .into_command_execution(
                    context.claims().clone(),
                    service.identity.agent_session_policy(),
                )
                .is_err()
            {
                let failure = service.internal_failure(
                    ServiceOperationV1::ExecuteCommand,
                    InternalDefect::ProofMismatch,
                );
                return Err(finish_failure(
                    service,
                    context,
                    &begun,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
            let (plan, outcome) = executed.into_parts();
            if plan.contract_lineage() != expected_plan.lineage()
                || plan.contract_version() != expected_plan.version()
                || plan.contract_bundle_hash() != expected_plan.bundle_hash()
                || plan.command_id() != expected_plan.command_id()
                || plan.command_plan_hash() != expected_plan.plan_hash()
            {
                let failure = service.internal_failure(
                    ServiceOperationV1::ExecuteCommand,
                    InternalDefect::ProofMismatch,
                );
                return Err(finish_failure(
                    service,
                    context,
                    &begun,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
            let outcome = match declared_outcome_view(
                service,
                ServiceOperationV1::ExecuteCommand,
                &outcome_catalog,
                outcome.outcome_id(),
                outcome.value().clone(),
            ) {
                Ok(outcome) => outcome,
                Err(failure) => {
                    return Err(finish_failure(
                        service,
                        context,
                        &begun,
                        failure,
                        TerminalKind::Ordinary,
                    )
                    .await);
                }
            };
            let result = match ReadOnlyCommandResult::new(outcome) {
                Ok(result) => result,
                Err(_) => {
                    let failure = service.internal_failure(
                        ServiceOperationV1::ExecuteCommand,
                        InternalDefect::ProofMismatch,
                    );
                    return Err(finish_failure(
                        service,
                        context,
                        &begun,
                        failure,
                        TerminalKind::Ordinary,
                    )
                    .await);
                }
            };
            let result = ExecuteCommandResult::ReadOnlyExecuted(result);
            if let Err(failure) = ensure_response_budget(&result) {
                return Err(finish_failure(
                    service,
                    context,
                    &begun,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
            finish_success(service, context, &begun, ServiceAuditLinkV1::None, false).await?;
            Ok(result)
        }
        Ok(ReadOnlyExecutionResult::ExecutionFailed(code)) => {
            let failure = PublicError::command_execution_failed(code).into();
            Err(finish_failure(service, context, &begun, failure, TerminalKind::Ordinary).await)
        }
        Err(error) => {
            let (failure, terminal) =
                map_command_execution(service, ExecutionClass::ReadOnly, error.kind());
            Err(finish_failure(service, context, &begun, failure, terminal).await)
        }
    }
}

async fn execute_mutation(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request: &ExecuteCommandRequest,
    active: ActiveCommand,
    targets: ServiceAuditTargetsV1,
) -> ServiceResult<ExecuteCommandResult> {
    service.classify_intrinsic_prestart(
        context,
        ServiceOperationV1::ExecuteCommand,
        targets.clone(),
    )?;
    let active_normalized = match normalize_command_input(
        &active.plan,
        &active.schema,
        &active.plan,
        request.input(),
    ) {
        Ok(normalized) => normalized,
        Err(error) => {
            let failure = input_error(service, ServiceOperationV1::ExecuteCommand, error);
            return Err(terminate_mutation(
                service,
                context,
                None,
                &targets,
                failure,
                TerminalKind::Ordinary,
            )
            .await);
        }
    };
    let caller_key = match extract_idempotency_key(&active.plan, &active_normalized) {
        Ok(caller_key) => caller_key,
        Err(()) => {
            let failure = service.internal_failure(
                ServiceOperationV1::ExecuteCommand,
                InternalDefect::ProofMismatch,
            );
            return Err(terminate_mutation(
                service,
                context,
                None,
                &targets,
                failure,
                TerminalKind::Ordinary,
            )
            .await);
        }
    };

    let mut begun: Option<BegunInvocation> = None;
    for _ in 0..crate::MAX_COMMAND_PREPARATION_ATTEMPTS {
        let inspection_request = CommandIdempotencyInspectionRequest::new(
            service.identity.database_id(),
            service.identity.environment().clone(),
            OperationTenantScope::grammar_v1_global()
                .tenant_scope()
                .clone(),
            context.principal().principal_id().clone(),
            active.catalog_request.lineage().clone(),
            active.catalog_request.command_id(),
            caller_key.clone(),
        );
        let inspection = match wait_with_control(
            context.control(),
            service.providers.deadline_scheduler.as_ref(),
            service.executors.idempotency.inspect(inspection_request),
        )
        .await
        {
            Ok(Ok(inspection)) => inspection,
            Ok(Err(error)) => {
                let failure = map_idempotency_inspection(service, error.kind());
                return Err(terminate_mutation(
                    service,
                    context,
                    begun.as_ref(),
                    &targets,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
            Err(error) => {
                let failure = map_controlled_wait(error);
                return Err(terminate_mutation(
                    service,
                    context,
                    begun.as_ref(),
                    &targets,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
        };

        let selected_request = match inspection.plan_selection() {
            CommandIdempotencyPlanSelection::Absent => active.catalog_request.clone(),
            CommandIdempotencyPlanSelection::Historical(reference) => {
                CatalogExecutablePlanRequest::new(
                    reference.contract_lineage().clone(),
                    reference.contract_version(),
                    reference.contract_bundle_hash(),
                    reference.command_id(),
                    reference.command_plan_hash(),
                )
            }
        };
        let selected = match load_plan(
            service,
            context,
            ServiceOperationV1::ExecuteCommand,
            selected_request.clone(),
        )
        .await
        {
            Ok(selected) => selected,
            Err(failure) => {
                return Err(terminate_mutation(
                    service,
                    context,
                    begun.as_ref(),
                    &targets,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
        };
        let normalized = match normalize_command_input(
            selected.plan(),
            selected.bundle().bundle().schema(),
            &active.plan,
            request.input(),
        ) {
            Ok(normalized) => normalized,
            Err(error) => {
                let failure = input_error(service, ServiceOperationV1::ExecuteCommand, error);
                return Err(terminate_mutation(
                    service,
                    context,
                    begun.as_ref(),
                    &targets,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
        };
        let facts = match derive_input_command_facts(selected.plan(), normalized.clone()) {
            Ok(facts) => facts,
            Err(error) => {
                let failure = evaluation_error(service, ServiceOperationV1::ExecuteCommand, error);
                return Err(terminate_mutation(
                    service,
                    context,
                    begun.as_ref(),
                    &targets,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
        };
        let operation = command_operation(&selected, CommandExecutionClass::Mutation, &facts);
        if begun.is_none() {
            begun = Some(
                service
                    .begin_invocation(context, operation, targets.clone(), AuditScope::Intrinsic)
                    .await?,
            );
        }
        drop(selected);

        let invocation = begun.as_ref().expect("mutation begins before admission");
        let permit = match wait_with_control(
            context.control(),
            service.providers.deadline_scheduler.as_ref(),
            service.executors.command.reserve_capacity(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(error)) => {
                let failure = map_command_admission(service, error);
                return Err(finish_failure(
                    service,
                    context,
                    invocation,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
            Err(error) => {
                let failure = map_controlled_wait(error);
                return Err(finish_failure(
                    service,
                    context,
                    invocation,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
        };

        let resolved = match load_plan(
            service,
            context,
            ServiceOperationV1::ExecuteCommand,
            selected_request.clone(),
        )
        .await
        {
            Ok(resolved) => resolved,
            Err(failure) => {
                return Err(finish_failure(
                    service,
                    context,
                    invocation,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
        };
        let normalized = match normalize_command_input(
            resolved.plan(),
            resolved.bundle().bundle().schema(),
            &active.plan,
            request.input(),
        ) {
            Ok(normalized) => normalized,
            Err(error) => {
                let failure = input_error(service, ServiceOperationV1::ExecuteCommand, error);
                return Err(finish_failure(
                    service,
                    context,
                    invocation,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
        };
        let facts = match derive_input_command_facts(resolved.plan(), normalized.clone()) {
            Ok(facts) => facts,
            Err(error) => {
                let failure = evaluation_error(service, ServiceOperationV1::ExecuteCommand, error);
                return Err(finish_failure(
                    service,
                    context,
                    invocation,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
        };
        let operation = command_operation(&resolved, CommandExecutionClass::Mutation, &facts);
        let outcome_catalog = CheckedOutcomeCatalog::from_resolved(&resolved);
        let idempotency_field = match resolved.plan().idempotency_input() {
            Some(field) => field,
            None => {
                let failure = service.internal_failure(
                    ServiceOperationV1::ExecuteCommand,
                    InternalDefect::ProofMismatch,
                );
                return Err(finish_failure(
                    service,
                    context,
                    invocation,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
        };
        let prepared_idempotency = match inspection.confirm_selected_plan(
            &normalized,
            idempotency_field,
            resolved.reference().clone(),
        ) {
            Ok(prepared) => prepared,
            Err(CommandIdempotencyConfirmationError::IdempotencyKeyMismatch) => {
                let failure = PublicError::idempotency_key_reuse().into();
                return Err(finish_failure(
                    service,
                    context,
                    invocation,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
            Err(_) => {
                let failure = service.internal_failure(
                    ServiceOperationV1::ExecuteCommand,
                    InternalDefect::ProofMismatch,
                );
                return Err(finish_failure(
                    service,
                    context,
                    invocation,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
        };
        let control = match context.control().command_control() {
            Ok(control) => control,
            Err(_) => {
                let failure = service.internal_failure(
                    ServiceOperationV1::ExecuteCommand,
                    InternalDefect::ProofMismatch,
                );
                return Err(finish_failure(
                    service,
                    context,
                    invocation,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
        };
        let authorization = invocation
            .reauthorize_request(service, context, operation)
            .await?;
        let authorization = match authorization.into_command_execution(
            context.claims().clone(),
            service.identity.agent_session_policy(),
        ) {
            Ok(authorization) => authorization,
            Err(_) => {
                let failure = service.internal_failure(
                    ServiceOperationV1::ExecuteCommand,
                    InternalDefect::ProofMismatch,
                );
                return Err(finish_failure(
                    service,
                    context,
                    invocation,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
        };
        let preparation = match CommandExecutionPreparation::new(
            service.identity.database_id(),
            service.identity.environment(),
            resolved,
            normalized,
            prepared_idempotency,
            facts,
            authorization,
            context.request_id(),
            control,
        ) {
            Ok(preparation) => preparation,
            Err(_) => {
                let failure = service.internal_failure(
                    ServiceOperationV1::ExecuteCommand,
                    InternalDefect::ProofMismatch,
                );
                return Err(finish_failure(
                    service,
                    context,
                    invocation,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
        };
        let receipt = match permit.submit(preparation) {
            Ok(receipt) => receipt,
            Err(error) => {
                let failure = map_command_admission(service, error);
                return Err(finish_failure(
                    service,
                    context,
                    invocation,
                    failure,
                    TerminalKind::Ordinary,
                )
                .await);
            }
        };
        match receipt.completion().await {
            Ok(CoordinatorCommandResult::Committed(outcome)) => {
                let (result, link) = match map_committed_outcome(
                    outcome,
                    &selected_request,
                    |defect| service.internal_failure(ServiceOperationV1::ExecuteCommand, defect),
                    |outcome_id, value| {
                        declared_outcome_view(
                            service,
                            ServiceOperationV1::ExecuteCommand,
                            &outcome_catalog,
                            outcome_id,
                            value,
                        )
                    },
                ) {
                    Ok(mapped) => mapped,
                    Err(failure) => {
                        return Err(finish_failure(
                            service,
                            context,
                            invocation,
                            failure,
                            TerminalKind::KnownCommitMappingFailure,
                        )
                        .await);
                    }
                };
                let result = ExecuteCommandResult::Journaled(result);
                let pending = PendingTerminalResponse::new(result, link, ensure_response_budget);
                finish_success(service, context, invocation, pending.terminal(), true).await?;
                return pending.into_response();
            }
            Ok(CoordinatorCommandResult::ExecutionFailed(code)) => {
                let failure = PublicError::command_execution_failed(code).into();
                return Err(finish_failure(
                    service,
                    context,
                    invocation,
                    failure,
                    TerminalKind::DurableFailure,
                )
                .await);
            }
            Ok(CoordinatorCommandResult::InputMismatch) => {
                let failure = PublicError::idempotency_key_reuse().into();
                return Err(finish_failure(
                    service,
                    context,
                    invocation,
                    failure,
                    TerminalKind::DurableFailure,
                )
                .await);
            }
            Ok(CoordinatorCommandResult::PreparationChanged) => {}
            Err(error) => {
                let (failure, terminal) = map_command_execution(
                    service,
                    ExecutionClass::IdempotentMutation,
                    error.kind(),
                );
                return Err(finish_failure(service, context, invocation, failure, terminal).await);
            }
        }
    }

    let invocation = begun.as_ref().expect("bounded mutation loop always begins");
    let failure = PublicError::concurrency_deadline_exceeded().into();
    Err(finish_failure(
        service,
        context,
        invocation,
        failure,
        TerminalKind::Ordinary,
    )
    .await)
}

fn command_operation(
    resolved: &ResolvedExecutablePlan,
    class: CommandExecutionClass,
    facts: &riffdb_invariant::InputDerivedCommandFacts,
) -> OperationRequest {
    OperationRequest::execute_command(
        resolved.reference().contract_lineage().clone(),
        resolved.reference().contract_version(),
        resolved.reference().command_id(),
        class,
        facts.partition_key().clone(),
    )
}

async fn resolve_command_outcome(
    service: &RiffDbServiceInner,
    context: RequestContext,
    request: ResolveCommandOutcomeRequest,
) -> ServiceResult<ResolveCommandOutcomeResult> {
    let active = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .catalog
            .prepare_active_catalog(context.control()),
    )
    .await
    .map_err(map_controlled_wait)?
    .map_err(|error| map_catalog_error(service, ServiceOperationV1::ResolveCommandOutcome, error))?
    .ok_or_else(|| invalid_root(ValidationCode::InvalidValue))?;
    if active.pointer().lineage() != request.lineage() {
        return Err(invalid_root(ValidationCode::InvalidValue).into());
    }
    let command = active
        .bundle()
        .bundle()
        .commands()
        .iter()
        .find(|plan| plan.name() == request.command().as_str())
        .filter(|plan| plan.execution_class() == ExecutionClass::IdempotentMutation)
        .ok_or_else(|| invalid_root(ValidationCode::InvalidValue))?;
    let command_id = command.command_id();
    let targets =
        ServiceAuditTargetMap::resolve_command_outcome(request.lineage().clone(), command_id)
            .map_err(|_| {
                service.internal_failure(
                    ServiceOperationV1::ResolveCommandOutcome,
                    InternalDefect::ProofMismatch,
                )
            })?;
    let policy_request =
        OperationRequest::resolve_command_outcome_pre_lookup(request.lineage().clone(), command_id);
    let begun = service
        .begin_invocation(
            &context,
            policy_request.clone(),
            targets,
            AuditScope::StandardRead,
        )
        .await?;
    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .authoritative
            .reserve_read_outcome(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => {
            let failure = map_port_admission(error);
            return Err(
                finish_failure(service, &context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
        Err(error) => {
            let failure = map_controlled_wait(error);
            return Err(
                finish_failure(service, &context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
    };
    let authorization = begun.reauthorize(service, &context).await?;
    if !outcome_authorization_matches(service, &authorization, &policy_request, None) {
        let failure = service.internal_failure(
            ServiceOperationV1::ResolveCommandOutcome,
            InternalDefect::ProofMismatch,
        );
        return Err(
            finish_failure(service, &context, &begun, failure, TerminalKind::Ordinary).await,
        );
    }
    let lower_request = AuthoritativeOutcomeRequest::new(
        request.lineage().clone(),
        command_id,
        context.principal().principal_id().clone(),
        OperationTenantScope::grammar_v1_global()
            .tenant_scope()
            .clone(),
        request.idempotency_key().clone(),
    );
    let receipt = match permit.submit(lower_request) {
        Ok(receipt) => receipt,
        Err(error) => {
            let failure = map_port_admission(error);
            return Err(
                finish_failure(service, &context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
    };
    let snapshot = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(snapshot))) => snapshot,
        Ok(Ok(Err(error))) => {
            let failure =
                map_authoritative_error(service, ServiceOperationV1::ResolveCommandOutcome, error);
            return Err(
                finish_failure(service, &context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
        Ok(Err(PortDriverStopped)) => {
            let failure = service.internal_failure(
                ServiceOperationV1::ResolveCommandOutcome,
                InternalDefect::LowerIntegrity,
            );
            return Err(
                finish_failure(service, &context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
        Err(error) => {
            let failure = map_controlled_wait(error);
            return Err(
                finish_failure(service, &context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
    };

    let Some(snapshot) = snapshot else {
        let result = ResolveCommandOutcomeResult::NotFound;
        if let Err(failure) = ensure_response_budget(&result) {
            return Err(
                finish_failure(service, &context, &begun, failure, TerminalKind::Ordinary).await,
            );
        }
        finish_success(service, &context, &begun, ServiceAuditLinkV1::None, false).await?;
        return Ok(result);
    };
    let facts = snapshot.facts();
    if facts.lineage() != request.lineage()
        || facts.command_id() != command_id
        || facts.owner_principal_id() != context.principal().principal_id()
        || facts.owner_tenant_scope() != &TenantScope::Global
    {
        let failure = service.internal_failure(
            ServiceOperationV1::ResolveCommandOutcome,
            InternalDefect::LowerIntegrity,
        );
        return Err(
            finish_failure(service, &context, &begun, failure, TerminalKind::Ordinary).await,
        );
    }
    let recovered_outcome_catalog =
        if matches!(&snapshot, AuthoritativeOutcomeSnapshot::Journaled { .. }) {
            let plan_request = CatalogExecutablePlanRequest::new(
                facts.lineage().clone(),
                facts.contract_version(),
                facts.bundle_hash(),
                facts.command_id(),
                facts.plan_hash(),
            );
            let resolved = match load_plan(
                service,
                &context,
                ServiceOperationV1::ResolveCommandOutcome,
                plan_request,
            )
            .await
            {
                Ok(resolved) => resolved,
                Err(failure) => {
                    return Err(finish_failure(
                        service,
                        &context,
                        &begun,
                        failure,
                        TerminalKind::KnownCommitMappingFailure,
                    )
                    .await);
                }
            };
            Some(CheckedOutcomeCatalog::from_resolved(&resolved))
        } else {
            None
        };
    let full_request = OperationRequest::resolve_command_outcome(
        facts.lineage().clone(),
        facts.contract_version(),
        facts.command_id(),
        facts.owner_principal_id().clone(),
        facts.owner_tenant_scope().clone(),
        facts.partition().clone(),
    );
    let expected_partition =
        ScopedPartitionV1::new(facts.lineage().clone(), facts.partition().clone());
    let authorization = begun
        .reauthorize_request(service, &context, full_request.clone())
        .await?;
    if !outcome_authorization_matches(
        service,
        &authorization,
        &full_request,
        Some(&expected_partition),
    ) {
        let failure = service.internal_failure(
            ServiceOperationV1::ResolveCommandOutcome,
            InternalDefect::ProofMismatch,
        );
        return Err(
            finish_failure(service, &context, &begun, failure, TerminalKind::Ordinary).await,
        );
    }
    match snapshot {
        AuthoritativeOutcomeSnapshot::Pending(_) => {
            let failure = PublicError::outcome_unknown().into();
            Err(finish_failure(
                service,
                &context,
                &begun,
                failure,
                TerminalKind::OutcomeUncertain,
            )
            .await)
        }
        AuthoritativeOutcomeSnapshot::Journaled { facts: _, result } => {
            let Some(outcome_catalog) = recovered_outcome_catalog.as_ref() else {
                let failure = service.internal_failure(
                    ServiceOperationV1::ResolveCommandOutcome,
                    InternalDefect::ProofMismatch,
                );
                return Err(finish_failure(
                    service,
                    &context,
                    &begun,
                    failure,
                    TerminalKind::KnownCommitMappingFailure,
                )
                .await);
            };
            let outcome = match declared_outcome_view(
                service,
                ServiceOperationV1::ResolveCommandOutcome,
                outcome_catalog,
                result.outcome_id(),
                result.value().clone(),
            ) {
                Ok(outcome) => outcome,
                Err(failure) => {
                    return Err(finish_failure(
                        service,
                        &context,
                        &begun,
                        failure,
                        TerminalKind::KnownCommitMappingFailure,
                    )
                    .await);
                }
            };
            let result = match JournaledCommandResult::new(
                JournaledCompletion::Replayed,
                result.commit_sequence(),
                outcome,
                result.provenance_id(),
                result.durability(),
            ) {
                Ok(result) => result,
                Err(_) => {
                    let failure = service.internal_failure(
                        ServiceOperationV1::ResolveCommandOutcome,
                        InternalDefect::ProofMismatch,
                    );
                    return Err(finish_failure(
                        service,
                        &context,
                        &begun,
                        failure,
                        TerminalKind::KnownCommitMappingFailure,
                    )
                    .await);
                }
            };
            let link = ServiceAuditLinkV1::Command {
                commit_sequence: result.commit_sequence(),
                provenance_id: result.provenance_id(),
            };
            let result = match RecoveredJournaledCommandResult::new(result) {
                Ok(result) => ResolveCommandOutcomeResult::Found(Box::new(result)),
                Err(_) => {
                    let failure = service.internal_failure(
                        ServiceOperationV1::ResolveCommandOutcome,
                        InternalDefect::ProofMismatch,
                    );
                    return Err(finish_failure(
                        service,
                        &context,
                        &begun,
                        failure,
                        TerminalKind::KnownCommitMappingFailure,
                    )
                    .await);
                }
            };
            let pending = PendingTerminalResponse::new(result, link, ensure_response_budget);
            finish_success(service, &context, &begun, pending.terminal(), true).await?;
            pending.into_response()
        }
        AuthoritativeOutcomeSnapshot::ExecutionFailed { code, .. } => {
            let failure = PublicError::command_execution_failed(code).into();
            Err(finish_failure(
                service,
                &context,
                &begun,
                failure,
                TerminalKind::DurableFailure,
            )
            .await)
        }
    }
}

async fn load_plan(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    operation: ServiceOperationV1,
    request: CatalogExecutablePlanRequest,
) -> ServiceResult<ResolvedExecutablePlan> {
    let expected = request.clone();
    let resolved = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .catalog
            .executable_plan(context.control(), request),
    )
    .await
    .map_err(map_controlled_wait)?
    .map_err(|error| map_catalog_error(service, operation, error))?;
    let actual = resolved.reference();
    if actual.contract_lineage() != expected.lineage()
        || actual.contract_version() != expected.version()
        || actual.contract_bundle_hash() != expected.bundle_hash()
        || actual.command_id() != expected.command_id()
        || actual.command_plan_hash() != expected.plan_hash()
    {
        return Err(service.internal_failure(operation, InternalDefect::ProofMismatch));
    }
    Ok(resolved)
}

enum InputPreparationError {
    Public(PublicError),
    Integrity,
}

fn normalize_command_input(
    selected: &CommandPlan,
    selected_schema: &SchemaIr,
    active: &CommandPlan,
    submitted: &CanonicalRecord,
) -> Result<CanonicalRecord, InputPreparationError> {
    if selected.command_id() != active.command_id() {
        return Err(InputPreparationError::Integrity);
    }
    let declared = selected.input().record().fields();
    let supplied = submitted.fields();
    let mut normalized = Vec::with_capacity(declared.len());
    let mut issues = Vec::new();
    let mut declared_index = 0;
    let mut supplied_index = 0;

    while declared_index < declared.len() || supplied_index < supplied.len() {
        match (declared.get(declared_index), supplied.get(supplied_index)) {
            (Some(field), Some((actual_id, value))) if field.id() == *actual_id => {
                let issue =
                    validate_input_value(selected_schema, field.value_type(), value, field.id())
                        .or_else(|| {
                            (selected.idempotency_input() == Some(field.id())
                        && matches!(value, CanonicalValue::String(value) if value.is_empty()))
                    .then(|| field_issue(ValidationCode::InvalidValue, field.id()))
                        });
                if let Some(issue) = issue {
                    push_issue(&mut issues, issue);
                } else {
                    normalized.push((field.id(), value.clone()));
                }
                declared_index += 1;
                supplied_index += 1;
            }
            (Some(field), Some((actual_id, _))) if field.id() < *actual_id => {
                if field.value_type().is_optional() {
                    normalized.push((field.id(), CanonicalValue::Null));
                } else {
                    push_issue(
                        &mut issues,
                        field_issue(ValidationCode::MissingRequiredValue, field.id()),
                    );
                }
                declared_index += 1;
            }
            (Some(_), Some((actual_id, value))) => {
                validate_historical_extra(active, *actual_id, value, &mut issues);
                supplied_index += 1;
            }
            (Some(field), None) => {
                if field.value_type().is_optional() {
                    normalized.push((field.id(), CanonicalValue::Null));
                } else {
                    push_issue(
                        &mut issues,
                        field_issue(ValidationCode::MissingRequiredValue, field.id()),
                    );
                }
                declared_index += 1;
            }
            (None, Some((actual_id, value))) => {
                validate_historical_extra(active, *actual_id, value, &mut issues);
                supplied_index += 1;
            }
            (None, None) => break,
        }
    }

    if !issues.is_empty() {
        let issues = ValidationIssues::new(issues).map_err(|_| InputPreparationError::Integrity)?;
        return Err(InputPreparationError::Public(PublicError::validation(
            issues,
        )));
    }
    CanonicalRecord::new(normalized).map_err(|_| InputPreparationError::Integrity)
}

fn validate_historical_extra(
    active: &CommandPlan,
    field_id: FieldId,
    value: &CanonicalValue,
    issues: &mut Vec<ValidationIssue>,
) {
    let compatible_null_addition = active
        .input()
        .record()
        .field(field_id)
        .is_some_and(|field| field.value_type().is_optional())
        && matches!(value, CanonicalValue::Null);
    if !compatible_null_addition {
        push_issue(issues, field_issue(ValidationCode::UnknownField, field_id));
    }
}

fn validate_input_value(
    schema: &SchemaIr,
    value_type: &ValueType,
    value: &CanonicalValue,
    field_id: FieldId,
) -> Option<ValidationIssue> {
    let mut path = vec![ValidationPathSegment::Field(field_id)];
    validate_value(schema, value_type, value, &mut path).map(|code| {
        let path = ValidationPath::new(path).unwrap_or_else(|_| ValidationPath::root());
        ValidationIssue::new(code, path)
    })
}

fn validate_value(
    schema: &SchemaIr,
    value_type: &ValueType,
    value: &CanonicalValue,
    path: &mut Vec<ValidationPathSegment>,
) -> Option<ValidationCode> {
    if matches!(value, CanonicalValue::Null) {
        return (!value_type.is_optional()).then_some(ValidationCode::TypeMismatch);
    }
    if let Some(inner) = value_type.optional_inner() {
        return validate_value(schema, inner, value, path);
    }
    match (value_type.tag(), value) {
        (ValueTypeTag::Bool, CanonicalValue::Bool(_))
        | (ValueTypeTag::I64, CanonicalValue::I64(_))
        | (ValueTypeTag::U64, CanonicalValue::U64(_))
        | (ValueTypeTag::Timestamp, CanonicalValue::Timestamp(_))
        | (ValueTypeTag::Date, CanonicalValue::Date(_))
        | (ValueTypeTag::Uuid, CanonicalValue::Uuid(_)) => None,
        (ValueTypeTag::Decimal, CanonicalValue::Decimal(actual)) => {
            (value_type.decimal_spec() != Some(actual.spec())).then_some(ValidationCode::OutOfRange)
        }
        (ValueTypeTag::Money, CanonicalValue::Money(actual)) => {
            if value_type.currency() != Some(actual.currency()) {
                Some(ValidationCode::InvalidValue)
            } else if value_type.validate_value(value).is_err() {
                Some(ValidationCode::OutOfRange)
            } else {
                None
            }
        }
        (ValueTypeTag::String, CanonicalValue::String(actual)) => (value_type
            .byte_bound()
            .is_some_and(|maximum| actual.len() > maximum))
        .then_some(ValidationCode::TooLong),
        (ValueTypeTag::Bytes, CanonicalValue::Bytes(actual)) => (value_type
            .byte_bound()
            .is_some_and(|maximum| actual.len() > maximum))
        .then_some(ValidationCode::TooLong),
        (
            ValueTypeTag::Enum,
            CanonicalValue::Enum {
                type_id,
                variant_id,
            },
        ) => {
            if value_type.enum_type_id() != Some(*type_id) {
                Some(ValidationCode::TypeMismatch)
            } else if schema
                .enumeration(*type_id)
                .is_none_or(|enumeration| !enumeration.contains_variant(*variant_id))
            {
                Some(ValidationCode::InvalidValue)
            } else {
                None
            }
        }
        (ValueTypeTag::List, CanonicalValue::List(values)) => {
            let Some((element, maximum)) = value_type.list_parts() else {
                return Some(ValidationCode::TypeMismatch);
            };
            if values.len() > maximum {
                return Some(ValidationCode::TooManyItems);
            }
            for (index, item) in values.values().iter().enumerate() {
                let pushed = path.len() < MAX_VALIDATION_PATH_SEGMENTS;
                if pushed {
                    path.push(ValidationPathSegment::ListIndex(index as u32));
                }
                if let Some(code) = validate_value(schema, element, item, path) {
                    return Some(code);
                }
                if pushed {
                    path.pop();
                }
            }
            None
        }
        _ => Some(ValidationCode::TypeMismatch),
    }
}

fn push_issue(issues: &mut Vec<ValidationIssue>, issue: ValidationIssue) {
    if issues.len() < MAX_VALIDATION_ISSUES {
        issues.push(issue);
    }
}

fn field_issue(code: ValidationCode, field_id: FieldId) -> ValidationIssue {
    let path = ValidationPath::new(vec![ValidationPathSegment::Field(field_id)])
        .unwrap_or_else(|_| ValidationPath::root());
    ValidationIssue::new(code, path)
}

fn invalid_root(code: ValidationCode) -> PublicError {
    PublicError::validation(ValidationIssues::one(ValidationIssue::new(
        code,
        ValidationPath::root(),
    )))
}

fn extract_idempotency_key(
    plan: &CommandPlan,
    normalized: &CanonicalRecord,
) -> Result<IdempotencyKey, ()> {
    let field = plan.idempotency_input().ok_or(())?;
    let value = normalized
        .fields()
        .binary_search_by_key(&field, |(candidate, _)| *candidate)
        .ok()
        .map(|index| &normalized.fields()[index].1)
        .ok_or(())?;
    let CanonicalValue::String(value) = value else {
        return Err(());
    };
    IdempotencyKey::new(value.as_str().to_owned()).map_err(|_| ())
}

fn map_committed_outcome(
    outcome: CommittedOutcome,
    expected_plan: &CatalogExecutablePlanRequest,
    internal_failure: impl Fn(InternalDefect) -> ServiceFailure,
    map_declared_outcome: impl FnOnce(OutcomeId, CanonicalRecord) -> ServiceResult<DeclaredOutcomeView>,
) -> ServiceResult<(JournaledCommandResult, ServiceAuditLinkV1)> {
    let completion = match outcome.disposition() {
        CommittedOutcomeDisposition::FirstCommit => JournaledCompletion::Committed,
        CommittedOutcomeDisposition::Replay => JournaledCompletion::Replayed,
    };
    let durability = map_committed_durability(&outcome, &internal_failure)?;
    let stored = outcome.stored_outcome();
    if stored.plan().contract_lineage() != expected_plan.lineage()
        || stored.plan().contract_version() != expected_plan.version()
        || stored.plan().contract_bundle_hash() != expected_plan.bundle_hash()
        || stored.plan().command_id() != expected_plan.command_id()
        || stored.plan().command_plan_hash() != expected_plan.plan_hash()
    {
        return Err(internal_failure(InternalDefect::ProofMismatch));
    }
    let declared_outcome = map_declared_outcome(
        stored.declared_outcome().outcome_id(),
        stored.declared_outcome().value().clone(),
    )?;
    let result = JournaledCommandResult::new(
        completion,
        stored.commit_sequence(),
        declared_outcome,
        stored.provenance_id(),
        durability,
    )
    .map_err(|_| internal_failure(InternalDefect::ProofMismatch))?;
    let link = ServiceAuditLinkV1::Command {
        commit_sequence: result.commit_sequence(),
        provenance_id: result.provenance_id(),
    };
    Ok((result, link))
}

fn map_committed_durability(
    outcome: &CommittedOutcome,
    internal_failure: impl FnOnce(InternalDefect) -> ServiceFailure,
) -> ServiceResult<CommandDurability> {
    match outcome.durability() {
        Ok(CoordinatorDurability::Sync) => Ok(CommandDurability::Synchronous),
        Ok(CoordinatorDurability::Group) => Ok(CommandDurability::Group),
        Err(_) => Err(internal_failure(InternalDefect::ProofMismatch)),
    }
}

fn declared_outcome_view(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    outcome_catalog: &CheckedOutcomeCatalog,
    outcome_id: OutcomeId,
    value: CanonicalRecord,
) -> ServiceResult<DeclaredOutcomeView> {
    let integrity_failure = || {
        if operation == ServiceOperationV1::ResolveCommandOutcome {
            service
                .providers
                .health
                .fail_authoritative_readiness(AuthoritativeReadinessFailure::Integrity);
            service.internal_failure(operation, InternalDefect::LowerIntegrity)
        } else {
            service.internal_failure(operation, InternalDefect::ProofMismatch)
        }
    };
    let schema = outcome_catalog
        .outcomes
        .iter()
        .find(|candidate| candidate.id() == outcome_id)
        .ok_or_else(&integrity_failure)?;
    DeclaredOutcomeView::from_checked_schema(
        outcome_catalog.plan.clone(),
        &outcome_catalog.contract_schema,
        schema,
        value,
    )
    .map_err(|_| integrity_failure())
}

#[derive(Clone, Copy)]
enum TerminalKind {
    Ordinary,
    DurableFailure,
    OutcomeUncertain,
    KnownCommitMappingFailure,
}

impl TerminalKind {
    const fn phase(self, failure: &ServiceFailure) -> ServiceAuditPhaseV1 {
        match self {
            Self::OutcomeUncertain => ServiceAuditPhaseV1::OutcomeUncertain,
            Self::KnownCommitMappingFailure => ServiceAuditPhaseV1::Failed,
            Self::Ordinary | Self::DurableFailure => match failure {
                ServiceFailure::Cancelled | ServiceFailure::DeadlineExceeded => {
                    ServiceAuditPhaseV1::Cancelled
                }
                ServiceFailure::Public(_)
                | ServiceFailure::ResponseTooLarge
                | ServiceFailure::EmergencyInternal(_) => ServiceAuditPhaseV1::Failed,
            },
        }
    }

    const fn audit_failure_is_outcome_unknown(self) -> bool {
        matches!(
            self,
            Self::DurableFailure | Self::OutcomeUncertain | Self::KnownCommitMappingFailure
        )
    }
}

async fn terminate_mutation(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: Option<&BegunInvocation>,
    targets: &ServiceAuditTargetsV1,
    failure: ServiceFailure,
    terminal: TerminalKind,
) -> ServiceFailure {
    if let Some(begun) = begun {
        return finish_failure(service, context, begun, failure, terminal).await;
    }
    if service
        .append_prestart_terminal_if_intrinsic(
            context,
            ServiceOperationV1::ExecuteCommand,
            targets.clone(),
            AuditScope::Intrinsic,
            terminal.phase(&failure),
        )
        .await
        .is_err()
    {
        return PublicError::storage_unavailable().into();
    }
    failure
}

async fn finish_success(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    link: ServiceAuditLinkV1,
    known_durable: bool,
) -> ServiceResult<()> {
    if begun
        .finish(service, context, ServiceAuditPhaseV1::Succeeded, link)
        .await
        .is_err()
    {
        service.note_audit_failure(begun.operation());
        return Err(if known_durable {
            PublicError::outcome_unknown().into()
        } else {
            PublicError::storage_unavailable().into()
        });
    }
    Ok(())
}

async fn finish_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    failure: ServiceFailure,
    terminal: TerminalKind,
) -> ServiceFailure {
    finish_failure_with_link(
        service,
        context,
        begun,
        failure,
        terminal,
        ServiceAuditLinkV1::None,
    )
    .await
}

async fn finish_failure_with_link(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    failure: ServiceFailure,
    terminal: TerminalKind,
    link: ServiceAuditLinkV1,
) -> ServiceFailure {
    if begun
        .finish(service, context, terminal.phase(&failure), link)
        .await
        .is_err()
    {
        service.note_audit_failure(begun.operation());
        return if terminal.audit_failure_is_outcome_unknown() {
            PublicError::outcome_unknown().into()
        } else {
            PublicError::storage_unavailable().into()
        };
    }
    failure
}

fn outcome_authorization_matches(
    service: &RiffDbServiceInner,
    authorization: &AuthorizedOperation,
    expected_request: &OperationRequest,
    expected_partition: Option<&ScopedPartitionV1>,
) -> bool {
    let obligations = authorization.obligations();
    let partition_matches = match (expected_partition, obligations.partition_constraint()) {
        (None, None) => true,
        (Some(expected), Some(PartitionConstraint::Exact(actual))) => actual == expected,
        _ => false,
    };
    authorization.database_id() == service.identity.database_id()
        && authorization.environment() == service.identity.environment()
        && authorization.request() == expected_request
        && authorization.operation() == ServiceOperationV1::ResolveCommandOutcome
        && obligations.effective_tenant_scope() == &TenantScope::Global
        && partition_matches
        && obligations.field_mask().is_none()
        && obligations.row_limit().is_none()
        && obligations.output_classification()
            == OutputClassification::PolicyFilteredApplicationData
}

fn input_error(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: InputPreparationError,
) -> ServiceFailure {
    match error {
        InputPreparationError::Public(error) => error.into(),
        InputPreparationError::Integrity => {
            service.internal_failure(operation, InternalDefect::ProofMismatch)
        }
    }
}

fn evaluation_error(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: EvaluationError,
) -> ServiceFailure {
    match error {
        EvaluationError::Arithmetic => invalid_root(ValidationCode::OutOfRange).into(),
        EvaluationError::Integrity => {
            service.internal_failure(operation, InternalDefect::ProofMismatch)
        }
    }
}

fn map_catalog_error(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: CatalogError,
) -> ServiceFailure {
    match error.kind() {
        CatalogErrorKind::Storage => PublicError::storage_unavailable().into(),
        _ => service.internal_failure(operation, InternalDefect::LowerIntegrity),
    }
}

fn map_controlled_wait(error: ControlledWaitError) -> ServiceFailure {
    match error {
        ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
        ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
    }
}

fn map_port_admission(error: PortAdmissionError) -> ServiceFailure {
    match error {
        PortAdmissionError::Cancelled => ServiceFailure::Cancelled,
        PortAdmissionError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
        PortAdmissionError::Unavailable | PortAdmissionError::Stopped => {
            PublicError::storage_unavailable().into()
        }
    }
}

fn map_authoritative_error(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: AuthoritativeReadError,
) -> ServiceFailure {
    match error {
        AuthoritativeReadError::Unavailable => PublicError::storage_unavailable().into(),
        AuthoritativeReadError::Integrity | AuthoritativeReadError::InvalidContinuation => {
            service.internal_failure(operation, InternalDefect::LowerIntegrity)
        }
    }
}

fn map_command_admission(
    service: &RiffDbServiceInner,
    error: CommandExecutionAdmissionError,
) -> ServiceFailure {
    match error {
        CommandExecutionAdmissionError::Draining | CommandExecutionAdmissionError::Stopped => {
            PublicError::storage_unavailable().into()
        }
        CommandExecutionAdmissionError::Fenced => {
            service.providers.health.fail_authoritative_readiness(
                crate::AuthoritativeReadinessFailure::CoordinatorFenced,
            );
            PublicError::storage_unavailable().into()
        }
    }
}

fn map_idempotency_inspection(
    service: &RiffDbServiceInner,
    kind: CommandIdempotencyInspectionErrorKind,
) -> ServiceFailure {
    match kind {
        CommandIdempotencyInspectionErrorKind::StorageUnavailable
        | CommandIdempotencyInspectionErrorKind::CoordinatorStopped => {
            PublicError::storage_unavailable().into()
        }
        CommandIdempotencyInspectionErrorKind::CoordinatorFenced => {
            service.providers.health.fail_authoritative_readiness(
                crate::AuthoritativeReadinessFailure::CoordinatorFenced,
            );
            PublicError::storage_unavailable().into()
        }
        CommandIdempotencyInspectionErrorKind::InternalDefect => service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::LowerIntegrity,
        ),
    }
}

fn map_command_execution(
    service: &RiffDbServiceInner,
    execution_class: ExecutionClass,
    kind: CommandExecutionErrorKind,
) -> (ServiceFailure, TerminalKind) {
    match kind {
        CommandExecutionErrorKind::Cancelled => (ServiceFailure::Cancelled, TerminalKind::Ordinary),
        CommandExecutionErrorKind::DeadlineExceeded => {
            (ServiceFailure::DeadlineExceeded, TerminalKind::Ordinary)
        }
        CommandExecutionErrorKind::RetryBudgetExhausted => (
            PublicError::concurrency_deadline_exceeded().into(),
            TerminalKind::Ordinary,
        ),
        CommandExecutionErrorKind::StorageUnavailable
        | CommandExecutionErrorKind::CoordinatorStopped => (
            PublicError::storage_unavailable().into(),
            TerminalKind::Ordinary,
        ),
        CommandExecutionErrorKind::OutcomeUnknown => match execution_class {
            ExecutionClass::ReadOnly => (
                service.internal_failure(
                    ServiceOperationV1::ExecuteCommand,
                    InternalDefect::LowerIntegrity,
                ),
                TerminalKind::Ordinary,
            ),
            ExecutionClass::IdempotentMutation => (
                PublicError::outcome_unknown().into(),
                TerminalKind::OutcomeUncertain,
            ),
        },
        CommandExecutionErrorKind::InternalDefect => (
            service.internal_failure(
                ServiceOperationV1::ExecuteCommand,
                InternalDefect::LowerIntegrity,
            ),
            TerminalKind::Ordinary,
        ),
        CommandExecutionErrorKind::CoordinatorFenced => {
            service.providers.health.fail_authoritative_readiness(
                crate::AuthoritativeReadinessFailure::CoordinatorFenced,
            );
            (
                PublicError::storage_unavailable().into(),
                TerminalKind::Ordinary,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
    use riffdb_contract_ir::{ContractBundle, EnumSchema};
    use riffdb_errors::{
        IncidentIdSource, IncidentIdSourceError, InternalError, PublicErrorDetails, PublicErrorKind,
    };
    use riffdb_storage_api::{
        DeclaredOutcome, DurabilityMode, ExecutablePlanRef, IdempotencyIdentity,
        IdempotencyKeyDigest, StoredAdmittedProvenanceClaimsV1, StoredOutcomeV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash,
        CommitSequence, ContractBundleHash, ContractLineage, ContractVersion, DatabaseId,
        DigestKeyId, EnumTypeId, EnumVariantId, Environment, FieldId, IncidentId, LogicalTime,
        OutcomeId, PartitionKeyBuilder, PlanHash, ProvenanceId, RequestId, TenantId, TenantScope,
        Timestamp, hash_partition_key,
    };

    use super::*;

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn memory_outcome() -> CommittedOutcome {
        let lineage = ContractLineage::new("memory-boundary").expect("lineage");
        let command_id = riffdb_types::CommandId::first();
        let plan = ExecutablePlanRef::new(
            lineage.clone(),
            ContractVersion::new(1).expect("contract version"),
            ContractBundleHash::from_bytes([0x41; 32]),
            command_id,
            PlanHash::from_bytes([0x42; 32]),
        );
        let tenant_scope = TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant"));
        let principal = ActorId::new("principal-a").expect("principal");
        let actor = AdmittedActorContext::new(
            principal.clone(),
            ActorKind::Human,
            tenant_scope.clone(),
            None,
        );
        let identity = IdempotencyIdentity::new(
            DatabaseId::from_bytes(uuid_bytes(0x11)).expect("database UUIDv7"),
            Environment::new("test").expect("environment"),
            tenant_scope,
            principal,
            lineage,
            command_id,
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key ID"),
                [0x43; 32],
            ),
        );
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
        partition.push_u64(9).expect("partition component");
        let partition = partition.finish().expect("partition key");
        let stored = StoredOutcomeV1::new(
            identity,
            CommitSequence::first(),
            RequestId::from_bytes(uuid_bytes(0x12)).expect("request UUIDv7"),
            plan,
            CanonicalInputHash::from_bytes([0x44; 32]),
            actor,
            LogicalTime::new(Timestamp::new(-7, 23).expect("timestamp")),
            partition.clone(),
            hash_partition_key(partition.as_bytes()),
            Vec::new(),
            DeclaredOutcome::new(
                OutcomeId::first(),
                CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::Bool(true))])
                    .expect("canonical outcome"),
            )
            .expect("declared outcome"),
            StoredAdmittedProvenanceClaimsV1::default(),
            ProvenanceId::from_bytes(uuid_bytes(0x13)).expect("provenance UUIDv7"),
            DurabilityMode::Memory,
        )
        .expect("valid test-only memory outcome");
        CommittedOutcome::first_commit(stored)
    }

    struct FixedIncidentId(IncidentId);

    impl IncidentIdSource for FixedIncidentId {
        fn next_incident_id(&self) -> Result<IncidentId, IncidentIdSourceError> {
            Ok(self.0)
        }
    }

    #[derive(Default)]
    struct RecordingTelemetry(Mutex<Vec<crate::ServiceTelemetryEvent>>);

    impl crate::ServiceTelemetry for RecordingTelemetry {
        fn record(&self, event: crate::ServiceTelemetryEvent) {
            self.0.lock().expect("telemetry mutex").push(event);
        }
    }

    #[derive(Default)]
    struct RecordingDiagnostics(AtomicUsize);

    impl crate::ServiceDiagnostics for RecordingDiagnostics {
        fn record_internal(&self, _error: InternalError) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[derive(Default)]
    struct RecordingHealth(AtomicUsize);

    impl crate::ServiceHealthHooks for RecordingHealth {
        fn fail_authoritative_readiness(&self, _reason: crate::AuthoritativeReadinessFailure) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[test]
    fn memory_committed_outcome_is_withheld_as_an_incident_backed_internal_defect() {
        let expected_incident = IncidentId::from_bytes(uuid_bytes(0x51)).expect("incident UUIDv7");
        let telemetry = RecordingTelemetry::default();
        let incidents = FixedIncidentId(expected_incident);
        let diagnostics = RecordingDiagnostics::default();
        let health = RecordingHealth::default();
        let outcome = memory_outcome();
        let stored_plan = outcome.stored_outcome().plan();
        let expected_plan = CatalogExecutablePlanRequest::new(
            stored_plan.contract_lineage().clone(),
            stored_plan.contract_version(),
            stored_plan.contract_bundle_hash(),
            stored_plan.command_id(),
            stored_plan.command_plan_hash(),
        );

        let failure = map_committed_outcome(
            outcome,
            &expected_plan,
            |defect| {
                crate::service::contained_internal_failure(
                    &telemetry,
                    &incidents,
                    &diagnostics,
                    &health,
                    ServiceOperationV1::ExecuteCommand,
                    defect,
                )
            },
            |_, _| panic!("Memory durability must fail before a declared result can be mapped"),
        )
        .expect_err("test-only Memory must never release a JournaledCommandResult");

        let public = failure
            .public_error()
            .expect("the injected incident source produces a caller-safe error");
        assert_eq!(public.kind(), PublicErrorKind::InternalDefect);
        assert_eq!(public.incident_id(), Some(&expected_incident));
        assert_eq!(diagnostics.0.load(Ordering::Acquire), 1);
        assert_eq!(health.0.load(Ordering::Acquire), 0);
        assert_eq!(
            telemetry.0.lock().expect("telemetry mutex").as_slice(),
            &[crate::ServiceTelemetryEvent::InternalIntegrity {
                operation: ServiceOperationV1::ExecuteCommand,
            }]
        );
    }

    fn normalization_source(version: u64, optional_note: bool) -> String {
        let optional_note = if optional_note {
            "    input note: optional<string<8>>\n"
        } else {
            ""
        };
        format!(
            r#"
contract InputNormalization version {version} {{
  enum Mode {{ Alpha, Beta }}
  entity Row {{ key (id: uuid) }}
  aggregate Rows {{ root Row partition_by id conflict_key (id) }}
  command Inspect {{
    input id: uuid
    input mode: Mode
    input values: list<i64, 2>
{optional_note}    read Row(id) as row else Missing {{ id: id }}
    return Found {{ row: row }}
  }}
}}
"#,
        )
    }

    fn mutation_source() -> &'static str {
        r#"
contract MutationInput version 1 {
  entity Row { key (id: uuid) field value: i64 }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Change {
    input idempotency_key: string<8>
    input id: uuid
    idempotency_key idempotency_key
    mutate Row(id) as row else Missing { id: id }
    set row.value = 1
    return Changed { row: row }
  }
}
"#
    }

    fn compile_normalization_fixture() -> ContractBundle {
        compile_contract_source(&normalization_source(1, true))
            .expect("normalization fixture compiles")
    }

    fn command(bundle: &ContractBundle) -> &CommandPlan {
        bundle.commands().first().expect("fixture command")
    }

    fn field_id(plan: &CommandPlan, name: &str) -> FieldId {
        plan.input()
            .record()
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .map(riffdb_contract_ir::FieldSchema::id)
            .expect("fixture field")
    }

    fn enumeration(bundle: &ContractBundle) -> &EnumSchema {
        bundle
            .schema()
            .enums()
            .iter()
            .find(|enumeration| enumeration.name() == "Mode")
            .expect("fixture enum")
    }

    fn valid_input(bundle: &ContractBundle, note: Option<CanonicalValue>) -> CanonicalRecord {
        let plan = command(bundle);
        let enumeration = enumeration(bundle);
        let alpha = enumeration
            .variants()
            .iter()
            .find(|variant| variant.name() == "Alpha")
            .expect("fixture variant");
        let mut fields = vec![
            (field_id(plan, "id"), CanonicalValue::Uuid([0x11; 16])),
            (
                field_id(plan, "mode"),
                CanonicalValue::Enum {
                    type_id: enumeration.id(),
                    variant_id: alpha.id(),
                },
            ),
            (
                field_id(plan, "values"),
                CanonicalValue::list(vec![CanonicalValue::I64(7)]).expect("bounded list"),
            ),
        ];
        if let Some(note) = note {
            fields.push((field_id(plan, "note"), note));
        }
        CanonicalRecord::new(fields).expect("canonical fixture input")
    }

    fn issue(error: InputPreparationError) -> ValidationIssue {
        let InputPreparationError::Public(error) = error else {
            panic!("expected public input validation failure");
        };
        let PublicErrorDetails::Validation(issues) = error.details() else {
            panic!("expected validation details");
        };
        assert_eq!(issues.as_slice().len(), 1);
        issues.as_slice()[0].clone()
    }

    fn value(record: &CanonicalRecord, field: FieldId) -> Option<&CanonicalValue> {
        record
            .fields()
            .binary_search_by_key(&field, |(candidate, _)| *candidate)
            .ok()
            .map(|index| &record.fields()[index].1)
    }

    #[test]
    fn omitted_optional_input_is_filled_with_canonical_null() {
        let bundle = compile_normalization_fixture();
        let plan = command(&bundle);
        let note = field_id(plan, "note");
        let submitted = valid_input(&bundle, None);

        let normalized = match normalize_command_input(plan, bundle.schema(), plan, &submitted) {
            Ok(normalized) => normalized,
            Err(_) => panic!("omitted optional field must normalize"),
        };

        assert_eq!(normalized.len(), plan.input().record().fields().len());
        assert!(matches!(
            value(&normalized, note),
            Some(CanonicalValue::Null)
        ));
    }

    #[test]
    fn unknown_field_is_rejected_at_its_stable_field_path() {
        let bundle = compile_normalization_fixture();
        let plan = command(&bundle);
        let unknown = FieldId::new(u32::MAX).expect("nonzero field ID");
        assert!(plan.input().record().field(unknown).is_none());
        let mut fields = valid_input(&bundle, None).fields().to_vec();
        fields.push((unknown, CanonicalValue::Null));
        let submitted = CanonicalRecord::new(fields).expect("canonical input");

        let issue = issue(
            normalize_command_input(plan, bundle.schema(), plan, &submitted)
                .expect_err("unknown field rejects"),
        );

        assert_eq!(issue.code(), ValidationCode::UnknownField);
        assert_eq!(
            issue.path().segments(),
            &[ValidationPathSegment::Field(unknown)]
        );
    }

    #[test]
    fn historical_normalization_omits_a_later_optional_null() {
        let historical = compile_contract_source(&normalization_source(1, false))
            .expect("historical fixture compiles");
        let active = compile_contract_successor(&normalization_source(2, true), &historical)
            .expect("optional successor compiles");
        let historical_plan = command(&historical);
        let active_plan = command(&active);
        let note = field_id(active_plan, "note");
        let submitted = valid_input(&active, Some(CanonicalValue::Null));

        let normalized = match normalize_command_input(
            historical_plan,
            historical.schema(),
            active_plan,
            &submitted,
        ) {
            Ok(normalized) => normalized,
            Err(_) => panic!("later optional null must be omitted for historical hashing"),
        };

        assert_eq!(
            normalized.len(),
            historical_plan.input().record().fields().len()
        );
        assert!(value(&normalized, note).is_none());
    }

    #[test]
    fn historical_normalization_rejects_a_later_optional_non_null_value() {
        let historical = compile_contract_source(&normalization_source(1, false))
            .expect("historical fixture compiles");
        let active = compile_contract_successor(&normalization_source(2, true), &historical)
            .expect("optional successor compiles");
        let historical_plan = command(&historical);
        let active_plan = command(&active);
        let note = field_id(active_plan, "note");
        let submitted = valid_input(
            &active,
            Some(CanonicalValue::string("present").expect("bounded string")),
        );

        let issue = issue(
            normalize_command_input(
                historical_plan,
                historical.schema(),
                active_plan,
                &submitted,
            )
            .expect_err("non-null later field rejects"),
        );

        assert_eq!(issue.code(), ValidationCode::UnknownField);
        assert_eq!(
            issue.path().segments(),
            &[ValidationPathSegment::Field(note)]
        );
    }

    #[test]
    fn enum_type_and_variant_failures_are_distinct() {
        let bundle = compile_normalization_fixture();
        let plan = command(&bundle);
        let mode = field_id(plan, "mode");
        let enumeration = enumeration(&bundle);
        let wrong_type = EnumTypeId::new(u32::MAX).expect("nonzero enum ID");
        let wrong_variant = EnumVariantId::new(u32::MAX).expect("nonzero variant ID");
        assert_ne!(wrong_type, enumeration.id());
        assert!(!enumeration.contains_variant(wrong_variant));

        for (submitted_value, expected_code) in [
            (
                CanonicalValue::Enum {
                    type_id: wrong_type,
                    variant_id: wrong_variant,
                },
                ValidationCode::TypeMismatch,
            ),
            (
                CanonicalValue::Enum {
                    type_id: enumeration.id(),
                    variant_id: wrong_variant,
                },
                ValidationCode::InvalidValue,
            ),
        ] {
            let mut fields = valid_input(&bundle, None).fields().to_vec();
            let index = fields
                .binary_search_by_key(&mode, |(candidate, _)| *candidate)
                .expect("mode field");
            fields[index].1 = submitted_value;
            let submitted = CanonicalRecord::new(fields).expect("canonical input");
            let issue = issue(
                normalize_command_input(plan, bundle.schema(), plan, &submitted)
                    .expect_err("invalid enum rejects"),
            );
            assert_eq!(issue.code(), expected_code);
            assert_eq!(
                issue.path().segments(),
                &[ValidationPathSegment::Field(mode)]
            );
        }
    }

    #[test]
    fn list_element_type_failure_retains_the_element_index_path() {
        let bundle = compile_normalization_fixture();
        let plan = command(&bundle);
        let values = field_id(plan, "values");
        let mut fields = valid_input(&bundle, None).fields().to_vec();
        let index = fields
            .binary_search_by_key(&values, |(candidate, _)| *candidate)
            .expect("values field");
        fields[index].1 =
            CanonicalValue::list(vec![CanonicalValue::I64(1), CanonicalValue::Bool(false)])
                .expect("bounded list");
        let submitted = CanonicalRecord::new(fields).expect("canonical input");

        let issue = issue(
            normalize_command_input(plan, bundle.schema(), plan, &submitted)
                .expect_err("wrong list element type rejects"),
        );

        assert_eq!(issue.code(), ValidationCode::TypeMismatch);
        assert_eq!(
            issue.path().segments(),
            &[
                ValidationPathSegment::Field(values),
                ValidationPathSegment::ListIndex(1),
            ]
        );
    }

    #[test]
    fn list_bound_and_scalar_type_failures_use_the_declared_field_path() {
        let bundle = compile_normalization_fixture();
        let plan = command(&bundle);
        let values = field_id(plan, "values");
        let id = field_id(plan, "id");

        let cases = [
            (
                values,
                CanonicalValue::list(vec![
                    CanonicalValue::I64(1),
                    CanonicalValue::I64(2),
                    CanonicalValue::I64(3),
                ])
                .expect("globally bounded list"),
                ValidationCode::TooManyItems,
            ),
            (id, CanonicalValue::I64(1), ValidationCode::TypeMismatch),
        ];
        for (field, submitted_value, expected_code) in cases {
            let mut fields = valid_input(&bundle, None).fields().to_vec();
            let index = fields
                .binary_search_by_key(&field, |(candidate, _)| *candidate)
                .expect("fixture field");
            fields[index].1 = submitted_value;
            let submitted = CanonicalRecord::new(fields).expect("canonical input");
            let issue = issue(
                normalize_command_input(plan, bundle.schema(), plan, &submitted)
                    .expect_err("invalid value rejects"),
            );
            assert_eq!(issue.code(), expected_code);
            assert_eq!(
                issue.path().segments(),
                &[ValidationPathSegment::Field(field)]
            );
        }
    }

    #[test]
    fn empty_direct_idempotency_key_is_public_invalid_value() {
        let bundle = compile_contract_source(mutation_source()).expect("mutation fixture compiles");
        let plan = command(&bundle);
        let idempotency = field_id(plan, "idempotency_key");
        let submitted = CanonicalRecord::new(vec![
            (
                idempotency,
                CanonicalValue::string("").expect("canonical empty string"),
            ),
            (field_id(plan, "id"), CanonicalValue::Uuid([0x22; 16])),
        ])
        .expect("canonical input");

        let issue = issue(
            normalize_command_input(plan, bundle.schema(), plan, &submitted)
                .expect_err("empty direct idempotency key rejects"),
        );

        assert_eq!(issue.code(), ValidationCode::InvalidValue);
        assert_eq!(
            issue.path().segments(),
            &[ValidationPathSegment::Field(idempotency)]
        );
    }

    #[test]
    fn terminal_kind_preserves_cancelled_and_known_durable_semantics() {
        let public = ServiceFailure::from(PublicError::storage_unavailable());

        assert_eq!(
            TerminalKind::Ordinary.phase(&ServiceFailure::Cancelled),
            ServiceAuditPhaseV1::Cancelled
        );
        assert_eq!(
            TerminalKind::DurableFailure.phase(&public),
            ServiceAuditPhaseV1::Failed
        );
        assert_eq!(
            TerminalKind::KnownCommitMappingFailure.phase(&public),
            ServiceAuditPhaseV1::Failed
        );
        assert!(!TerminalKind::Ordinary.audit_failure_is_outcome_unknown());
        assert!(TerminalKind::DurableFailure.audit_failure_is_outcome_unknown());
        assert!(TerminalKind::OutcomeUncertain.audit_failure_is_outcome_unknown());
        assert!(TerminalKind::KnownCommitMappingFailure.audit_failure_is_outcome_unknown());
    }
}
