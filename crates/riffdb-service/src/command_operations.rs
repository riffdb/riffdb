//! Checked command execution and uncertainty-recovery orchestration.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use riffdb_auth::AuthenticatedPrincipal;
use riffdb_catalog::{
    CatalogError, CatalogErrorKind, ResolvedExecutablePlan, ValidatedContractBundle,
};
use riffdb_commit::{
    CommandExecutionAdmissionError, CommandExecutionErrorKind, CommandExecutionPreparation,
    CommandExecutionResult as CoordinatorCommandResult, CommandIdempotencyConfirmationError,
    CommandIdempotencyInspectionErrorKind, CommandIdempotencyInspectionRequest,
    CommandIdempotencyPlanSelection, CommittedOutcome, CommittedOutcomeDisposition,
    CoordinatorDurability, PostEvaluationAuthorizationError, PostEvaluationCommandAuthorizer,
    ReadOnlyExecutionPreparation, ReadOnlyExecutionResult,
};
use riffdb_contract_ir::{
    CommandPlan, ExecutionClass, McpCommandToolNameV2, RecordSchema, RecordTypeRef, SchemaIr,
    ValueType, ValueTypeTag,
};
use riffdb_errors::{
    MAX_VALIDATION_ISSUES, MAX_VALIDATION_PATH_SEGMENTS, PublicError, ValidationCode,
    ValidationIssue, ValidationIssues, ValidationPath, ValidationPathSegment,
};
use riffdb_invariant::{EvaluationError, derive_input_command_facts};
use riffdb_policy::{
    AgentSessionAdmissionPolicy, AuthorizedCommandExecution, AuthorizedOperation,
    CommandExecutionClass, Decision, OperationRequest, OperationTenantScope, OutputClassification,
    PartitionConstraint, UntrustedInvocationClaims,
};
use riffdb_types::{
    CanonicalCodecError, CanonicalList, CanonicalRecord, CanonicalValue, CommandId, Decimal,
    DecimalSpec, FieldId, IdempotencyKey, MAX_DECIMAL_PRECISION, Money, OutcomeId,
    ScopedPartitionV1, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1,
    ServiceOperationV1, TenantScope, encode_canonical_record,
};

use crate::orchestration::{AuditScope, BegunInvocation};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    AuthoritativeOutcomeRequest, AuthoritativeOutcomeSnapshot, AuthoritativeReadError,
    AuthoritativeReadinessFailure, CatalogExecutablePlanRequest, CommandApplication,
    CommandDurability, CurrentPolicyPort, DeclaredOutcomeView, ExecuteCommandRequest,
    ExecuteCommandResult, InternalDefect, JournaledCommandResult, JournaledCompletion,
    OutcomeLocatorDigestEvidence, OutcomePlanBinding, OutcomeResourceLocator,
    PendingTerminalResponse, PortAdmissionError, PortDriverStopped, ReadOnlyCommandResult,
    RecoveredJournaledCommandResult, RequestContext, ResolveCommandOutcomeRequest,
    ResolveCommandOutcomeResult, ResolveCommandOutcomeSelectorRef, RiffDbService,
    RiffDbServiceInner, ServiceAuditTargetMap, ServiceFailure, ServiceFuture, ServiceResult,
    SubmittedFieldIdentity, SubmittedRecord, SubmittedValue, ensure_response_budget,
};

impl CommandApplication for RiffDbService {
    fn execute_command(
        &self,
        context: RequestContext,
        request: ExecuteCommandRequest,
    ) -> ServiceFuture<'_, ExecuteCommandResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::ExecuteCommand, ingress, async move {
            execute_command(service.as_ref(), context, request).await
        })
    }

    fn resolve_command_outcome(
        &self,
        context: RequestContext,
        request: ResolveCommandOutcomeRequest,
    ) -> ServiceFuture<'_, ResolveCommandOutcomeResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::ResolveCommandOutcome,
            ingress,
            async move { resolve_command_outcome(service.as_ref(), context, request).await },
        )
    }
}

struct ActiveCommand {
    resolved: ResolvedExecutablePlan,
    catalog_request: CatalogExecutablePlanRequest,
}

struct ServicePostEvaluationCommandAuthorizer {
    policy: Arc<dyn CurrentPolicyPort>,
    principal: AuthenticatedPrincipal,
    request: OperationRequest,
    claims: UntrustedInvocationClaims,
    agent_session_policy: AgentSessionAdmissionPolicy,
}

impl PostEvaluationCommandAuthorizer for ServicePostEvaluationCommandAuthorizer {
    fn authorize(&self) -> Result<AuthorizedCommandExecution, PostEvaluationAuthorizationError> {
        match self.policy.authorize(&self.principal, self.request.clone()) {
            Ok(Decision::Allow(authorization)) => authorization
                .into_command_execution(self.claims.clone(), self.agent_session_policy)
                .map_err(|_| PostEvaluationAuthorizationError::Integrity),
            Ok(Decision::Deny(_)) => Err(PostEvaluationAuthorizationError::Denied),
            Ok(Decision::PrepareCapabilityMutation(_)) => {
                Err(PostEvaluationAuthorizationError::Integrity)
            }
            Err(_) => Err(PostEvaluationAuthorizationError::Unavailable),
        }
    }
}

struct CheckedOutcomeCatalog {
    plan: OutcomePlanBinding,
    command_id: CommandId,
    bundle: ValidatedContractBundle,
    tool_name: Option<McpCommandToolNameV2>,
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
            command_id: reference.command_id(),
            bundle: resolved.bundle().clone(),
            tool_name: resolved
                .bundle()
                .bundle()
                .mcp_command_names()
                .get(reference.command_id())
                .map(|entry| entry.tool_name().clone()),
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
        active.resolved.plan().command_id(),
    )
    .map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;

    match active.resolved.plan().execution_class() {
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
    let resolved = snapshot
        .resolve_active_command(plan.command_id(), plan.plan_hash())
        .map_err(|error| map_catalog_error(service, ServiceOperationV1::ExecuteCommand, error))?;
    Ok(ActiveCommand {
        resolved,
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
    let initial = active.resolved;
    let normalized = normalize_command_input(
        initial.plan(),
        initial.bundle().bundle().schema(),
        initial.plan(),
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
    let resolved = initial;
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
        context.ingress(),
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
    let caller_key = match extract_submitted_idempotency_key(
        active.resolved.plan(),
        active.resolved.bundle().bundle().schema(),
        request.input(),
    ) {
        Ok(caller_key) => caller_key,
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
        let selected = match inspection.plan_selection() {
            CommandIdempotencyPlanSelection::Absent => active.resolved.clone(),
            CommandIdempotencyPlanSelection::Historical(_) => match load_plan(
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
            },
        };
        let normalized = match normalize_command_input(
            selected.plan(),
            selected.bundle().bundle().schema(),
            active.resolved.plan(),
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
                    .begin_compound_command_invocation(context, operation.clone(), targets.clone())
                    .await?,
            );
        }

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

        // The selected plan, canonical input, and input-derived facts are
        // immutable exact artifacts. The capacity wait does not grant catalog
        // or policy authority, so retain them and perform the required fresh
        // authorization below instead of loading, cloning, normalizing, and
        // evaluating the same material again.
        let resolved = selected;
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
        let post_evaluation_authorizer = ServicePostEvaluationCommandAuthorizer {
            policy: Arc::clone(&service.providers.policy),
            principal: context.principal().clone(),
            request: operation.clone(),
            claims: context.claims().clone(),
            agent_session_policy: service.identity.agent_session_policy(),
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
        let started = match invocation.compound_started_input(context) {
            Ok(started) => started,
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
            context.ingress(),
            control,
        )
        .and_then(|preparation| preparation.with_audited_lifecycle(Box::new(started)))
        .and_then(|preparation| {
            preparation.with_post_evaluation_authorizer(Box::new(post_evaluation_authorizer))
        }) {
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
                    outcome_catalog.tool_name.as_ref(),
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
                let first_commit = result.completion() == JournaledCompletion::Committed;
                let result = ExecuteCommandResult::Journaled(result);
                let pending = PendingTerminalResponse::new(result, link, ensure_response_budget);
                if first_commit {
                    invocation
                        .confirm_compound_success(pending.terminal())
                        .map_err(|_| {
                            service.internal_failure(
                                ServiceOperationV1::ExecuteCommand,
                                InternalDefect::ProofMismatch,
                            )
                        })?;
                } else {
                    finish_success(service, context, invocation, pending.terminal(), true).await?;
                }
                return pending.into_response();
            }
            Ok(CoordinatorCommandResult::ExecutionFailed(outcome)) => {
                let failure = PublicError::command_execution_failed(outcome.code()).into();
                if outcome.disposition() == CommittedOutcomeDisposition::FirstCommit {
                    invocation.confirm_compound_failure().map_err(|_| {
                        service.internal_failure(
                            ServiceOperationV1::ExecuteCommand,
                            InternalDefect::ProofMismatch,
                        )
                    })?;
                    return Err(failure);
                }
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
    let (lineage, command_id) = match request.selector() {
        ResolveCommandOutcomeSelectorRef::RawKey {
            lineage,
            source_command,
            idempotency_key: _,
        } => {
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
            .map_err(|error| {
                map_catalog_error(service, ServiceOperationV1::ResolveCommandOutcome, error)
            })?
            .ok_or_else(|| invalid_root(ValidationCode::InvalidValue))?;
            if active.pointer().lineage() != lineage {
                return Err(invalid_root(ValidationCode::InvalidValue).into());
            }
            let command = active
                .bundle()
                .bundle()
                .commands()
                .iter()
                .find(|plan| plan.name() == source_command.as_str())
                .filter(|plan| plan.execution_class() == ExecutionClass::IdempotentMutation)
                .ok_or_else(|| invalid_root(ValidationCode::InvalidValue))?;
            (lineage.clone(), command.command_id())
        }
        ResolveCommandOutcomeSelectorRef::Locator(locator) => {
            (locator.lineage().clone(), locator.command_id())
        }
    };
    let targets = ServiceAuditTargetMap::resolve_command_outcome(lineage.clone(), command_id)
        .map_err(|_| {
            service.internal_failure(
                ServiceOperationV1::ResolveCommandOutcome,
                InternalDefect::ProofMismatch,
            )
        })?;
    let policy_request =
        OperationRequest::resolve_command_outcome_pre_lookup(lineage.clone(), command_id);
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
    if matches!(
        request.selector(),
        ResolveCommandOutcomeSelectorRef::Locator(locator)
            if locator.owner_principal_id() != context.principal().principal_id()
    ) {
        return finish_outcome_not_found(service, &context, &begun).await;
    }
    let lower_request = match request.selector() {
        ResolveCommandOutcomeSelectorRef::RawKey {
            lineage,
            source_command: _,
            idempotency_key,
        } => AuthoritativeOutcomeRequest::raw_key(
            lineage.clone(),
            command_id,
            context.principal().principal_id().clone(),
            OperationTenantScope::grammar_v1_global()
                .tenant_scope()
                .clone(),
            idempotency_key.clone(),
        ),
        ResolveCommandOutcomeSelectorRef::Locator(locator) => {
            AuthoritativeOutcomeRequest::digested(
                locator.lineage().clone(),
                locator.command_id(),
                context.principal().principal_id().clone(),
                OperationTenantScope::grammar_v1_global()
                    .tenant_scope()
                    .clone(),
                locator.digest_evidence().clone(),
            )
        }
    };
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
        return finish_outcome_not_found(service, &context, &begun).await;
    };
    let facts = snapshot.facts().clone();
    let facts_mismatch = facts.lineage() != &lineage
        || facts.command_id() != command_id
        || facts.owner_principal_id() != context.principal().principal_id()
        || facts.owner_tenant_scope() != &TenantScope::Global;
    if facts_mismatch
        && matches!(
            request.selector(),
            ResolveCommandOutcomeSelectorRef::Locator(_)
        )
    {
        return finish_outcome_not_found(service, &context, &begun).await;
    }
    if facts_mismatch {
        let failure = service.internal_failure(
            ServiceOperationV1::ResolveCommandOutcome,
            InternalDefect::LowerIntegrity,
        );
        return Err(
            finish_failure(service, &context, &begun, failure, TerminalKind::Ordinary).await,
        );
    }
    if matches!(request.selector(), ResolveCommandOutcomeSelectorRef::Locator(locator)
        if locator.digest_evidence() != facts.locator_digest())
    {
        return finish_outcome_not_found(service, &context, &begun).await;
    }
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
    let recovered_outcome_catalog = CheckedOutcomeCatalog::from_resolved(&resolved);
    let Some(historical_tool_name) = recovered_outcome_catalog.tool_name.as_ref() else {
        let failure = service.internal_failure(
            ServiceOperationV1::ResolveCommandOutcome,
            InternalDefect::LowerIntegrity,
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
    if matches!(request.selector(), ResolveCommandOutcomeSelectorRef::Locator(locator)
        if locator.tool_name() != historical_tool_name.as_str())
    {
        return finish_outcome_not_found(service, &context, &begun).await;
    }
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
            let outcome = match declared_outcome_view(
                service,
                ServiceOperationV1::ResolveCommandOutcome,
                &recovered_outcome_catalog,
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
            let outcome_locator = match request.selector() {
                ResolveCommandOutcomeSelectorRef::RawKey { .. } => {
                    match OutcomeResourceLocator::mint(
                        facts.owner_principal_id().clone(),
                        facts.lineage().clone(),
                        facts.command_id(),
                        historical_tool_name,
                        facts.locator_digest().clone(),
                    ) {
                        Ok(locator) => locator,
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
                    }
                }
                ResolveCommandOutcomeSelectorRef::Locator(locator) => locator.clone(),
            };
            let result = match JournaledCommandResult::new(
                JournaledCompletion::Replayed,
                result.commit_sequence(),
                outcome,
                result.provenance_id(),
                result.durability(),
                outcome_locator,
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

async fn finish_outcome_not_found(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
) -> ServiceResult<ResolveCommandOutcomeResult> {
    let result = ResolveCommandOutcomeResult::NotFound;
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_failure(service, context, begun, failure, TerminalKind::Ordinary).await);
    }
    finish_success(service, context, begun, ServiceAuditLinkV1::None, false).await?;
    Ok(result)
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
    submitted: &SubmittedRecord,
) -> Result<CanonicalRecord, InputPreparationError> {
    if selected.command_id() != active.command_id() {
        return Err(InputPreparationError::Integrity);
    }
    let selected_record = selected.input().record();
    let active_record = active.input().record();
    let field_resolver = SubmittedFieldResolver::new(selected_record, active_record);
    let mut materialized = BTreeMap::new();
    let mut resolved_fields = BTreeSet::new();
    let mut issues = Vec::new();

    for field in submitted.fields() {
        if issues.len() == MAX_VALIDATION_ISSUES {
            break;
        }
        let field_id = match field_resolver.resolve(field.identity()) {
            Ok(field_id) => field_id,
            Err(code) => {
                if let Some(field_id) = field_resolver.known_component(field.identity())
                    && selected_record.field(field_id).is_some()
                {
                    resolved_fields.insert(field_id);
                }
                push_issue(
                    &mut issues,
                    identity_issue(code, field.identity(), &field_resolver),
                );
                continue;
            }
        };
        if !resolved_fields.insert(field_id) {
            push_issue(
                &mut issues,
                field_issue(ValidationCode::DuplicateField, field_id),
            );
            continue;
        }

        let Some(declared) = selected_record.field(field_id) else {
            let compatible_historical_addition = active_record
                .field(field_id)
                .is_some_and(|candidate| candidate.value_type().is_optional())
                && matches!(field.value(), SubmittedValue::Null);
            if !compatible_historical_addition {
                push_issue(
                    &mut issues,
                    field_issue(ValidationCode::UnknownField, field_id),
                );
            }
            continue;
        };

        let mut path = vec![ValidationPathSegment::Field(field_id)];
        match materialize_value(
            selected_schema,
            declared.value_type(),
            field.value(),
            &mut path,
        ) {
            Ok(value) => {
                if selected.idempotency_input() == Some(field_id)
                    && matches!(&value, CanonicalValue::String(value) if value.is_empty())
                {
                    push_issue(
                        &mut issues,
                        field_issue(ValidationCode::InvalidValue, field_id),
                    );
                } else {
                    materialized.insert(field_id, value);
                }
            }
            Err(MaterializationError::Public(code, path)) => {
                push_issue(&mut issues, validation_issue(code, path));
            }
            Err(MaterializationError::Integrity) => {
                return Err(InputPreparationError::Integrity);
            }
        }
    }

    let mut normalized = Vec::with_capacity(selected_record.fields().len());
    for declared in selected_record.fields() {
        if let Some(value) = materialized.remove(&declared.id()) {
            normalized.push((declared.id(), value));
        } else if declared.value_type().is_optional() {
            normalized.push((declared.id(), CanonicalValue::Null));
        } else if !resolved_fields.contains(&declared.id()) {
            push_issue(
                &mut issues,
                field_issue(ValidationCode::MissingRequiredValue, declared.id()),
            );
        }
    }

    if !issues.is_empty() {
        let issues = ValidationIssues::new(issues).map_err(|_| InputPreparationError::Integrity)?;
        return Err(InputPreparationError::Public(PublicError::validation(
            issues,
        )));
    }
    let normalized =
        CanonicalRecord::new(normalized).map_err(|_| InputPreparationError::Integrity)?;
    match encode_canonical_record(&normalized) {
        Ok(_) => Ok(normalized),
        Err(CanonicalCodecError::DocumentTooLarge { .. }) => Err(InputPreparationError::Public(
            invalid_root(ValidationCode::TooLong),
        )),
        Err(_) => Err(InputPreparationError::Integrity),
    }
}

enum MaterializationError {
    Public(ValidationCode, Vec<ValidationPathSegment>),
    Integrity,
}

pub(crate) enum SubmittedValueMaterializationError {
    Public(PublicError),
    Integrity,
}

pub(crate) fn materialize_submitted_value(
    schema: &SchemaIr,
    value_type: &ValueType,
    value: &SubmittedValue,
    path: Vec<ValidationPathSegment>,
) -> Result<CanonicalValue, SubmittedValueMaterializationError> {
    let mut path = path;
    match materialize_value(schema, value_type, value, &mut path) {
        Ok(value) => Ok(value),
        Err(MaterializationError::Public(code, path)) => {
            Err(SubmittedValueMaterializationError::Public(
                PublicError::validation(ValidationIssues::one(validation_issue(code, path))),
            ))
        }
        Err(MaterializationError::Integrity) => Err(SubmittedValueMaterializationError::Integrity),
    }
}

fn materialize_value(
    schema: &SchemaIr,
    value_type: &ValueType,
    value: &SubmittedValue,
    path: &mut Vec<ValidationPathSegment>,
) -> Result<CanonicalValue, MaterializationError> {
    if matches!(value, SubmittedValue::Null) {
        return if value_type.is_optional() {
            Ok(CanonicalValue::Null)
        } else {
            Err(public_materialization(ValidationCode::TypeMismatch, path))
        };
    }
    if let Some(inner) = value_type.optional_inner() {
        return materialize_value(schema, inner, value, path);
    }
    match (value_type.tag(), value) {
        (ValueTypeTag::Bool, SubmittedValue::Bool(value)) => Ok(CanonicalValue::Bool(*value)),
        (ValueTypeTag::I64, SubmittedValue::I64(value)) => Ok(CanonicalValue::I64(*value)),
        (ValueTypeTag::I64, SubmittedValue::U64(value)) => i64::try_from(*value)
            .map(CanonicalValue::I64)
            .map_err(|_| public_materialization(ValidationCode::OutOfRange, path)),
        (ValueTypeTag::U64, SubmittedValue::U64(value)) => Ok(CanonicalValue::U64(*value)),
        (ValueTypeTag::U64, SubmittedValue::I64(value)) => u64::try_from(*value)
            .map(CanonicalValue::U64)
            .map_err(|_| public_materialization(ValidationCode::OutOfRange, path)),
        (ValueTypeTag::Timestamp, SubmittedValue::Timestamp(value)) => {
            Ok(CanonicalValue::Timestamp(*value))
        }
        (ValueTypeTag::Date, SubmittedValue::Date(value)) => Ok(CanonicalValue::Date(*value)),
        (ValueTypeTag::Uuid, SubmittedValue::Uuid(value)) => Ok(CanonicalValue::Uuid(*value)),
        (ValueTypeTag::Uuid, SubmittedValue::String(value)) => parse_natural_uuid(value.as_str())
            .map(CanonicalValue::Uuid)
            .ok_or_else(|| public_materialization(ValidationCode::InvalidValue, path)),
        (ValueTypeTag::Decimal, SubmittedValue::Decimal(actual)) => {
            let spec = value_type
                .decimal_spec()
                .ok_or(MaterializationError::Integrity)?;
            if actual.scale() != spec.scale()
                || actual
                    .precision()
                    .is_some_and(|precision| precision != spec.precision())
            {
                return Err(public_materialization(ValidationCode::OutOfRange, path));
            }
            Decimal::new(spec, actual.coefficient())
                .map(CanonicalValue::Decimal)
                .map_err(|_| public_materialization(ValidationCode::OutOfRange, path))
        }
        (ValueTypeTag::Money, SubmittedValue::Money(actual)) => {
            let currency = value_type
                .currency()
                .ok_or(MaterializationError::Integrity)?;
            if actual.currency() != currency {
                return Err(public_materialization(ValidationCode::InvalidValue, path));
            }
            let spec = DecimalSpec::new(MAX_DECIMAL_PRECISION, 2)
                .map_err(|_| MaterializationError::Integrity)?;
            if actual.amount().scale() != spec.scale()
                || actual
                    .amount()
                    .precision()
                    .is_some_and(|precision| precision != spec.precision())
            {
                return Err(public_materialization(ValidationCode::OutOfRange, path));
            }
            let amount = Decimal::new(spec, actual.amount().coefficient())
                .map_err(|_| public_materialization(ValidationCode::OutOfRange, path))?;
            Ok(CanonicalValue::Money(Money::new(currency, amount)))
        }
        (ValueTypeTag::String, SubmittedValue::String(actual)) => {
            if value_type
                .byte_bound()
                .is_some_and(|maximum| actual.len() > maximum)
            {
                Err(public_materialization(ValidationCode::TooLong, path))
            } else {
                Ok(CanonicalValue::String(actual.clone()))
            }
        }
        (ValueTypeTag::Bytes, SubmittedValue::Bytes(actual)) => {
            if value_type
                .byte_bound()
                .is_some_and(|maximum| actual.len() > maximum)
            {
                Err(public_materialization(ValidationCode::TooLong, path))
            } else {
                Ok(CanonicalValue::Bytes(actual.clone()))
            }
        }
        (ValueTypeTag::Enum, SubmittedValue::Enum(actual)) => {
            let expected_type = value_type
                .enum_type_id()
                .ok_or(MaterializationError::Integrity)?;
            let enumeration = schema
                .enumeration(expected_type)
                .ok_or(MaterializationError::Integrity)?;
            let variant = match (actual.type_id(), actual.variant_id(), actual.name()) {
                (Some(type_id), Some(variant_id), name) => {
                    if type_id != expected_type {
                        return Err(public_materialization(ValidationCode::TypeMismatch, path));
                    }
                    let Some(variant) = enumeration
                        .variants()
                        .iter()
                        .find(|variant| variant.id() == variant_id)
                    else {
                        return Err(public_materialization(ValidationCode::InvalidValue, path));
                    };
                    if name.is_some_and(|name| name.as_str() != variant.name()) {
                        return Err(public_materialization(ValidationCode::InvalidValue, path));
                    }
                    variant
                }
                (None, None, Some(name)) => {
                    let Some(variant) = enumeration
                        .variants()
                        .iter()
                        .find(|variant| variant.name() == name.as_str())
                    else {
                        return Err(public_materialization(ValidationCode::InvalidValue, path));
                    };
                    variant
                }
                _ => return Err(MaterializationError::Integrity),
            };
            Ok(CanonicalValue::Enum {
                type_id: expected_type,
                variant_id: variant.id(),
            })
        }
        (ValueTypeTag::Enum, SubmittedValue::String(actual)) => {
            let expected_type = value_type
                .enum_type_id()
                .ok_or(MaterializationError::Integrity)?;
            let enumeration = schema
                .enumeration(expected_type)
                .ok_or(MaterializationError::Integrity)?;
            let variant = enumeration
                .variants()
                .iter()
                .find(|variant| variant.name() == actual.as_str())
                .ok_or_else(|| public_materialization(ValidationCode::InvalidValue, path))?;
            Ok(CanonicalValue::Enum {
                type_id: expected_type,
                variant_id: variant.id(),
            })
        }
        (ValueTypeTag::List, SubmittedValue::List(values)) => {
            let Some((element, maximum)) = value_type.list_parts() else {
                return Err(MaterializationError::Integrity);
            };
            if values.len() > maximum {
                return Err(public_materialization(ValidationCode::TooManyItems, path));
            }
            let mut materialized = Vec::with_capacity(values.len());
            for (index, item) in values.values().iter().enumerate() {
                let pushed = path.len() < MAX_VALIDATION_PATH_SEGMENTS;
                if pushed {
                    path.push(ValidationPathSegment::ListIndex(index as u32));
                }
                let result = materialize_value(schema, element, item, path);
                if pushed {
                    path.pop();
                }
                materialized.push(result?);
            }
            CanonicalList::new(materialized)
                .map(CanonicalValue::List)
                .map_err(|_| MaterializationError::Integrity)
        }
        (ValueTypeTag::Record, SubmittedValue::Record(record)) => {
            let record_schema =
                resolve_record_schema(schema, value_type).ok_or(MaterializationError::Integrity)?;
            materialize_exact_record(schema, record_schema, record, path)
                .map(CanonicalValue::Record)
        }
        _ => Err(public_materialization(ValidationCode::TypeMismatch, path)),
    }
}

pub(crate) fn parse_natural_uuid(text: &str) -> Option<[u8; 16]> {
    if text.len() != 36
        || text.as_bytes().get(8) != Some(&b'-')
        || text.as_bytes().get(13) != Some(&b'-')
        || text.as_bytes().get(18) != Some(&b'-')
        || text.as_bytes().get(23) != Some(&b'-')
    {
        return None;
    }
    let mut bytes = [0_u8; 16];
    let mut output = 0;
    let mut high = None;
    for byte in text.bytes() {
        if byte == b'-' {
            continue;
        }
        let nibble = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => return None,
        };
        if let Some(high) = high.take() {
            *bytes.get_mut(output)? = high << 4 | nibble;
            output += 1;
        } else {
            high = Some(nibble);
        }
    }
    (output == bytes.len() && high.is_none()).then_some(bytes)
}

fn materialize_exact_record(
    schema: &SchemaIr,
    record_schema: &RecordSchema,
    submitted: &SubmittedRecord,
    path: &mut Vec<ValidationPathSegment>,
) -> Result<CanonicalRecord, MaterializationError> {
    let field_resolver = SubmittedFieldResolver::new(record_schema, record_schema);
    let mut values = BTreeMap::new();
    for field in submitted.fields() {
        let field_id = field_resolver.resolve(field.identity()).map_err(|code| {
            let mut issue_path = path.clone();
            if let Some(field_id) = field.identity().field_id()
                && issue_path.len() < MAX_VALIDATION_PATH_SEGMENTS
            {
                issue_path.push(ValidationPathSegment::Field(field_id));
            }
            MaterializationError::Public(code, issue_path)
        })?;
        if values.contains_key(&field_id) {
            let mut issue_path = path.clone();
            push_path_field(&mut issue_path, field_id);
            return Err(MaterializationError::Public(
                ValidationCode::DuplicateField,
                issue_path,
            ));
        }
        let declared = record_schema
            .field(field_id)
            .ok_or(MaterializationError::Integrity)?;
        let pushed = path.len() < MAX_VALIDATION_PATH_SEGMENTS;
        if pushed {
            path.push(ValidationPathSegment::Field(field_id));
        }
        let result = materialize_value(schema, declared.value_type(), field.value(), path);
        if pushed {
            path.pop();
        }
        values.insert(field_id, result?);
    }

    let mut canonical = Vec::with_capacity(record_schema.fields().len());
    for declared in record_schema.fields() {
        if let Some(value) = values.remove(&declared.id()) {
            canonical.push((declared.id(), value));
        } else if declared.value_type().is_optional() {
            canonical.push((declared.id(), CanonicalValue::Null));
        } else {
            let mut issue_path = path.clone();
            push_path_field(&mut issue_path, declared.id());
            return Err(MaterializationError::Public(
                ValidationCode::MissingRequiredValue,
                issue_path,
            ));
        }
    }
    CanonicalRecord::new(canonical).map_err(|_| MaterializationError::Integrity)
}

fn resolve_record_schema<'a>(
    schema: &'a SchemaIr,
    value_type: &ValueType,
) -> Option<&'a RecordSchema> {
    match value_type.record_ref()? {
        RecordTypeRef::Entity(id) => schema.entity(*id).map(|entity| entity.record()),
        RecordTypeRef::Event(id) => schema.event(*id).map(|event| event.payload()),
        RecordTypeRef::CommandInput(_)
        | RecordTypeRef::CommandOutcome { .. }
        | RecordTypeRef::ProjectionResult(_) => None,
    }
}

struct SubmittedFieldResolver<'a> {
    selected: &'a RecordSchema,
    active: &'a RecordSchema,
    names: BTreeMap<&'a str, FieldId>,
}

impl<'a> SubmittedFieldResolver<'a> {
    fn new(selected: &'a RecordSchema, active: &'a RecordSchema) -> Self {
        let mut names = selected
            .fields()
            .iter()
            .map(|field| (field.name(), field.id()))
            .collect::<BTreeMap<_, _>>();
        for field in active.fields() {
            names.insert(field.name(), field.id());
        }
        Self {
            selected,
            active,
            names,
        }
    }

    fn resolve(&self, identity: &SubmittedFieldIdentity) -> Result<FieldId, ValidationCode> {
        let by_id = identity
            .field_id()
            .filter(|field_id| self.contains_id(*field_id));
        let by_name = identity
            .name()
            .and_then(|name| self.names.get(name.as_str()).copied());

        match (identity.field_id(), identity.name(), by_id, by_name) {
            (Some(_), None, Some(field_id), None) => Ok(field_id),
            (None, Some(_), None, Some(field_id)) => Ok(field_id),
            (Some(_), Some(_), Some(id), Some(named)) if id == named => Ok(id),
            (Some(_), Some(_), None, None)
            | (Some(_), None, None, None)
            | (None, Some(_), None, None) => Err(ValidationCode::UnknownField),
            (Some(_), Some(_), _, _) => Err(ValidationCode::InvalidValue),
            _ => Err(ValidationCode::UnknownField),
        }
    }

    fn known_component(&self, identity: &SubmittedFieldIdentity) -> Option<FieldId> {
        identity
            .field_id()
            .filter(|field_id| self.contains_id(*field_id))
            .or_else(|| {
                identity
                    .name()
                    .and_then(|name| self.names.get(name.as_str()).copied())
            })
    }

    fn contains_id(&self, field_id: FieldId) -> bool {
        self.active.field(field_id).is_some() || self.selected.field(field_id).is_some()
    }
}

fn identity_issue(
    code: ValidationCode,
    identity: &SubmittedFieldIdentity,
    resolver: &SubmittedFieldResolver<'_>,
) -> ValidationIssue {
    let field_id = resolver.known_component(identity).or(identity.field_id());
    field_id.map_or_else(
        || ValidationIssue::new(code, ValidationPath::root()),
        |field_id| field_issue(code, field_id),
    )
}

fn public_materialization(
    code: ValidationCode,
    path: &[ValidationPathSegment],
) -> MaterializationError {
    MaterializationError::Public(code, path.to_vec())
}

fn validation_issue(code: ValidationCode, path: Vec<ValidationPathSegment>) -> ValidationIssue {
    let path = ValidationPath::new(path).unwrap_or_else(|_| ValidationPath::root());
    ValidationIssue::new(code, path)
}

fn push_path_field(path: &mut Vec<ValidationPathSegment>, field_id: FieldId) {
    if path.len() < MAX_VALIDATION_PATH_SEGMENTS {
        path.push(ValidationPathSegment::Field(field_id));
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

fn extract_submitted_idempotency_key(
    plan: &CommandPlan,
    schema: &SchemaIr,
    submitted: &SubmittedRecord,
) -> Result<IdempotencyKey, InputPreparationError> {
    let target = plan
        .idempotency_input()
        .ok_or(InputPreparationError::Integrity)?;
    let record = plan.input().record();
    let resolver = SubmittedFieldResolver::new(record, record);
    let mut candidate = None;
    for field in submitted.fields() {
        match resolver.resolve(field.identity()) {
            Ok(field_id) if field_id == target && candidate.is_none() => {
                candidate = Some(field.value());
            }
            Ok(field_id) if field_id == target => {
                candidate = None;
                break;
            }
            Ok(_) | Err(_) => {}
        }
    }
    if let Some(SubmittedValue::String(value)) = candidate
        && let Ok(key) = IdempotencyKey::new(value.as_str().to_owned())
    {
        return Ok(key);
    }

    let normalized = normalize_command_input(plan, schema, plan, submitted)?;
    extract_idempotency_key(plan, &normalized).map_err(|()| InputPreparationError::Integrity)
}

fn map_committed_outcome(
    outcome: CommittedOutcome,
    expected_plan: &CatalogExecutablePlanRequest,
    tool_name: Option<&McpCommandToolNameV2>,
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
    let tool_name = tool_name.ok_or_else(|| internal_failure(InternalDefect::ProofMismatch))?;
    let digest = stored.identity().caller_key_digest();
    let digest =
        OutcomeLocatorDigestEvidence::new(digest.scheme(), digest.key_id(), *digest.as_bytes())
            .map_err(|_| internal_failure(InternalDefect::ProofMismatch))?;
    let outcome_locator = OutcomeResourceLocator::mint(
        stored.identity().principal_id().clone(),
        stored.plan().contract_lineage().clone(),
        stored.plan().command_id(),
        tool_name,
        digest,
    )
    .map_err(|_| internal_failure(InternalDefect::ProofMismatch))?;
    let result = JournaledCommandResult::new(
        completion,
        stored.commit_sequence(),
        declared_outcome,
        stored.provenance_id(),
        durability,
        outcome_locator,
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
    let contract = outcome_catalog.bundle.bundle();
    let schema = contract
        .commands()
        .iter()
        .find(|command| command.command_id() == outcome_catalog.command_id)
        .and_then(|command| {
            command
                .outcomes()
                .iter()
                .find(|candidate| candidate.id() == outcome_id)
        })
        .ok_or_else(&integrity_failure)?;
    DeclaredOutcomeView::from_checked_schema(
        outcome_catalog.plan.clone(),
        contract.schema(),
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
        AuthoritativeReadError::Cancelled => ServiceFailure::Cancelled,
        AuthoritativeReadError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
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
        CommandExecutionAdmissionError::RetainedByteCapacityExceeded
        | CommandExecutionAdmissionError::Draining
        | CommandExecutionAdmissionError::Stopped => PublicError::storage_unavailable().into(),
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
        CommandExecutionErrorKind::AuthorizationDenied => (
            PublicError::authorization_denied().into(),
            TerminalKind::Ordinary,
        ),
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
    use riffdb_contract_ir::{
        ContractBundle, EnumSchema, FieldSchema, MAX_DECLARATIONS_PER_KIND, RecordSchema,
        RecordTypeRef, ValueType,
    };
    use riffdb_errors::{
        IncidentIdSource, IncidentIdSourceError, InternalError, PublicErrorDetails, PublicErrorKind,
    };
    use riffdb_storage_api::{
        DeclaredOutcome, DurabilityMode, ExecutablePlanRef, IdempotencyIdentity,
        IdempotencyKeyDigest, StoredAdmittedProvenanceClaimsV1, StoredOutcomeV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash, CommandId,
        CommitSequence, ContractBundleHash, ContractLineage, ContractVersion, CurrencyCode,
        DatabaseId, DigestKeyId, EnumTypeId, EnumVariantId, Environment, FieldId, IncidentId,
        LogicalTime, OutcomeId, PartitionKeyBuilder, PlanHash, ProvenanceId, RequestId, TenantId,
        TenantScope, Timestamp, hash_command_input, hash_partition_key,
    };

    use crate::{SourceName, SubmittedDecimal, SubmittedEnum, SubmittedField, SubmittedMoney};

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
            None,
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
    input amount: decimal<28,2>
    input price: money<USD>
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

    fn large_decimal_list_source() -> &'static str {
        r#"
contract LargeDecimalInput version 1 {
  entity Row { key (id: uuid) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Inspect {
    input id: uuid
    input values: list<decimal<38,0>, 65535>
    read Row(id) as row else Missing { id: id }
    return Found { row: row }
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
            (
                field_id(plan, "amount"),
                CanonicalValue::Decimal(
                    Decimal::new(DecimalSpec::new(28, 2).expect("decimal spec"), 1_234)
                        .expect("decimal value"),
                ),
            ),
            (
                field_id(plan, "price"),
                CanonicalValue::Money(Money::new(
                    CurrencyCode::new("USD").expect("currency"),
                    Decimal::new(
                        DecimalSpec::new(MAX_DECIMAL_PRECISION, 2).expect("money spec"),
                        5_678,
                    )
                    .expect("money value"),
                )),
            ),
        ];
        if let Some(note) = note {
            fields.push((field_id(plan, "note"), note));
        }
        CanonicalRecord::new(fields).expect("canonical fixture input")
    }

    fn submitted_input(input: CanonicalRecord) -> SubmittedRecord {
        SubmittedRecord::try_from(input).expect("bounded submitted input")
    }

    fn replace_submitted_value(
        submitted: &SubmittedRecord,
        field_id: FieldId,
        value: SubmittedValue,
    ) -> SubmittedRecord {
        let fields = submitted
            .fields()
            .iter()
            .map(|field| {
                if field.identity().field_id() == Some(field_id) {
                    SubmittedField::new(field.identity().clone(), value.clone())
                } else {
                    field.clone()
                }
            })
            .collect();
        SubmittedRecord::new(fields).expect("bounded replacement input")
    }

    fn validation_issues(error: InputPreparationError) -> Vec<ValidationIssue> {
        let InputPreparationError::Public(error) = error else {
            panic!("expected public input validation failure");
        };
        let PublicErrorDetails::Validation(issues) = error.details() else {
            panic!("expected validation details");
        };
        issues.as_slice().to_vec()
    }

    fn issue(error: InputPreparationError) -> ValidationIssue {
        let issues = validation_issues(error);
        assert_eq!(issues.len(), 1);
        issues[0].clone()
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
        let submitted = submitted_input(valid_input(&bundle, None));

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
    fn field_id_name_and_redundant_identities_materialize_identically() {
        let bundle = compile_normalization_fixture();
        let plan = command(&bundle);
        let canonical = valid_input(&bundle, None);
        let by_id = submitted_input(canonical.clone());
        let make_named = |redundant: bool| {
            let fields = canonical
                .fields()
                .iter()
                .map(|(field_id, value)| {
                    let declared = plan
                        .input()
                        .record()
                        .field(*field_id)
                        .expect("declared field");
                    let name = SourceName::new(declared.name()).expect("source name");
                    let identity = if redundant {
                        SubmittedFieldIdentity::IdAndName {
                            id: *field_id,
                            name,
                        }
                    } else {
                        SubmittedFieldIdentity::Name(name)
                    };
                    SubmittedField::new(
                        identity,
                        SubmittedValue::try_from(value.clone()).expect("bounded submitted value"),
                    )
                })
                .collect();
            SubmittedRecord::new(fields).expect("bounded named input")
        };

        let expected = normalize_command_input(plan, bundle.schema(), plan, &by_id)
            .unwrap_or_else(|_| panic!("ID input materializes"));
        for submitted in [make_named(false), make_named(true)] {
            assert_eq!(
                normalize_command_input(plan, bundle.schema(), plan, &submitted)
                    .unwrap_or_else(|_| panic!("named input materializes")),
                expected
            );
        }
    }

    #[test]
    fn natural_json_scalars_materialize_from_the_declared_command_schema() {
        let bundle = compile_normalization_fixture();
        let plan = command(&bundle);
        let canonical = valid_input(&bundle, None);
        let fields = canonical
            .fields()
            .iter()
            .map(|(field_id, value)| {
                let declared = plan
                    .input()
                    .record()
                    .field(*field_id)
                    .expect("declared field");
                let value = match declared.name() {
                    "id" => SubmittedValue::string("11111111-1111-1111-1111-111111111111")
                        .expect("bounded UUID string"),
                    "mode" => SubmittedValue::string("Alpha").expect("bounded enum string"),
                    "values" => SubmittedValue::list(vec![SubmittedValue::U64(7)])
                        .expect("bounded natural integer list"),
                    _ => SubmittedValue::try_from(value.clone()).expect("bounded submitted scalar"),
                };
                SubmittedField::new(
                    SubmittedFieldIdentity::Name(
                        SourceName::new(declared.name()).expect("source name"),
                    ),
                    value,
                )
            })
            .collect();
        let submitted = SubmittedRecord::new(fields).expect("bounded natural record");

        let normalized = normalize_command_input(plan, bundle.schema(), plan, &submitted)
            .unwrap_or_else(|_| panic!("schema-directed natural values materialize"));

        assert_eq!(
            value(&normalized, field_id(plan, "id")),
            Some(&CanonicalValue::Uuid([0x11; 16]))
        );
        assert!(matches!(
            value(&normalized, field_id(plan, "mode")),
            Some(CanonicalValue::Enum { .. })
        ));
        assert_eq!(
            value(&normalized, field_id(plan, "values")),
            Some(
                &CanonicalValue::list(vec![CanonicalValue::I64(7)])
                    .expect("bounded canonical list")
            )
        );
    }

    #[test]
    fn natural_integer_coercion_is_exact_and_fails_closed_on_range() {
        let bundle = compile_normalization_fixture();
        let plan = command(&bundle);
        let values = field_id(plan, "values");
        let submitted = submitted_input(valid_input(&bundle, None));
        let out_of_range = replace_submitted_value(
            &submitted,
            values,
            SubmittedValue::list(vec![SubmittedValue::U64(
                u64::try_from(i64::MAX).expect("positive maximum") + 1,
            )])
            .expect("bounded natural integer list"),
        );

        let failure = issue(
            normalize_command_input(plan, bundle.schema(), plan, &out_of_range)
                .expect_err("out-of-range natural integer must fail"),
        );
        assert_eq!(failure.code(), ValidationCode::OutOfRange);
    }

    #[test]
    fn field_id_and_name_representations_produce_the_same_canonical_input_hash() {
        let bundle = compile_contract_source(mutation_source()).expect("mutation fixture compiles");
        let plan = command(&bundle);
        let idempotency = field_id(plan, "idempotency_key");
        let id = field_id(plan, "id");
        let fields = [
            (
                idempotency,
                SubmittedValue::string("key-1").expect("bounded caller key"),
            ),
            (id, SubmittedValue::Uuid([0x22; 16])),
        ];
        let submitted = |by_name: bool| {
            SubmittedRecord::new(
                fields
                    .iter()
                    .map(|(field_id, value)| {
                        let identity = if by_name {
                            let name = plan
                                .input()
                                .record()
                                .field(*field_id)
                                .expect("declared field")
                                .name();
                            SubmittedFieldIdentity::Name(
                                SourceName::new(name).expect("source name"),
                            )
                        } else {
                            SubmittedFieldIdentity::Id(*field_id)
                        };
                        SubmittedField::new(identity, value.clone())
                    })
                    .collect(),
            )
            .expect("bounded submitted input")
        };
        let normalize = |submitted: SubmittedRecord| {
            normalize_command_input(plan, bundle.schema(), plan, &submitted)
                .unwrap_or_else(|_| panic!("input materializes"))
        };
        let by_id = normalize(submitted(false));
        let by_name = normalize(submitted(true));
        let input_hash = |normalized: &CanonicalRecord| {
            let hash_record = CanonicalRecord::new(
                normalized
                    .fields()
                    .iter()
                    .filter(|(field_id, _)| *field_id != idempotency)
                    .cloned()
                    .collect(),
            )
            .expect("canonical hash record");
            hash_command_input(
                &encode_canonical_record(&hash_record).expect("encodable canonical input"),
            )
        };

        assert_eq!(input_hash(&by_id), input_hash(&by_name));
    }

    #[test]
    fn disagreeing_and_duplicate_resolved_field_identities_are_rejected() {
        let bundle = compile_normalization_fixture();
        let plan = command(&bundle);
        let id = field_id(plan, "id");
        let mode = field_id(plan, "mode");
        let valid = submitted_input(valid_input(&bundle, None));

        let mut mismatched = valid.fields().to_vec();
        let id_index = mismatched
            .iter()
            .position(|field| field.identity().field_id() == Some(id))
            .expect("ID field");
        mismatched[id_index] = SubmittedField::new(
            SubmittedFieldIdentity::IdAndName {
                id,
                name: SourceName::new("mode").expect("source name"),
            },
            SubmittedValue::Uuid([0x11; 16]),
        );
        let mismatch = SubmittedRecord::new(mismatched).expect("bounded mismatch");
        assert_eq!(
            issue(
                normalize_command_input(plan, bundle.schema(), plan, &mismatch)
                    .expect_err("mismatched redundant identity rejects")
            )
            .code(),
            ValidationCode::InvalidValue
        );

        let mut duplicate = valid.fields().to_vec();
        duplicate.push(SubmittedField::new(
            SubmittedFieldIdentity::Name(SourceName::new("mode").expect("source name")),
            SubmittedValue::Enum(SubmittedEnum::new(
                enumeration(&bundle).id(),
                enumeration(&bundle).variants()[0].id(),
                None,
            )),
        ));
        let duplicate = SubmittedRecord::new(duplicate).expect("bounded duplicate");
        let duplicate_issue = issue(
            normalize_command_input(plan, bundle.schema(), plan, &duplicate)
                .expect_err("duplicate resolved field rejects"),
        );
        assert_eq!(duplicate_issue.code(), ValidationCode::DuplicateField);
        assert_eq!(
            duplicate_issue.path().segments(),
            &[ValidationPathSegment::Field(mode)]
        );
    }

    #[test]
    fn decimal_and_money_materialization_uses_exact_declared_types() {
        let bundle = compile_normalization_fixture();
        let plan = command(&bundle);
        let valid = submitted_input(valid_input(&bundle, None));
        let amount = field_id(plan, "amount");
        let price = field_id(plan, "price");

        for (field, value, expected_code) in [
            (
                amount,
                SubmittedValue::Decimal(SubmittedDecimal::new(1_234, 3).expect("scale")),
                ValidationCode::OutOfRange,
            ),
            (
                amount,
                SubmittedValue::Decimal(
                    SubmittedDecimal::new(i128::MAX, 2).expect("structural decimal"),
                ),
                ValidationCode::OutOfRange,
            ),
            (
                price,
                SubmittedValue::Money(SubmittedMoney::new(
                    CurrencyCode::new("EUR").expect("currency"),
                    SubmittedDecimal::new(5_678, 2).expect("money amount"),
                )),
                ValidationCode::InvalidValue,
            ),
            (
                price,
                SubmittedValue::Money(SubmittedMoney::new(
                    CurrencyCode::new("USD").expect("currency"),
                    SubmittedDecimal::new(5_678, 3).expect("money amount"),
                )),
                ValidationCode::OutOfRange,
            ),
        ] {
            let submitted = replace_submitted_value(&valid, field, value);
            let materialization_issue = issue(
                normalize_command_input(plan, bundle.schema(), plan, &submitted)
                    .expect_err("invalid exact numeric type rejects"),
            );
            assert_eq!(materialization_issue.code(), expected_code);
            assert_eq!(
                materialization_issue.path().segments(),
                &[ValidationPathSegment::Field(field)]
            );
        }
    }

    #[test]
    fn query_component_materializer_reuses_declared_scalar_rules_and_paths() {
        let bundle = compile_normalization_fixture();
        let plan = command(&bundle);
        let amount = plan
            .input()
            .record()
            .field(field_id(plan, "amount"))
            .expect("declared amount field");
        let path = vec![ValidationPathSegment::ListIndex(2)];

        let value = materialize_submitted_value(
            bundle.schema(),
            amount.value_type(),
            &SubmittedValue::Decimal(SubmittedDecimal::new(1_234, 2).expect("submitted decimal")),
            path.clone(),
        )
        .unwrap_or_else(|_| panic!("matching query component materializes"));
        assert!(matches!(value, CanonicalValue::Decimal(_)));

        let failure = materialize_submitted_value(
            bundle.schema(),
            amount.value_type(),
            &SubmittedValue::Decimal(SubmittedDecimal::new(1_234, 3).expect("submitted decimal")),
            path.clone(),
        )
        .expect_err("wrong decimal scale is caller-correctable");
        let SubmittedValueMaterializationError::Public(error) = failure else {
            panic!("wrong query scalar must not become an integrity failure");
        };
        let PublicErrorDetails::Validation(issues) = error.details() else {
            panic!("query materialization uses validation details");
        };
        assert_eq!(issues.as_slice().len(), 1);
        assert_eq!(issues.as_slice()[0].code(), ValidationCode::OutOfRange);
        assert_eq!(issues.as_slice()[0].path().segments(), path);
    }

    #[test]
    fn enum_display_name_must_agree_with_the_selected_variant() {
        let bundle = compile_normalization_fixture();
        let plan = command(&bundle);
        let mode = field_id(plan, "mode");
        let enumeration = enumeration(&bundle);
        let alpha = enumeration
            .variants()
            .iter()
            .find(|variant| variant.name() == "Alpha")
            .expect("Alpha variant");
        let matching = replace_submitted_value(
            &submitted_input(valid_input(&bundle, None)),
            mode,
            SubmittedValue::Enum(SubmittedEnum::new(
                enumeration.id(),
                alpha.id(),
                Some(SourceName::new("Alpha").expect("variant name")),
            )),
        );
        let normalized = normalize_command_input(plan, bundle.schema(), plan, &matching)
            .unwrap_or_else(|_| panic!("matching enum display name materializes"));
        assert!(matches!(
            value(&normalized, mode),
            Some(CanonicalValue::Enum { type_id, variant_id })
                if *type_id == enumeration.id() && *variant_id == alpha.id()
        ));

        let mismatching = replace_submitted_value(
            &matching,
            mode,
            SubmittedValue::Enum(SubmittedEnum::new(
                enumeration.id(),
                alpha.id(),
                Some(SourceName::new("Beta").expect("variant name")),
            )),
        );

        assert_eq!(
            issue(
                normalize_command_input(plan, bundle.schema(), plan, &mismatching)
                    .expect_err("enum display-name mismatch rejects")
            )
            .code(),
            ValidationCode::InvalidValue
        );
    }

    #[test]
    fn unknown_field_is_rejected_at_its_stable_field_path() {
        let bundle = compile_normalization_fixture();
        let plan = command(&bundle);
        let unknown = FieldId::new(u32::MAX).expect("nonzero field ID");
        assert!(plan.input().record().field(unknown).is_none());
        let mut fields = valid_input(&bundle, None).fields().to_vec();
        fields.push((unknown, CanonicalValue::Null));
        let submitted = submitted_input(CanonicalRecord::new(fields).expect("canonical input"));

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
    fn field_name_resolution_is_indexed_at_maximum_schema_and_request_counts() {
        let record = RecordSchema::new(
            RecordTypeRef::CommandInput(CommandId::first()),
            (1..=MAX_DECLARATIONS_PER_KIND)
                .map(|raw| {
                    FieldSchema::new(
                        FieldId::new(raw as u32).expect("nonzero field ID"),
                        format!("known_{raw}"),
                        ValueType::bool(),
                    )
                    .expect("checked field")
                })
                .collect(),
        )
        .expect("maximum-size record schema");
        let resolver = SubmittedFieldResolver::new(&record, &record);

        for raw in 0..riffdb_types::MAX_RECORD_FIELDS {
            let identity = SubmittedFieldIdentity::Name(
                SourceName::new(format!("unknown_{raw}")).expect("source name"),
            );
            assert_eq!(
                resolver.resolve(&identity),
                Err(ValidationCode::UnknownField)
            );
        }
    }

    #[test]
    fn command_input_validation_stops_at_the_diagnostic_limit() {
        let bundle = compile_normalization_fixture();
        let plan = command(&bundle);
        let submitted = SubmittedRecord::new(
            (0..(MAX_VALIDATION_ISSUES * 4))
                .map(|raw| {
                    SubmittedField::new(
                        SubmittedFieldIdentity::Name(
                            SourceName::new(format!("unknown_{raw}")).expect("source name"),
                        ),
                        SubmittedValue::Null,
                    )
                })
                .collect(),
        )
        .expect("bounded unknown-name input");

        let issues = validation_issues(
            normalize_command_input(plan, bundle.schema(), plan, &submitted)
                .expect_err("unknown fields reject"),
        );
        assert_eq!(issues.len(), MAX_VALIDATION_ISSUES);
        assert!(
            issues
                .iter()
                .all(|issue| issue.code() == ValidationCode::UnknownField)
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
        let submitted = submitted_input(valid_input(&active, Some(CanonicalValue::Null)));

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
        let submitted = submitted_input(valid_input(
            &active,
            Some(CanonicalValue::string("present").expect("bounded string")),
        ));

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
            let submitted = submitted_input(CanonicalRecord::new(fields).expect("canonical input"));
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
        let submitted = submitted_input(CanonicalRecord::new(fields).expect("canonical input"));

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
            let submitted = submitted_input(CanonicalRecord::new(fields).expect("canonical input"));
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
    fn materialized_canonical_document_overflow_is_public_root_too_long() {
        const DECIMAL_COUNT: usize = 55_000;

        let bundle = compile_contract_source(large_decimal_list_source())
            .expect("large decimal-list fixture compiles");
        let plan = command(&bundle);
        let values = SubmittedValue::list(vec![
            SubmittedValue::Decimal(
                SubmittedDecimal::new(0, 0).expect("structural decimal"),
            );
            DECIMAL_COUNT
        ])
        .expect("submitted representation remains within one MiB");
        let submitted = SubmittedRecord::new(vec![
            SubmittedField::new(
                SubmittedFieldIdentity::Id(field_id(plan, "id")),
                SubmittedValue::Uuid([0x11; 16]),
            ),
            SubmittedField::new(SubmittedFieldIdentity::Id(field_id(plan, "values")), values),
        ])
        .expect("submitted record remains within one MiB");

        let issue = issue(
            normalize_command_input(plan, bundle.schema(), plan, &submitted)
                .expect_err("unencodable canonical document must reject before downstream use"),
        );
        assert_eq!(issue.code(), ValidationCode::TooLong);
        assert!(issue.path().segments().is_empty());
    }

    #[test]
    fn empty_direct_idempotency_key_is_public_invalid_value() {
        let bundle = compile_contract_source(mutation_source()).expect("mutation fixture compiles");
        let plan = command(&bundle);
        let idempotency = field_id(plan, "idempotency_key");
        let submitted = submitted_input(
            CanonicalRecord::new(vec![
                (
                    idempotency,
                    CanonicalValue::string("").expect("canonical empty string"),
                ),
                (field_id(plan, "id"), CanonicalValue::Uuid([0x22; 16])),
            ])
            .expect("canonical input"),
        );

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
