//! Fenced multi-step durable saga orchestration and step recovery (`ADR-0334`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use riffdb_auth::AuthenticatedPrincipal;
use riffdb_catalog::ValidatedContractBundle;
use riffdb_commit::{
    CommandExecutionPreparation, CommandExecutionResult as CoordinatorCommandResult,
    CommandIdempotencyConfirmationError, CommandIdempotencyInspectionRequest,
    CommandIdempotencyPlanSelection,
};
use riffdb_contract_ir::{
    BindingId, ExecutionClass, ExprId, RoleSchemaV1, SagaBranchTransitionV1, SagaCommandCallPlanV1,
    SagaOutcomeEffectClassV1, SagaPlanV1, SagaTerminalOutcomePlanV1, ValueTypeTag,
    saga_step_outcome_field_id,
};
use riffdb_errors::{
    PublicError, ValidationCode, ValidationIssue, ValidationIssues, ValidationPath,
};
use riffdb_invariant::{ExpressionValueSource, derive_input_command_facts, evaluate_expression};
use riffdb_policy::{CommandExecutionClass, Decision, OperationRequest, UntrustedInvocationClaims};
use riffdb_types::{
    CanonicalRecord, CanonicalString, CanonicalValue, CommittedSagaStepRecordV1, FieldId,
    OutcomeId, PendingStepInvocationV1, RequestId, SagaAdmissionStorageOutcomeV1, SagaCasOutcomeV1,
    SagaCompareExpectedV1, SagaFencingToken, SagaId, SagaLeaseStateV1, SagaLifecycleStatusV1,
    SagaRetainedAuthorityV1, SagaRevision, SagaStepDirectionV1, SagaStepEffectDispositionV1,
    SagaTerminalOutcomeRecordV1, ServiceIngressKindV1, ServiceOperationV1, StoredSagaInstanceV1,
    Timestamp, compute_saga_partition_hash, decode_canonical_value,
    derive_saga_step_idempotency_key, derive_saga_step_idempotency_uuid,
    encode_canonical_command_input, encode_canonical_record, format_saga_delegated_actor_id,
};

use crate::command_operations::{
    ServicePostEvaluationCommandAuthorizer, admit_command_capacity, command_operation, load_plan,
    normalize_saga_input,
};
use crate::service::{InternalDefect, RiffDbServiceInner};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    AuthoritativeOutcomeRequest, AuthoritativeOutcomeSnapshot, AwaitSagaRequest, CancelSagaRequest,
    CatalogExecutablePlanRequest, GetSagaRequest, GetSagaResult, PortDriverStopped, RequestContext,
    RequestControl, ResumeSagaRequest, RiffDbService, SagaApplication, SagaCoordinationPort,
    SagaInstanceView, SagaPortError, ServiceFailure, ServiceFuture, ServiceResult,
    StartSagaDisposition, StartSagaRequest, StartSagaResult, ensure_response_budget,
};

const SAGA_INLINE_LEASE_SECS: i64 = 30;
const SAGA_AWAIT_POLL_INTERVAL: Duration = Duration::from_millis(25);
const MAX_SAGA_INLINE_TRANSITIONS: usize = 64;

impl SagaApplication for RiffDbService {
    fn start_saga(
        &self,
        context: RequestContext,
        request: StartSagaRequest,
    ) -> ServiceFuture<'_, StartSagaResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            context.control().fork(),
            ServiceOperationV1::ExecuteCommand,
            ingress,
            async move { start_saga_impl(service, context, request).await },
        )
    }

    fn get_saga(
        &self,
        context: RequestContext,
        request: GetSagaRequest,
    ) -> ServiceFuture<'_, GetSagaResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            context.control().fork(),
            ServiceOperationV1::ResolveCommandOutcome,
            ingress,
            async move { get_saga_impl(service, context, request).await },
        )
    }

    fn await_saga(
        &self,
        context: RequestContext,
        request: AwaitSagaRequest,
    ) -> ServiceFuture<'_, GetSagaResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            context.control().fork(),
            ServiceOperationV1::ResolveCommandOutcome,
            ingress,
            async move { await_saga_impl(service, context, request).await },
        )
    }

    fn cancel_saga(
        &self,
        context: RequestContext,
        request: CancelSagaRequest,
    ) -> ServiceFuture<'_, GetSagaResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            context.control().fork(),
            ServiceOperationV1::ExecuteCommand,
            ingress,
            async move { cancel_saga_impl(service, context, request).await },
        )
    }

    fn resume_saga(
        &self,
        context: RequestContext,
        request: ResumeSagaRequest,
    ) -> ServiceFuture<'_, GetSagaResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            context.control().fork(),
            ServiceOperationV1::ExecuteCommand,
            ingress,
            async move { resume_saga_impl(service, context, request).await },
        )
    }
}

impl RiffDbService {
    /// Drives one already-claimed durable saga instance through bounded `PrepareSagaStep` / `AdvanceSagaStep` turns.
    pub fn advance_claimed_saga(
        &self,
        instance: StoredSagaInstanceV1,
        worker_now: Timestamp,
    ) -> ServiceFuture<'_, StoredSagaInstanceV1> {
        let service = Arc::clone(&self.inner);
        Box::pin(async move {
            let (control, _cancel) = RequestControl::new(
                Instant::now() + Duration::from_secs(SAGA_INLINE_LEASE_SECS as u64),
            );
            advance_saga_loop(&service, &control, instance, worker_now).await
        })
    }
}

fn saga_port(service: &RiffDbServiceInner) -> ServiceResult<&Arc<dyn SagaCoordinationPort>> {
    if service.executors.is_follower() {
        return Err(PublicError::follower_mode().into());
    }
    service
        .providers
        .saga_coordinator
        .as_ref()
        .ok_or_else(|| PublicError::storage_unavailable().into())
}

fn map_saga_port_error(service: &RiffDbServiceInner, error: SagaPortError) -> ServiceFailure {
    match error {
        SagaPortError::Unavailable | SagaPortError::SequenceExhausted => {
            PublicError::storage_unavailable().into()
        }
        SagaPortError::Integrity => service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::LowerIntegrity,
        ),
    }
}

fn map_wait_error(error: ControlledWaitError) -> ServiceFailure {
    match error {
        ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
        ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
    }
}

fn invalid_saga_request(code: ValidationCode) -> PublicError {
    PublicError::validation(ValidationIssues::one(ValidationIssue::new(
        code,
        ValidationPath::root(),
    )))
}

fn authorize_saga_role(
    service: &RiffDbServiceInner,
    principal: &AuthenticatedPrincipal,
    lineage: &riffdb_types::ContractLineage,
    role: &RoleSchemaV1,
) -> ServiceResult<()> {
    for command_id in role.commands() {
        let op = OperationRequest::resolve_command_outcome_pre_lookup(lineage.clone(), *command_id);
        match service.providers.policy.authorize(principal, op) {
            Ok(Decision::Allow(_)) => {}
            Ok(Decision::Deny(_)) | Ok(Decision::PrepareCapabilityMutation(_)) => {
                return Err(PublicError::authorization_denied().into());
            }
            Err(_) => return Err(PublicError::storage_unavailable().into()),
        }
    }
    Ok(())
}

fn sample_logical_now(principal: &AuthenticatedPrincipal) -> Timestamp {
    principal.authenticated_at()
}

async fn start_saga_impl(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: StartSagaRequest,
) -> ServiceResult<StartSagaResult> {
    let port = saga_port(&service)?;
    let active = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .catalog
            .prepare_active_catalog(context.control()),
    )
    .await
    .map_err(map_wait_error)?
    .map_err(|_| PublicError::storage_unavailable())?
    .ok_or_else(|| invalid_saga_request(ValidationCode::InvalidValue))?;

    if active.pointer().lineage() != request.lineage() {
        return Err(invalid_saga_request(ValidationCode::InvalidValue).into());
    }
    let bundle = active.bundle();
    let Some(saga_plan) = bundle
        .bundle()
        .schema()
        .saga_plan(request.saga_name().as_str())
    else {
        return Err(invalid_saga_request(ValidationCode::InvalidValue).into());
    };
    let Some(role_schema) = bundle
        .bundle()
        .schema()
        .role_spec(saga_plan.execution_role())
    else {
        return Err(service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        ));
    };

    authorize_saga_role(
        &service,
        context.principal(),
        request.lineage(),
        role_schema,
    )?;

    let (normalized_input, caller_idempotency_key) = normalize_saga_input(
        saga_plan,
        bundle.bundle().schema(),
        request.saga_id(),
        request.input(),
    )?;
    let saga_input_bytes = encode_canonical_record(&normalized_input)
        .map_err(|_| invalid_saga_request(ValidationCode::TooLong))?;
    let saga_plan_hash = saga_plan.plan_hash();
    let delegated_actor_id = format_saga_delegated_actor_id(
        request.lineage(),
        saga_plan.name(),
        saga_plan.execution_role(),
    )
    .map_err(|_| invalid_saga_request(ValidationCode::InvalidValue))?;
    let partition_hash = compute_saga_partition_hash(
        service.identity.database_id(),
        context.principal().tenant_scope(),
        request.lineage(),
        saga_plan.name(),
        request.saga_id(),
    );
    let now = sample_logical_now(context.principal());
    let retained_authority = SagaRetainedAuthorityV1::new(
        context.principal().capability_id(),
        context.principal().capability_revision(),
        context.principal().capability_principal_id().clone(),
        context.principal().actor_kind(),
        context.principal().audience().clone(),
        context.principal().tenant_scope().clone(),
        context.principal().authenticated_at(),
    );
    let candidate = StoredSagaInstanceV1::new(
        request.saga_id(),
        service.identity.database_id(),
        service.identity.environment().clone(),
        context.principal().tenant_scope().clone(),
        request.lineage().clone(),
        active.pointer().contract_version(),
        bundle.bundle_hash(),
        saga_plan_hash,
        saga_plan.name(),
        saga_plan.execution_role(),
        delegated_actor_id,
        partition_hash,
        caller_idempotency_key,
        retained_authority,
        SagaRevision::first(),
        SagaFencingToken::first(),
        SagaLifecycleStatusV1::RunningForward,
        0,
        false,
        0,
        now,
        None,
        saga_input_bytes,
        None,
        Vec::new(),
        None,
        None,
        now,
        now,
    )
    .map_err(|_| invalid_saga_request(ValidationCode::TooLong))?;

    let admission = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        port.admit_or_get_saga(candidate, context.control()),
    )
    .await
    .map_err(map_wait_error)?
    .map_err(|error| map_saga_port_error(&service, error))?;

    let (disposition, mut current) = match admission {
        SagaAdmissionStorageOutcomeV1::Admitted(admitted) => {
            (StartSagaDisposition::Admitted, *admitted)
        }
        SagaAdmissionStorageOutcomeV1::Replayed(existing) => {
            (StartSagaDisposition::Replayed, *existing)
        }
        SagaAdmissionStorageOutcomeV1::ConflictingIdempotencyKey(_) => {
            return Err(PublicError::idempotency_key_reuse().into());
        }
    };

    port.notify_work();

    if let Some(window) = request.await_window()
        && !window.is_zero()
        && !current.lifecycle_status().is_terminal()
    {
        current = drive_or_await_saga(&service, context.control(), current, now, window).await?;
    }

    let result = StartSagaResult::new(disposition, SagaInstanceView::from_stored(&current));
    ensure_response_budget(&result)?;
    Ok(result)
}

async fn get_saga_impl(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: GetSagaRequest,
) -> ServiceResult<GetSagaResult> {
    let port = saga_port(&service)?;
    let Some(instance) = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        port.read_saga(request.saga_id(), context.control()),
    )
    .await
    .map_err(map_wait_error)?
    .map_err(|error| map_saga_port_error(&service, error))?
    else {
        let result = GetSagaResult::NotFound;
        ensure_response_budget(&result)?;
        return Ok(result);
    };

    authorize_existing_saga(&service, &context, &instance).await?;
    let result = GetSagaResult::Found(Box::new(SagaInstanceView::from_stored(&instance)));
    ensure_response_budget(&result)?;
    Ok(result)
}

async fn await_saga_impl(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: AwaitSagaRequest,
) -> ServiceResult<GetSagaResult> {
    let port = saga_port(&service)?;
    let Some(initial) = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        port.read_saga(request.saga_id(), context.control()),
    )
    .await
    .map_err(map_wait_error)?
    .map_err(|error| map_saga_port_error(&service, error))?
    else {
        let result = GetSagaResult::NotFound;
        ensure_response_budget(&result)?;
        return Ok(result);
    };

    authorize_existing_saga(&service, &context, &initial).await?;
    let now = sample_logical_now(context.principal());
    let current = if initial.lifecycle_status().is_terminal() || request.maximum_wait().is_zero() {
        initial
    } else {
        drive_or_await_saga(
            &service,
            context.control(),
            initial,
            now,
            request.maximum_wait(),
        )
        .await?
    };
    let result = GetSagaResult::Found(Box::new(SagaInstanceView::from_stored(&current)));
    ensure_response_budget(&result)?;
    Ok(result)
}

async fn cancel_saga_impl(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: CancelSagaRequest,
) -> ServiceResult<GetSagaResult> {
    let port = saga_port(&service)?;
    let mut current = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        port.read_saga(request.saga_id(), context.control()),
    )
    .await
    .map_err(map_wait_error)?
    .map_err(|error| map_saga_port_error(&service, error))?
    {
        Some(instance) => instance,
        None => {
            let result = GetSagaResult::NotFound;
            ensure_response_budget(&result)?;
            return Ok(result);
        }
    };

    authorize_existing_saga(&service, &context, &current).await?;
    let now = sample_logical_now(context.principal());

    for _ in 0..8 {
        if current.lifecycle_status().is_terminal() {
            break;
        }
        let next_revision = current
            .revision()
            .checked_next()
            .ok_or_else(PublicError::storage_unavailable)?;
        let next_fencing = current
            .fencing_token()
            .checked_next()
            .ok_or_else(PublicError::storage_unavailable)?;
        let (next_status, next_step, next_lease) = if current.pending_invocation().is_none() {
            if let Some(comp_idx) = current.next_uncompensated_effectful_step_index() {
                (SagaLifecycleStatusV1::Compensating, comp_idx, None)
            } else {
                (
                    SagaLifecycleStatusV1::AbortedClean,
                    current.current_step_index(),
                    None,
                )
            }
        } else {
            (
                current.lifecycle_status(),
                current.current_step_index(),
                current.lease().copied(),
            )
        };
        let updated = StoredSagaInstanceV1::new(
            current.saga_id(),
            current.database_id(),
            current.environment().clone(),
            current.tenant_scope().clone(),
            current.contract_lineage().clone(),
            current.contract_version(),
            current.contract_bundle_hash(),
            current.saga_plan_hash(),
            current.saga_name().to_owned(),
            current.execution_role().to_owned(),
            current.delegated_actor_id().clone(),
            current.partition_hash(),
            current.caller_idempotency_key().clone(),
            current.retained_authority().clone(),
            next_revision,
            next_fencing,
            next_status,
            next_step,
            true,
            0,
            now,
            next_lease,
            current.saga_input_bytes().to_vec(),
            current.pending_invocation().cloned(),
            current.step_ledger().to_vec(),
            current.terminal_outcome().cloned(),
            current.diagnostic_reason().map(str::to_owned),
            current.created_at(),
            now,
        )
        .map_err(|_| {
            service.internal_failure(
                ServiceOperationV1::ExecuteCommand,
                InternalDefect::ProofMismatch,
            )
        })?;
        match wait_with_control(
            context.control(),
            service.providers.deadline_scheduler.as_ref(),
            port.compare_and_swap_saga(
                SagaCompareExpectedV1::revision_only(current.revision()),
                updated,
                context.control(),
            ),
        )
        .await
        .map_err(map_wait_error)?
        .map_err(|error| map_saga_port_error(&service, error))?
        {
            SagaCasOutcomeV1::Applied(applied) => {
                current = *applied;
                break;
            }
            SagaCasOutcomeV1::Conflict(Some(latest)) => {
                current = *latest;
            }
            SagaCasOutcomeV1::Conflict(None) => {
                return Ok(GetSagaResult::NotFound);
            }
        }
    }

    port.notify_work();
    let result = GetSagaResult::Found(Box::new(SagaInstanceView::from_stored(&current)));
    ensure_response_budget(&result)?;
    Ok(result)
}

async fn resume_saga_impl(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: ResumeSagaRequest,
) -> ServiceResult<GetSagaResult> {
    let port = saga_port(&service)?;
    let mut current = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        port.read_saga(request.saga_id(), context.control()),
    )
    .await
    .map_err(map_wait_error)?
    .map_err(|error| map_saga_port_error(&service, error))?
    {
        Some(instance) => instance,
        None => {
            let result = GetSagaResult::NotFound;
            ensure_response_budget(&result)?;
            return Ok(result);
        }
    };

    authorize_existing_saga(&service, &context, &current).await?;
    let now = sample_logical_now(context.principal());
    let fresh_authority = SagaRetainedAuthorityV1::new(
        context.principal().capability_id(),
        context.principal().capability_revision(),
        context.principal().capability_principal_id().clone(),
        context.principal().actor_kind(),
        context.principal().audience().clone(),
        context.principal().tenant_scope().clone(),
        context.principal().authenticated_at(),
    );

    for _ in 0..8 {
        if current.lifecycle_status().is_terminal() {
            break;
        }
        let next_revision = current
            .revision()
            .checked_next()
            .ok_or_else(PublicError::storage_unavailable)?;
        let next_fencing = current
            .fencing_token()
            .checked_next()
            .ok_or_else(PublicError::storage_unavailable)?;
        let next_status = match current.lifecycle_status() {
            SagaLifecycleStatusV1::BlockedAuthority
            | SagaLifecycleStatusV1::BlockedRetryable
            | SagaLifecycleStatusV1::CompensationFailed => {
                if current
                    .pending_invocation()
                    .is_some_and(|p| p.direction() == SagaStepDirectionV1::Compensate)
                    || current.cancel_requested()
                    || current.lifecycle_status() == SagaLifecycleStatusV1::CompensationFailed
                {
                    SagaLifecycleStatusV1::Compensating
                } else {
                    SagaLifecycleStatusV1::RunningForward
                }
            }
            other => other,
        };
        let updated = StoredSagaInstanceV1::new(
            current.saga_id(),
            current.database_id(),
            current.environment().clone(),
            current.tenant_scope().clone(),
            current.contract_lineage().clone(),
            current.contract_version(),
            current.contract_bundle_hash(),
            current.saga_plan_hash(),
            current.saga_name().to_owned(),
            current.execution_role().to_owned(),
            current.delegated_actor_id().clone(),
            current.partition_hash(),
            current.caller_idempotency_key().clone(),
            fresh_authority.clone(),
            next_revision,
            next_fencing,
            next_status,
            current.current_step_index(),
            current.cancel_requested(),
            0,
            now,
            None,
            current.saga_input_bytes().to_vec(),
            current.pending_invocation().cloned(),
            current.step_ledger().to_vec(),
            current.terminal_outcome().cloned(),
            None,
            current.created_at(),
            now,
        )
        .map_err(|_| {
            service.internal_failure(
                ServiceOperationV1::ExecuteCommand,
                InternalDefect::ProofMismatch,
            )
        })?;
        match wait_with_control(
            context.control(),
            service.providers.deadline_scheduler.as_ref(),
            port.compare_and_swap_saga(
                SagaCompareExpectedV1::revision_only(current.revision()),
                updated,
                context.control(),
            ),
        )
        .await
        .map_err(map_wait_error)?
        .map_err(|error| map_saga_port_error(&service, error))?
        {
            SagaCasOutcomeV1::Applied(applied) => {
                current = *applied;
                break;
            }
            SagaCasOutcomeV1::Conflict(Some(latest)) => {
                current = *latest;
            }
            SagaCasOutcomeV1::Conflict(None) => {
                return Ok(GetSagaResult::NotFound);
            }
        }
    }

    port.notify_work();
    let result = GetSagaResult::Found(Box::new(SagaInstanceView::from_stored(&current)));
    ensure_response_budget(&result)?;
    Ok(result)
}

async fn authorize_existing_saga(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    instance: &StoredSagaInstanceV1,
) -> ServiceResult<()> {
    if context.principal().tenant_scope() != instance.tenant_scope() {
        return Err(PublicError::authorization_denied().into());
    }
    let bundle = load_saga_bundle(service, context.control(), instance).await?;
    let Some(role_schema) = bundle
        .bundle()
        .schema()
        .role_spec(instance.execution_role())
    else {
        return Err(PublicError::authorization_denied().into());
    };
    authorize_saga_role(
        service,
        context.principal(),
        instance.contract_lineage(),
        role_schema,
    )
}

async fn load_saga_bundle(
    service: &RiffDbServiceInner,
    control: &RequestControl,
    instance: &StoredSagaInstanceV1,
) -> ServiceResult<ValidatedContractBundle> {
    let bundle = wait_with_control(
        control,
        service.providers.deadline_scheduler.as_ref(),
        service.providers.catalog.prepare_contract_version(
            control,
            instance.contract_lineage().clone(),
            instance.contract_version(),
        ),
    )
    .await
    .map_err(map_wait_error)?
    .map_err(|_| PublicError::storage_unavailable())?
    .ok_or_else(|| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::LowerIntegrity,
        )
    })?;
    if bundle.bundle_hash() != instance.contract_bundle_hash() {
        return Err(service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::LowerIntegrity,
        ));
    }
    Ok(bundle)
}

async fn drive_or_await_saga(
    service: &RiffDbServiceInner,
    control: &RequestControl,
    mut current: StoredSagaInstanceV1,
    now: Timestamp,
    window: Duration,
) -> ServiceResult<StoredSagaInstanceV1> {
    let port = saga_port(service)?;
    let deadline = Instant::now()
        .checked_add(window)
        .unwrap_or_else(Instant::now)
        .min(control.deadline());

    // Attempt to claim or advance directly if unleased or lease-expired.
    if current.lifecycle_status().is_claimable()
        && current.next_wakeup_at() <= now
        && current
            .lease()
            .is_none_or(|lease| lease.expires_at() <= now)
        && let Some(claimed) = try_claim_single_saga(service, control, &current, now).await?
    {
        current = advance_saga_loop(service, control, claimed, now).await?;
        if current.lifecycle_status().is_terminal()
            || matches!(
                current.lifecycle_status(),
                SagaLifecycleStatusV1::BlockedAuthority | SagaLifecycleStatusV1::CompensationFailed
            )
        {
            return Ok(current);
        }
    }

    while Instant::now() < deadline {
        if control.is_cancelled() {
            return Err(ServiceFailure::Cancelled);
        }
        let Some(latest) = wait_with_control(
            control,
            service.providers.deadline_scheduler.as_ref(),
            port.read_saga(current.saga_id(), control),
        )
        .await
        .map_err(map_wait_error)?
        .map_err(|error| map_saga_port_error(service, error))?
        else {
            break;
        };
        current = latest;
        if current.lifecycle_status().is_terminal()
            || matches!(
                current.lifecycle_status(),
                SagaLifecycleStatusV1::BlockedAuthority | SagaLifecycleStatusV1::CompensationFailed
            )
        {
            return Ok(current);
        }
        if current.lifecycle_status().is_claimable()
            && current.next_wakeup_at() <= now
            && current
                .lease()
                .is_none_or(|lease| lease.expires_at() <= now)
            && let Some(claimed) = try_claim_single_saga(service, control, &current, now).await?
        {
            current = advance_saga_loop(service, control, claimed, now).await?;
            if current.lifecycle_status().is_terminal()
                || matches!(
                    current.lifecycle_status(),
                    SagaLifecycleStatusV1::BlockedAuthority
                        | SagaLifecycleStatusV1::CompensationFailed
                )
            {
                return Ok(current);
            }
        }
        let sleep_until = (Instant::now() + SAGA_AWAIT_POLL_INTERVAL).min(deadline);
        let _ = wait_with_control(
            control,
            service.providers.deadline_scheduler.as_ref(),
            async {
                service
                    .providers
                    .deadline_scheduler
                    .wait_until(sleep_until)
                    .await;
                Ok::<(), ()>(())
            },
        )
        .await;
    }

    Ok(current)
}

async fn try_claim_single_saga(
    service: &RiffDbServiceInner,
    control: &RequestControl,
    current: &StoredSagaInstanceV1,
    now: Timestamp,
) -> ServiceResult<Option<StoredSagaInstanceV1>> {
    let port = saga_port(service)?;
    let Some(next_revision) = current.revision().checked_next() else {
        return Ok(None);
    };
    let Some(next_fencing) = current.fencing_token().checked_next() else {
        return Ok(None);
    };
    let lease_expires_at = Timestamp::new(
        now.seconds().saturating_add(SAGA_INLINE_LEASE_SECS),
        now.nanoseconds(),
    )
    .unwrap_or(now);
    let next_status = match current.lifecycle_status() {
        SagaLifecycleStatusV1::BlockedRetryable => {
            if current
                .pending_invocation()
                .is_some_and(|p| p.direction() == SagaStepDirectionV1::Compensate)
                || current.cancel_requested()
            {
                SagaLifecycleStatusV1::Compensating
            } else {
                SagaLifecycleStatusV1::RunningForward
            }
        }
        other => other,
    };
    let lease = SagaLeaseStateV1::new(
        current.saga_id().into_bytes(),
        next_fencing,
        lease_expires_at,
    );
    let updated = StoredSagaInstanceV1::new(
        current.saga_id(),
        current.database_id(),
        current.environment().clone(),
        current.tenant_scope().clone(),
        current.contract_lineage().clone(),
        current.contract_version(),
        current.contract_bundle_hash(),
        current.saga_plan_hash(),
        current.saga_name().to_owned(),
        current.execution_role().to_owned(),
        current.delegated_actor_id().clone(),
        current.partition_hash(),
        current.caller_idempotency_key().clone(),
        current.retained_authority().clone(),
        next_revision,
        next_fencing,
        next_status,
        current.current_step_index(),
        current.cancel_requested(),
        current.retry_count(),
        current.next_wakeup_at(),
        Some(lease),
        current.saga_input_bytes().to_vec(),
        current.pending_invocation().cloned(),
        current.step_ledger().to_vec(),
        current.terminal_outcome().cloned(),
        current.diagnostic_reason().map(str::to_owned),
        current.created_at(),
        now,
    )
    .map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;
    match wait_with_control(
        control,
        service.providers.deadline_scheduler.as_ref(),
        port.compare_and_swap_saga(
            SagaCompareExpectedV1::fenced(current.revision(), current.fencing_token()),
            updated,
            control,
        ),
    )
    .await
    .map_err(map_wait_error)?
    .map_err(|error| map_saga_port_error(service, error))?
    {
        SagaCasOutcomeV1::Applied(claimed) => Ok(Some(*claimed)),
        SagaCasOutcomeV1::Conflict(_) => Ok(None),
    }
}

pub(crate) async fn advance_saga_loop(
    service: &RiffDbServiceInner,
    control: &RequestControl,
    mut current: StoredSagaInstanceV1,
    now: Timestamp,
) -> ServiceResult<StoredSagaInstanceV1> {
    let bundle = load_saga_bundle(service, control, &current).await?;
    let Some(saga_plan) = bundle
        .bundle()
        .schema()
        .saga_plan(current.saga_name())
        .filter(|plan| plan.plan_hash() == current.saga_plan_hash())
    else {
        return Err(service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::LowerIntegrity,
        ));
    };
    let CanonicalValue::Record(saga_input_record) =
        decode_canonical_value(current.saga_input_bytes()).map_err(|_| {
            service.internal_failure(
                ServiceOperationV1::ExecuteCommand,
                InternalDefect::LowerIntegrity,
            )
        })?
    else {
        return Err(service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::LowerIntegrity,
        ));
    };

    for _ in 0..MAX_SAGA_INLINE_TRANSITIONS {
        if current.lifecycle_status().is_terminal()
            || matches!(
                current.lifecycle_status(),
                SagaLifecycleStatusV1::BlockedAuthority
                    | SagaLifecycleStatusV1::BlockedRetryable
                    | SagaLifecycleStatusV1::CompensationFailed
            )
        {
            break;
        }

        if current.pending_invocation().is_none() {
            let Some(prepared) = prepare_saga_step(
                service,
                control,
                &bundle,
                saga_plan,
                &saga_input_record,
                &current,
                now,
            )
            .await?
            else {
                break;
            };
            current = prepared;
            if current.lifecycle_status().is_terminal() {
                break;
            }
        }

        let Some(invocation) = current.pending_invocation().cloned() else {
            continue;
        };

        let step_outcome = execute_or_resolve_saga_step(
            service,
            control,
            &bundle,
            current.saga_id(),
            &invocation,
            current.retained_authority(),
            now,
        )
        .await?;

        let Some(advanced) = advance_saga_step(
            service,
            control,
            &bundle,
            saga_plan,
            &saga_input_record,
            &current,
            &invocation,
            step_outcome,
            now,
        )
        .await?
        else {
            break;
        };
        current = advanced;
    }

    Ok(current)
}

async fn prepare_saga_step(
    service: &RiffDbServiceInner,
    control: &RequestControl,
    bundle: &ValidatedContractBundle,
    saga_plan: &SagaPlanV1,
    saga_input_record: &CanonicalRecord,
    current: &StoredSagaInstanceV1,
    now: Timestamp,
) -> ServiceResult<Option<StoredSagaInstanceV1>> {
    let port = saga_port(service)?;
    let next_revision = current
        .revision()
        .checked_next()
        .ok_or_else(PublicError::storage_unavailable)?;

    let updated = match current.lifecycle_status() {
        SagaLifecycleStatusV1::RunningForward => {
            if current.cancel_requested() {
                let (next_status, next_step, next_lease) =
                    if let Some(comp_idx) = current.next_uncompensated_effectful_step_index() {
                        (
                            SagaLifecycleStatusV1::Compensating,
                            comp_idx,
                            current.lease().copied(),
                        )
                    } else {
                        (
                            SagaLifecycleStatusV1::AbortedClean,
                            current.current_step_index(),
                            None,
                        )
                    };
                StoredSagaInstanceV1::new(
                    current.saga_id(),
                    current.database_id(),
                    current.environment().clone(),
                    current.tenant_scope().clone(),
                    current.contract_lineage().clone(),
                    current.contract_version(),
                    current.contract_bundle_hash(),
                    current.saga_plan_hash(),
                    current.saga_name().to_owned(),
                    current.execution_role().to_owned(),
                    current.delegated_actor_id().clone(),
                    current.partition_hash(),
                    current.caller_idempotency_key().clone(),
                    current.retained_authority().clone(),
                    next_revision,
                    current.fencing_token(),
                    next_status,
                    next_step,
                    true,
                    0,
                    now,
                    next_lease,
                    current.saga_input_bytes().to_vec(),
                    None,
                    current.step_ledger().to_vec(),
                    current.terminal_outcome().cloned(),
                    current.diagnostic_reason().map(str::to_owned),
                    current.created_at(),
                    now,
                )
            } else {
                let step_index = current.current_step_index();
                let step_plan =
                    saga_plan
                        .steps()
                        .get(usize::from(step_index))
                        .ok_or_else(|| {
                            service.internal_failure(
                                ServiceOperationV1::ExecuteCommand,
                                InternalDefect::ProofMismatch,
                            )
                        })?;
                let invocation = build_pending_step_invocation(
                    service,
                    bundle,
                    saga_plan,
                    current,
                    saga_input_record,
                    step_index,
                    SagaStepDirectionV1::Forward,
                    step_plan.forward_command(),
                )?;
                StoredSagaInstanceV1::new(
                    current.saga_id(),
                    current.database_id(),
                    current.environment().clone(),
                    current.tenant_scope().clone(),
                    current.contract_lineage().clone(),
                    current.contract_version(),
                    current.contract_bundle_hash(),
                    current.saga_plan_hash(),
                    current.saga_name().to_owned(),
                    current.execution_role().to_owned(),
                    current.delegated_actor_id().clone(),
                    current.partition_hash(),
                    current.caller_idempotency_key().clone(),
                    current.retained_authority().clone(),
                    next_revision,
                    current.fencing_token(),
                    SagaLifecycleStatusV1::RunningForward,
                    step_index,
                    current.cancel_requested(),
                    current.retry_count(),
                    now,
                    current.lease().copied(),
                    current.saga_input_bytes().to_vec(),
                    Some(invocation),
                    current.step_ledger().to_vec(),
                    current.terminal_outcome().cloned(),
                    current.diagnostic_reason().map(str::to_owned),
                    current.created_at(),
                    now,
                )
            }
        }
        SagaLifecycleStatusV1::Compensating => {
            if let Some(comp_idx) = current.next_uncompensated_effectful_step_index() {
                let step_plan = saga_plan
                    .steps()
                    .get(usize::from(comp_idx))
                    .ok_or_else(|| {
                        service.internal_failure(
                            ServiceOperationV1::ExecuteCommand,
                            InternalDefect::ProofMismatch,
                        )
                    })?;
                let comp_call = step_plan.compensation().ok_or_else(|| {
                    service.internal_failure(
                        ServiceOperationV1::ExecuteCommand,
                        InternalDefect::ProofMismatch,
                    )
                })?;
                let invocation = build_pending_step_invocation(
                    service,
                    bundle,
                    saga_plan,
                    current,
                    saga_input_record,
                    comp_idx,
                    SagaStepDirectionV1::Compensate,
                    comp_call,
                )?;
                StoredSagaInstanceV1::new(
                    current.saga_id(),
                    current.database_id(),
                    current.environment().clone(),
                    current.tenant_scope().clone(),
                    current.contract_lineage().clone(),
                    current.contract_version(),
                    current.contract_bundle_hash(),
                    current.saga_plan_hash(),
                    current.saga_name().to_owned(),
                    current.execution_role().to_owned(),
                    current.delegated_actor_id().clone(),
                    current.partition_hash(),
                    current.caller_idempotency_key().clone(),
                    current.retained_authority().clone(),
                    next_revision,
                    current.fencing_token(),
                    SagaLifecycleStatusV1::Compensating,
                    comp_idx,
                    current.cancel_requested(),
                    current.retry_count(),
                    now,
                    current.lease().copied(),
                    current.saga_input_bytes().to_vec(),
                    Some(invocation),
                    current.step_ledger().to_vec(),
                    current.terminal_outcome().cloned(),
                    current.diagnostic_reason().map(str::to_owned),
                    current.created_at(),
                    now,
                )
            } else {
                let any_compensated = current
                    .step_ledger()
                    .iter()
                    .any(|entry| entry.direction() == SagaStepDirectionV1::Compensate);
                let terminal_status = if any_compensated {
                    SagaLifecycleStatusV1::Compensated
                } else {
                    SagaLifecycleStatusV1::AbortedClean
                };
                StoredSagaInstanceV1::new(
                    current.saga_id(),
                    current.database_id(),
                    current.environment().clone(),
                    current.tenant_scope().clone(),
                    current.contract_lineage().clone(),
                    current.contract_version(),
                    current.contract_bundle_hash(),
                    current.saga_plan_hash(),
                    current.saga_name().to_owned(),
                    current.execution_role().to_owned(),
                    current.delegated_actor_id().clone(),
                    current.partition_hash(),
                    current.caller_idempotency_key().clone(),
                    current.retained_authority().clone(),
                    next_revision,
                    current.fencing_token(),
                    terminal_status,
                    current.current_step_index(),
                    current.cancel_requested(),
                    0,
                    now,
                    None,
                    current.saga_input_bytes().to_vec(),
                    None,
                    current.step_ledger().to_vec(),
                    current.terminal_outcome().cloned(),
                    current.diagnostic_reason().map(str::to_owned),
                    current.created_at(),
                    now,
                )
            }
        }
        _ => return Ok(None),
    }
    .map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;

    match wait_with_control(
        control,
        service.providers.deadline_scheduler.as_ref(),
        port.compare_and_swap_saga(
            SagaCompareExpectedV1::fenced(current.revision(), current.fencing_token()),
            updated,
            control,
        ),
    )
    .await
    .map_err(map_wait_error)?
    .map_err(|error| map_saga_port_error(service, error))?
    {
        SagaCasOutcomeV1::Applied(applied) => Ok(Some(*applied)),
        SagaCasOutcomeV1::Conflict(_) => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_pending_step_invocation(
    service: &RiffDbServiceInner,
    bundle: &ValidatedContractBundle,
    saga_plan: &SagaPlanV1,
    current: &StoredSagaInstanceV1,
    saga_input_record: &CanonicalRecord,
    step_index: u16,
    direction: SagaStepDirectionV1,
    call_plan: &SagaCommandCallPlanV1,
) -> ServiceResult<PendingStepInvocationV1> {
    let command_plan = bundle
        .bundle()
        .commands()
        .iter()
        .find(|cmd| cmd.command_id() == call_plan.command_id())
        .ok_or_else(|| {
            service.internal_failure(
                ServiceOperationV1::ExecuteCommand,
                InternalDefect::ProofMismatch,
            )
        })?;
    let idempotency_field = command_plan.idempotency_input().ok_or_else(|| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;
    let step_idempotency_key =
        derive_saga_step_idempotency_key(current.saga_id(), step_index, direction).map_err(
            |_| {
                service.internal_failure(
                    ServiceOperationV1::ExecuteCommand,
                    InternalDefect::ProofMismatch,
                )
            },
        )?;
    let step_idempotency_uuid =
        derive_saga_step_idempotency_uuid(current.saga_id(), step_index, direction);

    let mut fields = BTreeMap::new();
    for arg in call_plan.arguments() {
        let value = evaluate_saga_expression(
            service,
            bundle,
            saga_plan,
            saga_input_record,
            current.step_ledger(),
            arg.expression(),
        )?;
        fields.insert(arg.field_id(), value);
    }

    let idem_declared = command_plan
        .input()
        .record()
        .field(idempotency_field)
        .ok_or_else(|| {
            service.internal_failure(
                ServiceOperationV1::ExecuteCommand,
                InternalDefect::ProofMismatch,
            )
        })?;
    let idem_value = if idem_declared.value_type().tag() == ValueTypeTag::Uuid {
        CanonicalValue::Uuid(step_idempotency_uuid)
    } else {
        CanonicalValue::String(
            CanonicalString::new(step_idempotency_key.expose_secret()).map_err(|_| {
                service.internal_failure(
                    ServiceOperationV1::ExecuteCommand,
                    InternalDefect::ProofMismatch,
                )
            })?,
        )
    };
    fields.insert(idempotency_field, idem_value);

    let mut ordered_fields = Vec::with_capacity(command_plan.input().record().fields().len());
    for declared in command_plan.input().record().fields() {
        if let Some(val) = fields.remove(&declared.id()) {
            ordered_fields.push((declared.id(), val));
        } else if declared.value_type().is_optional() {
            ordered_fields.push((declared.id(), CanonicalValue::Null));
        } else {
            return Err(service.internal_failure(
                ServiceOperationV1::ExecuteCommand,
                InternalDefect::ProofMismatch,
            ));
        }
    }
    let step_record = CanonicalRecord::new(ordered_fields).map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;
    let canonical_input_bytes = encode_canonical_command_input(&step_record).map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;

    PendingStepInvocationV1::new(
        step_index,
        direction,
        call_plan.command_id(),
        current.contract_lineage().clone(),
        current.contract_version(),
        current.contract_bundle_hash(),
        command_plan.plan_hash(),
        canonical_input_bytes,
        current.tenant_scope().clone(),
        current.delegated_actor_id().clone(),
        step_idempotency_key,
    )
    .map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })
}

fn evaluate_saga_expression(
    service: &RiffDbServiceInner,
    bundle: &ValidatedContractBundle,
    saga_plan: &SagaPlanV1,
    saga_input_record: &CanonicalRecord,
    step_ledger: &[CommittedSagaStepRecordV1],
    expr_id: ExprId,
) -> ServiceResult<CanonicalValue> {
    struct SagaEvalSource<'a> {
        bundle: &'a ValidatedContractBundle,
        saga_plan: &'a SagaPlanV1,
        saga_input_record: &'a CanonicalRecord,
        step_ledger: &'a [CommittedSagaStepRecordV1],
    }

    impl ExpressionValueSource for SagaEvalSource<'_> {
        fn input_field(&self, field: FieldId) -> Option<CanonicalValue> {
            lookup_record_field(self.saga_input_record, field)
        }

        fn bound_field(&self, binding: BindingId, field: FieldId) -> Option<CanonicalValue> {
            let step_index = u16::try_from(binding.get().checked_sub(1)?).ok()?;
            let step_plan = self.saga_plan.step(step_index)?;
            let entry = self.step_ledger.iter().rev().find(|record| {
                record.step_index() == step_index
                    && record.direction() == SagaStepDirectionV1::Forward
            })?;
            let cmd_plan = self
                .bundle
                .bundle()
                .commands()
                .iter()
                .find(|cmd| cmd.command_id() == step_plan.forward_command().command_id())?;
            let outcome_schema = cmd_plan
                .outcomes()
                .iter()
                .find(|o| o.id() == entry.outcome_id())?;
            let CanonicalValue::Record(outcome_record) =
                decode_canonical_value(entry.outcome_payload_bytes()).ok()?
            else {
                return None;
            };
            for field_schema in outcome_schema.payload().fields() {
                if saga_step_outcome_field_id(cmd_plan, field_schema.name()) == Some(field) {
                    return lookup_record_field(&outcome_record, field_schema.id());
                }
            }
            None
        }
    }

    evaluate_expression(
        saga_plan.expressions(),
        expr_id,
        &SagaEvalSource {
            bundle,
            saga_plan,
            saga_input_record,
            step_ledger,
        },
    )
    .map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })
}

fn lookup_record_field(record: &CanonicalRecord, field_id: FieldId) -> Option<CanonicalValue> {
    record
        .fields()
        .binary_search_by_key(&field_id, |(candidate, _)| *candidate)
        .ok()
        .map(|idx| record.fields()[idx].1.clone())
}

enum StepExecutionOutcome {
    Committed {
        outcome_id: OutcomeId,
        outcome_record: CanonicalRecord,
    },
    ExecutionFailed(riffdb_types::ExecutionFailureCode),
    BlockedAuthority(String),
    RetryableTransient(String),
}

fn derive_step_request_id(
    saga_id: SagaId,
    step_index: u16,
    direction: SagaStepDirectionV1,
) -> ServiceResult<RequestId> {
    let mask = derive_saga_step_idempotency_uuid(saga_id, step_index, direction);
    let mut bytes = saga_id.into_bytes();
    for i in 6..16 {
        bytes[i] ^= mask[i];
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    RequestId::from_bytes(bytes).map_err(|_| PublicError::storage_unavailable().into())
}

async fn execute_or_resolve_saga_step(
    service: &RiffDbServiceInner,
    control: &RequestControl,
    bundle: &ValidatedContractBundle,
    saga_id: SagaId,
    invocation: &PendingStepInvocationV1,
    retained_authority: &SagaRetainedAuthorityV1,
    _now: Timestamp,
) -> ServiceResult<StepExecutionOutcome> {
    // Step 1: Check whether a committed or failed outcome already exists in authoritative storage
    // under the exact persisted IdempotencyIdentity. This guarantees crash recovery between
    // PrepareSagaStep and AdvanceSagaStep even if the capability was later revoked.
    if let Ok(Ok(permit)) = wait_with_control(
        control,
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .authoritative
            .reserve_read_outcome(control),
    )
    .await
    {
        let lookup = AuthoritativeOutcomeRequest::raw_key(
            invocation.contract_lineage().clone(),
            invocation.command_id(),
            invocation.delegated_actor_id().clone(),
            invocation.tenant_scope().clone(),
            invocation.caller_idempotency_key().clone(),
        );
        if let Ok(receipt) = permit.submit(lookup) {
            match wait_with_control(
                control,
                service.providers.deadline_scheduler.as_ref(),
                receipt,
            )
            .await
            {
                Ok(Ok(Ok(Some(AuthoritativeOutcomeSnapshot::Journaled { result, .. })))) => {
                    return Ok(StepExecutionOutcome::Committed {
                        outcome_id: result.outcome_id(),
                        outcome_record: result.value().clone(),
                    });
                }
                Ok(Ok(Ok(Some(AuthoritativeOutcomeSnapshot::ExecutionFailed {
                    code, ..
                })))) => {
                    return Ok(StepExecutionOutcome::ExecutionFailed(code));
                }
                Ok(Ok(Ok(Some(AuthoritativeOutcomeSnapshot::Pending(_))))) => {
                    return Ok(StepExecutionOutcome::RetryableTransient(
                        "step command outcome is pending".to_owned(),
                    ));
                }
                Ok(Ok(Ok(None))) => {}
                Ok(Ok(Err(_))) | Ok(Err(PortDriverStopped)) | Err(_) => {
                    return Ok(StepExecutionOutcome::RetryableTransient(
                        "outcome lookup temporarily unavailable".to_owned(),
                    ));
                }
            }
        }
    }

    // Step 2: Verify live capability and role authority before submitting a new command execution.
    let delegated_principal = AuthenticatedPrincipal::from_retained_saga_authority(
        retained_authority.capability_id(),
        retained_authority.capability_revision(),
        retained_authority.principal_id().clone(),
        retained_authority.actor_kind(),
        retained_authority.audience().clone(),
        invocation.tenant_scope().clone(),
        retained_authority.authenticated_at(),
    )
    .with_delegated_actor(invocation.delegated_actor_id().clone());
    let empty_claims = UntrustedInvocationClaims::new(None, None, None, None, None);
    let step_request_id =
        derive_step_request_id(saga_id, invocation.step_index(), invocation.direction())?;
    let (step_control, _step_cancel) = RequestControl::new(control.deadline());
    let step_context = RequestContext::new(
        step_request_id,
        delegated_principal.clone(),
        ServiceIngressKindV1::Grpc,
        empty_claims.clone(),
        step_control,
        None,
    );

    let plan_request = CatalogExecutablePlanRequest::new(
        invocation.contract_lineage().clone(),
        invocation.contract_version(),
        invocation.contract_bundle_hash(),
        invocation.command_id(),
        invocation.command_plan_hash(),
    );
    let resolved = load_plan(
        service,
        &step_context,
        ServiceOperationV1::ExecuteCommand,
        plan_request,
    )
    .await?;

    let CanonicalValue::Record(normalized) = decode_canonical_value(invocation.input_bytes())
        .map_err(|_| {
            service.internal_failure(
                ServiceOperationV1::ExecuteCommand,
                InternalDefect::ProofMismatch,
            )
        })?
    else {
        return Err(service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        ));
    };

    if resolved.plan().execution_class() != ExecutionClass::IdempotentMutation {
        return Err(service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        ));
    }
    let facts = derive_input_command_facts(resolved.plan(), normalized.clone()).map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;
    let operation = command_operation(&resolved, CommandExecutionClass::Mutation, &facts);

    let authorization = match service
        .providers
        .policy
        .authorize(&delegated_principal, operation.clone())
    {
        Ok(Decision::Allow(auth)) => auth,
        Ok(Decision::Deny(_)) | Ok(Decision::PrepareCapabilityMutation(_)) => {
            return Ok(StepExecutionOutcome::BlockedAuthority(
                "saga step command denied by current capability or policy".to_owned(),
            ));
        }
        Err(_) => {
            return Ok(StepExecutionOutcome::RetryableTransient(
                "policy evaluation temporarily unavailable".to_owned(),
            ));
        }
    };
    let command_authorization = match authorization.into_command_execution(
        empty_claims.clone(),
        service.identity.agent_session_policy(),
    ) {
        Ok(auth) => auth,
        Err(_) => {
            return Ok(StepExecutionOutcome::BlockedAuthority(
                "saga delegated actor context rejected by session policy".to_owned(),
            ));
        }
    };

    let inspection_request = CommandIdempotencyInspectionRequest::new(
        service.identity.database_id(),
        service.identity.environment().clone(),
        invocation.tenant_scope().clone(),
        invocation.delegated_actor_id().clone(),
        invocation.contract_lineage().clone(),
        invocation.command_id(),
        invocation.caller_idempotency_key().clone(),
    );
    let inspection = match wait_with_control(
        step_context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .executors
            .writer()?
            .idempotency
            .inspect(inspection_request),
    )
    .await
    {
        Ok(Ok(inspection)) => inspection,
        Ok(Err(_)) | Err(_) => {
            return Ok(StepExecutionOutcome::RetryableTransient(
                "idempotency inspection unavailable".to_owned(),
            ));
        }
    };
    if matches!(
        inspection.plan_selection(),
        CommandIdempotencyPlanSelection::Historical(reference)
            if reference != resolved.reference()
    ) {
        return Err(PublicError::idempotency_key_reuse().into());
    }
    let idempotency_field = resolved.plan().idempotency_input().ok_or_else(|| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;
    let prepared_idempotency = match inspection.confirm_selected_plan(
        &normalized,
        idempotency_field,
        resolved.reference().clone(),
    ) {
        Ok(prepared) => prepared,
        Err(CommandIdempotencyConfirmationError::IdempotencyKeyMismatch) => {
            return Err(PublicError::idempotency_key_reuse().into());
        }
        Err(_) => {
            return Err(service.internal_failure(
                ServiceOperationV1::ExecuteCommand,
                InternalDefect::ProofMismatch,
            ));
        }
    };

    let permit = match admit_command_capacity(service, &step_context, &normalized).await {
        Ok(permit) => permit,
        Err(_) => {
            return Ok(StepExecutionOutcome::RetryableTransient(
                "command execution capacity temporarily overloaded".to_owned(),
            ));
        }
    };
    let cmd_control = step_context.control().command_control().map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;
    let post_evaluation_authorizer = ServicePostEvaluationCommandAuthorizer {
        policy: Arc::clone(&service.providers.policy),
        principal: delegated_principal,
        request: operation,
        claims: empty_claims,
        agent_session_policy: service.identity.agent_session_policy(),
    };
    let targets = crate::ServiceAuditTargetMap::execute_command(
        invocation.contract_lineage().clone(),
        invocation.contract_version(),
        invocation.command_id(),
    )
    .map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;
    let started = crate::ServiceAuditInput::new(
        &step_context,
        ServiceOperationV1::ExecuteCommand,
        riffdb_types::ServiceAuditPhaseV1::Started,
        targets,
        command_authorization.provenance().approval_id().cloned(),
        riffdb_types::ServiceAuditLinkV1::None,
    )
    .map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;
    let preparation = CommandExecutionPreparation::new(
        service.identity.database_id(),
        service.identity.environment(),
        resolved,
        normalized,
        prepared_idempotency,
        facts,
        command_authorization,
        step_context.request_id(),
        step_context.ingress(),
        cmd_control,
    )
    .and_then(|prep| prep.with_audited_lifecycle(Box::new(started)))
    .and_then(|prep| prep.with_post_evaluation_authorizer(Box::new(post_evaluation_authorizer)))
    .map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;

    let _ = bundle;
    let receipt = match permit.submit(preparation) {
        Ok(receipt) => receipt,
        Err(_) => {
            return Ok(StepExecutionOutcome::RetryableTransient(
                "command coordinator submission temporarily unavailable".to_owned(),
            ));
        }
    };
    match receipt.completion().await {
        Ok(CoordinatorCommandResult::Committed(committed)) => Ok(StepExecutionOutcome::Committed {
            outcome_id: committed.stored_outcome().declared_outcome().outcome_id(),
            outcome_record: committed
                .stored_outcome()
                .declared_outcome()
                .value()
                .clone(),
        }),
        Ok(CoordinatorCommandResult::ExecutionFailed(failed)) => {
            Ok(StepExecutionOutcome::ExecutionFailed(failed.code()))
        }
        Ok(CoordinatorCommandResult::InputMismatch) => {
            Err(PublicError::idempotency_key_reuse().into())
        }
        Ok(CoordinatorCommandResult::PreparationChanged) => Ok(
            StepExecutionOutcome::RetryableTransient("command preparation changed".to_owned()),
        ),
        Err(error) => {
            if matches!(
                error.kind(),
                riffdb_commit::CommandExecutionErrorKind::AuthorizationDenied
            ) {
                Ok(StepExecutionOutcome::BlockedAuthority(
                    "saga step denied by post-evaluation row policy or revoked capability"
                        .to_owned(),
                ))
            } else {
                Ok(StepExecutionOutcome::RetryableTransient(
                    "command execution transient contention or timeout".to_owned(),
                ))
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn advance_saga_step(
    service: &RiffDbServiceInner,
    control: &RequestControl,
    bundle: &ValidatedContractBundle,
    saga_plan: &SagaPlanV1,
    saga_input_record: &CanonicalRecord,
    current: &StoredSagaInstanceV1,
    invocation: &PendingStepInvocationV1,
    outcome: StepExecutionOutcome,
    now: Timestamp,
) -> ServiceResult<Option<StoredSagaInstanceV1>> {
    let port = saga_port(service)?;
    let next_revision = current
        .revision()
        .checked_next()
        .ok_or_else(PublicError::storage_unavailable)?;

    let updated = match outcome {
        StepExecutionOutcome::Committed {
            outcome_id,
            outcome_record,
        } => {
            let outcome_payload_bytes = encode_canonical_record(&outcome_record).map_err(|_| {
                service.internal_failure(
                    ServiceOperationV1::ExecuteCommand,
                    InternalDefect::ProofMismatch,
                )
            })?;
            let step_index = invocation.step_index();
            let step_plan = saga_plan
                .steps()
                .get(usize::from(step_index))
                .ok_or_else(|| {
                    service.internal_failure(
                        ServiceOperationV1::ExecuteCommand,
                        InternalDefect::ProofMismatch,
                    )
                })?;

            match invocation.direction() {
                SagaStepDirectionV1::Forward => {
                    let forward_cmd = bundle
                        .bundle()
                        .commands()
                        .iter()
                        .find(|cmd| cmd.command_id() == invocation.command_id())
                        .ok_or_else(|| {
                            service.internal_failure(
                                ServiceOperationV1::ExecuteCommand,
                                InternalDefect::ProofMismatch,
                            )
                        })?;
                    let _outcome_schema = forward_cmd
                        .outcomes()
                        .iter()
                        .find(|o| o.id() == outcome_id)
                        .ok_or_else(|| {
                            service.internal_failure(
                                ServiceOperationV1::ExecuteCommand,
                                InternalDefect::ProofMismatch,
                            )
                        })?;
                    let branch = step_plan.branch(outcome_id).ok_or_else(|| {
                        service.internal_failure(
                            ServiceOperationV1::ExecuteCommand,
                            InternalDefect::ProofMismatch,
                        )
                    })?;
                    let effect_disposition = match branch.effect_class() {
                        SagaOutcomeEffectClassV1::Effectful => {
                            SagaStepEffectDispositionV1::Effectful
                        }
                        SagaOutcomeEffectClassV1::NoEffect => SagaStepEffectDispositionV1::NoEffect,
                    };
                    let mut ledger = current.step_ledger().to_vec();
                    let ledger_entry = CommittedSagaStepRecordV1::new(
                        step_index,
                        SagaStepDirectionV1::Forward,
                        outcome_id,
                        effect_disposition,
                        outcome_payload_bytes,
                    )
                    .map_err(|_| {
                        service.internal_failure(
                            ServiceOperationV1::ExecuteCommand,
                            InternalDefect::ProofMismatch,
                        )
                    })?;
                    ledger.push(ledger_entry);

                    if current.cancel_requested() {
                        let (next_status, next_step, next_lease) = if let Some(comp_idx) =
                            find_next_uncompensated_effectful_step(&ledger)
                        {
                            (
                                SagaLifecycleStatusV1::Compensating,
                                comp_idx,
                                current.lease().copied(),
                            )
                        } else {
                            (SagaLifecycleStatusV1::AbortedClean, step_index, None)
                        };
                        StoredSagaInstanceV1::new(
                            current.saga_id(),
                            current.database_id(),
                            current.environment().clone(),
                            current.tenant_scope().clone(),
                            current.contract_lineage().clone(),
                            current.contract_version(),
                            current.contract_bundle_hash(),
                            current.saga_plan_hash(),
                            current.saga_name().to_owned(),
                            current.execution_role().to_owned(),
                            current.delegated_actor_id().clone(),
                            current.partition_hash(),
                            current.caller_idempotency_key().clone(),
                            current.retained_authority().clone(),
                            next_revision,
                            current.fencing_token(),
                            next_status,
                            next_step,
                            true,
                            0,
                            now,
                            next_lease,
                            current.saga_input_bytes().to_vec(),
                            None,
                            ledger,
                            current.terminal_outcome().cloned(),
                            None,
                            current.created_at(),
                            now,
                        )
                    } else {
                        match branch.transition() {
                            SagaBranchTransitionV1::Next(next_step_index) => {
                                StoredSagaInstanceV1::new(
                                    current.saga_id(),
                                    current.database_id(),
                                    current.environment().clone(),
                                    current.tenant_scope().clone(),
                                    current.contract_lineage().clone(),
                                    current.contract_version(),
                                    current.contract_bundle_hash(),
                                    current.saga_plan_hash(),
                                    current.saga_name().to_owned(),
                                    current.execution_role().to_owned(),
                                    current.delegated_actor_id().clone(),
                                    current.partition_hash(),
                                    current.caller_idempotency_key().clone(),
                                    current.retained_authority().clone(),
                                    next_revision,
                                    current.fencing_token(),
                                    SagaLifecycleStatusV1::RunningForward,
                                    *next_step_index,
                                    false,
                                    0,
                                    now,
                                    current.lease().copied(),
                                    current.saga_input_bytes().to_vec(),
                                    None,
                                    ledger,
                                    None,
                                    None,
                                    current.created_at(),
                                    now,
                                )
                            }
                            SagaBranchTransitionV1::Complete(terminal_plan) => {
                                let terminal = build_terminal_outcome_record(
                                    service,
                                    bundle,
                                    saga_plan,
                                    saga_input_record,
                                    &ledger,
                                    terminal_plan,
                                )?;
                                StoredSagaInstanceV1::new(
                                    current.saga_id(),
                                    current.database_id(),
                                    current.environment().clone(),
                                    current.tenant_scope().clone(),
                                    current.contract_lineage().clone(),
                                    current.contract_version(),
                                    current.contract_bundle_hash(),
                                    current.saga_plan_hash(),
                                    current.saga_name().to_owned(),
                                    current.execution_role().to_owned(),
                                    current.delegated_actor_id().clone(),
                                    current.partition_hash(),
                                    current.caller_idempotency_key().clone(),
                                    current.retained_authority().clone(),
                                    next_revision,
                                    current.fencing_token(),
                                    SagaLifecycleStatusV1::Completed,
                                    step_index,
                                    false,
                                    0,
                                    now,
                                    None,
                                    current.saga_input_bytes().to_vec(),
                                    None,
                                    ledger,
                                    Some(terminal),
                                    None,
                                    current.created_at(),
                                    now,
                                )
                            }
                            SagaBranchTransitionV1::Compensate(terminal_plan) => {
                                let terminal = build_terminal_outcome_record(
                                    service,
                                    bundle,
                                    saga_plan,
                                    saga_input_record,
                                    &ledger,
                                    terminal_plan,
                                )?;
                                let (next_status, next_step, next_lease) = if let Some(comp_idx) =
                                    find_next_uncompensated_effectful_step(&ledger)
                                {
                                    (
                                        SagaLifecycleStatusV1::Compensating,
                                        comp_idx,
                                        current.lease().copied(),
                                    )
                                } else {
                                    (SagaLifecycleStatusV1::AbortedClean, step_index, None)
                                };
                                StoredSagaInstanceV1::new(
                                    current.saga_id(),
                                    current.database_id(),
                                    current.environment().clone(),
                                    current.tenant_scope().clone(),
                                    current.contract_lineage().clone(),
                                    current.contract_version(),
                                    current.contract_bundle_hash(),
                                    current.saga_plan_hash(),
                                    current.saga_name().to_owned(),
                                    current.execution_role().to_owned(),
                                    current.delegated_actor_id().clone(),
                                    current.partition_hash(),
                                    current.caller_idempotency_key().clone(),
                                    current.retained_authority().clone(),
                                    next_revision,
                                    current.fencing_token(),
                                    next_status,
                                    next_step,
                                    current.cancel_requested(),
                                    0,
                                    now,
                                    next_lease,
                                    current.saga_input_bytes().to_vec(),
                                    None,
                                    ledger,
                                    Some(terminal),
                                    None,
                                    current.created_at(),
                                    now,
                                )
                            }
                            SagaBranchTransitionV1::AbortClean(terminal_plan) => {
                                let terminal = build_terminal_outcome_record(
                                    service,
                                    bundle,
                                    saga_plan,
                                    saga_input_record,
                                    &ledger,
                                    terminal_plan,
                                )?;
                                StoredSagaInstanceV1::new(
                                    current.saga_id(),
                                    current.database_id(),
                                    current.environment().clone(),
                                    current.tenant_scope().clone(),
                                    current.contract_lineage().clone(),
                                    current.contract_version(),
                                    current.contract_bundle_hash(),
                                    current.saga_plan_hash(),
                                    current.saga_name().to_owned(),
                                    current.execution_role().to_owned(),
                                    current.delegated_actor_id().clone(),
                                    current.partition_hash(),
                                    current.caller_idempotency_key().clone(),
                                    current.retained_authority().clone(),
                                    next_revision,
                                    current.fencing_token(),
                                    SagaLifecycleStatusV1::AbortedClean,
                                    step_index,
                                    current.cancel_requested(),
                                    0,
                                    now,
                                    None,
                                    current.saga_input_bytes().to_vec(),
                                    None,
                                    ledger,
                                    Some(terminal),
                                    None,
                                    current.created_at(),
                                    now,
                                )
                            }
                            SagaBranchTransitionV1::AbortUncompensated(terminal_plan) => {
                                let terminal = build_terminal_outcome_record(
                                    service,
                                    bundle,
                                    saga_plan,
                                    saga_input_record,
                                    &ledger,
                                    terminal_plan,
                                )?;
                                StoredSagaInstanceV1::new(
                                    current.saga_id(),
                                    current.database_id(),
                                    current.environment().clone(),
                                    current.tenant_scope().clone(),
                                    current.contract_lineage().clone(),
                                    current.contract_version(),
                                    current.contract_bundle_hash(),
                                    current.saga_plan_hash(),
                                    current.saga_name().to_owned(),
                                    current.execution_role().to_owned(),
                                    current.delegated_actor_id().clone(),
                                    current.partition_hash(),
                                    current.caller_idempotency_key().clone(),
                                    current.retained_authority().clone(),
                                    next_revision,
                                    current.fencing_token(),
                                    SagaLifecycleStatusV1::AbortedUncompensated,
                                    step_index,
                                    current.cancel_requested(),
                                    0,
                                    now,
                                    None,
                                    current.saga_input_bytes().to_vec(),
                                    None,
                                    ledger,
                                    Some(terminal),
                                    None,
                                    current.created_at(),
                                    now,
                                )
                            }
                        }
                    }
                }
                SagaStepDirectionV1::Compensate => {
                    let comp_cmd = bundle
                        .bundle()
                        .commands()
                        .iter()
                        .find(|cmd| cmd.command_id() == invocation.command_id())
                        .ok_or_else(|| {
                            service.internal_failure(
                                ServiceOperationV1::ExecuteCommand,
                                InternalDefect::ProofMismatch,
                            )
                        })?;
                    let outcome_schema = comp_cmd
                        .outcomes()
                        .iter()
                        .find(|o| o.id() == outcome_id)
                        .ok_or_else(|| {
                            service.internal_failure(
                                ServiceOperationV1::ExecuteCommand,
                                InternalDefect::ProofMismatch,
                            )
                        })?;
                    let mut ledger = current.step_ledger().to_vec();
                    let ledger_entry = CommittedSagaStepRecordV1::new(
                        step_index,
                        SagaStepDirectionV1::Compensate,
                        outcome_id,
                        SagaStepEffectDispositionV1::Effectful,
                        outcome_payload_bytes,
                    )
                    .map_err(|_| {
                        service.internal_failure(
                            ServiceOperationV1::ExecuteCommand,
                            InternalDefect::ProofMismatch,
                        )
                    })?;
                    ledger.push(ledger_entry);

                    if outcome_id == comp_cmd.success_outcome() {
                        let (next_status, next_step, next_lease) = if let Some(next_comp_idx) =
                            find_next_uncompensated_effectful_step(&ledger)
                        {
                            (
                                SagaLifecycleStatusV1::Compensating,
                                next_comp_idx,
                                current.lease().copied(),
                            )
                        } else {
                            (SagaLifecycleStatusV1::Compensated, step_index, None)
                        };
                        StoredSagaInstanceV1::new(
                            current.saga_id(),
                            current.database_id(),
                            current.environment().clone(),
                            current.tenant_scope().clone(),
                            current.contract_lineage().clone(),
                            current.contract_version(),
                            current.contract_bundle_hash(),
                            current.saga_plan_hash(),
                            current.saga_name().to_owned(),
                            current.execution_role().to_owned(),
                            current.delegated_actor_id().clone(),
                            current.partition_hash(),
                            current.caller_idempotency_key().clone(),
                            current.retained_authority().clone(),
                            next_revision,
                            current.fencing_token(),
                            next_status,
                            next_step,
                            current.cancel_requested(),
                            0,
                            now,
                            next_lease,
                            current.saga_input_bytes().to_vec(),
                            None,
                            ledger,
                            current.terminal_outcome().cloned(),
                            None,
                            current.created_at(),
                            now,
                        )
                    } else {
                        let reason = format!(
                            "compensating command for step {} returned non-success outcome {}",
                            step_index,
                            outcome_schema.name()
                        );
                        StoredSagaInstanceV1::new(
                            current.saga_id(),
                            current.database_id(),
                            current.environment().clone(),
                            current.tenant_scope().clone(),
                            current.contract_lineage().clone(),
                            current.contract_version(),
                            current.contract_bundle_hash(),
                            current.saga_plan_hash(),
                            current.saga_name().to_owned(),
                            current.execution_role().to_owned(),
                            current.delegated_actor_id().clone(),
                            current.partition_hash(),
                            current.caller_idempotency_key().clone(),
                            current.retained_authority().clone(),
                            next_revision,
                            current.fencing_token(),
                            SagaLifecycleStatusV1::CompensationFailed,
                            step_index,
                            current.cancel_requested(),
                            0,
                            now,
                            None,
                            current.saga_input_bytes().to_vec(),
                            None,
                            ledger,
                            current.terminal_outcome().cloned(),
                            Some(reason),
                            current.created_at(),
                            now,
                        )
                    }
                }
            }
        }
        StepExecutionOutcome::BlockedAuthority(reason) => StoredSagaInstanceV1::new(
            current.saga_id(),
            current.database_id(),
            current.environment().clone(),
            current.tenant_scope().clone(),
            current.contract_lineage().clone(),
            current.contract_version(),
            current.contract_bundle_hash(),
            current.saga_plan_hash(),
            current.saga_name().to_owned(),
            current.execution_role().to_owned(),
            current.delegated_actor_id().clone(),
            current.partition_hash(),
            current.caller_idempotency_key().clone(),
            current.retained_authority().clone(),
            next_revision,
            current.fencing_token(),
            SagaLifecycleStatusV1::BlockedAuthority,
            current.current_step_index(),
            current.cancel_requested(),
            current.retry_count(),
            now,
            None,
            current.saga_input_bytes().to_vec(),
            Some(invocation.clone()),
            current.step_ledger().to_vec(),
            current.terminal_outcome().cloned(),
            Some(reason),
            current.created_at(),
            now,
        ),
        StepExecutionOutcome::ExecutionFailed(code) => match invocation.direction() {
            SagaStepDirectionV1::Forward => {
                let reason = format!(
                    "forward step {} execution failed: {code:?}",
                    invocation.step_index(),
                );
                let (next_status, next_step, next_lease) =
                    if let Some(comp_idx) = current.next_uncompensated_effectful_step_index() {
                        (
                            SagaLifecycleStatusV1::Compensating,
                            comp_idx,
                            current.lease().copied(),
                        )
                    } else {
                        (
                            SagaLifecycleStatusV1::AbortedClean,
                            invocation.step_index(),
                            None,
                        )
                    };
                StoredSagaInstanceV1::new(
                    current.saga_id(),
                    current.database_id(),
                    current.environment().clone(),
                    current.tenant_scope().clone(),
                    current.contract_lineage().clone(),
                    current.contract_version(),
                    current.contract_bundle_hash(),
                    current.saga_plan_hash(),
                    current.saga_name().to_owned(),
                    current.execution_role().to_owned(),
                    current.delegated_actor_id().clone(),
                    current.partition_hash(),
                    current.caller_idempotency_key().clone(),
                    current.retained_authority().clone(),
                    next_revision,
                    current.fencing_token(),
                    next_status,
                    next_step,
                    current.cancel_requested(),
                    0,
                    now,
                    next_lease,
                    current.saga_input_bytes().to_vec(),
                    None,
                    current.step_ledger().to_vec(),
                    current.terminal_outcome().cloned(),
                    Some(reason),
                    current.created_at(),
                    now,
                )
            }
            SagaStepDirectionV1::Compensate => {
                let reason = format!(
                    "compensation step {} execution failed: {code:?}",
                    invocation.step_index(),
                );
                StoredSagaInstanceV1::new(
                    current.saga_id(),
                    current.database_id(),
                    current.environment().clone(),
                    current.tenant_scope().clone(),
                    current.contract_lineage().clone(),
                    current.contract_version(),
                    current.contract_bundle_hash(),
                    current.saga_plan_hash(),
                    current.saga_name().to_owned(),
                    current.execution_role().to_owned(),
                    current.delegated_actor_id().clone(),
                    current.partition_hash(),
                    current.caller_idempotency_key().clone(),
                    current.retained_authority().clone(),
                    next_revision,
                    current.fencing_token(),
                    SagaLifecycleStatusV1::CompensationFailed,
                    invocation.step_index(),
                    current.cancel_requested(),
                    0,
                    now,
                    None,
                    current.saga_input_bytes().to_vec(),
                    None,
                    current.step_ledger().to_vec(),
                    current.terminal_outcome().cloned(),
                    Some(reason),
                    current.created_at(),
                    now,
                )
            }
        },
        StepExecutionOutcome::RetryableTransient(reason) => {
            let next_retry = current
                .retry_count()
                .saturating_add(1)
                .min(riffdb_types::MAX_SAGA_RETRY_COUNT_V1);
            let backoff_secs = 1_i64 << u32::from(next_retry.min(6));
            let wakeup = Timestamp::new(
                now.seconds().saturating_add(backoff_secs),
                now.nanoseconds(),
            )
            .unwrap_or(now);
            StoredSagaInstanceV1::new(
                current.saga_id(),
                current.database_id(),
                current.environment().clone(),
                current.tenant_scope().clone(),
                current.contract_lineage().clone(),
                current.contract_version(),
                current.contract_bundle_hash(),
                current.saga_plan_hash(),
                current.saga_name().to_owned(),
                current.execution_role().to_owned(),
                current.delegated_actor_id().clone(),
                current.partition_hash(),
                current.caller_idempotency_key().clone(),
                current.retained_authority().clone(),
                next_revision,
                current.fencing_token(),
                SagaLifecycleStatusV1::BlockedRetryable,
                current.current_step_index(),
                current.cancel_requested(),
                next_retry,
                wakeup,
                None,
                current.saga_input_bytes().to_vec(),
                Some(invocation.clone()),
                current.step_ledger().to_vec(),
                current.terminal_outcome().cloned(),
                Some(reason),
                current.created_at(),
                now,
            )
        }
    }
    .map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;

    match wait_with_control(
        control,
        service.providers.deadline_scheduler.as_ref(),
        port.compare_and_swap_saga(
            SagaCompareExpectedV1::fenced(current.revision(), current.fencing_token()),
            updated,
            control,
        ),
    )
    .await
    .map_err(map_wait_error)?
    .map_err(|error| map_saga_port_error(service, error))?
    {
        SagaCasOutcomeV1::Applied(applied) => Ok(Some(*applied)),
        SagaCasOutcomeV1::Conflict(_) => Ok(None),
    }
}

fn find_next_uncompensated_effectful_step(ledger: &[CommittedSagaStepRecordV1]) -> Option<u16> {
    for entry in ledger.iter().rev() {
        if entry.direction() == SagaStepDirectionV1::Forward
            && entry.effect_disposition() == SagaStepEffectDispositionV1::Effectful
        {
            let step_idx = entry.step_index();
            let already_compensated = ledger.iter().any(|other| {
                other.direction() == SagaStepDirectionV1::Compensate
                    && other.step_index() == step_idx
            });
            if !already_compensated {
                return Some(step_idx);
            }
        }
    }
    None
}

fn build_terminal_outcome_record(
    service: &RiffDbServiceInner,
    bundle: &ValidatedContractBundle,
    saga_plan: &SagaPlanV1,
    saga_input_record: &CanonicalRecord,
    ledger: &[CommittedSagaStepRecordV1],
    terminal_plan: &SagaTerminalOutcomePlanV1,
) -> ServiceResult<SagaTerminalOutcomeRecordV1> {
    let mut ordered = Vec::with_capacity(terminal_plan.fields().len());
    for (idx, field_plan) in terminal_plan.fields().iter().enumerate() {
        let field_id = u32::try_from(idx + 1)
            .ok()
            .and_then(FieldId::new)
            .ok_or_else(|| {
                service.internal_failure(
                    ServiceOperationV1::ExecuteCommand,
                    InternalDefect::ProofMismatch,
                )
            })?;
        let value = evaluate_saga_expression(
            service,
            bundle,
            saga_plan,
            saga_input_record,
            ledger,
            field_plan.value(),
        )?;
        ordered.push((field_id, value));
    }
    let record = CanonicalRecord::new(ordered).map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;
    let payload_bytes = encode_canonical_record(&record).map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })?;
    SagaTerminalOutcomeRecordV1::new(
        OutcomeId::first(),
        terminal_plan.outcome_name(),
        payload_bytes,
    )
    .map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExecuteCommand,
            InternalDefect::ProofMismatch,
        )
    })
}
