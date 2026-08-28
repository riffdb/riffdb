//! API-neutral contract-migration authorization and receipt orchestration.

use std::sync::Arc;

use riffdb_errors::{
    PublicError, ValidationCode, ValidationIssue, ValidationIssues, ValidationPath,
};
use riffdb_policy::{
    AuthorizedContractMigration, ContractMigrationAuthorizationRequest, ContractMigrationDecision,
    ContractMigrationPolicyOperation, OutputClassification,
};
use riffdb_types::{ContractMigrationOperationKind, TenantScope};

use crate::service::{MaintenanceInternalDefect, MaintenanceSubmissionState, RiffDbServiceInner};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    ApplyContractMigrationRequest, AuthorizedContractMigrationObservation,
    AuthorizedContractMigrationStart, CheckContractMigrationRequest, ContractMigrationApplication,
    ContractMigrationObservationPortError, ContractMigrationOperationObservation,
    ContractMigrationStartPortError, ContractMigrationStartResult,
    GetContractMigrationOperationRequest, GetContractMigrationOperationResult, PortAdmissionError,
    PortDriverStopped, RequestContext, RiffDbService, ServiceFailure, ServiceFuture, ServiceResult,
    ensure_response_budget,
};

impl ContractMigrationApplication for RiffDbService {
    fn check_contract_migration(
        &self,
        context: RequestContext,
        request: CheckContractMigrationRequest,
    ) -> ServiceFuture<'_, ContractMigrationStartResult> {
        let service = Arc::clone(&self.inner);
        let submission = Arc::new(MaintenanceSubmissionState::new());
        let operation_submission = Arc::clone(&submission);
        self.spawn_tracked_maintenance_operation(submission, async move {
            start_check(service, context, request, operation_submission).await
        })
    }

    fn apply_contract_migration(
        &self,
        context: RequestContext,
        request: ApplyContractMigrationRequest,
    ) -> ServiceFuture<'_, ContractMigrationStartResult> {
        let service = Arc::clone(&self.inner);
        let submission = Arc::new(MaintenanceSubmissionState::new());
        let operation_submission = Arc::clone(&submission);
        self.spawn_tracked_maintenance_operation(submission, async move {
            start_apply(service, context, request, operation_submission).await
        })
    }

    fn get_contract_migration_operation(
        &self,
        context: RequestContext,
        request: GetContractMigrationOperationRequest,
    ) -> ServiceFuture<'_, GetContractMigrationOperationResult> {
        let service = Arc::clone(&self.inner);
        self.spawn_maintenance_operation(
            async move { get_operation(service, context, request).await },
        )
    }
}

async fn start_check(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: CheckContractMigrationRequest,
    submission: Arc<MaintenanceSubmissionState>,
) -> ServiceResult<ContractMigrationStartResult> {
    let policy = ContractMigrationAuthorizationRequest::start(
        request.operation_id(),
        ContractMigrationPolicyOperation::Check,
        request.artifacts().lineage().clone(),
        request.input_hash(),
    );
    authorize_current(&service, &context, policy.clone())?;
    let permit = reserve_start(&service, &context).await?;
    ensure_control_open(&context)?;
    let authorization = authorize_current(&service, &context, policy)?;
    let expected = request.clone();
    submission.mark_submit_in_flight();
    let receipt = match permit.submit(AuthorizedContractMigrationStart::Check {
        request_id: context.request_id(),
        ingress: context.ingress(),
        request,
        authorization,
    }) {
        Ok(receipt) => receipt,
        Err(error) => {
            submission.mark_submit_rejected();
            return Err(pre_submit_failure(error));
        }
    };
    let result = await_start(&service, &context, receipt).await?;
    if !start_result_matches(
        &result,
        result.operation(),
        ContractMigrationOperationKind::Check,
        expected.operation_id(),
        expected.input_hash(),
        expected.artifacts(),
    ) {
        return Err(integrity(&service));
    }
    ensure_response_budget(&result)?;
    Ok(result)
}

async fn start_apply(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: ApplyContractMigrationRequest,
    submission: Arc<MaintenanceSubmissionState>,
) -> ServiceResult<ContractMigrationStartResult> {
    let policy = ContractMigrationAuthorizationRequest::start(
        request.operation_id(),
        ContractMigrationPolicyOperation::Apply,
        request.artifacts().lineage().clone(),
        request.input_hash(),
    );
    authorize_current(&service, &context, policy.clone())?;
    let permit = reserve_start(&service, &context).await?;
    ensure_control_open(&context)?;
    let authorization = authorize_current(&service, &context, policy)?;
    let expected = request.clone();
    submission.mark_submit_in_flight();
    let receipt = match permit.submit(AuthorizedContractMigrationStart::Apply {
        request_id: context.request_id(),
        ingress: context.ingress(),
        request,
        authorization,
    }) {
        Ok(receipt) => receipt,
        Err(error) => {
            submission.mark_submit_rejected();
            return Err(pre_submit_failure(error));
        }
    };
    let result = await_start(&service, &context, receipt).await?;
    if !start_result_matches(
        &result,
        result.operation(),
        ContractMigrationOperationKind::Apply,
        expected.operation_id(),
        expected.input_hash(),
        expected.artifacts(),
    ) {
        return Err(integrity(&service));
    }
    ensure_response_budget(&result)?;
    Ok(result)
}

async fn get_operation(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: GetContractMigrationOperationRequest,
) -> ServiceResult<GetContractMigrationOperationResult> {
    ensure_control_open(&context)?;
    let coordinator = coordinator(&service)?;
    let lineage = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        coordinator.resolve_operation_lineage(request.operation_id(), context.control()),
    )
    .await
    {
        Ok(Ok(lineage)) => lineage,
        Ok(Err(error)) => return Err(observation_failure(&service, error)),
        Err(error) => return Err(controlled_failure(error)),
    };
    let Some(lineage) = lineage else {
        return Err(PublicError::authorization_denied().into());
    };
    let policy =
        ContractMigrationAuthorizationRequest::get_operation(request.operation_id(), lineage);
    authorize_current(&service, &context, policy.clone())?;
    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        coordinator.reserve_observation(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => return Err(pre_submit_failure(error)),
        Err(error) => return Err(controlled_failure(error)),
    };
    ensure_control_open(&context)?;
    let authorization = authorize_current(&service, &context, policy)?;
    let receipt = permit
        .submit(AuthorizedContractMigrationObservation::new(
            request,
            authorization,
        ))
        .map_err(pre_submit_failure)?;
    let observation = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(observation))) => observation,
        Ok(Ok(Err(error))) => return Err(observation_failure(&service, error)),
        Ok(Err(PortDriverStopped)) => return Err(PublicError::storage_unavailable().into()),
        Err(error) => return Err(controlled_failure(error)),
    };
    let result = observation.map_or(GetContractMigrationOperationResult::NotFound, |operation| {
        GetContractMigrationOperationResult::Found(Box::new(operation))
    });
    ensure_response_budget(&result)?;
    Ok(result)
}

async fn reserve_start(
    service: &RiffDbServiceInner,
    context: &RequestContext,
) -> ServiceResult<crate::ContractMigrationStartPermit> {
    let coordinator = coordinator(service)?;
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        coordinator.reserve_start(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => Ok(permit),
        Ok(Err(error)) => Err(pre_submit_failure(error)),
        Err(error) => Err(controlled_failure(error)),
    }
}

fn coordinator(
    service: &RiffDbServiceInner,
) -> ServiceResult<Arc<dyn crate::ContractMigrationCoordinatorPort>> {
    service
        .providers
        .migration
        .as_ref()
        .map(Arc::clone)
        .ok_or_else(|| PublicError::storage_unavailable().into())
}

fn authorize_current(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request: ContractMigrationAuthorizationRequest,
) -> ServiceResult<Box<AuthorizedContractMigration>> {
    let authorization = match service
        .providers
        .policy
        .authorize_contract_migration(context.principal(), request.clone())
    {
        Ok(ContractMigrationDecision::Allow(proof)) => proof,
        Ok(ContractMigrationDecision::Deny(_)) => {
            return Err(PublicError::authorization_denied().into());
        }
        Err(_) => return Err(PublicError::storage_unavailable().into()),
    };
    let obligations = authorization.obligations();
    if authorization.database_id() != service.identity.database_id()
        || authorization.environment() != service.identity.environment()
        || authorization.request() != &request
        || authorization.authorizing_capability_id() != context.principal().capability_id()
        || authorization.authorizing_capability_revision()
            != context.principal().capability_revision()
        || authorization.principal_id() != context.principal().principal_id()
        || authorization.actor_kind() != context.principal().actor_kind()
        || obligations.effective_tenant_scope() != &TenantScope::Global
        || obligations.partition_constraint().is_some()
        || obligations.field_mask().is_some()
        || obligations.row_limit().is_some()
        || obligations.audit_class().is_some()
        || obligations.output_classification() != OutputClassification::AdministrativeRedactedData
    {
        return Err(integrity(service));
    }
    Ok(authorization)
}

async fn await_start(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    receipt: crate::PortReceipt<ContractMigrationStartResult, ContractMigrationStartPortError>,
) -> ServiceResult<ContractMigrationStartResult> {
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(result))) => Ok(result),
        Ok(Ok(Err(error))) => Err(start_failure(service, error)),
        Ok(Err(PortDriverStopped)) | Err(_) => Err(PublicError::outcome_unknown().into()),
    }
}

fn start_result_matches(
    result: &ContractMigrationStartResult,
    observation: &ContractMigrationOperationObservation,
    kind: ContractMigrationOperationKind,
    operation_id: riffdb_types::ContractMigrationOperationId,
    input_hash: riffdb_types::ContractMigrationInputHash,
    artifacts: &crate::ContractMigrationArtifacts,
) -> bool {
    let operation_matches = result.disposition()
        == crate::ContractMigrationStartDisposition::AlreadyApplied
        || observation.operation_id() == operation_id;
    operation_matches
        && observation.kind() == kind
        && observation.lineage() == artifacts.lineage()
        && observation.input_hash() == input_hash
        && observation.parent_hash() == artifacts.parent_hash()
        && observation.candidate_hash() == artifacts.candidate_hash()
        && observation.migration_hash() == artifacts.migration_hash()
}

fn ensure_control_open(context: &RequestContext) -> ServiceResult<()> {
    if context.control().is_cancelled() {
        Err(ServiceFailure::Cancelled)
    } else if context.control().is_deadline_exceeded() {
        Err(ServiceFailure::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn pre_submit_failure(error: PortAdmissionError) -> ServiceFailure {
    match error {
        PortAdmissionError::Cancelled => ServiceFailure::Cancelled,
        PortAdmissionError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
        PortAdmissionError::Unavailable | PortAdmissionError::Stopped => {
            ServiceFailure::Public(PublicError::storage_unavailable())
        }
    }
}

fn controlled_failure(error: ControlledWaitError) -> ServiceFailure {
    match error {
        ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
        ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
    }
}

fn start_failure(
    service: &RiffDbServiceInner,
    error: ContractMigrationStartPortError,
) -> ServiceFailure {
    match error {
        ContractMigrationStartPortError::InputMismatch => {
            PublicError::idempotency_key_reuse().into()
        }
        ContractMigrationStartPortError::AlreadyAppliedMismatch => {
            PublicError::validation(ValidationIssues::one(ValidationIssue::new(
                ValidationCode::InvalidValue,
                ValidationPath::root(),
            )))
            .into()
        }
        ContractMigrationStartPortError::Unavailable => PublicError::storage_unavailable().into(),
        ContractMigrationStartPortError::OutcomeUnknown => PublicError::outcome_unknown().into(),
        ContractMigrationStartPortError::Integrity => integrity(service),
    }
}

fn observation_failure(
    service: &RiffDbServiceInner,
    error: ContractMigrationObservationPortError,
) -> ServiceFailure {
    match error {
        ContractMigrationObservationPortError::Unavailable => {
            PublicError::storage_unavailable().into()
        }
        ContractMigrationObservationPortError::Integrity => integrity(service),
    }
}

fn integrity(service: &RiffDbServiceInner) -> ServiceFailure {
    service.maintenance_internal_failure(MaintenanceInternalDefect::LowerIntegrity)
}
