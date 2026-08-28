//! Exact application installation authorization and campaign orchestration.

use std::sync::Arc;

use riffdb_errors::PublicError;
use riffdb_policy::{
    AuditClass, AuthorizedApplicationInstallation, AuthorizedOperation, Decision, OperationRequest,
    OutputClassification,
};
use riffdb_types::{ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceOperationV1, TenantScope};

use crate::audit::ServiceAuditTargetMap;
use crate::orchestration::{AuditScope, BegunInvocation};
use crate::service::{MaintenanceInternalDefect, RiffDbServiceInner};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    ApplicationInstallationApplication, ApplicationInstallationCoordinatorPort,
    ApplicationInstallationObservationPortError, ApplicationInstallationOperationResult,
    ApplicationInstallationStartPortError, AuthorizedApplicationInstallationObservation,
    AuthorizedApplicationInstallationStart, GetApplicationInstallationRequest,
    GetApplicationInstallationResult, PortAdmissionError, PortDriverStopped, RequestContext,
    RiffDbService, ServiceFailure, ServiceFuture, ServiceResult,
    StartApplicationInstallationRequest, ensure_response_budget,
};

impl ApplicationInstallationApplication for RiffDbService {
    fn start_application_installation(
        &self,
        context: RequestContext,
        request: StartApplicationInstallationRequest,
    ) -> ServiceFuture<'_, ApplicationInstallationOperationResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::StartApplicationInstallation,
            ingress,
            async move { start_campaign(service, context, request).await },
        )
    }

    fn get_application_installation(
        &self,
        context: RequestContext,
        request: GetApplicationInstallationRequest,
    ) -> ServiceFuture<'_, GetApplicationInstallationResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::GetApplicationInstallation,
            ingress,
            async move { get_campaign(service, context, request).await },
        )
    }
}

async fn start_campaign(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: StartApplicationInstallationRequest,
) -> ServiceResult<ApplicationInstallationOperationResult> {
    ensure_control_open(&context)?;
    let target = request.plan().input().target.clone();
    if target.environment() != service.identity.environment() {
        return Err(PublicError::authorization_denied().into());
    }
    let policy = OperationRequest::start_application_installation(target.lineage().clone());
    let targets = ServiceAuditTargetMap::start_application_installation(
        target.lineage().clone(),
        request.plan().input().contract.version(),
    )
    .map_err(|_| integrity(&service))?;
    let begun = service
        .begin_invocation(&context, policy.clone(), targets, AuditScope::Intrinsic)
        .await?;
    let result =
        start_campaign_after_begin(&service, &context, request, target, policy, &begun).await;
    finish_audited_result(
        &service,
        &context,
        &begun,
        ServiceOperationV1::StartApplicationInstallation,
        result,
    )
    .await
}

async fn start_campaign_after_begin(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request: StartApplicationInstallationRequest,
    target: riffdb_application::InstallationTarget,
    policy: OperationRequest,
    begun: &BegunInvocation,
) -> ServiceResult<ApplicationInstallationOperationResult> {
    validate_initial_authorization(service, begun.initial_authorization(), &policy)?;
    let coordinator = coordinator(service)?;
    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        coordinator.reserve_start(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => return Err(pre_submit_failure(error)),
        Err(error) => return Err(controlled_failure(error)),
    };
    ensure_control_open(context)?;
    let authorization = authorize_current(service, context, policy)?;
    let expected_campaign = request.campaign_id();
    let expected_plan = request.plan().identity();
    let expected_lineage = target.lineage().clone();
    let receipt = permit
        .submit(AuthorizedApplicationInstallationStart::new(
            context.request_id(),
            context.ingress(),
            request,
            authorization,
        ))
        .map_err(pre_submit_failure)?;
    let result = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(result))) => result,
        Ok(Ok(Err(error))) => return Err(start_failure(service, error)),
        Ok(Err(PortDriverStopped)) | Err(_) => return Err(PublicError::outcome_unknown().into()),
    };
    if result.lineage() != &expected_lineage
        || result.observation().campaign_id() != expected_campaign
        || result.observation().plan_hash() != expected_plan
    {
        return Err(integrity(service));
    }
    ensure_response_budget(&result)?;
    Ok(result)
}

async fn get_campaign(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: GetApplicationInstallationRequest,
) -> ServiceResult<GetApplicationInstallationResult> {
    ensure_control_open(&context)?;
    let coordinator = coordinator(&service)?;
    let lineage = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        coordinator.resolve_campaign_lineage(request.campaign_id(), context.control()),
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
    let policy = OperationRequest::get_application_installation(lineage.clone());
    let begun = service
        .begin_invocation(
            &context,
            policy.clone(),
            ServiceAuditTargetMap::get_application_installation(),
            AuditScope::Intrinsic,
        )
        .await?;
    let result =
        get_campaign_after_begin(&service, &context, request, lineage, policy, &begun).await;
    finish_audited_result(
        &service,
        &context,
        &begun,
        ServiceOperationV1::GetApplicationInstallation,
        result,
    )
    .await
}

async fn get_campaign_after_begin(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request: GetApplicationInstallationRequest,
    lineage: riffdb_types::ContractLineage,
    policy: OperationRequest,
    begun: &BegunInvocation,
) -> ServiceResult<GetApplicationInstallationResult> {
    validate_initial_authorization(service, begun.initial_authorization(), &policy)?;
    let coordinator = coordinator(service)?;
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
    ensure_control_open(context)?;
    let authorization = authorize_current(service, context, policy)?;
    let campaign_id = request.campaign_id();
    let receipt = permit
        .submit(AuthorizedApplicationInstallationObservation::new(
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
        Ok(Ok(Err(error))) => return Err(observation_failure(service, error)),
        Ok(Err(PortDriverStopped)) => return Err(PublicError::storage_unavailable().into()),
        Err(error) => return Err(controlled_failure(error)),
    };
    let result = match observation {
        None => GetApplicationInstallationResult::NotFound,
        Some(observation)
            if observation.lineage() == &lineage
                && observation.observation().campaign_id() == campaign_id =>
        {
            GetApplicationInstallationResult::Found(Box::new(observation))
        }
        Some(_) => return Err(integrity(service)),
    };
    ensure_response_budget(&result)?;
    Ok(result)
}

fn authorize_current(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request: OperationRequest,
) -> ServiceResult<Box<AuthorizedApplicationInstallation>> {
    let authorization = match service
        .providers
        .policy
        .authorize(context.principal(), request.clone())
    {
        Ok(Decision::Allow(proof)) => proof
            .into_application_installation()
            .ok_or_else(|| integrity(service))?,
        Ok(Decision::Deny(_)) => return Err(PublicError::authorization_denied().into()),
        Ok(Decision::PrepareCapabilityMutation(_)) => return Err(integrity(service)),
        Err(_) => return Err(PublicError::storage_unavailable().into()),
    };
    let obligations = authorization.obligations();
    let expected_audit = match request.operation() {
        ServiceOperationV1::StartApplicationInstallation => AuditClass::ControlPlaneMutation,
        ServiceOperationV1::GetApplicationInstallation => AuditClass::AdministrativeRead,
        _ => return Err(integrity(service)),
    };
    if authorization.database_id() != service.identity.database_id()
        || authorization.environment() != service.identity.environment()
        || authorization.operation() != request.operation()
        || authorization.authorizing_capability_id() != context.principal().capability_id()
        || authorization.authorizing_capability_revision()
            != context.principal().capability_revision()
        || authorization.principal_id() != context.principal().principal_id()
        || authorization.actor_kind() != context.principal().actor_kind()
        || obligations.effective_tenant_scope() != &TenantScope::Global
        || obligations.partition_constraint().is_some()
        || obligations.field_mask().is_some()
        || obligations.row_limit().is_some()
        || obligations.audit_class() != Some(expected_audit)
        || obligations.output_classification() != OutputClassification::AdministrativeRedactedData
    {
        return Err(integrity(service));
    }
    Ok(Box::new(authorization))
}

fn validate_initial_authorization(
    service: &RiffDbServiceInner,
    authorization: &AuthorizedOperation,
    request: &OperationRequest,
) -> ServiceResult<()> {
    let obligations = authorization.obligations();
    let expected_audit = match request.operation() {
        ServiceOperationV1::StartApplicationInstallation => AuditClass::ControlPlaneMutation,
        ServiceOperationV1::GetApplicationInstallation => AuditClass::AdministrativeRead,
        _ => return Err(integrity(service)),
    };
    if authorization.database_id() != service.identity.database_id()
        || authorization.environment() != service.identity.environment()
        || authorization.request() != request
        || obligations.effective_tenant_scope() != &TenantScope::Global
        || obligations.partition_constraint().is_some()
        || obligations.field_mask().is_some()
        || obligations.row_limit().is_some()
        || obligations.audit_class() != Some(expected_audit)
        || obligations.output_classification() != OutputClassification::AdministrativeRedactedData
    {
        return Err(integrity(service));
    }
    Ok(())
}

async fn finish_audited_result<T>(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    operation: ServiceOperationV1,
    result: ServiceResult<T>,
) -> ServiceResult<T> {
    let phase = match &result {
        Ok(_) => ServiceAuditPhaseV1::Succeeded,
        Err(ServiceFailure::Cancelled | ServiceFailure::DeadlineExceeded) => {
            ServiceAuditPhaseV1::Cancelled
        }
        Err(
            ServiceFailure::Public(_)
            | ServiceFailure::ResponseTooLarge
            | ServiceFailure::EmergencyInternal(_),
        ) => ServiceAuditPhaseV1::Failed,
    };
    if begun
        .finish(service, context, phase, ServiceAuditLinkV1::None)
        .await
        .is_err()
    {
        service.note_audit_failure(operation);
        Err(PublicError::storage_unavailable().into())
    } else {
        result
    }
}

fn coordinator(
    service: &RiffDbServiceInner,
) -> ServiceResult<Arc<dyn ApplicationInstallationCoordinatorPort>> {
    service
        .providers
        .installation
        .as_ref()
        .map(Arc::clone)
        .ok_or_else(|| PublicError::storage_unavailable().into())
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
    error: ApplicationInstallationStartPortError,
) -> ServiceFailure {
    match error {
        ApplicationInstallationStartPortError::InputMismatch => {
            PublicError::idempotency_key_reuse().into()
        }
        ApplicationInstallationStartPortError::Unavailable => {
            PublicError::storage_unavailable().into()
        }
        ApplicationInstallationStartPortError::OutcomeUnknown => {
            PublicError::outcome_unknown().into()
        }
        ApplicationInstallationStartPortError::Integrity => integrity(service),
    }
}

fn observation_failure(
    service: &RiffDbServiceInner,
    error: ApplicationInstallationObservationPortError,
) -> ServiceFailure {
    match error {
        ApplicationInstallationObservationPortError::Unavailable => {
            PublicError::storage_unavailable().into()
        }
        ApplicationInstallationObservationPortError::Integrity => integrity(service),
    }
}

fn integrity(service: &RiffDbServiceInner) -> ServiceFailure {
    service.maintenance_internal_failure(MaintenanceInternalDefect::LowerIntegrity)
}
