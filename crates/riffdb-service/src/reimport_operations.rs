//! Current-V7 authorization and bounded application-reimport orchestration.

use std::sync::Arc;

use riffdb_errors::PublicError;
use riffdb_policy::{
    ApplicationReimportAuthorizationRequestV1, ApplicationReimportDecisionV1,
    ApplicationReimportPolicyOperationV1, AuthorizedApplicationReimportV1,
};
use riffdb_types::{RequestId, ServiceAuditPhaseV1, ServiceOperationV1};

use crate::audit::ServiceAuditTargetMap;
use crate::service::{MaintenanceInternalDefect, RiffDbServiceInner};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    ApplicationReimportApplication, ApplicationReimportCoordinatorPort,
    ApplicationReimportMutationPortErrorV1, ApplicationReimportObservationPortErrorV1,
    ApplicationReimportOperationRequestV1, ApplicationReimportOperationResultV1,
    ApplyApplicationReimportPageRequestV1, AuthorizedApplicationReimportOperationV1,
    AuthorizedApplicationReimportPageV1, AuthorizedApplicationReimportStartV1,
    GetApplicationReimportResultV1, PortAdmissionError, PortDriverStopped, RequestContext,
    RiffDbService, ServiceFailure, ServiceFuture, ServiceResult, StartApplicationReimportRequestV1,
    ensure_response_budget,
};

impl ApplicationReimportApplication for RiffDbService {
    fn start_application_reimport(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: StartApplicationReimportRequestV1,
    ) -> ServiceFuture<'_, ApplicationReimportOperationResultV1> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::StartApplicationReimport,
            ingress,
            async move { start_reimport(service, context, request_id, request).await },
        )
    }

    fn apply_application_reimport_page(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: ApplyApplicationReimportPageRequestV1,
    ) -> ServiceFuture<'_, ApplicationReimportOperationResultV1> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::ApplyApplicationReimportPage,
            ingress,
            async move { apply_page(service, context, request_id, request).await },
        )
    }

    fn get_application_reimport(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: ApplicationReimportOperationRequestV1,
    ) -> ServiceFuture<'_, GetApplicationReimportResultV1> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::GetApplicationReimport,
            ingress,
            async move { observe_or_cancel(service, context, request_id, request, false).await },
        )
    }

    fn cancel_application_reimport(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: ApplicationReimportOperationRequestV1,
    ) -> ServiceFuture<'_, GetApplicationReimportResultV1> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::CancelApplicationReimport,
            ingress,
            async move { observe_or_cancel(service, context, request_id, request, true).await },
        )
    }
}

async fn start_reimport(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request_id: RequestId,
    request: StartApplicationReimportRequestV1,
) -> ServiceResult<ApplicationReimportOperationResultV1> {
    exact_request_id(&service, &context, request_id)?;
    let coordinator = coordinator(&service)?;
    let policy_request = ApplicationReimportAuthorizationRequestV1::new(
        request.campaign_id(),
        request.lineage().clone(),
        request.source().portability_manifest_hash(),
        request.scope(),
        ApplicationReimportPolicyOperationV1::Start,
    );
    let begun = service
        .begin_application_reimport_invocation(
            &context,
            policy_request.clone(),
            ServiceOperationV1::StartApplicationReimport,
            ServiceAuditTargetMap::application_reimport(request.lineage().clone())
                .map_err(|_| integrity(&service))?,
        )
        .await?;
    validate_authorization(
        &service,
        &context,
        &policy_request,
        begun.initial_authorization(),
    )?;
    let result = async {
        let permit = reserve(
            &service,
            &context,
            coordinator.reserve_application_reimport_start(context.control()),
        )
        .await?;
        ensure_control_open(&context)?;
        let authorization = authorize_current(&service, &context, policy_request)?;
        let expected_lineage = request.lineage().clone();
        let expected_manifest = request.source().portability_manifest_hash();
        let receipt = permit
            .submit(AuthorizedApplicationReimportStartV1::new(
                context.request_id(),
                context.ingress(),
                request,
                authorization,
            ))
            .map_err(pre_submit_failure)?;
        let result = wait_mutation(&service, &context, receipt).await?;
        if result.lineage() != &expected_lineage
            || result.campaign().source().portability_manifest_hash() != expected_manifest
            || result.campaign().source().page_hashes().is_empty()
            || result.campaign().authority().capability_id() != context.principal().capability_id()
        {
            return Err(integrity(&service));
        }
        ensure_response_budget(&result)?;
        Ok(result)
    }
    .await;
    finish(&service, &context, &begun, &result).await?;
    result
}

async fn apply_page(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request_id: RequestId,
    request: ApplyApplicationReimportPageRequestV1,
) -> ServiceResult<ApplicationReimportOperationResultV1> {
    exact_request_id(&service, &context, request_id)?;
    let coordinator = coordinator(&service)?;
    let binding = resolve_binding(&service, &context, &coordinator, request.campaign_id()).await?;
    let policy_request = policy_request(
        request.campaign_id(),
        &binding,
        ApplicationReimportPolicyOperationV1::Page,
    );
    let begun = service
        .begin_application_reimport_invocation(
            &context,
            policy_request.clone(),
            ServiceOperationV1::ApplyApplicationReimportPage,
            ServiceAuditTargetMap::application_reimport(binding.lineage().clone())
                .map_err(|_| integrity(&service))?,
        )
        .await?;
    validate_authorization(
        &service,
        &context,
        &policy_request,
        begun.initial_authorization(),
    )?;
    let result = async {
        let permit = reserve(
            &service,
            &context,
            coordinator.reserve_application_reimport_page(context.control()),
        )
        .await?;
        ensure_control_open(&context)?;
        let authorization = authorize_current(&service, &context, policy_request)?;
        let page_hash = request.page().page_hash();
        let receipt = permit
            .submit(AuthorizedApplicationReimportPageV1::new(
                request,
                authorization,
            ))
            .map_err(pre_submit_failure)?;
        let result = wait_mutation(&service, &context, receipt).await?;
        if result.lineage() != binding.lineage()
            || result.campaign().source().portability_manifest_hash()
                != binding.portability_manifest_hash()
            || !result
                .campaign()
                .source()
                .page_hashes()
                .contains(&page_hash)
        {
            return Err(integrity(&service));
        }
        ensure_response_budget(&result)?;
        Ok(result)
    }
    .await;
    finish(&service, &context, &begun, &result).await?;
    result
}

async fn observe_or_cancel(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request_id: RequestId,
    request: ApplicationReimportOperationRequestV1,
    cancel: bool,
) -> ServiceResult<GetApplicationReimportResultV1> {
    exact_request_id(&service, &context, request_id)?;
    let coordinator = coordinator(&service)?;
    let binding = resolve_binding(&service, &context, &coordinator, request.campaign_id()).await?;
    let (policy_operation, operation) = if cancel {
        (
            ApplicationReimportPolicyOperationV1::Cancel,
            ServiceOperationV1::CancelApplicationReimport,
        )
    } else {
        (
            ApplicationReimportPolicyOperationV1::Status,
            ServiceOperationV1::GetApplicationReimport,
        )
    };
    let policy_request = policy_request(request.campaign_id(), &binding, policy_operation);
    let begun = service
        .begin_application_reimport_invocation(
            &context,
            policy_request.clone(),
            operation,
            ServiceAuditTargetMap::application_reimport_operation(),
        )
        .await?;
    validate_authorization(
        &service,
        &context,
        &policy_request,
        begun.initial_authorization(),
    )?;
    let result = async {
        let permit = if cancel {
            EitherPermit::Cancel(
                reserve(
                    &service,
                    &context,
                    coordinator.reserve_application_reimport_cancel(context.control()),
                )
                .await?,
            )
        } else {
            EitherPermit::Observe(
                reserve(
                    &service,
                    &context,
                    coordinator.reserve_application_reimport_observation(context.control()),
                )
                .await?,
            )
        };
        ensure_control_open(&context)?;
        let authorization = authorize_current(&service, &context, policy_request)?;
        let authorized = AuthorizedApplicationReimportOperationV1::new(request, authorization);
        let observation = match permit {
            EitherPermit::Observe(permit) => {
                let receipt = permit.submit(authorized).map_err(pre_submit_failure)?;
                wait_observation(&service, &context, receipt).await?
            }
            EitherPermit::Cancel(permit) => {
                let receipt = permit.submit(authorized).map_err(pre_submit_failure)?;
                wait_mutation(&service, &context, receipt).await?
            }
        };
        let result = match observation {
            None => GetApplicationReimportResultV1::NotFound,
            Some(observation)
                if observation.lineage() == binding.lineage()
                    && observation.campaign().source().portability_manifest_hash()
                        == binding.portability_manifest_hash() =>
            {
                GetApplicationReimportResultV1::Found(Box::new(observation))
            }
            Some(_) => return Err(integrity(&service)),
        };
        ensure_response_budget(&result)?;
        Ok(result)
    }
    .await;
    finish(&service, &context, &begun, &result).await?;
    result
}

enum EitherPermit {
    Observe(crate::ApplicationReimportObservationPermitV1),
    Cancel(crate::ApplicationReimportCancelPermitV1),
}

async fn resolve_binding(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    coordinator: &Arc<dyn ApplicationReimportCoordinatorPort>,
    campaign_id: riffdb_types::ApplicationInstallationCampaignId,
) -> ServiceResult<crate::ApplicationReimportPolicyBindingV1> {
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        coordinator.resolve_application_reimport_binding(campaign_id, context.control()),
    )
    .await
    {
        Ok(Ok(Some(binding))) => Ok(binding),
        Ok(Ok(None)) => Err(PublicError::authorization_denied().into()),
        Ok(Err(error)) => Err(observation_failure(service, error)),
        Err(error) => Err(controlled_failure(error)),
    }
}

fn policy_request(
    campaign_id: riffdb_types::ApplicationInstallationCampaignId,
    binding: &crate::ApplicationReimportPolicyBindingV1,
    operation: ApplicationReimportPolicyOperationV1,
) -> ApplicationReimportAuthorizationRequestV1 {
    ApplicationReimportAuthorizationRequestV1::new(
        campaign_id,
        binding.lineage().clone(),
        binding.portability_manifest_hash(),
        binding.scope(),
        operation,
    )
}

fn authorize_current(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request: ApplicationReimportAuthorizationRequestV1,
) -> ServiceResult<Box<AuthorizedApplicationReimportV1>> {
    let authorization = match service
        .providers
        .policy
        .authorize_application_reimport(context.principal(), request.clone())
    {
        Ok(ApplicationReimportDecisionV1::Allow(authorization)) => authorization,
        Ok(ApplicationReimportDecisionV1::Deny(_)) => {
            return Err(PublicError::authorization_denied().into());
        }
        Err(_) => return Err(PublicError::storage_unavailable().into()),
    };
    validate_authorization(service, context, &request, &authorization)?;
    Ok(authorization)
}

fn validate_authorization(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request: &ApplicationReimportAuthorizationRequestV1,
    authorization: &AuthorizedApplicationReimportV1,
) -> ServiceResult<()> {
    if authorization.database_id() != service.identity.database_id()
        || authorization.environment() != service.identity.environment()
        || authorization.request() != request
        || authorization.authority().capability_id() != context.principal().capability_id()
        || authorization.authority().capability_revision()
            != context.principal().capability_revision()
        || authorization.principal_id() != context.principal().principal_id()
        || authorization.actor_kind() != context.principal().actor_kind()
    {
        return Err(integrity(service));
    }
    Ok(())
}

async fn reserve<T>(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    future: crate::PortFuture<'_, T, PortAdmissionError>,
) -> ServiceResult<T> {
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        future,
    )
    .await
    {
        Ok(Ok(permit)) => Ok(permit),
        Ok(Err(error)) => Err(pre_submit_failure(error)),
        Err(error) => Err(controlled_failure(error)),
    }
}

async fn wait_mutation<T>(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    receipt: crate::PortReceipt<T, ApplicationReimportMutationPortErrorV1>,
) -> ServiceResult<T> {
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(value))) => Ok(value),
        Ok(Ok(Err(error))) => Err(mutation_failure(service, error)),
        Ok(Err(PortDriverStopped)) | Err(_) => Err(PublicError::outcome_unknown().into()),
    }
}

async fn wait_observation<T>(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    receipt: crate::PortReceipt<T, ApplicationReimportObservationPortErrorV1>,
) -> ServiceResult<T> {
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(value))) => Ok(value),
        Ok(Ok(Err(error))) => Err(observation_failure(service, error)),
        Ok(Err(PortDriverStopped)) => Err(PublicError::storage_unavailable().into()),
        Err(error) => Err(controlled_failure(error)),
    }
}

async fn finish<T>(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &crate::orchestration::BegunApplicationReimportInvocation,
    result: &ServiceResult<T>,
) -> ServiceResult<()> {
    let phase = match result {
        Ok(_) => ServiceAuditPhaseV1::Succeeded,
        Err(ServiceFailure::Cancelled | ServiceFailure::DeadlineExceeded) => {
            ServiceAuditPhaseV1::Cancelled
        }
        Err(ServiceFailure::Public(error))
            if error.kind() == riffdb_errors::PublicErrorKind::OutcomeUnknown =>
        {
            ServiceAuditPhaseV1::OutcomeUncertain
        }
        Err(_) => ServiceAuditPhaseV1::Failed,
    };
    begun.finish(service, context, phase).await.map_err(|_| {
        service.note_audit_failure(begun.operation());
        PublicError::storage_unavailable().into()
    })
}

fn exact_request_id(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request_id: RequestId,
) -> ServiceResult<()> {
    if request_id != context.request_id() {
        return Err(integrity(service));
    }
    Ok(())
}

fn coordinator(
    service: &RiffDbServiceInner,
) -> ServiceResult<Arc<dyn ApplicationReimportCoordinatorPort>> {
    service
        .providers
        .application_reimport
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

const fn pre_submit_failure(error: PortAdmissionError) -> ServiceFailure {
    match error {
        PortAdmissionError::Cancelled => ServiceFailure::Cancelled,
        PortAdmissionError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
        PortAdmissionError::Unavailable | PortAdmissionError::Stopped => {
            ServiceFailure::Public(PublicError::storage_unavailable())
        }
    }
}

const fn controlled_failure(error: ControlledWaitError) -> ServiceFailure {
    match error {
        ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
        ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
    }
}

fn mutation_failure(
    service: &RiffDbServiceInner,
    error: ApplicationReimportMutationPortErrorV1,
) -> ServiceFailure {
    match error {
        ApplicationReimportMutationPortErrorV1::IdentityMismatch => {
            PublicError::idempotency_key_reuse().into()
        }
        ApplicationReimportMutationPortErrorV1::AuthorityChanged => {
            PublicError::authorization_denied().into()
        }
        ApplicationReimportMutationPortErrorV1::StorageUnavailable => {
            PublicError::storage_unavailable().into()
        }
        ApplicationReimportMutationPortErrorV1::Integrity => integrity(service),
        ApplicationReimportMutationPortErrorV1::SourceMismatch
        | ApplicationReimportMutationPortErrorV1::InvalidPhase
        | ApplicationReimportMutationPortErrorV1::CommandFailed => PublicError::validation(
            riffdb_errors::ValidationIssues::one(riffdb_errors::ValidationIssue::new(
                riffdb_errors::ValidationCode::InvalidValue,
                riffdb_errors::ValidationPath::root(),
            )),
        )
        .into(),
    }
}

fn observation_failure(
    service: &RiffDbServiceInner,
    error: ApplicationReimportObservationPortErrorV1,
) -> ServiceFailure {
    match error {
        ApplicationReimportObservationPortErrorV1::StorageUnavailable => {
            PublicError::storage_unavailable().into()
        }
        ApplicationReimportObservationPortErrorV1::Integrity => integrity(service),
    }
}

fn integrity(service: &RiffDbServiceInner) -> ServiceFailure {
    service.maintenance_internal_failure(MaintenanceInternalDefect::LowerIntegrity)
}
