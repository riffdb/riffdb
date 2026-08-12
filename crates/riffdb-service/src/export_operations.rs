//! Current-V5 authorization and bounded application-export orchestration.

use std::sync::Arc;

use riffdb_errors::PublicError;
use riffdb_policy::{
    ApplicationExportAuthorizationRequestV1, ApplicationExportDecisionV1,
    ApplicationExportPolicyOperationV1, AuthorizedApplicationExportV1,
};
use riffdb_types::{RequestId, ServiceAuditPhaseV1, ServiceOperationV1};

use crate::audit::ServiceAuditTargetMap;
use crate::service::{MaintenanceInternalDefect, RiffDbServiceInner};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    ApplicationExportApplication, ApplicationExportCoordinatorPort,
    ApplicationExportMutationPortErrorV1, ApplicationExportObservationPortErrorV1,
    ApplicationExportOperationRequest, ApplicationExportPageV1, ApplicationExportStartResultV1,
    AuthorizedApplicationExportOperationV1, AuthorizedApplicationExportPageV1,
    AuthorizedApplicationExportStartV1, GetApplicationExportPageRequest,
    GetApplicationExportResultV1, PortAdmissionError, PortDriverStopped, RequestContext,
    RiffDbService, ServiceFailure, ServiceFuture, ServiceResult, StartApplicationExportRequest,
    ensure_response_budget,
};

impl ApplicationExportApplication for RiffDbService {
    fn start_application_export(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: StartApplicationExportRequest,
    ) -> ServiceFuture<'_, ApplicationExportStartResultV1> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::StartApplicationExport,
            ingress,
            async move { start_export(service, context, request_id, request).await },
        )
    }

    fn get_application_export_page(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: GetApplicationExportPageRequest,
    ) -> ServiceFuture<'_, ApplicationExportPageV1> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::GetApplicationExportPage,
            ingress,
            async move { get_export_page(service, context, request_id, request).await },
        )
    }

    fn get_application_export(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: ApplicationExportOperationRequest,
    ) -> ServiceFuture<'_, GetApplicationExportResultV1> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::GetApplicationExport,
            ingress,
            async move { get_export(service, context, request_id, request).await },
        )
    }

    fn cancel_application_export(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: ApplicationExportOperationRequest,
    ) -> ServiceFuture<'_, GetApplicationExportResultV1> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::CancelApplicationExport,
            ingress,
            async move { cancel_export(service, context, request_id, request).await },
        )
    }
}

async fn start_export(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request_id: RequestId,
    request: StartApplicationExportRequest,
) -> ServiceResult<ApplicationExportStartResultV1> {
    exact_request_id(&service, &context, request_id)?;
    let coordinator = coordinator(&service)?;
    let policy_request = ApplicationExportAuthorizationRequestV1::new(
        request.operation_id(),
        request.selection().clone(),
        ApplicationExportPolicyOperationV1::Start,
    );
    let targets = ServiceAuditTargetMap::application_export(request.selection().lineage().clone())
        .map_err(|_| integrity(&service))?;
    let begun = service
        .begin_application_export_invocation(
            &context,
            policy_request.clone(),
            ServiceOperationV1::StartApplicationExport,
            targets,
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
            coordinator.reserve_application_export_start(context.control()),
        )
        .await?;
        ensure_control_open(&context)?;
        let authorization = authorize_current(&service, &context, policy_request)?;
        let operation_id = request.operation_id();
        let selection = request.selection().clone();
        let receipt = permit
            .submit(AuthorizedApplicationExportStartV1::new(
                context.request_id(),
                context.ingress(),
                request,
                authorization,
            ))
            .map_err(pre_submit_failure)?;
        let result = wait_mutation(&service, &context, receipt).await?;
        if result.operation().operation_id() != operation_id
            || result.operation().selection() != &selection
            || result
                .cursor()
                .is_some_and(|_| result.operation().phase().is_terminal())
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

async fn get_export_page(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request_id: RequestId,
    request: GetApplicationExportPageRequest,
) -> ServiceResult<ApplicationExportPageV1> {
    exact_request_id(&service, &context, request_id)?;
    let coordinator = coordinator(&service)?;
    let selection =
        resolve_selection(&service, &context, &coordinator, request.operation_id()).await?;
    let policy_request = ApplicationExportAuthorizationRequestV1::new(
        request.operation_id(),
        selection.clone(),
        ApplicationExportPolicyOperationV1::Page,
    );
    let targets = ServiceAuditTargetMap::application_export(selection.lineage().clone())
        .map_err(|_| integrity(&service))?;
    let begun = service
        .begin_application_export_invocation(
            &context,
            policy_request.clone(),
            ServiceOperationV1::GetApplicationExportPage,
            targets,
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
            coordinator.reserve_application_export_page(context.control()),
        )
        .await?;
        ensure_control_open(&context)?;
        let authorization = authorize_current(&service, &context, policy_request)?;
        let operation_id = request.operation_id();
        let receipt = permit
            .submit(AuthorizedApplicationExportPageV1::new(
                request,
                authorization,
            ))
            .map_err(pre_submit_failure)?;
        let page = wait_mutation(&service, &context, receipt).await?;
        if page.operation_id() != operation_id || !selection.includes(page.class()) {
            return Err(integrity(&service));
        }
        ensure_response_budget(&page)?;
        Ok(page)
    }
    .await;
    finish(&service, &context, &begun, &result).await?;
    result
}

async fn get_export(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request_id: RequestId,
    request: ApplicationExportOperationRequest,
) -> ServiceResult<GetApplicationExportResultV1> {
    exact_request_id(&service, &context, request_id)?;
    observe_or_cancel(service, context, request, false).await
}

async fn cancel_export(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request_id: RequestId,
    request: ApplicationExportOperationRequest,
) -> ServiceResult<GetApplicationExportResultV1> {
    exact_request_id(&service, &context, request_id)?;
    observe_or_cancel(service, context, request, true).await
}

async fn observe_or_cancel(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: ApplicationExportOperationRequest,
    cancel: bool,
) -> ServiceResult<GetApplicationExportResultV1> {
    let coordinator = coordinator(&service)?;
    let selection =
        resolve_selection(&service, &context, &coordinator, request.operation_id()).await?;
    let (policy_operation, operation) = if cancel {
        (
            ApplicationExportPolicyOperationV1::Cancel,
            ServiceOperationV1::CancelApplicationExport,
        )
    } else {
        (
            ApplicationExportPolicyOperationV1::Status,
            ServiceOperationV1::GetApplicationExport,
        )
    };
    let policy_request = ApplicationExportAuthorizationRequestV1::new(
        request.operation_id(),
        selection.clone(),
        policy_operation,
    );
    let begun = service
        .begin_application_export_invocation(
            &context,
            policy_request.clone(),
            operation,
            ServiceAuditTargetMap::application_export_operation(),
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
                    coordinator.reserve_application_export_cancel(context.control()),
                )
                .await?,
            )
        } else {
            EitherPermit::Observe(
                reserve(
                    &service,
                    &context,
                    coordinator.reserve_application_export_observation(context.control()),
                )
                .await?,
            )
        };
        ensure_control_open(&context)?;
        let authorization = authorize_current(&service, &context, policy_request)?;
        let operation_id = request.operation_id();
        let authorized = AuthorizedApplicationExportOperationV1::new(request, authorization);
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
            None => GetApplicationExportResultV1::NotFound,
            Some(operation)
                if operation.operation_id() == operation_id
                    && operation.selection() == &selection =>
            {
                GetApplicationExportResultV1::Found(Box::new(operation))
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
    Observe(crate::ApplicationExportObservationPermitV1),
    Cancel(crate::ApplicationExportCancelPermitV1),
}

async fn resolve_selection(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    coordinator: &Arc<dyn ApplicationExportCoordinatorPort>,
    operation_id: riffdb_types::ApplicationExportOperationId,
) -> ServiceResult<riffdb_types::ApplicationExportSelectionV1> {
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        coordinator.resolve_application_export_selection(operation_id, context.control()),
    )
    .await
    {
        Ok(Ok(Some(selection))) => Ok(selection),
        Ok(Ok(None)) => Err(PublicError::authorization_denied().into()),
        Ok(Err(error)) => Err(observation_failure(service, error)),
        Err(error) => Err(controlled_failure(error)),
    }
}

fn authorize_current(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request: ApplicationExportAuthorizationRequestV1,
) -> ServiceResult<Box<AuthorizedApplicationExportV1>> {
    let authorization = match service
        .providers
        .policy
        .authorize_application_export(context.principal(), request.clone())
    {
        Ok(ApplicationExportDecisionV1::Allow(authorization)) => authorization,
        Ok(ApplicationExportDecisionV1::Deny(_)) => {
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
    request: &ApplicationExportAuthorizationRequestV1,
    authorization: &AuthorizedApplicationExportV1,
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
    receipt: crate::PortReceipt<T, ApplicationExportMutationPortErrorV1>,
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
    receipt: crate::PortReceipt<T, ApplicationExportObservationPortErrorV1>,
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
    begun: &crate::orchestration::BegunApplicationExportInvocation,
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
) -> ServiceResult<Arc<dyn ApplicationExportCoordinatorPort>> {
    service
        .providers
        .application_export
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
    error: ApplicationExportMutationPortErrorV1,
) -> ServiceFailure {
    match error {
        ApplicationExportMutationPortErrorV1::InputMismatch => {
            PublicError::idempotency_key_reuse().into()
        }
        ApplicationExportMutationPortErrorV1::Unavailable
        | ApplicationExportMutationPortErrorV1::LimitExceeded => {
            PublicError::storage_unavailable().into()
        }
        ApplicationExportMutationPortErrorV1::OutcomeUnknown => {
            PublicError::outcome_unknown().into()
        }
        ApplicationExportMutationPortErrorV1::Integrity => integrity(service),
    }
}

fn observation_failure(
    service: &RiffDbServiceInner,
    error: ApplicationExportObservationPortErrorV1,
) -> ServiceFailure {
    match error {
        ApplicationExportObservationPortErrorV1::Unavailable => {
            PublicError::storage_unavailable().into()
        }
        ApplicationExportObservationPortErrorV1::Integrity => integrity(service),
    }
}

fn integrity(service: &RiffDbServiceInner) -> ServiceFailure {
    service.maintenance_internal_failure(MaintenanceInternalDefect::LowerIntegrity)
}
