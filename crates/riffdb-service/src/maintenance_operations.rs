//! API-neutral offline-maintenance authorization and lifecycle orchestration.

use std::sync::Arc;

use riffdb_errors::{IncidentIdSource, InternalError, PublicError};
use riffdb_policy::{
    AuthorizedOfflineMaintenance, OfflineMaintenanceAuthorizationRequest,
    OfflineMaintenanceDecision, OutputClassification,
};
use riffdb_types::{
    DatabaseId, Environment, OfflineMaintenanceInputHash, OfflineMaintenanceOperationId,
    OfflineMaintenanceOperationKind, TenantScope,
};

use crate::service::{
    MaintenanceInternalDefect, MaintenanceSubmissionState, RiffDbServiceInner,
    catch_maintenance_future_panic,
};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    AuthorizedOfflineMaintenanceObservation, AuthorizedOfflineMaintenanceStart,
    AuthorizedRestoreRetryStart, CreateOfflineBackupRequest, CurrentPolicyPort,
    GetOfflineMaintenanceOperationRequest, GetOfflineMaintenanceOperationResult,
    OfflineMaintenanceApplication, OfflineMaintenanceObservationPortError,
    OfflineMaintenanceOperationObservation, OfflineMaintenanceStartPortError,
    OfflineMaintenanceStartResult, PortAdmissionError, PortDriverStopped,
    RecoveryOfflineMaintenanceApplication, RecoveryOfflineMaintenanceCoordinatorPort,
    RecoveryOfflineMaintenancePortError, RecoveryOfflineMaintenanceRestore,
    RecoveryRestoreOfflineBackupInvocation, RequestContext, RequestDeadlineScheduler,
    RestoreOfflineBackupInvocation, RestoreOfflineBackupRequest,
    RestoreRetryOfflineMaintenanceApplication, RestoreRetryOfflineMaintenanceCoordinatorPort,
    RetireOfflineBackupRequest, RiffDbService, ServiceDiagnostics, ServiceFailure, ServiceFuture,
    ServiceHealthHooks, ServiceJobSpawner, ServiceResult, ensure_response_budget,
    port_completion_channel,
};

impl OfflineMaintenanceApplication for RiffDbService {
    fn create_offline_backup(
        &self,
        context: RequestContext,
        request: CreateOfflineBackupRequest,
    ) -> ServiceFuture<'_, OfflineMaintenanceStartResult> {
        let service = Arc::clone(&self.inner);
        let submission = Arc::new(MaintenanceSubmissionState::new());
        let operation_submission = Arc::clone(&submission);
        self.spawn_tracked_maintenance_operation(submission, async move {
            start_create_backup(service, context, request, operation_submission).await
        })
    }

    fn restore_offline_backup(
        &self,
        invocation: RestoreOfflineBackupInvocation,
    ) -> ServiceFuture<'_, OfflineMaintenanceStartResult> {
        let service = Arc::clone(&self.inner);
        let (context, request, credential) = invocation.into_parts();
        let submission = Arc::new(MaintenanceSubmissionState::new());
        let operation_submission = Arc::clone(&submission);
        self.spawn_tracked_maintenance_operation(submission, async move {
            start_restore_backup(service, context, request, credential, operation_submission).await
        })
    }

    fn retire_offline_backup(
        &self,
        context: RequestContext,
        request: RetireOfflineBackupRequest,
    ) -> ServiceFuture<'_, OfflineMaintenanceStartResult> {
        let service = Arc::clone(&self.inner);
        let submission = Arc::new(MaintenanceSubmissionState::new());
        let operation_submission = Arc::clone(&submission);
        self.spawn_tracked_maintenance_operation(submission, async move {
            start_retire_backup(service, context, request, operation_submission).await
        })
    }

    fn get_offline_maintenance_operation(
        &self,
        context: RequestContext,
        request: GetOfflineMaintenanceOperationRequest,
    ) -> ServiceFuture<'_, GetOfflineMaintenanceOperationResult> {
        let service = Arc::clone(&self.inner);
        self.spawn_maintenance_operation(
            async move { get_operation(service, context, request).await },
        )
    }
}

struct RecoveryOfflineMaintenanceServiceInner {
    coordinator: Arc<dyn RecoveryOfflineMaintenanceCoordinatorPort>,
    incident_ids: Arc<dyn IncidentIdSource>,
    diagnostics: Arc<dyn ServiceDiagnostics>,
    health: Arc<dyn ServiceHealthHooks>,
    spawner: Arc<dyn ServiceJobSpawner>,
    deadline_scheduler: Arc<dyn RequestDeadlineScheduler>,
}

/// Standalone recovery-only application capability.
///
/// This type has no normal application service, current policy, authenticated
/// principal, storage handle, create operation, or receipt-observation method.
#[derive(Clone)]
pub struct RecoveryOfflineMaintenanceService {
    inner: Arc<RecoveryOfflineMaintenanceServiceInner>,
}

impl RecoveryOfflineMaintenanceService {
    /// Composes the restricted restore capability from least-authority ports.
    #[must_use]
    pub fn new(
        coordinator: Arc<dyn RecoveryOfflineMaintenanceCoordinatorPort>,
        incident_ids: Arc<dyn IncidentIdSource>,
        diagnostics: Arc<dyn ServiceDiagnostics>,
        health: Arc<dyn ServiceHealthHooks>,
        spawner: Arc<dyn ServiceJobSpawner>,
        deadline_scheduler: Arc<dyn RequestDeadlineScheduler>,
    ) -> Self {
        Self {
            inner: Arc::new(RecoveryOfflineMaintenanceServiceInner {
                coordinator,
                incident_ids,
                diagnostics,
                health,
                spawner,
                deadline_scheduler,
            }),
        }
    }

    fn spawn_restore(
        &self,
        submission: Arc<MaintenanceSubmissionState>,
        future: impl std::future::Future<Output = ServiceResult<OfflineMaintenanceStartResult>>
        + Send
        + 'static,
    ) -> ServiceFuture<'static, OfflineMaintenanceStartResult> {
        let (sender, receipt) = port_completion_channel();
        let inner = Arc::clone(&self.inner);
        let job = Box::pin(async move {
            let result = match catch_maintenance_future_panic(future).await {
                Ok(result) => result,
                Err(()) if submission.may_have_been_submitted() => {
                    Err(PublicError::outcome_unknown().into())
                }
                Err(()) => Err(recovery_internal_failure(
                    &inner,
                    MaintenanceInternalDefect::Panic,
                )),
            };
            sender.complete(result);
        });
        self.inner.spawner.spawn(job);
        Box::pin(async move {
            match receipt.completion().await {
                Ok(result) => result,
                Err(PortDriverStopped) => {
                    panic!("accepted recovery service job stopped without publishing its result")
                }
            }
        })
    }
}

impl RecoveryOfflineMaintenanceApplication for RecoveryOfflineMaintenanceService {
    fn restore_offline_backup(
        &self,
        invocation: RecoveryRestoreOfflineBackupInvocation,
    ) -> ServiceFuture<'_, OfflineMaintenanceStartResult> {
        let inner = Arc::clone(&self.inner);
        let submission = Arc::new(MaintenanceSubmissionState::new());
        let operation_submission = Arc::clone(&submission);
        self.spawn_restore(submission, async move {
            start_recovery_restore(inner, invocation, operation_submission).await
        })
    }
}

impl std::fmt::Debug for RecoveryOfflineMaintenanceService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RecoveryOfflineMaintenanceService([CAPABILITY])")
    }
}

struct RestoreRetryOfflineMaintenanceServiceInner {
    operation_id: OfflineMaintenanceOperationId,
    input_hash: OfflineMaintenanceInputHash,
    database_id: DatabaseId,
    environment: Environment,
    policy: Arc<dyn CurrentPolicyPort>,
    coordinator: Arc<dyn RestoreRetryOfflineMaintenanceCoordinatorPort>,
    incident_ids: Arc<dyn IncidentIdSource>,
    diagnostics: Arc<dyn ServiceDiagnostics>,
    health: Arc<dyn ServiceHealthHooks>,
    spawner: Arc<dyn ServiceJobSpawner>,
    deadline_scheduler: Arc<dyn RequestDeadlineScheduler>,
}

/// Standalone current-database capability for one interrupted restore.
///
/// Construction freezes the receipt identity and canonical semantic input.
/// The type has no ordinary application surface, backup-create operation,
/// receipt-observation operation, or durable service-audit executor.
#[derive(Clone)]
pub struct RestoreRetryOfflineMaintenanceService {
    inner: Arc<RestoreRetryOfflineMaintenanceServiceInner>,
}

impl RestoreRetryOfflineMaintenanceService {
    /// Composes one exact retry from current policy and a restore-only coordinator.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        operation_id: OfflineMaintenanceOperationId,
        input_hash: OfflineMaintenanceInputHash,
        database_id: DatabaseId,
        environment: Environment,
        policy: Arc<dyn CurrentPolicyPort>,
        coordinator: Arc<dyn RestoreRetryOfflineMaintenanceCoordinatorPort>,
        incident_ids: Arc<dyn IncidentIdSource>,
        diagnostics: Arc<dyn ServiceDiagnostics>,
        health: Arc<dyn ServiceHealthHooks>,
        spawner: Arc<dyn ServiceJobSpawner>,
        deadline_scheduler: Arc<dyn RequestDeadlineScheduler>,
    ) -> Self {
        Self {
            inner: Arc::new(RestoreRetryOfflineMaintenanceServiceInner {
                operation_id,
                input_hash,
                database_id,
                environment,
                policy,
                coordinator,
                incident_ids,
                diagnostics,
                health,
                spawner,
                deadline_scheduler,
            }),
        }
    }

    fn spawn_restore(
        &self,
        submission: Arc<MaintenanceSubmissionState>,
        future: impl std::future::Future<Output = ServiceResult<OfflineMaintenanceStartResult>>
        + Send
        + 'static,
    ) -> ServiceFuture<'static, OfflineMaintenanceStartResult> {
        let (sender, receipt) = port_completion_channel();
        let inner = Arc::clone(&self.inner);
        let job = Box::pin(async move {
            let result = match catch_maintenance_future_panic(future).await {
                Ok(result) => result,
                Err(()) if submission.may_have_been_submitted() => {
                    Err(PublicError::outcome_unknown().into())
                }
                Err(()) => Err(restore_retry_internal_failure(
                    &inner,
                    MaintenanceInternalDefect::Panic,
                )),
            };
            sender.complete(result);
        });
        self.inner.spawner.spawn(job);
        Box::pin(async move {
            match receipt.completion().await {
                Ok(result) => result,
                Err(PortDriverStopped) => {
                    panic!(
                        "accepted restore-retry service job stopped without publishing its result"
                    )
                }
            }
        })
    }
}

impl RestoreRetryOfflineMaintenanceApplication for RestoreRetryOfflineMaintenanceService {
    fn restore_offline_backup(
        &self,
        invocation: RestoreOfflineBackupInvocation,
    ) -> ServiceFuture<'_, OfflineMaintenanceStartResult> {
        let inner = Arc::clone(&self.inner);
        let submission = Arc::new(MaintenanceSubmissionState::new());
        let operation_submission = Arc::clone(&submission);
        self.spawn_restore(submission, async move {
            start_restore_retry(inner, invocation, operation_submission).await
        })
    }
}

impl std::fmt::Debug for RestoreRetryOfflineMaintenanceService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RestoreRetryOfflineMaintenanceService([EXACT_CAPABILITY])")
    }
}

async fn start_create_backup(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: CreateOfflineBackupRequest,
    submission: Arc<MaintenanceSubmissionState>,
) -> ServiceResult<OfflineMaintenanceStartResult> {
    ensure_control_open(context.control())?;
    let policy_request = OfflineMaintenanceAuthorizationRequest::create_backup(
        request.operation_id(),
        request.input_hash(),
    );
    authorize_current(&service, &context, policy_request.clone())?;

    let coordinator = service
        .providers
        .maintenance
        .as_ref()
        .map(Arc::clone)
        .ok_or_else(|| ServiceFailure::from(PublicError::storage_unavailable()))?;
    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        coordinator.reserve_start(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => return Err(pre_submit_admission_failure(error)),
        Err(error) => return Err(controlled_wait_failure(error)),
    };

    ensure_control_open(context.control())?;
    let authorization = authorize_current(&service, &context, policy_request)?;
    ensure_control_open(context.control())?;
    let expected = request.clone();
    submission.mark_submit_in_flight();
    let receipt = match permit.submit(AuthorizedOfflineMaintenanceStart::CreateBackup {
        request,
        authorization,
    }) {
        Ok(receipt) => receipt,
        Err(error) => {
            submission.mark_submit_rejected();
            return Err(pre_submit_admission_failure(error));
        }
    };
    let result = await_start_result(&service, &context, receipt).await?;
    if !observation_matches_create(result.operation(), &expected) {
        return Err(service.maintenance_internal_failure(MaintenanceInternalDefect::LowerIntegrity));
    }
    ensure_response_budget(&result)?;
    Ok(result)
}

async fn start_restore_backup(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: RestoreOfflineBackupRequest,
    credential: riffdb_auth::RetainedOpaqueCredential,
    submission: Arc<MaintenanceSubmissionState>,
) -> ServiceResult<OfflineMaintenanceStartResult> {
    ensure_control_open(context.control())?;
    let policy_request = OfflineMaintenanceAuthorizationRequest::restore_backup(
        request.operation_id(),
        request.input_hash(),
    );
    authorize_current(&service, &context, policy_request.clone())?;

    let coordinator = service
        .providers
        .maintenance
        .as_ref()
        .map(Arc::clone)
        .ok_or_else(|| ServiceFailure::from(PublicError::storage_unavailable()))?;
    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        coordinator.reserve_start(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => return Err(pre_submit_admission_failure(error)),
        Err(error) => return Err(controlled_wait_failure(error)),
    };

    ensure_control_open(context.control())?;
    let authorization = authorize_current(&service, &context, policy_request)?;
    ensure_control_open(context.control())?;
    let expected = request.clone();
    submission.mark_submit_in_flight();
    let receipt = match permit.submit(AuthorizedOfflineMaintenanceStart::RestoreBackup {
        request,
        authorization,
        credential,
    }) {
        Ok(receipt) => receipt,
        Err(error) => {
            submission.mark_submit_rejected();
            return Err(pre_submit_admission_failure(error));
        }
    };
    let result = await_start_result(&service, &context, receipt).await?;
    if !observation_matches_restore(result.operation(), &expected) {
        return Err(service.maintenance_internal_failure(MaintenanceInternalDefect::LowerIntegrity));
    }
    ensure_response_budget(&result)?;
    Ok(result)
}

async fn start_retire_backup(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: RetireOfflineBackupRequest,
    submission: Arc<MaintenanceSubmissionState>,
) -> ServiceResult<OfflineMaintenanceStartResult> {
    ensure_control_open(context.control())?;
    let policy_request = OfflineMaintenanceAuthorizationRequest::retire_backup(
        request.operation_id(),
        request.input_hash(),
    );
    authorize_current(&service, &context, policy_request.clone())?;
    let coordinator = service
        .providers
        .maintenance
        .as_ref()
        .map(Arc::clone)
        .ok_or_else(|| ServiceFailure::from(PublicError::storage_unavailable()))?;
    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        coordinator.reserve_start(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => return Err(pre_submit_admission_failure(error)),
        Err(error) => return Err(controlled_wait_failure(error)),
    };
    ensure_control_open(context.control())?;
    let authorization = authorize_current(&service, &context, policy_request)?;
    ensure_control_open(context.control())?;
    let expected = request.clone();
    submission.mark_submit_in_flight();
    let receipt = match permit.submit(AuthorizedOfflineMaintenanceStart::RetireBackup {
        request,
        authorization,
    }) {
        Ok(receipt) => receipt,
        Err(error) => {
            submission.mark_submit_rejected();
            return Err(pre_submit_admission_failure(error));
        }
    };
    let result = await_start_result(&service, &context, receipt).await?;
    if !observation_matches_retire(result.operation(), &expected) {
        return Err(service.maintenance_internal_failure(MaintenanceInternalDefect::LowerIntegrity));
    }
    ensure_response_budget(&result)?;
    Ok(result)
}

async fn get_operation(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: GetOfflineMaintenanceOperationRequest,
) -> ServiceResult<GetOfflineMaintenanceOperationResult> {
    ensure_control_open(context.control())?;
    let policy_request =
        OfflineMaintenanceAuthorizationRequest::get_operation(request.operation_id());
    authorize_current(&service, &context, policy_request.clone())?;

    let coordinator = service
        .providers
        .maintenance
        .as_ref()
        .map(Arc::clone)
        .ok_or_else(|| ServiceFailure::from(PublicError::storage_unavailable()))?;
    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        coordinator.reserve_observation(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => return Err(pre_submit_admission_failure(error)),
        Err(error) => return Err(controlled_wait_failure(error)),
    };

    ensure_control_open(context.control())?;
    let authorization = authorize_current(&service, &context, policy_request)?;
    ensure_control_open(context.control())?;
    let receipt = permit
        .submit(AuthorizedOfflineMaintenanceObservation::new(
            request,
            authorization,
        ))
        .map_err(pre_submit_admission_failure)?;
    let observation = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(observation))) => observation,
        Ok(Ok(Err(error))) => return Err(observation_port_failure(&service, error)),
        Ok(Err(PortDriverStopped)) => {
            return Err(PublicError::storage_unavailable().into());
        }
        Err(error) => return Err(controlled_wait_failure(error)),
    };
    if observation
        .as_ref()
        .is_some_and(|observed| observed.operation_id() != request.operation_id())
    {
        return Err(service.maintenance_internal_failure(MaintenanceInternalDefect::LowerIntegrity));
    }
    let result = observation.map_or(
        GetOfflineMaintenanceOperationResult::NotFound,
        GetOfflineMaintenanceOperationResult::Found,
    );
    ensure_response_budget(&result)?;
    Ok(result)
}

async fn start_restore_retry(
    service: Arc<RestoreRetryOfflineMaintenanceServiceInner>,
    invocation: RestoreOfflineBackupInvocation,
    submission: Arc<MaintenanceSubmissionState>,
) -> ServiceResult<OfflineMaintenanceStartResult> {
    let (context, request, credential) = invocation.into_parts();
    if request.operation_id() != service.operation_id || request.input_hash() != service.input_hash
    {
        return Err(PublicError::idempotency_key_reuse().into());
    }

    ensure_control_open(context.control())?;
    let policy_request = OfflineMaintenanceAuthorizationRequest::restore_backup(
        request.operation_id(),
        request.input_hash(),
    );
    authorize_restore_retry(&service, &context, policy_request.clone())?;

    let permit = match wait_with_control(
        context.control(),
        service.deadline_scheduler.as_ref(),
        service.coordinator.reserve_restore(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => return Err(pre_submit_admission_failure(error)),
        Err(error) => return Err(controlled_wait_failure(error)),
    };

    ensure_control_open(context.control())?;
    let authorization = authorize_restore_retry(&service, &context, policy_request)?;
    ensure_control_open(context.control())?;
    let expected = request.clone();
    submission.mark_submit_in_flight();
    let receipt = match permit.submit(AuthorizedRestoreRetryStart::new(
        request,
        authorization,
        credential,
    )) {
        Ok(receipt) => receipt,
        Err(error) => {
            submission.mark_submit_rejected();
            return Err(pre_submit_admission_failure(error));
        }
    };
    let result = match wait_with_control(
        context.control(),
        service.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(result))) => result,
        Ok(Ok(Err(error))) => return Err(restore_retry_port_failure(&service, error)),
        Ok(Err(PortDriverStopped)) | Err(_) => {
            return Err(post_submit_start_uncertainty());
        }
    };
    if !observation_matches_restore(result.operation(), &expected) {
        return Err(restore_retry_internal_failure(
            &service,
            MaintenanceInternalDefect::LowerIntegrity,
        ));
    }
    ensure_response_budget(&result)?;
    Ok(result)
}

async fn start_recovery_restore(
    service: Arc<RecoveryOfflineMaintenanceServiceInner>,
    invocation: RecoveryRestoreOfflineBackupInvocation,
    submission: Arc<MaintenanceSubmissionState>,
) -> ServiceResult<OfflineMaintenanceStartResult> {
    let (request_id, control, request, credential) = invocation.into_parts();
    let permit = match wait_with_control(
        &control,
        service.deadline_scheduler.as_ref(),
        service.coordinator.reserve_restore(&control),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => return Err(pre_submit_admission_failure(error)),
        Err(error) => return Err(controlled_wait_failure(error)),
    };

    ensure_control_open(&control)?;
    let expected = request.clone();
    submission.mark_submit_in_flight();
    let receipt = match permit.submit(RecoveryOfflineMaintenanceRestore::new(
        request_id, request, credential,
    )) {
        Ok(receipt) => receipt,
        Err(error) => {
            submission.mark_submit_rejected();
            return Err(pre_submit_admission_failure(error));
        }
    };
    let result =
        match wait_with_control(&control, service.deadline_scheduler.as_ref(), receipt).await {
            Ok(Ok(Ok(result))) => result,
            Ok(Ok(Err(error))) => return Err(recovery_port_failure(&service, error)),
            Ok(Err(PortDriverStopped)) | Err(_) => {
                return Err(post_submit_start_uncertainty());
            }
        };
    if !observation_matches_restore(result.operation(), &expected) {
        return Err(recovery_internal_failure(
            &service,
            MaintenanceInternalDefect::LowerIntegrity,
        ));
    }
    ensure_response_budget(&result)?;
    Ok(result)
}

fn authorize_restore_retry(
    service: &RestoreRetryOfflineMaintenanceServiceInner,
    context: &RequestContext,
    request: OfflineMaintenanceAuthorizationRequest,
) -> ServiceResult<Box<AuthorizedOfflineMaintenance>> {
    let authorization = match service
        .policy
        .authorize_offline_maintenance(context.principal(), request.clone())
    {
        Ok(OfflineMaintenanceDecision::Allow(authorization)) => authorization,
        Ok(OfflineMaintenanceDecision::Deny(_)) => {
            return Err(PublicError::authorization_denied().into());
        }
        Err(_) => return Err(PublicError::storage_unavailable().into()),
    };
    if !valid_authorization_facts(
        service.database_id,
        &service.environment,
        context,
        &request,
        &authorization,
    ) {
        return Err(restore_retry_internal_failure(
            service,
            MaintenanceInternalDefect::ProofMismatch,
        ));
    }
    Ok(authorization)
}

fn authorize_current(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request: OfflineMaintenanceAuthorizationRequest,
) -> ServiceResult<Box<AuthorizedOfflineMaintenance>> {
    let authorization = match service
        .providers
        .policy
        .authorize_offline_maintenance(context.principal(), request.clone())
    {
        Ok(OfflineMaintenanceDecision::Allow(authorization)) => authorization,
        Ok(OfflineMaintenanceDecision::Deny(_)) => {
            return Err(PublicError::authorization_denied().into());
        }
        Err(_) => return Err(PublicError::storage_unavailable().into()),
    };
    if !valid_authorization_facts(
        service.identity.database_id(),
        service.identity.environment(),
        context,
        &request,
        &authorization,
    ) {
        return Err(service.maintenance_internal_failure(MaintenanceInternalDefect::ProofMismatch));
    }
    Ok(authorization)
}

fn valid_authorization_facts(
    database_id: DatabaseId,
    environment: &Environment,
    context: &RequestContext,
    request: &OfflineMaintenanceAuthorizationRequest,
    authorization: &AuthorizedOfflineMaintenance,
) -> bool {
    let obligations = authorization.obligations();
    authorization.database_id() == database_id
        && authorization.environment() == environment
        && authorization.request() == request
        && authorization.authorizing_capability_id() == context.principal().capability_id()
        && authorization.authorizing_capability_revision()
            == context.principal().capability_revision()
        && authorization.principal_id() == context.principal().principal_id()
        && authorization.actor_kind() == context.principal().actor_kind()
        && obligations.effective_tenant_scope() == &TenantScope::Global
        && obligations.partition_constraint().is_none()
        && obligations.field_mask().is_none()
        && obligations.row_limit().is_none()
        && obligations.audit_class().is_none()
        && obligations.output_classification() == OutputClassification::AdministrativeRedactedData
}

async fn await_start_result(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    receipt: crate::PortReceipt<OfflineMaintenanceStartResult, OfflineMaintenanceStartPortError>,
) -> ServiceResult<OfflineMaintenanceStartResult> {
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(result))) => Ok(result),
        Ok(Ok(Err(error))) => Err(start_port_failure(service, error)),
        Ok(Err(PortDriverStopped)) | Err(_) => Err(post_submit_start_uncertainty()),
    }
}

fn observation_matches_create(
    observation: &OfflineMaintenanceOperationObservation,
    request: &CreateOfflineBackupRequest,
) -> bool {
    observation.operation_id() == request.operation_id()
        && observation.kind() == OfflineMaintenanceOperationKind::CreateBackup
        && observation.backup_name() == request.backup_name()
        && observation.input_hash() == request.input_hash()
}

fn observation_matches_restore(
    observation: &OfflineMaintenanceOperationObservation,
    request: &RestoreOfflineBackupRequest,
) -> bool {
    observation.operation_id() == request.operation_id()
        && observation.kind() == OfflineMaintenanceOperationKind::RestoreBackup
        && observation.backup_name() == request.backup_name()
        && observation.input_hash() == request.input_hash()
}

fn observation_matches_retire(
    observation: &OfflineMaintenanceOperationObservation,
    request: &RetireOfflineBackupRequest,
) -> bool {
    observation.operation_id() == request.operation_id()
        && observation.kind() == OfflineMaintenanceOperationKind::RetireBackup
        && observation.backup_name() == request.backup_name()
        && observation.input_hash() == request.input_hash()
}

fn pre_submit_admission_failure(error: PortAdmissionError) -> ServiceFailure {
    match error {
        PortAdmissionError::Cancelled => ServiceFailure::Cancelled,
        PortAdmissionError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
        PortAdmissionError::Unavailable | PortAdmissionError::Stopped => {
            ServiceFailure::Public(PublicError::storage_unavailable())
        }
    }
}

fn controlled_wait_failure(error: ControlledWaitError) -> ServiceFailure {
    match error {
        ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
        ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
    }
}

fn ensure_control_open(control: &crate::RequestControl) -> ServiceResult<()> {
    if control.is_cancelled() {
        Err(ServiceFailure::Cancelled)
    } else if control.is_deadline_exceeded() {
        Err(ServiceFailure::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn post_submit_start_uncertainty() -> ServiceFailure {
    ServiceFailure::Public(PublicError::outcome_unknown())
}

fn start_port_failure(
    service: &RiffDbServiceInner,
    error: OfflineMaintenanceStartPortError,
) -> ServiceFailure {
    match error {
        OfflineMaintenanceStartPortError::InputMismatch => {
            PublicError::idempotency_key_reuse().into()
        }
        OfflineMaintenanceStartPortError::Unavailable => PublicError::storage_unavailable().into(),
        OfflineMaintenanceStartPortError::OutcomeUnknown => PublicError::outcome_unknown().into(),
        OfflineMaintenanceStartPortError::Integrity => {
            service.maintenance_internal_failure(MaintenanceInternalDefect::LowerIntegrity)
        }
    }
}

fn observation_port_failure(
    service: &RiffDbServiceInner,
    error: OfflineMaintenanceObservationPortError,
) -> ServiceFailure {
    match error {
        OfflineMaintenanceObservationPortError::Unavailable => {
            PublicError::storage_unavailable().into()
        }
        OfflineMaintenanceObservationPortError::Integrity => {
            service.maintenance_internal_failure(MaintenanceInternalDefect::LowerIntegrity)
        }
    }
}

fn recovery_port_failure(
    service: &RecoveryOfflineMaintenanceServiceInner,
    error: RecoveryOfflineMaintenancePortError,
) -> ServiceFailure {
    match error {
        RecoveryOfflineMaintenancePortError::InputMismatch => {
            PublicError::idempotency_key_reuse().into()
        }
        RecoveryOfflineMaintenancePortError::AuthorizationDenied => {
            PublicError::authorization_denied().into()
        }
        RecoveryOfflineMaintenancePortError::Unavailable => {
            PublicError::storage_unavailable().into()
        }
        RecoveryOfflineMaintenancePortError::OutcomeUnknown => {
            PublicError::outcome_unknown().into()
        }
        RecoveryOfflineMaintenancePortError::Integrity => {
            recovery_internal_failure(service, MaintenanceInternalDefect::LowerIntegrity)
        }
    }
}

fn restore_retry_port_failure(
    service: &RestoreRetryOfflineMaintenanceServiceInner,
    error: OfflineMaintenanceStartPortError,
) -> ServiceFailure {
    match error {
        OfflineMaintenanceStartPortError::InputMismatch => {
            PublicError::idempotency_key_reuse().into()
        }
        OfflineMaintenanceStartPortError::Unavailable => PublicError::storage_unavailable().into(),
        OfflineMaintenanceStartPortError::OutcomeUnknown => PublicError::outcome_unknown().into(),
        OfflineMaintenanceStartPortError::Integrity => {
            restore_retry_internal_failure(service, MaintenanceInternalDefect::LowerIntegrity)
        }
    }
}

fn restore_retry_internal_failure(
    service: &RestoreRetryOfflineMaintenanceServiceInner,
    defect: MaintenanceInternalDefect,
) -> ServiceFailure {
    if matches!(defect, MaintenanceInternalDefect::LowerIntegrity) {
        service
            .health
            .fail_authoritative_readiness(crate::AuthoritativeReadinessFailure::Integrity);
    }
    match service.incident_ids.next_incident_id() {
        Ok(incident_id) => {
            service
                .diagnostics
                .record_internal(InternalError::new(incident_id, defect));
            PublicError::internal_defect(incident_id).into()
        }
        Err(error) => {
            service
                .health
                .fail_authoritative_readiness(crate::AuthoritativeReadinessFailure::Integrity);
            riffdb_errors::EmergencyInternalFailure::from(error).into()
        }
    }
}

fn recovery_internal_failure(
    service: &RecoveryOfflineMaintenanceServiceInner,
    defect: MaintenanceInternalDefect,
) -> ServiceFailure {
    if matches!(defect, MaintenanceInternalDefect::LowerIntegrity) {
        service
            .health
            .fail_authoritative_readiness(crate::AuthoritativeReadinessFailure::Integrity);
    }
    match service.incident_ids.next_incident_id() {
        Ok(incident_id) => {
            service
                .diagnostics
                .record_internal(InternalError::new(incident_id, defect));
            PublicError::internal_defect(incident_id).into()
        }
        Err(error) => {
            service
                .health
                .fail_authoritative_readiness(crate::AuthoritativeReadinessFailure::Integrity);
            riffdb_errors::EmergencyInternalFailure::from(error).into()
        }
    }
}

#[cfg(test)]
mod tests {
    use riffdb_types::{
        BackupNameV1, OfflineMaintenanceOperationId, OfflineMaintenanceReplacementConfirmation,
    };

    use super::*;
    use crate::{
        OfflineMaintenanceObservationFailure, OfflineMaintenanceObservationPhase,
        OfflineMaintenanceStartDisposition,
    };

    fn operation_id() -> OfflineMaintenanceOperationId {
        OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [2; 10])
            .expect("operation ID")
    }

    #[test]
    fn start_result_must_match_the_exact_semantic_input() {
        let create = CreateOfflineBackupRequest::new(
            operation_id(),
            BackupNameV1::new("safe").expect("name"),
        )
        .expect("request");
        let observation = OfflineMaintenanceOperationObservation::new(
            create.operation_id(),
            OfflineMaintenanceOperationKind::CreateBackup,
            create.backup_name().clone(),
            create.input_hash(),
            OfflineMaintenanceObservationPhase::Accepted,
            None,
        )
        .expect("observation");
        let result = OfflineMaintenanceStartResult::new(
            OfflineMaintenanceStartDisposition::Accepted,
            observation,
        )
        .expect("result");
        assert!(observation_matches_create(result.operation(), &create));

        let restore = RestoreOfflineBackupRequest::new(
            operation_id(),
            create.backup_name().clone(),
            OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
        )
        .expect("request");
        assert!(!observation_matches_restore(result.operation(), &restore));
    }

    #[test]
    fn post_submit_start_control_loss_is_always_uncertain() {
        let result = post_submit_start_uncertainty();
        assert_eq!(
            result.public_error().map(riffdb_errors::PublicError::kind),
            Some(riffdb_errors::PublicErrorKind::OutcomeUnknown)
        );
    }

    #[test]
    fn failed_closed_observation_requires_one_safe_failure() {
        let name = BackupNameV1::new("safe").expect("name");
        let hash = riffdb_types::offline_maintenance_input_hash(
            OfflineMaintenanceOperationKind::CreateBackup,
            &name,
            OfflineMaintenanceReplacementConfirmation::NotProvided,
        );
        let terminal = OfflineMaintenanceOperationObservation::new(
            operation_id(),
            OfflineMaintenanceOperationKind::CreateBackup,
            name.clone(),
            hash,
            OfflineMaintenanceObservationPhase::FailedClosed,
            Some(OfflineMaintenanceObservationFailure::InternalFailure),
        )
        .expect("failed-closed observation");
        assert!(terminal.phase().is_terminal());
        assert!(
            OfflineMaintenanceStartResult::new(
                OfflineMaintenanceStartDisposition::Terminal,
                terminal.clone(),
            )
            .is_ok()
        );
        assert_eq!(
            OfflineMaintenanceOperationObservation::new(
                operation_id(),
                OfflineMaintenanceOperationKind::CreateBackup,
                name.clone(),
                hash,
                OfflineMaintenanceObservationPhase::FailedClosed,
                None,
            ),
            Err(crate::ServiceDtoError::InvalidShape)
        );
        assert_eq!(
            OfflineMaintenanceOperationObservation::new(
                operation_id(),
                OfflineMaintenanceOperationKind::CreateBackup,
                name,
                hash,
                OfflineMaintenanceObservationPhase::Accepted,
                Some(OfflineMaintenanceObservationFailure::InternalFailure),
            ),
            Err(crate::ServiceDtoError::InvalidShape)
        );
        assert_eq!(
            OfflineMaintenanceStartResult::new(
                OfflineMaintenanceStartDisposition::Accepted,
                terminal,
            ),
            Err(crate::ServiceDtoError::InvalidShape)
        );
    }

    #[test]
    fn maintenance_source_has_no_durable_service_audit_entrypoint() {
        let source = include_str!("maintenance_operations.rs");
        for forbidden in [
            concat!("ServiceOperation", "V1"),
            concat!("begin_", "invocation"),
            concat!("append_", "audit"),
            concat!("StoredServiceAuditRecord", "V1"),
        ] {
            assert!(!source.contains(forbidden), "{forbidden}");
        }
        assert_eq!(
            source
                .matches(concat!(".authorize_offline_", "maintenance("))
                .count(),
            2,
            "normal maintenance and the disjoint current retry each own one fresh-policy path"
        );
    }

    #[test]
    fn ready_service_records_no_transport_specific_operation_identity() {
        let _ = riffdb_types::ServiceIngressKindV1::Grpc;
        let source = include_str!("maintenance_operations.rs");
        assert!(!source.contains(concat!("riffdb_", "proto")));
        assert!(!source.contains(concat!("to", "nic")));
        assert!(!source.contains(concat!("pro", "st")));
    }
}
