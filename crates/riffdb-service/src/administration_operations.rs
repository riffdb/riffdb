//! Health, statistics, capability administration, and outbox-status orchestration.

use std::sync::Arc;

use riffdb_catalog::{CatalogError, CatalogErrorKind, validate_capability_partition_scope};
use riffdb_commit::{
    CapabilityBootstrapExecutionResult, CapabilityBootstrapOutcome, CapabilityBootstrapPreparation,
    CapabilityCreateOutcome, CapabilityCreatePreparation, CapabilityRevokeOutcome,
    CapabilityRevokePreparation, CapabilityTransition, ControlPlaneExecutionAdmissionError,
    ControlPlaneExecutionError, ControlPlaneExecutionErrorKind, ControlPlanePreparationError,
    ControlPlaneTerminalAudit,
};
use riffdb_errors::{
    PublicError, ValidationCode, ValidationIssue, ValidationIssues, ValidationPath,
};
use riffdb_policy::{
    AbsentCapabilityRevokeTargetFacts, AuditClass, AuthorizedOperation,
    CapabilityCreateTargetFacts, CapabilityRevokeTargetFacts, OperationRequest,
    OutputClassification, PartitionConstraint,
};
use riffdb_types::{
    PartitionScopeV1, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceOperationV1, TenantScope,
};

use crate::orchestration::{AuditScope, BegunCapabilityMutation, BegunInvocation};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    AdministrationApplication, AuthoritativeReadError, AuthoritativeReadinessFailure,
    BootstrapCapabilityRequest, BootstrapCapabilityResult, CapabilityIdentityView,
    CapabilityRevokeTargetSnapshot, CapabilityTokenIssueError, CapabilityTransitionView,
    CreateCapabilityInvocation, CreateCapabilityResult, CursorAccessError, HealthContext,
    HealthReport, HealthRequest, HealthResult, InternalDefect, ListPendingOutboxDeliveriesRequest,
    ListPendingOutboxDeliveriesResult, NormalCreateCapabilityRequest, NormalCreateCapabilityResult,
    OperationalStatusError, OutboxCursorLookup, OutboxCursorPolicy, OutboxCursorState,
    OutboxStatusPortError, OutboxStatusRequest, Page, PageLimit, PendingTerminalResponse,
    PortAdmissionError, PortDriverStopped, RequestContext, RevokeCapabilityRequest,
    RevokeCapabilityResult, RiffDbService, RiffDbServiceInner, ServiceAuditTargetMap,
    ServiceFailure, ServiceFuture, ServiceResult, ServiceTelemetryEvent, StatisticsRequest,
    StatisticsResult, ensure_response_budget, fit_page_items, pre_bootstrap_health_result,
};

const MAX_CAPABILITY_REVOKE_PREPARATION_ATTEMPTS: usize = 3;

impl AdministrationApplication for RiffDbService {
    fn fence_replication_primary(
        &self,
        context: RequestContext,
        request: crate::FenceReplicationPrimaryRequest,
    ) -> ServiceFuture<'_, crate::FenceReplicationPrimaryResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::FenceReplicationPrimary,
            ingress,
            async move {
                let request = riffdb_auth::PrimaryFenceRequestV1::new(
                    context.request_id(),
                    request.operation_id(),
                    request.target(),
                    request.generation(),
                );
                crate::primary_fence_operations::execute(service, context, request).await
            },
        )
    }

    fn register_follower(
        &self,
        context: RequestContext,
        request: crate::RegisterFollowerRequest,
    ) -> ServiceFuture<'_, crate::RegisterFollowerResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::RegisterFollower, ingress, async move {
            let request = riffdb_auth::ReplicationAdministrationRequestV1::register(
                context.request_id(),
                request.target(),
                request.budget(),
                request.expires_at(),
            );
            crate::replication_administration_operations::execute(service, context, request).await
        })
    }
    fn retire_follower(
        &self,
        context: RequestContext,
        request: crate::RetireFollowerRequest,
    ) -> ServiceFuture<'_, crate::RetireFollowerResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::RetireFollower, ingress, async move {
            let request = riffdb_auth::ReplicationAdministrationRequestV1::retire(
                context.request_id(),
                request.target(),
                request.generation(),
            );
            crate::replication_administration_operations::execute(service, context, request).await
        })
    }

    fn health(
        &self,
        context: HealthContext,
        _request: HealthRequest,
    ) -> ServiceFuture<'_, HealthResult> {
        match context {
            HealthContext::PreBootstrap(context) => {
                pre_bootstrap_health_result(&self.inner.pre_bootstrap_health, context)
            }
            HealthContext::Authenticated(context) => {
                let service = Arc::clone(&self.inner);
                let ingress = context.ingress();
                self.spawn_operation(ServiceOperationV1::GetHealth, ingress, async move {
                    authenticated_health(service, *context).await
                })
            }
        }
    }

    fn statistics(
        &self,
        context: RequestContext,
        _request: StatisticsRequest,
    ) -> ServiceFuture<'_, StatisticsResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::GetStatistics, ingress, async move {
            statistics(service, context).await
        })
    }

    fn create_capability(
        &self,
        invocation: CreateCapabilityInvocation,
    ) -> ServiceFuture<'_, CreateCapabilityResult> {
        let service = Arc::clone(&self.inner);
        let ingress = match &invocation {
            CreateCapabilityInvocation::Normal { context, .. } => context.ingress(),
            CreateCapabilityInvocation::Bootstrap { context, .. } => context.ingress(),
        };
        self.spawn_operation(ServiceOperationV1::CreateCapability, ingress, async move {
            match invocation {
                CreateCapabilityInvocation::Normal { context, request } => {
                    create_capability_normal(service, context, request).await
                }
                CreateCapabilityInvocation::Bootstrap { context, request } => {
                    create_capability_bootstrap(service, context, request).await
                }
            }
        })
    }

    fn revoke_capability(
        &self,
        context: RequestContext,
        request: RevokeCapabilityRequest,
    ) -> ServiceFuture<'_, RevokeCapabilityResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::RevokeCapability, ingress, async move {
            revoke_capability(service, context, request).await
        })
    }

    fn list_pending_outbox_deliveries(
        &self,
        context: RequestContext,
        request: ListPendingOutboxDeliveriesRequest,
    ) -> ServiceFuture<'_, ListPendingOutboxDeliveriesResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::ListPendingOutboxDeliveries,
            ingress,
            async move { list_pending_outbox_deliveries(service, context, request).await },
        )
    }
}

async fn authenticated_health(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
) -> ServiceResult<HealthResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::GetHealth;
    let begun = service
        .begin_invocation(
            &context,
            OperationRequest::get_health(),
            ServiceAuditTargetMap::health(),
            AuditScope::StandardRead,
        )
        .await?;

    let catalog_permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .catalog
            .reserve_active_catalog(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => {
            return Err(finish_read_admission(&service, &context, &begun, error).await);
        }
        Err(error) => {
            return Err(finish_read_wait(&service, &context, &begun, error).await);
        }
    };
    let authorization = begun.reauthorize(&service, &context).await?;
    if !valid_health_authorization(&service, &authorization) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_read_failure(&service, &context, &begun, failure).await);
    }
    let catalog_receipt = match catalog_permit.submit(()) {
        Ok(receipt) => receipt,
        Err(error) => {
            return Err(finish_read_admission(&service, &context, &begun, error).await);
        }
    };
    let active = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        catalog_receipt,
    )
    .await
    {
        Ok(Ok(Ok(active))) => active,
        Ok(Ok(Err(error))) => {
            let failure = catalog_failure(&service, OPERATION, error);
            return Err(finish_read_failure(&service, &context, &begun, failure).await);
        }
        Ok(Err(PortDriverStopped)) => {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_read_failure(&service, &context, &begun, failure).await);
        }
        Err(error) => {
            return Err(finish_read_wait(&service, &context, &begun, error).await);
        }
    };

    let health_permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .operational
            .reserve_health(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => {
            return Err(finish_read_admission(&service, &context, &begun, error).await);
        }
        Err(error) => {
            return Err(finish_read_wait(&service, &context, &begun, error).await);
        }
    };
    let authorization = begun.reauthorize(&service, &context).await?;
    if !valid_health_authorization(&service, &authorization) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_read_failure(&service, &context, &begun, failure).await);
    }
    let health_receipt = match health_permit.submit(()) {
        Ok(receipt) => receipt,
        Err(error) => {
            return Err(finish_read_admission(&service, &context, &begun, error).await);
        }
    };
    let operational_health = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        health_receipt,
    )
    .await
    {
        Ok(Ok(Ok(snapshot))) => snapshot,
        Ok(Ok(Err(error))) => {
            let failure = operational_failure(&service, OPERATION, error);
            return Err(finish_read_failure(&service, &context, &begun, failure).await);
        }
        Ok(Err(PortDriverStopped)) => {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_read_failure(&service, &context, &begun, failure).await);
        }
        Err(error) => {
            return Err(finish_read_wait(&service, &context, &begun, error).await);
        }
    };

    let statistics_permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .operational
            .reserve_statistics(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => {
            return Err(finish_read_admission(&service, &context, &begun, error).await);
        }
        Err(error) => {
            return Err(finish_read_wait(&service, &context, &begun, error).await);
        }
    };
    let authorization = begun.reauthorize(&service, &context).await?;
    if !valid_health_authorization(&service, &authorization) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_read_failure(&service, &context, &begun, failure).await);
    }
    let statistics_receipt = match statistics_permit.submit(()) {
        Ok(receipt) => receipt,
        Err(error) => {
            return Err(finish_read_admission(&service, &context, &begun, error).await);
        }
    };
    let operational_statistics = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        statistics_receipt,
    )
    .await
    {
        Ok(Ok(Ok(snapshot))) => snapshot,
        Ok(Ok(Err(error))) => {
            let failure = operational_failure(&service, OPERATION, error);
            return Err(finish_read_failure(&service, &context, &begun, failure).await);
        }
        Ok(Err(PortDriverStopped)) => {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_read_failure(&service, &context, &begun, failure).await);
        }
        Err(error) => {
            return Err(finish_read_wait(&service, &context, &begun, error).await);
        }
    };

    // Health is a potentially long aggregate read. Recheck before the complete
    // report becomes visible, even though each lower admission was also fresh.
    let authorization = begun.reauthorize(&service, &context).await?;
    if !valid_health_authorization(&service, &authorization) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_read_failure(&service, &context, &begun, failure).await);
    }
    let report = HealthReport::new(
        active.map(|active| active.bundle().contract_version()),
        operational_statistics.last_commit_sequence(),
        operational_health,
        service.process.started_at(),
        service.process.build().clone(),
    )
    .for_role(if service.executors.is_follower() {
        crate::ReplicationRole::Follower
    } else {
        crate::ReplicationRole::Primary
    });
    let result = HealthResult::Authenticated(report);
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_read_failure(&service, &context, &begun, failure).await);
    }
    finish_read_success(&service, &context, &begun).await?;
    Ok(result)
}

async fn statistics(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
) -> ServiceResult<StatisticsResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::GetStatistics;
    let begun = service
        .begin_invocation(
            &context,
            OperationRequest::get_statistics(),
            ServiceAuditTargetMap::statistics(),
            AuditScope::Intrinsic,
        )
        .await?;
    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .operational
            .reserve_statistics(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => {
            return Err(finish_read_admission(&service, &context, &begun, error).await);
        }
        Err(error) => {
            return Err(finish_read_wait(&service, &context, &begun, error).await);
        }
    };
    let authorization = begun.reauthorize(&service, &context).await?;
    if !valid_administrative_authorization(&service, &authorization, OPERATION, false) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_read_failure(&service, &context, &begun, failure).await);
    }
    let receipt = match permit.submit(()) {
        Ok(receipt) => receipt,
        Err(error) => {
            return Err(finish_read_admission(&service, &context, &begun, error).await);
        }
    };
    let operational = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(snapshot))) => snapshot,
        Ok(Ok(Err(error))) => {
            let failure = operational_failure(&service, OPERATION, error);
            return Err(finish_read_failure(&service, &context, &begun, failure).await);
        }
        Ok(Err(PortDriverStopped)) => {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_read_failure(&service, &context, &begun, failure).await);
        }
        Err(error) => {
            return Err(finish_read_wait(&service, &context, &begun, error).await);
        }
    };
    let active_cursors = match service.cursors.active_count() {
        Ok(count) => count,
        Err(_) => {
            service
                .providers
                .telemetry
                .record(ServiceTelemetryEvent::CursorUnavailable);
            return Err(finish_read_failure(
                &service,
                &context,
                &begun,
                PublicError::storage_unavailable().into(),
            )
            .await);
        }
    };
    let authorization = begun.reauthorize(&service, &context).await?;
    if !valid_administrative_authorization(&service, &authorization, OPERATION, false) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_read_failure(&service, &context, &begun, failure).await);
    }
    let result = StatisticsResult::new(
        active_cursors,
        service.active_commit_subscribers(),
        operational,
    )
    .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch));
    let result = match result {
        Ok(result) => result,
        Err(failure) => {
            return Err(finish_read_failure(&service, &context, &begun, failure).await);
        }
    };
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_read_failure(&service, &context, &begun, failure).await);
    }
    finish_read_success(&service, &context, &begun).await?;
    Ok(result)
}

async fn create_capability_normal(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: NormalCreateCapabilityRequest,
) -> ServiceResult<CreateCapabilityResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::CreateCapability;
    let capability_id = request.capability_id();
    let targets = ServiceAuditTargetMap::create_capability(capability_id)
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    service.classify_intrinsic_prestart(&context, OPERATION, targets.clone())?;

    if matches!(
        request.requested().grant().partition_scope(),
        PartitionScopeV1::Explicit(_)
    ) {
        let active = match wait_with_control(
            context.control(),
            service.providers.deadline_scheduler.as_ref(),
            service
                .providers
                .catalog
                .prepare_active_catalog(context.control()),
        )
        .await
        {
            Ok(Ok(Some(active))) => active,
            Ok(Ok(None)) => {
                let failure = root_validation(ValidationCode::InvalidValue).into();
                return Err(finish_create_capability_prestart_failure(
                    &service,
                    &context,
                    targets,
                    ServiceAuditPhaseV1::Failed,
                    failure,
                )
                .await);
            }
            Ok(Err(error)) => {
                let failure = catalog_failure(&service, OPERATION, error);
                return Err(finish_create_capability_prestart_failure(
                    &service,
                    &context,
                    targets,
                    ServiceAuditPhaseV1::Failed,
                    failure,
                )
                .await);
            }
            Err(error) => {
                let failure = controlled_wait_failure(error);
                return Err(finish_create_capability_prestart_failure(
                    &service,
                    &context,
                    targets,
                    ServiceAuditPhaseV1::Cancelled,
                    failure,
                )
                .await);
            }
        };
        if validate_capability_partition_scope(
            active.bundle(),
            request.requested().grant().partition_scope(),
        )
        .is_err()
        {
            let failure = root_validation(ValidationCode::InvalidValue).into();
            return Err(finish_create_capability_prestart_failure(
                &service,
                &context,
                targets,
                ServiceAuditPhaseV1::Failed,
                failure,
            )
            .await);
        }
    }

    let target = CapabilityCreateTargetFacts::new(
        context.request_id(),
        capability_id,
        request.requested().clone(),
    );
    let begun = service
        .begin_capability_mutation(
            &context,
            OperationRequest::create_capability(target),
            targets,
        )
        .await?;

    let issued = match service.providers.token_issuer.issue() {
        Ok(issued) => issued,
        Err(CapabilityTokenIssueError::Unavailable) => {
            return Err(finish_mutation_failure(
                &service,
                &context,
                &begun,
                ServiceAuditPhaseV1::Failed,
                PublicError::storage_unavailable().into(),
            )
            .await);
        }
        Err(CapabilityTokenIssueError::Integrity) => {
            let failure = service.internal_failure(OPERATION, InternalDefect::LowerIntegrity);
            return Err(finish_mutation_failure(
                &service,
                &context,
                &begun,
                ServiceAuditPhaseV1::Failed,
                failure,
            )
            .await);
        }
    };

    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service.executors.writer()?.control_plane.reserve_capacity(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => {
            return Err(finish_control_plane_admission(&service, &context, &begun, error).await);
        }
        Err(error) => {
            return Err(finish_mutation_wait(&service, &context, &begun, error).await);
        }
    };
    let authorization = begun.reauthorize(&service, &context).await?;
    let preparation = match CapabilityCreatePreparation::new(*authorization, issued.digest()) {
        Ok(preparation) => preparation,
        Err(_) => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_mutation_failure(
                &service,
                &context,
                &begun,
                ServiceAuditPhaseV1::Failed,
                failure,
            )
            .await);
        }
    };
    let receipt = match permit.submit_capability_create(preparation) {
        Ok(receipt) => receipt,
        Err(error) => {
            return Err(finish_control_plane_admission(&service, &context, &begun, error).await);
        }
    };
    let execution = match receipt.completion().await {
        Ok(execution) => execution,
        Err(error) => {
            return Err(finish_control_plane_execution(&service, &context, &begun, error).await);
        }
    };
    let terminal = execution.terminal_audit();
    let outcome = execution.into_outcome();
    let result = CreateCapabilityResult::Normal(match outcome {
        CapabilityCreateOutcome::Created(transition) => {
            let (token, _digest) = issued.into_parts();
            NormalCreateCapabilityResult::Created {
                transition: transition_view(transition),
                token,
            }
        }
        CapabilityCreateOutcome::AlreadyCreatedTokenUnavailable(identity) => {
            drop(issued);
            NormalCreateCapabilityResult::AlreadyCreatedTokenUnavailable(
                CapabilityIdentityView::new(identity.capability_id(), identity.revision()),
            )
        }
        CapabilityCreateOutcome::CapabilityIdConflict => {
            drop(issued);
            NormalCreateCapabilityResult::CapabilityIdConflict
        }
    });
    let pending = PendingTerminalResponse::new(result, terminal, ensure_response_budget);
    finish_mutation_terminal(&service, &context, &begun, pending.terminal()).await?;
    pending.into_response()
}

async fn create_capability_bootstrap(
    service: Arc<RiffDbServiceInner>,
    context: crate::BootstrapRequestContext,
    request: BootstrapCapabilityRequest,
) -> ServiceResult<CreateCapabilityResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::CreateCapability;
    if request.requested().database_id() != service.identity.database_id()
        || request
            .requested()
            .grant()
            .permissions()
            .contains_kind(riffdb_types::CapabilityPermissionKindV1::ReplicateChangelog)
        || request
            .requested()
            .grant()
            .permissions()
            .contains_kind(riffdb_types::CapabilityPermissionKindV1::FenceReplicationPrimary)
        || request.requested().environment() != service.identity.environment()
        || matches!(
            request.requested().grant().partition_scope(),
            PartitionScopeV1::Explicit(_)
        )
    {
        return Err(root_validation(ValidationCode::InvalidValue).into());
    }
    if context.control().is_cancelled() {
        return Err(ServiceFailure::Cancelled);
    }
    if context.control().is_deadline_exceeded() {
        return Err(ServiceFailure::DeadlineExceeded);
    }
    let targets = ServiceAuditTargetMap::create_capability(request.capability_id())
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let preparation = CapabilityBootstrapPreparation::new(
        request.capability_id(),
        request.requested(),
        context.digests().as_slice().to_vec(),
        context.digests().current(),
        context.request_id(),
        context.ingress(),
        targets,
    );
    let preparation = match preparation {
        Ok(preparation) => preparation,
        Err(ControlPlanePreparationError::InvalidBootstrap) => {
            return Err(root_validation(ValidationCode::InvalidValue).into());
        }
        Err(
            ControlPlanePreparationError::AuthorizationMismatch
            | ControlPlanePreparationError::InternalDefect,
        ) => return Err(service.internal_failure(OPERATION, InternalDefect::ProofMismatch)),
    };

    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service.executors.writer()?.control_plane.reserve_capacity(),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => return Err(bootstrap_admission_failure(&service, error)),
        Err(ControlledWaitError::Cancelled) => return Err(ServiceFailure::Cancelled),
        Err(ControlledWaitError::DeadlineExceeded) => return Err(ServiceFailure::DeadlineExceeded),
    };
    if context.control().is_cancelled() {
        return Err(ServiceFailure::Cancelled);
    }
    if context.control().is_deadline_exceeded() {
        return Err(ServiceFailure::DeadlineExceeded);
    }
    let receipt = match permit.submit_capability_bootstrap(preparation) {
        Ok(receipt) => receipt,
        Err(error) => return Err(bootstrap_admission_failure(&service, error)),
    };
    match receipt.completion().await {
        Ok(CapabilityBootstrapExecutionResult::BootstrapConflict) => {
            let result =
                CreateCapabilityResult::Bootstrap(BootstrapCapabilityResult::BootstrapConflict);
            ensure_response_budget(&result)?;
            Ok(result)
        }
        Ok(CapabilityBootstrapExecutionResult::Completed(completion)) => {
            let (outcome, terminal) = completion.into_parts();
            let result = CreateCapabilityResult::Bootstrap(match outcome {
                CapabilityBootstrapOutcome::Created(transition) => {
                    BootstrapCapabilityResult::Created(transition_view(transition))
                }
                CapabilityBootstrapOutcome::Replayed(transition) => {
                    BootstrapCapabilityResult::Replayed(transition_view(transition))
                }
            });
            let charge = ensure_response_budget(&result);
            if service
                .finish_bootstrap_terminal(context.control(), terminal)
                .await
                .is_err()
            {
                return Err(PublicError::outcome_unknown().into());
            }
            charge?;
            Ok(result)
        }
        Err(error) => Err(bootstrap_execution_failure(&service, error)),
    }
}

async fn finish_create_capability_prestart_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    targets: riffdb_types::ServiceAuditTargetsV1,
    phase: ServiceAuditPhaseV1,
    failure: ServiceFailure,
) -> ServiceFailure {
    match service
        .append_prestart_terminal_if_intrinsic(
            context,
            ServiceOperationV1::CreateCapability,
            targets,
            AuditScope::Intrinsic,
            phase,
        )
        .await
    {
        Ok(()) => failure,
        Err(audit_failure) => audit_failure,
    }
}

async fn revoke_capability(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: RevokeCapabilityRequest,
) -> ServiceResult<RevokeCapabilityResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::RevokeCapability;
    let targets = ServiceAuditTargetMap::revoke_capability(request.capability_id())
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    service.classify_intrinsic_prestart(&context, OPERATION, targets.clone())?;
    let initial_snapshot = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .authoritative
            .read_capability_revoke_target(context.control(), request.capability_id()),
    )
    .await
    {
        Ok(Ok(snapshot)) => snapshot,
        Ok(Err(error)) => {
            let failure = authoritative_failure(&service, OPERATION, error);
            service
                .append_prestart_terminal_if_intrinsic(
                    &context,
                    OPERATION,
                    targets,
                    AuditScope::Intrinsic,
                    ServiceAuditPhaseV1::Failed,
                )
                .await?;
            return Err(failure);
        }
        Err(error) => {
            let phase = ServiceAuditPhaseV1::Cancelled;
            service
                .append_prestart_terminal_if_intrinsic(
                    &context,
                    OPERATION,
                    targets,
                    AuditScope::Intrinsic,
                    phase,
                )
                .await?;
            return Err(controlled_wait_failure(error));
        }
    };
    let initial_policy = match revoke_policy_request(&service, &context, request, initial_snapshot)
    {
        Ok(request) => request,
        Err(failure) => {
            service
                .append_prestart_terminal_if_intrinsic(
                    &context,
                    OPERATION,
                    targets.clone(),
                    AuditScope::Intrinsic,
                    ServiceAuditPhaseV1::Failed,
                )
                .await?;
            return Err(failure);
        }
    };
    let begun = service
        .begin_capability_mutation(&context, initial_policy, targets)
        .await?;

    for _attempt in 0..MAX_CAPABILITY_REVOKE_PREPARATION_ATTEMPTS {
        let permit = match wait_with_control(
            context.control(),
            service.providers.deadline_scheduler.as_ref(),
            service.executors.writer()?.control_plane.reserve_capacity(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(error)) => {
                return Err(
                    finish_control_plane_admission(&service, &context, &begun, error).await,
                );
            }
            Err(error) => {
                return Err(finish_mutation_wait(&service, &context, &begun, error).await);
            }
        };

        let snapshot = match wait_with_control(
            context.control(),
            service.providers.deadline_scheduler.as_ref(),
            service
                .providers
                .authoritative
                .read_capability_revoke_target(context.control(), request.capability_id()),
        )
        .await
        {
            Ok(Ok(snapshot)) => snapshot,
            Ok(Err(error)) => {
                let failure = authoritative_failure(&service, OPERATION, error);
                return Err(finish_mutation_failure(
                    &service,
                    &context,
                    &begun,
                    ServiceAuditPhaseV1::Failed,
                    failure,
                )
                .await);
            }
            Err(error) => {
                return Err(finish_mutation_wait(&service, &context, &begun, error).await);
            }
        };
        let policy_request = match revoke_policy_request(&service, &context, request, snapshot) {
            Ok(request) => request,
            Err(failure) => {
                return Err(finish_mutation_failure(
                    &service,
                    &context,
                    &begun,
                    ServiceAuditPhaseV1::Failed,
                    failure,
                )
                .await);
            }
        };
        let authorization = begun
            .reauthorize_request(&service, &context, policy_request)
            .await?;
        let preparation = match CapabilityRevokePreparation::new(*authorization) {
            Ok(preparation) => preparation,
            Err(_) => {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_mutation_failure(
                    &service,
                    &context,
                    &begun,
                    ServiceAuditPhaseV1::Failed,
                    failure,
                )
                .await);
            }
        };
        let receipt = match permit.submit_capability_revoke(preparation) {
            Ok(receipt) => receipt,
            Err(error) => {
                return Err(
                    finish_control_plane_admission(&service, &context, &begun, error).await,
                );
            }
        };
        let execution = match receipt.completion().await {
            Ok(execution) => execution,
            Err(error) => {
                return Err(
                    finish_control_plane_execution(&service, &context, &begun, error).await,
                );
            }
        };
        if execution.outcome() == &CapabilityRevokeOutcome::CapabilityPreparationChanged {
            continue;
        }
        let terminal = match execution.terminal_audit() {
            Some(terminal) => terminal,
            None => {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_mutation_failure(
                    &service,
                    &context,
                    &begun,
                    ServiceAuditPhaseV1::Failed,
                    failure,
                )
                .await);
            }
        };
        let result = match execution.into_outcome() {
            CapabilityRevokeOutcome::Revoked(transition) => {
                RevokeCapabilityResult::Revoked(transition_view(transition))
            }
            CapabilityRevokeOutcome::AlreadyRevoked(transition) => {
                RevokeCapabilityResult::AlreadyRevoked(transition_view(transition))
            }
            CapabilityRevokeOutcome::CapabilityNotFound => {
                RevokeCapabilityResult::CapabilityNotFound
            }
            CapabilityRevokeOutcome::CapabilityPreparationChanged => {
                return Err(service.internal_failure(OPERATION, InternalDefect::ProofMismatch));
            }
        };
        let pending = PendingTerminalResponse::new(result, terminal, ensure_response_budget);
        finish_mutation_terminal(&service, &context, &begun, pending.terminal()).await?;
        return pending.into_response();
    }

    Err(finish_mutation_failure(
        &service,
        &context,
        &begun,
        ServiceAuditPhaseV1::Failed,
        PublicError::storage_unavailable().into(),
    )
    .await)
}

async fn list_pending_outbox_deliveries(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: ListPendingOutboxDeliveriesRequest,
) -> ServiceResult<ListPendingOutboxDeliveriesResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ListPendingOutboxDeliveries;
    let page_request = request.page();
    let policy_request =
        OperationRequest::list_pending_outbox_deliveries(page_request.limit().get());
    let begun = service
        .begin_invocation(
            &context,
            policy_request,
            ServiceAuditTargetMap::list_pending_outbox_deliveries(),
            AuditScope::Intrinsic,
        )
        .await?;
    if !valid_administrative_authorization(&service, begun.initial_authorization(), OPERATION, true)
    {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_read_failure(&service, &context, &begun, failure).await);
    }
    let Some(outbox) = service.providers.outbox.as_ref() else {
        return Err(finish_read_failure(
            &service,
            &context,
            &begun,
            PublicError::storage_unavailable().into(),
        )
        .await);
    };

    let lookup = OutboxCursorLookup::new(page_request.limit());
    let prior_state = match page_request.cursor() {
        Some(cursor) => match service.cursors.resolve_outbox(
            cursor,
            context.principal().principal_id(),
            &lookup,
        ) {
            Ok(state) => Some(state),
            Err(CursorAccessError::InvalidCursor) => {
                return Err(finish_read_failure(
                    &service,
                    &context,
                    &begun,
                    root_validation(ValidationCode::InvalidValue).into(),
                )
                .await);
            }
            Err(CursorAccessError::Unavailable) => {
                service
                    .providers
                    .telemetry
                    .record(ServiceTelemetryEvent::CursorUnavailable);
                return Err(finish_read_failure(
                    &service,
                    &context,
                    &begun,
                    PublicError::storage_unavailable().into(),
                )
                .await);
            }
        },
        None => None,
    };
    let initial_limit =
        match effective_page_limit(page_request.limit(), begun.initial_authorization()) {
            Some(limit) => limit,
            None => {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_read_failure(&service, &context, &begun, failure).await);
            }
        };
    let initial_policy = match constrain_outbox_policy(
        begun
            .initial_authorization()
            .obligations()
            .effective_tenant_scope(),
        begun
            .initial_authorization()
            .obligations()
            .partition_constraint(),
        initial_limit,
        prior_state.as_deref().map(OutboxCursorState::policy),
    ) {
        Some(policy) => policy,
        None => {
            return Err(finish_read_failure(
                &service,
                &context,
                &begun,
                root_validation(ValidationCode::InvalidValue).into(),
            )
            .await);
        }
    };

    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        outbox.reserve_pending_status(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => {
            return Err(finish_read_admission(&service, &context, &begun, error).await);
        }
        Err(error) => {
            return Err(finish_read_wait(&service, &context, &begun, error).await);
        }
    };
    let authorization = begun.reauthorize(&service, &context).await?;
    if !valid_administrative_authorization(&service, &authorization, OPERATION, true) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_read_failure(&service, &context, &begun, failure).await);
    }
    let current_limit = match effective_page_limit(page_request.limit(), &authorization) {
        Some(limit) => limit,
        None => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_read_failure(&service, &context, &begun, failure).await);
        }
    };
    let effective_policy = match constrain_outbox_policy(
        authorization.obligations().effective_tenant_scope(),
        authorization.obligations().partition_constraint(),
        current_limit,
        Some(&initial_policy),
    ) {
        Some(policy) => policy,
        None => {
            return Err(finish_read_failure(
                &service,
                &context,
                &begun,
                root_validation(ValidationCode::InvalidValue).into(),
            )
            .await);
        }
    };
    let effective_limit = effective_policy.effective_limit();
    let lower_request = OutboxStatusRequest::new(
        prior_state.as_ref().map(|state| state.after()),
        effective_limit,
    );
    let receipt = match permit.submit(lower_request) {
        Ok(receipt) => receipt,
        Err(error) => {
            return Err(finish_read_admission(&service, &context, &begun, error).await);
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
            let failure = outbox_failure(&service, OPERATION, error);
            return Err(finish_read_failure(&service, &context, &begun, failure).await);
        }
        Ok(Err(PortDriverStopped)) => {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_read_failure(&service, &context, &begun, failure).await);
        }
        Err(error) => {
            return Err(finish_read_wait(&service, &context, &begun, error).await);
        }
    };
    let return_authorization = begun.reauthorize(&service, &context).await?;
    if !valid_administrative_authorization(&service, &return_authorization, OPERATION, true) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_read_failure(&service, &context, &begun, failure).await);
    }
    let return_limit = match effective_page_limit(page_request.limit(), &return_authorization) {
        Some(limit) => limit,
        None => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_read_failure(&service, &context, &begun, failure).await);
        }
    };
    let return_policy = match constrain_outbox_policy(
        return_authorization.obligations().effective_tenant_scope(),
        return_authorization.obligations().partition_constraint(),
        return_limit,
        Some(&effective_policy),
    ) {
        Some(policy) => policy,
        None => {
            return Err(finish_read_failure(
                &service,
                &context,
                &begun,
                root_validation(ValidationCode::InvalidValue).into(),
            )
            .await);
        }
    };
    let return_limit = return_policy.effective_limit();
    let mut items = snapshot.items().to_vec();
    let more_due_to_limit = items.len() > usize::from(return_limit.get().get());
    items.truncate(usize::from(return_limit.get().get()));
    let fit = match fit_page_items(
        &items,
        &(),
        more_due_to_limit || snapshot.next_after().is_some(),
    ) {
        Ok(fit) => fit,
        Err(failure) => {
            return Err(finish_read_failure(&service, &context, &begun, failure).await);
        }
    };
    let continuation_after = if fit.has_more() {
        if fit.item_count() < items.len() || more_due_to_limit {
            items.get(fit.item_count() - 1).map(|item| item.event_id())
        } else {
            snapshot.next_after()
        }
    } else {
        None
    };
    items.truncate(fit.item_count());
    let cursor_guard = match continuation_after {
        Some(after) => match service.cursors.register_outbox_unpublished(
            context.principal().principal_id(),
            lookup,
            OutboxCursorState::new(after, return_policy),
        ) {
            Ok(guard) => Some(guard),
            Err(_) => {
                service
                    .providers
                    .telemetry
                    .record(ServiceTelemetryEvent::CursorUnavailable);
                return Err(finish_read_failure(
                    &service,
                    &context,
                    &begun,
                    PublicError::storage_unavailable().into(),
                )
                .await);
            }
        },
        None => None,
    };
    let next_cursor = cursor_guard
        .as_ref()
        .map(crate::CursorPublicationGuard::token);
    let page = match Page::new(return_limit, items, next_cursor, ()) {
        Ok(page) => page,
        Err(_) => {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_read_failure(&service, &context, &begun, failure).await);
        }
    };
    let result = ListPendingOutboxDeliveriesResult::new(page);
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_read_failure(&service, &context, &begun, failure).await);
    }
    finish_read_success(&service, &context, &begun).await?;
    if let Some(guard) = cursor_guard {
        guard.publish();
    }
    Ok(result)
}

fn revoke_policy_request(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    request: RevokeCapabilityRequest,
    snapshot: CapabilityRevokeTargetSnapshot,
) -> ServiceResult<OperationRequest> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::RevokeCapability;
    match snapshot {
        CapabilityRevokeTargetSnapshot::Absent(snapshot) => {
            if snapshot.capability_id() != request.capability_id()
                || snapshot.database_id() != service.identity.database_id()
                || snapshot.environment() != service.identity.environment()
            {
                return Err(service.internal_failure(OPERATION, InternalDefect::ProofMismatch));
            }
            Ok(OperationRequest::revoke_absent_capability(
                AbsentCapabilityRevokeTargetFacts::new(
                    context.request_id(),
                    snapshot.capability_id(),
                    snapshot.database_id(),
                    snapshot.environment().clone(),
                ),
                request.reason(),
            ))
        }
        CapabilityRevokeTargetSnapshot::Present(snapshot) => {
            if snapshot.capability_id() != request.capability_id()
                || snapshot.database_id() != service.identity.database_id()
                || snapshot.environment() != service.identity.environment()
            {
                return Err(service.internal_failure(OPERATION, InternalDefect::ProofMismatch));
            }
            let target = CapabilityRevokeTargetFacts::new(
                context.request_id(),
                snapshot.capability_id(),
                snapshot.revision(),
                snapshot.activity(),
                snapshot.database_id(),
                snapshot.environment().clone(),
                snapshot.principal_id().clone(),
                snapshot.actor_kind(),
                snapshot.audiences().to_vec(),
                snapshot.issued_at(),
                snapshot.expires_at(),
                snapshot.grant().clone(),
            )
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
            Ok(OperationRequest::revoke_capability(
                target,
                request.reason(),
            ))
        }
    }
}

fn valid_health_authorization(
    service: &RiffDbServiceInner,
    authorization: &AuthorizedOperation,
) -> bool {
    let obligations = authorization.obligations();
    authorization.database_id() == service.identity.database_id()
        && authorization.environment() == service.identity.environment()
        && authorization.operation() == ServiceOperationV1::GetHealth
        && obligations.audit_class().is_none()
        && obligations.output_classification() == OutputClassification::PublicMetadata
        && obligations.partition_constraint().is_none()
        && obligations.field_mask().is_none()
        && obligations.row_limit().is_none()
}

fn valid_administrative_authorization(
    service: &RiffDbServiceInner,
    authorization: &AuthorizedOperation,
    operation: ServiceOperationV1,
    permits_row_limit: bool,
) -> bool {
    let obligations = authorization.obligations();
    authorization.database_id() == service.identity.database_id()
        && authorization.environment() == service.identity.environment()
        && authorization.operation() == operation
        && obligations.audit_class() == Some(AuditClass::AdministrativeRead)
        && obligations.output_classification() == OutputClassification::AdministrativeRedactedData
        && obligations.effective_tenant_scope() == &TenantScope::Global
        && valid_administrative_partition(operation, obligations.partition_constraint())
        && obligations.field_mask().is_none()
        && (permits_row_limit || obligations.row_limit().is_none())
}

fn valid_administrative_partition(
    operation: ServiceOperationV1,
    partition: Option<&PartitionConstraint>,
) -> bool {
    match operation {
        ServiceOperationV1::GetStatistics => partition.is_none(),
        ServiceOperationV1::ListPendingOutboxDeliveries => {
            partition == Some(&PartitionConstraint::Filter(PartitionScopeV1::All))
        }
        _ => false,
    }
}

fn constrain_outbox_policy(
    current_tenant_scope: &TenantScope,
    current_partition_constraint: Option<&PartitionConstraint>,
    current_limit: PageLimit,
    prior: Option<&OutboxCursorPolicy>,
) -> Option<OutboxCursorPolicy> {
    let current_partition_constraint = current_partition_constraint?;
    if current_tenant_scope != &TenantScope::Global
        || current_partition_constraint != &PartitionConstraint::Filter(PartitionScopeV1::All)
    {
        return None;
    }
    let effective_limit = match prior {
        Some(prior) => {
            if prior.effective_tenant_scope() != current_tenant_scope
                || prior.partition_constraint() != current_partition_constraint
            {
                return None;
            }
            current_limit.min(prior.effective_limit())
        }
        None => current_limit,
    };
    Some(OutboxCursorPolicy::new(
        current_tenant_scope.clone(),
        current_partition_constraint.clone(),
        effective_limit,
    ))
}

fn effective_page_limit(
    requested: PageLimit,
    authorization: &AuthorizedOperation,
) -> Option<PageLimit> {
    let requested = requested.get().get();
    let effective = authorization
        .obligations()
        .row_limit()
        .map_or(requested, |policy| requested.min(policy.get()));
    PageLimit::new(effective).ok()
}

fn transition_view(transition: CapabilityTransition) -> CapabilityTransitionView {
    let identity = transition.identity();
    CapabilityTransitionView::new(
        CapabilityIdentityView::new(identity.capability_id(), identity.revision()),
        transition.administration_sequence(),
    )
}

fn bootstrap_admission_failure(
    service: &RiffDbServiceInner,
    error: ControlPlaneExecutionAdmissionError,
) -> ServiceFailure {
    if error == ControlPlaneExecutionAdmissionError::PrimaryFenced {
        return PublicError::primary_fenced().into();
    }
    if error == ControlPlaneExecutionAdmissionError::Fenced {
        service
            .providers
            .health
            .fail_authoritative_readiness(AuthoritativeReadinessFailure::CoordinatorFenced);
    }
    PublicError::storage_unavailable().into()
}

fn bootstrap_execution_failure(
    service: &RiffDbServiceInner,
    error: ControlPlaneExecutionError,
) -> ServiceFailure {
    match error.kind() {
        ControlPlaneExecutionErrorKind::OutcomeUnknown => {
            service
                .providers
                .health
                .fail_authoritative_readiness(AuthoritativeReadinessFailure::CoordinatorFenced);
            PublicError::outcome_unknown().into()
        }
        ControlPlaneExecutionErrorKind::CoordinatorFenced => {
            service
                .providers
                .health
                .fail_authoritative_readiness(AuthoritativeReadinessFailure::CoordinatorFenced);
            PublicError::storage_unavailable().into()
        }
        ControlPlaneExecutionErrorKind::InternalDefect => {
            lower_integrity_failure(service, ServiceOperationV1::CreateCapability)
        }
        ControlPlaneExecutionErrorKind::StorageUnavailable => {
            // Bootstrap's compound transition includes its principal-less start.
            // A proven-abort dependency failure therefore also fails the required
            // bootstrap audit path, including administration-clock outages.
            service.note_audit_failure(ServiceOperationV1::CreateCapability);
            PublicError::storage_unavailable().into()
        }
        ControlPlaneExecutionErrorKind::AuthorizationDenied
        | ControlPlaneExecutionErrorKind::CoordinatorStopped => {
            PublicError::storage_unavailable().into()
        }
    }
}

async fn finish_mutation_terminal(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunCapabilityMutation,
    terminal: ControlPlaneTerminalAudit,
) -> ServiceResult<()> {
    let (phase, link, failure) = match terminal {
        ControlPlaneTerminalAudit::Succeeded(link) => (
            ServiceAuditPhaseV1::Succeeded,
            link,
            PublicError::outcome_unknown(),
        ),
        ControlPlaneTerminalAudit::Failed => (
            ServiceAuditPhaseV1::Failed,
            ServiceAuditLinkV1::None,
            PublicError::storage_unavailable(),
        ),
    };
    if begun.finish(service, context, phase, link).await.is_err() {
        service.note_audit_failure(
            begun
                .initial_authorization()
                .create_target()
                .map_or(ServiceOperationV1::RevokeCapability, |_| {
                    ServiceOperationV1::CreateCapability
                }),
        );
        Err(failure.into())
    } else {
        Ok(())
    }
}

async fn finish_control_plane_execution(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunCapabilityMutation,
    error: ControlPlaneExecutionError,
) -> ServiceFailure {
    let operation = if begun.initial_authorization().create_target().is_some() {
        ServiceOperationV1::CreateCapability
    } else {
        ServiceOperationV1::RevokeCapability
    };
    let (phase, failure) = match error.kind() {
        ControlPlaneExecutionErrorKind::AuthorizationDenied => (
            ServiceAuditPhaseV1::Denied,
            PublicError::authorization_denied().into(),
        ),
        ControlPlaneExecutionErrorKind::OutcomeUnknown => (
            ServiceAuditPhaseV1::OutcomeUncertain,
            PublicError::outcome_unknown().into(),
        ),
        ControlPlaneExecutionErrorKind::InternalDefect => (
            ServiceAuditPhaseV1::Failed,
            lower_integrity_failure(service, operation),
        ),
        ControlPlaneExecutionErrorKind::StorageUnavailable
        | ControlPlaneExecutionErrorKind::CoordinatorStopped
        | ControlPlaneExecutionErrorKind::CoordinatorFenced => (
            ServiceAuditPhaseV1::Failed,
            PublicError::storage_unavailable().into(),
        ),
    };
    if matches!(
        error.kind(),
        ControlPlaneExecutionErrorKind::OutcomeUnknown
            | ControlPlaneExecutionErrorKind::CoordinatorFenced
    ) {
        service
            .providers
            .health
            .fail_authoritative_readiness(AuthoritativeReadinessFailure::CoordinatorFenced);
    }
    if begun
        .finish(service, context, phase, ServiceAuditLinkV1::None)
        .await
        .is_err()
    {
        service.note_audit_failure(operation);
        return match error.kind() {
            ControlPlaneExecutionErrorKind::AuthorizationDenied => failure,
            ControlPlaneExecutionErrorKind::OutcomeUnknown => PublicError::outcome_unknown().into(),
            _ => PublicError::storage_unavailable().into(),
        };
    }
    failure
}

async fn finish_control_plane_admission(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunCapabilityMutation,
    error: ControlPlaneExecutionAdmissionError,
) -> ServiceFailure {
    if error == ControlPlaneExecutionAdmissionError::PrimaryFenced {
        return finish_mutation_failure(
            service,
            context,
            begun,
            ServiceAuditPhaseV1::Denied,
            PublicError::primary_fenced().into(),
        )
        .await;
    }
    if error == ControlPlaneExecutionAdmissionError::Fenced {
        service
            .providers
            .health
            .fail_authoritative_readiness(AuthoritativeReadinessFailure::CoordinatorFenced);
    }
    finish_mutation_failure(
        service,
        context,
        begun,
        ServiceAuditPhaseV1::Failed,
        PublicError::storage_unavailable().into(),
    )
    .await
}

async fn finish_mutation_wait(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunCapabilityMutation,
    error: ControlledWaitError,
) -> ServiceFailure {
    finish_mutation_failure(
        service,
        context,
        begun,
        ServiceAuditPhaseV1::Cancelled,
        controlled_wait_failure(error),
    )
    .await
}

async fn finish_mutation_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunCapabilityMutation,
    phase: ServiceAuditPhaseV1,
    failure: ServiceFailure,
) -> ServiceFailure {
    if begun
        .finish(service, context, phase, ServiceAuditLinkV1::None)
        .await
        .is_err()
    {
        let operation = if begun.initial_authorization().create_target().is_some() {
            ServiceOperationV1::CreateCapability
        } else {
            ServiceOperationV1::RevokeCapability
        };
        service.note_audit_failure(operation);
        PublicError::storage_unavailable().into()
    } else {
        failure
    }
}

async fn finish_read_success(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
) -> ServiceResult<()> {
    if begun
        .finish(
            service,
            context,
            ServiceAuditPhaseV1::Succeeded,
            ServiceAuditLinkV1::None,
        )
        .await
        .is_err()
    {
        service.note_audit_failure(begun.operation());
        Err(PublicError::storage_unavailable().into())
    } else {
        Ok(())
    }
}

async fn finish_read_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    failure: ServiceFailure,
) -> ServiceFailure {
    if begun
        .finish(
            service,
            context,
            ServiceAuditPhaseV1::Failed,
            ServiceAuditLinkV1::None,
        )
        .await
        .is_err()
    {
        service.note_audit_failure(begun.operation());
        PublicError::storage_unavailable().into()
    } else {
        failure
    }
}

async fn finish_read_wait(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    error: ControlledWaitError,
) -> ServiceFailure {
    let failure = controlled_wait_failure(error);
    if begun
        .finish(
            service,
            context,
            ServiceAuditPhaseV1::Cancelled,
            ServiceAuditLinkV1::None,
        )
        .await
        .is_err()
    {
        service.note_audit_failure(begun.operation());
        PublicError::storage_unavailable().into()
    } else {
        failure
    }
}

async fn finish_read_admission(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    error: PortAdmissionError,
) -> ServiceFailure {
    match error {
        PortAdmissionError::Cancelled => {
            finish_read_wait(service, context, begun, ControlledWaitError::Cancelled).await
        }
        PortAdmissionError::DeadlineExceeded => {
            finish_read_wait(
                service,
                context,
                begun,
                ControlledWaitError::DeadlineExceeded,
            )
            .await
        }
        PortAdmissionError::Unavailable | PortAdmissionError::Stopped => {
            finish_read_failure(
                service,
                context,
                begun,
                PublicError::storage_unavailable().into(),
            )
            .await
        }
    }
}

const fn controlled_wait_failure(error: ControlledWaitError) -> ServiceFailure {
    match error {
        ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
        ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
    }
}

fn operational_failure(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: OperationalStatusError,
) -> ServiceFailure {
    match error {
        OperationalStatusError::Unavailable => PublicError::storage_unavailable().into(),
        OperationalStatusError::Integrity => lower_integrity_failure(service, operation),
    }
}

fn outbox_failure(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: OutboxStatusPortError,
) -> ServiceFailure {
    match error {
        OutboxStatusPortError::Unavailable => PublicError::storage_unavailable().into(),
        OutboxStatusPortError::Integrity => lower_integrity_failure(service, operation),
    }
}

fn authoritative_failure(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: AuthoritativeReadError,
) -> ServiceFailure {
    match error {
        AuthoritativeReadError::Unavailable => PublicError::storage_unavailable().into(),
        AuthoritativeReadError::Cancelled => ServiceFailure::Cancelled,
        AuthoritativeReadError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
        // Retired history is a correct-request client outcome (RDB-HISTORY-0102),
        // never a lower-integrity failure.
        AuthoritativeReadError::HistoryPruned => PublicError::history_pruned().into(),
        AuthoritativeReadError::Integrity | AuthoritativeReadError::InvalidContinuation => {
            lower_integrity_failure(service, operation)
        }
    }
}

fn catalog_failure(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: CatalogError,
) -> ServiceFailure {
    if error.kind() == CatalogErrorKind::Storage {
        PublicError::storage_unavailable().into()
    } else {
        lower_integrity_failure(service, operation)
    }
}

fn lower_integrity_failure(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
) -> ServiceFailure {
    service
        .providers
        .health
        .fail_authoritative_readiness(AuthoritativeReadinessFailure::Integrity);
    service.internal_failure(operation, InternalDefect::LowerIntegrity)
}

fn root_validation(code: ValidationCode) -> PublicError {
    PublicError::validation(ValidationIssues::one(ValidationIssue::new(
        code,
        ValidationPath::root(),
    )))
}

#[cfg(test)]
mod tests {
    use riffdb_catalog::ValidatedContractBundle;
    use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
    use riffdb_types::{
        AggregateTypeId, CanonicalValue, ContractLineage, EnumVariantId, PartitionKey,
        PartitionKeyBuilder, ScopedPartitionV1,
    };

    use super::*;

    const KEY_VALIDATION_SOURCE: &str = r#"
contract CapabilityKeys version 1 {
  enum Mode { Alpha }
  entity TextRow { key (id: string<4>) }
  entity BytesRow { key (id: bytes<4>) }
  entity EnumRow { key (id: Mode) }
  entity BoolRow { key (id: bool) }
  aggregate TextRows { root TextRow partition_by id conflict_key (id) }
  aggregate BytesRows { root BytesRow partition_by id conflict_key (id) }
  aggregate EnumRows { root EnumRow partition_by id conflict_key (id) }
  aggregate BoolRows { root BoolRow partition_by id conflict_key (id) }
}
"#;

    fn checked_bundle(source: &str) -> ValidatedContractBundle {
        ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(source).expect("key-validation contract compiles"),
        )
        .expect("compiler bundle is catalog-valid")
    }

    fn explicit_scope(bundle: &ValidatedContractBundle, key: PartitionKey) -> PartitionScopeV1 {
        PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(bundle.lineage().clone(), key)])
            .expect("one explicit partition")
    }

    fn aggregate<'a>(
        bundle: &'a ValidatedContractBundle,
        name: &str,
    ) -> &'a riffdb_contract_ir::AggregateSchema {
        bundle
            .bundle()
            .schema()
            .aggregates()
            .iter()
            .find(|aggregate| aggregate.name() == name)
            .expect("named aggregate")
    }

    #[test]
    fn administrative_partition_shape_is_exact_for_statistics_and_outbox() {
        let all = PartitionConstraint::Filter(PartitionScopeV1::All);

        assert!(valid_administrative_partition(
            ServiceOperationV1::GetStatistics,
            None,
        ));
        assert!(!valid_administrative_partition(
            ServiceOperationV1::GetStatistics,
            Some(&all),
        ));
        assert!(valid_administrative_partition(
            ServiceOperationV1::ListPendingOutboxDeliveries,
            Some(&all),
        ));
        assert!(!valid_administrative_partition(
            ServiceOperationV1::ListPendingOutboxDeliveries,
            None,
        ));
        assert!(!valid_administrative_partition(
            ServiceOperationV1::GetHealth,
            None,
        ));
    }

    #[test]
    fn capability_partition_validation_requires_the_active_lineage_owner_and_complete_key() {
        let bundle = checked_bundle(include_str!("../../../contracts/examples/budget.riff"));
        let aggregate = aggregate(&bundle, "AnnualBudget");
        let valid = aggregate
            .keys()
            .partition_schema()
            .encode_partition(&[CanonicalValue::Uuid([0x31; 16])])
            .expect("valid budget partition");

        assert!(
            validate_capability_partition_scope(&bundle, &PartitionScopeV1::All).is_ok(),
            "All carries no key requiring a schema"
        );
        assert!(
            validate_capability_partition_scope(&bundle, &explicit_scope(&bundle, valid.clone()))
                .is_ok()
        );

        let foreign = PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(
            ContractLineage::new("Foreign").expect("lineage"),
            valid,
        )])
        .expect("one foreign partition");
        assert!(validate_capability_partition_scope(&bundle, &foreign).is_err());

        let unknown_owner = aggregate
            .id()
            .checked_next()
            .unwrap_or_else(AggregateTypeId::first);
        let mut unknown = PartitionKeyBuilder::new(unknown_owner);
        unknown.push_uuid(&[0x31; 16]).expect("bounded UUID");
        assert!(
            validate_capability_partition_scope(
                &bundle,
                &explicit_scope(&bundle, unknown.finish().expect("structural key")),
            )
            .is_err()
        );

        let missing_component = PartitionKeyBuilder::new(aggregate.id())
            .finish()
            .expect("envelope-only partition");
        assert!(
            validate_capability_partition_scope(
                &bundle,
                &explicit_scope(&bundle, missing_component),
            )
            .is_err()
        );

        let mut trailing = PartitionKeyBuilder::new(aggregate.id());
        trailing.push_uuid(&[0x31; 16]).expect("bounded UUID");
        trailing.push_bool(true).expect("bounded trailing byte");
        assert!(
            validate_capability_partition_scope(
                &bundle,
                &explicit_scope(&bundle, trailing.finish().expect("structural key")),
            )
            .is_err()
        );
    }

    #[test]
    fn capability_partition_validation_rejects_schema_invalid_component_payloads() {
        let bundle = checked_bundle(KEY_VALIDATION_SOURCE);

        let text = aggregate(&bundle, "TextRows");
        let mut invalid_utf8 = PartitionKeyBuilder::new(text.id());
        invalid_utf8
            .push_bytes(&[0xff])
            .expect("structurally bounded bytes");
        let mut oversized_text = PartitionKeyBuilder::new(text.id());
        oversized_text
            .push_str("abcde")
            .expect("structurally bounded text");

        let bytes = aggregate(&bundle, "BytesRows");
        let mut oversized_bytes = PartitionKeyBuilder::new(bytes.id());
        oversized_bytes
            .push_bytes(&[0; 5])
            .expect("structurally bounded bytes");

        let enumeration = aggregate(&bundle, "EnumRows");
        let mut unknown_variant = PartitionKeyBuilder::new(enumeration.id());
        unknown_variant
            .push_enum_variant(EnumVariantId::new(2).expect("second variant ID"))
            .expect("structurally bounded enum");

        let boolean = aggregate(&bundle, "BoolRows");
        let mut invalid_bool = vec![0x50, 0x01];
        invalid_bool.extend_from_slice(&boolean.id().to_be_bytes());
        invalid_bool.push(2);
        let invalid_bool = PartitionKey::from_bytes(invalid_bool)
            .expect("envelope validation does not inspect Boolean payloads");

        for key in [
            invalid_utf8.finish().expect("structural key"),
            oversized_text.finish().expect("structural key"),
            oversized_bytes.finish().expect("structural key"),
            unknown_variant.finish().expect("structural key"),
            invalid_bool,
        ] {
            assert!(
                validate_capability_partition_scope(&bundle, &explicit_scope(&bundle, key))
                    .is_err()
            );
        }
    }

    #[test]
    fn compatible_activation_cannot_change_an_already_checked_partition_schema() {
        let genesis_compiled =
            compile_contract_source(KEY_VALIDATION_SOURCE).expect("genesis compiles");
        let successor_source = KEY_VALIDATION_SOURCE.replacen("version 1", "version 2", 1);
        let successor_compiled = compile_contract_successor(&successor_source, &genesis_compiled)
            .expect("unchanged successor is compatible");
        let genesis = ValidatedContractBundle::from_compiler_bundle(genesis_compiled)
            .expect("genesis is catalog-valid");
        let successor = ValidatedContractBundle::from_compiler_bundle(successor_compiled)
            .expect("successor is catalog-valid");
        let genesis_aggregate = aggregate(&genesis, "TextRows");
        let successor_aggregate = aggregate(&successor, "TextRows");
        assert_eq!(
            genesis_aggregate.keys().partition_schema(),
            successor_aggregate.keys().partition_schema(),
            "accepted successor compatibility freezes aggregate partition schemas"
        );
        let key = genesis_aggregate
            .keys()
            .partition_schema()
            .encode_partition(&[CanonicalValue::string("key").expect("bounded text")])
            .expect("valid text partition");
        let scope = explicit_scope(&genesis, key);

        assert!(validate_capability_partition_scope(&genesis, &scope).is_ok());
        assert!(validate_capability_partition_scope(&successor, &scope).is_ok());
    }
}

#[cfg(test)]
#[path = "primary_fenced_capability_tests.rs"]
mod primary_fenced_capability_tests;
