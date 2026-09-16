//! Archive restore uses the existing two current-policy safe points and durable job custody.
use super::*;

pub(super) async fn start_restore(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: crate::RestoreArchivedBackupRequest,
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
    let receipt = match permit.submit(AuthorizedOfflineMaintenanceStart::RestoreArchivedBackup {
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
    if !observation_matches_archive(result.operation(), &expected) {
        return Err(service.maintenance_internal_failure(MaintenanceInternalDefect::LowerIntegrity));
    }
    ensure_response_budget(&result)?;
    Ok(result)
}

fn observation_matches_archive(
    observation: &OfflineMaintenanceOperationObservation,
    request: &crate::RestoreArchivedBackupRequest,
) -> bool {
    observation.operation_id() == request.operation_id()
        && observation.kind() == OfflineMaintenanceOperationKind::RestoreBackup
        && observation.backup_name() == request.backup_name()
        && observation.input_hash() == request.input_hash()
        && observation.archive_restore().is_some_and(|archive| {
            archive.archive_name() == request.archive_name() && archive.stop() == request.stop()
        })
}

pub(super) async fn start_retry(
    service: Arc<RestoreRetryOfflineMaintenanceServiceInner>,
    invocation: crate::RestoreArchivedBackupInvocation,
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
    let receipt = match permit.submit(AuthorizedRestoreRetryStart::new_archive(
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
    if !observation_matches_archive(result.operation(), &expected) {
        return Err(restore_retry_internal_failure(
            &service,
            MaintenanceInternalDefect::LowerIntegrity,
        ));
    }
    ensure_response_budget(&result)?;
    Ok(result)
}
