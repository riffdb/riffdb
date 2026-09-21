//! Contract validation, explanation, deployment, and catalog-read orchestration.

use std::sync::Arc;

use riffdb_catalog::{
    ActiveCatalogSnapshot, CatalogError, CatalogErrorKind, CatalogPreparationResult,
    ValidatedContractBundle,
};
use riffdb_commit::{
    CatalogDeploymentOutcome, CatalogDeploymentPreparation, ControlPlaneExecutionAdmissionError,
    ControlPlaneExecutionError, ControlPlaneExecutionErrorKind, ControlPlaneTerminalAudit,
};
use riffdb_contract_compiler::{
    compile_contract_source, compile_contract_successor, validate_contract_source,
};
use riffdb_contract_ir::{CommandExplain, ContractBundle, SchemaArtifactKey, StableIdNamespaceTag};
use riffdb_errors::{
    ApplicationErrorCode, PublicError, ValidationCode, ValidationIssue, ValidationIssues,
    ValidationPath,
};
use riffdb_policy::OperationRequest;
use riffdb_types::{
    CommandId, ContractLineage, ContractVersion, ServiceAuditLinkV1, ServiceAuditPhaseV1,
    ServiceAuditTargetsV1, ServiceOperationV1, hash_source,
};

use crate::orchestration::{AuditScope, BegunInvocation};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    ContractApplication, ContractCompatibilityClass, ContractDescriptor, ContractSelection,
    ContractValidationResult, DeployContractRequest, DeployContractResult, ExplainCommandRequest,
    ExplainCommandResult, ExplainedCommand, GetActiveContractRequest, GetActiveContractResult,
    GetContractVersionRequest, GetContractVersionResult, InternalDefect, PendingTerminalResponse,
    PortAdmissionError, PortDriverStopped, RequestContext, RiffDbService, RiffDbServiceInner,
    ServiceAuditTargetMap, ServiceFailure, ServiceFuture, ServiceResult, ValidateContractRequest,
    ensure_response_budget,
};

impl ContractApplication for RiffDbService {
    fn validate_contract(
        &self,
        context: RequestContext,
        request: ValidateContractRequest,
    ) -> ServiceFuture<'_, ContractValidationResult> {
        let inner = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::ValidateContract, ingress, async move {
            validate_contract(&inner, context, request).await
        })
    }

    fn explain_command(
        &self,
        context: RequestContext,
        request: ExplainCommandRequest,
    ) -> ServiceFuture<'_, ExplainCommandResult> {
        let inner = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::ExplainCommand, ingress, async move {
            explain_command(&inner, context, request).await
        })
    }

    fn deploy_contract(
        &self,
        context: RequestContext,
        request: DeployContractRequest,
    ) -> ServiceFuture<'_, DeployContractResult> {
        let inner = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::DeployContract, ingress, async move {
            deploy_contract(&inner, context, request).await
        })
    }

    fn get_active_contract(
        &self,
        context: RequestContext,
        request: GetActiveContractRequest,
    ) -> ServiceFuture<'_, GetActiveContractResult> {
        let inner = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::GetActiveContract, ingress, async move {
            get_active_contract(&inner, context, request).await
        })
    }

    fn get_contract_version(
        &self,
        context: RequestContext,
        request: GetContractVersionRequest,
    ) -> ServiceFuture<'_, GetContractVersionResult> {
        let inner = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::GetContractVersion,
            ingress,
            async move { get_contract_version(&inner, context, request).await },
        )
    }
}

async fn validate_contract(
    service: &RiffDbServiceInner,
    context: RequestContext,
    request: ValidateContractRequest,
) -> ServiceResult<ContractValidationResult> {
    let begun = service
        .begin_invocation(
            &context,
            OperationRequest::validate_contract(),
            ServiceAuditTargetMap::validate_contract(),
            AuditScope::StandardRead,
        )
        .await?;

    let result = if request.previews_active_successor() {
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
            Ok(Ok(active)) => active,
            Ok(Err(error)) => {
                let failure = map_unprotected_catalog_error(
                    service,
                    ServiceOperationV1::ValidateContract,
                    error,
                );
                return Err(finish_terminal_failure(
                    service,
                    &context,
                    &begun,
                    ServiceOperationV1::ValidateContract,
                    failure,
                )
                .await);
            }
            Err(error) => {
                return Err(finish_terminal_failure(
                    service,
                    &context,
                    &begun,
                    ServiceOperationV1::ValidateContract,
                    controlled_wait_failure(error),
                )
                .await);
            }
        };
        match compile_deployment_candidate(
            request.source().as_str(),
            active.as_ref().map(ActiveCatalogSnapshot::bundle),
        ) {
            Ok(candidate) => ContractValidationResult::Candidate(Box::new(
                crate::CompiledContractCandidate::from_bundle(&candidate),
            )),
            Err(error) => ContractValidationResult::Invalid(error),
        }
    } else {
        match validate_contract_source(request.source().as_str()) {
            Ok(()) => ContractValidationResult::Valid,
            Err(error) => ContractValidationResult::Invalid(error),
        }
    };

    // Compilation is the protected long-running compute. Recheck current policy
    // after it completes and before any diagnostics leave the service.
    let _fresh = begun.reauthorize(service, &context).await?;
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_terminal_failure(
            service,
            &context,
            &begun,
            ServiceOperationV1::ValidateContract,
            failure,
        )
        .await);
    }
    finish_read_success(
        service,
        &context,
        &begun,
        ServiceOperationV1::ValidateContract,
    )
    .await?;
    Ok(result)
}

struct ResolvedExplainTarget {
    selection: ContractSelection,
    lineage: ContractLineage,
    version: ContractVersion,
    command_id: CommandId,
    active_bundle_hash: riffdb_types::ContractBundleHash,
}

async fn explain_command(
    service: &RiffDbServiceInner,
    context: RequestContext,
    request: ExplainCommandRequest,
) -> ServiceResult<ExplainCommandResult> {
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
        Ok(Ok(None)) => return Ok(ExplainCommandResult::NotFound),
        Ok(Err(error)) => {
            return Err(map_unprotected_catalog_error(
                service,
                ServiceOperationV1::ExplainCommand,
                error,
            ));
        }
        Err(error) => return Err(controlled_wait_failure(error)),
    };
    let Some(target) = resolve_explain_target(&active, &request) else {
        return Ok(ExplainCommandResult::NotFound);
    };
    let targets = ServiceAuditTargetMap::explain_command(
        target.lineage.clone(),
        target.version,
        target.command_id,
    )
    .map_err(|_| {
        service.internal_failure(
            ServiceOperationV1::ExplainCommand,
            InternalDefect::ProofMismatch,
        )
    })?;
    let begun = service
        .begin_invocation(
            &context,
            OperationRequest::explain_command(
                target.lineage.clone(),
                target.version,
                target.command_id,
            ),
            targets,
            AuditScope::StandardRead,
        )
        .await?;

    let bundle = match target.selection {
        ContractSelection::Active => {
            let permit = match wait_with_control(
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
                    return Err(finish_port_admission_failure(
                        service,
                        &context,
                        &begun,
                        ServiceOperationV1::ExplainCommand,
                        error,
                    )
                    .await);
                }
                Err(error) => {
                    return Err(finish_controlled_wait_failure(
                        service,
                        &context,
                        &begun,
                        ServiceOperationV1::ExplainCommand,
                        error,
                    )
                    .await);
                }
            };
            let _fresh = begun.reauthorize(service, &context).await?;
            let receipt = match permit.submit(()) {
                Ok(receipt) => receipt,
                Err(error) => {
                    return Err(finish_port_admission_failure(
                        service,
                        &context,
                        &begun,
                        ServiceOperationV1::ExplainCommand,
                        error,
                    )
                    .await);
                }
            };
            let observed = match wait_with_control(
                context.control(),
                service.providers.deadline_scheduler.as_ref(),
                receipt,
            )
            .await
            {
                Ok(Ok(Ok(Some(observed)))) => observed,
                Ok(Ok(Ok(None))) => {
                    let _fresh = begun.reauthorize(service, &context).await?;
                    let result = ExplainCommandResult::NotFound;
                    if let Err(failure) = ensure_response_budget(&result) {
                        return Err(finish_terminal_failure(
                            service,
                            &context,
                            &begun,
                            ServiceOperationV1::ExplainCommand,
                            failure,
                        )
                        .await);
                    }
                    finish_read_success(
                        service,
                        &context,
                        &begun,
                        ServiceOperationV1::ExplainCommand,
                    )
                    .await?;
                    return Ok(result);
                }
                Ok(Ok(Err(error))) => {
                    return Err(finish_catalog_failure(
                        service,
                        &context,
                        &begun,
                        ServiceOperationV1::ExplainCommand,
                        error,
                    )
                    .await);
                }
                Ok(Err(PortDriverStopped)) => {
                    return Err(finish_internal_failure(
                        service,
                        &context,
                        &begun,
                        ServiceOperationV1::ExplainCommand,
                    )
                    .await);
                }
                Err(error) => {
                    return Err(finish_controlled_wait_failure(
                        service,
                        &context,
                        &begun,
                        ServiceOperationV1::ExplainCommand,
                        error,
                    )
                    .await);
                }
            };
            let observed_bundle = observed.bundle();
            if observed_bundle.lineage() != &target.lineage
                || observed_bundle.contract_version() != target.version
                || observed_bundle.bundle_hash() != target.active_bundle_hash
            {
                return Err(finish_contract_change(
                    service,
                    &context,
                    &begun,
                    observed_bundle.contract_version(),
                )
                .await);
            }
            observed_bundle.clone()
        }
        ContractSelection::Exact { lineage, version } => {
            let permit = match wait_with_control(
                context.control(),
                service.providers.deadline_scheduler.as_ref(),
                service
                    .providers
                    .catalog
                    .reserve_contract_version(context.control()),
            )
            .await
            {
                Ok(Ok(permit)) => permit,
                Ok(Err(error)) => {
                    return Err(finish_port_admission_failure(
                        service,
                        &context,
                        &begun,
                        ServiceOperationV1::ExplainCommand,
                        error,
                    )
                    .await);
                }
                Err(error) => {
                    return Err(finish_controlled_wait_failure(
                        service,
                        &context,
                        &begun,
                        ServiceOperationV1::ExplainCommand,
                        error,
                    )
                    .await);
                }
            };
            let _fresh = begun.reauthorize(service, &context).await?;
            let receipt = match permit.submit((lineage, version)) {
                Ok(receipt) => receipt,
                Err(error) => {
                    return Err(finish_port_admission_failure(
                        service,
                        &context,
                        &begun,
                        ServiceOperationV1::ExplainCommand,
                        error,
                    )
                    .await);
                }
            };
            match wait_with_control(
                context.control(),
                service.providers.deadline_scheduler.as_ref(),
                receipt,
            )
            .await
            {
                Ok(Ok(Ok(Some(bundle)))) => bundle,
                Ok(Ok(Ok(None))) => {
                    let _fresh = begun.reauthorize(service, &context).await?;
                    let result = ExplainCommandResult::NotFound;
                    if let Err(failure) = ensure_response_budget(&result) {
                        return Err(finish_terminal_failure(
                            service,
                            &context,
                            &begun,
                            ServiceOperationV1::ExplainCommand,
                            failure,
                        )
                        .await);
                    }
                    finish_read_success(
                        service,
                        &context,
                        &begun,
                        ServiceOperationV1::ExplainCommand,
                    )
                    .await?;
                    return Ok(result);
                }
                Ok(Ok(Err(error))) => {
                    return Err(finish_catalog_failure(
                        service,
                        &context,
                        &begun,
                        ServiceOperationV1::ExplainCommand,
                        error,
                    )
                    .await);
                }
                Ok(Err(PortDriverStopped)) => {
                    return Err(finish_internal_failure(
                        service,
                        &context,
                        &begun,
                        ServiceOperationV1::ExplainCommand,
                    )
                    .await);
                }
                Err(error) => {
                    return Err(finish_controlled_wait_failure(
                        service,
                        &context,
                        &begun,
                        ServiceOperationV1::ExplainCommand,
                        error,
                    )
                    .await);
                }
            }
        }
    };

    if bundle.lineage() != &target.lineage || bundle.contract_version() != target.version {
        return Err(finish_internal_failure(
            service,
            &context,
            &begun,
            ServiceOperationV1::ExplainCommand,
        )
        .await);
    }
    let result =
        shape_explanation(&bundle, target.command_id, request.command().as_str()).map_err(|()| {
            service.internal_failure(
                ServiceOperationV1::ExplainCommand,
                InternalDefect::LowerIntegrity,
            )
        });
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            let _fresh = begun.reauthorize(service, &context).await?;
            if finish_phase(
                service,
                &context,
                &begun,
                ServiceOperationV1::ExplainCommand,
                ServiceAuditPhaseV1::Failed,
                ServiceAuditLinkV1::None,
            )
            .await
            .is_err()
            {
                return Err(PublicError::storage_unavailable().into());
            }
            return Err(error);
        }
    };
    let _fresh = begun.reauthorize(service, &context).await?;
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_terminal_failure(
            service,
            &context,
            &begun,
            ServiceOperationV1::ExplainCommand,
            failure,
        )
        .await);
    }
    finish_read_success(
        service,
        &context,
        &begun,
        ServiceOperationV1::ExplainCommand,
    )
    .await?;
    Ok(result)
}

async fn deploy_contract(
    service: &RiffDbServiceInner,
    context: RequestContext,
    request: DeployContractRequest,
) -> ServiceResult<DeployContractResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::DeployContract;
    let provisional_targets = ServiceAuditTargetsV1::empty();
    service.classify_intrinsic_prestart(&context, OPERATION, provisional_targets.clone())?;
    if context.control().is_cancelled() {
        return Err(finish_deploy_prestart_failure(
            service,
            &context,
            provisional_targets,
            ServiceAuditPhaseV1::Cancelled,
            ServiceFailure::Cancelled,
        )
        .await);
    }
    if context.control().is_deadline_exceeded() {
        return Err(finish_deploy_prestart_failure(
            service,
            &context,
            provisional_targets,
            ServiceAuditPhaseV1::Cancelled,
            ServiceFailure::DeadlineExceeded,
        )
        .await);
    }

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
        Ok(Ok(active)) => active,
        Ok(Err(error)) => {
            let failure = map_unprotected_catalog_error(service, OPERATION, error);
            return Err(finish_deploy_prestart_failure(
                service,
                &context,
                provisional_targets,
                ServiceAuditPhaseV1::Failed,
                failure,
            )
            .await);
        }
        Err(error) => {
            let failure = controlled_wait_failure(error);
            return Err(finish_deploy_prestart_failure(
                service,
                &context,
                provisional_targets,
                ServiceAuditPhaseV1::Cancelled,
                failure,
            )
            .await);
        }
    };
    let candidate = match compile_deployment_candidate(
        request.source().as_str(),
        active.as_ref().map(ActiveCatalogSnapshot::bundle),
    ) {
        Ok(candidate) => candidate,
        Err(error) => {
            return finish_deploy_prestart_result(
                service,
                &context,
                provisional_targets,
                DeployContractResult::InvalidSource(error),
            )
            .await;
        }
    };
    // ADR-0251 decision 3. A contract declaring a columnar source the running
    // process could never build is refused here, at the point the operator
    // acted, rather than accepted and left to fail at whoever queries first.
    // The check is read-only and precedes the commit, so a refusal leaves no
    // control behind; pruning is an offline operation and this process holds
    // the store's exclusive lock, so the answer cannot go stale before the
    // deploy completes.
    if !candidate.schema().vector_production_specs().is_empty()
        && let Some(admission) = service.providers.columnar_admission.as_ref()
    {
        match admission.fresh_source_is_replayable() {
            Ok(true) => {}
            Ok(false) => {
                let failure = deploy_application_failure(ApplicationErrorCode::HistoryPruned);
                return Err(finish_deploy_prestart_failure(
                    service,
                    &context,
                    provisional_targets,
                    ServiceAuditPhaseV1::Failed,
                    failure,
                )
                .await);
            }
            Err(_) => {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_deploy_prestart_failure(
                    service,
                    &context,
                    provisional_targets,
                    ServiceAuditPhaseV1::Failed,
                    failure,
                )
                .await);
            }
        }
    }
    let descriptor = contract_descriptor(&candidate);
    let targets = match ServiceAuditTargetMap::deploy_contract(
        descriptor.lineage().clone(),
        descriptor.version(),
    ) {
        Ok(targets) => targets,
        Err(_) => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_deploy_prestart_failure(
                service,
                &context,
                provisional_targets,
                ServiceAuditPhaseV1::Failed,
                failure,
            )
            .await);
        }
    };
    service.refine_intrinsic_prestart(&context, OPERATION, targets.clone())?;
    let actual_active_descriptor = active
        .as_ref()
        .map(|active| contract_descriptor(active.bundle().bundle()));
    let exact_candidate_already_active =
        request
            .expected_candidate_bundle_hash()
            .is_some_and(|expected| {
                descriptor.bundle_hash() == expected
                    && actual_active_descriptor
                        .as_ref()
                        .is_some_and(|actual| actual.bundle_hash() == expected)
            });
    let deployment_expected_active_version = if exact_candidate_already_active {
        actual_active_descriptor
            .as_ref()
            .map(ContractDescriptor::version)
    } else {
        request.expected_active_version()
    };
    let operation = OperationRequest::deploy_contract(
        descriptor.lineage().clone(),
        descriptor.version(),
        descriptor.bundle_hash(),
        deployment_expected_active_version,
    );
    let begun = service
        .begin_invocation(&context, operation, targets, AuditScope::Intrinsic)
        .await?;

    if let Some(result) =
        exact_application_identity_result(&request, actual_active_descriptor.as_ref(), &descriptor)
    {
        let phase = if matches!(result, DeployContractResult::AlreadyActive(_)) {
            ServiceAuditPhaseV1::Succeeded
        } else {
            ServiceAuditPhaseV1::Failed
        };
        let _fresh = begun.reauthorize(service, &context).await?;
        if let Err(failure) = ensure_response_budget(&result) {
            return Err(
                finish_terminal_failure(service, &context, &begun, OPERATION, failure).await,
            );
        }
        finish_phase(
            service,
            &context,
            &begun,
            OPERATION,
            phase,
            ServiceAuditLinkV1::None,
        )
        .await?;
        return Ok(result);
    }

    let actual_active_version = active
        .as_ref()
        .map(|active| active.bundle().contract_version());
    if descriptor.compatibility().overall() == ContractCompatibilityClass::Incompatible
        && actual_active_version == request.expected_active_version()
    {
        let _fresh = begun.reauthorize(service, &context).await?;
        let result = DeployContractResult::IncompatibleCandidate(descriptor);
        if let Err(failure) = ensure_response_budget(&result) {
            return Err(
                finish_terminal_failure(service, &context, &begun, OPERATION, failure).await,
            );
        }
        finish_phase(
            service,
            &context,
            &begun,
            OPERATION,
            ServiceAuditPhaseV1::Failed,
            ServiceAuditLinkV1::None,
        )
        .await?;
        return Ok(result);
    }

    if descriptor.compatibility().overall() == ContractCompatibilityClass::RequiresMigration
        && actual_active_version == request.expected_active_version()
    {
        let _fresh = begun.reauthorize(service, &context).await?;
        let result = DeployContractResult::MigrationRequired(descriptor);
        if let Err(failure) = ensure_response_budget(&result) {
            return Err(
                finish_terminal_failure(service, &context, &begun, OPERATION, failure).await,
            );
        }
        finish_phase(
            service,
            &context,
            &begun,
            OPERATION,
            ServiceAuditPhaseV1::Failed,
            ServiceAuditLinkV1::None,
        )
        .await?;
        return Ok(result);
    }

    let preparation = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service.providers.catalog.prepare_deployment(
            context.control(),
            candidate,
            deployment_expected_active_version,
        ),
    )
    .await
    {
        Ok(preparation) => preparation,
        Err(error) => {
            return Err(finish_controlled_wait_failure(
                service, &context, &begun, OPERATION, error,
            )
            .await);
        }
    };

    let prepared = match preparation {
        Ok(CatalogPreparationResult::Prepared(prepared)) => prepared,
        Ok(CatalogPreparationResult::ExpectedActiveVersionMismatch { actual }) => {
            let _fresh = begun.reauthorize(service, &context).await?;
            let result = DeployContractResult::ExpectedActiveVersionMismatch { actual };
            if let Err(failure) = ensure_response_budget(&result) {
                return Err(
                    finish_terminal_failure(service, &context, &begun, OPERATION, failure).await,
                );
            }
            finish_phase(
                service,
                &context,
                &begun,
                ServiceOperationV1::DeployContract,
                ServiceAuditPhaseV1::Failed,
                ServiceAuditLinkV1::None,
            )
            .await?;
            return Ok(result);
        }
        Err(error) => {
            let _fresh = begun.reauthorize(service, &context).await?;
            return finish_deployment_preparation_error(service, &context, &begun, error).await;
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
            return Err(
                finish_control_plane_admission_failure(service, &context, &begun, error).await,
            );
        }
        Err(error) => {
            return Err(finish_controlled_wait_failure(
                service,
                &context,
                &begun,
                ServiceOperationV1::DeployContract,
                error,
            )
            .await);
        }
    };
    let authorization = begun.reauthorize(service, &context).await?;
    let authorization = match (*authorization).into_catalog_deployment(
        descriptor.lineage(),
        descriptor.version(),
        descriptor.bundle_hash(),
        deployment_expected_active_version,
    ) {
        Ok(authorization) => authorization,
        Err(_) => {
            return Err(finish_internal_failure(
                service,
                &context,
                &begun,
                ServiceOperationV1::DeployContract,
            )
            .await);
        }
    };
    let preparation =
        match CatalogDeploymentPreparation::new(context.request_id(), prepared, authorization) {
            Ok(preparation) => preparation,
            Err(_) => {
                return Err(finish_internal_failure(
                    service,
                    &context,
                    &begun,
                    ServiceOperationV1::DeployContract,
                )
                .await);
            }
        };
    let receipt = match permit.submit_catalog_deployment(preparation) {
        Ok(receipt) => receipt,
        Err(error) => {
            return Err(
                finish_control_plane_admission_failure(service, &context, &begun, error).await,
            );
        }
    };
    let result = match receipt.completion().await {
        Ok(result) => result,
        Err(error) => {
            return Err(
                finish_control_plane_execution_failure(service, &context, &begun, error).await,
            );
        }
    };

    let terminal = result.terminal_audit();
    let shaped = match shape_deployment_outcome(result.outcome(), &descriptor) {
        Ok(shaped) => shaped,
        Err(()) => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(
                finish_terminal_failure(service, &context, &begun, OPERATION, failure).await,
            );
        }
    };
    // ADR-0251 decision 1. The active catalog is now the contract just
    // deployed, so its declared sources are admitted from it, by the same
    // resolution startup uses. A source registered here is cold: the first
    // projected query demands it and WP-777's activation does the rest.
    //
    // The replayability refusal happened before the commit, so this cannot
    // report HistoryPruned for a deploy that got this far unless the store was
    // pruned underneath a running process, which an offline-only prune cannot
    // do. It is still handled rather than assumed away.
    if matches!(shaped, DeployContractResult::Activated(_))
        && let Some(admission) = service.providers.columnar_admission.as_ref()
        && !matches!(
            admission.admit_active_catalog_sources(),
            Ok(crate::ColumnarAdmissionOutcome::Admitted)
        )
    {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_terminal_failure(service, &context, &begun, OPERATION, failure).await);
    }
    let pending = PendingTerminalResponse::new(shaped, terminal, ensure_response_budget);
    finish_control_plane_terminal(service, &context, &begun, pending.terminal()).await?;
    pending.into_response()
}

async fn get_active_contract(
    service: &RiffDbServiceInner,
    context: RequestContext,
    _request: GetActiveContractRequest,
) -> ServiceResult<GetActiveContractResult> {
    let begun = service
        .begin_invocation(
            &context,
            OperationRequest::get_active_contract(),
            ServiceAuditTargetMap::get_active_contract(),
            AuditScope::StandardRead,
        )
        .await?;
    let permit = match wait_with_control(
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
            return Err(finish_port_admission_failure(
                service,
                &context,
                &begun,
                ServiceOperationV1::GetActiveContract,
                error,
            )
            .await);
        }
        Err(error) => {
            return Err(finish_controlled_wait_failure(
                service,
                &context,
                &begun,
                ServiceOperationV1::GetActiveContract,
                error,
            )
            .await);
        }
    };
    let _fresh = begun.reauthorize(service, &context).await?;
    let receipt = match permit.submit(()) {
        Ok(receipt) => receipt,
        Err(error) => {
            return Err(finish_port_admission_failure(
                service,
                &context,
                &begun,
                ServiceOperationV1::GetActiveContract,
                error,
            )
            .await);
        }
    };
    let result = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(Some(active)))) => {
            GetActiveContractResult::Present(contract_descriptor(active.bundle().bundle()))
        }
        Ok(Ok(Ok(None))) => GetActiveContractResult::Absent,
        Ok(Ok(Err(error))) => {
            return Err(finish_catalog_failure(
                service,
                &context,
                &begun,
                ServiceOperationV1::GetActiveContract,
                error,
            )
            .await);
        }
        Ok(Err(PortDriverStopped)) => {
            return Err(finish_internal_failure(
                service,
                &context,
                &begun,
                ServiceOperationV1::GetActiveContract,
            )
            .await);
        }
        Err(error) => {
            return Err(finish_controlled_wait_failure(
                service,
                &context,
                &begun,
                ServiceOperationV1::GetActiveContract,
                error,
            )
            .await);
        }
    };
    let _fresh = begun.reauthorize(service, &context).await?;
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_terminal_failure(
            service,
            &context,
            &begun,
            ServiceOperationV1::GetActiveContract,
            failure,
        )
        .await);
    }
    finish_read_success(
        service,
        &context,
        &begun,
        ServiceOperationV1::GetActiveContract,
    )
    .await?;
    Ok(result)
}

async fn get_contract_version(
    service: &RiffDbServiceInner,
    context: RequestContext,
    request: GetContractVersionRequest,
) -> ServiceResult<GetContractVersionResult> {
    let targets =
        ServiceAuditTargetMap::get_contract_version(request.lineage().clone(), request.version())
            .map_err(|_| {
            service.internal_failure(
                ServiceOperationV1::GetContractVersion,
                InternalDefect::ProofMismatch,
            )
        })?;
    let begun = service
        .begin_invocation(
            &context,
            OperationRequest::get_contract_version(request.lineage().clone(), request.version()),
            targets,
            AuditScope::StandardRead,
        )
        .await?;
    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .catalog
            .reserve_contract_version(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => {
            return Err(finish_port_admission_failure(
                service,
                &context,
                &begun,
                ServiceOperationV1::GetContractVersion,
                error,
            )
            .await);
        }
        Err(error) => {
            return Err(finish_controlled_wait_failure(
                service,
                &context,
                &begun,
                ServiceOperationV1::GetContractVersion,
                error,
            )
            .await);
        }
    };
    let _fresh = begun.reauthorize(service, &context).await?;
    let receipt = match permit.submit((request.lineage().clone(), request.version())) {
        Ok(receipt) => receipt,
        Err(error) => {
            return Err(finish_port_admission_failure(
                service,
                &context,
                &begun,
                ServiceOperationV1::GetContractVersion,
                error,
            )
            .await);
        }
    };
    let result = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(Some(bundle)))) => {
            if bundle.lineage() != request.lineage()
                || bundle.contract_version() != request.version()
            {
                return Err(finish_internal_failure(
                    service,
                    &context,
                    &begun,
                    ServiceOperationV1::GetContractVersion,
                )
                .await);
            }
            GetContractVersionResult::Found(contract_descriptor(bundle.bundle()))
        }
        Ok(Ok(Ok(None))) => GetContractVersionResult::NotFound,
        Ok(Ok(Err(error))) => {
            return Err(finish_catalog_failure(
                service,
                &context,
                &begun,
                ServiceOperationV1::GetContractVersion,
                error,
            )
            .await);
        }
        Ok(Err(PortDriverStopped)) => {
            return Err(finish_internal_failure(
                service,
                &context,
                &begun,
                ServiceOperationV1::GetContractVersion,
            )
            .await);
        }
        Err(error) => {
            return Err(finish_controlled_wait_failure(
                service,
                &context,
                &begun,
                ServiceOperationV1::GetContractVersion,
                error,
            )
            .await);
        }
    };
    let _fresh = begun.reauthorize(service, &context).await?;
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_terminal_failure(
            service,
            &context,
            &begun,
            ServiceOperationV1::GetContractVersion,
            failure,
        )
        .await);
    }
    finish_read_success(
        service,
        &context,
        &begun,
        ServiceOperationV1::GetContractVersion,
    )
    .await?;
    Ok(result)
}

fn resolve_explain_target(
    active: &ActiveCatalogSnapshot,
    request: &ExplainCommandRequest,
) -> Option<ResolvedExplainTarget> {
    let active_bundle = active.bundle().bundle();
    let (lineage, version) = match request.contract() {
        ContractSelection::Active => (
            active_bundle.lineage().clone(),
            active_bundle.contract_version(),
        ),
        ContractSelection::Exact { lineage, version } => {
            if lineage != active_bundle.lineage() {
                return None;
            }
            (lineage.clone(), *version)
        }
    };
    let command_id = command_id_from_lineage(active_bundle, request.command().as_str())?;
    Some(ResolvedExplainTarget {
        selection: request.contract().clone(),
        lineage,
        version,
        command_id,
        active_bundle_hash: active_bundle.bundle_hash(),
    })
}

fn command_id_from_lineage(bundle: &ContractBundle, source_name: &str) -> Option<CommandId> {
    bundle
        .ledger()
        .allocations()
        .iter()
        .find(|allocation| {
            allocation.namespace().tag() == StableIdNamespaceTag::Command
                && allocation.namespace().owner_kind() == 0
                && allocation.namespace().owner_ids().is_empty()
        })?
        .entries()
        .iter()
        .find(|entry| entry.name() == source_name)
        // Tombstones are intentionally retained: an exact older version may
        // still contain the command. Active selection rejects below when the
        // current bundle has no matching plan.
        .and_then(|entry| CommandId::new(entry.id()))
}

fn shape_explanation(
    bundle: &ValidatedContractBundle,
    command_id: CommandId,
    source_name: &str,
) -> Result<ExplainCommandResult, ()> {
    let bundle = bundle.bundle();
    let Some(plan) = bundle
        .command(command_id)
        .filter(|plan| plan.name() == source_name)
    else {
        return Ok(ExplainCommandResult::NotFound);
    };
    let input_schema = bundle
        .schema_artifacts()
        .iter()
        .find(|artifact| artifact.key() == SchemaArtifactKey::CommandInput(command_id))
        .cloned()
        .ok_or(())?;
    let outcome_schema = bundle
        .schema_artifacts()
        .iter()
        .find(|artifact| artifact.key() == SchemaArtifactKey::CommandOutcomeUnion(command_id))
        .cloned()
        .ok_or(())?;
    let tool_name = bundle
        .mcp_command_names()
        .get(command_id)
        .map(|entry| entry.tool_name().clone())
        .ok_or(())?;
    let explained = ExplainedCommand::new(
        contract_descriptor(bundle),
        command_id,
        tool_name,
        plan.plan_hash(),
        CommandExplain::from_plan(plan),
        input_schema,
        outcome_schema,
    )
    .map_err(|_| ())?;
    Ok(ExplainCommandResult::Found(Box::new(explained)))
}

fn compile_deployment_candidate(
    source: &str,
    active: Option<&ValidatedContractBundle>,
) -> Result<ContractBundle, riffdb_contract_compiler::CompilationError> {
    match active {
        Some(active) if active.bundle().source_hash() == hash_source(source.as_bytes()) => {
            validate_contract_source(source)?;
            Ok(active.bundle().clone())
        }
        Some(active) => compile_contract_successor(source, active.bundle()),
        None => compile_contract_source(source),
    }
}

fn contract_descriptor(bundle: &ContractBundle) -> ContractDescriptor {
    ContractDescriptor::from_bundle(bundle)
}

fn exact_application_identity_result(
    request: &DeployContractRequest,
    actual_active: Option<&ContractDescriptor>,
    compiled_candidate: &ContractDescriptor,
) -> Option<DeployContractResult> {
    let expected_candidate = request.expected_candidate_bundle_hash()?;
    if actual_active.is_some_and(|actual| {
        actual.bundle_hash() == expected_candidate
            && compiled_candidate.bundle_hash() == expected_candidate
    }) {
        return None;
    }
    let active_matches = actual_active.map(|actual| (actual.version(), actual.bundle_hash()))
        == request
            .expected_active_version()
            .zip(request.expected_active_bundle_hash());
    if active_matches && compiled_candidate.bundle_hash() == expected_candidate {
        None
    } else {
        Some(DeployContractResult::ExpectedApplicationIdentityMismatch {
            actual_active: actual_active.cloned(),
            compiled_candidate: compiled_candidate.clone(),
        })
    }
}

fn shape_deployment_outcome(
    outcome: &CatalogDeploymentOutcome,
    candidate: &ContractDescriptor,
) -> Result<DeployContractResult, ()> {
    match outcome {
        CatalogDeploymentOutcome::Activated(active) => {
            require_activated_candidate(active, candidate)?;
            Ok(DeployContractResult::Activated(candidate.clone()))
        }
        CatalogDeploymentOutcome::AlreadyActive(active) => {
            require_activated_candidate(active, candidate)?;
            Ok(DeployContractResult::AlreadyActive(candidate.clone()))
        }
        CatalogDeploymentOutcome::ExpectedActiveVersionMismatch { actual } => {
            Ok(DeployContractResult::ExpectedActiveVersionMismatch { actual: *actual })
        }
        CatalogDeploymentOutcome::BundleConflict => Ok(DeployContractResult::BundleConflict),
    }
}

fn require_activated_candidate(
    active: &riffdb_commit::ActivatedCatalog,
    candidate: &ContractDescriptor,
) -> Result<(), ()> {
    if active.lineage() == candidate.lineage()
        && active.version() == candidate.version()
        && active.bundle_hash() == candidate.bundle_hash()
    {
        Ok(())
    } else {
        Err(())
    }
}

/// A typed refusal carrying the application code that names the condition.
fn deploy_application_failure(application_code: ApplicationErrorCode) -> ServiceFailure {
    let error = PublicError::validation(ValidationIssues::one(ValidationIssue::new(
        ValidationCode::InvalidValue,
        ValidationPath::root(),
    )));
    error
        .with_application_code_hint(application_code)
        .map_or_else(
            |_| {
                ServiceFailure::from(PublicError::validation(ValidationIssues::one(
                    ValidationIssue::new(ValidationCode::InvalidValue, ValidationPath::root()),
                )))
            },
            ServiceFailure::from,
        )
}

async fn finish_deploy_prestart_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    targets: ServiceAuditTargetsV1,
    phase: ServiceAuditPhaseV1,
    failure: ServiceFailure,
) -> ServiceFailure {
    if service
        .append_prestart_terminal_if_intrinsic(
            context,
            ServiceOperationV1::DeployContract,
            targets,
            AuditScope::Intrinsic,
            phase,
        )
        .await
        .is_err()
    {
        PublicError::storage_unavailable().into()
    } else {
        failure
    }
}

async fn finish_deploy_prestart_result(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    targets: ServiceAuditTargetsV1,
    result: DeployContractResult,
) -> ServiceResult<DeployContractResult> {
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_deploy_prestart_failure(
            service,
            context,
            targets,
            ServiceAuditPhaseV1::Failed,
            failure,
        )
        .await);
    }
    if service
        .append_prestart_terminal_if_intrinsic(
            context,
            ServiceOperationV1::DeployContract,
            targets,
            AuditScope::Intrinsic,
            ServiceAuditPhaseV1::Failed,
        )
        .await
        .is_err()
    {
        return Err(PublicError::storage_unavailable().into());
    }
    Ok(result)
}

const fn controlled_wait_failure(error: ControlledWaitError) -> ServiceFailure {
    match error {
        ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
        ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
    }
}

async fn finish_deployment_preparation_error(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    error: CatalogError,
) -> ServiceResult<DeployContractResult> {
    finish_phase(
        service,
        context,
        begun,
        ServiceOperationV1::DeployContract,
        ServiceAuditPhaseV1::Failed,
        ServiceAuditLinkV1::None,
    )
    .await?;
    match classify_deployment_preparation_error(error.kind()) {
        DeploymentPreparationDisposition::BundleConflict => {
            Ok(DeployContractResult::BundleConflict)
        }
        DeploymentPreparationDisposition::Validation(code) => Err(root_validation(code).into()),
        DeploymentPreparationDisposition::Unavailable => {
            Err(PublicError::storage_unavailable().into())
        }
        DeploymentPreparationDisposition::Integrity => {
            service
                .providers
                .health
                .fail_authoritative_readiness(crate::AuthoritativeReadinessFailure::Integrity);
            Err(service.internal_failure(
                ServiceOperationV1::DeployContract,
                InternalDefect::LowerIntegrity,
            ))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeploymentPreparationDisposition {
    BundleConflict,
    Validation(ValidationCode),
    Unavailable,
    Integrity,
}

const fn classify_deployment_preparation_error(
    kind: CatalogErrorKind,
) -> DeploymentPreparationDisposition {
    match kind {
        CatalogErrorKind::BundleIdentityConflict => {
            DeploymentPreparationDisposition::BundleConflict
        }
        CatalogErrorKind::LineageBundleCountLimit => {
            DeploymentPreparationDisposition::Validation(ValidationCode::TooManyItems)
        }
        CatalogErrorKind::LineageCanonicalBytesLimit
        | CatalogErrorKind::LineageMaterializationProofLimit => {
            DeploymentPreparationDisposition::Validation(ValidationCode::TooLong)
        }
        CatalogErrorKind::Storage | CatalogErrorKind::ActiveCatalogMismatch => {
            DeploymentPreparationDisposition::Unavailable
        }
        CatalogErrorKind::InvalidBundle
        | CatalogErrorKind::UnsupportedBundleVersion
        | CatalogErrorKind::InvalidCommandRegistry
        | CatalogErrorKind::IncompatibleContract
        | CatalogErrorKind::UnknownExecutablePlan
        | CatalogErrorKind::InvalidHistoricalEvidence
        | CatalogErrorKind::InvalidHistoricalKey => DeploymentPreparationDisposition::Integrity,
    }
}

async fn finish_control_plane_terminal(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    terminal: ControlPlaneTerminalAudit,
) -> ServiceResult<()> {
    let (phase, link, audit_failure) = match terminal {
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
    finish_phase(
        service,
        context,
        begun,
        ServiceOperationV1::DeployContract,
        phase,
        link,
    )
    .await
    .map_err(|_| audit_failure.into())
}

async fn finish_control_plane_execution_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    error: ControlPlaneExecutionError,
) -> ServiceFailure {
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
            service.internal_failure(
                ServiceOperationV1::DeployContract,
                InternalDefect::LowerIntegrity,
            ),
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
            .fail_authoritative_readiness(crate::AuthoritativeReadinessFailure::CoordinatorFenced);
    } else if error.kind() == ControlPlaneExecutionErrorKind::InternalDefect {
        service
            .providers
            .health
            .fail_authoritative_readiness(crate::AuthoritativeReadinessFailure::Integrity);
    }
    if finish_phase(
        service,
        context,
        begun,
        ServiceOperationV1::DeployContract,
        phase,
        ServiceAuditLinkV1::None,
    )
    .await
    .is_err()
    {
        return match error.kind() {
            ControlPlaneExecutionErrorKind::AuthorizationDenied => failure,
            ControlPlaneExecutionErrorKind::OutcomeUnknown => PublicError::outcome_unknown().into(),
            _ => PublicError::storage_unavailable().into(),
        };
    }
    failure
}

async fn finish_control_plane_admission_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    error: ControlPlaneExecutionAdmissionError,
) -> ServiceFailure {
    if error == ControlPlaneExecutionAdmissionError::PrimaryFenced {
        return match begun.finish_primary_fence_denial(service, context).await {
            Ok(()) => PublicError::primary_fenced().into(),
            Err(_) => PublicError::storage_unavailable().into(),
        };
    }
    if matches!(error, ControlPlaneExecutionAdmissionError::Fenced) {
        service
            .providers
            .health
            .fail_authoritative_readiness(crate::AuthoritativeReadinessFailure::CoordinatorFenced);
    }
    let _ = finish_phase(
        service,
        context,
        begun,
        ServiceOperationV1::DeployContract,
        ServiceAuditPhaseV1::Failed,
        ServiceAuditLinkV1::None,
    )
    .await;
    PublicError::storage_unavailable().into()
}

async fn finish_catalog_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    operation: ServiceOperationV1,
    error: CatalogError,
) -> ServiceFailure {
    if finish_phase(
        service,
        context,
        begun,
        operation,
        ServiceAuditPhaseV1::Failed,
        ServiceAuditLinkV1::None,
    )
    .await
    .is_err()
    {
        return PublicError::storage_unavailable().into();
    }
    map_unprotected_catalog_error(service, operation, error)
}

fn map_unprotected_catalog_error(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: CatalogError,
) -> ServiceFailure {
    if error.kind() == CatalogErrorKind::Storage {
        PublicError::storage_unavailable().into()
    } else {
        service
            .providers
            .health
            .fail_authoritative_readiness(crate::AuthoritativeReadinessFailure::Integrity);
        service.internal_failure(operation, InternalDefect::LowerIntegrity)
    }
}

async fn finish_internal_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    operation: ServiceOperationV1,
) -> ServiceFailure {
    if finish_phase(
        service,
        context,
        begun,
        operation,
        ServiceAuditPhaseV1::Failed,
        ServiceAuditLinkV1::None,
    )
    .await
    .is_err()
    {
        PublicError::storage_unavailable().into()
    } else {
        service.internal_failure(operation, InternalDefect::LowerIntegrity)
    }
}

async fn finish_contract_change(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    active_version: ContractVersion,
) -> ServiceFailure {
    if finish_phase(
        service,
        context,
        begun,
        ServiceOperationV1::ExplainCommand,
        ServiceAuditPhaseV1::Failed,
        ServiceAuditLinkV1::None,
    )
    .await
    .is_err()
    {
        PublicError::storage_unavailable().into()
    } else {
        PublicError::contract_mismatch(active_version).into()
    }
}

async fn finish_port_admission_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    operation: ServiceOperationV1,
    error: PortAdmissionError,
) -> ServiceFailure {
    match error {
        PortAdmissionError::Cancelled => {
            finish_terminal_failure(
                service,
                context,
                begun,
                operation,
                ServiceFailure::Cancelled,
            )
            .await
        }
        PortAdmissionError::DeadlineExceeded => {
            finish_terminal_failure(
                service,
                context,
                begun,
                operation,
                ServiceFailure::DeadlineExceeded,
            )
            .await
        }
        PortAdmissionError::Unavailable | PortAdmissionError::Stopped => {
            finish_terminal_failure(
                service,
                context,
                begun,
                operation,
                PublicError::storage_unavailable().into(),
            )
            .await
        }
    }
}

async fn finish_controlled_wait_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    operation: ServiceOperationV1,
    error: ControlledWaitError,
) -> ServiceFailure {
    let failure = match error {
        ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
        ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
    };
    finish_terminal_failure(service, context, begun, operation, failure).await
}

async fn finish_terminal_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    operation: ServiceOperationV1,
    failure: ServiceFailure,
) -> ServiceFailure {
    let phase = match failure {
        ServiceFailure::Cancelled | ServiceFailure::DeadlineExceeded => {
            ServiceAuditPhaseV1::Cancelled
        }
        ServiceFailure::Public(_)
        | ServiceFailure::ResponseTooLarge
        | ServiceFailure::EmergencyInternal(_) => ServiceAuditPhaseV1::Failed,
    };
    if finish_phase(
        service,
        context,
        begun,
        operation,
        phase,
        ServiceAuditLinkV1::None,
    )
    .await
    .is_err()
    {
        PublicError::storage_unavailable().into()
    } else {
        failure
    }
}

async fn finish_read_success(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    operation: ServiceOperationV1,
) -> ServiceResult<()> {
    finish_phase(
        service,
        context,
        begun,
        operation,
        ServiceAuditPhaseV1::Succeeded,
        ServiceAuditLinkV1::None,
    )
    .await
    .map_err(|_| PublicError::storage_unavailable().into())
}

async fn finish_phase(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    operation: ServiceOperationV1,
    phase: ServiceAuditPhaseV1,
    link: ServiceAuditLinkV1,
) -> ServiceResult<()> {
    if begun.finish(service, context, phase, link).await.is_err() {
        service.note_audit_failure(operation);
        Err(PublicError::storage_unavailable().into())
    } else {
        Ok(())
    }
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
    use riffdb_errors::{PublicErrorDetails, RecoveryAction};
    use riffdb_types::AdministrationSequence;

    use super::*;

    #[test]
    fn lineage_limits_have_the_exact_root_validation_mapping() {
        let cases = [
            (
                CatalogErrorKind::LineageBundleCountLimit,
                ValidationCode::TooManyItems,
            ),
            (
                CatalogErrorKind::LineageCanonicalBytesLimit,
                ValidationCode::TooLong,
            ),
            (
                CatalogErrorKind::LineageMaterializationProofLimit,
                ValidationCode::TooLong,
            ),
        ];

        for (kind, expected) in cases {
            assert_eq!(
                classify_deployment_preparation_error(kind),
                DeploymentPreparationDisposition::Validation(expected)
            );
            let error = root_validation(expected);
            assert_eq!(error.recovery_action(), RecoveryAction::CorrectRequest);
            let PublicErrorDetails::Validation(issues) = error.details() else {
                panic!("lineage limit must be public validation");
            };
            assert_eq!(issues.as_slice().len(), 1);
            assert_eq!(issues.as_slice()[0].code(), expected);
            assert!(issues.as_slice()[0].path().segments().is_empty());
        }
    }

    #[test]
    fn exact_active_source_reuses_the_validated_bundle_for_replay() {
        let source = include_str!("../../../contracts/examples/budget.riff");
        let compiled = compile_contract_source(source).expect("example contract compiles");
        let active = ValidatedContractBundle::from_compiler_bundle(compiled.clone())
            .expect("compiler bundle validates at catalog boundary");

        let replay = compile_deployment_candidate(source, Some(&active))
            .expect("exact active source remains deployable as a replay");

        assert_eq!(replay.canonical_bytes(), compiled.canonical_bytes());
        assert_eq!(replay.bundle_hash(), compiled.bundle_hash());
    }

    #[test]
    fn exact_application_identity_is_checked_before_catalog_preparation() {
        let source = include_str!("../../../contracts/examples/budget.riff");
        let genesis = compile_contract_source(source).expect("genesis");
        let successor_source = source.replacen("version 1", "version 2", 1);
        let successor =
            compile_contract_successor(&successor_source, &genesis).expect("compatible successor");
        let active = contract_descriptor(&genesis);
        let candidate = contract_descriptor(&successor);
        let request = DeployContractRequest::new_exact(
            crate::ContractSource::new(successor_source).expect("source"),
            Some(genesis.contract_version()),
            Some(genesis.bundle_hash()),
            successor.bundle_hash(),
        )
        .expect("exact request");

        assert_eq!(
            exact_application_identity_result(&request, Some(&active), &candidate),
            None,
            "matching parent and candidate proceed to catalog CAS"
        );
        assert_eq!(
            exact_application_identity_result(&request, Some(&candidate), &candidate),
            None,
            "already-active exact candidate proceeds through read-only catalog preparation so the original audited outcome can be recovered"
        );

        let wrong_candidate = DeployContractRequest::new_exact(
            crate::ContractSource::new(source).expect("source"),
            Some(genesis.contract_version()),
            Some(genesis.bundle_hash()),
            genesis.bundle_hash(),
        )
        .expect("wrong candidate request");
        assert!(matches!(
            exact_application_identity_result(&wrong_candidate, Some(&active), &candidate),
            Some(DeployContractResult::ExpectedApplicationIdentityMismatch { .. })
        ));
    }

    #[test]
    fn candidate_preview_carries_the_exact_parent_and_canonical_bundle() {
        let source = include_str!("../../../contracts/examples/budget.riff");
        let genesis = compile_contract_source(source).expect("genesis");
        let successor =
            compile_contract_successor(&source.replacen("version 1", "version 2", 1), &genesis)
                .expect("successor");
        let preview = crate::CompiledContractCandidate::from_bundle(&successor);

        assert_eq!(preview.parent_version(), Some(genesis.contract_version()));
        assert_eq!(preview.parent_bundle_hash(), Some(genesis.bundle_hash()));
        assert_eq!(preview.candidate().bundle_hash(), successor.bundle_hash());
        assert_eq!(
            ContractBundle::decode(preview.canonical_bundle())
                .expect("canonical preview")
                .bundle_hash(),
            successor.bundle_hash()
        );
    }

    #[test]
    fn deployment_oversize_retains_the_executor_terminal_classification() {
        let source = include_str!("../../../contracts/examples/budget.riff");
        let bundle = compile_contract_source(source).expect("example contract compiles");
        let link = ServiceAuditLinkV1::ControlPlane {
            administration_sequence: AdministrationSequence::first(),
        };
        let known = PendingTerminalResponse::new(
            DeployContractResult::AlreadyActive(contract_descriptor(&bundle)),
            ControlPlaneTerminalAudit::Succeeded(link),
            |_| Err(ServiceFailure::ResponseTooLarge),
        );
        let no_transition = PendingTerminalResponse::new(
            DeployContractResult::BundleConflict,
            ControlPlaneTerminalAudit::Failed,
            |_| Err(ServiceFailure::ResponseTooLarge),
        );

        assert_eq!(known.terminal(), ControlPlaneTerminalAudit::Succeeded(link));
        assert!(matches!(
            known.into_response(),
            Err(ServiceFailure::ResponseTooLarge)
        ));
        assert_eq!(no_transition.terminal(), ControlPlaneTerminalAudit::Failed);
        assert!(matches!(
            no_transition.into_response(),
            Err(ServiceFailure::ResponseTooLarge)
        ));
    }
}

#[cfg(test)]
#[path = "primary_fenced_contract_tests.rs"]
mod primary_fenced_contract_tests;
