//! Authoritative query, projection, and policy-filtered discovery orchestration.

use std::collections::BTreeMap;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use riffdb_catalog::{
    CatalogError, CatalogErrorKind, ValidatedContractBundle, ValidatedQueryModule,
};
use riffdb_contract_ir::{
    BoundProjectionGroupSchema, EntitySchema, GeneratedSchemaArtifact, IndexSchema, RecordSchema,
    SchemaArtifactKey, SchemaIr, ValueType,
};
use riffdb_errors::{
    PublicError, ValidationCode, ValidationIssue, ValidationIssues, ValidationPath,
    ValidationPathSegment,
};
use riffdb_invariant::{EvaluationError, ExpressionValueSource, evaluate_expression};
use riffdb_policy::{
    AuthorizedOperation, CommandToolCandidate, DiscoveryResource, DiscoveryVisibility,
    EntitySchemaCandidate, FixedToolCandidate, MAX_DISCOVERY_PAGE_ITEMS, NamedQueryToolCandidate,
    OperationRequest, OperationTenantScope, OutputClassification, PartitionConstraint,
    ResourceDiscoveryVisibility,
};
use riffdb_query_module::generate_mcp_tools;
use riffdb_types::{
    CanonicalRecord, CanonicalValue, ContractLineage, EntityKey, FieldId, FrontierPosition,
    HashDomain, MAX_CAPABILITY_FIELD_VISIBILITY, PartitionKey, PartitionScopeV1,
    ProjectionGeneration, QueryOperationName, ScopedPartitionV1, ServiceAuditLinkV1,
    ServiceAuditPhaseV1, ServiceOperationV1, TenantScope, hash,
};

use crate::command_operations::{SubmittedValueMaterializationError, materialize_submitted_value};
use crate::orchestration::{AuditScope, BegunInvocation, BegunInvocationCompletion};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    AuthoritativeEntityRequest, AuthoritativeEntitySnapshot, AuthoritativeIndexPage,
    AuthoritativeIndexRequest, AuthoritativeReadError, AuthoritativeReadinessFailure,
    AuthoritativeSchemaBinding, CommandDiscoveryCursorLookup, CommandDiscoveryCursorState,
    CommandToolDescriptor, CommandToolDiscoveryItem, ContractSelection,
    DiscoverCommandToolsRequest, DiscoverCommandToolsResult, DiscoverResourcesRequest,
    DiscoverResourcesResult, DiscoveryCatalogFence, DiscoveryRepresentation, EntityView,
    FieldSelection, FixedToolKind, GetEntityRequest, GetEntityResult, GetProjectionStatusRequest,
    GetProjectionStatusResult, GetReactiveWakeupResult, IndexRowView, IndexScanCursorLookup,
    IndexScanCursorPolicy, IndexScanCursorState, IndexScanFence, InternalDefect,
    NamedQueryToolDescriptor, NamedQueryToolSchemaArtifact, OperationSchemaCatalog, Page,
    PageLimit, PortAdmissionError, PortDriverStopped, ProjectionCursorLookup,
    ProjectionCursorPolicy, ProjectionCursorState, ProjectionPageFence, ProjectionPortError,
    ProjectionPortReady, ProjectionPortRequest, ProjectionPortResult, ProjectionStateFence,
    QueryApplication, QueryProjectionReady, QueryProjectionRequest, QueryProjectionResult,
    ReactiveWakeupGeneration, RequestContext, ResourceDescriptor, ResourceDiscoveryCursorLookup,
    ResourceDiscoveryCursorState, ResourceDiscoveryCursorVisibility, RiffDbService,
    RiffDbServiceInner, ScanIndexRequest, ScanIndexResult, ServiceAuditTargetMap, ServiceFailure,
    ServiceFuture, ServiceResult, ServiceTelemetryEvent, SubmittedValue, ensure_response_budget,
    fit_full_command_discovery_page_items, fit_full_resource_discovery_page_items, fit_page_items,
    fit_sparse_page_items,
};
use crate::{CursorAccessError, CursorContractIdentity};

// Bounds policy reloads and lower-port admissions even if a provider emits an
// adversarial sequence of immediate wake observations within the 30-second wait.
const MAX_PROJECTION_WAIT_OBSERVATIONS: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProjectionOperationWaitError {
    Controlled(ControlledWaitError),
    ProjectionDeadlineElapsed,
}

async fn wait_for_projection_operation<F>(
    control: &crate::RequestControl,
    deadline_scheduler: &dyn crate::RequestDeadlineScheduler,
    projection_deadline: Option<Instant>,
    future: F,
) -> Result<F::Output, ProjectionOperationWaitError>
where
    F: Future,
{
    let mut future = Box::pin(future);
    let mut cancelled = Box::pin(control.cancelled());
    let mut request_deadline = deadline_scheduler.wait_until(control.deadline());
    let mut projection_deadline_wait =
        projection_deadline.map(|deadline| deadline_scheduler.wait_until(deadline));

    poll_fn(|context| {
        if control.is_cancelled() || Pin::as_mut(&mut cancelled).poll(context).is_ready() {
            return Poll::Ready(Err(ProjectionOperationWaitError::Controlled(
                ControlledWaitError::Cancelled,
            )));
        }
        if control.is_deadline_exceeded()
            || Pin::as_mut(&mut request_deadline).poll(context).is_ready()
        {
            return Poll::Ready(Err(ProjectionOperationWaitError::Controlled(
                ControlledWaitError::DeadlineExceeded,
            )));
        }
        if projection_deadline.is_some_and(|deadline| Instant::now() >= deadline)
            || projection_deadline_wait
                .as_mut()
                .is_some_and(|deadline| Pin::as_mut(deadline).poll(context).is_ready())
        {
            return Poll::Ready(Err(ProjectionOperationWaitError::ProjectionDeadlineElapsed));
        }
        Pin::as_mut(&mut future).poll(context).map(Ok)
    })
    .await
}

impl QueryApplication for RiffDbService {
    fn get_entity(
        &self,
        context: RequestContext,
        request: GetEntityRequest,
    ) -> ServiceFuture<'_, GetEntityResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::GetEntity, ingress, async move {
            get_entity(service, context, request).await
        })
    }

    fn scan_index(
        &self,
        context: RequestContext,
        request: ScanIndexRequest,
    ) -> ServiceFuture<'_, ScanIndexResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::ScanIndex, ingress, async move {
            scan_index(service, context, request).await
        })
    }

    fn query_projection(
        &self,
        context: RequestContext,
        request: QueryProjectionRequest,
    ) -> ServiceFuture<'_, QueryProjectionResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::QueryProjection, ingress, async move {
            query_projection(service, context, request).await
        })
    }

    fn get_projection_status(
        &self,
        context: RequestContext,
        request: GetProjectionStatusRequest,
    ) -> ServiceFuture<'_, GetProjectionStatusResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::GetProjectionStatus,
            ingress,
            async move { get_projection_status(service, context, request).await },
        )
    }
}

impl crate::DiscoveryApplication for RiffDbService {
    fn discover_command_tools(
        &self,
        context: RequestContext,
        request: DiscoverCommandToolsRequest,
    ) -> ServiceFuture<'_, DiscoverCommandToolsResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::DiscoverCommandTools,
            ingress,
            async move { discover_command_tools(service, context, request).await },
        )
    }

    fn discover_resources(
        &self,
        context: RequestContext,
        request: DiscoverResourcesRequest,
    ) -> ServiceFuture<'_, DiscoverResourcesResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::DiscoverResources, ingress, async move {
            discover_resources(service, context, request).await
        })
    }

    fn get_reactive_wakeup(
        &self,
        context: RequestContext,
    ) -> ServiceFuture<'_, GetReactiveWakeupResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::GetReactiveWakeup, ingress, async move {
            get_reactive_wakeup(service, context).await
        })
    }
}

async fn get_reactive_wakeup(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
) -> ServiceResult<GetReactiveWakeupResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::GetReactiveWakeup;
    let begun = service
        .begin_invocation(
            &context,
            OperationRequest::get_reactive_wakeup(),
            ServiceAuditTargetMap::reactive_wakeup(),
            AuditScope::Intrinsic,
        )
        .await?;
    if !valid_reactive_wakeup_authorization(&service, begun.initial_authorization()) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }

    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .authoritative
            .reserve_application_head(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => {
            return Err(finish_admission_failure(&service, &context, &begun, error).await);
        }
        Err(error) => {
            return Err(finish_controlled_wait(&service, &context, &begun, error).await);
        }
    };

    let authorization = begun.reauthorize(&service, &context).await?;
    if !valid_reactive_wakeup_authorization(&service, &authorization) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    let receipt = match permit.submit(()) {
        Ok(receipt) => receipt,
        Err(error) => {
            return Err(finish_admission_failure(&service, &context, &begun, error).await);
        }
    };
    let head = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(head))) => head,
        Ok(Ok(Err(error))) => {
            let failure = authoritative_read_failure(&service, OPERATION, error);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        Ok(Err(PortDriverStopped)) => {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        Err(error) => {
            return Err(finish_controlled_wait(&service, &context, &begun, error).await);
        }
    };

    let authorization = begun.reauthorize(&service, &context).await?;
    if !valid_reactive_wakeup_authorization(&service, &authorization) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    let result = GetReactiveWakeupResult::new(reactive_wakeup_generation(
        service.identity.database_id(),
        service.identity.history_incarnation(),
        head,
    ));
    finish_success(&service, &context, &begun).await?;
    Ok(result)
}

fn valid_reactive_wakeup_authorization(
    service: &RiffDbServiceInner,
    authorization: &AuthorizedOperation,
) -> bool {
    let obligations = authorization.obligations();
    authorization.database_id() == service.identity.database_id()
        && authorization.environment() == service.identity.environment()
        && authorization.operation() == ServiceOperationV1::GetReactiveWakeup
        && obligations.audit_class() == Some(riffdb_policy::AuditClass::AdministrativeRead)
        && obligations.output_classification() == OutputClassification::PublicMetadata
        && obligations.partition_constraint().is_none()
        && obligations.field_mask().is_none()
        && obligations.row_limit().is_none()
}

fn reactive_wakeup_generation(
    database_id: riffdb_types::DatabaseId,
    history_incarnation: u64,
    head: FrontierPosition,
) -> ReactiveWakeupGeneration {
    let mut transcript = [0_u8; 33];
    transcript[..16].copy_from_slice(database_id.as_bytes());
    transcript[16..24].copy_from_slice(&history_incarnation.to_be_bytes());
    if let FrontierPosition::AppliedThrough(sequence) = head {
        transcript[24] = 1;
        transcript[25..].copy_from_slice(&sequence.to_be_bytes());
    }
    ReactiveWakeupGeneration::from_bytes(*hash(HashDomain::ReactiveWakeup, &transcript).as_bytes())
}

async fn get_entity(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: GetEntityRequest,
) -> ServiceResult<GetEntityResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::GetEntity;
    let bundle =
        prepare_selected_contract(&service, &context, request.contract(), OPERATION).await?;
    let contract = bundle.bundle();
    let lineage = contract.lineage().clone();
    let version = contract.contract_version();
    let entity = contract
        .schema()
        .entity(request.entity_type_id())
        .cloned()
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    validate_field_selection(&entity, request.fields().as_slice())?;
    // ADR-0118: the classification rides the compiled schema; the release
    // point in `filter_record` withholds these fields regardless of the
    // ordinary visibility mask.
    let secret_fields = contract.schema().secret_fields_for_entity(entity.id());
    let partition = derive_entity_partition(contract.schema(), &entity, request.key())
        .map_err(|error| preparation_failure(&service, OPERATION, error))?;
    let policy_request = OperationRequest::get_entity(
        lineage.clone(),
        version,
        entity.id(),
        OperationTenantScope::global_only(),
        partition.clone(),
        request.fields().as_slice().to_vec(),
    )
    .and_then(|request| request.with_secret_classified_fields(secret_fields.clone()))
    .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let targets = ServiceAuditTargetMap::get_entity(lineage.clone(), version, entity.id())
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let begun = service
        .begin_invocation(&context, policy_request, targets, AuditScope::StandardRead)
        .await?;

    let authorization = begun.reauthorize(&service, &context).await?;
    let _initial_visible_fields = match entity_authorization(
        &service,
        &authorization,
        &lineage,
        &entity,
        &partition,
        request.fields().as_slice(),
    ) {
        Some(fields) => fields,
        None => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };

    let lower_request =
        AuthoritativeEntityRequest::new(lineage.clone(), version, request.key().clone());
    // Port admission + lower read retry together; audit begin/finish stay outside.
    let snapshot = match crate::read_retry::with_read_retry(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service.providers.telemetry.as_ref(),
        OPERATION,
        |_attempt| {
            let service = &service;
            let context = &context;
            let lower_request = lower_request.clone();
            async move {
                let permit = match wait_with_control(
                    context.control(),
                    service.providers.deadline_scheduler.as_ref(),
                    service
                        .providers
                        .authoritative
                        .reserve_read_entity(context.control()),
                )
                .await
                {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(PortAdmissionError::Unavailable)) => {
                        return Err(Ok(crate::read_retry::RetryableReadFault::PortUnavailable));
                    }
                    Ok(Err(error)) => {
                        return Err(Err(admission_error_failure(error)));
                    }
                    Err(error) => return Err(Err(controlled_wait_failure(error))),
                };
                let receipt = match permit.submit(lower_request) {
                    Ok(receipt) => receipt,
                    Err(PortAdmissionError::Unavailable) => {
                        return Err(Ok(crate::read_retry::RetryableReadFault::PortUnavailable));
                    }
                    Err(error) => return Err(Err(admission_error_failure(error))),
                };
                match wait_with_control(
                    context.control(),
                    service.providers.deadline_scheduler.as_ref(),
                    receipt,
                )
                .await
                {
                    Ok(Ok(Ok(snapshot))) => Ok(snapshot),
                    Ok(Ok(Err(AuthoritativeReadError::Unavailable))) => Err(Ok(
                        crate::read_retry::RetryableReadFault::BackendUnavailable,
                    )),
                    Ok(Ok(Err(error))) => {
                        Err(Err(authoritative_read_failure(service, OPERATION, error)))
                    }
                    Ok(Err(PortDriverStopped)) => {
                        Err(Err(lower_integrity_failure(service, OPERATION)))
                    }
                    Err(error) => Err(Err(controlled_wait_failure(error))),
                }
            }
        },
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(failure) => return Err(finish_failure(&service, &context, &begun, failure).await),
    };

    let return_authorization = begun.reauthorize(&service, &context).await?;
    let visible_fields = match entity_authorization(
        &service,
        &return_authorization,
        &lineage,
        &entity,
        &partition,
        request.fields().as_slice(),
    ) {
        Some(fields) => fields,
        None => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    // Explicit secret reveal authority (ADR-0118) is derived only from the
    // grant's dedicated secret-visibility naming, never from the ordinary
    // mask; without it every secret field releases as its redaction marker.
    let secret_reveal =
        secret_reveal_authorities(&return_authorization, &lineage, &entity, &secret_fields);
    let result = match snapshot {
        None => GetEntityResult::NotFound,
        Some(snapshot) => {
            let view = match entity_view(
                &service,
                OPERATION,
                &entity,
                request.key(),
                &visible_fields,
                request.fields().as_slice(),
                &secret_fields,
                &secret_reveal,
                snapshot,
            ) {
                Ok(view) => view,
                Err(failure) => {
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            };
            GetEntityResult::Found(view)
        }
    };
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    finish_success(&service, &context, &begun).await?;
    Ok(result)
}

async fn scan_index(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: ScanIndexRequest,
) -> ServiceResult<ScanIndexResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ScanIndex;
    let bundle =
        prepare_selected_contract(&service, &context, request.contract(), OPERATION).await?;
    let contract = bundle.bundle();
    let lineage = contract.lineage().clone();
    let version = contract.contract_version();
    let (entity, index) = find_index(contract.schema(), request.index_id())
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    validate_field_selection(&entity, request.fields().as_slice())?;
    // ADR-0118: the classification rides the compiled schema; the release
    // point in `filter_record` withholds these fields regardless of the
    // ordinary visibility mask.
    let secret_fields = contract.schema().secret_fields_for_entity(entity.id());
    let leading_components = materialize_query_components(
        &service,
        OPERATION,
        contract.schema(),
        index
            .key_schema()
            .components()
            .iter()
            .map(|component| component.value_type()),
        request.leading_components(),
    )?;
    let prefix = index
        .key_schema()
        .encode_index_prefix(&leading_components)
        .map_err(|_| validation_failure(ValidationCode::InvalidValue))?;
    let page_request = request.page();
    let cursor_lookup = IndexScanCursorLookup::new(
        CursorContractIdentity::new(lineage.clone(), version, bundle.bundle_hash()),
        index.id(),
        entity.id(),
        leading_components.clone(),
        prefix.clone(),
        request.fields().clone(),
        page_request.limit(),
    )
    .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    // An index whose fields include a secret-classified field embeds that
    // field's canonical bytes in every released IndexEntryKey, so scanning
    // it IS a projection of the value (ADR-0118): the embedded secrets join
    // the requested set and the authorizer denies the scan without the
    // grant's dedicated secret naming. Uniqueness enforcement and exact
    // probes run in the commit path and never consult visibility.
    let mut policy_fields = request.fields().as_slice().to_vec();
    for field in index.fields() {
        if secret_fields.binary_search(field).is_ok() && !policy_fields.contains(field) {
            policy_fields.push(*field);
        }
    }
    let policy_request = OperationRequest::scan_index(
        lineage.clone(),
        version,
        index.id(),
        entity.id(),
        OperationTenantScope::global_only(),
        policy_fields,
        page_request.limit().get(),
    )
    .and_then(|request| request.with_secret_classified_fields(secret_fields.clone()))
    .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let targets = ServiceAuditTargetMap::scan_index(lineage.clone(), version, index.id())
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let begun = service
        .begin_invocation(&context, policy_request, targets, AuditScope::StandardRead)
        .await?;

    let Some(initial_policy) = current_scan_policy(
        &service,
        begun.initial_authorization(),
        &lineage,
        &entity,
        page_request.limit(),
        request.fields().as_slice(),
    ) else {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    let cursor_state = match page_request.cursor() {
        Some(token) => match service.cursors.resolve_index_scan(
            token,
            context.principal().principal_id(),
            &cursor_lookup,
        ) {
            Ok(state) => Some(state),
            Err(CursorAccessError::InvalidCursor) => {
                return Err(
                    finish_failure(&service, &context, &begun, invalid_cursor_failure()).await,
                );
            }
            Err(CursorAccessError::Unavailable) => {
                let failure = cursor_unavailable_failure(&service);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        },
        None => None,
    };
    if constrain_index_scan_policy(
        initial_policy,
        cursor_state.as_deref().map(IndexScanCursorState::policy),
    )
    .is_none()
    {
        return Err(begun.finish_authorization_denial(&service, &context).await);
    }

    let authorization = begun.reauthorize(&service, &context).await?;
    let Some(current_policy) = current_scan_policy(
        &service,
        &authorization,
        &lineage,
        &entity,
        page_request.limit(),
        request.fields().as_slice(),
    ) else {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    let Some(effective_policy) = constrain_index_scan_policy(
        current_policy,
        cursor_state.as_deref().map(IndexScanCursorState::policy),
    ) else {
        return Err(begun.finish_authorization_denial(&service, &context).await);
    };
    let effective_limit = effective_policy.effective_limit();
    let lower_request = match AuthoritativeIndexRequest::new(
        lineage.clone(),
        version,
        index.id(),
        leading_components.clone(),
        prefix,
        effective_policy.partition_constraint().clone(),
        cursor_state
            .as_deref()
            .map(IndexScanCursorState::after)
            .cloned(),
        effective_limit,
    ) {
        Ok(request) => request,
        Err(_) => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    // Port admission + lower page retry together; audit begin/finish stay outside.
    // Stale/invalid continuations are not retried.
    let lower_page = match crate::read_retry::with_read_retry(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service.providers.telemetry.as_ref(),
        OPERATION,
        |_attempt| {
            let service = &service;
            let context = &context;
            let lower_request = lower_request.clone();
            let has_cursor = cursor_state.is_some();
            async move {
                let permit = match wait_with_control(
                    context.control(),
                    service.providers.deadline_scheduler.as_ref(),
                    service
                        .providers
                        .authoritative
                        .reserve_scan_index(context.control()),
                )
                .await
                {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(PortAdmissionError::Unavailable)) => {
                        return Err(Ok(crate::read_retry::RetryableReadFault::PortUnavailable));
                    }
                    Ok(Err(error)) => return Err(Err(admission_error_failure(error))),
                    Err(error) => return Err(Err(controlled_wait_failure(error))),
                };
                let receipt = match permit.submit(lower_request) {
                    Ok(receipt) => receipt,
                    Err(PortAdmissionError::Unavailable) => {
                        return Err(Ok(crate::read_retry::RetryableReadFault::PortUnavailable));
                    }
                    Err(error) => return Err(Err(admission_error_failure(error))),
                };
                match wait_with_control(
                    context.control(),
                    service.providers.deadline_scheduler.as_ref(),
                    receipt,
                )
                .await
                {
                    Ok(Ok(Ok(page))) => Ok(page),
                    Ok(Ok(Err(AuthoritativeReadError::Unavailable))) => Err(Ok(
                        crate::read_retry::RetryableReadFault::BackendUnavailable,
                    )),
                    Ok(Ok(Err(AuthoritativeReadError::InvalidContinuation))) if has_cursor => {
                        Err(Err(invalid_cursor_failure()))
                    }
                    Ok(Ok(Err(error))) => {
                        Err(Err(authoritative_read_failure(service, OPERATION, error)))
                    }
                    Ok(Err(PortDriverStopped)) => {
                        Err(Err(lower_integrity_failure(service, OPERATION)))
                    }
                    Err(error) => Err(Err(controlled_wait_failure(error))),
                }
            }
        },
    )
    .await
    {
        Ok(page) => page,
        Err(failure) => return Err(finish_failure(&service, &context, &begun, failure).await),
    };

    if cursor_state
        .as_deref()
        .is_some_and(|state| state.epoch() != lower_page.epoch())
    {
        return Err(finish_failure(&service, &context, &begun, invalid_cursor_failure()).await);
    }
    let derived_partitions = match validate_authoritative_index_rows(
        &service,
        &context,
        &bundle,
        &lineage,
        entity.id(),
        index.id(),
        effective_policy.partition_constraint(),
        &lower_page,
    )
    .await
    {
        Ok(partitions) => partitions,
        Err(IndexRowValidationError::Controlled(error)) => {
            return Err(finish_controlled_wait(&service, &context, &begun, error).await);
        }
        Err(IndexRowValidationError::Catalog(error)) => {
            let failure = catalog_failure(&service, OPERATION, error);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        Err(IndexRowValidationError::Integrity) => {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let return_authorization = begun.reauthorize(&service, &context).await?;
    let Some(return_current_policy) = current_scan_policy(
        &service,
        &return_authorization,
        &lineage,
        &entity,
        page_request.limit(),
        request.fields().as_slice(),
    ) else {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    let Some(return_policy) =
        constrain_index_scan_policy(return_current_policy, Some(&effective_policy))
    else {
        return Err(begun.finish_authorization_denial(&service, &context).await);
    };
    let return_limit = return_policy.effective_limit();
    let secret_reveal =
        secret_reveal_authorities(&return_authorization, &lineage, &entity, &secret_fields);
    let mut rows = match index_views(
        &entity,
        &lineage,
        return_policy.partition_constraint(),
        return_policy.visible_fields().as_slice(),
        request.fields().as_slice(),
        &secret_fields,
        &secret_reveal,
        &lower_page,
        &derived_partitions,
    ) {
        Ok(rows) => rows,
        Err(()) => {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let more_due_to_limit = rows.len() > usize::from(return_limit.get().get());
    rows.truncate(usize::from(return_limit.get().get()));
    let fence = IndexScanFence::new(lower_page.epoch());
    let fit = match fit_sparse_page_items(
        &rows,
        &fence,
        more_due_to_limit || lower_page.scanned_through().is_some(),
    ) {
        Ok(fit) => fit,
        Err(failure) => {
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let continuation_after = match index_continuation_after(
        &rows,
        fit.item_count(),
        fit.has_more(),
        more_due_to_limit,
        lower_page.scanned_through(),
    ) {
        Ok(continuation) => continuation,
        Err(()) => {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    rows.truncate(fit.item_count());
    let cursor_guard = match continuation_after {
        Some(after) => {
            let state = IndexScanCursorState::new(after, lower_page.epoch(), return_policy.clone());
            match service.cursors.register_index_scan_unpublished(
                context.principal().principal_id(),
                cursor_lookup,
                state,
            ) {
                Ok(guard) => Some(guard),
                Err(_) => {
                    let failure = cursor_unavailable_failure(&service);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            }
        }
        None => None,
    };
    let next_cursor = cursor_guard
        .as_ref()
        .map(crate::CursorPublicationGuard::token);
    let public_page = match Page::new_sparse_progress(return_limit, rows, next_cursor, fence) {
        Ok(page) => page,
        Err(_) => {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let result = ScanIndexResult::new(public_page);
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    finish_success(&service, &context, &begun).await?;
    if let Some(guard) = cursor_guard {
        guard.publish();
    }
    Ok(result)
}

async fn query_projection(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: QueryProjectionRequest,
) -> ServiceResult<QueryProjectionResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::QueryProjection;
    let bundle =
        prepare_selected_contract(&service, &context, request.contract(), OPERATION).await?;
    let contract = bundle.bundle();
    let lineage = contract.lineage().clone();
    let version = contract.contract_version();
    let schema = contract
        .bound_projection_group_schema(request.projection_id())
        .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
    let leading_components = materialize_query_components(
        &service,
        OPERATION,
        contract.schema(),
        schema
            .schema()
            .group_components()
            .iter()
            .map(|component| component.value_type()),
        request.leading_components(),
    )?;
    schema
        .group_prefix(ProjectionGeneration::first(), &leading_components)
        .map_err(|_| validation_failure(ValidationCode::InvalidValue))?;
    let page_request = request.page();
    let cursor_lookup = ProjectionCursorLookup::new(
        CursorContractIdentity::new(lineage.clone(), version, bundle.bundle_hash()),
        schema.identity().clone(),
        leading_components.clone(),
        request.required_sequence(),
        request.wait(),
        page_request.limit(),
    )
    .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let policy_request = OperationRequest::query_projection(
        version,
        schema.identity().clone(),
        leading_components.clone(),
        page_request.limit().get(),
    )
    .map_err(|_| validation_failure(ValidationCode::InvalidValue))?;
    let targets =
        ServiceAuditTargetMap::query_projection(lineage.clone(), version, request.projection_id())
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let begun = service
        .begin_invocation(&context, policy_request, targets, AuditScope::StandardRead)
        .await?;

    let Some(initial_policy) = current_projection_policy(
        &service,
        begun.initial_authorization(),
        page_request.limit(),
    ) else {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    let cursor_state = match page_request.cursor() {
        Some(token) => match service.cursors.resolve_projection(
            token,
            context.principal().principal_id(),
            &cursor_lookup,
        ) {
            Ok(state) => Some(state),
            Err(CursorAccessError::InvalidCursor) => {
                return Err(
                    finish_failure(&service, &context, &begun, invalid_cursor_failure()).await,
                );
            }
            Err(CursorAccessError::Unavailable) => {
                let failure = cursor_unavailable_failure(&service);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        },
        None => None,
    };
    let Some(mut effective_policy) = constrain_projection_policy(
        initial_policy,
        cursor_state.as_deref().map(ProjectionCursorState::policy),
    ) else {
        return Err(begun.finish_authorization_denial(&service, &context).await);
    };
    let Some(wait_deadline) = Instant::now()
        .checked_add(request.wait())
        .map(|deadline| deadline.min(context.control().deadline()))
    else {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    // A zero wait is an immediate projection observation, so queue admission
    // remains governed by the outer request deadline. A real bounded wait owns
    // this one absolute deadline across every fresh capacity reservation.
    let projection_wait_deadline = (!request.wait().is_zero()).then_some(wait_deadline);
    let mut observation_count = 0_usize;
    let lower_result = loop {
        let permit = match wait_for_projection_operation(
            context.control(),
            service.providers.deadline_scheduler.as_ref(),
            projection_wait_deadline,
            service
                .providers
                .projection
                .reserve_query_projection(context.control()),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(error)) => {
                return Err(finish_admission_failure(&service, &context, &begun, error).await);
            }
            Err(ProjectionOperationWaitError::Controlled(error)) => {
                return Err(finish_controlled_wait(&service, &context, &begun, error).await);
            }
            Err(ProjectionOperationWaitError::ProjectionDeadlineElapsed) => {
                return Err(finish_controlled_wait(
                    &service,
                    &context,
                    &begun,
                    ControlledWaitError::DeadlineExceeded,
                )
                .await);
            }
        };

        let authorization = begun.reauthorize(&service, &context).await?;
        let Some(current_policy) =
            current_projection_policy(&service, &authorization, page_request.limit())
        else {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        };
        let Some(submission_policy) =
            constrain_projection_policy(current_policy, Some(&effective_policy))
        else {
            return Err(begun.finish_authorization_denial(&service, &context).await);
        };
        effective_policy = submission_policy;
        let lower_request = match ProjectionPortRequest::new(
            schema.identity().clone(),
            leading_components.clone(),
            request.required_sequence(),
            wait_deadline,
            effective_policy.effective_limit(),
            cursor_state
                .as_deref()
                .map(ProjectionCursorState::continuation)
                .cloned(),
        ) {
            Ok(request) => request,
            Err(_) => {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        };
        let receipt = match permit.submit(lower_request) {
            Ok(receipt) => receipt,
            Err(error) => {
                return Err(finish_admission_failure(&service, &context, &begun, error).await);
            }
        };
        let observed = match wait_for_projection_operation(
            context.control(),
            service.providers.deadline_scheduler.as_ref(),
            projection_wait_deadline,
            receipt,
        )
        .await
        {
            Ok(Ok(Ok(result))) => result,
            Ok(Ok(Err(error))) => {
                let failure = projection_failure(&service, OPERATION, error);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
            Ok(Err(PortDriverStopped)) => {
                let failure = lower_integrity_failure(&service, OPERATION);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
            Err(ProjectionOperationWaitError::Controlled(error)) => {
                return Err(finish_controlled_wait(&service, &context, &begun, error).await);
            }
            Err(ProjectionOperationWaitError::ProjectionDeadlineElapsed) => {
                let failure =
                    projection_failure(&service, OPERATION, ProjectionPortError::Unavailable);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        };
        observation_count += 1;

        let ProjectionPortResult::PendingObservation { fence } = observed else {
            break observed;
        };
        match validate_pending_projection_observation(
            &fence,
            schema.identity(),
            request.required_sequence(),
            request.wait(),
            cursor_state
                .as_deref()
                .map(ProjectionCursorState::page_fence),
        ) {
            Ok(()) => {}
            Err(ProjectionObservationValidationError::Integrity) => {
                let failure = lower_integrity_failure(&service, OPERATION);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
            Err(ProjectionObservationValidationError::InvalidCursor) => {
                return Err(
                    finish_failure(&service, &context, &begun, invalid_cursor_failure()).await,
                );
            }
        }

        // This is the mandatory post-wake safe point. Its proof cannot be
        // reused after the next bounded capacity wait; the loop reauthorizes
        // again while holding the newly reserved permit before submission.
        let wake_authorization = begun.reauthorize(&service, &context).await?;
        let Some(wake_policy) =
            current_projection_policy(&service, &wake_authorization, page_request.limit())
        else {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        };
        let Some(narrowed_policy) =
            constrain_projection_policy(wake_policy, Some(&effective_policy))
        else {
            return Err(begun.finish_authorization_denial(&service, &context).await);
        };
        effective_policy = narrowed_policy;

        if observation_count >= MAX_PROJECTION_WAIT_OBSERVATIONS {
            let failure = projection_failure(&service, OPERATION, ProjectionPortError::Unavailable);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };

    // A bounded projection wait is a second policy safe point. No result from
    // the lower wait is shaped or released before this fresh decision.
    let return_authorization = begun.reauthorize(&service, &context).await?;
    let Some(return_current_policy) =
        current_projection_policy(&service, &return_authorization, page_request.limit())
    else {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    let Some(return_policy) =
        constrain_projection_policy(return_current_policy, Some(&effective_policy))
    else {
        return Err(begun.finish_authorization_denial(&service, &context).await);
    };
    let return_limit = return_policy.effective_limit();
    let mut result_cursor_guard = None;
    let result = match lower_result {
        ProjectionPortResult::Ready(ready) => {
            let fence = ProjectionPageFence::new(
                ready.identity().clone(),
                ready.generation(),
                ready.frontier(),
            );
            match validate_projection_page_observation(
                &fence,
                schema.identity(),
                cursor_state
                    .as_deref()
                    .map(ProjectionCursorState::page_fence),
            ) {
                Ok(()) => {}
                Err(ProjectionObservationValidationError::Integrity) => {
                    let failure = lower_integrity_failure(&service, OPERATION);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
                Err(ProjectionObservationValidationError::InvalidCursor) => {
                    return Err(finish_failure(
                        &service,
                        &context,
                        &begun,
                        invalid_cursor_failure(),
                    )
                    .await);
                }
            }
            if validate_projection_ready(&schema, &ready).is_err()
                || request.required_sequence().is_some_and(|required| {
                    ready.frontier() < riffdb_types::FrontierPosition::AppliedThrough(required)
                })
            {
                let failure = lower_integrity_failure(&service, OPERATION);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
            let mut rows = ready.rows().to_vec();
            let more_due_to_limit = rows.len() > usize::from(return_limit.get().get());
            rows.truncate(usize::from(return_limit.get().get()));
            let fit = match fit_page_items(
                &rows,
                &fence,
                more_due_to_limit || ready.continuation().is_some(),
            ) {
                Ok(fit) => fit,
                Err(failure) => {
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            };
            let continuation = if fit.has_more() {
                if fit.item_count() < rows.len() || more_due_to_limit {
                    let Some(last) = rows.get(fit.item_count() - 1) else {
                        let failure = lower_integrity_failure(&service, OPERATION);
                        return Err(finish_failure(&service, &context, &begun, failure).await);
                    };
                    let prefix = match schema.group_prefix(ready.generation(), &leading_components)
                    {
                        Ok(prefix) => prefix,
                        Err(_) => {
                            let failure = lower_integrity_failure(&service, OPERATION);
                            return Err(finish_failure(&service, &context, &begun, failure).await);
                        }
                    };
                    let key = match schema.group_key(ready.generation(), last.group()) {
                        Ok(key) => key,
                        Err(_) => {
                            let failure = lower_integrity_failure(&service, OPERATION);
                            return Err(finish_failure(&service, &context, &begun, failure).await);
                        }
                    };
                    match crate::ProjectionContinuation::from_provider(
                        schema.identity().clone(),
                        ready.generation(),
                        prefix,
                        key,
                        ready.frontier(),
                    ) {
                        Ok(continuation) => Some(continuation),
                        Err(_) => {
                            let failure = lower_integrity_failure(&service, OPERATION);
                            return Err(finish_failure(&service, &context, &begun, failure).await);
                        }
                    }
                } else {
                    ready.continuation().cloned()
                }
            } else {
                None
            };
            rows.truncate(fit.item_count());
            let cursor_guard = match continuation {
                Some(continuation) => {
                    let state = ProjectionCursorState::new(continuation, return_policy.clone());
                    match service.cursors.register_projection_unpublished(
                        context.principal().principal_id(),
                        cursor_lookup,
                        state,
                    ) {
                        Ok(guard) => Some(guard),
                        Err(_) => {
                            let failure = cursor_unavailable_failure(&service);
                            return Err(finish_failure(&service, &context, &begun, failure).await);
                        }
                    }
                }
                None => None,
            };
            let next_cursor = cursor_guard
                .as_ref()
                .map(crate::CursorPublicationGuard::token);
            let page = match Page::new(return_limit, rows, next_cursor, fence) {
                Ok(page) => page,
                Err(_) => {
                    let failure = lower_integrity_failure(&service, OPERATION);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            };
            let ready = match QueryProjectionReady::new(page, ready.frontier()) {
                Ok(ready) => ready,
                Err(_) => {
                    let failure = lower_integrity_failure(&service, OPERATION);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            };
            result_cursor_guard = cursor_guard;
            QueryProjectionResult::Ready(ready)
        }
        ProjectionPortResult::PendingObservation { .. } => {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        ProjectionPortResult::WaitTimedOut { required, fence } => {
            match validate_projection_page_observation(
                &fence,
                schema.identity(),
                cursor_state
                    .as_deref()
                    .map(ProjectionCursorState::page_fence),
            ) {
                Ok(()) => {}
                Err(ProjectionObservationValidationError::Integrity) => {
                    let failure = lower_integrity_failure(&service, OPERATION);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
                Err(ProjectionObservationValidationError::InvalidCursor) => {
                    return Err(finish_failure(
                        &service,
                        &context,
                        &begun,
                        invalid_cursor_failure(),
                    )
                    .await);
                }
            }
            let current = fence.frontier();
            if request.required_sequence() != Some(required)
                || current >= riffdb_types::FrontierPosition::AppliedThrough(required)
            {
                let failure = lower_integrity_failure(&service, OPERATION);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
            QueryProjectionResult::WaitTimedOut { required, current }
        }
        ProjectionPortResult::Degraded { fence, reason } => {
            match validate_projection_state_observation(
                &fence,
                schema.identity(),
                cursor_state
                    .as_deref()
                    .map(ProjectionCursorState::page_fence),
            ) {
                Ok(()) => {}
                Err(ProjectionObservationValidationError::Integrity) => {
                    let failure = lower_integrity_failure(&service, OPERATION);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
                Err(ProjectionObservationValidationError::InvalidCursor) => {
                    return Err(finish_failure(
                        &service,
                        &context,
                        &begun,
                        invalid_cursor_failure(),
                    )
                    .await);
                }
            }
            if fence.generation().is_none()
                && !matches!(reason, crate::ProjectionUnavailableReason::Building)
            {
                let failure = lower_integrity_failure(&service, OPERATION);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
            let current = fence.frontier();
            QueryProjectionResult::Degraded { current, reason }
        }
        ProjectionPortResult::Invalid { fence, reason } => {
            match validate_projection_page_observation(
                &fence,
                schema.identity(),
                cursor_state
                    .as_deref()
                    .map(ProjectionCursorState::page_fence),
            ) {
                Ok(()) => {}
                Err(ProjectionObservationValidationError::Integrity) => {
                    let failure = lower_integrity_failure(&service, OPERATION);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
                Err(ProjectionObservationValidationError::InvalidCursor) => {
                    return Err(finish_failure(
                        &service,
                        &context,
                        &begun,
                        invalid_cursor_failure(),
                    )
                    .await);
                }
            }
            QueryProjectionResult::Invalid { reason }
        }
        ProjectionPortResult::ContinuationInvalidated => {
            let failure = if cursor_state.is_some() {
                invalid_cursor_failure()
            } else {
                lower_integrity_failure(&service, OPERATION)
            };
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    finish_success(&service, &context, &begun).await?;
    if let Some(guard) = result_cursor_guard {
        guard.publish();
    }
    Ok(result)
}

async fn get_projection_status(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: GetProjectionStatusRequest,
) -> ServiceResult<GetProjectionStatusResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::GetProjectionStatus;
    let bundle =
        prepare_selected_contract(&service, &context, request.contract(), OPERATION).await?;
    let contract = bundle.bundle();
    let lineage = contract.lineage().clone();
    let version = contract.contract_version();
    let policy_request =
        OperationRequest::get_projection_status(lineage.clone(), version, request.projection_id());
    let targets =
        ServiceAuditTargetMap::get_projection_status(lineage, version, request.projection_id())
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let begun = service
        .begin_invocation(&context, policy_request, targets, AuditScope::StandardRead)
        .await?;
    let Some(identity) = contract
        .bound_projection_group_schema(request.projection_id())
        .map(|schema| schema.identity().clone())
    else {
        if let Err(failure) = validate_public_metadata_authorization(
            &service,
            begun.initial_authorization(),
            OPERATION,
        ) {
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        let result = GetProjectionStatusResult::NotFound;
        if let Err(failure) = ensure_response_budget(&result) {
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        finish_success(&service, &context, &begun).await?;
        return Ok(result);
    };

    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .projection
            .reserve_projection_status(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => {
            return Err(finish_admission_failure(&service, &context, &begun, error).await);
        }
        Err(error) => {
            return Err(finish_controlled_wait(&service, &context, &begun, error).await);
        }
    };
    let authorization = begun.reauthorize(&service, &context).await?;
    if let Err(failure) =
        validate_public_metadata_authorization(&service, &authorization, OPERATION)
    {
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    let receipt = match permit.submit(identity.clone()) {
        Ok(receipt) => receipt,
        Err(error) => {
            return Err(finish_admission_failure(&service, &context, &begun, error).await);
        }
    };
    let snapshot = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(Some(snapshot)))) if snapshot.identity() == &identity => snapshot,
        Ok(Ok(Ok(Some(_)) | Ok(None))) => {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        Ok(Ok(Err(error))) => {
            let failure = projection_failure(&service, OPERATION, error);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        Ok(Err(PortDriverStopped)) => {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        Err(error) => {
            return Err(finish_controlled_wait(&service, &context, &begun, error).await);
        }
    };
    let return_authorization = begun.reauthorize(&service, &context).await?;
    if let Err(failure) =
        validate_public_metadata_authorization(&service, &return_authorization, OPERATION)
    {
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    let result = GetProjectionStatusResult::Found(snapshot);
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    finish_success(&service, &context, &begun).await?;
    Ok(result)
}

async fn discover_command_tools(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: DiscoverCommandToolsRequest,
) -> ServiceResult<DiscoverCommandToolsResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::DiscoverCommandTools;
    let page_request = request.page();
    let begun = service
        .begin_invocation(
            &context,
            OperationRequest::discover_command_tools(),
            ServiceAuditTargetMap::discover_command_tools(),
            AuditScope::StandardRead,
        )
        .await?;
    let (active, authorization) =
        read_current_discovery_catalog(&service, &context, &begun, OPERATION).await?;
    let operation_schemas = match OperationSchemaCatalog::accepted() {
        Ok(operation_schemas) => operation_schemas,
        Err(_) => {
            let failure = service.internal_failure(OPERATION, InternalDefect::LowerIntegrity);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let active_query_module = match active.as_ref() {
        Some(bundle) => {
            match read_active_query_module_for_discovery(
                &service,
                &context,
                bundle.clone(),
                OPERATION,
            )
            .await
            {
                Ok(module) => module,
                Err(failure) => {
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            }
        }
        None => None,
    };
    let fence = discovery_catalog_fence(
        active.as_ref(),
        active_query_module.as_ref(),
        operation_schemas.identity(),
    );
    if request.prior_fence() == Some(&fence) {
        drop(authorization);
        let result = match DiscoverCommandToolsResult::catalog_unchanged(&request, fence) {
            Ok(result) => result,
            Err(_) => {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        };
        if let Err(failure) = ensure_response_budget(&result) {
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        finish_success(&service, &context, &begun).await?;
        return Ok(result);
    }

    let mut command_entries = Vec::new();
    if let Some(bundle) = active.as_ref() {
        let contract = bundle.bundle();
        for command in contract.commands() {
            let name = match contract.mcp_command_names().get(command.command_id()) {
                Some(name) => name,
                None => {
                    let failure = lower_integrity_failure(&service, OPERATION);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            };
            let source_command = match crate::SourceName::new(name.source_command_name().to_owned())
            {
                Ok(source_command) => source_command,
                Err(_) => {
                    let failure = lower_integrity_failure(&service, OPERATION);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            };
            let input_schema = match schema_artifact(
                contract.schema_artifacts(),
                SchemaArtifactKey::CommandInput(command.command_id()),
            ) {
                Some(schema) => schema,
                None => {
                    let failure = lower_integrity_failure(&service, OPERATION);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            };
            let outcome_schema = match schema_artifact(
                contract.schema_artifacts(),
                SchemaArtifactKey::CommandOutcomeUnion(command.command_id()),
            ) {
                Some(schema) => schema,
                None => {
                    let failure = lower_integrity_failure(&service, OPERATION);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            };
            let descriptor = match CommandToolDescriptor::new(
                name.tool_name().clone(),
                source_command,
                contract.lineage().clone(),
                contract.contract_version(),
                command.command_id(),
                input_schema.clone(),
                outcome_schema.clone(),
            ) {
                Ok(descriptor) => descriptor,
                Err(_) => {
                    let failure = lower_integrity_failure(&service, OPERATION);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            };
            command_entries.push((
                CommandToolCandidate::new(bundle.lineage().clone(), command.command_id()),
                descriptor,
            ));
        }
    }
    command_entries.sort_unstable_by(|left, right| left.1.name().cmp(right.1.name()));
    let mut query_entries = Vec::new();
    if let Some(module) = active_query_module.as_ref() {
        let generated = match generate_mcp_tools(module.module()) {
            Ok(generated) => generated,
            Err(_) => {
                let failure = lower_integrity_failure(&service, OPERATION);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        };
        if generated.len() != module.module().queries().len() {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        for (query, generated) in module.module().queries().iter().zip(generated) {
            let query_name = match QueryOperationName::new(query.name().to_owned()) {
                Ok(query_name) => query_name,
                Err(_) => {
                    let failure = lower_integrity_failure(&service, OPERATION);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            };
            let input_schema = match NamedQueryToolSchemaArtifact::new(generated.input_schema) {
                Ok(schema) => schema,
                Err(_) => {
                    let failure = lower_integrity_failure(&service, OPERATION);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            };
            let result_schema = match NamedQueryToolSchemaArtifact::new(generated.result_schema) {
                Ok(schema) => schema,
                Err(_) => {
                    let failure = lower_integrity_failure(&service, OPERATION);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            };
            let descriptor = match NamedQueryToolDescriptor::new(
                generated.name,
                query_name.clone(),
                module.module().contract_lineage().clone(),
                module.module().contract_version(),
                module.module().name().clone(),
                module.module().version(),
                module.identity(),
                input_schema,
                result_schema,
            ) {
                Ok(descriptor) => descriptor,
                Err(_) => {
                    let failure = lower_integrity_failure(&service, OPERATION);
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            };
            query_entries.push((
                NamedQueryToolCandidate::new(
                    module.module().contract_lineage().clone(),
                    module.identity(),
                    query_name,
                ),
                descriptor,
            ));
        }
    }
    query_entries.sort_unstable_by(|left, right| left.1.name().cmp(right.1.name()));
    let command_candidates = command_entries
        .iter()
        .map(|(candidate, _)| candidate.clone())
        .collect::<Vec<_>>();
    let query_candidates = query_entries
        .iter()
        .map(|(candidate, _)| candidate.clone())
        .collect::<Vec<_>>();
    let current_visibility = filter_command_discovery_visibility(
        &service,
        &context,
        &begun,
        authorization,
        &command_candidates,
        &query_candidates,
    )
    .await?;
    let (_, completion) = begun.into_initial_authorization_and_completion();
    let shaped = (|| {
        let lookup =
            CommandDiscoveryCursorLookup::new(page_request.limit(), request.representation());
        let prior_state = match page_request.cursor() {
            Some(cursor) => match service.cursors.resolve_command_discovery(
                cursor,
                context.principal().principal_id(),
                &lookup,
            ) {
                Ok(state) if state.fence() == &fence => Some(state),
                Ok(_) | Err(CursorAccessError::InvalidCursor) => {
                    return Err(invalid_cursor_failure());
                }
                Err(CursorAccessError::Unavailable) => {
                    return Err(cursor_unavailable_failure(&service));
                }
            },
            None => None,
        };
        let effective_limit = prior_state
            .as_deref()
            .map_or(page_request.limit(), |prior| {
                page_request.limit().min(prior.effective_limit())
            });
        let mut catalog = FixedToolCandidate::ALL
            .iter()
            .copied()
            .map(FixedToolKind::from_policy)
            .map(CommandToolDiscoveryItem::Fixed)
            .collect::<Vec<_>>();
        let mut dynamic = command_entries
            .into_iter()
            .enumerate()
            .map(|(index, (_, descriptor))| {
                (
                    descriptor.name().as_str().to_owned(),
                    current_visibility.command_tools[index],
                    CommandToolDiscoveryItem::Command(Box::new(descriptor)),
                )
            })
            .chain(
                query_entries
                    .into_iter()
                    .enumerate()
                    .map(|(index, (_, descriptor))| {
                        (
                            descriptor.name().to_owned(),
                            current_visibility.named_query_tools[index],
                            CommandToolDiscoveryItem::NamedQuery(Box::new(descriptor)),
                        )
                    }),
            )
            .collect::<Vec<_>>();
        dynamic.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        if dynamic.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(service.internal_failure(OPERATION, InternalDefect::LowerIntegrity));
        }
        let mut combined_visibility = current_visibility.fixed_tools;
        combined_visibility.extend(dynamic.iter().map(|(_, visible, _)| *visible));
        let effective_visibility = constrain_command_discovery_visibility(
            combined_visibility,
            prior_state
                .as_deref()
                .map(CommandDiscoveryCursorState::visibility),
        )
        .ok_or_else(invalid_cursor_failure)?;
        catalog.extend(dynamic.into_iter().map(|(_, _, item)| item));
        if catalog.len() != effective_visibility.len() {
            return Err(service.internal_failure(OPERATION, InternalDefect::ProofMismatch));
        }
        let start = prior_state.as_deref().map_or(Ok(0), |state| {
            state
                .after_candidate()
                .checked_add(1)
                .ok_or_else(invalid_cursor_failure)
        })?;
        if start > catalog.len() {
            return Err(invalid_cursor_failure());
        }
        let mut raw_indices = Vec::new();
        let mut items = Vec::new();
        for (raw_index, item) in catalog.into_iter().enumerate().skip(start) {
            if effective_visibility[raw_index] {
                raw_indices.push(raw_index);
                items.push(item);
            }
        }
        let more_due_to_limit = items.len() > usize::from(effective_limit.get().get());
        items.truncate(usize::from(effective_limit.get().get()));
        raw_indices.truncate(items.len());
        let compact_items =
            (request.representation() == DiscoveryRepresentation::CompactObservation).then(|| {
                items
                    .iter()
                    .map(CommandToolDiscoveryItem::compact)
                    .collect::<Vec<_>>()
            });
        let fit = match compact_items.as_ref() {
            Some(items) => fit_page_items(items, &fence, more_due_to_limit)?,
            None => fit_full_command_discovery_page_items(
                &items,
                &fence,
                &operation_schemas,
                more_due_to_limit,
            )?,
        };
        let continuation_after = if fit.has_more() {
            Some(
                raw_indices
                    .get(fit.item_count() - 1)
                    .copied()
                    .ok_or_else(|| {
                        service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
                    })?,
            )
        } else {
            None
        };
        items.truncate(fit.item_count());
        let compact_items = compact_items.map(|mut items| {
            items.truncate(fit.item_count());
            items
        });
        let cursor_guard = match continuation_after {
            Some(after_candidate) => {
                let state = CommandDiscoveryCursorState::new(
                    after_candidate,
                    fence.clone(),
                    effective_visibility,
                    effective_limit,
                )
                .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
                Some(
                    service
                        .cursors
                        .register_command_discovery_unpublished(
                            context.principal().principal_id(),
                            lookup,
                            state,
                        )
                        .map_err(|_| cursor_unavailable_failure(&service))?,
                )
            }
            None => None,
        };
        let next_cursor = cursor_guard
            .as_ref()
            .map(crate::CursorPublicationGuard::token);
        let result = match compact_items {
            Some(items) => {
                let page = Page::new(effective_limit, items, next_cursor, fence).map_err(|_| {
                    service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
                })?;
                DiscoverCommandToolsResult::compact_page(&request, page)
            }
            None => {
                let page = Page::new(effective_limit, items, next_cursor, fence).map_err(|_| {
                    service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
                })?;
                DiscoverCommandToolsResult::page(&request, page, operation_schemas)
            }
        }
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
        ensure_response_budget(&result)?;
        Ok((result, cursor_guard))
    })();
    let (result, cursor_guard) =
        finish_discovery_result(&service, &context, &completion, shaped).await?;
    if let Some(guard) = cursor_guard {
        guard.publish();
    }
    Ok(result)
}

async fn discover_resources(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: DiscoverResourcesRequest,
) -> ServiceResult<DiscoverResourcesResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::DiscoverResources;
    let page_request = request.page();
    let begun = service
        .begin_invocation(
            &context,
            OperationRequest::discover_resources(),
            ServiceAuditTargetMap::discover_resources(),
            AuditScope::StandardRead,
        )
        .await?;
    let (active, authorization) =
        read_current_discovery_catalog(&service, &context, &begun, OPERATION).await?;
    let operation_schemas = match OperationSchemaCatalog::accepted() {
        Ok(operation_schemas) => operation_schemas,
        Err(_) => {
            let failure = service.internal_failure(OPERATION, InternalDefect::LowerIntegrity);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let active_query_module = match active.as_ref() {
        Some(bundle) => {
            match read_active_query_module_for_discovery(
                &service,
                &context,
                bundle.clone(),
                OPERATION,
            )
            .await
            {
                Ok(module) => module,
                Err(failure) => {
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
            }
        }
        None => None,
    };
    let fence = discovery_catalog_fence(
        active.as_ref(),
        active_query_module.as_ref(),
        operation_schemas.identity(),
    );
    if request.prior_fence() == Some(&fence) {
        drop(authorization);
        let result = match DiscoverResourcesResult::catalog_unchanged(&request, fence) {
            Ok(result) => result,
            Err(_) => {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        };
        if let Err(failure) = ensure_response_budget(&result) {
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        finish_success(&service, &context, &begun).await?;
        return Ok(result);
    }

    let mut candidates = Vec::new();
    push_resource(
        &mut candidates,
        DiscoveryResource::ActiveContract,
        ResourceDescriptor::active_contract(),
    );
    if let Some(bundle) = active.as_ref()
        && let Err(failure) =
            append_contract_resources(&service, OPERATION, bundle, &mut candidates)
    {
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    push_resource(
        &mut candidates,
        DiscoveryResource::Commit,
        ResourceDescriptor::commit_class(),
    );
    push_resource(
        &mut candidates,
        DiscoveryResource::Provenance,
        ResourceDescriptor::provenance_class(),
    );
    push_resource(
        &mut candidates,
        DiscoveryResource::Health,
        ResourceDescriptor::server_health(),
    );
    push_resource(
        &mut candidates,
        DiscoveryResource::ReactiveWakeup,
        ResourceDescriptor::reactive_wakeup(),
    );
    candidates.retain(|candidate| candidate.descriptor.matches_discovery_kind(request.kind()));
    candidates.sort_unstable_by_key(|candidate| candidate.descriptor.canonical_identity_key());
    let current_visibility = filter_resource_discovery_visibility(
        &service,
        &context,
        &begun,
        authorization,
        active.as_ref(),
        &candidates,
    )
    .await?;
    let (_, completion) = begun.into_initial_authorization_and_completion();
    let shaped = (|| {
        let lookup = ResourceDiscoveryCursorLookup::new(
            page_request.limit(),
            request.representation(),
            request.kind(),
        );
        let prior_state = match page_request.cursor() {
            Some(cursor) => match service.cursors.resolve_resource_discovery(
                cursor,
                context.principal().principal_id(),
                &lookup,
            ) {
                Ok(state) if state.fence() == &fence => Some(state),
                Ok(_) | Err(CursorAccessError::InvalidCursor) => {
                    return Err(invalid_cursor_failure());
                }
                Err(CursorAccessError::Unavailable) => {
                    return Err(cursor_unavailable_failure(&service));
                }
            },
            None => None,
        };
        let effective_visibility = constrain_resource_discovery_visibility(
            current_visibility,
            prior_state
                .as_deref()
                .map(ResourceDiscoveryCursorState::visibility),
        )
        .ok_or_else(invalid_cursor_failure)?;
        let effective_limit = prior_state
            .as_deref()
            .map_or(page_request.limit(), |prior| {
                page_request.limit().min(prior.effective_limit())
            });
        if candidates.len() != effective_visibility.len() {
            return Err(service.internal_failure(OPERATION, InternalDefect::ProofMismatch));
        }
        let start = prior_state.as_deref().map_or(Ok(0), |state| {
            state
                .after_candidate()
                .checked_add(1)
                .ok_or_else(invalid_cursor_failure)
        })?;
        if start > candidates.len() {
            return Err(invalid_cursor_failure());
        }
        let mut raw_indices = Vec::new();
        let mut resources = Vec::new();
        for (raw_index, candidate) in candidates.iter().enumerate().skip(start) {
            let descriptor = match &effective_visibility[raw_index] {
                ResourceDiscoveryCursorVisibility::Hidden => continue,
                ResourceDiscoveryCursorVisibility::Visible => candidate.descriptor.clone(),
                ResourceDiscoveryCursorVisibility::VisibleEntityFields(fields) => {
                    let bundle = active.as_ref().ok_or_else(|| {
                        service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
                    })?;
                    let entity = candidate.entity_schema.as_ref().ok_or_else(|| {
                        service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
                    })?;
                    let artifact =
                        filtered_entity_schema_artifact(entity, bundle.bundle().schema(), fields)
                            .map_err(|()| {
                            service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
                        })?;
                    ResourceDescriptor::entity_schema(
                        bundle.lineage().clone(),
                        entity.id(),
                        artifact,
                    )
                    .map_err(|_| {
                        service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
                    })?
                }
            };
            raw_indices.push(raw_index);
            resources.push(descriptor);
        }
        let more_due_to_limit = resources.len() > usize::from(effective_limit.get().get());
        resources.truncate(usize::from(effective_limit.get().get()));
        raw_indices.truncate(resources.len());
        let compact_resources =
            (request.representation() == DiscoveryRepresentation::CompactObservation).then(|| {
                resources
                    .iter()
                    .map(ResourceDescriptor::compact)
                    .collect::<Vec<_>>()
            });
        let fit = match compact_resources.as_ref() {
            Some(resources) => fit_page_items(resources, &fence, more_due_to_limit)?,
            None => fit_full_resource_discovery_page_items(&resources, &fence, more_due_to_limit)?,
        };
        let continuation_after = if fit.has_more() {
            Some(
                raw_indices
                    .get(fit.item_count() - 1)
                    .copied()
                    .ok_or_else(|| {
                        service.internal_failure(OPERATION, InternalDefect::ProofMismatch)
                    })?,
            )
        } else {
            None
        };
        resources.truncate(fit.item_count());
        let compact_resources = compact_resources.map(|mut resources| {
            resources.truncate(fit.item_count());
            resources
        });
        let cursor_guard = match continuation_after {
            Some(after_candidate) => {
                let state = ResourceDiscoveryCursorState::new(
                    after_candidate,
                    fence.clone(),
                    effective_visibility,
                    effective_limit,
                )
                .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
                Some(
                    service
                        .cursors
                        .register_resource_discovery_unpublished(
                            context.principal().principal_id(),
                            lookup,
                            state,
                        )
                        .map_err(|_| cursor_unavailable_failure(&service))?,
                )
            }
            None => None,
        };
        let next_cursor = cursor_guard
            .as_ref()
            .map(crate::CursorPublicationGuard::token);
        let result =
            match compact_resources {
                Some(resources) => {
                    let page = Page::new(effective_limit, resources, next_cursor, fence).map_err(
                        |_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch),
                    )?;
                    DiscoverResourcesResult::compact_page(&request, page)
                }
                None => {
                    let page = Page::new(effective_limit, resources, next_cursor, fence).map_err(
                        |_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch),
                    )?;
                    DiscoverResourcesResult::page(&request, page)
                }
            }
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
        ensure_response_budget(&result)?;
        Ok((result, cursor_guard))
    })();
    let (result, cursor_guard) =
        finish_discovery_result(&service, &context, &completion, shaped).await?;
    if let Some(guard) = cursor_guard {
        guard.publish();
    }
    Ok(result)
}

#[derive(Clone)]
struct ResourceCandidate {
    policy_candidate: DiscoveryResource,
    descriptor: ResourceDescriptor,
    entity_schema: Option<EntitySchema>,
}

async fn read_current_discovery_catalog(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    operation: ServiceOperationV1,
) -> ServiceResult<(Option<ValidatedContractBundle>, Box<AuthorizedOperation>)> {
    if !valid_discovery_authorization(service, begun.initial_authorization(), operation) {
        let failure = service.internal_failure(operation, InternalDefect::ProofMismatch);
        return Err(finish_failure(service, context, begun, failure).await);
    }
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
            return Err(finish_admission_failure(service, context, begun, error).await);
        }
        Err(error) => {
            return Err(finish_controlled_wait(service, context, begun, error).await);
        }
    };
    let authorization = begun.reauthorize(service, context).await?;
    if !valid_discovery_authorization(service, &authorization, operation) {
        let failure = service.internal_failure(operation, InternalDefect::ProofMismatch);
        return Err(finish_failure(service, context, begun, failure).await);
    }
    let receipt = match permit.submit(()) {
        Ok(receipt) => receipt,
        Err(error) => {
            return Err(finish_admission_failure(service, context, begun, error).await);
        }
    };
    let active = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(active))) => active.map(|snapshot| snapshot.bundle().clone()),
        Ok(Ok(Err(error))) => {
            let failure = catalog_failure(service, operation, error);
            return Err(finish_failure(service, context, begun, failure).await);
        }
        Ok(Err(PortDriverStopped)) => {
            let failure = lower_integrity_failure(service, operation);
            return Err(finish_failure(service, context, begun, failure).await);
        }
        Err(error) => {
            return Err(finish_controlled_wait(service, context, begun, error).await);
        }
    };
    Ok((active, authorization))
}

struct ToolDiscoveryVisibility {
    fixed_tools: Vec<bool>,
    command_tools: Vec<bool>,
    named_query_tools: Vec<bool>,
}

async fn filter_command_discovery_visibility(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    first_authorization: Box<AuthorizedOperation>,
    command_candidates: &[CommandToolCandidate],
    named_query_candidates: &[NamedQueryToolCandidate],
) -> ServiceResult<ToolDiscoveryVisibility> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::DiscoverCommandTools;
    let mut first_authorization = Some(first_authorization);
    let mut fixed_visibility = None;
    let mut command_visibility = Vec::with_capacity(command_candidates.len());
    let mut named_query_visibility = Vec::with_capacity(named_query_candidates.len());
    let mut command_start = 0usize;
    let mut query_start = 0usize;
    loop {
        let command_end = (command_start + MAX_DISCOVERY_PAGE_ITEMS).min(command_candidates.len());
        let remaining = MAX_DISCOVERY_PAGE_ITEMS - (command_end - command_start);
        let query_end = (query_start + remaining).min(named_query_candidates.len());
        let command_batch = &command_candidates[command_start..command_end];
        let query_batch = &named_query_candidates[query_start..query_end];
        let authorization = match first_authorization.take() {
            Some(authorization) => authorization,
            None => begun.reauthorize(service, context).await?,
        };
        if !valid_discovery_authorization(service, &authorization, OPERATION) {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(service, context, begun, failure).await);
        }
        let discovery = match authorization.into_discovery() {
            Ok(discovery) => discovery,
            Err(_) => {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_failure(service, context, begun, failure).await);
            }
        };
        let visibility = match discovery.tool_catalog(
            FixedToolCandidate::ALL.as_slice(),
            command_batch,
            query_batch,
        ) {
            Ok(visibility) => visibility,
            Err(_) => {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_failure(service, context, begun, failure).await);
            }
        };
        if visibility.fixed_tools().len() != FixedToolCandidate::ALL.len()
            || visibility.command_tools().len() != command_batch.len()
            || visibility.named_query_tools().len() != query_batch.len()
        {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(service, context, begun, failure).await);
        }
        let batch_fixed_visibility = visibility
            .fixed_tools()
            .iter()
            .map(|visibility| *visibility == DiscoveryVisibility::Visible)
            .collect::<Vec<_>>();
        if fixed_visibility
            .as_ref()
            .is_some_and(|prior| prior != &batch_fixed_visibility)
        {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(service, context, begun, failure).await);
        }
        fixed_visibility.get_or_insert(batch_fixed_visibility);
        command_visibility.extend(
            visibility
                .command_tools()
                .iter()
                .map(|visibility| *visibility == DiscoveryVisibility::Visible),
        );
        named_query_visibility.extend(
            visibility
                .named_query_tools()
                .iter()
                .map(|visibility| *visibility == DiscoveryVisibility::Visible),
        );
        command_start = command_end;
        query_start = query_end;
        if command_start == command_candidates.len() && query_start == named_query_candidates.len()
        {
            break;
        }
    }
    let Some(fixed_tools) = fixed_visibility else {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(service, context, begun, failure).await);
    };
    Ok(ToolDiscoveryVisibility {
        fixed_tools,
        command_tools: command_visibility,
        named_query_tools: named_query_visibility,
    })
}

async fn filter_resource_discovery_visibility(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    first_authorization: Box<AuthorizedOperation>,
    active: Option<&ValidatedContractBundle>,
    candidates: &[ResourceCandidate],
) -> ServiceResult<Vec<ResourceDiscoveryCursorVisibility>> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::DiscoverResources;
    let mut first_authorization = Some(first_authorization);
    let mut visibility = Vec::with_capacity(candidates.len());
    let mut start = 0;
    loop {
        let Some(end) = resource_discovery_batch_end(candidates, start) else {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(service, context, begun, failure).await);
        };
        let batch = &candidates[start..end];
        let authorization = match first_authorization.take() {
            Some(authorization) => authorization,
            None => begun.reauthorize(service, context).await?,
        };
        if !valid_discovery_authorization(service, &authorization, OPERATION) {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(service, context, begun, failure).await);
        }
        let discovery = match authorization.into_discovery() {
            Ok(discovery) => discovery,
            Err(_) => {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_failure(service, context, begun, failure).await);
            }
        };
        let policy_candidates = batch
            .iter()
            .map(|candidate| candidate.policy_candidate.clone())
            .collect::<Vec<_>>();
        let batch_visibility = match discovery.resource_catalog(&policy_candidates) {
            Ok(visibility) if visibility.len() == batch.len() => visibility,
            Ok(_) | Err(_) => {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_failure(service, context, begun, failure).await);
            }
        };
        visibility.extend(batch_visibility);
        if end == candidates.len() {
            break;
        }
        start = end;
    }
    match normalize_resource_discovery_visibility(active, candidates, visibility) {
        Ok(visibility) => Ok(visibility),
        Err(()) => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            Err(finish_failure(service, context, begun, failure).await)
        }
    }
}

fn resource_discovery_batch_end(candidates: &[ResourceCandidate], start: usize) -> Option<usize> {
    if start > candidates.len() {
        return None;
    }
    let mut end = start;
    let mut candidate_fields = 0usize;
    while end < candidates.len() && end - start < MAX_DISCOVERY_PAGE_ITEMS {
        let fields = match &candidates[end].policy_candidate {
            DiscoveryResource::EntitySchema(candidate) => candidate.non_key_fields().len(),
            _ => 0,
        };
        let next_fields = candidate_fields.checked_add(fields)?;
        if next_fields > MAX_CAPABILITY_FIELD_VISIBILITY {
            break;
        }
        candidate_fields = next_fields;
        end += 1;
    }
    (start == candidates.len() || end != start).then_some(end)
}

fn push_resource(
    candidates: &mut Vec<ResourceCandidate>,
    candidate: DiscoveryResource,
    descriptor: ResourceDescriptor,
) {
    candidates.push(ResourceCandidate {
        policy_candidate: candidate,
        descriptor,
        entity_schema: None,
    });
}

fn append_contract_resources(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    bundle: &ValidatedContractBundle,
    candidates: &mut Vec<ResourceCandidate>,
) -> ServiceResult<()> {
    let contract = bundle.bundle();
    let lineage = contract.lineage().clone();
    push_resource(
        candidates,
        DiscoveryResource::ContractVersion {
            lineage: lineage.clone(),
            version: contract.contract_version(),
        },
        ResourceDescriptor::contract_version(lineage.clone(), contract.contract_version()),
    );
    for entity in contract.schema().entities() {
        // ADR-0118: secret-classified fields stay out of the ordinary
        // candidate list; discovery classifies them against the grant's
        // dedicated secret naming.
        let entity_secret_fields = contract.schema().secret_fields_for_entity(entity.id());
        let non_key_fields = entity
            .record()
            .fields()
            .iter()
            .map(|field| field.id())
            .filter(|field| !entity.primary_key_fields().contains(field))
            .filter(|field| entity_secret_fields.binary_search(field).is_err())
            .collect::<Vec<_>>();
        let Ok(policy_candidate) =
            EntitySchemaCandidate::new(lineage.clone(), entity.id(), non_key_fields.clone())
                .and_then(|candidate| {
                    candidate.with_secret_classified_fields(entity_secret_fields)
                })
        else {
            // A legal entity can exceed the discovery-policy field candidate
            // bound. Omitting that one schema is safer than treating the
            // contract itself as corrupt or exposing an unfiltered artifact.
            continue;
        };
        let artifact = schema_artifact(
            contract.schema_artifacts(),
            SchemaArtifactKey::Entity(entity.id()),
        )
        .ok_or_else(|| lower_integrity_failure(service, operation))?;
        let descriptor =
            ResourceDescriptor::entity_schema(lineage.clone(), entity.id(), artifact.clone())
                .map_err(|_| lower_integrity_failure(service, operation))?;
        candidates.push(ResourceCandidate {
            policy_candidate: DiscoveryResource::EntitySchema(policy_candidate),
            descriptor,
            entity_schema: Some(entity.clone()),
        });
    }
    for command in contract.commands() {
        let name = contract
            .mcp_command_names()
            .get(command.command_id())
            .ok_or_else(|| lower_integrity_failure(service, operation))?;
        let source_command = crate::SourceName::new(name.source_command_name().to_owned())
            .map_err(|_| lower_integrity_failure(service, operation))?;
        let outcome_descriptor = ResourceDescriptor::command_outcome(
            lineage.clone(),
            command.command_id(),
            name.tool_name().clone(),
        )
        .map_err(|_| lower_integrity_failure(service, operation))?;
        for (candidate, descriptor) in [
            (
                DiscoveryResource::CommandPlan {
                    lineage: lineage.clone(),
                    command_id: command.command_id(),
                },
                ResourceDescriptor::command_plan(
                    lineage.clone(),
                    contract.contract_version(),
                    command.command_id(),
                    source_command.clone(),
                ),
            ),
            (
                DiscoveryResource::CommandDocumentation {
                    lineage: lineage.clone(),
                    command_id: command.command_id(),
                },
                ResourceDescriptor::command_documentation(
                    lineage.clone(),
                    contract.contract_version(),
                    command.command_id(),
                    source_command.clone(),
                ),
            ),
            (
                DiscoveryResource::CommandOutcome {
                    lineage: lineage.clone(),
                    command_id: command.command_id(),
                },
                outcome_descriptor,
            ),
        ] {
            push_resource(candidates, candidate, descriptor);
        }
    }
    for projection in contract.projections() {
        push_resource(
            candidates,
            DiscoveryResource::ProjectionStatus {
                lineage: lineage.clone(),
                projection_id: projection.projection_id(),
            },
            ResourceDescriptor::projection_status(lineage.clone(), projection.projection_id()),
        );
    }
    Ok(())
}

fn discovery_catalog_fence(
    active: Option<&ValidatedContractBundle>,
    active_query_module: Option<&ValidatedQueryModule>,
    operation_schemas: crate::OperationSchemaCatalogIdentity,
) -> DiscoveryCatalogFence {
    match active {
        None => DiscoveryCatalogFence::no_active_contract(operation_schemas),
        Some(bundle) => DiscoveryCatalogFence::active_contract_with_query_module(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            active_query_module.map(ValidatedQueryModule::identity),
            operation_schemas,
        ),
    }
}

pub(crate) async fn read_active_query_module_for_discovery(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    contract: ValidatedContractBundle,
    operation: ServiceOperationV1,
) -> ServiceResult<Option<ValidatedQueryModule>> {
    let Some(modules) = service.providers.query_modules.as_ref() else {
        return Ok(None);
    };
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        modules.prepare_active_query_module(context.control(), contract),
    )
    .await
    {
        Ok(Ok(module)) => Ok(module),
        Ok(Err(crate::QueryModuleReadError::Unavailable)) => {
            Err(PublicError::storage_unavailable().into())
        }
        Ok(Err(crate::QueryModuleReadError::Integrity)) => {
            Err(service.internal_failure(operation, InternalDefect::LowerIntegrity))
        }
        Err(ControlledWaitError::Cancelled) => Err(ServiceFailure::Cancelled),
        Err(ControlledWaitError::DeadlineExceeded) => Err(ServiceFailure::DeadlineExceeded),
    }
}

fn constrain_command_discovery_visibility(
    current: Vec<bool>,
    prior: Option<&[bool]>,
) -> Option<Vec<bool>> {
    let Some(prior) = prior else {
        return Some(current);
    };
    if current.len() != prior.len() {
        return None;
    }
    Some(
        current
            .into_iter()
            .zip(prior)
            .map(|(current, prior)| current && *prior)
            .collect(),
    )
}

fn normalize_resource_discovery_visibility(
    active: Option<&ValidatedContractBundle>,
    candidates: &[ResourceCandidate],
    visibility: Vec<ResourceDiscoveryVisibility>,
) -> Result<Vec<ResourceDiscoveryCursorVisibility>, ()> {
    if candidates.len() != visibility.len() {
        return Err(());
    }
    candidates
        .iter()
        .zip(visibility)
        .map(
            |(candidate, visibility)| match (visibility, candidate.entity_schema.as_ref()) {
                (ResourceDiscoveryVisibility::Hidden, _) => {
                    Ok(ResourceDiscoveryCursorVisibility::Hidden)
                }
                (ResourceDiscoveryVisibility::Visible { field_mask: None }, None) => {
                    Ok(ResourceDiscoveryCursorVisibility::Visible)
                }
                (
                    ResourceDiscoveryVisibility::Visible {
                        field_mask: Some(mask),
                    },
                    Some(entity),
                ) => {
                    let bundle = active.ok_or(())?;
                    if mask.lineage() != bundle.lineage()
                        || mask.entity_type_id() != entity.id()
                        || filtered_entity_schema_artifact(
                            entity,
                            bundle.bundle().schema(),
                            mask.fields(),
                        )
                        .is_err()
                    {
                        return Err(());
                    }
                    Ok(ResourceDiscoveryCursorVisibility::VisibleEntityFields(
                        mask.fields().to_vec(),
                    ))
                }
                (ResourceDiscoveryVisibility::Visible { .. }, _) => Err(()),
            },
        )
        .collect()
}

fn constrain_resource_discovery_visibility(
    current: Vec<ResourceDiscoveryCursorVisibility>,
    prior: Option<&[ResourceDiscoveryCursorVisibility]>,
) -> Option<Vec<ResourceDiscoveryCursorVisibility>> {
    let Some(prior) = prior else {
        return Some(current);
    };
    if current.len() != prior.len() {
        return None;
    }
    current
        .into_iter()
        .zip(prior)
        .map(|(current, prior)| match (current, prior) {
            (ResourceDiscoveryCursorVisibility::Hidden, _)
            | (_, ResourceDiscoveryCursorVisibility::Hidden) => {
                Some(ResourceDiscoveryCursorVisibility::Hidden)
            }
            (
                ResourceDiscoveryCursorVisibility::Visible,
                ResourceDiscoveryCursorVisibility::Visible,
            ) => Some(ResourceDiscoveryCursorVisibility::Visible),
            (
                ResourceDiscoveryCursorVisibility::VisibleEntityFields(current),
                ResourceDiscoveryCursorVisibility::VisibleEntityFields(prior),
            ) => Some(ResourceDiscoveryCursorVisibility::VisibleEntityFields(
                intersect_sorted_fields(&current, prior),
            )),
            _ => None,
        })
        .collect()
}

fn intersect_sorted_fields(current: &[FieldId], prior: &[FieldId]) -> Vec<FieldId> {
    let mut current_index = 0;
    let mut prior_index = 0;
    let mut intersection = Vec::new();
    while current_index < current.len() && prior_index < prior.len() {
        match current[current_index].cmp(&prior[prior_index]) {
            std::cmp::Ordering::Less => current_index += 1,
            std::cmp::Ordering::Greater => prior_index += 1,
            std::cmp::Ordering::Equal => {
                intersection.push(current[current_index]);
                current_index += 1;
                prior_index += 1;
            }
        }
    }
    intersection
}

fn filtered_entity_schema_artifact(
    entity: &EntitySchema,
    schema: &riffdb_contract_ir::SchemaIr,
    visible_non_key_fields: &[FieldId],
) -> Result<GeneratedSchemaArtifact, ()> {
    if visible_non_key_fields
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
        || visible_non_key_fields.iter().any(|field_id| {
            entity.record().field(*field_id).is_none()
                || entity.primary_key_fields().contains(field_id)
        })
    {
        return Err(());
    }
    let fields = entity
        .record()
        .fields()
        .iter()
        .filter(|field| {
            entity.primary_key_fields().contains(&field.id())
                || visible_non_key_fields.binary_search(&field.id()).is_ok()
        })
        .cloned()
        .collect();
    let record = RecordSchema::new(entity.record().owner().clone(), fields).map_err(|_| ())?;
    GeneratedSchemaArtifact::entity(entity.id(), &record, schema).map_err(|_| ())
}

pub(crate) async fn prepare_selected_contract(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    selection: &ContractSelection,
    operation: ServiceOperationV1,
) -> ServiceResult<ValidatedContractBundle> {
    match selection {
        ContractSelection::Active => prepare_active_contract(service, context, operation)
            .await?
            .ok_or_else(|| PublicError::storage_unavailable().into()),
        ContractSelection::Exact { lineage, version } => {
            let result = wait_with_control(
                context.control(),
                service.providers.deadline_scheduler.as_ref(),
                service.providers.catalog.prepare_contract_version(
                    context.control(),
                    lineage.clone(),
                    *version,
                ),
            )
            .await
            .map_err(controlled_wait_failure)?
            .map_err(|error| catalog_failure(service, operation, error))?;
            let Some(bundle) = result else {
                return Err(PublicError::contract_mismatch(*version).into());
            };
            if bundle.lineage() != lineage || bundle.contract_version() != *version {
                return Err(lower_integrity_failure(service, operation));
            }
            Ok(bundle)
        }
    }
}

async fn prepare_active_contract(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    operation: ServiceOperationV1,
) -> ServiceResult<Option<ValidatedContractBundle>> {
    let active = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .catalog
            .prepare_active_catalog(context.control()),
    )
    .await
    .map_err(controlled_wait_failure)?
    .map_err(|error| catalog_failure(service, operation, error))?;
    Ok(active.map(|snapshot| snapshot.bundle().clone()))
}

fn find_index(
    schema: &riffdb_contract_ir::SchemaIr,
    index_id: riffdb_types::IndexId,
) -> Option<(EntitySchema, IndexSchema)> {
    schema.entities().iter().find_map(|entity| {
        entity
            .indexes()
            .iter()
            .find(|index| index.id() == index_id)
            .map(|index| (entity.clone(), index.clone()))
    })
}

fn validate_field_selection(entity: &EntitySchema, fields: &[FieldId]) -> ServiceResult<()> {
    for field in fields {
        if entity.record().field(*field).is_none()
            || entity.primary_key_fields().binary_search(field).is_ok()
        {
            return Err(validation_failure(ValidationCode::UnknownField));
        }
    }
    Ok(())
}

enum IndexRowValidationError {
    Controlled(ControlledWaitError),
    Catalog(CatalogError),
    Integrity,
}

#[allow(clippy::too_many_arguments)]
async fn validate_authoritative_index_rows(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    selected_bundle: &ValidatedContractBundle,
    target_lineage: &ContractLineage,
    expected_entity: riffdb_types::EntityTypeId,
    index_id: riffdb_types::IndexId,
    read_constraint: &PartitionConstraint,
    page: &AuthoritativeIndexPage,
) -> Result<Vec<PartitionKey>, IndexRowValidationError> {
    if !matches!(read_constraint, PartitionConstraint::Filter(_)) {
        return Err(IndexRowValidationError::Integrity);
    }

    let selected_binding = AuthoritativeSchemaBinding::new(
        selected_bundle.lineage().clone(),
        selected_bundle.contract_version(),
        selected_bundle.bundle_hash(),
    );
    let mut bundles = BTreeMap::new();
    bundles.insert(selected_binding, selected_bundle.clone());
    let mut partitions = Vec::with_capacity(page.rows().len());

    for row in page.rows() {
        let binding = row.schema_binding();
        if binding.lineage() != target_lineage {
            return Err(IndexRowValidationError::Integrity);
        }
        if !bundles.contains_key(binding) {
            let observed = wait_with_control(
                context.control(),
                service.providers.deadline_scheduler.as_ref(),
                service.providers.catalog.prepare_contract_version(
                    context.control(),
                    binding.lineage().clone(),
                    binding.contract_version(),
                ),
            )
            .await
            .map_err(IndexRowValidationError::Controlled)?
            .map_err(IndexRowValidationError::Catalog)?;
            let historical = observed.ok_or(IndexRowValidationError::Integrity)?;
            if historical.lineage() != binding.lineage()
                || historical.contract_version() != binding.contract_version()
                || historical.bundle_hash() != binding.bundle_hash()
            {
                return Err(IndexRowValidationError::Integrity);
            }
            bundles.insert(binding.clone(), historical);
        }

        let historical = bundles
            .get(binding)
            .ok_or(IndexRowValidationError::Integrity)?;
        let (entity, index) = find_index(historical.bundle().schema(), index_id)
            .ok_or(IndexRowValidationError::Integrity)?;
        if entity.id() != expected_entity {
            return Err(IndexRowValidationError::Integrity);
        }
        let decoded = index
            .key_schema()
            .decode_index(row.key())
            .map_err(|_| IndexRowValidationError::Integrity)?;
        let partition =
            derive_entity_partition(historical.bundle().schema(), &entity, decoded.entity_key())
                .map_err(|_| IndexRowValidationError::Integrity)?;
        if &partition != row.stored_partition()
            || !partition_allowed(read_constraint, target_lineage, &partition)
        {
            return Err(IndexRowValidationError::Integrity);
        }
        partitions.push(partition);
    }
    Ok(partitions)
}

enum PreparationError {
    Invalid,
    Arithmetic,
    Integrity,
}

fn derive_entity_partition(
    schema: &riffdb_contract_ir::SchemaIr,
    entity: &EntitySchema,
    key: &EntityKey,
) -> Result<PartitionKey, PreparationError> {
    let key_values = entity
        .primary_key()
        .decode_entity(key)
        .map_err(|_| PreparationError::Invalid)?;
    let aggregate = schema
        .aggregate_for_entity(entity.id())
        .ok_or(PreparationError::Integrity)?;
    let root = schema
        .entity(aggregate.root())
        .ok_or(PreparationError::Integrity)?;
    if key_values.len() < root.primary_key_fields().len() {
        return Err(PreparationError::Integrity);
    }
    let values = RootKeyValues {
        entity_type: root.id(),
        fields: root.primary_key_fields(),
        values: &key_values[..root.primary_key_fields().len()],
    };
    let component = evaluate_expression(
        aggregate.keys().expressions(),
        aggregate.keys().partition_expression(),
        &values,
    )
    .map_err(|error| match error {
        EvaluationError::Arithmetic => PreparationError::Arithmetic,
        EvaluationError::Integrity => PreparationError::Integrity,
    })?;
    aggregate
        .keys()
        .partition_schema()
        .encode_partition(&[component])
        .map_err(|_| PreparationError::Integrity)
}

struct RootKeyValues<'a> {
    entity_type: riffdb_types::EntityTypeId,
    fields: &'a [FieldId],
    values: &'a [CanonicalValue],
}

impl ExpressionValueSource for RootKeyValues<'_> {
    fn schema_field(
        &self,
        entity_type: riffdb_types::EntityTypeId,
        field: FieldId,
    ) -> Option<CanonicalValue> {
        if entity_type != self.entity_type {
            return None;
        }
        self.fields
            .iter()
            .position(|candidate| *candidate == field)
            .and_then(|position| self.values.get(position))
            .cloned()
    }
}

fn preparation_failure(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: PreparationError,
) -> ServiceFailure {
    match error {
        PreparationError::Invalid => validation_failure(ValidationCode::InvalidValue),
        PreparationError::Arithmetic => validation_failure(ValidationCode::OutOfRange),
        PreparationError::Integrity => {
            service.internal_failure(operation, InternalDefect::ProofMismatch)
        }
    }
}

fn entity_authorization(
    service: &RiffDbServiceInner,
    authorization: &AuthorizedOperation,
    lineage: &ContractLineage,
    entity: &EntitySchema,
    partition: &PartitionKey,
    requested_fields: &[FieldId],
) -> Option<Vec<FieldId>> {
    let obligations = authorization.obligations();
    let expected_partition = ScopedPartitionV1::new(lineage.clone(), partition.clone());
    let mask = obligations.field_mask()?;
    if authorization.database_id() != service.identity.database_id()
        || authorization.environment() != service.identity.environment()
        || authorization.operation() != ServiceOperationV1::GetEntity
        || obligations.effective_tenant_scope() != &TenantScope::Global
        || obligations.partition_constraint()
            != Some(&PartitionConstraint::Exact(expected_partition))
        || obligations.row_limit().is_some()
        || obligations.output_classification()
            != OutputClassification::PolicyFilteredApplicationData
        || mask.lineage() != lineage
        || mask.entity_type_id() != entity.id()
        || !mask
            .fields()
            .iter()
            .all(|field| requested_fields.binary_search(field).is_ok())
    {
        return None;
    }
    Some(mask.fields().to_vec())
}

fn current_scan_policy(
    service: &RiffDbServiceInner,
    authorization: &AuthorizedOperation,
    lineage: &ContractLineage,
    entity: &EntitySchema,
    requested_limit: PageLimit,
    requested_fields: &[FieldId],
) -> Option<IndexScanCursorPolicy> {
    let obligations = authorization.obligations();
    let constraint = obligations.partition_constraint()?.clone();
    let PartitionConstraint::Filter(_) = &constraint else {
        return None;
    };
    let mask = obligations.field_mask()?;
    let policy_limit = obligations.row_limit()?;
    let effective = requested_limit.get().get().min(policy_limit.get());
    if authorization.database_id() != service.identity.database_id()
        || authorization.environment() != service.identity.environment()
        || authorization.operation() != ServiceOperationV1::ScanIndex
        || obligations.effective_tenant_scope() != &TenantScope::Global
        || obligations.output_classification()
            != OutputClassification::PolicyFilteredApplicationData
        || mask.lineage() != lineage
        || mask.entity_type_id() != entity.id()
        || !mask
            .fields()
            .iter()
            .all(|field| requested_fields.binary_search(field).is_ok())
    {
        return None;
    }
    Some(IndexScanCursorPolicy::new(
        obligations.effective_tenant_scope().clone(),
        constraint,
        FieldSelection::new(mask.fields().to_vec()).ok()?,
        PageLimit::new(effective).ok()?,
    ))
}

/// Resolves the sealed per-field reveal authorities (ADR-0118) minted by
/// the policy evaluation's mask.
///
/// The authorities themselves are unforgeable — minted only by
/// [`riffdb_policy::FieldMask::secret_reveal_authorities`], and a mask
/// exists only through the authorizer's evaluation of a real grant. This
/// helper adds the service-side scope checks: a mask for the wrong lineage
/// or entity, an absent mask, or a granted secret the schema no longer
/// classifies all yield no authority.
fn secret_reveal_authorities(
    authorization: &AuthorizedOperation,
    lineage: &ContractLineage,
    entity: &EntitySchema,
    secret_fields: &[FieldId],
) -> Vec<riffdb_policy::SecretRevealAuthority> {
    let Some(mask) = authorization.obligations().field_mask() else {
        return Vec::new();
    };
    if mask.lineage() != lineage || mask.entity_type_id() != entity.id() {
        return Vec::new();
    }
    mask.secret_reveal_authorities()
        .into_iter()
        .filter(|authority| secret_fields.binary_search(&authority.field()).is_ok())
        .collect()
}

fn constrain_index_scan_policy(
    current: IndexScanCursorPolicy,
    prior: Option<&IndexScanCursorPolicy>,
) -> Option<IndexScanCursorPolicy> {
    let Some(prior) = prior else {
        return Some(current);
    };
    if current.effective_tenant_scope() != prior.effective_tenant_scope() {
        return None;
    }
    let partition_constraint = intersect_partition_constraints(
        current.partition_constraint(),
        prior.partition_constraint(),
    )?;
    let visible_fields = intersect_field_selections(
        current.visible_fields().as_slice(),
        prior.visible_fields().as_slice(),
    )?;
    Some(IndexScanCursorPolicy::new(
        current.effective_tenant_scope().clone(),
        partition_constraint,
        visible_fields,
        current.effective_limit().min(prior.effective_limit()),
    ))
}

fn intersect_partition_constraints(
    current: &PartitionConstraint,
    prior: &PartitionConstraint,
) -> Option<PartitionConstraint> {
    let (PartitionConstraint::Filter(current), PartitionConstraint::Filter(prior)) =
        (current, prior)
    else {
        return None;
    };
    let scope = match (current, prior) {
        (PartitionScopeV1::All, scope) | (scope, PartitionScopeV1::All) => scope.clone(),
        (PartitionScopeV1::Explicit(current), PartitionScopeV1::Explicit(prior)) => {
            let current_keys: Vec<_> = current
                .iter()
                .map(ScopedPartitionV1::canonical_key)
                .collect();
            let prior_keys: Vec<_> = prior.iter().map(ScopedPartitionV1::canonical_key).collect();
            let mut current_index = 0;
            let mut prior_index = 0;
            let mut intersection = Vec::new();
            while current_index < current.len() && prior_index < prior.len() {
                match current_keys[current_index].cmp(&prior_keys[prior_index]) {
                    std::cmp::Ordering::Less => current_index += 1,
                    std::cmp::Ordering::Greater => prior_index += 1,
                    std::cmp::Ordering::Equal => {
                        intersection.push(current[current_index].clone());
                        current_index += 1;
                        prior_index += 1;
                    }
                }
            }
            PartitionScopeV1::explicit(intersection).ok()?
        }
    };
    Some(PartitionConstraint::Filter(scope))
}

fn intersect_field_selections(current: &[FieldId], prior: &[FieldId]) -> Option<FieldSelection> {
    let mut current_index = 0;
    let mut prior_index = 0;
    let mut intersection = Vec::new();
    while current_index < current.len() && prior_index < prior.len() {
        match current[current_index].cmp(&prior[prior_index]) {
            std::cmp::Ordering::Less => current_index += 1,
            std::cmp::Ordering::Greater => prior_index += 1,
            std::cmp::Ordering::Equal => {
                intersection.push(current[current_index]);
                current_index += 1;
                prior_index += 1;
            }
        }
    }
    FieldSelection::new(intersection).ok()
}

fn current_projection_policy(
    service: &RiffDbServiceInner,
    authorization: &AuthorizedOperation,
    requested_limit: PageLimit,
) -> Option<ProjectionCursorPolicy> {
    let obligations = authorization.obligations();
    let partition_constraint = obligations.partition_constraint()?.clone();
    let all_partitions = matches!(
        &partition_constraint,
        PartitionConstraint::Filter(PartitionScopeV1::All)
    );
    let policy_limit = obligations.row_limit()?;
    if authorization.database_id() != service.identity.database_id()
        || authorization.environment() != service.identity.environment()
        || authorization.operation() != ServiceOperationV1::QueryProjection
        || obligations.effective_tenant_scope() != &TenantScope::Global
        || !all_partitions
        || obligations.field_mask().is_some()
        || obligations.output_classification()
            != OutputClassification::PolicyFilteredApplicationData
    {
        return None;
    }
    Some(ProjectionCursorPolicy::new(
        obligations.effective_tenant_scope().clone(),
        partition_constraint,
        PageLimit::new(requested_limit.get().get().min(policy_limit.get())).ok()?,
    ))
}

fn constrain_projection_policy(
    current: ProjectionCursorPolicy,
    prior: Option<&ProjectionCursorPolicy>,
) -> Option<ProjectionCursorPolicy> {
    let Some(prior) = prior else {
        return Some(current);
    };
    if current.effective_tenant_scope() != prior.effective_tenant_scope()
        || current.partition_constraint() != prior.partition_constraint()
    {
        return None;
    }
    Some(ProjectionCursorPolicy::new(
        current.effective_tenant_scope().clone(),
        current.partition_constraint().clone(),
        current.effective_limit().min(prior.effective_limit()),
    ))
}

fn validate_public_metadata_authorization(
    service: &RiffDbServiceInner,
    authorization: &AuthorizedOperation,
    operation: ServiceOperationV1,
) -> ServiceResult<()> {
    let obligations = authorization.obligations();
    if authorization.database_id() != service.identity.database_id()
        || authorization.environment() != service.identity.environment()
        || authorization.operation() != operation
        || obligations.partition_constraint().is_some()
        || obligations.field_mask().is_some()
        || obligations.row_limit().is_some()
        || obligations.output_classification() != OutputClassification::PublicMetadata
    {
        return Err(service.internal_failure(operation, InternalDefect::ProofMismatch));
    }
    Ok(())
}

fn valid_discovery_authorization(
    service: &RiffDbServiceInner,
    authorization: &AuthorizedOperation,
    operation: ServiceOperationV1,
) -> bool {
    let obligations = authorization.obligations();
    authorization.database_id() == service.identity.database_id()
        && authorization.environment() == service.identity.environment()
        && authorization.operation() == operation
        && obligations.partition_constraint().is_none()
        && obligations.field_mask().is_none()
        && obligations.row_limit().is_none()
        && obligations.output_classification() == OutputClassification::PublicMetadata
}

#[allow(clippy::too_many_arguments)]
fn entity_view(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    entity: &EntitySchema,
    requested_key: &EntityKey,
    visible_fields: &[FieldId],
    requested_fields: &[FieldId],
    secret_fields: &[FieldId],
    reveal: &[riffdb_policy::SecretRevealAuthority],
    snapshot: AuthoritativeEntitySnapshot,
) -> ServiceResult<EntityView> {
    if snapshot.key() != requested_key {
        return Err(lower_integrity_failure(service, operation));
    }
    let (fields, redacted) = filter_record(
        entity.record(),
        snapshot.fields(),
        visible_fields,
        requested_fields,
        secret_fields,
        reveal,
    )
    .map_err(|()| lower_integrity_failure(service, operation))?;
    Ok(EntityView::new(
        snapshot.key().clone(),
        snapshot.entity_version(),
        snapshot.written_by_contract(),
        fields,
    )
    .with_redacted_fields(redacted))
}

#[allow(clippy::too_many_arguments)]
fn index_views(
    entity: &EntitySchema,
    lineage: &ContractLineage,
    constraint: &PartitionConstraint,
    visible_fields: &[FieldId],
    requested_fields: &[FieldId],
    secret_fields: &[FieldId],
    reveal: &[riffdb_policy::SecretRevealAuthority],
    page: &AuthoritativeIndexPage,
    derived_partitions: &[PartitionKey],
) -> Result<Vec<IndexRowView>, ()> {
    if page.rows().len() != derived_partitions.len() {
        return Err(());
    }
    let mut views = Vec::with_capacity(page.rows().len());
    for (row, partition) in page.rows().iter().zip(derived_partitions) {
        if !partition_allowed(constraint, lineage, partition) {
            continue;
        }
        let (values, redacted) = filter_record(
            entity.record(),
            row.values(),
            visible_fields,
            requested_fields,
            secret_fields,
            reveal,
        )?;
        views.push(IndexRowView::new(row.key().clone(), values).with_redacted_fields(redacted));
    }
    Ok(views)
}

fn index_continuation_after(
    releasable_rows: &[IndexRowView],
    emitted_count: usize,
    has_more: bool,
    more_due_to_limit: bool,
    scanned_through: Option<&riffdb_types::IndexEntryKey>,
) -> Result<Option<riffdb_types::IndexEntryKey>, ()> {
    if emitted_count > releasable_rows.len() {
        return Err(());
    }
    if !has_more {
        return Ok(None);
    }
    if emitted_count < releasable_rows.len() || more_due_to_limit {
        let emitted_index = emitted_count.checked_sub(1).ok_or(())?;
        let row = releasable_rows.get(emitted_index).ok_or(())?;
        return Ok(Some(row.key().clone()));
    }
    scanned_through.cloned().map(Some).ok_or(())
}

fn partition_allowed(
    constraint: &PartitionConstraint,
    lineage: &ContractLineage,
    partition: &PartitionKey,
) -> bool {
    let scoped = ScopedPartitionV1::new(lineage.clone(), partition.clone());
    match constraint {
        PartitionConstraint::Exact(expected) => expected == &scoped,
        PartitionConstraint::Filter(PartitionScopeV1::All) => true,
        PartitionConstraint::Filter(PartitionScopeV1::Explicit(entries)) => {
            entries.iter().any(|candidate| candidate == &scoped)
        }
    }
}

/// The single release point where record values become displayable (ADR-0118).
///
/// Non-secret fields release exactly as before: present when the visibility
/// mask names them, absent otherwise. Secret-classified fields NEVER release
/// through the mask alone — even a mask that lists one (for example via a
/// role default that enumerated every field) yields only its redaction
/// marker. The sole path to a released secret value is a
/// [`riffdb_policy::SecretRevealAuthority`] for that exact field, consumed by
/// [`riffdb_policy::SecretValue::reveal_for_authorized_display`]; the
/// display-surface architecture test enumerates that method's call sites.
fn filter_record(
    schema: &RecordSchema,
    record: &CanonicalRecord,
    visible_fields: &[FieldId],
    requested_fields: &[FieldId],
    secret_fields: &[FieldId],
    reveal: &[riffdb_policy::SecretRevealAuthority],
) -> Result<(CanonicalRecord, Vec<riffdb_types::RedactedSecretField>), ()> {
    let mut filtered = Vec::new();
    let mut redacted = Vec::new();
    for (field_id, value) in record.fields() {
        let Some(field) = schema.field(*field_id) else {
            // Compatible later-version fields remain invisible under an older
            // selected schema.
            continue;
        };
        field.value_type().validate_value(value).map_err(|_| ())?;
        if secret_fields.binary_search(field_id).is_ok() {
            // Selection-consistent markers: an explicit field selection that
            // does not name this secret omits it entirely, exactly like any
            // other unselected field. (Explicitly SELECTING a secret either
            // denies upstream or arrives here with reveal authority, so the
            // marker appears only under the default whole-record selection.)
            if !requested_fields.is_empty() && requested_fields.binary_search(field_id).is_err() {
                continue;
            }
            let secret =
                riffdb_policy::SecretValue::classify(*field_id, field.name(), value.clone());
            let authority = reveal
                .iter()
                .find(|authority| authority.field() == *field_id);
            match authority {
                Some(authority) => match secret.reveal_for_authorized_display(authority) {
                    Ok(revealed) => filtered.push((*field_id, revealed)),
                    Err(withheld) => redacted.push(withheld.into_redacted()),
                },
                None => redacted.push(secret.into_redacted()),
            }
            continue;
        }
        if visible_fields.binary_search(field_id).is_ok() {
            filtered.push((*field_id, value.clone()));
        }
    }
    CanonicalRecord::new(filtered)
        .map(|record| (record, redacted))
        .map_err(|_| ())
}

fn validate_projection_ready(
    schema: &BoundProjectionGroupSchema,
    ready: &ProjectionPortReady,
) -> Result<(), ()> {
    for row in ready.rows() {
        schema
            .group_key(ready.generation(), row.group())
            .map_err(|_| ())?;
        validate_exact_record(schema.schema().measures(), row.values())?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProjectionObservationValidationError {
    Integrity,
    InvalidCursor,
}

fn validate_pending_projection_observation(
    fence: &ProjectionPageFence,
    expected_identity: &riffdb_types::ProjectionIdentity,
    required_sequence: Option<riffdb_types::CommitSequence>,
    wait: std::time::Duration,
    cursor_fence: Option<ProjectionPageFence>,
) -> Result<(), ProjectionObservationValidationError> {
    let Some(required_sequence) = required_sequence else {
        return Err(ProjectionObservationValidationError::Integrity);
    };
    if wait.is_zero()
        || fence.frontier() >= riffdb_types::FrontierPosition::AppliedThrough(required_sequence)
    {
        return Err(ProjectionObservationValidationError::Integrity);
    }
    validate_projection_page_observation(fence, expected_identity, cursor_fence)
}

fn validate_projection_page_observation(
    fence: &ProjectionPageFence,
    expected_identity: &riffdb_types::ProjectionIdentity,
    cursor_fence: Option<ProjectionPageFence>,
) -> Result<(), ProjectionObservationValidationError> {
    if fence.identity() != expected_identity {
        return Err(ProjectionObservationValidationError::Integrity);
    }
    if cursor_fence
        .as_ref()
        .is_some_and(|cursor_fence| cursor_fence != fence)
    {
        return Err(ProjectionObservationValidationError::InvalidCursor);
    }
    Ok(())
}

fn validate_projection_state_observation(
    fence: &ProjectionStateFence,
    expected_identity: &riffdb_types::ProjectionIdentity,
    cursor_fence: Option<ProjectionPageFence>,
) -> Result<(), ProjectionObservationValidationError> {
    if fence.identity() != expected_identity {
        return Err(ProjectionObservationValidationError::Integrity);
    }
    if let Some(cursor_fence) = cursor_fence {
        let Some(generation) = fence.generation() else {
            return Err(ProjectionObservationValidationError::InvalidCursor);
        };
        let observed =
            ProjectionPageFence::new(fence.identity().clone(), generation, fence.frontier());
        if observed != cursor_fence {
            return Err(ProjectionObservationValidationError::InvalidCursor);
        }
    }
    Ok(())
}

fn validate_exact_record(schema: &RecordSchema, record: &CanonicalRecord) -> Result<(), ()> {
    if schema.fields().len() != record.fields().len() {
        return Err(());
    }
    for (expected, (actual_id, value)) in schema.fields().iter().zip(record.fields()) {
        if expected.id() != *actual_id || expected.value_type().validate_value(value).is_err() {
            return Err(());
        }
    }
    Ok(())
}

fn schema_artifact(
    artifacts: &[GeneratedSchemaArtifact],
    key: SchemaArtifactKey,
) -> Option<&GeneratedSchemaArtifact> {
    artifacts
        .binary_search_by_key(&key, GeneratedSchemaArtifact::key)
        .ok()
        .map(|position| &artifacts[position])
}

fn validation_failure(code: ValidationCode) -> ServiceFailure {
    PublicError::validation(ValidationIssues::one(ValidationIssue::new(
        code,
        ValidationPath::root(),
    )))
    .into()
}

fn materialize_query_components<'a>(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    schema: &SchemaIr,
    component_types: impl IntoIterator<Item = &'a ValueType>,
    submitted: &[SubmittedValue],
) -> ServiceResult<Vec<CanonicalValue>> {
    let component_types: Vec<_> = component_types.into_iter().collect();
    if submitted.len() > component_types.len() {
        return Err(validation_failure(ValidationCode::TooManyItems));
    }

    let mut canonical = Vec::with_capacity(submitted.len());
    for (index, (value_type, value)) in component_types.iter().zip(submitted).enumerate() {
        let index = u32::try_from(index)
            .map_err(|_| service.internal_failure(operation, InternalDefect::ProofMismatch))?;
        match materialize_submitted_value(
            schema,
            value_type,
            value,
            vec![ValidationPathSegment::ListIndex(index)],
        ) {
            Ok(value) => canonical.push(value),
            Err(SubmittedValueMaterializationError::Public(error)) => return Err(error.into()),
            Err(SubmittedValueMaterializationError::Integrity) => {
                return Err(service.internal_failure(operation, InternalDefect::ProofMismatch));
            }
        }
    }
    Ok(canonical)
}

fn invalid_cursor_failure() -> ServiceFailure {
    validation_failure(ValidationCode::InvalidValue)
}

fn cursor_unavailable_failure(service: &RiffDbServiceInner) -> ServiceFailure {
    service
        .providers
        .telemetry
        .record(ServiceTelemetryEvent::CursorUnavailable);
    PublicError::storage_unavailable().into()
}

fn controlled_wait_failure(error: ControlledWaitError) -> ServiceFailure {
    match error {
        ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
        ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
    }
}

/// Maps admission failures to typed service failures without finishing audit.
///
/// Used inside the internal read-retry loop so terminal classifications can be
/// finished exactly once by the outer orchestration path.
fn admission_error_failure(error: PortAdmissionError) -> ServiceFailure {
    match error {
        PortAdmissionError::Cancelled => ServiceFailure::Cancelled,
        PortAdmissionError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
        PortAdmissionError::Unavailable | PortAdmissionError::Stopped => {
            PublicError::storage_unavailable().into()
        }
    }
}

fn catalog_failure(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: CatalogError,
) -> ServiceFailure {
    match error.kind() {
        CatalogErrorKind::Storage => PublicError::storage_unavailable().into(),
        _ => lower_integrity_failure(service, operation),
    }
}

fn authoritative_read_failure(
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

fn projection_failure(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: ProjectionPortError,
) -> ServiceFailure {
    match error {
        ProjectionPortError::Unavailable => PublicError::storage_unavailable().into(),
        ProjectionPortError::Integrity => lower_integrity_failure(service, operation),
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

pub(crate) async fn finish_discovery_result<T>(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    completion: &BegunInvocationCompletion,
    result: ServiceResult<T>,
) -> ServiceResult<T> {
    let (phase, result) = match result {
        Ok(result) => (ServiceAuditPhaseV1::Succeeded, Ok(result)),
        Err(failure) => (ServiceAuditPhaseV1::Failed, Err(failure)),
    };
    if completion
        .finish(service, context, phase, ServiceAuditLinkV1::None)
        .await
        .is_err()
    {
        service.note_audit_failure(completion.operation());
        return Err(PublicError::storage_unavailable().into());
    }
    result
}

pub(crate) async fn finish_success(
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
        service.note_audit_failure(begun.initial_authorization().operation());
        return Err(PublicError::storage_unavailable().into());
    }
    Ok(())
}

pub(crate) async fn finish_failure(
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
        service.note_audit_failure(begun.initial_authorization().operation());
        return PublicError::storage_unavailable().into();
    }
    failure
}

async fn finish_controlled_wait(
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
        service.note_audit_failure(begun.initial_authorization().operation());
        return PublicError::storage_unavailable().into();
    }
    failure
}

async fn finish_admission_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    error: PortAdmissionError,
) -> ServiceFailure {
    match error {
        PortAdmissionError::Cancelled => {
            finish_controlled_wait(service, context, begun, ControlledWaitError::Cancelled).await
        }
        PortAdmissionError::DeadlineExceeded => {
            finish_controlled_wait(
                service,
                context,
                begun,
                ControlledWaitError::DeadlineExceeded,
            )
            .await
        }
        PortAdmissionError::Unavailable | PortAdmissionError::Stopped => {
            finish_failure(
                service,
                context,
                begun,
                PublicError::storage_unavailable().into(),
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, pending, ready};
    use std::pin::pin;
    use std::task::{Context, Waker};
    use std::time::Duration;

    use riffdb_contract_ir::{
        EntitySchema, FieldSchema, KeyComponentSchema, KeyPurpose, KeySchema, RecordSchema,
        RecordTypeRef, SchemaArtifactKey, SchemaIr, ValueType,
    };
    use riffdb_types::{
        AggregateTypeId, CanonicalVector, CommitSequence, EntityKeyBuilder, EntityTypeId,
        FrontierPosition, IndexEntryKeyBuilder, IndexId, PartitionKeyBuilder, ProjectionId,
        ProjectionIdentity, ProjectionPlanHash, VectorDimension,
    };

    use super::*;

    struct SelectiveDeadlineScheduler {
        ready_deadline: Option<Instant>,
    }

    impl crate::RequestDeadlineScheduler for SelectiveDeadlineScheduler {
        fn wait_until(&self, deadline: Instant) -> crate::RequestDeadlineFuture<'_> {
            let ready_now = self.ready_deadline == Some(deadline);
            Box::pin(async move {
                if ready_now {
                    return;
                }
                pending::<()>().await;
            })
        }
    }

    fn ready_in_one_poll<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let mut context = Context::from_waker(Waker::noop());
        let mut future = pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("test future should resolve on its first deterministic poll"),
        }
    }

    fn page_limit(value: u16) -> PageLimit {
        PageLimit::new(value).expect("test page limit")
    }

    #[test]
    fn reactive_wakeup_generation_is_opaque_stable_and_identity_bound() {
        let database = riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1, [1; 10])
            .expect("database ID");
        let other_database =
            riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1, [2; 10])
                .expect("database ID");
        let before_first = FrontierPosition::BeforeFirst;
        let first = FrontierPosition::AppliedThrough(CommitSequence::first());

        let baseline = reactive_wakeup_generation(database, 1, before_first);
        assert_eq!(
            baseline,
            reactive_wakeup_generation(database, 1, before_first)
        );
        assert_ne!(baseline, reactive_wakeup_generation(database, 1, first));
        assert_ne!(
            baseline,
            reactive_wakeup_generation(database, 2, before_first)
        );
        assert_ne!(
            baseline,
            reactive_wakeup_generation(other_database, 1, before_first)
        );
        assert_eq!(baseline.as_bytes().len(), 32);
        assert_eq!(
            format!("{baseline:?}"),
            "ReactiveWakeupGeneration([OPAQUE])"
        );
    }

    fn index_view(component: u64) -> IndexRowView {
        let mut entity_key = EntityKeyBuilder::new(EntityTypeId::first());
        entity_key
            .push_u64(component)
            .expect("bounded entity component");
        let mut index_key = IndexEntryKeyBuilder::new(IndexId::first());
        index_key
            .push_u64(component)
            .expect("bounded index component");
        IndexRowView::new(
            index_key
                .finish(entity_key.finish().expect("entity key"))
                .expect("index key"),
            CanonicalRecord::new(Vec::new()).expect("empty values"),
        )
    }

    #[test]
    fn index_continuation_uses_physical_progress_only_after_all_releasable_rows() {
        let rows = vec![index_view(1), index_view(2)];
        let scanned_through = index_view(3);
        assert_eq!(
            index_continuation_after(&rows, rows.len(), true, false, Some(scanned_through.key()),),
            Ok(Some(scanned_through.key().clone()))
        );
        assert_eq!(
            index_continuation_after(&rows, 1, true, true, Some(scanned_through.key())),
            Ok(Some(rows[0].key().clone())),
            "limit truncation must resume after the last emitted row"
        );
        assert_eq!(
            index_continuation_after(&[], 0, true, false, Some(scanned_through.key())),
            Ok(Some(scanned_through.key().clone())),
            "an empty visible page still carries lower physical progress"
        );
        assert_eq!(
            index_continuation_after(&rows, rows.len(), false, false, None),
            Ok(None)
        );
    }

    #[test]
    fn projection_capacity_wait_honors_the_absolute_projection_deadline() {
        let now = Instant::now();
        let projection_deadline = now + Duration::from_secs(10);
        let request_deadline = now + Duration::from_secs(20);
        let (control, _cancellation) = crate::RequestControl::new(request_deadline);
        let scheduler = SelectiveDeadlineScheduler {
            ready_deadline: Some(projection_deadline),
        };

        let result = ready_in_one_poll(wait_for_projection_operation(
            &control,
            &scheduler,
            Some(projection_deadline),
            pending::<()>(),
        ));

        assert_eq!(
            result,
            Err(ProjectionOperationWaitError::ProjectionDeadlineElapsed)
        );
    }

    #[test]
    fn projection_capacity_wait_preserves_outer_deadline_and_cancellation_priority() {
        let now = Instant::now();
        let request_deadline = now + Duration::from_secs(10);
        let projection_deadline = now + Duration::from_secs(20);
        let (control, cancellation) = crate::RequestControl::new(request_deadline);
        let scheduler = SelectiveDeadlineScheduler {
            ready_deadline: Some(request_deadline),
        };
        assert_eq!(
            ready_in_one_poll(wait_for_projection_operation(
                &control,
                &scheduler,
                Some(projection_deadline),
                pending::<()>(),
            )),
            Err(ProjectionOperationWaitError::Controlled(
                ControlledWaitError::DeadlineExceeded
            ))
        );

        cancellation.cancel();
        assert_eq!(
            ready_in_one_poll(wait_for_projection_operation(
                &control,
                &scheduler,
                Some(projection_deadline),
                ready(7_u8),
            )),
            Err(ProjectionOperationWaitError::Controlled(
                ControlledWaitError::Cancelled
            ))
        );
    }

    #[test]
    fn immediate_projection_observation_uses_only_outer_request_control() {
        let now = Instant::now();
        let request_deadline = now + Duration::from_secs(10);
        let (control, _cancellation) = crate::RequestControl::new(request_deadline);
        let scheduler = SelectiveDeadlineScheduler {
            ready_deadline: None,
        };

        assert_eq!(
            ready_in_one_poll(wait_for_projection_operation(
                &control,
                &scheduler,
                None,
                ready(7_u8),
            )),
            Ok(7)
        );
    }

    fn projection_identity(hash_byte: u8) -> ProjectionIdentity {
        ProjectionIdentity::new(
            ContractLineage::new("projection-wake-test").expect("lineage"),
            ProjectionId::first(),
            ProjectionPlanHash::from_bytes([hash_byte; 32]),
        )
    }

    fn sequence(value: u64) -> CommitSequence {
        CommitSequence::try_from(value).expect("nonzero sequence")
    }

    fn scoped_partition(lineage: &ContractLineage, value: u64) -> ScopedPartitionV1 {
        let mut builder = PartitionKeyBuilder::new(AggregateTypeId::first());
        builder.push_u64(value).expect("bounded partition value");
        ScopedPartitionV1::new(
            lineage.clone(),
            builder.finish().expect("bounded partition key"),
        )
    }

    fn explicit_scope(entries: Vec<ScopedPartitionV1>) -> PartitionConstraint {
        PartitionConstraint::Filter(
            PartitionScopeV1::explicit(entries).expect("canonical explicit scope"),
        )
    }

    #[test]
    fn index_cursor_policy_intersects_partition_fields_and_limit() {
        let lineage = ContractLineage::new("ledger").expect("lineage");
        let first_partition = scoped_partition(&lineage, 1);
        let shared_partition = scoped_partition(&lineage, 2);
        let last_partition = scoped_partition(&lineage, 3);
        let first_field = FieldId::first();
        let shared_field = first_field.checked_next().expect("second field");
        let last_field = shared_field.checked_next().expect("third field");
        let prior = IndexScanCursorPolicy::new(
            TenantScope::Global,
            explicit_scope(vec![first_partition, shared_partition.clone()]),
            FieldSelection::new(vec![first_field, shared_field]).expect("prior fields"),
            page_limit(100),
        );
        let current = IndexScanCursorPolicy::new(
            TenantScope::Global,
            explicit_scope(vec![shared_partition.clone(), last_partition]),
            FieldSelection::new(vec![shared_field, last_field]).expect("current fields"),
            page_limit(80),
        );

        let effective =
            constrain_index_scan_policy(current, Some(&prior)).expect("overlapping policy");

        assert_eq!(effective.effective_limit(), page_limit(80));
        assert_eq!(effective.visible_fields().as_slice(), &[shared_field]);
        assert_eq!(
            effective.partition_constraint(),
            &explicit_scope(vec![shared_partition])
        );
    }

    #[test]
    fn pending_projection_observation_rejects_wrong_identity() {
        let expected = projection_identity(0x11);
        let fence = ProjectionPageFence::new(
            projection_identity(0x22),
            ProjectionGeneration::first(),
            FrontierPosition::BeforeFirst,
        );

        assert_eq!(
            validate_pending_projection_observation(
                &fence,
                &expected,
                Some(sequence(2)),
                std::time::Duration::from_secs(1),
                None,
            ),
            Err(ProjectionObservationValidationError::Integrity)
        );
    }

    #[test]
    fn pending_projection_observation_rejects_satisfied_frontier() {
        let identity = projection_identity(0x11);
        let required = sequence(2);
        let fence = ProjectionPageFence::new(
            identity.clone(),
            ProjectionGeneration::first(),
            FrontierPosition::AppliedThrough(required),
        );

        assert_eq!(
            validate_pending_projection_observation(
                &fence,
                &identity,
                Some(required),
                std::time::Duration::from_secs(1),
                None,
            ),
            Err(ProjectionObservationValidationError::Integrity)
        );
    }

    #[test]
    fn pending_projection_observation_rejects_zero_wait() {
        let identity = projection_identity(0x11);
        let fence = ProjectionPageFence::new(
            identity.clone(),
            ProjectionGeneration::first(),
            FrontierPosition::BeforeFirst,
        );

        assert_eq!(
            validate_pending_projection_observation(
                &fence,
                &identity,
                Some(sequence(2)),
                std::time::Duration::ZERO,
                None,
            ),
            Err(ProjectionObservationValidationError::Integrity)
        );
    }

    #[test]
    fn pending_projection_observation_invalidates_cursor_generation_or_frontier_drift() {
        let identity = projection_identity(0x11);
        let required = sequence(3);
        let observed = ProjectionPageFence::new(
            identity.clone(),
            ProjectionGeneration::first(),
            FrontierPosition::BeforeFirst,
        );
        let generation_drift = ProjectionPageFence::new(
            identity.clone(),
            ProjectionGeneration::first()
                .checked_next()
                .expect("second generation"),
            FrontierPosition::BeforeFirst,
        );
        let frontier_drift = ProjectionPageFence::new(
            identity.clone(),
            ProjectionGeneration::first(),
            FrontierPosition::AppliedThrough(sequence(1)),
        );

        assert_eq!(
            validate_pending_projection_observation(
                &observed,
                &identity,
                Some(required),
                std::time::Duration::from_secs(1),
                Some(generation_drift),
            ),
            Err(ProjectionObservationValidationError::InvalidCursor)
        );
        assert_eq!(
            validate_pending_projection_observation(
                &observed,
                &identity,
                Some(required),
                std::time::Duration::from_secs(1),
                Some(frontier_drift),
            ),
            Err(ProjectionObservationValidationError::InvalidCursor)
        );
    }

    #[test]
    fn terminal_projection_state_observation_checks_identity_and_cursor_fence() {
        let identity = projection_identity(0x11);
        let generation = ProjectionGeneration::first();
        let state = ProjectionStateFence::new(
            identity.clone(),
            Some(generation),
            FrontierPosition::BeforeFirst,
        )
        .expect("retained generation state fence");
        let matching_cursor =
            ProjectionPageFence::new(identity.clone(), generation, FrontierPosition::BeforeFirst);
        let drifting_cursor = ProjectionPageFence::new(
            identity.clone(),
            generation.checked_next().expect("second generation"),
            FrontierPosition::BeforeFirst,
        );

        assert_eq!(
            validate_projection_state_observation(
                &state,
                &projection_identity(0x22),
                Some(matching_cursor.clone()),
            ),
            Err(ProjectionObservationValidationError::Integrity)
        );
        assert_eq!(
            validate_projection_state_observation(&state, &identity, Some(drifting_cursor),),
            Err(ProjectionObservationValidationError::InvalidCursor)
        );
        assert_eq!(
            validate_projection_state_observation(&state, &identity, Some(matching_cursor)),
            Ok(())
        );

        let no_control =
            ProjectionStateFence::new(identity.clone(), None, FrontierPosition::BeforeFirst)
                .expect("known identity without projection control");
        assert_eq!(
            validate_projection_state_observation(
                &no_control,
                &identity,
                Some(ProjectionPageFence::new(
                    identity.clone(),
                    generation,
                    FrontierPosition::BeforeFirst,
                )),
            ),
            Err(ProjectionObservationValidationError::InvalidCursor)
        );
    }

    #[test]
    fn index_cursor_policy_rejects_disjoint_partition_scopes() {
        let lineage = ContractLineage::new("ledger").expect("lineage");
        let prior = IndexScanCursorPolicy::new(
            TenantScope::Global,
            explicit_scope(vec![scoped_partition(&lineage, 1)]),
            FieldSelection::new(Vec::new()).expect("empty field visibility"),
            page_limit(50),
        );
        let current = IndexScanCursorPolicy::new(
            TenantScope::Global,
            explicit_scope(vec![scoped_partition(&lineage, 2)]),
            FieldSelection::new(Vec::new()).expect("empty field visibility"),
            page_limit(50),
        );

        assert!(constrain_index_scan_policy(current, Some(&prior)).is_none());
    }

    #[test]
    fn filtered_entity_schema_keeps_keys_and_visible_non_keys_only() {
        let entity_id = EntityTypeId::first();
        let key_field = FieldId::first();
        let visible_field = key_field.checked_next().expect("second field");
        let hidden_field = visible_field.checked_next().expect("third field");
        let record = RecordSchema::new(
            RecordTypeRef::Entity(entity_id),
            vec![
                FieldSchema::new(key_field, "primary_key", ValueType::u64()).expect("key field"),
                FieldSchema::new(visible_field, "visible_value", ValueType::bool())
                    .expect("visible field"),
                FieldSchema::new(hidden_field, "hidden_value", ValueType::bool())
                    .expect("hidden field"),
            ],
        )
        .expect("record");
        let key_schema = KeySchema::new(
            KeyPurpose::Entity(entity_id),
            vec![KeyComponentSchema::new(ValueType::u64(), Vec::new()).expect("key component")],
        )
        .expect("key schema");
        let entity = EntitySchema::new(
            entity_id,
            "account",
            record,
            vec![key_field],
            key_schema,
            Vec::new(),
            Vec::new(),
        )
        .expect("entity");
        let schema = SchemaIr::new(vec![entity.clone()], Vec::new(), Vec::new(), Vec::new())
            .expect("schema");

        let artifact = filtered_entity_schema_artifact(&entity, &schema, &[visible_field])
            .expect("filtered artifact");

        assert_eq!(artifact.key(), SchemaArtifactKey::Entity(entity_id));
        assert!(artifact.canonical_json().contains("primary_key"));
        assert!(artifact.canonical_json().contains("visible_value"));
        assert!(!artifact.canonical_json().contains("hidden_value"));
        assert!(filtered_entity_schema_artifact(&entity, &schema, &[key_field]).is_err());
    }

    // ─── ADR-0118 release point (WP-597) ───

    const SECRET_CANARY: &str = "wp597-release-canary-7c41";

    fn secret_release_fixture() -> (RecordSchema, CanonicalRecord, FieldId, FieldId) {
        let plain_field = FieldId::first();
        let secret_field = plain_field.checked_next().expect("second field");
        let schema = RecordSchema::new(
            RecordTypeRef::Entity(EntityTypeId::first()),
            vec![
                FieldSchema::new(plain_field, "plain_value", ValueType::bool())
                    .expect("plain field"),
                FieldSchema::new(
                    secret_field,
                    "token_hash",
                    ValueType::string(256).expect("string type"),
                )
                .expect("secret field"),
            ],
        )
        .expect("record schema");
        let record = CanonicalRecord::new(vec![
            (plain_field, CanonicalValue::Bool(true)),
            (
                secret_field,
                CanonicalValue::String(
                    riffdb_types::CanonicalString::new(SECRET_CANARY.to_owned())
                        .expect("canary string"),
                ),
            ),
        ])
        .expect("canonical record");
        (schema, record, plain_field, secret_field)
    }

    /// The release point withholds a secret field even when the ORDINARY
    /// visibility mask lists it (the enumerate-all sweep a role default
    /// produces), releasing only its redaction marker.
    ///
    /// Non-empty triggering set: the record provably carries the canary and
    /// the mask provably names the field — the control assertion shows the
    /// same call releases the plain field through the same mask.
    #[test]
    fn release_point_withholds_secret_fields_from_an_enumerate_all_mask() {
        let (schema, record, plain_field, secret_field) = secret_release_fixture();
        let visible = [plain_field, secret_field];
        let secrets = [secret_field];
        let (fields, redacted) = filter_record(&schema, &record, &visible, &[], &secrets, &[])
            .expect("release succeeds");
        // Control: the ordinary field released through the same mask.
        assert_eq!(
            fields
                .fields()
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
            vec![plain_field]
        );
        assert_eq!(redacted.len(), 1);
        assert_eq!(redacted[0].field(), secret_field);
        assert_eq!(redacted[0].redaction_marker(), "[redacted:token_hash]");
        // The withheld entry renders only the marker in every form.
        assert_eq!(format!("{}", redacted[0]), "[redacted:token_hash]");
        assert_eq!(format!("{:?}", redacted[0]), "[redacted:token_hash]");
    }

    #[test]
    fn release_point_withholds_secret_vectors_while_releasing_vector_controls() {
        let visible_field = FieldId::first();
        let secret_field = visible_field.checked_next().expect("second field");
        let dimension = VectorDimension::new(3).expect("bounded vector dimension");
        let schema = RecordSchema::new(
            RecordTypeRef::Entity(EntityTypeId::first()),
            vec![
                FieldSchema::new(
                    visible_field,
                    "public_embedding",
                    ValueType::vector(dimension),
                )
                .expect("visible vector field"),
                FieldSchema::new(
                    secret_field,
                    "secret_embedding",
                    ValueType::vector(dimension),
                )
                .expect("secret vector field"),
            ],
        )
        .expect("record schema");
        let visible_vector = CanonicalValue::Vector(
            CanonicalVector::new(vec![1.5, -2.25, 0.5]).expect("visible vector"),
        );
        let secret_vector = CanonicalValue::Vector(
            CanonicalVector::new(vec![12_345.5, -9_876.25, 0.125]).expect("secret vector"),
        );
        let record = CanonicalRecord::new(vec![
            (visible_field, visible_vector.clone()),
            (secret_field, secret_vector),
        ])
        .expect("canonical record");

        let (fields, redacted) = filter_record(
            &schema,
            &record,
            &[visible_field, secret_field],
            &[],
            &[secret_field],
            &[],
        )
        .expect("vector release succeeds");
        assert_eq!(fields.fields(), &[(visible_field, visible_vector)]);
        assert_eq!(redacted.len(), 1);
        assert_eq!(redacted[0].field(), secret_field);
        assert_eq!(
            redacted[0].redaction_marker(),
            "[redacted:secret_embedding]"
        );
        let rendered = format!("{:?}", redacted);
        for canary in ["12345.5", "-9876.25", "0.125"] {
            assert!(
                !rendered.contains(canary),
                "secret vector component escaped through the redaction marker: {canary}"
            );
        }
    }

    /// Markers are selection-consistent: an explicit field selection that
    /// does not name the secret omits it entirely — exactly like any other
    /// unselected field — while the default whole-record selection shows
    /// the marker.
    #[test]
    fn release_point_markers_follow_the_field_selection() {
        let (schema, record, plain_field, secret_field) = secret_release_fixture();
        let visible = [plain_field];
        let secrets = [secret_field];
        // Explicit narrow selection: no marker, no value.
        let requested = [plain_field];
        let (fields, redacted) =
            filter_record(&schema, &record, &visible, &requested, &secrets, &[])
                .expect("release succeeds");
        assert_eq!(fields.fields().len(), 1);
        assert!(
            redacted.is_empty(),
            "an unselected secret field must be omitted, not marked"
        );
        // Default whole-record selection: the marker appears.
        let (_, redacted) = filter_record(&schema, &record, &visible, &[], &secrets, &[])
            .expect("release succeeds");
        assert_eq!(redacted.len(), 1);
        assert_eq!(redacted[0].field(), secret_field);
    }

    /// Explicit reveal authority for the exact field releases the value;
    /// authority for a different field stays withheld (fail closed).
    #[test]
    fn release_point_reveals_only_under_exact_field_authority() {
        let (schema, record, plain_field, secret_field) = secret_release_fixture();
        let visible = [plain_field];
        let secrets = [secret_field];

        let exact = [riffdb_policy::SecretRevealAuthority::test_fixture(
            secret_field,
        )];
        let (fields, redacted) = filter_record(&schema, &record, &visible, &[], &secrets, &exact)
            .expect("release succeeds");
        assert!(redacted.is_empty());
        let revealed = fields
            .fields()
            .iter()
            .find(|(id, _)| *id == secret_field)
            .map(|(_, value)| value)
            .expect("revealed value present");
        assert_eq!(
            revealed,
            &CanonicalValue::String(
                riffdb_types::CanonicalString::new(SECRET_CANARY.to_owned())
                    .expect("canary string"),
            ),
            "the released value must be the exact stored bytes"
        );

        let wrong = [riffdb_policy::SecretRevealAuthority::test_fixture(
            plain_field,
        )];
        let (fields, redacted) = filter_record(&schema, &record, &visible, &[], &secrets, &wrong)
            .expect("release succeeds");
        assert!(
            fields.fields().iter().all(|(id, _)| *id != secret_field),
            "wrong-field authority must not release the value"
        );
        assert_eq!(redacted.len(), 1);
        assert_eq!(redacted[0].field(), secret_field);
    }

    fn resource_candidate_with_fields(entity: u32, field_count: u32) -> ResourceCandidate {
        let entity_type_id = EntityTypeId::new(entity).expect("nonzero entity ID");
        let fields = (1..=field_count)
            .map(|field| FieldId::new(field).expect("nonzero field ID"))
            .collect();
        ResourceCandidate {
            policy_candidate: DiscoveryResource::EntitySchema(
                EntitySchemaCandidate::new(
                    ContractLineage::new("batch_test").expect("bounded lineage"),
                    entity_type_id,
                    fields,
                )
                .expect("bounded entity-schema candidate"),
            ),
            descriptor: ResourceDescriptor::active_contract(),
            entity_schema: None,
        }
    }

    #[test]
    fn resource_discovery_batches_bound_candidates_and_aggregate_fields() {
        let count_limited = (0..501)
            .map(|_| ResourceCandidate {
                policy_candidate: DiscoveryResource::Health,
                descriptor: ResourceDescriptor::server_health(),
                entity_schema: None,
            })
            .collect::<Vec<_>>();
        assert_eq!(resource_discovery_batch_end(&count_limited, 0), Some(500));
        assert_eq!(resource_discovery_batch_end(&count_limited, 500), Some(501));

        let mut field_limited = (1..=15)
            .map(|entity| resource_candidate_with_fields(entity, 4_096))
            .collect::<Vec<_>>();
        field_limited.push(resource_candidate_with_fields(16, 4_095));
        field_limited.push(resource_candidate_with_fields(17, 1));
        assert_eq!(
            resource_discovery_batch_end(&field_limited, 0),
            Some(16),
            "the exact 65,535-field aggregate remains in one policy batch"
        );
        assert_eq!(
            resource_discovery_batch_end(&field_limited, 16),
            Some(17),
            "the next field starts a separately authorized batch"
        );
    }

    #[test]
    fn command_discovery_continuation_visibility_can_only_narrow() {
        assert_eq!(
            constrain_command_discovery_visibility(
                vec![true, true, false, true],
                Some(&[true, false, true, true]),
            ),
            Some(vec![true, false, false, true])
        );
        assert!(constrain_command_discovery_visibility(vec![true], Some(&[true, false])).is_none());
    }

    #[test]
    fn resource_discovery_continuation_intersects_fields_and_hidden_items() {
        let first = FieldId::first();
        let second = first.checked_next().expect("second field");
        let third = second.checked_next().expect("third field");
        let current = vec![
            ResourceDiscoveryCursorVisibility::VisibleEntityFields(vec![first, second]),
            ResourceDiscoveryCursorVisibility::Visible,
            ResourceDiscoveryCursorVisibility::Hidden,
        ];
        let prior = vec![
            ResourceDiscoveryCursorVisibility::VisibleEntityFields(vec![second, third]),
            ResourceDiscoveryCursorVisibility::Hidden,
            ResourceDiscoveryCursorVisibility::Visible,
        ];

        assert_eq!(
            constrain_resource_discovery_visibility(current, Some(&prior)),
            Some(vec![
                ResourceDiscoveryCursorVisibility::VisibleEntityFields(vec![second]),
                ResourceDiscoveryCursorVisibility::Hidden,
                ResourceDiscoveryCursorVisibility::Hidden,
            ])
        );
        assert!(
            constrain_resource_discovery_visibility(
                vec![ResourceDiscoveryCursorVisibility::Visible],
                Some(&[ResourceDiscoveryCursorVisibility::VisibleEntityFields(
                    vec![first,]
                )]),
            )
            .is_none()
        );
    }
}
