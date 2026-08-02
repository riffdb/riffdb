//! Commit-log, subscription-establishment, and provenance service orchestration.

use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use riffdb_catalog::{CatalogErrorKind, ValidatedContractBundle};
use riffdb_errors::{
    PublicError, ValidationCode, ValidationIssue, ValidationIssues, ValidationPath,
};
use riffdb_policy::{
    AuditClass, AuthorizedOperation, Decision, OperationRequest, OutputClassification,
    PartitionConstraint, ProvenanceSelector,
};
use riffdb_types::{
    ContractLineage, ContractVersion, FrontierPosition, PartitionScopeV1, ServiceAuditLinkV1,
    ServiceAuditPhaseV1, ServiceOperationV1, TenantScope,
};

use crate::orchestration::{AuditScope, BegunInvocation};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    AffectedEntityView, AuthoritativeCommitNotification, AuthoritativeCommitScanRequest,
    AuthoritativeCommitSnapshot, AuthoritativeCommitSubscriptionRequest, AuthoritativeReadError,
    AuthoritativeReadinessFailure, CommitApplication, CommitScanCursorLookup,
    CommitScanCursorPolicy, CommitScanCursorState, CommitScanFence, CommitSubscriberLease,
    CommitSubscription, CommitSubscriptionEndReason, CommitSubscriptionEvent,
    CommitSubscriptionTerminal, CommitView, CursorAccessError, GetCommitRequest, GetCommitResult,
    InternalDefect, Page, PageLimit, PortAdmissionError, PortDriverStopped, ProvenanceSelection,
    ProvenanceView, RequestContext, RiffDbService, RiffDbServiceInner, ScanCommitsRequest,
    ScanCommitsResult, ServiceAuditTargetMap, ServiceFailure, ServiceFuture, ServiceResult,
    ServiceTelemetryEvent, SubscribeToCommitsRequest, SubscribeToCommitsResult,
    TraceProvenanceRequest, TraceProvenanceResult, catch_continuation_panic,
    ensure_response_budget, ensure_subscription_establishment_budget, fit_page_items,
};

impl CommitApplication for RiffDbService {
    fn get_commit(
        &self,
        context: RequestContext,
        request: GetCommitRequest,
    ) -> ServiceFuture<'_, GetCommitResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::GetCommit, ingress, async move {
            get_commit(service, context, request).await
        })
    }

    fn scan_commits(
        &self,
        context: RequestContext,
        request: ScanCommitsRequest,
    ) -> ServiceFuture<'_, ScanCommitsResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::ScanCommits, ingress, async move {
            scan_commits(service, context, request).await
        })
    }

    fn subscribe_to_commits(
        &self,
        context: RequestContext,
        request: SubscribeToCommitsRequest,
    ) -> ServiceFuture<'_, SubscribeToCommitsResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(
            ServiceOperationV1::SubscribeToCommits,
            ingress,
            async move { subscribe_to_commits(service, context, request).await },
        )
    }

    fn trace_provenance(
        &self,
        context: RequestContext,
        request: TraceProvenanceRequest,
    ) -> ServiceFuture<'_, TraceProvenanceResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::TraceProvenance, ingress, async move {
            trace_provenance(service, context, request).await
        })
    }
}

fn check_observed_history_incarnation(
    service: &RiffDbServiceInner,
    observed: Option<u64>,
) -> ServiceResult<()> {
    if let Some(observed) = observed
        && observed != service.identity.history_incarnation()
    {
        return Err(ServiceFailure::Public(
            PublicError::history_incarnation_mismatch(),
        ));
    }
    Ok(())
}

async fn get_commit(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: GetCommitRequest,
) -> ServiceResult<GetCommitResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::GetCommit;
    check_observed_history_incarnation(&service, request.observed_history_incarnation())?;
    let sequence = request.sequence();
    let targets = ServiceAuditTargetMap::get_commit(sequence)
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let policy_request = OperationRequest::get_commit(sequence);
    let begun = service
        .begin_invocation(&context, policy_request, targets, AuditScope::Intrinsic)
        .await?;

    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .authoritative
            .reserve_read_commit(context.control()),
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
    if !valid_administrative_authorization(
        &service,
        &authorization,
        ServiceOperationV1::GetCommit,
        false,
    ) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }

    let receipt = match permit.submit(sequence) {
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
        Ok(Ok(Ok(snapshot))) => snapshot,
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

    if let Some(snapshot) = snapshot.as_ref() {
        if snapshot.sequence() != sequence {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        if let Err(error) = validate_affected_entity_keys(
            &service,
            &context,
            snapshot.lineage(),
            snapshot.contract_version(),
            snapshot.affected_entities(),
        )
        .await
        {
            return Err(finish_output_key_validation_failure(
                &service, &context, &begun, OPERATION, error,
            )
            .await);
        }
    }

    let return_authorization = begun.reauthorize(&service, &context).await?;
    if !valid_administrative_authorization(
        &service,
        &return_authorization,
        ServiceOperationV1::GetCommit,
        false,
    ) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }

    let result = match snapshot {
        Some(snapshot) => GetCommitResult::Found(Box::new(redact_commit(snapshot))),
        None => GetCommitResult::NotFound,
    };

    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    finish_success(&service, &context, &begun).await?;
    Ok(result)
}

async fn scan_commits(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: ScanCommitsRequest,
) -> ServiceResult<ScanCommitsResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ScanCommits;
    check_observed_history_incarnation(&service, request.observed_history_incarnation())?;
    let page_request = request.page();
    let policy_request = OperationRequest::scan_commits(page_request.limit().get());
    let begun = service
        .begin_invocation(
            &context,
            policy_request,
            ServiceAuditTargetMap::scan_commits(),
            AuditScope::Intrinsic,
        )
        .await?;

    if !valid_administrative_authorization(&service, begun.initial_authorization(), OPERATION, true)
    {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }

    let cursor_lookup = CommitScanCursorLookup::new(page_request.limit());
    let cursor_state = match page_request.cursor() {
        Some(token) => match service.cursors.resolve_commit_scan(
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
    let initial_limit =
        match effective_page_limit(page_request.limit(), begun.initial_authorization()) {
            Some(limit) => limit,
            None => {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        };
    let initial_policy = match constrain_commit_scan_policy(
        begun
            .initial_authorization()
            .obligations()
            .effective_tenant_scope(),
        begun
            .initial_authorization()
            .obligations()
            .partition_constraint(),
        initial_limit,
        cursor_state.as_deref().map(CommitScanCursorState::policy),
    ) {
        Some(policy) => policy,
        None => {
            return Err(finish_failure(&service, &context, &begun, invalid_cursor_failure()).await);
        }
    };

    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .authoritative
            .reserve_scan_commits(context.control()),
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
    if !valid_administrative_authorization(&service, &authorization, OPERATION, true) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    let current_limit = match effective_page_limit(page_request.limit(), &authorization) {
        Some(limit) => limit,
        None => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let effective_policy = match constrain_commit_scan_policy(
        authorization.obligations().effective_tenant_scope(),
        authorization.obligations().partition_constraint(),
        current_limit,
        Some(&initial_policy),
    ) {
        Some(policy) => policy,
        None => {
            return Err(finish_failure(&service, &context, &begun, invalid_cursor_failure()).await);
        }
    };
    let effective_limit = effective_policy.effective_limit();
    let lower_request = match commit_scan_request(cursor_state.as_deref(), effective_limit) {
        Ok(request) => request,
        Err(()) => {
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
    let lower_page = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(page))) => page,
        Ok(Ok(Err(error))) => {
            let failure = match error {
                AuthoritativeReadError::InvalidContinuation if cursor_state.is_some() => {
                    invalid_cursor_failure()
                }
                error => authoritative_read_failure(&service, OPERATION, error),
            };
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

    if !commit_page_matches_request(lower_request, &lower_page) {
        let failure = lower_integrity_failure(&service, OPERATION);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }

    if let Err(error) = validate_commit_page_keys(&service, &context, lower_page.commits()).await {
        return Err(finish_output_key_validation_failure(
            &service, &context, &begun, OPERATION, error,
        )
        .await);
    }

    let return_authorization = begun.reauthorize(&service, &context).await?;
    if !valid_administrative_authorization(
        &service,
        &return_authorization,
        ServiceOperationV1::ScanCommits,
        true,
    ) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    let return_limit = match effective_page_limit(page_request.limit(), &return_authorization) {
        Some(limit) => limit,
        None => {
            let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let return_policy = match constrain_commit_scan_policy(
        return_authorization.obligations().effective_tenant_scope(),
        return_authorization.obligations().partition_constraint(),
        return_limit,
        Some(&effective_policy),
    ) {
        Some(policy) => policy,
        None => {
            return Err(begun.finish_authorization_denial(&service, &context).await);
        }
    };
    let return_limit = return_policy.effective_limit();

    let mut views: Vec<_> = lower_page
        .commits()
        .iter()
        .cloned()
        .map(redact_commit)
        .collect();
    let more_due_to_limit = views.len() > usize::from(return_limit.get().get());
    views.truncate(usize::from(return_limit.get().get()));
    let fence = CommitScanFence::new(lower_page.inclusive_upper());
    let fit = match fit_page_items(
        &views,
        &fence,
        more_due_to_limit || lower_page.next_after().is_some(),
    ) {
        Ok(fit) => fit,
        Err(failure) => {
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let continuation_after = if fit.has_more() {
        if fit.item_count() < views.len() || more_due_to_limit {
            views
                .get(fit.item_count() - 1)
                .map(|commit| commit.as_snapshot().sequence())
        } else {
            lower_page.next_after()
        }
    } else {
        None
    };
    views.truncate(fit.item_count());
    let cursor_guard = match continuation_after {
        Some(after) => {
            let FrontierPosition::AppliedThrough(inclusive_upper) = lower_page.inclusive_upper()
            else {
                let failure = lower_integrity_failure(&service, OPERATION);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            };
            let state =
                match CommitScanCursorState::new(after, inclusive_upper, return_policy.clone()) {
                    Ok(state) => state,
                    Err(_) => {
                        let failure = lower_integrity_failure(&service, OPERATION);
                        return Err(finish_failure(&service, &context, &begun, failure).await);
                    }
                };
            match service.cursors.register_commit_scan_unpublished(
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
    let public_page = match Page::new(return_limit, views, next_cursor, fence) {
        Ok(page) => page,
        Err(_) => {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
    };
    let result = ScanCommitsResult::new(public_page);
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    finish_success(&service, &context, &begun).await?;
    if let Some(guard) = cursor_guard {
        guard.publish();
    }
    Ok(result)
}

async fn subscribe_to_commits(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: SubscribeToCommitsRequest,
) -> ServiceResult<SubscribeToCommitsResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::SubscribeToCommits;
    check_observed_history_incarnation(&service, request.observed_history_incarnation())?;
    let policy_request = OperationRequest::subscribe_to_commits();
    let begun = service
        .begin_invocation(
            &context,
            policy_request.clone(),
            ServiceAuditTargetMap::subscribe_to_commits(),
            AuditScope::Intrinsic,
        )
        .await?;

    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .authoritative
            .reserve_subscribe_to_commits(context.control()),
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
    if !valid_administrative_authorization(
        &service,
        &authorization,
        ServiceOperationV1::SubscribeToCommits,
        false,
    ) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }

    let lower_request =
        match AuthoritativeCommitSubscriptionRequest::new(request.after(), PageLimit::default()) {
            Ok(request) => request,
            Err(_) => {
                let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
                return Err(finish_failure(&service, &context, &begun, failure).await);
            }
        };
    let subscriber_lease = match service.reserve_commit_subscriber() {
        Ok(lease) => lease,
        Err(_) => {
            return Err(finish_failure(
                &service,
                &context,
                &begun,
                PublicError::storage_unavailable().into(),
            )
            .await);
        }
    };
    let receipt = match permit.submit(lower_request) {
        Ok(receipt) => receipt,
        Err(error) => {
            return Err(finish_admission_failure(&service, &context, &begun, error).await);
        }
    };
    let source = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(source))) => source,
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

    let return_authorization = begun.reauthorize(&service, &context).await?;
    if !valid_administrative_authorization(
        &service,
        &return_authorization,
        ServiceOperationV1::SubscribeToCommits,
        false,
    ) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }

    let Some(lifetime_deadline) = Instant::now().checked_add(request.maximum_lifetime()) else {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    };
    let subscription = ServiceCommitSubscription {
        service: Arc::clone(&service),
        context,
        source: Some(source),
        policy_request,
        lifetime_deadline,
        last_delivered: request.after().map_or(
            FrontierPosition::BeforeFirst,
            FrontierPosition::AppliedThrough,
        ),
        pending_upper: None,
        terminal: None,
        response_too_large: false,
        subscriber_lease: Some(subscriber_lease),
    };

    if let Err(failure) = ensure_subscription_establishment_budget() {
        return Err(finish_failure(&service, &subscription.context, &begun, failure).await);
    }
    finish_success(&service, &subscription.context, &begun).await?;
    Ok(SubscribeToCommitsResult::new(Box::new(subscription)))
}

async fn trace_provenance(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: TraceProvenanceRequest,
) -> ServiceResult<TraceProvenanceResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::TraceProvenance;
    let selector = provenance_selector(request.selector());
    let targets = ServiceAuditTargetMap::trace_provenance(selector)
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let policy_request = OperationRequest::trace_provenance(selector);
    let begun = service
        .begin_invocation(&context, policy_request, targets, AuditScope::Intrinsic)
        .await?;

    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .authoritative
            .reserve_trace_provenance(context.control()),
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
    if !valid_administrative_authorization(
        &service,
        &authorization,
        ServiceOperationV1::TraceProvenance,
        false,
    ) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }

    let receipt = match permit.submit(selector) {
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
        Ok(Ok(Ok(snapshot))) => snapshot,
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

    if let Some(snapshot) = snapshot.as_ref() {
        if !provenance_matches(selector, snapshot) {
            let failure = lower_integrity_failure(&service, OPERATION);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }
        if let Err(error) = validate_affected_entity_keys(
            &service,
            &context,
            snapshot.lineage(),
            snapshot.contract_version(),
            snapshot.affected_entities(),
        )
        .await
        {
            return Err(finish_output_key_validation_failure(
                &service, &context, &begun, OPERATION, error,
            )
            .await);
        }
    }

    let return_authorization = begun.reauthorize(&service, &context).await?;
    if !valid_administrative_authorization(
        &service,
        &return_authorization,
        ServiceOperationV1::TraceProvenance,
        false,
    ) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }

    let result = match snapshot {
        Some(snapshot) => TraceProvenanceResult::Found(Box::new(ProvenanceView::new(snapshot))),
        None => TraceProvenanceResult::NotFound,
    };
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    finish_success(&service, &context, &begun).await?;
    Ok(result)
}

fn provenance_selector(selection: ProvenanceSelection) -> ProvenanceSelector {
    match selection {
        ProvenanceSelection::Commit(sequence) => ProvenanceSelector::Commit(sequence),
        ProvenanceSelection::Provenance(provenance_id) => {
            ProvenanceSelector::Provenance(provenance_id)
        }
    }
}

fn provenance_matches(
    selector: ProvenanceSelector,
    snapshot: &crate::AuthoritativeProvenanceSnapshot,
) -> bool {
    match selector {
        ProvenanceSelector::Commit(sequence) => snapshot.commit_sequence() == sequence,
        ProvenanceSelector::Provenance(provenance_id) => snapshot.provenance_id() == provenance_id,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutputKeyValidationError {
    Controlled(ControlledWaitError),
    Unavailable,
    Integrity,
}

async fn validate_commit_page_keys(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    snapshots: &[AuthoritativeCommitSnapshot],
) -> Result<(), OutputKeyValidationError> {
    let mut bundles: Vec<(ContractLineage, ContractVersion, ValidatedContractBundle)> = Vec::new();
    for snapshot in snapshots {
        if snapshot.affected_entities().is_empty() {
            continue;
        }
        let lineage = snapshot.lineage();
        let version = snapshot.contract_version();
        let bundle = match bundles.iter().find(|(cached_lineage, cached_version, _)| {
            cached_lineage == lineage && *cached_version == version
        }) {
            Some((_, _, bundle)) => bundle.clone(),
            None => {
                let bundle = prepare_output_bundle(service, context, lineage, version).await?;
                bundles.push((lineage.clone(), version, bundle.clone()));
                bundle
            }
        };
        if !bundle_validates_affected_entities(
            &bundle,
            lineage,
            version,
            snapshot.affected_entities(),
        ) {
            return Err(OutputKeyValidationError::Integrity);
        }
    }
    Ok(())
}

async fn validate_affected_entity_keys(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    lineage: &ContractLineage,
    version: ContractVersion,
    affected_entities: &[AffectedEntityView],
) -> Result<(), OutputKeyValidationError> {
    if affected_entities.is_empty() {
        return Ok(());
    }
    let bundle = prepare_output_bundle(service, context, lineage, version).await?;
    if !bundle_validates_affected_entities(&bundle, lineage, version, affected_entities) {
        return Err(OutputKeyValidationError::Integrity);
    }
    Ok(())
}

async fn prepare_output_bundle(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    lineage: &ContractLineage,
    version: ContractVersion,
) -> Result<ValidatedContractBundle, OutputKeyValidationError> {
    let observed = wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service.providers.catalog.prepare_contract_version(
            context.control(),
            lineage.clone(),
            version,
        ),
    )
    .await
    .map_err(OutputKeyValidationError::Controlled)?
    .map_err(|error| {
        if error.kind() == CatalogErrorKind::Storage {
            OutputKeyValidationError::Unavailable
        } else {
            OutputKeyValidationError::Integrity
        }
    })?;
    let bundle = observed.ok_or(OutputKeyValidationError::Integrity)?;
    if bundle.lineage() != lineage || bundle.contract_version() != version {
        return Err(OutputKeyValidationError::Integrity);
    }
    Ok(bundle)
}

fn bundle_validates_affected_entities(
    bundle: &ValidatedContractBundle,
    lineage: &ContractLineage,
    version: ContractVersion,
    affected_entities: &[AffectedEntityView],
) -> bool {
    bundle.lineage() == lineage
        && bundle.contract_version() == version
        && affected_entities.iter().all(|affected| {
            bundle
                .bundle()
                .schema()
                .entity(affected.key().entity_type_id())
                .is_some_and(|entity| entity.primary_key().decode_entity(affected.key()).is_ok())
        })
}

async fn finish_output_key_validation_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    operation: ServiceOperationV1,
    error: OutputKeyValidationError,
) -> ServiceFailure {
    match error {
        OutputKeyValidationError::Controlled(error) => {
            finish_controlled_wait(service, context, begun, error).await
        }
        OutputKeyValidationError::Unavailable => {
            finish_failure(
                service,
                context,
                begun,
                PublicError::storage_unavailable().into(),
            )
            .await
        }
        OutputKeyValidationError::Integrity => {
            let failure = lower_integrity_failure(service, operation);
            finish_failure(service, context, begun, failure).await
        }
    }
}

fn redact_commit(snapshot: AuthoritativeCommitSnapshot) -> CommitView {
    // The lower value is already the service-owned bounded semantic DTO rather
    // than a durable record or storage handle. Administrative authorization
    // permits this fixed public-redacted shape; wrapping occurs only after its
    // exact output-classification obligation has been checked.
    CommitView::new(snapshot)
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
        && obligations.partition_constraint()
            == Some(&PartitionConstraint::Filter(PartitionScopeV1::All))
        && obligations.field_mask().is_none()
        && (permits_row_limit || obligations.row_limit().is_none())
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

fn constrain_commit_scan_policy(
    current_tenant_scope: &riffdb_types::TenantScope,
    current_partition_constraint: Option<&PartitionConstraint>,
    current_limit: PageLimit,
    prior: Option<&CommitScanCursorPolicy>,
) -> Option<CommitScanCursorPolicy> {
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
    Some(CommitScanCursorPolicy::new(
        current_tenant_scope.clone(),
        current_partition_constraint.clone(),
        effective_limit,
    ))
}

fn commit_scan_request(
    cursor: Option<&CommitScanCursorState>,
    effective_limit: PageLimit,
) -> Result<AuthoritativeCommitScanRequest, ()> {
    match cursor {
        Some(cursor) => AuthoritativeCommitScanRequest::continuing(
            cursor.after(),
            cursor.inclusive_upper(),
            effective_limit,
        )
        .map_err(|_| ()),
        None => Ok(AuthoritativeCommitScanRequest::initial(effective_limit)),
    }
}

fn commit_page_matches_request(
    request: AuthoritativeCommitScanRequest,
    page: &crate::AuthoritativeCommitPage,
) -> bool {
    commit_page_shape_matches_request(
        request,
        page.inclusive_upper(),
        page.commits()
            .iter()
            .map(AuthoritativeCommitSnapshot::sequence),
        page.next_after(),
    )
}

fn commit_page_shape_matches_request<Sequences>(
    request: AuthoritativeCommitScanRequest,
    inclusive_upper: FrontierPosition,
    sequences: Sequences,
    next_after: Option<riffdb_types::CommitSequence>,
) -> bool
where
    Sequences: Clone + DoubleEndedIterator<Item = riffdb_types::CommitSequence> + ExactSizeIterator,
{
    if sequences.len() > usize::from(request.limit().get().get()) {
        return false;
    }

    let upper = match inclusive_upper {
        FrontierPosition::BeforeFirst => None,
        FrontierPosition::AppliedThrough(sequence) => Some(sequence),
    };
    if request
        .inclusive_upper()
        .is_some_and(|expected| upper != Some(expected))
    {
        return false;
    }

    let mut expected = match request.after() {
        Some(after) => after.checked_next(),
        None => Some(riffdb_types::CommitSequence::first()),
    };
    let last = sequences.clone().next_back();
    for sequence in sequences {
        if expected != Some(sequence) || upper.is_some_and(|upper| sequence > upper) {
            return false;
        }
        expected = sequence.checked_next();
    }

    match (upper, last.or(request.after())) {
        (None, None) => last.is_none() && next_after.is_none(),
        (None, Some(_)) => false,
        (Some(upper), Some(position)) if position == upper => next_after.is_none(),
        (Some(upper), Some(position)) if position < upper => last.is_some() && next_after == last,
        (Some(_), Some(_)) => false,
        (Some(_), None) => false,
    }
}

fn invalid_cursor_failure() -> ServiceFailure {
    PublicError::validation(ValidationIssues::one(ValidationIssue::new(
        ValidationCode::InvalidValue,
        ValidationPath::root(),
    )))
    .into()
}

fn cursor_unavailable_failure(service: &RiffDbServiceInner) -> ServiceFailure {
    service
        .providers
        .telemetry
        .record(ServiceTelemetryEvent::CursorUnavailable);
    PublicError::storage_unavailable().into()
}

async fn finish_success(
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

async fn finish_failure(
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
    let failure = match error {
        ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
        ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
    };
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

struct ServiceCommitSubscription {
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    source: Option<Box<dyn crate::CommitNotificationSource>>,
    policy_request: OperationRequest,
    lifetime_deadline: Instant,
    last_delivered: FrontierPosition,
    pending_upper: Option<riffdb_types::CommitSequence>,
    terminal: Option<CommitSubscriptionTerminal>,
    response_too_large: bool,
    subscriber_lease: Option<CommitSubscriberLease>,
}

impl CommitSubscription for ServiceCommitSubscription {
    fn next(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = ServiceResult<CommitSubscriptionEvent>> + Send + '_>> {
        Box::pin(async move {
            let prior_last_delivered = self.last_delivered;
            let prior_pending_upper = self.pending_upper;

            let observed = catch_continuation_panic(async {
                if self.response_too_large {
                    return Err(ServiceFailure::ResponseTooLarge);
                }
                let event = self.next_event().await;
                if let Err(failure) = ensure_response_budget(&event) {
                    self.last_delivered = prior_last_delivered;
                    self.pending_upper = prior_pending_upper;
                    self.response_too_large = true;
                    self.release_stream_resources();
                    return Err(failure);
                }
                if let CommitSubscriptionEvent::Commit(commit) = &event {
                    let acknowledgement = self
                        .source
                        .as_mut()
                        .map_or(Err(AuthoritativeReadError::Integrity), |source| {
                            source.acknowledge(commit.as_snapshot().sequence())
                        });
                    if let Err(error) = acknowledgement {
                        self.last_delivered = prior_last_delivered;
                        self.pending_upper = prior_pending_upper;
                        let terminal = match error {
                            AuthoritativeReadError::Unavailable
                            | AuthoritativeReadError::Cancelled
                            | AuthoritativeReadError::DeadlineExceeded => {
                                self.end(CommitSubscriptionEndReason::Unavailable)
                            }
                            // Prune is offline-only: an acknowledged live
                            // commit can never be pruned under this handle.
                            AuthoritativeReadError::Integrity
                            | AuthoritativeReadError::InvalidContinuation
                            | AuthoritativeReadError::HistoryPruned => self.end_integrity(),
                        };
                        ensure_response_budget(&terminal)?;
                        return Ok(terminal);
                    }
                }
                Ok(event)
            })
            .await;

            match observed {
                Ok(result) => result,
                Err(()) => {
                    // The failed item never became externally visible. Freeze the
                    // continuation at the last safely delivered frontier before
                    // invoking any diagnostic hook that could itself fail.
                    self.last_delivered = prior_last_delivered;
                    self.pending_upper = prior_pending_upper;
                    self.response_too_large = false;
                    self.terminal = Some(CommitSubscriptionTerminal::new(
                        CommitSubscriptionEndReason::Unavailable,
                        prior_last_delivered,
                    ));
                    self.release_stream_resources_contained();
                    Err(self.service.internal_failure(
                        ServiceOperationV1::SubscribeToCommits,
                        InternalDefect::Panic,
                    ))
                }
            }
        })
    }
}

impl ServiceCommitSubscription {
    async fn next_event(&mut self) -> CommitSubscriptionEvent {
        if let Some(terminal) = self.terminal {
            return CommitSubscriptionEvent::Terminal(terminal);
        }

        let scheduler = Arc::clone(&self.service.providers.deadline_scheduler);
        let sequence = loop {
            let Some(expected) = self.expected_next() else {
                return self.end_integrity();
            };
            if let Some(upper) = self.pending_upper {
                if expected <= upper {
                    break expected;
                }
                return self.end_integrity();
            }

            let Some(source) = self.source.as_mut() else {
                return self.end_integrity();
            };
            let notification = match wait_stream(
                self.context.control(),
                scheduler.as_ref(),
                self.lifetime_deadline,
                source.next(),
            )
            .await
            {
                Ok(Ok(notification)) => notification,
                // A pruned resume point ends the contiguous scan exactly like
                // a gap: the subscriber must restart above the watermark.
                Ok(Err(
                    AuthoritativeReadError::InvalidContinuation
                    | AuthoritativeReadError::HistoryPruned,
                )) => {
                    return self.end(CommitSubscriptionEndReason::ScanGap);
                }
                Ok(Err(
                    AuthoritativeReadError::Unavailable
                    | AuthoritativeReadError::Cancelled
                    | AuthoritativeReadError::DeadlineExceeded,
                )) => {
                    return self.end(CommitSubscriptionEndReason::Unavailable);
                }
                Ok(Err(AuthoritativeReadError::Integrity)) => {
                    return self.end_integrity();
                }
                Err(error) => return self.end_wait(error),
            };

            match notification {
                AuthoritativeCommitNotification::Advanced(upper) if upper >= expected => {
                    self.pending_upper = Some(upper);
                }
                // A coalesced source may leave a stale wake queued. It conveys
                // no new item and must neither duplicate output nor create a gap.
                AuthoritativeCommitNotification::Advanced(_) => {}
                AuthoritativeCommitNotification::Gap { resume_after } => {
                    if resume_after != self.last_delivered {
                        return self.end_integrity();
                    }
                    return self.end(CommitSubscriptionEndReason::ScanGap);
                }
                AuthoritativeCommitNotification::Lagged { resume_after } => {
                    if resume_after != self.last_delivered {
                        return self.end_integrity();
                    }
                    return self.end(CommitSubscriptionEndReason::Lagged);
                }
                AuthoritativeCommitNotification::Closed => {
                    return self.end(CommitSubscriptionEndReason::ServiceShutdown);
                }
            }
        };

        if let Err(reason) = self.authorize_item() {
            return self.end_authorization(reason);
        }

        let permit = match wait_stream(
            self.context.control(),
            scheduler.as_ref(),
            self.lifetime_deadline,
            self.service
                .providers
                .authoritative
                .reserve_read_commit(self.context.control()),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(error)) => return self.end_port_admission(error),
            Err(error) => return self.end_wait(error),
        };

        // Capacity waits invalidate the preceding allow. This synchronous check
        // is the final safe point before the held permit is consumed.
        if let Err(reason) = self.authorize_item() {
            return self.end_authorization(reason);
        }
        let receipt = match permit.submit(sequence) {
            Ok(receipt) => receipt,
            Err(error) => return self.end_port_admission(error),
        };
        let snapshot = match wait_stream(
            self.context.control(),
            scheduler.as_ref(),
            self.lifetime_deadline,
            receipt,
        )
        .await
        {
            Ok(Ok(Ok(Some(snapshot)))) => snapshot,
            Ok(Ok(Ok(None)))
            // Prune is offline-only: a frontier-advanced commit can never be
            // pruned under this live handle.
            | Ok(Ok(Err(
                AuthoritativeReadError::Integrity
                | AuthoritativeReadError::InvalidContinuation
                | AuthoritativeReadError::HistoryPruned,
            )))
            | Ok(Err(PortDriverStopped)) => return self.end_integrity(),
            Ok(Ok(Err(
                AuthoritativeReadError::Unavailable
                | AuthoritativeReadError::Cancelled
                | AuthoritativeReadError::DeadlineExceeded,
            ))) => {
                return self.end(CommitSubscriptionEndReason::Unavailable);
            }
            Err(error) => return self.end_wait(error),
        };
        if snapshot.sequence() != sequence {
            return self.end_integrity();
        }
        if let Err(error) = self.validate_snapshot_keys(&snapshot).await {
            return match error {
                StreamOutputKeyValidationError::Wait(error) => self.end_wait(error),
                StreamOutputKeyValidationError::Unavailable => {
                    self.end(CommitSubscriptionEndReason::Unavailable)
                }
                StreamOutputKeyValidationError::Integrity => self.end_integrity(),
            };
        }

        // The read and catalog completions are wakes. Recheck current policy
        // after both and before the first byte of this item can become visible.
        if let Err(reason) = self.authorize_item() {
            return self.end_authorization(reason);
        }
        self.last_delivered = FrontierPosition::AppliedThrough(sequence);
        if self.pending_upper == Some(sequence) {
            self.pending_upper = None;
        }
        CommitSubscriptionEvent::Commit(Box::new(redact_commit(snapshot)))
    }

    async fn validate_snapshot_keys(
        &mut self,
        snapshot: &AuthoritativeCommitSnapshot,
    ) -> Result<(), StreamOutputKeyValidationError> {
        if snapshot.affected_entities().is_empty() {
            return Ok(());
        }
        let observed = wait_stream(
            self.context.control(),
            self.service.providers.deadline_scheduler.as_ref(),
            self.lifetime_deadline,
            self.service.providers.catalog.prepare_contract_version(
                self.context.control(),
                snapshot.lineage().clone(),
                snapshot.contract_version(),
            ),
        )
        .await
        .map_err(StreamOutputKeyValidationError::Wait)?
        .map_err(|error| {
            if error.kind() == CatalogErrorKind::Storage {
                StreamOutputKeyValidationError::Unavailable
            } else {
                StreamOutputKeyValidationError::Integrity
            }
        })?;
        let bundle = observed.ok_or(StreamOutputKeyValidationError::Integrity)?;
        if !bundle_validates_affected_entities(
            &bundle,
            snapshot.lineage(),
            snapshot.contract_version(),
            snapshot.affected_entities(),
        ) {
            return Err(StreamOutputKeyValidationError::Integrity);
        }
        Ok(())
    }

    fn authorize_item(&self) -> Result<(), StreamAuthorizationEnd> {
        match self
            .service
            .providers
            .policy
            .authorize(self.context.principal(), self.policy_request.clone())
        {
            Ok(Decision::Allow(authorization))
                if valid_administrative_authorization(
                    &self.service,
                    &authorization,
                    ServiceOperationV1::SubscribeToCommits,
                    false,
                ) =>
            {
                Ok(())
            }
            Ok(Decision::Deny(_)) => Err(StreamAuthorizationEnd::Denied),
            Ok(Decision::Allow(_) | Decision::PrepareCapabilityMutation(_)) => {
                Err(StreamAuthorizationEnd::Integrity)
            }
            Err(_) => Err(StreamAuthorizationEnd::Unavailable),
        }
    }

    fn expected_next(&self) -> Option<riffdb_types::CommitSequence> {
        match self.last_delivered {
            FrontierPosition::BeforeFirst => Some(riffdb_types::CommitSequence::first()),
            FrontierPosition::AppliedThrough(sequence) => sequence.checked_next(),
        }
    }

    fn end_wait(&mut self, error: StreamWaitError) -> CommitSubscriptionEvent {
        self.end(match error {
            StreamWaitError::Cancelled => CommitSubscriptionEndReason::Cancelled,
            StreamWaitError::RequestDeadline => CommitSubscriptionEndReason::DeadlineExceeded,
            StreamWaitError::Lifetime => CommitSubscriptionEndReason::LifetimeElapsed,
        })
    }

    fn end_port_admission(&mut self, error: PortAdmissionError) -> CommitSubscriptionEvent {
        self.end(match error {
            PortAdmissionError::Cancelled => CommitSubscriptionEndReason::Cancelled,
            PortAdmissionError::DeadlineExceeded => CommitSubscriptionEndReason::DeadlineExceeded,
            PortAdmissionError::Unavailable | PortAdmissionError::Stopped => {
                CommitSubscriptionEndReason::Unavailable
            }
        })
    }

    fn end_authorization(&mut self, reason: StreamAuthorizationEnd) -> CommitSubscriptionEvent {
        match reason {
            StreamAuthorizationEnd::Denied => {
                self.service
                    .providers
                    .telemetry
                    .record(ServiceTelemetryEvent::StreamClosedByPolicy);
                self.end(CommitSubscriptionEndReason::PolicyDenied)
            }
            StreamAuthorizationEnd::Unavailable => {
                self.end(CommitSubscriptionEndReason::Unavailable)
            }
            StreamAuthorizationEnd::Integrity => self.end_integrity(),
        }
    }

    fn end_integrity(&mut self) -> CommitSubscriptionEvent {
        self.service
            .providers
            .health
            .fail_authoritative_readiness(AuthoritativeReadinessFailure::Integrity);
        self.end(CommitSubscriptionEndReason::Unavailable)
    }

    fn end(&mut self, reason: CommitSubscriptionEndReason) -> CommitSubscriptionEvent {
        let terminal = CommitSubscriptionTerminal::new(reason, self.last_delivered);
        self.terminal = Some(terminal);
        self.release_stream_resources();
        CommitSubscriptionEvent::Terminal(terminal)
    }

    fn release_stream_resources(&mut self) {
        let source = self.source.take();
        let subscriber_lease = self.subscriber_lease.take();
        drop(subscriber_lease);
        drop(source);
    }

    fn release_stream_resources_contained(&mut self) {
        let source = self.source.take();
        let subscriber_lease = self.subscriber_lease.take();
        drop(subscriber_lease);
        let _ = catch_unwind(AssertUnwindSafe(|| drop(source)));
    }
}

#[derive(Clone, Copy)]
enum StreamAuthorizationEnd {
    Denied,
    Unavailable,
    Integrity,
}

#[derive(Clone, Copy)]
enum StreamOutputKeyValidationError {
    Wait(StreamWaitError),
    Unavailable,
    Integrity,
}

#[derive(Clone, Copy)]
enum StreamWaitError {
    Cancelled,
    RequestDeadline,
    Lifetime,
}

async fn wait_stream<F>(
    control: &crate::RequestControl,
    deadline_scheduler: &dyn crate::RequestDeadlineScheduler,
    lifetime_deadline: Instant,
    future: F,
) -> Result<F::Output, StreamWaitError>
where
    F: Future,
{
    let mut future = Box::pin(future);
    let mut cancelled = Box::pin(control.cancelled());
    let mut request_deadline = deadline_scheduler.wait_until(control.deadline());
    let mut lifetime = deadline_scheduler.wait_until(lifetime_deadline);

    poll_fn(|context| {
        if control.is_cancelled() || Pin::as_mut(&mut cancelled).poll(context).is_ready() {
            return Poll::Ready(Err(StreamWaitError::Cancelled));
        }
        if control.is_deadline_exceeded()
            || Pin::as_mut(&mut request_deadline).poll(context).is_ready()
        {
            return Poll::Ready(Err(StreamWaitError::RequestDeadline));
        }
        if Pin::as_mut(&mut lifetime).poll(context).is_ready() {
            return Poll::Ready(Err(StreamWaitError::Lifetime));
        }
        Pin::as_mut(&mut future).poll(context).map(Ok)
    })
    .await
}

#[cfg(test)]
mod tests {
    use riffdb_errors::{ApplicationErrorCode, PublicError, PublicErrorDetails, PublicErrorKind};
    use riffdb_types::{CommitSequence, TenantId, TenantScope};

    use super::*;
    use crate::PageRequest;

    #[test]
    fn history_incarnation_mismatch_is_rdb_history_0101_on_all_three_request_shapes() {
        // Wire-level public error identity shared by GetCommit / ScanCommits /
        // SubscribeCommits (check_observed_history_incarnation).
        let error = PublicError::history_incarnation_mismatch();
        assert_eq!(error.kind(), PublicErrorKind::HistoryIncarnationMismatch);
        assert_eq!(
            ApplicationErrorCode::from_public_kind(error.kind()).as_str(),
            "RDB-HISTORY-0101"
        );
        assert_eq!(
            error.safe_message(),
            "observed history predates a database restore"
        );
        // The three request builders all accept an observed fence; mismatch is
        // independent of sequence/page contents.
        let get = GetCommitRequest::new(CommitSequence::first())
            .with_observed_history_incarnation(Some(1));
        assert_eq!(get.observed_history_incarnation(), Some(1));
        let page = PageRequest::new(PageLimit::new(1).expect("limit"), None);
        let scan = ScanCommitsRequest::new(page).with_observed_history_incarnation(Some(1));
        assert_eq!(scan.observed_history_incarnation(), Some(1));
        let subscribe = SubscribeToCommitsRequest::new(None, std::time::Duration::from_secs(1))
            .expect("subscribe")
            .with_observed_history_incarnation(Some(1));
        assert_eq!(subscribe.observed_history_incarnation(), Some(1));
    }

    #[test]
    fn stored_commit_policy_can_only_hold_or_narrow() {
        let requested = PageLimit::new(50).expect("bounded request");
        let prior_limit = PageLimit::new(20).expect("bounded prior limit");
        let narrower = PageLimit::new(10).expect("bounded narrower limit");
        let all_partitions = PartitionConstraint::Filter(PartitionScopeV1::All);
        let prior =
            CommitScanCursorPolicy::new(TenantScope::Global, all_partitions.clone(), prior_limit);

        let held = constrain_commit_scan_policy(
            &TenantScope::Global,
            Some(&all_partitions),
            requested,
            Some(&prior),
        )
        .expect("same policy remains valid");
        assert_eq!(held.effective_limit(), prior_limit);
        assert_eq!(held.effective_tenant_scope(), &TenantScope::Global);
        assert_eq!(held.partition_constraint(), &all_partitions);

        let narrowed = constrain_commit_scan_policy(
            &TenantScope::Global,
            Some(&all_partitions),
            narrower,
            Some(&prior),
        )
        .expect("lower row policy remains valid");
        assert_eq!(narrowed.effective_limit(), narrower);

        let tenant = TenantScope::Tenant(TenantId::new("tenant-a").expect("bounded tenant"));
        assert!(
            constrain_commit_scan_policy(&tenant, Some(&all_partitions), narrower, Some(&prior))
                .is_none()
        );
        assert!(
            constrain_commit_scan_policy(&TenantScope::Global, None, narrower, Some(&prior))
                .is_none()
        );
    }

    #[test]
    fn stored_commit_cursor_drives_lower_continuation_and_fence() {
        let first = CommitSequence::first();
        let second = first.checked_next().expect("second sequence");
        let third = second.checked_next().expect("third sequence");
        let limit = PageLimit::new(7).expect("bounded limit");
        let state = CommitScanCursorState::new(
            first,
            third,
            CommitScanCursorPolicy::new(
                TenantScope::Global,
                PartitionConstraint::Filter(PartitionScopeV1::All),
                limit,
            ),
        )
        .expect("ordered cursor state");

        assert_eq!(
            commit_scan_request(Some(&state), limit),
            Ok(AuthoritativeCommitScanRequest::Continue {
                after: first,
                inclusive_upper: third,
                limit,
            })
        );
        assert_eq!(
            commit_scan_request(None, limit),
            Ok(AuthoritativeCommitScanRequest::Initial { limit })
        );
    }

    #[test]
    fn commit_page_shape_must_be_contiguous_and_truthful_about_more_rows() {
        let first = CommitSequence::first();
        let second = first.checked_next().expect("second sequence");
        let third = second.checked_next().expect("third sequence");
        let fourth = third.checked_next().expect("fourth sequence");
        let limit = PageLimit::new(2).expect("bounded limit");
        let initial = AuthoritativeCommitScanRequest::initial(limit);

        assert!(commit_page_shape_matches_request(
            initial,
            FrontierPosition::AppliedThrough(third),
            [first, second].into_iter(),
            Some(second),
        ));
        assert!(!commit_page_shape_matches_request(
            initial,
            FrontierPosition::AppliedThrough(third),
            [first, second].into_iter(),
            None,
        ));
        assert!(!commit_page_shape_matches_request(
            initial,
            FrontierPosition::AppliedThrough(third),
            [second, third].into_iter(),
            None,
        ));
        assert!(commit_page_shape_matches_request(
            initial,
            FrontierPosition::BeforeFirst,
            [].into_iter(),
            None,
        ));
        assert!(!commit_page_shape_matches_request(
            initial,
            FrontierPosition::AppliedThrough(first),
            [].into_iter(),
            None,
        ));

        let continuation = AuthoritativeCommitScanRequest::continuing(second, fourth, limit)
            .expect("ordered continuation");
        assert!(commit_page_shape_matches_request(
            continuation,
            FrontierPosition::AppliedThrough(fourth),
            [third, fourth].into_iter(),
            None,
        ));
        assert!(!commit_page_shape_matches_request(
            continuation,
            FrontierPosition::AppliedThrough(third),
            [third].into_iter(),
            None,
        ));
        assert!(!commit_page_shape_matches_request(
            continuation,
            FrontierPosition::AppliedThrough(fourth),
            [third, fourth].into_iter(),
            Some(fourth),
        ));
    }

    #[test]
    fn invalid_cursor_is_one_generic_root_validation_issue() {
        let failure = invalid_cursor_failure();
        let error = failure.public_error().expect("public cursor failure");
        assert_eq!(error.kind(), PublicErrorKind::Validation);
        let PublicErrorDetails::Validation(issues) = error.details() else {
            panic!("cursor failure must carry validation details");
        };
        assert_eq!(issues.as_slice().len(), 1);
        assert_eq!(issues.as_slice()[0].code(), ValidationCode::InvalidValue);
        assert_eq!(issues.as_slice()[0].path(), &ValidationPath::root());
    }
}
