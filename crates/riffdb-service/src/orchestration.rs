//! Shared authorization and durable service-audit orchestration.

use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use riffdb_commit::{
    AdministrationAuditExecutionError, CapabilityBootstrapTerminalPreparation,
    ControlPlaneExecutionAdmissionError, ControlPlaneExecutionErrorKind, CoordinatorLifecycleState,
};
use riffdb_errors::PublicError;
use riffdb_policy::{
    AuditClass, AuthorizedCapabilityMutationPreparation, AuthorizedOperation, Decision,
    OperationRequest,
};
use riffdb_types::{
    ApprovalId, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceOperationV1,
};

use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    InternalDefect, RequestContext, RequestControl, RiffDbServiceInner, ServiceAuditInput,
    ServiceFailure, ServiceResult,
};

thread_local! {
    static CURRENT_AUDIT_LIFECYCLE: RefCell<Vec<Arc<OperationAuditLifecycle>>> =
        const { RefCell::new(Vec::new()) };
}

/// Whether failures before an allow decision are intrinsically auditable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AuditScope {
    StandardRead,
    Intrinsic,
}

/// Which cancellation authority may stop an audit-capacity wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuditAppendControl {
    /// Pre-admission work remains cancellable with its originating request.
    Invocation,
    /// A selected terminal phase ignores late caller cancellation.
    Terminal,
}

/// One invocation after its initial current-policy safe point and optional start.
pub(crate) struct BegunInvocation {
    request: OperationRequest,
    operation: ServiceOperationV1,
    targets: ServiceAuditTargetsV1,
    audit_class: Option<AuditClass>,
    approval_id: Option<ApprovalId>,
    started: bool,
    initial_authorization: Box<AuthorizedOperation>,
    lifecycle: Arc<OperationAuditLifecycle>,
}

/// Retained audit authority after a one-use initial policy proof is consumed.
pub(crate) struct BegunInvocationCompletion {
    operation: ServiceOperationV1,
    targets: ServiceAuditTargetsV1,
    approval_id: Option<ApprovalId>,
    started: bool,
    lifecycle: Arc<OperationAuditLifecycle>,
}

/// One capability mutation after its initial current-policy proof and durable start.
pub(crate) struct BegunCapabilityMutation {
    request: OperationRequest,
    operation: ServiceOperationV1,
    targets: ServiceAuditTargetsV1,
    approval_id: Option<ApprovalId>,
    initial_authorization: Box<AuthorizedCapabilityMutationPreparation>,
    lifecycle: Arc<OperationAuditLifecycle>,
}

/// Per-service-job ownership of one possible durable Started-to-terminal lifecycle.
pub(crate) struct OperationAuditLifecycle {
    operation: ServiceOperationV1,
    state: Mutex<OperationAuditState>,
}

enum OperationAuditState {
    Unstarted,
    PrestartClassified(PanicTerminalAudit),
    StartInFlight(PanicTerminalAudit),
    Started(PendingTerminalAudit),
    StartFailed,
    TerminalInFlight(ContainedAuditFailure),
    TerminalDurable,
    TerminalFailed(ContainedAuditFailure),
}

pub(crate) struct PanicTerminalAudit {
    input: ServiceAuditInput,
    deadline: Instant,
}

enum PendingTerminalAudit {
    Authenticated(PanicTerminalAudit),
    Bootstrap {
        preparation: CapabilityBootstrapTerminalPreparation,
        deadline: Instant,
    },
    #[cfg(test)]
    TestAuthenticated,
}

enum ContainedFailureAuditAction {
    None,
    Append(PendingTerminalAudit),
    Unavailable(ContainedAuditFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ContainedAuditFailure {
    StorageUnavailable,
    OutcomeUnknown,
}

impl OperationAuditLifecycle {
    pub(crate) fn new(operation: ServiceOperationV1) -> Self {
        Self {
            operation,
            state: Mutex::new(OperationAuditState::Unstarted),
        }
    }

    fn prepare_start(
        &self,
        operation: ServiceOperationV1,
        terminal: PanicTerminalAudit,
    ) -> Result<(), AuditLifecycleTransitionError> {
        if operation != self.operation {
            return Err(AuditLifecycleTransitionError);
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(
            *state,
            OperationAuditState::Unstarted | OperationAuditState::PrestartClassified(_)
        ) {
            return Err(AuditLifecycleTransitionError);
        }
        *state = OperationAuditState::StartInFlight(terminal);
        Ok(())
    }

    fn classify_prestart(
        &self,
        operation: ServiceOperationV1,
        terminal: PanicTerminalAudit,
    ) -> Result<(), AuditLifecycleTransitionError> {
        if operation != self.operation {
            return Err(AuditLifecycleTransitionError);
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(
            *state,
            OperationAuditState::Unstarted | OperationAuditState::PrestartClassified(_)
        ) {
            return Err(AuditLifecycleTransitionError);
        }
        *state = OperationAuditState::PrestartClassified(terminal);
        Ok(())
    }

    fn refine_prestart(
        &self,
        operation: ServiceOperationV1,
        terminal: PanicTerminalAudit,
    ) -> Result<(), AuditLifecycleTransitionError> {
        if operation != self.operation {
            return Err(AuditLifecycleTransitionError);
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(*state, OperationAuditState::PrestartClassified(_)) {
            return Err(AuditLifecycleTransitionError);
        }
        *state = OperationAuditState::PrestartClassified(terminal);
        Ok(())
    }

    fn begin_prestart_terminal(
        &self,
        operation: ServiceOperationV1,
    ) -> Result<(), AuditLifecycleTransitionError> {
        if operation != self.operation {
            return Err(AuditLifecycleTransitionError);
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(
            *state,
            OperationAuditState::Unstarted | OperationAuditState::PrestartClassified(_)
        ) {
            return Err(AuditLifecycleTransitionError);
        }
        *state = OperationAuditState::TerminalInFlight(ContainedAuditFailure::StorageUnavailable);
        Ok(())
    }

    fn confirm_start(&self) -> Result<(), AuditLifecycleTransitionError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prior = std::mem::replace(&mut *state, OperationAuditState::Unstarted);
        match prior {
            OperationAuditState::StartInFlight(terminal) => {
                *state =
                    OperationAuditState::Started(PendingTerminalAudit::Authenticated(terminal));
                Ok(())
            }
            prior => {
                *state = prior;
                Err(AuditLifecycleTransitionError)
            }
        }
    }

    fn fail_start(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(*state, OperationAuditState::StartInFlight(_)) {
            *state = OperationAuditState::StartFailed;
        }
    }

    fn prepare_bootstrap_terminal(
        &self,
        operation: ServiceOperationV1,
        preparation: CapabilityBootstrapTerminalPreparation,
        deadline: Instant,
    ) -> Result<(), AuditLifecycleTransitionError> {
        if operation != self.operation || operation != ServiceOperationV1::CreateCapability {
            return Err(AuditLifecycleTransitionError);
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(*state, OperationAuditState::Unstarted) {
            return Err(AuditLifecycleTransitionError);
        }
        *state = OperationAuditState::Started(PendingTerminalAudit::Bootstrap {
            preparation,
            deadline,
        });
        Ok(())
    }

    fn begin_bootstrap_terminal(
        &self,
    ) -> Result<(CapabilityBootstrapTerminalPreparation, Instant), AuditLifecycleTransitionError>
    {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prior = std::mem::replace(&mut *state, OperationAuditState::Unstarted);
        match prior {
            OperationAuditState::Started(PendingTerminalAudit::Bootstrap {
                preparation,
                deadline,
            }) => {
                *state =
                    OperationAuditState::TerminalInFlight(ContainedAuditFailure::OutcomeUnknown);
                Ok((preparation, deadline))
            }
            prior => {
                *state = prior;
                Err(AuditLifecycleTransitionError)
            }
        }
    }

    fn begin_terminal(
        &self,
        phase: ServiceAuditPhaseV1,
        link: ServiceAuditLinkV1,
    ) -> Result<(), AuditLifecycleTransitionError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prior = std::mem::replace(&mut *state, OperationAuditState::Unstarted);
        match prior {
            OperationAuditState::Started(PendingTerminalAudit::Authenticated(_)) => {
                *state = OperationAuditState::TerminalInFlight(terminal_failure_class(phase, link));
                Ok(())
            }
            #[cfg(test)]
            OperationAuditState::Started(PendingTerminalAudit::TestAuthenticated) => {
                *state = OperationAuditState::TerminalInFlight(terminal_failure_class(phase, link));
                Ok(())
            }
            prior => {
                *state = prior;
                Err(AuditLifecycleTransitionError)
            }
        }
    }

    fn finish_terminal(&self, durable: bool) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prior = std::mem::replace(&mut *state, OperationAuditState::Unstarted);
        if let OperationAuditState::TerminalInFlight(failure) = prior {
            *state = if durable {
                OperationAuditState::TerminalDurable
            } else {
                OperationAuditState::TerminalFailed(failure)
            };
        } else {
            *state = prior;
        }
    }

    pub(crate) fn normal_completion_requires_containment(&self, succeeded: bool) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*state {
            OperationAuditState::Unstarted | OperationAuditState::TerminalDurable => false,
            OperationAuditState::StartFailed | OperationAuditState::TerminalFailed(_) => succeeded,
            OperationAuditState::PrestartClassified(_)
            | OperationAuditState::StartInFlight(_)
            | OperationAuditState::Started(_)
            | OperationAuditState::TerminalInFlight(_) => true,
        }
    }

    #[cfg(test)]
    pub(crate) fn mark_started_for_test(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = OperationAuditState::Started(PendingTerminalAudit::TestAuthenticated);
    }

    fn contained_failure_action(&self) -> ContainedFailureAuditAction {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match std::mem::replace(
            &mut *state,
            OperationAuditState::TerminalFailed(ContainedAuditFailure::StorageUnavailable),
        ) {
            OperationAuditState::PrestartClassified(terminal) => {
                *state = OperationAuditState::TerminalInFlight(
                    ContainedAuditFailure::StorageUnavailable,
                );
                ContainedFailureAuditAction::Append(PendingTerminalAudit::Authenticated(terminal))
            }
            OperationAuditState::Started(terminal) => {
                let failure = pending_failure_class(&terminal);
                *state = OperationAuditState::TerminalInFlight(failure);
                ContainedFailureAuditAction::Append(terminal)
            }
            OperationAuditState::Unstarted => {
                *state = OperationAuditState::Unstarted;
                ContainedFailureAuditAction::None
            }
            OperationAuditState::TerminalDurable => {
                *state = OperationAuditState::TerminalDurable;
                ContainedFailureAuditAction::None
            }
            OperationAuditState::StartInFlight(_) | OperationAuditState::StartFailed => {
                ContainedFailureAuditAction::Unavailable(ContainedAuditFailure::StorageUnavailable)
            }
            OperationAuditState::TerminalInFlight(failure)
            | OperationAuditState::TerminalFailed(failure) => {
                ContainedFailureAuditAction::Unavailable(failure)
            }
        }
    }
}

const fn terminal_failure_class(
    phase: ServiceAuditPhaseV1,
    link: ServiceAuditLinkV1,
) -> ContainedAuditFailure {
    if !matches!(link, ServiceAuditLinkV1::None)
        || matches!(phase, ServiceAuditPhaseV1::OutcomeUncertain)
    {
        ContainedAuditFailure::OutcomeUnknown
    } else {
        ContainedAuditFailure::StorageUnavailable
    }
}

const fn pending_failure_class(terminal: &PendingTerminalAudit) -> ContainedAuditFailure {
    match terminal {
        PendingTerminalAudit::Authenticated(_) => ContainedAuditFailure::StorageUnavailable,
        PendingTerminalAudit::Bootstrap { .. } => ContainedAuditFailure::OutcomeUnknown,
        #[cfg(test)]
        PendingTerminalAudit::TestAuthenticated => ContainedAuditFailure::StorageUnavailable,
    }
}

impl PanicTerminalAudit {
    fn new(
        context: &RequestContext,
        operation: ServiceOperationV1,
        targets: ServiceAuditTargetsV1,
        approval_id: Option<ApprovalId>,
    ) -> Result<Self, AuditAppendFailure> {
        let input = ServiceAuditInput::new(
            context,
            operation,
            ServiceAuditPhaseV1::Failed,
            targets,
            approval_id,
            ServiceAuditLinkV1::None,
        )
        .map_err(|_| AuditAppendFailure)?;
        Ok(Self {
            input,
            deadline: context.control().deadline(),
        })
    }
}

pub(crate) fn current_operation_audit_lifecycle(
    operation: ServiceOperationV1,
) -> Arc<OperationAuditLifecycle> {
    CURRENT_AUDIT_LIFECYCLE
        .with(|lifecycles| lifecycles.borrow().last().cloned())
        .unwrap_or_else(|| Arc::new(OperationAuditLifecycle::new(operation)))
}

pub(crate) fn with_operation_audit_lifecycle<T>(
    lifecycle: &Arc<OperationAuditLifecycle>,
    poll: impl FnOnce() -> T,
) -> T {
    CURRENT_AUDIT_LIFECYCLE.with(|lifecycles| {
        lifecycles.borrow_mut().push(Arc::clone(lifecycle));
    });
    let _scope = OperationAuditScope;
    poll()
}

struct OperationAuditScope;

impl Drop for OperationAuditScope {
    fn drop(&mut self) {
        CURRENT_AUDIT_LIFECYCLE.with(|lifecycles| {
            let _ = lifecycles.borrow_mut().pop();
        });
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AuditLifecycleTransitionError;

impl BegunCapabilityMutation {
    /// Borrows the initial proof only for exact binding checks before a capacity wait.
    pub(crate) const fn initial_authorization(&self) -> &AuthorizedCapabilityMutationPreparation {
        &self.initial_authorization
    }

    /// Performs the mandatory fresh policy check after the control-plane capacity wait.
    pub(crate) async fn reauthorize(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
    ) -> ServiceResult<Box<AuthorizedCapabilityMutationPreparation>> {
        self.reauthorize_request(service, context, self.request.clone())
            .await
    }

    /// Reauthorizes newly loaded capability-target facts under the existing audit start.
    pub(crate) async fn reauthorize_request(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
        request: OperationRequest,
    ) -> ServiceResult<Box<AuthorizedCapabilityMutationPreparation>> {
        if request.operation() != self.operation {
            if self
                .finish_reauthorization_phase(service, context, ServiceAuditPhaseV1::Failed)
                .await
                .is_err()
            {
                return Err(PublicError::storage_unavailable().into());
            }
            return Err(service.internal_failure(self.operation, InternalDefect::ProofMismatch));
        }
        if context.control().is_cancelled() {
            if self
                .finish_reauthorization_phase(service, context, ServiceAuditPhaseV1::Cancelled)
                .await
                .is_err()
            {
                return Err(PublicError::storage_unavailable().into());
            }
            return Err(ServiceFailure::Cancelled);
        }
        if context.control().is_deadline_exceeded() {
            if self
                .finish_reauthorization_phase(service, context, ServiceAuditPhaseV1::Cancelled)
                .await
                .is_err()
            {
                return Err(PublicError::storage_unavailable().into());
            }
            return Err(ServiceFailure::DeadlineExceeded);
        }

        match service
            .providers
            .policy
            .authorize(context.principal(), request)
        {
            Ok(Decision::PrepareCapabilityMutation(authorization)) => {
                if authorization.validated_approval() != self.approval_id.as_ref() {
                    if self
                        .finish_reauthorization_phase(service, context, ServiceAuditPhaseV1::Failed)
                        .await
                        .is_err()
                    {
                        return Err(PublicError::storage_unavailable().into());
                    }
                    return Err(
                        service.internal_failure(self.operation, InternalDefect::ProofMismatch)
                    );
                }
                Ok(authorization)
            }
            Ok(Decision::Deny(_)) => {
                let _ = self
                    .finish_reauthorization_phase(service, context, ServiceAuditPhaseV1::Denied)
                    .await;
                Err(PublicError::authorization_denied().into())
            }
            Ok(Decision::Allow(_)) => {
                if self
                    .finish_reauthorization_phase(service, context, ServiceAuditPhaseV1::Failed)
                    .await
                    .is_err()
                {
                    return Err(PublicError::storage_unavailable().into());
                }
                Err(service.internal_failure(self.operation, InternalDefect::ProofMismatch))
            }
            Err(_) => {
                if self
                    .finish_reauthorization_phase(service, context, ServiceAuditPhaseV1::Failed)
                    .await
                    .is_err()
                {
                    return Err(PublicError::storage_unavailable().into());
                }
                Err(PublicError::storage_unavailable().into())
            }
        }
    }

    /// Appends the one terminal phase before releasing a mutation result.
    pub(crate) async fn finish(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
        phase: ServiceAuditPhaseV1,
        link: ServiceAuditLinkV1,
    ) -> Result<(), AuditAppendFailure> {
        let input = ServiceAuditInput::new(
            context,
            self.operation,
            phase,
            self.targets.clone(),
            self.approval_id.clone(),
            link,
        )
        .map_err(|_| AuditAppendFailure)?;
        self.lifecycle
            .begin_terminal(phase, link)
            .map_err(|_| AuditAppendFailure)?;
        let control = terminal_audit_control(context.control());
        let result = service.append_prepared_audit(&control, input).await;
        self.lifecycle.finish_terminal(result.is_ok());
        result
    }

    async fn finish_reauthorization_phase(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
        phase: ServiceAuditPhaseV1,
    ) -> Result<(), AuditAppendFailure> {
        let result = self
            .finish(service, context, phase, ServiceAuditLinkV1::None)
            .await;
        if result.is_err() {
            service.note_audit_failure(self.operation);
        }
        result
    }
}

impl BegunInvocation {
    /// Returns the closed operation retained by this audit lifecycle.
    pub(crate) const fn operation(&self) -> ServiceOperationV1 {
        self.operation
    }

    /// Borrows the initial proof only for preparation that precedes a capacity wait.
    pub(crate) const fn initial_authorization(&self) -> &AuthorizedOperation {
        &self.initial_authorization
    }

    /// Separates a one-use policy proof from the invocation's terminal-audit authority.
    pub(crate) fn into_initial_authorization_and_completion(
        self,
    ) -> (Box<AuthorizedOperation>, BegunInvocationCompletion) {
        (
            self.initial_authorization,
            BegunInvocationCompletion {
                operation: self.operation,
                targets: self.targets,
                approval_id: self.approval_id,
                started: self.started,
                lifecycle: self.lifecycle,
            },
        )
    }

    /// Performs the mandatory fresh exact-facts authorization after capacity wait.
    pub(crate) async fn reauthorize(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
    ) -> ServiceResult<Box<AuthorizedOperation>> {
        self.reauthorize_request(service, context, self.request.clone())
            .await
    }

    /// Reauthorizes newly loaded exact facts for the same audited operation.
    pub(crate) async fn reauthorize_request(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
        request: OperationRequest,
    ) -> ServiceResult<Box<AuthorizedOperation>> {
        if request.operation() != self.operation {
            if self
                .finish_reauthorization_phase(service, context, ServiceAuditPhaseV1::Failed)
                .await
                .is_err()
            {
                return Err(PublicError::storage_unavailable().into());
            }
            return Err(service.internal_failure(self.operation, InternalDefect::ProofMismatch));
        }
        if context.control().is_cancelled() {
            if self
                .finish_reauthorization_phase(service, context, ServiceAuditPhaseV1::Cancelled)
                .await
                .is_err()
            {
                return Err(PublicError::storage_unavailable().into());
            }
            return Err(ServiceFailure::Cancelled);
        }
        if context.control().is_deadline_exceeded() {
            if self
                .finish_reauthorization_phase(service, context, ServiceAuditPhaseV1::Cancelled)
                .await
                .is_err()
            {
                return Err(PublicError::storage_unavailable().into());
            }
            return Err(ServiceFailure::DeadlineExceeded);
        }

        match service
            .providers
            .policy
            .authorize(context.principal(), request)
        {
            Ok(Decision::Allow(authorization)) => {
                let obligations = authorization.obligations();
                if obligations.audit_class() != self.audit_class
                    || obligations.validated_approval() != self.approval_id.as_ref()
                {
                    if self
                        .finish_reauthorization_phase(service, context, ServiceAuditPhaseV1::Failed)
                        .await
                        .is_err()
                    {
                        return Err(PublicError::storage_unavailable().into());
                    }
                    return Err(
                        service.internal_failure(self.operation, InternalDefect::ProofMismatch)
                    );
                }
                Ok(authorization)
            }
            Ok(Decision::Deny(_)) => {
                let _ = self
                    .finish_reauthorization_phase(service, context, ServiceAuditPhaseV1::Denied)
                    .await;
                Err(PublicError::authorization_denied().into())
            }
            Ok(Decision::PrepareCapabilityMutation(_)) => {
                if self
                    .finish_reauthorization_phase(service, context, ServiceAuditPhaseV1::Failed)
                    .await
                    .is_err()
                {
                    return Err(PublicError::storage_unavailable().into());
                }
                Err(service.internal_failure(self.operation, InternalDefect::ProofMismatch))
            }
            Err(_) => {
                if self
                    .finish_reauthorization_phase(service, context, ServiceAuditPhaseV1::Failed)
                    .await
                    .is_err()
                {
                    return Err(PublicError::storage_unavailable().into());
                }
                Err(PublicError::storage_unavailable().into())
            }
        }
    }

    /// Records a denial derived while monotonically intersecting current policy.
    ///
    /// Like a direct policy denial, an audit outage is noted but cannot disclose
    /// whether the denial record itself became durable.
    pub(crate) async fn finish_authorization_denial(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
    ) -> ServiceFailure {
        let _ = self
            .finish_reauthorization_phase(service, context, ServiceAuditPhaseV1::Denied)
            .await;
        PublicError::authorization_denied().into()
    }

    /// Appends the one terminal phase before releasing protected output.
    pub(crate) async fn finish(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
        phase: ServiceAuditPhaseV1,
        link: ServiceAuditLinkV1,
    ) -> Result<(), AuditAppendFailure> {
        if !self.started {
            return Ok(());
        }
        let input = ServiceAuditInput::new(
            context,
            self.operation,
            phase,
            self.targets.clone(),
            self.approval_id.clone(),
            link,
        )
        .map_err(|_| AuditAppendFailure)?;
        self.lifecycle
            .begin_terminal(phase, link)
            .map_err(|_| AuditAppendFailure)?;
        let control = terminal_audit_control(context.control());
        let result = service.append_prepared_audit(&control, input).await;
        self.lifecycle.finish_terminal(result.is_ok());
        result
    }

    async fn finish_reauthorization_phase(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
        phase: ServiceAuditPhaseV1,
    ) -> Result<(), AuditAppendFailure> {
        let result = if self.started {
            self.finish(service, context, phase, ServiceAuditLinkV1::None)
                .await
        } else if phase == ServiceAuditPhaseV1::Denied {
            service
                .append_audit(
                    context,
                    self.operation,
                    phase,
                    self.targets.clone(),
                    None,
                    ServiceAuditLinkV1::None,
                    AuditAppendControl::Terminal,
                )
                .await
        } else {
            Ok(())
        };
        if result.is_err() {
            service.note_audit_failure(self.operation);
        }
        result
    }
}

impl BegunInvocationCompletion {
    /// Returns the closed operation retained by this audit lifecycle.
    pub(crate) const fn operation(&self) -> ServiceOperationV1 {
        self.operation
    }

    /// Appends the one terminal phase before releasing protected output.
    pub(crate) async fn finish(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
        phase: ServiceAuditPhaseV1,
        link: ServiceAuditLinkV1,
    ) -> Result<(), AuditAppendFailure> {
        if !self.started {
            return Ok(());
        }
        let input = ServiceAuditInput::new(
            context,
            self.operation,
            phase,
            self.targets.clone(),
            self.approval_id.clone(),
            link,
        )
        .map_err(|_| AuditAppendFailure)?;
        self.lifecycle
            .begin_terminal(phase, link)
            .map_err(|_| AuditAppendFailure)?;
        let control = terminal_audit_control(context.control());
        let result = service.append_prepared_audit(&control, input).await;
        self.lifecycle.finish_terminal(result.is_ok());
        result
    }
}

impl RiffDbServiceInner {
    /// Enters intrinsic audit scope before exact semantic targets can be resolved.
    pub(crate) fn classify_intrinsic_prestart(
        &self,
        context: &RequestContext,
        operation: ServiceOperationV1,
        targets: ServiceAuditTargetsV1,
    ) -> ServiceResult<()> {
        let terminal = PanicTerminalAudit::new(context, operation, targets, None)
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
        current_operation_audit_lifecycle(operation)
            .classify_prestart(operation, terminal)
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))
    }

    /// Replaces the provisional intrinsic panic target after exact resolution.
    pub(crate) fn refine_intrinsic_prestart(
        &self,
        context: &RequestContext,
        operation: ServiceOperationV1,
        targets: ServiceAuditTargetsV1,
    ) -> ServiceResult<()> {
        let terminal = PanicTerminalAudit::new(context, operation, targets, None)
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
        current_operation_audit_lifecycle(operation)
            .refine_prestart(operation, terminal)
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))
    }

    /// Runs initial capability-mutation policy and appends its intrinsic start.
    pub(crate) async fn begin_capability_mutation(
        &self,
        context: &RequestContext,
        request: OperationRequest,
        targets: ServiceAuditTargetsV1,
    ) -> ServiceResult<BegunCapabilityMutation> {
        let operation = request.operation();
        if !matches!(
            operation,
            ServiceOperationV1::CreateCapability | ServiceOperationV1::RevokeCapability
        ) {
            self.append_prestart_terminal_if_intrinsic(
                context,
                operation,
                targets,
                AuditScope::Intrinsic,
                ServiceAuditPhaseV1::Failed,
            )
            .await?;
            return Err(self.internal_failure(operation, InternalDefect::ProofMismatch));
        }
        self.classify_intrinsic_prestart(context, operation, targets.clone())?;
        if context.control().is_cancelled() {
            self.append_prestart_terminal_if_intrinsic(
                context,
                operation,
                targets,
                AuditScope::Intrinsic,
                ServiceAuditPhaseV1::Cancelled,
            )
            .await?;
            return Err(ServiceFailure::Cancelled);
        }
        if context.control().is_deadline_exceeded() {
            self.append_prestart_terminal_if_intrinsic(
                context,
                operation,
                targets,
                AuditScope::Intrinsic,
                ServiceAuditPhaseV1::Cancelled,
            )
            .await?;
            return Err(ServiceFailure::DeadlineExceeded);
        }

        let authorization = match self
            .providers
            .policy
            .authorize(context.principal(), request.clone())
        {
            Ok(Decision::PrepareCapabilityMutation(authorization)) => authorization,
            Ok(Decision::Deny(_)) => {
                self.append_prestart_terminal_if_intrinsic(
                    context,
                    operation,
                    targets,
                    AuditScope::Intrinsic,
                    ServiceAuditPhaseV1::Denied,
                )
                .await?;
                return Err(PublicError::authorization_denied().into());
            }
            Ok(Decision::Allow(_)) => {
                self.append_prestart_terminal_if_intrinsic(
                    context,
                    operation,
                    targets,
                    AuditScope::Intrinsic,
                    ServiceAuditPhaseV1::Failed,
                )
                .await?;
                return Err(self.internal_failure(operation, InternalDefect::ProofMismatch));
            }
            Err(_) => {
                self.append_prestart_terminal_if_intrinsic(
                    context,
                    operation,
                    targets,
                    AuditScope::Intrinsic,
                    ServiceAuditPhaseV1::Failed,
                )
                .await?;
                return Err(PublicError::storage_unavailable().into());
            }
        };

        let approval_id = authorization.validated_approval().cloned();
        let lifecycle = current_operation_audit_lifecycle(operation);
        let panic_terminal =
            PanicTerminalAudit::new(context, operation, targets.clone(), approval_id.clone())
                .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
        lifecycle
            .prepare_start(operation, panic_terminal)
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
        if self
            .append_audit(
                context,
                operation,
                ServiceAuditPhaseV1::Started,
                targets.clone(),
                approval_id.clone(),
                ServiceAuditLinkV1::None,
                AuditAppendControl::Invocation,
            )
            .await
            .is_err()
        {
            lifecycle.fail_start();
            self.note_audit_failure(operation);
            return Err(PublicError::storage_unavailable().into());
        }
        if lifecycle.confirm_start().is_err() {
            self.note_audit_failure(operation);
            return Err(self.internal_failure(operation, InternalDefect::ProofMismatch));
        }

        Ok(BegunCapabilityMutation {
            request,
            operation,
            targets,
            approval_id,
            initial_authorization: authorization,
            lifecycle,
        })
    }

    /// Runs initial policy and appends the exact start or standalone denial.
    pub(crate) async fn begin_invocation(
        &self,
        context: &RequestContext,
        request: OperationRequest,
        targets: ServiceAuditTargetsV1,
        scope: AuditScope,
    ) -> ServiceResult<BegunInvocation> {
        let operation = request.operation();
        if scope == AuditScope::Intrinsic {
            self.classify_intrinsic_prestart(context, operation, targets.clone())?;
        }
        if context.control().is_cancelled() {
            self.append_prestart_terminal_if_intrinsic(
                context,
                operation,
                targets,
                scope,
                ServiceAuditPhaseV1::Cancelled,
            )
            .await?;
            return Err(ServiceFailure::Cancelled);
        }
        if context.control().is_deadline_exceeded() {
            self.append_prestart_terminal_if_intrinsic(
                context,
                operation,
                targets,
                scope,
                ServiceAuditPhaseV1::Cancelled,
            )
            .await?;
            return Err(ServiceFailure::DeadlineExceeded);
        }

        let authorization = match self
            .providers
            .policy
            .authorize(context.principal(), request.clone())
        {
            Ok(Decision::Allow(authorization)) => authorization,
            Ok(Decision::Deny(_)) => {
                if scope == AuditScope::Intrinsic {
                    self.append_prestart_terminal_if_intrinsic(
                        context,
                        operation,
                        targets,
                        scope,
                        ServiceAuditPhaseV1::Denied,
                    )
                    .await?;
                } else if self
                    .append_audit(
                        context,
                        operation,
                        ServiceAuditPhaseV1::Denied,
                        targets,
                        None,
                        ServiceAuditLinkV1::None,
                        AuditAppendControl::Terminal,
                    )
                    .await
                    .is_err()
                {
                    self.note_audit_failure(operation);
                }
                return Err(PublicError::authorization_denied().into());
            }
            Ok(Decision::PrepareCapabilityMutation(_)) => {
                self.append_prestart_terminal_if_intrinsic(
                    context,
                    operation,
                    targets,
                    scope,
                    ServiceAuditPhaseV1::Failed,
                )
                .await?;
                return Err(self.internal_failure(operation, InternalDefect::ProofMismatch));
            }
            Err(_) => {
                self.append_prestart_terminal_if_intrinsic(
                    context,
                    operation,
                    targets,
                    scope,
                    ServiceAuditPhaseV1::Failed,
                )
                .await?;
                return Err(PublicError::storage_unavailable().into());
            }
        };

        let audit_class = authorization.obligations().audit_class();
        let approval_id = authorization.obligations().validated_approval().cloned();
        if scope == AuditScope::Intrinsic && audit_class.is_none() {
            self.append_prestart_terminal_if_intrinsic(
                context,
                operation,
                targets,
                scope,
                ServiceAuditPhaseV1::Failed,
            )
            .await?;
            return Err(self.internal_failure(operation, InternalDefect::ProofMismatch));
        }
        let started = audit_class.is_some();
        let lifecycle = current_operation_audit_lifecycle(operation);
        if started {
            let panic_terminal =
                PanicTerminalAudit::new(context, operation, targets.clone(), approval_id.clone())
                    .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
            lifecycle
                .prepare_start(operation, panic_terminal)
                .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
            if self
                .append_audit(
                    context,
                    operation,
                    ServiceAuditPhaseV1::Started,
                    targets.clone(),
                    approval_id.clone(),
                    ServiceAuditLinkV1::None,
                    AuditAppendControl::Invocation,
                )
                .await
                .is_err()
            {
                lifecycle.fail_start();
                self.note_audit_failure(operation);
                return Err(PublicError::storage_unavailable().into());
            }
            if lifecycle.confirm_start().is_err() {
                self.note_audit_failure(operation);
                return Err(self.internal_failure(operation, InternalDefect::ProofMismatch));
            }
        }

        Ok(BegunInvocation {
            request,
            operation,
            targets,
            audit_class,
            approval_id,
            started,
            initial_authorization: authorization,
            lifecycle,
        })
    }

    pub(crate) async fn append_prestart_terminal_if_intrinsic(
        &self,
        context: &RequestContext,
        operation: ServiceOperationV1,
        targets: ServiceAuditTargetsV1,
        scope: AuditScope,
        phase: ServiceAuditPhaseV1,
    ) -> ServiceResult<()> {
        if scope == AuditScope::StandardRead {
            return Ok(());
        }
        let lifecycle = current_operation_audit_lifecycle(operation);
        lifecycle
            .begin_prestart_terminal(operation)
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
        let result = self
            .append_audit(
                context,
                operation,
                phase,
                targets,
                None,
                ServiceAuditLinkV1::None,
                AuditAppendControl::Terminal,
            )
            .await;
        lifecycle.finish_terminal(result.is_ok());
        if result.is_err() {
            self.note_audit_failure(operation);
        }
        result.map_err(|_| PublicError::storage_unavailable().into())
    }

    #[allow(clippy::too_many_arguments)]
    async fn append_audit(
        &self,
        context: &RequestContext,
        operation: ServiceOperationV1,
        phase: ServiceAuditPhaseV1,
        targets: ServiceAuditTargetsV1,
        approval_id: Option<ApprovalId>,
        link: ServiceAuditLinkV1,
        control_mode: AuditAppendControl,
    ) -> Result<(), AuditAppendFailure> {
        let input = ServiceAuditInput::new(context, operation, phase, targets, approval_id, link)
            .map_err(|_| AuditAppendFailure)?;
        let terminal_control;
        let control = match control_mode {
            AuditAppendControl::Invocation => context.control(),
            AuditAppendControl::Terminal => {
                terminal_control = terminal_audit_control(context.control());
                &terminal_control
            }
        };
        self.append_prepared_audit(control, input).await
    }

    async fn append_prepared_audit(
        &self,
        control: &RequestControl,
        input: ServiceAuditInput,
    ) -> Result<(), AuditAppendFailure> {
        let permit = wait_with_control(
            control,
            self.providers.deadline_scheduler.as_ref(),
            self.executors.audit.reserve_capacity(),
        )
        .await
        .map_err(|_: ControlledWaitError| AuditAppendFailure)?
        .map_err(|_| AuditAppendFailure)?;
        let receipt = permit
            .submit(Box::new(input))
            .map_err(|_| AuditAppendFailure)?;
        match receipt.completion().await {
            Ok(()) => Ok(()),
            Err(error) => {
                if matches!(error, AdministrationAuditExecutionError::CoordinatorFenced)
                    || self.executors.audit.lifecycle_state() == CoordinatorLifecycleState::Fenced
                {
                    self.providers.health.fail_authoritative_readiness(
                        crate::AuthoritativeReadinessFailure::CoordinatorFenced,
                    );
                } else {
                    self.providers.health.fail_authoritative_readiness(
                        crate::AuthoritativeReadinessFailure::AuditUnavailable,
                    );
                }
                Err(AuditAppendFailure)
            }
        }
    }

    /// Resolves the audit side of a contained panic or unterminated operation.
    pub(crate) async fn settle_contained_failure_audit(
        &self,
        lifecycle: &OperationAuditLifecycle,
    ) -> Result<(), ContainedAuditFailure> {
        match lifecycle.contained_failure_action() {
            ContainedFailureAuditAction::None => Ok(()),
            ContainedFailureAuditAction::Unavailable(failure) => {
                self.note_audit_failure(lifecycle.operation);
                Err(failure)
            }
            ContainedFailureAuditAction::Append(PendingTerminalAudit::Authenticated(terminal)) => {
                let (control, _unused_cancellation) = RequestControl::new(terminal.deadline);
                let result = self.append_prepared_audit(&control, terminal.input).await;
                lifecycle.finish_terminal(result.is_ok());
                if result.is_err() {
                    self.note_audit_failure(lifecycle.operation);
                }
                result.map_err(|_| ContainedAuditFailure::StorageUnavailable)
            }
            ContainedFailureAuditAction::Append(PendingTerminalAudit::Bootstrap {
                preparation,
                deadline,
            }) => {
                let (control, _unused_cancellation) = RequestControl::new(deadline);
                let result = self
                    .append_prepared_bootstrap_terminal(&control, preparation)
                    .await;
                lifecycle.finish_terminal(result.is_ok());
                result.map_err(|_| ContainedAuditFailure::OutcomeUnknown)
            }
            #[cfg(test)]
            ContainedFailureAuditAction::Append(PendingTerminalAudit::TestAuthenticated) => {
                lifecycle.finish_terminal(false);
                Err(ContainedAuditFailure::StorageUnavailable)
            }
        }
    }

    /// Arms and appends the terminal for a known completed compound bootstrap.
    pub(crate) async fn finish_bootstrap_terminal(
        &self,
        request_control: &RequestControl,
        preparation: CapabilityBootstrapTerminalPreparation,
    ) -> Result<(), AuditAppendFailure> {
        let operation = ServiceOperationV1::CreateCapability;
        let lifecycle = current_operation_audit_lifecycle(operation);
        lifecycle
            .prepare_bootstrap_terminal(operation, preparation, request_control.deadline())
            .map_err(|_| AuditAppendFailure)?;
        let (preparation, deadline) = lifecycle
            .begin_bootstrap_terminal()
            .map_err(|_| AuditAppendFailure)?;
        let (control, _unused_cancellation) = RequestControl::new(deadline);
        let result = self
            .append_prepared_bootstrap_terminal(&control, preparation)
            .await;
        lifecycle.finish_terminal(result.is_ok());
        result
    }

    async fn append_prepared_bootstrap_terminal(
        &self,
        control: &RequestControl,
        preparation: CapabilityBootstrapTerminalPreparation,
    ) -> Result<(), AuditAppendFailure> {
        let permit = match wait_with_control(
            control,
            self.providers.deadline_scheduler.as_ref(),
            self.executors.control_plane.reserve_capacity(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(error)) => {
                self.note_bootstrap_terminal_failure(
                    error == ControlPlaneExecutionAdmissionError::Fenced,
                );
                return Err(AuditAppendFailure);
            }
            Err(_) => {
                self.note_bootstrap_terminal_failure(false);
                return Err(AuditAppendFailure);
            }
        };
        let receipt = match permit.submit_capability_bootstrap_terminal(preparation) {
            Ok(receipt) => receipt,
            Err(error) => {
                self.note_bootstrap_terminal_failure(
                    error == ControlPlaneExecutionAdmissionError::Fenced,
                );
                return Err(AuditAppendFailure);
            }
        };
        match receipt.completion().await {
            Ok(()) => Ok(()),
            Err(error) => {
                self.note_bootstrap_terminal_failure(matches!(
                    error.kind(),
                    ControlPlaneExecutionErrorKind::OutcomeUnknown
                        | ControlPlaneExecutionErrorKind::CoordinatorFenced
                ));
                Err(AuditAppendFailure)
            }
        }
    }

    fn note_bootstrap_terminal_failure(&self, fenced: bool) {
        self.providers
            .telemetry
            .record(crate::ServiceTelemetryEvent::AuditUnavailable {
                operation: ServiceOperationV1::CreateCapability,
            });
        self.providers
            .health
            .fail_authoritative_readiness(if fenced {
                crate::AuthoritativeReadinessFailure::CoordinatorFenced
            } else {
                crate::AuthoritativeReadinessFailure::AuditUnavailable
            });
    }

    pub(crate) fn note_audit_failure(&self, operation: ServiceOperationV1) {
        self.providers
            .telemetry
            .record(crate::ServiceTelemetryEvent::AuditUnavailable { operation });
        self.providers
            .health
            .fail_authoritative_readiness(crate::AuthoritativeReadinessFailure::AuditUnavailable);
    }
}

pub(crate) fn terminal_audit_control(request: &RequestControl) -> RequestControl {
    let (control, _unused_cancellation) = RequestControl::new(request.deadline());
    control
}

/// Required audit work could not be proven durable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AuditAppendFailure;

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn terminal_audit_control_keeps_deadline_without_caller_cancellation() {
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(60))
            .expect("test deadline");
        let (request, cancellation) = RequestControl::new(deadline);
        cancellation.cancel();

        let terminal = terminal_audit_control(&request);

        assert!(request.is_cancelled());
        assert!(!terminal.is_cancelled());
        assert_eq!(terminal.deadline(), deadline);
    }

    #[test]
    fn contained_failure_selects_only_one_terminal_attempt() {
        let lifecycle = OperationAuditLifecycle::new(ServiceOperationV1::GetEntity);
        lifecycle.mark_started_for_test();

        assert!(lifecycle.normal_completion_requires_containment(false));
        assert!(matches!(
            lifecycle.contained_failure_action(),
            ContainedFailureAuditAction::Append(PendingTerminalAudit::TestAuthenticated)
        ));
        assert!(matches!(
            lifecycle.contained_failure_action(),
            ContainedFailureAuditAction::Unavailable(ContainedAuditFailure::StorageUnavailable)
        ));
    }

    #[test]
    fn normal_terminal_selection_rejects_a_second_attempt() {
        let lifecycle = OperationAuditLifecycle::new(ServiceOperationV1::GetEntity);
        lifecycle.mark_started_for_test();

        assert!(
            lifecycle
                .begin_terminal(ServiceAuditPhaseV1::Succeeded, ServiceAuditLinkV1::None)
                .is_ok()
        );
        assert!(
            lifecycle
                .begin_terminal(ServiceAuditPhaseV1::Failed, ServiceAuditLinkV1::None)
                .is_err()
        );
        lifecycle.finish_terminal(true);
        assert!(!lifecycle.normal_completion_requires_containment(true));
    }

    #[test]
    fn authoritative_links_preserve_outcome_unknown_on_terminal_outage() {
        let link = ServiceAuditLinkV1::ControlPlane {
            administration_sequence: riffdb_types::AdministrationSequence::first(),
        };
        assert_eq!(
            terminal_failure_class(ServiceAuditPhaseV1::Succeeded, link),
            ContainedAuditFailure::OutcomeUnknown
        );
        assert_eq!(
            terminal_failure_class(ServiceAuditPhaseV1::Succeeded, ServiceAuditLinkV1::None),
            ContainedAuditFailure::StorageUnavailable
        );
    }
}
