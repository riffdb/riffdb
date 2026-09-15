//! Shared authorization and durable service-audit orchestration.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use riffdb_commit::{
    AdministrationAuditExecutionError, CapabilityBootstrapTerminalPreparation,
    ControlPlaneExecutionAdmissionError, ControlPlaneExecutionErrorKind, CoordinatorLifecycleState,
};
use riffdb_errors::PublicError;
use riffdb_policy::{
    ApplicationExportAuthorizationRequestV1, ApplicationExportDecisionV1,
    ApplicationExportPolicyOperationV1, ApplicationReimportAuthorizationRequestV1,
    ApplicationReimportDecisionV1, ApplicationReimportPolicyOperationV1, AuditClass,
    AuthorizedApplicationExportV1, AuthorizedApplicationReimportV1,
    AuthorizedCapabilityMutationPreparation, AuthorizedOperation, Decision, OperationRequest,
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
    deferred_start: AtomicBool,
    initial_authorization: Box<AuthorizedOperation>,
    /// Capability-view generation observed immediately *before* the begin evaluation.
    ///
    /// Only [`BegunInvocation::reauthorize_read`] consumes it. `None` when the
    /// policy port publishes no generation, which permanently disables the
    /// revision-checked path for this invocation.
    policy_generation_at_begin: Option<u64>,
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

/// One export invocation after a specialized current-V5 safe point and durable start.
pub(crate) struct BegunApplicationExportInvocation {
    authorization: Box<AuthorizedApplicationExportV1>,
    completion: BegunInvocationCompletion,
}

/// One reimport invocation after a specialized current-V7 safe point and durable start.
pub(crate) struct BegunApplicationReimportInvocation {
    authorization: Box<AuthorizedApplicationReimportV1>,
    completion: BegunInvocationCompletion,
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
    /// True only after a durable Started audit row was successfully appended.
    ///
    /// Deferred compounds mark Started in-process without setting this flag until
    /// the post-admission append succeeds. Pre-admission settle is safe only when
    /// this remains false.
    durable_start: AtomicBool,
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
            durable_start: AtomicBool::new(false),
        }
    }

    /// Records that a durable Started audit row was proven.
    pub(crate) fn mark_durable_start(&self) {
        self.durable_start.store(true, Ordering::Release);
    }

    /// Returns whether a durable Started audit has been proven for this job.
    #[must_use]
    pub(crate) fn has_durable_started(&self) -> bool {
        self.durable_start.load(Ordering::Acquire)
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

    /// Forces a settled terminal state after a pre-admission rejection when the
    /// normal Started→terminal transition is unavailable.
    pub(crate) fn force_terminal_settled_for_pre_admission(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = OperationAuditState::TerminalDurable;
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
        .map_err(|_| AuditAppendFailure::subsystem())?;
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

fn settle_lifecycle_pre_admission(lifecycle: &OperationAuditLifecycle) {
    if lifecycle
        .begin_terminal(ServiceAuditPhaseV1::Failed, ServiceAuditLinkV1::None)
        .is_ok()
    {
        lifecycle.finish_terminal(true);
        return;
    }
    lifecycle.force_terminal_settled_for_pre_admission();
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
        .map_err(|_| AuditAppendFailure::subsystem())?;
        self.lifecycle
            .begin_terminal(phase, link)
            .map_err(|_| AuditAppendFailure::subsystem())?;
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
        if let Err(failure) = &result {
            service.note_audit_failure_with_cause(self.operation, failure.cause());
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

    pub(crate) fn compound_started_input(
        &self,
        context: &RequestContext,
    ) -> Result<ServiceAuditInput, AuditAppendFailure> {
        if self.operation != ServiceOperationV1::ExecuteCommand || !self.started {
            return Err(AuditAppendFailure::subsystem());
        }
        ServiceAuditInput::new(
            context,
            self.operation,
            ServiceAuditPhaseV1::Started,
            self.targets.clone(),
            self.approval_id.clone(),
            ServiceAuditLinkV1::None,
        )
        .map_err(|_| AuditAppendFailure::subsystem())
    }

    pub(crate) fn confirm_compound_success(
        &self,
        link: ServiceAuditLinkV1,
    ) -> Result<(), AuditAppendFailure> {
        self.deferred_start.store(false, Ordering::Release);
        self.lifecycle
            .begin_terminal(ServiceAuditPhaseV1::Succeeded, link)
            .map_err(|_| AuditAppendFailure::subsystem())?;
        self.lifecycle.finish_terminal(true);
        Ok(())
    }

    pub(crate) fn confirm_compound_failure(&self) -> Result<(), AuditAppendFailure> {
        self.deferred_start.store(false, Ordering::Release);
        self.lifecycle
            .begin_terminal(ServiceAuditPhaseV1::Failed, ServiceAuditLinkV1::None)
            .map_err(|_| AuditAppendFailure::subsystem())?;
        self.lifecycle.finish_terminal(true);
        Ok(())
    }

    /// True while a deferred compound Started has not yet been durably written.
    #[must_use]
    pub(crate) fn is_deferred_start_pending(&self) -> bool {
        self.deferred_start.load(Ordering::Acquire)
    }

    /// Settles a pre-admission capacity rejection without durable audit I/O.
    ///
    /// ADR-0071 sanctions this only for capacity rejections when no durable
    /// Started was written (deferred compound start). Also settles the active
    /// TLS lifecycle so the outer spawn wrapper does not enter capacity-backed
    /// containment under saturation.
    pub(crate) fn settle_pre_admission_rejection(&self) {
        self.deferred_start.store(false, Ordering::Release);
        settle_lifecycle_pre_admission(&self.lifecycle);
        CURRENT_AUDIT_LIFECYCLE.with(|lifecycles| {
            if let Some(current) = lifecycles.borrow().last()
                && !Arc::ptr_eq(current, &self.lifecycle)
            {
                settle_lifecycle_pre_admission(current);
            }
        });
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

    /// Performs the read pipeline's revision-checked reauthorization safe point.
    ///
    /// This is deliberately a *separate* entry point from
    /// [`BegunInvocation::reauthorize`]: the revision-checked shortcut is
    /// available only to the symbolic read pipeline. Commit, command,
    /// contract, discovery, and administration reauthorization keep
    /// unconditional full evaluation, including the mandatory recheck after a
    /// capacity wait.
    ///
    /// The safe point still executes. It observes live current state through
    /// [`CurrentPolicyPort::capability_view_checkpoint`] and reissues the begin
    /// proof only when [`AuthorizedOperation::reissue_for_unchanged_view`]
    /// proves the world (generation), the clock (validity window), and the
    /// request are all still what the full evaluation decided against.
    /// Anything else falls through to a byte-identical full re-evaluation.
    pub(crate) async fn reauthorize_read(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
    ) -> ServiceResult<Box<AuthorizedOperation>> {
        let request = self.request.clone();
        self.check_reauthorization_preconditions(service, context, &request)
            .await?;
        if let Some(baseline) = self.policy_generation_at_begin
            && let Some(observed) = service.providers.policy.capability_view_checkpoint()
            && let Some(proof) = self
                .initial_authorization
                .reissue_for_unchanged_view(baseline, observed, &request)
        {
            return Ok(Box::new(proof));
        }
        self.full_reauthorize(service, context, request).await
    }

    /// Reauthorizes newly loaded exact facts for the same audited operation.
    ///
    /// Always a full evaluation; no revision-checked shortcut applies here.
    pub(crate) async fn reauthorize_request(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
        request: OperationRequest,
    ) -> ServiceResult<Box<AuthorizedOperation>> {
        self.check_reauthorization_preconditions(service, context, &request)
            .await?;
        self.full_reauthorize(service, context, request).await
    }

    /// Applies the proof-shape, cancellation, and deadline gates shared by
    /// every reauthorization entry point.
    async fn check_reauthorization_preconditions(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
        request: &OperationRequest,
    ) -> ServiceResult<()> {
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
        Ok(())
    }

    /// Reloads exact current facts and re-decides — the historical safe point.
    async fn full_reauthorize(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
        request: OperationRequest,
    ) -> ServiceResult<Box<AuthorizedOperation>> {
        match service
            .providers
            .policy
            .authorize(context.principal(), request)
        {
            Ok(Decision::Allow(authorization)) => {
                let obligations = authorization.obligations();
                if service.executors.is_follower()
                    && self.operation != ServiceOperationV1::GetStatistics
                    && obligations.audit_class().is_some()
                {
                    // A new audit obligation at a read safe point cannot be
                    // satisfied by telemetry or release protected data.
                    return Err(PublicError::follower_mode().into());
                }
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
        // When a deferred Started is still pending, fuse Started+terminal into
        // one coordinator message and one durable transition (PERF-006 replay).
        if self.deferred_start.load(Ordering::Acquire) {
            let started_input = ServiceAuditInput::new(
                context,
                self.operation,
                ServiceAuditPhaseV1::Started,
                self.targets.clone(),
                self.approval_id.clone(),
                ServiceAuditLinkV1::None,
            )
            .map_err(|_| AuditAppendFailure::subsystem())?;
            let terminal_input = ServiceAuditInput::new(
                context,
                self.operation,
                phase,
                self.targets.clone(),
                self.approval_id.clone(),
                link,
            )
            .map_err(|_| AuditAppendFailure::subsystem())?;
            self.lifecycle
                .begin_terminal(phase, link)
                .map_err(|_| AuditAppendFailure::subsystem())?;
            let control = terminal_audit_control(context.control());
            let result = service
                .append_prepared_audit_pair(&control, started_input, terminal_input)
                .await;
            if result.is_ok() {
                self.lifecycle.mark_durable_start();
                self.deferred_start.store(false, Ordering::Release);
            }
            self.lifecycle.finish_terminal(result.is_ok());
            return result;
        }
        let input = ServiceAuditInput::new(
            context,
            self.operation,
            phase,
            self.targets.clone(),
            self.approval_id.clone(),
            link,
        )
        .map_err(|_| AuditAppendFailure::subsystem())?;
        self.lifecycle
            .begin_terminal(phase, link)
            .map_err(|_| AuditAppendFailure::subsystem())?;
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
        } else if phase == ServiceAuditPhaseV1::Denied && !service.executors.is_follower() {
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
        if let Err(failure) = &result {
            service.note_audit_failure_with_cause(self.operation, failure.cause());
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
        .map_err(|_| AuditAppendFailure::subsystem())?;
        self.lifecycle
            .begin_terminal(phase, link)
            .map_err(|_| AuditAppendFailure::subsystem())?;
        let control = terminal_audit_control(context.control());
        let result = service.append_prepared_audit(&control, input).await;
        self.lifecycle.finish_terminal(result.is_ok());
        result
    }
}

impl BegunApplicationExportInvocation {
    /// Closed operation retained by this audit lifecycle.
    pub(crate) const fn operation(&self) -> ServiceOperationV1 {
        self.completion.operation()
    }

    /// Borrows the one-use proof only until the final post-capacity safe point.
    pub(crate) const fn initial_authorization(&self) -> &AuthorizedApplicationExportV1 {
        &self.authorization
    }

    /// Appends the terminal audit before any protected output is released.
    pub(crate) async fn finish(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
        phase: ServiceAuditPhaseV1,
    ) -> Result<(), AuditAppendFailure> {
        self.completion
            .finish(service, context, phase, ServiceAuditLinkV1::None)
            .await
    }
}

impl BegunApplicationReimportInvocation {
    /// Closed operation retained by this audit lifecycle.
    pub(crate) const fn operation(&self) -> ServiceOperationV1 {
        self.completion.operation()
    }

    /// Borrows the one-use proof only until the final post-capacity safe point.
    pub(crate) const fn initial_authorization(&self) -> &AuthorizedApplicationReimportV1 {
        &self.authorization
    }

    /// Appends the terminal audit before any protected output is released.
    pub(crate) async fn finish(
        &self,
        service: &RiffDbServiceInner,
        context: &RequestContext,
        phase: ServiceAuditPhaseV1,
    ) -> Result<(), AuditAppendFailure> {
        self.completion
            .finish(service, context, phase, ServiceAuditLinkV1::None)
            .await
    }
}

impl RiffDbServiceInner {
    /// Performs the specialized V5 export safe point and durably starts its
    /// intrinsic audit lifecycle. Export authority never passes through the
    /// ordinary capability-permission registry.
    pub(crate) async fn begin_application_export_invocation(
        &self,
        context: &RequestContext,
        request: ApplicationExportAuthorizationRequestV1,
        operation: ServiceOperationV1,
        targets: ServiceAuditTargetsV1,
    ) -> ServiceResult<BegunApplicationExportInvocation> {
        let expected = match request.operation() {
            ApplicationExportPolicyOperationV1::Start => ServiceOperationV1::StartApplicationExport,
            ApplicationExportPolicyOperationV1::Page => {
                ServiceOperationV1::GetApplicationExportPage
            }
            ApplicationExportPolicyOperationV1::Status => ServiceOperationV1::GetApplicationExport,
            ApplicationExportPolicyOperationV1::Cancel => {
                ServiceOperationV1::CancelApplicationExport
            }
        };
        if operation != expected {
            return Err(self.internal_failure(operation, InternalDefect::ProofMismatch));
        }
        self.classify_intrinsic_prestart(context, operation, targets.clone())?;
        if context.control().is_cancelled() || context.control().is_deadline_exceeded() {
            self.append_prestart_terminal_if_intrinsic(
                context,
                operation,
                targets,
                AuditScope::Intrinsic,
                ServiceAuditPhaseV1::Cancelled,
            )
            .await?;
            return if context.control().is_cancelled() {
                Err(ServiceFailure::Cancelled)
            } else {
                Err(ServiceFailure::DeadlineExceeded)
            };
        }
        let authorization = match self
            .providers
            .policy
            .authorize_application_export(context.principal(), request.clone())
        {
            Ok(ApplicationExportDecisionV1::Allow(authorization)) => authorization,
            Ok(ApplicationExportDecisionV1::Deny(_)) => {
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
        if authorization.database_id() != self.identity.database_id()
            || authorization.environment() != self.identity.environment()
            || authorization.request() != &request
            || authorization.authority().capability_id() != context.principal().capability_id()
            || authorization.authority().capability_revision()
                != context.principal().capability_revision()
            || authorization.principal_id() != context.principal().principal_id()
            || authorization.actor_kind() != context.principal().actor_kind()
        {
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

        let lifecycle = current_operation_audit_lifecycle(operation);
        let panic_terminal = PanicTerminalAudit::new(context, operation, targets.clone(), None)
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
        lifecycle
            .prepare_start(operation, panic_terminal)
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
        if let Err(failure) = self
            .append_audit(
                context,
                operation,
                ServiceAuditPhaseV1::Started,
                targets.clone(),
                None,
                ServiceAuditLinkV1::None,
                AuditAppendControl::Invocation,
            )
            .await
        {
            lifecycle.fail_start();
            self.note_audit_failure_with_cause(operation, failure.cause());
            return Err(PublicError::storage_unavailable().into());
        }
        lifecycle.mark_durable_start();
        lifecycle
            .confirm_start()
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
        Ok(BegunApplicationExportInvocation {
            authorization,
            completion: BegunInvocationCompletion {
                operation,
                targets,
                approval_id: None,
                started: true,
                lifecycle,
            },
        })
    }

    /// Performs the specialized V7 reimport safe point and durably starts its
    /// intrinsic audit lifecycle. Reimport authority never passes through the
    /// ordinary application command registry.
    pub(crate) async fn begin_application_reimport_invocation(
        &self,
        context: &RequestContext,
        request: ApplicationReimportAuthorizationRequestV1,
        operation: ServiceOperationV1,
        targets: ServiceAuditTargetsV1,
    ) -> ServiceResult<BegunApplicationReimportInvocation> {
        let expected = match request.operation() {
            ApplicationReimportPolicyOperationV1::Start => {
                ServiceOperationV1::StartApplicationReimport
            }
            ApplicationReimportPolicyOperationV1::Page => {
                ServiceOperationV1::ApplyApplicationReimportPage
            }
            ApplicationReimportPolicyOperationV1::Status => {
                ServiceOperationV1::GetApplicationReimport
            }
            ApplicationReimportPolicyOperationV1::Cancel => {
                ServiceOperationV1::CancelApplicationReimport
            }
        };
        if operation != expected {
            return Err(self.internal_failure(operation, InternalDefect::ProofMismatch));
        }
        self.classify_intrinsic_prestart(context, operation, targets.clone())?;
        if context.control().is_cancelled() || context.control().is_deadline_exceeded() {
            self.append_prestart_terminal_if_intrinsic(
                context,
                operation,
                targets,
                AuditScope::Intrinsic,
                ServiceAuditPhaseV1::Cancelled,
            )
            .await?;
            return if context.control().is_cancelled() {
                Err(ServiceFailure::Cancelled)
            } else {
                Err(ServiceFailure::DeadlineExceeded)
            };
        }
        let authorization = match self
            .providers
            .policy
            .authorize_application_reimport(context.principal(), request.clone())
        {
            Ok(ApplicationReimportDecisionV1::Allow(authorization)) => authorization,
            Ok(ApplicationReimportDecisionV1::Deny(_)) => {
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
        if authorization.database_id() != self.identity.database_id()
            || authorization.environment() != self.identity.environment()
            || authorization.request() != &request
            || authorization.authority().capability_id() != context.principal().capability_id()
            || authorization.authority().capability_revision()
                != context.principal().capability_revision()
            || authorization.principal_id() != context.principal().principal_id()
            || authorization.actor_kind() != context.principal().actor_kind()
        {
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

        let lifecycle = current_operation_audit_lifecycle(operation);
        let panic_terminal = PanicTerminalAudit::new(context, operation, targets.clone(), None)
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
        lifecycle
            .prepare_start(operation, panic_terminal)
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
        if let Err(failure) = self
            .append_audit(
                context,
                operation,
                ServiceAuditPhaseV1::Started,
                targets.clone(),
                None,
                ServiceAuditLinkV1::None,
                AuditAppendControl::Invocation,
            )
            .await
        {
            lifecycle.fail_start();
            self.note_audit_failure_with_cause(operation, failure.cause());
            return Err(PublicError::storage_unavailable().into());
        }
        lifecycle.mark_durable_start();
        lifecycle
            .confirm_start()
            .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
        Ok(BegunApplicationReimportInvocation {
            authorization,
            completion: BegunInvocationCompletion {
                operation,
                targets,
                approval_id: None,
                started: true,
                lifecycle,
            },
        })
    }

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
        if let Err(failure) = self
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
        {
            lifecycle.fail_start();
            self.note_audit_failure_with_cause(operation, failure.cause());
            return Err(PublicError::storage_unavailable().into());
        }
        lifecycle.mark_durable_start();
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
        self.begin_invocation_with_start(context, request, targets, scope, false)
            .await
    }

    pub(crate) async fn begin_compound_command_invocation(
        &self,
        context: &RequestContext,
        request: OperationRequest,
        targets: ServiceAuditTargetsV1,
    ) -> ServiceResult<BegunInvocation> {
        if request.operation() != ServiceOperationV1::ExecuteCommand {
            return Err(self.internal_failure(request.operation(), InternalDefect::ProofMismatch));
        }
        self.begin_invocation_with_start(context, request, targets, AuditScope::Intrinsic, true)
            .await
    }

    async fn begin_invocation_with_start(
        &self,
        context: &RequestContext,
        request: OperationRequest,
        targets: ServiceAuditTargetsV1,
        scope: AuditScope,
        defer_command_start: bool,
    ) -> ServiceResult<BegunInvocation> {
        let operation = request.operation();
        // ADR-0178 / SPEC 13.5 explicitly permit follower operational telemetry.
        // Keep the original policy proof (including its administrative audit
        // class) for exact reauthorization and result shaping. No local audit
        // lifecycle is created for this intrinsic diagnostic exception.
        let follower_statistics =
            self.executors.is_follower() && operation == ServiceOperationV1::GetStatistics;
        let scope = if follower_statistics {
            AuditScope::StandardRead
        } else {
            scope
        };
        if scope == AuditScope::Intrinsic {
            self.executors.writer()?;
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

        // Capture generation *before* the begin evaluation. A concurrent
        // publish (create/update/revoke/bootstrap) that races the evaluation
        // either:
        //   - is already visible to authorize (decision reflects it), or
        //   - lands after this load so recheck sees a moved generation and
        //     falls through to full re-evaluation.
        // Capturing after authorize would allow a revoke between the decision
        // and the generation load to stamp a post-revoke generation onto an
        // Allow decision, wrongly enabling the cheap reissue path.
        let policy_generation_at_begin = self.providers.policy.capability_view_generation();

        let authorization = match self
            .providers
            .policy
            .authorize(context.principal(), request.clone())
        {
            Ok(Decision::Allow(authorization)) => authorization,
            Ok(Decision::Deny(_)) => {
                if self.executors.is_follower() {
                    // The operation wrapper records bounded redacted terminal
                    // telemetry. This is not an audit-subsystem failure.
                    return Err(PublicError::authorization_denied().into());
                }
                if scope == AuditScope::Intrinsic {
                    self.append_prestart_terminal_if_intrinsic(
                        context,
                        operation,
                        targets,
                        scope,
                        ServiceAuditPhaseV1::Denied,
                    )
                    .await?;
                } else if let Err(failure) = self
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
                {
                    self.note_audit_failure_with_cause(operation, failure.cause());
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
        if audit_class.is_some() && !follower_statistics {
            self.executors.writer()?;
        }
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
        let started = audit_class.is_some() && !follower_statistics;
        let lifecycle = current_operation_audit_lifecycle(operation);
        if started {
            let panic_terminal =
                PanicTerminalAudit::new(context, operation, targets.clone(), approval_id.clone())
                    .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
            lifecycle
                .prepare_start(operation, panic_terminal)
                .map_err(|_| self.internal_failure(operation, InternalDefect::ProofMismatch))?;
            if !defer_command_start {
                if let Err(failure) = self
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
                {
                    lifecycle.fail_start();
                    self.note_audit_failure_with_cause(operation, failure.cause());
                    return Err(PublicError::storage_unavailable().into());
                }
                lifecycle.mark_durable_start();
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
            deferred_start: AtomicBool::new(defer_command_start && started),
            initial_authorization: authorization,
            policy_generation_at_begin,
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
        if let Err(failure) = &result {
            self.note_audit_failure_with_cause(operation, failure.cause());
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
            .map_err(|_| AuditAppendFailure::subsystem())?;
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
            self.executors
                .writer()
                .map_err(|_| AuditAppendFailure::subsystem())?
                .audit
                .reserve_capacity(),
        )
        .await
        .map_err(|error: ControlledWaitError| match error {
            ControlledWaitError::Cancelled | ControlledWaitError::DeadlineExceeded => {
                AuditAppendFailure::request_scoped()
            }
        })?
        // Coordinator admission failures are always subsystem-level.
        .map_err(|_| AuditAppendFailure::subsystem())?;
        let receipt = permit
            .submit(Box::new(input))
            .map_err(|_| AuditAppendFailure::subsystem())?;
        self.await_audit_receipt(receipt).await
    }

    async fn append_prepared_audit_pair(
        &self,
        control: &RequestControl,
        started: ServiceAuditInput,
        terminal: ServiceAuditInput,
    ) -> Result<(), AuditAppendFailure> {
        let permit = wait_with_control(
            control,
            self.providers.deadline_scheduler.as_ref(),
            self.executors
                .writer()
                .map_err(|_| AuditAppendFailure::subsystem())?
                .audit
                .reserve_capacity(),
        )
        .await
        .map_err(|error: ControlledWaitError| match error {
            ControlledWaitError::Cancelled | ControlledWaitError::DeadlineExceeded => {
                AuditAppendFailure::request_scoped()
            }
        })?
        .map_err(|_| AuditAppendFailure::subsystem())?;
        let receipt = permit
            .submit_fused_pair(Box::new(started), Box::new(terminal))
            .map_err(|_| AuditAppendFailure::subsystem())?;
        self.await_audit_receipt(receipt).await
    }

    async fn await_audit_receipt(
        &self,
        receipt: riffdb_commit::AdministrationAuditReceipt,
    ) -> Result<(), AuditAppendFailure> {
        match receipt.completion().await {
            Ok(()) => {
                self.audit_failures.reset();
                Ok(())
            }
            Err(error) => {
                let fenced = matches!(error, AdministrationAuditExecutionError::CoordinatorFenced)
                    || self
                        .executors
                        .writer()
                        .map_err(|_| AuditAppendFailure::subsystem())?
                        .audit
                        .lifecycle_state()
                        == CoordinatorLifecycleState::Fenced;
                if fenced {
                    self.providers.health.fail_authoritative_readiness(
                        crate::AuthoritativeReadinessFailure::CoordinatorFenced,
                    );
                }
                Err(AuditAppendFailure::subsystem())
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
            .map_err(|_| AuditAppendFailure::subsystem())?;
        let (preparation, deadline) = lifecycle
            .begin_bootstrap_terminal()
            .map_err(|_| AuditAppendFailure::subsystem())?;
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
            self.executors
                .writer()
                .map_err(|_| AuditAppendFailure::subsystem())?
                .control_plane
                .reserve_capacity(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(error)) => {
                self.note_bootstrap_terminal_failure(
                    error == ControlPlaneExecutionAdmissionError::Fenced,
                );
                return Err(AuditAppendFailure::subsystem());
            }
            Err(_) => {
                self.note_bootstrap_terminal_failure(false);
                return Err(AuditAppendFailure::subsystem());
            }
        };
        let receipt = match permit.submit_capability_bootstrap_terminal(preparation) {
            Ok(receipt) => receipt,
            Err(error) => {
                self.note_bootstrap_terminal_failure(
                    error == ControlPlaneExecutionAdmissionError::Fenced,
                );
                return Err(AuditAppendFailure::subsystem());
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
                Err(AuditAppendFailure::subsystem())
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
        self.note_audit_failure_with_cause(operation, AuditFailureCause::SubsystemUnavailable);
    }

    pub(crate) fn note_audit_failure_with_cause(
        &self,
        operation: ServiceOperationV1,
        cause: AuditFailureCause,
    ) {
        self.providers
            .telemetry
            .record(crate::ServiceTelemetryEvent::AuditUnavailable { operation });
        match cause {
            AuditFailureCause::SubsystemUnavailable => {
                self.providers.health.fail_authoritative_readiness(
                    crate::AuthoritativeReadinessFailure::AuditUnavailable,
                );
            }
            AuditFailureCause::RequestScoped => {
                if self.audit_failures.note_request_scoped_failure() {
                    self.providers.health.fail_authoritative_readiness(
                        crate::AuthoritativeReadinessFailure::AuditUnavailable,
                    );
                }
            }
        }
    }
}

pub(crate) fn terminal_audit_control(request: &RequestControl) -> RequestControl {
    let (control, _unused_cancellation) = RequestControl::new(request.deadline());
    control
}

/// Required audit work could not be proven durable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AuditAppendFailure {
    cause: AuditFailureCause,
}

impl AuditAppendFailure {
    pub(crate) const fn subsystem() -> Self {
        Self {
            cause: AuditFailureCause::SubsystemUnavailable,
        }
    }

    pub(crate) const fn request_scoped() -> Self {
        Self {
            cause: AuditFailureCause::RequestScoped,
        }
    }

    pub(crate) const fn cause(self) -> AuditFailureCause {
        self.cause
    }
}

/// Closed classification for one audit-append failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AuditFailureCause {
    /// Coordinator stopped/draining/fenced or an unknown audit outcome.
    SubsystemUnavailable,
    /// Deadline, cancellation, or capacity for this request only.
    RequestScoped,
}

/// Consecutive request-scoped audit failures before permanent readiness stop.
pub(crate) const MAX_CONSECUTIVE_AUDIT_FAILURES: u32 = 8;

/// Process-local consecutive request-scoped audit failure counter.
pub(crate) struct AuditFailureTracker {
    consecutive: std::sync::atomic::AtomicU32,
}

impl AuditFailureTracker {
    pub(crate) const fn new() -> Self {
        Self {
            consecutive: std::sync::atomic::AtomicU32::new(0),
        }
    }

    pub(crate) fn reset(&self) {
        self.consecutive
            .store(0, std::sync::atomic::Ordering::Release);
    }

    /// Records one request-scoped failure. Returns true when the closed
    /// consecutive threshold has been crossed and routing must stop.
    pub(crate) fn note_request_scoped_failure(&self) -> bool {
        let next = self
            .consecutive
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            .saturating_add(1);
        next >= MAX_CONSECUTIVE_AUDIT_FAILURES
    }
}

impl Default for AuditFailureTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn request_scoped_audit_tracker_resets_on_success_and_trips_at_eight() {
        let tracker = AuditFailureTracker::new();
        for _ in 0..7 {
            assert!(!tracker.note_request_scoped_failure());
        }
        tracker.reset();
        for _ in 0..7 {
            assert!(!tracker.note_request_scoped_failure());
        }
        assert!(tracker.note_request_scoped_failure());
    }

    #[test]
    fn request_scoped_cause_does_not_equal_subsystem_cause() {
        assert_ne!(
            AuditFailureCause::RequestScoped,
            AuditFailureCause::SubsystemUnavailable
        );
        assert_eq!(
            AuditAppendFailure::request_scoped().cause(),
            AuditFailureCause::RequestScoped
        );
        assert_eq!(
            AuditAppendFailure::subsystem().cause(),
            AuditFailureCause::SubsystemUnavailable
        );
    }

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
