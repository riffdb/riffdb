//! One authenticated attempt's bounded external audit evidence. Persistence
//! must also freeze selection across every attempt of the same operation.
//! A new invocation cannot use a new receipt to choose another applied point.

use super::{ReplicationPromotionRequestV1, ReplicationPromotionSelectionV1};
use crate::{AuditPrincipalV1, StorageValueError};
use riffdb_types::{ApprovalId, RequestId, Timestamp};

/// Fixed bound for phase and uncertain-outcome evidence in one authenticated attempt.
pub const MAX_REPLICATION_PROMOTION_STEPS_V1: usize = 16;

/// Closed progress of one explicit attempt. These are audit facts, not permits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationPromotionPhaseV1 {
    /// Authenticated request and actor captured before policy or lifecycle changes.
    Attempted,
    /// Fresh authorization admitted draining of the received prefix.
    Draining,
    /// Receiver/applier and all readers are closed under exclusive ownership.
    Offline,
    /// Exact source proof, applied point and derived successors are frozen.
    Selected,
    /// Intent is durable before the sole possible cutover; outcome may be uncertain.
    CutoverPending,
    /// Exact committed authority has been independently reconciled with this receipt.
    CutoverCommitted,
    /// Complete ordinary source validation has succeeded for the selected lineage.
    Validated,
    /// Terminal audit result; receipt durability must precede primary readiness.
    Succeeded,
}

impl ReplicationPromotionPhaseV1 {
    fn successor(self) -> Option<Self> {
        Some(match self {
            Self::Attempted => Self::Draining,
            Self::Draining => Self::Offline,
            Self::Offline => Self::Selected,
            Self::Selected => Self::CutoverPending,
            Self::CutoverPending => Self::CutoverCommitted,
            Self::CutoverCommitted => Self::Validated,
            Self::Validated => Self::Succeeded,
            Self::Succeeded => return None,
        })
    }
    fn cutover_may_exist(self) -> bool {
        matches!(
            self,
            Self::CutoverPending | Self::CutoverCommitted | Self::Validated | Self::Succeeded
        )
    }
}

/// Closed redacted outcomes; no paths, bearer values or free-form diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationPromotionFailureV1 {
    /// Current policy or required approval did not authorize this invocation.
    AuthorizationDenied,
    /// An existing operation fixes a different request or selection.
    SelectionConflict,
    /// The configured authenticated source cannot supply fence proof.
    FenceUnavailable,
    /// Source proof, lineage, retained ancestry or registration is invalid.
    FenceInvalid,
    /// The received prefix or readers could not be drained under their bound.
    DrainFailed,
    /// A required incarnation or epoch successor is unrepresentable.
    CounterExhausted,
    /// Required storage or receipt durability could not be established.
    StorageUnavailable,
    /// Complete source validation has not established readiness.
    ValidationFailed,
}

/// Append-only attempt history. An uncertain event preserves the preceding
/// phase and never permits another cutover or a different selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationPromotionStepV1 {
    /// One ordered phase, with selection persisted together with Selected.
    Phase(ReplicationPromotionPhaseV1),
    /// Terminal authenticated refusal before a cutover could have occurred.
    Denied(ReplicationPromotionFailureV1),
    /// Terminal failure before a cutover could have occurred.
    FailedClosed(ReplicationPromotionFailureV1),
    /// Possible cutover or incomplete validation requires exact reconciliation.
    Uncertain(ReplicationPromotionFailureV1),
}

/// Promotion-only external receipt for one authenticated invocation. A retry
/// with another request ID is a separate audited attempt of the same operation.
/// The exclusive ledger owner must enforce the first frozen request/selection
/// across attempts, including attempts that fail before cutover. A denied
/// conflicting request remains audit evidence and cannot replace that choice.
///
/// This value has no persistence, peer authentication, authorization or writer
/// capability. Every phase's runtime preconditions need independent proof.
#[derive(Clone, Eq, PartialEq)]
pub struct ReplicationPromotionReceiptV1 {
    request: ReplicationPromotionRequestV1,
    request_id: RequestId,
    principal: AuditPrincipalV1,
    approval_id: Option<ApprovalId>,
    timestamp: Timestamp,
    selection: Option<ReplicationPromotionSelectionV1>,
    steps: Vec<ReplicationPromotionStepV1>,
}

impl ReplicationPromotionReceiptV1 {
    /// Captures authenticated identity before authorization. The ledger owner
    /// must make this evidence durable before lifecycle work or a terminal denial.
    #[must_use]
    pub fn attempted(
        request: ReplicationPromotionRequestV1,
        request_id: RequestId,
        principal: AuditPrincipalV1,
        approval_id: Option<ApprovalId>,
        timestamp: Timestamp,
    ) -> Self {
        Self {
            request,
            request_id,
            principal,
            approval_id,
            timestamp,
            selection: None,
            steps: vec![ReplicationPromotionStepV1::Phase(
                ReplicationPromotionPhaseV1::Attempted,
            )],
        }
    }

    /// Reconstructs bounded decoded evidence through the same phase validator.
    /// A decoder must check the step count before allocating the input vector.
    #[allow(clippy::too_many_arguments)]
    pub fn from_canonical_parts(
        request: ReplicationPromotionRequestV1,
        request_id: RequestId,
        principal: AuditPrincipalV1,
        approval_id: Option<ApprovalId>,
        timestamp: Timestamp,
        selection: Option<ReplicationPromotionSelectionV1>,
        steps: Vec<ReplicationPromotionStepV1>,
    ) -> Result<Self, StorageValueError> {
        let value = Self {
            request,
            request_id,
            principal,
            approval_id,
            timestamp,
            selection,
            steps,
        };
        value.validate()?;
        Ok(value)
    }

    /// Binds the exact offline choice and Selected phase together. Failed
    /// changes leave the original evidence untouched; exact retries are no-ops.
    pub fn record_selection(
        &mut self,
        selection: ReplicationPromotionSelectionV1,
    ) -> Result<(), StorageValueError> {
        if self.selection.as_ref() == Some(&selection) {
            return Ok(());
        }
        if self.selection.is_some()
            || self.is_terminal()
            || self.phase() != ReplicationPromotionPhaseV1::Offline
        {
            return Err(StorageValueError::InvalidShape);
        }
        let mut next = self.clone();
        next.selection = Some(selection);
        next.steps.push(ReplicationPromotionStepV1::Phase(
            ReplicationPromotionPhaseV1::Selected,
        ));
        self.install(next)
    }

    /// Appends a legal progress/outcome fact. This establishes no runtime
    /// precondition or durability; the exclusive owner must supply both.
    pub fn advance(&mut self, step: ReplicationPromotionStepV1) -> Result<(), StorageValueError> {
        if self.steps.last() == Some(&step) {
            return Ok(());
        }
        if self.is_terminal()
            || matches!(
                step,
                ReplicationPromotionStepV1::Phase(ReplicationPromotionPhaseV1::Selected)
            )
        {
            return Err(StorageValueError::InvalidShape);
        }
        let mut next = self.clone();
        next.steps.push(step);
        self.install(next)
    }

    fn install(&mut self, next: Self) -> Result<(), StorageValueError> {
        next.validate()?;
        if !next.monotonically_extends(self) {
            return Err(StorageValueError::IdentityMismatch);
        }
        *self = next;
        Ok(())
    }

    fn validate(&self) -> Result<(), StorageValueError> {
        use ReplicationPromotionFailureV1 as Failure;
        use ReplicationPromotionPhaseV1 as Phase;
        use ReplicationPromotionStepV1 as Step;
        if self.steps.len() > MAX_REPLICATION_PROMOTION_STEPS_V1 {
            return Err(StorageValueError::LimitExceeded);
        }
        if self.steps.first() != Some(&Step::Phase(Phase::Attempted)) {
            return Err(StorageValueError::InvalidShape);
        }
        let mut phase = Phase::Attempted;
        let mut terminal = false;
        let mut selected = false;
        for (index, step) in self.steps.iter().enumerate().skip(1) {
            if terminal || self.steps[index - 1] == *step {
                return Err(StorageValueError::InvalidShape);
            }
            match *step {
                Step::Phase(next) => {
                    if phase.successor() != Some(next) {
                        return Err(StorageValueError::InvalidShape);
                    }
                    selected |= next == Phase::Selected;
                    phase = next;
                    terminal = phase == Phase::Succeeded;
                }
                Step::Denied(Failure::AuthorizationDenied) if !phase.cutover_may_exist() => {
                    terminal = true
                }
                Step::Denied(Failure::SelectionConflict)
                    if !phase.cutover_may_exist() && self.selection.is_none() =>
                {
                    terminal = true
                }
                Step::FailedClosed(failure)
                    if !phase.cutover_may_exist()
                        && !matches!(
                            failure,
                            Failure::AuthorizationDenied | Failure::SelectionConflict
                        ) =>
                {
                    terminal = true
                }
                Step::Uncertain(Failure::StorageUnavailable | Failure::ValidationFailed)
                    if phase.cutover_may_exist() => {}
                _ => return Err(StorageValueError::InvalidShape),
            }
        }
        if selected != self.selection.is_some() {
            return Err(StorageValueError::InvalidShape);
        }
        if self
            .selection
            .as_ref()
            .is_some_and(|s| s.request() != self.request)
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(())
    }

    /// Exact request, actor, approval and prior history cannot be rewritten.
    #[must_use]
    pub fn monotonically_extends(&self, prior: &Self) -> bool {
        (!prior.is_terminal() || self == prior)
            && self.request == prior.request
            && self.request_id == prior.request_id
            && self.principal == prior.principal
            && self.approval_id == prior.approval_id
            && self.timestamp == prior.timestamp
            && self.steps.starts_with(&prior.steps)
            && prior
                .selection
                .as_ref()
                .is_none_or(|s| self.selection.as_ref() == Some(s))
    }
    /// Frozen operation request.
    #[must_use]
    pub const fn request(&self) -> ReplicationPromotionRequestV1 {
        self.request
    }
    /// This authenticated invocation, distinct from its stable operation.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }
    /// Digest-free actor and exact capability revision; no bearer.
    #[must_use]
    pub const fn principal(&self) -> &AuditPrincipalV1 {
        &self.principal
    }
    /// Original checked approval identity, if policy required one.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }
    /// Initial authenticated audit sample, never a cutover authorization sample.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
    /// Frozen selection, never reconstructed from a later follower frontier.
    #[must_use]
    pub const fn selection(&self) -> Option<&ReplicationPromotionSelectionV1> {
        self.selection.as_ref()
    }
    /// Bounded append-only audit and progress facts.
    #[must_use]
    pub fn steps(&self) -> &[ReplicationPromotionStepV1] {
        &self.steps
    }
    /// Last progress phase; uncertain events never advance it.
    #[must_use]
    pub fn phase(&self) -> ReplicationPromotionPhaseV1 {
        self.steps
            .iter()
            .rev()
            .find_map(|step| match step {
                ReplicationPromotionStepV1::Phase(phase) => Some(*phase),
                _ => None,
            })
            .unwrap_or(ReplicationPromotionPhaseV1::Attempted)
    }
    /// A final denial/failure, or a validated successful result.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.steps.last(),
            Some(
                ReplicationPromotionStepV1::Denied(_)
                    | ReplicationPromotionStepV1::FailedClosed(_)
                    | ReplicationPromotionStepV1::Phase(ReplicationPromotionPhaseV1::Succeeded)
            )
        )
    }
}

impl std::fmt::Debug for ReplicationPromotionReceiptV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationPromotionReceiptV1([redacted])")
    }
}
