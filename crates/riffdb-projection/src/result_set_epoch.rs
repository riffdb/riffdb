//! Runtime-only result-set epoch witnesses and deterministic lag health (ADR-0130).

use std::error::Error;
use std::fmt;

use riffdb_types::{
    ApplicationRoleHash, CommitSequence, ProjectionGeneration, ProjectionProviderDescriptorHash,
    QueryPlanHash,
};

/// Maximum providers in one compiler-sealed result-set plan.
pub const MAX_RESULT_SET_PROVIDERS_V1: usize = 8;

/// Closed provider lifecycle observed at one negotiation safe point.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderLifecycleV1 {
    /// Provider can serve its reported interval.
    Ready,
    /// Generation is rebuilding and cannot serve normal success.
    Rebuilding,
    /// Provider has been retired.
    Retired,
    /// Provider has exceeded admitted lag and is unavailable.
    Unavailable,
}

/// One atomic provider observation used only while opening a result set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderEpochObservationV1 {
    descriptor: ProjectionProviderDescriptorHash,
    state_schema_hash: [u8; 32],
    history_incarnation: u64,
    generation: ProjectionGeneration,
    floor: CommitSequence,
    ceiling: CommitSequence,
    lifecycle: ProviderLifecycleV1,
}

impl ProviderEpochObservationV1 {
    /// Constructs one checked servable-interval observation.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        descriptor: ProjectionProviderDescriptorHash,
        state_schema_hash: [u8; 32],
        history_incarnation: u64,
        generation: ProjectionGeneration,
        floor: CommitSequence,
        ceiling: CommitSequence,
        lifecycle: ProviderLifecycleV1,
    ) -> Result<Self, ResultSetEpochError> {
        if floor > ceiling {
            return Err(ResultSetEpochError::InvalidInterval);
        }
        Ok(Self {
            descriptor,
            state_schema_hash,
            history_incarnation,
            generation,
            floor,
            ceiling,
            lifecycle,
        })
    }

    /// Exact descriptor digest.
    #[must_use]
    pub const fn descriptor(&self) -> ProjectionProviderDescriptorHash {
        self.descriptor
    }
    /// Immutable provider-state schema identity.
    #[must_use]
    pub const fn state_schema_hash(&self) -> [u8; 32] {
        self.state_schema_hash
    }
    /// Authoritative history incarnation.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }
    /// Never-reused provider generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }
    /// Earliest retained servable epoch.
    #[must_use]
    pub const fn floor(&self) -> CommitSequence {
        self.floor
    }
    /// Latest completely published servable epoch.
    #[must_use]
    pub const fn ceiling(&self) -> CommitSequence {
        self.ceiling
    }
    /// Current lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> ProviderLifecycleV1 {
        self.lifecycle
    }
}

/// Minimal process-local witness retained by one opened result set.
///
/// This type deliberately has no serialization API. A future public
/// continuation must use its own separately versioned proof format; this
/// witness cannot become an authority token or durable compatibility boundary
/// by accident.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResultSetEpochProofV1 {
    plan_identity: QueryPlanHash,
    policy_shape_identity: ApplicationRoleHash,
    selected_epoch: CommitSequence,
    history_incarnation: u64,
    participant_count: u8,
    participants: [Option<ProviderEpochObservationV1>; MAX_RESULT_SET_PROVIDERS_V1],
}

/// Exact compiler-owned identities shared by every provider in one result set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResultSetEpochContextV1 {
    plan_identity: QueryPlanHash,
    policy_shape_identity: ApplicationRoleHash,
}

impl ResultSetEpochContextV1 {
    /// Binds the exact query plan and separately compiled authorization shape.
    #[must_use]
    pub const fn new(
        plan_identity: QueryPlanHash,
        policy_shape_identity: ApplicationRoleHash,
    ) -> Self {
        Self {
            plan_identity,
            policy_shape_identity,
        }
    }
}

/// Compiler/service-owned epoch constraint for opening or continuing a result set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultSetEpochRequirementV1 {
    /// Select the newest common epoch.
    Latest,
    /// Select the newest common epoch at least as new as a causal fence.
    AtLeast(CommitSequence),
    /// Resume at one exact retained epoch without silently advancing it.
    Exact(CommitSequence),
}

impl ResultSetEpochProofV1 {
    /// Exact query-plan identity for which this proof was negotiated.
    #[must_use]
    pub const fn plan_identity(&self) -> QueryPlanHash {
        self.plan_identity
    }
    /// Exact compiled role/policy shape used for authorization.
    #[must_use]
    pub const fn policy_shape_identity(&self) -> ApplicationRoleHash {
        self.policy_shape_identity
    }
    /// Newest epoch present in every participant interval.
    #[must_use]
    pub const fn selected_epoch(&self) -> CommitSequence {
        self.selected_epoch
    }
    /// Shared authoritative history incarnation.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }
    /// Exact descriptor/state/generation observations bound into the proof.
    #[must_use]
    pub fn participants(&self) -> impl ExactSizeIterator<Item = &ProviderEpochObservationV1> {
        self.participants[..usize::from(self.participant_count)]
            .iter()
            .map(|participant| participant.as_ref().expect("occupied bounded proof slot"))
    }
}

/// Selects the newest common epoch exactly once while opening a result set.
pub fn negotiate_result_set_epoch_v1(
    context: ResultSetEpochContextV1,
    participants: &[ProviderEpochObservationV1],
    requirement: ResultSetEpochRequirementV1,
) -> Result<ResultSetEpochProofV1, ResultSetEpochError> {
    if participants.is_empty() {
        return Err(ResultSetEpochError::EmptyParticipants);
    }
    if participants.len() > MAX_RESULT_SET_PROVIDERS_V1 {
        return Err(ResultSetEpochError::TooManyParticipants);
    }
    let incarnation = participants[0].history_incarnation;
    let mut floor = participants[0].floor;
    let mut ceiling = participants[0].ceiling;
    for participant in participants {
        match participant.lifecycle {
            ProviderLifecycleV1::Ready => {}
            ProviderLifecycleV1::Retired => return Err(ResultSetEpochError::Retired),
            ProviderLifecycleV1::Rebuilding => return Err(ResultSetEpochError::Rebuilding),
            ProviderLifecycleV1::Unavailable => return Err(ResultSetEpochError::Unavailable),
        }
        if participant.history_incarnation != incarnation {
            return Err(ResultSetEpochError::IncarnationMismatch);
        }
        floor = floor.max(participant.floor);
        ceiling = ceiling.min(participant.ceiling);
    }
    if floor > ceiling {
        return Err(ResultSetEpochError::Diverged);
    }
    let selected_epoch = match requirement {
        ResultSetEpochRequirementV1::Latest => ceiling,
        ResultSetEpochRequirementV1::AtLeast(required) => {
            if required > ceiling {
                return Err(ResultSetEpochError::FreshnessUnavailable);
            }
            ceiling
        }
        ResultSetEpochRequirementV1::Exact(required) => {
            if required < floor {
                return Err(ResultSetEpochError::EpochExpired);
            }
            if required > ceiling {
                return Err(ResultSetEpochError::FreshnessUnavailable);
            }
            required
        }
    };
    let mut bounded_participants = [None; MAX_RESULT_SET_PROVIDERS_V1];
    for (slot, participant) in bounded_participants.iter_mut().zip(participants) {
        *slot = Some(*participant);
    }
    Ok(ResultSetEpochProofV1 {
        plan_identity: context.plan_identity,
        policy_shape_identity: context.policy_shape_identity,
        selected_epoch,
        history_incarnation: incarnation,
        participant_count: u8::try_from(participants.len()).expect("participant bound fits u8"),
        participants: bounded_participants,
    })
}

/// Closed negotiation failures, never ordinary partial success.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultSetEpochError {
    /// A compiled result-set plan had no provider participants.
    EmptyParticipants,
    /// Participant count exceeds the fixed compiler bound.
    TooManyParticipants,
    /// A provider reported floor greater than ceiling.
    InvalidInterval,
    /// Provider histories differ, commonly after restore.
    IncarnationMismatch,
    /// Provider intervals have no common epoch.
    Diverged,
    /// Requested continuation epoch is no longer retained.
    EpochExpired,
    /// Requested causal epoch is newer than the common ceiling.
    FreshnessUnavailable,
    /// A required provider generation is rebuilding.
    Rebuilding,
    /// A required provider generation is retired.
    Retired,
    /// A required provider is unavailable under admitted load.
    Unavailable,
}

impl fmt::Display for ResultSetEpochError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "projection result-set epoch unavailable: {self:?}"
        )
    }
}

impl Error for ResultSetEpochError {}

/// Deterministic provider-health projection from sustained admitted lag.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultSetLagHealthV1 {
    /// Lag is within the descriptor's limit or not yet sustained.
    Ready,
    /// Sustained lag is observable but normal reads may still be recovering.
    Degraded,
    /// Sustained lag refuses normal success until recovery.
    Unavailable,
}

/// Pure bounded state machine; wall clocks and scheduling stay outside it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResultSetLagMonitorV1 {
    max_lag: u64,
    degraded_after: u16,
    unavailable_after: u16,
    consecutive_breaches: u16,
    health: ResultSetLagHealthV1,
}

impl ResultSetLagMonitorV1 {
    /// Constructs fixed thresholds from compiler/operator-owned policy.
    pub fn new(
        max_lag: u64,
        degraded_after: u16,
        unavailable_after: u16,
    ) -> Result<Self, ResultSetLagMonitorError> {
        if degraded_after == 0 || unavailable_after < degraded_after {
            return Err(ResultSetLagMonitorError);
        }
        Ok(Self {
            max_lag,
            degraded_after,
            unavailable_after,
            consecutive_breaches: 0,
            health: ResultSetLagHealthV1::Ready,
        })
    }

    /// Observes one schedule step; non-admitted work cannot worsen health.
    pub fn observe(&mut self, admitted_load: bool, lag: u64) -> ResultSetLagHealthV1 {
        if !admitted_load {
            return self.health;
        }
        if lag <= self.max_lag {
            self.consecutive_breaches = 0;
            self.health = ResultSetLagHealthV1::Ready;
            return self.health;
        }
        self.consecutive_breaches = self.consecutive_breaches.saturating_add(1);
        self.health = if self.consecutive_breaches >= self.unavailable_after {
            ResultSetLagHealthV1::Unavailable
        } else if self.consecutive_breaches >= self.degraded_after {
            ResultSetLagHealthV1::Degraded
        } else {
            ResultSetLagHealthV1::Ready
        };
        self.health
    }
}

/// Invalid deterministic lag-monitor thresholds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResultSetLagMonitorError;

impl fmt::Display for ResultSetLagMonitorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("result-set lag thresholds are invalid")
    }
}

impl Error for ResultSetLagMonitorError {}
