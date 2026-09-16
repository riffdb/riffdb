//! Source-only follower registration policy in the existing bounded hold domain.
//! Values and decoded bytes provide neither admission nor release authority.

use super::{
    ChangelogHistoryPointV3 as Point, ChangelogTransactionSequence, ChangelogV3Error as Error,
    FollowerHoldBudget, ReplicationSourceHoldKindV1, ReplicationSourceHoldV1,
};
use riffdb_types::CommitSequence;

/// Closed registration lifecycle, distinct from the frozen V1 hold-owner tags.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FollowerRegistrationPhaseV1 {
    /// Registration pins its exact predecessor until a new bootstrap attaches.
    AwaitingBootstrap,
    /// A durably attached follower advances its acknowledgement fence.
    Attached,
    /// An audited retirement/expiry completed; this value grants no reattachment.
    Retired,
}

/// Successor source-hold value retaining the exact V1 fence and checked policy.
/// The owner must use the registry migration and audited administration path;
/// constructing this value does not perform or prove either transition.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ReplicationSourceHoldV2 {
    hold: ReplicationSourceHoldV1,
    registered_at: Point,
    generation: ChangelogTransactionSequence,
    budget: FollowerHoldBudget,
    expires_at: Option<CommitSequence>,
    phase: FollowerRegistrationPhaseV1,
    degraded_at: Option<Point>,
}

impl ReplicationSourceHoldV2 {
    /// Checks policy shape without inferring receipt ancestry or permission.
    /// `registered_at` is the source predecessor of the registration transaction;
    /// its checked successor is the immutable registration generation.
    /// Expiry is an explicit future application sequence, never a wall clock.
    pub fn new(
        hold: ReplicationSourceHoldV1,
        registered_at: Point,
        budget: FollowerHoldBudget,
        expires_at: Option<CommitSequence>,
        phase: FollowerRegistrationPhaseV1,
        degraded_at: Option<Point>,
    ) -> Result<Self, Error> {
        let generation = registered_at
            .sequence()
            .checked_next()
            .ok_or(Error::InvalidEncoding)?;
        if hold.kind() != ReplicationSourceHoldKindV1::FollowerAcknowledgement
            || (!hold.fence().precedes_or_equals(registered_at)
                && !registered_at.precedes_or_equals(hold.fence()))
            || (phase == FollowerRegistrationPhaseV1::AwaitingBootstrap
                && hold.fence() != registered_at)
            || expires_at.is_some_and(|expiry| {
                expiry.get()
                    <= registered_at
                        .frontier()
                        .application()
                        .map_or(0, |s| s.get())
            })
        {
            return Err(Error::InvalidEncoding);
        }
        if let Some(observed) = degraded_at {
            if !hold.fence().precedes_or_equals(observed)
                || !registered_at.precedes_or_equals(observed)
            {
                return Err(Error::PredecessorMismatch);
            }
            let head = observed.frontier().application().map_or(0, |s| s.get());
            let ack = hold.fence().frontier().application().map_or(0, |s| s.get());
            let lag = head.checked_sub(ack).ok_or(Error::PredecessorMismatch)?;
            if lag < budget.sequences() && expires_at.is_none_or(|expiry| head < expiry.get()) {
                return Err(Error::InvalidEncoding);
            }
        }
        Ok(Self {
            hold,
            registered_at,
            generation,
            budget,
            expires_at,
            phase,
            degraded_at,
        })
    }

    /// Exact V1 owner, lineage, identity and acknowledgement bytes.
    #[must_use]
    pub const fn hold(self) -> ReplicationSourceHoldV1 {
        self.hold
    }
    /// Source predecessor pinned by the registration writer.
    #[must_use]
    pub const fn registered_at(self) -> Point {
        self.registered_at
    }
    /// Registration identity within this hold's lineage. Not an authority token.
    #[must_use]
    pub const fn generation(self) -> ChangelogTransactionSequence {
        self.generation
    }
    /// Explicit sequence allowance.
    #[must_use]
    pub const fn budget(self) -> FollowerHoldBudget {
        self.budget
    }
    /// Optional configured source application sequence for expiry.
    #[must_use]
    pub const fn expires_at(self) -> Option<CommitSequence> {
        self.expires_at
    }
    /// Stored lifecycle; callers may not infer a transition from a decoded value.
    #[must_use]
    pub const fn phase(self) -> FollowerRegistrationPhaseV1 {
        self.phase
    }
    /// Persisted degradation observation. This is not an expiry-release permit.
    #[must_use]
    pub const fn degraded_at(self) -> Option<Point> {
        self.degraded_at
    }
}

impl std::fmt::Debug for ReplicationSourceHoldV2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationSourceHoldV2([redacted])")
    }
}
