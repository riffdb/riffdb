//! Sequence-based follower hold accounting. Observations carry no authority to
//! register, retire, expire or release a source fence (ADR-0178 section 6).

use std::num::NonZeroU64;

use super::{
    ChangelogHistoryStateV3, ChangelogV3Error, ReplicationSourceHoldKindV1, ReplicationSourceHoldV1,
};

/// Nonzero application-sequence allowance for one follower registration.
/// This semantic value has no standalone durable encoding or registration
/// authority. The registration owner must persist and authorize its policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FollowerHoldBudget(NonZeroU64);

impl FollowerHoldBudget {
    /// Zero is not an allowance; no value means an unconfigured budget, never
    /// an unlimited budget or permission to discard the follower's fence.
    #[must_use]
    pub const fn new(sequences: u64) -> Option<Self> {
        match NonZeroU64::new(sequences) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Accepted application-sequence allowance.
    #[must_use]
    pub const fn sequences(self) -> u64 {
        self.0.get()
    }

    /// Computes a health observation from one published history and hold.
    ///
    /// Checks role, lineage, retained position bounds, monotone dual frontiers,
    /// and exact hashes where positions coincide. The storage owner must prove
    /// receipt ancestry and same-pin provenance before supplying these values;
    /// an observation does not provide either proof or any release capability.
    /// Administration and physical receipt counts never consume this allowance.
    pub fn observe(
        self,
        hold: ReplicationSourceHoldV1,
        history: ChangelogHistoryStateV3,
    ) -> Result<FollowerHoldBudgetObservation, ChangelogV3Error> {
        if hold.kind() != ReplicationSourceHoldKindV1::FollowerAcknowledgement
            || hold.lineage() != history.lineage()
            || !history.minimum_resume().precedes_or_equals(hold.fence())
            || !hold.fence().precedes_or_equals(history.tail())
        {
            return Err(ChangelogV3Error::PredecessorMismatch);
        }
        let head = history
            .tail()
            .frontier()
            .application()
            .map_or(0, |s| s.get());
        let acknowledged = hold.fence().frontier().application().map_or(0, |s| s.get());
        let lag = head
            .checked_sub(acknowledged)
            .ok_or(ChangelogV3Error::PredecessorMismatch)?;
        Ok(FollowerHoldBudgetObservation {
            application_lag_sequences: lag,
            remaining_sequences: self.sequences().saturating_sub(lag),
        })
    }
}

/// A bounded health input. Exhaustion never removes or advances a retention
/// fence; only the separately authorized retirement/expiry owner may do that.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FollowerHoldBudgetObservation {
    application_lag_sequences: u64,
    remaining_sequences: u64,
}

impl FollowerHoldBudgetObservation {
    /// Distance between the published application head and durable acknowledgement.
    #[must_use]
    pub const fn application_lag_sequences(self) -> u64 {
        self.application_lag_sequences
    }

    /// Unused allowance, zero once the budget is exhausted.
    #[must_use]
    pub const fn remaining_sequences(self) -> u64 {
        self.remaining_sequences
    }

    /// Typed degradation condition, including exact equality with the allowance.
    #[must_use]
    pub const fn is_exhausted(self) -> bool {
        self.remaining_sequences == 0
    }
}
