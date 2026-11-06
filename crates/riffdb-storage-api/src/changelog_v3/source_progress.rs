//! Bounded operational observation, with no persistence or retention authority.
use std::num::NonZeroU32;

use riffdb_types::CommitSequence;

use super::{
    ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogV3Error,
    MAX_REPLICATION_SOURCE_HOLDS_V1,
};

/// Bounded maximum voter followers that can be required for a quorum commit.
pub const MAX_QUORUM_ACK_FOLLOWERS_V1: usize = 15;

/// Source head and slowest registered follower acknowledgement from one pin.
/// Acknowledgement is evidence of the follower's durable applied prefix; the
/// source cannot observe later unacknowledged application. No hold IDs escape.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ReplicationSourceProgressV3 {
    history: ChangelogHistoryStateV3,
    follower_count: u32,
    oldest_acknowledged: Option<ChangelogHistoryPointV3>,
    top_acknowledged: [Option<ChangelogHistoryPointV3>; MAX_QUORUM_ACK_FOLLOWERS_V1],
    degraded_followers: u32,
    maintenance_pending: bool,
}

impl ReplicationSourceProgressV3 {
    /// Checks bounded population and position consistency. The storage reader
    /// owns same-pin provenance; this value never authorizes replay or pruning.
    pub fn new(
        history: ChangelogHistoryStateV3,
        follower_count: u32,
        oldest_acknowledged: Option<ChangelogHistoryPointV3>,
    ) -> Result<Self, ChangelogV3Error> {
        if u64::from(follower_count) > MAX_REPLICATION_SOURCE_HOLDS_V1 {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        if (follower_count == 0) != oldest_acknowledged.is_none() {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let mut top_acknowledged = [None; MAX_QUORUM_ACK_FOLLOWERS_V1];
        if let Some(point) = oldest_acknowledged {
            if point.sequence() < history.minimum_resume().sequence() {
                return Err(ChangelogV3Error::PredecessorMismatch);
            }
            ChangelogHistoryStateV3::new(
                history.lineage(),
                history.minimum_resume(),
                history.tail(),
                point,
            )?;
            let fill = (follower_count as usize).min(MAX_QUORUM_ACK_FOLLOWERS_V1);
            for slot in top_acknowledged.iter_mut().take(fill) {
                *slot = Some(point);
            }
        }
        Ok(Self {
            history,
            follower_count,
            oldest_acknowledged,
            top_acknowledged,
            degraded_followers: 0,
            maintenance_pending: false,
        })
    }
    /// Records the highest acknowledged active follower frontiers in descending
    /// sequence order (`top_acknowledged[0]` is the highest follower frontier).
    pub fn with_top_acknowledged(
        mut self,
        top_acknowledged: [Option<ChangelogHistoryPointV3>; MAX_QUORUM_ACK_FOLLOWERS_V1],
    ) -> Result<Self, ChangelogV3Error> {
        let expected_present = (self.follower_count as usize).min(MAX_QUORUM_ACK_FOLLOWERS_V1);
        let mut prev_seq = None;
        for (idx, item) in top_acknowledged.iter().enumerate() {
            if idx < expected_present {
                let Some(point) = *item else {
                    return Err(ChangelogV3Error::InvalidEncoding);
                };
                if point.sequence() < self.history.minimum_resume().sequence() {
                    return Err(ChangelogV3Error::PredecessorMismatch);
                }
                ChangelogHistoryStateV3::new(
                    self.history.lineage(),
                    self.history.minimum_resume(),
                    self.history.tail(),
                    point,
                )?;
                if let Some(prev) = prev_seq
                    && point.sequence() > prev
                {
                    return Err(ChangelogV3Error::InvalidEncoding);
                }
                if let Some(oldest) = self.oldest_acknowledged
                    && point.sequence() < oldest.sequence()
                {
                    return Err(ChangelogV3Error::InvalidEncoding);
                }
                prev_seq = Some(point.sequence());
            } else if item.is_some() {
                return Err(ChangelogV3Error::InvalidEncoding);
            }
        }
        self.top_acknowledged = top_acknowledged;
        Ok(self)
    }
    /// Returns the `k`-th highest active follower acknowledgement (`1`-indexed).
    #[must_use]
    pub const fn kth_follower_acknowledged(
        &self,
        k: NonZeroU32,
    ) -> Option<ChangelogHistoryPointV3> {
        let idx = (k.get() - 1) as usize;
        if idx >= MAX_QUORUM_ACK_FOLLOWERS_V1 {
            return None;
        }
        self.top_acknowledged[idx]
    }
    /// Returns `true` when at least `required_followers` active registered
    /// followers have durably acknowledged an application frontier at or above
    /// `commit_sequence` in this published lineage.
    #[must_use]
    pub fn has_quorum_acknowledged_application_commit(
        &self,
        commit_sequence: CommitSequence,
        required_followers: NonZeroU32,
    ) -> bool {
        self.kth_follower_acknowledged(required_followers)
            .and_then(|point| point.frontier().application())
            .is_some_and(|acknowledged| acknowledged >= commit_sequence)
    }
    /// Published source history; never a private writer frontier.
    #[must_use]
    pub const fn history(self) -> ChangelogHistoryStateV3 {
        self.history
    }
    /// Number of durable follower holds, excluding bootstrap and archive holds.
    #[must_use]
    pub const fn follower_count(self) -> u32 {
        self.follower_count
    }
    /// Binds the bounded count of live policies whose budget or configured expiry
    /// is reached in this same source pin. This is health, never release authority.
    pub fn with_degraded_followers(mut self, degraded: u32) -> Result<Self, ChangelogV3Error> {
        if degraded > self.follower_count || (degraded == 0 && self.maintenance_pending) {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        self.degraded_followers = degraded;
        Ok(self)
    }
    /// Live registrations with reached policy limits, including caught-up expiry.
    #[must_use]
    pub const fn degraded_followers(self) -> u32 {
        self.degraded_followers
    }
    /// Adds a scheduling hint from the same bounded policy observation. This
    /// never authorizes a transition or selects a registration for the writer.
    pub fn with_registration_maintenance_pending(
        mut self,
        pending: bool,
    ) -> Result<Self, ChangelogV3Error> {
        if pending && self.degraded_followers == 0 {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        self.maintenance_pending = pending;
        Ok(self)
    }
    /// At least one live policy requires degradation persistence or expiry review.
    #[must_use]
    pub const fn registration_maintenance_pending(self) -> bool {
        self.maintenance_pending
    }
    /// Slowest exact acknowledgement. None means no registered followers.
    #[must_use]
    pub const fn oldest_acknowledged(self) -> Option<ChangelogHistoryPointV3> {
        self.oldest_acknowledged
    }
}
impl std::fmt::Debug for ReplicationSourceProgressV3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationSourceProgressV3([redacted])")
    }
}
