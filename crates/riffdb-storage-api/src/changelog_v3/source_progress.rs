//! Bounded operational observation, with no persistence or retention authority.
use super::{
    ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogV3Error,
    MAX_REPLICATION_SOURCE_HOLDS_V1,
};

/// Source head and slowest registered follower acknowledgement from one pin.
/// Acknowledgement is evidence of the follower's durable applied prefix; the
/// source cannot observe later unacknowledged application. No hold IDs escape.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ReplicationSourceProgressV3 {
    history: ChangelogHistoryStateV3,
    follower_count: u32,
    oldest_acknowledged: Option<ChangelogHistoryPointV3>,
    degraded_followers: u32,
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
        }
        Ok(Self {
            history,
            follower_count,
            oldest_acknowledged,
            degraded_followers: 0,
        })
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
        if degraded > self.follower_count {
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
