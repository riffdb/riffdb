//! Fixed operational progress. None is unknown/not applicable, never zero lag.
use crate::ServiceDtoError;
use riffdb_types::DualFrontier;

/// The local node's configured replication role.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationRole {
    /// Authoritative source of the replicated history.
    Primary,
    /// Read-serving replica with a sole durable applier.
    Follower,
}

/// Bounded counters from completed durable observations. These values cannot
/// satisfy read freshness or authorize writes, acknowledgements or promotion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicationStatistics {
    role: ReplicationRole,
    source_frontier: Option<DualFrontier>,
    applied_frontier: DualFrontier,
    acknowledged_frontier: Option<DualFrontier>,
    registered_followers: Option<u32>,
    application_lag_sequences: Option<u64>,
    administration_lag_sequences: Option<u64>,
}

impl ReplicationStatistics {
    /// Primary lag measures its head against the oldest registered follower
    /// acknowledgement. Later unacknowledged follower progress is unknown.
    pub fn primary(
        head: DualFrontier,
        follower_count: u32,
        acknowledged: Option<DualFrontier>,
    ) -> Result<Self, ServiceDtoError> {
        if (follower_count == 0) != acknowledged.is_none()
            || acknowledged.is_some_and(|ack| !covers(head, ack))
        {
            return Err(ServiceDtoError::OutOfRange);
        }
        let (application_lag_sequences, administration_lag_sequences) =
            lag(Some(head), acknowledged);
        Ok(Self {
            role: ReplicationRole::Primary,
            source_frontier: Some(head),
            applied_frontier: head,
            acknowledged_frontier: acknowledged,
            registered_followers: Some(follower_count),
            application_lag_sequences,
            administration_lag_sequences,
        })
    }

    /// Follower lag measures its applied head against the last validated source
    /// report. Local acknowledgement must be within the applied history. The
    /// storage adapter owns physical-sequence checks and lineage binding of the
    /// source report to the stream handshake before supplying these frontiers.
    pub fn follower(
        applied: DualFrontier,
        acknowledged: Option<DualFrontier>,
        source_frontier: Option<DualFrontier>,
    ) -> Result<Self, ServiceDtoError> {
        if acknowledged.is_some_and(|ack| !covers(applied, ack))
            || source_frontier.is_some_and(|head| !covers(head, applied))
        {
            return Err(ServiceDtoError::OutOfRange);
        }
        let (application_lag_sequences, administration_lag_sequences) =
            lag(source_frontier, Some(applied));
        Ok(Self {
            role: ReplicationRole::Follower,
            source_frontier,
            applied_frontier: applied,
            acknowledged_frontier: acknowledged,
            registered_followers: None,
            application_lag_sequences,
            administration_lag_sequences,
        })
    }
    /// Local configured role, with no node-selection authority.
    #[must_use]
    pub const fn role(self) -> ReplicationRole {
        self.role
    }
    /// Last observed published source head; absent means unknown.
    #[must_use]
    pub const fn source_frontier(self) -> Option<DualFrontier> {
        self.source_frontier
    }
    /// Local completed durable head. On a follower this is its applied head.
    #[must_use]
    pub const fn applied_frontier(self) -> DualFrontier {
        self.applied_frontier
    }
    /// Primary's oldest registered follower acknowledgement, or the follower's
    /// own local durable acknowledgement. Absence means no registered followers
    /// on a primary, or no recorded local acknowledgement on a follower.
    #[must_use]
    pub const fn acknowledged_frontier(self) -> Option<DualFrontier> {
        self.acknowledged_frontier
    }
    /// Primary-only registered follower count; not a count of live connections.
    #[must_use]
    pub const fn registered_followers(self) -> Option<u32> {
        self.registered_followers
    }
    /// Application sequence distance to the observed source head.
    #[must_use]
    pub const fn application_lag_sequences(self) -> Option<u64> {
        self.application_lag_sequences
    }
    /// Administration sequence distance to the observed source head.
    #[must_use]
    pub const fn administration_lag_sequences(self) -> Option<u64> {
        self.administration_lag_sequences
    }
    /// Both logical frontiers are known to match. Control-only transactions
    /// allocate neither logical frontier and do not invent lag.
    #[must_use]
    pub fn is_caught_up(self) -> bool {
        self.application_lag_sequences == Some(0) && self.administration_lag_sequences == Some(0)
    }
}

fn lag(source: Option<DualFrontier>, target: Option<DualFrontier>) -> (Option<u64>, Option<u64>) {
    let Some((source, target)) = source.zip(target) else {
        return (None, None);
    };
    (
        source
            .application()
            .map_or(0, |s| s.get())
            .checked_sub(target.application().map_or(0, |s| s.get())),
        source
            .administration()
            .map_or(0, |s| s.get())
            .checked_sub(target.administration().map_or(0, |s| s.get())),
    )
}

fn covers(head: DualFrontier, before: DualFrontier) -> bool {
    head == before || head.advances_from(before)
}
