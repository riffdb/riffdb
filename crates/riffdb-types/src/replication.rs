//! Shared checked replication identities; constructing one grants no authority.

use std::num::NonZeroU64;

/// Nonzero lineage-shared leadership fence, distinct from a transaction position.
/// Constructing a value never grants leadership or authorizes its persistence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LeadershipEpochV1(NonZeroU64);

impl LeadershipEpochV1 {
    /// Reconstructs a nonzero fence; zero is never an active leadership epoch.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Initial leadership fence for receipted activation.
    #[must_use]
    pub const fn initial() -> Self {
        Self(NonZeroU64::MIN)
    }

    /// Returns the exact nonzero position.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Preflights a promotion fence without mutation; exhaustion never wraps.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        match self.get().checked_add(1) {
            Some(next) => Self::new(next),
            None => None,
        }
    }
}

/// Opaque storage-local hold identity, never a NodeId or authority token.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReplicationSourceHoldIdV1([u8; 16]);

impl ReplicationSourceHoldIdV1 {
    /// Zero is unassigned. Constructing an ID grants no mutation capability.
    #[must_use]
    pub fn new(bytes: [u8; 16]) -> Option<Self> {
        (bytes != [0; 16]).then_some(Self(bytes))
    }

    /// Canonical bytes for the storage owner's codec and key.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl std::fmt::Debug for ReplicationSourceHoldIdV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationSourceHoldIdV1([redacted])")
    }
}

/// A request-selected follower registration in one exact source lineage.
/// This target grants no authority and contains no transport or credential data.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct ReplicationFollowerAuditTargetV1 {
    database_id: crate::DatabaseId,
    history_incarnation: NonZeroU64,
    leadership_epoch: LeadershipEpochV1,
    hold_id: ReplicationSourceHoldIdV1,
}

impl ReplicationFollowerAuditTargetV1 {
    /// Checks the remaining nonzero lineage component before target construction.
    #[must_use]
    pub const fn new(
        database_id: crate::DatabaseId,
        history_incarnation: u64,
        leadership_epoch: LeadershipEpochV1,
        hold_id: ReplicationSourceHoldIdV1,
    ) -> Option<Self> {
        match NonZeroU64::new(history_incarnation) {
            Some(history_incarnation) => Some(Self {
                database_id,
                history_incarnation,
                leadership_epoch,
                hold_id,
            }),
            None => None,
        }
    }

    /// Exact source database identity.
    #[must_use]
    pub const fn database_id(self) -> crate::DatabaseId {
        self.database_id
    }
    /// Nonzero source history incarnation.
    #[must_use]
    pub const fn history_incarnation(self) -> u64 {
        self.history_incarnation.get()
    }
    /// Nonzero source leadership epoch.
    #[must_use]
    pub const fn leadership_epoch(self) -> LeadershipEpochV1 {
        self.leadership_epoch
    }
    /// Opaque source-local registration identity.
    #[must_use]
    pub const fn hold_id(self) -> ReplicationSourceHoldIdV1 {
        self.hold_id
    }
}

impl std::fmt::Debug for ReplicationFollowerAuditTargetV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationFollowerAuditTargetV1([REDACTED])")
    }
}
